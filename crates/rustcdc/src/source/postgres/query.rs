use std::time::Duration;

use tokio_postgres::Client;

#[cfg(feature = "tls")]
pub(super) use crate::core::rustls_client_config;
#[cfg(feature = "tls")]
pub(super) use crate::core::transport_tls::build_tls_client_config;

use crate::core::{Error, Result};

use super::parser::quote_pg_identifier;

use super::parser::{format_pg_lsn, parse_pg_lsn};

/// Abstraction over the two Postgres I/O operations required by startup slot
/// reconciliation.  Allows the self-heal logic to be unit-tested without a
/// live database connection.
pub(super) trait ReconcileOps {
    async fn query_confirmed_lsn(&self, slot_name: &str) -> Result<u64>;
    async fn advance_slot(&self, slot_name: &str, lsn: u64) -> Result<()>;
}

impl ReconcileOps for Client {
    async fn query_confirmed_lsn(&self, slot_name: &str) -> Result<u64> {
        query_slot_confirmed_lsn(self, slot_name).await
    }

    async fn advance_slot(&self, slot_name: &str, lsn: u64) -> Result<()> {
        advance_replication_slot(self, slot_name, lsn).await
    }
}

/// Primary-key columns of every table in `publication`, keyed by `(schema, table)`.
///
/// # Why the stream needs this at all
///
/// pgoutput's RELATION message flags each column with `LOGICALREP_IS_REPLICA_IDENTITY`, and that
/// flag is **not** "part of the primary key". PostgreSQL sets it on *every* column of a table with
/// `REPLICA IDENTITY FULL` — its own source says so: "REPLICA IDENTITY FULL means all columns are
/// sent as part of key." Reading the flag as a primary key therefore reports the whole row as the
/// key for such a table, which:
///
/// * makes the key change whenever **any** column changes, so a log-compacted topic can never
///   collapse a row's history and per-key routing sends one row's versions to several partitions;
/// * disagrees with the snapshot path, which reads the real key from this catalog — so the same
///   row is keyed one way while being snapshotted and another way while being streamed, defeating
///   the handoff's deduplication and the idempotency digest;
/// * turns an unchanged-TOAST update into no write at all, because one of the "key" columns is
///   unavailable and a partial key must be refused rather than widened.
///
/// The flag is right for `DEFAULT` and `INDEX` identities, where it names the primary key or the
/// nominated index. Only `FULL` needs this lookup, and only the catalog can answer it.
///
/// Ordering matters: a composite key is returned in index order, matching
/// [`query_primary_key_columns_and_types`] so the two paths produce identical keys.
pub(super) async fn query_publication_primary_keys(
    client: &Client,
    publication: &str,
) -> Result<std::collections::HashMap<(String, String), Vec<String>>> {
    let rows = client
        .query(
            "
            SELECT
              published.schemaname,
              published.tablename,
              attribute.attname
            FROM pg_catalog.pg_publication_tables published
            JOIN pg_catalog.pg_class class_def
              ON class_def.relname = published.tablename
            JOIN pg_catalog.pg_namespace namespace_def
              ON namespace_def.oid = class_def.relnamespace
             AND namespace_def.nspname = published.schemaname
            JOIN pg_catalog.pg_index index_def
              ON index_def.indrelid = class_def.oid
             AND index_def.indisprimary
            JOIN LATERAL unnest(index_def.indkey) WITH ORDINALITY AS key_attnum(attnum, ord) ON TRUE
            JOIN pg_catalog.pg_attribute attribute
              ON attribute.attrelid = class_def.oid
             AND attribute.attnum = key_attnum.attnum
            WHERE published.pubname = $1
            ORDER BY published.schemaname, published.tablename, key_attnum.ord
            ",
            &[&publication],
        )
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "failed querying primary keys for publication '{publication}': {error}"
            ))
        })?;

    let mut keys: std::collections::HashMap<(String, String), Vec<String>> =
        std::collections::HashMap::new();
    for row in rows {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let column: String = row.get(2);
        keys.entry((schema, table)).or_default().push(column);
    }
    Ok(keys)
}

pub(super) async fn query_primary_key_columns_and_types(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<(Vec<String>, Vec<String>)> {
    let rows = client
        .query(
            "
            SELECT
              attribute.attname,
              pg_catalog.format_type(attribute.atttypid, attribute.atttypmod)
            FROM pg_catalog.pg_index index_def
            JOIN pg_catalog.pg_class class_def ON class_def.oid = index_def.indrelid
            JOIN pg_catalog.pg_namespace namespace_def ON namespace_def.oid = class_def.relnamespace
            JOIN LATERAL unnest(index_def.indkey) WITH ORDINALITY AS key_attnum(attnum, ord) ON TRUE
            JOIN pg_catalog.pg_attribute attribute
              ON attribute.attrelid = index_def.indrelid
             AND attribute.attnum = key_attnum.attnum
            WHERE index_def.indisprimary
              AND namespace_def.nspname = $1
              AND class_def.relname = $2
            ORDER BY key_attnum.ord
            ",
            &[&schema, &table],
        )
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "failed querying primary key columns for table '{schema}.{table}': {error}"
            ))
        })?;

    let mut columns = Vec::with_capacity(rows.len());
    let mut types = Vec::with_capacity(rows.len());
    for row in rows {
        columns.push(row.get::<usize, String>(0));
        types.push(row.get::<usize, String>(1));
    }

    Ok((columns, types))
}

pub(super) async fn reconcile_stream_resume_lsn_with_retry(
    client: &Client,
    checkpoint_lsn: u64,
    slot_name: &str,
    attempts: usize,
    retry_delay: Duration,
) -> Result<u64> {
    reconcile_with_ops(client, checkpoint_lsn, slot_name, attempts, retry_delay).await
}

/// Core reconciliation logic, decoupled from I/O for unit-testability.
/// See [`reconcile_stream_resume_lsn_with_retry`] for the production entry point.
async fn reconcile_with_ops(
    ops: &impl ReconcileOps,
    checkpoint_lsn: u64,
    slot_name: &str,
    attempts: usize,
    retry_delay: Duration,
) -> Result<u64> {
    let attempts = attempts.max(1);
    let mut last_slot_lsn = 0_u64;

    for attempt in 0..attempts {
        let slot_lsn = ops.query_confirmed_lsn(slot_name).await?;
        last_slot_lsn = slot_lsn;
        if checkpoint_lsn <= slot_lsn {
            return Ok(checkpoint_lsn);
        }

        if attempt + 1 < attempts {
            tokio::time::sleep(retry_delay).await;
        }
    }

    // The checkpoint is ahead of the slot's confirmed_flush_lsn.  This happens
    // when a previous `confirm_lsn` call succeeded at the checkpoint layer but
    // failed to advance the replication slot (e.g. transient network error,
    // Postgres restart, or the type-casting bug fixed in 0.6.4).  Rather than
    // returning a fatal "operator intervention required" error that causes an
    // infinite restart loop, self-heal by advancing the slot to the checkpoint
    // position.  The checkpoint guarantees those events were durably processed,
    // so advancing the slot is safe and correct.
    tracing::warn!(
        target: "rustcdc::source::postgres",
        slot_name,
        checkpoint_lsn = %format_pg_lsn(checkpoint_lsn),
        slot_confirmed_lsn = %format_pg_lsn(last_slot_lsn),
        "replication slot behind checkpoint after confirm_lsn failure; \
         self-healing by advancing slot to checkpoint LSN",
    );
    ops.advance_slot(slot_name, checkpoint_lsn).await?;
    Ok(checkpoint_lsn)
}

/// Advance a replication slot to the given LSN.  Used both during startup
/// self-healing (see `reconcile_stream_resume_lsn_with_retry`) and by
/// [`super::decoder::LivePgOutputMessageProvider::confirm_lsn`].
pub(super) async fn advance_replication_slot(
    client: &Client,
    slot_name: &str,
    lsn: u64,
) -> Result<()> {
    let lsn_str = format_pg_lsn(lsn);
    client
        .query(
            "SELECT 1 FROM pg_replication_slot_advance($1::text::name, $2::text::pg_lsn)",
            &[&slot_name.to_string(), &lsn_str],
        )
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "failed to advance replication slot '{slot_name}' to LSN {lsn_str}: {error}"
            ))
        })?;
    Ok(())
}

pub(super) async fn query_current_wal_lsn(client: &Client) -> Result<u64> {
    let lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .map_err(|error| Error::SourceError(format!("failed querying WAL LSN: {error}")))?
        .get(0);
    parse_pg_lsn(&lsn)
}

async fn query_slot_confirmed_lsn(client: &Client, slot_name: &str) -> Result<u64> {
    let row = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "failed querying replication slot state for '{slot_name}': {error}"
            ))
        })?
        .ok_or_else(|| {
            Error::SourceError(format!(
                "replication slot '{slot_name}' not found while validating checkpoint alignment"
            ))
        })?;

    let lsn_text = row.get::<usize, Option<String>>(0).ok_or_else(|| {
        Error::SourceError(format!(
            "replication slot '{slot_name}' has no confirmed_flush_lsn"
        ))
    })?;
    parse_pg_lsn(&lsn_text)
}

/// Every column of `schema.table`, in ordinal order.
///
/// Needed because the row payload is built column by column: see [`row_as_text_json`] for
/// why a whole-row conversion cannot produce the same text the live stream does.
pub(super) async fn query_all_columns(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT attname \
             FROM pg_attribute \
             WHERE attrelid = format('%I.%I', $1::text, $2::text)::regclass \
               AND attnum > 0 AND NOT attisdropped \
             ORDER BY attnum",
            &[&schema, &table],
        )
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "failed reading columns for '{schema}.{table}': {error}"
            ))
        })?;
    Ok(rows.iter().map(|row| row.get::<_, String>(0)).collect())
}

/// Build the row-payload projection: every column cast with its own type output function.
///
/// # Why not `row_to_json(t)`
///
/// Two reasons, and the second is the subtle one.
///
/// 1. **It disagrees with the live stream on type.** `row_to_json` preserves SQL types, so a
///    row backfilled by a snapshot gave `{"id": 1}` while the same row updated a moment later
///    gave `{"id": "1"}` — pgoutput delivers values in text format. A sink reaching for
///    `as_i64()` read one and silently saw `None` for the other. It is also lossy:
///    `numeric(38,4)` and `int8` above 2^53 do not survive a JSON number, which is an
///    IEEE-754 double by the time most consumers see it.
/// 2. **Getting to text is not one conversion but two, and only one of them matches.**
///    Routing through `json_each_text(row_to_json(t))` fixes the type and not the value:
///    `row_to_json` turns a `boolean` into JSON `true`, whose text is `"true"`. Nor does
///    `::text` — that is a *cast*, and PostgreSQL's `bool`→`text` cast also yields `true`.
///    pgoutput emits `t`, because it calls the type's **output function**. `format('%s', …)`
///    is what invokes that same function, so the two paths agree character for character.
///
/// `format` renders SQL NULL as the empty string, which would erase the distinction between
/// a NULL column and an empty one — so each column is guarded by a `CASE`, leaving NULL as
/// SQL NULL for `json_build_object` to render as JSON `null`.
///
/// Verified against PostgreSQL 16:
/// `{"b": "t", "n": null, "e": "", "x": "9223372036854775807"}`.
pub(super) fn row_as_text_json(columns: &[String]) -> String {
    if columns.is_empty() {
        return "'{}'::json::text".to_string();
    }
    let pairs = columns
        .iter()
        .map(|column| {
            let quoted = quote_pg_identifier(column);
            format!(
                "{}, CASE WHEN t.{quoted} IS NULL THEN NULL ELSE format('%s', t.{quoted}) END",
                quote_pg_literal(column)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("json_build_object({pairs})::text")
}

/// Quote a string as a SQL literal, doubling any embedded quote.
fn quote_pg_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    // ── Mock ReconcileOps ─────────────────────────────────────────────────────

    struct MockReconcileOps {
        /// LSN values returned by successive `query_confirmed_lsn` calls (FIFO).
        slot_lsn_sequence: Arc<Mutex<Vec<u64>>>,
        /// Records each `(slot_name, lsn)` pair passed to `advance_slot`.
        advance_calls: Arc<Mutex<Vec<(String, u64)>>>,
    }

    impl MockReconcileOps {
        fn new(slot_lsns: Vec<u64>) -> Self {
            Self {
                slot_lsn_sequence: Arc::new(Mutex::new(slot_lsns)),
                advance_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn advance_calls_snapshot(&self) -> Vec<(String, u64)> {
            self.advance_calls.lock().unwrap().clone()
        }
    }

    impl ReconcileOps for MockReconcileOps {
        async fn query_confirmed_lsn(&self, _slot_name: &str) -> Result<u64> {
            let mut seq = self.slot_lsn_sequence.lock().unwrap();
            if seq.is_empty() {
                return Err(Error::SourceError(
                    "mock: no more slot LSN values configured".into(),
                ));
            }
            Ok(seq.remove(0))
        }

        async fn advance_slot(&self, slot_name: &str, lsn: u64) -> Result<()> {
            self.advance_calls
                .lock()
                .unwrap()
                .push((slot_name.to_string(), lsn));
            Ok(())
        }
    }

    // ── reconcile_with_ops tests ──────────────────────────────────────────────

    /// Normal path: checkpoint == slot_lsn → returns immediately, no advance.
    #[tokio::test]
    async fn reconcile_returns_checkpoint_when_slot_equals_checkpoint() {
        let ops = MockReconcileOps::new(vec![100]);
        let result = reconcile_with_ops(&ops, 100, "demo_slot", 3, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(result, 100);
        assert!(
            ops.advance_calls_snapshot().is_empty(),
            "no advance when slot == checkpoint"
        );
    }

    /// Normal path: slot ahead of checkpoint → returns checkpoint, no advance.
    #[tokio::test]
    async fn reconcile_returns_checkpoint_when_slot_is_ahead() {
        let ops = MockReconcileOps::new(vec![200]); // slot at 200, checkpoint at 100
        let result = reconcile_with_ops(&ops, 100, "demo_slot", 1, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(result, 100);
        assert!(
            ops.advance_calls_snapshot().is_empty(),
            "no advance when slot is ahead of checkpoint"
        );
    }

    /// Self-heal path: checkpoint > slot after all retries → advance is called.
    #[tokio::test]
    async fn reconcile_self_heals_when_checkpoint_ahead_of_slot() {
        let ops = MockReconcileOps::new(vec![50]); // slot at 50, checkpoint at 100
        let result = reconcile_with_ops(&ops, 100, "demo_slot", 1, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(result, 100, "self-heal must return checkpoint_lsn");
        let calls = ops.advance_calls_snapshot();
        assert_eq!(calls.len(), 1, "advance must be called exactly once");
        assert_eq!(
            calls[0],
            ("demo_slot".to_string(), 100),
            "advance must target checkpoint_lsn"
        );
    }

    /// Retry path: slot catches up on second attempt → returns without advancing.
    #[tokio::test]
    async fn reconcile_short_circuits_when_slot_catches_up_during_retry() {
        // First query: slot behind. Second query: slot caught up.
        let ops = MockReconcileOps::new(vec![50, 100]);
        let result = reconcile_with_ops(&ops, 100, "demo_slot", 3, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(result, 100);
        assert!(
            ops.advance_calls_snapshot().is_empty(),
            "no advance when slot eventually catches up within retry budget"
        );
    }

    /// Retry exhaustion: slot stays behind across all attempts → single advance.
    #[tokio::test]
    async fn reconcile_advances_once_after_all_retries_fail() {
        let ops = MockReconcileOps::new(vec![50, 50, 50]);
        let result = reconcile_with_ops(&ops, 100, "demo_slot", 3, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(result, 100);
        let calls = ops.advance_calls_snapshot();
        assert_eq!(
            calls.len(),
            1,
            "advance must be called exactly once after retries"
        );
        assert_eq!(calls[0].1, 100);
    }
}
