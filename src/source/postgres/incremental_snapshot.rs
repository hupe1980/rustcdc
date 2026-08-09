//! PostgreSQL backend for the DBLog incremental snapshot.
//!
//! The watermark algorithm itself lives in
//! [`crate::source::IncrementalSnapshotDriver`]; this module supplies only what is
//! specific to PostgreSQL: WAL LSNs as the position type, keyset-paginated chunk
//! SELECTs against a regular (non-replication) connection, and the
//! [`PostgresOffset`](crate::checkpoint::PostgresOffset) encoding that carries the
//! snapshot state inside the checkpoint record.

use std::sync::Arc;

use async_trait::async_trait;
use tokio_postgres::Client;

use crate::{
    core::{Error, Event, Offset, Result},
    source::{
        ChunkRow, IncrementalSnapshotBackend, IncrementalSnapshotConfig, IncrementalSnapshotDriver,
        IncrementalSnapshotState, SnapshotTable, StreamHandle,
    },
};

use super::query::{query_all_columns, row_as_text_json};
use super::{
    parse_pg_lsn, parse_table_reference, qualified_table_name,
    query_primary_key_columns_and_types, quote_pg_identifier,
};

/// A [`StreamHandle`] that interleaves PostgreSQL chunk reads with the live
/// replication stream.
///
/// Obtain one via `PostgresConnection::start_incremental_snapshot`.
pub type IncrementalSnapshotHandle = IncrementalSnapshotDriver<PostgresSnapshotBackend>;

/// Build the PostgreSQL incremental-snapshot handle.
pub(super) async fn start(
    inner: Box<dyn StreamHandle>,
    query_client: Arc<Client>,
    config: IncrementalSnapshotConfig,
    source_name: String,
    resume: Option<IncrementalSnapshotState>,
) -> Result<IncrementalSnapshotHandle> {
    IncrementalSnapshotDriver::new(
        PostgresSnapshotBackend { query_client },
        inner,
        config,
        source_name,
        resume,
    )
    .await
}

/// Convert a persisted keyset cursor back into the connector's text representation.
///
/// The chunk SELECT binds cursor values as `text` and casts them inside SQL to the
/// column's real type, so every value must render as a scalar string. A cursor whose
/// arity disagrees with the table's primary key is rejected rather than silently
/// ignored: continuing from a truncated cursor would skip rows.
fn decode_pk_cursor(
    cursor: &[serde_json::Value],
    expected_columns: usize,
    qualified: &str,
) -> Result<Vec<String>> {
    if cursor.len() != expected_columns {
        return Err(Error::CheckpointError(format!(
            "incremental snapshot: persisted keyset cursor for '{qualified}' has {} value(s) but \
             the table's primary key has {expected_columns} column(s). The primary key changed \
             since the checkpoint was written; restart the snapshot with a fresh checkpoint \
             directory rather than resuming from an incompatible cursor",
            cursor.len()
        )));
    }

    cursor
        .iter()
        .map(|value| match value {
            serde_json::Value::String(text) => Ok(text.clone()),
            serde_json::Value::Number(number) => Ok(number.to_string()),
            serde_json::Value::Bool(flag) => Ok(flag.to_string()),
            other => Err(Error::CheckpointError(format!(
                "incremental snapshot: persisted keyset cursor for '{qualified}' contains a \
                 non-scalar value ({other}); only scalar primary keys are supported"
            ))),
        })
        .collect()
}

/// Render a table's row filter as a SQL fragment, or nothing when it has none.
///
/// `lead_in` is `" WHERE "` when the filter is the only predicate and `" AND "` when it
/// joins the keyset seek. Parenthesised so an `OR` inside the operator's expression cannot
/// escape and widen the seek — `a > b AND x = 1 OR y = 2` would otherwise return rows
/// before the cursor and re-read them on every chunk.
fn condition_clause(table: &SnapshotTable, lead_in: &str) -> String {
    table
        .condition
        .as_deref()
        .map(|condition| format!("{lead_in}({condition})"))
        .unwrap_or_default()
}

/// A PostgreSQL snapshot's visibility fence: `xmax` plus the in-progress xid list.
///
/// Parsed from `pg_current_snapshot()::text`, whose format is `xmin:xmax:xip1,xip2,…`. Read as
/// one value in one round trip so the two halves cannot disagree.
///
/// Values are truncated to 32 bits, because pgoutput's `BEGIN` carries a bare 32-bit `xid` while
/// this function reports epoch-extended `xid8`. Comparing them unreduced would never match.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PgSnapshotFence {
    /// `xmax`, reduced to 32 bits. A transaction at or above this is invisible.
    xmax: u64,
    /// In-progress xids below `xmax`, reduced to 32 bits.
    xip: std::collections::HashSet<u64>,
    /// `false` when no snapshot was recorded, which disables the visibility half of the test.
    present: bool,
}

impl PgSnapshotFence {
    /// Parse `pg_current_snapshot()::text` — `xmin:xmax:xip1,xip2,…`.
    ///
    /// # Errors
    ///
    /// Returns an error rather than an empty fence for a malformed value. An empty fence silently
    /// disables the visibility test and reopens the race, so guessing is worse than failing.
    fn parse(raw: &str) -> Result<Self> {
        let raw = raw.trim();
        let mut parts = raw.split(':');
        let (Some(_xmin), Some(xmax_raw)) = (parts.next(), parts.next()) else {
            return Err(Error::SourceError(format!(
                "could not parse pg_current_snapshot() '{raw}': expected xmin:xmax:xip"
            )));
        };

        let reduce = |value: &str| -> Result<u64> {
            value
                .trim()
                .parse::<u64>()
                .map(|xid| xid & u64::from(u32::MAX))
                .map_err(|error| {
                    Error::SourceError(format!(
                        "could not parse xid '{value}' in pg_current_snapshot() '{raw}': {error}"
                    ))
                })
        };

        let xmax = reduce(xmax_raw)?;
        let mut xip = std::collections::HashSet::new();
        for entry in parts.flat_map(|list| list.split(',')) {
            if entry.trim().is_empty() {
                continue;
            }
            xip.insert(reduce(entry)?);
        }

        Ok(Self {
            xmax,
            xip,
            present: true,
        })
    }

    /// Whether `xid` was invisible to the snapshot this fence came from.
    ///
    /// PostgreSQL's own rule: `xid >= xmax || xip.contains(xid)`. Both halves matter — `xmax` is
    /// `latestCompletedXid + 1`, so a lone in-flight transaction sits *at* `xmax` and never
    /// appears in `xip`.
    fn was_invisible(&self, xid: u64) -> bool {
        self.present && (xid >= self.xmax || self.xip.contains(&xid))
    }
}

/// A PostgreSQL watermark: a WAL position, and the visibility fence read alongside it.
///
/// `Ord` compares the **LSN only**. Ordering answers "has the stream reached the high watermark?",
/// which is a question about log position. The fence answers a different one — "could the chunk
/// read have seen this?" — and no LSN comparison can express it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgWatermark {
    /// WAL position.
    pub lsn: u64,
    /// Visibility fence, empty for an event's position (an event is one transaction, not a
    /// snapshot).
    snapshot: PgSnapshotFence,
}

impl Ord for PgWatermark {
    /// Compares the LSN only; see the type documentation.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.lsn.cmp(&other.lsn)
    }
}

impl PartialOrd for PgWatermark {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// PostgreSQL half of the incremental snapshot.
pub struct PostgresSnapshotBackend {
    /// Regular (non-replication) connection used for chunk SELECTs and LSN checks.
    /// Never holds a transaction open — that is the point of the DBLog design.
    query_client: Arc<Client>,
}

#[async_trait]
impl IncrementalSnapshotBackend for PostgresSnapshotBackend {
    type Position = PgWatermark;

    async fn describe_table(&mut self, table_ref: &str) -> Result<SnapshotTable> {
        let (schema, name) = parse_table_reference(table_ref)?;
        let (pk_columns, pk_types) =
            query_primary_key_columns_and_types(&self.query_client, &schema, &name).await?;
        let qualified = qualified_table_name(&schema, &name);
        // Needed for the row projection: the payload is built column by column so its text
        // matches what pgoutput produces. See `query::row_as_text_json`.
        let columns = query_all_columns(&self.query_client, &schema, &name).await?;
        Ok(SnapshotTable {
            // Filled in by the driver from `IncrementalSnapshotConfig::table_conditions`.
            condition: None,
            schema,
            name,
            qualified,
            pk_columns,
            pk_types,
            columns,
        })
    }

    /// The current WAL position **and** the snapshot visibility fence, in one round trip.
    ///
    /// Both come from a single `SELECT` so they describe the same instant. The fence is what
    /// closes the commit-visibility race — see
    /// [`event_in_bracket`](Self::event_in_bracket).
    async fn current_position(&mut self) -> Result<PgWatermark> {
        let row = self
            .query_client
            .query_one(
                "SELECT pg_current_wal_lsn()::text, pg_current_snapshot()::text",
                &[],
            )
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "failed reading WAL LSN and snapshot fence: {error}. The incremental \
                     snapshot needs both to bracket a chunk read correctly"
                ))
            })?;

        let lsn: String = row.get(0);
        let snapshot: String = row.get(1);
        Ok(PgWatermark {
            lsn: parse_pg_lsn(&lsn)?,
            snapshot: PgSnapshotFence::parse(&snapshot)?,
        })
    }

    /// Classify a live event against the bracket, using PostgreSQL's own visibility rule.
    ///
    /// # Why the position test alone is wrong
    ///
    /// Committing is not atomic with respect to the WAL. PostgreSQL writes the commit record
    /// (advancing `pg_current_wal_lsn()`), flushes it, and only *then* clears the transaction
    /// from the proc array — which is what makes it visible to new snapshots. So a transaction
    /// can sit at commit LSN 500 with a low watermark of 600 and still be invisible to the chunk
    /// `SELECT`: the chunk holds its pre-image, `position > low` is false, nothing is suppressed,
    /// and the stale value is emitted over the newer one.
    ///
    /// # The rule, which is PostgreSQL's own
    ///
    /// A snapshot is `(xmin, xmax, xip)`, and a transaction is **invisible** to it exactly when
    ///
    /// ```text
    /// xid >= xmax  ||  xip.contains(xid)
    /// ```
    ///
    /// Both halves are needed, and getting that wrong is not academic: `xmax` is
    /// `latestCompletedXid + 1`, so an in-flight transaction whose xid is the highest assigned —
    /// which is the common case for one mid-commit — sits **at or above `xmax` and is therefore
    /// absent from `xip`**. An earlier version of this connector tested `xip` alone and so missed
    /// precisely the transactions the bracket exists to catch. `pg_current_snapshot()` on a
    /// single-writer database reports `733:733:` with 733 in flight: empty `xip`, and the whole
    /// answer in `xmax`.
    ///
    /// # Why this closes the window
    ///
    /// The snapshot is read **after** the low watermark and **before** the chunk read. Let `S` be
    /// the chunk read's snapshot and `S₀` the one recorded here. Any transaction invisible to `S`
    /// is invisible to `S₀` — `S₀` is earlier — so by the rule above it is either at/above
    /// `S₀.xmax` or in `S₀.xip`, and either way this test flags it. Nothing that the chunk could
    /// not see escapes.
    ///
    /// Over-suppression in the other direction is harmless: the chunk row is dropped and the
    /// stream event carries the same or a newer value.
    ///
    /// The upper bound stays the LSN, because "did this commit after the chunk read finished?" is
    /// a question about log position and the high watermark is one.
    fn event_in_bracket(
        &self,
        event: &Event,
        position: &PgWatermark,
        low: &PgWatermark,
        high: &PgWatermark,
    ) -> crate::source::BracketPosition {
        use crate::source::BracketPosition;

        if position.lsn > high.lsn {
            return BracketPosition::After;
        }

        let invisible_to_chunk = position.lsn > low.lsn
            || event
                .transaction
                .as_ref()
                .is_some_and(|tx| low.snapshot.was_invisible(tx.tx_id));

        if invisible_to_chunk {
            BracketPosition::Inside
        } else {
            BracketPosition::Before
        }
    }

    async fn fetch_chunk(
        &mut self,
        table: &SnapshotTable,
        cursor: Option<&[serde_json::Value]>,
        limit: usize,
    ) -> Result<Vec<ChunkRow>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let table_ref = &table.qualified;

        let order_expr = table
            .pk_columns
            .iter()
            .map(|column| format!("t.{}", quote_pg_identifier(column)))
            .collect::<Vec<_>>()
            .join(", ");
        let key_value_expr = table
            .pk_columns
            .iter()
            .map(|column| format!("t.{}::text", quote_pg_identifier(column)))
            .collect::<Vec<_>>()
            .join(", ");

        let raw_rows = if let Some(cursor) = cursor {
            let cursor = decode_pk_cursor(cursor, table.pk_columns.len(), &table.qualified)?;
            // Bind as text and cast inside SQL to the actual PK type, so one code
            // path serves every key type without a per-type `ToSql` match.
            let predicate_expr = table
                .pk_types
                .iter()
                .enumerate()
                .map(|(index, pg_type)| format!("${}::text::{pg_type}", index + 1))
                .collect::<Vec<_>>()
                .join(", ");
            let query = format!(
                "SELECT ARRAY[{key_value_expr}], {row_json} \
                 FROM {table_ref} t \
                 WHERE ({order_expr}) > ({predicate_expr}){filter} \
                 ORDER BY {order_expr} \
                 LIMIT ${}",
                table.pk_columns.len() + 1,
                filter = condition_clause(table, " AND "),
                row_json = row_as_text_json(&table.columns),
            );
            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                Vec::with_capacity(cursor.len() + 1);
            for value in &cursor {
                params.push(value as &(dyn tokio_postgres::types::ToSql + Sync));
            }
            params.push(&limit);
            self.query_client.query(&query, &params).await
        } else {
            let query = format!(
                "SELECT ARRAY[{key_value_expr}], {row_json} \
                 FROM {table_ref} t{filter} \
                 ORDER BY {order_expr} \
                 LIMIT $1",
                filter = condition_clause(table, " WHERE "),
                row_json = row_as_text_json(&table.columns),
            );
            self.query_client.query(&query, &[&limit]).await
        }
        .map_err(|error| {
            Error::SourceError(format!(
                "incremental snapshot chunk failed for '{}': {error}",
                table.qualified
            ))
        })?;

        let mut decoded = Vec::with_capacity(raw_rows.len());
        for row in raw_rows {
            let key_values: Vec<Option<String>> = row.get(0);
            let cursor = key_values
                .into_iter()
                .map(|value| {
                    value.map(serde_json::Value::String).ok_or_else(|| {
                        Error::SourceError(format!(
                            "incremental snapshot: NULL primary-key column for '{}'",
                            table.qualified
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let payload: String = row.get(1);
            let row = serde_json::from_str(&payload).map_err(|error| {
                Error::SerializationError(format!(
                    "incremental snapshot: JSON decode failed for '{}': {error}",
                    table.qualified
                ))
            })?;
            decoded.push(ChunkRow { cursor, row });
        }
        Ok(decoded)
    }

    fn position_of_event(&self, event: &Event) -> Option<PgWatermark> {
        Some(PgWatermark {
            lsn: parse_pg_lsn(&event.source.offset).ok()?,
            // An event is one transaction, not a snapshot; membership is answered against the
            // *watermarks'* fences.
            snapshot: PgSnapshotFence::default(),
        })
    }

    fn render_position(&self, position: &PgWatermark) -> String {
        super::format_pg_lsn(position.lsn)
    }

    fn offset_with_snapshot_state(
        &self,
        inner: &dyn Offset,
        state: IncrementalSnapshotState,
    ) -> Option<Box<dyn Offset>> {
        let encoded = inner.encode().ok()?;
        let mut offset = crate::checkpoint::PostgresOffset::from_bytes(&encoded).ok()?;
        offset.incremental_snapshot = Some(state);
        Some(Box::new(offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_persisted_keyset_cursor_decodes_back_to_the_connector_text_form() {
        // The chunk SELECT binds cursor values as text; a JSON number must survive
        // the round trip through the checkpoint without becoming `"42.0"` or similar.
        let cursor = vec![json!("acme"), json!(42), json!(true)];
        assert_eq!(
            decode_pk_cursor(&cursor, 3, "public.t").expect("scalar cursor decodes"),
            vec!["acme".to_string(), "42".to_string(), "true".to_string()],
        );
    }

    #[test]
    fn a_cursor_whose_arity_no_longer_matches_the_primary_key_is_rejected() {
        // Silently resuming from a truncated cursor would skip every row between the
        // truncated position and the real one, permanently.
        let error = decode_pk_cursor(&[json!(1)], 2, "public.t")
            .expect_err("arity mismatch must be rejected");
        assert!(
            error.to_string().contains("primary key changed"),
            "the error must name the cause and the remedy, got: {error}"
        );
    }

    #[test]
    fn a_non_scalar_cursor_value_is_rejected_rather_than_stringified() {
        // `{"a":1}.to_string()` would produce a value that compares as text and
        // silently mispaginates.
        let error = decode_pk_cursor(&[json!({ "a": 1 })], 1, "public.t")
            .expect_err("object cursor must be rejected");
        assert!(error.to_string().contains("non-scalar"), "got: {error}");
    }
}
