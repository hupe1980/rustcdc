//! pgoutput binary protocol decoder.
//!
//! This module contains all type definitions and decode functions for the PostgreSQL
//! logical replication pgoutput protocol.  It is intentionally kept free of any
//! connection-management or snapshot logic so that it can be read and tested in isolation.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio_postgres::Client;

use crate::core::{Error, Result};

use super::parser;

// ─── pgoutput wire types ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PgColumn {
    pub(super) name: String,
    pub(super) flags: u8,
    pub(super) type_oid: u32,
    pub(super) type_modifier: i32,
}

impl PgColumn {
    pub(super) fn is_key(&self) -> bool {
        (self.flags & 0x01) != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PgValue {
    Null,
    Unchanged,
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PgRelation {
    pub(super) oid: u32,
    pub(super) namespace: String,
    pub(super) name: String,
    pub(super) replica_identity: u8,
    pub(super) columns: Vec<PgColumn>,
}

/// BEGIN message — marks the start of a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PgBegin {
    /// Final LSN of the transaction (the commit LSN).
    pub(super) final_lsn: u64,
    /// Commit timestamp in microseconds since the PostgreSQL epoch (2000-01-01 UTC).
    pub(super) commit_timestamp_us: i64,
    /// Transaction XID.
    pub(super) xid: u32,
}

/// COMMIT message — marks the end of a successfully committed transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PgCommit {
    /// Unused flags byte (reserved for future use).
    pub(super) flags: u8,
    /// LSN of the commit WAL record.
    pub(super) commit_lsn: u64,
    /// LSN immediately after the commit record (next WAL position).
    pub(super) end_lsn: u64,
    /// Commit timestamp in microseconds since the PostgreSQL epoch.
    pub(super) commit_timestamp_us: i64,
}

/// INSERT message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PgInsert {
    pub(super) relation_oid: u32,
    pub(super) new_tuple: Vec<PgValue>,
}

/// UPDATE message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PgUpdate {
    pub(super) relation_oid: u32,
    /// Key-only old tuple (present when replica identity = DEFAULT and key columns changed).
    pub(super) key_tuple: Option<Vec<PgValue>>,
    /// Full old tuple (present when replica identity = FULL).
    pub(super) old_tuple: Option<Vec<PgValue>>,
    pub(super) new_tuple: Vec<PgValue>,
}

/// DELETE message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PgDelete {
    pub(super) relation_oid: u32,
    /// Key-only old tuple (replica identity = DEFAULT/INDEX).
    pub(super) key_tuple: Option<Vec<PgValue>>,
    /// Full old tuple (replica identity = FULL).
    pub(super) old_tuple: Option<Vec<PgValue>>,
}

/// TRUNCATE message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PgTruncate {
    /// Bit 0 = CASCADE, bit 1 = RESTART SEQS.
    pub(super) option_bits: u8,
    pub(super) relation_oids: Vec<u32>,
}

/// A decoded pgoutput protocol message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PgOutputMessage {
    Begin(PgBegin),
    Commit(PgCommit),
    Relation(PgRelation),
    Insert(PgInsert),
    Update(PgUpdate),
    Delete(PgDelete),
    Truncate(PgTruncate),
    /// Message type not handled by this decoder (Origin, Type, LogicalMessage, etc.).
    Unknown(u8),
}

// ─── Pgoutput binary decoder ─────────────────────────────────────────────────

/// Cursor over a byte slice for sequential big-endian decoding.
pub(super) struct BytesCursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> BytesCursor<'a> {
    pub(super) fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub(super) fn read_u8(&mut self) -> Result<u8> {
        self.data
            .get(self.pos)
            .ok_or_else(|| {
                Error::SourceError("unexpected end of pgoutput message reading u8".into())
            })
            .map(|&b| {
                self.pos += 1;
                b
            })
    }

    pub(super) fn read_u16_be(&mut self) -> Result<u16> {
        let b = self.read_n_bytes(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    pub(super) fn read_u32_be(&mut self) -> Result<u32> {
        let b = self.read_n_bytes(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub(super) fn read_i32_be(&mut self) -> Result<i32> {
        let b = self.read_n_bytes(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub(super) fn read_u64_be(&mut self) -> Result<u64> {
        let b = self.read_n_bytes(8)?;
        Ok(u64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    pub(super) fn read_i64_be(&mut self) -> Result<i64> {
        let b = self.read_n_bytes(8)?;
        Ok(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    pub(super) fn read_cstring(&mut self) -> Result<String> {
        let start = self.pos;
        while self.pos < self.data.len() && self.data[self.pos] != 0 {
            self.pos += 1;
        }
        if self.pos >= self.data.len() {
            return Err(Error::SourceError(
                "unterminated cstring in pgoutput message".into(),
            ));
        }
        let s = std::str::from_utf8(&self.data[start..self.pos])
            .map_err(|error| {
                Error::SourceError(format!("non-UTF8 cstring in pgoutput message: {error}"))
            })?
            .to_string();
        self.pos += 1;
        Ok(s)
    }

    pub(super) fn read_n_bytes(&mut self, n: usize) -> Result<&[u8]> {
        let end = self.pos + n;
        if end > self.data.len() {
            return Err(Error::SourceError(format!(
                "unexpected end of pgoutput message: need {n} bytes at offset {} but only {} remain",
                self.pos,
                self.data.len() - self.pos
            )));
        }
        let slice = &self.data[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
}

pub(super) fn decode_pgoutput_message(data: &[u8]) -> Result<PgOutputMessage> {
    if data.is_empty() {
        return Err(Error::SourceError("empty pgoutput message".into()));
    }
    let mut cur = BytesCursor::new(data);
    match cur.read_u8()? {
        b'B' => Ok(PgOutputMessage::Begin(decode_begin(&mut cur)?)),
        b'C' => Ok(PgOutputMessage::Commit(decode_commit(&mut cur)?)),
        b'R' => Ok(PgOutputMessage::Relation(decode_relation(&mut cur)?)),
        b'I' => Ok(PgOutputMessage::Insert(decode_insert(&mut cur)?)),
        b'U' => Ok(PgOutputMessage::Update(decode_update(&mut cur)?)),
        b'D' => Ok(PgOutputMessage::Delete(decode_delete(&mut cur)?)),
        b'T' => Ok(PgOutputMessage::Truncate(decode_truncate(&mut cur)?)),
        other => Ok(PgOutputMessage::Unknown(other)),
    }
}

fn decode_begin(cur: &mut BytesCursor) -> Result<PgBegin> {
    let final_lsn = cur.read_u64_be()?;
    let commit_timestamp_us = cur.read_i64_be()?;
    let xid = cur.read_u32_be()?;
    Ok(PgBegin {
        final_lsn,
        commit_timestamp_us,
        xid,
    })
}

fn decode_commit(cur: &mut BytesCursor) -> Result<PgCommit> {
    let flags = cur.read_u8()?;
    let commit_lsn = cur.read_u64_be()?;
    let end_lsn = cur.read_u64_be()?;
    let commit_timestamp_us = cur.read_i64_be()?;
    Ok(PgCommit {
        flags,
        commit_lsn,
        end_lsn,
        commit_timestamp_us,
    })
}

fn decode_relation(cur: &mut BytesCursor) -> Result<PgRelation> {
    let oid = cur.read_u32_be()?;
    let namespace = cur.read_cstring()?;
    let name = cur.read_cstring()?;
    let replica_identity = cur.read_u8()?;
    let ncols = cur.read_u16_be()?;
    let mut columns = Vec::with_capacity(ncols as usize);
    for _ in 0..ncols {
        let flags = cur.read_u8()?;
        let name = cur.read_cstring()?;
        let type_oid = cur.read_u32_be()?;
        let type_modifier = cur.read_i32_be()?;
        columns.push(PgColumn {
            name,
            flags,
            type_oid,
            type_modifier,
        });
    }
    Ok(PgRelation {
        oid,
        namespace,
        name,
        replica_identity,
        columns,
    })
}

fn decode_insert(cur: &mut BytesCursor) -> Result<PgInsert> {
    let relation_oid = cur.read_u32_be()?;
    let marker = cur.read_u8()?;
    if marker != b'N' {
        return Err(Error::SourceError(format!(
            "expected 'N' marker in INSERT message, got {marker:#x}"
        )));
    }
    let new_tuple = decode_tuple_data(cur)?;
    Ok(PgInsert {
        relation_oid,
        new_tuple,
    })
}

fn decode_update(cur: &mut BytesCursor) -> Result<PgUpdate> {
    let relation_oid = cur.read_u32_be()?;
    let marker = cur.read_u8()?;
    let (key_tuple, old_tuple, new_tuple) = match marker {
        b'K' => {
            let key = decode_tuple_data(cur)?;
            let next = cur.read_u8()?;
            if next != b'N' {
                return Err(Error::SourceError(format!(
                    "expected 'N' after key tuple in UPDATE, got {next:#x}"
                )));
            }
            let new = decode_tuple_data(cur)?;
            (Some(key), None, new)
        }
        b'O' => {
            let old = decode_tuple_data(cur)?;
            let next = cur.read_u8()?;
            if next != b'N' {
                return Err(Error::SourceError(format!(
                    "expected 'N' after old tuple in UPDATE, got {next:#x}"
                )));
            }
            let new = decode_tuple_data(cur)?;
            (None, Some(old), new)
        }
        b'N' => {
            let new = decode_tuple_data(cur)?;
            (None, None, new)
        }
        other => {
            return Err(Error::SourceError(format!(
                "unknown UPDATE marker: {other:#x}"
            )));
        }
    };
    Ok(PgUpdate {
        relation_oid,
        key_tuple,
        old_tuple,
        new_tuple,
    })
}

fn decode_delete(cur: &mut BytesCursor) -> Result<PgDelete> {
    let relation_oid = cur.read_u32_be()?;
    let marker = cur.read_u8()?;
    let (key_tuple, old_tuple) = match marker {
        b'K' => (Some(decode_tuple_data(cur)?), None),
        b'O' => (None, Some(decode_tuple_data(cur)?)),
        other => {
            return Err(Error::SourceError(format!(
                "unknown DELETE marker: {other:#x}"
            )));
        }
    };
    Ok(PgDelete {
        relation_oid,
        key_tuple,
        old_tuple,
    })
}

fn decode_truncate(cur: &mut BytesCursor) -> Result<PgTruncate> {
    let num_rels = usize::try_from(cur.read_u32_be()?).unwrap_or(0);
    let option_bits = cur.read_u8()?;
    let mut relation_oids = Vec::with_capacity(num_rels);
    for _ in 0..num_rels {
        relation_oids.push(cur.read_u32_be()?);
    }
    Ok(PgTruncate {
        option_bits,
        relation_oids,
    })
}

pub(super) fn decode_tuple_data(cur: &mut BytesCursor) -> Result<Vec<PgValue>> {
    let num_cols = usize::from(cur.read_u16_be()?);
    let mut values = Vec::with_capacity(num_cols);
    for _ in 0..num_cols {
        let datum_kind = cur.read_u8()?;
        let value = match datum_kind {
            b'n' => PgValue::Null,
            b'u' => PgValue::Unchanged,
            b't' => {
                let len = usize::try_from(cur.read_i32_be()?).map_err(|_| {
                    Error::SourceError("negative text datum length in pgoutput tuple".into())
                })?;
                let bytes = cur.read_n_bytes(len)?;
                let text = std::str::from_utf8(bytes)
                    .map_err(|error| {
                        Error::SourceError(format!(
                            "non-UTF8 text datum in pgoutput tuple: {error}"
                        ))
                    })?
                    .to_string();
                PgValue::Text(text)
            }
            b'b' => {
                // Binary datum (pgoutput v2+): hex-encode for safe transport.
                let len = usize::try_from(cur.read_i32_be()?).map_err(|_| {
                    Error::SourceError("negative binary datum length in pgoutput tuple".into())
                })?;
                let bytes = cur.read_n_bytes(len)?;
                let hex = bytes
                    .iter()
                    .fold(String::with_capacity(len * 2 + 2), |mut acc, b| {
                        use std::fmt::Write;
                        let _ = write!(acc, "{b:02x}");
                        acc
                    });
                PgValue::Text(format!("\\x{hex}"))
            }
            other => {
                return Err(Error::SourceError(format!(
                    "unknown datum kind {other:#x} in pgoutput tuple"
                )));
            }
        };
        values.push(value);
    }
    Ok(values)
}

// ─── Streaming provider ──────────────────────────────────────────────────────

#[derive(Debug)]
pub(super) struct PgOutputXLogData {
    pub(super) lsn: u64,
    pub(super) data: Vec<u8>,
}

/// Result of a single WAL poll.
///
/// The distinction between [`PollOutcome::Data`] with an empty vector and
/// [`PollOutcome::TimedOut`] is load-bearing: an empty batch means the slot is
/// genuinely caught up and it is safe to advance it during idle periods, whereas a
/// timed-out poll means there is *more* backlog than could be decoded. Collapsing
/// the two lets an idle-advance discard un-consumed WAL.
#[derive(Debug)]
pub(super) enum PollOutcome {
    /// The poll completed. The vector may be empty, which means the slot is caught up.
    Data(Vec<PgOutputXLogData>),
    /// The poll exceeded its time budget before the server finished decoding.
    /// The slot must **not** be treated as idle.
    TimedOut,
}

impl PollOutcome {
    /// Whether the slot is provably caught up — i.e. a completed poll with no rows.
    pub(super) fn is_caught_up(&self) -> bool {
        matches!(self, Self::Data(rows) if rows.is_empty())
    }

    /// Consume into the decoded rows, discarding the timeout distinction.
    pub(super) fn into_rows(self) -> Vec<PgOutputXLogData> {
        match self {
            Self::Data(rows) => rows,
            Self::TimedOut => Vec::new(),
        }
    }
}

#[async_trait]
pub(super) trait PgOutputMessageProvider: Send + Sync {
    /// Poll for up to `max_messages` decoded WAL rows.
    ///
    /// The implementation **must** return within `poll_timeout`. For the
    /// SQL-polling backend this is enforced via a server-side `statement_timeout`
    /// so the underlying connection is always returned to a ready state before the
    /// next call; that cancellation surfaces as [`PollOutcome::TimedOut`].
    async fn poll_xlog_data(
        &mut self,
        max_messages: usize,
        poll_timeout: Duration,
    ) -> Result<PollOutcome>;
    async fn confirm_lsn(&mut self, lsn: u64) -> Result<()>;
    /// Advance the replication slot to the current WAL LSN during idle periods.
    ///
    /// Called when no events have been delivered for `slot_idle_advance_interval_ms`.
    /// Prevents PostgreSQL from accumulating WAL indefinitely when there are no
    /// committed events (e.g., aborted-transaction bursts, idle upstream).
    ///
    /// Returns the current WAL lag in bytes (`pg_current_wal_lsn - confirmed_flush_lsn`)
    /// after the advance (or `0` if the slot is already current). This value is
    /// forwarded to the metrics collector as `rustcdc_replication_slot_lag_bytes`.
    ///
    /// The default implementation returns `Ok(0)`, used by test doubles.
    async fn idle_advance(&mut self) -> Result<u64> {
        Ok(0)
    }
}

pub(super) struct LivePgOutputMessageProvider {
    pub(super) client: Arc<Client>,
    pub(super) slot_name: String,
    pub(super) publication_name: String,
    pub(super) confirmed_lsn: u64,
}

#[async_trait]
impl PgOutputMessageProvider for LivePgOutputMessageProvider {
    async fn poll_xlog_data(
        &mut self,
        max_messages: usize,
        poll_timeout: Duration,
    ) -> Result<PollOutcome> {
        // pg_logical_slot_peek_binary_changes expects upto_nchanges as int4.
        let capped = i32::try_from(max_messages.max(1)).unwrap_or(i32::MAX);

        // Enforce the per-poll time budget server-side via statement_timeout.
        // This produces a proper QueryCanceled response over the normal protocol,
        // leaving the connection in a valid state — unlike dropping the query
        // future mid-flight which can corrupt the tokio-postgres connection.
        let timeout_ms = u64::try_from(poll_timeout.as_millis()).unwrap_or(u64::MAX);
        self.client
            .execute(&format!("SET statement_timeout = {timeout_ms}"), &[])
            .await
            .map_err(|e| Error::SourceError(format!("failed setting statement_timeout: {e}")))?;

        let result = self
            .client
            .query(
                "SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes($1, NULL, \
                 $2, 'proto_version', '1', 'publication_names', $3)",
                &[
                    &self.slot_name as &(dyn tokio_postgres::types::ToSql + Sync),
                    &capped,
                    &self.publication_name,
                ],
            )
            .await;

        // Always reset statement_timeout to 0 (no limit) after the poll completes,
        // regardless of outcome. The `Arc<Client>` is shared with snapshot chunk
        // queries (incremental snapshot) and the confirm_lsn advance query — those
        // must not inherit the short streaming poll budget.
        let _ = self.client.execute("SET statement_timeout = 0", &[]).await;

        let rows = match result {
            Ok(rows) => rows,
            Err(ref e) if e.code() == Some(&tokio_postgres::error::SqlState::QUERY_CANCELED) => {
                // A cancelled poll is NOT an empty backlog — it is the opposite. The
                // peek is non-consuming, so it re-decodes the whole un-acked backlog on
                // every call; a timeout means that backlog has grown large enough not to
                // decode within the budget. Reporting this as "no data" would let the
                // caller conclude the slot is idle and advance it to
                // `pg_current_wal_lsn()`, permanently discarding exactly the backlog
                // that caused the timeout. Surface it distinctly instead.
                tracing::warn!(
                    target: "rustcdc::source::postgres",
                    slot = %self.slot_name,
                    timeout_ms,
                    "postgres poll_xlog_data hit statement_timeout; the un-acked WAL \
                     backlog could not be decoded within the poll budget. Treating this \
                     as backlog pressure, not as an idle slot.",
                );
                return Ok(PollOutcome::TimedOut);
            }
            Err(e) => {
                return Err(parser::map_pgoutput_poll_error(
                    &self.slot_name,
                    &e.to_string(),
                ));
            }
        };

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            let lsn_text: String = row.get(0);
            let data: Vec<u8> = row.get(1);
            messages.push(PgOutputXLogData {
                lsn: parser::parse_pg_lsn(&lsn_text)?,
                data,
            });
        }

        Ok(PollOutcome::Data(messages))
    }

    async fn confirm_lsn(&mut self, lsn: u64) -> Result<()> {
        // Use strict `<` (not `<=`) so that confirm_lsn(X) still calls
        // pg_replication_slot_advance when X equals the LSN last set by
        // idle_advance.  Without this, a transaction committing at exactly
        // the idle-advance LSN silently skips the SQL, the checkpoint is
        // never written, the idempotency guard deduplicates the same events
        // on the next poll, and the pipeline stalls permanently.
        if lsn < self.confirmed_lsn {
            return Ok(());
        }

        let lsn_text = parser::format_pg_lsn(lsn);
        self.client
            .query_opt(
                "SELECT 1 FROM pg_replication_slot_advance($1::text::name, $2::text::pg_lsn)",
                &[
                    &self.slot_name as &(dyn tokio_postgres::types::ToSql + Sync),
                    &lsn_text as &(dyn tokio_postgres::types::ToSql + Sync),
                ],
            )
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "failed advancing replication slot '{}' to {}: {error}",
                    self.slot_name, lsn_text
                ))
            })?;
        self.confirmed_lsn = lsn;
        Ok(())
    }

    async fn idle_advance(&mut self) -> Result<u64> {
        // Query the current write-ahead log LSN and advance the replication slot
        // to that position. This releases WAL segments that PostgreSQL would
        // otherwise retain indefinitely when no committed events are flowing.
        let rows = self
            .client
            .query("SELECT pg_current_wal_lsn()::text", &[])
            .await
            .map_err(|e| {
                Error::SourceError(format!(
                    "idle advance: failed querying current WAL LSN: {e}"
                ))
            })?;

        let Some(row) = rows.first() else {
            return Ok(0);
        };
        let lsn_text: String = row.get(0);
        let current_lsn = parser::parse_pg_lsn(&lsn_text)?;

        // Compute lag before advancing so we return the pre-advance slack.
        let lag_bytes = current_lsn.saturating_sub(self.confirmed_lsn);

        if current_lsn > self.confirmed_lsn {
            self.client
                .query_opt(
                    "SELECT 1 FROM pg_replication_slot_advance($1::text::name, $2::text::pg_lsn)",
                    &[
                        &self.slot_name as &(dyn tokio_postgres::types::ToSql + Sync),
                        &lsn_text as &(dyn tokio_postgres::types::ToSql + Sync),
                    ],
                )
                .await
                .map_err(|e| {
                    Error::SourceError(format!(
                        "idle advance: failed advancing slot '{}' to {}: {e}",
                        self.slot_name, lsn_text
                    ))
                })?;
            self.confirmed_lsn = current_lsn;
            tracing::debug!(
                target: "rustcdc::source::postgres",
                slot = %self.slot_name,
                lsn = %lsn_text,
                lag_bytes,
                "postgres replication slot advanced during idle period",
            );
        }
        Ok(lag_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_begin_msg() -> Vec<u8> {
        let mut msg = vec![b'B'];
        msg.extend_from_slice(&100u64.to_be_bytes()); // final_lsn
        msg.extend_from_slice(&999i64.to_be_bytes()); // commit_timestamp_us
        msg.extend_from_slice(&42u32.to_be_bytes()); // xid
        msg
    }

    fn make_relation_msg(oid: u32, schema: &str, table: &str, cols: &[(&str, u32)]) -> Vec<u8> {
        let mut msg = vec![b'R'];
        msg.extend_from_slice(&oid.to_be_bytes());
        msg.extend_from_slice(schema.as_bytes());
        msg.push(0); // null terminator
        msg.extend_from_slice(table.as_bytes());
        msg.push(0);
        msg.push(b'd'); // replica_identity = DEFAULT
        let ncols = cols.len() as u16;
        msg.extend_from_slice(&ncols.to_be_bytes());
        for (name, type_oid) in cols {
            msg.push(0x01); // flags: is_key
            msg.extend_from_slice(name.as_bytes());
            msg.push(0);
            msg.extend_from_slice(&type_oid.to_be_bytes());
            msg.extend_from_slice(&(-1i32).to_be_bytes()); // type_modifier
        }
        msg
    }

    #[test]
    fn decode_begin_roundtrip() {
        let msg = make_begin_msg();
        let decoded = decode_pgoutput_message(&msg).unwrap();
        match decoded {
            PgOutputMessage::Begin(b) => {
                assert_eq!(b.final_lsn, 100);
                assert_eq!(b.commit_timestamp_us, 999);
                assert_eq!(b.xid, 42);
            }
            other => panic!("expected Begin, got {other:?}"),
        }
    }

    #[test]
    fn decode_relation_roundtrip() {
        let msg = make_relation_msg(1234, "public", "users", &[("id", 23)]);
        let decoded = decode_pgoutput_message(&msg).unwrap();
        match decoded {
            PgOutputMessage::Relation(r) => {
                assert_eq!(r.oid, 1234);
                assert_eq!(r.namespace, "public");
                assert_eq!(r.name, "users");
                assert_eq!(r.columns.len(), 1);
                assert_eq!(r.columns[0].name, "id");
                assert!(r.columns[0].is_key());
            }
            other => panic!("expected Relation, got {other:?}"),
        }
    }

    #[test]
    fn decode_unknown_message_type() {
        let msg = vec![b'X'];
        let decoded = decode_pgoutput_message(&msg).unwrap();
        assert!(matches!(decoded, PgOutputMessage::Unknown(b'X')));
    }

    #[test]
    fn decode_empty_message_errors() {
        let result = decode_pgoutput_message(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn bytes_cursor_unterminated_cstring_errors() {
        // A cstring with no null terminator should error.
        let mut cur = BytesCursor::new(b"hello");
        assert!(cur.read_cstring().is_err());
    }

    /// Regression test for the idle-advance stall bug:
    /// `confirm_lsn(X)` must call `pg_replication_slot_advance` even when
    /// `X == confirmed_lsn` (i.e. the same LSN set by a prior `idle_advance`).
    /// The old `<=` guard would skip the SQL, leaving the checkpoint unwritten
    /// and causing a permanent silent stall via the idempotency filter.
    #[tokio::test]
    async fn confirm_lsn_calls_advance_when_lsn_equals_confirmed() {
        use std::sync::{Arc, Mutex};

        // Track how many times advance is called and at what LSN.
        let advance_calls: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::clone(&advance_calls);

        struct SpyProvider {
            confirmed_lsn: u64,
            advance_calls: Arc<Mutex<Vec<u64>>>,
        }

        #[async_trait::async_trait]
        impl PgOutputMessageProvider for SpyProvider {
            async fn poll_xlog_data(
                &mut self,
                _max: usize,
                _timeout: std::time::Duration,
            ) -> crate::core::Result<PollOutcome> {
                Ok(PollOutcome::Data(vec![]))
            }

            async fn confirm_lsn(&mut self, lsn: u64) -> crate::core::Result<()> {
                if lsn < self.confirmed_lsn {
                    return Ok(());
                }
                self.advance_calls.lock().unwrap().push(lsn);
                self.confirmed_lsn = lsn;
                Ok(())
            }
        }

        let provider = SpyProvider {
            confirmed_lsn: 200, // simulates state after idle_advance to LSN 200
            advance_calls: Arc::clone(&calls),
        };

        // confirm_lsn(200) — same as idle-advance LSN — must NOT be skipped.
        let mut p = provider;
        p.confirm_lsn(200).await.unwrap();
        assert_eq!(
            *calls.lock().unwrap(),
            vec![200],
            "confirm_lsn must call advance when lsn == confirmed_lsn (idle-advance stall regression)"
        );

        // confirm_lsn(150) — strictly below — must be skipped.
        calls.lock().unwrap().clear();
        p.confirm_lsn(150).await.unwrap();
        assert!(
            calls.lock().unwrap().is_empty(),
            "confirm_lsn must skip advance when lsn < confirmed_lsn"
        );
    }
}
