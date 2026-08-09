//! MySQL and MariaDB backend for the DBLog incremental snapshot.
//!
//! The watermark algorithm itself lives in
//! [`crate::source::IncrementalSnapshotDriver`]; this module supplies only what is
//! specific to the binlog protocol: `(file, position)` coordinates as the position
//! type, keyset-paginated chunk SELECTs over the connection pool, and the
//! [`MysqlOffset`](crate::checkpoint::MysqlOffset) encoding that carries the snapshot
//! state inside the checkpoint record.

use async_trait::async_trait;
use mysql_async::{prelude::Queryable, Pool as MySqlPool};

use crate::{
    core::{Error, Event, Offset, Result},
    source::{
        ChunkRow, IncrementalSnapshotBackend, IncrementalSnapshotConfig, IncrementalSnapshotDriver,
        IncrementalSnapshotState, SnapshotTable, StreamHandle,
    },
};

use super::{
    parser::{parse_mysql_source_offset, quoted_mysql_identifier, split_table_reference},
    query::mysql_json_value_to_param,
    state::compare_binlog_position,
};

/// A [`StreamHandle`] that interleaves MySQL chunk reads with the live binlog stream.
///
/// Obtain one via `MysqlConnection::start_incremental_snapshot`.
pub type MysqlIncrementalSnapshotHandle = IncrementalSnapshotDriver<MysqlSnapshotBackend>;

/// Build the MySQL incremental-snapshot handle.
pub(super) async fn start(
    inner: Box<dyn StreamHandle>,
    pool: MySqlPool,
    config: IncrementalSnapshotConfig,
    source_name: String,
    default_database: String,
    resume: Option<IncrementalSnapshotState>,
) -> Result<MysqlIncrementalSnapshotHandle> {
    IncrementalSnapshotDriver::new(
        MysqlSnapshotBackend {
            pool,
            default_database,
        },
        inner,
        config,
        source_name,
        resume,
    )
    .await
}

/// Binlog coordinate: `(file, position)`.
///
/// Ordering is deliberately **not** derived. `"binlog.000010"` sorts before
/// `"binlog.000009"` as text but follows it numerically, so a derived `Ord` would
/// compare positions backwards at every file rollover — and the override window
/// would then suppress rows it should emit, or emit rows it should suppress, with no
/// error anywhere. The connector's own `compare_binlog_position` compares the numeric
/// file suffix first, then the byte offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinlogPos {
    /// Binlog file name, e.g. `binlog.000042`.
    pub file: String,
    /// Byte offset within the file.
    pub position: u32,
    /// `Executed_Gtid_Set` as of this watermark, when the server reports one.
    ///
    /// Empty for an **event's** position — an event carries one transaction, not a set — and
    /// empty on a server with `gtid_mode = OFF`. The connector treats an empty low watermark as
    /// "no GTID information available" and falls back to the ordinal bracket, which is the
    /// pre-0.12 behaviour and its documented residual window.
    ///
    /// This field takes no part in [`Ord`]. Ordering answers "has the stream caught up to the
    /// high watermark yet?", which is a question about the binlog coordinate and is safe on it:
    /// every GTID in a watermark's set was written to the binlog before that watermark was read,
    /// so a stream past the coordinate has decoded them all. Membership answers a different
    /// question — "could the chunk read have seen this?" — and that one the set answers and the
    /// coordinate cannot.
    pub(super) executed_gtids: super::gtid::GtidSet,
}

impl Ord for BinlogPos {
    /// Compares the binlog coordinate only; see [`BinlogPos::executed_gtids`].
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_binlog_position(&self.file, self.position, &other.file, other.position)
    }
}

impl PartialOrd for BinlogPos {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// The single GTID an event carries, from its `<file>:<pos>#gtid=<uuid:seqno>` offset.
///
/// `None` when the offset has no `#gtid=` suffix, which is what `gtid_mode = OFF` produces.
fn event_gtid(event: &Event) -> Option<String> {
    let (_, gtid) = event.source.offset.split_once("#gtid=")?;
    let gtid = gtid.trim();
    (!gtid.is_empty()).then(|| gtid.to_string())
}

/// The ordinal bracket test, factored out so the GTID path's fallback and the trait's default
/// cannot drift apart.
fn default_bracket(
    event: &Event,
    position: &BinlogPos,
    low: &BinlogPos,
    high: &BinlogPos,
) -> crate::source::BracketPosition {
    use crate::source::BracketPosition;

    let _ = event;
    if position > high {
        return BracketPosition::After;
    }
    if position > low {
        BracketPosition::Inside
    } else {
        BracketPosition::Before
    }
}

/// MySQL/MariaDB half of the incremental snapshot.
pub struct MysqlSnapshotBackend {
    pool: MySqlPool,
    /// Schema applied to an unqualified table reference.
    default_database: String,
}

/// Render a table's row filter as a SQL fragment, or nothing when it has none.
///
/// Parenthesised so an `OR` inside the operator's expression cannot escape and widen the
/// keyset seek — `a > b AND x = 1 OR y = 2` would otherwise return rows before the cursor
/// and re-read them on every chunk.
fn condition_clause(table: &SnapshotTable, lead_in: &str) -> String {
    table
        .condition
        .as_deref()
        .map(|condition| format!("{lead_in}({condition})"))
        .unwrap_or_default()
}

#[async_trait]
impl IncrementalSnapshotBackend for MysqlSnapshotBackend {
    type Position = BinlogPos;

    async fn describe_table(&mut self, table_ref: &str) -> Result<SnapshotTable> {
        let (schema_opt, name) = split_table_reference(table_ref)?;
        let schema = schema_opt.unwrap_or_else(|| self.default_database.clone());

        let mut conn = self.pool.get_conn().await.map_err(|error| {
            Error::SourceError(format!(
                "incremental snapshot: failed to acquire mysql connection: {error}"
            ))
        })?;
        let pk_columns: Vec<String> = conn
            .exec(
                "SELECT COLUMN_NAME \
                 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' \
                 ORDER BY ORDINAL_POSITION",
                (&schema, &name),
            )
            .await
            .map_err(|error| {
                Error::ConfigError(format!(
                    "incremental snapshot: PK query failed for '{schema}.{name}': {error}"
                ))
            })?;

        let qualified = format!(
            "{}.{}",
            quoted_mysql_identifier(&schema),
            quoted_mysql_identifier(&name)
        );
        Ok(SnapshotTable {
            // Filled in by the driver from `IncrementalSnapshotConfig::table_conditions`.
            condition: None,
            schema,
            name,
            qualified,
            pk_columns,
            pk_types: Vec::new(),
            columns: Vec::new(),
        })
    }

    /// The server's current binlog coordinate **and** its executed-GTID set.
    ///
    /// # Why the GTID set is part of the watermark
    ///
    /// The coordinate alone cannot bracket a chunk read correctly. With
    /// `binlog_order_commits = ON` (the default) a transaction is written to the binlog in the
    /// **flush** stage and engine-committed afterwards, and `File`/`Position` advance at the
    /// flush. So a transaction can sit *below* the low watermark and still have been invisible to
    /// a chunk `SELECT` that started next — the chunk holds its pre-image, the ordinal test finds
    /// nothing to suppress, and the stale value is emitted over the newer one.
    ///
    /// `Executed_Gtid_Set` is updated **after** the engine commit, so a GTID present in it
    /// belongs to a transaction whose rows are already visible. Bracketing by set difference
    /// asks exactly the right question, and
    /// [`event_in_bracket`](Self::event_in_bracket) does. This is the mechanism Debezium's
    /// read-only incremental snapshot uses, and it requires `gtid_mode = ON`.
    ///
    /// The set comes from the same row as the coordinate — it is the last column of
    /// `SHOW MASTER STATUS` / `SHOW BINARY LOG STATUS` — so it costs no extra round trip.
    ///
    /// # Without GTID mode
    ///
    /// The set is empty and the bracket falls back to the ordinal test, whose residual window is
    /// one commit's flush-to-engine-commit gap and matters only for a row *both* modified inside
    /// it *and* present in the chunk being read. Enable `gtid_mode` to close it; failing that,
    /// snapshot from a quiesced replica or restrict the snapshot with
    /// [`IncrementalSnapshotConfig::table_conditions`](crate::source::IncrementalSnapshotConfig::table_conditions).
    ///
    /// [`IncrementalSnapshotBackend::in_flight_transactions`] stays at its empty default here,
    /// and deliberately: no in-flight *transaction id* is available on a scale a binlog event
    /// shares, so returning InnoDB ids would look plausible and never match. The GTID set closes
    /// the same gap without needing one.
    async fn current_position(&mut self) -> Result<BinlogPos> {
        let mut conn = self.pool.get_conn().await.map_err(|error| {
            Error::SourceError(format!(
                "incremental snapshot: failed to get mysql conn: {error}"
            ))
        })?;
        // `SHOW MASTER STATUS` was removed in MySQL 8.4 in favour of
        // `SHOW BINARY LOG STATUS`. Try the historical spelling first — it is what
        // every supported MariaDB and MySQL 8.0 understands — and fall back on error
        // rather than gating on a version probe.
        let mut row: mysql_async::Row = match conn.query_first("SHOW MASTER STATUS").await {
            Ok(Some(row)) => row,
            Ok(None) => {
                return Err(Error::SourceError(
                    "incremental snapshot: SHOW MASTER STATUS returned no row".into(),
                ));
            }
            Err(primary_error) => conn
                .query_first("SHOW BINARY LOG STATUS")
                .await
                .map_err(|fallback_error| {
                    Error::SourceError(format!(
                        "incremental snapshot: failed to read mysql binary log status (SHOW MASTER STATUS error: {primary_error}; SHOW BINARY LOG STATUS error: {fallback_error})"
                    ))
                })?
                .ok_or_else(|| {
                    Error::SourceError(
                        "incremental snapshot: SHOW BINARY LOG STATUS returned no row".into(),
                    )
                })?,
        };
        let file: String = row.take(0).unwrap_or_default();
        let raw_position: u64 = row.take(1).unwrap_or(4);
        let position = u32::try_from(raw_position).map_err(|_| {
            Error::SourceError(format!(
                "incremental snapshot: mysql binlog position exceeds u32: {raw_position}"
            ))
        })?;

        // `Executed_Gtid_Set` is the last column of `SHOW MASTER STATUS` /
        // `SHOW BINARY LOG STATUS`, so it costs no extra round trip — the row is already here.
        // Absent or empty means `gtid_mode = OFF`, and the bracket falls back to the ordinal
        // test; a *malformed* value is an error rather than an empty set, because silently
        // shrinking a watermark is the failure this whole mechanism exists to avoid.
        let raw_gtids: String = row.take(4).unwrap_or_default();
        let executed_gtids = super::gtid::GtidSet::parse(&raw_gtids).map_err(|error| {
            Error::SourceError(format!(
                "incremental snapshot: could not parse Executed_Gtid_Set '{raw_gtids}': {error}. \
                 Refusing to continue with a partially-parsed watermark, which would suppress \
                 snapshot rows it should emit or emit rows it should suppress."
            ))
        })?;

        Ok(BinlogPos {
            file,
            position,
            executed_gtids,
        })
    }

    /// Classify a live event using **executed-GTID set membership** when the server provides it.
    ///
    /// This is what closes the commit-visibility race on MySQL. `SHOW MASTER STATUS`'s
    /// file-and-position advances at the binlog **flush** stage, before the InnoDB engine commit
    /// that makes rows visible — so a transaction can sit below the low watermark and still have
    /// been invisible to the chunk read, leaving its chunk row unsuppressed and the stale value
    /// emitted over the newer one.
    ///
    /// `Executed_Gtid_Set` is updated **after** the engine commit, so a GTID present in it
    /// belongs to a transaction whose rows are already visible. Bracketing by set difference —
    /// inside iff in `high` and not in `low` — therefore asks exactly the right question. It is
    /// the mechanism Debezium's read-only incremental snapshot uses.
    ///
    /// # Both bounds come from the set, deliberately
    ///
    /// Mixing a set-based lower bound with an ordinal upper bound is unsound and easy to reach by
    /// accident: an event inside the ordinal high bound but absent from `high`'s set committed
    /// *after* that read, so suppressing it would discard the newer value. `After` is therefore
    /// decided by set membership too.
    ///
    /// # When it falls back
    ///
    /// Two cases defer to the default ordinal test, which is the pre-0.12 behaviour and its
    /// documented residual window:
    ///
    /// - **`gtid_mode = OFF`**, so the watermarks carry no set. There is nothing to be done here;
    ///   the residual window is inherent to file-and-position watermarks.
    /// - **An event with no GTID** while the watermarks do have sets. With GTID mode on every
    ///   transaction has one, so this is a non-transactional or synthetic event rather than a
    ///   normal row change. Falling back is conservative: treating a missing GTID as "not in
    ///   `high`" would defer the event past the chunk on no evidence.
    fn event_in_bracket(
        &self,
        event: &Event,
        position: &BinlogPos,
        low: &BinlogPos,
        high: &BinlogPos,
    ) -> crate::source::BracketPosition {
        use crate::source::BracketPosition;

        // No GTID information: the ordinal test is all there is.
        if low.executed_gtids.is_empty() && high.executed_gtids.is_empty() {
            return default_bracket(event, position, low, high);
        }

        let Some(gtid) = event_gtid(event) else {
            return default_bracket(event, position, low, high);
        };

        if !high.executed_gtids.contains_gtid(&gtid) {
            return BracketPosition::After;
        }
        if low.executed_gtids.contains_gtid(&gtid) {
            BracketPosition::Before
        } else {
            BracketPosition::Inside
        }
    }

    async fn fetch_chunk(
        &mut self,
        table: &SnapshotTable,
        cursor: Option<&[serde_json::Value]>,
        limit: usize,
    ) -> Result<Vec<ChunkRow>> {
        let table_ref = &table.qualified;
        let order_expr = table
            .pk_columns
            .iter()
            .map(|column| quoted_mysql_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");

        let mut conn = self.pool.get_conn().await.map_err(|error| {
            Error::SourceError(format!(
                "incremental snapshot: chunk fetch failed to get conn for '{}': {error}",
                table.qualified
            ))
        })?;

        let rows: Vec<mysql_async::Row> = if let Some(cursor) = cursor {
            // MySQL supports the row-value constructor `(pk1, pk2) > (?, ?)`, so a
            // composite key needs no manual expansion into an OR-chain.
            let placeholders = cursor.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let sql = format!(
                "SELECT * FROM {table_ref} AS t WHERE ({order_expr}) > ({placeholders}){filter} \
                 ORDER BY {order_expr} LIMIT {limit}",
                filter = condition_clause(table, " AND "),
            );
            let params: Vec<mysql_async::Value> = cursor
                .iter()
                .map(mysql_json_value_to_param)
                .collect::<Result<Vec<_>>>()?;
            conn.exec(sql, params).await
        } else {
            let sql = format!(
                "SELECT * FROM {table_ref} AS t{filter} ORDER BY {order_expr} LIMIT {limit}",
                filter = condition_clause(table, " WHERE "),
            );
            conn.exec(sql, ()).await
        }
        .map_err(|error| {
            Error::SourceError(format!(
                "incremental snapshot: chunk SELECT failed for '{}': {error}",
                table.qualified
            ))
        })?;

        let mut decoded = Vec::with_capacity(rows.len());
        for row in rows {
            let json = mysql_row_to_json(&row);
            let cursor: Vec<serde_json::Value> = table
                .pk_columns
                .iter()
                .map(|column| json.get(column).cloned().unwrap_or(serde_json::Value::Null))
                .collect();
            if cursor.iter().any(serde_json::Value::is_null) {
                return Err(Error::SourceError(format!(
                    "incremental snapshot: NULL primary-key column for '{}'",
                    table.qualified
                )));
            }
            decoded.push(ChunkRow { cursor, row: json });
        }
        Ok(decoded)
    }

    fn position_of_event(&self, event: &Event) -> Option<BinlogPos> {
        let (file, position) = parse_mysql_source_offset(&event.source.offset)?;
        Some(BinlogPos {
            // An event carries one transaction, not a set; membership is answered against the
            // *watermarks'* sets, never against an event's.
            executed_gtids: super::gtid::GtidSet::default(),
            file: file.to_string(),
            position,
        })
    }

    fn render_position(&self, position: &BinlogPos) -> String {
        format!("{}:{}", position.file, position.position)
    }

    fn offset_with_snapshot_state(
        &self,
        inner: &dyn Offset,
        state: IncrementalSnapshotState,
    ) -> Option<Box<dyn Offset>> {
        let encoded = inner.encode().ok()?;
        let mut offset = crate::checkpoint::MysqlOffset::from_bytes(&encoded).ok()?;
        offset.incremental_snapshot = Some(state);
        Some(Box::new(offset))
    }
}

// ─── Row decoding ─────────────────────────────────────────────────────────────

fn mysql_row_to_json(row: &mysql_async::Row) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (idx, col) in row.columns_ref().iter().enumerate() {
        let name = col.name_str().to_string();
        let value = match row.as_ref(idx) {
            Some(v) => mysql_value_to_json(v),
            None => serde_json::Value::Null,
        };
        map.insert(name, value);
    }
    serde_json::Value::Object(map)
}

/// Convert a chunk-read value using the connector's single shared rule.
///
/// This module used to carry its own near-identical copy, which is how two paths of the
/// same connector can drift on something as load-bearing as the JSON type of a column.
use super::query::mysql_value_to_json;

#[cfg(test)]
mod tests {
    use super::*;

    fn pos(file: &str, position: u32) -> BinlogPos {
        BinlogPos {
            file: file.to_string(),
            position,
            executed_gtids: crate::source::mysql::gtid::GtidSet::default(),
        }
    }

    #[test]
    fn binlog_positions_order_by_file_number_not_by_text() {
        // `"binlog.000010" < "binlog.000009"` lexicographically. Getting this wrong
        // makes the override window compare backwards at every file rollover.
        assert!(pos("binlog.000009", 4) < pos("binlog.000010", 4));
        assert!(pos("binlog.000010", 4) > pos("binlog.000009", 999_999));
    }

    #[test]
    fn within_one_file_positions_order_by_offset() {
        assert!(pos("binlog.000001", 4) < pos("binlog.000001", 120));
        assert_eq!(pos("binlog.000001", 120), pos("binlog.000001", 120));
    }

    #[test]
    fn non_utf8_column_data_is_hex_encoded_rather_than_lossily_transcoded() {
        // A replacement character would be delivered as if it were the stored value.
        let value = mysql_value_to_json(&mysql_common::value::Value::Bytes(vec![0xff, 0xfe]));
        assert_eq!(value, serde_json::Value::String("fffe".to_string()));
    }

    #[test]
    fn utf8_column_data_stays_readable() {
        let value =
            mysql_value_to_json(&mysql_common::value::Value::Bytes(b"caf\xc3\xa9".to_vec()));
        assert_eq!(value, serde_json::Value::String("café".to_string()));
    }
}

#[cfg(test)]
mod gtid_bracket_tests {
    use super::{event_gtid, BinlogPos, MysqlSnapshotBackend};
    use crate::core::{Event, Operation, SourceMetadata, TransactionMetadata};
    use crate::source::{BracketPosition, IncrementalSnapshotBackend};

    const A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

    fn watermark(file: &str, position: u32, gtids: &str) -> BinlogPos {
        BinlogPos {
            file: file.to_string(),
            position,
            executed_gtids: super::super::gtid::GtidSet::parse(gtids).expect("parses"),
        }
    }

    /// An event as the connector renders it: `<file>:<pos>#gtid=<uuid:seqno>`.
    fn event_at(file: &str, position: u32, gtid: Option<&str>) -> Event {
        let offset = match gtid {
            Some(gtid) => format!("{file}:{position}#gtid={gtid}"),
            None => format!("{file}:{position}"),
        };
        Event::builder("users", Operation::Update)
            .schema("app")
            .source(SourceMetadata::new("mysql", offset, 1))
            .ts(1)
            .before(serde_json::json!({ "id": "1" }))
            .after(serde_json::json!({ "id": "1", "v": "2" }))
            .primary_key(["id"])
            .transaction(TransactionMetadata::new(7, 0, None))
            .build()
    }

    fn backend() -> MysqlSnapshotBackend {
        // `event_in_bracket` is pure over its arguments; the pool is never touched.
        MysqlSnapshotBackend {
            pool: mysql_async::Pool::new(
                mysql_async::OptsBuilder::default()
                    .ip_or_hostname("127.0.0.1")
                    .tcp_port(1),
            ),
            default_database: "app".to_string(),
        }
    }

    /// The race this closes, expressed exactly.
    ///
    /// A transaction is written to the binlog in the flush stage and engine-committed after, so
    /// its **coordinate can sit below the low watermark while its rows were still invisible** to
    /// the chunk read. The ordinal test therefore answers `Before` — do not suppress — and the
    /// chunk's stale pre-image is emitted over the newer value.
    ///
    /// Its GTID is absent from the low watermark's executed set precisely because it had not
    /// engine-committed when that set was read, so set membership answers `Inside`.
    #[test]
    fn a_transaction_below_the_low_coordinate_but_not_yet_visible_is_inside_the_bracket() {
        let backend = backend();

        // Coordinate 500 is *below* the low watermark's 600 — the flush already happened.
        let event = event_at("binlog.000001", 500, Some(&format!("{A}:11")));
        let position = watermark("binlog.000001", 500, "");
        let low = watermark("binlog.000001", 600, &format!("{A}:1-10"));
        let high = watermark("binlog.000001", 900, &format!("{A}:1-15"));

        assert_eq!(
            super::default_bracket(&event, &position, &low, &high),
            BracketPosition::Before,
            "the ordinal test is what gets this wrong: 500 <= 600, so it sees nothing to suppress"
        );
        assert_eq!(
            backend.event_in_bracket(&event, &position, &low, &high),
            BracketPosition::Inside,
            "GTID 11 is absent from the low watermark's set because it had not engine-committed \
             when that set was read, so the chunk could not have seen it"
        );
    }

    #[test]
    fn a_transaction_the_chunk_did_see_is_before_the_bracket() {
        let backend = backend();
        let event = event_at("binlog.000001", 700, Some(&format!("{A}:5")));
        assert_eq!(
            backend.event_in_bracket(
                &event,
                &watermark("binlog.000001", 700, ""),
                &watermark("binlog.000001", 600, &format!("{A}:1-10")),
                &watermark("binlog.000001", 900, &format!("{A}:1-15")),
            ),
            BracketPosition::Before,
            "GTID 5 is in the low watermark's set, so it was visible to the chunk read"
        );
    }

    /// Both bounds come from the set. An event inside the ordinal high bound but absent from the
    /// high watermark's set committed *after* that read, so suppressing it would discard the
    /// newer value — this is the mixing that must not happen.
    #[test]
    fn an_event_absent_from_the_high_set_is_after_even_when_its_coordinate_is_below_high() {
        let backend = backend();
        let event = event_at("binlog.000001", 800, Some(&format!("{A}:20")));
        let position = watermark("binlog.000001", 800, "");
        let low = watermark("binlog.000001", 600, &format!("{A}:1-10"));
        let high = watermark("binlog.000001", 900, &format!("{A}:1-15"));

        assert_eq!(
            super::default_bracket(&event, &position, &low, &high),
            BracketPosition::Inside,
            "the ordinal test would suppress it: 800 is inside (600, 900]"
        );
        assert_eq!(
            backend.event_in_bracket(&event, &position, &low, &high),
            BracketPosition::After,
            "GTID 20 is not in the high watermark's set, so it committed after the chunk read \
             finished and the chunk must be emitted before it"
        );
    }

    /// `gtid_mode = OFF`: the watermarks carry no set, and the ordinal test is all there is.
    #[test]
    fn without_gtid_mode_the_ordinal_bracket_is_used_unchanged() {
        let backend = backend();
        for (position, expected) in [
            (500u32, BracketPosition::Before),
            (700, BracketPosition::Inside),
            (950, BracketPosition::After),
        ] {
            let event = event_at("binlog.000001", position, None);
            let pos = watermark("binlog.000001", position, "");
            let low = watermark("binlog.000001", 600, "");
            let high = watermark("binlog.000001", 900, "");
            assert_eq!(
                backend.event_in_bracket(&event, &pos, &low, &high),
                expected,
                "with no GTID sets the classification must match the ordinal test at {position}"
            );
            assert_eq!(
                super::default_bracket(&event, &pos, &low, &high),
                expected,
            );
        }
    }

    /// An event with no GTID while the watermarks have sets is non-transactional or synthetic.
    /// Falling back is conservative: reading a missing GTID as "not in `high`" would defer it
    /// past the chunk on no evidence.
    #[test]
    fn an_event_without_a_gtid_falls_back_rather_than_being_deferred() {
        let backend = backend();
        let event = event_at("binlog.000001", 700, None);
        let position = watermark("binlog.000001", 700, "");
        let low = watermark("binlog.000001", 600, &format!("{A}:1-10"));
        let high = watermark("binlog.000001", 900, &format!("{A}:1-15"));
        assert_eq!(
            backend.event_in_bracket(&event, &position, &low, &high),
            BracketPosition::Inside,
            "the ordinal test applies, not `After`"
        );
    }

    #[test]
    fn an_events_gtid_is_read_from_its_offset_suffix() {
        assert_eq!(
            event_gtid(&event_at("binlog.000001", 4, Some(&format!("{A}:9")))).as_deref(),
            Some(format!("{A}:9").as_str())
        );
        assert!(event_gtid(&event_at("binlog.000001", 4, None)).is_none());
    }

    /// The set takes no part in ordering: `Ord` answers "has the stream caught up?", which is a
    /// question about the coordinate. Mixing the two would make the deferral depend on set
    /// contents.
    #[test]
    fn the_gtid_set_does_not_participate_in_ordering() {
        let bare = watermark("binlog.000001", 500, "");
        let with_set = watermark("binlog.000001", 500, &format!("{A}:1-99"));
        assert_eq!(bare.cmp(&with_set), std::cmp::Ordering::Equal);
        assert!(watermark("binlog.000001", 400, &format!("{A}:1-99")) < bare);
    }
}
