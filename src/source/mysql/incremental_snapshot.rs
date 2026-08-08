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
}

impl Ord for BinlogPos {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_binlog_position(&self.file, self.position, &other.file, other.position)
    }
}

impl PartialOrd for BinlogPos {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
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
        Ok(BinlogPos { file, position })
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
