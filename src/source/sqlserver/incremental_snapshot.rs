//! SQL Server backend for the DBLog incremental snapshot.
//!
//! The watermark algorithm itself lives in
//! [`crate::source::IncrementalSnapshotDriver`]; this module supplies only what is
//! specific to SQL Server: CDC LSNs as the position type, keyset-paginated chunk
//! SELECTs projected through `FOR JSON PATH`, and the
//! [`SqlServerOffset`](crate::checkpoint::SqlServerOffset) encoding that carries the
//! snapshot state inside the checkpoint record.

use async_trait::async_trait;

use crate::{
    core::{Error, Event, Offset, Result},
    source::{
        ChunkRow, IncrementalSnapshotBackend, IncrementalSnapshotConfig, IncrementalSnapshotDriver,
        IncrementalSnapshotState, SnapshotTable, StreamHandle,
    },
};

use super::{
    parser::{
        build_snapshot_fetch_sql, compare_lsn, lsn_bytes_to_hex, lsn_from_source_offset,
        parse_schema_table, qualified_table_name,
    },
    query::connect_client,
    SqlClient, SqlServerSourceConfig,
};

/// A [`StreamHandle`] that interleaves SQL Server chunk reads with the live CDC stream.
///
/// Obtain one via `SqlServerConnection::start_incremental_snapshot`.
pub type SqlServerIncrementalSnapshotHandle = IncrementalSnapshotDriver<SqlServerSnapshotBackend>;

/// Build the SQL Server incremental-snapshot handle.
pub(super) async fn start(
    inner: Box<dyn StreamHandle>,
    config: SqlServerSourceConfig,
    snapshot_config: IncrementalSnapshotConfig,
    source_name: String,
    resume: Option<IncrementalSnapshotState>,
) -> Result<SqlServerIncrementalSnapshotHandle> {
    // A dedicated connection: chunk reads must not contend with the CDC poll loop,
    // and tiberius clients are not shareable across concurrent queries.
    let client = connect_client(&config).await?;
    IncrementalSnapshotDriver::new(
        SqlServerSnapshotBackend { client },
        inner,
        snapshot_config,
        source_name,
        resume,
    )
    .await
}

/// CDC log sequence number, as the 10-byte `binary(10)` SQL Server uses.
///
/// Ordering is deliberately **not** derived. A `[u8; 10]` would compare
/// lexicographically, which happens to agree with LSN order — but relying on that
/// coincidence rather than on the documented comparison is how the equivalent MySQL
/// code got it wrong. The connector's own `compare_lsn` is the definition of record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CdcLsn(pub [u8; 10]);

impl Ord for CdcLsn {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_lsn(&self.0, &other.0)
    }
}

impl PartialOrd for CdcLsn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A primary-key value bound into the keyset predicate.
///
/// tiberius binds by concrete type, so a JSON cursor value has to be narrowed back
/// to one before it can be a query parameter.
#[derive(Debug)]
enum CursorParam {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl CursorParam {
    fn bind(&self, query: &mut tiberius::Query) {
        match self {
            Self::Bool(value) => query.bind(*value),
            Self::Int(value) => query.bind(*value),
            Self::Float(value) => query.bind(*value),
            Self::Text(value) => query.bind(value.clone()),
        }
    }
}

fn json_value_to_cursor_param(value: &serde_json::Value) -> Result<CursorParam> {
    match value {
        serde_json::Value::Null => Err(Error::CheckpointError(
            "sqlserver incremental snapshot cursor does not support NULL pk values".into(),
        )),
        serde_json::Value::Bool(flag) => Ok(CursorParam::Bool(*flag)),
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                Ok(CursorParam::Int(value))
            } else if let Some(value) = number.as_f64() {
                Ok(CursorParam::Float(value))
            } else {
                Err(Error::CheckpointError(
                    "sqlserver incremental snapshot: unsupported numeric pk value".into(),
                ))
            }
        }
        serde_json::Value::String(text) => Ok(CursorParam::Text(text.clone())),
        _ => Err(Error::CheckpointError(
            "sqlserver incremental snapshot: only scalar pk values are supported".into(),
        )),
    }
}

/// SQL Server half of the incremental snapshot.
pub struct SqlServerSnapshotBackend {
    client: SqlClient,
}

#[async_trait]
impl IncrementalSnapshotBackend for SqlServerSnapshotBackend {
    type Position = CdcLsn;

    async fn describe_table(&mut self, table_ref: &str) -> Result<SnapshotTable> {
        let (schema, name) = parse_schema_table(table_ref)?;
        let pk_columns = load_pk_columns(&mut self.client, &schema, &name).await?;
        let columns = load_all_columns(&mut self.client, &schema, &name).await?;
        let qualified = qualified_table_name(&schema, &name);
        Ok(SnapshotTable {
            // Filled in by the driver from `IncrementalSnapshotConfig::table_conditions`.
            condition: None,
            schema,
            name,
            qualified,
            pk_columns,
            pk_types: Vec::new(),
            columns,
        })
    }

    async fn current_position(&mut self) -> Result<CdcLsn> {
        let rows = self
            .client
            .query(
                "SELECT sys.fn_varbintohexstr(sys.fn_cdc_get_max_lsn())",
                &[],
            )
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "incremental snapshot: max LSN query failed: {error}"
                ))
            })?
            .into_first_result()
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "incremental snapshot: max LSN decode failed: {error}"
                ))
            })?;

        let hex: &str = rows.first().and_then(|row| row.get(0)).ok_or_else(|| {
            Error::SourceError("incremental snapshot: max LSN returned no row".into())
        })?;
        super::parser::lsn_hex_to_bytes(hex).map(CdcLsn)
    }

    async fn fetch_chunk(
        &mut self,
        table: &SnapshotTable,
        cursor: Option<&[serde_json::Value]>,
        limit: usize,
    ) -> Result<Vec<ChunkRow>> {
        // The limit parameter is numbered after the cursor parameters.
        let cursor_param_count = cursor.map_or(0, <[serde_json::Value]>::len);
        let sql = build_snapshot_fetch_sql(
            &table.qualified,
            &table.pk_columns,
            &table.columns,
            cursor_param_count + 1,
            cursor.is_some(),
            table.condition.as_deref(),
        );
        let limit = i32::try_from(limit.min(i32::MAX as usize)).unwrap_or(i32::MAX);

        let mut query = tiberius::Query::new(&sql);
        if let Some(cursor) = cursor {
            for value in cursor {
                json_value_to_cursor_param(value)?.bind(&mut query);
            }
        }
        query.bind(limit);

        let rows = query
            .query(&mut self.client)
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "incremental snapshot: chunk SELECT failed for '{}': {error}",
                    table.qualified
                ))
            })?
            .into_first_result()
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "incremental snapshot: chunk SELECT decode failed for '{}': {error}",
                    table.qualified
                ))
            })?;

        let mut decoded = Vec::with_capacity(rows.len());
        for row in &rows {
            // Both projections are `FOR JSON PATH ... WITHOUT_ARRAY_WRAPPER`, so each
            // is a single JSON object rather than an array.
            let cursor_json: &str = row.get(0).ok_or_else(|| {
                Error::SourceError(format!(
                    "incremental snapshot: missing cursor_json for '{}'",
                    table.qualified
                ))
            })?;
            let row_json: &str = row.get(1).ok_or_else(|| {
                Error::SourceError(format!(
                    "incremental snapshot: missing row_json for '{}'",
                    table.qualified
                ))
            })?;

            let cursor_object: serde_json::Value =
                serde_json::from_str(cursor_json).map_err(|error| {
                    Error::SerializationError(format!(
                        "incremental snapshot: cursor_json parse failed for '{}': {error}",
                        table.qualified
                    ))
                })?;
            let row_object = super::parser::decode_row_json_as_text(row_json).map_err(|error| {
                Error::SerializationError(format!(
                    "incremental snapshot: row_json parse failed for '{}': {error}",
                    table.qualified
                ))
            })?;

            let cursor: Vec<serde_json::Value> = table
                .pk_columns
                .iter()
                .map(|column| {
                    cursor_object
                        .get(column)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect();
            if cursor.iter().any(serde_json::Value::is_null) {
                return Err(Error::SourceError(format!(
                    "incremental snapshot: NULL primary-key column for '{}'",
                    table.qualified
                )));
            }

            decoded.push(ChunkRow {
                cursor,
                row: row_object,
            });
        }
        Ok(decoded)
    }

    fn position_of_event(&self, event: &Event) -> Option<CdcLsn> {
        lsn_from_source_offset(&event.source.offset).map(CdcLsn)
    }

    fn render_position(&self, position: &CdcLsn) -> String {
        lsn_bytes_to_hex(&position.0)
    }

    fn offset_with_snapshot_state(
        &self,
        inner: &dyn Offset,
        state: IncrementalSnapshotState,
    ) -> Option<Box<dyn Offset>> {
        let encoded = inner.encode().ok()?;
        let mut offset = crate::checkpoint::SqlServerOffset::from_bytes(&encoded).ok()?;
        offset.incremental_snapshot = Some(state);
        Some(Box::new(offset))
    }
}

// ─── Catalog lookups ──────────────────────────────────────────────────────────

async fn load_pk_columns(client: &mut SqlClient, schema: &str, table: &str) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT k.COLUMN_NAME \
             FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
             JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k \
               ON tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME \
              AND tc.TABLE_SCHEMA = k.TABLE_SCHEMA \
             WHERE tc.TABLE_SCHEMA = @P1 \
               AND tc.TABLE_NAME = @P2 \
               AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \
             ORDER BY k.ORDINAL_POSITION",
            &[&schema, &table],
        )
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "incremental snapshot: PK query failed for '{schema}.{table}': {error}"
            ))
        })?
        .into_first_result()
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "incremental snapshot: PK decode failed for '{schema}.{table}': {error}"
            ))
        })?;

    Ok(rows
        .into_iter()
        .filter_map(|row| row.get::<&str, _>(0).map(ToOwned::to_owned))
        .collect())
}

async fn load_all_columns(
    client: &mut SqlClient,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT COLUMN_NAME \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = @P1 AND TABLE_NAME = @P2 \
             ORDER BY ORDINAL_POSITION",
            &[&schema, &table],
        )
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "incremental snapshot: columns query failed for '{schema}.{table}': {error}"
            ))
        })?
        .into_first_result()
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "incremental snapshot: columns decode failed for '{schema}.{table}': {error}"
            ))
        })?;

    Ok(rows
        .into_iter()
        .filter_map(|row| row.get::<&str, _>(0).map(ToOwned::to_owned))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn lsn(low: u8) -> CdcLsn {
        let mut bytes = [0u8; 10];
        bytes[9] = low;
        CdcLsn(bytes)
    }

    #[test]
    fn cdc_lsns_order_by_the_connectors_own_comparison() {
        assert!(lsn(1) < lsn(2));
        assert_eq!(lsn(3), lsn(3));

        // A high-order byte must dominate the low-order ones.
        let mut high = [0u8; 10];
        high[0] = 1;
        assert!(CdcLsn(high) > lsn(255));
    }

    #[test]
    fn a_null_cursor_value_is_rejected_rather_than_bound_as_a_default() {
        // Binding NULL would make the keyset predicate `> NULL`, which SQL Server
        // evaluates as unknown — silently returning zero rows and marking the table
        // complete with rows unread.
        let error = json_value_to_cursor_param(&serde_json::Value::Null)
            .expect_err("NULL must be rejected");
        assert!(error.to_string().contains("NULL pk values"), "got: {error}");
    }

    #[test]
    fn a_non_scalar_cursor_value_is_rejected() {
        let error =
            json_value_to_cursor_param(&json!({ "a": 1 })).expect_err("object must be rejected");
        assert!(error.to_string().contains("scalar"), "got: {error}");
    }

    #[test]
    fn scalar_cursor_values_narrow_to_their_bindable_type() {
        assert!(matches!(
            json_value_to_cursor_param(&json!(42)).expect("int"),
            CursorParam::Int(42)
        ));
        assert!(matches!(
            json_value_to_cursor_param(&json!("k")).expect("text"),
            CursorParam::Text(_)
        ));
        assert!(matches!(
            json_value_to_cursor_param(&json!(true)).expect("bool"),
            CursorParam::Bool(true)
        ));
        assert!(matches!(
            json_value_to_cursor_param(&json!(1.5)).expect("float"),
            CursorParam::Float(_)
        ));
    }
}
