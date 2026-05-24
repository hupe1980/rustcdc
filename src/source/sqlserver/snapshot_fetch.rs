use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::core::{Error, Result};

use super::{
    build_snapshot_fetch_sql, qualified_table_name, sqlserver_json_value_to_param, SqlClient,
    TableSnapshotState,
};

#[async_trait]
pub(super) trait SqlServerSnapshotRowFetcher: Send + Sync {
    async fn fetch_keyset_rows(
        &self,
        table: &TableSnapshotState,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, serde_json::Value)>>;
}

pub(super) struct DisconnectedSqlServerSnapshotRowFetcher;

#[async_trait]
impl SqlServerSnapshotRowFetcher for DisconnectedSqlServerSnapshotRowFetcher {
    async fn fetch_keyset_rows(
        &self,
        _table: &TableSnapshotState,
        _cursor: Option<&str>,
        _limit: usize,
    ) -> Result<Vec<(String, serde_json::Value)>> {
        Err(Error::StateError(
            "sqlserver snapshot handle is missing an active row fetcher".into(),
        ))
    }
}

pub(super) struct LiveSqlServerSnapshotRowFetcher {
    pub(super) client: Arc<Mutex<SqlClient>>,
}

#[async_trait]
impl SqlServerSnapshotRowFetcher for LiveSqlServerSnapshotRowFetcher {
    async fn fetch_keyset_rows(
        &self,
        table: &TableSnapshotState,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, serde_json::Value)>> {
        if table.primary_key_columns.is_empty() {
            return Err(Error::SourceError(format!(
                "sqlserver snapshot requires a PRIMARY KEY for keyset pagination: {}.{}",
                table.schema, table.table
            )));
        }

        let mut client = self.client.lock().await;
        let table_ref = qualified_table_name(&table.schema, &table.table);

        let mut cursor_params = Vec::new();
        let has_cursor = if let Some(raw_cursor) = cursor {
            let parsed_cursor: Vec<serde_json::Value> =
                match serde_json::from_str::<serde_json::Value>(raw_cursor).map_err(|error| {
                    Error::CheckpointError(format!(
                        "sqlserver snapshot cursor decode failed for table '{}.{}': {error}",
                        table.schema, table.table
                    ))
                })? {
                    serde_json::Value::Array(values) => values,
                    serde_json::Value::Object(values) => table
                        .primary_key_columns
                        .iter()
                        .map(|column| values.get(column).cloned().unwrap_or(serde_json::Value::Null))
                        .collect(),
                    _ => {
                        return Err(Error::CheckpointError(format!(
                            "sqlserver snapshot cursor decode failed for table '{}.{}': expected JSON array or object",
                            table.schema, table.table
                        )))
                    }
                };
            if parsed_cursor.len() != table.primary_key_columns.len() {
                return Err(Error::CheckpointError(format!(
                    "sqlserver snapshot cursor width mismatch for table '{}.{}'",
                    table.schema, table.table
                )));
            }

            cursor_params = parsed_cursor
                .iter()
                .map(sqlserver_json_value_to_param)
                .collect::<Result<Vec<_>>>()?;

            true
        } else {
            false
        };

        let limit_param_index = cursor_params.len() + 1;

        let sql = build_snapshot_fetch_sql(
            &table_ref,
            &table.primary_key_columns,
            &table.column_names,
            limit_param_index,
            has_cursor,
        );

        let limit_value = i64::try_from(limit).map_err(|_| {
            Error::SourceError(format!(
                "sqlserver snapshot chunk size exceeds i64: {limit}"
            ))
        })?;

        let mut query = tiberius::Query::new(sql);
        for param in &cursor_params {
            param.bind(&mut query);
        }
        query.bind(limit_value);

        let rows = query
            .query(&mut *client)
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "sqlserver snapshot query failed for table '{}.{}': {error}",
                    table.schema, table.table
                ))
            })?
            .into_first_result()
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "sqlserver snapshot decode failed for table '{}.{}': {error}",
                    table.schema, table.table
                ))
            })?;

        let mut decoded = Vec::with_capacity(rows.len());
        for row in rows {
            let cursor_json = row
                .get::<&str, _>(0)
                .map(ToOwned::to_owned)
                .ok_or_else(|| {
                    Error::SourceError(format!(
                        "sqlserver snapshot row missing cursor_json for table '{}.{}'",
                        table.schema, table.table
                    ))
                })?;
            let row_json = row
                .get::<&str, _>(1)
                .map(ToOwned::to_owned)
                .ok_or_else(|| {
                    Error::SourceError(format!(
                        "sqlserver snapshot row missing row_json for table '{}.{}'",
                        table.schema, table.table
                    ))
                })?;
            let parsed_row: serde_json::Value =
                serde_json::from_str(&row_json).map_err(|error| {
                    Error::SerializationError(format!(
                        "sqlserver snapshot JSON parse failed for table '{}.{}': {error}",
                        table.schema, table.table
                    ))
                })?;
            decoded.push((cursor_json, parsed_row));
        }

        Ok(decoded)
    }
}
