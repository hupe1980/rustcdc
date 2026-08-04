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

use super::{
    parse_pg_lsn, parse_table_reference, qualified_table_name, query_current_wal_lsn,
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

/// PostgreSQL half of the incremental snapshot.
pub struct PostgresSnapshotBackend {
    /// Regular (non-replication) connection used for chunk SELECTs and LSN checks.
    /// Never holds a transaction open — that is the point of the DBLog design.
    query_client: Arc<Client>,
}

#[async_trait]
impl IncrementalSnapshotBackend for PostgresSnapshotBackend {
    type Position = u64;

    async fn describe_table(&mut self, table_ref: &str) -> Result<SnapshotTable> {
        let (schema, name) = parse_table_reference(table_ref)?;
        let (pk_columns, pk_types) =
            query_primary_key_columns_and_types(&self.query_client, &schema, &name).await?;
        let qualified = qualified_table_name(&schema, &name);
        Ok(SnapshotTable {
            schema,
            name,
            qualified,
            pk_columns,
            pk_types,
            columns: Vec::new(),
        })
    }

    async fn current_position(&mut self) -> Result<u64> {
        query_current_wal_lsn(&self.query_client).await
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
                "SELECT ARRAY[{key_value_expr}], row_to_json(t)::text \
                 FROM {table_ref} t \
                 WHERE ({order_expr}) > ({predicate_expr}) \
                 ORDER BY {order_expr} \
                 LIMIT ${}",
                table.pk_columns.len() + 1
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
                "SELECT ARRAY[{key_value_expr}], row_to_json(t)::text \
                 FROM {table_ref} t \
                 ORDER BY {order_expr} \
                 LIMIT $1"
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

    fn position_of_event(&self, event: &Event) -> Option<u64> {
        parse_pg_lsn(&event.source.offset).ok()
    }

    fn render_position(&self, position: &u64) -> String {
        super::format_pg_lsn(*position)
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
