use async_trait::async_trait;
use serde_json::Value;

use crate::{
    checkpoint::{Checkpoint, GenericOffset},
    core::{Error, Offset, Result},
};

const OFFSET_ENCODING_JSON: &str = "json";
const OFFSET_ENCODING_BYTES: &str = "bytes";

#[derive(Debug, Clone)]
pub struct PostgresCheckpoint {
    pub pool: sqlx::PgPool,
    pub table_name: String,
    pub source_id: String,
}

impl PostgresCheckpoint {
    pub fn new(
        pool: sqlx::PgPool,
        table_name: impl Into<String>,
        source_id: impl Into<String>,
    ) -> Result<Self> {
        let table_name = table_name.into();
        validate_identifier(&table_name)?;

        let source_id = source_id.into();
        if source_id.trim().is_empty() {
            return Err(Error::ConfigError(
                "postgres checkpoint source_id must not be empty".into(),
            ));
        }

        Ok(Self {
            pool,
            table_name,
            source_id,
        })
    }

    pub fn default_table_name() -> &'static str {
        "cdc_checkpoints"
    }

    async fn ensure_table_exists(&self) -> Result<()> {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} (\
                source_id TEXT PRIMARY KEY, \
                offset_payload JSONB NOT NULL, \
                committed_count BIGINT NOT NULL, \
                updated_at TIMESTAMP NOT NULL\
            )",
            self.table_name
        );

        sqlx::query(&query)
            .execute(&self.pool)
            .await
            .map_err(|error| {
                Error::CheckpointError(format!("failed to ensure checkpoint table exists: {error}"))
            })?;
        Ok(())
    }

    fn encode_offset(offset: &dyn Offset) -> Result<Value> {
        let bytes = offset.encode()?;
        if let Ok(json_payload) = serde_json::from_slice::<Value>(&bytes) {
            return Ok(serde_json::json!({
                "encoding": OFFSET_ENCODING_JSON,
                "payload": json_payload,
            }));
        }

        Ok(serde_json::json!({
            "encoding": OFFSET_ENCODING_BYTES,
            "payload": bytes,
        }))
    }

    fn decode_offset(source_id: String, wrapper: &Value) -> Result<GenericOffset> {
        let Some(encoding) = wrapper.get("encoding").and_then(Value::as_str) else {
            return Err(Error::SerializationError(
                "checkpoint offset payload missing encoding".into(),
            ));
        };
        let Some(payload) = wrapper.get("payload") else {
            return Err(Error::SerializationError(
                "checkpoint offset payload missing payload".into(),
            ));
        };

        let bytes = match encoding {
            OFFSET_ENCODING_JSON => serde_json::to_vec(payload)?,
            OFFSET_ENCODING_BYTES => serde_json::from_value::<Vec<u8>>(payload.clone())
                .map_err(|error| Error::SerializationError(error.to_string()))?,
            other => {
                return Err(Error::SerializationError(format!(
                    "unsupported checkpoint offset encoding: {other}"
                )));
            }
        };

        Ok(GenericOffset::new(source_id, bytes))
    }
}

#[async_trait]
impl Checkpoint for PostgresCheckpoint {
    async fn save(&mut self, offset: &dyn Offset, committed_event_count: u64) -> Result<()> {
        self.ensure_table_exists().await?;

        let query = format!(
            "INSERT INTO {} (source_id, offset_payload, committed_count, updated_at) \
             VALUES ($1, $2, $3, NOW()) \
             ON CONFLICT (source_id) DO UPDATE SET \
                offset_payload = EXCLUDED.offset_payload, \
                committed_count = EXCLUDED.committed_count, \
                updated_at = NOW()",
            self.table_name
        );

        let wrapped_offset = Self::encode_offset(offset)?;
        let committed_count = i64::try_from(committed_event_count).map_err(|_| {
            Error::CheckpointError(format!(
                "committed_event_count exceeds i64 range: {committed_event_count}"
            ))
        })?;

        sqlx::query(&query)
            .bind(&self.source_id)
            .bind(sqlx::types::Json(wrapped_offset))
            .bind(committed_count)
            .execute(&self.pool)
            .await
            .map_err(|error| {
                Error::CheckpointError(format!("failed to save checkpoint row: {error}"))
            })?;

        Ok(())
    }

    async fn load(&self) -> Result<Option<Box<dyn Offset>>> {
        self.ensure_table_exists().await?;

        let query = format!(
            "SELECT offset_payload FROM {} WHERE source_id = $1",
            self.table_name
        );

        let row = sqlx::query_as::<_, (sqlx::types::Json<Value>,)>(&query)
            .bind(&self.source_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| {
                Error::CheckpointError(format!("failed to load checkpoint row: {error}"))
            })?;

        let Some((wrapped_offset,)) = row else {
            return Ok(None);
        };

        let offset = Self::decode_offset(self.source_id.clone(), &wrapped_offset.0)?;
        Ok(Some(Box::new(offset)))
    }

    async fn get_committed_count(&self) -> Result<u64> {
        self.ensure_table_exists().await?;

        let query = format!(
            "SELECT committed_count FROM {} WHERE source_id = $1",
            self.table_name
        );

        let row = sqlx::query_as::<_, (i64,)>(&query)
            .bind(&self.source_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| {
                Error::CheckpointError(format!("failed to load checkpoint commit count: {error}"))
            })?;

        let Some((committed_count,)) = row else {
            return Ok(0);
        };

        u64::try_from(committed_count).map_err(|_| {
            Error::CheckpointError(format!(
                "stored committed_count is negative for source_id {}",
                self.source_id
            ))
        })
    }
}

fn validate_identifier(identifier: &str) -> Result<()> {
    if identifier.is_empty() {
        return Err(Error::ConfigError(
            "postgres checkpoint table_name must not be empty".into(),
        ));
    }

    if identifier
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Ok(());
    }

    Err(Error::ConfigError(format!(
        "postgres checkpoint table_name must be alphanumeric/underscore: {identifier}"
    )))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use sqlx::postgres::PgPoolOptions;

    use crate::checkpoint::{Checkpoint, PostgresOffset};

    use super::{validate_identifier, PostgresCheckpoint};

    #[test]
    fn table_identifier_validation_rejects_unsafe_names() {
        assert!(validate_identifier("cdc_checkpoints").is_ok());
        assert!(validate_identifier("cdc-checkpoints").is_err());
        assert!(validate_identifier("cdc checkpoints").is_err());
    }

    #[tokio::test]
    async fn new_rejects_empty_source_id() {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/postgres")
            .unwrap();
        let result = PostgresCheckpoint::new(pool, "cdc_checkpoints", "");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn new_rejects_unsafe_table_name() {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/postgres")
            .unwrap();
        let result = PostgresCheckpoint::new(pool, "cdc-checkpoints", "postgres");
        assert!(result.is_err());
    }

    #[test]
    fn offset_wrapper_supports_json_and_opaque_bytes() {
        let postgres_offset = PostgresOffset {
            lsn: 42,
            slot_name: "slot-a".into(),
        };

        let wrapped_json = PostgresCheckpoint::encode_offset(&postgres_offset).unwrap();
        let decoded_json =
            PostgresCheckpoint::decode_offset("postgres".into(), &wrapped_json).unwrap();
        let reconstructed_json: PostgresOffset =
            serde_json::from_slice(&decoded_json.bytes).unwrap();
        assert_eq!(reconstructed_json, postgres_offset);

        let wrapped_bytes = json!({
            "encoding": "bytes",
            "payload": [1, 2, 3, 4],
        });
        let decoded_bytes =
            PostgresCheckpoint::decode_offset("postgres".into(), &wrapped_bytes).unwrap();
        assert_eq!(decoded_bytes.bytes, vec![1, 2, 3, 4]);
    }

    #[test]
    fn decode_offset_rejects_missing_encoding_field() {
        let bad = json!({"payload": {"lsn": 1}});
        assert!(PostgresCheckpoint::decode_offset("postgres".into(), &bad).is_err());
    }

    #[test]
    fn decode_offset_rejects_unsupported_encoding() {
        let bad = json!({"encoding": "base58", "payload": "deadbeef"});
        assert!(PostgresCheckpoint::decode_offset("postgres".into(), &bad).is_err());
    }

    #[tokio::test]
    async fn db_unreachable_returns_checkpoint_error() {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(50))
            .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/postgres")
            .unwrap();

        let checkpoint = PostgresCheckpoint::new(pool, "cdc_checkpoints", "postgres").unwrap();
        let error = checkpoint.get_committed_count().await.unwrap_err();
        assert!(matches!(error, crate::core::Error::CheckpointError(_)));
    }
}
