use std::path::Path;

use crate::config::schema::StateBackend;
use crate::error::AppError;

mod kafka;
mod local_fs;
mod opendal;

/// Build the schema-history backend from configuration.
pub(super) async fn build(
    backend: &StateBackend,
    dir: &Path,
) -> Result<Box<dyn rustcdc::schema_history::SchemaHistory>, AppError> {
    match backend {
        StateBackend::LocalFs => {
            let schema_history = local_fs::build_schema_history(dir).await?;
            Ok(Box::new(schema_history))
        }
        StateBackend::KafkaTopic(kafka_config) => {
            let schema_history = kafka::build_schema_history(dir, kafka_config).await?;
            Ok(Box::new(schema_history))
        }
        StateBackend::Redis(redis_config) => {
            let op = opendal::build_redis_op(redis_config)?;
            let schema_history = opendal::build_schema_history(op, dir).await?;
            Ok(Box::new(schema_history))
        }
        StateBackend::Postgresql(pg_config) => {
            let op = opendal::build_postgresql_op(pg_config)?;
            let schema_history = opendal::build_schema_history(op, dir).await?;
            Ok(Box::new(schema_history))
        }
    }
}
