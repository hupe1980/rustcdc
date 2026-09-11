use std::path::Path;

use crate::config::schema::KafkaTopicStateConfig;
use crate::error::AppError;
use crate::state::offset::kafka::{self, KafkaTopicSchemaHistory};

/// Build the Kafka-backed schema-history.
pub(super) async fn build_schema_history(
    state_dir: &Path,
    config: &KafkaTopicStateConfig,
) -> Result<KafkaTopicSchemaHistory, AppError> {
    kafka::build_schema_history(state_dir, config).await
}
