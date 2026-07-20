use std::path::Path;

use crate::config::schema::StateBackend;
use crate::error::AppError;

use crate::state::CheckpointAgeSource;

pub(crate) mod kafka;
pub(crate) mod local_fs;
pub(crate) mod opendal;

/// Build the offset (checkpoint) backend from configuration.
pub(super) async fn build(
    backend: &StateBackend,
    dir: &Path,
) -> Result<
    (
        Box<dyn rustcdc::checkpoint::Checkpoint>,
        CheckpointAgeSource,
    ),
    AppError,
> {
    match backend {
        StateBackend::LocalFs => {
            let (checkpoint, age_source) = local_fs::build_checkpoint(dir).await?;
            Ok((Box::new(checkpoint), age_source))
        }
        StateBackend::KafkaTopic(kafka_config) => {
            let (checkpoint, writer) = kafka::build_checkpoint(dir, kafka_config).await?;
            Ok((
                Box::new(checkpoint),
                CheckpointAgeSource::KafkaTopic { writer },
            ))
        }
        StateBackend::Redis(redis_config) => {
            let op = opendal::build_redis_operator(redis_config)?;
            let checkpoint = opendal::build_checkpoint(op, dir).await?;
            Ok((Box::new(checkpoint), local_fs::fallback_age_source(dir)))
        }
        StateBackend::Postgresql(pg_config) => {
            let op = opendal::build_postgresql_operator(pg_config)?;
            let checkpoint = opendal::build_checkpoint(op, dir).await?;
            Ok((Box::new(checkpoint), local_fs::fallback_age_source(dir)))
        }
    }
}
