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
    sink_transaction: Option<crate::sink::KafkaTransactionHandle>,
) -> Result<
    (
        Box<dyn rustcdc::checkpoint::Checkpoint>,
        CheckpointAgeSource,
        Option<crate::state::StateLease>,
    ),
    AppError,
> {
    match backend {
        StateBackend::LocalFs => {
            let (checkpoint, age_source, owner) = local_fs::build_checkpoint(dir).await?;
            Ok((
                Box::new(checkpoint),
                age_source,
                Some(crate::state::StateLease::LocalFs(owner)),
            ))
        }
        StateBackend::KafkaTopic(kafka_config) => {
            let (checkpoint, writer) =
                kafka::build_checkpoint(dir, kafka_config, sink_transaction).await?;
            Ok((
                Box::new(checkpoint),
                CheckpointAgeSource::KafkaTopic { writer },
                // Kafka fences at the broker via the transactional id; there is no
                // lease record to release.
                None,
            ))
        }
        StateBackend::Redis(redis_config) => {
            let op = opendal::build_redis_operator(redis_config)?;
            let checkpoint = opendal::build_checkpoint(op, dir, "redis").await?;
            let lease = checkpoint.lease();
            Ok((
                Box::new(checkpoint),
                local_fs::fallback_age_source(dir),
                Some(crate::state::StateLease::Remote(lease)),
            ))
        }
        StateBackend::Postgresql(pg_config) => {
            let op = opendal::build_postgresql_operator(pg_config)?;
            let checkpoint = opendal::build_checkpoint(op, dir, "postgresql").await?;
            let lease = checkpoint.lease();
            Ok((
                Box::new(checkpoint),
                local_fs::fallback_age_source(dir),
                Some(crate::state::StateLease::Remote(lease)),
            ))
        }
    }
}
