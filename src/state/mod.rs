use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use rustcdc::checkpoint::Checkpoint;
use rustcdc::core::{Offset, Result as RtResult};
use rustcdc::schema_history::{DDLEvent, SchemaHistory, SchemaHistoryRetention, TableSchema};

use crate::config::schema::StateConfig;
use crate::error::AppError;

pub(crate) mod metrics;
pub(crate) mod offset;
pub(crate) mod remote_lease;
mod schema_history;

// ─────────────────────────────────────────────────────────────────────────────
// Newtype wrappers — delegate Box<dyn Trait> through the rustcdc traits so
// RuntimeState can be passed to CdcRuntime without generic type parameters.
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) struct CheckpointBox(pub(crate) Box<dyn Checkpoint>);

#[async_trait]
impl Checkpoint for CheckpointBox {
    async fn save(&mut self, offset: &dyn Offset, committed_event_count: u64) -> RtResult<()> {
        self.0.save(offset, committed_event_count).await
    }

    async fn load(&self) -> RtResult<Option<Box<dyn Offset>>> {
        self.0.load().await
    }

    async fn get_committed_count(&self) -> RtResult<u64> {
        self.0.get_committed_count().await
    }
}

pub(crate) struct SchemaHistoryBox(pub(crate) Box<dyn SchemaHistory>);

#[async_trait]
impl SchemaHistory for SchemaHistoryBox {
    async fn record_ddl(&mut self, ddl: DDLEvent) -> RtResult<u32> {
        self.0.record_ddl(ddl).await
    }

    async fn get_schema_at_version(
        &self,
        table: &str,
        version: u32,
    ) -> RtResult<Option<TableSchema>> {
        self.0.get_schema_at_version(table, version).await
    }

    async fn get_schema_at_timestamp(&self, table: &str, ts: u64) -> RtResult<Option<TableSchema>> {
        self.0.get_schema_at_timestamp(table, ts).await
    }

    async fn latest_schema(&self, table: &str) -> RtResult<Option<TableSchema>> {
        self.0.latest_schema(table).await
    }

    async fn apply_retention(&mut self, retention: SchemaHistoryRetention) -> RtResult<usize> {
        self.0.apply_retention(retention).await
    }
}

pub(crate) use metrics::{
    kafka_topic_state_bootstrap_seeded_total, kafka_topic_state_corruption_detected_total,
    opendal_state_write_failures_total,
};

/// Callers must call `cdc init-state` first; after that this function always
/// succeeds or returns a descriptive error.
pub(crate) async fn initialize_kafka_topic_state(
    config: &crate::config::schema::KafkaTopicStateConfig,
    force: bool,
) -> Result<(), AppError> {
    offset::kafka::initialize(config, force).await
}

// ─────────────────────────────────────────────────────────────────────────────
// Core types
// ─────────────────────────────────────────────────────────────────────────────

/// Non-generic runtime state. Checkpoint and schema-history are wrapped in
/// newtype adapters (`CheckpointBox`/`SchemaHistoryBox`) that implement the
/// respective rustcdc traits, so `CdcRuntime` can accept them without any
/// generic parameters at the call site.
pub(crate) struct RuntimeState {
    pub(crate) checkpoint: CheckpointBox,
    pub(crate) schema_history: SchemaHistoryBox,
    pub(crate) checkpoint_age_source: CheckpointAgeSource,
    /// Remote owner lease, for the backends that hold one (`redis`, `postgresql`).
    ///
    /// `local_fs` uses a lease *file* that its own `Drop` removes, and `kafka_topic`
    /// fences at the broker by producer epoch — neither has a record to release.
    pub(crate) remote_lease: Option<std::sync::Arc<offset::opendal::OwnedLease>>,
}

/// Source of checkpoint freshness metrics, resolved at build time.
#[derive(Clone)]
pub(crate) enum CheckpointAgeSource {
    LocalFs {
        checkpoint_dir: PathBuf,
        schema_history_path: PathBuf,
    },
    KafkaTopic {
        writer: Arc<offset::kafka::KafkaTopicStateWriter>,
    },
}

impl CheckpointAgeSource {
    pub(crate) async fn checkpoint_age_seconds(&self) -> Option<f64> {
        match self {
            Self::LocalFs {
                checkpoint_dir,
                schema_history_path,
            } => offset::local_fs::age_seconds(checkpoint_dir, schema_history_path),
            Self::KafkaTopic { writer } => writer.checkpoint_age_seconds().await,
        }
    }

    pub(crate) fn backend_name(&self) -> &'static str {
        match self {
            Self::LocalFs { .. } => "local_fs",
            Self::KafkaTopic { .. } => "kafka_topic",
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Build
// ─────────────────────────────────────────────────────────────────────────────

/// Build both offset and schema-history backends from configuration.
/// Build the runtime's state backends.
///
/// `sink_transaction` is a share in the Kafka sink's transaction, when there is one. The
/// `kafka_topic` offset backend uses it to write each checkpoint *inside* the transaction
/// that carries the batch's data, which is what makes `effectively_once` exactly-once
/// end to end rather than only at the sink. Every other backend ignores it.
pub(crate) async fn build(
    config: &StateConfig,
    sink_transaction: Option<crate::sink::KafkaTransactionHandle>,
) -> Result<RuntimeState, AppError> {
    let (checkpoint, checkpoint_age_source, remote_lease) =
        offset::build(&config.offset.backend, &config.offset.dir, sink_transaction).await?;
    let schema_history =
        schema_history::build(&config.schema_history.backend, &config.schema_history.dir).await?;

    Ok(RuntimeState {
        checkpoint: CheckpointBox(checkpoint),
        schema_history: SchemaHistoryBox(schema_history),
        checkpoint_age_source,
        remote_lease,
    })
}
