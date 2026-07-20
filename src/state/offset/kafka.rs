use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use krafka::admin::{AdminClient, ConfigEntry, DescribeConfigsRequest};
use krafka::consumer::CompactedTopicConsumer;
use krafka::producer::{Acks, Producer};
use rustcdc::checkpoint::{Checkpoint, FileCheckpoint, GenericOffset};
use rustcdc::core::Offset;
use rustcdc::schema_history::{
    DDLEvent, FileSchemaHistory, SchemaHistory, SchemaHistoryRetention, TableSchema,
};
use serde::{Deserialize, Serialize};

use crate::config::schema::KafkaTopicStateConfig;
use crate::error::AppError;

use crate::state::metrics;

// ─────────────────────────────────────────────────────────────────────────────
// Wire-format constants
// ─────────────────────────────────────────────────────────────────────────────

/// Kafka compacted-topic key for checkpoint records (v2 layout).
pub(crate) const KAFKA_STATE_KEY_CHECKPOINT: &[u8] = b"checkpoint";

/// Kafka compacted-topic key for schema-history records (v2 layout).
pub(crate) const KAFKA_STATE_KEY_SCHEMA_HISTORY: &[u8] = b"schema_history";

/// Recognised during bootstrap detection only; never written by v2 code.
const KAFKA_STATE_KEY_LEGACY_SNAPSHOT: &[u8] = b"state";

const KAFKA_STATE_BOOTSTRAP_SOURCE_TYPE: &str = "__bootstrap_empty__";

/// Wire version for checkpoint records. Bump when the JSON schema changes in a
/// backward-incompatible way.
pub(crate) const KAFKA_TOPIC_CHECKPOINT_VERSION: u8 = 2;

/// Wire version for schema-history records.
pub(crate) const KAFKA_TOPIC_SCHEMA_HISTORY_VERSION: u8 = 2;

// ─────────────────────────────────────────────────────────────────────────────
// Wire-format types
// ─────────────────────────────────────────────────────────────────────────────

/// Checkpoint record stored under [`KAFKA_STATE_KEY_CHECKPOINT`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct KafkaTopicCheckpointRecord {
    pub(crate) record_version: u8,
    pub(crate) source_type: String,
    pub(crate) offset_hex: String,
    pub(crate) committed_event_count: u64,
    pub(crate) saved_at_unix_ms: u64,
}

/// Schema-history envelope stored under [`KAFKA_STATE_KEY_SCHEMA_HISTORY`].
/// Only written when schema history actually changes (dirty-flag optimisation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct KafkaTopicSchemaHistoryRecord {
    pub(crate) record_version: u8,
    /// Raw bytes of the `FileSchemaHistory` state file.
    pub(crate) schema_history_bytes: Vec<u8>,
    pub(crate) saved_at_unix_ms: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// State writer
// ─────────────────────────────────────────────────────────────────────────────

/// Publishes checkpoint and schema-history records to the Kafka compacted topic.
///
/// Holds the last published values so that callers can compute checkpoint age
/// and roll back schema-history on publish failure.
pub(crate) struct KafkaTopicStateWriter {
    producer: tokio::sync::Mutex<Producer>,
    topic: String,
    last_checkpoint: tokio::sync::Mutex<Option<KafkaTopicCheckpointRecord>>,
    last_schema_history_bytes: tokio::sync::Mutex<Option<Vec<u8>>>,
    /// Set when schema history has changed since the last Kafka publish.
    schema_history_dirty: AtomicBool,
}

impl KafkaTopicStateWriter {
    pub(super) async fn new(config: &KafkaTopicStateConfig) -> Result<Self, AppError> {
        let auth = config.security.to_auth_config().map_err(AppError::Other)?;
        let producer = Producer::builder()
            .bootstrap_servers(config.brokers.clone())
            .client_id(format!("{}-state-writer", config.client_id))
            .acks(Acks::All)
            .idempotent(true)
            .request_timeout(std::time::Duration::from_millis(config.request_timeout_ms))
            .auth(auth)
            .build()
            .await
            .map_err(|e| {
                AppError::Other(format!("failed to build kafka topic state producer: {e}"))
            })?;

        Ok(Self {
            producer: tokio::sync::Mutex::new(producer),
            topic: config.topic.clone(),
            last_checkpoint: tokio::sync::Mutex::new(None),
            last_schema_history_bytes: tokio::sync::Mutex::new(None),
            schema_history_dirty: AtomicBool::new(false),
        })
    }

    async fn publish_record(&self, key: &[u8], payload: &[u8]) -> Result<(), AppError> {
        let producer = self.producer.lock().await;
        let _record_metadata = producer
            .send(&self.topic, Some(key), payload)
            .await
            .map_err(|e| {
                AppError::Other(format!(
                    "failed writing kafka state record to topic '{}': {e}",
                    self.topic
                ))
            })?;
        Ok(())
    }

    pub(super) async fn seed_bootstrap(&self) -> Result<(), AppError> {
        let checkpoint = bootstrap_checkpoint_record();
        let schema_history = KafkaTopicSchemaHistoryRecord {
            record_version: KAFKA_TOPIC_SCHEMA_HISTORY_VERSION,
            schema_history_bytes: Vec::new(),
            saved_at_unix_ms: now_unix_ms(),
        };

        let checkpoint_payload = serde_json::to_vec(&checkpoint).map_err(|e| {
            AppError::Other(format!("failed to serialize bootstrap checkpoint: {e}"))
        })?;
        let schema_history_payload = serde_json::to_vec(&schema_history).map_err(|e| {
            AppError::Other(format!("failed to serialize bootstrap schema history: {e}"))
        })?;

        self.publish_record(KAFKA_STATE_KEY_CHECKPOINT, &checkpoint_payload)
            .await?;
        self.publish_record(KAFKA_STATE_KEY_SCHEMA_HISTORY, &schema_history_payload)
            .await?;

        *self.last_checkpoint.lock().await = Some(checkpoint);
        *self.last_schema_history_bytes.lock().await = Some(Vec::new());
        self.schema_history_dirty.store(false, Ordering::Relaxed);

        Ok(())
    }

    pub(super) async fn restore_from_loaded(
        &self,
        checkpoint: Option<KafkaTopicCheckpointRecord>,
        schema_history_bytes: Option<Vec<u8>>,
    ) {
        *self.last_checkpoint.lock().await = checkpoint;
        *self.last_schema_history_bytes.lock().await = schema_history_bytes;
        self.schema_history_dirty.store(false, Ordering::Relaxed);
    }

    async fn update_checkpoint(
        &self,
        checkpoint_record: KafkaTopicCheckpointRecord,
    ) -> Result<(), AppError> {
        let payload = serde_json::to_vec(&checkpoint_record)
            .map_err(|e| AppError::Other(format!("failed to serialize checkpoint record: {e}")))?;
        self.publish_record(KAFKA_STATE_KEY_CHECKPOINT, &payload)
            .await?;
        *self.last_checkpoint.lock().await = Some(checkpoint_record);
        Ok(())
    }

    /// Publish schema history only when the dirty flag is set, avoiding
    /// write-amplification when schema history has not changed since last commit.
    async fn flush_schema_history_if_dirty(
        &self,
        schema_history_bytes: Vec<u8>,
    ) -> Result<(), AppError> {
        if !self.schema_history_dirty.load(Ordering::Relaxed) {
            return Ok(());
        }

        let record = KafkaTopicSchemaHistoryRecord {
            record_version: KAFKA_TOPIC_SCHEMA_HISTORY_VERSION,
            schema_history_bytes: schema_history_bytes.clone(),
            saved_at_unix_ms: now_unix_ms(),
        };
        let payload = serde_json::to_vec(&record).map_err(|e| {
            AppError::Other(format!("failed to serialize schema history record: {e}"))
        })?;
        self.publish_record(KAFKA_STATE_KEY_SCHEMA_HISTORY, &payload)
            .await?;
        *self.last_schema_history_bytes.lock().await = Some(schema_history_bytes);
        self.schema_history_dirty.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn mark_schema_history_dirty(&self) {
        self.schema_history_dirty.store(true, Ordering::Relaxed);
    }

    async fn last_published_schema_history_bytes(&self) -> Option<Vec<u8>> {
        self.last_schema_history_bytes.lock().await.clone()
    }

    pub(crate) async fn checkpoint_age_seconds(&self) -> Option<f64> {
        let guard = self.last_checkpoint.lock().await;
        let record = guard.as_ref()?;
        checkpoint_record_age_seconds(record)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Checkpoint adapter
// ─────────────────────────────────────────────────────────────────────────────

/// Wraps a `FileCheckpoint` with a Kafka durable publish on every `save()`.
///
/// The local file is updated only after the Kafka publish succeeds, so the
/// authoritative offset is always the value in Kafka.
pub(crate) struct KafkaTopicCheckpoint {
    inner: FileCheckpoint,
    writer: Arc<KafkaTopicStateWriter>,
}

impl KafkaTopicCheckpoint {
    pub(crate) fn new(inner: FileCheckpoint, writer: Arc<KafkaTopicStateWriter>) -> Self {
        Self { inner, writer }
    }
}

#[async_trait]
impl Checkpoint for KafkaTopicCheckpoint {
    async fn save(
        &mut self,
        offset: &dyn Offset,
        committed_event_count: u64,
    ) -> rustcdc::core::Result<()> {
        let record = KafkaTopicCheckpointRecord {
            record_version: KAFKA_TOPIC_CHECKPOINT_VERSION,
            source_type: offset.source_type().to_string(),
            offset_hex: hex::encode(offset.encode()?),
            committed_event_count,
            saved_at_unix_ms: now_unix_ms(),
        };

        self.writer
            .update_checkpoint(record)
            .await
            .map_err(|e| rustcdc::core::Error::StateError(e.to_string()))?;

        // Persist to local file only after Kafka publish succeeds.
        self.inner.save(offset, committed_event_count).await?;

        Ok(())
    }

    async fn load(&self) -> rustcdc::core::Result<Option<Box<dyn Offset>>> {
        self.inner.load().await
    }

    async fn get_committed_count(&self) -> rustcdc::core::Result<u64> {
        self.inner.get_committed_count().await
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Schema-history adapter
// ─────────────────────────────────────────────────────────────────────────────

/// Wraps a `FileSchemaHistory` with a Kafka durable publish after DDL events
/// and retention runs, using a dirty flag to avoid unnecessary writes.
pub(crate) struct KafkaTopicSchemaHistory {
    inner: FileSchemaHistory,
    writer: Arc<KafkaTopicStateWriter>,
    state_file: PathBuf,
}

impl KafkaTopicSchemaHistory {
    pub(crate) fn new(
        inner: FileSchemaHistory,
        writer: Arc<KafkaTopicStateWriter>,
        state_file: PathBuf,
    ) -> Self {
        Self {
            inner,
            writer,
            state_file,
        }
    }

    /// Read the local schema-history state file and publish to Kafka when dirty.
    /// On publish failure, rolls back the local file to the last successfully
    /// published bytes to keep local and remote state consistent.
    async fn flush_dirty_snapshot(&mut self) -> rustcdc::core::Result<()> {
        let snapshot_bytes = std::fs::read(&self.state_file).map_err(|e| {
            rustcdc::core::Error::StateError(format!(
                "failed reading schema history snapshot '{}': {e}",
                self.state_file.display()
            ))
        })?;

        self.writer.mark_schema_history_dirty();

        if let Err(e) = self
            .writer
            .flush_schema_history_if_dirty(snapshot_bytes)
            .await
        {
            let rollback_result = match self.writer.last_published_schema_history_bytes().await {
                Some(previous_bytes) => std::fs::write(&self.state_file, previous_bytes),
                None => match std::fs::remove_file(&self.state_file) {
                    Ok(()) => Ok(()),
                    Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
                    Err(err) => Err(err),
                },
            };

            return match rollback_result {
                Ok(()) => match FileSchemaHistory::new(&self.state_file).await {
                    Ok(reloaded) => {
                        self.inner = reloaded;
                        Err(rustcdc::core::Error::StateError(format!(
                            "failed publishing schema history to kafka topic state: {e}; \
                             local schema history rolled back"
                        )))
                    }
                    Err(reload_err) => Err(rustcdc::core::Error::StateError(format!(
                        "failed publishing schema history to kafka topic state: {e}; \
                         local rollback completed but in-memory reload failed: {reload_err}"
                    ))),
                },
                Err(rollback_err) => Err(rustcdc::core::Error::StateError(format!(
                    "failed publishing schema history to kafka topic state: {e}; \
                     rollback also failed: {rollback_err}"
                ))),
            };
        }

        Ok(())
    }
}

#[async_trait]
impl SchemaHistory for KafkaTopicSchemaHistory {
    async fn record_ddl(&mut self, ddl: DDLEvent) -> rustcdc::core::Result<u32> {
        let version = self.inner.record_ddl(ddl).await?;
        self.flush_dirty_snapshot().await?;
        Ok(version)
    }

    async fn get_schema_at_version(
        &self,
        table: &str,
        version: u32,
    ) -> rustcdc::core::Result<Option<TableSchema>> {
        self.inner.get_schema_at_version(table, version).await
    }

    async fn get_schema_at_timestamp(
        &self,
        table: &str,
        ts: u64,
    ) -> rustcdc::core::Result<Option<TableSchema>> {
        self.inner.get_schema_at_timestamp(table, ts).await
    }

    async fn latest_schema(&self, table: &str) -> rustcdc::core::Result<Option<TableSchema>> {
        self.inner.latest_schema(table).await
    }

    async fn apply_retention(
        &mut self,
        retention: SchemaHistoryRetention,
    ) -> rustcdc::core::Result<usize> {
        let removed = self.inner.apply_retention(retention).await?;
        self.flush_dirty_snapshot().await?;
        Ok(removed)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Build + initialize
// ─────────────────────────────────────────────────────────────────────────────

/// Build a fully initialised Kafka-backed `RuntimeState` from the loaded
/// topic records.  Restores local filesystem mirrors from the Kafka snapshot
/// so that `FileCheckpoint` / `FileSchemaHistory` remain the immediate-access
/// layer while Kafka is the authoritative durability layer.
/// Build both checkpoint and schema-history backed by a shared Kafka topic writer.
/// Returns checkpoint, schema_history, and the shared writer (for `CheckpointAgeSource`).
async fn build_both(
    state_dir: &Path,
    config: &KafkaTopicStateConfig,
) -> Result<
    (
        KafkaTopicCheckpoint,
        KafkaTopicSchemaHistory,
        Arc<KafkaTopicStateWriter>,
    ),
    AppError,
> {
    std::fs::create_dir_all(state_dir)?;
    validate_prerequisites(config).await?;

    let loaded = load_records(config).await?;
    validate_loaded_records(&loaded, config)?;

    let checkpoint_dir = state_dir.join("checkpoint");
    std::fs::create_dir_all(&checkpoint_dir)?;
    let mut file_checkpoint = FileCheckpoint::new(checkpoint_dir.clone());
    if let Some(record) = loaded.checkpoint.clone() {
        if !is_bootstrap_checkpoint_record(&record) {
            let offset_bytes = hex::decode(&record.offset_hex).map_err(|e| {
                AppError::Other(format!(
                    "state.backend.kafka_topic snapshot checkpoint has invalid offset_hex: {e}"
                ))
            })?;
            let offset = GenericOffset::new(record.source_type, offset_bytes);
            file_checkpoint
                .save(&offset, record.committed_event_count)
                .await
                .map_err(|e| {
                    AppError::Other(format!(
                        "failed restoring checkpoint from kafka topic state: {e}"
                    ))
                })?;
        }
    }

    let schema_history_path = state_dir.join("schema_history");
    if let Some(bytes) = loaded.schema_history_bytes.clone() {
        std::fs::write(&schema_history_path, bytes)?;
    }
    let file_schema_history = FileSchemaHistory::new(&schema_history_path)
        .await
        .map_err(|e| {
            AppError::Other(format!(
                "failed opening schema history file '{}': {e}",
                schema_history_path.display()
            ))
        })?;

    let writer = Arc::new(KafkaTopicStateWriter::new(config).await?);
    writer
        .restore_from_loaded(loaded.checkpoint, loaded.schema_history_bytes)
        .await;

    Ok((
        KafkaTopicCheckpoint::new(file_checkpoint, writer.clone()),
        KafkaTopicSchemaHistory::new(file_schema_history, writer.clone(), schema_history_path),
        writer,
    ))
}

/// Build the Kafka-backed checkpoint. Returns the checkpoint and the shared
/// writer (to be wrapped in `CheckpointAgeSource::KafkaTopic`).
pub(crate) async fn build_checkpoint(
    state_dir: &Path,
    config: &KafkaTopicStateConfig,
) -> Result<(KafkaTopicCheckpoint, Arc<KafkaTopicStateWriter>), AppError> {
    let (checkpoint, _schema_history, writer) = build_both(state_dir, config).await?;
    Ok((checkpoint, writer))
}

/// Build the Kafka-backed schema-history.
pub(crate) async fn build_schema_history(
    state_dir: &Path,
    config: &KafkaTopicStateConfig,
) -> Result<KafkaTopicSchemaHistory, AppError> {
    let (_checkpoint, schema_history, _writer) = build_both(state_dir, config).await?;
    Ok(schema_history)
}

/// Seed an empty compacted topic with bootstrap records so that subsequent
/// `build()` calls find a consistent (checkpoint + schema_history) pair.
pub(crate) async fn initialize(
    config: &KafkaTopicStateConfig,
    force: bool,
) -> Result<(), AppError> {
    validate_prerequisites(config).await?;

    let loaded = load_records(config).await?;
    let has_existing_state = loaded.checkpoint.is_some() || loaded.schema_history.is_some();

    if has_existing_state && !force {
        return Err(AppError::Other(format!(
            "state.backend.kafka_topic.topic '{}' already contains state records; \
             re-run with --force to overwrite",
            config.topic
        )));
    }

    let writer = KafkaTopicStateWriter::new(config).await?;
    writer.seed_bootstrap().await?;

    metrics::mark_bootstrap_seeded();
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Container for the two records read from the compacted topic.
pub(crate) struct LoadedKafkaRecords {
    pub(crate) checkpoint: Option<KafkaTopicCheckpointRecord>,
    pub(crate) schema_history_bytes: Option<Vec<u8>>,
    pub(crate) schema_history: Option<KafkaTopicSchemaHistoryRecord>,
    /// `true` when the topic still contains the legacy monolithic `b"state"` key.
    pub(crate) legacy_format_detected: bool,
}

fn bootstrap_checkpoint_record() -> KafkaTopicCheckpointRecord {
    KafkaTopicCheckpointRecord {
        record_version: KAFKA_TOPIC_CHECKPOINT_VERSION,
        source_type: KAFKA_STATE_BOOTSTRAP_SOURCE_TYPE.to_string(),
        offset_hex: String::new(),
        committed_event_count: 0,
        saved_at_unix_ms: now_unix_ms(),
    }
}

pub(crate) fn is_bootstrap_checkpoint_record(record: &KafkaTopicCheckpointRecord) -> bool {
    record.source_type == KAFKA_STATE_BOOTSTRAP_SOURCE_TYPE
        && record.offset_hex.is_empty()
        && record.committed_event_count == 0
}

pub(crate) fn checkpoint_record_age_seconds(record: &KafkaTopicCheckpointRecord) -> Option<f64> {
    if is_bootstrap_checkpoint_record(record) {
        return None;
    }
    let now_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()?
        .as_millis() as u64;
    if now_ms < record.saved_at_unix_ms {
        return Some(0.0);
    }
    Some((now_ms - record.saved_at_unix_ms) as f64 / 1000.0)
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn validate_prerequisites(config: &KafkaTopicStateConfig) -> Result<(), AppError> {
    let auth = config.security.to_auth_config().map_err(AppError::Other)?;
    let admin = AdminClient::builder()
        .bootstrap_servers(config.brokers.clone())
        .client_id(format!("{}-state-admin", config.client_id))
        .request_timeout(std::time::Duration::from_millis(config.request_timeout_ms))
        .auth(auth)
        .build()
        .await
        .map_err(|e| {
            AppError::Other(format!(
                "failed to build kafka admin client for state backend durability checks: {e}"
            ))
        })?;

    let topics = admin
        .describe_topics(std::slice::from_ref(&config.topic))
        .await
        .map_err(|e| {
            AppError::Other(format!(
                "failed to describe kafka topic '{}' for state backend: {e}",
                config.topic
            ))
        })?;

    let Some((_, topic)) = topics.into_iter().find(|(name, _)| name == &config.topic) else {
        return Err(AppError::Other(format!(
            "state.backend.kafka_topic.topic '{}' not found in broker metadata",
            config.topic
        )));
    };

    if topic.partitions.is_empty() {
        return Err(AppError::Other(format!(
            "state.backend.kafka_topic.topic '{}' has no partitions",
            config.topic
        )));
    }

    for partition in topic.partitions.values() {
        let replication_factor = partition.replicas.len() as u16;
        let in_sync_replicas = partition.isr.len() as u16;

        if replication_factor < config.min_replication_factor {
            return Err(AppError::Other(format!(
                "state.backend.kafka_topic.topic '{}' partition {} replication factor {} is \
                 below required {}",
                config.topic,
                partition.partition,
                replication_factor,
                config.min_replication_factor
            )));
        }
        if in_sync_replicas < config.min_insync_replicas {
            return Err(AppError::Other(format!(
                "state.backend.kafka_topic.topic '{}' partition {} ISR {} is below required {}",
                config.topic, partition.partition, in_sync_replicas, config.min_insync_replicas
            )));
        }
        if !partition.offline_replicas.is_empty() {
            return Err(AppError::Other(format!(
                "state.backend.kafka_topic.topic '{}' partition {} has offline replicas: {:?}",
                config.topic, partition.partition, partition.offline_replicas
            )));
        }
    }

    let config_entries = admin
        .describe_configs(DescribeConfigsRequest::for_topic(config.topic.clone()))
        .await
        .map_err(|e| {
            AppError::Other(format!(
                "failed to describe kafka topic configs for '{}': {e}",
                config.topic
            ))
        })?;

    let config_values = extract_topic_config_values(&config_entries);
    validate_topic_config_entries(config, &config_values)?;

    Ok(())
}

fn extract_topic_config_values(entries: &[ConfigEntry]) -> HashMap<String, String> {
    entries
        .iter()
        .filter_map(|e| e.value.as_ref().map(|v| (e.name.clone(), v.clone())))
        .collect()
}

pub(crate) fn validate_topic_config_entries(
    config: &KafkaTopicStateConfig,
    values: &HashMap<String, String>,
) -> Result<(), AppError> {
    if values.is_empty() {
        return Err(AppError::Other(format!(
            "state.backend.kafka_topic.topic '{}' returned no topic config entries; \
             cannot verify cleanup.policy/min.insync.replicas",
            config.topic
        )));
    }

    let cleanup_policy = values.get("cleanup.policy").ok_or_else(|| {
        AppError::Other(format!(
            "state.backend.kafka_topic.topic '{}' missing required topic config \
                 'cleanup.policy'",
            config.topic
        ))
    })?;

    let has_compact = cleanup_policy
        .split(',')
        .map(str::trim)
        .any(|v| v.eq_ignore_ascii_case("compact"));
    if !has_compact {
        return Err(AppError::Other(format!(
            "state.backend.kafka_topic.topic '{}' cleanup.policy='{}' must include 'compact'",
            config.topic, cleanup_policy
        )));
    }

    let min_isr = values.get("min.insync.replicas").ok_or_else(|| {
        AppError::Other(format!(
            "state.backend.kafka_topic.topic '{}' missing required topic config \
                 'min.insync.replicas'",
            config.topic
        ))
    })?;

    let min_isr_value = min_isr.parse::<u16>().map_err(|e| {
        AppError::Other(format!(
            "state.backend.kafka_topic.topic '{}' has invalid min.insync.replicas='{}': {e}",
            config.topic, min_isr
        ))
    })?;

    if min_isr_value < config.min_insync_replicas {
        return Err(AppError::Other(format!(
            "state.backend.kafka_topic.topic '{}' min.insync.replicas {} is below required {}",
            config.topic, min_isr_value, config.min_insync_replicas
        )));
    }

    Ok(())
}

async fn load_records(config: &KafkaTopicStateConfig) -> Result<LoadedKafkaRecords, AppError> {
    let auth = config.security.to_auth_config().map_err(AppError::Other)?;

    let mut consumer = CompactedTopicConsumer::builder()
        .bootstrap_servers(config.brokers.clone())
        .topic(config.topic.clone())
        .client_id(format!("{}-state-readback", config.client_id))
        .request_timeout(std::time::Duration::from_millis(config.request_timeout_ms))
        .auth(auth)
        .build()
        .await
        .map_err(|e| {
            AppError::Other(format!(
                "failed to build compacted topic scanner for state backend: {e}"
            ))
        })?;

    consumer
        .scan(std::time::Duration::from_millis(
            config.readback_poll_timeout_ms,
        ))
        .await
        .map_err(|e| {
            AppError::Other(format!(
                "failed scanning compacted kafka state topic '{}': {e}",
                config.topic
            ))
        })?;

    let table = consumer.table();

    let mut checkpoint: Option<KafkaTopicCheckpointRecord> = None;
    let mut schema_history_record: Option<KafkaTopicSchemaHistoryRecord> = None;
    let mut legacy_format_detected = false;

    if let Some(bytes) = table.get(KAFKA_STATE_KEY_CHECKPOINT) {
        checkpoint = Some(serde_json::from_slice(bytes.value.as_ref()).map_err(|e| {
            metrics::mark_corruption_detected();
            AppError::Other(format!(
                "topic '{}' checkpoint record is corrupted: {e}",
                config.topic
            ))
        })?);
    }

    if let Some(bytes) = table.get(KAFKA_STATE_KEY_SCHEMA_HISTORY) {
        schema_history_record =
            Some(serde_json::from_slice(bytes.value.as_ref()).map_err(|e| {
                metrics::mark_corruption_detected();
                AppError::Other(format!(
                    "topic '{}' schema_history record is corrupted: {e}",
                    config.topic
                ))
            })?);
    }

    if table.contains_key(KAFKA_STATE_KEY_LEGACY_SNAPSHOT) {
        legacy_format_detected = true;
        tracing::warn!(
            topic = config.topic.as_str(),
            "Detected legacy monolithic state snapshot (key=`state`) in topic. \
             This format is no longer supported. Run `cdc init-state --force` \
             after upgrading to v2 key layout."
        );
    }

    consumer.close().await.map_err(|e| {
        AppError::Other(format!(
            "failed to close compacted topic scanner for '{}': {e}",
            config.topic
        ))
    })?;

    let schema_history_bytes = schema_history_record
        .as_ref()
        .map(|r| r.schema_history_bytes.clone());

    Ok(LoadedKafkaRecords {
        checkpoint,
        schema_history_bytes,
        schema_history: schema_history_record,
        legacy_format_detected,
    })
}

/// Read raw state bytes from a Kafka compacted-topic for migration purposes.
///
/// Returns `(checkpoint_json_bytes, schema_history_bytes_opt)`.  The checkpoint
/// bytes are the raw JSON from the Kafka record; schema history bytes are the
/// inner `FileSchemaHistory` blob (already unwrapped from the Kafka envelope).
pub(crate) async fn load_raw_bytes_for_migration(
    config: &KafkaTopicStateConfig,
) -> Result<(Option<Vec<u8>>, Option<Vec<u8>>), AppError> {
    let loaded = load_records(config).await?;

    let checkpoint_bytes = loaded
        .checkpoint
        .as_ref()
        .map(|rec| serde_json::to_vec(rec).expect("KafkaTopicCheckpointRecord serializes cleanly"));

    Ok((checkpoint_bytes, loaded.schema_history_bytes))
}

pub(crate) fn validate_loaded_records(
    loaded: &LoadedKafkaRecords,
    config: &KafkaTopicStateConfig,
) -> Result<(), AppError> {
    if loaded.legacy_format_detected {
        return Err(AppError::Other(format!(
            "state.backend.kafka_topic.topic '{}' contains a legacy monolithic state snapshot \
             (key=`state`). Re-initialize with `cdc init-state --force`.",
            config.topic
        )));
    }

    match (&loaded.checkpoint, &loaded.schema_history) {
        (None, None) => Err(AppError::Other(format!(
            "state.backend.kafka_topic.topic '{}' has no state records. \
             Run `cdc init-state` first or check topic compaction configuration.",
            config.topic
        ))),
        (Some(_), None) | (None, Some(_)) => {
            metrics::mark_corruption_detected();
            Err(AppError::Other(format!(
                "state.backend.kafka_topic.topic '{}' has inconsistent state: \
                 checkpoint and schema_history records must both be present or both absent. \
                 Topic may have been partially written or compacted unevenly.",
                config.topic
            )))
        }
        (Some(chk), Some(_)) => {
            if chk.record_version != KAFKA_TOPIC_CHECKPOINT_VERSION {
                metrics::mark_corruption_detected();
                return Err(AppError::Other(format!(
                    "state.backend.kafka_topic.topic '{}' checkpoint record_version={} is not \
                     supported (expected {}). Please re-initialize state.",
                    config.topic, chk.record_version, KAFKA_TOPIC_CHECKPOINT_VERSION
                )));
            }
            Ok(())
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::schema::{KafkaSecurityConfig, KafkaTopicStateConfig};
    use crate::state::metrics::kafka_topic_state_corruption_detected_total;

    fn sample_config() -> KafkaTopicStateConfig {
        KafkaTopicStateConfig {
            brokers: "localhost:9092".to_string(),
            topic: "cdc-state".to_string(),
            client_id: "cdc-server".to_string(),
            request_timeout_ms: 1_000,
            readback_poll_timeout_ms: 250,
            min_replication_factor: 1,
            min_insync_replicas: 1,
            durability_profile: crate::config::schema::KafkaStateDurabilityProfile::Development,
            security: KafkaSecurityConfig::default(),
        }
    }

    fn sample_checkpoint() -> KafkaTopicCheckpointRecord {
        KafkaTopicCheckpointRecord {
            record_version: KAFKA_TOPIC_CHECKPOINT_VERSION,
            source_type: "postgres".to_string(),
            offset_hex: "001122".to_string(),
            committed_event_count: 42,
            saved_at_unix_ms: 99,
        }
    }

    fn sample_schema_history() -> KafkaTopicSchemaHistoryRecord {
        KafkaTopicSchemaHistoryRecord {
            record_version: KAFKA_TOPIC_SCHEMA_HISTORY_VERSION,
            schema_history_bytes: br#"[{"version":1}]"#.to_vec(),
            saved_at_unix_ms: 99,
        }
    }

    fn loaded_with_both(
        chk: KafkaTopicCheckpointRecord,
        sh: KafkaTopicSchemaHistoryRecord,
    ) -> LoadedKafkaRecords {
        LoadedKafkaRecords {
            checkpoint: Some(chk),
            schema_history_bytes: Some(sh.schema_history_bytes.clone()),
            schema_history: Some(sh),
            legacy_format_detected: false,
        }
    }

    fn loaded_empty() -> LoadedKafkaRecords {
        LoadedKafkaRecords {
            checkpoint: None,
            schema_history_bytes: None,
            schema_history: None,
            legacy_format_detected: false,
        }
    }

    #[test]
    fn rejects_empty_topic() {
        let err = validate_loaded_records(&loaded_empty(), &sample_config())
            .expect_err("empty topic must fail closed");
        assert!(err.to_string().contains("no state records"));
    }

    #[test]
    fn rejects_checkpoint_without_schema_history() {
        let before = kafka_topic_state_corruption_detected_total();
        let loaded = LoadedKafkaRecords {
            checkpoint: Some(sample_checkpoint()),
            schema_history_bytes: None,
            schema_history: None,
            legacy_format_detected: false,
        };
        let err = validate_loaded_records(&loaded, &sample_config())
            .expect_err("incomplete records must fail closed");
        assert!(err.to_string().contains("inconsistent state"));
        assert!(kafka_topic_state_corruption_detected_total() > before);
    }

    #[test]
    fn rejects_schema_history_without_checkpoint() {
        let before = kafka_topic_state_corruption_detected_total();
        let sh = sample_schema_history();
        let loaded = LoadedKafkaRecords {
            checkpoint: None,
            schema_history_bytes: Some(sh.schema_history_bytes.clone()),
            schema_history: Some(sh),
            legacy_format_detected: false,
        };
        let err = validate_loaded_records(&loaded, &sample_config())
            .expect_err("schema history without checkpoint must fail closed");
        assert!(err.to_string().contains("inconsistent state"));
        assert!(kafka_topic_state_corruption_detected_total() > before);
    }

    #[test]
    fn rejects_legacy_format() {
        let loaded = LoadedKafkaRecords {
            checkpoint: None,
            schema_history_bytes: None,
            schema_history: None,
            legacy_format_detected: true,
        };
        let err = validate_loaded_records(&loaded, &sample_config())
            .expect_err("legacy format must fail with guidance");
        assert!(err.to_string().contains("legacy"));
    }

    #[test]
    fn rejects_unsupported_checkpoint_version() {
        let before = kafka_topic_state_corruption_detected_total();
        let mut chk = sample_checkpoint();
        chk.record_version = KAFKA_TOPIC_CHECKPOINT_VERSION + 1;
        let err = validate_loaded_records(
            &loaded_with_both(chk, sample_schema_history()),
            &sample_config(),
        )
        .expect_err("unsupported version must fail");
        assert!(err.to_string().contains("record_version"));
        assert!(kafka_topic_state_corruption_detected_total() > before);
    }

    #[test]
    fn checkpoint_record_json_round_trip() {
        let record = sample_checkpoint();
        let encoded = serde_json::to_vec(&record).expect("serialize");
        let decoded: KafkaTopicCheckpointRecord =
            serde_json::from_slice(&encoded).expect("deserialize");
        assert_eq!(decoded.record_version, KAFKA_TOPIC_CHECKPOINT_VERSION);
        assert_eq!(decoded.source_type, "postgres");
        assert_eq!(decoded.offset_hex, "001122");
        assert_eq!(decoded.committed_event_count, 42);
    }

    #[test]
    fn schema_history_record_json_round_trip() {
        let record = sample_schema_history();
        let encoded = serde_json::to_vec(&record).expect("serialize");
        let decoded: KafkaTopicSchemaHistoryRecord =
            serde_json::from_slice(&encoded).expect("deserialize");
        assert_eq!(decoded.record_version, KAFKA_TOPIC_SCHEMA_HISTORY_VERSION);
        assert_eq!(decoded.schema_history_bytes, br#"[{"version":1}]"#);
    }

    #[test]
    fn bootstrap_record_is_identified() {
        let record = bootstrap_checkpoint_record();
        assert!(is_bootstrap_checkpoint_record(&record));
        let mut non_bootstrap = record.clone();
        non_bootstrap.source_type = "postgres".to_string();
        assert!(!is_bootstrap_checkpoint_record(&non_bootstrap));
    }

    #[test]
    fn checkpoint_age_ignores_bootstrap_marker() {
        let record = bootstrap_checkpoint_record();
        assert!(
            checkpoint_record_age_seconds(&record).is_none(),
            "bootstrap marker must not report authoritative checkpoint freshness"
        );
    }

    #[test]
    fn checkpoint_age_clamps_future_timestamp_to_zero() {
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("unix now")
            .as_millis() as u64;
        let record = KafkaTopicCheckpointRecord {
            record_version: KAFKA_TOPIC_CHECKPOINT_VERSION,
            source_type: "postgres".to_string(),
            offset_hex: "001122".to_string(),
            committed_event_count: 42,
            saved_at_unix_ms: now_ms.saturating_add(30_000),
        };
        let age = checkpoint_record_age_seconds(&record).expect("age must be Some");
        assert_eq!(age, 0.0, "future timestamps must clamp to zero");
    }

    #[test]
    fn topic_config_rejects_cleanup_policy_without_compact() {
        let cfg = sample_config();
        let entries: HashMap<String, String> = [
            ("cleanup.policy".to_string(), "delete".to_string()),
            ("min.insync.replicas".to_string(), "1".to_string()),
        ]
        .into();
        let err = validate_topic_config_entries(&cfg, &entries)
            .expect_err("cleanup policy without compact must fail");
        assert!(err.to_string().contains("must include 'compact'"));
    }

    #[test]
    fn topic_config_rejects_low_min_insync_replicas() {
        let mut cfg = sample_config();
        cfg.min_insync_replicas = 2;
        let entries: HashMap<String, String> = [
            ("cleanup.policy".to_string(), "compact".to_string()),
            ("min.insync.replicas".to_string(), "1".to_string()),
        ]
        .into();
        let err = validate_topic_config_entries(&cfg, &entries).expect_err("low min ISR must fail");
        assert!(err.to_string().contains("below required"));
    }

    #[test]
    fn topic_config_accepts_valid_durability_settings() {
        let mut cfg = sample_config();
        cfg.min_insync_replicas = 2;
        let entries: HashMap<String, String> = [
            ("cleanup.policy".to_string(), "delete,compact".to_string()),
            ("min.insync.replicas".to_string(), "2".to_string()),
        ]
        .into();
        validate_topic_config_entries(&cfg, &entries).expect("valid durability settings must pass");
    }
}
