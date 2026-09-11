use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use async_trait::async_trait;
use krafka::admin::{AdminClient, ConfigEntry, DescribeConfigsRequest};
use krafka::consumer::CompactedTopicConsumer;
use krafka::producer::TransactionalProducer;
use rustcdc::checkpoint::{
    Checkpoint, FileCheckpoint, GenericOffset, StoredCheckpointRecord, validate_checkpoint_progress,
};
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

/// Where the state writer's records are produced from.
///
/// The distinction is the whole of end-to-end exactly-once.
///
/// * [`StateProducer::Own`] — a private transactional producer. Each state write is its
///   own transaction, so the checkpoint becomes durable independently of the batch it
///   describes. A crash between the sink's commit and this write replays the batch, and a
///   `read_committed` consumer sees the duplicates. That window is the reason
///   `effectively_once` was documented as *batch-atomic* rather than exactly-once.
///
/// * [`StateProducer::Shared`] — the sink's own producer. The checkpoint record is written
///   into the *same* transaction as the batch's data, so a single `commit_transaction`
///   makes both durable together and there is no window between them. A crash before the
///   commit discards both; after it, both survive.
///
/// Either way the producer is transactional, because that is also what fences a second
/// writer: `init_transactions()` bumps the producer epoch at the broker and permanently
/// fences any earlier holder of the same `transactional.id` (KIP-447). This topic once
/// used a plain idempotent producer, so two instances — which a rolling update creates on
/// every deploy — both wrote, and last-write-wins could move the durable position
/// *backwards*.
enum StateProducer {
    /// Boxed: a `TransactionalProducer` is ~1.3 KB and would otherwise size every
    /// `StateProducer`, including the shared arm that is a pair of pointers.
    Own(Box<tokio::sync::Mutex<TransactionalProducer>>),
    Shared(crate::sink::KafkaTransactionHandle),
}

/// Publishes checkpoint and schema-history records to the Kafka compacted topic.
///
/// Holds the last published values so that callers can compute checkpoint age and roll
/// back schema history on publish failure.
pub(crate) struct KafkaTopicStateWriter {
    /// How state records reach Kafka.
    ///
    /// Two shapes, and which one is in use decides whether `effectively_once` is
    /// end-to-end exactly-once or only atomic at the sink — see [`StateProducer`].
    producer: StateProducer,
    topic: String,
    last_checkpoint: tokio::sync::Mutex<Option<KafkaTopicCheckpointRecord>>,
    last_schema_history_bytes: tokio::sync::Mutex<Option<Vec<u8>>>,
    /// Set when schema history has changed since the last Kafka publish.
    schema_history_dirty: AtomicBool,
}

impl KafkaTopicStateWriter {
    /// Build a writer with its own transactional producer.
    ///
    /// Used by `init-state` and by every backend combination that is not end-to-end
    /// exactly-once. See [`Self::with_sink_transaction`] for the shared-producer form.
    pub(super) async fn new(config: &KafkaTopicStateConfig) -> Result<Self, AppError> {
        Ok(Self::wrap(config, Self::own_producer(config).await?))
    }

    /// Build a writer that produces through the sink's transaction.
    ///
    /// The handle's producer is *already* fenced by its own `init_transactions()`, and it
    /// is deliberately the only writer of this topic in this mode: a second transactional
    /// id writing the same topic would give two independent fencing domains, so neither
    /// would exclude the other.
    pub(super) fn with_sink_transaction(
        config: &KafkaTopicStateConfig,
        handle: crate::sink::KafkaTransactionHandle,
    ) -> Self {
        tracing::info!(
            topic = %config.topic,
            "kafka state writer is sharing the sink's transaction; checkpoints commit \
             atomically with the data they describe"
        );
        Self::wrap(config, StateProducer::Shared(handle))
    }

    fn wrap(config: &KafkaTopicStateConfig, producer: StateProducer) -> Self {
        Self {
            producer,
            topic: config.topic.clone(),
            last_checkpoint: tokio::sync::Mutex::new(None),
            last_schema_history_bytes: tokio::sync::Mutex::new(None),
            schema_history_dirty: AtomicBool::new(false),
        }
    }

    async fn own_producer(config: &KafkaTopicStateConfig) -> Result<StateProducer, AppError> {
        let auth = config.security.to_auth_config().map_err(AppError::Other)?;
        // Derived from the state topic and client id so it is stable across restarts of
        // the same logical pipeline — which is the whole point, since fencing works by
        // two instances claiming the *same* id — and distinct between pipelines sharing
        // a cluster.
        let transactional_id = format!("{}-state-{}", config.client_id, config.topic);

        let producer = TransactionalProducer::builder()
            .bootstrap_servers(config.brokers.clone())
            .client_id(format!("{}-state-writer", config.client_id))
            .transactional_id(transactional_id.clone())
            .request_timeout(std::time::Duration::from_millis(config.request_timeout_ms))
            .connect_timeout(crate::sink::kafka_connect_timeout(
                std::time::Duration::from_millis(config.request_timeout_ms),
            ))
            .auth(auth)
            .build()
            .await
            .map_err(|e| {
                AppError::Other(format!("failed to build kafka topic state producer: {e}"))
            })?;

        // Fences any previous owner of this transactional id.
        producer.init_transactions().await.map_err(|e| {
            AppError::Other(format!(
                "failed to initialize the kafka state producer's transactional state                  (transactional_id '{transactional_id}'): {e}. If another instance is                  running against this pipeline, stop it first — two writers on one state                  topic interleave checkpoints and the durable position can move backwards."
            ))
        })?;

        tracing::info!(
            transactional_id = %transactional_id,
            "kafka state writer fenced any previous owner"
        );

        Ok(StateProducer::Own(Box::new(tokio::sync::Mutex::new(
            producer,
        ))))
    }

    /// Publish one state record.
    ///
    /// Returns `true` when the record joined the sink's open transaction and is therefore
    /// not durable until that transaction commits. Callers use this to decide whether the
    /// in-memory "last published" mirror may be updated yet.
    async fn publish_record(&self, key: &[u8], payload: &[u8]) -> Result<bool, AppError> {
        match &self.producer {
            StateProducer::Shared(handle) => handle
                .send_in_transaction(&self.topic, key, payload)
                .await
                .map_err(|e| {
                    AppError::Other(format!(
                        "failed writing kafka state record to topic '{}': {e}. A \
                         producer-fenced error here means another instance has taken over \
                         this pipeline's state.",
                        self.topic
                    ))
                }),
            StateProducer::Own(producer) => {
                let producer = producer.lock().await;

                producer.begin_transaction().map_err(|e| {
                    AppError::Other(format!("failed to begin kafka state transaction: {e}"))
                })?;

                let send_result = producer.send(&self.topic, Some(key), Some(payload)).await;

                let record_metadata = match send_result {
                    Ok(metadata) => metadata,
                    Err(e) => {
                        // Abort is best-effort: a fenced producer cannot abort either, and
                        // the send error is the one worth surfacing.
                        if let Err(abort_err) = producer.abort_transaction().await {
                            tracing::warn!(
                                error = %abort_err,
                                "aborting the kafka state transaction failed after a send error"
                            );
                        }
                        return Err(AppError::Other(format!(
                            "failed writing kafka state record to topic '{}': {e}. A \
                             producer-fenced error here means another instance has taken over \
                             this pipeline's state.",
                            self.topic
                        )));
                    }
                };

                producer.commit_transaction().await.map_err(|e| {
                    AppError::Other(format!(
                        "failed to commit the kafka state transaction for topic '{}': {e}",
                        self.topic
                    ))
                })?;
                // State records are checkpoints — an unacknowledged write here would let
                // the pipeline advance past a position Kafka never durably stored.
                crate::sink::enforce_durable_confirmation(&record_metadata, "state")
                    .map_err(|e| AppError::Other(e.to_string()))?;
                Ok(false)
            }
        }
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

    /// Publish a checkpoint record.
    ///
    /// Returns `true` when the record joined the sink's open transaction, and is therefore
    /// only durable once that transaction commits.
    async fn update_checkpoint(
        &self,
        checkpoint_record: KafkaTopicCheckpointRecord,
    ) -> Result<bool, AppError> {
        let payload = serde_json::to_vec(&checkpoint_record)
            .map_err(|e| AppError::Other(format!("failed to serialize checkpoint record: {e}")))?;
        let joined = self
            .publish_record(KAFKA_STATE_KEY_CHECKPOINT, &payload)
            .await?;
        // The mirror is what `/status` reports as checkpoint age. Updating it for a record
        // still inside an uncommitted transaction would report a freshness the cluster has
        // not confirmed; if the commit then fails the process exits, so the mirror is never
        // observed to be stale for long.
        if !joined {
            *self.last_checkpoint.lock().await = Some(checkpoint_record);
        }
        Ok(joined)
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
    /// The last record known to be published, for the pre-write guard.
    ///
    /// Seeded from the topic at startup and advanced after each accepted write, so the
    /// guard needs no read of the state topic on the durability path. Transactional
    /// fencing on the state topic is what makes an in-memory copy sound: a second writer
    /// for the same transactional id is fenced out before it can publish.
    last: Option<StoredCheckpointRecord>,
}

impl KafkaTopicCheckpoint {
    pub(crate) fn new(
        inner: FileCheckpoint,
        writer: Arc<KafkaTopicStateWriter>,
        last: Option<StoredCheckpointRecord>,
    ) -> Self {
        Self {
            inner,
            writer,
            last,
        }
    }
}

#[async_trait]
impl Checkpoint for KafkaTopicCheckpoint {
    async fn save(
        &mut self,
        offset: &dyn Offset,
        committed_event_count: u64,
    ) -> rustcdc::core::Result<()> {
        // Validate before publishing. The guard refuses a record whose committed-event
        // count regresses, whose count collides with a different payload, or whose
        // connector-native stream position rewinds while the count keeps rising — the
        // shape a connector defect takes, and the one that makes a pipeline resume before
        // data the sink already committed while every counter reports health.
        //
        // This used to lean on `FileCheckpoint::save` for that validation and therefore
        // had to write the local file first. That worked, but it inverted the two stores:
        // the topic is authoritative and the file is a cache `build_both` rebuilds from it
        // on every startup. Calling the guard directly removes the constraint, so the
        // publish can go first and the ordering can follow from which store is the truth.
        //
        // Under `at_least_once` on this backend the publish is outside any transaction, so
        // a record that reached the topic *is* durable — there is no barrier abort to
        // retract it. That is precisely why the check cannot come after it.
        let next = StoredCheckpointRecord::from_offset(offset, committed_event_count)?;
        validate_checkpoint_progress(self.last.as_ref(), &next)?;

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

        // The local file trails the topic: it is a cache, and `build_both` purges and
        // rebuilds it from the topic on every startup, so a file that ran ahead of an
        // aborted transaction is corrected before it is ever read.
        self.inner.save(offset, committed_event_count).await?;
        self.last = Some(next);

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
        // `tokio::fs` rather than `std::fs`: this runs on the checkpoint/flush path, and
        // a blocking read here stalls the whole runtime worker. Small files make that
        // cheap today, but the checkpoint path is the one that must not head-of-line
        // block on a slow or network-backed filesystem.
        let snapshot_bytes = tokio::fs::read(&self.state_file).await.map_err(|e| {
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
                Some(previous_bytes) => tokio::fs::write(&self.state_file, previous_bytes).await,
                None => match tokio::fs::remove_file(&self.state_file).await {
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
    async fn record_ddl(&mut self, ddl_id: &str, ddl: DDLEvent) -> rustcdc::core::Result<u32> {
        // See the note in `state/schema_history/opendal.rs`: the identity is what makes a
        // replayed DDL idempotent, and this adapter must not absorb it.
        let version = self.inner.record_ddl(ddl_id, ddl).await?;
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

/// Rebuild the local checkpoint mirror from the topic, and the guard's baseline with it.
///
/// The local file is a **cache** of what Kafka holds, never a second source of truth, so
/// it is rebuilt on every startup — including when the topic says there is no position
/// yet.
///
/// Purging first is the part that matters. Restoring only when Kafka has a real record
/// left a stale local file in place whenever the topic held nothing, and `load()` reads
/// the file: the pipeline would resume from a position Kafka had never confirmed and skip
/// everything after it. That is silent data loss, and it became easier to reach once the
/// checkpoint moved inside the sink transaction, where an aborted transaction deliberately
/// leaves the topic without the record the local file may already have.
///
/// Split out of [`build_both`] so it can be tested against a `FakeBroker`, which does not
/// implement `DescribeConfigs` and so cannot reach the durability preflight `build_both`
/// runs first.
async fn restore_checkpoint_mirror(
    state_dir: &Path,
    checkpoint: Option<KafkaTopicCheckpointRecord>,
) -> Result<(FileCheckpoint, Option<StoredCheckpointRecord>), AppError> {
    let checkpoint_dir = state_dir.join("checkpoint");
    if checkpoint_dir.exists() {
        std::fs::remove_dir_all(&checkpoint_dir)?;
    }
    std::fs::create_dir_all(&checkpoint_dir)?;
    let mut file_checkpoint = FileCheckpoint::new(checkpoint_dir.clone());

    let Some(record) = checkpoint.filter(|r| !is_bootstrap_checkpoint_record(r)) else {
        return Ok((file_checkpoint, None));
    };

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

    // Seed the pre-write guard from what the topic already holds, so the first write after
    // a restart is guarded like every other one. A restart is exactly when a repointed
    // source or a recreated slot first surfaces.
    let baseline = StoredCheckpointRecord::from_offset(&offset, record.committed_event_count)
        .map_err(|e| {
            AppError::Other(format!(
                "state topic checkpoint offset payload is not JSON, so the stream-position \
                 guard cannot be applied: {e}"
            ))
        })?;

    Ok((file_checkpoint, Some(baseline)))
}

/// Build a fully initialised Kafka-backed `RuntimeState` from the loaded
/// topic records.  Restores local filesystem mirrors from the Kafka snapshot
/// so that `FileCheckpoint` / `FileSchemaHistory` remain the immediate-access
/// layer while Kafka is the authoritative durability layer.
/// Build both checkpoint and schema-history backed by a shared Kafka topic writer.
/// Returns checkpoint, schema_history, and the shared writer (for `CheckpointAgeSource`).
async fn build_both(
    state_dir: &Path,
    config: &KafkaTopicStateConfig,
    sink_transaction: Option<crate::sink::KafkaTransactionHandle>,
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

    let (file_checkpoint, restored) =
        restore_checkpoint_mirror(state_dir, loaded.checkpoint.clone()).await?;

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

    // A shared handle means the sink's producer writes this topic, so no second
    // transactional producer is built — and, importantly, no second fencing domain is
    // created for the same topic.
    let writer = Arc::new(match sink_transaction {
        Some(handle) => KafkaTopicStateWriter::with_sink_transaction(config, handle),
        None => KafkaTopicStateWriter::new(config).await?,
    });
    writer
        .restore_from_loaded(loaded.checkpoint, loaded.schema_history_bytes)
        .await;

    Ok((
        KafkaTopicCheckpoint::new(file_checkpoint, writer.clone(), restored),
        KafkaTopicSchemaHistory::new(file_schema_history, writer.clone(), schema_history_path),
        writer,
    ))
}

/// Build the Kafka-backed checkpoint. Returns the checkpoint and the shared
/// writer (to be wrapped in `CheckpointAgeSource::KafkaTopic`).
pub(crate) async fn build_checkpoint(
    state_dir: &Path,
    config: &KafkaTopicStateConfig,
    sink_transaction: Option<crate::sink::KafkaTransactionHandle>,
) -> Result<(KafkaTopicCheckpoint, Arc<KafkaTopicStateWriter>), AppError> {
    let (checkpoint, _schema_history, writer) =
        build_both(state_dir, config, sink_transaction).await?;
    Ok((checkpoint, writer))
}

/// Build the Kafka-backed schema-history.
pub(crate) async fn build_schema_history(
    state_dir: &Path,
    config: &KafkaTopicStateConfig,
) -> Result<KafkaTopicSchemaHistory, AppError> {
    // Schema history is built on its own path (a different `[state.schema_history]`
    // backend), so it never shares the sink transaction: a DDL record is not part of any
    // one batch.
    let (_checkpoint, schema_history, _writer) = build_both(state_dir, config, None).await?;
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
        .connect_timeout(crate::sink::kafka_connect_timeout(
            std::time::Duration::from_millis(config.request_timeout_ms),
        ))
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

/// Build the compacted-topic scanner used to rebuild local state from the topic.
///
/// **The isolation level is the whole reason this needs saying.**
///
/// Under `effectively_once` the checkpoint record is published *inside the sink's Kafka
/// transaction*, precisely so an aborted batch leaves neither the data nor the position
/// describing it. A `read_uncommitted` scanner sees the record anyway: on restart after a
/// crash, `build_both` would rebuild the local checkpoint from a transaction that was
/// aborted (or was still open and would be), and the pipeline would resume past events
/// that were never published. That is the exact data-loss window the shared transaction
/// exists to close, reopened by the reader.
///
/// It was invisible because the test that proves the property
/// (`an_aborted_batch_publishes_neither_data_nor_checkpoint`) reads the topic through its
/// own `read_committed` consumer rather than through this path — so it asserted the
/// broker's behaviour, not the server's.
///
/// `from_consumer_builder` takes the real `ConsumerBuilder` and *imposes* `ReadCommitted`,
/// `Earliest` and no auto-commit — requirements of materialising a table, not preferences —
/// then does the metadata refresh and partition assignment itself. Reading committed-only
/// is what makes the scan correct: an aborted checkpoint must never be read back as durable.
async fn build_state_scanner(
    config: &KafkaTopicStateConfig,
) -> Result<CompactedTopicConsumer, AppError> {
    let auth = config.security.to_auth_config().map_err(AppError::Other)?;
    let connect_timeout = crate::sink::kafka_connect_timeout(std::time::Duration::from_millis(
        config.request_timeout_ms,
    ));
    // krafka validates `request_timeout >= connect_timeout` at build time, and readback is
    // a startup-time operation where a generous network budget costs nothing.
    let request_timeout =
        std::time::Duration::from_millis(config.request_timeout_ms).max(connect_timeout);

    CompactedTopicConsumer::from_consumer_builder(
        krafka::consumer::Consumer::builder()
            .bootstrap_servers(config.brokers.clone())
            .client_id(format!("{}-state-readback", config.client_id))
            .request_timeout(request_timeout)
            .connect_timeout(connect_timeout)
            .auth(auth),
        config.topic.clone(),
    )
    .await
    .map_err(|e| {
        AppError::Other(format!(
            "failed to build the state readback consumer for topic '{}': {e}",
            config.topic
        ))
    })
}

async fn load_records(config: &KafkaTopicStateConfig) -> Result<LoadedKafkaRecords, AppError> {
    let mut consumer = build_state_scanner(config).await?;

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

    // Mapped to an error rather than `.expect(...)`. The claim that this record always
    // serialises is true of the struct as written today and is not enforced by anything —
    // a future non-string map key or a non-finite float in a nested `serde_json::Value`
    // would make `to_vec` fail, and `migrate-state` would abort a disaster-recovery
    // operation with a panic and a backtrace instead of a diagnosable message.
    let checkpoint_bytes = match loaded.checkpoint.as_ref() {
        Some(record) => Some(serde_json::to_vec(record).map_err(|e| {
            AppError::Other(format!(
                "state.backend.kafka_topic checkpoint record could not be serialized for \
                 migration: {e}"
            ))
        })?),
        None => None,
    };

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

    // ── End-to-end exactly-once: checkpoint inside the sink's transaction ─────
    //
    // These are the evidence that `effectively_once` is exactly-once end to end and not
    // merely atomic at the sink. The property is not "the checkpoint is written" — the old
    // design wrote it too. It is that the checkpoint and the data it describes share one
    // transaction, so **neither can survive without the other**.

    /// Build a transactional Kafka sink and a state writer that shares its transaction,
    /// the way `run_pipeline` wires them together.
    async fn eos_pipeline(
        broker: &krafka::testing::FakeBroker,
        data_topic: &str,
        state_topic: &str,
    ) -> (crate::sink::KafkaSink, KafkaTopicStateWriter) {
        broker.create_topic(data_topic, 1);
        broker.create_topic(state_topic, 1);

        let mut sink_config: crate::config::schema::KafkaSinkConfig =
            serde_json::from_value(serde_json::json!({
                "brokers": broker.bootstrap_servers(),
                "topic": data_topic,
                "delivery_mode": "transactional",
                "transactional_id": "cdc-eos-harness",
                "ack_timeout_ms": 1_000,
            }))
            .expect("kafka sink config");
        sink_config.client_id = "cdc-eos-harness".to_string();

        let sink = crate::sink::KafkaSink::new(&sink_config)
            .await
            .expect("transactional sink");
        let handle = sink
            .transaction_handle()
            .expect("a transactional sink must offer a transaction handle");

        let mut state_config = sample_config();
        state_config.brokers = broker.bootstrap_servers();
        state_config.topic = state_topic.to_string();

        let writer = KafkaTopicStateWriter::with_sink_transaction(&state_config, handle);
        (sink, writer)
    }

    /// Read a topic as a `read_committed` consumer does — aborted records excluded.
    async fn committed_values(brokers: &str, topic: &str) -> Vec<String> {
        use krafka::consumer::{AutoOffsetReset, Consumer, IsolationLevel};

        let consumer = Consumer::builder()
            .bootstrap_servers(brokers.to_string())
            .group_id(format!("{topic}-verify"))
            .client_id(format!("{topic}-verify"))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .isolation_level(IsolationLevel::ReadCommitted)
            .enable_auto_commit(false)
            .build()
            .await
            .expect("verification consumer");
        consumer.subscribe(&[topic]).await.expect("subscribe");

        let mut values = Vec::new();
        for _ in 0..8 {
            for record in consumer
                .poll(std::time::Duration::from_millis(250))
                .await
                .expect("poll")
            {
                if let Some(value) = &record.value {
                    values.push(String::from_utf8_lossy(value.as_ref()).into_owned());
                }
            }
        }
        consumer.close().await.expect("close");
        values
    }

    fn checkpoint_at(offset_hex: &str) -> KafkaTopicCheckpointRecord {
        KafkaTopicCheckpointRecord {
            record_version: KAFKA_TOPIC_CHECKPOINT_VERSION,
            source_type: "postgres".to_string(),
            offset_hex: offset_hex.to_string(),
            committed_event_count: 3,
            saved_at_unix_ms: now_unix_ms(),
        }
    }

    /// **The reader must honour the transaction too.**
    ///
    /// `an_aborted_batch_publishes_neither_data_nor_checkpoint` proves the broker hides an
    /// aborted checkpoint from a `read_committed` consumer — but it builds that consumer
    /// itself. Production rebuilds local state through `load_records`, and that path used
    /// the compacted-topic *builder*, which offers no isolation level and therefore scanned
    /// at krafka's `read_uncommitted` default.
    ///
    /// So the aborted checkpoint was hidden from the test and visible to the server. After
    /// a crash, `build_both` would restore it and the pipeline would resume past events
    /// that were never published — the exact loss the shared transaction exists to prevent,
    /// reintroduced by the reader.
    ///
    /// Dropping `.isolation_level(IsolationLevel::ReadCommitted)` from `build_state_scanner`
    /// fails this test.
    #[tokio::test]
    async fn the_state_readback_does_not_see_an_aborted_checkpoint() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        let (mut sink, writer) =
            eos_pipeline(&broker, "eos.data.readback", "eos.state.readback").await;

        let mut config = sample_config();
        config.brokers = broker.bootstrap_servers();
        config.topic = "eos.state.readback".to_string();

        // A committed checkpoint first, so the assertion below distinguishes "reads only
        // committed records" from "reads nothing at all".
        sink.begin_checkpoint_barrier()
            .await
            .expect("begin barrier");
        writer
            .update_checkpoint(checkpoint_at("00aa"))
            .await
            .expect("checkpoint joins the transaction");
        sink.commit_checkpoint_barrier()
            .await
            .expect("commit barrier");

        // Then one that is aborted, exactly as a crashed batch leaves it.
        sink.begin_checkpoint_barrier()
            .await
            .expect("begin second barrier");
        sink.send_encoded(
            bytes::Bytes::from_static(b"k1"),
            bytes::Bytes::from_static(b"doomed-row"),
        )
        .await
        .expect("send accepted");
        writer
            .update_checkpoint(checkpoint_at("00ff"))
            .await
            .expect("checkpoint joins the transaction");
        sink.abort_checkpoint_barrier()
            .await
            .expect("abort barrier");

        let loaded = load_records(&config).await.expect("readback scan");
        let record = loaded
            .checkpoint
            .expect("the committed checkpoint must still be found");
        assert_eq!(
            record.offset_hex, "00aa",
            "the readback must resume from the last committed position, not from the \
             aborted one that describes data nobody published"
        );
    }

    /// **The crash window, closed.** An aborted batch must leave *neither* the data nor
    /// the checkpoint that describes it visible.
    ///
    /// This is the case the old design could not handle. The checkpoint went through its
    /// own producer and its own transaction, so it committed independently of the batch: a
    /// crash after the sink's commit and before the checkpoint replayed the batch, and a
    /// crash the other way round advanced the position past data that was never published.
    /// Sharing one transaction removes both, and this asserts the first.
    #[tokio::test]
    async fn an_aborted_batch_publishes_neither_data_nor_checkpoint() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        let (mut sink, writer) = eos_pipeline(&broker, "eos.data.abort", "eos.state.abort").await;

        sink.begin_checkpoint_barrier()
            .await
            .expect("begin barrier");
        sink.send_encoded(
            bytes::Bytes::from_static(b"k1"),
            bytes::Bytes::from_static(b"doomed-row"),
        )
        .await
        .expect("send accepted");

        let joined = writer
            .update_checkpoint(checkpoint_at("00ff"))
            .await
            .expect("checkpoint written into the open transaction");
        assert!(
            joined,
            "the checkpoint must join the sink's transaction, not open its own — \
             otherwise it commits independently and the crash window is still there"
        );

        sink.abort_checkpoint_barrier()
            .await
            .expect("abort barrier");

        let brokers = broker.bootstrap_servers();
        assert!(
            committed_values(&brokers, "eos.data.abort")
                .await
                .is_empty(),
            "aborted data must not be visible to a read_committed consumer"
        );
        assert!(
            committed_values(&brokers, "eos.state.abort")
                .await
                .is_empty(),
            "the checkpoint must be discarded with the batch it describes; if it survives, \
             the pipeline resumes past data that was never published"
        );

        sink.close().await.expect("close");
    }

    /// The other half: a committed batch publishes both, atomically.
    #[tokio::test]
    async fn a_committed_batch_publishes_data_and_checkpoint_together() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        let (mut sink, writer) = eos_pipeline(&broker, "eos.data.commit", "eos.state.commit").await;

        sink.begin_checkpoint_barrier()
            .await
            .expect("begin barrier");
        sink.send_encoded(
            bytes::Bytes::from_static(b"k1"),
            bytes::Bytes::from_static(b"row-1"),
        )
        .await
        .expect("send accepted");
        writer
            .update_checkpoint(checkpoint_at("0100"))
            .await
            .expect("checkpoint written into the open transaction");

        sink.commit_checkpoint_barrier()
            .await
            .expect("commit barrier");

        let brokers = broker.bootstrap_servers();
        assert_eq!(
            committed_values(&brokers, "eos.data.commit").await,
            vec!["row-1".to_string()],
            "committed data must be visible"
        );
        let state = committed_values(&brokers, "eos.state.commit").await;
        assert_eq!(
            state.len(),
            1,
            "exactly one checkpoint record, got {state:?}"
        );
        assert!(
            state[0].contains("\"offset_hex\":\"0100\""),
            "the committed checkpoint must be the one written inside the transaction: {}",
            state[0]
        );

        sink.close().await.expect("close");
    }

    /// A state write outside any barrier still needs its own transaction, or it would sit
    /// uncommitted forever. Bootstrap seeding and startup restores take this path.
    #[tokio::test]
    async fn a_state_write_outside_a_barrier_commits_on_its_own() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        let (mut sink, writer) = eos_pipeline(&broker, "eos.data.solo", "eos.state.solo").await;

        // No `begin_checkpoint_barrier` — nothing is open.
        let joined = writer
            .update_checkpoint(checkpoint_at("0200"))
            .await
            .expect("checkpoint written");
        assert!(
            !joined,
            "with no barrier open the writer must open and commit its own transaction"
        );

        let state = committed_values(&broker.bootstrap_servers(), "eos.state.solo").await;
        assert_eq!(
            state.len(),
            1,
            "the record must be durable immediately, got {state:?}"
        );

        sink.close().await.expect("close");
    }

    /// **A second instance fences the first at the broker.**
    ///
    /// This state topic used to use a plain idempotent producer, so two instances — which
    /// a Kubernetes rolling update creates on every deploy — both wrote checkpoints, and
    /// last-write-wins could move the durable position *backwards*. A transactional id
    /// makes the broker enforce single-writer: `init_transactions()` bumps the producer
    /// epoch and permanently fences the previous holder.
    ///
    /// Asserting on the epoch is the real check. A test that only asserted "the second
    /// instance started" would pass just as well with no fencing at all.
    #[tokio::test]
    async fn a_second_state_writer_fences_the_first() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        broker.create_topic("cdc-state-fencing", 1);

        let mut config = sample_config();
        config.brokers = broker.bootstrap_servers();
        config.topic = "cdc-state-fencing".to_string();

        let _first = KafkaTopicStateWriter::new(&config)
            .await
            .expect("first state writer");
        let expected_id = format!("{}-state-{}", config.client_id, config.topic);
        let (_, epoch_before) = broker
            .transactional_producer(&expected_id)
            .expect("the state writer must register under the derived transactional id");

        let _second = KafkaTopicStateWriter::new(&config)
            .await
            .expect("second state writer");
        let (_, epoch_after) = broker
            .transactional_producer(&expected_id)
            .expect("producer still registered");

        assert!(
            epoch_after > epoch_before,
            "the second instance must bump the producer epoch, fencing the first \
             (before {epoch_before}, after {epoch_after})"
        );
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

    /// Full state round-trip against krafka's in-process fake broker: seed the
    /// bootstrap sentinel, publish a real checkpoint record (durability
    /// confirmation enforced by `publish_record`), then read everything back
    /// through the same compacted-scan path the runtime uses at startup.
    #[tokio::test]
    async fn fake_broker_state_roundtrip_seeds_and_reloads_checkpoint() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        broker.create_topic("cdc.fake.state", 1);

        let config = KafkaTopicStateConfig {
            brokers: broker.bootstrap_servers(),
            topic: "cdc.fake.state".to_string(),
            client_id: "fake-state-test".to_string(),
            request_timeout_ms: 2_000,
            readback_poll_timeout_ms: 500,
            min_replication_factor: 1,
            min_insync_replicas: 1,
            durability_profile: crate::config::schema::KafkaStateDurabilityProfile::Development,
            security: crate::config::schema::KafkaSecurityConfig::default(),
        };

        let writer = KafkaTopicStateWriter::new(&config).await.expect("writer");
        writer.seed_bootstrap().await.expect("seed bootstrap");

        let checkpoint = KafkaTopicCheckpointRecord {
            record_version: KAFKA_TOPIC_CHECKPOINT_VERSION,
            source_type: "postgres".to_string(),
            offset_hex: hex::encode(br#"{"lsn":281474976711680,"slot_name":"fake_slot"}"#),
            committed_event_count: 7,
            saved_at_unix_ms: now_unix_ms(),
        };
        writer
            .update_checkpoint(checkpoint)
            .await
            .expect("checkpoint publish");

        let loaded = load_records(&config).await.expect("readback scan");
        assert!(!loaded.legacy_format_detected);
        let record = loaded.checkpoint.expect("checkpoint record present");
        assert_eq!(record.source_type, "postgres");
        assert_eq!(record.committed_event_count, 7);
        assert!(!is_bootstrap_checkpoint_record(&record));
        assert_eq!(
            hex::decode(&record.offset_hex).expect("offset hex decodes"),
            br#"{"lsn":281474976711680,"slot_name":"fake_slot"}"#
        );
        assert!(
            loaded.schema_history.is_some(),
            "bootstrap schema history present"
        );
    }

    /// The state topic must never hold a position the guard would refuse.
    ///
    /// A stream position that rewinds while the committed-event count keeps climbing is
    /// refused. Under `at_least_once` the publish is not wrapped in a sink transaction
    /// that could retract it, so a check that ran *after* the publish would be reporting
    /// damage that is already the durable truth.
    ///
    /// Moving the `validate_checkpoint_progress` call in `KafkaTopicCheckpoint::save`
    /// below the `update_checkpoint` publish fails the final assertion.
    #[tokio::test]
    async fn a_refused_rewind_never_reaches_the_state_topic() {
        use rustcdc::checkpoint::PostgresOffset;

        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        broker.create_topic("cdc.fake.guard", 1);

        let config = KafkaTopicStateConfig {
            brokers: broker.bootstrap_servers(),
            topic: "cdc.fake.guard".to_string(),
            client_id: "fake-guard-test".to_string(),
            request_timeout_ms: 2_000,
            readback_poll_timeout_ms: 500,
            min_replication_factor: 1,
            min_insync_replicas: 1,
            durability_profile: crate::config::schema::KafkaStateDurabilityProfile::Development,
            security: KafkaSecurityConfig::default(),
        };

        let writer = Arc::new(KafkaTopicStateWriter::new(&config).await.expect("writer"));
        writer.seed_bootstrap().await.expect("seed bootstrap");

        let temp = tempfile::tempdir().expect("tempdir");
        let mut checkpoint =
            KafkaTopicCheckpoint::new(FileCheckpoint::new(temp.path()), Arc::clone(&writer), None);

        checkpoint
            .save(&PostgresOffset::new(0x16B_6A70, "slot"), 1)
            .await
            .expect("the first checkpoint must be accepted");

        // A zero LSN is not a position the stream can reach, so rustcdc reads it as a
        // decode defect rather than an out-of-order pgoutput commit.
        let refused = checkpoint
            .save(&PostgresOffset::new(0, "slot"), 2)
            .await
            .expect_err("a rewound stream position must be refused");
        assert!(
            refused.to_string().contains("backwards"),
            "the error must name the regression: {refused}"
        );

        let loaded = load_records(&config).await.expect("readback scan");
        let record = loaded.checkpoint.expect("checkpoint record present");
        assert_eq!(
            record.committed_event_count, 1,
            "the refused record must not have reached the state topic"
        );
    }

    /// The guard must survive a restart, because a restart is when it matters most.
    ///
    /// The comparison baseline lives in memory so the durability path needs no read of the
    /// state topic. That makes the *first* write of each process the one at risk: if the
    /// baseline started empty, a rewound position would sail through and become durable.
    /// And a restart is exactly when a repointed source or a recreated replication slot
    /// first shows up — the conditions that produce a rewind in the first place.
    ///
    /// Returning `None` instead of `Some(baseline)` from `restore_checkpoint_mirror`
    /// fails this test.
    ///
    /// The restart is simulated through `restore_checkpoint_mirror` rather than
    /// `build_checkpoint`, because the latter starts with a `DescribeConfigs` durability
    /// preflight the `FakeBroker` does not implement. Everything downstream of that
    /// preflight — the topic readback, the mirror rebuild, the baseline seeding, and the
    /// wiring into `KafkaTopicCheckpoint` — is the real code path.
    #[tokio::test]
    async fn the_guard_baseline_survives_a_restart() {
        use rustcdc::checkpoint::PostgresOffset;

        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        broker.create_topic("cdc.fake.restart", 1);

        let config = KafkaTopicStateConfig {
            brokers: broker.bootstrap_servers(),
            topic: "cdc.fake.restart".to_string(),
            client_id: "fake-restart-test".to_string(),
            request_timeout_ms: 2_000,
            readback_poll_timeout_ms: 500,
            min_replication_factor: 1,
            min_insync_replicas: 1,
            durability_profile: crate::config::schema::KafkaStateDurabilityProfile::Development,
            security: KafkaSecurityConfig::default(),
        };

        let writer = Arc::new(KafkaTopicStateWriter::new(&config).await.expect("writer"));
        writer.seed_bootstrap().await.expect("seed bootstrap");
        let temp = tempfile::tempdir().expect("tempdir");

        // ── First process: record a healthy position, then exit ───────────────
        {
            let (mirror, baseline) = restore_checkpoint_mirror(temp.path(), None)
                .await
                .expect("first mirror rebuild");
            assert!(baseline.is_none(), "nothing durable yet");
            let mut checkpoint = KafkaTopicCheckpoint::new(mirror, Arc::clone(&writer), baseline);
            checkpoint
                .save(&PostgresOffset::new(0x16B_6A70, "slot"), 1)
                .await
                .expect("the first checkpoint must be accepted");
        }

        // ── Second process: rebuild from the topic, rewind on the first write ─
        let loaded = load_records(&config).await.expect("restart readback");
        let (mirror, baseline) = restore_checkpoint_mirror(temp.path(), loaded.checkpoint)
            .await
            .expect("second mirror rebuild");
        let mut checkpoint = KafkaTopicCheckpoint::new(mirror, Arc::clone(&writer), baseline);

        let refused = checkpoint
            .save(&PostgresOffset::new(0, "slot"), 2)
            .await
            .expect_err("a rewound stream position must be refused after a restart");
        assert!(
            refused.to_string().contains("backwards"),
            "the error must name the regression: {refused}"
        );

        let loaded = load_records(&config).await.expect("readback scan");
        let record = loaded.checkpoint.expect("checkpoint record present");
        assert_eq!(
            record.committed_event_count, 1,
            "the refused record must not have reached the state topic"
        );
    }
}
