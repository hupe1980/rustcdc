//! OpenDAL-backed key-value offset/checkpoint backend (Redis, PostgreSQL).
//!
//! Checkpoint artifacts are stored as opaque JSON blobs under the well-known
//! key `checkpoint` inside the configured OpenDAL operator namespace.
//!
//! The adapter delegates to a local `FileCheckpoint` mirror so that the
//! standard file-based persistence remains the source of truth for crash
//! recovery, while the OpenDAL layer provides replication to a remote store.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use opendal::Operator;
use rustcdc::checkpoint::{Checkpoint, FileCheckpoint};
use rustcdc::core::{Error as RtError, Offset};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::config::schema::{PostgresStateConfig, RedisStateConfig};
use crate::error::AppError;

// ─────────────────────────────────────────────────────────────────────────────
// Key constants
// ─────────────────────────────────────────────────────────────────────────────

const KEY_CHECKPOINT: &str = "checkpoint";

// ─────────────────────────────────────────────────────────────────────────────
// Serializable checkpoint record
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteCheckpointRecord {
    source_type: String,
    /// Offset encoded via `Offset::encode()`.
    offset_bytes: Vec<u8>,
    committed_event_count: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// OpenDAL checkpoint adapter
// ─────────────────────────────────────────────────────────────────────────────

/// Checkpoint implementation backed by an OpenDAL operator.
///
/// Writes are mirrored to a local `FileCheckpoint` and then replicated to the
/// remote OpenDAL store.  Reads return the latest committed state from the
/// in-memory mirror so no round-trip to the remote is needed on the hot path.
pub(crate) struct OpenDalCheckpoint {
    op: Arc<Mutex<Operator>>,
    mirror: FileCheckpoint,
}

impl OpenDalCheckpoint {
    fn new(op: Arc<Mutex<Operator>>, mirror: FileCheckpoint) -> Self {
        Self { op, mirror }
    }
}

#[async_trait]
impl Checkpoint for OpenDalCheckpoint {
    async fn save(
        &mut self,
        offset: &dyn Offset,
        committed_event_count: u64,
    ) -> Result<(), RtError> {
        // ── 1. Write to the remote OpenDAL store FIRST ────────────────────────
        //
        // Crash-safety contract: the remote store is always authoritative.
        // On restart, `build_checkpoint` restores the local mirror from the
        // remote, so the remote must never be behind the mirror.  Writing
        // mirror-first (the old order) could leave the local mirror ahead of the
        // remote after a crash between the two writes; the next startup would
        // then silently load stale remote state and regress the offset.
        let record = RemoteCheckpointRecord {
            source_type: offset.source_type().to_string(),
            offset_bytes: offset.encode()?,
            committed_event_count,
        };
        let bytes = serde_json::to_vec(&record).map_err(|e| {
            RtError::SerializationError(format!("checkpoint remote serialize: {e}"))
        })?;

        {
            let op = self.op.lock().await;
            op.write(KEY_CHECKPOINT, bytes).await.map_err(|e| {
                crate::state::metrics::mark_opendal_write_failure();
                RtError::StateError(format!("OpenDAL checkpoint write: {e}"))
            })?;
        }

        // ── 2. Mirror to local filesystem AFTER remote write confirms ─────────
        //
        // The local mirror is the hot-path read cache used by `load()`.
        // If this write fails, the remote already has the latest state and will
        // be restored at next startup, so we surface the error to retry the
        // batch (ensuring eventual consistency) without losing data.
        self.mirror.save(offset, committed_event_count).await?;

        Ok(())
    }

    async fn load(&self) -> Result<Option<Box<dyn Offset>>, RtError> {
        // Delegate to the local mirror — remote was restored on startup.
        self.mirror.load().await
    }

    async fn get_committed_count(&self) -> Result<u64, RtError> {
        self.mirror.get_committed_count().await
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Build helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Build an OpenDAL-backed state backend from a pre-configured operator.
///
/// If existing state is present in the remote store it is restored to the
/// local mirror directory before constructing the runtime state handle.
pub(crate) async fn build_checkpoint(
    op: Operator,
    mirror_dir: &Path,
) -> Result<OpenDalCheckpoint, AppError> {
    std::fs::create_dir_all(mirror_dir).map_err(AppError::from)?;

    let shared_op = Arc::new(Mutex::new(op));

    // ── Restore checkpoint mirror from remote ──────────────────────────────
    let checkpoint_dir = mirror_dir.join("checkpoint");
    std::fs::create_dir_all(&checkpoint_dir).map_err(AppError::from)?;

    {
        let op = shared_op.lock().await;
        match op.read(KEY_CHECKPOINT).await {
            Ok(buf) => {
                // Parse to extract source_type so we can seed the right file name.
                let record: RemoteCheckpointRecord =
                    serde_json::from_slice(buf.to_bytes().as_ref()).map_err(|e| {
                        AppError::Other(format!("remote checkpoint JSON invalid: {e}"))
                    })?;
                // Seed the local mirror through the official restore path.
                //
                // `offset_bytes` came from `Offset::encode()` at save time, which is
                // exactly what `restore_from_record` expects — the seeded file is
                // byte-compatible with one written by `Checkpoint::save`, carries the
                // mandatory `content_checksum`, and is written 0600 + fsynced.
                // Hand-writing the JSON here (the pre-0.7.0 approach) produced a file
                // that failed both the integrity check and offset decoding on load.
                FileCheckpoint::restore_from_record(
                    &checkpoint_dir,
                    &record.source_type,
                    record.offset_bytes,
                    record.committed_event_count,
                )
                .map_err(|e| {
                    AppError::Other(format!("restore checkpoint mirror from remote: {e}"))
                })?;
            }
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                // No remote checkpoint yet — first run.
            }
            Err(e) => {
                return Err(AppError::Other(format!("OpenDAL checkpoint read: {e}")));
            }
        }
    }

    let file_checkpoint = FileCheckpoint::new(&checkpoint_dir);

    Ok(OpenDalCheckpoint::new(shared_op, file_checkpoint))
}

/// Build an OpenDAL [`Operator`] for a Redis endpoint.
pub(crate) fn build_redis_operator(config: &RedisStateConfig) -> Result<Operator, AppError> {
    let url = config
        .url
        .resolve()
        .map_err(|e| AppError::Other(format!("failed to resolve state.backend.redis.url: {e}")))?;

    let mut builder = opendal::services::Redis::default()
        .endpoint(&url)
        .db(config.db)
        .root(&config.key_root);

    if let Some(username) = &config.username {
        builder = builder.username(username);
    }
    if let Some(password) = &config.password {
        let resolved = password.resolve().map_err(|e| {
            AppError::Other(format!(
                "failed to resolve state.backend.redis.password: {e}"
            ))
        })?;
        builder = builder.password(&resolved);
    }

    Ok(Operator::new(builder)
        .map_err(|e| AppError::Other(format!("failed to build OpenDAL Redis operator: {e}")))?
        .finish())
}

/// Build an OpenDAL [`Operator`] for a PostgreSQL endpoint.
pub(crate) fn build_postgresql_operator(
    config: &PostgresStateConfig,
) -> Result<Operator, AppError> {
    let url = config.url.resolve().map_err(|e| {
        AppError::Other(format!(
            "state.backend.postgresql.url could not be resolved: {e}"
        ))
    })?;
    let builder = opendal::services::Postgresql::default()
        .connection_string(&url)
        .table(&config.checkpoint_table)
        .root("/");

    Ok(Operator::new(builder)
        .map_err(|e| AppError::Other(format!("failed to build OpenDAL PostgreSQL operator: {e}")))?
        .finish())
}
