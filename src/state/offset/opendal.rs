//! OpenDAL-backed key-value offset/checkpoint backend (Redis, PostgreSQL).
//!
//! Checkpoint artifacts are stored as opaque JSON blobs under the well-known
//! key `checkpoint` inside the configured OpenDAL operator namespace.
//!
//! The adapter delegates to a local `FileCheckpoint` mirror so that the
//! standard file-based persistence remains the source of truth for crash
//! recovery, while the OpenDAL layer provides replication to a remote store.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use opendal::Operator;
use rustcdc::checkpoint::{Checkpoint, FileCheckpoint};
use rustcdc::core::{Error as RtError, Offset};
use serde::{Deserialize, Serialize};

use crate::config::schema::{PostgresStateConfig, RedisStateConfig};
use crate::error::AppError;
use crate::state::remote_lease::{self, LeaseRecord, LEASE_HEARTBEAT, LEASE_KEY, LEASE_TTL};

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
    /// Held directly rather than behind a mutex.
    ///
    /// `Operator` is `Clone + Send + Sync` and does its own connection pooling, so the
    /// `Arc<Mutex<_>>` this used to carry only serialised the durability path — and it
    /// would have serialised the lease heartbeat against it too.
    op: Operator,
    mirror: FileCheckpoint,
    /// This process's lease identity, re-verified before every checkpoint write.
    owner: Arc<OwnedLease>,
}

/// A lease this process believes it holds on the remote store.
pub(crate) struct OwnedLease {
    op: Operator,
    owner: String,
    epoch: u64,
    backend: String,
    /// Unix milliseconds of the last renewal we performed.
    last_renewed_ms: AtomicU64,
}

impl OwnedLease {
    /// Renew the lease, failing if another owner has taken it.
    ///
    /// Losing the lease is terminal by design. An instance that has been fenced out must
    /// stop writing rather than keep going and interleave checkpoints with its
    /// successor — that is the failure this whole mechanism exists to prevent, and
    /// discovering it and continuing anyway would be worse than never checking.
    pub(crate) async fn renew(&self) -> Result<(), AppError> {
        let now_ms = remote_lease::now_unix_ms();

        if let Some(existing) = read_lease(&self.op).await? {
            if existing.owner != self.owner && existing.is_live(now_ms, LEASE_TTL) {
                return Err(AppError::Other(format!(
                    "{} state lease was taken by '{}' while this process held it                      (ours: '{}', epoch {}). This instance has been fenced out and will                      stop rather than write checkpoints alongside the new owner.",
                    self.backend, existing.owner, self.owner, self.epoch,
                )));
            }
        }

        write_lease(
            &self.op,
            &LeaseRecord {
                owner: self.owner.clone(),
                renewed_at_ms: now_ms,
                epoch: self.epoch,
            },
        )
        .await?;
        self.last_renewed_ms.store(now_ms, Ordering::Relaxed);
        Ok(())
    }

    /// Renew only if a heartbeat interval has elapsed.
    ///
    /// Checkpoint writes happen once per batch, which under load is far more often than
    /// the lease needs refreshing. Renewing on every write would put an extra remote
    /// round-trip on the durability path for no benefit.
    async fn renew_if_due(&self) -> Result<(), AppError> {
        let now_ms = remote_lease::now_unix_ms();
        let last = self.last_renewed_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) < LEASE_HEARTBEAT.as_millis() as u64 {
            return Ok(());
        }
        self.renew().await
    }

    /// Release the lease so a successor need not wait out the TTL.
    pub(crate) async fn release(&self) {
        if let Err(error) = self.op.delete(LEASE_KEY).await {
            // Best-effort: the TTL is the correctness mechanism, this is only courtesy.
            tracing::warn!(
                error = %error,
                backend = %self.backend,
                "could not release the remote state lease; a successor will wait for it                  to expire"
            );
        }
    }
}

async fn read_lease(op: &Operator) -> Result<Option<LeaseRecord>, AppError> {
    match op.read(LEASE_KEY).await {
        Ok(buf) => match serde_json::from_slice::<LeaseRecord>(buf.to_bytes().as_ref()) {
            Ok(record) => Ok(Some(record)),
            // An unparseable lease is treated as absent rather than as a hard failure:
            // a format change must not brick every deployment. The write below replaces
            // it, and the window this opens is no worse than the no-lease status quo.
            Err(error) => {
                tracing::warn!(error = %error, "remote state lease is unreadable; replacing it");
                Ok(None)
            }
        },
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(AppError::Other(format!("remote state lease read: {e}"))),
    }
}

async fn write_lease(op: &Operator, record: &LeaseRecord) -> Result<(), AppError> {
    let bytes = serde_json::to_vec(record)
        .map_err(|e| AppError::Other(format!("remote state lease serialize: {e}")))?;
    op.write(LEASE_KEY, bytes)
        .await
        .map_err(|e| AppError::Other(format!("remote state lease write: {e}")))?;
    Ok(())
}

/// Take ownership of the remote state, refusing if a live owner already holds it.
pub(crate) async fn acquire_lease(
    op: &Operator,
    backend: &str,
) -> Result<Arc<OwnedLease>, AppError> {
    let now_ms = remote_lease::now_unix_ms();
    let existing = read_lease(op).await?;

    let epoch = match &existing {
        Some(record) if record.is_live(now_ms, LEASE_TTL) => {
            return Err(remote_lease::conflict_error(backend, record, now_ms));
        }
        Some(record) => {
            tracing::warn!(
                previous_owner = %record.owner,
                previous_epoch = record.epoch,
                "taking over an expired remote state lease"
            );
            record.epoch.saturating_add(1)
        }
        None => 1,
    };

    let owner = remote_lease::owner_id();
    write_lease(
        op,
        &LeaseRecord {
            owner: owner.clone(),
            renewed_at_ms: now_ms,
            epoch,
        },
    )
    .await?;

    tracing::info!(owner = %owner, epoch, backend, "acquired remote state lease");

    Ok(Arc::new(OwnedLease {
        op: op.clone(),
        owner,
        epoch,
        backend: backend.to_string(),
        last_renewed_ms: AtomicU64::new(now_ms),
    }))
}

impl OpenDalCheckpoint {
    fn new(op: Operator, mirror: FileCheckpoint, owner: Arc<OwnedLease>) -> Self {
        Self { op, mirror, owner }
    }

    /// The lease this checkpoint holds, so shutdown can release it.
    ///
    /// Releasing on a clean exit is what makes `strategy: Recreate` fast: without it a
    /// successor waits out the full TTL before it may start, turning every deploy into
    /// a minute of stalled capture for no reason.
    pub(crate) fn lease(&self) -> Arc<OwnedLease> {
        Arc::clone(&self.owner)
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

        // Confirm we still own the state before making it durable. An instance that
        // has been fenced out must not keep writing: interleaved checkpoints from two
        // owners can move the durable position *backwards*, which replays silently on
        // the next restart.
        self.owner
            .renew_if_due()
            .await
            .map_err(|e| RtError::StateError(e.to_string()))?;

        self.op.write(KEY_CHECKPOINT, bytes).await.map_err(|e| {
            crate::state::metrics::mark_opendal_write_failure();
            RtError::StateError(format!("OpenDAL checkpoint write: {e}"))
        })?;

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
    backend_label: &str,
) -> Result<OpenDalCheckpoint, AppError> {
    std::fs::create_dir_all(mirror_dir).map_err(AppError::from)?;

    // Claim the remote state before reading it. Acquiring first means a losing
    // instance never even restores a mirror it has no right to write from.
    let owner = acquire_lease(&op, backend_label).await?;

    // ── Restore checkpoint mirror from remote ──────────────────────────────
    let checkpoint_dir = mirror_dir.join("checkpoint");
    std::fs::create_dir_all(&checkpoint_dir).map_err(AppError::from)?;

    {
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

    Ok(OpenDalCheckpoint::new(op, file_checkpoint, owner))
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
