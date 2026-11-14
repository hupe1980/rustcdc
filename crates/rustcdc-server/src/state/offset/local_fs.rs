use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use async_trait::async_trait;
use rustcdc::checkpoint::{Checkpoint, FileCheckpoint};
use rustcdc::core::{Error as RtError, Offset};

use crate::error::AppError;

use crate::state::CheckpointAgeSource;
use crate::state::file_lock::{self, LockDomain, TryLock};
use crate::state::remote_lease::{self, LEASE_HEARTBEAT, LEASE_TTL, LeaseRecord};

/// The owner lease file, beside the checkpoint it protects.
const OWNER_LEASE_FILE: &str = "owner_lease.json";

/// The file the owner holds an advisory lock on for as long as it runs.
const OWNER_LOCK_FILE: &str = "owner_lease.lock";

/// A `local_fs` state directory this process believes it owns.
///
/// # Why a filesystem backend needs a lease at all
///
/// Every other backend is fenced: `redis` and `postgresql` take an owner+epoch lease
/// through OpenDAL, and `kafka_topic` is fenced at the broker by its transactional id.
/// `local_fs` — the **default** backend, and the one used outside Kubernetes — once had
/// only a PID file. A PID from another host is indistinguishable from a stale one, so such
/// a lock is simply overwritten. Two `rustcdc run` invocations
/// pointed at the same NFS or EFS state directory from different hosts therefore both
/// proceeded, interleaving checkpoints on a last-write-wins basis — and if the lagging one
/// wrote last, the durable position moved *backwards*, which is a silent replay on the
/// next restart.
///
/// Correctness was resting entirely on the operator honouring `accessModes:
/// [ReadWriteOnce]` and `strategy: Recreate`. Those are the right things to document and
/// the wrong things to depend on: RWO is a *node*-level guarantee, so two pods on one node
/// can both mount the volume, and outside Kubernetes nothing enforces anything.
///
/// # Two signals: a lock and a lease
///
/// The owner holds an advisory lock on [`OWNER_LOCK_FILE`] for as long as it runs. The
/// kernel releases it when the owner exits, however it exits, so on a local filesystem the
/// lock answers "is the owner alive" outright: a second instance that cannot take it is
/// refused, and one that can, finding a live lease whose owner held the lock **in the same
/// [`LockDomain`]** (this kernel, this filesystem device), is looking at a crash and takes
/// over at once. That holds whatever the PIDs are — in a container the crashed owner and
/// its successor are both PID 1 — and needs no external program.
///
/// The lease covers what the lock cannot see: an owner on another kernel or another mount,
/// which on a shared or network path may hold a lock this one is never told about, and an
/// owner that could not lock at all. There the lease is
/// authoritative, with the same contract as [`crate::state::remote_lease`]: one record
/// type, one TTL, one heartbeat, one conflict message. A crashed owner's lease expires by
/// itself after `LEASE_TTL`; the owner renews it on a timer so an idle pipeline keeps it;
/// an owner whose lease is stolen discovers it on the next renewal and stops writing rather
/// than continuing blind.
///
/// # What this does not guarantee
///
/// The lease is **not** a consensus lease. Acquisition is read-then-write, not compare-and-swap,
/// so two instances starting inside the same read-write window can both see a free slot.
/// On a filesystem that does not honour ordinary write visibility across hosts it
/// guarantees nothing at all. It converts the common silent interleave into a loud
/// refusal; it is not a substitute for a store with real CAS.
pub(crate) struct OwnedStateDir {
    path: PathBuf,
    owner: String,
    epoch: u64,
    /// Where our lock is visible; `None` without a lock, so our lease never claims one.
    domain: Option<LockDomain>,
    last_renewed_ms: AtomicU64,
    /// The advisory lock, held until [`release`](Self::release) or drop. `None` when the
    /// filesystem refused to lock; the lease alone decides then.
    lock: std::sync::Mutex<Option<std::fs::File>>,
}

impl OwnedStateDir {
    /// Re-assert ownership, failing if another live owner has taken it.
    ///
    /// Losing the lease is terminal by design: an instance that has been fenced out must
    /// stop writing rather than interleave checkpoints with its successor, which is the
    /// failure the lease exists to prevent.
    fn renew(&self) -> Result<(), AppError> {
        let now_ms = remote_lease::now_unix_ms();

        if let Some(existing) = read_lease(&self.path)?
            && existing.owner != self.owner
            && existing.is_live(now_ms, LEASE_TTL)
        {
            return Err(AppError::Other(format!(
                "local_fs state lease was taken by '{}' while this process held it \
                     (ours: '{}', epoch {}). This instance has been fenced out and will \
                     stop rather than write checkpoints alongside the new owner.",
                existing.owner, self.owner, self.epoch,
            )));
        }

        write_lease(
            &self.path,
            &LeaseRecord {
                owner: self.owner.clone(),
                renewed_at_ms: now_ms,
                epoch: self.epoch,
                boot_id: self.domain.as_ref().map(|d| d.boot_id.clone()),
                lock_dev: self.domain.as_ref().map(|d| d.dev),
            },
        )?;
        self.last_renewed_ms.store(now_ms, Ordering::Relaxed);
        Ok(())
    }

    /// Renew only once a heartbeat interval has elapsed.
    ///
    /// Checkpoints are written once per batch, far more often than the lease needs
    /// refreshing; renewing on every write would put an fsync on the durability path for
    /// no benefit.
    fn renew_if_due(&self) -> Result<(), AppError> {
        let now_ms = remote_lease::now_unix_ms();
        let last = self.last_renewed_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) < LEASE_HEARTBEAT.as_millis() as u64 {
            return Ok(());
        }
        self.renew()
    }

    /// Release the lock and the lease, so a successor need not wait out the TTL.
    pub(crate) fn release(&self) {
        if let Ok(mut lock) = self.lock.lock() {
            lock.take();
        }
        // The lease only if we still hold it: a fenced-out instance deleting the file would
        // hand the directory to a third process while the real owner is still writing.
        match read_lease(&self.path) {
            Ok(Some(existing)) if existing.owner != self.owner => return,
            Err(_) => return,
            _ => {}
        }
        if let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                error = %error,
                path = %self.path.display(),
                "could not release the local_fs state lease; a successor will wait for \
                 it to expire"
            );
        }
    }
}

/// Release on the way out, so a successor need not wait out the TTL.
///
/// The `Arc` is held by both the [`StateLease`](crate::state::StateLease) handle and the
/// [`LeasedFileCheckpoint`], so this fires only once *both* are gone — i.e. when nothing
/// can write through this lease any more. A crash skips `Drop` entirely and the TTL takes
/// over, which is the correct split: a clean exit releases immediately, an unclean one
/// falls back to the timeout.
impl Drop for OwnedStateDir {
    fn drop(&mut self) {
        self.release();
    }
}

fn read_lease(path: &Path) -> Result<Option<LeaseRecord>, AppError> {
    match std::fs::read(path) {
        Ok(bytes) => match serde_json::from_slice::<LeaseRecord>(&bytes) {
            Ok(record) => Ok(Some(record)),
            // An unparseable lease is treated as absent rather than fatal: a format
            // change must not brick every deployment, and the write below replaces it.
            // The window this opens is no worse than the no-lease status quo.
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    path = %path.display(),
                    "local_fs state lease is unreadable; replacing it"
                );
                Ok(None)
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(AppError::Other(format!(
            "local_fs state lease read ({}): {error}",
            path.display()
        ))),
    }
}

/// Write the lease durably: temp file, fsync, rename.
///
/// A torn lease record reads as unparseable, which `read_lease` treats as absent — i.e. as
/// a free directory. Writing in place would make a crash mid-write hand the state to the
/// next starter.
fn write_lease(path: &Path, record: &LeaseRecord) -> Result<(), AppError> {
    use std::io::Write as _;

    let bytes = serde_json::to_vec(record)
        .map_err(|e| AppError::Other(format!("local_fs state lease serialize: {e}")))?;
    let temp = path.with_extension("json.tmp");
    {
        let mut file = std::fs::File::create(&temp).map_err(AppError::from)?;
        file.write_all(&bytes).map_err(AppError::from)?;
        file.sync_all().map_err(AppError::from)?;
    }
    std::fs::rename(&temp, path).map_err(AppError::from)?;
    Ok(())
}

/// Take ownership of a `local_fs` state directory, refusing if a live owner holds it.
pub(crate) fn acquire_state_dir(state_dir: &Path) -> Result<Arc<OwnedStateDir>, AppError> {
    acquire_with(state_dir, file_lock::try_lock)
}

/// [`acquire_state_dir`] with the lock attempt supplied, so a test can stand in a
/// filesystem that refuses locks.
fn acquire_with(
    state_dir: &Path,
    try_lock: impl FnOnce(&Path) -> Result<TryLock, AppError>,
) -> Result<Arc<OwnedStateDir>, AppError> {
    let path = state_dir.join(OWNER_LEASE_FILE);
    let lock_path = state_dir.join(OWNER_LOCK_FILE);
    let now_ms = remote_lease::now_unix_ms();

    let lock = match try_lock(&lock_path)? {
        TryLock::Held(file) => Some(file),
        // A live owner holds the lock. Refused whatever its lease says: an idle owner whose
        // lease looks expired is still running.
        TryLock::Busy => {
            return Err(match read_lease(&path)? {
                Some(record) => remote_lease::conflict_error("local_fs", &record, now_ms),
                None => AppError::Other(format!(
                    "local_fs state directory '{}' is already owned by another running \
                     process, which holds '{}'. Two instances writing one pipeline's state \
                     interleave checkpoints, so this process will not start. Under \
                     Kubernetes, set `strategy: {{ type: Recreate }}` on the Deployment.",
                    state_dir.display(),
                    lock_path.display(),
                )),
            });
        }
        TryLock::Unsupported(error) => {
            tracing::warn!(
                error = %error,
                path = %lock_path.display(),
                "the filesystem refused an advisory lock on the local_fs state directory; \
                 the owner lease alone decides ownership, so a crashed owner blocks a \
                 restart until its lease expires"
            );
            None
        }
    };

    let domain = lock.as_ref().and_then(file_lock::lock_domain);
    let existing = read_lease(&path)?;
    let epoch = match &existing {
        // We hold the lock, and the lease's owner held it in the same domain: it would
        // still hold it if it were running, so it has exited — a crash, not a conflict.
        // Waiting out the TTL here would refuse a restart by systemd or a container runtime
        // for a full minute, for a directory nobody holds.
        Some(record)
            if record.is_live(now_ms, LEASE_TTL)
                && domain.as_ref().is_some_and(|ours| {
                    record.boot_id.as_deref() == Some(ours.boot_id.as_str())
                        && record.lock_dev == Some(ours.dev)
                }) =>
        {
            tracing::warn!(
                previous_owner = %record.owner,
                previous_epoch = record.epoch,
                "taking over a local_fs state lease from an owner on this kernel that no \
                 longer holds its lock"
            );
            record.epoch.saturating_add(1)
        }
        // An owner in another domain, one that held no lock, or one we cannot place: its
        // lock, if any, may be invisible here, so its lease is authoritative until it
        // expires.
        Some(record) if record.is_live(now_ms, LEASE_TTL) => {
            return Err(remote_lease::conflict_error("local_fs", record, now_ms));
        }
        Some(record) => {
            tracing::warn!(
                previous_owner = %record.owner,
                previous_epoch = record.epoch,
                "taking over an expired local_fs state lease"
            );
            record.epoch.saturating_add(1)
        }
        None => 1,
    };

    let owner = remote_lease::owner_id();
    write_lease(
        &path,
        &LeaseRecord {
            owner: owner.clone(),
            renewed_at_ms: now_ms,
            epoch,
            boot_id: domain.as_ref().map(|d| d.boot_id.clone()),
            lock_dev: domain.as_ref().map(|d| d.dev),
        },
    )?;

    tracing::info!(owner = %owner, epoch, "acquired local_fs state lease");

    Ok(Arc::new(OwnedStateDir {
        path,
        owner,
        epoch,
        domain,
        last_renewed_ms: AtomicU64::new(now_ms),
        lock: std::sync::Mutex::new(lock),
    }))
}

/// Renew the lease every `every` while the owner lives, so an idle pipeline — one writing
/// no checkpoints — keeps it.
///
/// Stops when the owner is dropped, or when a renewal finds the lease taken: the write
/// path then refuses too, so the instance stops rather than writing alongside its successor.
pub(crate) fn spawn_lease_renewal(
    owner: &Arc<OwnedStateDir>,
    every: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    let owner = Arc::downgrade(owner);
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(every);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            let Some(owner) = owner.upgrade() else {
                return;
            };
            if let Err(error) = owner.renew_if_due() {
                tracing::error!(error = %error, "local_fs state lease renewal failed");
                return;
            }
        }
    })
}

/// `FileCheckpoint` that re-asserts its lease before every durable write.
///
/// The lease is only worth having if losing it stops the writes. Checking at startup alone
/// would catch the rolling-update case and miss the one that matters more: an instance
/// partitioned long enough for its lease to expire, whose successor has already taken over,
/// happily continuing to write.
pub(crate) struct LeasedFileCheckpoint {
    inner: FileCheckpoint,
    owner: Arc<OwnedStateDir>,
}

#[async_trait]
impl Checkpoint for LeasedFileCheckpoint {
    async fn save(
        &mut self,
        offset: &dyn Offset,
        committed_event_count: u64,
    ) -> Result<(), RtError> {
        self.owner
            .renew_if_due()
            .map_err(|e| RtError::StateError(e.to_string()))?;
        self.inner.save(offset, committed_event_count).await
    }

    async fn load(&self) -> Result<Option<Box<dyn Offset>>, RtError> {
        self.inner.load().await
    }

    async fn get_committed_count(&self) -> Result<u64, RtError> {
        self.inner.get_committed_count().await
    }
}

pub(super) async fn build_checkpoint(
    state_dir: &Path,
) -> Result<
    (
        LeasedFileCheckpoint,
        CheckpointAgeSource,
        Arc<OwnedStateDir>,
    ),
    AppError,
> {
    std::fs::create_dir_all(state_dir)?;

    // Before opening anything. A losing instance must not even read state it has no right
    // to write from, and must certainly not create directories in it.
    let owner = acquire_state_dir(state_dir)?;
    spawn_lease_renewal(&owner, LEASE_HEARTBEAT);

    let checkpoint_dir = state_dir.join("checkpoint");
    std::fs::create_dir_all(&checkpoint_dir)?;

    let checkpoint = LeasedFileCheckpoint {
        inner: FileCheckpoint::new(checkpoint_dir.clone()),
        owner: Arc::clone(&owner),
    };
    let schema_history_path = state_dir.join("schema_history");

    let age_source = CheckpointAgeSource::LocalFs {
        checkpoint_dir,
        schema_history_path,
    };

    Ok((checkpoint, age_source, owner))
}

/// Fallback age source for OpenDAL backends: reports LocalFs age against the
/// same directory (will typically return `None` since no files are written).
pub(super) fn fallback_age_source(state_dir: &Path) -> CheckpointAgeSource {
    let checkpoint_dir = state_dir.join("checkpoint");
    let schema_history_path = state_dir.join("schema_history");
    CheckpointAgeSource::LocalFs {
        checkpoint_dir,
        schema_history_path,
    }
}

/// Returns the age in seconds of the most recently modified checkpoint or
/// schema-history artifact in the local filesystem state directory.
pub(crate) fn age_seconds(checkpoint_dir: &Path, schema_history_path: &Path) -> Option<f64> {
    let newest = newest_artifact_modified_at(checkpoint_dir, schema_history_path)?;
    let age = SystemTime::now().duration_since(newest).ok()?;
    Some(age.as_secs_f64())
}

fn newest_artifact_modified_at(
    checkpoint_dir: &Path,
    schema_history_path: &Path,
) -> Option<SystemTime> {
    let mut newest = SystemTime::UNIX_EPOCH;

    if let Ok(entries) = std::fs::read_dir(checkpoint_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !name.starts_with("checkpoint_") || !name.ends_with(".json") {
                continue;
            }
            let modified = entry
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            if modified > newest {
                newest = modified;
            }
        }
    }

    if let Ok(metadata) = std::fs::metadata(schema_history_path)
        && let Ok(modified) = metadata.modified()
        && modified > newest
    {
        newest = modified;
    }

    if newest == SystemTime::UNIX_EPOCH {
        None
    } else {
        Some(newest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn age_seconds_includes_schema_history_when_checkpoint_dir_is_empty() {
        let temp = tempdir().expect("tempdir");
        let checkpoint_dir = temp.path().join("checkpoint");
        std::fs::create_dir_all(&checkpoint_dir).expect("checkpoint dir");
        let schema_history_path = temp.path().join("schema_history");
        std::fs::write(&schema_history_path, b"[]").expect("schema history file");

        let age = age_seconds(&checkpoint_dir, &schema_history_path);
        assert!(age.is_some(), "schema history must contribute to freshness");
    }
}

#[cfg(test)]
mod lease_tests {

    /// This host's name, as the owner id records it.
    ///
    /// The tests that seed a lease "from this host" need the name the production code
    /// writes. On a machine where it cannot be resolved they skip.
    fn this_host() -> Option<String> {
        remote_lease::owner_host()
    }
    use super::*;
    use rustcdc::checkpoint::PostgresOffset;

    /// A second instance must refuse to start while a live lease is held.
    ///
    /// This is the whole point. `local_fs` is the default backend and the one used outside
    /// Kubernetes, and until it had a lease, two `rustcdc run` invocations against the same
    /// NFS state directory from different hosts both proceeded — `StateDirLock`'s PID
    /// checks are host-local, so a PID from another host reads as stale and the lock is
    /// overwritten. They then interleaved checkpoints, and if the lagging one wrote last
    /// the durable position moved backwards.
    #[tokio::test]
    async fn a_second_instance_refuses_a_live_state_dir() {
        let dir = tempfile::tempdir().expect("tempdir");

        let (_first, _age, owner) = build_checkpoint(dir.path()).await.expect("first build");

        let error = build_checkpoint(dir.path())
            .await
            .err()
            .expect("a second instance must refuse a live lease");
        let rendered = error.to_string();
        assert!(
            rendered.contains("already owned by"),
            "the refusal must name the holder: {rendered}"
        );
        assert!(
            rendered.contains("Recreate"),
            "the refusal must name the Kubernetes setting that causes it: {rendered}"
        );

        // Released, a successor starts immediately rather than waiting out the TTL.
        owner.release();
        build_checkpoint(dir.path())
            .await
            .expect("a released directory must be takeable");
    }

    /// An expired lease is taken over, and the epoch advances.
    #[tokio::test]
    async fn an_expired_lease_is_taken_over_with_a_new_epoch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(OWNER_LEASE_FILE);

        // A lease last renewed longer ago than the TTL: a crashed owner.
        let stale = remote_lease::now_unix_ms() - (LEASE_TTL.as_millis() as u64) - 1_000;
        write_lease(
            &path,
            &LeaseRecord {
                owner: "other-host:1:deadbeef".to_string(),
                renewed_at_ms: stale,
                epoch: 7,
                boot_id: None,
                lock_dev: None,
            },
        )
        .expect("seed a stale lease");

        let owner = acquire_state_dir(dir.path()).expect("an expired lease must be takeable");
        assert_eq!(
            owner.epoch, 8,
            "taking over must advance the epoch, so a returning owner is distinguishable"
        );
    }

    /// A fenced-out instance must stop writing checkpoints.
    ///
    /// Checking only at startup catches the rolling-update case and misses the worse one:
    /// an instance partitioned long enough for its lease to expire, whose successor has
    /// already taken over, continuing to write into state it no longer owns.
    ///
    /// Deleting the `renew_if_due` call in `LeasedFileCheckpoint::save` fails this test.
    #[tokio::test]
    async fn a_fenced_instance_stops_writing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut checkpoint, _age, owner) = build_checkpoint(dir.path()).await.expect("build");

        checkpoint
            .save(&PostgresOffset::new(100, "slot"), 1)
            .await
            .expect("the owner's write must be accepted");

        // Somebody else takes the directory, and our heartbeat is now overdue.
        write_lease(
            &owner.path,
            &LeaseRecord {
                owner: "successor:2:feedface".to_string(),
                renewed_at_ms: remote_lease::now_unix_ms(),
                epoch: owner.epoch + 1,
                boot_id: None,
                lock_dev: None,
            },
        )
        .expect("successor takes the lease");
        owner.last_renewed_ms.store(
            remote_lease::now_unix_ms() - (LEASE_HEARTBEAT.as_millis() as u64) - 1,
            Ordering::Relaxed,
        );

        let refused = checkpoint
            .save(&PostgresOffset::new(200, "slot"), 2)
            .await
            .expect_err("a fenced instance must refuse to write");
        assert!(
            refused.to_string().contains("fenced out"),
            "the error must say the instance was fenced: {refused}"
        );
    }

    /// A fenced instance must not release the lease it no longer holds.
    ///
    /// Otherwise shutting down a fenced-out process hands the directory to a *third*
    /// starter while the real owner is still writing — the lease deleting itself is worse
    /// than never having taken it.
    #[tokio::test]
    async fn a_fenced_instance_does_not_release_the_successors_lease() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_checkpoint, _age, owner) = build_checkpoint(dir.path()).await.expect("build");

        write_lease(
            &owner.path,
            &LeaseRecord {
                owner: "successor:2:feedface".to_string(),
                renewed_at_ms: remote_lease::now_unix_ms(),
                epoch: 99,
                boot_id: None,
                lock_dev: None,
            },
        )
        .expect("successor takes the lease");

        owner.release();

        let held = read_lease(&owner.path)
            .expect("lease readable")
            .expect("the successor's lease must survive our release");
        assert_eq!(held.owner, "successor:2:feedface");
    }

    /// A crashed process on this host must not block its own restart for the TTL.
    ///
    /// Without this, a `local_fs` pipeline killed and restarted by systemd or a container
    /// runtime — seconds later, which is the normal case — would refuse to start for a
    /// full `LEASE_TTL` over a directory nobody holds.
    ///
    /// The lease is seeded **fresh** (TTL-live), written on this kernel, and nothing holds
    /// the lock: precisely the crashed-and-restarted shape.
    #[tokio::test]
    async fn a_crashed_owner_on_this_host_does_not_block_a_restart() {
        let (Some(host), Some(boot_id)) = (this_host(), this_boot_id()) else {
            return;
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(OWNER_LEASE_FILE);

        write_lease(
            &path,
            &LeaseRecord {
                owner: format!("{host}:0:00000000"),
                renewed_at_ms: remote_lease::now_unix_ms(),
                epoch: 3,
                boot_id: Some(boot_id),
                lock_dev: Some(this_lock_dev(dir.path())),
            },
        )
        .expect("seed a fresh lease from a crashed owner");

        let owner = acquire_state_dir(dir.path())
            .expect("a lease whose owner on this kernel holds no lock must be takeable at once");
        assert_eq!(owner.epoch, 4, "the takeover must still advance the epoch");
    }

    /// A lease without a kernel id — written by an older version — is judged by its TTL.
    ///
    /// Without the kernel id the lock cannot vouch for it: its owner may have run anywhere.
    /// Refusing until it expires is the conservative reading, and costs one TTL at upgrade.
    #[tokio::test]
    async fn a_lease_without_a_kernel_id_is_judged_by_its_ttl() {
        let Some(host) = this_host() else {
            return;
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(OWNER_LEASE_FILE);

        write_lease(
            &path,
            &LeaseRecord {
                owner: format!("{host}:{}:00000000", std::process::id()),
                renewed_at_ms: remote_lease::now_unix_ms(),
                epoch: 1,
                boot_id: None,
                lock_dev: None,
            },
        )
        .expect("seed a live lease");

        acquire_state_dir(dir.path())
            .err()
            .expect("a live lease without a kernel id must be refused");
    }

    /// A lease from another kernel is judged by its TTL even when its hostname is ours.
    ///
    /// A pinned hostname is shared by containers on different hosts, and on a network
    /// filesystem such an owner's lock may be invisible here: taking our lock proves
    /// nothing about it. The kernel id, not the hostname, decides whether the lock can.
    #[tokio::test]
    async fn a_lease_from_another_kernel_with_our_hostname_is_judged_by_its_ttl() {
        let Some(host) = this_host() else {
            return;
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(OWNER_LEASE_FILE);

        write_lease(
            &path,
            &LeaseRecord {
                owner: format!("{host}:1:00000000"),
                renewed_at_ms: remote_lease::now_unix_ms(),
                epoch: 1,
                boot_id: Some("another-kernel".to_string()),
                lock_dev: Some(this_lock_dev(dir.path())),
            },
        )
        .expect("seed a live lease from another kernel");

        acquire_state_dir(dir.path())
            .err()
            .expect("a live lease from another kernel must be refused until it expires");
    }

    /// An idle owner keeps its lease: the renewal runs on a timer, not only on writes.
    #[tokio::test]
    async fn an_idle_owner_renews_its_lease_on_a_timer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let owner = acquire_state_dir(dir.path()).expect("owner");
        let path = dir.path().join(OWNER_LEASE_FILE);

        // As if the owner had written nothing for most of a TTL.
        let aged = remote_lease::now_unix_ms() - (LEASE_TTL.as_millis() as u64) + 1_000;
        let mut lease = read_lease(&path).expect("read").expect("lease");
        lease.renewed_at_ms = aged;
        write_lease(&path, &lease).expect("age the lease");
        owner.last_renewed_ms.store(0, Ordering::Relaxed);

        let renewal = spawn_lease_renewal(&owner, std::time::Duration::from_millis(20));
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let renewed = read_lease(&path).expect("read").expect("lease");
        assert!(
            renewed.renewed_at_ms > aged + 1_000,
            "the timer must renew an idle owner's lease"
        );

        drop(owner);
        tokio::time::timeout(std::time::Duration::from_secs(1), renewal)
            .await
            .expect("the timer stops once the owner is gone")
            .expect("the timer task ends cleanly");
    }

    /// A lease from another host is judged by the TTL alone.
    ///
    /// Its PID means nothing here — treating a foreign PID as stale is exactly the
    /// `StateDirLock` mistake that let two hosts share an NFS state directory.
    #[tokio::test]
    async fn a_foreign_host_lease_is_not_second_guessed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(OWNER_LEASE_FILE);

        write_lease(
            &path,
            &LeaseRecord {
                owner: "some-other-host:0:00000000".to_string(),
                renewed_at_ms: remote_lease::now_unix_ms(),
                epoch: 1,
                boot_id: None,
                lock_dev: None,
            },
        )
        .expect("seed a foreign lease");

        let error = acquire_state_dir(dir.path())
            .err()
            .expect("a live foreign lease must be refused whatever its pid says");
        assert!(error.to_string().contains("some-other-host"));
    }

    /// This kernel's boot id, as the lease records it; `None` off Linux.
    fn this_boot_id() -> Option<String> {
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .ok()
            .map(|id| id.trim().to_string())
            .filter(|id| !id.is_empty())
    }

    /// The device of the filesystem a test's state directory is on, as the lease records it.
    fn this_lock_dev(dir: &Path) -> u64 {
        use std::os::unix::fs::MetadataExt as _;
        std::fs::metadata(dir).expect("state dir").dev()
    }

    /// A lease from this kernel but another filesystem device is judged by its TTL.
    ///
    /// Two mounts of one network directory can each keep their own locks. Our lock on one
    /// says nothing about an owner that locked through the other, so the boot id alone does
    /// not decide.
    #[tokio::test]
    async fn a_lease_from_this_kernel_on_another_device_is_judged_by_its_ttl() {
        let (Some(host), Some(boot_id)) = (this_host(), this_boot_id()) else {
            return;
        };
        let dir = tempfile::tempdir().expect("tempdir");
        seed_lease(
            dir.path(),
            serde_json::json!({
                "owner": format!("{host}:1:00000000"),
                "renewed_at_ms": remote_lease::now_unix_ms(),
                "epoch": 1,
                "boot_id": boot_id,
                "lock_dev": this_lock_dev(dir.path()).wrapping_add(1),
            }),
        );

        acquire_state_dir(dir.path())
            .err()
            .expect("a live lease locked through another device must be refused until it expires");
    }

    /// Write a lease as raw JSON, so these tests do not depend on the record's Rust shape.
    fn seed_lease(dir: &Path, value: serde_json::Value) {
        std::fs::write(
            dir.join(OWNER_LEASE_FILE),
            serde_json::to_vec(&value).expect("json"),
        )
        .expect("seed the lease");
    }

    /// A crashed owner with the same PID as its successor is taken over at once.
    ///
    /// In a container both are PID 1, so a liveness check that compares PIDs reads the dead
    /// owner as this very process and waits out the TTL. Here the lease names our own PID
    /// on this kernel, and nothing holds the lock: that owner is gone.
    #[tokio::test]
    async fn a_crashed_owner_with_our_own_pid_on_this_kernel_is_taken_over_at_once() {
        let (Some(host), Some(boot_id)) = (this_host(), this_boot_id()) else {
            return;
        };
        let dir = tempfile::tempdir().expect("tempdir");
        seed_lease(
            dir.path(),
            serde_json::json!({
                "owner": format!("{host}:{}:00000001", std::process::id()),
                "renewed_at_ms": remote_lease::now_unix_ms(),
                "epoch": 5,
                "boot_id": boot_id,
                "lock_dev": this_lock_dev(dir.path()),
            }),
        );

        let owner = acquire_state_dir(dir.path())
            .expect("a crashed owner with our own PID must not hold the directory for the TTL");
        assert_eq!(owner.epoch, 6);
    }

    /// A running owner keeps the directory even when its lease looks expired.
    ///
    /// An idle pipeline writes no checkpoints. Liveness must not depend on its lease being
    /// fresh: here the lease is aged past the TTL while its owner still runs.
    #[tokio::test]
    async fn a_running_owner_whose_lease_looks_expired_is_still_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let owner = acquire_state_dir(dir.path()).expect("first owner");
        let expired = remote_lease::now_unix_ms() - (LEASE_TTL.as_millis() as u64) - 1_000;
        let mut lease: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.path().join(OWNER_LEASE_FILE)).expect("lease"),
        )
        .expect("json");
        lease["renewed_at_ms"] = serde_json::json!(expired);
        seed_lease(dir.path(), lease);

        acquire_state_dir(dir.path())
            .err()
            .expect("a running owner must keep the directory whatever its lease's age");
        drop(owner);
    }

    /// The probe `a_running_owner_is_refused_without_a_kill_binary` runs in a child process.
    #[tokio::test]
    async fn lease_probe_child() {
        let Ok(dir) = std::env::var("RUSTCDC_LEASE_PROBE_DIR") else {
            return;
        };
        match acquire_state_dir(Path::new(&dir)) {
            Ok(_) => println!("LEASE-PROBE: acquired"),
            Err(_) => println!("LEASE-PROBE: refused"),
        }
    }

    /// Liveness must not depend on an external program.
    ///
    /// The release image ships no binaries at all, so a check that runs `kill -0` cannot
    /// run there and reads every owner as dead. A second process with an empty `PATH`
    /// must still be refused while this one holds the directory.
    #[tokio::test]
    async fn a_running_owner_is_refused_without_a_kill_binary() {
        let dir = tempfile::tempdir().expect("tempdir");
        let owner = acquire_state_dir(dir.path()).expect("first owner");

        let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "state::offset::local_fs::lease_tests::lease_probe_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("RUSTCDC_LEASE_PROBE_DIR", dir.path())
            .env("PATH", "")
            .output()
            .expect("run the probe");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("LEASE-PROBE: refused"),
            "a second process without `kill` on its PATH must be refused while the owner \
             runs; it said: {}",
            stdout
                .lines()
                .find(|line| line.contains("LEASE-PROBE"))
                .unwrap_or("nothing")
        );
        drop(owner);
    }

    /// The checkpoint a pipeline builds keeps its lease while it writes nothing: the
    /// renewal timer is started with it, not left to the caller.
    #[tokio::test(start_paused = true)]
    async fn a_pipeline_checkpoint_keeps_its_lease_while_idle() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_checkpoint, _age, owner) = build_checkpoint(dir.path()).await.expect("build");
        let path = dir.path().join(OWNER_LEASE_FILE);

        let aged = remote_lease::now_unix_ms() - (LEASE_TTL.as_millis() as u64) + 1_000;
        let mut lease = read_lease(&path).expect("read").expect("lease");
        lease.renewed_at_ms = aged;
        write_lease(&path, &lease).expect("age the lease");
        owner.last_renewed_ms.store(0, Ordering::Relaxed);

        // One heartbeat, with no checkpoint written in between.
        tokio::time::sleep(LEASE_HEARTBEAT + std::time::Duration::from_secs(1)).await;
        let renewed = read_lease(&path).expect("read").expect("lease");
        assert!(
            renewed.renewed_at_ms > aged + 1_000,
            "a pipeline's lease must be renewed while it is idle"
        );
    }

    /// A lease written by an owner that could not lock is never read as "its owner would
    /// hold the lock".
    ///
    /// Locks can fail transiently (`ENOLCK`), so a successor may take the lock that the
    /// first owner was refused. If the first owner's lease claimed the lock, that successor
    /// would conclude the owner had crashed and take over a live process.
    #[tokio::test]
    async fn a_lease_only_owner_is_not_taken_over_when_locks_work_again() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = acquire_with(dir.path(), |_| {
            Ok(TryLock::Unsupported(std::io::Error::other("no locks")))
        })
        .expect("an owner on a filesystem that refuses locks");

        acquire_state_dir(dir.path())
            .err()
            .expect("an owner that never held the lock must be fenced by its lease");
        drop(first);
    }

    /// An unparseable lease is replaced rather than being fatal.
    #[tokio::test]
    async fn an_unreadable_lease_is_replaced() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(dir.path()).expect("dir");
        std::fs::write(dir.path().join(OWNER_LEASE_FILE), b"{not json").expect("corrupt lease");

        acquire_state_dir(dir.path()).expect("a corrupt lease must not brick the directory");
    }
}
