use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use async_trait::async_trait;
use rustcdc::checkpoint::{Checkpoint, FileCheckpoint};
use rustcdc::core::{Error as RtError, Offset};

use crate::error::AppError;

use crate::state::CheckpointAgeSource;
use crate::state::remote_lease::{self, LEASE_HEARTBEAT, LEASE_TTL, LeaseRecord};

/// The owner lease file, beside the checkpoint it protects.
const OWNER_LEASE_FILE: &str = "owner_lease.json";

/// A `local_fs` state directory this process believes it owns.
///
/// # Why a filesystem backend needs a lease at all
///
/// Every other backend is fenced: `redis` and `postgresql` take an owner+epoch lease
/// through OpenDAL, and `kafka_topic` is fenced at the broker by its transactional id.
/// `local_fs` — the **default** backend, and the one used outside Kubernetes — had only
/// `StateDirLock`, a PID file validated with `kill -0` and `/proc/<pid>/cmdline`.
///
/// Every one of those checks is host-local. A PID from another host is indistinguishable
/// from a stale one, so the lock is simply overwritten. Two `rustcdc run` invocations
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
/// # What this guarantees, and what it does not
///
/// The same contract as [`crate::state::remote_lease`], deliberately: one record type, one
/// TTL, one heartbeat, one conflict message. A second instance starting against a live
/// lease refuses to run and names the holder; a crashed owner's lease expires by itself
/// after `LEASE_TTL`; an owner whose lease is stolen discovers it on the next renewal and
/// stops writing rather than continuing blind.
///
/// It is **not** a consensus lease. Acquisition is read-then-write, not compare-and-swap,
/// so two instances starting inside the same read-write window can both see a free slot.
/// On a filesystem that does not honour ordinary write visibility across hosts it
/// guarantees nothing at all. It converts the common silent interleave into a loud
/// refusal; it is not a substitute for a store with real CAS.
pub(crate) struct OwnedStateDir {
    path: PathBuf,
    owner: String,
    epoch: u64,
    last_renewed_ms: AtomicU64,
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

    /// Release the lease so a successor need not wait out the TTL.
    pub(crate) fn release(&self) {
        // Only if we still hold it: a fenced-out instance deleting the file would hand
        // the directory to a third process while the real owner is still writing.
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

/// Does this lease belong to a process on **this host** that is no longer running?
///
/// `owner_id()` is `host:pid:nonce`. A different host, or an unparseable id, answers
/// `false` — the TTL is the only evidence available then, and guessing would be the
/// `StateDirLock` mistake this lease exists to correct.
fn is_dead_local_owner(record: &LeaseRecord) -> bool {
    let mut parts = record.owner.splitn(3, ':');
    let (Some(host), Some(pid), Some(_nonce)) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    // `None` means this process could not determine its own hostname. A PID is only
    // checkable against the local process table when we know the lease was written on
    // *this* machine, and "both sides failed to resolve a name" is not that knowledge —
    // treating it as a match would let a host with a shared state volume steal a lease
    // from a live writer on another host.
    let Some(local_host) = remote_lease::owner_host() else {
        return false;
    };
    if host != local_host {
        return false;
    }
    let Ok(pid) = pid.parse::<u32>() else {
        return false;
    };
    // Our own PID is by definition alive; treating it as dead would let one process take
    // a directory from itself, which is how the delivery-contract harness would silently
    // run two owners in one process.
    if pid == std::process::id() {
        return false;
    }
    !crate::commands::run::is_cdc_process_alive(pid)
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
    let path = state_dir.join(OWNER_LEASE_FILE);
    let now_ms = remote_lease::now_unix_ms();
    let existing = read_lease(&path)?;

    let epoch = match &existing {
        // A TTL-live lease held by a **dead process on this host** is a crash, not a
        // conflict, and waiting out the TTL for it would be a regression: a filesystem
        // backend restarted by systemd or a container runtime seconds after a crash would
        // refuse to start for a full minute, for a directory nobody holds.
        //
        // This is the evidence a local filesystem gives that a remote store cannot: the
        // owner's PID is checkable, precisely, right now. It only applies same-host —
        // a PID from another machine means nothing here, which is exactly the case the
        // old `StateDirLock` got wrong by treating it as stale.
        Some(record) if record.is_live(now_ms, LEASE_TTL) && is_dead_local_owner(record) => {
            tracing::warn!(
                previous_owner = %record.owner,
                previous_epoch = record.epoch,
                "taking over a local_fs state lease from a dead process on this host"
            );
            record.epoch.saturating_add(1)
        }
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
        },
    )?;

    tracing::info!(owner = %owner, epoch, "acquired local_fs state lease");

    Ok(Arc::new(OwnedStateDir {
        path,
        owner,
        epoch,
        last_renewed_ms: AtomicU64::new(now_ms),
    }))
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

    /// This host's name as `is_dead_local_owner` will compare it.
    ///
    /// The tests that seed a lease "from this host" need the same answer the production
    /// code gets. On a machine where the name cannot be resolved at all the same-host
    /// liveness shortcut is deliberately disabled, so those tests have nothing to assert
    /// and skip — the TTL path is covered separately by
    /// `a_lease_from_another_host_is_judged_by_the_ttl_alone`.
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
    /// full `LEASE_TTL` over a directory nobody holds. That is a worse operational
    /// regression than the interleave the lease exists to prevent, and it is avoidable:
    /// on the same host the owner's PID is checkable right now.
    ///
    /// The lease is seeded **fresh** (renewed a moment ago, so TTL-live) with a PID that
    /// cannot be running, which is precisely the crashed-and-restarted shape. Deleting the
    /// `is_dead_local_owner` arm from `acquire_state_dir` fails this test.
    #[tokio::test]
    async fn a_crashed_owner_on_this_host_does_not_block_a_restart() {
        let Some(host) = this_host() else {
            return;
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(OWNER_LEASE_FILE);

        // PID 0 is never a live user process, and the host matches ours.
        write_lease(
            &path,
            &LeaseRecord {
                owner: format!("{host}:0:00000000"),
                renewed_at_ms: remote_lease::now_unix_ms(),
                epoch: 3,
            },
        )
        .expect("seed a fresh lease from a dead pid");

        let owner = acquire_state_dir(dir.path())
            .expect("a lease held by a dead local process must be takeable immediately");
        assert_eq!(owner.epoch, 4, "the takeover must still advance the epoch");
    }

    /// …but a *live* owner on this host is still a conflict.
    ///
    /// The liveness check must not become a way to take a directory from a running
    /// process. Our own PID is the sharpest case: it is unambiguously alive.
    #[tokio::test]
    async fn a_live_owner_on_this_host_is_still_refused() {
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
            },
        )
        .expect("seed a live lease");

        acquire_state_dir(dir.path())
            .err()
            .expect("a live owner must still be refused");
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
            },
        )
        .expect("seed a foreign lease");

        let error = acquire_state_dir(dir.path())
            .err()
            .expect("a live foreign lease must be refused whatever its pid says");
        assert!(error.to_string().contains("some-other-host"));
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
