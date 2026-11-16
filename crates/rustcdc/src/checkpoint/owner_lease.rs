//! Exclusive ownership for single-writer file stores.
//!
//! The owner holds an advisory lock on `<sentinel>.lock` for the lease's lifetime, and the
//! sentinel records `HOSTNAME:PID`. The kernel releases the lock when its holder exits,
//! however it exits, so the lock decides:
//!
//! - **Held by someone else**: a running owner. Refused.
//! - **Free**: whoever the sentinel names is gone (a crash, a replaced container with another
//!   hostname, a restart with another PID), so it is taken over. No process table is read
//!   and no external program runs.
//! - **The filesystem refuses locks**: a sentinel naming another owner is refused, with the
//!   file to delete if that owner is gone; nothing here can tell.
//!
//! A filesystem that does not share locks between hosts cannot keep two hosts apart. There
//! the sentinel fences: before every durable write the owner checks the sentinel still holds
//! its token, so a displaced owner stops at its next write.
//!
//! **`FileCheckpoint` and `FileSchemaHistory` are not for concurrent use from several hosts.**
//! Use a dedicated directory per runtime instance.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

use crate::core::{Error, Result};

// ─── Lease type ─────────────────────────────────────────────────────────────

/// RAII guard that releases the lease on drop.
///
/// Do not hold this across `await` points — use a `std::sync::Mutex<Option<OwnerLease>>`
/// and re-acquire it synchronously within each operation.
#[derive(Debug)]
pub(crate) struct OwnerLease {
    pub(crate) lock_path: PathBuf,
    /// The exact token written when this lease was acquired.
    ///
    /// Held so [`OwnerLease::verify_still_held`] can fence writes: a lease that was
    /// acquired successfully is not thereby held *forever*.
    token: String,
    /// The lock that makes this process the owner; `None` where the filesystem refuses locks.
    lock: Option<fs::File>,
}

impl OwnerLease {
    /// Confirm this process still owns the lease, before a durable write.
    ///
    /// Acquiring a lease once is not the same as holding it. An operator can delete
    /// the sentinel file to clear what looks like a stuck lease; a peer that saw this
    /// process as dead can take it over; a shared-filesystem mount can be swapped
    /// underneath. Unchecked, the original owner carries on writing in every one of those
    /// cases, and two writers rewriting the same whole-file store silently destroy each
    /// other's records.
    ///
    /// Checking costs one small read per durable write — the write itself is an
    /// fsync plus a rename, so the read is not measurable — and converts silent
    /// mutual corruption into a loud, named error.
    pub(crate) fn verify_still_held(&self, store_label: &str) -> Result<()> {
        let raw = match fs::read_to_string(&self.lock_path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::StateError(format!(
                    "{store_label} owner lease file '{}' has disappeared; this process no \
                     longer holds exclusive write access. Another instance may have taken \
                     over, or the file was removed manually. Refusing to write, because two \
                     writers against one store destroy each other's records. Restart this \
                     runtime after confirming no other instance is running.",
                    self.lock_path.display()
                )));
            }
            Err(error) => return Err(Error::from(error)),
        };

        if raw.trim() == self.token {
            return Ok(());
        }

        Err(Error::StateError(format!(
            "{store_label} owner lease at '{}' is no longer held by this process \
             (expected '{}', found '{}'). Another instance has taken ownership. Refusing \
             to write, because two writers against one store destroy each other's records. \
             Use a dedicated {store_label} directory per runtime instance.",
            self.lock_path.display(),
            self.token,
            raw.trim()
        )))
    }
}

impl Drop for OwnerLease {
    fn drop(&mut self) {
        let remaining = decrement_lease_ref(&self.lock_path);
        if remaining == 0 {
            let _ = fs::remove_file(&self.lock_path);
        }
        // Explicitly, not by closing: a child process holds a copy of the lock's file
        // description from its creation until it execs, and would keep the lock held.
        if let Some(lock) = self.lock.take() {
            let _ = lock.unlock();
        }
    }
}

// ─── Ref-count registry ─────────────────────────────────────────────────────

static LEASE_REFS: OnceLock<Mutex<std::collections::HashMap<PathBuf, usize>>> = OnceLock::new();

fn lease_ref_counts() -> &'static Mutex<std::collections::HashMap<PathBuf, usize>> {
    LEASE_REFS.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

pub(crate) fn decrement_lease_ref(lock_path: &Path) -> usize {
    let Ok(mut refs) = lease_ref_counts().lock() else {
        return 0;
    };
    let Some(entry) = refs.get_mut(lock_path) else {
        return 0;
    };
    if *entry > 1 {
        *entry -= 1;
        *entry
    } else {
        refs.remove(lock_path);
        0
    }
}

// ─── Hostname ────────────────────────────────────────────────────────────────

/// Return the hostname for lease identity, cached after first call.
///
/// Resolution order: `HOSTNAME` env var → `hostname` command → `"unknown"`.
/// The value is cached in a `OnceLock` so the command is executed at most once
/// per process lifetime.
pub(crate) fn current_hostname() -> &'static str {
    static HOSTNAME: OnceLock<String> = OnceLock::new();
    HOSTNAME.get_or_init(|| {
        // `HOSTNAME` is set automatically in most Linux/container environments.
        if let Ok(h) = std::env::var("HOSTNAME") {
            let h = h.trim().to_owned();
            if !h.is_empty() {
                return h;
            }
        }
        // Fall back to the `hostname` command on Unix.
        #[cfg(unix)]
        if let Ok(output) = std::process::Command::new("hostname").output()
            && let Ok(s) = std::str::from_utf8(&output.stdout)
        {
            let s = s.trim().to_owned();
            if !s.is_empty() {
                return s;
            }
        }
        "unknown".to_owned()
    })
}

/// Serialise a lease token into the on-disk format `HOSTNAME:PID`.
pub(crate) fn format_lease(hostname: &str, pid: u32) -> String {
    format!("{hostname}:{pid}")
}

/// Parse a lease token in `HOSTNAME:PID` format.
///
/// Returns `None` when the content is empty or malformed.
#[cfg(test)]
pub(crate) fn parse_lease(contents: &str) -> Option<(String, u32)> {
    let contents = contents.trim();
    if let Some(colon) = contents.rfind(':') {
        let host = &contents[..colon];
        let pid_str = &contents[colon + 1..];
        if !host.is_empty()
            && let Ok(pid) = pid_str.parse::<u32>()
        {
            return Some((host.to_owned(), pid));
        }
    }
    None
}

// ─── Acquire ────────────────────────────────────────────────────────────────

/// Atomically replace (or create) `lock_path` with `content`.
///
/// Writes to a sibling temp file then renames over `lock_path`.  On POSIX,
/// `rename(2)` is atomic within the same filesystem, eliminating the TOCTOU
/// window present in a `remove → create_new` pattern.
///
/// # Atomicity guarantees
/// - POSIX: the rename is atomic — no other process can observe a moment
///   where `lock_path` is absent.
/// - Windows: `fs::rename` is not atomic with respect to an existing target;
///   the method falls back to a compare-and-swap attempt which is
///   best-effort but still better than remove-then-create.
pub(crate) fn atomic_write_lease(lock_path: &Path, content: &str) -> std::io::Result<()> {
    let parent = lock_path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "lock_path has no parent")
    })?;

    // Generate a unique temp path in the same directory (same device → rename works).
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let tmp_path = parent.join(format!(".owner_lease_{stamp}.tmp"));

    let mut tmp_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp_path)?;
    tmp_file.write_all(content.as_bytes())?;
    tmp_file.sync_all()?;
    drop(tmp_file);

    // Atomic replace.
    if let Err(err) = fs::rename(&tmp_path, lock_path) {
        let _ = fs::remove_file(&tmp_path); // best-effort cleanup
        return Err(err);
    }

    Ok(())
}

/// Reject a second in-process holder of the same lease path.
///
/// Checked before the lock so the error says what happened: a second `FileCheckpoint` or
/// `FileSchemaHistory` on the same directory in the same process would hold independent
/// in-memory state and rewrite the whole file, destroying the other's records.
///
/// Instances are sequential-safe: `Drop` decrements the count, so constructing one
/// after another has been dropped is fine. Clones share a single `OwnerLease` and
/// never re-acquire.
fn reserve_in_process(lock_path: &Path, store_label: &str) -> Result<()> {
    let mut refs = lease_ref_counts().lock().map_err(|_| {
        Error::StateError(format!("{store_label} owner lease registry lock poisoned"))
    })?;

    if refs.get(lock_path).copied().unwrap_or(0) > 0 {
        return Err(Error::StateError(format!(
            "{store_label} owner lease for '{}' is already held by another instance in \
             this process. Two instances against one store hold independent in-memory \
             state and rewrite the whole file, silently destroying each other's records. \
             Share one instance (it is cheap to clone), or use a dedicated \
             {store_label} directory per instance.",
            lock_path.display()
        )));
    }

    refs.insert(lock_path.to_path_buf(), 1);
    Ok(())
}

pub(crate) fn acquire(lock_path: &Path, store_label: &str) -> Result<OwnerLease> {
    // Claim in-process ownership first, so the on-disk work is skipped entirely when
    // a sibling instance already holds this path. Release the reservation if the
    // on-disk acquire then fails, otherwise a rejected attempt would poison the path
    // for the rest of the process lifetime.
    reserve_in_process(lock_path, store_label)?;
    match acquire_on_disk(lock_path, store_label) {
        Ok(lease) => Ok(lease),
        Err(error) => {
            decrement_lease_ref(lock_path);
            Err(error)
        }
    }
}

fn acquire_on_disk(lock_path: &Path, store_label: &str) -> Result<OwnerLease> {
    let parent_dir = lock_path
        .parent()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    let existing = fs::read_to_string(lock_path)
        .ok()
        .map(|raw| raw.trim().to_owned())
        .filter(|raw| !raw.is_empty());

    let lock = match try_lock(&lock_file_for(lock_path))? {
        TryLock::Held(file) => Some(file),
        TryLock::Busy => {
            return Err(Error::StateError(format!(
                "{store_label} owner lease conflict for '{parent_dir}': a running process \
                 holds it ({}). Use a dedicated {store_label} directory per runtime instance.",
                existing.as_deref().unwrap_or("owner unknown"),
            )));
        }
        TryLock::Unsupported(error) => {
            tracing::warn!(
                target: "rustcdc::owner_lease",
                store_label,
                store_dir = %parent_dir,
                error = %error,
                "the filesystem refused an advisory lock; the {store_label} owner lease \
                 cannot tell a crashed owner from a running one here"
            );
            None
        }
    };

    let token = format_lease(current_hostname(), std::process::id());
    if let Some(previous) = existing.filter(|previous| *previous != token) {
        if lock.is_none() {
            return Err(Error::StateError(format!(
                "{store_label} owner lease conflict for '{parent_dir}': held by '{previous}', \
                 and this filesystem refuses locks, so whether it is still running cannot be \
                 told. If it is not, delete '{}' and retry.",
                lock_path.display(),
            )));
        }
        tracing::warn!(
            target: "rustcdc::owner_lease",
            store_label,
            store_dir = %parent_dir,
            previous_owner = %previous,
            "taking over a {store_label} owner lease whose owner no longer holds its lock"
        );
    }
    atomic_write_lease(lock_path, &token).map_err(Error::from)?;

    Ok(OwnerLease {
        lock_path: lock_path.to_path_buf(),
        token,
        lock,
    })
}

/// The lock file beside a sentinel: `<sentinel>.lock`. Never removed: deleting a lock file
/// races a successor that already opened it.
fn lock_file_for(sentinel: &Path) -> PathBuf {
    let mut path = sentinel.as_os_str().to_owned();
    path.push(".lock");
    PathBuf::from(path)
}

enum TryLock {
    Held(fs::File),
    Busy,
    Unsupported(std::io::Error),
}

fn try_lock(path: &Path) -> Result<TryLock> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(Error::from)?;
    match file.try_lock() {
        Ok(()) => Ok(TryLock::Held(file)),
        Err(fs::TryLockError::WouldBlock) => Ok(TryLock::Busy),
        Err(fs::TryLockError::Error(error)) => Ok(TryLock::Unsupported(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn format_and_parse_round_trip() {
        let token = format_lease("myhost", 12345);
        assert_eq!(token, "myhost:12345");
        let (host, pid) = parse_lease(&token).unwrap();
        assert_eq!(host, "myhost");
        assert_eq!(pid, 12345);
    }

    #[test]
    fn parse_rejects_old_pid_only_format() {
        assert!(parse_lease("12345").is_none());
    }

    #[test]
    fn parse_handles_hostname_with_dots() {
        let token = format_lease("host.example.com", 99);
        let (host, pid) = parse_lease(&token).unwrap();
        assert_eq!(host, "host.example.com");
        assert_eq!(pid, 99);
    }

    #[test]
    fn parse_handles_hostname_with_hyphens() {
        let token = "my-container-1:42";
        let (host, pid) = parse_lease(token).unwrap();
        assert_eq!(host, "my-container-1");
        assert_eq!(pid, 42);
    }

    #[test]
    fn parse_rejects_empty() {
        assert!(parse_lease("").is_none());
        assert!(parse_lease("  ").is_none());
    }

    // ── acquire() tests ──────────────────────────────────────────────────────

    /// Fresh directory: acquire creates the lease file and returns an OwnerLease.
    #[test]
    fn acquire_creates_new_lease() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("test.owner");
        let lease = acquire(&lock_path, "test").unwrap();
        assert_eq!(lease.lock_path, lock_path);
        let contents = std::fs::read_to_string(&lock_path).unwrap();
        let (host, pid) = parse_lease(&contents).unwrap();
        assert_eq!(host, current_hostname());
        assert_eq!(pid, std::process::id());
        // OwnerLease::drop removes the file.
        drop(lease);
        assert!(!lock_path.exists());
    }

    /// Dropping the lease removes the sentinel file. A second in-process `acquire` is
    /// refused — see `a_second_instance_in_this_process_is_refused`.
    #[test]
    fn dropping_the_lease_removes_the_sentinel_file() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("test.owner");
        let lease = acquire(&lock_path, "test").unwrap();
        assert!(lock_path.exists());
        drop(lease);
        assert!(!lock_path.exists());
    }

    /// A running owner holds the lock: refused, whatever its sentinel says.
    #[test]
    fn a_running_owner_holding_the_lock_is_refused() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("test.owner");
        std::fs::write(&lock_path, "other-host:4242").unwrap();
        let held = match try_lock(&lock_file_for(&lock_path)).unwrap() {
            TryLock::Held(file) => file,
            _ => panic!("the test must hold the lock"),
        };
        let error = acquire(&lock_path, "test").unwrap_err();
        assert!(
            error.to_string().contains("a running process holds it"),
            "{error}"
        );
        // Explicitly: a child another test forks holds a copy until it execs.
        held.unlock().unwrap();
        assert!(
            acquire(&lock_path, "test").is_ok(),
            "free once its holder is gone"
        );
    }

    /// Dropping the lease releases its lock, so a successor is admitted at once.
    #[test]
    fn dropping_the_lease_releases_its_lock() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("test.owner");
        let lease = acquire(&lock_path, "test").unwrap();
        assert!(matches!(
            try_lock(&lock_file_for(&lock_path)).unwrap(),
            TryLock::Busy
        ));
        drop(lease);
        assert!(matches!(
            try_lock(&lock_file_for(&lock_path)).unwrap(),
            TryLock::Held(_)
        ));
    }

    /// A crashed owner on this host leaves its sentinel and no lock: taken over.
    #[test]
    fn acquire_clears_stale_dead_pid_lease() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("test.owner");
        let stale = format_lease(current_hostname(), u32::MAX);
        std::fs::write(&lock_path, stale.as_bytes()).unwrap();
        let lease = acquire(&lock_path, "test").unwrap();
        let contents = std::fs::read_to_string(&lock_path).unwrap();
        let (host, pid) = parse_lease(&contents).unwrap();
        assert_eq!(host, current_hostname());
        assert_eq!(pid, std::process::id());
        drop(lease);
    }

    /// What a crashed owner in a replaced container leaves behind: its own hostname, PID 1.
    /// Nothing holds the lock, so the owner is gone, whatever the sentinel names.
    #[test]
    fn a_lease_left_by_another_container_is_taken_over() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("test.owner");
        std::fs::write(&lock_path, "old-container-hostname:1").unwrap();
        let lease = acquire(&lock_path, "test").expect("nothing holds the lock");
        let contents = std::fs::read_to_string(&lock_path).unwrap();
        assert_eq!(
            parse_lease(&contents),
            Some((current_hostname().to_owned(), std::process::id()))
        );
        drop(lease);
    }

    #[test]
    fn decrementing_past_zero_is_a_no_op() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("refcount.owner");
        assert_eq!(decrement_lease_ref(&lock_path), 0);
    }

    #[test]
    fn a_second_instance_in_this_process_is_refused() {
        // Each instance would rewrite the whole store file from its own in-memory state,
        // destroying the other's records.
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("second.owner");

        let first = acquire(&lock_path, "checkpoint").expect("first acquire must succeed");
        let error = acquire(&lock_path, "checkpoint")
            .expect_err("a second in-process instance must be refused");
        let message = error.to_string();
        assert!(
            message.contains("already held by another instance in this process"),
            "message must name the real cause; got: {message}"
        );

        // Sequential use is still fine.
        drop(first);
        acquire(&lock_path, "checkpoint").expect("acquire after release must succeed");
    }

    #[test]
    fn a_rejected_acquire_does_not_poison_the_path() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("poison.owner");
        // A held lock makes the on-disk acquire fail.
        let held = match try_lock(&lock_file_for(&lock_path)).unwrap() {
            TryLock::Held(file) => file,
            _ => panic!("the test must hold the lock"),
        };

        assert!(acquire(&lock_path, "checkpoint").is_err());

        // The in-process reservation must have been released, so a later attempt is
        // judged on its own merits rather than inheriting the failed one.
        // Explicitly: a child another test forks holds a copy until it execs.
        held.unlock().unwrap();
        acquire(&lock_path, "checkpoint").expect("path must not be poisoned by the failure");
    }

    #[test]
    fn verify_still_held_detects_a_stolen_or_deleted_lease() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("fence.owner");
        let lease = acquire(&lock_path, "checkpoint").unwrap();

        lease
            .verify_still_held("checkpoint")
            .expect("freshly acquired lease must verify");

        // Someone else takes ownership.
        std::fs::write(&lock_path, format_lease("other-host", 999)).unwrap();
        let error = lease.verify_still_held("checkpoint").unwrap_err();
        assert!(error.to_string().contains("no longer held by this process"));

        // Or the file is removed entirely.
        std::fs::remove_file(&lock_path).unwrap();
        let error = lease.verify_still_held("checkpoint").unwrap_err();
        assert!(error.to_string().contains("disappeared"));
    }
}
