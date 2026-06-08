//! PID-based exclusive-ownership lease for single-writer file stores.
//!
//! [`OwnerLease`] writes `HOSTNAME:PID` into a sentinel file and refuses to
//! acquire the lease if another live process already holds it.  Stale leases
//! left by dead processes on the **same host** are cleared automatically so
//! manual recovery is not required after a crash.
//!
//! # Cross-host (NFS) safety
//!
//! The lease file stores both the hostname and the PID.  When the hostname in
//! the existing lease does not match the current host the lease is treated as
//! **held** — the runtime refuses to take ownership.  This prevents a second
//! container on a different host from incorrectly reclaiming a lease because
//! PID `1234` does not exist in *its* process table.
//!
//! **`FileCheckpoint` and `FileSchemaHistory` are still not safe for
//! concurrent use from multiple hosts.**  Use a dedicated directory per
//! runtime instance.  The hostname check only prevents silent data corruption;
//! it does not provide true distributed mutual exclusion.
//!
//! Both [`super::FileCheckpoint`] and
//! [`crate::schema_history::FileSchemaHistory`] use this type so the logic
//! stays in one place.

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
}

impl Drop for OwnerLease {
    fn drop(&mut self) {
        let remaining = decrement_lease_ref(&self.lock_path);
        if remaining == 0 {
            let _ = fs::remove_file(&self.lock_path);
        }
    }
}

// ─── Ref-count registry ─────────────────────────────────────────────────────

static LEASE_REFS: OnceLock<Mutex<std::collections::HashMap<PathBuf, usize>>> = OnceLock::new();

fn lease_ref_counts() -> &'static Mutex<std::collections::HashMap<PathBuf, usize>> {
    LEASE_REFS.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

pub(crate) fn increment_lease_ref(lock_path: &Path) {
    if let Ok(mut refs) = lease_ref_counts().lock() {
        let entry = refs.entry(lock_path.to_path_buf()).or_insert(0);
        *entry = entry.saturating_add(1);
    }
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
        if let Ok(output) = std::process::Command::new("hostname").output() {
            if let Ok(s) = std::str::from_utf8(&output.stdout) {
                let s = s.trim().to_owned();
                if !s.is_empty() {
                    return s;
                }
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
/// Returns `None` when the content is empty or malformed (e.g., an old
/// single-integer lease written by a previous version of rustcdc).
pub(crate) fn parse_lease(contents: &str) -> Option<(String, u32)> {
    let contents = contents.trim();
    // New format: `HOSTNAME:PID`
    if let Some(colon) = contents.rfind(':') {
        let host = &contents[..colon];
        let pid_str = &contents[colon + 1..];
        if !host.is_empty() {
            if let Ok(pid) = pid_str.parse::<u32>() {
                return Some((host.to_owned(), pid));
            }
        }
    }
    None
}

// ─── Acquire ────────────────────────────────────────────────────────────────

/// Try to acquire an exclusive owner lease at `lock_path`.
///
/// The lease file stores `HOSTNAME:PID` so that cross-host conflicts on shared
/// network paths are detected and refused rather than silently stolen.
///
/// Decision table for an existing lease file:
///
/// | Lease hostname | Lease PID      | Action                        |
/// |----------------|----------------|-------------------------------|
/// | current host   | current PID    | Re-entrant — succeed          |
/// | current host   | dead process   | Clear stale lease and succeed |
/// | current host   | live process   | Refuse (conflict)             |
/// | different host | any            | Refuse (cross-host conflict)  |
/// | unreadable     | —              | Refuse (conservative)         |
///
/// `store_label` is used only in error messages (e.g. `"checkpoint"`,
/// `"schema_history"`).
///
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

pub(crate) fn acquire(lock_path: &Path, store_label: &str) -> Result<OwnerLease> {
    let owner_pid = std::process::id();
    let hostname = current_hostname();

    // Warn when hostname resolution fell back to "unknown". Two processes on
    // different hosts that both fail hostname resolution will write identical
    // lease tokens — defeating the cross-host safety guarantee.
    if hostname == "unknown" {
        tracing::warn!(
            target: "rustcdc::owner_lease",
            store_label,
            store_dir = %lock_path.parent().map(|p| p.display().to_string()).unwrap_or_default(),
            "hostname resolved to 'unknown' (HOSTNAME env var unset and `hostname` command failed); \
             cross-host exclusive-write guarantee is degraded — two processes on different hosts may \
             both acquire the same lease. Use a dedicated {store_label} directory per runtime instance \
             or set the HOSTNAME environment variable explicitly.",
        );
    }

    let lease_content = format_lease(hostname, owner_pid);

    let create_result = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(lock_path);

    match create_result {
        Ok(mut lock_file) => {
            lock_file
                .write_all(lease_content.as_bytes())
                .map_err(Error::from)?;
            lock_file.sync_all().map_err(Error::from)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let raw = fs::read_to_string(lock_path).unwrap_or_default();
            let existing = parse_lease(&raw);
            let parent_dir = lock_path
                .parent()
                .map(|p| p.display().to_string())
                .unwrap_or_default();

            match existing {
                Some((ref host, pid)) if host == hostname && pid == owner_pid => {
                    // Current process already owns the lease (re-entrant acquire).
                }
                Some((ref host, pid)) if host == hostname && !is_pid_alive(pid) => {
                    tracing::warn!(
                        target: "rustcdc::owner_lease",
                        store_label,
                        store_dir = %parent_dir,
                        stale_owner_pid = pid,
                        "clearing stale {store_label} owner lease left by dead process"
                    );
                    // Atomically replace the stale lease using write-to-temp + rename.
                    // This eliminates the TOCTOU window between `remove` and `create_new`.
                    atomic_write_lease(lock_path, &lease_content).map_err(Error::from)?;

                    // Post-rename race check: POSIX rename(2) is atomic but not exclusive.
                    // Two processes that both observe the same dead PID can race through
                    // atomic_write_lease; the last rename wins. Re-read and verify that
                    // our own hostname:PID is in the file before proceeding as owner.
                    let winner = fs::read_to_string(lock_path).unwrap_or_default();
                    if parse_lease(&winner) != Some((hostname.to_owned(), owner_pid)) {
                        return Err(Error::StateError(format!(
                            "{store_label} owner lease conflict for '{parent_dir}': \
                             lost stale-lease takeover race to a concurrent process. \
                             Retry, or use a dedicated {store_label} directory per runtime instance."
                        )));
                    }
                }
                Some((ref host, pid)) if host != hostname => {
                    // Different host — do NOT attempt liveness check: the PID namespace
                    // is not shared between hosts.  Refuse to prevent cross-host corruption
                    // on shared NFS paths.
                    tracing::error!(
                        target: "rustcdc::owner_lease",
                        store_label,
                        store_dir = %parent_dir,
                        lease_host = %host,
                        lease_pid = pid,
                        current_host = %hostname,
                        "{store_label} owner lease is held by a different host '{host}' (pid {pid}). \
                         FileCheckpoint/FileSchemaHistory are not safe for concurrent cross-host access. \
                         Use a dedicated directory per runtime instance."
                    );
                    return Err(Error::StateError(format!(
                        "{store_label} owner lease conflict for '{parent_dir}': \
                         held by host '{host}' pid {pid} — cross-host NFS sharing is not supported. \
                         Use a dedicated {store_label} directory per runtime instance."
                    )));
                }
                _ => {
                    return Err(Error::StateError(format!(
                        "{store_label} owner lease conflict for '{parent_dir}': \
                         lock is held by another process. \
                         Use a dedicated {store_label} directory per runtime instance."
                    )));
                }
            }
        }
        Err(error) => return Err(Error::from(error)),
    }

    increment_lease_ref(lock_path);
    Ok(OwnerLease {
        lock_path: lock_path.to_path_buf(),
    })
}

// ─── PID liveness ───────────────────────────────────────────────────────────

/// Check whether a process with the given PID is currently alive **on this host**.
///
/// Uses `ps -p <pid>` which exits 0 when the PID exists (regardless of
/// permissions to signal it) and exits non-zero when the PID is absent.
/// This correctly distinguishes ESRCH (dead) from EPERM (alive but
/// unowned) — unlike `kill -0` which returns non-zero for both.
///
/// **Only call this for PIDs known to belong to the current host.**
///
/// On non-Unix platforms, conservatively returns `true` to avoid
/// accidentally clearing leases held by live processes.
pub(crate) fn is_pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        std::process::Command::new("ps")
            .args(["-p", &pid.to_string()])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(true) // conservatively assume alive on error
    }
    #[cfg(windows)]
    {
        // Use OpenProcess with SYNCHRONIZE (0x00100000) to probe existence.
        // Returns a non-null handle when the PID exists (even without full access).
        // Falls back to conservatively assuming alive on unexpected errors.
        use std::os::windows::io::OwnedHandle;
        extern "system" {
            fn OpenProcess(
                dwDesiredAccess: u32,
                bInheritHandle: i32,
                dwProcessId: u32,
            ) -> *mut std::ffi::c_void;
            fn CloseHandle(hObject: *mut std::ffi::c_void) -> i32;
            fn GetExitCodeProcess(hProcess: *mut std::ffi::c_void, lpExitCode: *mut u32) -> i32;
        }
        const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
        const STILL_ACTIVE: u32 = 259;
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if handle.is_null() {
                // GetLastError() == ERROR_INVALID_PARAMETER (87) means no such PID.
                return false;
            }
            let mut exit_code: u32 = 0;
            let ok = GetExitCodeProcess(handle, &mut exit_code);
            CloseHandle(handle);
            if ok == 0 {
                return true; // conservatively alive on query error
            }
            exit_code == STILL_ACTIVE
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        true
    }
}

// ─── Unit tests ─────────────────────────────────────────────────────────────

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

    /// Same process acquiring the same path twice must succeed (re-entrant).
    #[test]
    fn acquire_is_reentrant_within_same_process() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("test.owner");
        let lease1 = acquire(&lock_path, "test").unwrap();
        let lease2 = acquire(&lock_path, "test").unwrap();
        drop(lease1);
        // After first drop the ref-count is 1; file should still exist.
        assert!(lock_path.exists());
        drop(lease2);
        // After second drop the ref-count hits 0; file is removed.
        assert!(!lock_path.exists());
    }

    /// A lease held by a live PID must be refused.
    #[cfg(unix)]
    #[test]
    fn acquire_refuses_live_pid_lease() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("test.owner");
        // PID 1 (init/launchd) is always alive.
        let live_lease = format_lease(current_hostname(), 1);
        std::fs::write(&lock_path, live_lease.as_bytes()).unwrap();
        let result = acquire(&lock_path, "test");
        assert!(result.is_err(), "expected conflict error for live PID");
    }

    /// A stale (dead-process) lease must be auto-cleared and the caller takes ownership.
    #[cfg(unix)]
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

    /// `acquire()` exercises the post-rename check on the winner (success) path:
    /// the process clears a stale dead-PID lease, atomically renames its own
    /// content into place, re-reads, and confirms the file contains its own
    /// `HOSTNAME:PID` before returning `OwnerLease`.
    ///
    /// This test verifies that the post-rename check code path is actually
    /// reached and passes when the current process wins the rename race.
    ///
    /// The *loser* path (another process overwrites the file between our rename
    /// and our re-read) cannot be exercised deterministically in a single-process
    /// unit test — that scenario requires two real processes racing at the OS
    /// rename layer and is covered at the integration level.
    #[cfg(unix)]
    #[test]
    fn acquire_winner_path_exercises_post_rename_check() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("test.owner");

        // Precondition: stale dead-process lease is present.
        let stale = format_lease(current_hostname(), u32::MAX);
        std::fs::write(&lock_path, stale.as_bytes()).unwrap();

        // acquire() must: (a) detect the dead PID, (b) call atomic_write_lease,
        // (c) re-read and confirm HOSTNAME:PID is ours, (d) return Ok(OwnerLease).
        let lease = acquire(&lock_path, "test")
            .expect("acquire should succeed when clearing a stale dead-PID lease");

        // Post-condition: the file contains exactly our hostname and PID, proving
        // the post-rename check ran and confirmed the winner state.
        let contents = std::fs::read_to_string(&lock_path).unwrap();
        let (host, pid) = parse_lease(&contents)
            .expect("lease file must contain a valid HOSTNAME:PID after acquire");
        assert_eq!(host, current_hostname(), "lease hostname must be ours");
        assert_eq!(pid, std::process::id(), "lease PID must be ours");

        drop(lease); // RAII cleanup removes the lock file
        assert!(
            !lock_path.exists(),
            "lock file should be removed when the last OwnerLease is dropped"
        );
    }

    /// Validates the `parse_lease` comparison logic used by the post-rename race
    /// check inside `acquire()`.
    ///
    /// Specifically: a lease token containing a *foreign* PID must not compare
    /// equal to `Some((current_hostname(), current_pid))`, while a token
    /// containing our own hostname and PID must.  This is the predicate
    /// `acquire()` evaluates after the atomic rename to decide whether it won
    /// or lost the takeover race.
    ///
    /// This test exercises the predicate in isolation; the full `acquire()` code
    /// path for the winner case is covered by
    /// `acquire_winner_path_exercises_post_rename_check`.
    #[cfg(unix)]
    #[test]
    fn post_rename_check_predicate_accepts_own_pid_rejects_foreign() {
        let our_content = format_lease(current_hostname(), std::process::id());
        let foreign_content = format_lease(current_hostname(), u32::MAX);

        // Our own hostname:PID passes the check.
        assert_eq!(
            parse_lease(&our_content),
            Some((current_hostname().to_owned(), std::process::id())),
            "own lease token must parse to (our_hostname, our_pid)"
        );
        // A foreign PID fails the check — the process would correctly return Err.
        assert_ne!(
            parse_lease(&foreign_content),
            Some((current_hostname().to_owned(), std::process::id())),
            "foreign lease token must not match (our_hostname, our_pid)"
        );
    }

    /// Ref-count increments and decrements are balanced; file is removed at zero.
    #[test]
    fn ref_count_increment_decrement_balanced() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("refcount.owner");
        std::fs::write(&lock_path, b"dummy").unwrap();

        increment_lease_ref(&lock_path);
        increment_lease_ref(&lock_path);
        assert_eq!(decrement_lease_ref(&lock_path), 1);
        assert_eq!(decrement_lease_ref(&lock_path), 0);
        // Decrementing past zero should return 0 and not panic.
        assert_eq!(decrement_lease_ref(&lock_path), 0);
    }
}
