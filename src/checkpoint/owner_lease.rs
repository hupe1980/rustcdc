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
pub(crate) fn acquire(lock_path: &Path, store_label: &str) -> Result<OwnerLease> {
    let owner_pid = std::process::id();
    let hostname = current_hostname();
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
                    let _ = fs::remove_file(lock_path);
                    let mut lock_file = OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(lock_path)
                        .map_err(Error::from)?;
                    lock_file
                        .write_all(lease_content.as_bytes())
                        .map_err(Error::from)?;
                    lock_file.sync_all().map_err(Error::from)?;
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
    #[cfg(not(unix))]
    {
        let _ = pid;
        true
    }
}

// ─── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
}
