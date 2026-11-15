//! An advisory lock on a file, held for as long as its handle lives.
//!
//! The kernel releases it when its holder exits, however it exits, so a lock taken here is
//! evidence that no live process holds it — evidence that needs no PID, no process table
//! and no external program. `std::fs::File::try_lock` provides it on every platform the
//! server supports, without `unsafe` or a new dependency.
//!
//! What the lock cannot see is a holder whose locks this kernel is not told about: a state
//! directory on a network filesystem that does not propagate them. Callers decide what that
//! means for them; [`TryLock::Unsupported`] covers a filesystem that refuses locks outright.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;

use crate::error::AppError;

/// The outcome of [`try_lock`].
pub(crate) enum TryLock {
    /// Locked, until the [`HeldLock`] is released or dropped.
    Held(HeldLock),
    /// Another open handle holds it: a live process, here or on a filesystem that
    /// propagates locks.
    Busy,
    /// The filesystem refused to lock at all.
    Unsupported(std::io::Error),
}

/// Take the exclusive lock on `path`, creating the file if needed, without waiting.
pub(crate) fn try_lock(path: &Path) -> Result<TryLock, AppError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| {
            AppError::Other(format!(
                "could not open lock file '{}': {error}",
                path.display()
            ))
        })?;
    match file.try_lock() {
        Ok(()) => Ok(TryLock::Held(HeldLock { file })),
        Err(TryLockError::WouldBlock) => Ok(TryLock::Busy),
        Err(TryLockError::Error(error)) => Ok(TryLock::Unsupported(error)),
    }
}

/// A lock this process holds.
///
/// Released explicitly, not by closing the handle: the lock belongs to the open file
/// description, and a child process holds a copy of that from its creation until it execs.
/// Closing ours alone would leave the lock held for that window, refusing a successor
/// that the release was meant to admit.
pub(crate) struct HeldLock {
    file: File,
}

impl HeldLock {
    /// The locked file, for writing diagnostics into it.
    pub(crate) fn file(&mut self) -> &mut File {
        &mut self.file
    }

    /// Where this lock is visible; see [`lock_domain`].
    pub(crate) fn domain(&self) -> Option<LockDomain> {
        lock_domain(&self.file)
    }

    /// Release the lock for every copy of its description.
    pub(crate) fn release(self) -> std::io::Result<()> {
        self.file.unlock()
    }
}

impl Drop for HeldLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Where a lock held on a file is visible: one kernel's lock table for one filesystem.
///
/// Two openers on one kernel that see the same device share that filesystem's locks, so if
/// one of them can take a lock the other held, the other no longer holds it. A different
/// kernel, or a different mount of a network directory, is a different device or a
/// different boot id, and proves nothing either way.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LockDomain {
    /// `/proc/sys/kernel/random/boot_id`: shared by every container on a host, distinct
    /// across hosts and reboots.
    pub(crate) boot_id: String,
    /// The device of the filesystem the lock file lives on.
    pub(crate) dev: u64,
}

/// The domain of a lock held on `file`; `None` where the platform does not expose a boot id
/// or a device number.
fn lock_domain(file: &File) -> Option<LockDomain> {
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())?;
    Some(LockDomain {
        boot_id,
        dev: device_of(file)?,
    })
}

#[cfg(unix)]
fn device_of(file: &File) -> Option<u64> {
    use std::os::unix::fs::MetadataExt as _;
    file.metadata().ok().map(|metadata| metadata.dev())
}

#[cfg(not(unix))]
fn device_of(_: &File) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two handles, even in one process, are two holders: that is what lets the tests run
    /// a second instance in-process.
    #[test]
    fn a_second_handle_is_refused_until_the_first_is_dropped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("probe.lock");

        let first = match try_lock(&path).expect("open") {
            TryLock::Held(lock) => lock,
            _ => panic!("the first handle must lock"),
        };
        assert!(matches!(try_lock(&path).expect("open"), TryLock::Busy));

        drop(first);
        assert!(matches!(try_lock(&path).expect("open"), TryLock::Held(_)));
    }
}
