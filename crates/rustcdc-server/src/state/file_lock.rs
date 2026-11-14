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
    /// Locked. The lock lasts as long as this handle.
    Held(File),
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
        Ok(()) => Ok(TryLock::Held(file)),
        Err(TryLockError::WouldBlock) => Ok(TryLock::Busy),
        Err(TryLockError::Error(error)) => Ok(TryLock::Unsupported(error)),
    }
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
            TryLock::Held(file) => file,
            _ => panic!("the first handle must lock"),
        };
        assert!(matches!(try_lock(&path).expect("open"), TryLock::Busy));

        drop(first);
        assert!(matches!(try_lock(&path).expect("open"), TryLock::Held(_)));
    }
}
