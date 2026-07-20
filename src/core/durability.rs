//! Durability primitives shared by the file-backed state stores.

use std::path::Path;

use crate::core::Result;

/// fsync the parent directory of `file_path` so a preceding rename is durable.
///
/// Without this, a `rename` that returned `Ok` can still be lost on ext4/xfs if the
/// machine loses power before the directory entry reaches disk — the file the caller
/// just committed simply is not there on restart.
///
/// No-op on non-Unix platforms. Windows cannot open a directory through the ordinary
/// file API: `File::open` on one fails with `ERROR_ACCESS_DENIED` unless the handle is
/// created with `FILE_FLAG_BACKUP_SEMANTICS`, which `std` does not expose. Windows also
/// does not need it — `MoveFileEx` commits the directory entry itself.
pub(crate) fn fsync_parent_directory(file_path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let Some(parent) = file_path.parent() else {
            return Ok(());
        };

        let directory = std::fs::File::open(parent).map_err(crate::core::Error::from)?;
        directory.sync_all().map_err(crate::core::Error::from)?;
    }
    #[cfg(not(unix))]
    let _ = file_path;

    Ok(())
}
