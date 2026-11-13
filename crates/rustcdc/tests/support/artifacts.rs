//! Where cargo puts the binaries a test runs and the files it writes, in every build layout.
//!
//! From Cargo 1.100, test executables live under `<target>/<profile>/build/<pkg>/<hash>/out/`, so
//! walking up from `current_exe()` does not find the profile directory. `CARGO_TARGET_TMPDIR` is
//! `<target>/tmp` in both layouts; final binaries and examples are in `<target>/<profile>/`.
#![allow(dead_code)]

use std::path::PathBuf;

/// The target directory, honouring `CARGO_TARGET_DIR`.
pub fn target_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .parent()
        .expect("CARGO_TARGET_TMPDIR is <target>/tmp")
        .to_path_buf()
}

/// Where this build's binaries and examples land: `<target>/debug` or `<target>/release`.
pub fn profile_dir() -> PathBuf {
    target_dir().join(if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    })
}
