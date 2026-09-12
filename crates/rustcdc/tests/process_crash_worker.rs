use std::{
    collections::HashMap,
    path::Path,
    path::PathBuf,
    process::Command,
    sync::{Mutex, OnceLock},
};

/// The crate that declares the process-crash helper binaries.
const CRASH_WORKER_PACKAGE: &str = "crash-workers";

static WORKER_BIN_CACHE: OnceLock<Mutex<HashMap<String, PathBuf>>> = OnceLock::new();

fn cache() -> &'static Mutex<HashMap<String, PathBuf>> {
    WORKER_BIN_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cache_get(bin: &str) -> rustcdc::Result<Option<PathBuf>> {
    let cache = cache().lock().map_err(|_| {
        rustcdc::Error::StateError("process crash worker cache lock poisoned".into())
    })?;
    Ok(cache.get(bin).cloned())
}

fn cache_set(bin: &str, path: &Path) -> rustcdc::Result<()> {
    let mut cache = cache().lock().map_err(|_| {
        rustcdc::Error::StateError("process crash worker cache lock poisoned".into())
    })?;
    cache.insert(bin.to_string(), path.to_path_buf());
    Ok(())
}

fn build_crash_worker(bin: &str, feature: &str) -> rustcdc::Result<()> {
    let status = Command::new("cargo")
        .args([
            "build",
            "-p",
            CRASH_WORKER_PACKAGE,
            "--bin",
            bin,
            "--features",
            feature,
        ])
        .status()
        .map_err(rustcdc::Error::IoError)?;

    if status.success() {
        Ok(())
    } else {
        Err(rustcdc::Error::StateError(format!(
            "failed to build {bin} in the {CRASH_WORKER_PACKAGE} crate"
        )))
    }
}

/// Locate the helper binary a process-crash suite kills, building it if absent.
///
/// The caller passes only the binary and the feature that gates it. Everything else is
/// derived: the four call sites used to spell out the `cargo build` command in a
/// not-found hint, and every one of those hints still named `-p xtask` long after the
/// workers moved to their own crate — telling a developer to run a command that fails.
pub fn resolve_crash_worker_bin(bin: &str, feature: &str) -> rustcdc::Result<PathBuf> {
    if let Some(path) = cache_get(bin)? {
        return Ok(path);
    }

    // Cargo only sets `CARGO_BIN_EXE_<name>` for integration tests in the package that
    // declares the binary, which this is not, so this is an explicit override rather
    // than something cargo fills in: point it at a prebuilt worker to skip the build.
    if let Ok(path) = std::env::var(format!("CARGO_BIN_EXE_{bin}")) {
        let path = PathBuf::from(path);
        if path.exists() {
            cache_set(bin, &path)?;
            return Ok(path);
        }
    }

    let test_exe = std::env::current_exe().map_err(rustcdc::Error::IoError)?;
    if let Some(debug_dir) = test_exe.parent().and_then(|deps| deps.parent()) {
        let candidate = debug_dir.join(bin);
        if candidate.exists() {
            cache_set(bin, &candidate)?;
            return Ok(candidate);
        }

        build_crash_worker(bin, feature)?;
        if candidate.exists() {
            cache_set(bin, &candidate)?;
            return Ok(candidate);
        }
    }

    Err(rustcdc::Error::StateError(format!(
        "{bin} not found and could not be built; run \
         `cargo build -p {CRASH_WORKER_PACKAGE} --bin {bin} --features {feature}`"
    )))
}
