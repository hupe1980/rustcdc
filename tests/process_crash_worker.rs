use std::{
    collections::HashMap,
    path::Path,
    path::PathBuf,
    process::Command,
    sync::{Mutex, OnceLock},
};

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

fn build_xtask_worker(bin: &str, feature: &str) -> rustcdc::Result<()> {
    let status = Command::new("cargo")
        .args(["build", "-p", "xtask", "--bin", bin, "--features", feature])
        .status()
        .map_err(rustcdc::Error::IoError)?;

    if status.success() {
        Ok(())
    } else {
        Err(rustcdc::Error::StateError(format!(
            "failed to build {bin} in xtask crate"
        )))
    }
}

pub fn resolve_xtask_worker_bin(
    bin: &str,
    feature: &str,
    cargo_bin_env_var: &str,
    not_found_hint: &str,
) -> rustcdc::Result<PathBuf> {
    if let Some(path) = cache_get(bin)? {
        return Ok(path);
    }

    if let Ok(path) = std::env::var(cargo_bin_env_var) {
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

        build_xtask_worker(bin, feature)?;
        if candidate.exists() {
            cache_set(bin, &candidate)?;
            return Ok(candidate);
        }
    }

    Err(rustcdc::Error::StateError(not_found_hint.to_string()))
}
