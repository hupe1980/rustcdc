pub mod worker_common {
    use std::{env, fs, path::Path};

    pub fn required_env(name: &str) -> rustcdc::Result<String> {
        env::var(name).map_err(|_| rustcdc::Error::ConfigError(format!("missing env var {name}")))
    }

    pub fn required_u16_env(name: &str) -> rustcdc::Result<u16> {
        let value = required_env(name)?;
        value
            .parse::<u16>()
            .map_err(|error| rustcdc::Error::ConfigError(format!("invalid {name}: {error}")))
    }

    pub fn required_u32_env(name: &str) -> rustcdc::Result<u32> {
        let value = required_env(name)?;
        value
            .parse::<u32>()
            .map_err(|error| rustcdc::Error::ConfigError(format!("invalid {name}: {error}")))
    }

    pub fn optional_bool_env(name: &str) -> bool {
        matches!(
            env::var(name).ok().as_deref(),
            Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
        )
    }

    pub fn optional_snapshot_tables() -> Vec<String> {
        env::var("CDC_RS_WORKER_SNAPSHOT_TABLES")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|table| !table.is_empty())
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    pub fn event_ids(batch: &rustcdc::EventBatch) -> Vec<String> {
        batch
            .events()
            .iter()
            .filter_map(|event| {
                event
                    .after
                    .as_ref()
                    .and_then(|after| after.get("id"))
                    .map(|id| id.to_string())
            })
            .collect()
    }

    pub fn write_marker_atomic(path: &Path, payload: &str) -> rustcdc::Result<()> {
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, payload).map_err(rustcdc::Error::IoError)?;
        fs::rename(&tmp, path).map_err(rustcdc::Error::IoError)
    }
}
