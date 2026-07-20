use std::path::Path;
use std::time::SystemTime;

use rustcdc::checkpoint::FileCheckpoint;

use crate::error::AppError;

use crate::state::CheckpointAgeSource;

pub(super) async fn build_checkpoint(
    state_dir: &Path,
) -> Result<(FileCheckpoint, CheckpointAgeSource), AppError> {
    std::fs::create_dir_all(state_dir)?;

    let checkpoint_dir = state_dir.join("checkpoint");
    std::fs::create_dir_all(&checkpoint_dir)?;

    let checkpoint = FileCheckpoint::new(checkpoint_dir.clone());
    let schema_history_path = state_dir.join("schema_history");

    let age_source = CheckpointAgeSource::LocalFs {
        checkpoint_dir,
        schema_history_path,
    };

    Ok((checkpoint, age_source))
}

/// Fallback age source for OpenDAL backends: reports LocalFs age against the
/// same directory (will typically return `None` since no files are written).
pub(super) fn fallback_age_source(state_dir: &Path) -> CheckpointAgeSource {
    let checkpoint_dir = state_dir.join("checkpoint");
    let schema_history_path = state_dir.join("schema_history");
    CheckpointAgeSource::LocalFs {
        checkpoint_dir,
        schema_history_path,
    }
}

/// Returns the age in seconds of the most recently modified checkpoint or
/// schema-history artifact in the local filesystem state directory.
pub(crate) fn age_seconds(checkpoint_dir: &Path, schema_history_path: &Path) -> Option<f64> {
    let newest = newest_artifact_modified_at(checkpoint_dir, schema_history_path)?;
    let age = SystemTime::now().duration_since(newest).ok()?;
    Some(age.as_secs_f64())
}

fn newest_artifact_modified_at(
    checkpoint_dir: &Path,
    schema_history_path: &Path,
) -> Option<SystemTime> {
    let mut newest = SystemTime::UNIX_EPOCH;

    if let Ok(entries) = std::fs::read_dir(checkpoint_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !name.starts_with("checkpoint_") || !name.ends_with(".json") {
                continue;
            }
            let modified = entry
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            if modified > newest {
                newest = modified;
            }
        }
    }

    if let Ok(metadata) = std::fs::metadata(schema_history_path) {
        if let Ok(modified) = metadata.modified() {
            if modified > newest {
                newest = modified;
            }
        }
    }

    if newest == SystemTime::UNIX_EPOCH {
        None
    } else {
        Some(newest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn age_seconds_includes_schema_history_when_checkpoint_dir_is_empty() {
        let temp = tempdir().expect("tempdir");
        let checkpoint_dir = temp.path().join("checkpoint");
        std::fs::create_dir_all(&checkpoint_dir).expect("checkpoint dir");
        let schema_history_path = temp.path().join("schema_history");
        std::fs::write(&schema_history_path, b"[]").expect("schema history file");

        let age = age_seconds(&checkpoint_dir, &schema_history_path);
        assert!(age.is_some(), "schema history must contribute to freshness");
    }
}
