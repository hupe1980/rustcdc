use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::AppError;

const RECONCILIATION_MARKER_FILE: &str = "checkpoint_txn_reconciliation.json";
const RECOVERED_RECONCILIATION_MARKER_PREFIX: &str = "checkpoint_txn_reconciliation.recovered";
const RECONCILIATION_MARKER_VERSION: u16 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct CheckpointTxnReconciliationMarker {
    marker_version: u16,
    sink: String,
    delivery_contract: String,
    armed_event_count: u64,
    armed_at_unix_ms: u128,
}

#[derive(Debug, Clone)]
pub(super) struct RecoveredMarkerInfo {
    pub(super) parse_ok: bool,
    pub(super) detail: String,
}

pub(super) struct CheckpointTxnReconciler {
    enabled: bool,
    marker_path: PathBuf,
    sink_name: String,
    delivery_contract: String,
}

impl CheckpointTxnReconciler {
    pub(super) fn marker_path_for(state_dir: &Path) -> PathBuf {
        state_dir.join(RECONCILIATION_MARKER_FILE)
    }

    pub(super) fn recover_unresolved_marker(
        state_dir: &Path,
    ) -> Result<Option<RecoveredMarkerInfo>, AppError> {
        let marker_path = Self::marker_path_for(state_dir);
        if !marker_path.exists() {
            return Ok(None);
        }

        let (parse_ok, marker_details) = match fs::read_to_string(&marker_path) {
            Ok(content) => serde_json::from_str::<CheckpointTxnReconciliationMarker>(&content)
                .ok()
                .map(|marker| {
                    (
                        true,
                        format!(
                            "sink={}, contract={}, armed_event_count={}, armed_at_unix_ms={}",
                            marker.sink,
                            marker.delivery_contract,
                            marker.armed_event_count,
                            marker.armed_at_unix_ms,
                        ),
                    )
                })
                .unwrap_or_else(|| (false, "details=unavailable(parse-failed)".to_string())),
            Err(_) => (false, "details=unavailable(read-failed)".to_string()),
        };

        let recovered_marker_path = Self::next_recovered_marker_path(state_dir);
        fs::rename(&marker_path, &recovered_marker_path).map_err(|e| {
            AppError::Other(format!(
                "failed to recover checkpoint-transaction reconciliation marker {} to {}: {e}",
                marker_path.display(),
                recovered_marker_path.display()
            ))
        })?;

        Ok(Some(RecoveredMarkerInfo {
            parse_ok,
            detail: format!(
                "Recovered unresolved checkpoint-transaction reconciliation marker from {} to {}; {}",
                marker_path.display(),
                recovered_marker_path.display(),
                marker_details,
            ),
        }))
    }

    /// Include a UUID v4 nonce to prevent filename collision on
    /// fast restart with PID reuse (same ms + same PID after container recycle).
    fn next_recovered_marker_path(state_dir: &Path) -> PathBuf {
        let now = current_unix_ms();
        let pid = std::process::id();
        let nonce = uuid::Uuid::new_v4().simple();
        state_dir.join(format!(
            "{RECOVERED_RECONCILIATION_MARKER_PREFIX}.{now}.{pid}.{nonce}.json"
        ))
    }

    pub(super) fn new(
        state_dir: PathBuf,
        enabled: bool,
        sink_name: String,
        delivery_contract: String,
    ) -> Self {
        Self {
            enabled,
            marker_path: Self::marker_path_for(&state_dir),
            sink_name,
            delivery_contract,
        }
    }

    pub(super) fn arm(&self, event_count: u64) -> Result<(), AppError> {
        if !self.enabled {
            return Ok(());
        }

        let marker = CheckpointTxnReconciliationMarker {
            marker_version: RECONCILIATION_MARKER_VERSION,
            sink: self.sink_name.clone(),
            delivery_contract: self.delivery_contract.clone(),
            armed_event_count: event_count,
            armed_at_unix_ms: current_unix_ms(),
        };

        let payload = serde_json::to_vec_pretty(&marker).map_err(|e| {
            AppError::Other(format!(
                "failed to serialize checkpoint-transaction reconciliation marker: {e}"
            ))
        })?;

        let mut file = File::create(&self.marker_path).map_err(|e| {
            AppError::Other(format!(
                "failed to create checkpoint-transaction reconciliation marker {}: {e}",
                self.marker_path.display()
            ))
        })?;

        file.write_all(&payload).map_err(|e| {
            AppError::Other(format!(
                "failed to write checkpoint-transaction reconciliation marker {}: {e}",
                self.marker_path.display()
            ))
        })?;

        file.sync_all().map_err(|e| {
            AppError::Other(format!(
                "failed to fsync checkpoint-transaction reconciliation marker {}: {e}",
                self.marker_path.display()
            ))
        })?;

        Ok(())
    }

    pub(super) fn clear(&self) -> Result<(), AppError> {
        if !self.enabled {
            return Ok(());
        }

        match fs::remove_file(&self.marker_path) {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(AppError::Other(format!(
                "failed to remove checkpoint-transaction reconciliation marker {}: {e}",
                self.marker_path.display()
            ))),
        }
    }

    pub(super) fn validate_armed_event_count(
        &self,
        expected_event_count: u64,
    ) -> Result<(), AppError> {
        if !self.enabled {
            return Ok(());
        }

        let content = fs::read_to_string(&self.marker_path).map_err(|e| {
            AppError::Other(format!(
                "failed to read checkpoint-transaction reconciliation marker {} during armed-event-count validation: {e}",
                self.marker_path.display()
            ))
        })?;

        let marker: CheckpointTxnReconciliationMarker =
            serde_json::from_str(&content).map_err(|e| {
                AppError::Other(format!(
                    "failed to parse checkpoint-transaction reconciliation marker {} during armed-event-count validation: {e}",
                    self.marker_path.display()
                ))
            })?;

        if marker.armed_event_count != expected_event_count {
            return Err(AppError::Other(format!(
                "checkpoint-transaction reconciliation marker armed_event_count mismatch: expected {}, found {} in {}",
                expected_event_count,
                marker.armed_event_count,
                self.marker_path.display()
            )));
        }

        Ok(())
    }
}

fn current_unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::CheckpointTxnReconciler;

    #[test]
    fn marker_arm_recovery_and_clear_round_trip() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let reconciler = CheckpointTxnReconciler::new(
            tmp.path().to_path_buf(),
            true,
            "kafka".to_string(),
            "effectively_once".to_string(),
        );

        reconciler.arm(7).expect("arm marker");
        let marker_path = CheckpointTxnReconciler::marker_path_for(tmp.path());
        assert!(marker_path.exists(), "marker file should be created");

        let recovered = CheckpointTxnReconciler::recover_unresolved_marker(tmp.path())
            .expect("startup recovery should pass when marker exists")
            .expect("recovery message");
        assert!(
            recovered
                .detail
                .contains("Recovered unresolved checkpoint-transaction reconciliation marker"),
            "unexpected recovery message: {}",
            recovered.detail
        );
        assert!(
            recovered.parse_ok,
            "expected parse_ok=true for valid marker"
        );
        assert!(
            !marker_path.exists(),
            "marker file should be moved to recovery artifact"
        );

        reconciler.clear().expect("clear marker");
        assert!(!marker_path.exists(), "marker file should be removed");
        CheckpointTxnReconciler::recover_unresolved_marker(tmp.path())
            .expect("startup should pass after marker clear");
    }

    #[test]
    fn malformed_marker_is_recovered_without_blocking_startup() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let marker_path = CheckpointTxnReconciler::marker_path_for(tmp.path());
        std::fs::write(&marker_path, "{").expect("write malformed marker");

        let recovered = CheckpointTxnReconciler::recover_unresolved_marker(tmp.path())
            .expect("startup recovery should succeed")
            .expect("recovery message");
        assert!(
            recovered
                .detail
                .contains("details=unavailable(parse-failed)"),
            "unexpected recovery details: {}",
            recovered.detail
        );
        assert!(
            !recovered.parse_ok,
            "expected parse_ok=false for malformed marker"
        );
        assert!(
            !marker_path.exists(),
            "malformed marker should be moved away"
        );
    }

    #[test]
    fn disabled_reconciler_is_noop() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let reconciler = CheckpointTxnReconciler::new(
            tmp.path().to_path_buf(),
            false,
            "kafka".to_string(),
            "effectively_once".to_string(),
        );

        reconciler.arm(3).expect("disabled arm no-op");
        let marker_path = CheckpointTxnReconciler::marker_path_for(tmp.path());
        assert!(
            !marker_path.exists(),
            "disabled reconciler must not create marker"
        );
        reconciler.clear().expect("disabled clear no-op");
    }

    #[test]
    fn marker_validation_rejects_mismatched_armed_event_count() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let reconciler = CheckpointTxnReconciler::new(
            tmp.path().to_path_buf(),
            true,
            "kafka".to_string(),
            "effectively_once".to_string(),
        );

        reconciler.arm(7).expect("arm marker");
        let err = reconciler
            .validate_armed_event_count(8)
            .expect_err("mismatch must fail closed");
        assert!(
            err.to_string().contains("armed_event_count mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn marker_validation_accepts_matching_armed_event_count() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let reconciler = CheckpointTxnReconciler::new(
            tmp.path().to_path_buf(),
            true,
            "kafka".to_string(),
            "effectively_once".to_string(),
        );

        reconciler.arm(9).expect("arm marker");
        reconciler
            .validate_armed_event_count(9)
            .expect("matching event count should pass");
    }
}
