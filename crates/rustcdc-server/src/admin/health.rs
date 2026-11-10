//! Publishing the runtime's health between committed batches.
//!
//! `record_batch` latches the verdict only after a batch commits, so while a batch is held
//! for retry nothing else would publish one.

use super::AdminState;
use crate::runtime::metrics::RuntimeMetricsSnapshot;

impl AdminState {
    /// Publish the runtime's verdict and metrics while a batch is being retried.
    ///
    /// `record_batch` runs only after a batch commits, so during a delivery retry the
    /// published verdict froze at the last good batch — `idle` for the whole outage. This
    /// latches the current one without counting a batch or resetting error state.
    pub(crate) async fn record_runtime_health(&self, runtime_metrics: RuntimeMetricsSnapshot) {
        let mut d = self.data.write().await;
        d.record_health_verdict(&runtime_metrics.runtime_admin.health);
        d.runtime_metrics = runtime_metrics.render_prometheus();
    }
}

#[cfg(test)]
impl AdminState {
    /// The latched verdict and stall cause, as `/livez` and `/readyz` read them.
    pub(crate) async fn health_snapshot_for_tests(&self) -> (Option<String>, Option<String>) {
        let d = self.data.read().await;
        (d.health_verdict.clone(), d.health_stall_cause.clone())
    }

    /// Batches counted as processed, as `/status` reports them.
    pub(crate) async fn batches_processed_for_tests(&self) -> u64 {
        self.data.read().await.batches_processed
    }
}
