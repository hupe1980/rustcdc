use std::{fs, path::Path};

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct LatencySummary {
    pub profile: &'static str,
    pub rows_inserted: u64,
    pub events_committed: u64,
    pub batches: usize,
    pub poll_latency_ms_p50: f64,
    pub poll_latency_ms_p95: f64,
    pub poll_latency_ms_p99: f64,
    pub commit_latency_ms_p50: f64,
    pub commit_latency_ms_p95: f64,
    pub commit_latency_ms_p99: f64,
    pub batch_size_p50: f64,
    pub batch_size_p95: f64,
    pub batch_size_p99: f64,
    pub end_to_end_ms: u128,
}

pub fn percentile(values: &[f64], pct: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| {
        left.partial_cmp(right)
            .expect("latency values should be finite")
    });

    let rank = ((pct / 100.0) * ((sorted.len() - 1) as f64)).round() as usize;
    sorted[rank]
}

pub fn write_latency_artifacts(prefix: &str, summary: &LatencySummary) -> cdc_rs::Result<()> {
    let target_dir = Path::new("target");
    fs::create_dir_all(target_dir).map_err(cdc_rs::Error::IoError)?;

    let json_path = target_dir.join(format!("{prefix}-latency-evidence.json"));
    let json = serde_json::to_string_pretty(summary)
        .map_err(|error| cdc_rs::Error::SerializationError(error.to_string()))?;
    fs::write(&json_path, json).map_err(cdc_rs::Error::IoError)?;

    let markdown_path = target_dir.join(format!("{prefix}-latency-evidence.md"));
    let markdown = format!(
        "# {} Latency Evidence\n\n- Profile: {}\n- Rows inserted: {}\n- Events committed: {}\n- Batches: {}\n- End-to-end (ms): {}\n\n## Poll Latency (ms)\n\n- p50: {:.3}\n- p95: {:.3}\n- p99: {:.3}\n\n## Commit Latency (ms)\n\n- p50: {:.3}\n- p95: {:.3}\n- p99: {:.3}\n\n## Batch Size\n\n- p50: {:.1}\n- p95: {:.1}\n- p99: {:.1}\n",
        prefix.to_ascii_uppercase(),
        summary.profile,
        summary.rows_inserted,
        summary.events_committed,
        summary.batches,
        summary.end_to_end_ms,
        summary.poll_latency_ms_p50,
        summary.poll_latency_ms_p95,
        summary.poll_latency_ms_p99,
        summary.commit_latency_ms_p50,
        summary.commit_latency_ms_p95,
        summary.commit_latency_ms_p99,
        summary.batch_size_p50,
        summary.batch_size_p95,
        summary.batch_size_p99,
    );
    fs::write(&markdown_path, markdown).map_err(cdc_rs::Error::IoError)?;

    Ok(())
}
