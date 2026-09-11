use rustcdc::core::{Event, Operation, SourceMetadata};
use serde_json::json;
use std::path::Path;

use crate::pipeline::{binding::build_router, transform};
use crate::{
    cli::{CheckpointParityMode, DryRunArgs},
    config::{self, AppConfig},
    error::AppError,
};
use rustcdc::sink::SinkAdapter;

use crate::runtime::batch;

pub async fn execute(args: DryRunArgs, config_path: Option<&Path>) -> Result<(), AppError> {
    let config_path = config_path.ok_or(crate::error::ConfigError::NoConfigFile)?;

    let app_config = config::load(config_path)?;
    dry_run_pipeline(app_config, args.event_count, args.checkpoint_parity_mode).await
}

async fn dry_run_pipeline(
    app_config: AppConfig,
    event_limit: usize,
    checkpoint_parity_mode: CheckpointParityMode,
) -> Result<(), AppError> {
    let mut sink = build_router(&app_config).await?.router;

    // Fail fast if the requested parity mode is incompatible with the delivery
    // contract and this sink's capabilities.
    batch::validate_parity_contract(checkpoint_parity_mode, &sink, app_config.delivery_contract)?;

    let checkpoint_parity_plan = batch::checkpoint_parity_plan(checkpoint_parity_mode, &sink);
    let transform_pipeline = transform::TransformPipeline::from_config(
        app_config.pipeline.transform_runtime.clone(),
        app_config.pipeline.transforms.clone(),
    )?;

    tracing::info!(event_limit, "dry-run starting (synthetic events)");

    let stats = batch::process_batch_events_with_optional_checkpoint_barrier(
        &mut sink,
        (0..event_limit).map(synthetic_event),
        &transform_pipeline,
        app_config.runtime.prepare_parallelism,
        app_config.runtime.sink_flush_interval_events,
        app_config.runtime.sink_delivery_queue_capacity,
        app_config.runtime.sink_send_timeout_ms,
        app_config.runtime.sink_flush_timeout_ms,
        checkpoint_parity_mode,
        // No quarantine for a one-shot diagnostic: a failure here should be seen.
        None,
    )
    .await?;

    let events_seen = stats.delivery.sink_send_ops_total as usize;
    sink.close().await?;
    tracing::info!(
        events_seen,
        checkpoint_parity_mode = ?checkpoint_parity_mode,
        checkpoint_parity_requested = checkpoint_parity_plan.requested,
        checkpoint_parity_effective = checkpoint_parity_plan.effective,
        runtime_ack_commit_executed = false,
        "dry-run complete"
    );
    Ok(())
}

fn synthetic_event(idx: usize) -> Event {
    let row_id = idx as i64 + 1;
    let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;

    Event::builder("dry_run_events", Operation::Insert)
        .after(json!({
            "id": row_id,
            "name": format!("dry-run-{row_id}"),
        }))
        .source(SourceMetadata::new(
            "dry-run",
            format!("synthetic-{row_id}"),
            now_ms,
        ))
        .ts(now_ms)
        .schema("public")
        .primary_key(["id"])
        .build()
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::cli::{CheckpointParityMode, DryRunArgs};
    use std::path::Path;
    use tempfile::tempdir;

    fn write_file_jsonl_dry_run_config(
        path: &Path,
        state_dir: &Path,
        output_path: &Path,
        max_event_bytes: usize,
    ) {
        let config = format!(
            r#"api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "cdc"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "file_jsonl"
path = "{}"

[state]
dir = "{}"

[runtime]
max_event_bytes = {}
"#,
            output_path.display(),
            state_dir.display(),
            max_event_bytes,
        );

        std::fs::write(path, config).expect("write file_jsonl dry-run config");
    }

    #[tokio::test]
    async fn dry_run_enforces_runtime_max_event_bytes() {
        let dir = tempdir().expect("tempdir");
        let config_path = dir.path().join("cdc.toml");
        let state_dir = dir.path().join("state");
        let output_path = dir.path().join("dry-run-output.jsonl");
        std::fs::create_dir_all(&state_dir).expect("create state dir");

        write_file_jsonl_dry_run_config(&config_path, &state_dir, &output_path, 16);

        let err = execute(
            DryRunArgs {
                event_count: 1,
                checkpoint_parity_mode: CheckpointParityMode::Enabled,
            },
            Some(&config_path),
        )
        .await
        .expect_err("oversized dry-run event must fail closed");

        assert!(
            err.to_string().contains("runtime.max_event_bytes"),
            "unexpected error: {err}"
        );
    }
}
