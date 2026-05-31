#[cfg(feature = "sqlserver")]
use std::{path::PathBuf, time::Duration};

#[cfg(feature = "sqlserver")]
use rustcdc::{
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory, CdcRuntime, RuntimeConfig,
    RuntimeSourceConfig, SqlServerSourceConfig, TransportConfig,
};
#[cfg(feature = "sqlserver")]
use xtask::worker_common::{
    event_ids, optional_bool_env, optional_snapshot_tables, required_env, required_u16_env,
    write_marker_atomic,
};

#[cfg(feature = "sqlserver")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> rustcdc::Result<()> {
    let host = required_env("CDC_RS_WORKER_HOST")?;
    let port = required_u16_env("CDC_RS_WORKER_PORT")?;
    let user = required_env("CDC_RS_WORKER_USER")?;
    let password = required_env("CDC_RS_WORKER_PASSWORD")?;
    let database = required_env("CDC_RS_WORKER_DATABASE")?;
    let checkpoint_dir = PathBuf::from(required_env("CDC_RS_WORKER_CHECKPOINT_DIR")?);
    let marker_file = PathBuf::from(required_env("CDC_RS_WORKER_MARKER_FILE")?);
    let ack_first_batch = optional_bool_env("CDC_RS_WORKER_ACK_FIRST_BATCH");
    let snapshot_tables = optional_snapshot_tables();

    let source_cfg = SqlServerSourceConfig {
        host,
        port,
        user,
        password: password.into(),
        database,
        instance_name: None,
        transport: TransportConfig::plaintext(),
        conn_timeout_secs: 30,
        cdc_enabled: true,
        cdc_schema: "cdc".to_string(),
        prereq_pool_size: 2,
        stream_poll_interval_ms: 150,
        max_events_per_poll: 5_000,
        ..Default::default()
    };

    let mut runtime_config = RuntimeConfig::new(
        RuntimeSourceConfig::SqlServer(source_cfg),
        FileCheckpoint::new(&checkpoint_dir),
        InMemorySchemaHistory::default(),
    )
    .with_max_buffer_size(256)
    .with_max_poll_wait_ms(150);
    if !snapshot_tables.is_empty() {
        runtime_config = runtime_config.with_snapshot_tables(snapshot_tables);
    }

    let mut runtime = CdcRuntime::new(runtime_config)?;

    runtime.start().await?;

    for _ in 0..120 {
        let batch = runtime.poll_event_batch().await?;
        if !batch.is_empty() {
            if ack_first_batch {
                let token = batch
                    .ack_token()
                    .ok_or_else(|| rustcdc::Error::StateError("missing ack token".into()))?;
                runtime.commit_ack(token).await?;
            }
            let ids = event_ids(&batch).join(",");
            let payload = format!(
                "events={}\nacked={}\nids={}\n",
                batch.len(),
                if ack_first_batch { 1 } else { 0 },
                ids
            );
            write_marker_atomic(&marker_file, &payload)?;
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
    }

    Err(rustcdc::Error::TimeoutError(
        "worker timed out waiting for stream events".into(),
    ))
}

#[cfg(not(feature = "sqlserver"))]
fn main() {
    eprintln!("sqlserver_crash_worker requires the 'sqlserver' feature");
}
