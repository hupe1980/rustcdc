#[cfg(feature = "mysql")]
use std::{env, fs, path::PathBuf, time::Duration};

#[cfg(feature = "mysql")]
use rustcdc::{
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory, CdcRuntime,
    MysqlSourceConfig, RuntimeConfig, RuntimeSourceConfig, TransportConfig,
};

#[cfg(feature = "mysql")]
fn required_env(name: &str) -> rustcdc::Result<String> {
    env::var(name).map_err(|_| rustcdc::Error::ConfigError(format!("missing env var {name}")))
}

#[cfg(feature = "mysql")]
fn required_u16_env(name: &str) -> rustcdc::Result<u16> {
    let value = required_env(name)?;
    value
        .parse::<u16>()
        .map_err(|error| rustcdc::Error::ConfigError(format!("invalid {name}: {error}")))
}

#[cfg(feature = "mysql")]
fn required_u32_env(name: &str) -> rustcdc::Result<u32> {
    let value = required_env(name)?;
    value
        .parse::<u32>()
        .map_err(|error| rustcdc::Error::ConfigError(format!("invalid {name}: {error}")))
}

#[cfg(feature = "mysql")]
fn optional_bool_env(name: &str) -> bool {
    matches!(
        env::var(name).ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

#[cfg(feature = "mysql")]
fn optional_snapshot_tables() -> Vec<String> {
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

#[cfg(feature = "mysql")]
fn event_ids(batch: &rustcdc::EventBatch) -> Vec<String> {
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

#[cfg(feature = "mysql")]
fn write_marker_atomic(path: &std::path::Path, payload: &str) -> rustcdc::Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, payload).map_err(rustcdc::Error::IoError)?;
    fs::rename(&tmp, path).map_err(rustcdc::Error::IoError)
}

#[cfg(feature = "mysql")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> rustcdc::Result<()> {
    let host = required_env("CDC_RS_WORKER_HOST")?;
    let port = required_u16_env("CDC_RS_WORKER_PORT")?;
    let user = required_env("CDC_RS_WORKER_USER")?;
    let password = required_env("CDC_RS_WORKER_PASSWORD")?;
    let database = required_env("CDC_RS_WORKER_DATABASE")?;
    let server_id = required_u32_env("CDC_RS_WORKER_SERVER_ID")?;
    let checkpoint_dir = PathBuf::from(required_env("CDC_RS_WORKER_CHECKPOINT_DIR")?);
    let marker_file = PathBuf::from(required_env("CDC_RS_WORKER_MARKER_FILE")?);
    let ack_first_batch = optional_bool_env("CDC_RS_WORKER_ACK_FIRST_BATCH");
    let snapshot_tables = optional_snapshot_tables();

    let source_cfg = MysqlSourceConfig {
        host,
        port,
        user,
        password: password.into(),
        database,
        server_id,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
    };

    let mut runtime_config = RuntimeConfig::new(
        RuntimeSourceConfig::Mysql(source_cfg),
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

    for _ in 0..80 {
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

#[cfg(not(feature = "mysql"))]
fn main() {
    eprintln!("mysql_crash_worker requires the 'mysql' feature");
}
