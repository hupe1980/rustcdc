#![cfg(feature = "sqlserver")]

use std::{
    path::{Path, PathBuf},
    process::{Child, Command},
    time::Duration,
};

use rustcdc::{
    checkpoint::{Checkpoint, FileCheckpoint, GenericOffset},
    core::Operation,
    schema_history::InMemorySchemaHistory,
    CdcRuntime, RuntimeConfig, RuntimeSourceConfig,
};
#[cfg(feature = "encryption")]
use rustcdc::{
    core::SecretString,
    transform::{MaskHashConfig, MaskHashTransform, MaskRule},
};

mod process_crash_marker;
#[path = "sqlserver_testkit.rs"]
mod sqlserver_testkit;
use process_crash_marker::{read_worker_batch_len, read_worker_marker, wait_for_marker};

type SqlClient = tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>;

async fn sql_exec(client: &mut SqlClient, sql: &str) -> rustcdc::Result<()> {
    client
        .execute(sql, &[])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    Ok(())
}

async fn query_min_lsn_hex(
    client: &mut SqlClient,
    capture_instance: &str,
) -> rustcdc::Result<String> {
    let rows = client
        .query("SELECT sys.fn_cdc_get_min_lsn(@P1)", &[&capture_instance])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .into_first_result()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    Ok(rows
        .into_iter()
        .next()
        .and_then(|row| row.get::<&[u8], _>(0).map(lsn_bytes_to_hex))
        .unwrap_or_else(|| "0x00000000000000000000".to_string()))
}

#[tokio::test]
async fn runtime_sqlserver_process_kill_replays_uncommitted_batch() -> rustcdc::Result<()> {
    run_sqlserver_process_kill_replay_scenario(false).await
}

#[tokio::test]
async fn runtime_sqlserver_process_kill_resumes_snapshot_after_committed_batch(
) -> rustcdc::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver snapshot crash-resume integration test") {
        return Ok(());
    }

    let container = sqlserver_testkit::start_sqlserver_container("2019-latest").await?;
    let (host, port) = sqlserver_testkit::host_and_port(&container).await?;

    let mut admin =
        sqlserver_testkit::connect_admin_with_retry(&host, port, 40, Duration::from_secs(2))
            .await?;

    sql_exec(
        &mut admin,
        "IF DB_ID('rustcdc_crash_snapshot') IS NULL CREATE DATABASE rustcdc_crash_snapshot",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_crash_snapshot; IF OBJECT_ID('dbo.runtime_crash_snapshot_users', 'U') IS NULL CREATE TABLE dbo.runtime_crash_snapshot_users (id INT NOT NULL PRIMARY KEY, payload NVARCHAR(100) NOT NULL)",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_crash_snapshot; DELETE FROM dbo.runtime_crash_snapshot_users",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_crash_snapshot; IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0 EXEC sys.sp_cdc_enable_db",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_crash_snapshot; IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.runtime_crash_snapshot_users')) EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='runtime_crash_snapshot_users', @role_name=NULL, @supports_net_changes=0",
    )
    .await?;

    let total_rows = 600;
    for id in 1..=total_rows {
        let payload = format!("payload-{id}");
        admin
            .execute(
                "USE rustcdc_crash_snapshot; INSERT INTO dbo.runtime_crash_snapshot_users (id, payload) VALUES (@P1, @P2)",
                &[&id, &payload],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    sql_exec(
        &mut admin,
        "USE rustcdc_crash_snapshot; EXEC sys.sp_cdc_scan",
    )
    .await?;

    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;
    let marker_file = checkpoint_dir.path().join("worker-polled.marker");

    let mut worker = spawn_crash_worker(
        &host,
        port,
        checkpoint_dir.path(),
        &marker_file,
        "rustcdc_crash_snapshot",
        Some("dbo.runtime_crash_snapshot_users"),
        true,
    )?;

    wait_for_marker(&marker_file, Duration::from_secs(60))?;
    let marker = read_worker_marker(&marker_file)?;
    assert!(marker.acked, "worker should ack first snapshot batch");
    assert!(!marker.ids.is_empty(), "worker should record acked ids");

    worker.kill().map_err(rustcdc::Error::IoError)?;
    let _ = worker.wait().map_err(rustcdc::Error::IoError)?;

    let reader_after_worker = FileCheckpoint::new(checkpoint_dir.path());
    let saved = reader_after_worker.load().await?.ok_or_else(|| {
        rustcdc::Error::StateError("checkpoint should exist after worker ack".into())
    })?;
    assert_eq!(saved.source_type(), "sqlserver_snapshot");
    assert_eq!(
        reader_after_worker.get_committed_count().await?,
        marker.events as u64
    );

    let source_cfg =
        sqlserver_testkit::source_config(host, port, "rustcdc_crash_snapshot".to_string(), 30);

    let mut runtime = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::SqlServer(source_cfg),
            FileCheckpoint::new(checkpoint_dir.path()),
            InMemorySchemaHistory::default(),
        )
        .with_snapshot_tables(vec!["dbo.runtime_crash_snapshot_users".to_string()])
        .with_max_buffer_size(256)
        .with_max_poll_wait_ms(150),
    )?;

    runtime.start().await?;

    let mut resumed_snapshot_ids = std::collections::HashSet::new();
    for _ in 0..120 {
        let batch = runtime.poll_event_batch().await?;
        if batch.is_empty() {
            continue;
        }

        for event in batch.events() {
            if event.op != Operation::Read {
                continue;
            }
            let id = event
                .after
                .as_ref()
                .and_then(|after| after.get("id"))
                .and_then(|value| value.as_i64())
                .ok_or_else(|| rustcdc::Error::StateError("snapshot event id missing".into()))?;
            resumed_snapshot_ids.insert(id.to_string());
        }

        runtime
            .commit_ack(batch.ack_token().expect("ack token should exist"))
            .await?;
        if resumed_snapshot_ids.len() >= (total_rows as usize).saturating_sub(marker.ids.len()) {
            break;
        }
    }

    assert!(
        marker.ids.is_disjoint(&resumed_snapshot_ids),
        "resumed snapshot should not replay ids already commit-acked before crash"
    );
    assert!(
        resumed_snapshot_ids.len() >= (total_rows as usize).saturating_sub(marker.ids.len()),
        "expected resumed snapshot to deliver remaining rows"
    );

    Ok(())
}

#[cfg(feature = "encryption")]
#[tokio::test]
async fn runtime_sqlserver_process_kill_replays_uncommitted_batch_with_encryption_transform(
) -> rustcdc::Result<()> {
    run_sqlserver_process_kill_replay_scenario(true).await
}

async fn run_sqlserver_process_kill_replay_scenario(
    _enable_encryption_transform: bool,
) -> rustcdc::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver process crash integration test") {
        return Ok(());
    }

    let container = sqlserver_testkit::start_sqlserver_container("2019-latest").await?;
    let (host, port) = sqlserver_testkit::host_and_port(&container).await?;

    let mut admin =
        sqlserver_testkit::connect_admin_with_retry(&host, port, 40, Duration::from_secs(2))
            .await?;

    sql_exec(
        &mut admin,
        "IF DB_ID('rustcdc_crash') IS NULL CREATE DATABASE rustcdc_crash",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_crash; IF OBJECT_ID('dbo.runtime_crash_users', 'U') IS NULL CREATE TABLE dbo.runtime_crash_users (id INT NOT NULL PRIMARY KEY, payload NVARCHAR(100) NOT NULL)",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_crash; DELETE FROM dbo.runtime_crash_users",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_crash; IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0 EXEC sys.sp_cdc_enable_db",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_crash; IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.runtime_crash_users')) EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='runtime_crash_users', @role_name=NULL, @supports_net_changes=0",
    )
    .await?;

    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;
    let marker_file = checkpoint_dir.path().join("worker-polled.marker");

    let mut seed_checkpoint = FileCheckpoint::new(checkpoint_dir.path());
    tokio::time::sleep(Duration::from_secs(3)).await;
    let baseline_lsn_hex = query_min_lsn_hex(&mut admin, "dbo_runtime_crash_users").await?;
    seed_checkpoint
        .save(
            &GenericOffset::new(
                "sqlserver",
                serde_json::to_vec(&baseline_lsn_hex)
                    .map_err(|error| rustcdc::Error::SerializationError(error.to_string()))?,
            ),
            0,
        )
        .await?;

    for id in 1..=100 {
        let payload = format!("payload-{id}");
        admin
            .execute(
                "USE rustcdc_crash; INSERT INTO dbo.runtime_crash_users (id, payload) VALUES (@P1, @P2)",
                &[&id, &payload],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    sql_exec(&mut admin, "USE rustcdc_crash; EXEC sys.sp_cdc_scan").await?;

    let mut worker = spawn_crash_worker(
        &host,
        port,
        checkpoint_dir.path(),
        &marker_file,
        "rustcdc_crash",
        None,
        false,
    )?;

    wait_for_marker(&marker_file, Duration::from_secs(60))?;
    let worker_batch_len = read_worker_batch_len(&marker_file)?;

    worker.kill().map_err(rustcdc::Error::IoError)?;
    let _ = worker.wait().map_err(rustcdc::Error::IoError)?;

    let reader_before = FileCheckpoint::new(checkpoint_dir.path());
    assert_eq!(reader_before.get_committed_count().await?, 0);

    let source_cfg = sqlserver_testkit::source_config(host, port, "rustcdc_crash".to_string(), 30);

    let mut runtime = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::SqlServer(source_cfg),
            FileCheckpoint::new(checkpoint_dir.path()),
            InMemorySchemaHistory::default(),
        )
        .with_max_buffer_size(256)
        .with_max_poll_wait_ms(150),
    )?;

    #[cfg(feature = "encryption")]
    if _enable_encryption_transform {
        use ahash::AHashMap as HashMap;

        let mut mask_rules = HashMap::new();
        mask_rules.insert(
            "payload".to_string(),
            MaskRule::Encrypt(SecretString::new("integration-key")),
        );
        runtime.add_transform(Box::new(MaskHashTransform::new(MaskHashConfig {
            mask_rules,
            default_rule: MaskRule::Hash,
        })));
    }

    runtime.start().await?;
    let replay_batch = poll_until_batch_at_least(&mut runtime, 1, 80).await?;
    assert_eq!(replay_batch.len(), worker_batch_len);

    #[cfg(feature = "encryption")]
    if _enable_encryption_transform {
        for event in replay_batch.events() {
            let payload = event
                .after
                .as_ref()
                .and_then(|after| after.get("payload"))
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    rustcdc::Error::StateError(
                        "expected encrypted payload string in replay batch".into(),
                    )
                })?;
            assert!(
                payload.starts_with("enc:v1:"),
                "expected encrypted payload format, got: {payload}"
            );
        }
    }

    let replay_ack = replay_batch.ack_token().expect("ack token should exist");

    runtime.commit_ack(replay_ack).await?;

    let reader_after = FileCheckpoint::new(checkpoint_dir.path());
    assert_eq!(
        reader_after.get_committed_count().await?,
        worker_batch_len as u64
    );

    Ok(())
}

fn spawn_crash_worker(
    host: &str,
    port: u16,
    checkpoint_dir: &Path,
    marker_file: &Path,
    database: &str,
    snapshot_table: Option<&str>,
    ack_first_batch: bool,
) -> rustcdc::Result<Child> {
    let worker_bin = resolve_worker_bin()?;

    let mut command = Command::new(worker_bin);
    command
        .env("CDC_RS_WORKER_HOST", host)
        .env("CDC_RS_WORKER_PORT", port.to_string())
        .env("CDC_RS_WORKER_USER", "sa")
        .env(
            "CDC_RS_WORKER_PASSWORD",
            sqlserver_testkit::SQLSERVER_SA_PASSWORD,
        )
        .env("CDC_RS_WORKER_DATABASE", database)
        .env("CDC_RS_WORKER_CHECKPOINT_DIR", checkpoint_dir)
        .env("CDC_RS_WORKER_MARKER_FILE", marker_file)
        .env(
            "CDC_RS_WORKER_ACK_FIRST_BATCH",
            if ack_first_batch { "1" } else { "0" },
        );
    if let Some(table) = snapshot_table {
        command.env("CDC_RS_WORKER_SNAPSHOT_TABLES", table);
    }
    command.spawn().map_err(rustcdc::Error::IoError)
}

fn resolve_worker_bin() -> rustcdc::Result<PathBuf> {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_sqlserver_crash_worker") {
        let path = PathBuf::from(path);
        if path.exists() {
            return Ok(path);
        }
    }

    let test_exe = std::env::current_exe().map_err(rustcdc::Error::IoError)?;
    if let Some(debug_dir) = test_exe.parent().and_then(|deps| deps.parent()) {
        let candidate = debug_dir.join("sqlserver_crash_worker");
        if candidate.exists() {
            return Ok(candidate);
        }

        build_xtask_worker(
            "sqlserver_crash_worker",
            "sqlserver,rustcdc/insecure-test-overrides",
        )?;
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    Err(rustcdc::Error::StateError(
        "sqlserver crash worker binary not found; build with `cargo build -p xtask --bin sqlserver_crash_worker --features sqlserver`"
            .into(),
    ))
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

fn lsn_bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::from("0x");
    for byte in bytes {
        out.push_str(&format!("{byte:02X}"));
    }
    out
}

async fn poll_until_batch_at_least(
    runtime: &mut CdcRuntime<FileCheckpoint, InMemorySchemaHistory>,
    expected: usize,
    rounds: usize,
) -> rustcdc::Result<rustcdc::EventBatch> {
    for _ in 0..rounds {
        let batch = runtime.poll_event_batch().await?;
        if batch.len() >= expected {
            return Ok(batch);
        }
    }

    Err(rustcdc::Error::TimeoutError(format!(
        "timed out waiting for event batch of at least {expected} events"
    )))
}
