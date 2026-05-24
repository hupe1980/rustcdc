#![cfg(feature = "mysql")]

use std::{
    path::{Path, PathBuf},
    process::{Child, Command},
    time::Duration,
};

use cdc_rs::{
    checkpoint::{Checkpoint, FileCheckpoint, MysqlOffset},
    core::Operation,
    schema_history::InMemorySchemaHistory,
    CdcRuntime, MysqlSourceConfig, RuntimeConfig, RuntimeSourceConfig, TransportConfig,
};
#[cfg(feature = "encryption")]
use cdc_rs::{
    core::SecretString,
    transform::{MaskHashConfig, MaskHashTransform, MaskRule},
};
use sqlx::Row;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

mod process_crash_marker;
use process_crash_marker::{read_worker_batch_len, read_worker_marker, wait_for_marker};

async fn connect_admin_pool(dsn: &str) -> cdc_rs::Result<sqlx::MySqlPool> {
    let mut last_error = None;
    for _ in 0..30 {
        match sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(1)
            .connect(dsn)
            .await
        {
            Ok(pool) => return Ok(pool),
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(cdc_rs::Error::SourceError(format!(
        "failed to connect mysql admin pool: {}",
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    )))
}

#[tokio::test]
async fn runtime_mysql_process_kill_replays_uncommitted_batch() -> cdc_rs::Result<()> {
    run_mysql_process_kill_replay_scenario(false).await
}

#[tokio::test]
async fn runtime_mysql_process_kill_resumes_snapshot_after_committed_batch() -> cdc_rs::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql snapshot crash-resume integration test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = GenericImage::new("mysql", "8.0")
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
        .start()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let host_text = host.to_string();
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let admin_pool = connect_admin_pool(&admin_dsn).await?;

    sqlx::query("DROP TABLE IF EXISTS runtime_crash_snapshot_users")
        .execute(&admin_pool)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE runtime_crash_snapshot_users (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            payload VARCHAR(255) NOT NULL
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let total_rows = 600_i64;
    for id in 1_i64..=total_rows {
        sqlx::query("INSERT INTO runtime_crash_snapshot_users (payload) VALUES (?)")
            .bind(format!("payload-{id}"))
            .execute(&admin_pool)
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    let checkpoint_dir = tempfile::tempdir().map_err(cdc_rs::Error::IoError)?;
    let marker_file = checkpoint_dir.path().join("worker-polled.marker");

    let mut worker = spawn_crash_worker(
        &host_text,
        port,
        checkpoint_dir.path(),
        &marker_file,
        410,
        Some("runtime_crash_snapshot_users"),
        true,
    )?;

    wait_for_marker(&marker_file, Duration::from_secs(90))?;
    let marker = read_worker_marker(&marker_file)?;
    assert!(marker.acked, "worker should ack first snapshot batch");
    assert!(!marker.ids.is_empty(), "worker should record acked ids");

    worker.kill().map_err(cdc_rs::Error::IoError)?;
    let _ = worker.wait().map_err(cdc_rs::Error::IoError)?;

    let reader_after_worker = FileCheckpoint::new(checkpoint_dir.path());
    let saved = reader_after_worker
        .load()
        .await?
        .ok_or_else(|| cdc_rs::Error::StateError("checkpoint should exist after worker ack".into()))?;
    assert_eq!(saved.source_type(), "mysql_snapshot");
    assert_eq!(reader_after_worker.get_committed_count().await?, marker.events as u64);

    let source_cfg = MysqlSourceConfig {
        host: host_text,
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id: 411,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
    };

    let mut runtime = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Mysql(source_cfg),
            FileCheckpoint::new(checkpoint_dir.path()),
            InMemorySchemaHistory::default(),
        )
        .with_snapshot_tables(vec!["runtime_crash_snapshot_users".to_string()])
        .with_max_buffer_size(256)
        .with_max_poll_wait_ms(150),
    )?;

    runtime.start().await?;

    let mut resumed_snapshot_ids = std::collections::HashSet::new();
    for _ in 0..80 {
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
                .ok_or_else(|| cdc_rs::Error::StateError("snapshot event id missing".into()))?;
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
async fn runtime_mysql_process_kill_replays_uncommitted_batch_with_encryption_transform(
) -> cdc_rs::Result<()> {
    run_mysql_process_kill_replay_scenario(true).await
}

async fn run_mysql_process_kill_replay_scenario(
    _enable_encryption_transform: bool,
) -> cdc_rs::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql process crash integration test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = GenericImage::new("mysql", "8.0")
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
        .start()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let host_text = host.to_string();
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let admin_pool = connect_admin_pool(&admin_dsn).await?;

    sqlx::query("DROP TABLE IF EXISTS runtime_crash_users")
        .execute(&admin_pool)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE runtime_crash_users (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            payload VARCHAR(255) NOT NULL
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let status: sqlx::mysql::MySqlRow = sqlx::query("SHOW MASTER STATUS")
        .fetch_one(&admin_pool)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let baseline_file: String = status.try_get(0).unwrap_or_default();
    let baseline_pos: u32 = status.try_get::<u64, _>(1).unwrap_or(4) as u32;
    let baseline_gtid: String = sqlx::query_scalar("SELECT @@GLOBAL.GTID_EXECUTED")
        .fetch_optional(&admin_pool)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();

    let checkpoint_dir = tempfile::tempdir().map_err(cdc_rs::Error::IoError)?;
    let marker_file = checkpoint_dir.path().join("worker-polled.marker");

    let mut seed_checkpoint = FileCheckpoint::new(checkpoint_dir.path());
    seed_checkpoint
        .save(
            &MysqlOffset {
                gtid: baseline_gtid,
                binlog_file: baseline_file,
                binlog_pos: baseline_pos,
            },
            0,
        )
        .await?;

    for id in 1_i64..=100_i64 {
        sqlx::query("INSERT INTO runtime_crash_users (payload) VALUES (?)")
            .bind(format!("payload-{id}"))
            .execute(&admin_pool)
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    let mut worker =
        spawn_crash_worker(&host_text, port, checkpoint_dir.path(), &marker_file, 402, None, false)?;

    wait_for_marker(&marker_file, Duration::from_secs(90))?;
    let worker_batch_len = read_worker_batch_len(&marker_file)?;

    worker.kill().map_err(cdc_rs::Error::IoError)?;
    let _ = worker.wait().map_err(cdc_rs::Error::IoError)?;

    let reader_before = FileCheckpoint::new(checkpoint_dir.path());
    assert_eq!(reader_before.get_committed_count().await?, 0);

    let source_cfg = MysqlSourceConfig {
        host: host_text,
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id: 403,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
    };

    let mut runtime = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Mysql(source_cfg),
            FileCheckpoint::new(checkpoint_dir.path()),
            InMemorySchemaHistory::default(),
        )
        .with_max_buffer_size(256)
        .with_max_poll_wait_ms(150),
    )?;

    #[cfg(feature = "encryption")]
    if _enable_encryption_transform {
        use std::collections::HashMap;

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

    let replay_batch = poll_until_batch_at_least(&mut runtime, 1, 50).await?;
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
                    cdc_rs::Error::StateError(
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
    server_id: u32,
    snapshot_table: Option<&str>,
    ack_first_batch: bool,
) -> cdc_rs::Result<Child> {
    let worker_bin = resolve_worker_bin()?;

    let mut command = Command::new(worker_bin);
    command
        .env("CDC_RS_WORKER_HOST", host)
        .env("CDC_RS_WORKER_PORT", port.to_string())
        .env("CDC_RS_WORKER_USER", "root")
        .env("CDC_RS_WORKER_PASSWORD", "rootpass")
        .env("CDC_RS_WORKER_DATABASE", "cdc")
        .env("CDC_RS_WORKER_SERVER_ID", server_id.to_string())
        .env("CDC_RS_WORKER_CHECKPOINT_DIR", checkpoint_dir)
        .env("CDC_RS_WORKER_MARKER_FILE", marker_file)
        .env(
            "CDC_RS_WORKER_ACK_FIRST_BATCH",
            if ack_first_batch { "1" } else { "0" },
        );
    if let Some(table) = snapshot_table {
        command.env("CDC_RS_WORKER_SNAPSHOT_TABLES", table);
    }
    command.spawn().map_err(cdc_rs::Error::IoError)
}

fn resolve_worker_bin() -> cdc_rs::Result<PathBuf> {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_mysql_crash_worker") {
        let path = PathBuf::from(path);
        if path.exists() {
            return Ok(path);
        }
    }

    let test_exe = std::env::current_exe().map_err(cdc_rs::Error::IoError)?;
    if let Some(debug_dir) = test_exe.parent().and_then(|deps| deps.parent()) {
        let candidate = debug_dir.join("mysql_crash_worker");
        if candidate.exists() {
            return Ok(candidate);
        }

        build_xtask_worker("mysql_crash_worker", "mysql")?;
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    Err(cdc_rs::Error::StateError(
        "mysql crash worker binary not found; build with `cargo build -p xtask --bin mysql_crash_worker --features mysql`"
            .into(),
    ))
}

fn build_xtask_worker(bin: &str, feature: &str) -> cdc_rs::Result<()> {
    let status = Command::new("cargo")
        .args([
            "build",
            "-p",
            "xtask",
            "--bin",
            bin,
            "--features",
            feature,
        ])
        .status()
        .map_err(cdc_rs::Error::IoError)?;

    if status.success() {
        Ok(())
    } else {
        Err(cdc_rs::Error::StateError(format!(
            "failed to build {bin} in xtask crate"
        )))
    }
}

async fn poll_until_batch_at_least(
    runtime: &mut CdcRuntime<FileCheckpoint, InMemorySchemaHistory>,
    expected: usize,
    rounds: usize,
) -> cdc_rs::Result<cdc_rs::EventBatch> {
    for _ in 0..rounds {
        let batch = runtime.poll_event_batch().await?;
        if batch.len() >= expected {
            return Ok(batch);
        }
    }

    Err(cdc_rs::Error::TimeoutError(format!(
        "timed out waiting for event batch of at least {expected} events"
    )))
}
