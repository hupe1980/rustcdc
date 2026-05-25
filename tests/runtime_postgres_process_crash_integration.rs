#![cfg(feature = "postgres")]

use std::{
    path::{Path, PathBuf},
    process::{Child, Command},
    time::Duration,
};

use cdc_rs::{
    checkpoint::{Checkpoint, FileCheckpoint, PostgresOffset},
    core::Operation,
    schema_history::InMemorySchemaHistory,
    CdcRuntime, PostgresSourceConfig, RuntimeConfig, RuntimeSourceConfig,
};
#[cfg(feature = "encryption")]
use cdc_rs::{
    core::SecretString,
    transform::{MaskHashConfig, MaskHashTransform, MaskRule},
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

mod process_crash_marker;
use process_crash_marker::{read_worker_batch_len, read_worker_marker, wait_for_marker};

#[tokio::test]
async fn runtime_postgres_process_kill_replays_uncommitted_batch() -> cdc_rs::Result<()> {
    run_postgres_process_kill_replay_scenario(false).await
}

#[tokio::test]
async fn runtime_postgres_process_kill_resumes_snapshot_after_committed_batch() -> cdc_rs::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!(
            "skipping postgres snapshot crash-resume integration test (set CDC_RS_RUN_DOCKER_TESTS=1)"
        );
        return Ok(());
    }

    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "cdc")
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=8",
            "-c",
            "max_wal_senders=8",
        ])
        .start()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let host_text = host.to_string();
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin_client, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    admin_client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS public.runtime_crash_snapshot_users (
              id BIGINT PRIMARY KEY,
              payload TEXT NOT NULL
            );
            ALTER TABLE public.runtime_crash_snapshot_users REPLICA IDENTITY FULL;
            DROP PUBLICATION IF EXISTS cdc_runtime_crash_snapshot_pub;
            CREATE PUBLICATION cdc_runtime_crash_snapshot_pub FOR TABLE public.runtime_crash_snapshot_users;
            TRUNCATE TABLE public.runtime_crash_snapshot_users;
            ",
        )
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let _lsn_text: String = admin_client
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&"cdc_runtime_crash_snapshot_slot"],
        )
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?
        .get(0);

    let total_rows = 600_i64;
    for id in 1_i64..=total_rows {
        admin_client
            .execute(
                "INSERT INTO public.runtime_crash_snapshot_users (id, payload) VALUES ($1, $2)",
                &[&id, &format!("payload-{id}")],
            )
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
        "cdc_runtime_crash_snapshot_slot",
        "cdc_runtime_crash_snapshot_pub",
        Some("public.runtime_crash_snapshot_users"),
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
    assert_eq!(saved.source_type(), "postgres_snapshot");
    assert_eq!(reader_after_worker.get_committed_count().await?, marker.events as u64);

    admin_client
        .query_one(
            "SELECT end_lsn::text FROM pg_replication_slot_advance($1, pg_current_wal_lsn())",
            &[&"cdc_runtime_crash_snapshot_slot"],
        )
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".into(),
        database: "cdc".to_string(),
        replication_slot_name: "cdc_runtime_crash_snapshot_slot".to_string(),
        publication_name: "cdc_runtime_crash_snapshot_pub".to_string(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..PostgresSourceConfig::default()
    };

    let mut runtime = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Postgres(source_cfg),
            FileCheckpoint::new(checkpoint_dir.path()),
            InMemorySchemaHistory::default(),
        )
        .with_snapshot_tables(vec!["public.runtime_crash_snapshot_users".to_string()])
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
async fn runtime_postgres_process_kill_replays_uncommitted_batch_with_encryption_transform(
) -> cdc_rs::Result<()> {
    run_postgres_process_kill_replay_scenario(true).await
}

async fn run_postgres_process_kill_replay_scenario(
    _enable_encryption_transform: bool,
) -> cdc_rs::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!(
            "skipping postgres process crash integration test (set CDC_RS_RUN_DOCKER_TESTS=1)"
        );
        return Ok(());
    }

    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "cdc")
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=8",
            "-c",
            "max_wal_senders=8",
        ])
        .start()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let host_text = host.to_string();
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin_client, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    admin_client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS public.runtime_crash_users (
              id BIGINT PRIMARY KEY,
              payload TEXT NOT NULL
            );
            ALTER TABLE public.runtime_crash_users REPLICA IDENTITY FULL;
            DROP PUBLICATION IF EXISTS cdc_runtime_crash_pub;
            CREATE PUBLICATION cdc_runtime_crash_pub FOR TABLE public.runtime_crash_users;
            TRUNCATE TABLE public.runtime_crash_users;
            ",
        )
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    // Ensure the replication slot exists before inserts so stream events are
    // guaranteed to be visible to the crash worker.
    let lsn_text: String = admin_client
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&"cdc_runtime_crash_slot"],
        )
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?
        .get(0);
    let baseline_lsn = parse_pg_lsn(&lsn_text)?;

    let checkpoint_dir = tempfile::tempdir().map_err(cdc_rs::Error::IoError)?;
    let marker_file = checkpoint_dir.path().join("worker-polled.marker");

    let mut seed_checkpoint = FileCheckpoint::new(checkpoint_dir.path());
    seed_checkpoint
        .save(
            &PostgresOffset {
                lsn: baseline_lsn,
                slot_name: "cdc_runtime_crash_slot".to_string(),
            },
            0,
        )
        .await?;

    for id in 1_i64..=100_i64 {
        admin_client
            .execute(
                "INSERT INTO public.runtime_crash_users (id, payload) VALUES ($1, $2)",
                &[&id, &format!("payload-{id}")],
            )
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    let mut worker = spawn_crash_worker(
        &host_text,
        port,
        checkpoint_dir.path(),
        &marker_file,
        "cdc_runtime_crash_slot",
        "cdc_runtime_crash_pub",
        None,
        false,
    )?;

    wait_for_marker(&marker_file, Duration::from_secs(90))?;
    let worker_batch_len = read_worker_batch_len(&marker_file)?;

    // External hard kill simulates real process termination without graceful shutdown.
    worker.kill().map_err(cdc_rs::Error::IoError)?;
    let _ = worker.wait().map_err(cdc_rs::Error::IoError)?;

    let reader_before = FileCheckpoint::new(checkpoint_dir.path());
    assert_eq!(reader_before.get_committed_count().await?, 0);

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".into(),
        database: "cdc".to_string(),
        replication_slot_name: "cdc_runtime_crash_slot".to_string(),
        publication_name: "cdc_runtime_crash_pub".to_string(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..PostgresSourceConfig::default()
    };

    let mut runtime = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Postgres(source_cfg),
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

    let replay_batch = poll_until_batch_at_least(&mut runtime, 1, 40).await?;
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
    slot: &str,
    publication: &str,
    snapshot_table: Option<&str>,
    ack_first_batch: bool,
) -> cdc_rs::Result<Child> {
    let worker_bin = resolve_worker_bin()?;

    let mut command = Command::new(worker_bin);
    command
        .env("CDC_RS_WORKER_HOST", host)
        .env("CDC_RS_WORKER_PORT", port.to_string())
        .env("CDC_RS_WORKER_USER", "postgres")
        .env("CDC_RS_WORKER_PASSWORD", "postgres")
        .env("CDC_RS_WORKER_DATABASE", "cdc")
        .env("CDC_RS_WORKER_SLOT", slot)
        .env("CDC_RS_WORKER_PUBLICATION", publication)
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
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_postgres_crash_worker") {
        let path = PathBuf::from(path);
        if path.exists() {
            return Ok(path);
        }
    }

    // Fallback for `cargo test` invocations that do not set CARGO_BIN_EXE_*.
    let test_exe = std::env::current_exe().map_err(cdc_rs::Error::IoError)?;
    if let Some(debug_dir) = test_exe.parent().and_then(|deps| deps.parent()) {
        let candidate = debug_dir.join("postgres_crash_worker");
        if candidate.exists() {
            return Ok(candidate);
        }

        build_xtask_worker("postgres_crash_worker", "postgres")?;
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    Err(cdc_rs::Error::StateError(
        "postgres crash worker binary not found; build with `cargo build -p xtask --bin postgres_crash_worker --features postgres`"
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

fn parse_pg_lsn(value: &str) -> cdc_rs::Result<u64> {
    let (high, low) = value.split_once('/').ok_or_else(|| {
        cdc_rs::Error::SourceError(format!("invalid postgres lsn format: {value}"))
    })?;
    let high = u64::from_str_radix(high, 16)
        .map_err(|error| cdc_rs::Error::SourceError(format!("invalid lsn high bits: {error}")))?;
    let low = u64::from_str_radix(low, 16)
        .map_err(|error| cdc_rs::Error::SourceError(format!("invalid lsn low bits: {error}")))?;
    Ok((high << 32) | low)
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
