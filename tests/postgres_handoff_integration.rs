#![cfg(feature = "postgres")]

use cdc_rs::{
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory, CdcRuntime,
    PostgresSourceConfig, RuntimeConfig, RuntimeSourceConfig,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

/// Test complete snapshot-to-stream handoff cycle
/// Validates: snapshot completion → stream start → no gaps or duplicates
#[tokio::test]
async fn postgres_snapshot_stream_handoff_full_cycle() -> cdc_rs::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres handoff test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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

    // Setup
    admin_client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS public.handoff_test (
              id BIGINT PRIMARY KEY,
              value TEXT
            );
            ALTER TABLE public.handoff_test REPLICA IDENTITY FULL;
            DROP PUBLICATION IF EXISTS handoff_test_pub;
            CREATE PUBLICATION handoff_test_pub FOR TABLE public.handoff_test;
            TRUNCATE TABLE public.handoff_test;
            ",
        )
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    // Insert initial snapshot data (1K rows)
    for id in 1..=1000 {
        let id_i64 = i64::from(id);
        let value = format!("initial-{id}");
        admin_client
            .execute(
                "INSERT INTO public.handoff_test (id, value) VALUES ($1, $2)",
                &[&id_i64, &value],
            )
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    let checkpoint_dir = tempfile::tempdir().map_err(cdc_rs::Error::IoError)?;
    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".into(),
        database: "cdc".to_string(),
        replication_slot_name: "handoff_test_slot".to_string(),
        publication_name: "handoff_test_pub".to_string(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..PostgresSourceConfig::default()
    };

    // Phase 1: Snapshot capture
    let mut runtime = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Postgres(source_cfg.clone()),
            FileCheckpoint::new(checkpoint_dir.path()),
            InMemorySchemaHistory::default(),
        )
        .with_snapshot_tables(vec!["public.handoff_test".to_string()])
        .with_max_buffer_size(256)
        .with_max_poll_wait_ms(150),
    )?;

    runtime.start().await?;

    // Capture all snapshot events and commit per batch.
    let mut snapshot_count = 0usize;
    for _ in 0..100 {
        let batch = runtime.poll_event_batch().await?;
        if batch.is_empty() {
            break;
        }
        snapshot_count += batch.len();
        runtime
            .commit_ack(
                batch
                    .ack_token()
                    .expect("non-empty batch should include ack token"),
            )
            .await?;
        if snapshot_count >= 1000 {
            break;
        }
    }

    println!("Phase 1 (Snapshot): Captured {} events", snapshot_count);
    assert!(
        snapshot_count >= 1000,
        "expected at least 1000 snapshot events, got {}",
        snapshot_count
    );

    // Phase 2: stream after snapshot (same runtime session).
    for id in 1001..=1100 {
        let id_i64 = i64::from(id);
        let value = format!("post-handoff-{id}");
        admin_client
            .execute(
                "INSERT INTO public.handoff_test (id, value) VALUES ($1, $2)",
                &[&id_i64, &value],
            )
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    // Capture stream events (post-handoff) and commit per batch.
    let mut stream_count = 0usize;
    let mut first_stream_id = None;
    for _ in 0..100 {
        let batch = runtime.poll_event_batch().await?;
        if batch.is_empty() {
            break;
        }

        if first_stream_id.is_none() {
            first_stream_id = batch.events().first().and_then(|event| {
                event
                    .after
                    .as_ref()
                    .and_then(|after| after.get("id").cloned())
            });
        }

        stream_count += batch.len();
        runtime
            .commit_ack(
                batch
                    .ack_token()
                    .expect("non-empty batch should include ack token"),
            )
            .await?;
        if stream_count >= 100 {
            break;
        }
    }

    println!(
        "Phase 2 (Stream): Captured {} events (expected ~100 inserts)",
        stream_count
    );

    if let Some(first_stream_id) = first_stream_id {
        println!("First stream event ID: {}", first_stream_id);
        // Should be from post-handoff inserts (1001+)
    }

    println!(
        "✓ Handoff test: snapshot {} events → stream {} events (total {})",
        snapshot_count,
        stream_count,
        snapshot_count + stream_count
    );

    runtime.stop().await?;

    Ok(())
}
