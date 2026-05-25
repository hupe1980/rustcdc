#![cfg(feature = "postgres")]

use std::time::{Duration, Instant};

use rustcdc::{
    checkpoint::{Checkpoint, FileCheckpoint, PostgresOffset},
    schema_history::InMemorySchemaHistory,
    CdcRuntime, PostgresSourceConfig, RuntimeConfig, RuntimeSourceConfig,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

#[path = "latency_evidence_common.rs"]
mod latency_evidence_common;

use latency_evidence_common::{percentile, write_latency_artifacts, LatencySummary};

#[tokio::test]
async fn postgres_connector_latency_evidence_stream_commit_percentiles() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!(
            "skipping postgres latency evidence test (set CDC_RS_RUN_DOCKER_TESTS=1 to enable)"
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
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin_client, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    admin_client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS public.latency_evidence_users (
              id BIGINT PRIMARY KEY,
              payload TEXT NOT NULL
            );
            ALTER TABLE public.latency_evidence_users REPLICA IDENTITY FULL;
            DROP PUBLICATION IF EXISTS cdc_latency_evidence_pub;
            CREATE PUBLICATION cdc_latency_evidence_pub FOR TABLE public.latency_evidence_users;
            TRUNCATE TABLE public.latency_evidence_users;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let lsn_text: String = admin_client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .get(0);
    let baseline_lsn = parse_pg_lsn(&lsn_text)?;

    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;
    let mut seed_checkpoint = FileCheckpoint::new(checkpoint_dir.path());
    let seed_offset = PostgresOffset {
        lsn: baseline_lsn,
        slot_name: "cdc_latency_evidence_slot".to_string(),
    };
    seed_checkpoint.save(&seed_offset, 0).await?;

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".into(),
        database: "cdc".to_string(),
        replication_slot_name: "cdc_latency_evidence_slot".to_string(),
        publication_name: "cdc_latency_evidence_pub".to_string(),
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
        .with_max_buffer_size(4_096)
        .with_max_poll_wait_ms(100),
    )?;
    runtime.start().await?;

    let rows_inserted = 2_000_u64;
    for id in 1_i64..=rows_inserted as i64 {
        admin_client
            .execute(
                "INSERT INTO public.latency_evidence_users (id, payload) VALUES ($1, $2)",
                &[&id, &format!("payload-{id}")],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    let mut poll_latencies_ms = Vec::new();
    let mut commit_latencies_ms = Vec::new();
    let mut batch_sizes = Vec::new();
    let mut events_committed = 0_u64;
    let started = Instant::now();

    let deadline = Instant::now() + Duration::from_secs(90);
    while events_committed < rows_inserted {
        if Instant::now() > deadline {
            return Err(rustcdc::Error::TimeoutError(format!(
                "timed out while collecting latency evidence (committed={events_committed}, expected={rows_inserted})"
            )));
        }

        let poll_start = Instant::now();
        let batch = runtime.poll_event_batch().await?;
        let poll_ms = poll_start.elapsed().as_secs_f64() * 1000.0;

        if batch.is_empty() {
            continue;
        }

        let batch_len = batch.len();
        batch_sizes.push(batch_len as f64);
        poll_latencies_ms.push(poll_ms);

        let token = batch
            .ack_token()
            .expect("non-empty batch should provide ack token");

        let commit_start = Instant::now();
        runtime.commit_ack(token).await?;
        let commit_ms = commit_start.elapsed().as_secs_f64() * 1000.0;
        commit_latencies_ms.push(commit_ms);

        events_committed = events_committed.saturating_add(batch_len as u64);
    }

    let summary = LatencySummary {
        profile: "postgres_stream_commit",
        rows_inserted,
        events_committed,
        batches: poll_latencies_ms.len(),
        poll_latency_ms_p50: percentile(&poll_latencies_ms, 50.0),
        poll_latency_ms_p95: percentile(&poll_latencies_ms, 95.0),
        poll_latency_ms_p99: percentile(&poll_latencies_ms, 99.0),
        commit_latency_ms_p50: percentile(&commit_latencies_ms, 50.0),
        commit_latency_ms_p95: percentile(&commit_latencies_ms, 95.0),
        commit_latency_ms_p99: percentile(&commit_latencies_ms, 99.0),
        batch_size_p50: percentile(&batch_sizes, 50.0),
        batch_size_p95: percentile(&batch_sizes, 95.0),
        batch_size_p99: percentile(&batch_sizes, 99.0),
        end_to_end_ms: started.elapsed().as_millis(),
    };

    assert_eq!(summary.events_committed, rows_inserted);
    assert!(summary.batches > 0, "expected at least one committed batch");

    write_latency_artifacts("postgres", &summary)?;
    println!(
        "latency evidence recorded: batches={} poll_p95_ms={:.3} commit_p95_ms={:.3}",
        summary.batches, summary.poll_latency_ms_p95, summary.commit_latency_ms_p95
    );

    Ok(())
}

fn parse_pg_lsn(value: &str) -> rustcdc::Result<u64> {
    let (high, low) = value.split_once('/').ok_or_else(|| {
        rustcdc::Error::SourceError(format!("invalid postgres lsn format: {value}"))
    })?;
    let high = u64::from_str_radix(high, 16)
        .map_err(|error| rustcdc::Error::SourceError(format!("invalid lsn high bits: {error}")))?;
    let low = u64::from_str_radix(low, 16)
        .map_err(|error| rustcdc::Error::SourceError(format!("invalid lsn low bits: {error}")))?;
    Ok((high << 32) | low)
}
