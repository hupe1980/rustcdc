#![cfg(feature = "postgres")]

use std::time::Instant;

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

use latency_evidence_common::{
    assert_sample_is_meaningful, now_micros, stamped_payload, write_latency_artifacts,
    LatencyRecorder, ProgressDeadline, WriterStatus,
};

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
        incremental_snapshot: None,
    };
    seed_checkpoint.save(&seed_offset, 0).await?;
    // Release the seeding handle before the runtime takes ownership of the directory.
    drop(seed_checkpoint);

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".into(),
        database: "cdc".to_string(),
        replication_slot_name: SLOT_NAME.to_string(),
        publication_name: "cdc_latency_evidence_pub".to_string(),
        // Ephemeral test container: the slot legitimately does not exist yet.
        create_replication_slot_if_missing: true,
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

    // Write concurrently with polling. The previous version inserted every row *before*
    // the measurement loop started, so it timed draining an already-full in-process
    // buffer — a microbenchmark of the runtime's own bookkeeping against a pipeline that
    // was never under load. Capture latency only means something under live writes.
    // Published so the collection loop can tell "the pipeline stopped delivering" from
    // "the writer never wrote the rows". The writer's own `Result` is not observable until
    // after the loop, and a stalled loop never gets there.
    let writer_status = WriterStatus::new();
    let writer_progress = std::sync::Arc::clone(&writer_status);
    let writer = tokio::spawn(async move {
        for id in 1_i64..=rows_inserted as i64 {
            // The payload carries this process's wall clock, so capture latency is
            // measured against a single clock and container/host drift cannot skew it.
            let payload = stamped_payload(&format!("row-{id}"));
            if let Err(error) = admin_client
                .execute(
                    "INSERT INTO public.latency_evidence_users (id, payload) VALUES ($1, $2)",
                    &[&id, &payload],
                )
                .await
                .map_err(|error| rustcdc::Error::SourceError(error.to_string()))
            {
                writer_progress.record_failure(&error);
                return Err(error);
            }
            writer_progress.record_row();
        }
        Ok::<_, rustcdc::Error>(admin_client)
    });

    // Separate connection: the admin client is moved into the writer task.
    let (diag_client, diag_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = diag_conn.await;
    });

    let mut recorder = LatencyRecorder::new();
    let mut events_committed = 0_u64;
    let started = Instant::now();
    let mut deadline = ProgressDeadline::with_defaults("postgres capture latency", rows_inserted)
        .watching_writer(std::sync::Arc::clone(&writer_status));

    while events_committed < rows_inserted {
        // On a stall, dump the state that decides *where* to look before failing. Two CI
        // runs were spent narrowing this by elimination; a stall message that carries the
        // slot position, the runtime's own verdict and the connector's lag turns the next
        // one into an answer rather than another round of guessing.
        if let Err(stall) = deadline.check(events_committed) {
            let admin = runtime.admin_snapshot();
            let slot_state = diagnose_replication_slot(&diag_client, SLOT_NAME).await;
            eprintln!(
                "STALL DIAGNOSTIC\n  committed={events_committed}/{rows_inserted}\n  \
                 health={:?}\n  state={:?}\n  events_polled={}\n  \
                 slot_lag_bytes={:?}\n  {slot_state}",
                admin.health,
                admin.state,
                admin.total_events_polled,
                admin.replication_slot_lag_bytes,
            );
            return Err(stall);
        }

        let poll_start = Instant::now();
        let batch = runtime.poll_event_batch().await?;
        // Sample delivery time immediately, before any per-batch work: attributing the
        // batch's own processing to its last event would flatter the tail.
        let delivered_at = now_micros();
        let poll_ms = poll_start.elapsed().as_secs_f64() * 1000.0;

        if batch.is_empty() {
            continue;
        }

        let batch_len = batch.len();
        recorder.observe_poll_ms(poll_ms);
        recorder.observe_batch(batch.events(), delivered_at);

        let commit_start = Instant::now();
        runtime.commit_ack(batch.ack_mode()).await?;
        recorder.observe_commit_ms(commit_start.elapsed().as_secs_f64() * 1000.0);

        events_committed = events_committed.saturating_add(batch_len as u64);
    }

    let wall_clock_ms = started.elapsed().as_millis();
    writer
        .await
        .map_err(|error| rustcdc::Error::SourceError(format!("writer task panicked: {error}")))??;

    let summary = recorder.finish(
        "postgres_stream_capture",
        rows_inserted,
        events_committed,
        wall_clock_ms,
    );

    assert_eq!(summary.events_committed, rows_inserted);
    assert_sample_is_meaningful(&summary, rows_inserted / 2);

    write_latency_artifacts("postgres", &summary)?;
    println!(
        "postgres capture latency: p50={:.1}ms p95={:.1}ms p99={:.1}ms max={:.1}ms \
         throughput={:.0}/s batches={} clock_skew={:+.1}ms",
        summary.capture_latency_ms_p50,
        summary.capture_latency_ms_p95,
        summary.capture_latency_ms_p99,
        summary.capture_latency_ms_max,
        summary.events_per_second,
        summary.batches,
        summary.source_commit_skew_ms,
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

/// Slot name used by the latency suite, shared with the stall diagnostic.
const SLOT_NAME: &str = "cdc_latency_evidence_slot";

/// Report what PostgreSQL itself thinks the slot's position is.
///
/// A stall is either "the slot still holds changes we have not consumed" (a consumer-side
/// problem) or "the slot has been advanced past them" (data loss). `confirmed_flush_lsn`
/// versus `pg_current_wal_lsn()` distinguishes the two, and nothing in the runtime's own
/// metrics can.
async fn diagnose_replication_slot(client: &tokio_postgres::Client, slot: &str) -> String {
    let query = "SELECT s.confirmed_flush_lsn::text, pg_current_wal_lsn()::text, \
                 pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn)::text, \
                 s.active \
                 FROM pg_replication_slots s WHERE s.slot_name = $1";
    match client.query_opt(query, &[&slot]).await {
        Ok(Some(row)) => {
            let confirmed: String = row.get(0);
            let current: String = row.get(1);
            let lag: String = row.get(2);
            let active: bool = row.get(3);
            format!(
                "slot: confirmed_flush_lsn={confirmed} current_wal_lsn={current} \
                 lag_bytes={lag} active={active} \
                 (lag>0 means the slot still holds undelivered changes — a consumer-side \
                 stall; lag==0 with events missing means the slot was advanced past them)"
            )
        }
        Ok(None) => format!("slot: '{slot}' does not exist"),
        Err(error) => format!("slot: query failed: {error}"),
    }
}
