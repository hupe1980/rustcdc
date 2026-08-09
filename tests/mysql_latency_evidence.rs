#![cfg(feature = "mysql")]

use std::time::{Duration, Instant};

use rustcdc::{
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory, CdcRuntime,
    MysqlSourceConfig, RuntimeConfig, RuntimeSourceConfig, TransportConfig,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::time::sleep;

#[path = "rustls_provider_common.rs"]
mod rustls_provider_common;
use rustls_provider_common::install_rustls_provider;

#[path = "latency_evidence_common.rs"]
mod latency_evidence_common;

use latency_evidence_common::{
    assert_sample_is_meaningful, now_micros, stamped_payload, write_latency_artifacts,
    LatencyRecorder, ProgressDeadline, WriterStatus,
};

async fn connect_admin_pool(dsn: &str) -> rustcdc::Result<sqlx::MySqlPool> {
    install_rustls_provider();
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
                sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(rustcdc::Error::SourceError(format!(
        "failed to connect mysql admin pool: {}",
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    )))
}

#[tokio::test]
async fn mysql_connector_latency_evidence_stream_commit_percentiles() -> rustcdc::Result<()> {
    install_rustls_provider();
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql latency evidence test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = GenericImage::new("mysql", "8.0")
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
        // rustcdc requires FULL row metadata and row images. MySQL 8 defaults
        // binlog_row_metadata to MINIMAL, under which the binlog carries no column
        // names and no primary-key flags — events would be emitted with positional
        // placeholder keys ("@0", "@1") and no key at all. connect() rejects that, so
        // the test server must be configured the way a production server must be.
        .with_cmd(vec![
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-metadata=FULL",
            "--binlog-row-image=FULL",
            "--server-id=1",
        ])
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let admin_pool = connect_admin_pool(&admin_dsn).await?;

    sqlx::query("DROP TABLE IF EXISTS latency_evidence_users")
        .execute(&admin_pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE latency_evidence_users (
            id BIGINT PRIMARY KEY,
            payload TEXT NOT NULL
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let source_cfg = MysqlSourceConfig {
        host: host.to_string(),
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id: 2026,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::plaintext(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    };

    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;
    let mut runtime = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Mysql(source_cfg),
            FileCheckpoint::new(checkpoint_dir.path()),
            InMemorySchemaHistory::default(),
        )
        .with_max_buffer_size(4_096)
        .with_max_poll_wait_ms(100),
    )?;

    runtime.start().await?;

    let rows_inserted = 2_000_u64;

    // Write concurrently with polling — see `latency_evidence_common` for why measuring a
    // pre-filled buffer measures nothing an operator can use.
    // Published so a stall can name which side stopped, and so a writer failure is
    // reported with its own error instead of as an ambiguous timeout.
    let writer_status = WriterStatus::new();
    let writer_progress = std::sync::Arc::clone(&writer_status);
    let writer = tokio::spawn(async move {
        for id in 1_i64..=rows_inserted as i64 {
            // Payload carries this process's wall clock, so capture latency is measured
            // against a single clock and container/host drift cannot skew it.
            let payload = stamped_payload(&format!("row-{id}"));
            if let Err(error) =
                sqlx::query("INSERT INTO latency_evidence_users (id, payload) VALUES (?, ?)")
                    .bind(id)
                    .bind(payload)
                    .execute(&admin_pool)
                    .await
                    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))
            {
                writer_progress.record_failure(&error);
                return Err(error);
            }
            writer_progress.record_row();
        }
        Ok::<_, rustcdc::Error>(admin_pool)
    });

    let mut recorder = LatencyRecorder::new();
    let mut events_committed = 0_u64;
    let started = Instant::now();
    let mut deadline = ProgressDeadline::with_defaults("mysql capture latency", rows_inserted)
        .watching_writer(std::sync::Arc::clone(&writer_status));

    while events_committed < rows_inserted {
        deadline.check(events_committed)?;

        let poll_start = Instant::now();
        let batch = runtime.poll_event_batch().await?;
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
        "mysql_stream_capture",
        rows_inserted,
        events_committed,
        wall_clock_ms,
    );

    assert!(summary.events_committed >= rows_inserted);
    assert_sample_is_meaningful(&summary, rows_inserted / 2);

    write_latency_artifacts("mysql", &summary)?;
    println!(
        "mysql capture latency: p50={:.1}ms p95={:.1}ms p99={:.1}ms max={:.1}ms \
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
