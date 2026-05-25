#![cfg(feature = "mysql")]

use std::time::{Duration, Instant};

use cdc_rs::{
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory, CdcRuntime,
    MysqlSourceConfig, RuntimeConfig, RuntimeSourceConfig, TransportConfig,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::time::sleep;

#[path = "latency_evidence_common.rs"]
mod latency_evidence_common;

use latency_evidence_common::{percentile, write_latency_artifacts, LatencySummary};

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
                sleep(Duration::from_millis(500)).await;
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
async fn mysql_connector_latency_evidence_stream_commit_percentiles() -> cdc_rs::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql latency evidence test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let admin_pool = connect_admin_pool(&admin_dsn).await?;

    sqlx::query("DROP TABLE IF EXISTS latency_evidence_users")
        .execute(&admin_pool)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE latency_evidence_users (
            id BIGINT PRIMARY KEY,
            payload TEXT NOT NULL
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let source_cfg = MysqlSourceConfig {
        host: host.to_string(),
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id: 2026,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    };

    let checkpoint_dir = tempfile::tempdir().map_err(cdc_rs::Error::IoError)?;
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
    for id in 1_i64..=rows_inserted as i64 {
        sqlx::query("INSERT INTO latency_evidence_users (id, payload) VALUES (?, ?)")
            .bind(id)
            .bind(format!("payload-{id}"))
            .execute(&admin_pool)
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    let mut poll_latencies_ms = Vec::new();
    let mut commit_latencies_ms = Vec::new();
    let mut batch_sizes = Vec::new();
    let mut events_committed = 0_u64;
    let started = Instant::now();

    let deadline = Instant::now() + Duration::from_secs(90);
    while events_committed < rows_inserted {
        if Instant::now() > deadline {
            return Err(cdc_rs::Error::TimeoutError(format!(
                "timed out while collecting mysql latency evidence (committed={events_committed}, expected={rows_inserted})"
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
        profile: "mysql_stream_commit",
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

    assert!(summary.events_committed >= rows_inserted);
    assert!(summary.batches > 0, "expected at least one committed batch");

    write_latency_artifacts("mysql", &summary)?;
    println!(
        "mysql latency evidence recorded: batches={} poll_p95_ms={:.3} commit_p95_ms={:.3}",
        summary.batches, summary.poll_latency_ms_p95, summary.commit_latency_ms_p95
    );

    Ok(())
}
