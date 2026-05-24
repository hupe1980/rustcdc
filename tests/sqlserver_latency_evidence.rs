#![cfg(feature = "sqlserver")]

use std::time::{Duration, Instant};

use cdc_rs::{
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory, CdcRuntime, RuntimeConfig,
    RuntimeSourceConfig,
};
#[path = "sqlserver_testkit.rs"]
mod sqlserver_testkit;

#[path = "latency_evidence_common.rs"]
mod latency_evidence_common;

use latency_evidence_common::{percentile, write_latency_artifacts, LatencySummary};

async fn sql_exec(client: &mut sqlserver_testkit::SqlClient, sql: &str) -> cdc_rs::Result<()> {
    client
        .execute(sql, &[])
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    Ok(())
}

#[tokio::test]
async fn sqlserver_connector_latency_evidence_stream_commit_percentiles() -> cdc_rs::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver latency evidence test") {
        return Ok(());
    }

    let container = sqlserver_testkit::start_sqlserver_container("2019-latest").await?;
    let (host, port) = sqlserver_testkit::host_and_port(&container).await?;

    let mut admin =
        sqlserver_testkit::connect_admin_with_retry(&host, port, 40, Duration::from_secs(2))
            .await?;

    sql_exec(
        &mut admin,
        "IF DB_ID('cdc_rs_latency') IS NULL CREATE DATABASE cdc_rs_latency",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE cdc_rs_latency; IF OBJECT_ID('dbo.latency_users', 'U') IS NULL CREATE TABLE dbo.latency_users (id INT NOT NULL PRIMARY KEY, payload NVARCHAR(255) NOT NULL)",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE cdc_rs_latency; DELETE FROM dbo.latency_users",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE cdc_rs_latency; IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0 EXEC sys.sp_cdc_enable_db",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE cdc_rs_latency; IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.latency_users')) EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='latency_users', @role_name=NULL, @supports_net_changes=0",
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let source_cfg = sqlserver_testkit::source_config(host, port, "cdc_rs_latency".into(), 30);

    let checkpoint_dir = tempfile::tempdir().map_err(cdc_rs::Error::IoError)?;
    let mut runtime = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::SqlServer(source_cfg),
            FileCheckpoint::new(checkpoint_dir.path()),
            InMemorySchemaHistory::default(),
        )
        .with_max_buffer_size(128)
        .with_max_poll_wait_ms(100),
    )?;

    runtime.start().await?;

    let rows_inserted = 4_096_u64;
    for id in 1_u64..=rows_inserted {
        let sql = format!(
            "USE cdc_rs_latency; INSERT INTO dbo.latency_users (id, payload) VALUES ({}, 'payload-{}')",
            id, id
        );
        sql_exec(&mut admin, &sql).await?;
    }

    let mut poll_latencies_ms = Vec::new();
    let mut commit_latencies_ms = Vec::new();
    let mut batch_sizes = Vec::new();
    let mut events_committed = 0_u64;
    let started = Instant::now();

    let cdc_scan_sql = "USE cdc_rs_latency; EXEC sys.sp_cdc_scan";
    let deadline = Instant::now() + Duration::from_secs(180);
    while events_committed < rows_inserted {
        if Instant::now() > deadline {
            return Err(cdc_rs::Error::TimeoutError(format!(
                "timed out while collecting sqlserver latency evidence (committed={events_committed}, expected={rows_inserted})"
            )));
        }

        let poll_start = Instant::now();
        let batch = runtime.poll_event_batch().await?;
        let poll_ms = poll_start.elapsed().as_secs_f64() * 1000.0;

        if batch.is_empty() {
            let _ = sql_exec(&mut admin, cdc_scan_sql).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
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
        profile: "sqlserver_stream_commit",
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
    assert!(
        summary.batches >= 16,
        "expected sustained multi-batch evidence"
    );

    write_latency_artifacts("sqlserver", &summary)?;
    println!(
        "sqlserver latency evidence recorded: batches={} poll_p95_ms={:.3} commit_p95_ms={:.3}",
        summary.batches, summary.poll_latency_ms_p95, summary.commit_latency_ms_p95
    );

    Ok(())
}
