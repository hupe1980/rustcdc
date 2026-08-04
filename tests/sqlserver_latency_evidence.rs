#![cfg(feature = "sqlserver")]

use std::time::{Duration, Instant};

use rustcdc::{
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory, CdcRuntime, RuntimeConfig,
    RuntimeSourceConfig,
};
#[path = "sqlserver_testkit.rs"]
mod sqlserver_testkit;

#[path = "latency_evidence_common.rs"]
mod latency_evidence_common;

use latency_evidence_common::{
    assert_sample_is_meaningful, now_micros, stamped_payload, write_latency_artifacts,
    LatencyRecorder, ProgressDeadline,
};

async fn sql_exec(client: &mut sqlserver_testkit::SqlClient, sql: &str) -> rustcdc::Result<()> {
    client
        .execute(sql, &[])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    Ok(())
}

#[tokio::test]
async fn sqlserver_connector_latency_evidence_stream_commit_percentiles() -> rustcdc::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver latency evidence test") {
        return Ok(());
    }

    let container = match sqlserver_testkit::start_sqlserver_container("2022-latest").await {
        Ok(c) => c,
        Err(ref e) if sqlserver_testkit::is_skip_error(e) => return Ok(()),
        Err(e) => return Err(e),
    };
    let (host, port) = sqlserver_testkit::host_and_port(&container).await?;

    let mut admin =
        sqlserver_testkit::connect_admin_with_retry(&host, port, 40, Duration::from_secs(2))
            .await?;

    sql_exec(
        &mut admin,
        "IF DB_ID('rustcdc_latency') IS NULL CREATE DATABASE rustcdc_latency",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_latency; IF OBJECT_ID('dbo.latency_users', 'U') IS NULL CREATE TABLE dbo.latency_users (id INT NOT NULL PRIMARY KEY, payload NVARCHAR(255) NOT NULL)",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_latency; DELETE FROM dbo.latency_users",
    )
    .await?;
    sqlserver_testkit::enable_cdc(&host, port, "rustcdc_latency").await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_latency; IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.latency_users')) EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='latency_users', @role_name=NULL, @supports_net_changes=0",
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let (writer_host, writer_port) = (host.clone(), port);
    let source_cfg = sqlserver_testkit::source_config(host, port, "rustcdc_latency".into(), 30);

    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;
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

    // Write concurrently with polling, on a dedicated connection: `admin` stays with the
    // reader loop, which drives `sp_cdc_scan` to keep the capture job moving.
    let writer = tokio::spawn(async move {
        let mut writer_client = sqlserver_testkit::connect_admin_with_retry(
            &writer_host,
            writer_port,
            40,
            Duration::from_secs(2),
        )
        .await?;
        for id in 1_u64..=rows_inserted {
            // Payload carries this process's wall clock — capture latency is measured
            // against a single clock, so container/host drift cannot skew it.
            let payload = stamped_payload(&format!("row-{id}"));
            let sql = format!(
                "USE rustcdc_latency; INSERT INTO dbo.latency_users (id, payload) \
                 VALUES ({id}, '{payload}')"
            );
            sql_exec(&mut writer_client, &sql).await?;
        }
        Ok::<_, rustcdc::Error>(())
    });

    let mut recorder = LatencyRecorder::new();
    let mut events_committed = 0_u64;
    let started = Instant::now();

    let cdc_scan_sql = "USE rustcdc_latency; EXEC sys.sp_cdc_scan";
    // SQL Server CDC is polling-based and its capture job runs on the server's schedule,
    // so a longer stall window is legitimate here — but it is still a stall window, not a
    // total-time budget.
    let mut deadline = ProgressDeadline::new(
        "sqlserver capture latency",
        rows_inserted,
        std::time::Duration::from_secs(120),
        std::time::Duration::from_secs(900),
    );
    while events_committed < rows_inserted {
        deadline.check(events_committed)?;

        let poll_start = Instant::now();
        let batch = runtime.poll_event_batch().await?;
        let delivered_at = now_micros();
        let poll_ms = poll_start.elapsed().as_secs_f64() * 1000.0;

        if batch.is_empty() {
            // SQL Server CDC is capture-agent based: rows are not visible to the
            // connector until the capture job has scanned the log. Driving the scan
            // manually is what a latency-sensitive deployment does with a tightened
            // `sp_cdc_change_job` polling interval; without it this measures the agent's
            // default 5-second cadence, not the connector.
            let _ = sql_exec(&mut admin, cdc_scan_sql).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
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
        "sqlserver_stream_capture",
        rows_inserted,
        events_committed,
        wall_clock_ms,
    );

    assert!(summary.events_committed >= rows_inserted);
    assert_sample_is_meaningful(&summary, rows_inserted / 2);
    assert!(
        summary.batches >= 16,
        "expected sustained multi-batch evidence"
    );

    write_latency_artifacts("sqlserver", &summary)?;
    println!(
        "sqlserver capture latency: p50={:.1}ms p95={:.1}ms p99={:.1}ms max={:.1}ms \
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
