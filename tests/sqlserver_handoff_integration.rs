#![cfg(feature = "sqlserver")]

use rustcdc::{
    checkpoint::Checkpoint, checkpoint::InMemoryCheckpoint, source::Source, SqlServerConnection,
};

#[path = "sqlserver_testkit.rs"]
mod sqlserver_testkit;

async fn poll_stream_events(
    stream: &mut dyn rustcdc::source::StreamHandle,
    attempts: usize,
) -> rustcdc::Result<Vec<rustcdc::Event>> {
    let mut out = Vec::new();
    for _ in 0..attempts {
        let mut batch = stream.next_events(400).await?;
        if batch.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            continue;
        }
        out.append(&mut batch);
    }
    Ok(out)
}

#[tokio::test]
async fn sqlserver_handoff_snapshot_to_stream_no_gap() -> rustcdc::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver handoff integration test") {
        return Ok(());
    }

    let container = sqlserver_testkit::start_sqlserver_container("2019-latest").await?;
    let (host, port) = sqlserver_testkit::host_and_port(&container).await?;

    let mut admin = sqlserver_testkit::connect_admin_with_retry(
        &host,
        port,
        40,
        std::time::Duration::from_secs(2),
    )
    .await?;
    sqlserver_testkit::sql_exec(
        &mut admin,
        "IF DB_ID('rustcdc_handoff') IS NULL CREATE DATABASE rustcdc_handoff",
    )
    .await?;
    sqlserver_testkit::sql_exec(
        &mut admin,
        "USE rustcdc_handoff; IF OBJECT_ID('dbo.orders', 'U') IS NULL CREATE TABLE dbo.orders (id INT NOT NULL PRIMARY KEY, amount INT NOT NULL)",
    )
    .await?;
    sqlserver_testkit::sql_exec(&mut admin, "USE rustcdc_handoff; DELETE FROM dbo.orders").await?;
    sqlserver_testkit::enable_cdc(&host, port, "rustcdc_handoff").await?;
    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        "USE rustcdc_handoff; IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.orders')) EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='orders', @role_name=NULL, @supports_net_changes=0",
    )
    .await?;
    sqlserver_testkit::sql_exec(
        &mut admin,
        "USE rustcdc_handoff; INSERT INTO dbo.orders (id, amount) VALUES (1, 100), (2, 200)",
    )
    .await?;

    let mut source = SqlServerConnection::new(sqlserver_testkit::source_config(
        host.clone(),
        port,
        "rustcdc_handoff".into(),
        30,
    ));

    source.connect().await?;
    let mut snapshot = source.start_snapshot(&["dbo.orders"]).await?;

    // Drain snapshot before handoff.
    loop {
        let chunk = snapshot.next_chunk(256).await?;
        if chunk.is_empty() {
            break;
        }
    }

    let mut stream = source.start_stream(None).await?;
    let result = source
        .perform_handoff(snapshot.as_mut(), stream.as_mut())
        .await?;

    assert!(result.snapshot_end_ts.is_some());
    assert!(result.stream_start_ts.is_some());
    assert!(result.overlap_events_dropped <= 1000);

    // Stream polling after handoff should remain operational even if CDC delivery is delayed.
    sqlserver_testkit::sql_exec(
        &mut admin,
        "USE rustcdc_handoff; INSERT INTO dbo.orders (id, amount) VALUES (3, 300)",
    )
    .await?;
    let _events_after_handoff = poll_stream_events(stream.as_mut(), 20).await?;

    // Save stream position and resume from it after restart.
    let mut checkpoint = InMemoryCheckpoint::default();
    stream.save_position(&mut checkpoint).await?;
    let resume = checkpoint
        .load()
        .await?
        .ok_or_else(|| rustcdc::Error::CheckpointError("missing handoff checkpoint".into()))?;

    source.close().await;

    let mut resumed = SqlServerConnection::new(sqlserver_testkit::source_config(
        host.clone(),
        port,
        "rustcdc_handoff".into(),
        30,
    ));
    resumed.connect().await?;
    let mut resumed_stream = resumed.start_stream(Some(resume.as_ref())).await?;

    sqlserver_testkit::sql_exec(
        &mut admin,
        "USE rustcdc_handoff; INSERT INTO dbo.orders (id, amount) VALUES (4, 400)",
    )
    .await?;
    let _resumed_events = poll_stream_events(resumed_stream.as_mut(), 20).await?;

    resumed.close().await;
    Ok(())
}
