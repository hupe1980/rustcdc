#![cfg(feature = "sqlserver")]

use rustcdc::{
    checkpoint::Checkpoint, checkpoint::InMemoryCheckpoint, source::Source, Operation,
    SqlServerConnection,
};

#[path = "sqlserver_testkit.rs"]
mod sqlserver_testkit;

async fn sql_exec(
    client: &mut tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>,
    sql: &str,
) -> rustcdc::Result<()> {
    client
        .execute(sql, &[])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    Ok(())
}

async fn sql_exec_with_retry(
    client: &mut tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>,
    sql: &str,
) -> rustcdc::Result<()> {
    const MAX_ATTEMPTS: usize = 8;

    for attempt in 1..=MAX_ATTEMPTS {
        match sql_exec(client, sql).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                let message = error.to_string().to_ascii_lowercase();
                let is_deadlock =
                    message.contains("deadlock victim") || message.contains("code: 1205");
                if is_deadlock && attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
                return Err(error);
            }
        }
    }

    Err(rustcdc::Error::StateError(
        "sql_exec_with_retry exhausted attempts unexpectedly".to_string(),
    ))
}

async fn collect_events_with_deadline(
    stream: &mut dyn rustcdc::source::StreamHandle,
    admin: &mut tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>,
    database: &str,
    max_wait: std::time::Duration,
    require_full_crud: bool,
) -> rustcdc::Result<Vec<rustcdc::Event>> {
    let deadline = std::time::Instant::now() + max_wait;
    let mut events = Vec::new();
    let cdc_scan_sql = format!("USE {database}; EXEC sys.sp_cdc_scan");

    while std::time::Instant::now() < deadline {
        let mut batch = stream.next_events(200).await?;
        if batch.is_empty() {
            // Force a capture pass so CDC rows become visible promptly in CI/containers.
            let _ = sql_exec(admin, &cdc_scan_sql).await;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        events.append(&mut batch);
        if require_full_crud {
            let has_insert = events
                .iter()
                .any(|event| event.op == rustcdc::Operation::Insert);
            let has_update = events
                .iter()
                .any(|event| event.op == rustcdc::Operation::Update);
            let has_delete = events
                .iter()
                .any(|event| event.op == rustcdc::Operation::Delete);
            if has_insert && has_update && has_delete {
                break;
            }
        } else if !events.is_empty() {
            break;
        }
    }

    Ok(events)
}

#[tokio::test]
async fn sqlserver_stream_insert_update_delete_and_resume() -> rustcdc::Result<()> {
    const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

    tokio::time::timeout(TEST_TIMEOUT, async {
        run_sqlserver_stream_insert_update_delete_and_resume().await
    })
    .await
    .map_err(|_| {
        rustcdc::Error::TimeoutError(
            "sqlserver stream integration exceeded 300s timeout".to_string(),
        )
    })?
}

async fn run_sqlserver_stream_insert_update_delete_and_resume() -> rustcdc::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver stream integration test") {
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

    sql_exec(
        &mut admin,
        "IF DB_ID('rustcdc_test') IS NULL CREATE DATABASE rustcdc_test",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_test; IF OBJECT_ID('dbo.users', 'U') IS NULL CREATE TABLE dbo.users (id INT NOT NULL PRIMARY KEY, name NVARCHAR(100) NOT NULL)",
    )
    .await?;
    sql_exec(&mut admin, "USE rustcdc_test; DELETE FROM dbo.users").await?;
    sql_exec_with_retry(
        &mut admin,
        "USE rustcdc_test; IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0 EXEC sys.sp_cdc_enable_db",
    )
    .await?;
    sql_exec_with_retry(
        &mut admin,
        "USE rustcdc_test; IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.users')) EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='users', @role_name=NULL, @supports_net_changes=0",
    )
    .await?;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let config = sqlserver_testkit::source_config(host.clone(), port, "rustcdc_test".into(), 30);

    let mut source = SqlServerConnection::new(config.clone());
    source.connect().await?;
    let mut stream = source.start_stream(None).await?;

    // initial change batch
    sql_exec(
        &mut admin,
        "USE rustcdc_test; INSERT INTO dbo.users (id, name) VALUES (1, 'alice'), (2, 'bob')",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_test; UPDATE dbo.users SET name='alice_v2' WHERE id=1",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_test; DELETE FROM dbo.users WHERE id=2",
    )
    .await?;

    let events = collect_events_with_deadline(
        &mut *stream,
        &mut admin,
        "rustcdc_test",
        std::time::Duration::from_secs(90),
        true,
    )
    .await?;

    let has_insert = events
        .iter()
        .any(|event| event.op == rustcdc::Operation::Insert);
    let has_update = events
        .iter()
        .any(|event| event.op == rustcdc::Operation::Update);
    let has_delete = events
        .iter()
        .any(|event| event.op == rustcdc::Operation::Delete);

    assert!(
        has_insert && has_update && has_delete,
        "expected insert/update/delete in initial stream batch, got {} events",
        events.len()
    );

    let mut checkpoint = InMemoryCheckpoint::default();
    stream.save_position(&mut checkpoint).await?;
    let resume: Box<dyn rustcdc::Offset> = checkpoint
        .load()
        .await?
        .ok_or_else(|| rustcdc::Error::CheckpointError("missing stream checkpoint".into()))?;

    source.close().await;

    // restart from checkpoint
    let mut resumed_source = SqlServerConnection::new(config);
    resumed_source.connect().await?;
    let mut resumed_stream = resumed_source.start_stream(Some(resume.as_ref())).await?;

    sql_exec(
        &mut admin,
        "USE rustcdc_test; INSERT INTO dbo.users (id, name) VALUES (3, 'carol')",
    )
    .await?;

    let resumed_events = collect_events_with_deadline(
        &mut *resumed_stream,
        &mut admin,
        "rustcdc_test",
        std::time::Duration::from_secs(90),
        false,
    )
    .await?;

    assert!(
        !resumed_events.is_empty(),
        "expected resumed sqlserver stream to emit events within deadline"
    );
    assert!(
        resumed_events.iter().any(|event| event
            .after
            .as_ref()
            .and_then(|after| after.get("id"))
            .map(|value| value.to_string())
            == Some("3".into())),
        "expected resumed stream to include insert id=3"
    );

    resumed_source.close().await;
    Ok(())
}

#[tokio::test]
async fn sqlserver_stream_emits_schema_change_for_capture_metadata_refresh() -> rustcdc::Result<()>
{
    const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

    tokio::time::timeout(TEST_TIMEOUT, async {
        run_sqlserver_stream_emits_schema_change_for_capture_metadata_refresh().await
    })
    .await
    .map_err(|_| {
        rustcdc::Error::TimeoutError(
            "sqlserver schema-change integration exceeded 300s timeout".to_string(),
        )
    })?
}

async fn run_sqlserver_stream_emits_schema_change_for_capture_metadata_refresh(
) -> rustcdc::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver schema-change integration test") {
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

    sql_exec(
        &mut admin,
        "IF DB_ID('rustcdc_schema_change') IS NULL CREATE DATABASE rustcdc_schema_change",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_schema_change; IF OBJECT_ID('dbo.users', 'U') IS NULL CREATE TABLE dbo.users (id INT NOT NULL PRIMARY KEY, name NVARCHAR(100) NOT NULL)",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_schema_change; DELETE FROM dbo.users",
    )
    .await?;
    sql_exec_with_retry(
        &mut admin,
        "USE rustcdc_schema_change; IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0 EXEC sys.sp_cdc_enable_db",
    )
    .await?;
    sql_exec_with_retry(
        &mut admin,
        "USE rustcdc_schema_change; IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.users')) EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='users', @role_name=NULL, @supports_net_changes=0",
    )
    .await?;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let config =
        sqlserver_testkit::source_config(host.clone(), port, "rustcdc_schema_change".into(), 30);

    let mut source = SqlServerConnection::new(config);
    source.connect().await?;
    let mut stream = source.start_stream(None).await?;

    // Trigger capture metadata drift by changing source table and recreating capture instance.
    sql_exec_with_retry(
        &mut admin,
        "USE rustcdc_schema_change; ALTER TABLE dbo.users ADD email NVARCHAR(255) NULL",
    )
    .await?;
    sql_exec_with_retry(
        &mut admin,
        "USE rustcdc_schema_change; EXEC sys.sp_cdc_disable_table @source_schema='dbo', @source_name='users', @capture_instance='dbo_users'",
    )
    .await?;
    sql_exec_with_retry(
        &mut admin,
        "USE rustcdc_schema_change; EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='users', @role_name=NULL, @supports_net_changes=0",
    )
    .await?;

    let events = collect_events_with_deadline(
        stream.as_mut(),
        &mut admin,
        "rustcdc_schema_change",
        std::time::Duration::from_secs(90),
        false,
    )
    .await?;

    assert!(
        events
            .iter()
            .any(|event| event.op == Operation::SchemaChange),
        "expected schema_change event after SQL Server capture metadata refresh"
    );

    source.close().await;
    Ok(())
}
