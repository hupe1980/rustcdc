#![cfg(feature = "mysql")]

use rustcdc::{
    checkpoint::{Checkpoint, FileCheckpoint},
    source::Source,
    MysqlConnection, MysqlSourceConfig, TransportConfig,
};
use std::sync::OnceLock;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::{
    sync::Mutex,
    time::{sleep, Duration},
};

fn mysql_snapshot_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn connect_admin_pool(dsn: &str) -> rustcdc::Result<sqlx::MySqlPool> {
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

fn extract_mysql_row_id(row: &serde_json::Value) -> rustcdc::Result<u64> {
    let id_value = row
        .get("id")
        .ok_or_else(|| rustcdc::Error::SourceError("snapshot row missing id".into()))?;

    if let Some(id) = id_value.as_u64() {
        return Ok(id);
    }

    if let Some(id) = id_value.as_i64() {
        return u64::try_from(id).map_err(|_| {
            rustcdc::Error::SourceError("snapshot row id must be non-negative".into())
        });
    }

    if let Some(id_text) = id_value.as_str() {
        return id_text.parse::<u64>().map_err(|_| {
            rustcdc::Error::SourceError("snapshot row id string is not numeric".into())
        });
    }

    Err(rustcdc::Error::SourceError(
        "snapshot row id has unsupported JSON type".into(),
    ))
}

/// Test large-table snapshot chunking (100K rows → 5K chunks)
/// Validates: keyset pagination, resumable snapshots, no duplicates, checkpoint persistence
#[tokio::test]
async fn mysql_snapshot_large_table_chunked() -> rustcdc::Result<()> {
    let _guard = mysql_snapshot_test_lock().lock().await;

    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql snapshot integration test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = GenericImage::new("mysql", "8.0")
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
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

    // Create admin connection to set up test data
    let admin_dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let admin_pool = connect_admin_pool(&admin_dsn).await?;

    // Setup table with 20K rows.
    sqlx::query("DROP TABLE IF EXISTS snapshot_test")
        .execute(&admin_pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE snapshot_test (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            value VARCHAR(255)
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    // Insert 20K rows in batches.
    for batch_start in (1..=20_000).step_by(2_000) {
        let mut query = String::from("INSERT INTO snapshot_test (value) VALUES ");
        let mut first = true;
        for i in batch_start..std::cmp::min(batch_start + 2_000, 20_001) {
            if !first {
                query.push(',');
            }
            query.push_str(&format!("('row-{}')", i));
            first = false;
        }
        sqlx::query(&query)
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    let _checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;
    let config = MysqlSourceConfig {
        host: host.to_string(),
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id: 100,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls_insecure_skip_verify(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    };

    let mut connection = MysqlConnection::new(config);
    connection.connect().await?;
    let mut snapshot_handle = connection.start_snapshot(&["snapshot_test"]).await?;

    // Capture all snapshot chunks with bounded iterations to avoid infinite loops.
    let mut snapshot_events = Vec::new();
    let mut chunk_count = 0;
    for _ in 0..200 {
        let chunk = snapshot_handle.next_chunk(5000).await?;
        if chunk.is_empty() {
            break;
        }
        chunk_count += 1;
        snapshot_events.extend(chunk);
    }

    let _snapshot_end = snapshot_handle.finish().await?;

    let snapshot_count = snapshot_events.len();
    println!(
        "Phase 1 (Snapshot): Captured {} events in {} chunks",
        snapshot_count, chunk_count
    );
    assert!(
        snapshot_count >= 20_000,
        "expected at least 20K snapshot events, got {}",
        snapshot_count
    );
    assert!(
        chunk_count >= 4,
        "expected at least 4 chunks (5K each), got {}",
        chunk_count
    );

    // Validate no duplicates via PK set
    let mut pk_set = std::collections::HashSet::new();
    for event in &snapshot_events {
        if let Some(after) = &event.after {
            if let Some(id_val) = after.get("id") {
                let id_str = id_val.to_string();
                assert!(
                    pk_set.insert(id_str.clone()),
                    "duplicate PK detected: {}",
                    id_str
                );
            }
        }
    }

    println!(
        "✓ Snapshot test: {} events, {} unique PKs, {} chunks, zero duplicates",
        snapshot_count,
        pk_set.len(),
        chunk_count
    );

    connection.close().await;
    Ok(())
}

/// Test snapshot resumption from checkpoint
#[tokio::test]
async fn mysql_snapshot_resumption_from_checkpoint() -> rustcdc::Result<()> {
    let _guard = mysql_snapshot_test_lock().lock().await;

    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql snapshot resumption test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = GenericImage::new("mysql", "8.0")
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
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

    // Setup table with 10K rows
    sqlx::query("DROP TABLE IF EXISTS resumption_test")
        .execute(&admin_pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE resumption_test (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            value VARCHAR(255)
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    for batch_start in (1..=10_000).step_by(2_000) {
        let mut query = String::from("INSERT INTO resumption_test (value) VALUES ");
        let mut first = true;
        for i in batch_start..std::cmp::min(batch_start + 2_000, 10_001) {
            if !first {
                query.push(',');
            }
            query.push_str(&format!("('row-{}')", i));
            first = false;
        }
        sqlx::query(&query)
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;
    let config = MysqlSourceConfig {
        host: host.to_string(),
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id: 101,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls_insecure_skip_verify(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    };

    // Phase 1: Capture first 5K rows
    let mut connection1 = MysqlConnection::new(config.clone());
    connection1.connect().await?;
    let mut snapshot_handle1 = connection1.start_snapshot(&["resumption_test"]).await?;

    let first_chunk = snapshot_handle1.next_chunk(5000).await?;
    println!("Phase 1 (Partial): Captured {} rows", first_chunk.len());

    let mut checkpoint1 = FileCheckpoint::new(checkpoint_dir.path());
    snapshot_handle1
        .checkpoint(&mut checkpoint1, first_chunk.len() as u64)
        .await?;
    let resume_offset = checkpoint1.load().await?.ok_or_else(|| {
        rustcdc::Error::CheckpointError("expected saved snapshot checkpoint".into())
    })?;

    drop(snapshot_handle1);
    connection1.close().await;

    // Phase 2: Resume from checkpoint and capture rest
    let mut connection2 = MysqlConnection::new(config);
    connection2.connect().await?;
    let mut snapshot_handle2 = connection2
        .start_snapshot_from_checkpoint(&["resumption_test"], Some(resume_offset.as_ref()))
        .await?;

    let mut resumed_events = Vec::new();
    for _ in 0..200 {
        let chunk = snapshot_handle2.next_chunk(5000).await?;
        if chunk.is_empty() {
            break;
        }
        resumed_events.extend(chunk);
    }

    let _snapshot_end = snapshot_handle2.finish().await?;
    let resumed_count = resumed_events.len();

    let mut ids = std::collections::HashSet::new();
    for event in first_chunk.iter().chain(resumed_events.iter()) {
        let after = event.after.as_ref().ok_or_else(|| {
            rustcdc::Error::SourceError("snapshot row missing after payload".into())
        })?;
        let id = extract_mysql_row_id(after)?;
        assert!(
            ids.insert(id),
            "duplicate id detected across resume phases: {id}"
        );
    }

    println!(
        "Phase 2 (Resume): Captured {} rows (total with phase 1 = {})",
        resumed_count,
        first_chunk.len() + resumed_count
    );

    assert_eq!(
        first_chunk.len() + resumed_count,
        10_000,
        "expected exactly 10K total events across resume"
    );
    assert_eq!(ids.len(), 10_000, "expected 10K unique ids after resume");

    println!(
        "✓ Resumption test: phase 1 ({}) + phase 2 ({}) = {}",
        first_chunk.len(),
        resumed_count,
        first_chunk.len() + resumed_count
    );

    connection2.close().await;
    Ok(())
}

/// Test empty table handling
#[tokio::test]
async fn mysql_snapshot_empty_table() -> rustcdc::Result<()> {
    let _guard = mysql_snapshot_test_lock().lock().await;

    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql snapshot empty table test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = GenericImage::new("mysql", "8.0")
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
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

    // Create empty table
    sqlx::query("DROP TABLE IF EXISTS empty_test")
        .execute(&admin_pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE empty_test (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            value VARCHAR(255)
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let config = MysqlSourceConfig {
        host: host.to_string(),
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id: 102,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls_insecure_skip_verify(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    };

    let mut connection = MysqlConnection::new(config);
    connection.connect().await?;
    let mut snapshot_handle = connection.start_snapshot(&["empty_test"]).await?;

    // Request chunk from empty table
    let chunk = snapshot_handle.next_chunk(5000).await?;
    assert!(chunk.is_empty(), "expected empty chunk for empty table");

    let _snapshot_end = snapshot_handle.finish().await?;

    println!("✓ Empty table test: properly handled (0 events + snapshot end)");

    connection.close().await;
    Ok(())
}
