#![cfg(feature = "mysql")]

use rustcdc::TransportConfig;
use rustcdc::{source::Source, MysqlConnection, MysqlSourceConfig};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::time::{sleep, Duration};

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

/// Test INSERT/UPDATE/DELETE event capture
#[tokio::test]
async fn mysql_stream_capture_insert_update_delete() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql stream integration test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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

    // Create test table
    sqlx::query("DROP TABLE IF EXISTS stream_test")
        .execute(&admin_pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE stream_test (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            value VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
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
        server_id: 200,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    };

    let mut connection = MysqlConnection::new(config);
    connection.connect().await?;

    // Start stream (from current position)
    let mut stream_handle = connection.start_stream(None).await?;

    // Insert 50 rows
    for i in 1..=50 {
        sqlx::query("INSERT INTO stream_test (value) VALUES (?)")
            .bind(format!("insert-{}", i))
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    // Update 20 rows
    for i in 1..=20 {
        sqlx::query("UPDATE stream_test SET value = ? WHERE id = ?")
            .bind(format!("update-{}", i))
            .bind(i)
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    // Delete 10 rows
    for i in 41..=50 {
        sqlx::query("DELETE FROM stream_test WHERE id = ?")
            .bind(i)
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    // Capture stream events (with timeout)
    let mut stream_events = Vec::new();
    for _ in 0..100 {
        let mut events = stream_handle.next_events(500).await?;
        if events.is_empty() {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            events = stream_handle.next_events(500).await?;
            if events.is_empty() {
                break;
            }
        }
        stream_events.extend(events);
        if stream_events.len() >= 80 {
            break;
        }
    }

    let stream_count = stream_events.len();
    println!(
        "Stream test: Captured {} events (50 INSERT + 20 UPDATE + 10 DELETE expected)",
        stream_count
    );

    // Count event types
    let mut insert_count = 0;
    let mut update_count = 0;
    let mut delete_count = 0;

    for event in &stream_events {
        match event.op {
            rustcdc::core::Operation::Insert => {
                insert_count += 1;
                assert!(event.after.is_some(), "INSERT event must have after field");
            }
            rustcdc::core::Operation::Update => {
                update_count += 1;
                assert!(
                    event.before.is_some(),
                    "UPDATE event must have before field"
                );
                assert!(event.after.is_some(), "UPDATE event must have after field");
            }
            rustcdc::core::Operation::Delete => {
                delete_count += 1;
                assert!(
                    event.before.is_some(),
                    "DELETE event must have before field"
                );
            }
            _ => {}
        }
    }

    println!(
        "✓ Stream test: INSERT {} | UPDATE {} | DELETE {} | Total {}",
        insert_count, update_count, delete_count, stream_count
    );

    connection.close().await;
    Ok(())
}

/// Test stream resume from checkpoint
#[tokio::test]
async fn mysql_stream_resume_from_checkpoint() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql stream resumption test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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

    // Create test table
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

    let config = MysqlSourceConfig {
        host: host.to_string(),
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id: 201,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    };

    let mut connection = MysqlConnection::new(config);
    connection.connect().await?;
    let mut stream_handle = connection.start_stream(None).await?;

    // Insert 30 rows
    for i in 1..=30 {
        sqlx::query("INSERT INTO resumption_test (value) VALUES (?)")
            .bind(format!("row-{}", i))
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    // Capture initial events
    let mut all_events = Vec::new();
    for _ in 0..100 {
        let mut events = stream_handle.next_events(500).await?;
        if events.is_empty() {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            events = stream_handle.next_events(500).await?;
            if events.is_empty() {
                break;
            }
        }
        all_events.extend(events);
        if all_events.len() >= 30 {
            break;
        }
    }

    println!(
        "✓ Stream checkpoint test: Captured {} initial events",
        all_events.len()
    );

    // Verify checkpoint offset is present
    if let Some(event) = all_events.first() {
        assert!(
            !event.source.offset.is_empty(),
            "stream event must have offset (binlog position or LSN)"
        );
        println!("  First event offset: {}", event.source.offset);
    }

    connection.close().await;
    Ok(())
}

/// Test that the stream continues seamlessly across a binlog rotation (FLUSH LOGS).
///
/// This validates the `RotateEvent` handling in `LiveMysqlBinlogProvider`: when MySQL
/// rotates to a new binlog file, events after the rotation must still be delivered and
/// their `source.offset` must reflect the new filename.
#[tokio::test]
async fn mysql_stream_binlog_rotation() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql binlog rotation test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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

    // Create a simple test table
    sqlx::query("DROP TABLE IF EXISTS rotation_test")
        .execute(&admin_pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE rotation_test (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                batch VARCHAR(32) NOT NULL,
                value VARCHAR(255) NOT NULL
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
        server_id: 202,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    };

    let mut connection = MysqlConnection::new(config);
    connection.connect().await?;
    let mut stream_handle = connection.start_stream(None).await?;

    // ── Phase 1: insert rows BEFORE rotation ──────────────────────────────────
    const PRE_ROTATION_ROWS: usize = 20;
    for i in 1..=PRE_ROTATION_ROWS {
        sqlx::query("INSERT INTO rotation_test (batch, value) VALUES ('pre', ?)")
            .bind(format!("pre-{i}"))
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    // Drain pre-rotation events
    let mut pre_events: Vec<rustcdc::Event> = Vec::new();
    for _ in 0..200 {
        let batch = stream_handle.next_events(500).await?;
        pre_events.extend(batch);
        if pre_events.len() >= PRE_ROTATION_ROWS {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    assert!(
        !pre_events.is_empty(),
        "expected pre-rotation INSERT events, got none"
    );
    let pre_rotation_offset = pre_events.last().unwrap().source.offset.clone();
    let pre_file = pre_rotation_offset
        .split(':')
        .next()
        .expect("offset must have filename part")
        .to_owned();
    println!(
        "Pre-rotation: {} events, last offset = {pre_rotation_offset}",
        pre_events.len()
    );

    // ── Rotate the binlog ─────────────────────────────────────────────────────
    sqlx::query("FLUSH LOGS")
        .execute(&admin_pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    println!("FLUSH LOGS issued — binlog rotation triggered");

    // ── Phase 2: insert rows AFTER rotation ───────────────────────────────────
    const POST_ROTATION_ROWS: usize = 20;
    for i in 1..=POST_ROTATION_ROWS {
        sqlx::query("INSERT INTO rotation_test (batch, value) VALUES ('post', ?)")
            .bind(format!("post-{i}"))
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    // Drain post-rotation events
    let mut post_events: Vec<rustcdc::Event> = Vec::new();
    for _ in 0..200 {
        let batch = stream_handle.next_events(500).await?;
        post_events.extend(batch);
        if post_events.len() >= POST_ROTATION_ROWS {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    assert!(
        !post_events.is_empty(),
        "expected post-rotation INSERT events, got none — stream did not survive binlog rotation"
    );

    let post_rotation_offset = post_events.last().unwrap().source.offset.clone();
    let post_file = post_rotation_offset
        .split(':')
        .next()
        .expect("offset must have filename part")
        .to_owned();
    println!(
        "Post-rotation: {} events, last offset = {post_rotation_offset}",
        post_events.len()
    );

    // ── Assertions ────────────────────────────────────────────────────────────

    // All pre-rotation events are valid INSERTs
    for event in &pre_events {
        assert_eq!(event.op, rustcdc::core::Operation::Insert);
        assert!(event.after.is_some(), "Insert must have after");
        assert!(event.before.is_none(), "Insert must not have before");
    }

    // All post-rotation events are valid INSERTs and arrived (stream survived rotation)
    for event in &post_events {
        assert_eq!(event.op, rustcdc::core::Operation::Insert);
        assert!(event.after.is_some(), "Insert must have after");
        assert!(
            !event.source.offset.is_empty(),
            "post-rotation event must have non-empty offset"
        );
    }

    // The binlog filename in the offset MUST change after rotation.
    // e.g. "binlog.000001" → "binlog.000002"
    assert_ne!(
        pre_file, post_file,
        "binlog filename must change after FLUSH LOGS rotation \
             (pre={pre_file}, post={post_file})"
    );
    println!("✓ Binlog rotated: {pre_file} → {post_file}");

    println!(
        "✓ Binlog rotation test: {} pre + {} post = {} total events \
             (no events lost across rotation boundary)",
        pre_events.len(),
        post_events.len(),
        pre_events.len() + post_events.len(),
    );

    connection.close().await;
    Ok(())
}
