#![cfg(feature = "mysql")]

use rustcdc::TransportConfig;
use rustcdc::{source::Source, MysqlConnection, MysqlSourceConfig};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::time::{sleep, Duration};

async fn connect_with_retry(connection: &MysqlConnection) -> rustcdc::Result<()> {
    let mut last_error = None;
    for _ in 0..30 {
        match connection.connect().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        rustcdc::Error::SourceError("mysql connection did not become ready in time".into())
    }))
}

/// Test MySQL 8.0 connection lifecycle
#[tokio::test]
async fn mysql_connection_8_0() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql connection integration test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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

    // Test connection
    let connection = MysqlConnection::new(config);
    connect_with_retry(&connection).await?;
    assert_eq!(connection.source_type(), "mysql");

    // Verify connection is alive
    assert!(connection.is_connected().await);

    // Test clean close
    connection.close().await;

    println!("✓ MySQL 8.0 connection test passed");
    Ok(())
}

/// Test MySQL 8.1 connection lifecycle
#[tokio::test]
async fn mysql_connection_8_1() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql connection integration test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = GenericImage::new("mysql", "8.1")
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

    // Test connection with GTID validation
    let connection = MysqlConnection::new(config);
    connect_with_retry(&connection).await?;
    assert_eq!(connection.source_type(), "mysql");

    // Verify connection is alive
    assert!(connection.is_connected().await);

    // Test clean close
    connection.close().await;

    println!("✓ MySQL 8.1 connection test passed");
    Ok(())
}

/// Test config validation: no credentials logged
#[test]
fn mysql_config_debug_no_credentials() {
    let config = MysqlSourceConfig {
        host: "localhost".to_string(),
        port: 3306,
        user: "testuser".to_string(),
        password: "secretpass".to_string().into(),
        database: "testdb".to_string(),
        server_id: 1,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls_insecure_skip_verify(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    };

    let debug_str = format!("{:?}", config);
    assert!(
        !debug_str.contains("secretpass"),
        "password should not be in debug output"
    );
    assert!(
        debug_str.contains("***redacted***"),
        "password should be redacted"
    );
    println!("✓ Config credentials properly redacted");
}

/// Test MysqlSourceConfig defaults
#[test]
fn mysql_config_defaults() {
    let config = MysqlSourceConfig::default();
    assert_eq!(config.host, "localhost");
    assert_eq!(config.port, 3306);
    assert_eq!(config.server_id, 1);
    assert!(!config.gtid_mode_enabled);
    assert!(config.binlog_format_check);
    assert_eq!(config.conn_timeout_secs, 30);
    println!("✓ MySQL config defaults correct");
}
