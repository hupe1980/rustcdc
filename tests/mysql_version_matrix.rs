#![cfg(feature = "mysql")]

use cdc_rs::{source::Source, MysqlConnection, MysqlSourceConfig};
use cdc_rs::TransportConfig;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::time::{sleep, Duration};

fn skip_mysql_version_matrix_case(case_label: &str) -> bool {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!(
            "skipping {case_label} (set CDC_RS_RUN_DOCKER_TESTS=1 for docker-backed integration tests)"
        );
        true
    } else {
        false
    }
}

async fn connect_with_retry(connection: &MysqlConnection) -> cdc_rs::Result<()> {
    let mut last_error = None;
    for _ in 0..45 {
        match connection.connect().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        cdc_rs::Error::SourceError("mysql connection did not become ready in time".into())
    }))
}

async fn run_mysql_connection_lifecycle(version: &str, server_id: u32) -> cdc_rs::Result<()> {
    let container = GenericImage::new("mysql", version)
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

    let config = MysqlSourceConfig {
        host: host.to_string(),
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    };

    let connection = MysqlConnection::new(config);
    connect_with_retry(&connection).await?;
    assert_eq!(connection.source_type(), "mysql");
    assert!(connection.is_connected().await);
    connection.close().await;
    Ok(())
}

macro_rules! mysql_connection_test {
    ($name:ident, $version:literal, $server_id:literal, $label:literal) => {
        #[tokio::test]
        async fn $name() -> cdc_rs::Result<()> {
            if skip_mysql_version_matrix_case($label) {
                return Ok(());
            }
            run_mysql_connection_lifecycle($version, $server_id).await
        }
    };
}

mysql_connection_test!(
    mysql_connection_8_0_matrix,
    "8.0",
    301,
    "mysql 8.0 version matrix connection test"
);

mysql_connection_test!(
    mysql_connection_8_4_matrix,
    "8.4",
    302,
    "mysql 8.4 version matrix connection test"
);

#[tokio::test]
async fn mysql_capabilities_are_consistent_in_matrix_profile() -> cdc_rs::Result<()> {
    if skip_mysql_version_matrix_case("mysql capability matrix test") {
        return Ok(());
    }

    let container = GenericImage::new("mysql", "8.4")
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

    let config = MysqlSourceConfig {
        host: host.to_string(),
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id: 303,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    };

    let connection = MysqlConnection::new(config);
    connect_with_retry(&connection).await?;

    let caps = connection.capabilities();
    assert!(caps.snapshot);
    assert!(caps.snapshot_checkpoint_resume);
    assert!(caps.handoff);
    assert!(caps.ddl_capture);
    assert!(caps.heartbeat);
    assert!(caps.schema_introspection);

    connection.close().await;
    Ok(())
}
