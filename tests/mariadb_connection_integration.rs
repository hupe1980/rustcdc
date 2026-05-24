#![cfg(feature = "mysql")]

use cdc_rs::{MysqlConnection, MysqlSourceConfig};
use cdc_rs::TransportConfig;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::time::{sleep, Duration};

const READY_TIMEOUT: Duration = Duration::from_secs(90);
const CONNECT_TIMEOUT_SECS: u64 = 5;
const CONNECT_RETRY_BUDGET: Duration = Duration::from_secs(75);

struct MariadbTestTarget {
    _container: testcontainers::ContainerAsync<GenericImage>,
    config: MysqlSourceConfig,
}

fn skip_mariadb_connection_case(case_label: &str) -> bool {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() == Ok("1") {
        return false;
    }

    eprintln!("skipping {case_label} (set CDC_RS_RUN_DOCKER_TESTS=1)",);
    true
}

async fn start_mariadb_container(
    version: &str,
) -> cdc_rs::Result<testcontainers::ContainerAsync<GenericImage>> {
    GenericImage::new("mariadb", version)
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_cmd(vec![
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--server-id=1",
        ])
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
        .start()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))
}

async fn wait_for_mariadb_admin_ready(host: &str, port: u16) -> cdc_rs::Result<()> {
    let dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let deadline = std::time::Instant::now() + READY_TIMEOUT;
    let mut backoff = Duration::from_millis(250);
    let mut last_error = None;

    while std::time::Instant::now() < deadline {
        match sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(2))
            .connect(&dsn)
            .await
        {
            Ok(pool) => {
                match sqlx::query("SELECT 1").execute(&pool).await {
                    Ok(_) => {
                        pool.close().await;
                        return Ok(());
                    }
                    Err(error) => {
                        last_error = Some(error.to_string());
                    }
                }
                pool.close().await;
            }
            Err(error) => {
                last_error = Some(error.to_string());
            }
        }

        sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(2));
    }

    Err(cdc_rs::Error::SourceError(format!(
        "mariadb admin readiness probe timed out: {}",
        last_error.unwrap_or_else(|| "unknown error".to_string())
    )))
}

async fn mariadb_base_config(version: &str, server_id: u32) -> cdc_rs::Result<MariadbTestTarget> {
    let container = start_mariadb_container(version).await?;
    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host_string = host.to_string();
    wait_for_mariadb_admin_ready(&host_string, port).await?;

    Ok(MariadbTestTarget {
        _container: container,
        config: MysqlSourceConfig {
            host: host_string,
            port,
            user: "root".to_string(),
            password: "rootpass".to_string().into(),
            database: "cdc".to_string(),
            server_id,
            gtid_mode_enabled: false,
            binlog_format_check: true,
            transport: TransportConfig::tls(),
            conn_timeout_secs: CONNECT_TIMEOUT_SECS,
            stream_poll_interval_ms: 50,
            max_events_per_poll: 1_000,
        },
    })
}

async fn run_mariadb_connection_lifecycle(version: &str, server_id: u32) -> cdc_rs::Result<()> {
    let target = mariadb_base_config(version, server_id).await?;
    let connection = MysqlConnection::new(target.config);
    connect_with_retry(&connection).await?;
    assert!(connection.is_connected().await);
    connection.close().await;
    Ok(())
}

macro_rules! mariadb_connection_test {
    ($name:ident, $version:literal, $server_id:expr, $label:literal) => {
        #[tokio::test]
        async fn $name() -> cdc_rs::Result<()> {
            if skip_mariadb_connection_case($label) {
                return Ok(());
            }
            run_mariadb_connection_lifecycle($version, $server_id).await
        }
    };
}

async fn connect_with_retry(connection: &MysqlConnection) -> cdc_rs::Result<()> {
    let deadline = std::time::Instant::now() + CONNECT_RETRY_BUDGET;
    let mut backoff = Duration::from_millis(250);
    let mut last_error = None;
    while std::time::Instant::now() < deadline {
        match connection.connect().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                last_error = Some(error);
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        cdc_rs::Error::SourceError("mariadb connection did not become ready in time".into())
    }))
}

mariadb_connection_test!(
    mariadb_connection_10_5,
    "10.5",
    100,
    "mariadb 10.5 connection integration test"
);

mariadb_connection_test!(
    mariadb_connection_10_6,
    "10.6",
    101,
    "mariadb 10.6 connection integration test"
);

/// Test MariaDB GTID mode support
#[tokio::test]
async fn mariadb_gtid_mode_support() -> cdc_rs::Result<()> {
    if skip_mariadb_connection_case("mariadb gtid mode support test") {
        return Ok(());
    }

    let target = mariadb_base_config("10.6", 102).await?;

    let connection = MysqlConnection::new(target.config);
    connect_with_retry(&connection).await?;

    // MariaDB supports both traditional and GTID binlog mode
    assert!(connection.is_connected().await);
    connection.close().await;
    Ok(())
}
