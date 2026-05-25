#![cfg(feature = "sqlserver")]

use rustcdc::{SqlServerSourceConfig, TransportConfig};
use std::time::Duration;
use testcontainers::{
    core::IntoContainerPort, runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt,
};
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_util::compat::TokioAsyncWriteCompatExt;

pub const SQLSERVER_SA_PASSWORD: &str = "StrongPass!123";
pub type SqlClient = tiberius::Client<tokio_util::compat::Compat<TcpStream>>;

pub fn skip_docker_test(case_label: &str) -> bool {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() == Ok("1") {
        return false;
    }

    eprintln!("skipping {case_label} (set CDC_RS_RUN_DOCKER_TESTS=1)");
    true
}

pub async fn start_sqlserver_container(
    image_tag: &str,
) -> rustcdc::Result<ContainerAsync<GenericImage>> {
    GenericImage::new("mcr.microsoft.com/mssql/server", image_tag)
        .with_exposed_port(1433.tcp())
        .with_env_var("ACCEPT_EULA", "Y")
        .with_env_var("MSSQL_SA_PASSWORD", SQLSERVER_SA_PASSWORD)
        .with_env_var("MSSQL_PID", "Developer")
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))
}

pub async fn host_and_port(
    container: &ContainerAsync<GenericImage>,
) -> rustcdc::Result<(String, u16)> {
    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(1433.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    Ok((host, port))
}

#[allow(dead_code)]
pub fn source_config(
    host: String,
    port: u16,
    database: String,
    conn_timeout_secs: u64,
) -> SqlServerSourceConfig {
    SqlServerSourceConfig {
        host,
        port,
        user: "sa".to_string(),
        password: SQLSERVER_SA_PASSWORD.to_string().into(),
        database,
        instance_name: None,
        transport: TransportConfig::tls(),
        conn_timeout_secs,
        cdc_enabled: true,
        cdc_schema: "cdc".into(),
        prereq_pool_size: 2,
        stream_poll_interval_ms: 250,
        max_events_per_poll: 5_000,
        ..Default::default()
    }
}

pub async fn connect_admin(host: &str, port: u16) -> rustcdc::Result<SqlClient> {
    let mut config = tiberius::Config::new();
    config.host(host);
    config.port(port);
    config.database("master");
    config.authentication(tiberius::AuthMethod::sql_server(
        "sa",
        SQLSERVER_SA_PASSWORD,
    ));
    config.trust_cert();
    config.encryption(tiberius::EncryptionLevel::NotSupported);

    let tcp = TcpStream::connect((host, port))
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    tcp.set_nodelay(true)
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    tiberius::Client::connect(config, tcp.compat_write())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))
}

pub async fn connect_admin_with_retry(
    host: &str,
    port: u16,
    attempts: usize,
    delay: Duration,
) -> rustcdc::Result<SqlClient> {
    let mut last_error = None;
    for _ in 0..attempts {
        match connect_admin(host, port).await {
            Ok(client) => return Ok(client),
            Err(error) => {
                last_error = Some(error);
                sleep(delay).await;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        rustcdc::Error::SourceError(
            "sqlserver admin connection did not become ready in time".into(),
        )
    }))
}

#[allow(dead_code)]
pub async fn enable_cdc(host: &str, port: u16, database: &str) -> rustcdc::Result<()> {
    let mut client = connect_admin_with_retry(host, port, 60, Duration::from_millis(500)).await?;

    let create_db_sql = format!("IF DB_ID('{database}') IS NULL CREATE DATABASE {database}");
    sql_exec_with_retry(&mut client, &create_db_sql).await?;

    let enable_cdc_sql = format!(
        "USE {database}; IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0 EXEC sys.sp_cdc_enable_db"
    );
    sql_exec_with_retry(&mut client, &enable_cdc_sql).await?;

    let validate_sql = format!(
        "USE {database}; SELECT CAST(is_cdc_enabled AS INT) FROM sys.databases WHERE name = DB_NAME()"
    );
    let rows = client
        .query(&validate_sql, &[])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .into_first_result()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| rustcdc::Error::SourceError("missing SQL Server CDC status row".into()))?;
    let cdc_enabled = row
        .get::<i32, _>(0)
        .ok_or_else(|| rustcdc::Error::SourceError("missing SQL Server CDC status value".into()))?;
    if cdc_enabled != 1 {
        return Err(rustcdc::Error::SourceError(
            "sqlserver CDC was not enabled after setup".into(),
        ));
    }

    Ok(())
}

#[allow(dead_code)]
pub async fn sql_exec(client: &mut SqlClient, sql: &str) -> rustcdc::Result<()> {
    client
        .execute(sql, &[])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    Ok(())
}

#[allow(dead_code)]
pub async fn sql_exec_with_retry(client: &mut SqlClient, sql: &str) -> rustcdc::Result<()> {
    const MAX_ATTEMPTS: usize = 8;

    for attempt in 1..=MAX_ATTEMPTS {
        match sql_exec(client, sql).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                let message = error.to_string().to_ascii_lowercase();
                let is_deadlock =
                    message.contains("deadlock victim") || message.contains("code: 1205");
                if is_deadlock && attempt < MAX_ATTEMPTS {
                    sleep(Duration::from_millis(500)).await;
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
