#![cfg(feature = "sqlserver")]

use rustcdc::{source::Source, SqlServerConnection, SqlServerSourceConfig};
use std::time::Duration;
use tokio::time::sleep;

#[path = "sqlserver_testkit.rs"]
mod sqlserver_testkit;

fn skip_sqlserver_version_matrix_case(case_label: &str) -> bool {
    sqlserver_testkit::skip_docker_test(case_label)
}

async fn sqlserver_base_config(
    image_tag: &str,
    database: &str,
) -> rustcdc::Result<(
    SqlServerSourceConfig,
    testcontainers::ContainerAsync<testcontainers::GenericImage>,
)> {
    let container = sqlserver_testkit::start_sqlserver_container(image_tag).await?;
    let (host, port) = sqlserver_testkit::host_and_port(&container).await?;
    sqlserver_testkit::enable_cdc(&host, port, database).await?;

    let config = sqlserver_testkit::source_config(host, port, database.to_string(), 60);

    Ok((config, container))
}

async fn run_sqlserver_connection_lifecycle(image_tag: &str) -> rustcdc::Result<()> {
    let database = "rustcdc_connection";
    let (config, _container) = sqlserver_base_config(image_tag, database).await?;
    let connection = SqlServerConnection::new(config);
    connect_with_retry(&connection).await?;
    assert!(connection.is_connected().await);
    connection.close().await;
    Ok(())
}

macro_rules! sqlserver_connection_test {
    ($name:ident, $image_tag:literal, $label:literal) => {
        #[tokio::test]
        async fn $name() -> rustcdc::Result<()> {
            if skip_sqlserver_version_matrix_case($label) {
                return Ok(());
            }

            run_sqlserver_connection_lifecycle($image_tag).await
        }
    };
}

async fn connect_with_retry(connection: &SqlServerConnection) -> rustcdc::Result<()> {
    let mut last_error = None;
    for _ in 0..60 {
        match connection.connect().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        rustcdc::Error::SourceError("sqlserver connection did not become ready in time".into())
    }))
}

sqlserver_connection_test!(
    sqlserver_connection_2019,
    "2019-latest",
    "sqlserver 2019 connection integration test"
);

sqlserver_connection_test!(
    sqlserver_connection_2022,
    "2022-latest",
    "sqlserver 2022 connection integration test"
);

/// Verify CDC connector capabilities are consistent across SQL Server versions
#[tokio::test]
async fn sqlserver_cdc_capabilities_are_consistent() -> rustcdc::Result<()> {
    if skip_sqlserver_version_matrix_case("sqlserver cdc capabilities test") {
        return Ok(());
    }

    let database = "rustcdc_connection";
    let (config, _container) = sqlserver_base_config("2022-latest", database).await?;

    let connection = SqlServerConnection::new(config);
    connect_with_retry(&connection).await?;

    let capabilities = connection.capabilities();
    assert!(capabilities.snapshot, "SQL Server should support snapshots");
    assert!(
        capabilities.handoff,
        "SQL Server should support snapshot-to-stream handoff"
    );
    assert!(
        capabilities.ddl_capture,
        "SQL Server DDL capture should be enabled"
    );
    assert!(
        capabilities.heartbeat,
        "SQL Server should support heartbeats"
    );
    assert!(
        capabilities.schema_introspection,
        "SQL Server should support schema introspection"
    );

    connection.close().await;
    Ok(())
}
