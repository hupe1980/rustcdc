#![cfg(feature = "sqlserver")]

use rustcdc::{source::Source, SqlServerConnection, SqlServerSourceConfig};

#[path = "sqlserver_testkit.rs"]
mod sqlserver_testkit;

#[tokio::test]
async fn sqlserver_connection_2019_and_cdc_validation() -> rustcdc::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver connection integration test") {
        return Ok(());
    }

    let container = sqlserver_testkit::start_sqlserver_container("2019-latest").await?;
    let (host_text, port) = sqlserver_testkit::host_and_port(&container).await?;
    let database = "rustcdc_connection";

    // Retry to absorb SQL Server startup warm-up.
    let mut last_error = None;
    for _ in 0..40 {
        if let Err(error) = sqlserver_testkit::enable_cdc(&host_text, port, database).await {
            last_error = Some(error);
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        }

        let config: SqlServerSourceConfig =
            sqlserver_testkit::source_config(host_text.clone(), port, database.into(), 30);

        let connection = SqlServerConnection::new(config);
        match connection.connect().await {
            Ok(()) => {
                assert_eq!(connection.source_type(), "sqlserver");
                assert!(connection.is_connected().await);
                connection.close().await;
                return Ok(());
            }
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        rustcdc::Error::SourceError(
            "sqlserver connection test timed out while waiting for container readiness".into(),
        )
    }))
}
