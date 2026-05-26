use std::time::Duration;

use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::core::{Error, Result};

use super::{SqlClient, SqlServerPrereqSnapshot, SqlServerSourceConfig};

pub(super) async fn connect_client(config: &SqlServerSourceConfig) -> Result<SqlClient> {
    let tcp = tokio::time::timeout(
        Duration::from_secs(config.conn_timeout_secs),
        TcpStream::connect((config.host.as_str(), config.port)),
    )
    .await
    .map_err(|_| {
        Error::TimeoutError(format!(
            "sqlserver connection to {}:{} timed out",
            config.host, config.port
        ))
    })?
    .map_err(|error| Error::SourceError(format!("sqlserver tcp connect failed: {error}")))?;
    tcp.set_nodelay(true)
        .map_err(|error| Error::SourceError(format!("sqlserver tcp setup failed: {error}")))?;

    tiberius::Client::connect(config.to_tiberius_config()?, tcp.compat_write())
        .await
        .map_err(|error| Error::SourceError(format!("sqlserver authentication failed: {error}")))
}

pub(super) async fn query_bool(
    client: &mut SqlClient,
    operation: &str,
    query: &str,
) -> Result<bool> {
    Ok(query_i32(client, operation, query).await? != 0)
}

pub(super) async fn query_u32(client: &mut SqlClient, operation: &str, query: &str) -> Result<u32> {
    let value = query_i32(client, operation, query).await?;
    u32::try_from(value).map_err(|_| {
        Error::SourceError(format!(
            "sqlserver prerequisite operation '{operation}' returned unexpected negative value: {value}"
        ))
    })
}

pub(super) async fn query_i32(client: &mut SqlClient, operation: &str, query: &str) -> Result<i32> {
    let rows = client
        .query(query, &[])
        .await
        .map_err(|error| Error::SourceError(format!("sqlserver query failed: {error}")))?
        .into_first_result()
        .await
        .map_err(|error| Error::SourceError(format!("sqlserver result decode failed: {error}")))?;

    let row = rows.into_iter().next().ok_or_else(|| {
        Error::SourceError(format!(
            "sqlserver operation '{operation}' returned no rows"
        ))
    })?;
    row.get::<i32, _>(0).ok_or_else(|| {
        Error::SourceError(format!("sqlserver operation '{operation}' returned NULL"))
    })
}

pub(super) fn validate_prereq_snapshot(
    config: &SqlServerSourceConfig,
    snapshot: &SqlServerPrereqSnapshot,
) -> Result<()> {
    if config.cdc_enabled && !snapshot.cdc_enabled {
        return Err(Error::SourceError(
            "sqlserver CDC is disabled on target database".into(),
        ));
    }
    if !snapshot.has_cdc_admin_role {
        return Err(Error::SourceError(
            "sqlserver user is missing CDC admin role (requires db_owner/db_ddladmin/sysadmin)"
                .into(),
        ));
    }
    if snapshot.major_version < 13 {
        return Err(Error::SourceError(format!(
            "sqlserver version {} is not supported; requires SQL Server 2016+",
            snapshot.major_version
        )));
    }
    Ok(())
}
