use std::time::Duration;

use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::core::{Error, Result};

use super::{SqlClient, SqlServerPrereqSnapshot, SqlServerSourceConfig};

/// A row returned by the truncate-event shadow table poll.
pub(super) struct TruncateEventRow {
    pub(super) id: i64,
    pub(super) schema_name: String,
    pub(super) table_name: String,
    pub(super) max_lsn_bytes: Option<[u8; 10]>,
    pub(super) ts_ms: u64,
}

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

/// Create the truncate-event shadow table and DDL trigger if they do not yet
/// exist in the given CDC schema.
///
/// Both objects are created idempotently: repeated calls are safe.  The DDL
/// trigger fires synchronously during each `TRUNCATE TABLE` statement and
/// records the affected table together with the current CDC maximum LSN.
pub(super) async fn ensure_truncate_capture_setup(
    client: &mut SqlClient,
    cdc_schema: &str,
) -> Result<()> {
    // Validate cdc_schema contains only safe identifier characters.
    if !cdc_schema
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(Error::ConfigError(format!(
            "sqlserver cdc_schema '{cdc_schema}' contains characters that are not safe for SQL identifiers"
        )));
    }

    let create_table = format!(
        "IF NOT EXISTS (
            SELECT 1 FROM sys.objects
            WHERE object_id = OBJECT_ID(N'[{cdc_schema}].[rustcdc_truncate_events]')
              AND type = N'U'
        )
        BEGIN
            CREATE TABLE [{cdc_schema}].[rustcdc_truncate_events] (
                id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
                schema_name         NVARCHAR(128) NOT NULL,
                table_name          NVARCHAR(128) NOT NULL,
                event_time          DATETIME2(7)  NOT NULL DEFAULT SYSUTCDATETIME(),
                max_lsn_at_truncate VARBINARY(10) NULL,
                consumed            BIT           NOT NULL DEFAULT 0
            )
        END"
    );

    let create_trigger = format!(
        "IF NOT EXISTS (
            SELECT 1 FROM sys.triggers
            WHERE parent_class = 0
              AND name = N'rustcdc_truncate_capture'
        )
        BEGIN
            EXEC sp_executesql N'
                CREATE TRIGGER [rustcdc_truncate_capture]
                ON DATABASE
                FOR TRUNCATE_TABLE
                AS
                BEGIN
                    SET NOCOUNT ON;
                    DECLARE @data   XML          = EVENTDATA();
                    DECLARE @schema NVARCHAR(128) = @data.value(
                        ''(/EVENT_INSTANCE/SchemaName)[1]'', ''NVARCHAR(128)'');
                    DECLARE @table  NVARCHAR(128) = @data.value(
                        ''(/EVENT_INSTANCE/ObjectName)[1]'', ''NVARCHAR(128)'');
                    INSERT INTO [{cdc_schema}].[rustcdc_truncate_events]
                        (schema_name, table_name, max_lsn_at_truncate)
                    VALUES
                        (@schema, @table, sys.fn_cdc_get_max_lsn());
                END
            ';
        END"
    );

    client
        .execute(&create_table, &[])
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "sqlserver truncate capture setup (create table) failed: {error}"
            ))
        })?;

    client
        .execute(&create_trigger, &[])
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "sqlserver truncate capture setup (create trigger) failed: {error}"
            ))
        })?;

    Ok(())
}

/// Fetch all unconsumed truncate events whose captured LSN is within
/// `[zero, lsn_end_hex]`.  Returns raw rows; callers must mark consumed after
/// delivery.
pub(super) async fn fetch_pending_truncate_events(
    client: &mut SqlClient,
    cdc_schema: &str,
    lsn_end_hex: &str,
) -> Result<Vec<TruncateEventRow>> {
    if !cdc_schema
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(Error::ConfigError(format!(
            "sqlserver cdc_schema '{cdc_schema}' contains characters that are not safe for SQL identifiers"
        )));
    }

    // Convert ms timestamp from DATETIME2 epoch.
    // DATEDIFF_BIG returns milliseconds since 1970-01-01 UTC.
    let sql = format!(
        "SELECT id,
                schema_name,
                table_name,
                max_lsn_at_truncate,
                DATEDIFF_BIG(MILLISECOND, '1970-01-01', event_time) AS ts_ms
         FROM [{cdc_schema}].[rustcdc_truncate_events]
         WHERE consumed = 0
           AND (max_lsn_at_truncate IS NULL
                OR sys.fn_varbintohexstr(max_lsn_at_truncate) <= '{lsn_end_hex}')
         ORDER BY max_lsn_at_truncate, id"
    );

    let rows = client
        .query(&sql, &[])
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "sqlserver truncate-event poll failed: {error}"
            ))
        })?
        .into_first_result()
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "sqlserver truncate-event poll decode failed: {error}"
            ))
        })?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i64 = row.get::<i64, _>(0).ok_or_else(|| {
            Error::SourceError("sqlserver truncate event row missing id".into())
        })?;
        let schema_name: String = row
            .get::<&str, _>(1)
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                Error::SourceError("sqlserver truncate event row missing schema_name".into())
            })?;
        let table_name: String = row
            .get::<&str, _>(2)
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                Error::SourceError("sqlserver truncate event row missing table_name".into())
            })?;
        let max_lsn_bytes: Option<[u8; 10]> = row
            .get::<&[u8], _>(3)
            .and_then(|b| b.try_into().ok());
        let ts_ms: u64 = row
            .get::<i64, _>(4)
            .and_then(|v| u64::try_from(v).ok())
            .unwrap_or_default();
        out.push(TruncateEventRow {
            id,
            schema_name,
            table_name,
            max_lsn_bytes,
            ts_ms,
        });
    }
    Ok(out)
}

/// Mark a batch of truncate shadow-table rows as consumed by ID.
pub(super) async fn mark_truncate_events_consumed(
    client: &mut SqlClient,
    cdc_schema: &str,
    ids: &[i64],
) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    if !cdc_schema
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(Error::ConfigError(format!(
            "sqlserver cdc_schema '{cdc_schema}' contains characters that are not safe for SQL identifiers"
        )));
    }

    let id_list: String = ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "UPDATE [{cdc_schema}].[rustcdc_truncate_events]
         SET consumed = 1
         WHERE id IN ({id_list})"
    );
    client.execute(&sql, &[]).await.map_err(|error| {
        Error::SourceError(format!(
            "sqlserver truncate-event mark-consumed failed: {error}"
        ))
    })?;
    Ok(())
}

/// Delete consumed truncate shadow-table rows older than 24 hours to prevent
/// unbounded table growth.
pub(super) async fn cleanup_consumed_truncate_events(
    client: &mut SqlClient,
    cdc_schema: &str,
) -> Result<()> {
    if !cdc_schema
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(Error::ConfigError(format!(
            "sqlserver cdc_schema '{cdc_schema}' contains characters that are not safe for SQL identifiers"
        )));
    }

    let sql = format!(
        "DELETE FROM [{cdc_schema}].[rustcdc_truncate_events]
         WHERE consumed = 1
           AND event_time < DATEADD(HOUR, -24, SYSUTCDATETIME())"
    );
    client.execute(&sql, &[]).await.map_err(|error| {
        Error::SourceError(format!(
            "sqlserver truncate-event cleanup failed: {error}"
        ))
    })?;
    Ok(())
}
