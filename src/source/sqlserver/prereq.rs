use std::sync::Arc;

use async_trait::async_trait;

use crate::core::{Error, Result};

use super::{query, SqlServerSourceConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SqlServerPrereqSnapshot {
    pub(super) cdc_enabled: bool,
    pub(super) has_cdc_admin_role: bool,
    pub(super) major_version: u32,
}

#[async_trait]
pub(super) trait SqlServerPrereqProbe: Send + Sync {
    async fn probe(&self, config: &SqlServerSourceConfig) -> Result<SqlServerPrereqSnapshot>;
    async fn heartbeat(&self, config: &SqlServerSourceConfig) -> Result<()>;
}

pub(super) struct LiveSqlServerPrereqProbe {
    connection_slots: Arc<tokio::sync::Semaphore>,
}

impl LiveSqlServerPrereqProbe {
    pub(super) fn new(pool_size: usize) -> Self {
        Self {
            connection_slots: Arc::new(tokio::sync::Semaphore::new(pool_size.max(1))),
        }
    }
}

#[async_trait]
impl SqlServerPrereqProbe for LiveSqlServerPrereqProbe {
    async fn probe(&self, config: &SqlServerSourceConfig) -> Result<SqlServerPrereqSnapshot> {
        let _permit = self.connection_slots.acquire().await.map_err(|_| {
            Error::StateError("sqlserver connection pool semaphore was closed".into())
        })?;
        let mut client = query::connect_client(config).await?;

        let cdc_enabled = query::query_bool(
            &mut client,
            "cdc-enabled probe",
            "SELECT CAST(is_cdc_enabled AS INT) FROM sys.databases WHERE name = DB_NAME()",
        )
        .await?;
        let has_cdc_admin_role = query::query_bool(
            &mut client,
            "cdc-admin-role probe",
            "SELECT CASE WHEN IS_SRVROLEMEMBER('sysadmin') = 1 OR IS_ROLEMEMBER('db_owner') = 1 OR IS_ROLEMEMBER('db_ddladmin') = 1 THEN 1 ELSE 0 END",
        )
        .await?;
        let major_version = query::query_u32(
            &mut client,
            "server-version probe",
            "SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS INT)",
        )
        .await?;

        Ok(SqlServerPrereqSnapshot {
            cdc_enabled,
            has_cdc_admin_role,
            major_version,
        })
    }

    async fn heartbeat(&self, config: &SqlServerSourceConfig) -> Result<()> {
        let _permit = self.connection_slots.acquire().await.map_err(|_| {
            Error::StateError("sqlserver connection pool semaphore was closed".into())
        })?;
        let mut client = query::connect_client(config).await?;
        let _ = query::query_u32(&mut client, "heartbeat", "SELECT 1").await?;
        Ok(())
    }
}
