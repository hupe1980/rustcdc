use async_trait::async_trait;
use tokio_postgres::Client;

use crate::core::{Error, Result};

use super::PostgresSourceConfig;

pub(super) async fn validate_connected_postgres_client(
    config: &PostgresSourceConfig,
    client: &Client,
) -> Result<()> {
    let backend = LiveValidationBackend { client };
    validate_with_backend(config, &backend).await
}

pub(super) async fn validate_with_backend(
    config: &PostgresSourceConfig,
    backend: &dyn ValidationBackend,
) -> Result<()> {
    if !backend
        .replication_slot_exists(&config.replication_slot_name)
        .await?
    {
        backend
            .create_replication_slot(&config.replication_slot_name)
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "postgres replication slot validation failed for '{}': {error}",
                    config.replication_slot_name
                ))
            })?;
    }

    if !backend.publication_exists(&config.publication_name).await? {
        return Err(Error::SourceError(format!(
            "postgres publication '{}' not found",
            config.publication_name
        )));
    }

    if !backend.has_replication_privilege().await? {
        return Err(Error::SourceError(
            "postgres user lacks REPLICATION privilege".into(),
        ));
    }

    Ok(())
}

#[async_trait]
pub(super) trait ValidationBackend: Send + Sync {
    async fn replication_slot_exists(&self, slot_name: &str) -> Result<bool>;
    async fn create_replication_slot(&self, slot_name: &str) -> Result<()>;
    async fn publication_exists(&self, publication_name: &str) -> Result<bool>;
    async fn has_replication_privilege(&self) -> Result<bool>;
}

struct LiveValidationBackend<'a> {
    client: &'a Client,
}

#[async_trait]
impl ValidationBackend for LiveValidationBackend<'_> {
    async fn replication_slot_exists(&self, slot_name: &str) -> Result<bool> {
        let row = self
            .client
            .query_opt(
                "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&slot_name],
            )
            .await
            .map_err(|error| {
                Error::SourceError(format!("failed to inspect replication slots: {error}"))
            })?;
        Ok(row.is_some())
    }

    async fn create_replication_slot(&self, slot_name: &str) -> Result<()> {
        self.client
            .query_one(
                "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot_name],
            )
            .await
            .map_err(|error| Error::SourceError(format!("failed to create replication slot: {error}")))?;
        Ok(())
    }

    async fn publication_exists(&self, publication_name: &str) -> Result<bool> {
        let row = self
            .client
            .query_opt(
                "SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1",
                &[&publication_name],
            )
            .await
            .map_err(|error| {
                Error::SourceError(format!("failed to inspect publications: {error}"))
            })?;
        Ok(row.is_some())
    }

    async fn has_replication_privilege(&self) -> Result<bool> {
        let row = self
            .client
            .query_one(
                "SELECT rolreplication FROM pg_catalog.pg_roles WHERE rolname = current_user",
                &[],
            )
            .await
            .map_err(|error| {
                Error::SourceError(format!("failed to inspect role privileges: {error}"))
            })?;
        Ok(row.get::<usize, bool>(0))
    }
}
