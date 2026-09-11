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
        // A missing slot is only safe to create when the caller has explicitly opted
        // in. Creating it unconditionally on every `connect()` turns "the slot was
        // dropped" — by an operator, by a failover to a replica that never had it, or
        // by `max_slot_wal_keep_size` invalidation — into a silent restart at the
        // *current* WAL position, permanently discarding every change since the last
        // confirmed_flush_lsn. That is indistinguishable from normal operation.
        if !config.create_replication_slot_if_missing {
            return Err(Error::Unrecoverable(format!(
                "postgres replication slot '{}' does not exist. rustcdc will not create it \
                 automatically, because a slot that disappeared after the stream was already \
                 running would be recreated at the current WAL position — silently skipping \
                 every change written since the last confirmed position. \
                 If this is first-time provisioning, set \
                 `PostgresSourceConfig::create_replication_slot_if_missing = true` (or create \
                 the slot out of band with \
                 `SELECT pg_create_logical_replication_slot('{}', 'pgoutput')`). \
                 If the pipeline was previously running, the slot was dropped or invalidated: \
                 the WAL it was retaining is gone, and the affected tables must be \
                 re-snapshotted from a fresh checkpoint.",
                config.replication_slot_name, config.replication_slot_name
            )));
        }

        backend
            .create_replication_slot(&config.replication_slot_name, config.failover_slot)
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
    async fn create_replication_slot(&self, slot_name: &str, failover: bool) -> Result<()>;
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

    async fn create_replication_slot(&self, slot_name: &str, failover: bool) -> Result<()> {
        if failover {
            // PostgreSQL 17+ only. Signature is positional:
            //   pg_create_logical_replication_slot(slot_name, plugin,
            //                                      temporary, twophase, failover)
            // so `temporary` and `twophase` must be supplied explicitly to reach the
            // 5th argument. A failover-enabled slot is synchronized to standbys, which
            // is what lets logical replication resume after a promotion instead of
            // losing the slot — and with it every change since the last confirmed LSN.
            self.client
                .query_one(
                    "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot(\
                     $1, 'pgoutput', false, false, true)",
                    &[&slot_name],
                )
                .await
                .map_err(|error| {
                    Error::SourceError(format!(
                        "failed creating failover-enabled replication slot '{slot_name}': \
                         {error}. Failover slots require PostgreSQL 17 or later; on an older \
                         server set `failover_slot = false` (and accept that the slot is lost \
                         on promotion, requiring a re-snapshot)."
                    ))
                })?;
            return Ok(());
        }

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
