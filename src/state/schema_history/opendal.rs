//! OpenDAL-backed schema-history backend (Redis, PostgreSQL).
//!
//! Schema history is stored as an opaque JSON blob under the well-known key
//! `schema_history` inside the configured OpenDAL operator namespace.
//!
//! The adapter delegates to a local `FileSchemaHistory` mirror so that
//! standard file-based persistence remains the source of truth for crash
//! recovery, while the OpenDAL layer provides replication to a remote store.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use opendal::Operator;
use rustcdc::core::{Error as RtError, Result as RtResult};
use rustcdc::schema_history::{
    DDLEvent, FileSchemaHistory, SchemaHistory, SchemaHistoryRetention, TableSchema,
};
use tokio::sync::Mutex;

use crate::config::schema::{PostgresStateConfig, RedisStateConfig};
use crate::error::AppError;

// ─────────────────────────────────────────────────────────────────────────────
// Key constants
// ─────────────────────────────────────────────────────────────────────────────

const KEY_SCHEMA_HISTORY: &str = "schema_history";

// ─────────────────────────────────────────────────────────────────────────────
// OpenDAL schema history adapter
// ─────────────────────────────────────────────────────────────────────────────

/// OpenDAL-backed schema history.  Delegates all storage to a local
/// `FileSchemaHistory` mirror and replicates changes to the remote store
/// by re-reading the mirror file after each mutation.
pub(super) struct OpenDalSchemaHistory {
    op: Arc<Mutex<Operator>>,
    mirror: FileSchemaHistory,
    mirror_path: PathBuf,
}

impl OpenDalSchemaHistory {
    fn new(op: Arc<Mutex<Operator>>, mirror: FileSchemaHistory, mirror_path: PathBuf) -> Self {
        Self {
            op,
            mirror,
            mirror_path,
        }
    }

    async fn flush_to_remote(&self) -> Result<(), RtError> {
        let bytes = tokio::fs::read(&self.mirror_path).await.map_err(|e| {
            RtError::StateError(format!(
                "read schema history mirror '{}': {e}",
                self.mirror_path.display()
            ))
        })?;

        let op = self.op.lock().await;
        op.write(KEY_SCHEMA_HISTORY, bytes).await.map_err(|e| {
            crate::state::metrics::mark_opendal_write_failure();
            RtError::StateError(format!("OpenDAL schema history write: {e}"))
        })?;

        Ok(())
    }
}

#[async_trait]
impl SchemaHistory for OpenDalSchemaHistory {
    async fn record_ddl(&mut self, ddl: DDLEvent) -> RtResult<u32> {
        let version = self.mirror.record_ddl(ddl).await?;
        self.flush_to_remote().await?;
        Ok(version)
    }

    async fn get_schema_at_version(
        &self,
        table: &str,
        version: u32,
    ) -> RtResult<Option<TableSchema>> {
        self.mirror.get_schema_at_version(table, version).await
    }

    async fn get_schema_at_timestamp(&self, table: &str, ts: u64) -> RtResult<Option<TableSchema>> {
        self.mirror.get_schema_at_timestamp(table, ts).await
    }

    async fn latest_schema(&self, table: &str) -> RtResult<Option<TableSchema>> {
        self.mirror.latest_schema(table).await
    }

    async fn apply_retention(&mut self, retention: SchemaHistoryRetention) -> RtResult<usize> {
        let removed = self.mirror.apply_retention(retention).await?;
        if removed > 0 {
            self.flush_to_remote().await?;
        }
        Ok(removed)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Build helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Build an OpenDAL-backed schema history from a pre-configured operator.
///
/// If existing schema history is present in the remote store it is restored
/// to the local mirror file before constructing the runtime handle.
pub(super) async fn build_schema_history(
    op: Operator,
    mirror_dir: &Path,
) -> Result<OpenDalSchemaHistory, AppError> {
    std::fs::create_dir_all(mirror_dir).map_err(AppError::from)?;

    let shared_op = Arc::new(Mutex::new(op));

    // ── Restore schema history mirror from remote ──────────────────────────
    let schema_history_file = mirror_dir.join("schema_history.json");

    {
        let op = shared_op.lock().await;
        match op.read(KEY_SCHEMA_HISTORY).await {
            Ok(buf) => {
                tokio::fs::write(&schema_history_file, buf.to_bytes().as_ref())
                    .await
                    .map_err(AppError::from)?;
            }
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(AppError::Other(format!("OpenDAL schema history read: {e}")));
            }
        }
    }

    let file_schema_history = FileSchemaHistory::new(&schema_history_file)
        .await
        .map_err(|e| AppError::Other(format!("failed to open schema history mirror: {e}")))?;

    Ok(OpenDalSchemaHistory::new(
        shared_op,
        file_schema_history,
        schema_history_file,
    ))
}

/// Build an OpenDAL [`Operator`] for a Redis endpoint.
pub(super) fn build_redis_op(config: &RedisStateConfig) -> Result<Operator, AppError> {
    let url = config.url.resolve().map_err(|e| {
        AppError::Other(format!(
            "failed to resolve Redis URL from secret reference: {e}"
        ))
    })?;
    let mut builder = opendal::services::Redis::default()
        .endpoint(url.trim())
        .db(config.db)
        .root(&config.key_root);

    if let Some(username) = &config.username {
        builder = builder.username(username);
    }
    if let Some(password) = &config.password {
        let pw = password.resolve().map_err(|e| {
            AppError::Other(format!(
                "failed to resolve Redis password from secret reference: {e}"
            ))
        })?;
        builder = builder.password(pw.trim());
    }

    Ok(Operator::new(builder)
        .map_err(|e| AppError::Other(format!("failed to build OpenDAL Redis operator: {e}")))?
        .finish())
}

/// Build an OpenDAL [`Operator`] for a PostgreSQL endpoint.
pub(super) fn build_postgresql_op(config: &PostgresStateConfig) -> Result<Operator, AppError> {
    let url = config.url.resolve().map_err(|e| {
        AppError::Other(format!(
            "state.backend.postgresql.url could not be resolved: {e}"
        ))
    })?;
    let builder = opendal::services::Postgresql::default()
        .connection_string(&url)
        .table(&config.checkpoint_table)
        .root("/");

    Ok(Operator::new(builder)
        .map_err(|e| AppError::Other(format!("failed to build OpenDAL PostgreSQL operator: {e}")))?
        .finish())
}
