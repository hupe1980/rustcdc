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
    async fn record_ddl(&mut self, ddl_id: &str, ddl: DDLEvent) -> RtResult<u32> {
        // `ddl_id` is the source log position the DDL was captured at, and recording is
        // idempotent on it: a DDL redelivered under at-least-once replay returns the
        // version it already had. Passing it straight through is the whole job here — the
        // mirror owns the identity check, and swallowing the id would make a replayed
        // `AlterTableDiff` re-apply its operations to a schema that already has them,
        // which fails the poll identically on every subsequent restart.
        let version = self.mirror.record_ddl(ddl_id, ddl).await?;
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
    // `schema_history_table`, not `checkpoint_table`.
    //
    // This read `checkpoint_table`, so the configured `schema_history_table` — declared in
    // the schema, defaulted to a distinct name and documented in the reference — was never
    // used, and both artifacts shared the checkpoint's table. Nothing corrupted (the two
    // keys differ), but the setting silently did nothing, and the two artifacts have
    // genuinely different lifecycles: truncating the checkpoint table to force a
    // re-snapshot is a routine recovery step, and doing it also destroyed the schema
    // history that MySQL and SQL Server need to decode their logs at all.
    let builder = opendal::services::Postgresql::default()
        .connection_string(&url)
        .table(&config.schema_history_table)
        .root("/");

    Ok(Operator::new(builder)
        .map_err(|e| AppError::Other(format!("failed to build OpenDAL PostgreSQL operator: {e}")))?
        .finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::schema::PostgresStateConfig;
    use rustcdc::SecretString;

    /// The schema-history operator must target the **schema-history** table.
    ///
    /// It was built against `checkpoint_table`, so the configured `schema_history_table`
    /// did nothing and both artifacts shared one table. A config round-trip test cannot
    /// catch that — the value parses either way; the operator is where the setting either
    /// takes effect or does not, so that is what this asserts.
    ///
    /// `opendal` builds lazily, so this needs no database.
    #[test]
    fn the_schema_history_operator_targets_the_schema_history_table() {
        let config = PostgresStateConfig {
            url: SecretString::new("postgres://u:p@localhost:5432/db"),
            checkpoint_table: "cp_table".to_string(),
            schema_history_table: "sh_table".to_string(),
        };

        let schema_history = build_postgresql_op(&config).expect("schema history operator");
        let checkpoint = crate::state::offset::opendal::build_postgresql_operator(&config)
            .expect("checkpoint operator");

        let schema_history_name = schema_history.info().name().to_string();
        let checkpoint_name = checkpoint.info().name().to_string();

        assert_eq!(
            schema_history_name, "sh_table",
            "the schema-history operator must use schema_history_table"
        );
        assert_eq!(
            checkpoint_name, "cp_table",
            "the checkpoint operator must use checkpoint_table"
        );
        assert_ne!(
            schema_history_name, checkpoint_name,
            "sharing one table means truncating the checkpoint to force a re-snapshot \
             also destroys the schema history MySQL and SQL Server need"
        );
    }
}
