//! Schema history abstractions and backends.

use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::core::{Error, Event, Result, ValidationError};
use crate::ddl_capture::{SchemaDiff, SchemaDiffOperation};

/// A single column definition captured from schema history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name as reported by the source schema.
    pub name: String,
    /// Source-declared logical data type.
    pub data_type: String,
    /// Whether the column accepts null values.
    pub nullable: bool,
    /// Additional column-level constraints such as primary key markers.
    pub constraints: Vec<String>,
}

/// Full schema snapshot for a table at a specific version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    /// Schema or namespace name.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Ordered column definitions.
    pub columns: Vec<ColumnDef>,
    /// Primary key column names.
    pub primary_keys: Vec<String>,
    /// Monotonic schema version assigned by the history store.
    pub version: u32,
}

/// DDL changes recorded by the schema-history store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DDLEvent {
    /// A new table definition.
    CreateTable(TableSchema),
    /// A schema evolution event that supersedes the previous version.
    AlterTable(TableSchema),
    /// A schema evolution event represented as an ordered diff over the previous version.
    AlterTableDiff {
        schema: String,
        table: String,
        diff: SchemaDiff,
    },
    /// Table removal.
    DropTable { schema: String, table: String },
}

/// Abstraction for recording and querying table schema history.
#[async_trait]
pub trait SchemaHistory: Send + Sync {
    /// Record a DDL change and return the resulting schema version.
    async fn record_ddl(&mut self, ddl: DDLEvent) -> Result<u32>;
    /// Look up a schema by version.
    async fn get_schema_at_version(&self, table: &str, version: u32)
        -> Result<Option<TableSchema>>;
    /// Look up the most recent schema at or before a timestamp.
    async fn get_schema_at_timestamp(&self, table: &str, ts: u64) -> Result<Option<TableSchema>>;
    /// Return the latest known schema for a table.
    async fn latest_schema(&self, table: &str) -> Result<Option<TableSchema>>;
    /// Apply retention policy and prune old history entries.
    async fn apply_retention(&mut self, retention: SchemaHistoryRetention) -> Result<usize>;
}

/// Retention policy for schema-history pruning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaHistoryRetention {
    /// Maximum number of historical versions retained per table key.
    pub max_versions_per_table: usize,
}

impl SchemaHistoryRetention {
    /// Create a policy retaining only the latest `max_versions_per_table` entries.
    pub fn keep_last(max_versions_per_table: usize) -> Result<Self> {
        if max_versions_per_table == 0 {
            return Err(Error::ConfigError(
                "schema history retention max_versions_per_table must be greater than zero"
                    .into(),
            ));
        }
        Ok(Self {
            max_versions_per_table,
        })
    }
}

type SchemaStore = HashMap<String, Vec<VersionedSchema>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VersionedSchema {
    version: u32,
    recorded_at: u64,
    schema: Option<TableSchema>,
}

/// In-memory schema-history implementation for tests and local development.
#[derive(Debug, Clone, Default)]
pub struct InMemorySchemaHistory {
    schemas: Arc<RwLock<SchemaStore>>,
}

/// Durable schema-history backend persisted to a local JSON file.
///
/// `FileSchemaHistory` uses write-rename persistence with file and directory fsync
/// to provide crash-safe single-process durability semantics.
#[derive(Debug, Clone)]
pub struct FileSchemaHistory {
    path: Arc<PathBuf>,
    file_mode: u32,
    schemas: Arc<RwLock<SchemaStore>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct FileSchemaHistoryState {
    schemas: SchemaStore,
}

fn table_key(schema: &str, table: &str) -> String {
    format!("{schema}.{table}")
}

fn current_time() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn next_version(store: &SchemaStore, key: &str) -> u32 {
    store
        .get(key)
        .and_then(|entries| entries.last().map(|entry| entry.version + 1))
        .unwrap_or(1)
}

fn record_ddl_in_store(store: &mut SchemaStore, ddl: DDLEvent, timestamp: u64) -> Result<u32> {
    match ddl {
        DDLEvent::CreateTable(mut schema) | DDLEvent::AlterTable(mut schema) => {
            let key = table_key(&schema.schema, &schema.table);
            let version = next_version(store, &key);
            schema.version = version;
            store.entry(key).or_default().push(VersionedSchema {
                version,
                recorded_at: timestamp,
                schema: Some(schema),
            });
            Ok(version)
        }
        DDLEvent::AlterTableDiff {
            schema,
            table,
            diff,
        } => {
            let key = table_key(&schema, &table);
            let version = next_version(store, &key);
            let mut next_schema = store
                .get(&key)
                .and_then(|entries| entries.last())
                .and_then(|entry| entry.schema.clone())
                .ok_or_else(|| {
                    Error::SchemaError(format!(
                        "cannot apply ALTER TABLE diff to unknown table '{key}'"
                    ))
                })?;

            apply_schema_diff(&mut next_schema, &diff)?;
            next_schema.version = version;

            store.entry(key).or_default().push(VersionedSchema {
                version,
                recorded_at: timestamp,
                schema: Some(next_schema),
            });
            Ok(version)
        }
        DDLEvent::DropTable { schema, table } => {
            let key = table_key(&schema, &table);
            let version = next_version(store, &key);
            store.entry(key).or_default().push(VersionedSchema {
                version,
                recorded_at: timestamp,
                schema: None,
            });
            Ok(version)
        }
    }
}

fn schema_at_version(store: &SchemaStore, table: &str, version: u32) -> Option<TableSchema> {
    store
        .get(table)
        .and_then(|entries| entries.iter().find(|entry| entry.version == version))
        .and_then(|entry| entry.schema.clone())
}

fn schema_at_timestamp(store: &SchemaStore, table: &str, ts: u64) -> Option<TableSchema> {
    store
        .get(table)
        .and_then(|entries| entries.iter().rev().find(|entry| entry.recorded_at <= ts))
        .and_then(|entry| entry.schema.clone())
}

fn latest_schema_for_table(store: &SchemaStore, table: &str) -> Option<TableSchema> {
    store
        .get(table)
        .and_then(|entries| entries.last())
        .and_then(|entry| entry.schema.clone())
}

fn apply_store_retention(store: &mut SchemaStore, retention: SchemaHistoryRetention) -> usize {
    let mut removed = 0usize;

    for entries in store.values_mut() {
        if entries.len() > retention.max_versions_per_table {
            let trim_count = entries.len() - retention.max_versions_per_table;
            entries.drain(0..trim_count);
            removed = removed.saturating_add(trim_count);
        }
    }

    removed
}

impl FileSchemaHistory {
    const DEFAULT_FILE_MODE: u32 = 0o600;
    const TEMP_FILE_ATTEMPTS: u32 = 8;

    /// Create a durable schema-history backend stored at `path`.
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let schemas = if path.exists() {
            Self::load_store(&path)?
        } else {
            HashMap::new()
        };

        Ok(Self {
            path: Arc::new(path),
            file_mode: Self::DEFAULT_FILE_MODE,
            schemas: Arc::new(RwLock::new(schemas)),
        })
    }

    fn load_store(path: &Path) -> Result<SchemaStore> {
        let bytes = fs::read(path)?;
        if bytes.is_empty() {
            return Ok(HashMap::new());
        }

        let state: FileSchemaHistoryState = serde_json::from_slice(&bytes).map_err(|error| {
            Error::SerializationError(format!(
                "failed to parse schema history file '{}': {error}",
                path.display()
            ))
        })?;

        Ok(state.schemas)
    }

    fn persist_store(&self, store: &SchemaStore) -> Result<()> {
        let state = FileSchemaHistoryState {
            schemas: store.clone(),
        };

        let bytes = serde_json::to_vec_pretty(&state).map_err(|error| {
            Error::SerializationError(format!(
                "failed to serialize schema history for '{}': {error}",
                self.path.display()
            ))
        })?;

        let (tmp_path, mut file) = self.create_temp_file()?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);

        fs::rename(&tmp_path, self.path.as_path())?;

        if let Some(parent) = self.path.parent() {
            fs::File::open(parent)?.sync_all()?;
        }

        Ok(())
    }

    fn create_temp_file(&self) -> Result<(PathBuf, fs::File)> {
        for _ in 0..Self::TEMP_FILE_ATTEMPTS {
            let tmp_path = Self::temp_path(self.path.as_path());
            let file_result = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&tmp_path);

            match file_result {
                Ok(file) => {
                    self.apply_file_mode(&file)?;
                    return Ok((tmp_path, file));
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        }

        Err(Error::SchemaError(format!(
            "failed to create unique schema history temp file after {} attempts",
            Self::TEMP_FILE_ATTEMPTS
        )))
    }

    fn apply_file_mode(&self, file: &fs::File) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            file.set_permissions(fs::Permissions::from_mode(self.file_mode))?;
        }

        Ok(())
    }

    fn temp_path(path: &Path) -> PathBuf {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();

        let mut tmp = path.to_path_buf();
        let ext = path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("json");
        tmp.set_extension(format!("{ext}.{stamp}.tmp"));
        tmp
    }
}

#[async_trait]
impl SchemaHistory for InMemorySchemaHistory {
    async fn record_ddl(&mut self, ddl: DDLEvent) -> Result<u32> {
        let mut store = self.schemas.write().await;
        record_ddl_in_store(&mut store, ddl, current_time())
    }

    async fn get_schema_at_version(
        &self,
        table: &str,
        version: u32,
    ) -> Result<Option<TableSchema>> {
        let store = self.schemas.read().await;
        Ok(schema_at_version(&store, table, version))
    }

    async fn get_schema_at_timestamp(&self, table: &str, ts: u64) -> Result<Option<TableSchema>> {
        let store = self.schemas.read().await;
        Ok(schema_at_timestamp(&store, table, ts))
    }

    async fn latest_schema(&self, table: &str) -> Result<Option<TableSchema>> {
        let store = self.schemas.read().await;
        Ok(latest_schema_for_table(&store, table))
    }

    async fn apply_retention(&mut self, retention: SchemaHistoryRetention) -> Result<usize> {
        let mut store = self.schemas.write().await;
        Ok(apply_store_retention(&mut store, retention))
    }
}

#[async_trait]
impl SchemaHistory for FileSchemaHistory {
    async fn record_ddl(&mut self, ddl: DDLEvent) -> Result<u32> {
        let mut store = self.schemas.write().await;
        let version = record_ddl_in_store(&mut store, ddl, current_time())?;
        self.persist_store(&store)?;
        Ok(version)
    }

    async fn get_schema_at_version(
        &self,
        table: &str,
        version: u32,
    ) -> Result<Option<TableSchema>> {
        let store = self.schemas.read().await;
        Ok(schema_at_version(&store, table, version))
    }

    async fn get_schema_at_timestamp(&self, table: &str, ts: u64) -> Result<Option<TableSchema>> {
        let store = self.schemas.read().await;
        Ok(schema_at_timestamp(&store, table, ts))
    }

    async fn latest_schema(&self, table: &str) -> Result<Option<TableSchema>> {
        let store = self.schemas.read().await;
        Ok(latest_schema_for_table(&store, table))
    }

    async fn apply_retention(&mut self, retention: SchemaHistoryRetention) -> Result<usize> {
        let mut store = self.schemas.write().await;
        let removed = apply_store_retention(&mut store, retention);
        if removed > 0 {
            self.persist_store(&store)?;
        }
        Ok(removed)
    }
}

fn apply_schema_diff(schema: &mut TableSchema, diff: &SchemaDiff) -> Result<()> {
    for operation in &diff.operations {
        match operation {
            SchemaDiffOperation::AddColumn { column } => {
                if schema
                    .columns
                    .iter()
                    .any(|existing| existing.name == column.name)
                {
                    return Err(Error::SchemaError(format!(
                        "cannot apply ALTER TABLE diff: column '{}' already exists on {}.{}",
                        column.name, schema.schema, schema.table
                    )));
                }

                schema.columns.push(column.clone());

                if column
                    .constraints
                    .iter()
                    .any(|constraint| constraint.eq_ignore_ascii_case("primary_key"))
                    && !schema.primary_keys.iter().any(|key| key == &column.name)
                {
                    schema.primary_keys.push(column.name.clone());
                }
            }
            SchemaDiffOperation::DropColumn { name } => {
                if !schema.columns.iter().any(|column| column.name == *name) {
                    return Err(Error::SchemaError(format!(
                        "cannot apply ALTER TABLE diff: column '{}' does not exist on {}.{}",
                        name, schema.schema, schema.table
                    )));
                }
                schema.columns.retain(|column| column.name != *name);
                schema.primary_keys.retain(|key| key != name);
            }
            SchemaDiffOperation::RenameColumn { from, to } => {
                if schema.columns.iter().any(|column| column.name == *to) {
                    return Err(Error::SchemaError(format!(
                        "cannot apply ALTER TABLE diff: rename target '{}' already exists on {}.{}",
                        to, schema.schema, schema.table
                    )));
                }

                let Some(column) = schema
                    .columns
                    .iter_mut()
                    .find(|column| column.name == *from)
                else {
                    return Err(Error::SchemaError(format!(
                        "cannot apply ALTER TABLE diff: source column '{}' does not exist on {}.{}",
                        from, schema.schema, schema.table
                    )));
                };

                column.name = to.clone();
                for key in &mut schema.primary_keys {
                    if key == from {
                        *key = to.clone();
                    }
                }
            }
            SchemaDiffOperation::Unsupported { clause } => {
                return Err(Error::SchemaError(format!(
                    "cannot apply ALTER TABLE diff with unsupported clause '{}' on {}.{}",
                    clause, schema.schema, schema.table
                )));
            }
        }
    }

    Ok(())
}

pub struct SchemaValidator<H> {
    history: Arc<H>,
}

impl<H> SchemaValidator<H>
where
    H: SchemaHistory + 'static,
{
    pub fn new(history: Arc<H>) -> Self {
        Self { history }
    }

    pub async fn validate_event(
        &self,
        event: &Event,
    ) -> std::result::Result<(), Vec<ValidationError>> {
        let schema_name = event.schema.clone().unwrap_or_else(|| "public".into());
        let key = table_key(&schema_name, &event.table);
        let Some(table_schema) = self.history.latest_schema(&key).await.map_err(|error| {
            vec![ValidationError {
                field: "schema".into(),
                message: error.to_string(),
            }]
        })?
        else {
            return Err(vec![ValidationError {
                field: "schema".into(),
                message: format!("no schema registered for {key}"),
            }]);
        };

        let mut errors = Vec::new();
        for (field_name, payload) in [
            ("before", event.before.as_ref()),
            ("after", event.after.as_ref()),
        ] {
            if let Some(serde_json::Value::Object(object)) = payload {
                for column in &table_schema.columns {
                    match object.get(&column.name) {
                        Some(value) => {
                            if value.is_null() && !column.nullable {
                                errors.push(ValidationError {
                                    field: format!("{field_name}.{}", column.name),
                                    message: "non-nullable column contains null".into(),
                                });
                            } else if !value.is_null() && !matches_type(value, &column.data_type) {
                                errors.push(ValidationError {
                                    field: format!("{field_name}.{}", column.name),
                                    message: format!(
                                        "value does not match declared type {}",
                                        column.data_type
                                    ),
                                });
                            }
                        }
                        None if !column.nullable => errors.push(ValidationError {
                            field: format!("{field_name}.{}", column.name),
                            message: "required column missing from payload".into(),
                        }),
                        None => {}
                    }
                }

                for unknown in object.keys().filter(|key| {
                    !table_schema
                        .columns
                        .iter()
                        .any(|column| column.name == **key)
                }) {
                    errors.push(ValidationError {
                        field: format!("{field_name}.{unknown}"),
                        message: "column not present in schema".into(),
                    });
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

fn matches_type(value: &serde_json::Value, data_type: &str) -> bool {
    let normalized = data_type.to_ascii_lowercase();
    if normalized.contains("json") {
        return true;
    }
    if normalized.contains("bool") {
        return value.is_boolean();
    }
    if normalized.contains("int")
        || normalized.contains("numeric")
        || normalized.contains("decimal")
        || normalized.contains("float")
    {
        return value.is_number();
    }
    if normalized.contains("char")
        || normalized.contains("text")
        || normalized.contains("string")
        || normalized.contains("uuid")
    {
        return value.is_string();
    }
    true
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use crate::core::{Error, Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    use crate::ddl_capture::{SchemaDiff, SchemaDiffOperation};

    use super::{
        ColumnDef, DDLEvent, FileSchemaHistory, InMemorySchemaHistory, SchemaHistory,
        SchemaHistoryRetention, SchemaValidator, TableSchema,
    };
    use tempfile::tempdir;

    fn schema() -> TableSchema {
        TableSchema {
            schema: "public".into(),
            table: "users".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: "integer".into(),
                    nullable: false,
                    constraints: vec!["primary_key".into()],
                },
                ColumnDef {
                    name: "name".into(),
                    data_type: "text".into(),
                    nullable: false,
                    constraints: Vec::new(),
                },
                ColumnDef {
                    name: "nickname".into(),
                    data_type: "text".into(),
                    nullable: true,
                    constraints: Vec::new(),
                },
            ],
            primary_keys: vec!["id".into()],
            version: 0,
        }
    }

    fn event(after: serde_json::Value) -> Event {
        Event {
            before: None,
            after: Some(after),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "1".into(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".into()),
            table: "users".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    #[tokio::test]
    async fn schema_history_round_trip() {
        let mut history = InMemorySchemaHistory::default();
        let version = history
            .record_ddl(DDLEvent::CreateTable(schema()))
            .await
            .unwrap();
        assert_eq!(version, 1);
        let loaded = history
            .latest_schema("public.users")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.table, "users");
    }

    #[tokio::test]
    async fn validator_detects_unknown_and_missing_columns() {
        let mut history = InMemorySchemaHistory::default();
        history
            .record_ddl(DDLEvent::CreateTable(schema()))
            .await
            .unwrap();
        let validator = SchemaValidator::new(Arc::new(history));

        let errors = validator
            .validate_event(&event(json!({"id": 1, "extra": true})))
            .await
            .unwrap_err();
        assert!(errors.iter().any(|error| error.field.ends_with("name")));
        assert!(errors.iter().any(|error| error.field.ends_with("extra")));
    }

    #[tokio::test]
    async fn validator_accepts_nullable_missing_column() {
        let mut history = InMemorySchemaHistory::default();
        history
            .record_ddl(DDLEvent::CreateTable(schema()))
            .await
            .unwrap();
        let validator = SchemaValidator::new(Arc::new(history));
        assert!(validator
            .validate_event(&event(json!({"id": 1, "name": "alice"})))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn schema_history_tracks_version_and_timestamp_semantics() {
        let mut history = InMemorySchemaHistory::default();
        let mut schema = schema();

        assert_eq!(
            history
                .record_ddl(DDLEvent::CreateTable(schema.clone()))
                .await
                .unwrap(),
            1
        );

        schema.columns.push(ColumnDef {
            name: "email".into(),
            data_type: "text".into(),
            nullable: true,
            constraints: Vec::new(),
        });
        assert_eq!(
            history
                .record_ddl(DDLEvent::AlterTable(schema.clone()))
                .await
                .unwrap(),
            2
        );

        let loaded = history
            .get_schema_at_version("public.users", 2)
            .await
            .unwrap()
            .expect("version 2 schema should exist");
        assert_eq!(loaded.version, 2);
        assert!(loaded.columns.iter().any(|column| column.name == "email"));

        assert!(history
            .get_schema_at_timestamp("public.users", 0)
            .await
            .unwrap()
            .is_none());

        assert_eq!(
            history
                .record_ddl(DDLEvent::DropTable {
                    schema: "public".into(),
                    table: "users".into(),
                })
                .await
                .unwrap(),
            3
        );
        assert!(history
            .latest_schema("public.users")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn alter_table_diff_applies_incremental_schema_changes() {
        let mut history = InMemorySchemaHistory::default();
        history
            .record_ddl(DDLEvent::CreateTable(schema()))
            .await
            .unwrap();

        let version = history
            .record_ddl(DDLEvent::AlterTableDiff {
                schema: "public".into(),
                table: "users".into(),
                diff: SchemaDiff {
                    operations: vec![
                        SchemaDiffOperation::AddColumn {
                            column: ColumnDef {
                                name: "email".into(),
                                data_type: "text".into(),
                                nullable: true,
                                constraints: Vec::new(),
                            },
                        },
                        SchemaDiffOperation::RenameColumn {
                            from: "name".into(),
                            to: "full_name".into(),
                        },
                        SchemaDiffOperation::DropColumn {
                            name: "nickname".into(),
                        },
                    ],
                },
            })
            .await
            .unwrap();

        assert_eq!(version, 2);

        let loaded = history
            .latest_schema("public.users")
            .await
            .unwrap()
            .expect("schema should still exist after alter diff");
        assert_eq!(loaded.version, 2);
        assert!(loaded.columns.iter().any(|column| column.name == "email"));
        assert!(loaded
            .columns
            .iter()
            .any(|column| column.name == "full_name"));
        assert!(!loaded
            .columns
            .iter()
            .any(|column| column.name == "nickname"));
    }

    #[tokio::test]
    async fn alter_table_diff_rejects_unsupported_clause_without_mutating_history() {
        let mut history = InMemorySchemaHistory::default();
        history
            .record_ddl(DDLEvent::CreateTable(schema()))
            .await
            .unwrap();

        let error = history
            .record_ddl(DDLEvent::AlterTableDiff {
                schema: "public".into(),
                table: "users".into(),
                diff: SchemaDiff {
                    operations: vec![SchemaDiffOperation::Unsupported {
                        clause: "REPLICA IDENTITY FULL".into(),
                    }],
                },
            })
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("unsupported clause 'REPLICA IDENTITY FULL'"));

        let loaded = history
            .latest_schema("public.users")
            .await
            .unwrap()
            .expect("schema should remain at the previous version");
        assert_eq!(loaded.version, 1);
        assert!(loaded.columns.iter().any(|column| column.name == "name"));
    }

    #[tokio::test]
    async fn alter_table_diff_rejects_invalid_column_operations() {
        let mut history = InMemorySchemaHistory::default();
        history
            .record_ddl(DDLEvent::CreateTable(schema()))
            .await
            .unwrap();

        let error = history
            .record_ddl(DDLEvent::AlterTableDiff {
                schema: "public".into(),
                table: "users".into(),
                diff: SchemaDiff {
                    operations: vec![SchemaDiffOperation::RenameColumn {
                        from: "missing".into(),
                        to: "display_name".into(),
                    }],
                },
            })
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("source column 'missing' does not exist"));

        let loaded = history
            .latest_schema("public.users")
            .await
            .unwrap()
            .expect("schema should remain unchanged after invalid alter diff");
        assert_eq!(loaded.version, 1);
        assert!(loaded.columns.iter().any(|column| column.name == "name"));
    }

    #[tokio::test]
    async fn file_schema_history_persists_and_reloads_versions() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("schema-history.json");

        let mut history = FileSchemaHistory::new(&path).await.unwrap();
        history
            .record_ddl(DDLEvent::CreateTable(schema()))
            .await
            .unwrap();

        let mut altered = schema();
        altered.columns.push(ColumnDef {
            name: "email".into(),
            data_type: "text".into(),
            nullable: true,
            constraints: Vec::new(),
        });
        history
            .record_ddl(DDLEvent::AlterTable(altered))
            .await
            .unwrap();

        let reloaded = FileSchemaHistory::new(&path).await.unwrap();
        let latest = reloaded
            .latest_schema("public.users")
            .await
            .unwrap()
            .expect("schema should be persisted and reloaded");

        assert_eq!(latest.version, 2);
        assert!(latest.columns.iter().any(|column| column.name == "email"));
    }

    #[tokio::test]
    async fn file_schema_history_rejects_corrupt_payload() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("schema-history.json");
        std::fs::write(&path, b"{not-json").unwrap();

        let error = FileSchemaHistory::new(&path).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("failed to parse schema history file"));
    }

    #[tokio::test]
    async fn in_memory_schema_history_applies_retention_per_table() {
        let mut history = InMemorySchemaHistory::default();
        let mut v1 = schema();
        history
            .record_ddl(DDLEvent::CreateTable(v1.clone()))
            .await
            .unwrap();

        v1.columns.push(ColumnDef {
            name: "email".into(),
            data_type: "text".into(),
            nullable: true,
            constraints: Vec::new(),
        });
        history.record_ddl(DDLEvent::AlterTable(v1)).await.unwrap();

        let mut v3 = schema();
        v3.columns.push(ColumnDef {
            name: "phone".into(),
            data_type: "text".into(),
            nullable: true,
            constraints: Vec::new(),
        });
        history.record_ddl(DDLEvent::AlterTable(v3)).await.unwrap();

        let removed = history
            .apply_retention(SchemaHistoryRetention::keep_last(2).unwrap())
            .await
            .unwrap();
        assert_eq!(removed, 1);

        assert!(history
            .get_schema_at_version("public.users", 1)
            .await
            .unwrap()
            .is_none());
        assert!(history
            .get_schema_at_version("public.users", 2)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            history
                .latest_schema("public.users")
                .await
                .unwrap()
                .expect("latest schema should exist")
                .version,
            3
        );
    }

    #[tokio::test]
    async fn file_schema_history_retention_persists_after_reload() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("schema-history.json");

        let mut history = FileSchemaHistory::new(&path).await.unwrap();
        history
            .record_ddl(DDLEvent::CreateTable(schema()))
            .await
            .unwrap();

        let mut altered = schema();
        altered.columns.push(ColumnDef {
            name: "email".into(),
            data_type: "text".into(),
            nullable: true,
            constraints: Vec::new(),
        });
        history
            .record_ddl(DDLEvent::AlterTable(altered))
            .await
            .unwrap();

        let mut altered_again = schema();
        altered_again.columns.push(ColumnDef {
            name: "phone".into(),
            data_type: "text".into(),
            nullable: true,
            constraints: Vec::new(),
        });
        history
            .record_ddl(DDLEvent::AlterTable(altered_again))
            .await
            .unwrap();

        let removed = history
            .apply_retention(SchemaHistoryRetention::keep_last(1).unwrap())
            .await
            .unwrap();
        assert_eq!(removed, 2);

        let reloaded = FileSchemaHistory::new(&path).await.unwrap();
        assert!(reloaded
            .get_schema_at_version("public.users", 1)
            .await
            .unwrap()
            .is_none());
        assert!(reloaded
            .get_schema_at_version("public.users", 2)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            reloaded
                .latest_schema("public.users")
                .await
                .unwrap()
                .expect("latest schema should remain after retention")
                .version,
            3
        );
    }

    #[tokio::test]
    async fn schema_history_retention_rejects_zero_limit() {
        let error = SchemaHistoryRetention::keep_last(0).unwrap_err();
        assert!(matches!(error, Error::ConfigError(_)));
    }
}
