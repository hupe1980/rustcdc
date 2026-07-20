//! Checkpoint abstractions and in-memory implementations.

mod barrier;
pub(crate) mod owner_lease;

use std::{
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::core::{Offset, Result};

pub use barrier::{BarrierState, CommitBarrier};

const FILE_CHECKPOINT_FORMAT_VERSION: u16 = 1;
const FILE_CHECKPOINT_DEFAULT_FILE_MODE: u32 = 0o600;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileCheckpointRecord {
    checkpoint_format_version: u16,
    source_type: String,
    committed_event_count: u64,
    offset: serde_json::Value,
    /// SHA-256 over the other three fields, hex-encoded.
    ///
    /// A checkpoint is the one piece of state whose silent corruption is unrecoverable:
    /// a flipped bit in an LSN or binlog position does not fail to parse, it resumes
    /// capture from a *wrong* position — skipping events forever with no error anywhere.
    ///
    /// `default` so that a file missing the field reaches
    /// [`FileCheckpointRecord::verify_checksum`], which names the real problem, rather
    /// than failing with a raw serde "missing field" error.
    #[serde(default)]
    content_checksum: String,
}

impl FileCheckpointRecord {
    /// Build a record with its checksum already computed.
    fn new(source_type: String, committed_event_count: u64, offset: serde_json::Value) -> Self {
        let mut record = Self {
            checkpoint_format_version: FILE_CHECKPOINT_FORMAT_VERSION,
            source_type,
            committed_event_count,
            offset,
            content_checksum: String::new(),
        };
        record.content_checksum = record.compute_checksum();
        record
    }

    /// SHA-256 over the record's semantic content, excluding the checksum field itself.
    ///
    /// The pre-image is a `serde_json` serialization of the three content fields. That is
    /// deterministic here because `serde` emits struct fields in declaration order and
    /// `serde_json::Value::Object` is a `BTreeMap` (key-sorted) unless the `preserve_order`
    /// feature is enabled — which this crate does not enable. Re-serializing a parsed record
    /// therefore reproduces the exact bytes that were hashed at write time.
    fn compute_checksum(&self) -> String {
        use sha2::{Digest as _, Sha256};

        #[derive(Serialize)]
        struct ChecksumPreimage<'a> {
            checkpoint_format_version: u16,
            source_type: &'a str,
            committed_event_count: u64,
            offset: &'a serde_json::Value,
        }

        let preimage = ChecksumPreimage {
            checkpoint_format_version: self.checkpoint_format_version,
            source_type: &self.source_type,
            committed_event_count: self.committed_event_count,
            offset: &self.offset,
        };
        // Serializing a struct of plain scalars plus an already-parsed `Value` cannot fail.
        let bytes = serde_json::to_vec(&preimage).unwrap_or_default();
        let digest = Sha256::digest(&bytes);
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// Reject a record whose bytes do not match the checksum written with them.
    fn verify_checksum(&self, path: &Path) -> Result<()> {
        let expected = self.compute_checksum();
        if self.content_checksum == expected {
            return Ok(());
        }

        Err(crate::core::Error::CheckpointError(format!(
            "checkpoint file '{}' failed its integrity check (recorded checksum {}, computed {}). \
             The file has been corrupted or edited by hand. Resuming from it risks silently \
             skipping or replaying events, so the runtime refuses to load it. Restore the file \
             from backup, or re-seed it with `FileCheckpoint::restore_from_record`.",
            path.display(),
            if self.content_checksum.is_empty() {
                "<absent>"
            } else {
                &self.content_checksum
            },
            expected
        )))
    }
}

/// Concrete PostgreSQL checkpoint offset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresOffset {
    /// Log sequence number representing the durable WAL position.
    pub lsn: u64,
    /// Replication slot used to resume from this offset.
    pub slot_name: String,
}

impl PostgresOffset {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

impl Offset for PostgresOffset {
    fn source_type(&self) -> &str {
        "postgres"
    }

    fn encode(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
}

/// Concrete MySQL checkpoint offset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MysqlOffset {
    /// GTID set representing the durable source position.
    ///
    /// A full set (`uuid:1-500,uuid2:1-7`), not a single GTID — resuming from a single
    /// GTID would tell the server the replica has executed only that one transaction
    /// and trigger a mass replay of everything before it.
    pub gtid: String,
    /// Binlog file containing the committed position.
    pub binlog_file: String,
    /// Position inside the binlog file.
    pub binlog_pos: u32,
    /// Server flavor that produced this offset: `"mysql"` or `"mariadb"`.
    ///
    /// This is part of the offset because it determines the **checkpoint file name**.
    /// It was previously hardcoded to `"mysql"`, so a MariaDB stream wrote
    /// `checkpoint_mysql.json`, found nothing under `checkpoint_mariadb.json` on
    /// restart, and silently resumed from the *current* binlog position — losing every
    /// change since the crash. It also guards against resuming a MariaDB position on a
    /// MySQL server or vice versa, where the GTID formats are mutually unintelligible.
    #[serde(default = "MysqlOffset::default_flavor")]
    pub source_flavor: String,
}

impl MysqlOffset {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn default_flavor() -> String {
        "mysql".to_string()
    }

    /// Construct an offset for a specific server flavor (`"mysql"` or `"mariadb"`).
    pub fn new(
        source_flavor: impl Into<String>,
        binlog_file: impl Into<String>,
        binlog_pos: u32,
        gtid: impl Into<String>,
    ) -> Self {
        Self {
            gtid: gtid.into(),
            binlog_file: binlog_file.into(),
            binlog_pos,
            source_flavor: source_flavor.into(),
        }
    }
}

impl Offset for MysqlOffset {
    fn source_type(&self) -> &str {
        &self.source_flavor
    }

    fn encode(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
}

/// Generic opaque offset for tests and runtime scaffolding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenericOffset {
    /// Source connector name associated with this opaque offset.
    pub source: String,
    /// Opaque serialized offset bytes.
    pub bytes: Vec<u8>,
}

impl GenericOffset {
    pub fn new(source: impl Into<String>, bytes: Vec<u8>) -> Self {
        Self {
            source: source.into(),
            bytes,
        }
    }
}

impl Offset for GenericOffset {
    fn source_type(&self) -> &str {
        &self.source
    }

    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.bytes.clone())
    }
}

/// Stored checkpoint entry.
#[derive(Clone)]
pub struct StoredCheckpoint {
    /// Durable offset snapshot stored by the checkpoint backend.
    pub offset: Box<dyn Offset>,
    /// Number of events durably committed at this offset.
    pub committed_event_count: u64,
}

impl std::fmt::Debug for StoredCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredCheckpoint")
            .field("source_type", &self.offset.source_type())
            .field("committed_event_count", &self.committed_event_count)
            .finish()
    }
}

/// Checkpoint abstraction for durable source progress.
#[async_trait]
pub trait Checkpoint: Send + Sync {
    async fn save(&mut self, offset: &dyn Offset, committed_event_count: u64) -> Result<()>;
    async fn load(&self) -> Result<Option<Box<dyn Offset>>>;
    async fn get_committed_count(&self) -> Result<u64>;
}

/// In-memory checkpoint store for **testing and examples only**.
///
/// # Warning
///
/// `InMemoryCheckpoint` **must not be used in production**. All checkpoint
/// state is held in memory and is irrecoverably lost on process restart.
/// After restart the runtime will perform a full replay from the origin LSN,
/// producing duplicate events visible to downstream consumers.
///
/// For production use, choose [`FileCheckpoint`] (single-process, local
/// filesystem) or implement the [`Checkpoint`] trait against your own storage
/// backend (database, object store, Redis, etc.).
///
/// Suitable for tests, short-lived processes, and embeddings where checkpoint
/// state does not need to survive a restart. For production use, prefer
/// [`FileCheckpoint`] to avoid full replay on every process restart.
#[derive(Debug, Clone, Default)]
pub struct InMemoryCheckpoint {
    entries: Arc<Mutex<VecDeque<StoredCheckpoint>>>,
}

#[cfg(any(test, feature = "test-harnesses"))]
impl InMemoryCheckpoint {
    pub fn history_len(&self) -> usize {
        self.entries
            .lock()
            .map(|entries| entries.len())
            .unwrap_or_default()
    }
}

/// File-backed checkpoint store for local durability.
#[derive(Debug)]
pub struct FileCheckpoint {
    /// Directory containing checkpoint files.
    pub checkpoint_dir: PathBuf,
    /// Unix file mode used when creating checkpoint files.
    pub file_mode: u32,
    lease: Mutex<Option<owner_lease::OwnerLease>>,
}

impl FileCheckpoint {
    const OWNER_LEASE_FILENAME: &str = ".rustcdc_checkpoint.owner";

    fn source_family(source_type: &str) -> &str {
        source_type.strip_suffix("_snapshot").unwrap_or(source_type)
    }

    /// Create a new file checkpoint store.
    pub fn new(checkpoint_dir: impl Into<PathBuf>) -> Self {
        Self {
            checkpoint_dir: checkpoint_dir.into(),
            file_mode: FILE_CHECKPOINT_DEFAULT_FILE_MODE,
            lease: Mutex::new(None),
        }
    }

    fn lease_path(&self) -> PathBuf {
        self.checkpoint_dir.join(Self::OWNER_LEASE_FILENAME)
    }

    fn ensure_owner_lease(&self) -> Result<()> {
        let mut lease = self.lease.lock().map_err(|_| {
            crate::core::Error::CheckpointError(
                "checkpoint owner lease lock poisoned during acquisition".into(),
            )
        })?;

        if lease.is_some() {
            return Ok(());
        }

        self.ensure_directory()?;

        let lock_path = self.lease_path();
        let acquired = owner_lease::acquire(&lock_path, "checkpoint")
            .map_err(|e| crate::core::Error::CheckpointError(e.to_string()))?;

        // Warn if checkpoint files already exist — a new lease on a non-empty
        // directory may indicate an unclean handoff from a previous instance.
        let has_existing_checkpoints = fs::read_dir(&self.checkpoint_dir)
            .ok()
            .map(|entries| {
                entries.flatten().any(|e| {
                    let name = e.file_name();
                    let n = name.to_string_lossy();
                    n.ends_with(".json") && n != "owner.lock"
                })
            })
            .unwrap_or(false);
        if has_existing_checkpoints {
            tracing::warn!(
                target: "rustcdc::checkpoint",
                checkpoint_dir = %self.checkpoint_dir.display(),
                owner_pid = std::process::id(),
                "checkpoint directory already contains checkpoint files — new process is \
                 taking over. Ensure no other runtime instance is running against this \
                 directory to avoid concurrent write corruption."
            );
        }

        *lease = Some(acquired);
        Ok(())
    }

    fn checkpoint_path(&self, source_type: &str) -> PathBuf {
        self.checkpoint_dir
            .join(format!("checkpoint_{source_type}.json"))
    }

    fn temp_path(&self, source_type: &str) -> PathBuf {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        self.checkpoint_dir
            .join(format!("checkpoint_{source_type}.{stamp}.tmp"))
    }

    fn ensure_directory(&self) -> Result<()> {
        if !self.checkpoint_dir.exists() {
            return Err(crate::core::Error::CheckpointError(format!(
                "checkpoint directory does not exist: {}",
                self.checkpoint_dir.display()
            )));
        }
        if !self.checkpoint_dir.is_dir() {
            return Err(crate::core::Error::CheckpointError(format!(
                "checkpoint path is not a directory: {}",
                self.checkpoint_dir.display()
            )));
        }
        Ok(())
    }

    fn checkpoint_files(&self) -> Result<Vec<(std::time::SystemTime, PathBuf)>> {
        self.ensure_directory()?;
        let mut files = Vec::new();
        for entry in fs::read_dir(&self.checkpoint_dir).map_err(crate::core::Error::from)? {
            let entry = entry.map_err(crate::core::Error::from)?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !name.starts_with("checkpoint_") || !name.ends_with(".json") {
                continue;
            }

            let modified = entry
                .metadata()
                .and_then(|metadata| metadata.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            files.push((modified, path));
        }

        Ok(files)
    }

    fn load_latest_record(&self) -> Result<Option<FileCheckpointRecord>> {
        let files = self.checkpoint_files()?;
        if files.is_empty() {
            return Ok(None);
        }

        let mut records = Vec::with_capacity(files.len());
        for (modified, path) in files {
            let record = Self::read_record(&path)?;
            records.push((modified, path, record));
        }

        let mut source_families = std::collections::BTreeSet::new();
        for (_, _, record) in &records {
            source_families.insert(Self::source_family(&record.source_type));
        }

        if source_families.len() > 1 {
            let joined = source_families.into_iter().collect::<Vec<_>>().join(", ");
            return Err(crate::core::Error::CheckpointError(format!(
                "mixed checkpoint source families found in directory '{}': {}. use a dedicated checkpoint directory per source family",
                self.checkpoint_dir.display(),
                joined
            )));
        }

        // Order by durable progress FIRST, mtime only as a tie-break.
        //
        // mtime is not a safe primary key for "furthest along": a snapshot checkpoint
        // written moments after a far-ahead stream checkpoint would shadow it, and
        // `load()` would resume from the snapshot offset — triggering a full re-snapshot
        // plus a large duplicate flood. mtime is also unreliable in its own right
        // (coarse-resolution filesystems, clock skew, `touch`, restore-from-backup).
        //
        // `committed_event_count` is monotonic per source family (enforced by
        // `validate_monotonic_progress`), so the record with the highest count is by
        // definition the furthest durable position. Ties fall back to mtime, then to
        // path, so ordering is total and deterministic.
        records.sort_by(
            |(left_time, left_path, left_record), (right_time, right_path, right_record)| {
                left_record
                    .committed_event_count
                    .cmp(&right_record.committed_event_count)
                    .then_with(|| left_time.cmp(right_time))
                    .then_with(|| left_path.cmp(right_path))
            },
        );

        Ok(records.pop().map(|(_, _, record)| record))
    }

    fn read_record(path: &Path) -> Result<FileCheckpointRecord> {
        Self::check_file_permissions(path)?;
        let record: FileCheckpointRecord =
            serde_json::from_slice(&fs::read(path).map_err(crate::core::Error::from)?)
                .map_err(crate::core::Error::from)?;
        Self::validate_record_version(path, &record)?;
        record.verify_checksum(path)?;
        Ok(record)
    }

    /// Reject checkpoint files that are readable or writable by group/other.
    ///
    /// If permissions cannot be read (e.g., non-Unix platform) this is a no-op.
    fn check_file_permissions(path: &Path) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let meta = std::fs::metadata(path).map_err(crate::core::Error::from)?;
            let mode = meta.mode() & 0o777;
            if mode & 0o077 != 0 {
                return Err(crate::core::Error::CheckpointError(format!(
                    "checkpoint file '{}' has insecure permissions {:04o}; \
                     expected 0600 (no access for group/other). \
                     Run: chmod 600 {}",
                    path.display(),
                    mode,
                    path.display(),
                )));
            }
        }
        #[cfg(not(unix))]
        let _ = path;
        Ok(())
    }

    fn validate_record_version(path: &Path, record: &FileCheckpointRecord) -> Result<()> {
        if record.checkpoint_format_version == FILE_CHECKPOINT_FORMAT_VERSION {
            return Ok(());
        }

        Err(crate::core::Error::CheckpointError(format!(
            "unsupported checkpoint file format version {} in '{}'; supported version is {}",
            record.checkpoint_format_version,
            path.display(),
            FILE_CHECKPOINT_FORMAT_VERSION
        )))
    }

    fn write_permissions(&self, file: &File) -> Result<()> {
        set_checkpoint_file_mode(file, self.file_mode)
    }

    fn sync_parent_directory(&self, file_path: &Path) -> Result<()> {
        crate::core::durability::fsync_parent_directory(file_path)
    }

    fn validate_monotonic_progress(
        &self,
        source_type: &str,
        next: &FileCheckpointRecord,
    ) -> Result<()> {
        let checkpoint_path = self.checkpoint_path(source_type);
        if !checkpoint_path.exists() {
            return Ok(());
        }

        let existing = Self::read_record(&checkpoint_path)?;

        if existing.committed_event_count > next.committed_event_count {
            return Err(crate::core::Error::CheckpointError(format!(
                "refusing non-monotonic checkpoint write for source '{}': existing committed_event_count={} is greater than next committed_event_count={}",
                source_type, existing.committed_event_count, next.committed_event_count
            )));
        }

        if existing.committed_event_count == next.committed_event_count
            && existing.offset != next.offset
        {
            return Err(crate::core::Error::CheckpointError(format!(
                "refusing conflicting checkpoint write for source '{}': committed_event_count={} matches existing record but offset payload differs",
                source_type, next.committed_event_count
            )));
        }

        Ok(())
    }

    /// Seed a checkpoint file directly from raw offset bytes and an event count.
    ///
    /// This is useful for migrations, disaster recovery, and integration testing
    /// where a known-good offset needs to be injected into a checkpoint directory
    /// without going through a live [`Checkpoint::save`] cycle.
    ///
    /// # Arguments
    ///
    /// * `dir` — directory that would be passed to [`FileCheckpoint::new`]
    /// * `source_type` — source identifier, e.g. `"postgres"` or `"mysql"`
    /// * `offset_bytes` — raw bytes as returned by [`Offset::encode`]
    /// * `committed_event_count` — event counter to persist in the record
    ///
    /// # Errors
    ///
    /// Returns [`crate::core::Error::CheckpointError`] if the directory does not exist, is not
    /// writable, or if `offset_bytes` cannot be parsed as JSON.
    pub fn restore_from_record(
        dir: &Path,
        source_type: &str,
        offset_bytes: Vec<u8>,
        committed_event_count: u64,
    ) -> Result<()> {
        Self::restore_from_record_with_mode(
            dir,
            source_type,
            offset_bytes,
            committed_event_count,
            FILE_CHECKPOINT_DEFAULT_FILE_MODE,
        )
    }

    /// Same as [`FileCheckpoint::restore_from_record`], but with an explicit file mode.
    ///
    /// Use this when the [`FileCheckpoint`] that will subsequently read the restored
    /// checkpoint was configured with a non-default
    /// [`FileCheckpoint::file_mode`].  The restored file **must** be written with
    /// the same mode the reader enforces, otherwise [`Checkpoint::load`] rejects it as
    /// having insecure permissions.
    ///
    /// # Errors
    ///
    /// Returns [`crate::core::Error::CheckpointError`] if the directory does not exist, is not
    /// writable, or if `offset_bytes` cannot be parsed as JSON.
    pub fn restore_from_record_with_mode(
        dir: &Path,
        source_type: &str,
        offset_bytes: Vec<u8>,
        committed_event_count: u64,
        file_mode: u32,
    ) -> Result<()> {
        use std::io::Write as _;

        if !dir.exists() {
            return Err(crate::core::Error::CheckpointError(format!(
                "checkpoint directory does not exist: {}",
                dir.display()
            )));
        }
        if !dir.is_dir() {
            return Err(crate::core::Error::CheckpointError(format!(
                "checkpoint path is not a directory: {}",
                dir.display()
            )));
        }

        let offset_value: serde_json::Value =
            serde_json::from_slice(&offset_bytes).map_err(|e| {
                crate::core::Error::CheckpointError(format!(
                    "restore_from_record: offset_bytes are not valid JSON: {e}"
                ))
            })?;

        let record =
            FileCheckpointRecord::new(source_type.to_string(), committed_event_count, offset_value);

        let final_path = dir.join(format!("checkpoint_{source_type}.json"));
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let temp_path = dir.join(format!("checkpoint_{source_type}.{stamp}.tmp"));

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .map_err(crate::core::Error::from)?;
        // The restored file must carry the same restrictive mode `save()` writes, or
        // `load()`'s permission check rejects it and the runtime refuses to start on
        // exactly the checkpoint an operator just seeded for disaster recovery.
        set_checkpoint_file_mode(&file, file_mode)?;
        let payload = serde_json::to_vec_pretty(&record)?;
        file.write_all(&payload).map_err(crate::core::Error::from)?;
        file.sync_all().map_err(crate::core::Error::from)?;
        drop(file);

        fs::rename(&temp_path, &final_path).map_err(crate::core::Error::from)?;
        // fsync the directory so the rename itself survives a crash — without this the
        // seeded checkpoint can vanish on ext4/xfs even though this call returned Ok.
        crate::core::durability::fsync_parent_directory(&final_path)?;
        Ok(())
    }
}

/// Apply the checkpoint file mode. No-op on non-unix platforms.
fn set_checkpoint_file_mode(file: &File, mode: u32) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        file.set_permissions(std::fs::Permissions::from_mode(mode))
            .map_err(crate::core::Error::from)?;
    }
    #[cfg(not(unix))]
    let _ = (file, mode);
    Ok(())
}

#[async_trait]
impl Checkpoint for FileCheckpoint {
    async fn save(&mut self, offset: &dyn Offset, committed_event_count: u64) -> Result<()> {
        self.ensure_owner_lease()?;
        self.ensure_directory()?;

        let source_type = offset.source_type().to_string();
        let record = FileCheckpointRecord::new(
            source_type.clone(),
            committed_event_count,
            serde_json::from_slice(&offset.encode()?)?,
        );
        self.validate_monotonic_progress(&source_type, &record)?;

        let temp_path = self.temp_path(&source_type);
        let final_path = self.checkpoint_path(&source_type);

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .map_err(crate::core::Error::from)?;
        self.write_permissions(&file)?;
        let payload = serde_json::to_vec_pretty(&record)?;
        file.write_all(&payload).map_err(crate::core::Error::from)?;
        file.sync_all().map_err(crate::core::Error::from)?;
        drop(file);

        fs::rename(&temp_path, &final_path).map_err(crate::core::Error::from)?;
        self.sync_parent_directory(&final_path)?;
        Ok(())
    }

    async fn load(&self) -> Result<Option<Box<dyn Offset>>> {
        self.ensure_owner_lease()?;
        let Some(record) = self.load_latest_record()? else {
            return Ok(None);
        };

        let encoded = serde_json::to_vec(&record.offset)?;
        let offset: Box<dyn Offset> = match record.source_type.as_str() {
            "postgres" => Box::new(PostgresOffset::from_bytes(&encoded)?),
            "mysql" => Box::new(MysqlOffset::from_bytes(&encoded)?),
            "mariadb" => {
                // Validate MariaDB checkpoints with the MySQL offset schema but
                // preserve the source namespace for strict resume checks.
                let _validated = MysqlOffset::from_bytes(&encoded)?;
                Box::new(GenericOffset::new("mariadb", encoded))
            }
            other => Box::new(GenericOffset::new(other, encoded)),
        };
        Ok(Some(offset))
    }

    async fn get_committed_count(&self) -> Result<u64> {
        self.ensure_owner_lease()?;
        let Some(record) = self.load_latest_record()? else {
            return Ok(0);
        };

        Ok(record.committed_event_count)
    }
}

impl Drop for FileCheckpoint {
    fn drop(&mut self) {
        // OwnerLease::drop handles ref-count decrement and lease file removal.
        // Take while holding the Mutex to prevent a concurrent ensure_owner_lease
        // from re-acquiring between the take and the OwnerLease drop.
        if let Ok(mut guard) = self.lease.lock() {
            drop(guard.take());
        }
    }
}

#[async_trait]
impl Checkpoint for InMemoryCheckpoint {
    async fn save(&mut self, offset: &dyn Offset, committed_event_count: u64) -> Result<()> {
        tracing::warn!(
            target: "rustcdc::checkpoint",
            "InMemoryCheckpoint::save called — all checkpoint state is held in memory and will \
             be lost on process restart, causing full replay and potential duplicate event delivery. \
             Use FileCheckpoint or a durable backend for production deployments."
        );
        self.entries
            .lock()
            .map_err(|_| {
                crate::core::Error::CheckpointError("checkpoint lock poisoned during save".into())
            })?
            .push_back(StoredCheckpoint {
                offset: offset.clone_box(),
                committed_event_count,
            });
        Ok(())
    }

    async fn load(&self) -> Result<Option<Box<dyn Offset>>> {
        Ok(self
            .entries
            .lock()
            .map_err(|_| {
                crate::core::Error::CheckpointError("checkpoint lock poisoned during load".into())
            })?
            .back()
            .map(|entry| entry.offset.clone()))
    }

    async fn get_committed_count(&self) -> Result<u64> {
        Ok(self
            .entries
            .lock()
            .map_err(|_| {
                crate::core::Error::CheckpointError(
                    "checkpoint lock poisoned during committed count lookup".into(),
                )
            })?
            .back()
            .map(|entry| entry.committed_event_count)
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::{
        Checkpoint, FileCheckpoint, FileCheckpointRecord, InMemoryCheckpoint, MysqlOffset,
        PostgresOffset, FILE_CHECKPOINT_FORMAT_VERSION,
    };

    #[tokio::test]
    async fn in_memory_checkpoint_round_trips_offsets() {
        let mut checkpoint = InMemoryCheckpoint::default();
        let offset = PostgresOffset {
            lsn: 42,
            slot_name: "slot-a".into(),
        };

        checkpoint.save(&offset, 7).await.unwrap();
        let loaded = checkpoint.load().await.unwrap().unwrap();
        assert_eq!(loaded.source_type(), "postgres");
        assert_eq!(checkpoint.get_committed_count().await.unwrap(), 7);
    }

    #[test]
    fn mysql_offset_decodes_from_bytes() {
        let offset = MysqlOffset {
            gtid: "1-2-3".into(),
            binlog_file: "mysql-bin.000001".into(),
            binlog_pos: 8,
            source_flavor: "mysql".into(),
        };
        let encoded = crate::core::Offset::encode(&offset).unwrap();
        let decoded = MysqlOffset::from_bytes(&encoded).unwrap();
        assert_eq!(offset, decoded);
    }

    #[tokio::test]
    async fn file_checkpoint_round_trips_offsets() {
        let dir = tempdir().unwrap();
        let mut checkpoint = FileCheckpoint::new(dir.path());
        let offset = PostgresOffset {
            lsn: 99,
            slot_name: "slot-a".into(),
        };

        checkpoint.save(&offset, 11).await.unwrap();
        let loaded = checkpoint.load().await.unwrap().unwrap();
        assert_eq!(loaded.source_type(), "postgres");
        assert_eq!(checkpoint.get_committed_count().await.unwrap(), 11);
    }

    /// A MariaDB offset must checkpoint under `mariadb`, not `mysql`.
    ///
    /// `MysqlOffset::source_type()` was hardcoded to `"mysql"`, and `FileCheckpoint`
    /// derives the filename from it — so a MariaDB stream wrote `checkpoint_mysql.json`,
    /// found nothing under `checkpoint_mariadb.json` on restart, and silently resumed
    /// from the *current* binlog position, losing everything since the crash.
    #[tokio::test]
    async fn mariadb_offset_checkpoints_under_its_own_source_type() {
        let dir = tempdir().unwrap();
        let mut checkpoint = FileCheckpoint::new(dir.path());

        let offset = MysqlOffset::new("mariadb", "mariadb-bin.000004", 1234, "0-1-100");
        assert_eq!(crate::core::Offset::source_type(&offset), "mariadb");

        checkpoint.save(&offset, 7).await.unwrap();
        assert!(
            dir.path().join("checkpoint_mariadb.json").exists(),
            "a MariaDB offset must write checkpoint_mariadb.json"
        );
        assert!(
            !dir.path().join("checkpoint_mysql.json").exists(),
            "a MariaDB offset must NOT write checkpoint_mysql.json"
        );

        let loaded = checkpoint.load().await.unwrap().unwrap();
        assert_eq!(loaded.source_type(), "mariadb");
        let decoded = MysqlOffset::from_bytes(&loaded.encode().unwrap()).unwrap();
        assert_eq!(decoded.binlog_file, "mariadb-bin.000004");
        assert_eq!(decoded.binlog_pos, 1234);
        assert_eq!(decoded.gtid, "0-1-100");
        assert_eq!(decoded.source_flavor, "mariadb");
    }

    #[tokio::test]
    async fn file_checkpoint_rejects_missing_directory() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("missing");
        let mut checkpoint = FileCheckpoint::new(&missing);
        let offset = MysqlOffset {
            gtid: "1-2-3".into(),
            binlog_file: "binlog.000001".into(),
            binlog_pos: 4,
            source_flavor: "mysql".into(),
        };

        let error = checkpoint.save(&offset, 1).await.unwrap_err();
        assert!(matches!(error, crate::core::Error::CheckpointError(_)));
    }

    #[tokio::test]
    async fn file_checkpoint_rejects_corrupt_json() {
        let dir = tempdir().unwrap();
        let checkpoint = FileCheckpoint::new(dir.path());
        let path = dir.path().join("checkpoint_postgres.json");
        std::fs::write(&path, b"{not-json").unwrap();
        #[cfg(unix)]
        std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();

        let error = checkpoint.load().await.unwrap_err();
        assert!(matches!(error, crate::core::Error::SerializationError(_)));
    }

    #[tokio::test]
    async fn file_checkpoint_rejects_mixed_source_types_in_single_directory() {
        let dir = tempdir().unwrap();
        let mut checkpoint = FileCheckpoint::new(dir.path());

        checkpoint
            .save(
                &PostgresOffset {
                    lsn: 1,
                    slot_name: "slot-a".into(),
                },
                1,
            )
            .await
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        checkpoint
            .save(
                &MysqlOffset {
                    gtid: "gtid-1".into(),
                    binlog_file: "mysql-bin.000001".into(),
                    binlog_pos: 4,
                    source_flavor: "mysql".into(),
                },
                2,
            )
            .await
            .unwrap();

        let load_error = checkpoint.load().await.unwrap_err();
        let count_error = checkpoint.get_committed_count().await.unwrap_err();
        assert!(matches!(load_error, crate::core::Error::CheckpointError(_)));
        assert!(matches!(
            count_error,
            crate::core::Error::CheckpointError(_)
        ));
    }

    /// A single flipped bit in a checkpoint offset does not fail to parse — it resumes
    /// capture from a wrong position and silently skips events. The checksum must catch it.
    #[tokio::test]
    async fn corrupted_checkpoint_offset_is_rejected_rather_than_silently_resumed() {
        let dir = tempdir().unwrap();
        let mut checkpoint = FileCheckpoint::new(dir.path());
        checkpoint
            .save(
                &PostgresOffset {
                    lsn: 4_294_967_296,
                    slot_name: "slot-a".into(),
                },
                7,
            )
            .await
            .unwrap();

        // Tamper with the offset exactly as bit-rot or a hand edit would: valid JSON,
        // valid schema, wrong value.
        let path = dir.path().join("checkpoint_postgres.json");
        let mut record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        record["offset"]["lsn"] = json!(4_294_967_297u64);
        std::fs::write(&path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
        #[cfg(unix)]
        std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();

        let error = checkpoint.load().await.unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("integrity check"),
            "the operator must be told the file is corrupt, not given a wrong offset: {message}"
        );
        assert!(
            message.contains("restore_from_record"),
            "the error must name the remedy: {message}"
        );
    }

    /// A file with no integrity field cannot be trusted, and the operator must be told
    /// why — not handed a raw serde "missing field" error.
    #[tokio::test]
    async fn checkpoint_without_a_checksum_is_rejected_with_a_legible_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint_postgres.json");
        std::fs::write(
            &path,
            serde_json::to_vec(&json!({
                "checkpoint_format_version": FILE_CHECKPOINT_FORMAT_VERSION,
                "source_type": "postgres",
                "committed_event_count": 5,
                "offset": { "lsn": 42, "slot_name": "slot-a" }
            }))
            .unwrap(),
        )
        .unwrap();
        #[cfg(unix)]
        std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();

        let checkpoint = FileCheckpoint::new(dir.path());
        let message = checkpoint.load().await.unwrap_err().to_string();
        assert!(
            message.contains("integrity check"),
            "a checkpoint with no checksum must be rejected as un-trustworthy: {message}"
        );
        assert!(
            message.contains("<absent>"),
            "the error must distinguish a missing checksum from a wrong one: {message}"
        );
    }

    /// A checkpoint written by `save` and one seeded by `restore_from_record` must both
    /// verify, or disaster recovery hands the operator a file the runtime then refuses.
    #[tokio::test]
    async fn round_trip_and_seeded_checkpoints_both_pass_the_integrity_check() {
        let dir = tempdir().unwrap();
        let mut checkpoint = FileCheckpoint::new(dir.path());
        checkpoint
            .save(
                &PostgresOffset {
                    lsn: 99,
                    slot_name: "slot-a".into(),
                },
                3,
            )
            .await
            .unwrap();
        assert!(checkpoint.load().await.unwrap().is_some());

        let seeded = tempdir().unwrap();
        FileCheckpoint::restore_from_record(
            seeded.path(),
            "postgres",
            serde_json::to_vec(&json!({ "lsn": 150, "slot_name": "slot-a" })).unwrap(),
            8,
        )
        .unwrap();
        let seeded_checkpoint = FileCheckpoint::new(seeded.path());
        assert_eq!(seeded_checkpoint.get_committed_count().await.unwrap(), 8);
        assert!(seeded_checkpoint.load().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn file_checkpoint_allows_snapshot_and_stream_variants_in_single_directory() {
        let dir = tempdir().unwrap();
        let checkpoint = FileCheckpoint::new(dir.path());

        let snapshot_path = dir.path().join("checkpoint_postgres_snapshot.json");
        let stream_path = dir.path().join("checkpoint_postgres.json");

        let snapshot_record = FileCheckpointRecord::new(
            "postgres_snapshot".to_string(),
            3,
            json!({ "snapshot_id": "snap-1" }),
        );
        let stream_record = FileCheckpointRecord::new(
            "postgres".to_string(),
            9,
            json!({ "lsn": 99, "slot_name": "slot-a" }),
        );

        std::fs::write(
            &snapshot_path,
            serde_json::to_vec(&snapshot_record).unwrap(),
        )
        .unwrap();
        #[cfg(unix)]
        std::fs::set_permissions(
            &snapshot_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )
        .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        std::fs::write(&stream_path, serde_json::to_vec(&stream_record).unwrap()).unwrap();
        #[cfg(unix)]
        std::fs::set_permissions(
            &stream_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )
        .unwrap();

        let loaded = checkpoint.load().await.unwrap().unwrap();
        assert_eq!(loaded.source_type(), "postgres");
        assert_eq!(checkpoint.get_committed_count().await.unwrap(), 9);
    }

    #[tokio::test]
    async fn file_checkpoint_writes_current_format_version() {
        let dir = tempdir().unwrap();
        let mut checkpoint = FileCheckpoint::new(dir.path());
        let offset = PostgresOffset {
            lsn: 123,
            slot_name: "slot-a".into(),
        };

        checkpoint.save(&offset, 3).await.unwrap();
        let payload = std::fs::read_to_string(dir.path().join("checkpoint_postgres.json")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(
            parsed
                .get("checkpoint_format_version")
                .and_then(|value| value.as_u64()),
            Some(FILE_CHECKPOINT_FORMAT_VERSION as u64)
        );
    }

    #[tokio::test]
    async fn file_checkpoint_rejects_record_without_explicit_version() {
        let dir = tempdir().unwrap();
        let checkpoint = FileCheckpoint::new(dir.path());
        let path = dir.path().join("checkpoint_postgres.json");

        let missing_version_payload = json!({
            "source_type": "postgres",
            "committed_event_count": 7,
            "offset": {
                "lsn": 42,
                "slot_name": "slot-missing-version"
            }
        });
        std::fs::write(&path, serde_json::to_vec(&missing_version_payload).unwrap()).unwrap();
        #[cfg(unix)]
        std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();

        let error = checkpoint.load().await.unwrap_err();
        assert!(matches!(error, crate::core::Error::SerializationError(_)));
    }

    #[tokio::test]
    async fn file_checkpoint_rejects_unknown_record_version() {
        let dir = tempdir().unwrap();
        let checkpoint = FileCheckpoint::new(dir.path());
        let path = dir.path().join("checkpoint_postgres.json");

        let payload = json!({
            "checkpoint_format_version": 99,
            "source_type": "postgres",
            "committed_event_count": 1,
            "offset": {
                "lsn": 1,
                "slot_name": "slot"
            }
        });
        std::fs::write(&path, serde_json::to_vec(&payload).unwrap()).unwrap();

        let error = checkpoint.load().await.unwrap_err();
        assert!(matches!(error, crate::core::Error::CheckpointError(_)));
    }

    #[tokio::test]
    async fn file_checkpoint_rejects_non_monotonic_committed_count() {
        let dir = tempdir().unwrap();
        let mut checkpoint = FileCheckpoint::new(dir.path());

        checkpoint
            .save(
                &PostgresOffset {
                    lsn: 200,
                    slot_name: "slot-a".into(),
                },
                10,
            )
            .await
            .unwrap();

        let error = checkpoint
            .save(
                &PostgresOffset {
                    lsn: 150,
                    slot_name: "slot-a".into(),
                },
                9,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, crate::core::Error::CheckpointError(_)));
    }

    #[tokio::test]
    async fn file_checkpoint_rejects_conflicting_equal_count_offset() {
        let dir = tempdir().unwrap();
        let mut checkpoint = FileCheckpoint::new(dir.path());

        checkpoint
            .save(
                &PostgresOffset {
                    lsn: 300,
                    slot_name: "slot-a".into(),
                },
                21,
            )
            .await
            .unwrap();

        let error = checkpoint
            .save(
                &PostgresOffset {
                    lsn: 301,
                    slot_name: "slot-a".into(),
                },
                21,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, crate::core::Error::CheckpointError(_)));
    }

    /// PID 1 (init/launchd) is always alive on Unix; a lease file claiming
    /// ownership by PID 1 should trigger a conflict error because the process
    /// is running and the current process is not PID 1.
    #[tokio::test]
    #[cfg(unix)]
    async fn file_checkpoint_rejects_owner_lease_conflict_from_live_pid() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join(FileCheckpoint::OWNER_LEASE_FILENAME);
        // PID 1 (init/launchd) is always alive. Use HOSTNAME:PID format.
        let lease = crate::checkpoint::owner_lease::format_lease(
            crate::checkpoint::owner_lease::current_hostname(),
            1,
        );
        std::fs::write(&lock_path, lease.as_bytes()).unwrap();

        let checkpoint = FileCheckpoint::new(dir.path());
        let error = checkpoint.get_committed_count().await.unwrap_err();
        assert!(matches!(error, crate::core::Error::CheckpointError(_)));
    }

    /// A lease file claiming ownership by a PID that no longer exists should
    /// be auto-cleared so the new process can start without manual intervention.
    #[tokio::test]
    #[cfg(unix)]
    async fn file_checkpoint_recovers_from_stale_owner_lease_of_dead_process() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join(FileCheckpoint::OWNER_LEASE_FILENAME);
        // PID u32::MAX is extremely unlikely to be alive. Use HOSTNAME:PID format.
        let stale_lease = crate::checkpoint::owner_lease::format_lease(
            crate::checkpoint::owner_lease::current_hostname(),
            u32::MAX,
        );
        std::fs::write(&lock_path, stale_lease.as_bytes()).unwrap();

        let checkpoint = FileCheckpoint::new(dir.path());
        // Should succeed: stale lease auto-cleared.
        let count = checkpoint.get_committed_count().await.unwrap();
        assert_eq!(count, 0);
        // Lock file should now contain the current process in HOSTNAME:PID format.
        let contents = std::fs::read_to_string(&lock_path).unwrap();
        let (host, pid) = crate::checkpoint::owner_lease::parse_lease(&contents).unwrap();
        assert_eq!(host, crate::checkpoint::owner_lease::current_hostname());
        assert_eq!(pid, std::process::id());
    }

    #[tokio::test]
    async fn file_checkpoint_allows_reentrant_owner_lease_within_process() {
        let dir = tempdir().unwrap();
        let mut writer = FileCheckpoint::new(dir.path());
        let reader = FileCheckpoint::new(dir.path());

        writer
            .save(
                &PostgresOffset {
                    lsn: 77,
                    slot_name: "slot-a".into(),
                },
                5,
            )
            .await
            .unwrap();

        let loaded = reader.load().await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(reader.get_committed_count().await.unwrap(), 5);
    }
}
