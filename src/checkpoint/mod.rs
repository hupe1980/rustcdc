//! Checkpoint abstractions and in-memory implementations.

mod barrier;
pub(crate) mod owner_lease;

use std::{
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::core::{Offset, Result};

pub use barrier::{BarrierState, CommitBarrier};

const FILE_CHECKPOINT_FORMAT_VERSION: u16 = 2;
const FILE_CHECKPOINT_DEFAULT_FILE_MODE: u32 = 0o600;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileCheckpointRecord {
    checkpoint_format_version: u16,
    source_type: String,
    committed_event_count: u64,
    offset: serde_json::Value,
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
    pub gtid: String,
    /// Binlog file containing the committed position.
    pub binlog_file: String,
    /// Position inside the binlog file.
    pub binlog_pos: u32,
}

impl MysqlOffset {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

impl Offset for MysqlOffset {
    fn source_type(&self) -> &str {
        "mysql"
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
#[derive(Debug, Clone, Default)]
pub struct InMemoryCheckpoint {
    entries: Arc<Mutex<VecDeque<StoredCheckpoint>>>,
}

impl InMemoryCheckpoint {
    #[cfg(test)]
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
    lease: Mutex<Option<FileCheckpointLease>>,
}

#[derive(Debug, Clone)]
struct FileCheckpointLease {
    lock_path: PathBuf,
}

static FILE_CHECKPOINT_LEASE_REFS: OnceLock<Mutex<std::collections::HashMap<PathBuf, usize>>> =
    OnceLock::new();

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

    fn lease_ref_counts() -> &'static Mutex<std::collections::HashMap<PathBuf, usize>> {
        FILE_CHECKPOINT_LEASE_REFS.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
    }

    fn increment_lease_ref(lock_path: &Path) {
        if let Ok(mut refs) = Self::lease_ref_counts().lock() {
            let entry = refs.entry(lock_path.to_path_buf()).or_insert(0);
            *entry = entry.saturating_add(1);
        }
    }

    fn decrement_lease_ref(lock_path: &Path) -> usize {
        let Ok(mut refs) = Self::lease_ref_counts().lock() else {
            return 0;
        };

        let Some(entry) = refs.get_mut(lock_path) else {
            return 0;
        };

        if *entry > 1 {
            *entry -= 1;
            *entry
        } else {
            refs.remove(lock_path);
            0
        }
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
        let owner_pid = std::process::id();
        let owner_hostname = owner_lease::current_hostname();
        let owner_lease_str = owner_lease::format_lease(owner_hostname, owner_pid);

        let create_result = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_path);

        match create_result {
            Ok(mut lock_file) => {
                self.write_permissions(&lock_file)?;
                lock_file
                    .write_all(owner_lease_str.as_bytes())
                    .map_err(crate::core::Error::from)?;
                lock_file.sync_all().map_err(crate::core::Error::from)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let parsed = fs::read_to_string(&lock_path)
                    .ok()
                    .and_then(|contents| owner_lease::parse_lease(&contents));

                match parsed {
                    Some((ref host, pid)) if host == owner_hostname && pid == owner_pid => {
                        // Current process on this host already owns the lease.
                    }
                    Some((ref host, pid)) if host != owner_hostname => {
                        tracing::error!(
                            target: "rustcdc::checkpoint",
                            checkpoint_dir = %self.checkpoint_dir.display(),
                            lease_host = %host,
                            lease_pid = pid,
                            current_host = %owner_hostname,
                            "cross-host checkpoint owner lease detected — NFS sharing not supported"
                        );
                        return Err(crate::core::Error::CheckpointError(format!(
                            "checkpoint owner lease conflict for '{}': lease belongs to host '{}' \
                             pid {}. Cross-host NFS sharing is not supported; use a dedicated \
                             checkpoint directory per runtime instance.",
                            self.checkpoint_dir.display(),
                            host,
                            pid
                        )));
                    }
                    Some((_, pid)) if !Self::is_pid_alive(pid) => {
                        tracing::warn!(
                            target: "rustcdc::checkpoint",
                            checkpoint_dir = %self.checkpoint_dir.display(),
                            stale_owner_pid = pid,
                            "clearing stale checkpoint owner lease left by dead process"
                        );
                        let _ = fs::remove_file(&lock_path);
                        let mut lock_file = OpenOptions::new()
                            .create_new(true)
                            .write(true)
                            .open(&lock_path)
                            .map_err(crate::core::Error::from)?;
                        self.write_permissions(&lock_file)?;
                        lock_file
                            .write_all(owner_lease_str.as_bytes())
                            .map_err(crate::core::Error::from)?;
                        lock_file.sync_all().map_err(crate::core::Error::from)?;
                    }
                    _ => {
                        return Err(crate::core::Error::CheckpointError(format!(
                            "checkpoint owner lease conflict for '{}': lock owned by another \
                             process. Use a dedicated checkpoint directory per runtime process.",
                            self.checkpoint_dir.display(),
                        )));
                    }
                }
            }
            Err(error) => return Err(crate::core::Error::from(error)),
        }

        Self::increment_lease_ref(&lock_path);
        *lease = Some(FileCheckpointLease { lock_path });
        Ok(())
    }

    /// Check whether a process with the given PID is currently alive.
    ///
    /// Uses `ps -p <pid>` which exits 0 when the PID exists (regardless of
    /// permissions to signal it) and exits non-zero when the PID is absent.
    /// This correctly distinguishes ESRCH (dead) from EPERM (alive but
    /// unowned) — unlike `kill -0` which returns non-zero for both.
    ///
    /// On non-Unix platforms, conservatively returns `true` to avoid
    /// accidentally clearing leases held by live processes.
    fn is_pid_alive(pid: u32) -> bool {
        #[cfg(unix)]
        {
            std::process::Command::new("ps")
                .args(["-p", &pid.to_string()])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(true) // conservatively assume alive on error
        }
        #[cfg(not(unix))]
        {
            let _ = pid;
            true // cannot check without platform API; conserve existing behavior
        }
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

        records.sort_by(|(left_time, left_path, _), (right_time, right_path, _)| {
            left_time
                .cmp(right_time)
                .then_with(|| left_path.cmp(right_path))
        });

        Ok(records.pop().map(|(_, _, record)| record))
    }

    fn read_record(path: &Path) -> Result<FileCheckpointRecord> {
        Self::check_file_permissions(path)?;
        let record: FileCheckpointRecord =
            serde_json::from_slice(&fs::read(path).map_err(crate::core::Error::from)?)
                .map_err(crate::core::Error::from)?;
        Self::validate_record_version(path, &record)?;
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
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            file.set_permissions(std::fs::Permissions::from_mode(self.file_mode))
                .map_err(crate::core::Error::from)?;
        }
        Ok(())
    }

    fn sync_parent_directory(&self, file_path: &Path) -> Result<()> {
        #[cfg(unix)]
        {
            let Some(parent) = file_path.parent() else {
                return Ok(());
            };

            let directory = File::open(parent).map_err(crate::core::Error::from)?;
            directory.sync_all().map_err(crate::core::Error::from)?;
        }

        Ok(())
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
}

#[async_trait]
impl Checkpoint for FileCheckpoint {
    async fn save(&mut self, offset: &dyn Offset, committed_event_count: u64) -> Result<()> {
        self.ensure_owner_lease()?;
        self.ensure_directory()?;

        let source_type = offset.source_type().to_string();
        let record = FileCheckpointRecord {
            checkpoint_format_version: FILE_CHECKPOINT_FORMAT_VERSION,
            source_type: source_type.clone(),
            committed_event_count,
            offset: serde_json::from_slice(&offset.encode()?)?,
        };
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
        let Ok(mut lease) = self.lease.lock() else {
            return;
        };
        let Some(current_lease) = lease.take() else {
            return;
        };

        if Self::decrement_lease_ref(&current_lease.lock_path) == 0 {
            let _ = fs::remove_file(current_lease.lock_path);
        }
    }
}

#[async_trait]
impl Checkpoint for InMemoryCheckpoint {
    async fn save(&mut self, offset: &dyn Offset, committed_event_count: u64) -> Result<()> {
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
        Checkpoint, FileCheckpoint, InMemoryCheckpoint, MysqlOffset, PostgresOffset,
        FILE_CHECKPOINT_FORMAT_VERSION,
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

    #[tokio::test]
    async fn file_checkpoint_rejects_missing_directory() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("missing");
        let mut checkpoint = FileCheckpoint::new(&missing);
        let offset = MysqlOffset {
            gtid: "1-2-3".into(),
            binlog_file: "binlog.000001".into(),
            binlog_pos: 4,
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

    #[tokio::test]
    async fn file_checkpoint_allows_snapshot_and_stream_variants_in_single_directory() {
        let dir = tempdir().unwrap();
        let checkpoint = FileCheckpoint::new(dir.path());

        let snapshot_path = dir.path().join("checkpoint_postgres_snapshot.json");
        let stream_path = dir.path().join("checkpoint_postgres.json");

        let snapshot_record = json!({
            "checkpoint_format_version": FILE_CHECKPOINT_FORMAT_VERSION,
            "source_type": "postgres_snapshot",
            "committed_event_count": 3,
            "offset": {
                "snapshot_id": "snap-1"
            }
        });
        let stream_record = json!({
            "checkpoint_format_version": FILE_CHECKPOINT_FORMAT_VERSION,
            "source_type": "postgres",
            "committed_event_count": 9,
            "offset": {
                "lsn": 99,
                "slot_name": "slot-a"
            }
        });

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
