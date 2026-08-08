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
    /// Progress of an in-flight incremental (DBLog) snapshot, when one is running.
    ///
    /// Stored with the stream position rather than in a file of its own: a chunk
    /// cursor is only meaningful relative to the stream position it was captured
    /// against, so the two must become durable in the same atomic write. Without
    /// this the snapshot restarts from row zero on every process restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incremental_snapshot: Option<crate::source::IncrementalSnapshotState>,
}

impl PostgresOffset {
    /// Decode an offset from its [`Offset::encode`] representation.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    /// Construct a stream offset with no incremental-snapshot state attached.
    pub fn new(lsn: u64, slot_name: impl Into<String>) -> Self {
        Self {
            lsn,
            slot_name: slot_name.into(),
            incremental_snapshot: None,
        }
    }

    /// Attach incremental-snapshot progress to this offset.
    #[must_use]
    pub fn with_incremental_snapshot(
        mut self,
        state: Option<crate::source::IncrementalSnapshotState>,
    ) -> Self {
        self.incremental_snapshot = state;
        self
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
    /// Progress of an in-flight incremental (DBLog) snapshot, when one is running.
    ///
    /// See [`PostgresOffset::incremental_snapshot`] for why this travels with the
    /// stream position instead of in a separate record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incremental_snapshot: Option<crate::source::IncrementalSnapshotState>,
}

impl MysqlOffset {
    /// Decode an offset from its [`Offset::encode`] representation.
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
            incremental_snapshot: None,
        }
    }

    /// Attach incremental-snapshot progress to this offset.
    #[must_use]
    pub fn with_incremental_snapshot(
        mut self,
        state: Option<crate::source::IncrementalSnapshotState>,
    ) -> Self {
        self.incremental_snapshot = state;
        self
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

/// Concrete SQL Server checkpoint offset.
///
/// `cursor` is either a bare `"{lsn}"` window boundary or `"{lsn}:{seqval}:{op}"`,
/// a resume point *inside* a window that a previous poll truncated. The three-part
/// form is what makes a mid-LSN resume representable at all: a single commit can
/// produce more rows than `max_events_per_poll`, and `__$operation` is part of the
/// key because `'all update old'` emits op=3 and op=4 sharing one `(lsn, seqval)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerOffset {
    /// Encoded CDC cursor — `"{lsn}"` or `"{lsn}:{seqval}:{op}"`.
    pub cursor: String,
    /// Progress of an in-flight incremental (DBLog) snapshot, when one is running.
    ///
    /// See [`PostgresOffset::incremental_snapshot`] for why this travels with the
    /// stream position instead of in a separate record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incremental_snapshot: Option<crate::source::IncrementalSnapshotState>,
}

impl SqlServerOffset {
    /// Construct a stream offset with no incremental-snapshot state attached.
    pub fn new(cursor: impl Into<String>) -> Self {
        Self {
            cursor: cursor.into(),
            incremental_snapshot: None,
        }
    }

    /// Decode an offset from its [`Offset::encode`] representation.
    ///
    /// Accepts both the current object form and the **bare JSON string** written before
    /// this offset became a struct — `"0x0000002A00000B58003A"`. A checkpoint written by
    /// 0.7.x is in that older form, and refusing it would fail the load on upgrade with a
    /// serde type error, leaving an operator to guess whether capture had lost its
    /// position. `sqlserver_cursor_from_offset_bytes` already accepts both forms; this
    /// keeps the checkpoint loader from disagreeing with the cursor parser about the very
    /// same bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        match serde_json::from_slice::<Self>(bytes) {
            Ok(offset) => Ok(offset),
            Err(struct_error) => match serde_json::from_slice::<String>(bytes) {
                Ok(cursor) => Ok(Self::new(cursor)),
                // Report the struct error: it describes the form we actually expect.
                Err(_) => Err(struct_error.into()),
            },
        }
    }

    /// Attach incremental-snapshot progress to this offset.
    #[must_use]
    pub fn with_incremental_snapshot(
        mut self,
        state: Option<crate::source::IncrementalSnapshotState>,
    ) -> Self {
        self.incremental_snapshot = state;
        self
    }
}

impl Offset for SqlServerOffset {
    fn source_type(&self) -> &str {
        "sqlserver"
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
    /// Wrap already-encoded offset bytes under a source-type namespace.
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
    /// Durably persist `offset` and the running committed-event count.
    ///
    /// **Must be durable on return**, not merely buffered: the runtime treats a
    /// successful save as the point past which events will not be replayed. An
    /// implementation that returns before the write reaches stable storage converts a
    /// crash into silent data loss.
    ///
    /// Must also be **monotonic** per source: refusing a write that would move progress
    /// backwards is what stops a stale in-flight write from rewinding the position.
    async fn save(&mut self, offset: &dyn Offset, committed_event_count: u64) -> Result<()>;

    /// Load the furthest durable offset, or `None` when none exists.
    ///
    /// A store error **must** surface as `Err`, never as `Ok(None)`. Collapsing the two
    /// makes the runtime resume from the live head of the log, silently skipping
    /// everything since the last durable position.
    async fn load(&self) -> Result<Option<Box<dyn Offset>>>;

    /// Number of events durably committed, or `0` when no checkpoint exists.
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
    /// Number of retained checkpoint entries. Test-only introspection.
    pub fn history_len(&self) -> usize {
        self.entries
            .lock()
            .map(|entries| entries.len())
            .unwrap_or_default()
    }
}

/// File-backed checkpoint store for local durability.
///
/// # One writer per directory
///
/// [`FileCheckpoint::new`] takes an exclusive owner lease on the directory, and a second
/// writable instance against the same directory is **refused**. That is not
/// over-caution: each instance holds its own view of progress and rewrites the whole
/// record, so two of them silently overwrite each other — the checkpoint ends up wherever
/// the last writer happened to be, which is how a resume position regresses or jumps
/// forward with no error anywhere.
///
/// Reading is not dangerous, so it is not restricted. Use
/// [`FileCheckpoint::read_only`] for inspection — a health endpoint, an operator tool, a
/// test assertion — while a runtime owns the directory. A read-only handle takes no lease
/// and refuses to write.
#[derive(Debug)]
pub struct FileCheckpoint {
    /// Directory containing checkpoint files.
    pub checkpoint_dir: PathBuf,
    /// Unix file mode used when creating checkpoint files.
    pub file_mode: u32,
    lease: Mutex<Option<owner_lease::OwnerLease>>,
    /// When set, this handle never acquires the owner lease and refuses to write.
    read_only: bool,
}

impl FileCheckpoint {
    const OWNER_LEASE_FILENAME: &str = ".rustcdc_checkpoint.owner";

    fn source_family(source_type: &str) -> &str {
        source_type.strip_suffix("_snapshot").unwrap_or(source_type)
    }

    /// Create a writable checkpoint store, taking the directory's owner lease.
    ///
    /// The lease is acquired lazily on first use, and a second writable instance against
    /// the same directory is refused — see the type docs. For inspection alongside a
    /// running instance, use [`FileCheckpoint::read_only`].
    pub fn new(checkpoint_dir: impl Into<PathBuf>) -> Self {
        Self {
            checkpoint_dir: checkpoint_dir.into(),
            file_mode: FILE_CHECKPOINT_DEFAULT_FILE_MODE,
            lease: Mutex::new(None),
            read_only: false,
        }
    }

    /// Create a **read-only** handle for inspecting a checkpoint directory.
    ///
    /// Takes no owner lease, so it can be used freely alongside the runtime that owns the
    /// directory — a readiness endpoint reporting the committed count, an operator tool
    /// dumping the resume position, a test asserting progress. Concurrent readers cannot
    /// corrupt anything; only concurrent *writers* can, and this handle cannot write.
    ///
    /// [`Checkpoint::save`] returns a [`crate::core::Error::CheckpointError`] naming the
    /// remedy rather than silently doing nothing.
    ///
    /// ```no_run
    /// use rustcdc::checkpoint::{Checkpoint, FileCheckpoint};
    ///
    /// # async fn example() -> rustcdc::Result<()> {
    /// // The runtime owns this directory; this handle just looks.
    /// let inspector = FileCheckpoint::read_only("/var/rustcdc/checkpoints");
    /// let committed = inspector.get_committed_count().await?;
    /// println!("durably committed: {committed}");
    /// # Ok(())
    /// # }
    /// ```
    pub fn read_only(checkpoint_dir: impl Into<PathBuf>) -> Self {
        Self {
            checkpoint_dir: checkpoint_dir.into(),
            file_mode: FILE_CHECKPOINT_DEFAULT_FILE_MODE,
            lease: Mutex::new(None),
            read_only: true,
        }
    }

    /// Whether this handle refuses to write.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    fn lease_path(&self) -> PathBuf {
        self.checkpoint_dir.join(Self::OWNER_LEASE_FILENAME)
    }

    fn ensure_owner_lease(&self) -> Result<()> {
        // A read-only handle takes no lease: it cannot corrupt anything, and requiring one
        // would make inspecting a directory a runtime owns impossible.
        if self.read_only {
            return Ok(());
        }

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

    /// Confirm this process still owns the checkpoint directory before writing to it.
    ///
    /// Acquiring the lease once is not the same as holding it: an operator can delete
    /// the sentinel file to clear what looks like a stuck lease, and a peer that saw
    /// this process as dead can take it over. Both cases used to leave two writers
    /// against one directory, each advancing the checkpoint from its own view of
    /// progress — which is how a checkpoint silently regresses or jumps forward.
    fn verify_lease_still_held(&self) -> Result<()> {
        if self.read_only {
            return Err(crate::core::Error::CheckpointError(format!(
                "this FileCheckpoint handle for '{}' is read-only and cannot write. It was \
                 created with FileCheckpoint::read_only(), which takes no owner lease so it \
                 can safely inspect a directory another instance owns. Use \
                 FileCheckpoint::new() for the owning instance.",
                self.checkpoint_dir.display()
            )));
        }

        let lease = self.lease.lock().map_err(|_| {
            crate::core::Error::CheckpointError(
                "checkpoint owner lease lock poisoned during verification".into(),
            )
        })?;

        match lease.as_ref() {
            Some(lease) => lease
                .verify_still_held("checkpoint")
                .map_err(|error| crate::core::Error::CheckpointError(error.to_string())),
            None => Err(crate::core::Error::CheckpointError(
                "checkpoint owner lease is not held; refusing to write".into(),
            )),
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

        if let Some(detail) =
            stream_position_regression(source_type, &existing.offset, &next.offset)
        {
            return Err(crate::core::Error::CheckpointError(format!(
                "refusing checkpoint write for source '{source_type}': the stream position \
                 moved backwards ({detail}). The committed-event count still advanced, so \
                 this is not a replay — it means the connector handed the checkpoint a \
                 position it cannot have reached, and writing it would make the next \
                 restart resume before data that is already committed downstream. Either \
                 the source was reset or repointed at a different server (clear the \
                 checkpoint directory and re-snapshot), or this is a connector defect \
                 worth reporting."
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

/// Describe how `next` moved *behind* `existing`, or `None` if it did not.
///
/// The committed-event count catches a checkpoint that forgets progress. It does not
/// catch a checkpoint that keeps counting while the *position* rewinds — which is what
/// a connector bug looks like from here, and is strictly worse: the count says the
/// pipeline is healthy while the recorded resume point now sits before data the sink
/// has already accepted. The MySQL transaction-compression defect had exactly this
/// shape (every commit inside a compressed transaction recorded `<file>:0`), and
/// nothing in the checkpoint layer objected.
///
/// Only the connector-native stream coordinate is compared. Incremental-snapshot chunk
/// cursors travel in the same record but are not totally ordered across tables, and an
/// unrecognised source type is left alone rather than guessed at.
fn stream_position_regression(
    source_type: &str,
    existing: &serde_json::Value,
    next: &serde_json::Value,
) -> Option<String> {
    match source_type {
        "postgres" => {
            // **Only a zero is checked, deliberately.** A PostgreSQL checkpoint offset is
            // the individual change's own WAL LSN, and pgoutput emits changes in *commit*
            // order while each keeps its original position. Two transactions that
            // interleave in the WAL therefore arrive out of LSN order: if A writes at 100,
            // B writes at 110 and commits at 120, and A only commits at 130, the stream
            // yields B's change at 110 before A's at 100. The checkpoint legitimately goes
            // backwards, and resuming from the lower LSN re-reads B — a duplicate, which
            // is the documented at-least-once behaviour. A general comparison here would
            // refuse that write and wedge any pipeline with concurrent writers.
            //
            // Zero is different: it is not a position the stream can reach, so it can only
            // come from a decode or parse defect. That is the shape the MySQL
            // transaction-compression defect took, and the reason this guard exists.
            if existing.get("slot_name") != next.get("slot_name") {
                return None;
            }
            let (before, after) = (existing.get("lsn")?.as_u64()?, next.get("lsn")?.as_u64()?);
            (after == 0 && before > 0).then(|| format!("postgres LSN {before} → 0"))
        }
        "mysql" | "mariadb" => {
            // GTID is the authoritative coordinate whenever the server provides one,
            // and binlog file+position is server-local: after a failover the new
            // primary's coordinates are unrelated to the old one's and routinely
            // lower, which is a legitimate resume rather than a regression. Compare
            // file+position only when there is no GTID to defer to — which is the
            // default configuration, and the one the compression defect corrupted.
            let gtid = next.get("gtid").and_then(serde_json::Value::as_str);
            if gtid.is_some_and(|value| !value.is_empty()) {
                return None;
            }
            if existing.get("source_flavor") != next.get("source_flavor") {
                return None;
            }
            let before = binlog_coordinate(existing)?;
            let after = binlog_coordinate(next)?;
            (after < before).then(|| {
                format!(
                    "mysql binlog position {}:{} → {}:{}",
                    before.0, before.1, after.0, after.1
                )
            })
        }
        "sqlserver" => {
            // The cursor is `"{lsn}"` or `"{lsn}:{seqval}:{op}"`, and **both forms occur in
            // the same stream**: per-event checkpoints carry a bare commit LSN, while an
            // orderly shutdown records the three-part within-window position. Comparing
            // the strings whole would make the bare form look *behind* the three-part form
            // at the same LSN — it is a prefix of it — so the first commit after a
            // graceful restart would be refused and the pipeline would wedge on a false
            // positive.
            //
            // Only the LSN is compared. It is fixed-width lowercase hex with a `0x`
            // prefix, so a string comparison of that field is the numeric one, and
            // seqval-level regression detection within a single commit LSN would add
            // nothing worth this hazard.
            let before = cdc_cursor_lsn(existing)?;
            let after = cdc_cursor_lsn(next)?;
            (after < before).then(|| format!("sqlserver CDC commit LSN {before} → {after}"))
        }
        _ => None,
    }
}

/// The commit-LSN field of a SQL Server CDC cursor, whichever encoding it is in.
fn cdc_cursor_lsn(offset: &serde_json::Value) -> Option<&str> {
    let cursor = offset.get("cursor")?.as_str()?;
    Some(cursor.split_once(':').map_or(cursor, |(lsn, _)| lsn))
}

/// A binlog coordinate ordered the way the server orders it.
///
/// Binlog files are named `<base>.<6-digit sequence>` and roll over past `.999999`
/// into seven digits, so the numeric suffix orders them and the textual name does not
/// (`binlog.1000000` sorts before `binlog.999999` as text). A name that does not carry
/// a numeric suffix is not comparable, so the caller skips the check rather than
/// guessing.
fn binlog_coordinate(offset: &serde_json::Value) -> Option<(u64, u32)> {
    let file = offset.get("binlog_file")?.as_str()?;
    let sequence = file.rsplit_once('.')?.1.parse::<u64>().ok()?;
    let position = u32::try_from(offset.get("binlog_pos")?.as_u64()?).ok()?;
    Some((sequence, position))
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
        // Fence the write: ownership acquired at startup is not ownership now. This also
        // rejects a read-only handle, which by construction never took the lease.
        self.verify_lease_still_held()?;
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
            "sqlserver" => Box::new(SqlServerOffset::from_bytes(&encoded)?),
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
        stream_position_regression, Checkpoint, FileCheckpoint, FileCheckpointRecord,
        InMemoryCheckpoint, MysqlOffset, PostgresOffset, FILE_CHECKPOINT_FORMAT_VERSION,
    };

    #[tokio::test]
    async fn in_memory_checkpoint_round_trips_offsets() {
        let mut checkpoint = InMemoryCheckpoint::default();
        let offset = PostgresOffset {
            lsn: 42,
            slot_name: "slot-a".into(),
            incremental_snapshot: None,
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
            incremental_snapshot: None,
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
            incremental_snapshot: None,
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
            incremental_snapshot: None,
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
                    incremental_snapshot: None,
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
                    incremental_snapshot: None,
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
                    incremental_snapshot: None,
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
                    incremental_snapshot: None,
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
            incremental_snapshot: None,
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
                    incremental_snapshot: None,
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
                    incremental_snapshot: None,
                },
                9,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, crate::core::Error::CheckpointError(_)));
    }

    #[tokio::test]
    async fn file_checkpoint_rejects_a_rewound_stream_position_while_the_count_advances() {
        // The count-based check cannot see this: the pipeline keeps committing events,
        // so `committed_event_count` keeps rising while the recorded resume point moves
        // *behind* data the sink has already accepted. That is what a connector defect
        // looks like from the checkpoint layer, and it used to be written without
        // complaint.
        let dir = tempdir().unwrap();
        let mut checkpoint = FileCheckpoint::new(dir.path());

        checkpoint
            .save(
                &MysqlOffset::new("mysql", "binlog.000042", 88_371, String::new()),
                10,
            )
            .await
            .unwrap();

        let error = checkpoint
            .save(
                &MysqlOffset::new("mysql", "binlog.000042", 0, String::new()),
                20,
            )
            .await
            .unwrap_err();
        let rendered = error.to_string();
        assert!(
            rendered.contains("moved backwards") && rendered.contains("88371"),
            "the error must name the regression so an operator can act on it: {rendered}"
        );
    }

    #[tokio::test]
    async fn a_postgres_checkpoint_may_move_backwards_because_pgoutput_legitimately_does() {
        // pgoutput emits changes in *commit* order while each keeps its own WAL position,
        // so two transactions interleaved in the WAL arrive out of LSN order and the
        // checkpoint genuinely goes backwards. Resuming from the lower LSN re-reads the
        // later-positioned change, which is the documented at-least-once behaviour.
        // Refusing it would wedge every pipeline that has concurrent writers.
        let dir = tempdir().unwrap();
        let mut checkpoint = FileCheckpoint::new(dir.path());

        for (lsn, count) in [(110_u64, 10_u64), (100, 20)] {
            checkpoint
                .save(
                    &PostgresOffset {
                        lsn,
                        slot_name: "slot-a".into(),
                        incremental_snapshot: None,
                    },
                    count,
                )
                .await
                .expect("an out-of-order pgoutput LSN is not a regression");
        }
    }

    #[test]
    fn a_postgres_lsn_of_zero_is_still_caught() {
        // Zero is not a position the stream can reach, so it can only come from a decode
        // or parse defect — the shape the MySQL compression defect took.
        let before = json!({ "lsn": 5_000, "slot_name": "slot-a" });
        let after = json!({ "lsn": 0, "slot_name": "slot-a" });
        assert!(stream_position_regression("postgres", &before, &after).is_some());
        assert!(
            stream_position_regression("postgres", &after, &before).is_none(),
            "recovering from zero must not itself be reported as a regression"
        );
    }

    #[test]
    fn a_binlog_position_of_zero_is_caught_as_a_regression() {
        // The exact shape of the MySQL transaction-compression defect: events unpacked
        // from a compressed payload carried `log_pos = 0`, so the connector offered the
        // checkpoint `<file>:0` while the committed count kept climbing.
        let existing = json!({
            "gtid": "", "binlog_file": "binlog.000042", "binlog_pos": 88_371,
            "source_flavor": "mysql"
        });
        let next = json!({
            "gtid": "", "binlog_file": "binlog.000042", "binlog_pos": 0,
            "source_flavor": "mysql"
        });
        assert!(stream_position_regression("mysql", &existing, &next).is_some());
        assert!(
            stream_position_regression("mysql", &next, &existing).is_none(),
            "recovering from zero must not itself be reported as a regression"
        );
    }

    #[test]
    fn binlog_files_are_ordered_by_sequence_number_not_by_text() {
        // `binlog.1000000` sorts before `binlog.999999` as text, so a rollover past six
        // digits would look like a regression to a string comparison.
        let older = json!({
            "gtid": "", "binlog_file": "binlog.999999", "binlog_pos": 900,
            "source_flavor": "mysql"
        });
        let newer = json!({
            "gtid": "", "binlog_file": "binlog.1000000", "binlog_pos": 4,
            "source_flavor": "mysql"
        });
        assert!(stream_position_regression("mysql", &older, &newer).is_none());
        assert!(stream_position_regression("mysql", &newer, &older).is_some());
    }

    #[test]
    fn a_gtid_positioned_stream_is_not_judged_on_server_local_coordinates() {
        // After a failover the new primary's binlog coordinates are unrelated to the
        // old primary's and are routinely lower. GTID is what actually resumes the
        // stream there, so file+position must not veto a legitimate recovery.
        let before = json!({
            "gtid": "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-500",
            "binlog_file": "binlog.000042", "binlog_pos": 88_371,
            "source_flavor": "mysql"
        });
        let after = json!({
            "gtid": "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-620",
            "binlog_file": "binlog.000001", "binlog_pos": 4,
            "source_flavor": "mysql"
        });
        assert!(stream_position_regression("mysql", &before, &after).is_none());
    }

    #[test]
    fn a_renamed_replication_slot_is_not_comparable() {
        // Two slots are two independent WAL positions; comparing them says nothing.
        let before = json!({ "lsn": 5_000, "slot_name": "slot-a" });
        let after = json!({ "lsn": 10, "slot_name": "slot-b" });
        assert!(stream_position_regression("postgres", &before, &after).is_none());
    }

    #[test]
    fn a_sqlserver_cursor_regression_is_caught_across_both_encodings() {
        let later = json!({ "cursor": "0x000000230000015a0004:0x000000230000015a0005:2" });
        let earlier = json!({ "cursor": "0x000000230000015a0002" });
        assert!(stream_position_regression("sqlserver", &later, &earlier).is_some());
        assert!(stream_position_regression("sqlserver", &earlier, &later).is_none());
    }

    #[test]
    fn the_two_sqlserver_cursor_encodings_do_not_look_like_a_regression_at_one_lsn() {
        // Both forms occur in the same stream: an orderly shutdown records the three-part
        // within-window position, and the per-event checkpoints that follow a restart carry
        // a bare commit LSN. The bare form is a *prefix* of the three-part one, so a whole
        // string comparison would call the first commit after a graceful restart a
        // regression and wedge the pipeline on a false positive.
        let three_part = json!({ "cursor": "0x000000230000015a0004:0x000000230000015a0005:2" });
        let bare = json!({ "cursor": "0x000000230000015a0004" });
        assert!(
            stream_position_regression("sqlserver", &three_part, &bare).is_none(),
            "the same commit LSN in the other encoding must not be read as a rewind"
        );
        assert!(stream_position_regression("sqlserver", &bare, &three_part).is_none());
    }

    #[test]
    fn an_unrecognised_source_type_is_left_alone() {
        let before = json!({ "anything": 9 });
        let after = json!({ "anything": 1 });
        assert!(stream_position_regression("custom-source", &before, &after).is_none());
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
                    incremental_snapshot: None,
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
                    incremental_snapshot: None,
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
    async fn a_second_file_checkpoint_on_the_same_directory_is_refused() {
        // This test previously asserted that a second `FileCheckpoint` on the same
        // directory worked ("re-entrant lease"). It did — and that is exactly how two
        // instances silently overwrote each other's records, because each holds its
        // own lease state and rewrites the whole file. One instance per directory.
        let dir = tempdir().unwrap();
        let mut writer = FileCheckpoint::new(dir.path());

        writer
            .save(&PostgresOffset::new(77, "slot-a"), 5)
            .await
            .unwrap();

        let second = FileCheckpoint::new(dir.path());
        let error = second
            .load()
            .await
            .expect_err("a second instance must be refused, not silently allowed");
        assert!(
            error
                .to_string()
                .contains("already held by another instance in this process"),
            "error must name the real cause; got: {error}"
        );

        // The single owning instance keeps working, and sequential reuse is fine.
        assert_eq!(writer.get_committed_count().await.unwrap(), 5);
        drop(writer);
        let reopened = FileCheckpoint::new(dir.path());
        assert_eq!(reopened.get_committed_count().await.unwrap(), 5);
    }
}

#[cfg(test)]
mod sqlserver_offset_compat_tests {
    use super::SqlServerOffset;
    use crate::core::Offset;

    #[test]
    fn the_current_object_form_round_trips() {
        let offset = SqlServerOffset::new("0x0000002A00000B58003A");
        let decoded =
            SqlServerOffset::from_bytes(&offset.encode().expect("encode")).expect("decode");
        assert_eq!(decoded.cursor, "0x0000002A00000B58003A");
        assert!(decoded.incremental_snapshot.is_none());
    }

    #[test]
    fn a_pre_0_8_bare_string_checkpoint_still_loads() {
        // 0.7.x wrote the cursor as a bare JSON string. Rejecting it would fail the load
        // on upgrade with a serde type error and leave capture without a position.
        let legacy = serde_json::to_vec("0x0000002A00000B58003A").expect("encode legacy");
        let decoded = SqlServerOffset::from_bytes(&legacy).expect("legacy form must load");
        assert_eq!(decoded.cursor, "0x0000002A00000B58003A");
        assert!(decoded.incremental_snapshot.is_none());
    }

    #[test]
    fn incremental_snapshot_state_survives_the_object_form() {
        let offset = SqlServerOffset::new("0x01").with_incremental_snapshot(Some(
            crate::source::IncrementalSnapshotState {
                snapshot_id: "snap-1".into(),
                tables: Vec::new(),
            },
        ));
        let decoded =
            SqlServerOffset::from_bytes(&offset.encode().expect("encode")).expect("decode");
        assert_eq!(
            decoded
                .incremental_snapshot
                .as_ref()
                .map(|state| state.snapshot_id.as_str()),
            Some("snap-1"),
        );
    }

    #[test]
    fn genuinely_malformed_bytes_report_the_object_form_error() {
        // The message must describe the form we expect, not the legacy fallback.
        let error = SqlServerOffset::from_bytes(b"{not json").expect_err("must reject");
        assert!(!error.to_string().is_empty());
    }
}
