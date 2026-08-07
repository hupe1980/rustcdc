#[cfg(test)]
use std::path::PathBuf;
use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::Instant,
};

use arrow_array::{builder::StringDictionaryBuilder, types::Int8Type, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::ErrorKind;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{
    RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use rustcdc::{
    core::{Error as RtError, Event},
    fingerprint_event_stable,
    sink::SinkAdapter,
};
use tokio::time::{sleep, Duration};

use crate::config::schema::{IcebergSchemaMode, IcebergSinkConfig, IcebergStorageConfig};

pub struct IcebergSink {
    cfg: IcebergSinkConfig,
    pending: Vec<Event>,
    /// Running byte estimate for the in-memory pending buffer.
    /// Used to enforce `cfg.max_pending_bytes` backpressure.
    pending_bytes: usize,
    closed: bool,
    catalog: Arc<RestCatalog>,
    table_ident: TableIdent,
    file_name_prefix: String,
    flush_lock: Arc<tokio::sync::Mutex<()>>,
    flush_concurrent_attempts: Arc<AtomicU64>,
    flush_lock_contention_ms_total: Arc<AtomicU64>,
    flush_lock_contention_ms_max: Arc<AtomicU64>,
    /// Counts data files written to storage that could not be committed to the
    /// Iceberg catalog (orphaned by a terminal commit failure). Non-zero values
    /// indicate storage bloat that an operator should clean up manually.
    orphaned_data_files_total: Arc<AtomicU64>,
    /// Epoch-millis of the last snapshot-expiry run; `0` means "never run".
    last_snapshot_expiry_ms: Arc<AtomicU64>,
}

/// Wall-clock milliseconds since the Unix epoch.
fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl IcebergSink {
    pub async fn open(cfg: &IcebergSinkConfig) -> rustcdc::core::Result<Self> {
        cfg.validate().map_err(RtError::ConfigError)?;

        // Create the local table directory only for filesystem-backed warehouses.
        // Cloud-backed warehouses (S3, GCS, ADLS) do not use the local table_path
        // as actual storage; attempting fs::create_dir_all on an s3:// path would
        // silently create a local directory with that literal name.
        let warehouse = cfg.catalog.rest.warehouse.trim();
        let is_local = warehouse.starts_with("file://")
            || warehouse.starts_with('/')
            || (!warehouse.contains("://"));
        if is_local {
            fs::create_dir_all(&cfg.table_path).map_err(RtError::IoError)?;
        }

        let storage_factory = {
            // If the user left storage at the default (LocalFs) but the
            // warehouse URI indicates a cloud backend, auto-infer the factory.
            // This preserves backward compatibility while enabling cloud URIs
            // without requiring explicit storage config.
            let effective_storage = match &cfg.storage {
                IcebergStorageConfig::LocalFs if !is_local => infer_storage_config(warehouse),
                other => other.clone(),
            };
            build_storage_factory_from(&effective_storage, cfg)?
        };

        let mut catalog_props = HashMap::from([
            (
                REST_CATALOG_PROP_URI.to_string(),
                cfg.catalog.rest.uri.trim().to_string(),
            ),
            (
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                warehouse.to_string(),
            ),
        ]);
        if let Some(token) = &cfg.catalog.rest.token {
            let resolved = token.resolve().map_err(|e| {
                RtError::ConfigError(format!(
                    "sink.iceberg.catalog.rest.token could not be resolved: {e}"
                ))
            })?;
            catalog_props.insert("token".to_string(), resolved.trim().to_string());
        }
        if let Some(credential) = &cfg.catalog.rest.credential {
            let resolved = credential.resolve().map_err(|e| {
                RtError::ConfigError(format!(
                    "sink.iceberg.catalog.rest.credential could not be resolved: {e}"
                ))
            })?;
            catalog_props.insert("credential".to_string(), resolved.trim().to_string());
        }

        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(storage_factory))
            .load("cdc-rest", catalog_props)
            .await
            .map_err(map_iceberg_error)?;

        let namespace = NamespaceIdent::new(cfg.namespace.clone());
        let table_ident = TableIdent::new(namespace.clone(), cfg.table_name.clone());

        if !catalog
            .namespace_exists(&namespace)
            .await
            .map_err(map_iceberg_error)?
        {
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .map_err(map_iceberg_error)?;
        }

        ensure_table_exists(
            &catalog,
            &cfg.table_path,
            &namespace,
            &table_ident,
            cfg.schema_mode,
        )
        .await?;

        Ok(Self {
            cfg: cfg.clone(),
            pending: Vec::new(),
            pending_bytes: 0,
            closed: false,
            catalog: Arc::new(catalog),
            table_ident,
            file_name_prefix: format!("cdc-{}", uuid::Uuid::new_v4().simple()),
            flush_lock: Arc::new(tokio::sync::Mutex::new(())),
            flush_concurrent_attempts: Arc::new(AtomicU64::new(0)),
            flush_lock_contention_ms_total: Arc::new(AtomicU64::new(0)),
            flush_lock_contention_ms_max: Arc::new(AtomicU64::new(0)),
            orphaned_data_files_total: Arc::new(AtomicU64::new(0)),
            last_snapshot_expiry_ms: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn send_event_json_bytes(
        &mut self,
        event: &Event,
        _event_json: &[u8],
    ) -> rustcdc::core::Result<()> {
        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }

        // Accurate byte estimate: use the actual serialized JSON length of
        // the row payload rather than a fixed 256-byte constant.  This bounds
        // the pending buffer correctly for wide rows (JSON columns, text blobs)
        // while only paying the serialization cost once per event.
        let after_bytes = event
            .after
            .as_ref()
            .map(|v| v.to_string().len())
            .unwrap_or(0);
        let before_bytes = event
            .before
            .as_ref()
            .map(|v| v.to_string().len())
            .unwrap_or(0);
        let event_byte_estimate =
            std::mem::size_of::<Event>() + event.table.len() + after_bytes + before_bytes;

        if self.pending.len() >= self.cfg.max_pending_events
            || self.pending_bytes + event_byte_estimate > self.cfg.max_pending_bytes
        {
            tracing::warn!(
                metric = "rustcdc_iceberg_pending_limit_flush",
                pending_events = self.pending.len(),
                pending_bytes = self.pending_bytes,
                max_pending_events = self.cfg.max_pending_events,
                max_pending_bytes = self.cfg.max_pending_bytes,
                "iceberg pending buffer limit reached; triggering early flush"
            );
            self.commit_pending_with_retry().await?;
        }

        self.pending_bytes += event_byte_estimate;
        self.pending.push(event.clone());
        Ok(())
    }

    /// Decode a serialised event and buffer it through the guarded path.
    ///
    /// This used to push straight onto `pending` with no byte accounting and no
    /// `max_pending_events` / `max_pending_bytes` check — a second entry point that
    /// bypassed the backpressure guard entirely. The guard exists because this buffer
    /// grows during catalog outages, which is exactly when an unbounded second door
    /// gets used. It now delegates to [`Self::send_event_json_bytes`].
    pub async fn send_json_bytes(&mut self, event_json: &[u8]) -> rustcdc::core::Result<()> {
        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }

        let event: Event = serde_json::from_slice(event_json).map_err(|e| {
            RtError::SerializationError(format!("failed to decode event json: {e}"))
        })?;
        self.send_event_json_bytes(&event, event_json).await
    }

    async fn commit_pending_with_retry(&mut self) -> rustcdc::core::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let started_at = Instant::now();
        let snapshot_rows = self.pending.len();
        let write_table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(map_iceberg_error)?;
        let batch = events_to_record_batch(&self.pending, self.cfg.schema_mode)?;
        let data_files =
            write_data_files(&write_table, batch, &self.file_name_prefix, &self.cfg).await?;

        let mut backoff_ms = self.cfg.retry_backoff_ms;
        for attempt in 1..=self.cfg.max_commit_retries {
            let table = self
                .catalog
                .load_table(&self.table_ident)
                .await
                .map_err(map_iceberg_error)?;
            let tx = Transaction::new(&table);
            let append_action = tx.fast_append().add_data_files(data_files.clone());
            let tx = append_action
                .apply(tx)
                .map_err(|e| RtError::StateError(format!("iceberg append action failed: {e}")))?;

            match tx.commit(self.catalog.as_ref()).await {
                Ok(_) => {
                    tracing::info!(
                        metric = "rustcdc_iceberg_commit",
                        outcome = "success",
                        snapshot_rows,
                        attempt,
                        retries = attempt - 1,
                        latency_us = started_at.elapsed().as_micros() as u64,
                        "iceberg commit completed"
                    );
                    self.pending.clear();
                    self.pending_bytes = 0;
                    self.maybe_expire_snapshots().await;
                    return Ok(());
                }
                Err(err) if attempt < self.cfg.max_commit_retries && should_retry_commit(&err) => {
                    let failure_class = commit_failure_class(&err);
                    tracing::warn!(
                        metric = "rustcdc_iceberg_commit",
                        outcome = "retry",
                        failure_class,
                        attempt,
                        max_attempts = self.cfg.max_commit_retries,
                        backoff_ms,
                        retryable = err.retryable(),
                        error = %err,
                        "iceberg commit failed, retrying"
                    );
                    sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms.saturating_mul(2)).min(self.cfg.retry_backoff_max_ms);
                }
                Err(err) => {
                    let failure_class = commit_failure_class(&err);
                    let orphaned_paths: Vec<String> = data_files
                        .iter()
                        .map(|f| f.file_path().to_owned())
                        .collect();
                    let orphaned_count = orphaned_paths.len() as u64;
                    tracing::error!(
                        metric = "rustcdc_iceberg_commit",
                        outcome = "failure",
                        failure_class,
                        snapshot_rows,
                        attempt,
                        retries = attempt - 1,
                        latency_us = started_at.elapsed().as_micros() as u64,
                        retryable = err.retryable(),
                        error = %err,
                        orphaned_data_file_count = orphaned_count,
                        ?orphaned_paths,
                        "iceberg commit failed; attempting best-effort orphan cleanup"
                    );

                    // Best-effort cleanup so storage does not accumulate unboundedly.
                    // Failure to delete is logged but does not mask the original error.
                    //
                    // This goes through the table's own `FileIO`, not `tokio::fs`:
                    // `DataFile::file_path()` is a warehouse URI, so on S3, GCS or ADLS
                    // — where an orphaned file costs money for as long as it exists —
                    // `remove_file("s3://bucket/…")` could only ever fail with
                    // NotFound, and every cloud deployment leaked silently.
                    let file_io = write_table.file_io();
                    for path in &orphaned_paths {
                        match file_io.delete(path).await {
                            Ok(()) => tracing::info!(
                                orphaned_path = %path,
                                "deleted orphaned iceberg data file"
                            ),
                            Err(e) => tracing::warn!(
                                orphaned_path = %path,
                                error = %e,
                                "failed to delete orphaned iceberg data file; \
                                 manual cleanup required"
                            ),
                        }
                    }

                    self.orphaned_data_files_total
                        .fetch_add(orphaned_count, Ordering::Relaxed);
                    return Err(RtError::StateError(format!(
                        "iceberg commit failed after {attempt} attempt(s): {err}"
                    )));
                }
            }
        }

        Err(RtError::StateError(
            "iceberg commit retry loop exhausted unexpectedly".to_string(),
        ))
    }

    /// Expire old snapshots, at most once per `snapshot_expiry.interval_ms`.
    ///
    /// A CDC sink commits on every flush, so the table gains a snapshot per flush and
    /// its metadata is read in full on every planning pass. Nothing prunes that on its
    /// own, so a long-running pipeline degrades read planning until someone runs
    /// maintenance by hand.
    ///
    /// Failure is logged, never propagated: expiry is housekeeping, and the events it
    /// runs after are already durably committed. Turning a maintenance hiccup into a
    /// delivery error would fail a batch that succeeded.
    async fn maybe_expire_snapshots(&self) {
        let expiry = &self.cfg.snapshot_expiry;
        if !expiry.enabled {
            return;
        }

        let now_ms = now_epoch_ms();
        let last = self.last_snapshot_expiry_ms.load(Ordering::Relaxed);
        if last != 0 && now_ms.saturating_sub(last) < expiry.interval_ms {
            return;
        }
        // Claim the slot before doing the work so two concurrent flushes cannot both
        // start an expiry against the same table.
        if self
            .last_snapshot_expiry_ms
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let older_than_ms = now_ms.saturating_sub(expiry.older_than_ms) as i64;
        let result = async {
            let table = self.catalog.load_table(&self.table_ident).await?;
            let tx = Transaction::new(&table);
            let action = tx
                .expire_snapshots()
                .expire_older_than_ms(older_than_ms)
                .retain_last(expiry.retain_last);
            let tx = action.apply(tx)?;
            tx.commit(self.catalog.as_ref()).await
        }
        .await;

        match result {
            Ok(_) => tracing::info!(
                metric = "rustcdc_iceberg_snapshot_expiry",
                outcome = "success",
                retain_last = expiry.retain_last,
                older_than_ms = expiry.older_than_ms,
                "expired old iceberg snapshots"
            ),
            Err(e) => tracing::warn!(
                metric = "rustcdc_iceberg_snapshot_expiry",
                outcome = "failure",
                error = %e,
                "iceberg snapshot expiry failed; table metadata will keep growing \
                 until the next attempt succeeds"
            ),
        }
    }

    fn observe_flush_lock_wait(&self, wait_ms: u64) {
        if wait_ms == 0 {
            return;
        }

        self.flush_concurrent_attempts
            .fetch_add(1, Ordering::Relaxed);
        self.flush_lock_contention_ms_total
            .fetch_add(wait_ms, Ordering::Relaxed);

        let mut current_max = self.flush_lock_contention_ms_max.load(Ordering::Relaxed);
        while wait_ms > current_max {
            match self.flush_lock_contention_ms_max.compare_exchange_weak(
                current_max,
                wait_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current_max = observed,
            }
        }
    }

    pub fn flush_lock_contention_events_total(&self) -> u64 {
        self.flush_concurrent_attempts.load(Ordering::Relaxed)
    }

    pub fn flush_lock_contention_ms_total(&self) -> u64 {
        self.flush_lock_contention_ms_total.load(Ordering::Relaxed)
    }

    pub fn flush_lock_contention_ms_max(&self) -> u64 {
        self.flush_lock_contention_ms_max.load(Ordering::Relaxed)
    }

    /// Total number of data files written to storage but never committed to the
    /// catalog due to terminal commit failures. Exposed as
    /// `cdc_iceberg_orphaned_data_files_total` in Prometheus metrics. Non-zero
    /// values require operator attention to reclaim storage.
    pub fn orphaned_data_files_total(&self) -> u64 {
        self.orphaned_data_files_total.load(Ordering::Relaxed)
    }
}

impl SinkAdapter for IcebergSink {
    fn name(&self) -> &str {
        "iceberg"
    }

    async fn send(&mut self, event: &Event) -> rustcdc::core::Result<()> {
        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }

        self.send_event_json_bytes(event, &[]).await
    }

    async fn flush(&mut self) -> rustcdc::core::Result<()> {
        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }

        let lock_wait_started = Instant::now();
        let flush_lock = Arc::clone(&self.flush_lock);
        let _flush_guard = flush_lock.lock().await;
        let lock_wait_ms = lock_wait_started.elapsed().as_micros() as u64;
        self.observe_flush_lock_wait(lock_wait_ms);
        if lock_wait_ms > 0 {
            tracing::warn!(
                metric = "rustcdc_iceberg_flush_lock_contention_ms",
                lock_wait_ms,
                contention_total_ms = self.flush_lock_contention_ms_total.load(Ordering::Relaxed),
                contention_max_ms = self.flush_lock_contention_ms_max.load(Ordering::Relaxed),
                contention_events = self.flush_concurrent_attempts.load(Ordering::Relaxed),
                "iceberg flush lock contention observed"
            );
        }

        self.commit_pending_with_retry().await
    }

    async fn close(&mut self) -> rustcdc::core::Result<()> {
        if !self.closed {
            self.commit_pending_with_retry().await?;
            self.closed = true;
        }
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed
    }

    fn delivery_guarantee(&self) -> rustcdc::sink::SinkDeliveryGuarantee {
        // Iceberg sinks buffer writes in memory and commit on flush; delivery
        // is at-least-once (duplicates possible after a crash before commit).
        rustcdc::sink::SinkDeliveryGuarantee::AtLeastOnce
    }
}

async fn ensure_table_exists(
    catalog: &RestCatalog,
    table_path: &Path,
    namespace: &NamespaceIdent,
    table_ident: &TableIdent,
    _schema_mode: IcebergSchemaMode,
) -> rustcdc::core::Result<()> {
    if catalog
        .table_exists(table_ident)
        .await
        .map_err(map_iceberg_error)?
    {
        return Ok(());
    }

    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::optional(
                1,
                "event_json",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                2,
                "schema_name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                3,
                "table_name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                4,
                "operation",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                5,
                "event_ts_ms",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                6,
                "source_name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                7,
                "source_offset",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                8,
                "source_ts_ms",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                9,
                "fingerprint_hex",
                Type::Primitive(PrimitiveType::String),
            )),
            // Partial-image marker (rustcdc ≥ 0.7.0): `false` means the source could
            // not supply every column of `after` (PostgreSQL unchanged-TOAST). Kept as
            // a first-class column so data-quality queries can find partial events with
            // a cheap columnar predicate; the full per-column list lives inside
            // `event_json` when `schema_mode = "normalized_with_raw"`.
            Arc::new(NestedField::required(
                10,
                "has_complete_after_image",
                Type::Primitive(PrimitiveType::Boolean),
            )),
        ])
        .build()
        .map_err(map_iceberg_error)?;

    let table_location = to_file_uri(table_path)?;
    let table_creation = TableCreation::builder()
        .name(table_ident.name.clone())
        .schema(schema)
        .location(table_location)
        .properties(HashMap::new())
        .build();

    catalog
        .create_table(namespace, table_creation)
        .await
        .map_err(map_iceberg_error)?;

    Ok(())
}

fn iceberg_field(name: &str, data_type: DataType, nullable: bool, field_id: u32) -> Field {
    let mut metadata = HashMap::new();
    metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
    Field::new(name, data_type, nullable).with_metadata(metadata)
}

fn events_to_record_batch(
    events: &[Event],
    schema_mode: IcebergSchemaMode,
) -> rustcdc::core::Result<RecordBatch> {
    let schema = Arc::new(ArrowSchema::new(vec![
        iceberg_field("event_json", DataType::Utf8, true, 1),
        iceberg_field("schema_name", DataType::Utf8, true, 2),
        iceberg_field("table_name", DataType::Utf8, false, 3),
        iceberg_field(
            "operation",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
            4,
        ),
        iceberg_field("event_ts_ms", DataType::Int64, false, 5),
        iceberg_field("source_name", DataType::Utf8, false, 6),
        iceberg_field("source_offset", DataType::Utf8, false, 7),
        iceberg_field("source_ts_ms", DataType::Int64, false, 8),
        iceberg_field("fingerprint_hex", DataType::Utf8, false, 9),
        iceberg_field("has_complete_after_image", DataType::Boolean, false, 10),
    ]));

    let mut event_json = Vec::with_capacity(events.len());
    let mut schema_name = Vec::with_capacity(events.len());
    let mut table_name = Vec::with_capacity(events.len());
    // Use dictionary encoding for `operation`: the set of CDC operations is tiny
    // (insert/update/delete/read/truncate ≤ 5 distinct values), so dictionary
    // encoding cuts per-row Parquet storage by ~8× and speeds up predicate pushdown.
    let mut operation = StringDictionaryBuilder::<Int8Type>::new();
    let mut event_ts_ms = Vec::with_capacity(events.len());
    let mut source_name = Vec::with_capacity(events.len());
    let mut source_offset = Vec::with_capacity(events.len());
    let mut source_ts_ms = Vec::with_capacity(events.len());
    let mut fingerprint_hex = Vec::with_capacity(events.len());
    let mut has_complete_after_image = Vec::with_capacity(events.len());

    for event in events {
        let raw_event_json = match schema_mode {
            IcebergSchemaMode::Normalized => None,
            IcebergSchemaMode::NormalizedWithRaw => {
                Some(serde_json::to_string(event).map_err(|e| {
                    RtError::SerializationError(format!("iceberg event serialization failed: {e}"))
                })?)
            }
        };
        event_json.push(raw_event_json);
        schema_name.push(event.schema.clone());
        table_name.push(event.table.clone());
        operation.append_value(format!("{:?}", event.op).to_lowercase());
        event_ts_ms.push(i64::try_from(event.ts).unwrap_or(i64::MAX));
        source_name.push(event.source.source_name.clone());
        source_offset.push(event.source.offset.clone());
        source_ts_ms.push(i64::try_from(event.source.timestamp).unwrap_or(i64::MAX));
        fingerprint_hex.push(
            fingerprint_event_stable(event)
                .map_err(|e| RtError::SerializationError(e.to_string()))?,
        );
        has_complete_after_image.push(event.has_complete_after_image());
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(event_json)),
            Arc::new(StringArray::from(schema_name)),
            Arc::new(StringArray::from(table_name)),
            Arc::new(operation.finish()),
            Arc::new(arrow_array::Int64Array::from(event_ts_ms)),
            Arc::new(StringArray::from(source_name)),
            Arc::new(StringArray::from(source_offset)),
            Arc::new(arrow_array::Int64Array::from(source_ts_ms)),
            Arc::new(StringArray::from(fingerprint_hex)),
            Arc::new(arrow_array::BooleanArray::from(has_complete_after_image)),
        ],
    )
    .map_err(|e| RtError::StateError(format!("failed to build record batch: {e}")))
}

async fn write_data_files(
    table: &Table,
    batch: RecordBatch,
    file_name_prefix: &str,
    cfg: &IcebergSinkConfig,
) -> rustcdc::core::Result<Vec<iceberg::spec::DataFile>> {
    let location_generator =
        DefaultLocationGenerator::new(table.metadata()).map_err(map_iceberg_error)?;
    let file_name_generator =
        DefaultFileNameGenerator::new(file_name_prefix.to_string(), None, DataFileFormat::Parquet);
    // parquet's own default is UNCOMPRESSED. CDC payloads are JSON-shaped and highly
    // repetitive, so leaving it there stores several times the bytes for no gain.
    let writer_properties = WriterProperties::builder()
        .set_compression(cfg.parquet_compression.to_parquet())
        .set_max_row_group_row_count(Some(cfg.parquet_row_group_rows))
        .build();
    let parquet_writer_builder =
        ParquetWriterBuilder::new(writer_properties, table.metadata().current_schema().clone());
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let mut data_file_writer = DataFileWriterBuilder::new(rolling_writer_builder)
        .build(None)
        .await
        .map_err(map_iceberg_error)?;

    data_file_writer
        .write(batch)
        .await
        .map_err(map_iceberg_error)?;
    data_file_writer.close().await.map_err(map_iceberg_error)
}

#[cfg(test)]
fn find_latest_metadata_file(table_path: &Path) -> rustcdc::core::Result<Option<String>> {
    let metadata_dir = table_path.join("metadata");
    if !metadata_dir.is_dir() {
        return Ok(None);
    }

    let mut newest: Option<(i32, PathBuf)> = None;
    for entry in fs::read_dir(&metadata_dir).map_err(RtError::IoError)? {
        let entry = entry.map_err(RtError::IoError)?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|v| v.to_str()) else {
            continue;
        };
        let Some(version) = metadata_file_version(name) else {
            continue;
        };

        match &newest {
            Some((current_version, _)) if version <= *current_version => {}
            _ => newest = Some((version, path)),
        }
    }

    Ok(newest.map(|(_, path)| path.to_string_lossy().to_string()))
}

#[cfg(test)]
fn metadata_file_version(file_name: &str) -> Option<i32> {
    let version = file_name.strip_suffix(".metadata.json")?;
    let (version, _) = version.split_once('-')?;
    version.parse::<i32>().ok()
}

fn map_iceberg_error(err: iceberg::Error) -> RtError {
    RtError::StateError(format!("iceberg error: {err}"))
}

fn commit_failure_class(err: &iceberg::Error) -> &'static str {
    if err.kind() == ErrorKind::CatalogCommitConflicts {
        "catalog_commit_conflict"
    } else if err.retryable() {
        "retryable_commit_error"
    } else {
        "commit_error"
    }
}

fn should_retry_commit(err: &iceberg::Error) -> bool {
    matches!(
        commit_failure_class(err),
        "catalog_commit_conflict" | "retryable_commit_error"
    )
}

fn to_file_uri(path: &Path) -> rustcdc::core::Result<String> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(RtError::IoError)?
            .join(path)
    };
    let canonical = absolute.canonicalize().map_err(RtError::IoError)?;

    let file_url = url::Url::from_file_path(&canonical).map_err(|_| {
        RtError::StateError(format!(
            "failed to convert path '{}' to file URI",
            canonical.display()
        ))
    })?;

    Ok(file_url.to_string())
}

/// Build an `OpenDalStorageFactory` for the Iceberg catalog.
///
/// Selects the correct cloud storage backend based on the explicit
/// `storage` config field or auto-detects from the warehouse URI scheme.
/// This replaces the previous hard-coded `OpenDalStorageFactory::Fs` which
/// silently failed (or created spurious local directories) for cloud URIs.
fn build_storage_factory_from(
    storage: &IcebergStorageConfig,
    cfg: &IcebergSinkConfig,
) -> rustcdc::core::Result<OpenDalStorageFactory> {
    match storage {
        IcebergStorageConfig::LocalFs => Ok(OpenDalStorageFactory::Fs),

        IcebergStorageConfig::S3 => {
            // OpenDAL's S3 service resolves credentials via its standard chain:
            // environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
            // AWS_DEFAULT_REGION, AWS_ENDPOINT_URL), shared credential file,
            // and IMDSv2 — in that order.  Set those vars in the process
            // environment before startup or use IAM instance/task roles.
            //
            // iceberg-storage-opendal 0.10 dropped `configured_scheme`: the S3 storage
            // now matches `s3://` and `s3a://` itself, so a warehouse URI written either
            // way resolves without the sink having to sniff the prefix.
            Ok(OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            })
        }

        IcebergStorageConfig::Gcs => {
            // Credentials via Application Default Credentials (ADC):
            // GOOGLE_APPLICATION_CREDENTIALS env var, or gcloud auth,
            // or GCE metadata service.
            Ok(OpenDalStorageFactory::Gcs)
        }

        IcebergStorageConfig::Adls => {
            // Azure credentials via AZURE_STORAGE_ACCOUNT_KEY,
            // AZURE_CLIENT_ID + AZURE_TENANT_ID + AZURE_CLIENT_SECRET,
            // or managed identity.
            //
            // AzureStorageScheme is a private type in iceberg-storage-opendal;
            // construct the factory variant via serde_json deserialization
            // (external tagging with the enum variant name).
            let warehouse = cfg.catalog.rest.warehouse.trim();
            let scheme_str =
                if warehouse.starts_with("abfs://") || warehouse.starts_with("abfss://") {
                    "Abfs"
                } else {
                    "Adls"
                };
            serde_json::from_str::<OpenDalStorageFactory>(&format!(
                r#"{{"Azdls":{{"configured_scheme":"{scheme_str}"}}}}"#
            ))
            .map_err(|e| RtError::ConfigError(format!("Failed to build ADLS storage factory: {e}")))
        }
    }
}

/// Auto-detect `IcebergStorageConfig` variant from a warehouse URI when
/// the user left `storage` at the default `local_fs` but the warehouse
/// indicates a cloud backend.
pub fn infer_storage_config(warehouse: &str) -> IcebergStorageConfig {
    if warehouse.starts_with("s3://") || warehouse.starts_with("s3a://") {
        IcebergStorageConfig::S3
    } else if warehouse.starts_with("gs://") || warehouse.starts_with("gcs://") {
        IcebergStorageConfig::Gcs
    } else if warehouse.starts_with("az://")
        || warehouse.starts_with("abfs://")
        || warehouse.starts_with("abfss://")
    {
        IcebergStorageConfig::Adls
    } else {
        // file://, bare path, or unknown scheme → local FS
        IcebergStorageConfig::LocalFs
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use crate::config::schema::{
        IcebergCatalogConfig, IcebergRestCatalogConfig, IcebergSchemaMode, IcebergWriteMode,
    };
    use arrow_array::{Array, StringArray};
    use iceberg::Error;
    use tokio::sync::Barrier;

    use super::*;

    fn event_json(id: i64, name: &str) -> String {
        serde_json::json!({
            "before": null,
            "after": { "id": id, "name": name },
            "op": "insert",
            "source": {
                "source_name": "postgres",
                "offset": format!("lsn-{id}"),
                "timestamp": 1
            },
            "ts": 1,
            "schema": "public",
            "table": "users",
            "primary_key": ["id"],
            "snapshot": null,
            "transaction": null,
            "envelope_version": 1
        })
        .to_string()
    }

    async fn send_json(sink: &mut IcebergSink, event_json: String) {
        sink.send_json_bytes(event_json.as_bytes())
            .await
            .expect("send_json")
    }

    fn append_cfg(path: PathBuf) -> IcebergSinkConfig {
        let catalog = IcebergCatalogConfig {
            rest: IcebergRestCatalogConfig {
                uri: std::env::var("CDC_TEST_ICEBERG_REST_URI")
                    .unwrap_or_else(|_| "http://127.0.0.1:8181".to_string()),
                warehouse: std::env::var("CDC_TEST_ICEBERG_REST_WAREHOUSE")
                    .unwrap_or_else(|_| "file:///tmp/cdc-iceberg-warehouse".to_string()),
                token: std::env::var("CDC_TEST_ICEBERG_REST_TOKEN")
                    .ok()
                    .map(rustcdc::SecretString::new),
                credential: std::env::var("CDC_TEST_ICEBERG_REST_CREDENTIAL")
                    .ok()
                    .map(rustcdc::SecretString::new),
            },
        };

        IcebergSinkConfig {
            table_path: path,
            catalog,
            namespace: "cdc".to_string(),
            table_name: "events".to_string(),
            write_mode: IcebergWriteMode::Append,
            schema_mode: IcebergSchemaMode::Normalized,
            max_commit_retries: 3,
            retry_backoff_ms: 5,
            retry_backoff_max_ms: 20,
            parquet_compression: Default::default(),
            parquet_row_group_rows: 1_048_576,
            snapshot_expiry: Default::default(),
            max_pending_events: 100_000,
            max_pending_bytes: 256 * 1024 * 1024,
            storage: Default::default(),
        }
    }

    fn has_iceberg_rest_test_env() -> bool {
        std::env::var("CDC_TEST_ICEBERG_REST_URI").is_ok()
            && std::env::var("CDC_TEST_ICEBERG_REST_WAREHOUSE").is_ok()
    }

    #[tokio::test]
    async fn append_mode_writes_iceberg_metadata_and_parquet_files() {
        if !has_iceberg_rest_test_env() {
            eprintln!(
                "skipping iceberg rest integration test (CDC_TEST_ICEBERG_REST_URI/CDC_TEST_ICEBERG_REST_WAREHOUSE not set)"
            );
            return;
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let table_root = dir.path().join("tbl");
        let mut sink = IcebergSink::open(&append_cfg(table_root.clone()))
            .await
            .expect("open sink");

        send_json(&mut sink, event_json(1, "a")).await;
        send_json(&mut sink, event_json(2, "b")).await;
        sink.flush().await.expect("flush");

        assert!(has_file_suffix(&table_root, ".metadata.json"));
        assert!(has_file_suffix(&table_root, ".parquet"));
    }

    #[test]
    fn find_latest_metadata_file_prefers_versioned_filename_over_mtime() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_root = dir.path().join("tbl");
        let metadata_dir = table_root.join("metadata");
        fs::create_dir_all(&metadata_dir).expect("metadata dir");

        let newer_name = "00002-acde-acde-acde-acde00000002.metadata.json";
        let older_name = "00001-acde-acde-acde-acde00000001.metadata.json";

        let newer_path = metadata_dir.join(newer_name);
        let older_path = metadata_dir.join(older_name);

        fs::File::create(&newer_path)
            .and_then(|mut file| file.write_all(b"newer"))
            .expect("write newer metadata");
        fs::File::create(&older_path)
            .and_then(|mut file| file.write_all(b"older"))
            .expect("write older metadata");

        let latest = find_latest_metadata_file(&table_root)
            .expect("latest metadata lookup")
            .expect("metadata file expected");

        assert_eq!(Path::new(&latest), newer_path.as_path());
    }

    #[test]
    fn find_latest_metadata_file_ignores_malformed_metadata_names() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_root = dir.path().join("tbl");
        let metadata_dir = table_root.join("metadata");
        fs::create_dir_all(&metadata_dir).expect("metadata dir");

        let malformed_path = metadata_dir.join("zzz.metadata.json");
        let valid_path = metadata_dir.join("00003-acde-acde-acde-acde00000003.metadata.json");

        fs::File::create(&malformed_path).expect("write malformed metadata");
        fs::File::create(&valid_path).expect("write valid metadata");

        let latest = find_latest_metadata_file(&table_root)
            .expect("latest metadata lookup")
            .expect("metadata file expected");

        assert_eq!(Path::new(&latest), valid_path.as_path());
    }

    #[tokio::test]
    async fn reopen_reuses_latest_metadata_and_can_append_again() {
        if !has_iceberg_rest_test_env() {
            eprintln!(
                "skipping iceberg rest integration test (CDC_TEST_ICEBERG_REST_URI/CDC_TEST_ICEBERG_REST_WAREHOUSE not set)"
            );
            return;
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let table_root = dir.path().join("tbl");

        let mut sink_a = IcebergSink::open(&append_cfg(table_root.clone()))
            .await
            .expect("open sink a");
        send_json(&mut sink_a, event_json(1, "a")).await;
        sink_a.flush().await.expect("flush a");
        drop(sink_a);

        let mut sink_b = IcebergSink::open(&append_cfg(table_root.clone()))
            .await
            .expect("open sink b");
        send_json(&mut sink_b, event_json(2, "b")).await;
        sink_b.flush().await.expect("flush b");

        assert!(has_file_suffix(&table_root, ".metadata.json"));
        assert!(has_file_suffix(&table_root, ".parquet"));
    }

    #[tokio::test]
    async fn concurrent_flushes_succeed_with_conflict_retries() {
        if !has_iceberg_rest_test_env() {
            eprintln!(
                "skipping iceberg rest integration test (CDC_TEST_ICEBERG_REST_URI/CDC_TEST_ICEBERG_REST_WAREHOUSE not set)"
            );
            return;
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let table_root = dir.path().join("tbl");
        run_concurrent_flushes(&table_root, 2, 6).await;

        assert!(count_files_with_suffix(&table_root, ".metadata.json") >= 3);
        assert!(count_files_with_suffix(&table_root, ".parquet") >= 2);
    }

    #[tokio::test]
    async fn high_contention_flushes_succeed_with_four_writers() {
        if !has_iceberg_rest_test_env() {
            eprintln!(
                "skipping iceberg rest integration test (CDC_TEST_ICEBERG_REST_URI/CDC_TEST_ICEBERG_REST_WAREHOUSE not set)"
            );
            return;
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let table_root = dir.path().join("tbl");

        run_concurrent_flushes(&table_root, 4, 10).await;

        assert!(count_files_with_suffix(&table_root, ".metadata.json") >= 5);
        assert!(count_files_with_suffix(&table_root, ".parquet") >= 4);

        let metadata_names = file_names_with_suffix(&table_root, ".metadata.json");
        let parquet_names = file_names_with_suffix(&table_root, ".parquet");

        assert!(metadata_names.len() >= 5);
        assert!(parquet_names.len() >= 4);
        assert!(all_unique(&metadata_names));
        assert!(all_unique(&parquet_names));
    }

    #[test]
    fn commit_failure_class_maps_conflict_and_retryable_errors() {
        let conflict = Error::new(ErrorKind::CatalogCommitConflicts, "conflict");
        assert_eq!(commit_failure_class(&conflict), "catalog_commit_conflict");

        let retryable = Error::new(ErrorKind::Unexpected, "retryable").with_retryable(true);
        assert_eq!(commit_failure_class(&retryable), "retryable_commit_error");

        let terminal = Error::new(ErrorKind::Unexpected, "terminal");
        assert_eq!(commit_failure_class(&terminal), "commit_error");
    }

    #[test]
    fn should_retry_commit_only_for_conflict_or_retryable_errors() {
        let conflict = Error::new(ErrorKind::CatalogCommitConflicts, "conflict");
        assert!(should_retry_commit(&conflict));

        let retryable = Error::new(ErrorKind::Unexpected, "retryable").with_retryable(true);
        assert!(should_retry_commit(&retryable));

        let terminal = Error::new(ErrorKind::Unexpected, "terminal");
        assert!(!should_retry_commit(&terminal));
    }

    fn has_file_suffix(root: &Path, suffix: &str) -> bool {
        count_files_with_suffix(root, suffix) > 0
    }

    fn count_files_with_suffix(root: &Path, suffix: &str) -> usize {
        let mut stack = vec![root.to_path_buf()];
        let mut count = 0;
        while let Some(dir) = stack.pop() {
            let Ok(entries) = fs::read_dir(dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if let Some(name) = path.file_name().and_then(|v| v.to_str()) {
                    if name.ends_with(suffix) {
                        count += 1;
                    }
                }
            }
        }

        count
    }

    fn file_names_with_suffix(root: &Path, suffix: &str) -> Vec<String> {
        let mut stack = vec![root.to_path_buf()];
        let mut names = Vec::new();
        while let Some(dir) = stack.pop() {
            let Ok(entries) = fs::read_dir(dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if let Some(name) = path.file_name().and_then(|v| v.to_str()) {
                    if name.ends_with(suffix) {
                        names.push(name.to_string());
                    }
                }
            }
        }

        names
    }

    fn all_unique(names: &[String]) -> bool {
        let mut seen = std::collections::HashSet::new();
        names.iter().all(|name| seen.insert(name))
    }

    #[test]
    fn normalized_schema_mode_omits_raw_event_payload_column_values() {
        let event: Event = serde_json::from_str(&event_json(1, "normalized"))
            .expect("event payload should deserialize");

        let batch =
            events_to_record_batch(&[event], IcebergSchemaMode::Normalized).expect("record batch");

        let raw_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("raw payload column should be StringArray");
        assert!(raw_col.is_null(0));
    }

    #[test]
    fn normalized_with_raw_schema_mode_keeps_raw_event_payload_column_values() {
        let event: Event =
            serde_json::from_str(&event_json(2, "raw")).expect("event payload should deserialize");

        let batch = events_to_record_batch(&[event], IcebergSchemaMode::NormalizedWithRaw)
            .expect("record batch");

        let raw_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("raw payload column should be StringArray");
        assert!(!raw_col.is_null(0));
        let value = raw_col.value(0);
        assert!(value.contains("\"table\":\"users\""));
    }

    async fn run_concurrent_flushes(table_root: &Path, writer_count: usize, max_retries: u32) {
        let cfg = IcebergSinkConfig {
            table_path: table_root.to_path_buf(),
            catalog: IcebergCatalogConfig {
                rest: IcebergRestCatalogConfig {
                    uri: std::env::var("CDC_TEST_ICEBERG_REST_URI")
                        .unwrap_or_else(|_| "http://127.0.0.1:8181".to_string()),
                    warehouse: std::env::var("CDC_TEST_ICEBERG_REST_WAREHOUSE")
                        .unwrap_or_else(|_| "file:///tmp/cdc-iceberg-warehouse".to_string()),
                    token: std::env::var("CDC_TEST_ICEBERG_REST_TOKEN")
                        .ok()
                        .map(rustcdc::SecretString::new),
                    credential: std::env::var("CDC_TEST_ICEBERG_REST_CREDENTIAL")
                        .ok()
                        .map(rustcdc::SecretString::new),
                },
            },
            write_mode: IcebergWriteMode::Append,
            schema_mode: IcebergSchemaMode::Normalized,
            namespace: "cdc".to_string(),
            table_name: "events".to_string(),
            max_commit_retries: max_retries,
            retry_backoff_ms: 5,
            retry_backoff_max_ms: 40,
            parquet_compression: Default::default(),
            parquet_row_group_rows: 1_048_576,
            snapshot_expiry: Default::default(),
            max_pending_events: 100_000,
            max_pending_bytes: 256 * 1024 * 1024,
            storage: Default::default(),
        };

        let barrier = Arc::new(Barrier::new(writer_count));
        let mut flushes = Vec::with_capacity(writer_count);

        for index in 0..writer_count {
            let mut sink = IcebergSink::open(&cfg)
                .await
                .unwrap_or_else(|e| panic!("open sink {index} failed: {e}"));
            send_json(
                &mut sink,
                event_json(10 + index as i64, &format!("writer-{index}")),
            )
            .await;

            let barrier = Arc::clone(&barrier);
            flushes.push(tokio::spawn(async move {
                barrier.wait().await;
                sink.flush().await
            }));
        }

        for (index, flush) in flushes.into_iter().enumerate() {
            flush
                .await
                .unwrap_or_else(|e| panic!("join {index} failed: {e}"))
                .unwrap_or_else(|e| panic!("flush {index} failed: {e}"));
        }
    }
}
