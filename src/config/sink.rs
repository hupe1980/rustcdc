use super::codec::CodecConfig;
use rustcdc::SecretString;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

// ─────────────────────────────────────────────────────────────────────────────
// Sink (transport)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkConfig {
    Stdout(StdoutSinkConfig),
    FileJsonl(FileJsonlSinkConfig),
    Http(HttpSinkConfig),
    Kafka(KafkaSinkConfig),
    Iceberg(IcebergSinkConfig),
    /// Deliver each event to all child sinks in sequence.
    ///
    /// The effective delivery guarantee is the weakest guarantee of any child
    /// sink.  All child sinks must flush successfully before the pipeline
    /// advances its checkpoint.
    Fan(FanSinkConfig),
}

/// A named sink configuration, used with `[[sinks]]` + `[[pipeline.routes]]`.
///
/// ```toml
/// [[sinks]]
/// name = "kafka_avro"
/// type = "kafka"
/// brokers = ["localhost:9092"]
/// topic_prefix = "app"
/// [sinks.codec]
/// type = "avro_confluent"
/// ```
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NamedSinkConfig {
    /// Unique name referenced by `[[pipeline.routes]]`.
    pub name: String,
    /// Sink transport + codec configuration (flattened into the same table).
    #[serde(flatten)]
    pub sink: SinkConfig,
}

/// Configuration for the fan-out meta-sink.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FanSinkConfig {
    /// Ordered list of child sink configurations.  At least two sinks are
    /// required; use a single-sink config directly otherwise.
    pub sinks: Vec<SinkConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct StdoutSinkConfig {}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FileJsonlSinkConfig {
    /// Destination file path.  The parent directory must exist.
    pub path: PathBuf,

    /// Rotate after this many bytes (0 = never rotate, default 100 MiB).
    #[serde(default = "default_rotate_size")]
    pub rotate_size_bytes: u64,

    /// fsync every N flushes (1 = every flush, default 1).
    #[serde(default = "default_fsync_every")]
    pub fsync_every: u32,
}

fn default_rotate_size() -> u64 {
    100 * 1024 * 1024
}

fn default_fsync_every() -> u32 {
    1
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HttpSinkConfig {
    /// Target endpoint URL.
    pub url: String,

    /// Per-request timeout in milliseconds.
    #[serde(default = "default_http_timeout_ms")]
    pub timeout_ms: u64,

    /// Maximum events per HTTP batch request payload.
    #[serde(default = "default_http_batch_max_events")]
    pub batch_max_events: usize,

    /// Maximum enqueue delay in milliseconds before a pending batch is flushed.
    #[serde(default = "default_http_batch_max_delay_ms")]
    pub batch_max_delay_ms: u64,

    /// Maximum aggregate bytes buffered in-memory before forcing flush/fail-close.
    #[serde(default = "default_http_max_pending_bytes")]
    pub max_pending_bytes: u64,

    /// Maximum retry attempts for retryable failures.
    #[serde(default = "default_http_max_retries")]
    pub max_retries: u32,

    /// Hard wall-clock retry budget per flushed batch in milliseconds.
    /// Once exceeded, remaining retries/salvage attempts fail closed to prevent retry storms.
    #[serde(default = "default_http_batch_retry_time_budget_ms")]
    pub batch_retry_time_budget_ms: u64,

    /// Initial retry backoff in milliseconds.
    #[serde(default = "default_http_backoff_initial_ms")]
    pub backoff_initial_ms: u64,

    /// Maximum retry backoff in milliseconds.
    #[serde(default = "default_http_backoff_max_ms")]
    pub backoff_max_ms: u64,

    /// Multiplier applied to each retry backoff step.
    #[serde(default = "default_http_backoff_multiplier")]
    pub backoff_multiplier: f64,

    /// Custom static headers to include in each request.
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Optional bearer token used for Authorization header.
    #[serde(default)]
    pub bearer_token: Option<SecretString>,

    /// Whether to verify peer TLS certificates.
    #[serde(default = "bool_true")]
    pub verify_tls: bool,

    /// Optional dead-letter JSONL file for terminal delivery failures.
    #[serde(default)]
    pub dlq_path: Option<PathBuf>,

    /// Maximum allowed DLQ file size in bytes before writes fail closed.
    #[serde(default = "default_http_dlq_max_bytes")]
    pub dlq_max_bytes: u64,

    /// Maximum number of idle connections kept alive per host in the reqwest
    /// connection pool.  Increase for high-throughput sinks; reduce for
    /// single-target low-traffic deployments.
    #[serde(default = "default_http_pool_max_idle_per_host")]
    pub pool_max_idle_per_host: usize,

    /// TCP keep-alive interval in seconds.  `null` disables keep-alive probes.
    /// Recommended: 30 s on cloud environments where NAT/LB tables expire idle
    /// connections in < 60 s.
    #[serde(default = "default_http_tcp_keepalive_secs")]
    pub tcp_keepalive_secs: Option<u64>,

    /// How long in seconds an idle pooled connection is kept before being
    /// reaped.  Should be less than server-side keep-alive timeout to avoid
    /// "connection reset on reuse" errors.
    #[serde(default = "default_http_pool_idle_timeout_secs")]
    pub pool_idle_timeout_secs: Option<u64>,

    /// Per-sink output serialisation codec.
    /// Defaults to JSON if omitted.
    #[serde(default)]
    pub codec: Option<CodecConfig>,
}

fn default_http_timeout_ms() -> u64 {
    5_000
}

fn default_http_batch_max_events() -> usize {
    256
}

fn default_http_batch_max_delay_ms() -> u64 {
    250
}

fn default_http_max_pending_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_http_max_retries() -> u32 {
    5
}

fn default_http_batch_retry_time_budget_ms() -> u64 {
    30_000
}

fn default_http_dlq_max_bytes() -> u64 {
    128 * 1024 * 1024
}

fn default_http_pool_max_idle_per_host() -> usize {
    8
}

fn default_http_tcp_keepalive_secs() -> Option<u64> {
    Some(30)
}

fn default_http_pool_idle_timeout_secs() -> Option<u64> {
    Some(90)
}

fn default_http_backoff_initial_ms() -> u64 {
    200
}

fn default_http_backoff_max_ms() -> u64 {
    5_000
}

fn default_http_backoff_multiplier() -> f64 {
    2.0
}

// ─────────────────────────────────────────────────────────────────────────────
// Iceberg sink
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum IcebergWriteMode {
    #[default]
    Append,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum IcebergSchemaMode {
    #[default]
    Normalized,
    NormalizedWithRaw,
}

/// Storage backend for the Iceberg table data files.
///
/// The backend is selected automatically from the `catalog.rest.warehouse` URI
/// scheme when `storage` is not specified in config:
/// - `file://` or bare path → `local_fs`
/// - `s3://` or `s3a://`    → `s3` (credentials from AWS standard chain)
/// - `gs://`                → `gcs` (credentials from Application Default Credentials)
/// - `az://` or `abfs://`   → `adls` (credentials from Azure standard chain)
///
/// Set `storage` explicitly to override the auto-detected backend type.
/// Credentials should be provided via environment variables following each
/// cloud provider's standard credential resolution chain:
///
/// S3  → `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`,
///        `AWS_ENDPOINT_URL` (for S3-compatible stores).
/// GCS → `GOOGLE_APPLICATION_CREDENTIALS` (path to service account JSON).
/// ADLS → `AZURE_STORAGE_ACCOUNT_KEY` or `AZURE_CLIENT_SECRET` + `AZURE_TENANT_ID`.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum IcebergStorageConfig {
    /// Local filesystem (default for `file://` warehouses).
    #[default]
    LocalFs,

    /// AWS S3 or S3-compatible object storage.
    /// Credentials resolved from AWS standard chain (env vars, shared
    /// credential file, IAM instance role, ECS task role, IMDSv2).
    S3,

    /// Google Cloud Storage.
    /// Credentials resolved from Application Default Credentials (ADC).
    Gcs,

    /// Azure Data Lake Storage Gen2 (ABFS / ADLS Gen2).
    Adls,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct IcebergCatalogConfig {
    pub rest: IcebergRestCatalogConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct IcebergRestCatalogConfig {
    /// Iceberg REST catalog base URI.
    pub uri: String,

    /// Catalog warehouse location (for example, `s3://warehouse` or `file:///var/lib/warehouse`).
    pub warehouse: String,

    /// Optional static bearer token for REST catalog authentication.
    #[serde(default)]
    pub token: Option<SecretString>,

    /// Optional OAuth credential (`<client_id>:<client_secret>`).
    #[serde(default)]
    pub credential: Option<SecretString>,
}

fn validate_optional_secret(secret: &Option<SecretString>, field: &str) -> Result<(), String> {
    let Some(secret) = secret else {
        return Ok(());
    };

    // Plaintext literals in the config file are rejected at load time on the raw
    // document (`enforce_deferred_secret_literals` in the loader) — by the time a
    // config reaches this validation, an inline value is the resolved form of an
    // `{ env = … }` reference or was constructed programmatically by an embedder.
    let resolved = secret
        .resolve()
        .map_err(|e| format!("{field} could not be resolved: {e}"))?;
    if resolved.trim().is_empty() {
        return Err(format!("{field} must not be empty when configured"));
    }

    Ok(())
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct IcebergSinkConfig {
    /// Root directory for the iceberg table (local filesystem path or URI).
    ///
    /// For cloud warehouses this field is still required but should point to
    /// a local path used only for the Iceberg catalog REST communication;
    /// actual data files are written to the `catalog.rest.warehouse` location.
    pub table_path: PathBuf,

    /// Iceberg catalog backend configuration.
    pub catalog: IcebergCatalogConfig,

    /// Storage backend for data files.
    ///
    /// When not configured the backend is inferred from the warehouse URI
    /// scheme: `file://` → `local_fs`, `s3://` → `s3`, `gs://` → `gcs`,
    /// `az://` / `abfs://` → `adls`.  Set this explicitly to supply
    /// non-default credentials or to override the automatic detection.
    #[serde(default)]
    pub storage: IcebergStorageConfig,

    /// Catalog namespace for the events table (default: `"cdc"`).
    #[serde(default = "default_iceberg_namespace")]
    pub namespace: String,

    /// Table name within the catalog namespace (default: `"events"`).
    #[serde(default = "default_iceberg_table_name")]
    pub table_name: String,

    /// Write mode: append.
    #[serde(default)]
    pub write_mode: IcebergWriteMode,

    /// Storage schema mode for persisted events.
    #[serde(default)]
    pub schema_mode: IcebergSchemaMode,

    /// Maximum number of events that may accumulate in the in-memory pending
    /// buffer before an early flush is triggered (default: 100 000).
    ///
    /// Without this cap the buffer can grow without bound during catalog
    /// outages or sustained high-throughput bursts, leading to OOM kills.
    #[serde(default = "default_iceberg_max_pending_events")]
    pub max_pending_events: usize,

    /// Maximum total byte size of the in-memory pending buffer before an early
    /// flush is triggered (default: 256 MiB).  Applied in addition to
    /// `max_pending_events`; whichever limit is hit first triggers the flush.
    #[serde(default = "default_iceberg_max_pending_bytes")]
    pub max_pending_bytes: usize,

    /// Maximum number of commit retries when conflicts are detected.
    #[serde(default = "default_iceberg_max_commit_retries")]
    pub max_commit_retries: u32,

    /// Initial backoff in milliseconds between retries.
    #[serde(default = "default_iceberg_retry_backoff_ms")]
    pub retry_backoff_ms: u64,

    /// Maximum retry backoff in milliseconds.
    #[serde(default = "default_iceberg_retry_backoff_max_ms")]
    pub retry_backoff_max_ms: u64,
}

impl IcebergSinkConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.table_path.as_os_str().is_empty() {
            return Err("sink.iceberg.table_path must not be empty".to_string());
        }

        if self.catalog.rest.uri.trim().is_empty() {
            return Err("sink.iceberg.catalog.rest.uri must not be empty".to_string());
        }

        if self.catalog.rest.warehouse.trim().is_empty() {
            return Err("sink.iceberg.catalog.rest.warehouse must not be empty".to_string());
        }

        if self.namespace.trim().is_empty() {
            return Err("sink.iceberg.namespace must not be empty".to_string());
        }

        if self.table_name.trim().is_empty() {
            return Err("sink.iceberg.table_name must not be empty".to_string());
        }

        validate_optional_secret(&self.catalog.rest.token, "sink.iceberg.catalog.rest.token")?;
        validate_optional_secret(
            &self.catalog.rest.credential,
            "sink.iceberg.catalog.rest.credential",
        )?;

        if self.max_commit_retries == 0 {
            return Err("sink.iceberg.max_commit_retries must be > 0".to_string());
        }

        if self.retry_backoff_ms == 0 {
            return Err("sink.iceberg.retry_backoff_ms must be > 0".to_string());
        }

        if self.retry_backoff_max_ms == 0 {
            return Err("sink.iceberg.retry_backoff_max_ms must be > 0".to_string());
        }

        if self.retry_backoff_ms > self.retry_backoff_max_ms {
            return Err(
                "sink.iceberg.retry_backoff_ms must be <= retry_backoff_max_ms".to_string(),
            );
        }

        if self.write_mode != IcebergWriteMode::Append {
            return Err("sink.iceberg.write_mode must be \"append\"".to_string());
        }

        if self.max_pending_events == 0 {
            return Err("sink.iceberg.max_pending_events must be > 0".to_string());
        }

        if self.max_pending_bytes == 0 {
            return Err("sink.iceberg.max_pending_bytes must be > 0".to_string());
        }

        Ok(())
    }
}

fn default_iceberg_namespace() -> String {
    "cdc".to_string()
}

fn default_iceberg_table_name() -> String {
    "events".to_string()
}

fn default_iceberg_max_pending_events() -> usize {
    100_000
}

fn default_iceberg_max_pending_bytes() -> usize {
    // 256 MiB
    256 * 1024 * 1024
}

fn default_iceberg_max_commit_retries() -> u32 {
    5
}

fn default_iceberg_retry_backoff_ms() -> u64 {
    50
}

fn default_iceberg_retry_backoff_max_ms() -> u64 {
    1_000
}

// ─────────────────────────────────────────────────────────────────────────────
// Kafka sink + shared Kafka types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum KafkaSecurityProtocol {
    #[default]
    Plaintext,
    Tls,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum KafkaCompression {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum KafkaDeliveryMode {
    #[default]
    AtLeastOnceIdempotent,
    Transactional,
}

impl KafkaCompression {
    pub fn to_krafka(self) -> Result<krafka::protocol::Compression, String> {
        let codec = match self {
            Self::None => krafka::protocol::Compression::None,
            Self::Gzip => krafka::protocol::Compression::Gzip,
            Self::Snappy => krafka::protocol::Compression::Snappy,
            Self::Lz4 => krafka::protocol::Compression::Lz4,
            Self::Zstd => krafka::protocol::Compression::Zstd,
        };

        if codec.is_available() {
            Ok(codec)
        } else {
            let feature = codec.required_feature().unwrap_or("compression");
            Err(format!(
                "krafka compression codec {self:?} is unavailable in this build; enable the `{feature}` feature"
            ))
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
pub struct KafkaSecurityConfig {
    /// Kafka security protocol.
    #[serde(default)]
    pub protocol: KafkaSecurityProtocol,

    /// Optional CA bundle path for TLS verification.
    #[serde(default)]
    pub ssl_ca_location: Option<PathBuf>,

    /// Whether to verify peer certificates.
    #[serde(default = "bool_true")]
    pub verify_peer: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct KafkaSinkConfig {
    /// Comma-separated broker list.
    pub brokers: String,

    /// Kafka topic to write CDC events to.
    pub topic: String,

    /// Client identifier for broker-side observability.
    #[serde(default = "default_kafka_client_id")]
    pub client_id: String,

    /// Ack timeout passed to the producer.
    #[serde(default = "default_kafka_ack_timeout_ms")]
    pub ack_timeout_ms: u64,

    /// Retry backoff used when Kafka rejects a send because the queue is full.
    #[serde(default = "default_kafka_retry_backoff_ms")]
    pub retry_backoff_ms: u64,

    /// Maximum retry attempts for producer-side retries.
    #[serde(default = "default_kafka_max_retries")]
    pub retry_max_attempts: u32,

    /// Message compression applied by the producer.
    #[serde(default)]
    pub compression: KafkaCompression,

    /// Kafka delivery mode contract.
    #[serde(default = "default_kafka_delivery_mode")]
    pub delivery_mode: KafkaDeliveryMode,

    /// Transactional producer identity required for transactional mode.
    #[serde(default)]
    pub transactional_id: Option<String>,

    /// Transaction timeout used by exactly-once mode.
    #[serde(default = "default_kafka_transaction_timeout_ms")]
    pub transaction_timeout_ms: u64,

    /// Security profile for the producer client.
    #[serde(default)]
    pub security: KafkaSecurityConfig,

    /// Per-sink output serialisation codec.
    ///
    /// Defaults to `json` when omitted.  Use `avro_confluent` for
    /// Confluent wire-format Avro encoding with a schema registry.
    #[serde(default)]
    pub codec: Option<CodecConfig>,
}

impl KafkaSinkConfig {
    pub fn normalized_brokers(&self) -> Vec<String> {
        self.brokers
            .split(',')
            .map(str::trim)
            .filter(|broker| !broker.is_empty())
            .map(ToString::to_string)
            .collect()
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.brokers.trim().is_empty() {
            return Err("sink.kafka.brokers must not be empty".to_string());
        }

        let brokers = self.normalized_brokers();

        if brokers.is_empty() {
            return Err("sink.kafka.brokers must contain at least one broker".to_string());
        }

        if self.topic.trim().is_empty() {
            return Err("sink.kafka.topic must not be empty".to_string());
        }

        if self.client_id.trim().is_empty() {
            return Err("sink.kafka.client_id must not be empty".to_string());
        }

        if self.ack_timeout_ms == 0 {
            return Err("sink.kafka.ack_timeout_ms must be > 0".to_string());
        }

        if self.retry_backoff_ms == 0 {
            return Err("sink.kafka.retry_backoff_ms must be > 0".to_string());
        }

        if self.retry_max_attempts == 0 {
            return Err("sink.kafka.retry_max_attempts must be > 0".to_string());
        }

        if self.transaction_timeout_ms == 0 {
            return Err("sink.kafka.transaction_timeout_ms must be > 0".to_string());
        }

        if self.transaction_timeout_ms > i32::MAX as u64 {
            return Err(format!(
                "sink.kafka.transaction_timeout_ms must be <= {}",
                i32::MAX
            ));
        }

        match self.delivery_mode {
            KafkaDeliveryMode::Transactional => {
                let transactional_id = self
                    .transactional_id
                    .as_ref()
                    .ok_or_else(|| {
                        "sink.kafka.transactional_id is required when sink.kafka.delivery_mode=\"transactional\""
                            .to_string()
                    })?
                    .trim();

                if transactional_id.is_empty() {
                    return Err(
                        "sink.kafka.transactional_id must not be empty when sink.kafka.delivery_mode=\"transactional\""
                            .to_string(),
                    );
                }
            }
            KafkaDeliveryMode::AtLeastOnceIdempotent => {
                if self.transactional_id.is_some() {
                    return Err(
                        "sink.kafka.transactional_id is only valid when sink.kafka.delivery_mode=\"transactional\""
                            .to_string(),
                    );
                }
            }
        }

        self.security.validate()?;

        Ok(())
    }
}

impl KafkaSecurityConfig {
    pub fn to_auth_config(&self) -> Result<krafka::auth::AuthConfig, String> {
        match self.protocol {
            KafkaSecurityProtocol::Plaintext => Ok(krafka::auth::AuthConfig::plaintext()),
            KafkaSecurityProtocol::Tls => {
                let tls = match &self.ssl_ca_location {
                    Some(ca_path) => krafka::auth::TlsConfig::new()
                        .with_ca_cert(ca_path.display().to_string())
                        .with_kafka_alpn(),
                    None => krafka::auth::TlsConfig::new().with_kafka_alpn(),
                };

                Ok(krafka::auth::AuthConfig::ssl(tls))
            }
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self.protocol {
            KafkaSecurityProtocol::Plaintext => Ok(()),
            KafkaSecurityProtocol::Tls => {
                if !self.verify_peer {
                    return Err(
                        "sink.kafka.security.verify_peer must be true when protocol = \"tls\""
                            .to_string(),
                    );
                }

                if let Some(path) = &self.ssl_ca_location {
                    if !path.is_file() {
                        return Err(format!(
                            "sink.kafka.security.ssl_ca_location does not point to a file: {}",
                            path.display()
                        ));
                    }
                }

                Ok(())
            }
        }
    }
}

fn default_kafka_client_id() -> String {
    "rustcdc-server".to_string()
}

pub(super) fn default_kafka_ack_timeout_ms() -> u64 {
    1_000
}

pub(super) fn default_kafka_retry_backoff_ms() -> u64 {
    100
}

pub(super) fn default_kafka_max_retries() -> u32 {
    5
}

fn default_kafka_delivery_mode() -> KafkaDeliveryMode {
    KafkaDeliveryMode::AtLeastOnceIdempotent
}

fn default_kafka_transaction_timeout_ms() -> u64 {
    60_000
}

// ─────────────────────────────────────────────────────────────────────────────
// Admin Kafka configs (Kafka-backed notification fan-out + signal ingress)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct AdminNotificationKafkaConfig {
    /// Comma-separated broker list.
    pub brokers: String,

    /// Kafka topic receiving notification CloudEvents.
    pub topic: String,

    /// Client identifier for broker-side observability.
    #[serde(default = "default_admin_notification_kafka_client_id")]
    pub client_id: String,

    /// Ack timeout passed to the producer.
    #[serde(default = "default_kafka_ack_timeout_ms")]
    pub ack_timeout_ms: u64,

    /// Retry backoff used when Kafka rejects a send because the queue is full.
    #[serde(default = "default_kafka_retry_backoff_ms")]
    pub retry_backoff_ms: u64,

    /// Maximum retry attempts for producer-side retries.
    #[serde(default = "default_kafka_max_retries")]
    pub retry_max_attempts: u32,

    /// Message compression applied by the producer.
    #[serde(default)]
    pub compression: KafkaCompression,

    /// Security profile for the producer client.
    #[serde(default)]
    pub security: KafkaSecurityConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct AdminSignalIngressKafkaConfig {
    /// Comma-separated broker list.
    pub brokers: String,

    /// Kafka topic receiving signal ingress payloads.
    pub topic: String,

    /// Consumer group used for signal ingress processing.
    pub group_id: String,

    /// Client identifier for broker-side observability.
    #[serde(default = "default_admin_signal_ingress_kafka_client_id")]
    pub client_id: String,

    /// Poll timeout for consumer fetch operations.
    #[serde(default = "default_admin_signal_ingress_kafka_poll_timeout_ms")]
    pub poll_timeout_ms: u64,

    /// Security profile for the consumer client.
    #[serde(default)]
    pub security: KafkaSecurityConfig,
}

impl AdminNotificationKafkaConfig {
    pub fn normalized_brokers(&self) -> Vec<String> {
        self.brokers
            .split(',')
            .map(str::trim)
            .filter(|broker| !broker.is_empty())
            .map(ToString::to_string)
            .collect()
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.brokers.trim().is_empty() {
            return Err("admin.notification_kafka.brokers must not be empty".to_string());
        }

        if self.normalized_brokers().is_empty() {
            return Err(
                "admin.notification_kafka.brokers must contain at least one broker".to_string(),
            );
        }

        if self.topic.trim().is_empty() {
            return Err("admin.notification_kafka.topic must not be empty".to_string());
        }

        if self.client_id.trim().is_empty() {
            return Err("admin.notification_kafka.client_id must not be empty".to_string());
        }

        if self.ack_timeout_ms == 0 {
            return Err("admin.notification_kafka.ack_timeout_ms must be > 0".to_string());
        }

        if self.retry_backoff_ms == 0 {
            return Err("admin.notification_kafka.retry_backoff_ms must be > 0".to_string());
        }

        if self.retry_max_attempts == 0 {
            return Err("admin.notification_kafka.retry_max_attempts must be > 0".to_string());
        }

        self.security.validate().map_err(|err| {
            err.replace("sink.kafka.security", "admin.notification_kafka.security")
        })?;

        Ok(())
    }
}

impl AdminSignalIngressKafkaConfig {
    pub fn normalized_brokers(&self) -> Vec<String> {
        self.brokers
            .split(',')
            .map(str::trim)
            .filter(|broker| !broker.is_empty())
            .map(ToString::to_string)
            .collect()
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.brokers.trim().is_empty() {
            return Err("admin.signal_ingress_kafka.brokers must not be empty".to_string());
        }

        if self.normalized_brokers().is_empty() {
            return Err(
                "admin.signal_ingress_kafka.brokers must contain at least one broker".to_string(),
            );
        }

        if self.topic.trim().is_empty() {
            return Err("admin.signal_ingress_kafka.topic must not be empty".to_string());
        }

        if self.group_id.trim().is_empty() {
            return Err("admin.signal_ingress_kafka.group_id must not be empty".to_string());
        }

        if self.client_id.trim().is_empty() {
            return Err("admin.signal_ingress_kafka.client_id must not be empty".to_string());
        }

        if self.poll_timeout_ms == 0 {
            return Err("admin.signal_ingress_kafka.poll_timeout_ms must be > 0".to_string());
        }

        self.security.validate().map_err(|err| {
            err.replace("sink.kafka.security", "admin.signal_ingress_kafka.security")
        })?;

        Ok(())
    }
}

fn default_admin_notification_kafka_client_id() -> String {
    "rustcdc-server-admin-notifications".to_string()
}

fn default_admin_signal_ingress_kafka_client_id() -> String {
    "rustcdc-server-admin-signal-ingress".to_string()
}

fn default_admin_signal_ingress_kafka_poll_timeout_ms() -> u64 {
    500
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

pub(super) fn bool_true() -> bool {
    true
}
