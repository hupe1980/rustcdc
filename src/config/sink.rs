use super::codec::CodecConfig;
use rustcdc::SecretString;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Duration;

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

    /// Parquet compression for the data files this sink writes.
    ///
    /// CDC payloads are JSON-shaped and repetitive, so the default (`zstd`) typically
    /// cuts stored bytes several-fold over uncompressed at negligible write cost.
    #[serde(default)]
    pub parquet_compression: IcebergParquetCompression,

    /// Rows per Parquet row group (default: 1 048 576).
    ///
    /// Row groups are the unit a reader can skip with statistics; smaller groups
    /// prune better on selective scans and cost more metadata.
    #[serde(default = "default_iceberg_row_group_rows")]
    pub parquet_row_group_rows: usize,

    /// Periodic snapshot expiry.
    #[serde(default)]
    pub snapshot_expiry: IcebergSnapshotExpiryConfig,
}

/// Parquet compression codec for Iceberg data files.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum IcebergParquetCompression {
    /// No compression. Fastest to write, largest on disk.
    Uncompressed,
    /// Fast, moderate ratio.
    Snappy,
    /// Best ratio at CDC-shaped data; the default.
    #[default]
    Zstd,
    /// Widely supported, slower than zstd at a similar ratio.
    Gzip,
    /// Fastest decompression, weakest ratio.
    Lz4,
}

impl IcebergParquetCompression {
    pub fn to_parquet(self) -> parquet::basic::Compression {
        use parquet::basic::{Compression, GzipLevel, ZstdLevel};
        match self {
            Self::Uncompressed => Compression::UNCOMPRESSED,
            Self::Snappy => Compression::SNAPPY,
            Self::Zstd => Compression::ZSTD(ZstdLevel::default()),
            Self::Gzip => Compression::GZIP(GzipLevel::default()),
            Self::Lz4 => Compression::LZ4_RAW,
        }
    }
}

/// Periodic Iceberg snapshot expiry.
///
/// A CDC sink commits on every flush, so the table accumulates one snapshot per
/// flush — metadata that is read in full on every planning pass and never shrinks on
/// its own. Expiry drops snapshots past the retention window and the manifests only
/// they referenced.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct IcebergSnapshotExpiryConfig {
    /// Run expiry after commits (default: `false`).
    #[serde(default)]
    pub enabled: bool,

    /// Expire snapshots older than this many milliseconds (default: 7 days).
    ///
    /// This is the time-travel window: a reader can no longer query a snapshot once
    /// it is expired, so size it for the longest query or rollback you must support.
    #[serde(default = "default_iceberg_expire_older_than_ms")]
    pub older_than_ms: u64,

    /// Always keep at least this many recent snapshots, whatever their age
    /// (default: 10).
    #[serde(default = "default_iceberg_retain_last")]
    pub retain_last: usize,

    /// Minimum interval between expiry runs, in milliseconds (default: 1 hour).
    ///
    /// Expiry rewrites table metadata, so running it on every flush would multiply
    /// commit traffic against the catalog for no benefit.
    #[serde(default = "default_iceberg_expiry_interval_ms")]
    pub interval_ms: u64,
}

impl Default for IcebergSnapshotExpiryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            older_than_ms: default_iceberg_expire_older_than_ms(),
            retain_last: default_iceberg_retain_last(),
            interval_ms: default_iceberg_expiry_interval_ms(),
        }
    }
}

fn default_iceberg_row_group_rows() -> usize {
    1_048_576
}

fn default_iceberg_expire_older_than_ms() -> u64 {
    7 * 24 * 60 * 60 * 1_000
}

fn default_iceberg_retain_last() -> usize {
    10
}

fn default_iceberg_expiry_interval_ms() -> u64 {
    60 * 60 * 1_000
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

        if self.parquet_row_group_rows == 0 {
            return Err("sink.iceberg.parquet_row_group_rows must be > 0".to_string());
        }

        if self.snapshot_expiry.enabled {
            if self.snapshot_expiry.retain_last == 0 {
                return Err(
                    "sink.iceberg.snapshot_expiry.retain_last must be > 0; expiring every \
                     snapshot would leave the table with no readable state"
                        .to_string(),
                );
            }
            if self.snapshot_expiry.interval_ms == 0 {
                return Err(
                    "sink.iceberg.snapshot_expiry.interval_ms must be > 0; expiry rewrites \
                     table metadata and running it per flush multiplies catalog traffic"
                        .to_string(),
                );
            }
            if self.snapshot_expiry.older_than_ms == 0 {
                return Err(
                    "sink.iceberg.snapshot_expiry.older_than_ms must be > 0; it is the \
                     time-travel window readers can still query"
                        .to_string(),
                );
            }
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

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum KafkaSecurityProtocol {
    /// No encryption, no authentication. Local development only.
    #[default]
    Plaintext,

    /// TLS transport, no SASL. Client certificates authenticate the client (mTLS)
    /// when `ssl_certificate_location` / `ssl_key_location` are set.
    #[serde(alias = "ssl")]
    Tls,

    /// SASL over a plaintext transport.
    ///
    /// The mechanism's own protection is all there is: SCRAM keeps the password off
    /// the wire, but PLAIN and OAUTHBEARER send the credential in the clear. Use
    /// `sasl_ssl` unless the network itself is already trusted.
    SaslPlaintext,

    /// SASL over TLS. The standard choice for Confluent Cloud, MSK and Redpanda Cloud.
    #[serde(alias = "sasl_tls")]
    SaslSsl,
}

impl KafkaSecurityProtocol {
    pub fn uses_tls(self) -> bool {
        matches!(self, Self::Tls | Self::SaslSsl)
    }

    pub fn uses_sasl(self) -> bool {
        matches!(self, Self::SaslPlaintext | Self::SaslSsl)
    }
}

/// SASL mechanism.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum KafkaSaslMechanism {
    /// Username + password sent as-is. Requires `sasl_ssl` in practice.
    #[default]
    Plain,
    /// Salted challenge-response; the password never crosses the wire.
    #[serde(alias = "scram-sha-256")]
    ScramSha256,
    /// SCRAM with SHA-512.
    #[serde(alias = "scram-sha-512")]
    ScramSha512,
    /// OAuth 2 bearer token — either a static `token` or an `[.oidc]` provider that
    /// fetches a fresh one per broker connection.
    #[serde(alias = "oauth_bearer")]
    OauthBearer,
    /// AWS MSK IAM, signed with SigV4.
    #[serde(alias = "aws_msk_iam")]
    AwsMskIam,
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

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct KafkaSecurityConfig {
    /// Kafka security protocol.
    #[serde(default)]
    pub protocol: KafkaSecurityProtocol,

    /// Optional CA bundle path for TLS verification.
    ///
    /// Omitted, the platform's native root store is used — which is what a managed
    /// broker with a publicly-issued certificate needs.
    #[serde(default)]
    pub ssl_ca_location: Option<PathBuf>,

    /// Client certificate chain (PEM) for mTLS.
    ///
    /// Set together with `ssl_key_location` to authenticate this client to the broker
    /// with a certificate rather than a password.
    #[serde(default)]
    pub ssl_certificate_location: Option<PathBuf>,

    /// Private key (PEM) matching `ssl_certificate_location`.
    #[serde(default)]
    pub ssl_key_location: Option<PathBuf>,

    /// Override the SNI hostname presented during the TLS handshake.
    ///
    /// Needed when brokers are reached through an address that does not match the
    /// name on their certificate — a load balancer, a port-forward, a VPC endpoint.
    #[serde(default)]
    pub sni_hostname: Option<String>,

    /// Whether to verify peer certificates.
    #[serde(default = "bool_true")]
    pub verify_peer: bool,

    /// SASL credentials. Required when `protocol` is `sasl_plaintext` or `sasl_ssl`.
    #[serde(default)]
    pub sasl: Option<Box<KafkaSaslConfig>>,
}

impl Default for KafkaSecurityConfig {
    /// Hand-written because a *derived* `Default` ignores serde field defaults.
    ///
    /// `verify_peer` is `#[serde(default = "bool_true")]`, which only applies when the
    /// field is missing from a `[sink.kafka.security]` table that exists. Omit the
    /// table entirely and serde falls back to `Default::default()` — where a derived
    /// impl produced `verify_peer: false`, silently disabling certificate verification
    /// for anyone who set `protocol` through an environment override.
    fn default() -> Self {
        Self {
            protocol: KafkaSecurityProtocol::default(),
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            sni_hostname: None,
            verify_peer: true,
            sasl: None,
        }
    }
}

/// SASL authentication parameters.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct KafkaSaslConfig {
    /// Mechanism to negotiate.
    #[serde(default)]
    pub mechanism: KafkaSaslMechanism,

    /// Username / API key. Required for `plain` and both SCRAM mechanisms.
    #[serde(default)]
    pub username: Option<String>,

    /// Password / API secret. Required for `plain` and both SCRAM mechanisms.
    #[serde(default)]
    pub password: Option<SecretString>,

    /// A pre-issued OAuth 2 bearer token, for `oauth_bearer` without an `[.oidc]` block.
    ///
    /// Static tokens expire; the broker rejects every connection made after that,
    /// including reconnects. Prefer `[.oidc]`, which fetches a fresh token per
    /// connection.
    #[serde(default)]
    pub token: Option<SecretString>,

    /// SASL extensions sent alongside an OAUTHBEARER token.
    ///
    /// Confluent Cloud uses `logicalCluster` and `identityPoolId` here.
    #[serde(default)]
    pub extensions: BTreeMap<String, String>,

    /// Fetch OAUTHBEARER tokens from an OIDC provider using the `client_credentials`
    /// grant (KIP-768).
    #[serde(default)]
    pub oidc: Option<Box<KafkaOidcConfig>>,

    /// AWS region for `aws_msk_iam`.
    ///
    /// Omitted, the region and credentials are read from the standard environment
    /// variables (`AWS_REGION` / `AWS_DEFAULT_REGION`, `AWS_ACCESS_KEY_ID`,
    /// `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`).
    #[serde(default)]
    pub region: Option<String>,
}

/// OIDC `client_credentials` token provider for SASL/OAUTHBEARER.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct KafkaOidcConfig {
    /// The provider's token endpoint, e.g. `https://idp.example.com/oauth2/v1/token`.
    pub token_endpoint: String,

    /// OAuth client id.
    pub client_id: String,

    /// OAuth client secret.
    pub client_secret: SecretString,

    /// Requested scope, if the provider requires one.
    #[serde(default)]
    pub scope: Option<String>,

    /// Extra form parameters sent with the token request (e.g. `audience`).
    #[serde(default)]
    pub form_parameters: BTreeMap<String, String>,

    /// Request timeout for the token endpoint, in milliseconds.
    #[serde(default = "default_kafka_oidc_timeout_ms")]
    pub request_timeout_ms: u64,
}

fn default_kafka_oidc_timeout_ms() -> u64 {
    10_000
}

/// Build AWS MSK IAM credentials from the standard environment variables.
///
/// Both paths read `AWS_SESSION_TOKEN` unconditionally, which is what makes an assumed
/// role, an EC2/ECS instance profile or an EKS web identity work — a temporary
/// credential without its session token is not a usable credential, and MSK rejects the
/// SigV4 signature made without it.
fn msk_credentials(region: Option<&str>) -> Result<krafka::auth::AwsMskIamCredentials, String> {
    match region {
        Some(region) => krafka::auth::AwsMskIamCredentials::from_env_with_region(region),
        None => krafka::auth::AwsMskIamCredentials::from_env(),
    }
    .map_err(|e| format!("sink.kafka.security.sasl (aws_msk_iam): {e}"))
}

/// Transport-level tuning for every Kafka connection this sink opens.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
pub struct KafkaTransportConfig {
    /// TCP keepalive probe interval, in milliseconds. `0` leaves the OS default.
    ///
    /// A broker that disappears without a FIN — a network partition, a killed VM —
    /// otherwise leaves the connection open until the OS notices, which on Linux is
    /// two hours by default.
    #[serde(default)]
    pub tcp_keepalive_ms: u64,

    /// Close a connection after this many milliseconds of inactivity. `0` disables
    /// idle eviction.
    #[serde(default)]
    pub connections_max_idle_ms: u64,

    /// Cap on simultaneously open broker connections. `0` means unlimited.
    ///
    /// Set this where the process runs under a tight file-descriptor limit and the
    /// cluster is large.
    #[serde(default)]
    pub max_connections: usize,

    /// Re-read the TLS certificate and key from disk every N milliseconds
    /// (KIP-1288). `0` disables reloading.
    ///
    /// Certificates issued by cert-manager or Vault rotate on their own schedule; a
    /// long-lived producer that read them once keeps presenting an expired client
    /// certificate until it is restarted.
    #[serde(default)]
    pub tls_reload_interval_ms: u64,

    /// SOCKS5 proxy to reach the brokers through, `host:port`.
    #[serde(default)]
    pub socks5_proxy: Option<String>,
}

impl KafkaTransportConfig {
    pub fn to_krafka(&self) -> Result<krafka::network::TransportConfig, String> {
        let mut builder = krafka::network::TransportConfig::builder();
        if self.tcp_keepalive_ms > 0 {
            builder = builder.tcp_keepalive(Some(Duration::from_millis(self.tcp_keepalive_ms)));
        }
        if self.connections_max_idle_ms > 0 {
            builder = builder
                .connections_max_idle(Some(Duration::from_millis(self.connections_max_idle_ms)));
        }
        if self.max_connections > 0 {
            builder = builder.max_connections(Some(self.max_connections));
        }
        if self.tls_reload_interval_ms > 0 {
            builder = builder
                .tls_reload_interval(Some(Duration::from_millis(self.tls_reload_interval_ms)));
        }
        builder
            .build()
            .map_err(|e| format!("sink.kafka.transport is invalid: {e}"))
    }

    pub fn validate(&self, protocol: KafkaSecurityProtocol) -> Result<(), String> {
        // Reloading a certificate the connection never presents is a setting that
        // reads as active and does nothing.
        if self.tls_reload_interval_ms > 0 && !protocol.uses_tls() {
            return Err(
                "sink.kafka.transport.tls_reload_interval_ms is set but the security \
                 protocol is not tls or sasl_ssl; there is no certificate to reload"
                    .to_string(),
            );
        }
        if let Some(proxy) = &self.socks5_proxy {
            if !proxy.contains(':') {
                return Err(format!(
                    "sink.kafka.transport.socks5_proxy '{proxy}' must be \"host:port\""
                ));
            }
        }
        Ok(())
    }
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

    /// Codec-specific compression level.
    ///
    /// Gzip accepts 0–9, Zstd 1–22. Snappy and LZ4 have no level, and setting one on
    /// them is rejected at startup rather than silently ignored. Omitted, each codec's
    /// own default is used.
    #[serde(default)]
    pub compression_level: Option<i32>,

    /// Bytes accumulated per partition batch before a send (default: 16 KiB).
    ///
    /// Larger batches compress better and cost fewer round-trips; they also raise the
    /// latency floor of the first event in each batch by up to `linger_ms`.
    #[serde(default = "default_kafka_batch_size")]
    pub batch_size: usize,

    /// How long a partially-filled batch waits for more events (default: `0`).
    ///
    /// This is standard Kafka linger: the trade of a bounded latency increase for larger,
    /// better-compressed batches and fewer round-trips. Up to `max_pipelined_sends`
    /// records are in flight at once, so a batch fills from concurrent sends and the
    /// linger is amortised across all of them.
    ///
    /// The sink used to await each record's acknowledgement before creating the
    /// next send, so a batch could never accumulate and every record paid the full linger
    /// alone — capping throughput at roughly `1000 / linger_ms` events per second. This
    /// field's documentation used to say so, and told operators to leave it at zero. That
    /// advice no longer applies.
    ///
    /// The default remains `0` because it is the lowest-latency setting and CDC consumers
    /// are usually latency-sensitive; raise it if you have measured that your workload
    /// prefers throughput.
    #[serde(default = "default_kafka_linger_ms")]
    pub linger_ms: u64,

    /// Records accepted for delivery before the sink waits for an acknowledgement
    /// (default: 128).
    ///
    /// This is the pipelining window. `1` restores the older behaviour of one broker
    /// round-trip per record. Records still reach the broker in submission order — see
    /// `KafkaSink::send_encoded` for why that survives pipelining — so per-partition
    /// ordering is unaffected.
    ///
    /// Two other settings cap the effective depth: `runtime.sink_flush_interval_events`,
    /// because a flush drains the window, and `runtime.sink_delivery_queue_capacity`,
    /// because it bounds how far the prepare stage may run ahead. Raising this alone past
    /// either has no effect.
    ///
    /// Memory cost is bounded by this many encoded payloads held for retry.
    #[serde(default = "default_kafka_max_pipelined_sends")]
    pub max_pipelined_sends: usize,

    /// Unacknowledged requests allowed per broker connection (default: 5).
    ///
    /// Kafka's idempotent producer preserves ordering only up to 5; a higher value is
    /// rejected here rather than reordering records under retry.
    #[serde(default = "default_kafka_max_in_flight")]
    pub max_in_flight: usize,

    /// Transport-level tuning, shared with the preflight admin client.
    #[serde(default)]
    pub transport: KafkaTransportConfig,

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

        if self.batch_size == 0 {
            return Err("sink.kafka.batch_size must be > 0".to_string());
        }

        if self.max_pipelined_sends == 0 {
            return Err(
                "sink.kafka.max_pipelined_sends must be > 0; use 1 for one broker \
                 round-trip per record"
                    .to_string(),
            );
        }

        if self.max_pipelined_sends > KAFKA_MAX_PIPELINED_SENDS_LIMIT {
            return Err(format!(
                "sink.kafka.max_pipelined_sends must be <= {KAFKA_MAX_PIPELINED_SENDS_LIMIT}; \
                 every outstanding send holds its encoded payload in memory, and a window \
                 this deep already exceeds any useful batch size"
            ));
        }

        if self.max_in_flight == 0 {
            return Err("sink.kafka.max_in_flight must be > 0".to_string());
        }

        if self.max_in_flight > KAFKA_MAX_IN_FLIGHT_LIMIT {
            return Err(format!(
                "sink.kafka.max_in_flight must be <= {KAFKA_MAX_IN_FLIGHT_LIMIT}; Kafka's \
                 idempotent producer preserves ordering only up to that many \
                 unacknowledged requests per connection, and beyond it a retried batch \
                 can land after one that was produced later"
            ));
        }

        // A level the codec has no concept of is not a preference the producer can
        // honour — silently dropping it makes the config a lie about what was sent.
        if let Some(level) = self.compression_level {
            match self.compression {
                KafkaCompression::Gzip if !(0..=9).contains(&level) => {
                    return Err(format!(
                        "sink.kafka.compression_level {level} is out of range for gzip (0–9)"
                    ))
                }
                KafkaCompression::Zstd if !(1..=22).contains(&level) => {
                    return Err(format!(
                        "sink.kafka.compression_level {level} is out of range for zstd (1–22)"
                    ))
                }
                KafkaCompression::None | KafkaCompression::Snappy | KafkaCompression::Lz4 => {
                    return Err(format!(
                        "sink.kafka.compression_level is set but compression = \
                         \"{}\" has no level",
                        match self.compression {
                            KafkaCompression::None => "none",
                            KafkaCompression::Snappy => "snappy",
                            _ => "lz4",
                        }
                    ))
                }
                _ => {}
            }
        }

        self.transport.validate(self.security.protocol)?;

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
    /// Build the TLS half of the configuration.
    fn to_tls_config(&self) -> Result<krafka::auth::TlsConfig, String> {
        let mut tls = krafka::auth::TlsConfig::new();
        match &self.ssl_ca_location {
            Some(ca_path) => tls = tls.with_ca_cert(ca_path.display().to_string()),
            // A managed broker presents a publicly-issued certificate, which the
            // bundled root store does not contain. Without this, every connection to
            // Confluent Cloud / MSK fails with an unknown-issuer error.
            None => tls = tls.with_native_roots(),
        }
        if let (Some(cert), Some(key)) = (&self.ssl_certificate_location, &self.ssl_key_location) {
            tls = tls.with_client_cert(cert.display().to_string(), key.display().to_string());
        }
        if let Some(hostname) = &self.sni_hostname {
            tls = tls.with_sni_hostname(hostname.clone());
        }
        Ok(tls.with_kafka_alpn())
    }

    pub fn to_auth_config(&self) -> Result<krafka::auth::AuthConfig, String> {
        use krafka::auth::AuthConfig;

        if !self.protocol.uses_sasl() {
            return Ok(match self.protocol {
                KafkaSecurityProtocol::Plaintext => AuthConfig::plaintext(),
                _ => AuthConfig::ssl(self.to_tls_config()?),
            });
        }

        let sasl = self.sasl.as_ref().ok_or_else(|| {
            "sink.kafka.security.sasl is required when protocol is sasl_plaintext or sasl_ssl"
                .to_string()
        })?;
        let tls = self
            .protocol
            .uses_tls()
            .then(|| self.to_tls_config())
            .transpose()?;

        let credential = |field: &str, value: Option<&SecretString>| -> Result<String, String> {
            value
                .ok_or_else(|| {
                    format!(
                        "sink.kafka.security.sasl.{field} is required for mechanism {:?}",
                        sasl.mechanism
                    )
                })?
                .resolve()
                .map_err(|e| format!("sink.kafka.security.sasl.{field}: {e}"))
        };

        // Build the mechanism first, then layer TLS on with one `with_tls`. krafka 0.16
        // made that composition structural, so a mechanism cannot end up reachable over
        // `sasl_plaintext` but not `sasl_ssl` — which is what previously forced this
        // sink to refuse SCRAM-over-TLS, the default listener on most managed brokers.
        let username = |mechanism: KafkaSaslMechanism| -> Result<String, String> {
            sasl.username.clone().ok_or_else(|| {
                format!("sink.kafka.security.sasl.username is required for mechanism {mechanism:?}")
            })
        };

        let auth = match sasl.mechanism {
            KafkaSaslMechanism::Plain => AuthConfig::sasl_plain(
                username(sasl.mechanism)?,
                credential("password", sasl.password.as_ref())?,
            )
            .map_err(|e| format!("sink.kafka.security.sasl: {e}"))?,
            KafkaSaslMechanism::ScramSha256 => AuthConfig::sasl_scram_sha256(
                username(sasl.mechanism)?,
                credential("password", sasl.password.as_ref())?,
            ),
            KafkaSaslMechanism::ScramSha512 => AuthConfig::sasl_scram_sha512(
                username(sasl.mechanism)?,
                credential("password", sasl.password.as_ref())?,
            ),
            KafkaSaslMechanism::OauthBearer => self.oauthbearer_auth(sasl)?,
            // MSK IAM's constructor already implies SASL_SSL; `with_tls` below keeps
            // the protocol and takes our CA / client-certificate / SNI settings.
            KafkaSaslMechanism::AwsMskIam => {
                AuthConfig::aws_msk_iam_with_credentials(msk_credentials(sasl.region.as_deref())?)
            }
        };

        Ok(match tls {
            Some(tls) => auth.with_tls(tls),
            None => auth,
        })
    }

    fn oauthbearer_auth(&self, sasl: &KafkaSaslConfig) -> Result<krafka::auth::AuthConfig, String> {
        use krafka::auth::{AuthConfig, OAuthBearerToken};

        if let Some(oidc) = &sasl.oidc {
            let mut builder = krafka::auth::oidc::OidcTokenProvider::builder(&oidc.token_endpoint)
                .credentials(krafka::auth::oidc::ClientCredentials::secret(
                    oidc.client_id.clone(),
                    oidc.client_secret
                        .resolve()
                        .map_err(|e| format!("sink.kafka.security.sasl.oidc.client_secret: {e}"))?,
                ))
                .request_timeout(std::time::Duration::from_millis(oidc.request_timeout_ms));
            if let Some(scope) = &oidc.scope {
                builder = builder.scope(scope.clone());
            }
            for (key, value) in &oidc.form_parameters {
                builder = builder.form_parameter(key.clone(), value.clone());
            }
            for (key, value) in &sasl.extensions {
                builder = builder.sasl_extension(key.clone(), value.clone());
            }
            let provider = builder
                .build()
                .map_err(|e| format!("sink.kafka.security.sasl.oidc: {e}"))?;
            // TLS is layered on by the caller via `with_tls`, uniformly for every
            // mechanism.
            return Ok(AuthConfig::sasl_oauthbearer_provider(provider));
        }

        let raw = sasl
            .token
            .as_ref()
            .ok_or_else(|| {
                "sink.kafka.security.sasl needs either `token` or an [.oidc] block for \
                 mechanism oauth_bearer"
                    .to_string()
            })?
            .resolve()
            .map_err(|e| format!("sink.kafka.security.sasl.token: {e}"))?;

        let mut token = OAuthBearerToken::new(raw);
        for (key, value) in &sasl.extensions {
            token = token.with_extension(key.clone(), value.clone());
        }
        Ok(AuthConfig::sasl_oauthbearer_token(token))
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.protocol.uses_tls() {
            if !self.verify_peer {
                return Err(format!(
                    "sink.kafka.security.verify_peer must be true when protocol = \"{}\"; \
                     an unverified TLS connection authenticates nothing and is \
                     indistinguishable from a man-in-the-middle",
                    match self.protocol {
                        KafkaSecurityProtocol::SaslSsl => "sasl_ssl",
                        _ => "tls",
                    }
                ));
            }

            for (field, path) in [
                ("ssl_ca_location", &self.ssl_ca_location),
                ("ssl_certificate_location", &self.ssl_certificate_location),
                ("ssl_key_location", &self.ssl_key_location),
            ] {
                if let Some(path) = path {
                    if !path.is_file() {
                        return Err(format!(
                            "sink.kafka.security.{field} does not point to a file: {}",
                            path.display()
                        ));
                    }
                }
            }

            // A certificate without its key (or the reverse) silently falls back to a
            // server-only handshake, so the broker sees an unauthenticated client and
            // rejects it with an error that names neither field.
            match (&self.ssl_certificate_location, &self.ssl_key_location) {
                (Some(_), None) => {
                    return Err("sink.kafka.security.ssl_key_location is required when \
                                ssl_certificate_location is set"
                        .to_string())
                }
                (None, Some(_)) => {
                    return Err(
                        "sink.kafka.security.ssl_certificate_location is required when \
                                ssl_key_location is set"
                            .to_string(),
                    )
                }
                _ => {}
            }
        } else if self.ssl_ca_location.is_some()
            || self.ssl_certificate_location.is_some()
            || self.ssl_key_location.is_some()
        {
            return Err(
                "sink.kafka.security has TLS material configured but protocol is not \
                 tls or sasl_ssl; the connection would be plaintext and the settings \
                 silently ignored"
                    .to_string(),
            );
        }

        if !self.protocol.uses_sasl() {
            if self.sasl.is_some() {
                return Err(
                    "sink.kafka.security.sasl is set but protocol is not sasl_plaintext or \
                     sasl_ssl; the credentials would never be sent"
                        .to_string(),
                );
            }
            return Ok(());
        }

        let sasl = self.sasl.as_ref().ok_or_else(|| {
            "sink.kafka.security.sasl is required when protocol is sasl_plaintext or sasl_ssl"
                .to_string()
        })?;

        match sasl.mechanism {
            KafkaSaslMechanism::Plain
            | KafkaSaslMechanism::ScramSha256
            | KafkaSaslMechanism::ScramSha512 => {
                if sasl.username.as_deref().unwrap_or("").trim().is_empty() {
                    return Err(format!(
                        "sink.kafka.security.sasl.username is required for mechanism {:?}",
                        sasl.mechanism
                    ));
                }
                if sasl.password.is_none() {
                    return Err(format!(
                        "sink.kafka.security.sasl.password is required for mechanism {:?}",
                        sasl.mechanism
                    ));
                }
                if sasl.mechanism == KafkaSaslMechanism::Plain
                    && self.protocol == KafkaSecurityProtocol::SaslPlaintext
                {
                    return Err(
                        "sink.kafka.security: mechanism \"plain\" sends the password in the \
                         clear, so it must not be paired with protocol = \"sasl_plaintext\". \
                         Use protocol = \"sasl_ssl\", or a SCRAM mechanism if the broker \
                         genuinely has no TLS listener."
                            .to_string(),
                    );
                }
            }
            KafkaSaslMechanism::OauthBearer => {
                match (&sasl.token, &sasl.oidc) {
                    (None, None) => {
                        return Err(
                            "sink.kafka.security.sasl needs either `token` or an [.oidc] block \
                             for mechanism oauth_bearer"
                                .to_string(),
                        )
                    }
                    (Some(_), Some(_)) => {
                        return Err(
                            "sink.kafka.security.sasl declares both a static `token` and an \
                             [.oidc] block; pick one"
                                .to_string(),
                        )
                    }
                    _ => {}
                }
                if let Some(oidc) = &sasl.oidc {
                    if oidc.token_endpoint.trim().is_empty() {
                        return Err(
                            "sink.kafka.security.sasl.oidc.token_endpoint must not be empty"
                                .to_string(),
                        );
                    }
                    if oidc.token_endpoint.starts_with("http://") {
                        return Err(format!(
                            "sink.kafka.security.sasl.oidc.token_endpoint '{}' uses plaintext \
                             HTTP; the client secret and the issued token would both cross \
                             the wire unencrypted",
                            oidc.token_endpoint
                        ));
                    }
                    if oidc.request_timeout_ms == 0 {
                        return Err(
                            "sink.kafka.security.sasl.oidc.request_timeout_ms must be > 0"
                                .to_string(),
                        );
                    }
                }
            }
            KafkaSaslMechanism::AwsMskIam => {
                if self.protocol != KafkaSecurityProtocol::SaslSsl {
                    return Err("sink.kafka.security: mechanism \"aws_msk_iam\" requires \
                         protocol = \"sasl_ssl\"; MSK IAM listeners are TLS-only"
                        .to_string());
                }
            }
        }

        Ok(())
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

fn default_kafka_batch_size() -> usize {
    16 * 1024
}

fn default_kafka_linger_ms() -> u64 {
    0
}

/// Deep enough to fill a 16 KiB batch from typical CDC payloads without waiting, and
/// bounded so a stalled broker cannot pin an unbounded number of encoded records in
/// memory. Matches `runtime.sink_delivery_queue_capacity`'s default, which is the other
/// half of the same window.
fn default_kafka_max_pipelined_sends() -> usize {
    128
}

/// A window this deep already exceeds any useful batch size, and every entry is a
/// retained encoded payload. Past this the setting buys nothing and costs memory.
pub(crate) const KAFKA_MAX_PIPELINED_SENDS_LIMIT: usize = 100_000;

/// Kafka's idempotent producer guarantees ordering only up to five unacknowledged
/// requests per connection.
pub(crate) const KAFKA_MAX_IN_FLIGHT_LIMIT: usize = 5;

fn default_kafka_max_in_flight() -> usize {
    KAFKA_MAX_IN_FLIGHT_LIMIT
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
