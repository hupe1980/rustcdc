pub use super::pipeline::*;
pub use super::sink::*;
pub use super::source::*;
pub use super::state::*;

use super::registry::ConfluentRegistryConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

// ─────────────────────────────────────────────────────────────────────────────
// Top-level versioned config document
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AppConfig {
    /// Must be `"v1"`.  Legacy configs with `api_version = "v1alpha1"` are
    /// automatically accepted by the migration layer in `config/migrations.rs`.
    pub api_version: String,

    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub state: StateConfig,
    #[serde(default)]
    pub admin: AdminConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,

    /// Requested end-to-end delivery contract for runtime/sink pairing.
    #[serde(default)]
    pub delivery_contract: DeliveryContract,

    /// RuntimeConfig tuning knobs.
    #[serde(default)]
    pub runtime: RuntimeTuningConfig,

    /// Transform rules and runtime execution settings.
    #[serde(default)]
    pub pipeline: PipelineConfig,

    /// Named schema registry pool.
    #[serde(default)]
    pub registries: BTreeMap<String, ConfluentRegistryConfig>,

    /// Named sink bindings used by `[[pipeline.routes]]`.
    ///
    /// Each entry must have a unique `name` field.  Routes that reference an
    /// unknown name produce a configuration error at startup.
    #[serde(default)]
    pub sinks: Vec<NamedSinkConfig>,

    /// Tables to include in the initial snapshot, `"schema.table"` format.
    #[serde(default)]
    pub snapshot_tables: Vec<String>,
}

impl AppConfig {
    pub const SUPPORTED_API_VERSION: &'static str = "v1";
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryContract {
    #[default]
    AtLeastOnce,
    EffectivelyOnce,
    /// Best-effort delivery: checkpoint is advanced **before** sink delivery
    /// so that failed events are skipped on recovery rather than replayed.
    ///
    /// Use for audit-log archival and analytics workloads where duplicates are
    /// worse than occasional gaps.  Retries are still attempted within the
    /// configured retry budget, but after the checkpoint has advanced.
    ///
    /// **Warning**: data loss on delivery failure is expected and by design.
    AtMostOnce,
}

impl DeliveryContract {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::AtLeastOnce => "at_least_once",
            Self::EffectivelyOnce => "effectively_once",
            Self::AtMostOnce => "at_most_once",
        }
    }

    pub fn requires_idempotent_delivery(self) -> bool {
        matches!(self, Self::EffectivelyOnce)
    }

    /// Whether this contract requires the checkpoint to be committed **before**
    /// sink delivery (at-most-once semantics).
    pub fn advance_checkpoint_before_delivery(self) -> bool {
        matches!(self, Self::AtMostOnce)
    }

    pub fn requires_transactional_checkpoint_barrier(self) -> bool {
        matches!(self, Self::EffectivelyOnce)
    }

    pub fn is_satisfied_by(
        self,
        idempotent_delivery_capable: bool,
        transactional_checkpoint_barrier_capable: bool,
    ) -> bool {
        if self.requires_idempotent_delivery() && !idempotent_delivery_capable {
            return false;
        }

        if self.requires_transactional_checkpoint_barrier()
            && !transactional_checkpoint_barrier_capable
        {
            return false;
        }

        true
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Admin API
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AdminConfig {
    /// Whether the admin HTTP server is enabled (default: true).
    #[serde(default = "bool_true")]
    pub enabled: bool,

    /// Bind address (default: `"127.0.0.1:8080"`).
    #[serde(default = "default_admin_bind")]
    pub bind: String,

    /// Request timeout in milliseconds (default: 5 000).
    #[serde(default = "default_admin_timeout_ms")]
    pub timeout_ms: u64,

    /// Sustained per-client request rate allowed for `/metrics`.
    #[serde(default = "default_admin_metrics_rate_limit_rps")]
    pub metrics_rate_limit_rps: u32,

    /// Per-client burst capacity for `/metrics`.
    #[serde(default = "default_admin_metrics_rate_limit_burst")]
    pub metrics_rate_limit_burst: u32,

    /// Sustained per-client request rate allowed for `/readyz`.
    #[serde(default = "default_admin_readyz_rate_limit_rps")]
    pub readyz_rate_limit_rps: u32,

    /// Per-client burst capacity for `/readyz`.
    #[serde(default = "default_admin_readyz_rate_limit_burst")]
    pub readyz_rate_limit_burst: u32,

    /// Sustained per-client request rate allowed for `/status`.
    #[serde(default = "default_admin_status_rate_limit_rps")]
    pub status_rate_limit_rps: u32,

    /// Per-client burst capacity for `/status`.
    #[serde(default = "default_admin_status_rate_limit_burst")]
    pub status_rate_limit_burst: u32,

    /// Environment variable containing bearer token for read-only admin endpoints.
    #[serde(default)]
    pub read_token_env: Option<String>,

    /// Environment variable containing bearer token for write admin endpoints.
    #[serde(default)]
    pub write_token_env: Option<String>,

    /// Readiness probe auth policy.
    #[serde(default)]
    pub probe_auth_mode: AdminProbeAuthMode,

    /// Optional JSON token manifest with rotation/expiry/revocation metadata.
    #[serde(default)]
    pub token_manifest_file: Option<PathBuf>,

    /// Trusted Ed25519 public keys (hex-encoded, 32 bytes) accepted for token-manifest signatures.
    #[serde(default)]
    pub token_manifest_trusted_public_keys_hex: Vec<String>,

    /// Refresh interval in milliseconds for reloading `token_manifest_file` at runtime.
    #[serde(default = "default_admin_token_manifest_refresh_ms")]
    pub token_manifest_refresh_ms: u64,

    /// Optional maximum age in milliseconds of the last successful manifest reload
    /// before admin auth fails closed.
    #[serde(default)]
    pub token_manifest_max_staleness_ms: Option<u64>,

    /// Optional environment variable containing hex-encoded Ed25519 private key
    /// used to sign support-bundle audit trail exports.
    #[serde(default)]
    pub audit_signing_key_env: Option<String>,

    /// Pseudonymise source IP addresses in the audit trail.
    ///
    /// When `true` (the default), `actor_source_ip` is replaced with the first
    /// 8 bytes of `SHA-256(salt || ip)` encoded as 16 lowercase hex characters
    /// before the canonical entry string is built and signed.  Raw IPs never
    /// appear in the audit log file, the in-memory ring buffer, or support
    /// bundles.
    ///
    /// Set `false` only when operating under a legitimate legal basis to retain
    /// raw IP addresses and your data-retention policies cover audit log files.
    #[serde(default = "bool_true")]
    pub audit_ip_pseudonymise: bool,

    /// Environment variable containing a 32-byte (64 lowercase hex character)
    /// pseudonymisation salt.  When set the same salt is used across process
    /// restarts, enabling cross-session IP correlation for forensic analysis.
    /// When unset a random 16-byte salt is generated at startup — IPs from
    /// different process lifetimes cannot be correlated.
    #[serde(default)]
    pub audit_ip_salt_env: Option<String>,

    /// Optional append-only JSONL file receiving durable control-plane audit entries.
    #[serde(default)]
    pub audit_log_file: Option<PathBuf>,

    /// Optional append-only JSONL file receiving CloudEvents notification fan-out records.
    #[serde(default)]
    pub notification_log_file: Option<PathBuf>,

    /// Optional append-only JSONL file providing external control-plane signal ingress.
    #[serde(default)]
    pub signal_ingress_file: Option<PathBuf>,

    /// Optional Kafka topic providing external control-plane signal ingress.
    #[serde(default)]
    pub signal_ingress_kafka: Option<AdminSignalIngressKafkaConfig>,

    /// Optional Kafka topic receiving CloudEvents notification fan-out records.
    #[serde(default)]
    pub notification_kafka: Option<AdminNotificationKafkaConfig>,

    /// Optional TLS/mTLS server configuration for admin endpoints.
    #[serde(default)]
    pub tls: Option<AdminTlsConfig>,

    /// Proxy IPs trusted to provide client IP forwarding headers.
    #[serde(default)]
    pub trusted_proxy_ips: Vec<String>,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: default_admin_bind(),
            timeout_ms: default_admin_timeout_ms(),
            metrics_rate_limit_rps: default_admin_metrics_rate_limit_rps(),
            metrics_rate_limit_burst: default_admin_metrics_rate_limit_burst(),
            readyz_rate_limit_rps: default_admin_readyz_rate_limit_rps(),
            readyz_rate_limit_burst: default_admin_readyz_rate_limit_burst(),
            status_rate_limit_rps: default_admin_status_rate_limit_rps(),
            status_rate_limit_burst: default_admin_status_rate_limit_burst(),
            read_token_env: None,
            write_token_env: None,
            probe_auth_mode: AdminProbeAuthMode::RequireReadToken,
            token_manifest_file: None,
            token_manifest_trusted_public_keys_hex: Vec::new(),
            token_manifest_refresh_ms: default_admin_token_manifest_refresh_ms(),
            token_manifest_max_staleness_ms: None,
            audit_signing_key_env: None,
            audit_ip_pseudonymise: true,
            audit_ip_salt_env: None,
            audit_log_file: None,
            notification_log_file: None,
            signal_ingress_file: None,
            signal_ingress_kafka: None,
            notification_kafka: None,
            tls: None,
            trusted_proxy_ips: Vec::new(),
        }
    }
}

impl AdminConfig {
    pub fn endpoint_scheme(&self) -> &'static str {
        if self.tls.is_some() {
            "https"
        } else {
            "http"
        }
    }

    pub fn base_url(&self) -> String {
        format!("{}://{}", self.endpoint_scheme(), self.bind)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AdminTlsConfig {
    /// PEM-encoded server certificate chain file.
    pub cert_file: PathBuf,

    /// PEM-encoded server private key file (PKCS#8 or PKCS#1/RSA).
    pub key_file: PathBuf,

    /// Optional PEM-encoded client CA bundle used for mTLS verification.
    #[serde(default)]
    pub client_ca_file: Option<PathBuf>,

    /// Require a valid client certificate for admin requests.
    #[serde(default)]
    pub require_client_cert: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdminProbeAuthMode {
    #[default]
    RequireReadToken,
    AllowUnauthenticatedLoopback,
}

fn default_admin_bind() -> String {
    "127.0.0.1:8080".to_string()
}

fn default_admin_timeout_ms() -> u64 {
    5_000
}

fn default_admin_metrics_rate_limit_rps() -> u32 {
    20
}

fn default_admin_metrics_rate_limit_burst() -> u32 {
    40
}

fn default_admin_readyz_rate_limit_rps() -> u32 {
    20
}

fn default_admin_readyz_rate_limit_burst() -> u32 {
    40
}

fn default_admin_status_rate_limit_rps() -> u32 {
    20
}

fn default_admin_status_rate_limit_burst() -> u32 {
    40
}

fn default_admin_token_manifest_refresh_ms() -> u64 {
    5_000
}

// ─────────────────────────────────────────────────────────────────────────────
// Observability
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ObservabilityConfig {
    /// OTLP traces endpoint.  `None` disables trace export.
    pub otlp_endpoint: Option<String>,

    /// OTLP metrics endpoint.  Defaults to `otlp_endpoint` when set.
    /// Explicitly set to send metrics to a different collector than traces.
    /// `None` disables OTLP metrics export (Prometheus pull endpoint still active).
    pub otlp_metrics_endpoint: Option<String>,

    /// Export interval for OTLP metrics in seconds (default: 30).
    #[serde(default = "default_otlp_metrics_interval_secs")]
    pub otlp_metrics_interval_secs: u64,

    /// OTLP protocol for traces: `"grpc"` (default) or `"http"`.
    #[serde(default = "default_otel_protocol")]
    pub otlp_protocol: String,

    /// Service name reported to the trace/metrics backend.
    #[serde(default = "default_service_name")]
    pub service_name: String,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: None,
            otlp_metrics_endpoint: None,
            otlp_metrics_interval_secs: default_otlp_metrics_interval_secs(),
            otlp_protocol: default_otel_protocol(),
            service_name: default_service_name(),
        }
    }
}

fn default_otlp_metrics_interval_secs() -> u64 {
    30
}

fn default_otel_protocol() -> String {
    "grpc".to_string()
}

fn default_service_name() -> String {
    "rustcdc-server".to_string()
}

// ─────────────────────────────────────────────────────────────────────────────
// Runtime tuning
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RuntimeTuningConfig {
    #[serde(default = "default_max_buffer")]
    pub max_buffer_size: usize,

    #[serde(default = "default_max_poll_wait")]
    pub max_poll_wait_ms: u64,

    #[serde(default = "default_runtime_max_event_bytes")]
    pub max_event_bytes: usize,

    #[serde(default = "default_runtime_sink_flush_interval_events")]
    pub sink_flush_interval_events: usize,

    #[serde(default = "default_runtime_sink_delivery_queue_capacity")]
    pub sink_delivery_queue_capacity: usize,

    #[serde(default = "default_runtime_prepare_parallelism")]
    pub prepare_parallelism: usize,

    #[serde(default = "default_runtime_sink_send_timeout_ms")]
    pub sink_send_timeout_ms: u64,

    #[serde(default = "default_runtime_sink_flush_timeout_ms")]
    pub sink_flush_timeout_ms: u64,

    #[serde(default)]
    pub transform_error_policy: RuntimeTransformErrorPolicy,

    #[serde(default)]
    pub post_commit_source_confirm_policy: RuntimePostCommitSourceConfirmPolicy,

    #[serde(default)]
    pub source_connection_retry: RuntimeConnectionRetryConfig,

    #[serde(default = "default_runtime_recoverable_error_backoff_initial_ms")]
    pub recoverable_error_backoff_initial_ms: u64,

    #[serde(default = "default_runtime_recoverable_error_backoff_max_ms")]
    pub recoverable_error_backoff_max_ms: u64,

    #[serde(default = "default_runtime_recoverable_error_backoff_multiplier")]
    pub recoverable_error_backoff_multiplier: f64,

    #[serde(default = "default_runtime_recoverable_error_backoff_jitter_ratio")]
    pub recoverable_error_backoff_jitter_ratio: f64,

    #[serde(default = "default_runtime_recoverable_error_breaker_consecutive_threshold")]
    pub recoverable_error_breaker_consecutive_threshold: u32,

    #[serde(default = "default_runtime_recoverable_error_breaker_cooldown_ms")]
    pub recoverable_error_breaker_cooldown_ms: u64,

    #[serde(default = "default_runtime_recoverable_error_breaker_max_open_cycles")]
    pub recoverable_error_breaker_max_open_cycles: u32,

    #[serde(default = "default_correctness_dedup_window_size")]
    pub correctness_dedup_window_size: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeTransformErrorPolicy {
    #[default]
    Halt,
    Skip,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePostCommitSourceConfirmPolicy {
    Continue,
    #[default]
    FailFast,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RuntimeConnectionRetryConfig {
    #[serde(default = "bool_true")]
    pub enabled: bool,

    #[serde(default = "default_runtime_source_connection_retry_max_retries")]
    pub max_retries: Option<u32>,

    #[serde(default = "default_runtime_source_connection_retry_initial_delay_ms")]
    pub initial_delay_ms: u64,

    #[serde(default = "default_runtime_source_connection_retry_max_delay_ms")]
    pub max_delay_ms: u64,
}

impl Default for RuntimeConnectionRetryConfig {
    fn default() -> Self {
        Self {
            enabled: bool_true(),
            max_retries: default_runtime_source_connection_retry_max_retries(),
            initial_delay_ms: default_runtime_source_connection_retry_initial_delay_ms(),
            max_delay_ms: default_runtime_source_connection_retry_max_delay_ms(),
        }
    }
}

impl Default for RuntimeTuningConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: default_max_buffer(),
            max_poll_wait_ms: default_max_poll_wait(),
            max_event_bytes: default_runtime_max_event_bytes(),
            sink_flush_interval_events: default_runtime_sink_flush_interval_events(),
            sink_delivery_queue_capacity: default_runtime_sink_delivery_queue_capacity(),
            prepare_parallelism: default_runtime_prepare_parallelism(),
            sink_send_timeout_ms: default_runtime_sink_send_timeout_ms(),
            sink_flush_timeout_ms: default_runtime_sink_flush_timeout_ms(),
            transform_error_policy: RuntimeTransformErrorPolicy::default(),
            post_commit_source_confirm_policy: RuntimePostCommitSourceConfirmPolicy::default(),
            source_connection_retry: RuntimeConnectionRetryConfig::default(),
            recoverable_error_backoff_initial_ms:
                default_runtime_recoverable_error_backoff_initial_ms(),
            recoverable_error_backoff_max_ms: default_runtime_recoverable_error_backoff_max_ms(),
            recoverable_error_backoff_multiplier:
                default_runtime_recoverable_error_backoff_multiplier(),
            recoverable_error_backoff_jitter_ratio:
                default_runtime_recoverable_error_backoff_jitter_ratio(),
            recoverable_error_breaker_consecutive_threshold:
                default_runtime_recoverable_error_breaker_consecutive_threshold(),
            recoverable_error_breaker_cooldown_ms:
                default_runtime_recoverable_error_breaker_cooldown_ms(),
            recoverable_error_breaker_max_open_cycles:
                default_runtime_recoverable_error_breaker_max_open_cycles(),
            correctness_dedup_window_size: default_correctness_dedup_window_size(),
        }
    }
}

impl RuntimeTuningConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_buffer_size == 0 {
            return Err("runtime.max_buffer_size must be > 0".to_string());
        }

        if self.max_poll_wait_ms == 0 {
            return Err("runtime.max_poll_wait_ms must be > 0".to_string());
        }

        if self.max_event_bytes == 0 {
            return Err("runtime.max_event_bytes must be > 0".to_string());
        }

        if self.sink_flush_interval_events == 0 {
            return Err("runtime.sink_flush_interval_events must be > 0".to_string());
        }

        if self.sink_flush_interval_events > self.max_buffer_size {
            return Err(
                "runtime.sink_flush_interval_events must be <= runtime.max_buffer_size".to_string(),
            );
        }

        if self.sink_delivery_queue_capacity == 0 {
            return Err("runtime.sink_delivery_queue_capacity must be > 0".to_string());
        }

        if self.sink_delivery_queue_capacity > self.max_buffer_size {
            return Err(
                "runtime.sink_delivery_queue_capacity must be <= runtime.max_buffer_size"
                    .to_string(),
            );
        }

        if self.prepare_parallelism == 0 {
            return Err("runtime.prepare_parallelism must be > 0".to_string());
        }

        if self.prepare_parallelism > self.max_buffer_size {
            return Err(
                "runtime.prepare_parallelism must be <= runtime.max_buffer_size".to_string(),
            );
        }

        if self.sink_send_timeout_ms == 0 {
            return Err("runtime.sink_send_timeout_ms must be > 0".to_string());
        }

        if self.sink_flush_timeout_ms == 0 {
            return Err("runtime.sink_flush_timeout_ms must be > 0".to_string());
        }

        if self.sink_send_timeout_ms > self.sink_flush_timeout_ms {
            return Err(
                "runtime.sink_send_timeout_ms must be <= runtime.sink_flush_timeout_ms".to_string(),
            );
        }

        if self.recoverable_error_backoff_initial_ms == 0 {
            return Err("runtime.recoverable_error_backoff_initial_ms must be > 0".to_string());
        }

        if self.recoverable_error_backoff_max_ms == 0 {
            return Err("runtime.recoverable_error_backoff_max_ms must be > 0".to_string());
        }

        if self.recoverable_error_backoff_initial_ms > self.recoverable_error_backoff_max_ms {
            return Err(
                "runtime.recoverable_error_backoff_initial_ms must be <= runtime.recoverable_error_backoff_max_ms".to_string(),
            );
        }

        if self.recoverable_error_backoff_multiplier < 1.0 {
            return Err("runtime.recoverable_error_backoff_multiplier must be >= 1.0".to_string());
        }

        if !self.recoverable_error_backoff_multiplier.is_finite() {
            return Err("runtime.recoverable_error_backoff_multiplier must be finite".to_string());
        }

        if !(0.0..=1.0).contains(&self.recoverable_error_backoff_jitter_ratio) {
            return Err(
                "runtime.recoverable_error_backoff_jitter_ratio must be between 0.0 and 1.0"
                    .to_string(),
            );
        }

        if self.recoverable_error_breaker_consecutive_threshold == 0 {
            return Err(
                "runtime.recoverable_error_breaker_consecutive_threshold must be > 0".to_string(),
            );
        }

        if self.recoverable_error_breaker_cooldown_ms == 0 {
            return Err("runtime.recoverable_error_breaker_cooldown_ms must be > 0".to_string());
        }

        if self.recoverable_error_breaker_max_open_cycles == 0 {
            return Err(
                "runtime.recoverable_error_breaker_max_open_cycles must be > 0".to_string(),
            );
        }

        if self.correctness_dedup_window_size == 0 {
            return Err("runtime.correctness_dedup_window_size must be > 0".to_string());
        }

        if self.source_connection_retry.enabled {
            if self.source_connection_retry.initial_delay_ms == 0 {
                return Err(
                    "runtime.source_connection_retry.initial_delay_ms must be > 0".to_string(),
                );
            }

            if self.source_connection_retry.max_delay_ms == 0 {
                return Err("runtime.source_connection_retry.max_delay_ms must be > 0".to_string());
            }

            if self.source_connection_retry.initial_delay_ms
                > self.source_connection_retry.max_delay_ms
            {
                return Err(
                    "runtime.source_connection_retry.initial_delay_ms must be <= runtime.source_connection_retry.max_delay_ms".to_string(),
                );
            }
        }

        Ok(())
    }
}

fn default_max_buffer() -> usize {
    1_000
}

fn default_max_poll_wait() -> u64 {
    100
}

fn default_runtime_max_event_bytes() -> usize {
    1024 * 1024
}

fn default_runtime_sink_flush_interval_events() -> usize {
    100
}

fn default_runtime_sink_delivery_queue_capacity() -> usize {
    128
}

fn default_runtime_prepare_parallelism() -> usize {
    8
}

fn default_runtime_sink_send_timeout_ms() -> u64 {
    15_000
}

fn default_runtime_sink_flush_timeout_ms() -> u64 {
    60_000
}

fn default_runtime_source_connection_retry_max_retries() -> Option<u32> {
    Some(5)
}

fn default_runtime_source_connection_retry_initial_delay_ms() -> u64 {
    300
}

fn default_runtime_source_connection_retry_max_delay_ms() -> u64 {
    10_000
}

fn default_runtime_recoverable_error_backoff_initial_ms() -> u64 {
    100
}

fn default_runtime_recoverable_error_backoff_max_ms() -> u64 {
    5_000
}

fn default_runtime_recoverable_error_backoff_multiplier() -> f64 {
    2.0
}

fn default_runtime_recoverable_error_backoff_jitter_ratio() -> f64 {
    0.2
}

fn default_runtime_recoverable_error_breaker_consecutive_threshold() -> u32 {
    10
}

fn default_runtime_recoverable_error_breaker_cooldown_ms() -> u64 {
    30_000
}

fn default_runtime_recoverable_error_breaker_max_open_cycles() -> u32 {
    3
}

fn default_correctness_dedup_window_size() -> usize {
    50_000
}

fn bool_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::{
        AdminSignalIngressKafkaConfig, IcebergCatalogConfig, IcebergRestCatalogConfig,
        IcebergSchemaMode, IcebergSinkConfig, IcebergWriteMode, KafkaCompression,
        KafkaDeliveryMode, KafkaSecurityConfig, KafkaSinkConfig, KafkaStateDurabilityProfile,
        KafkaTopicStateConfig, PostgresStateConfig, RuntimeConnectionRetryConfig,
        RuntimeTuningConfig, TransformActionConfig, TransformRuleConfig, TransformRuntimeConfig,
        TransformRuntimeMode, WasmTransformConfig,
    };
    use rustcdc::SecretString;

    fn sample_kafka_config(brokers: &str) -> KafkaSinkConfig {
        KafkaSinkConfig {
            brokers: brokers.to_string(),
            topic: "cdc-events".to_string(),
            client_id: "cdc-test".to_string(),
            ack_timeout_ms: 1_000,
            retry_backoff_ms: 100,
            retry_max_attempts: 3,
            compression: KafkaCompression::None,
            delivery_mode: KafkaDeliveryMode::AtLeastOnceIdempotent,
            transactional_id: None,
            transaction_timeout_ms: 60_000,
            security: KafkaSecurityConfig::default(),
            codec: None,
        }
    }

    #[test]
    fn normalized_brokers_trims_and_discards_empty_entries() {
        let cfg = sample_kafka_config(" kafka-1:9092, ,kafka-2:9092 ,,kafka-3:9092 ");
        assert_eq!(
            cfg.normalized_brokers(),
            vec!["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]
        );
    }

    #[test]
    fn validate_rejects_when_all_brokers_are_blank() {
        let cfg = sample_kafka_config(" , , ");
        let err = cfg.validate().expect_err("expected invalid broker list");
        assert!(err.contains("sink.kafka.brokers"));
    }

    #[test]
    fn kafka_sink_validate_rejects_zero_retry_and_timeout_settings() {
        let mut cfg = sample_kafka_config("kafka-1:9092");

        cfg.client_id = "   ".to_string();
        let err = cfg.validate().expect_err("expected invalid client id");
        assert!(err.contains("sink.kafka.client_id"));

        cfg.client_id = "cdc-test".to_string();
        cfg.ack_timeout_ms = 0;
        let err = cfg.validate().expect_err("expected invalid ack timeout");
        assert!(err.contains("sink.kafka.ack_timeout_ms"));

        cfg.ack_timeout_ms = 1_000;
        cfg.retry_backoff_ms = 0;
        let err = cfg.validate().expect_err("expected invalid retry backoff");
        assert!(err.contains("sink.kafka.retry_backoff_ms"));

        cfg.retry_backoff_ms = 100;
        cfg.retry_max_attempts = 0;
        let err = cfg
            .validate()
            .expect_err("expected invalid retry attempt count");
        assert!(err.contains("sink.kafka.retry_max_attempts"));
    }

    #[test]
    fn kafka_sink_validate_requires_transactional_id_for_transactional_mode() {
        let mut cfg = sample_kafka_config("kafka-1:9092");
        cfg.delivery_mode = KafkaDeliveryMode::Transactional;

        let err = cfg
            .validate()
            .expect_err("expected missing transactional_id to fail validation");
        assert!(err.contains("transactional_id is required"));

        cfg.transactional_id = Some("   ".to_string());
        let err = cfg
            .validate()
            .expect_err("expected empty transactional_id to fail validation");
        assert!(err.contains("transactional_id must not be empty"));

        cfg.transactional_id = Some("cdc-eos-1".to_string());
        cfg.validate()
            .expect("transactional mode should validate with transactional_id");
    }

    #[test]
    fn kafka_sink_validate_rejects_transactional_id_without_transactional_mode() {
        let mut cfg = sample_kafka_config("kafka-1:9092");
        cfg.transactional_id = Some("cdc-eos-1".to_string());

        let err = cfg
            .validate()
            .expect_err("expected transactional_id to fail in non-transactional mode");
        assert!(err.contains("transactional_id is only valid"));
    }

    fn sample_kafka_topic_state_config() -> KafkaTopicStateConfig {
        KafkaTopicStateConfig {
            brokers: "kafka-1:9092, kafka-2:9092".to_string(),
            topic: "cdc-checkpoint-state".to_string(),
            client_id: "rustcdc-state".to_string(),
            request_timeout_ms: 3_000,
            readback_poll_timeout_ms: 250,
            min_replication_factor: 3,
            min_insync_replicas: 2,
            durability_profile: KafkaStateDurabilityProfile::Production,
            security: KafkaSecurityConfig::default(),
        }
    }

    fn sample_admin_signal_ingress_kafka_config(
        brokers: &str,
        topic: &str,
        group_id: &str,
    ) -> AdminSignalIngressKafkaConfig {
        AdminSignalIngressKafkaConfig {
            brokers: brokers.to_string(),
            topic: topic.to_string(),
            group_id: group_id.to_string(),
            client_id: "rustcdc-admin-ingress".to_string(),
            poll_timeout_ms: 500,
            security: KafkaSecurityConfig::default(),
        }
    }

    #[test]
    fn admin_signal_ingress_kafka_validate_rejects_empty_brokers() {
        let cfg = sample_admin_signal_ingress_kafka_config("   ", "signals", "group-a");
        let err = cfg
            .validate()
            .expect_err("expected invalid admin signal ingress kafka config");
        assert!(err.contains("admin.signal_ingress_kafka.brokers"));
    }

    #[test]
    fn admin_signal_ingress_kafka_validate_rejects_empty_group_id() {
        let cfg = sample_admin_signal_ingress_kafka_config("localhost:9092", "signals", "  ");
        let err = cfg
            .validate()
            .expect_err("expected invalid admin signal ingress group id");
        assert!(err.contains("admin.signal_ingress_kafka.group_id"));
    }

    #[test]
    fn admin_signal_ingress_kafka_validate_accepts_plaintext_defaults() {
        let cfg = sample_admin_signal_ingress_kafka_config(
            "localhost:9092,localhost:9093",
            "signals",
            "group-a",
        );
        cfg.validate()
            .expect("expected valid admin signal ingress kafka config");
    }

    #[test]
    fn kafka_topic_state_validate_rejects_invalid_thresholds() {
        let mut cfg = sample_kafka_topic_state_config();
        cfg.min_replication_factor = 1;
        cfg.min_insync_replicas = 2;
        let err = cfg
            .validate()
            .expect_err("expected invalid ISR/replication thresholds");
        assert!(err.contains("min_insync_replicas"));
    }

    #[test]
    fn kafka_topic_state_defaults_are_production_safe() {
        let cfg = toml::from_str::<KafkaTopicStateConfig>(
            r#"
brokers = "kafka-1:9092"
topic = "cdc-checkpoint-state"
"#,
        )
        .expect("parse kafka topic state config");

        assert_eq!(cfg.min_replication_factor, 3);
        assert_eq!(cfg.min_insync_replicas, 2);
    }

    #[test]
    fn kafka_topic_state_validate_rejects_blank_client_id() {
        let mut cfg = sample_kafka_topic_state_config();
        cfg.client_id = "   ".to_string();
        let err = cfg
            .validate()
            .expect_err("expected invalid kafka topic client id");
        assert!(err.contains("state.backend.kafka_topic.client_id"));
    }

    #[test]
    fn kafka_topic_state_normalizes_brokers() {
        let mut cfg = sample_kafka_topic_state_config();
        cfg.brokers = " kafka-1:9092, , kafka-2:9092,, ".to_string();
        assert_eq!(
            cfg.normalized_brokers(),
            vec!["kafka-1:9092", "kafka-2:9092"]
        );
    }

    #[test]
    fn transform_rule_validate_requires_actions() {
        let rule = TransformRuleConfig {
            name: "noop".to_string(),
            ..Default::default()
        };
        let err = rule.validate().expect_err("rule must require actions");
        assert!(err.contains("at least one action"));
    }

    #[test]
    fn transform_rule_validate_rejects_empty_fields() {
        let rule = TransformRuleConfig {
            name: "bad".to_string(),
            actions: vec![TransformActionConfig::Unwrap {
                field: "  ".to_string(),
            }],
            ..Default::default()
        };
        let err = rule.validate().expect_err("empty unwrap field must fail");
        assert!(err.contains("empty field"));
    }

    #[test]
    fn transform_runtime_native_mode_is_valid_by_default() {
        let cfg = TransformRuntimeConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.mode, TransformRuntimeMode::Native);
    }

    #[test]
    fn transform_runtime_wasm_mode_requires_module_path() {
        let cfg = TransformRuntimeConfig {
            mode: TransformRuntimeMode::Wasm,
            wasm: WasmTransformConfig::default(),
        };
        let err = cfg
            .validate()
            .expect_err("wasm mode without module path must fail");
        assert!(err.contains("module_path"));
    }

    #[test]
    fn transform_runtime_wasm_rejects_invalid_instance_pool_size() {
        let wasm = WasmTransformConfig {
            module_path: Some(std::env::current_exe().expect("current exe path")),
            instance_pool_size: 0,
            ..Default::default()
        };

        let cfg = TransformRuntimeConfig {
            mode: TransformRuntimeMode::Wasm,
            wasm,
        };

        let err = cfg
            .validate()
            .expect_err("instance_pool_size=0 must fail validation");
        assert!(err.contains("instance_pool_size"));
    }

    #[test]
    fn runtime_tuning_rejects_invalid_backoff_jitter_and_breaker_config() {
        let cfg = RuntimeTuningConfig {
            recoverable_error_backoff_jitter_ratio: 1.5,
            ..RuntimeTuningConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("expected invalid backoff jitter ratio");
        assert!(err.contains("recoverable_error_backoff_jitter_ratio"));

        let cfg = RuntimeTuningConfig {
            recoverable_error_breaker_consecutive_threshold: 0,
            ..RuntimeTuningConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("expected invalid breaker threshold");
        assert!(err.contains("recoverable_error_breaker_consecutive_threshold"));

        let cfg = RuntimeTuningConfig {
            recoverable_error_breaker_cooldown_ms: 0,
            ..RuntimeTuningConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("expected invalid breaker cooldown");
        assert!(err.contains("recoverable_error_breaker_cooldown_ms"));

        let cfg = RuntimeTuningConfig {
            recoverable_error_breaker_max_open_cycles: 0,
            ..RuntimeTuningConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("expected invalid breaker max open cycles");
        assert!(err.contains("recoverable_error_breaker_max_open_cycles"));

        let cfg = RuntimeTuningConfig {
            prepare_parallelism: 0,
            ..RuntimeTuningConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("expected invalid prepare parallelism");
        assert!(err.contains("runtime.prepare_parallelism"));

        let cfg = RuntimeTuningConfig {
            sink_delivery_queue_capacity: 0,
            ..RuntimeTuningConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("expected invalid sink delivery queue capacity");
        assert!(err.contains("runtime.sink_delivery_queue_capacity"));

        let cfg = RuntimeTuningConfig {
            max_buffer_size: 32,
            sink_flush_interval_events: 16,
            sink_delivery_queue_capacity: 64,
            ..RuntimeTuningConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("expected sink delivery queue capacity above max buffer");
        assert!(err.contains("runtime.sink_delivery_queue_capacity"));

        let cfg = RuntimeTuningConfig {
            sink_send_timeout_ms: 0,
            ..RuntimeTuningConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("expected invalid sink send timeout");
        assert!(err.contains("runtime.sink_send_timeout_ms"));

        let cfg = RuntimeTuningConfig {
            sink_flush_timeout_ms: 0,
            ..RuntimeTuningConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("expected invalid sink flush timeout");
        assert!(err.contains("runtime.sink_flush_timeout_ms"));

        let cfg = RuntimeTuningConfig {
            sink_send_timeout_ms: 2_000,
            sink_flush_timeout_ms: 1_000,
            ..RuntimeTuningConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("expected invalid sink timeout ordering");
        assert!(err.contains("runtime.sink_send_timeout_ms"));

        let cfg = RuntimeTuningConfig {
            source_connection_retry: RuntimeConnectionRetryConfig {
                enabled: true,
                initial_delay_ms: 0,
                ..RuntimeConnectionRetryConfig::default()
            },
            ..RuntimeTuningConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("expected invalid source connection retry initial delay");
        assert!(err.contains("runtime.source_connection_retry.initial_delay_ms"));

        let cfg = RuntimeTuningConfig {
            source_connection_retry: RuntimeConnectionRetryConfig {
                enabled: true,
                initial_delay_ms: 2_000,
                max_delay_ms: 1_000,
                ..RuntimeConnectionRetryConfig::default()
            },
            ..RuntimeTuningConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("expected invalid source connection retry delay ordering");
        assert!(err.contains("runtime.source_connection_retry.initial_delay_ms"));
    }

    #[test]
    fn postgres_state_validates_non_empty_url() {
        use rustcdc::SecretString;
        let cfg = PostgresStateConfig {
            url: SecretString::new(""),
            ..PostgresStateConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("expected empty URL to be rejected");
        assert!(err.contains("state.backend.postgres.url must not be empty"));
    }

    #[test]
    fn iceberg_sink_validate_accepts_append_mode() {
        let cfg = IcebergSinkConfig {
            table_path: std::path::PathBuf::from("/tmp/iceberg-table"),
            catalog: IcebergCatalogConfig {
                rest: IcebergRestCatalogConfig {
                    uri: "http://127.0.0.1:8181".to_string(),
                    warehouse: "file:///tmp/iceberg-warehouse".to_string(),
                    token: None,
                    credential: None,
                },
            },
            write_mode: IcebergWriteMode::Append,
            schema_mode: IcebergSchemaMode::Normalized,
            namespace: "cdc".to_string(),
            table_name: "events".to_string(),
            max_commit_retries: 5,
            retry_backoff_ms: 50,
            retry_backoff_max_ms: 1_000,
            max_pending_events: 100_000,
            max_pending_bytes: 256 * 1024 * 1024,
            storage: Default::default(),
        };

        cfg.validate()
            .expect("append mode should be accepted for iceberg sink");
    }

    /// Plaintext literals in the config *file* are rejected by the loader's
    /// raw-document check (`enforce_deferred_secret_literals`); at the struct
    /// level a token is the resolved form of an `{ env = … }` reference, so
    /// validate() only enforces non-emptiness.
    #[test]
    fn iceberg_sink_validate_rejects_empty_catalog_secret() {
        let cfg = IcebergSinkConfig {
            table_path: std::path::PathBuf::from("/tmp/iceberg-table"),
            catalog: IcebergCatalogConfig {
                rest: IcebergRestCatalogConfig {
                    uri: "http://127.0.0.1:8181".to_string(),
                    warehouse: "file:///tmp/iceberg-warehouse".to_string(),
                    token: Some(SecretString::new("   ")),
                    credential: None,
                },
            },
            write_mode: IcebergWriteMode::Append,
            schema_mode: IcebergSchemaMode::Normalized,
            namespace: "cdc".to_string(),
            table_name: "events".to_string(),
            max_commit_retries: 5,
            retry_backoff_ms: 50,
            retry_backoff_max_ms: 1_000,
            max_pending_events: 100_000,
            max_pending_bytes: 256 * 1024 * 1024,
            storage: Default::default(),
        };

        let err = cfg
            .validate()
            .expect_err("blank catalog secrets must be rejected");
        assert!(err.contains("must not be empty"));
    }

    #[test]
    fn iceberg_sink_write_mode_rejects_upsert_literal() {
        let err = serde_json::from_str::<IcebergWriteMode>("\"upsert\"")
            .expect_err("upsert should be rejected");
        assert!(err.to_string().contains("unknown variant"));
    }
}
