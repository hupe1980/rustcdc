use rustcdc::SecretString;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::sink::KafkaSecurityConfig;

// ─────────────────────────────────────────────────────────────────────────────
// State / durability
// ─────────────────────────────────────────────────────────────────────────────

fn default_state_dir() -> PathBuf {
    PathBuf::from("./state")
}

/// Per-store (offset) state configuration.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OffsetStoreConfig {
    /// Root directory for checkpoint files.
    #[serde(default = "default_state_dir")]
    pub dir: PathBuf,

    #[serde(default)]
    pub backend: StateBackend,
}

impl Default for OffsetStoreConfig {
    fn default() -> Self {
        Self {
            dir: default_state_dir(),
            backend: StateBackend::default(),
        }
    }
}

/// Per-store (schema history) state configuration.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SchemaHistoryStoreConfig {
    /// Root directory for schema-history files.
    #[serde(default = "default_state_dir")]
    pub dir: PathBuf,

    #[serde(default)]
    pub backend: StateBackend,
}

impl Default for SchemaHistoryStoreConfig {
    fn default() -> Self {
        Self {
            dir: default_state_dir(),
            backend: StateBackend::default(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct StateConfig {
    /// Offset / checkpoint store configuration.
    #[serde(default)]
    pub offset: OffsetStoreConfig,

    /// Schema-history store configuration.
    #[serde(default)]
    pub schema_history: SchemaHistoryStoreConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum StateBackend {
    /// Uses `rustcdc::checkpoint::FileCheckpoint` and
    /// `rustcdc::schema_history::FileSchemaHistory` directly.
    #[default]
    LocalFs,

    /// Kafka-topic backed state with checkpoint and schema-history storage in compacted topics.
    KafkaTopic(KafkaTopicStateConfig),

    /// OpenDAL-backed Redis state — checkpoint and schema history stored under two keys.
    Redis(RedisStateConfig),

    /// OpenDAL-backed PostgreSQL state — checkpoint and schema history stored in two tables.
    Postgresql(PostgresStateConfig),
}

/// Configuration for the Redis state backend (backed by OpenDAL `services-redis`).
///
/// Credentials are **never** stored inline as plain strings.  Use
/// `SecretString` inline values (`"redis://localhost"`) or env-var references
/// (`{ env = "REDIS_URL" }`) for all credential-bearing fields.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RedisStateConfig {
    /// Redis connection URL, e.g. `redis://localhost:6379` or
    /// `rediss://user:password@host:6380` for TLS.
    ///
    /// The URL may embed credentials; it is stored as a `SecretString` so it
    /// is redacted from logs and config-dump endpoints.
    pub url: SecretString,

    /// Optional username for ACL authentication.
    #[serde(default)]
    pub username: Option<String>,

    /// Optional password for AUTH / ACL authentication.  Stored as a
    /// `SecretString`; never appears in logs or serialised admin snapshots.
    #[serde(default)]
    pub password: Option<SecretString>,

    /// Redis database index (0–15).
    #[serde(default)]
    pub db: i64,

    /// Key prefix for all state records (default `cdc-state/`).
    #[serde(default = "default_redis_key_root")]
    pub key_root: String,
}

impl Default for RedisStateConfig {
    fn default() -> Self {
        Self {
            url: SecretString::new("redis://localhost:6379"),
            username: None,
            password: None,
            db: 0,
            key_root: default_redis_key_root(),
        }
    }
}

fn default_redis_key_root() -> String {
    "rustcdc-state/".to_string()
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PostgresStateConfig {
    /// PostgreSQL connection URL used for the durable state backend.
    ///
    /// The URL typically embeds credentials (`postgres://user:pass@host/db`);
    /// it is stored as a `SecretString` so it is redacted from logs and
    /// config-dump endpoints.  Use an env-var reference
    /// (`{ env = "POSTGRES_STATE_URL" }`) instead of an inline value.
    pub url: SecretString,

    /// Table storing the checkpoint artifact.
    #[serde(default = "default_checkpoint_table")]
    pub checkpoint_table: String,

    /// Table storing the schema-history artifact.
    #[serde(default = "default_schema_history_table")]
    pub schema_history_table: String,
}

impl Default for PostgresStateConfig {
    fn default() -> Self {
        Self {
            url: SecretString::new(""),
            checkpoint_table: default_checkpoint_table(),
            schema_history_table: default_schema_history_table(),
        }
    }
}

impl PostgresStateConfig {
    pub fn validate(&self) -> Result<(), String> {
        let resolved = self.url.resolve().unwrap_or_default();
        if resolved.trim().is_empty() {
            return Err("state.backend.postgres.url must not be empty".to_string());
        }
        Ok(())
    }
}

fn default_checkpoint_table() -> String {
    "rustcdc_state_checkpoint".to_string()
}

fn default_schema_history_table() -> String {
    "rustcdc_state_schema_history".to_string()
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum KafkaStateDurabilityProfile {
    #[default]
    Production,
    Development,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct KafkaTopicStateConfig {
    /// Comma-separated broker list.
    pub brokers: String,

    /// Kafka topic used to store compacted checkpoint records.
    pub topic: String,

    /// Client identifier for admin/producer/consumer operations.
    #[serde(default = "default_kafka_state_client_id")]
    pub client_id: String,

    /// Request timeout for Kafka admin and producer operations.
    #[serde(default = "default_kafka_state_request_timeout_ms")]
    pub request_timeout_ms: u64,

    /// Poll timeout used while scanning compacted topic state during recovery.
    #[serde(default = "default_kafka_state_readback_poll_timeout_ms")]
    pub readback_poll_timeout_ms: u64,

    /// Minimum partition replication factor required by startup durability checks.
    #[serde(default = "default_kafka_state_min_replication_factor")]
    pub min_replication_factor: u16,

    /// Minimum in-sync replicas required by startup durability checks.
    #[serde(default = "default_kafka_state_min_insync_replicas")]
    pub min_insync_replicas: u16,

    /// Durability policy profile for Kafka topic checkpoint state.
    #[serde(default = "default_kafka_state_durability_profile")]
    pub durability_profile: KafkaStateDurabilityProfile,

    /// Security profile for Kafka connections.
    #[serde(default)]
    pub security: KafkaSecurityConfig,
}

impl KafkaTopicStateConfig {
    pub fn normalized_brokers(&self) -> Vec<String> {
        self.brokers
            .split(',')
            .map(str::trim)
            .filter(|broker| !broker.is_empty())
            .map(ToString::to_string)
            .collect()
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.normalized_brokers().is_empty() {
            return Err(
                "state.backend.kafka_topic.brokers must contain at least one broker".to_string(),
            );
        }

        if self.topic.trim().is_empty() {
            return Err("state.backend.kafka_topic.topic must not be empty".to_string());
        }

        if self.client_id.trim().is_empty() {
            return Err("state.backend.kafka_topic.client_id must not be empty".to_string());
        }

        if self.request_timeout_ms == 0 {
            return Err("state.backend.kafka_topic.request_timeout_ms must be > 0".to_string());
        }

        if self.readback_poll_timeout_ms == 0 {
            return Err(
                "state.backend.kafka_topic.readback_poll_timeout_ms must be > 0".to_string(),
            );
        }

        if self.min_replication_factor == 0 {
            return Err("state.backend.kafka_topic.min_replication_factor must be > 0".to_string());
        }

        if self.min_insync_replicas == 0 {
            return Err("state.backend.kafka_topic.min_insync_replicas must be > 0".to_string());
        }

        if self.min_insync_replicas > self.min_replication_factor {
            return Err(
                "state.backend.kafka_topic.min_insync_replicas must be <= min_replication_factor"
                    .to_string(),
            );
        }

        let weak_replication =
            self.min_replication_factor < default_kafka_state_min_replication_factor();
        let weak_isr = self.min_insync_replicas < default_kafka_state_min_insync_replicas();

        if matches!(
            self.durability_profile,
            KafkaStateDurabilityProfile::Production
        ) && (weak_replication || weak_isr)
        {
            return Err(
                "state.backend.kafka_topic.durability_profile=production requires min_replication_factor>=3 and min_insync_replicas>=2; use durability_profile=development only for local/test overrides"
                    .to_string(),
            );
        }

        self.security.validate()
    }
}

fn default_kafka_state_client_id() -> String {
    "rustcdc-server-state".to_string()
}

fn default_kafka_state_request_timeout_ms() -> u64 {
    3_000
}

fn default_kafka_state_readback_poll_timeout_ms() -> u64 {
    250
}

pub(super) fn default_kafka_state_min_replication_factor() -> u16 {
    3
}

pub(super) fn default_kafka_state_min_insync_replicas() -> u16 {
    2
}

fn default_kafka_state_durability_profile() -> KafkaStateDurabilityProfile {
    KafkaStateDurabilityProfile::Production
}
