//! Dead-letter queue configuration.

use serde::{Deserialize, Serialize};

use super::schema::KafkaSecurityConfig;

/// Where undeliverable events are quarantined, if anywhere.
///
/// Absent by default. Quarantining an event means advancing the checkpoint past
/// something that was never delivered — data loss, recorded rather than silent, but
/// loss all the same. That is a trade an operator must choose; the default is to stop
/// and page someone.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DlqConfig {
    /// Turn quarantining on. With this `false` (the default) a non-recoverable
    /// delivery failure terminates the pipeline.
    #[serde(default)]
    pub enabled: bool,

    #[serde(flatten)]
    pub target: DlqTarget,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            target: DlqTarget::File(FileDlqConfig::default()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DlqTarget {
    File(FileDlqConfig),
    Kafka(Box<KafkaDlqConfig>),
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FileDlqConfig {
    /// Destination JSONL path. The parent directory is created if missing.
    ///
    /// On a container filesystem this is lost with the pod. Mount a volume, or prefer
    /// the `kafka` target — the dead-letter queue is what you read during an incident,
    /// which is exactly when the pod has been replaced.
    #[serde(default = "default_dlq_path")]
    pub path: String,

    /// Refuse further writes beyond this size rather than filling the volume.
    #[serde(default = "default_dlq_max_bytes")]
    pub max_bytes: u64,
}

impl Default for FileDlqConfig {
    fn default() -> Self {
        Self {
            path: default_dlq_path(),
            max_bytes: default_dlq_max_bytes(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct KafkaDlqConfig {
    pub brokers: String,
    pub topic: String,

    #[serde(default = "default_dlq_client_id")]
    pub client_id: String,

    #[serde(default = "default_dlq_request_timeout_ms")]
    pub request_timeout_ms: u64,

    #[serde(default)]
    pub security: KafkaSecurityConfig,
}

fn default_dlq_path() -> String {
    "/var/lib/rustcdc/dlq.jsonl".to_string()
}

fn default_dlq_max_bytes() -> u64 {
    128 * 1024 * 1024
}

fn default_dlq_client_id() -> String {
    "rustcdc-dlq".to_string()
}

fn default_dlq_request_timeout_ms() -> u64 {
    10_000
}

impl DlqConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }

        match &self.target {
            DlqTarget::File(file) => {
                if file.path.trim().is_empty() {
                    return Err("dlq.path must not be empty when dlq.enabled = true".to_string());
                }
                if file.max_bytes == 0 {
                    return Err(
                        "dlq.max_bytes must be greater than zero; 0 would refuse every \
                         write and turn the first undeliverable event into an outage"
                            .to_string(),
                    );
                }
            }
            DlqTarget::Kafka(kafka) => {
                if kafka.brokers.trim().is_empty() {
                    return Err("dlq.brokers must not be empty for the kafka target".to_string());
                }
                if kafka.topic.trim().is_empty() {
                    return Err("dlq.topic must not be empty for the kafka target".to_string());
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quarantining_is_off_unless_asked_for() {
        assert!(
            !DlqConfig::default().enabled,
            "advancing the checkpoint past an undelivered event is data loss; it must \
             be an explicit choice"
        );
    }

    #[test]
    fn a_zero_size_cap_is_rejected() {
        let config: DlqConfig = serde_json::from_value(serde_json::json!({
            "enabled": true,
            "type": "file",
            "path": "/tmp/dlq.jsonl",
            "max_bytes": 0,
        }))
        .expect("parses");
        assert!(config.validate().is_err());
    }

    #[test]
    fn the_kafka_target_requires_brokers_and_a_topic() {
        let config: DlqConfig = serde_json::from_value(serde_json::json!({
            "enabled": true,
            "type": "kafka",
            "brokers": "",
            "topic": "cdc.dlq",
        }))
        .expect("parses");
        assert!(config.validate().is_err());
    }

    #[test]
    fn a_disabled_dlq_skips_target_validation_entirely() {
        let config: DlqConfig = serde_json::from_value(serde_json::json!({
            "type": "kafka",
            "brokers": "",
            "topic": "",
        }))
        .expect("parses");
        config
            .validate()
            .expect("an unconfigured target is irrelevant while disabled");
    }
}
