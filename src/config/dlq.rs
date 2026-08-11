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
    /// Amazon SQS.
    ///
    /// # Why SQS is offered here and *not* as a sink
    ///
    /// A dead-letter queue is a terminal work queue: a human or a repair job reads a
    /// record, acts on it, and deletes it. That is exactly what SQS is, and it brings two
    /// things the file and Kafka targets cannot — **redrive-to-source**, which replays a
    /// DLQ back to its origin with one API call, and broker-level age alarms
    /// (`ApproximateAgeOfOldestMessage`) that page someone when a quarantined record is
    /// going stale.
    ///
    /// It also closes a real hole. Before this, the only durable target was a Kafka topic,
    /// so an AWS deployment writing to Snowflake or Iceberg with no Kafka anywhere had
    /// nothing but a file on a pod filesystem — which is gone at exactly the moment the
    /// operator reaches for it.
    ///
    /// A **sink** is the opposite shape and SQS is deliberately not offered as one. A CDC
    /// consumer re-reads history: it joins late, replays from a position, runs a second
    /// consumer group for a backfill. SQS consumers *delete* what they read, retention caps
    /// at 14 days, there are no consumer groups and no compaction, and FIFO deduplication
    /// covers a 5-minute window — which cannot underpin any delivery contract this server
    /// advertises. Shipping it as a sink would mean a destination that looks like the
    /// others and silently supports neither replay nor `effectively_once`.
    Sqs(Box<SqsDlqConfig>),
}

/// Amazon SQS dead-letter target.
///
/// Credentials come from the standard AWS chain — environment, profile, IMDS, EKS web
/// identity — the same chain the `glue` codec, the S3 Tables catalog and MSK IAM use. There
/// is deliberately nowhere to write an access key.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SqsDlqConfig {
    /// Full queue URL, e.g. `https://sqs.eu-central-1.amazonaws.com/123456789012/cdc-dlq`.
    ///
    /// A URL ending `.fifo` selects FIFO behaviour: records are sent with a
    /// `MessageGroupId` and a `MessageDeduplicationId`, so a retried write cannot enqueue
    /// the same quarantined event twice.
    pub queue_url: String,

    /// Region override. Inferred from the queue URL when absent.
    #[serde(default)]
    pub region: Option<String>,

    /// `MessageGroupId` for a FIFO queue.
    ///
    /// One group means strict order over the whole DLQ, which is what an operator reading
    /// it wants. Ignored for a standard queue.
    #[serde(default = "default_sqs_message_group_id")]
    pub message_group_id: String,
}

fn default_sqs_message_group_id() -> String {
    "rustcdc-dlq".to_string()
}

/// SQS refuses a message body above this size.
///
/// A quarantined event can exceed it — "too large for the sink" is one of the commonest
/// reasons to be quarantined in the first place — so the record's payload is truncated
/// rather than the write being refused. See `dlq/sqs.rs` for why that is the right trade.
pub const SQS_MAX_MESSAGE_BYTES: usize = 256 * 1024;

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
            DlqTarget::Sqs(sqs) => {
                let url = sqs.queue_url.trim();
                if url.is_empty() {
                    return Err("dlq.queue_url must not be empty for the sqs target".to_string());
                }
                // Checked at load rather than at first use: the DLQ is written during an
                // incident, and a malformed URL discovered then turns a quarantine into an
                // outage.
                if !url.starts_with("https://") {
                    return Err(format!(
                        "dlq.queue_url '{url}' must be an https:// SQS queue URL; quarantined \
                         records carry row data and must not cross the network in the clear"
                    ));
                }
                if !url.contains("sqs.") || url.rsplit('/').count() < 4 {
                    return Err(format!(
                        "dlq.queue_url '{url}' does not look like an SQS queue URL; it should \
                         be like https://sqs.<region>.amazonaws.com/<account-id>/<queue-name>"
                    ));
                }
                if url.ends_with(".fifo") && sqs.message_group_id.trim().is_empty() {
                    return Err(
                        "dlq.message_group_id must not be empty for a FIFO queue".to_string()
                    );
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

    /// The queue URL is checked at load, because the dead-letter queue is written *during*
    /// an incident — a malformed URL discovered then turns a quarantine into an outage.
    #[test]
    fn the_sqs_target_refuses_a_url_that_is_not_an_https_sqs_queue() {
        let config = |url: &str| -> DlqConfig {
            serde_json::from_value(serde_json::json!({
                "enabled": true,
                "type": "sqs",
                "queue_url": url,
            }))
            .expect("parses")
        };

        config("https://sqs.eu-central-1.amazonaws.com/123456789012/cdc-dlq")
            .validate()
            .expect("a well-formed queue URL must be accepted");

        // Plaintext: quarantined records carry row data.
        let err = config("http://sqs.eu-central-1.amazonaws.com/123456789012/cdc-dlq")
            .validate()
            .expect_err("plaintext must be refused");
        assert!(err.contains("https://"), "{err}");

        // A queue *name* rather than a URL is the usual mistake.
        let err = config("https://example.com/cdc-dlq")
            .validate()
            .expect_err("a non-SQS URL must be refused");
        assert!(err.contains("does not look like an SQS queue URL"), "{err}");
    }

    /// A FIFO queue needs a message group; a missing one is rejected by SQS at send time,
    /// which is again during an incident.
    #[test]
    fn a_fifo_queue_requires_a_message_group() {
        let config: DlqConfig = serde_json::from_value(serde_json::json!({
            "enabled": true,
            "type": "sqs",
            "queue_url": "https://sqs.eu-central-1.amazonaws.com/123456789012/cdc-dlq.fifo",
            "message_group_id": "  ",
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
