use std::path::Path;

use crate::pipeline::transform;
use crate::{
    cli::{ReplayArgs, ReplaySink},
    config,
    config::schema::DeliveryContract,
    error::AppError,
    sink,
};
use rustcdc::sink::SinkAdapter as _;

use crate::runtime::batch;

pub async fn execute(args: ReplayArgs, config_path: Option<&Path>) -> Result<(), AppError> {
    // Build the sink from config (or fall back to stdout if no config given).
    let (sink_config, transform_runtime, transform_rules, runtime_tuning, delivery_contract) =
        if let Some(cfg_path) = config_path {
            let cfg = config::load(cfg_path)?;
            (
                cfg.sink,
                cfg.pipeline.transform_runtime,
                cfg.pipeline.transforms,
                cfg.runtime,
                cfg.delivery_contract,
            )
        } else {
            (
                crate::config::schema::SinkConfig::Stdout(Default::default()),
                Default::default(),
                Vec::new(),
                crate::config::schema::RuntimeTuningConfig::default(),
                DeliveryContract::default(),
            )
        };

    let override_sink = match args.sink {
        ReplaySink::Stdout => Some(crate::config::schema::SinkConfig::Stdout(Default::default())),
        ReplaySink::FileJsonl => {
            ensure_replay_config_sink(
                config_path,
                &sink_config,
                "file_jsonl",
                matches!(sink_config, crate::config::schema::SinkConfig::FileJsonl(_)),
            )?;
            None
        }
        ReplaySink::Http => {
            ensure_replay_config_sink(
                config_path,
                &sink_config,
                "http",
                matches!(sink_config, crate::config::schema::SinkConfig::Http(_)),
            )?;
            None
        }
        ReplaySink::Kafka => {
            ensure_replay_config_sink(
                config_path,
                &sink_config,
                "kafka",
                matches!(sink_config, crate::config::schema::SinkConfig::Kafka(_)),
            )?;
            None
        }
        ReplaySink::Iceberg => {
            ensure_replay_config_sink(
                config_path,
                &sink_config,
                "iceberg",
                matches!(sink_config, crate::config::schema::SinkConfig::Iceberg(_)),
            )?;
            None
        }
    };
    let effective_sink_config = override_sink.unwrap_or(sink_config);
    let mut sink = crate::pipeline::router::single(
        sink::build_binding(&effective_sink_config, runtime_tuning.max_event_bytes).await?,
    );

    // Fail fast if the parity mode is incompatible with the delivery contract
    // and this sink's capabilities.
    batch::validate_parity_contract(args.checkpoint_parity_mode, &sink, delivery_contract)?;

    let checkpoint_parity_plan = batch::checkpoint_parity_plan(args.checkpoint_parity_mode, &sink);
    let transform_pipeline =
        transform::TransformPipeline::from_config(transform_runtime, transform_rules)?;

    // Parse the idempotency guard offset.
    let skip_before: Option<u64> = match args.skip_before_offset.as_deref() {
        Some(raw) => {
            let parsed = parse_source_offset(raw).ok_or_else(|| {
                AppError::Other(format!(
                    "--skip-before-offset '{}' is not a valid source offset \
                     (expected WAL LSN like 'A/1B2C3D4E' or a hex u64)",
                    raw
                ))
            })?;
            tracing::info!(
                skip_before_offset = raw,
                skip_before_numeric = parsed,
                "replay: idempotency guard active — events before this offset will be skipped"
            );
            Some(parsed)
        }
        None => {
            tracing::warn!(
                "replay: no --skip-before-offset provided; events may be re-delivered \
                 to the sink if it was already partially populated in a prior run"
            );
            None
        }
    };

    let metadata = std::fs::metadata(&args.event_file).map_err(|e| {
        AppError::Other(format!(
            "failed to stat replay file {}: {e}",
            args.event_file.display()
        ))
    })?;

    if let Some(max_file_bytes) = args.max_file_bytes
        && metadata.len() > max_file_bytes
    {
        return Err(AppError::Other(format!(
            "replay file {} is {} bytes and exceeds --max-file-bytes {}",
            args.event_file.display(),
            metadata.len(),
            max_file_bytes
        )));
    }

    let file = std::fs::File::open(&args.event_file).map_err(|e| {
        AppError::Other(format!(
            "failed to open replay file {}: {e}",
            args.event_file.display()
        ))
    })?;
    let mut reader = std::io::BufReader::new(file);

    let limit = args.limit.unwrap_or(usize::MAX);
    let mut replayed = 0usize;
    let mut skipped = 0usize;
    let max_batch_events = runtime_tuning.max_buffer_size.max(1);
    loop {
        if replayed >= limit {
            break;
        }

        let remaining = limit.saturating_sub(replayed);
        let batch_limit = if limit == usize::MAX {
            max_batch_events
        } else {
            max_batch_events.min(remaining.max(1))
        };

        let raw_events = read_replay_event_batch(
            &mut reader,
            &args.event_file,
            args.max_line_bytes,
            batch_limit,
        )?;

        if raw_events.is_empty() {
            break;
        }

        // Apply the idempotency offset guard: drop events whose source offset
        // is strictly less than the skip threshold.
        let events: Vec<_> = if let Some(threshold) = skip_before {
            raw_events
                .into_iter()
                .filter(|e| {
                    let keep = parse_source_offset(&e.source.offset)
                        .map(|v| v >= threshold)
                        .unwrap_or(true); // unknown format → keep to be safe
                    if !keep {
                        skipped = skipped.saturating_add(1);
                    }
                    keep
                })
                .collect()
        } else {
            raw_events
        };

        if events.is_empty() {
            // All events in this batch were below the skip threshold; keep
            // reading to look for events that meet the threshold.
            continue;
        }

        let batch_stats = batch::process_batch_events_with_optional_checkpoint_barrier(
            &mut sink,
            events,
            &transform_pipeline,
            runtime_tuning.prepare_parallelism,
            runtime_tuning.sink_flush_interval_events,
            runtime_tuning.sink_delivery_queue_capacity,
            runtime_tuning.sink_send_timeout_ms,
            runtime_tuning.sink_flush_timeout_ms,
            args.checkpoint_parity_mode,
            // No quarantine for a one-shot command: a failure here should be seen.
            None,
        )
        .await?;

        replayed = replayed.saturating_add(batch_stats.delivery.sink_send_ops_total as usize);
    }

    sink.close().await?;
    tracing::info!(
        replayed,
        skipped,
        checkpoint_parity_mode = ?args.checkpoint_parity_mode,
        checkpoint_parity_requested = checkpoint_parity_plan.requested,
        checkpoint_parity_effective = checkpoint_parity_plan.effective,
        runtime_ack_commit_executed = false,
        "replay complete"
    );
    Ok(())
}

/// Parse a source offset string into a comparable u64.
///
/// Accepts:
/// - PostgreSQL WAL LSN: `A/B` (hex segments) → `(A << 32) | B`
/// - Plain hex: `0x1A2B3C4D` or `1A2B3C4D`
/// - Plain decimal: `1234567890`
fn parse_source_offset(s: &str) -> Option<u64> {
    let s = s.trim();

    // WAL LSN format: A/B (both hex)
    if let Some((hi, lo)) = s.split_once('/') {
        let hi = u64::from_str_radix(hi.trim(), 16).ok()?;
        let lo = u64::from_str_radix(lo.trim(), 16).ok()?;
        return Some((hi << 32) | lo);
    }

    // Hex with 0x prefix
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        return u64::from_str_radix(hex, 16).ok();
    }

    // Bare hex (all hex digits, at least one non-decimal digit)
    if s.chars().all(|c| c.is_ascii_hexdigit()) && s.chars().any(|c| c.is_ascii_alphabetic()) {
        return u64::from_str_radix(s, 16).ok();
    }

    // Decimal fallback
    s.parse::<u64>().ok()
}

#[cfg(test)]
mod offset_parser_tests {
    use super::parse_source_offset;

    #[test]
    fn parse_wal_lsn() {
        assert_eq!(parse_source_offset("0/1B2C3D4E"), Some(0x1B2C3D4E));
        assert_eq!(parse_source_offset("1/0"), Some(1u64 << 32));
        assert_eq!(parse_source_offset("A/B"), Some((0xA << 32) | 0xB));
    }

    #[test]
    fn parse_hex_prefix() {
        assert_eq!(parse_source_offset("0x1A2B3C4D"), Some(0x1A2B3C4D));
    }

    #[test]
    fn parse_decimal() {
        assert_eq!(parse_source_offset("1000000"), Some(1_000_000));
    }

    #[test]
    fn invalid_returns_none() {
        assert_eq!(parse_source_offset("not_a_number"), None);
    }
}

fn read_replay_event_batch(
    reader: &mut std::io::BufReader<std::fs::File>,
    replay_file: &Path,
    max_line_bytes: usize,
    batch_limit: usize,
) -> Result<Vec<rustcdc::core::Event>, AppError> {
    let mut events = Vec::with_capacity(batch_limit);
    let mut line = String::new();

    while events.len() < batch_limit {
        line.clear();
        let read = std::io::BufRead::read_line(reader, &mut line).map_err(|e| {
            AppError::Other(format!(
                "failed while reading replay file {}: {e}",
                replay_file.display()
            ))
        })?;

        if read == 0 {
            break;
        }

        if read > max_line_bytes {
            return Err(AppError::Other(format!(
                "replay line exceeds --max-line-bytes {} (observed {} bytes)",
                max_line_bytes, read
            )));
        }

        let line = line.trim();
        if line.is_empty() || line.starts_with("//") {
            continue;
        }

        let event: rustcdc::core::Event = serde_json::from_str(line).map_err(|e| {
            AppError::Other(format!("failed to parse event line: {e}\n  line: {line}"))
        })?;
        events.push(event);
    }

    Ok(events)
}

fn ensure_replay_config_sink(
    config_path: Option<&Path>,
    sink_config: &crate::config::schema::SinkConfig,
    requested_sink: &str,
    matches_requested_sink: bool,
) -> Result<(), AppError> {
    let Some(cfg_path) = config_path else {
        return Err(AppError::Other(format!(
            "--sink {requested_sink} requires --config-file with matching sink configuration"
        )));
    };

    if !matches_requested_sink {
        return Err(AppError::Other(format!(
            "--sink {requested_sink} requested, but config sink in {} does not match",
            cfg_path.display()
        )));
    }

    let _ = sink_config;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::cli::{CheckpointParityMode, ReplayArgs, ReplaySink};
    use crate::config::schema::{KafkaSecurityConfig, KafkaSecurityProtocol};
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use rustcdc::core::{Event, Operation, SourceMetadata};
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;
    use tokio::time::{Duration, sleep};

    fn test_suffix() -> String {
        format!(
            "{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        )
    }

    fn sample_event(offset: String, ts: u64) -> Event {
        Event::builder("users", Operation::Insert)
            .after(json!({"id": ts, "name": "replay"}))
            .source(SourceMetadata::new("postgres", offset, ts))
            .ts(ts)
            .schema("public")
            .primary_key(["id"])
            .build()
    }

    fn write_replay_file(path: &Path, events: &[Event]) {
        let mut file = std::fs::File::create(path).expect("create replay file");
        for event in events {
            let line = serde_json::to_string(event).expect("serialize event");
            writeln!(&mut file, "{line}").expect("write replay line");
        }
    }

    fn decode_event_offset(record_value: &[u8]) -> String {
        let event: Event =
            serde_json::from_slice(record_value).expect("kafka record value must decode as Event");
        event.source.offset
    }

    async fn consume_offset_sequence_until_seen(
        consumer: &Consumer,
        expected_offsets: &BTreeSet<String>,
        attempts: usize,
    ) -> Vec<String> {
        let mut seen_offsets = BTreeSet::new();
        let mut sequence = Vec::new();

        for _ in 0..attempts {
            let records = consumer
                .poll(Duration::from_millis(250))
                .await
                .expect("consumer poll should succeed");

            for record in records {
                if let Some(value) = &record.value {
                    let offset = decode_event_offset(value.as_ref());
                    sequence.push(offset.clone());
                    seen_offsets.insert(offset);
                }
            }

            if expected_offsets.is_subset(&seen_offsets) {
                break;
            }

            sleep(Duration::from_millis(100)).await;
        }

        sequence
    }

    fn kafka_auth_from_env() -> krafka::auth::AuthConfig {
        let security = KafkaSecurityConfig {
            protocol: match std::env::var("CDC_TEST_KAFKA_PROTOCOL") {
                Ok(protocol) if protocol.eq_ignore_ascii_case("tls") => KafkaSecurityProtocol::Tls,
                _ => KafkaSecurityProtocol::Plaintext,
            },
            ssl_ca_location: std::env::var("CDC_TEST_KAFKA_CA").ok().map(PathBuf::from),
            ..KafkaSecurityConfig::default()
        };

        security
            .to_auth_config()
            .expect("kafka auth config should be valid")
    }

    fn write_replay_config(path: &Path, brokers: &str, topic: &str) {
        let mut config = format!(
            r#"api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "cdc"
replication_slot_name = "rustcdc_slot"
publication_name = "rustcdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"

[sink.kafka]
brokers = "{brokers}"
topic = "{topic}"
client_id = "cdc-replay-test"
ack_timeout_ms = 1000
retry_backoff_ms = 100
retry_max_attempts = 3

[state]
dir = "/tmp/cdc-state"

[admin]
bind = "127.0.0.1:8080"

[observability]
service_name = "cdc-server"
"#
        );

        match std::env::var("CDC_TEST_KAFKA_PROTOCOL") {
            Ok(protocol) if protocol.eq_ignore_ascii_case("tls") => {
                config
                    .push_str("\n[sink.kafka.security]\nprotocol = \"tls\"\nverify_peer = true\n");
                if let Ok(ca) = std::env::var("CDC_TEST_KAFKA_CA") {
                    config.push_str(&format!("ssl_ca_location = \"{ca}\"\n"));
                }
            }
            _ => {
                config.push_str("\n[sink.kafka.security]\nprotocol = \"plaintext\"\n");
            }
        }

        std::fs::write(path, config).expect("write replay config");
    }

    fn write_file_jsonl_replay_config(
        path: &Path,
        state_dir: &Path,
        output_path: &Path,
        max_event_bytes: usize,
    ) {
        let config = format!(
            r#"api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "cdc"
replication_slot_name = "rustcdc_slot"
publication_name = "rustcdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "file_jsonl"
path = "{}"

[state]
dir = "{}"

[runtime]
max_event_bytes = {}
"#,
            output_path.display(),
            state_dir.display(),
            max_event_bytes,
        );

        std::fs::write(path, config).expect("write file_jsonl replay config");
    }

    #[tokio::test]
    async fn replay_command_enforces_runtime_max_event_bytes() {
        let dir = tempdir().expect("tempdir");
        let config_path = dir.path().join("cdc.toml");
        let state_dir = dir.path().join("state");
        let output_path = dir.path().join("replay-output.jsonl");
        std::fs::create_dir_all(&state_dir).expect("create state dir");
        write_file_jsonl_replay_config(&config_path, &state_dir, &output_path, 16);

        let replay_file = dir.path().join("replay.jsonl");
        write_replay_file(
            &replay_file,
            &[sample_event("0/TOO-LARGE".to_string(), 101)],
        );

        let err = execute(
            ReplayArgs {
                event_file: replay_file,
                sink: ReplaySink::FileJsonl,
                limit: None,
                max_file_bytes: Some(10 * 1024 * 1024),
                max_line_bytes: 1024 * 1024,
                checkpoint_parity_mode: CheckpointParityMode::Enabled,
                skip_before_offset: None,
            },
            Some(&config_path),
        )
        .await
        .expect_err("oversized replay event must fail closed");

        assert!(
            err.to_string().contains("runtime.max_event_bytes"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn replay_command_preserves_order_and_no_duplicates_across_restart_when_env_is_set() {
        let Ok(brokers) = std::env::var("CDC_TEST_KAFKA_BROKERS") else {
            eprintln!("skipping replay kafka test (CDC_TEST_KAFKA_BROKERS is not set)");
            return;
        };
        let Ok(topic) = std::env::var("CDC_TEST_KAFKA_TOPIC") else {
            eprintln!("skipping replay kafka test (CDC_TEST_KAFKA_TOPIC is not set)");
            return;
        };

        let suffix = test_suffix();
        let dir = tempdir().expect("tempdir");
        let config_path = dir.path().join("cdc.toml");
        write_replay_config(&config_path, &brokers, &topic);

        let first_offsets = [
            format!("0/REPLAY-{suffix}-1"),
            format!("0/REPLAY-{suffix}-2"),
        ];
        let second_offsets = vec![
            format!("0/REPLAY-{suffix}-3"),
            format!("0/REPLAY-{suffix}-4"),
        ];

        let first_events = vec![
            sample_event(first_offsets[0].clone(), 101),
            sample_event(first_offsets[1].clone(), 102),
        ];
        let second_events = vec![
            sample_event(second_offsets[0].clone(), 201),
            sample_event(second_offsets[1].clone(), 202),
        ];

        let first_file = dir.path().join("replay-1.jsonl");
        let second_file = dir.path().join("replay-2.jsonl");
        write_replay_file(&first_file, &first_events);
        write_replay_file(&second_file, &second_events);

        execute(
            ReplayArgs {
                event_file: first_file,
                sink: ReplaySink::Stdout,
                limit: None,
                max_file_bytes: Some(10 * 1024 * 1024),
                max_line_bytes: 1024 * 1024,
                checkpoint_parity_mode: CheckpointParityMode::Disabled,
                skip_before_offset: None,
            },
            Some(&config_path),
        )
        .await
        .expect("first replay execute should succeed");

        let auth = kafka_auth_from_env();
        let group_id = format!("cdc-replay-e2e-{suffix}");
        let consumer_a = Consumer::builder()
            .bootstrap_servers(brokers.clone())
            .group_id(group_id.clone())
            .client_id(format!("cdc-replay-e2e-a-{suffix}"))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .request_timeout(Duration::from_millis(1_000))
            .connect_timeout(crate::sink::kafka_connect_timeout(Duration::from_millis(
                1_000,
            )))
            .auth(auth.clone())
            .build()
            .await
            .expect("consumer A should build");
        consumer_a
            .subscribe(&[topic.as_str()])
            .await
            .expect("consumer A should subscribe");

        let expected_first_set = first_offsets.iter().cloned().collect::<BTreeSet<_>>();
        let seen_by_a_sequence =
            consume_offset_sequence_until_seen(&consumer_a, &expected_first_set, 60).await;
        let seen_by_a = seen_by_a_sequence.iter().cloned().collect::<BTreeSet<_>>();
        assert!(
            expected_first_set.is_subset(&seen_by_a),
            "consumer A did not observe first replay batch"
        );
        consumer_a
            .commit()
            .await
            .expect("consumer A commit should succeed");
        consumer_a
            .close()
            .await
            .expect("consumer A close should succeed");

        execute(
            ReplayArgs {
                event_file: second_file,
                sink: ReplaySink::Stdout,
                limit: None,
                max_file_bytes: Some(10 * 1024 * 1024),
                max_line_bytes: 1024 * 1024,
                checkpoint_parity_mode: CheckpointParityMode::Disabled,
                skip_before_offset: None,
            },
            Some(&config_path),
        )
        .await
        .expect("second replay execute should succeed");

        let consumer_b = Consumer::builder()
            .bootstrap_servers(brokers)
            .group_id(group_id)
            .client_id(format!("cdc-replay-e2e-b-{suffix}"))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .request_timeout(Duration::from_millis(1_000))
            .connect_timeout(crate::sink::kafka_connect_timeout(Duration::from_millis(
                1_000,
            )))
            .auth(auth)
            .build()
            .await
            .expect("consumer B should build");
        consumer_b
            .subscribe(&[topic.as_str()])
            .await
            .expect("consumer B should subscribe");

        let expected_second_set = second_offsets.iter().cloned().collect::<BTreeSet<_>>();
        let seen_by_b_sequence =
            consume_offset_sequence_until_seen(&consumer_b, &expected_second_set, 60).await;
        let seen_by_b = seen_by_b_sequence.iter().cloned().collect::<BTreeSet<_>>();
        assert!(
            expected_second_set.is_subset(&seen_by_b),
            "consumer B did not observe second replay batch"
        );

        let replayed_from_first = seen_by_b_sequence
            .iter()
            .filter(|offset| expected_first_set.contains(*offset))
            .cloned()
            .collect::<Vec<_>>();
        assert!(
            replayed_from_first.is_empty(),
            "consumer B replayed first-batch offsets after restart boundary: {replayed_from_first:?}"
        );

        let seen_second_in_order = seen_by_b_sequence
            .iter()
            .filter(|offset| expected_second_set.contains(*offset))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            seen_second_in_order, second_offsets,
            "consumer B violated second replay batch ordering"
        );

        let mut second_counts = BTreeMap::<String, usize>::new();
        for offset in &seen_second_in_order {
            *second_counts.entry(offset.clone()).or_default() += 1;
        }
        for expected in &second_offsets {
            assert_eq!(
                second_counts.get(expected),
                Some(&1),
                "consumer B observed duplicate/missing second-batch offset {expected}"
            );
        }

        consumer_b
            .commit()
            .await
            .expect("consumer B commit should succeed");
        consumer_b
            .close()
            .await
            .expect("consumer B close should succeed");
    }
}
