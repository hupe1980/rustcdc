use chrono::Utc;
use rustcdc::checkpoint::{Checkpoint, FileCheckpoint};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{fs, io::Write, path::Path, path::PathBuf};

use crate::config::schema::KafkaTopicStateConfig;
use crate::state::offset::kafka as kafka_backend;
use crate::{cli::MigrateStateArgs, error::AppError};

#[derive(Debug, Serialize)]
struct ArtifactReport {
    kind: String,
    source_path: String,
    target_path: String,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Serialize)]
struct MigrationReport {
    generated_at: String,
    source_backend: String,
    target_backend: String,
    source_dir: String,
    target_dir: String,
    overwrite: bool,
    verified: bool,
    artifacts: Vec<ArtifactReport>,
    evidence: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
enum ArtifactKind {
    Checkpoint,
    SchemaHistory,
}

impl ArtifactKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoint",
            Self::SchemaHistory => "schema_history",
        }
    }
}

#[derive(serde::Deserialize)]
struct CheckpointEnvelope {
    source_type: String,
}

pub async fn execute(args: MigrateStateArgs, _config_path: Option<&Path>) -> Result<(), AppError> {
    run(args).await
}

async fn run(args: MigrateStateArgs) -> Result<(), AppError> {
    match args.source_backend.as_str() {
        "local_fs" | "" => run_local_fs(args).await,
        "kafka" => run_kafka_to_local_fs(args).await,
        other => Err(AppError::Other(format!(
            "--source-backend '{}' is not supported; choose one of: local_fs, kafka",
            other
        ))),
    }
}

async fn run_local_fs(args: MigrateStateArgs) -> Result<(), AppError> {
    let source_dir = args.source_dir.as_ref().ok_or_else(|| {
        AppError::Other("--source-dir is required when --source-backend=local_fs".to_string())
    })?;

    if *source_dir == args.target_dir {
        return Err(AppError::Other(
            "source and target backend locations must differ".to_string(),
        ));
    }

    if !source_dir.exists() {
        return Err(AppError::Other(format!(
            "source state directory does not exist: {}",
            source_dir.display()
        )));
    }

    fs::create_dir_all(&args.target_dir).map_err(AppError::from)?;

    let mut artifacts = Vec::new();

    copy_artifact(
        ArtifactKind::Checkpoint,
        source_dir,
        &args.target_dir,
        args.overwrite,
        &mut artifacts,
    )?;

    copy_artifact(
        ArtifactKind::SchemaHistory,
        source_dir,
        &args.target_dir,
        args.overwrite,
        &mut artifacts,
    )?;

    // A byte-identical copy preserves the 0.7.0 content_checksum, but the copy
    // is only as good as the source file — prove the target loads through the
    // runtime's own gates before reporting the migration as verified.
    if artifacts
        .iter()
        .any(|artifact| artifact.kind == "checkpoint")
    {
        verify_checkpoint_loads(&args.target_dir.join("checkpoint"), None).await?;
    }

    let report = MigrationReport {
        generated_at: Utc::now().to_rfc3339(),
        source_backend: "local_fs".to_string(),
        target_backend: "local_fs".to_string(),
        source_dir: source_dir.display().to_string(),
        target_dir: args.target_dir.display().to_string(),
        overwrite: args.overwrite,
        verified: true,
        evidence: vec![
            format!("source={}", source_dir.display()),
            format!("target={}", args.target_dir.display()),
            format!("artifacts={}", artifacts.len()),
        ],
        artifacts,
    };

    emit_report(report, args.output)
}

async fn run_kafka_to_local_fs(args: MigrateStateArgs) -> Result<(), AppError> {
    let brokers = args.kafka_brokers.as_deref().ok_or_else(|| {
        AppError::Other("--kafka-brokers is required when --source-backend=kafka".to_string())
    })?;
    let topic = args.kafka_topic.as_deref().ok_or_else(|| {
        AppError::Other("--kafka-topic is required when --source-backend=kafka".to_string())
    })?;

    let config = KafkaTopicStateConfig {
        brokers: brokers.to_string(),
        topic: topic.to_string(),
        client_id: args.kafka_client_id.clone(),
        request_timeout_ms: args.kafka_request_timeout_ms,
        readback_poll_timeout_ms: args.kafka_readback_poll_timeout_ms,
        // Use relaxed durability defaults for migration reads — we do not write
        // to the topic so replication-factor constraints are irrelevant.
        min_replication_factor: 1,
        min_insync_replicas: 1,
        durability_profile: crate::config::schema::KafkaStateDurabilityProfile::Development,
        security: Default::default(),
    };

    tracing::info!(
        brokers,
        topic,
        "migrate-state: scanning Kafka compacted topic"
    );

    let (checkpoint_bytes_opt, schema_history_bytes_opt) =
        kafka_backend::load_raw_bytes_for_migration(&config).await?;

    fs::create_dir_all(&args.target_dir).map_err(AppError::from)?;

    let mut artifacts = Vec::new();

    // ── Checkpoint ──────────────────────────────────────────────────────────
    //
    // The Kafka record is the server's own wire format (`offset_hex` + counters),
    // NOT a rustcdc checkpoint file. It must be re-materialized through
    // `FileCheckpoint::restore_from_record`, which parses the offset, computes the
    // mandatory `content_checksum`, and writes the file 0600 + fsynced. Writing the
    // raw Kafka bytes to disk (the pre-0.7.0 behaviour) produced a file the runtime
    // rejected on load.
    if let Some(checkpoint_bytes) = checkpoint_bytes_opt {
        let record: kafka_backend::KafkaTopicCheckpointRecord =
            serde_json::from_slice(&checkpoint_bytes).map_err(|e| {
                AppError::Other(format!(
                    "failed to parse Kafka checkpoint record for migration: {e}"
                ))
            })?;

        if kafka_backend::is_bootstrap_checkpoint_record(&record) {
            tracing::info!(
                "migrate-state: Kafka topic holds only the bootstrap checkpoint sentinel — \
                 nothing to migrate for the checkpoint artifact"
            );
        } else {
            let offset_bytes = hex::decode(&record.offset_hex).map_err(|e| {
                AppError::Other(format!(
                    "Kafka checkpoint record has invalid offset_hex: {e}"
                ))
            })?;

            let checkpoint_dir = args.target_dir.join("checkpoint");
            fs::create_dir_all(&checkpoint_dir).map_err(AppError::from)?;
            let checkpoint_name = format!("checkpoint_{}.json", record.source_type);
            let target_path = checkpoint_dir.join(&checkpoint_name);

            if target_path.exists() && !args.overwrite {
                return Err(AppError::Other(format!(
                    "destination already exists: {} (use --overwrite to replace it)",
                    target_path.display()
                )));
            }

            FileCheckpoint::restore_from_record(
                &checkpoint_dir,
                &record.source_type,
                offset_bytes,
                record.committed_event_count,
            )
            .map_err(|e| {
                AppError::Other(format!(
                    "failed to materialize checkpoint at {}: {e}",
                    target_path.display()
                ))
            })?;

            // Verify by loading through the same code path the runtime uses —
            // this exercises the checksum, permission, and offset-decoding gates,
            // which a byte-compare cannot.
            verify_checkpoint_loads(&checkpoint_dir, Some(record.committed_event_count)).await?;

            let persisted = fs::read(&target_path).map_err(AppError::from)?;
            artifacts.push(ArtifactReport {
                kind: "checkpoint".to_string(),
                source_path: format!("kafka://{}#{}", topic, "checkpoint"),
                target_path: target_path.display().to_string(),
                bytes: persisted.len() as u64,
                sha256: hex::encode(Sha256::digest(&persisted)),
            });
        }
    }

    // ── Schema history ──────────────────────────────────────────────────────
    if let Some(schema_history_bytes) = schema_history_bytes_opt {
        let target_path = args.target_dir.join("schema_history");

        if target_path.exists() && !args.overwrite {
            return Err(AppError::Other(format!(
                "destination already exists: {} (use --overwrite to replace it)",
                target_path.display()
            )));
        }

        write_local_file(&target_path, &schema_history_bytes)?;

        let persisted = fs::read(&target_path).map_err(AppError::from)?;
        let sha = hex::encode(Sha256::digest(&schema_history_bytes));
        let persisted_sha = hex::encode(Sha256::digest(&persisted));
        if persisted_sha != sha {
            return Err(AppError::Other(format!(
                "destination verification failed for schema_history at {}",
                target_path.display()
            )));
        }

        artifacts.push(ArtifactReport {
            kind: "schema_history".to_string(),
            source_path: format!("kafka://{}#{}", topic, "schema_history"),
            target_path: target_path.display().to_string(),
            bytes: schema_history_bytes.len() as u64,
            sha256: sha,
        });
    }

    if artifacts.is_empty() {
        tracing::warn!(
            "migrate-state: no state artifacts found in Kafka topic '{}'. \
             The topic may be empty or not yet initialized.",
            topic
        );
    }

    let report = MigrationReport {
        generated_at: Utc::now().to_rfc3339(),
        source_backend: "kafka".to_string(),
        target_backend: "local_fs".to_string(),
        source_dir: format!("kafka://{topic}"),
        target_dir: args.target_dir.display().to_string(),
        overwrite: args.overwrite,
        verified: true,
        evidence: vec![
            format!("brokers={brokers}"),
            format!("topic={topic}"),
            format!("artifacts={}", artifacts.len()),
        ],
        artifacts,
    };

    emit_report(report, args.output)
}

fn emit_report(report: MigrationReport, output: Option<PathBuf>) -> Result<(), AppError> {
    let report_json = serde_json::to_string_pretty(&report)
        .map_err(|e| AppError::Other(format!("failed to serialise migration report: {e}")))?;

    if let Some(path) = output {
        fs::write(&path, &report_json).map_err(|e| {
            AppError::Other(format!(
                "failed to write migration report {}: {e}",
                path.display()
            ))
        })?;
    }

    println!("{report_json}");
    Ok(())
}

fn copy_artifact(
    kind: ArtifactKind,
    source_root: &Path,
    target_root: &Path,
    overwrite: bool,
    artifacts: &mut Vec<ArtifactReport>,
) -> Result<(), AppError> {
    let source_path = match kind {
        ArtifactKind::Checkpoint => discover_local_checkpoint(source_root)?,
        ArtifactKind::SchemaHistory => {
            let path = source_root.join("schema_history");
            if path.exists() {
                Some(path)
            } else {
                None
            }
        }
    };

    let Some(source_path) = source_path else {
        return Ok(());
    };

    let bytes = fs::read(&source_path).map_err(AppError::from)?;
    let target_path = match kind {
        ArtifactKind::Checkpoint => {
            let source_type = checkpoint_source_type(&bytes)?;
            let checkpoint_name = format!("checkpoint_{source_type}.json");
            target_root.join("checkpoint").join(checkpoint_name)
        }
        ArtifactKind::SchemaHistory => target_root.join("schema_history"),
    };

    if target_path.exists() && !overwrite {
        return Err(AppError::Other(format!(
            "destination already exists: {} (use --overwrite to replace it)",
            target_path.display()
        )));
    }

    write_local_file(&target_path, &bytes)?;

    let source_sha = hex::encode(Sha256::digest(&bytes));
    let persisted = fs::read(&target_path).map_err(AppError::from)?;
    let persisted_sha = hex::encode(Sha256::digest(&persisted));
    if persisted_sha != source_sha || persisted.len() != bytes.len() {
        return Err(AppError::Other(format!(
            "destination verification failed for {} at {}",
            kind.as_str(),
            target_path.display()
        )));
    }

    artifacts.push(ArtifactReport {
        kind: kind.as_str().to_string(),
        source_path: source_path.display().to_string(),
        target_path: target_path.display().to_string(),
        bytes: bytes.len() as u64,
        sha256: source_sha,
    });

    Ok(())
}

fn discover_local_checkpoint(root: &Path) -> Result<Option<PathBuf>, AppError> {
    let checkpoint_dir = root.join("checkpoint");
    if !checkpoint_dir.exists() {
        return Ok(None);
    }

    let mut matches = Vec::new();
    for entry in fs::read_dir(&checkpoint_dir).map_err(AppError::from)? {
        let entry = entry.map_err(AppError::from)?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };

        if name.starts_with("checkpoint_") && name.ends_with(".json") {
            matches.push(path);
        }
    }

    if matches.len() > 1 {
        return Err(AppError::Other(format!(
            "source has multiple checkpoint files in {}. migrate-state currently supports exactly one",
            checkpoint_dir.display()
        )));
    }

    Ok(matches.pop())
}

fn checkpoint_source_type(bytes: &[u8]) -> Result<String, AppError> {
    let envelope: CheckpointEnvelope = serde_json::from_slice(bytes).map_err(|e| {
        AppError::Other(format!(
            "failed to parse checkpoint artifact while determining target file name: {e}"
        ))
    })?;
    Ok(envelope.source_type)
}

/// Verify a freshly materialized checkpoint through the exact code path the
/// runtime will use at startup: checksum, permission, and offset-decoding gates
/// included. A byte-compare cannot exercise any of those.
async fn verify_checkpoint_loads(
    checkpoint_dir: &Path,
    expected_committed_count: Option<u64>,
) -> Result<(), AppError> {
    let verifier = FileCheckpoint::new(checkpoint_dir);
    let loaded = verifier.load().await.map_err(|e| {
        AppError::Other(format!(
            "migrated checkpoint failed runtime load verification: {e}"
        ))
    })?;
    if loaded.is_none() {
        return Err(AppError::Other(
            "migrated checkpoint verification found no loadable record".to_string(),
        ));
    }
    let count = verifier.get_committed_count().await.map_err(|e| {
        AppError::Other(format!(
            "migrated checkpoint failed committed-count verification: {e}"
        ))
    })?;
    if let Some(expected) = expected_committed_count {
        if count != expected {
            return Err(AppError::Other(format!(
                "migrated checkpoint committed_event_count mismatch: expected {expected}, loaded {count}"
            )));
        }
    }
    Ok(())
}

fn write_local_file(target_path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent).map_err(AppError::from)?;
    }

    let tmp_path = temp_path(target_path);
    {
        let mut file = fs::File::create(&tmp_path).map_err(AppError::from)?;
        // State artifacts are trust anchors: rustcdc 0.7.0 rejects checkpoint
        // files readable by group/other, and schema history deserves the same
        // posture. Restrict before writing any bytes.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(AppError::from)?;
        }
        file.write_all(bytes).map_err(AppError::from)?;
        file.sync_all().map_err(AppError::from)?;
    }

    fs::rename(&tmp_path, target_path).map_err(AppError::from)?;

    if let Some(parent) = target_path.parent() {
        fs::File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(AppError::from)?;
    }

    Ok(())
}

fn temp_path(target_path: &Path) -> PathBuf {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();

    let mut tmp_path = target_path.to_path_buf();
    let ext = target_path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("tmp");
    tmp_path.set_extension(format!("{ext}.{stamp}.tmp"));
    tmp_path
}

#[cfg(test)]
mod tests {
    use super::{discover_local_checkpoint, temp_path, write_local_file};

    #[test]
    fn temp_path_has_unique_extension() {
        let path = std::path::Path::new("/tmp/schema_history");
        let tmp = temp_path(path);
        assert!(tmp.to_string_lossy().contains("schema_history"));
        assert!(tmp.to_string_lossy().contains(".tmp"));
    }

    #[test]
    fn copy_file_can_write_reported_artifact() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = dir.path().join("source.json");
        let target = dir.path().join("target.json");
        std::fs::write(&source, b"hello world").expect("write source");
        let bytes = std::fs::read(&source).expect("source bytes");
        write_local_file(&target, &bytes).expect("write");
        assert_eq!(
            std::fs::read(&target).expect("target bytes"),
            b"hello world"
        );
    }

    #[test]
    fn discover_local_checkpoint_rejects_multiple_checkpoint_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let checkpoint_dir = dir.path().join("checkpoint");
        std::fs::create_dir_all(&checkpoint_dir).expect("checkpoint dir");
        std::fs::write(checkpoint_dir.join("checkpoint_postgres.json"), b"{}").expect("write");
        std::fs::write(checkpoint_dir.join("checkpoint_mysql.json"), b"{}").expect("write");

        let err = discover_local_checkpoint(dir.path()).expect_err("must reject multiple files");
        assert!(
            err.to_string().contains("currently supports exactly one"),
            "unexpected error: {err}"
        );
    }
}
