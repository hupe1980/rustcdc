#![cfg(all(feature = "postgres", feature = "metrics"))]
//! # PostgreSQL to OpenTelemetry example
//!
//! Advanced streaming example with comprehensive observability:
//! - PostgreSQL snapshot + stream processing
//! - OTLP metrics + tracing export
//! - Structured JSON logs to stdout
//! - Deterministic graceful shutdown (max events or runtime budget)

use std::{
    env,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use cdc_rs::{
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory, CdcRuntime, EventTracer,
    MetricsCollector, OTelConfig, OTelEventTracer, OTelMetricsCollector, PostgresSourceConfig,
    RuntimeConfig, RuntimeObservability, RuntimeSourceConfig, StructuredLogger,
    TransportConfig,
};
use serde_json::json;

/// Runs a PostgreSQL CDC pipeline with OTLP metrics/tracing and structured lifecycle logs.
#[tokio::main(flavor = "current_thread")]
async fn main() -> cdc_rs::Result<()> {
    // Parse config from env/CLI so this sample can run both locally and in CI.
    let args = ExampleArgs::from_env_and_args()?;
    std::fs::create_dir_all(&args.checkpoint_dir).map_err(cdc_rs::Error::IoError)?;

    let otel_config = OTelConfig::new(
        args.otlp_endpoint.clone(),
        args.service_name.clone(),
        args.service_version.clone(),
        args.environment.clone(),
    );

    let metrics = Arc::new(OTelMetricsCollector::with_otlp_exporter(
        otel_config.clone(),
    )?);
    let tracer =
        Arc::new(OTelEventTracer::with_otlp_exporter(otel_config)?.with_source_type("postgres"));

    let runtime_metrics: Arc<dyn MetricsCollector> = metrics.clone();
    let runtime_tracer: Arc<dyn EventTracer> = tracer.clone();

    let source = PostgresSourceConfig {
        host: args.host.clone(),
        port: args.port,
        user: args.user.clone(),
        password: args.password.clone().into(),
        database: args.database.clone(),
        replication_slot_name: args.replication_slot_name.clone(),
        publication_name: args.publication_name.clone(),
        transport: TransportConfig::tls(),
        conn_timeout_secs: args.conn_timeout_secs,
        stream_poll_interval_ms: 1_000,
        max_events_per_poll: 20_000,
    };

    let mut runtime = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Postgres(source),
            FileCheckpoint::new(args.checkpoint_dir.clone()),
            InMemorySchemaHistory::default(),
        )
        .with_snapshot_tables(args.snapshot_tables.clone())
        .with_max_buffer_size(args.max_buffer_size)
        .with_max_poll_wait_ms(args.poll_wait_ms)
        .with_observability(
            RuntimeObservability::default()
                .with_metrics(runtime_metrics)
                .with_tracer(runtime_tracer),
        ),
    )?;

    let logger = StructuredLogger::new("postgres");

    // Start source runtime and emit structured lifecycle markers.
    runtime.start().await?;
    logger.source_connected();
    emit_log("source_connected", None, None, "runtime started");

    for table in &args.snapshot_tables {
        logger.snapshot_started(table);
        emit_log(
            "snapshot_started",
            Some(table),
            None,
            "snapshot table registered",
        );
    }

    logger.stream_started("runtime-managed");
    emit_log(
        "stream_started",
        None,
        Some("runtime-managed"),
        "stream loop started",
    );

    tracer.start_snapshot_span("example-snapshot-root", &args.snapshot_tables[0], 0);

    let mut processed = 0usize;
    let mut stream_span_index = 0u64;
    // Optional run budget for deterministic sample execution in CI/local demos.
    let runtime_deadline = if args.max_runtime_secs > 0 {
        Some(Instant::now() + Duration::from_secs(args.max_runtime_secs))
    } else {
        None
    };

    loop {
        if let Some(deadline) = runtime_deadline {
            if Instant::now() >= deadline {
                emit_log("max_runtime_reached", None, None, "runtime budget reached");
                break;
            }
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                emit_log("signal_received", None, None, "ctrl-c received, shutting down");
                break;
            }
            polled = runtime.poll_event_batch() => {
                let batch = polled?;
                if batch.is_empty() {
                    // Avoid tight spin when source has no new events.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }

                let ack = batch.ack_token();
                let batch_event_count = batch.len();
                let events = batch.into_events();

                for mut event in events {
                    let start = Instant::now();

                    let span_id = format!("example-stream-{stream_span_index}");
                    stream_span_index += 1;
                    tracer.start_stream_span(&span_id, Some(&event.table), 1);
                    let _ = tracer.propagate_baggage_to_event(&span_id, &mut event);

                    println!("{}", event.to_json()?);
                    emit_log(
                        "event_processed",
                        Some(&event.table),
                        Some(&event.source.offset),
                        "event emitted",
                    );

                    metrics.record_event_processed(event.op, start.elapsed().as_millis() as u64);
                    tracer.end_span(&span_id);

                    processed += 1;

                    if args.max_events > 0 && processed >= args.max_events {
                        // Graceful limit used by test/demo runs to terminate deterministically.
                        emit_log("max_events_reached", None, None, "graceful completion target reached");
                        break;
                    }
                }

                if args.max_events > 0 && processed >= args.max_events {
                    break;
                }

                if let Some(token) = ack {
                    let commit_start = Instant::now();
                    runtime.commit_ack(token).await?;
                    let latency_ms = commit_start.elapsed().as_millis() as u64;
                    metrics.record_checkpoint_committed(batch_event_count as u64, latency_ms);
                    logger.checkpoint_saved("runtime-managed", batch_event_count as u64);
                    emit_log("checkpoint_saved", None, Some("runtime-managed"), "checkpoint committed");
                }
            }
        }
    }

    tracer.end_span("example-snapshot-root");

    for table in &args.snapshot_tables {
        logger.snapshot_complete(table);
        emit_log(
            "snapshot_complete",
            Some(table),
            None,
            "snapshot table finalized",
        );
    }

    runtime.stop().await?;
    logger.source_disconnected();
    emit_log("source_disconnected", None, None, "runtime stopped");

    // Use bounded best-effort exporter shutdown so sample exit stays deterministic.
    let metrics_for_shutdown = metrics.clone();
    match tokio::time::timeout(
        Duration::from_secs(3),
        tokio::task::spawn_blocking(move || metrics_for_shutdown.shutdown()),
    )
    .await
    {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => {
            emit_log("metrics_shutdown_error", None, None, &format!("{error}"));
        }
        Ok(Err(join_error)) => {
            emit_log(
                "metrics_shutdown_error",
                None,
                None,
                &format!("join error: {join_error}"),
            );
        }
        Err(_) => {
            emit_log(
                "metrics_shutdown_timeout",
                None,
                None,
                "timed out while flushing metrics",
            );
        }
    }

    let tracer_for_shutdown = tracer.clone();
    match tokio::time::timeout(
        Duration::from_secs(2),
        tokio::task::spawn_blocking(move || tracer_for_shutdown.shutdown()),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(join_error)) => {
            emit_log(
                "tracer_shutdown_error",
                None,
                None,
                &format!("join error: {join_error}"),
            );
        }
        Err(_) => {
            emit_log(
                "tracer_shutdown_timeout",
                None,
                None,
                "timed out while shutting down tracer provider",
            );
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct ExampleArgs {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    replication_slot_name: String,
    publication_name: String,
    snapshot_tables: Vec<String>,
    checkpoint_dir: PathBuf,
    max_buffer_size: usize,
    poll_wait_ms: u64,
    conn_timeout_secs: u64,
    max_events: usize,
    max_runtime_secs: u64,
    otlp_endpoint: String,
    service_name: String,
    service_version: String,
    environment: String,
}

impl ExampleArgs {
    /// Parse args with env defaults first, then apply CLI overrides.
    fn from_env_and_args() -> cdc_rs::Result<Self> {
        let mut out = Self {
            host: env_or_default("CDC_RS_POSTGRES_HOST", "localhost"),
            port: env_or_default("CDC_RS_POSTGRES_PORT", "5432")
                .parse::<u16>()
                .map_err(|error| {
                    cdc_rs::Error::ConfigError(format!("invalid CDC_RS_POSTGRES_PORT: {error}"))
                })?,
            user: env_or_default("CDC_RS_POSTGRES_USER", "postgres"),
            password: env_or_default("CDC_RS_POSTGRES_PASSWORD", "postgres"),
            database: env_or_default("CDC_RS_POSTGRES_DB", "postgres"),
            replication_slot_name: env_or_default("CDC_RS_REPLICATION_SLOT_NAME", "cdc_rs_slot"),
            publication_name: env_or_default("CDC_RS_PUBLICATION_NAME", "cdc_rs_publication"),
            snapshot_tables: env_or_default("CDC_RS_SNAPSHOT_TABLES", "public.orders")
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect(),
            checkpoint_dir: PathBuf::from(env_or_default(
                "CDC_RS_CHECKPOINT_DIR",
                "./target/cdc-rs-checkpoints",
            )),
            max_buffer_size: env_or_default("CDC_RS_MAX_BUFFER_SIZE", "1000")
                .parse::<usize>()
                .map_err(|error| {
                    cdc_rs::Error::ConfigError(format!("invalid CDC_RS_MAX_BUFFER_SIZE: {error}"))
                })?,
            poll_wait_ms: env_or_default("CDC_RS_POLL_WAIT_MS", "500")
                .parse::<u64>()
                .map_err(|error| {
                    cdc_rs::Error::ConfigError(format!("invalid CDC_RS_POLL_WAIT_MS: {error}"))
                })?,
            conn_timeout_secs: env_or_default("CDC_RS_CONN_TIMEOUT_SECS", "30")
                .parse::<u64>()
                .map_err(|error| {
                    cdc_rs::Error::ConfigError(format!("invalid CDC_RS_CONN_TIMEOUT_SECS: {error}"))
                })?,
            max_events: env_or_default("CDC_RS_MAX_EVENTS", "0")
                .parse::<usize>()
                .map_err(|error| {
                    cdc_rs::Error::ConfigError(format!("invalid CDC_RS_MAX_EVENTS: {error}"))
                })?,
            max_runtime_secs: env_or_default("CDC_RS_MAX_RUNTIME_SECS", "0")
                .parse::<u64>()
                .map_err(|error| {
                    cdc_rs::Error::ConfigError(format!("invalid CDC_RS_MAX_RUNTIME_SECS: {error}"))
                })?,
            otlp_endpoint: env_or_default("CDC_RS_OTLP_ENDPOINT", "http://localhost:4317"),
            service_name: env_or_default("CDC_RS_SERVICE_NAME", "cdc-rs-postgres-example"),
            service_version: env_or_default("CDC_RS_SERVICE_VERSION", env!("CARGO_PKG_VERSION")),
            environment: env_or_default("CDC_RS_ENVIRONMENT", "dev"),
        };

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--host" => out.host = next_value(&mut args, "--host")?,
                "--port" => {
                    out.port = next_value(&mut args, "--port")?
                        .parse::<u16>()
                        .map_err(|error| {
                            cdc_rs::Error::ConfigError(format!("invalid --port: {error}"))
                        })?
                }
                "--user" => out.user = next_value(&mut args, "--user")?,
                "--password" => out.password = next_value(&mut args, "--password")?,
                "--db" | "--database" => out.database = next_value(&mut args, "--database")?,
                "--replication-slot-name" => {
                    out.replication_slot_name = next_value(&mut args, "--replication-slot-name")?
                }
                "--publication-name" => {
                    out.publication_name = next_value(&mut args, "--publication-name")?
                }
                "--snapshot-tables" => {
                    out.snapshot_tables = next_value(&mut args, "--snapshot-tables")?
                        .split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned)
                        .collect();
                }
                "--checkpoint-dir" => {
                    out.checkpoint_dir = PathBuf::from(next_value(&mut args, "--checkpoint-dir")?)
                }

                "--max-events" => {
                    out.max_events = next_value(&mut args, "--max-events")?
                        .parse::<usize>()
                        .map_err(|error| {
                            cdc_rs::Error::ConfigError(format!("invalid --max-events: {error}"))
                        })?
                }
                "--max-runtime-secs" => {
                    out.max_runtime_secs = next_value(&mut args, "--max-runtime-secs")?
                        .parse::<u64>()
                        .map_err(|error| {
                            cdc_rs::Error::ConfigError(format!(
                                "invalid --max-runtime-secs: {error}"
                            ))
                        })?
                }
                "--otlp-endpoint" => out.otlp_endpoint = next_value(&mut args, "--otlp-endpoint")?,
                "--service-name" => out.service_name = next_value(&mut args, "--service-name")?,
                "--service-version" => {
                    out.service_version = next_value(&mut args, "--service-version")?
                }
                "--environment" => out.environment = next_value(&mut args, "--environment")?,
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => {
                    return Err(cdc_rs::Error::ConfigError(format!(
                        "unknown argument: {other}"
                    )));
                }
            }
        }

        if out.snapshot_tables.is_empty() {
            return Err(cdc_rs::Error::ConfigError(
                "snapshot tables must not be empty; provide --snapshot-tables or CDC_RS_SNAPSHOT_TABLES".to_string(),
            ));
        }
        Ok(out)
    }
}

fn env_or_default(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> cdc_rs::Result<String> {
    args.next()
        .ok_or_else(|| cdc_rs::Error::ConfigError(format!("missing value for {flag}")))
}

/// Emit structured JSON lifecycle markers for humans and log backends.
fn emit_log(event: &str, table: Option<&str>, offset: Option<&str>, message: &str) {
    let value = json!({
        "kind": "log",
        "event": event,
        "source_type": "postgres",
        "table": table,
        "offset": offset,
        "message": message,
    });
    println!("{value}");
}

fn print_help() {
    println!(
        "postgres_to_otel\n\n\
Usage:\n  postgres_to_otel [options]\n\n\
Options:\n\
  --host <host>                 PostgreSQL host (default: localhost)\n\
  --port <port>                 PostgreSQL port (default: 5432)\n\
  --user <user>                 PostgreSQL user (default: postgres)\n\
  --password <password>         PostgreSQL password\n\
  --database <db>               PostgreSQL database\n\
  --replication-slot-name <name> Replication slot name (default: cdc_rs_slot)\n\
  --publication-name <name>     Publication name (default: cdc_rs_publication)\n\
  --snapshot-tables <csv>       Snapshot table list (default: public.orders)\n\
  --checkpoint-dir <path>       Checkpoint directory\n\
  --commit-every <n>            Commit cadence in events (default: 50)\n\
  --max-events <n>              Stop after N events (0 means run forever)\n\
  --max-runtime-secs <n>        Stop after N seconds (0 means no runtime cap)\n\
  --otlp-endpoint <url>         OTLP endpoint (default: http://localhost:4317)\n\
  --service-name <name>         OTel service name\n\
  --service-version <version>   OTel service version\n\
  --environment <name>          Deployment environment\n\
  -h, --help                    Show help"
    );
}
