#![cfg_attr(not(feature = "mariadb"), allow(dead_code, unused_imports))]

// Phase 1 embedding example:
// - Config can be supplied via CLI flags or CDC_RS_* environment variables.
// - Events are printed as JSON lines to stdout.
// - Every delivered batch is acknowledged with an opaque runtime token.
// - Ctrl+C triggers graceful shutdown and final commit.

#[cfg(feature = "mariadb")]
use std::{env, io::Write, path::PathBuf, sync::Arc};

#[cfg(feature = "mariadb")]
use rustcdc::{
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory, CdcRuntime,
    MariaDbSourceConfig, RuntimeConfig, RuntimeObservability, RuntimeSourceConfig,
    StructuredLogger,
};

#[cfg(feature = "mariadb")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> rustcdc::Result<()> {
    let args = ExampleArgs::from_env_and_args()?;

    let mut source = MariaDbSourceConfig::default()
        .with_host(args.host)
        .with_port(args.port)
        .with_user(args.user)
        .with_database(args.database);
    source.server_id = args.server_id;

    std::fs::create_dir_all(&args.checkpoint_dir).map_err(rustcdc::Error::IoError)?;

    let mut runtime = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::mariadb(source),
            FileCheckpoint::new(args.checkpoint_dir),
            InMemorySchemaHistory::default(),
        )
        .with_snapshot_tables(args.snapshot_tables)
        .with_max_buffer_size(args.max_buffer_size)
        .with_max_poll_wait_ms(args.poll_wait_ms)
        .with_observability(
            RuntimeObservability::default()
                .with_tracer(Arc::new(rustcdc::NoOpEventTracer))
                .with_metrics(Arc::new(rustcdc::NoOpMetricsCollector)),
        ),
    )?;

    runtime.start().await?;

    let logger = StructuredLogger::new("mariadb");
    logger.stream_started("example");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                let _ = runtime.stop().await?;
                logger.source_disconnected();
                break;
            }
            polled = runtime.poll_event_batch() => {
                let batch = polled?;
                if batch.is_empty() {
                    continue;
                }
                let token = batch.ack_token().ok_or_else(|| {
                    rustcdc::Error::StateError("runtime returned non-empty batch without ack token".into())
                })?;

                for event in batch.events() {
                    println!("{}", event.to_json()?);
                    std::io::stdout().flush().map_err(rustcdc::Error::IoError)?;
                }

                runtime.commit_ack(token).await?;
            }
        }
    }

    Ok(())
}

#[cfg(not(feature = "mariadb"))]
fn main() {
    eprintln!(
        "mariadb_to_stdout requires the mariadb feature. Run with: cargo run --example mariadb_to_stdout --features mariadb"
    );
}

#[cfg(feature = "mariadb")]
#[derive(Debug, Clone)]
struct ExampleArgs {
    host: String,
    port: u16,
    user: String,
    database: String,
    server_id: u32,
    snapshot_tables: Vec<String>,
    checkpoint_dir: PathBuf,
    max_buffer_size: usize,
    poll_wait_ms: u64,
}

#[cfg(feature = "mariadb")]
impl ExampleArgs {
    fn from_env_and_args() -> rustcdc::Result<Self> {
        let mut out = Self {
            host: env_or_default("CDC_RS_HOST", "localhost"),
            port: env_or_default("CDC_RS_PORT", "3306")
                .parse::<u16>()
                .map_err(|error| {
                    rustcdc::Error::ConfigError(format!("invalid CDC_RS_PORT: {error}"))
                })?,
            user: env_or_default("CDC_RS_USER", "cdc_user"),
            database: env_or_default("CDC_RS_DB", "events"),
            server_id: env_or_default("CDC_RS_SERVER_ID", "5401")
                .parse::<u32>()
                .map_err(|error| {
                    rustcdc::Error::ConfigError(format!("invalid CDC_RS_SERVER_ID: {error}"))
                })?,
            snapshot_tables: env_or_default("CDC_RS_SNAPSHOT_TABLES", "public.users")
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect(),
            checkpoint_dir: PathBuf::from(env_or_default(
                "CDC_RS_CHECKPOINT_DIR",
                "./target/rustcdc-checkpoints",
            )),
            max_buffer_size: env_or_default("CDC_RS_MAX_BUFFER_SIZE", "1000")
                .parse::<usize>()
                .map_err(|error| {
                    rustcdc::Error::ConfigError(format!("invalid CDC_RS_MAX_BUFFER_SIZE: {error}"))
                })?,
            poll_wait_ms: env_or_default("CDC_RS_POLL_WAIT_MS", "500")
                .parse::<u64>()
                .map_err(|error| {
                    rustcdc::Error::ConfigError(format!("invalid CDC_RS_POLL_WAIT_MS: {error}"))
                })?,
        };

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--host" => out.host = next_value(&mut args, "--host")?,
                "--port" => {
                    out.port = next_value(&mut args, "--port")?
                        .parse::<u16>()
                        .map_err(|error| {
                            rustcdc::Error::ConfigError(format!("invalid --port: {error}"))
                        })?
                }
                "--user" => out.user = next_value(&mut args, "--user")?,
                "--db" | "--database" => out.database = next_value(&mut args, "--database")?,
                "--server-id" => {
                    out.server_id = next_value(&mut args, "--server-id")?
                        .parse::<u32>()
                        .map_err(|error| {
                            rustcdc::Error::ConfigError(format!("invalid --server-id: {error}"))
                        })?
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
                "--max-buffer-size" => {
                    out.max_buffer_size = next_value(&mut args, "--max-buffer-size")?
                        .parse::<usize>()
                        .map_err(|error| {
                            rustcdc::Error::ConfigError(format!(
                                "invalid --max-buffer-size: {error}"
                            ))
                        })?
                }
                "--poll-wait-ms" => {
                    out.poll_wait_ms = next_value(&mut args, "--poll-wait-ms")?
                        .parse::<u64>()
                        .map_err(|error| {
                            rustcdc::Error::ConfigError(format!("invalid --poll-wait-ms: {error}"))
                        })?
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => {
                    return Err(rustcdc::Error::ConfigError(format!(
                        "unknown argument: {other}"
                    )));
                }
            }
        }

        Ok(out)
    }
}

#[cfg(feature = "mariadb")]
fn env_or_default(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

#[cfg(feature = "mariadb")]
fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> rustcdc::Result<String> {
    args.next()
        .ok_or_else(|| rustcdc::Error::ConfigError(format!("missing value for {flag}")))
}

#[cfg(feature = "mariadb")]
fn print_help() {
    eprintln!("Usage: mariadb_to_stdout [--host HOST] [--port PORT] [--user USER] [--db DATABASE]");
    eprintln!("                        [--server-id ID] [--snapshot-tables S1,S2]");
    eprintln!(
        "                        [--checkpoint-dir DIR] [--max-buffer-size N] [--poll-wait-ms MS]"
    );
}
