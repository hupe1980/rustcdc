#![cfg_attr(not(feature = "postgres"), allow(dead_code, unused_imports))]

// Phase 1 embedding example:
// - Config can be supplied via CLI flags or CDC_RS_* environment variables.
// - Events are printed as JSON lines to stdout.
// - Every delivered batch is acknowledged with an opaque runtime token.
// - Ctrl+C triggers graceful shutdown and final commit.

#[cfg(feature = "postgres")]
use std::{env, io::Write, path::PathBuf, sync::Arc};

#[cfg(feature = "postgres")]
use rustcdc::{
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory, CdcRuntime,
    PostgresSourceConfig, RuntimeConfig, RuntimeObservability, RuntimeSourceConfig,
    StructuredLogger,
};

#[cfg(feature = "postgres")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> rustcdc::Result<()> {
    let args = ExampleArgs::from_env_and_args()?;

    let source = PostgresSourceConfig {
        host: args.host,
        port: args.port,
        user: args.user,
        password: args.password.into(),
        database: args.database,
        replication_slot_name: args.replication_slot,
        publication_name: args.publication,
        conn_timeout_secs: args.conn_timeout_secs,
        ..PostgresSourceConfig::default()
    };

    std::fs::create_dir_all(&args.checkpoint_dir).map_err(rustcdc::Error::IoError)?;

    let mut runtime = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Postgres(source),
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

    let logger = StructuredLogger::new("postgres");
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

#[cfg(not(feature = "postgres"))]
fn main() {
    eprintln!(
        "pg_to_stdout requires the postgres feature. Run with: cargo run --example pg_to_stdout --features postgres"
    );
}

#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
struct ExampleArgs {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    replication_slot: String,
    publication: String,
    snapshot_tables: Vec<String>,
    checkpoint_dir: PathBuf,
    max_buffer_size: usize,
    poll_wait_ms: u64,
    conn_timeout_secs: u64,
}

#[cfg(feature = "postgres")]
impl ExampleArgs {
    fn from_env_and_args() -> rustcdc::Result<Self> {
        let mut out = Self {
            host: env_or_default("CDC_RS_HOST", "localhost"),
            port: env_or_default("CDC_RS_PORT", "5432")
                .parse::<u16>()
                .map_err(|error| {
                    rustcdc::Error::ConfigError(format!("invalid CDC_RS_PORT: {error}"))
                })?,
            user: env_or_default("CDC_RS_USER", "postgres"),
            password: env_or_default("CDC_RS_PASSWORD", "postgres"),
            database: env_or_default("CDC_RS_DB", "postgres"),
            replication_slot: env_or_default("CDC_RS_SLOT", "rustcdc_example_slot"),
            publication: env_or_default("CDC_RS_PUBLICATION", "rustcdc_example_pub"),
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
            conn_timeout_secs: env_or_default("CDC_RS_CONN_TIMEOUT_SECS", "30")
                .parse::<u64>()
                .map_err(|error| {
                    rustcdc::Error::ConfigError(format!(
                        "invalid CDC_RS_CONN_TIMEOUT_SECS: {error}"
                    ))
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
                "--password" => out.password = next_value(&mut args, "--password")?,
                "--db" | "--database" => out.database = next_value(&mut args, "--database")?,
                "--slot" => out.replication_slot = next_value(&mut args, "--slot")?,
                "--publication" => out.publication = next_value(&mut args, "--publication")?,
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
                "--conn-timeout-secs" => {
                    out.conn_timeout_secs = next_value(&mut args, "--conn-timeout-secs")?
                        .parse::<u64>()
                        .map_err(|error| {
                            rustcdc::Error::ConfigError(format!(
                                "invalid --conn-timeout-secs: {error}"
                            ))
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

#[cfg(feature = "postgres")]
fn env_or_default(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

#[cfg(feature = "postgres")]
fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> rustcdc::Result<String> {
    args.next()
        .ok_or_else(|| rustcdc::Error::ConfigError(format!("missing value for {flag}")))
}

#[cfg(feature = "postgres")]
fn print_help() {
    println!(
        "pg_to_stdout (Phase 1 example)\n\n\
Usage:\n  pg_to_stdout [options]\n\n\
Options:\n\
  --host <host>                  PostgreSQL host (default: localhost)\n\
  --port <port>                  PostgreSQL port (default: 5432)\n\
  --user <user>                  PostgreSQL user (default: postgres)\n\
  --password <password>          PostgreSQL password (default: postgres)\n\
  --database <db>                PostgreSQL database (default: postgres)\n\
  --slot <name>                  Replication slot (default: rustcdc_example_slot)\n\
  --publication <name>           Publication name (default: rustcdc_example_pub)\n\
  --snapshot-tables <csv>        Snapshot table list (default: public.users)\n\
  --checkpoint-dir <path>        Checkpoint directory (default: ./target/rustcdc-checkpoints)\n\
  --max-buffer-size <n>          Runtime max buffer size (default: 1000)\n\
  --poll-wait-ms <ms>            Poll timeout in milliseconds (default: 500)\n\
  --conn-timeout-secs <secs>     Postgres connect timeout seconds (default: 30)\n\
  -h, --help                     Show help\n\n\
Environment variable equivalents are also supported:\n\
  CDC_RS_HOST, CDC_RS_PORT, CDC_RS_USER, CDC_RS_PASSWORD, CDC_RS_DB,\n\
  CDC_RS_SLOT, CDC_RS_PUBLICATION, CDC_RS_SNAPSHOT_TABLES, CDC_RS_CHECKPOINT_DIR,\n\
    CDC_RS_MAX_BUFFER_SIZE, CDC_RS_POLL_WAIT_MS, CDC_RS_CONN_TIMEOUT_SECS"
    );
}
