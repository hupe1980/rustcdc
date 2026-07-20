use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Clone, Debug, ValueEnum)]
pub enum InitProfile {
    Dev,
    Prod,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CheckpointParityMode {
    /// Enable parity only for sinks that support transactional checkpoint barriers.
    Auto,
    /// Force parity mode on (warns and falls back when sink does not support barriers).
    Enabled,
    /// Disable checkpoint parity mode.
    Disabled,
}

/// Change-data-capture server powered by rustcdc.
#[derive(Debug, Parser)]
#[command(name = "rustcdc", author, about, long_about = None, disable_version_flag = true)]
pub struct Cli {
    /// Print version information and exit.
    #[arg(short = 'V', long)]
    pub version: bool,

    /// Path to the TOML configuration file.
    #[arg(short = 'c', long, global = true, value_name = "FILE")]
    pub config_file: Option<PathBuf>,

    /// Log level override (trace | debug | info | warn | error).
    #[arg(long, env = "RUSTCDC_LOG_LEVEL", global = true, value_name = "LEVEL")]
    pub log_level: Option<String>,

    /// Log format (text | json).  Defaults to `text`.
    #[arg(long, env = "RUSTCDC_LOG_FORMAT", global = true, value_name = "FORMAT")]
    pub log_format: Option<String>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Create a starter configuration file.
    Init(InitArgs),

    /// Start the CDC capture pipeline and forward events to the configured sink.
    Run(RunArgs),

    /// Load, parse, and validate the configuration file without starting the pipeline.
    ValidateConfig(ValidateConfigArgs),

    /// Query the status of a running CDC instance via its admin API.
    Status(StatusArgs),

    /// Execute a dry-run of the pipeline using `RuntimeSourceConfig::Disabled`.
    ///
    /// Useful for validating sink connectivity and event formatting without an
    /// active database connection.
    DryRun(DryRunArgs),

    /// Inspect the checkpoint and schema-history stored on disk.
    InspectCheckpoint(InspectCheckpointArgs),

    /// Replay events from a stored event file against the configured sink.
    Replay(ReplayArgs),

    /// Migrate file-backed checkpoint and schema-history state between directories.
    MigrateState(MigrateStateArgs),

    /// Initialize durable state artifacts for a configured backend.
    InitState(InitStateArgs),
}

// ── Init ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
pub struct InitArgs {
    /// Output config file path to create.
    #[arg(long, default_value = "cdc.toml", value_name = "FILE")]
    pub output: PathBuf,

    /// Replace an existing config file at --output.
    #[arg(long)]
    pub force: bool,

    /// Configuration hardening profile. Defaults to dev.
    #[arg(long, default_value = "dev", value_enum, value_name = "PROFILE")]
    pub profile: InitProfile,

    /// State directory in generated config.
    #[arg(long, default_value = "/var/lib/cdc/state", value_name = "DIR")]
    pub state_dir: PathBuf,

    /// Admin API bind address in generated config.
    #[arg(long, default_value = "127.0.0.1:8080", value_name = "ADDR")]
    pub admin_bind: String,
}

// ── Run ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
pub struct RunArgs {
    /// Override the state directory (default: value from config).
    #[arg(long, env = "RUSTCDC_STATE_DIR", value_name = "DIR")]
    pub state_dir: Option<PathBuf>,

    /// Tables to include in the initial snapshot, e.g. `public.orders`.
    /// Repeatable; overrides the config-file list.
    #[arg(long = "snapshot-table", value_name = "SCHEMA.TABLE")]
    pub snapshot_tables: Vec<String>,

    /// Checkpoint parity policy for the run loop.
    ///
    /// `auto`    — use barrier commits when the sink supports them (default).
    /// `enabled` — require barrier commits; fail at startup if the sink does
    ///             not support them AND `delivery_contract = effectively_once`.
    /// `disabled` — never use barrier commits regardless of sink capability.
    #[arg(long, default_value = "auto", value_enum, value_name = "MODE")]
    pub checkpoint_parity_mode: CheckpointParityMode,
}

// ── ValidateConfig ────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
pub struct ValidateConfigArgs {
    /// Emit the parsed (redacted) configuration as JSON.
    #[arg(long)]
    pub print_json: bool,
}

// ── Status ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Args, Default)]
pub struct AdminTlsClientArgs {
    /// Optional custom CA bundle (PEM) for admin API TLS verification.
    #[arg(long, value_name = "FILE")]
    pub admin_ca_file: Option<PathBuf>,

    /// Optional client certificate (PEM) for admin API mTLS.
    #[arg(long, value_name = "FILE")]
    pub admin_client_cert_file: Option<PathBuf>,

    /// Optional client private key (PEM) for admin API mTLS.
    #[arg(long, value_name = "FILE")]
    pub admin_client_key_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Args, Default)]
pub struct AdminAuthClientArgs {
    /// Explicit bearer token for admin read endpoints.
    #[arg(long, value_name = "TOKEN")]
    pub admin_read_token: Option<String>,

    /// Environment variable name containing the admin read bearer token.
    #[arg(long, value_name = "ENV_VAR")]
    pub admin_read_token_env: Option<String>,

    /// Explicit bearer token for admin write endpoints.
    #[arg(long, value_name = "TOKEN")]
    pub admin_write_token: Option<String>,

    /// Environment variable name containing the admin write bearer token.
    #[arg(long, value_name = "ENV_VAR")]
    pub admin_write_token_env: Option<String>,
}

#[derive(Debug, Parser)]
pub struct StatusArgs {
    /// Admin API base URL of the running instance.
    #[arg(
        long,
        env = "RUSTCDC_ADMIN_URL",
        default_value = "http://127.0.0.1:8080",
        value_name = "URL"
    )]
    pub admin_url: String,

    /// Return exit code 1 if the instance is not in `Running` state.
    #[arg(long)]
    pub require_running: bool,

    #[command(flatten)]
    pub admin_tls: AdminTlsClientArgs,

    #[command(flatten)]
    pub admin_auth: AdminAuthClientArgs,
}

// ── DryRun ────────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
pub struct DryRunArgs {
    /// Number of synthetic events to emit (default: 10).
    #[arg(long, default_value_t = 10, value_name = "N")]
    pub event_count: usize,

    /// Checkpoint parity policy for dry-run batch delivery.
    #[arg(long, default_value = "auto", value_enum, value_name = "MODE")]
    pub checkpoint_parity_mode: CheckpointParityMode,
}

// ── InspectCheckpoint ─────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
pub struct InspectCheckpointArgs {
    /// State directory to inspect.  Defaults to the config-file value.
    #[arg(long, env = "RUSTCDC_STATE_DIR", value_name = "DIR")]
    pub state_dir: Option<PathBuf>,

    /// Also print the full schema history.
    #[arg(long)]
    pub schema_history: bool,
}

// ── Replay ────────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
pub struct ReplayArgs {
    /// Path to the event replay file produced by `cdc run --replay-output`.
    #[arg(value_name = "FILE")]
    pub event_file: PathBuf,

    /// Sink type to use for replay output.
    #[arg(long, default_value = "stdout", value_enum, value_name = "SINK")]
    pub sink: ReplaySink,

    /// Stop after replaying this many events (default: replay all).
    #[arg(long, value_name = "N")]
    pub limit: Option<usize>,

    /// Reject replay files larger than this many bytes.
    #[arg(long, value_name = "BYTES")]
    pub max_file_bytes: Option<u64>,

    /// Reject any replay line larger than this many bytes.
    #[arg(long, default_value_t = 1_048_576, value_name = "BYTES")]
    pub max_line_bytes: usize,

    /// Checkpoint parity policy for replay batch delivery.
    #[arg(long, default_value = "auto", value_enum, value_name = "MODE")]
    pub checkpoint_parity_mode: CheckpointParityMode,

    /// Skip events whose source offset is strictly less than this value.
    ///
    /// Accepts a PostgreSQL WAL LSN (e.g. `A/1B2C3D4E`) or a plain hex u64.
    /// Use this when replaying into an already-partially-populated sink to
    /// avoid reprocessing events that were delivered in a prior run.
    #[arg(long, value_name = "OFFSET")]
    pub skip_before_offset: Option<String>,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum ReplaySink {
    Stdout,
    FileJsonl,
    Http,
    Kafka,
    Iceberg,
}

// ── MigrateState ─────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
pub struct MigrateStateArgs {
    /// Backend to read state from. One of: `local_fs` (default), `kafka`.
    #[arg(long, value_name = "BACKEND", default_value = "local_fs")]
    pub source_backend: String,

    /// Source state directory to copy from (required when source-backend=local_fs).
    #[arg(long, value_name = "DIR", required = false)]
    pub source_dir: Option<std::path::PathBuf>,

    /// Destination state directory to write into.
    #[arg(long, value_name = "DIR")]
    pub target_dir: std::path::PathBuf,

    /// Replace existing state files at the destination.
    #[arg(long)]
    pub overwrite: bool,

    /// Optional path to write the machine-readable migration report.
    #[arg(long, value_name = "FILE")]
    pub output: Option<PathBuf>,

    // ── Kafka source options (required when source-backend=kafka) ──────────
    /// Bootstrap broker list for Kafka source (e.g. `broker:9092`).
    #[arg(long, value_name = "BROKERS")]
    pub kafka_brokers: Option<String>,

    /// Kafka state topic to read from.
    #[arg(long, value_name = "TOPIC")]
    pub kafka_topic: Option<String>,

    /// Kafka client ID to use when scanning the state topic.
    #[arg(long, value_name = "ID", default_value = "cdc-migrate-state")]
    pub kafka_client_id: String,

    /// Request timeout in milliseconds for Kafka source reads.
    #[arg(long, value_name = "MS", default_value_t = 30000)]
    pub kafka_request_timeout_ms: u64,

    /// Poll timeout in milliseconds for the compacted-topic scan.
    #[arg(long, value_name = "MS", default_value_t = 5000)]
    pub kafka_readback_poll_timeout_ms: u64,
}

// ── InitState ────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
pub struct InitStateArgs {
    /// Replace existing bootstrap artifacts when they already exist.
    #[arg(long)]
    pub force: bool,
}
