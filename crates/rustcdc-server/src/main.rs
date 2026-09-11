// The binary this project ships contains no `unsafe`, and this enforces it.
//
// `forbid` cannot be overridden by any inner `#[allow]`, so this crate refuses to compile
// the moment `unsafe` appears anywhere in it. The library is one step weaker — `deny` with
// a single allowlisted site in `test_env`, which needs `std::env::set_var` for tests whose
// subject reads the process environment. See the note in `Cargo.toml`.
#![forbid(unsafe_code)]
// The dispatch future now nests the AWS SDK's own client stack (SQS for the dead-letter
// queue, S3 Tables for the Iceberg catalog) inside the pipeline future, and the default
// 128-deep type-layout query gives up on it. This raises the compiler's limit; it says
// nothing about runtime recursion.
#![recursion_limit = "256"]

use clap::Parser;

use rustcdc_server::cli::Cli;
use rustcdc_server::error::AppError;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const GIT_SHA: &str = env!("GIT_SHA");
const BUILD_PROFILE: &str = env!("BUILD_PROFILE");
const BUILD_TARGET: &str = env!("BUILD_TARGET");

#[tokio::main]
async fn main() {
    // Install the process-wide rustls crypto provider before anything can open a
    // TLS endpoint or connection. The dependency graph links more than one rustls
    // backend (aws-lc-rs via rustcdc, ring via transitive deps), so rustls cannot
    // pick a default on its own — without this, the first TLS use (e.g. the
    // admin server with `[admin.tls]`) panics at startup.
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        // Already installed (e.g. by an embedding test harness) — fine.
        tracing::debug!("rustls crypto provider was already installed");
    }

    let cli = Cli::parse();

    if cli.version {
        println!("rustcdc {VERSION} ({GIT_SHA}, {BUILD_PROFILE}, {BUILD_TARGET})");
        return;
    }

    // Pre-read the observability section so we can include the OTel exporter
    // in the single global subscriber initialisation call.  Config errors
    // here are non-fatal (OTel will just be disabled for that run).
    let obs = match cli.config_file.as_deref() {
        Some(path) => match rustcdc_server::config::load(path) {
            Ok(config) => Some(config.observability),
            Err(error) => {
                eprintln!(
                    "fatal: failed to load configuration {}: {}",
                    path.display(),
                    error
                );
                std::process::exit(1);
            }
        },
        None => None,
    };

    // One conversion, so every OTLP setting travels together and adding one is a struct
    // field rather than another positional argument threaded through three signatures.
    let otlp = obs
        .as_ref()
        .map(rustcdc_server::telemetry::OtlpOptions::from)
        .unwrap_or_default();

    // Initialise the global tracing subscriber exactly once, including the
    // optional OTel span-exporter and metrics-exporter layers.
    // The returned guard flushes both exporters on drop.
    let _telemetry_guard = match rustcdc_server::telemetry::init_with_metrics(
        cli.log_format.as_deref(),
        cli.log_level.as_deref(),
        &otlp,
    ) {
        Ok(g) => g,
        Err(e) => {
            eprintln!("fatal: failed to initialise telemetry: {e}");
            std::process::exit(1);
        }
    };

    let exit_code = match run(cli).await {
        Ok(()) => 0,
        Err(e) => {
            tracing::error!(error = %e, "rustcdc exited with error");
            eprintln!("error: {e}");
            1
        }
    };

    // Guard is dropped here – flushes OTel spans before the process exits.
    std::process::exit(exit_code);
}

async fn run(cli: Cli) -> Result<(), AppError> {
    let Some(command) = cli.command else {
        // Print help when invoked with no subcommand.
        <Cli as clap::CommandFactory>::command().print_help()?;
        println!();
        return Ok(());
    };

    rustcdc_server::commands::dispatch(command, cli.config_file.as_deref()).await
}
