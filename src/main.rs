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

    let otlp_endpoint = obs.as_ref().and_then(|o| o.otlp_endpoint.as_deref());
    let otlp_metrics_endpoint = obs
        .as_ref()
        .and_then(|o| o.otlp_metrics_endpoint.as_deref());
    let metrics_interval_secs = obs
        .as_ref()
        .map(|o| o.otlp_metrics_interval_secs)
        .unwrap_or(30);
    let service_name = obs
        .as_ref()
        .map(|o| o.service_name.as_str())
        .unwrap_or("rustcdc-server");

    // Initialise the global tracing subscriber exactly once, including the
    // optional OTel span-exporter and metrics-exporter layers.
    // The returned guard flushes both exporters on drop.
    let _telemetry_guard = match rustcdc_server::telemetry::init_with_metrics(
        cli.log_format.as_deref(),
        cli.log_level.as_deref(),
        otlp_endpoint,
        otlp_metrics_endpoint,
        metrics_interval_secs,
        service_name,
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
