use std::path::Path;

use crate::{cli::ValidateConfigArgs, config, error::AppError, redaction::redact_secrets};

pub async fn execute(args: ValidateConfigArgs, config_path: Option<&Path>) -> Result<(), AppError> {
    let config_path = config_path.ok_or(crate::error::ConfigError::NoConfigFile)?;

    // `config::load` already runs validation; errors surface as `AppError`.
    let config = config::load(config_path)?;

    println!("Configuration valid (api_version={})", config.api_version);

    if args.print_json {
        let json = match serde_json::to_string_pretty(&config) {
            Ok(json) => json,
            Err(_) => {
                // Some source config fields intentionally refuse serialization
                // (for example inline secret wrappers). Fall back to the raw
                // TOML document and render that as JSON for operator output.
                let raw = std::fs::read_to_string(config_path)?;
                let as_toml: toml::Value = toml::from_str(&raw).map_err(|e| {
                    AppError::Other(format!("failed to parse TOML for --print-json: {e}"))
                })?;
                serde_json::to_string_pretty(&as_toml)
                    .map_err(|e| AppError::Other(e.to_string()))?
            }
        };
        // Always redact secrets in printed output.
        println!("{}", redact_secrets(&json));
    }

    Ok(())
}
