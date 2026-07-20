use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("configuration error: {0}")]
    Config(Box<ConfigError>),

    #[error("runtime error: {0}")]
    Runtime(#[from] rustcdc::core::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP client error: {0}")]
    Http(String),

    #[error("{0}")]
    Other(String),
}

impl From<ConfigError> for AppError {
    fn from(value: ConfigError) -> Self {
        Self::Config(Box::new(value))
    }
}

impl From<clap::error::Error> for AppError {
    fn from(e: clap::error::Error) -> Self {
        AppError::Other(e.to_string())
    }
}

// ── Config errors ─────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("no configuration file found; provide one via --config-file")]
    NoConfigFile,

    #[error("failed to load configuration: {0}")]
    Load(Box<figment::Error>),

    #[error("api_version must be \"v1\", got \"{0}\"")]
    InvalidApiVersion(String),

    #[error("no source configured; set exactly one [source.<connector>] block in the config file")]
    NoSourceConfigured,

    #[error("invalid source configuration: {0}")]
    InvalidSource(String),

    #[error("invalid state configuration: {0}")]
    InvalidState(String),
}

impl From<figment::Error> for ConfigError {
    fn from(value: figment::Error) -> Self {
        Self::Load(Box::new(value))
    }
}
