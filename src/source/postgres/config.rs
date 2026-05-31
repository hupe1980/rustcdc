use std::{fmt, time::Duration};

#[cfg(feature = "tls")]
use std::path::Path;

use tokio_postgres::Config as PgConnectConfig;

use crate::core::{Error, Result, SecretString, TransportConfig};

use super::{DatabaseAuthMode, PostgresSourceConfig, MAX_EVENTS_PER_POLL, STREAM_POLL_INTERVAL_MS};

const MAX_CONN_TIMEOUT_SECS: u64 = 300;
const MAX_STREAM_POLL_INTERVAL_MS: u64 = 60_000;
const MAX_MAX_EVENTS_PER_POLL: usize = 100_000;

impl fmt::Debug for PostgresSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("PostgresSourceConfig");
        debug
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &"***redacted***")
            .field("auth_mode", &self.auth_mode)
            .field("database", &self.database)
            .field("replication_slot_name", &self.replication_slot_name)
            .field("publication_name", &self.publication_name)
            .field("conn_timeout_secs", &self.conn_timeout_secs)
            .field("stream_poll_interval_ms", &self.stream_poll_interval_ms)
            .field("max_events_per_poll", &self.max_events_per_poll);
        debug.field("transport", &self.transport);
        debug.finish()
    }
}

impl Default for PostgresSourceConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 5432,
            user: String::new(),
            password: SecretString::default(),
            auth_mode: DatabaseAuthMode::Password,
            database: String::new(),
            replication_slot_name: String::new(),
            publication_name: String::new(),
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            table_include_list: Vec::new(),
            table_exclude_list: Vec::new(),
        }
    }
}

impl PostgresSourceConfig {
    /// Return the connector name used by the source abstraction.
    pub const fn source_type() -> &'static str {
        "postgres"
    }

    /// Enable AWS IAM token-based database authentication mode.
    #[must_use]
    pub fn with_aws_iam_auth(mut self) -> Self {
        self.auth_mode = DatabaseAuthMode::AwsIamToken;
        self
    }

    /// Enable static password database authentication mode.
    #[must_use]
    pub fn with_password_auth(mut self) -> Self {
        self.auth_mode = DatabaseAuthMode::Password;
        self
    }

    /// Set plaintext transport explicitly.
    #[must_use]
    pub fn with_plaintext_transport(mut self) -> Self {
        self.transport = TransportConfig::plaintext();
        self
    }

    /// Set TLS transport explicitly.
    #[must_use]
    pub fn with_tls_transport(mut self) -> Self {
        self.transport = TransportConfig::tls();
        self
    }

    /// Validate configuration values before a connection attempt.
    pub fn validate(&self) -> Result<()> {
        if self.host.trim().is_empty() {
            return Err(Error::ConfigError("postgres host must not be empty".into()));
        }
        if self.port == 0 {
            return Err(Error::ConfigError(
                "postgres port must be greater than zero".into(),
            ));
        }
        if self.user.trim().is_empty() {
            return Err(Error::ConfigError("postgres user must not be empty".into()));
        }
        if self.password.resolve()?.trim().is_empty() {
            return Err(Error::ConfigError(
                "postgres password must not be empty".into(),
            ));
        }
        if matches!(self.auth_mode, DatabaseAuthMode::AwsIamToken) && !self.transport.is_tls() {
            return Err(Error::ConfigError(
                "postgres auth_mode=aws_iam_token requires TLS transport".into(),
            ));
        }
        if self.database.trim().is_empty() {
            return Err(Error::ConfigError(
                "postgres database must not be empty".into(),
            ));
        }
        if self.replication_slot_name.trim().is_empty() {
            return Err(Error::ConfigError(
                "postgres replication_slot_name must not be empty".into(),
            ));
        }
        if self.publication_name.trim().is_empty() {
            return Err(Error::ConfigError(
                "postgres publication_name must not be empty".into(),
            ));
        }
        if self.conn_timeout_secs == 0 {
            return Err(Error::ConfigError(
                "postgres conn_timeout_secs must be greater than zero".into(),
            ));
        }
        if self.conn_timeout_secs > MAX_CONN_TIMEOUT_SECS {
            return Err(Error::ConfigError(format!(
                "postgres conn_timeout_secs must be less than or equal to {MAX_CONN_TIMEOUT_SECS}"
            )));
        }
        if self.stream_poll_interval_ms == 0 {
            return Err(Error::ConfigError(
                "postgres stream_poll_interval_ms must be greater than zero".into(),
            ));
        }
        if self.stream_poll_interval_ms > MAX_STREAM_POLL_INTERVAL_MS {
            return Err(Error::ConfigError(format!(
                "postgres stream_poll_interval_ms must be less than or equal to {MAX_STREAM_POLL_INTERVAL_MS}"
            )));
        }
        if self.max_events_per_poll == 0 {
            return Err(Error::ConfigError(
                "postgres max_events_per_poll must be greater than zero".into(),
            ));
        }
        if self.max_events_per_poll > MAX_MAX_EVENTS_PER_POLL {
            return Err(Error::ConfigError(format!(
                "postgres max_events_per_poll must be less than or equal to {MAX_MAX_EVENTS_PER_POLL}"
            )));
        }

        if let TransportConfig::Tls {
            ca_cert_path,
            client_cert_path,
            client_key_path,
            ..
        } = &self.transport
        {
            #[cfg(not(feature = "tls"))]
            {
                let _ = (ca_cert_path, client_cert_path, client_key_path);
                return Err(Error::ConfigError(
                    "postgres connector requires crate feature 'tls' for TLS transport".into(),
                ));
            }

            #[cfg(feature = "tls")]
            {
                let _ = (client_cert_path, client_key_path);
                if let Some(ca_path) = ca_cert_path
                    .as_deref()
                    .map(str::trim)
                    .filter(|path| !path.is_empty())
                {
                    if !Path::new(ca_path).exists() {
                        return Err(Error::ConfigError(format!(
                            "postgres tls_ca_cert_path does not exist: {ca_path}"
                        )));
                    }
                }
                match (client_cert_path.as_deref(), client_key_path.as_deref()) {
                    (Some(_), None) => {
                        return Err(Error::ConfigError(
                            "postgres mTLS: client_cert_path requires client_key_path".into(),
                        ));
                    }
                    (None, Some(_)) => {
                        return Err(Error::ConfigError(
                            "postgres mTLS: client_key_path requires client_cert_path".into(),
                        ));
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// Whether TLS transport is requested by the config.
    pub const fn tls_enabled(&self) -> bool {
        self.transport.is_tls()
    }

    pub(super) fn build_connect_config(&self) -> Result<PgConnectConfig> {
        let mut config = PgConnectConfig::new();
        config
            .host(&self.host)
            .port(self.port)
            .user(&self.user)
            .password(self.password.resolve()?)
            .dbname(&self.database)
            .connect_timeout(Duration::from_secs(self.conn_timeout_secs));
        Ok(config)
    }
}
