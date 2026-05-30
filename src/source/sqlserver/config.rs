use std::fmt;

#[cfg(feature = "tls")]
use std::path::Path;

use crate::core::{Error, Result, SecretString, TransportConfig};
use crate::source::allow_insecure_test_transport;

use super::{
    SqlServerSourceConfig, DEFAULT_POOL_SIZE, DEFAULT_STREAM_POLL_INTERVAL_MS, MAX_EVENTS_PER_POLL,
};

const MAX_CONN_TIMEOUT_SECS: u64 = 300;
const MAX_PREREQ_POOL_SIZE: usize = 64;
const MAX_STREAM_POLL_INTERVAL_MS: u64 = 60_000;
const MAX_MAX_EVENTS_PER_POLL: usize = 100_000;

impl fmt::Debug for SqlServerSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("SqlServerSourceConfig");
        debug
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &"***redacted***")
            .field("database", &self.database)
            .field("instance_name", &self.instance_name)
            .field("conn_timeout_secs", &self.conn_timeout_secs)
            .field("cdc_enabled", &self.cdc_enabled)
            .field("cdc_schema", &self.cdc_schema)
            .field("prereq_pool_size", &self.prereq_pool_size)
            .field("stream_poll_interval_ms", &self.stream_poll_interval_ms)
            .field("max_events_per_poll", &self.max_events_per_poll);
        debug.field("transport", &self.transport);
        debug.finish()
    }
}

impl Default for SqlServerSourceConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 1433,
            user: String::new(),
            password: SecretString::default(),
            database: String::new(),
            instance_name: None,
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            cdc_enabled: true,
            cdc_schema: "cdc".into(),
            prereq_pool_size: DEFAULT_POOL_SIZE,
            stream_poll_interval_ms: DEFAULT_STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            table_include_list: Vec::new(),
            table_exclude_list: Vec::new(),
        }
    }
}

impl SqlServerSourceConfig {
    /// Return the connector name used by the source abstraction.
    pub const fn source_type() -> &'static str {
        "sqlserver"
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
            return Err(Error::ConfigError(
                "sqlserver host must not be empty".into(),
            ));
        }
        if self.port == 0 {
            return Err(Error::ConfigError(
                "sqlserver port must be greater than zero".into(),
            ));
        }
        if self.user.trim().is_empty() {
            return Err(Error::ConfigError(
                "sqlserver user must not be empty".into(),
            ));
        }
        if self.password.resolve()?.trim().is_empty() {
            return Err(Error::ConfigError(
                "sqlserver password must not be empty".into(),
            ));
        }
        if self.database.trim().is_empty() {
            return Err(Error::ConfigError(
                "sqlserver database must not be empty".into(),
            ));
        }
        if self.conn_timeout_secs == 0 {
            return Err(Error::ConfigError(
                "sqlserver conn_timeout_secs must be greater than zero".into(),
            ));
        }
        if self.conn_timeout_secs > MAX_CONN_TIMEOUT_SECS {
            return Err(Error::ConfigError(format!(
                "sqlserver conn_timeout_secs must be less than or equal to {MAX_CONN_TIMEOUT_SECS}"
            )));
        }
        if self.cdc_schema.trim().is_empty() {
            return Err(Error::ConfigError(
                "sqlserver cdc_schema must not be empty".into(),
            ));
        }
        if self.prereq_pool_size == 0 {
            return Err(Error::ConfigError(
                "sqlserver prereq_pool_size must be greater than zero".into(),
            ));
        }
        if self.prereq_pool_size > MAX_PREREQ_POOL_SIZE {
            return Err(Error::ConfigError(format!(
                "sqlserver prereq_pool_size must be less than or equal to {MAX_PREREQ_POOL_SIZE}"
            )));
        }
        if self.stream_poll_interval_ms == 0 {
            return Err(Error::ConfigError(
                "sqlserver stream_poll_interval_ms must be greater than zero".into(),
            ));
        }
        if self.stream_poll_interval_ms > MAX_STREAM_POLL_INTERVAL_MS {
            return Err(Error::ConfigError(format!(
                "sqlserver stream_poll_interval_ms must be less than or equal to {MAX_STREAM_POLL_INTERVAL_MS}"
            )));
        }
        if self.max_events_per_poll == 0 {
            return Err(Error::ConfigError(
                "sqlserver max_events_per_poll must be greater than zero".into(),
            ));
        }
        if self.max_events_per_poll > MAX_MAX_EVENTS_PER_POLL {
            return Err(Error::ConfigError(format!(
                "sqlserver max_events_per_poll must be less than or equal to {MAX_MAX_EVENTS_PER_POLL}"
            )));
        }
        if let TransportConfig::Tls { ca_cert_path, .. } = &self.transport {
            #[cfg(not(feature = "tls"))]
            {
                let _ = ca_cert_path;
                return Err(Error::ConfigError(
                    "sqlserver connector requires crate feature 'tls' for TLS transport".into(),
                ));
            }

            #[cfg(feature = "tls")]
            if let Some(ca_path) = ca_cert_path
                .as_deref()
                .map(str::trim)
                .filter(|path| !path.is_empty())
            {
                if !Path::new(ca_path).exists() {
                    return Err(Error::ConfigError(format!(
                        "sqlserver tls_ca_cert_path does not exist: {ca_path}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn tds_host(&self) -> String {
        match &self.instance_name {
            Some(instance) => format!("{}\\{}", self.host, instance),
            None => self.host.clone(),
        }
    }

    pub(super) fn to_tiberius_config(&self) -> Result<tiberius::Config> {
        let mut config = tiberius::Config::new();
        config.host(self.tds_host());
        config.port(self.port);
        config.database(&self.database);
        let password = self.password.resolve()?;
        config.authentication(tiberius::AuthMethod::sql_server(&self.user, &password));

        #[cfg(feature = "tls")]
        if self.transport.is_tls() {
            if allow_insecure_test_transport() {
                config.encryption(tiberius::EncryptionLevel::NotSupported);
            } else {
                config.encryption(tiberius::EncryptionLevel::Required);

                if let Some(ca_path) = self
                    .transport
                    .ca_cert_path()
                    .as_ref()
                    .map(|path| path.trim())
                    .filter(|path| !path.is_empty())
                {
                    config.trust_cert_ca(ca_path);
                }
            }
        } else {
            config.encryption(tiberius::EncryptionLevel::NotSupported);
        }

        #[cfg(not(feature = "tls"))]
        config.encryption(tiberius::EncryptionLevel::NotSupported);

        Ok(config)
    }
}
