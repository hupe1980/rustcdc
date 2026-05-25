use std::fmt;

#[cfg(feature = "tls")]
use std::path::Path;

#[cfg(feature = "tls")]
use mysql_async::SslOpts;

use crate::core::{Error, Result, SecretString, TransportConfig};

use super::{MysqlSourceConfig, MAX_EVENTS_PER_POLL, STREAM_POLL_INTERVAL_MS};

const MAX_CONN_TIMEOUT_SECS: u64 = 300;
const MAX_STREAM_POLL_INTERVAL_MS: u64 = 60_000;
const MAX_MAX_EVENTS_PER_POLL: usize = 100_000;

fn allow_insecure_test_transport() -> bool {
    #[cfg(feature = "insecure-test-overrides")]
    {
        return std::env::var("CDC_RS_ALLOW_INSECURE_TEST_TRANSPORT").as_deref() == Ok("1");
    }

    #[cfg(not(feature = "insecure-test-overrides"))]
    {
        false
    }
}

impl fmt::Debug for MysqlSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("MysqlSourceConfig");
        debug
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &"***redacted***")
            .field("database", &self.database)
            .field("server_id", &self.server_id)
            .field("gtid_mode_enabled", &self.gtid_mode_enabled)
            .field("binlog_format_check", &self.binlog_format_check)
            .field("conn_timeout_secs", &self.conn_timeout_secs)
            .field("stream_poll_interval_ms", &self.stream_poll_interval_ms)
            .field("max_events_per_poll", &self.max_events_per_poll);
        debug.field("transport", &self.transport);
        debug.finish()
    }
}

impl Default for MysqlSourceConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 3306,
            user: String::new(),
            password: SecretString::default(),
            database: String::new(),
            server_id: 1,
            gtid_mode_enabled: false,
            binlog_format_check: true,
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
        }
    }
}

impl MysqlSourceConfig {
    /// Return the connector name used by the source abstraction.
    pub const fn source_type() -> &'static str {
        "mysql"
    }

    /// Validate configuration values before a connection attempt.
    pub fn validate(&self) -> Result<()> {
        if self.host.trim().is_empty() {
            return Err(Error::ConfigError("mysql host must not be empty".into()));
        }
        if self.port == 0 {
            return Err(Error::ConfigError(
                "mysql port must be greater than zero".into(),
            ));
        }
        if self.user.trim().is_empty() {
            return Err(Error::ConfigError("mysql user must not be empty".into()));
        }
        if self.password.resolve()?.trim().is_empty() {
            return Err(Error::ConfigError(
                "mysql password must not be empty".into(),
            ));
        }
        if self.database.trim().is_empty() {
            return Err(Error::ConfigError(
                "mysql database must not be empty".into(),
            ));
        }
        if self.server_id == 0 {
            return Err(Error::ConfigError(
                "mysql server_id must be greater than zero".into(),
            ));
        }
        if self.conn_timeout_secs == 0 {
            return Err(Error::ConfigError(
                "mysql conn_timeout_secs must be greater than zero".into(),
            ));
        }
        if self.conn_timeout_secs > MAX_CONN_TIMEOUT_SECS {
            return Err(Error::ConfigError(format!(
                "mysql conn_timeout_secs must be less than or equal to {MAX_CONN_TIMEOUT_SECS}"
            )));
        }
        if self.stream_poll_interval_ms == 0 {
            return Err(Error::ConfigError(
                "mysql stream_poll_interval_ms must be greater than zero".into(),
            ));
        }
        if self.stream_poll_interval_ms > MAX_STREAM_POLL_INTERVAL_MS {
            return Err(Error::ConfigError(format!(
                "mysql stream_poll_interval_ms must be less than or equal to {MAX_STREAM_POLL_INTERVAL_MS}"
            )));
        }
        if self.max_events_per_poll == 0 {
            return Err(Error::ConfigError(
                "mysql max_events_per_poll must be greater than zero".into(),
            ));
        }
        if self.max_events_per_poll > MAX_MAX_EVENTS_PER_POLL {
            return Err(Error::ConfigError(format!(
                "mysql max_events_per_poll must be less than or equal to {MAX_MAX_EVENTS_PER_POLL}"
            )));
        }
        if let TransportConfig::Tls { ca_cert_path } = &self.transport {
            #[cfg(not(feature = "tls"))]
            {
                let _ = ca_cert_path;
                return Err(Error::ConfigError(
                    "mysql connector requires crate feature 'tls' for TLS transport".into(),
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
                        "mysql tls_ca_cert_path does not exist: {ca_path}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Build typed options for connection pooling.
    pub(super) fn build_pool_opts(&self) -> Result<mysql_async::Opts> {
        let builder = mysql_async::OptsBuilder::default()
            .ip_or_hostname(self.host.clone())
            .tcp_port(self.port)
            .user(Some(self.user.clone()))
            .pass(Some(self.password.resolve()?))
            .db_name(Some(self.database.clone()));

        #[cfg(feature = "tls")]
        let builder = if self.transport.is_tls() && !allow_insecure_test_transport() {
            let ssl_opts = self.build_ssl_opts()?;
            builder.ssl_opts(Some(ssl_opts))
        } else {
            builder
        };

        Ok(mysql_async::Opts::from(builder))
    }

    #[cfg(feature = "tls")]
    fn build_ssl_opts(&self) -> Result<SslOpts> {
        let mut ssl_opts = SslOpts::default();

        // Local integration containers often use ephemeral self-signed certs.
        // Keep secure defaults. Explicit test overrides require compile-time opt-in.
        #[cfg(feature = "insecure-test-overrides")]
        if std::env::var("CDC_RS_ALLOW_INSECURE_TEST_TLS").as_deref() == Ok("1") {
            ssl_opts = ssl_opts
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true);
        }

        if let Some(ca_path) = self.transport.ca_cert_path() {
            let ca_bytes = std::fs::read(ca_path).map_err(|error| {
                Error::ConfigError(format!(
                    "failed to read mysql tls_ca_cert_path '{}': {error}",
                    ca_path
                ))
            })?;
            ssl_opts = ssl_opts.with_root_certs(vec![ca_bytes.into()]);
        }

        Ok(ssl_opts)
    }
}
