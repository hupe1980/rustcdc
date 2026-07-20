use std::fmt;

#[cfg(feature = "tls")]
use std::path::Path;

use crate::core::{Error, Result, SecretString, TransportConfig};

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
        debug.field("capture_truncate_events", &self.capture_truncate_events);
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
            capture_truncate_events: false,
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

    /// Validate structural configuration values.
    ///
    /// Does **not** resolve deferred secrets. Password presence for
    /// provider-backed or callback-backed secrets is verified at connect time.
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
        if let Ok(pw) = self.password.expose_secret() {
            if pw.trim().is_empty() {
                return Err(Error::ConfigError(
                    "sqlserver password must not be empty".into(),
                ));
            }
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
        self.transport.warn_if_insecure("sqlserver");
        let mut config = tiberius::Config::new();
        config.host(self.tds_host());
        config.port(self.port);
        config.database(&self.database);
        let password = self.password.resolve()?;
        config.authentication(tiberius::AuthMethod::sql_server(&self.user, &password));

        #[cfg(feature = "tls")]
        if self.transport.is_tls() {
            config.encryption(tiberius::EncryptionLevel::Required);

            // See the equivalent guard in the MySQL connector: tiberius builds its own
            // TLS stack and cannot consume a pre-built `rustls::ClientConfig`. Ignoring
            // one would silently discard a pinning verifier or HSM-backed client
            // certificate while `is_tls()` continues to report `true`.
            if matches!(self.transport, TransportConfig::RustlsConfig { .. }) {
                return Err(Error::ConfigError(
                    "sqlserver transport was given a pre-built rustls ClientConfig \
                     (TransportConfig::RustlsConfig), but the SQL Server driver constructs its \
                     own TLS stack and cannot consume one. Applying it is impossible, and \
                     ignoring it would silently discard whatever the config carries — commonly \
                     a certificate-pinning verifier or an HSM-backed client certificate — while \
                     still reporting TLS as enabled. Use TransportConfig::tls() or \
                     tls_with_ca_cert_path(...) for SQL Server instead."
                        .into(),
                ));
            }

            // `allow_invalid_hostnames` must NOT reach `trust_cert()`.
            //
            // tiberius' `trust_cert()` disables the *entire* certificate chain check,
            // not just SAN/hostname matching. Treating hostname relaxation as implying
            // full trust silently escalates a narrow, common accommodation (AG
            // listeners, `host\instance` names, IP-based connections) into "accept any
            // certificate from anyone" — and, via the `else if` below, also discards the
            // operator's configured `ca_cert_path`. Since tiberius cannot express
            // hostname-only relaxation, reject the combination loudly instead of
            // widening it.
            if self.transport.allow_invalid_hostnames()
                && !self.transport.allow_invalid_certificates()
            {
                return Err(Error::ConfigError(
                    "sqlserver transport sets allow_invalid_hostnames without \
                     allow_invalid_certificates, but the SQL Server driver cannot skip \
                     hostname verification while still validating the certificate chain. \
                     Honouring this as requested is impossible; applying it would silently \
                     disable chain validation entirely and ignore any configured \
                     ca_cert_path. Either fix the certificate's SAN to match the \
                     connection host, or set allow_invalid_certificates = true to \
                     acknowledge that all certificate validation is disabled."
                        .into(),
                ));
            }

            if self.transport.allow_invalid_certificates() {
                config.trust_cert();
            } else if let Some(ca_path) = self
                .transport
                .ca_cert_path()
                .as_ref()
                .map(|path| path.trim())
                .filter(|path| !path.is_empty())
            {
                config.trust_cert_ca(ca_path);
            }
        } else {
            config.encryption(tiberius::EncryptionLevel::NotSupported);
        }

        #[cfg(not(feature = "tls"))]
        config.encryption(tiberius::EncryptionLevel::NotSupported);

        Ok(config)
    }

    /// Verify that the target SQL Server instance is currently acting as a primary
    /// (i.e. is not a read-only AG secondary or log-shipping replica).
    ///
    /// Issues a single query against `sys.dm_hadr_database_replica_states` and
    /// `sys.databases` to determine whether the current database is in a writable
    /// primary role. Falls back to checking `DATABASEPROPERTYEX(DB_NAME(), 'Updateability')`.
    ///
    /// Returns `Ok(true)` if the instance is a writable primary, `Ok(false)` if it
    /// is a read-only replica.
    ///
    /// CDC requires a writable primary. Call this before creating or resuming a
    /// capture session whenever topology changes are possible (e.g. AG failover).
    ///
    /// # Errors
    ///
    /// Returns [`Error::SourceError`] if the connection or query fails.
    pub async fn check_is_primary(&self) -> Result<bool> {
        use super::query::connect_client;

        let mut client = connect_client(self)
            .await
            .map_err(|e| Error::SourceError(format!("check_is_primary: {e}")))?;

        // DATABASEPROPERTYEX returns 'READ_WRITE' for primaries and 'READ_ONLY' for secondaries.
        let row = client
            .query("SELECT DATABASEPROPERTYEX(DB_NAME(), 'Updateability')", &[])
            .await
            .map_err(|e| Error::SourceError(format!("check_is_primary: query failed: {e}")))?
            .into_row()
            .await
            .map_err(|e| Error::SourceError(format!("check_is_primary: reading row failed: {e}")))?
            .ok_or_else(|| {
                Error::SourceError("check_is_primary: DATABASEPROPERTYEX returned no rows".into())
            })?;

        let updateability: &str = row.get(0).ok_or_else(|| {
            Error::SourceError("check_is_primary: could not read Updateability column".into())
        })?;

        Ok(updateability.eq_ignore_ascii_case("READ_WRITE"))
    }
}
