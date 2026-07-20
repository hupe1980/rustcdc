use rustcdc::{
    MariaDbSourceConfig, MysqlSourceConfig, PostgresSourceConfig, SecretString,
    SqlServerSourceConfig, TransportConfig,
};
use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────────────────────
// Source
// ─────────────────────────────────────────────────────────────────────────────

/// Exactly one source driver must be configured.
///
/// The `type` discriminator selects the CDC connector; all connection
/// parameters live at the same nesting level as `type`.
///
/// # TOML structure
///
/// ```toml
/// [source]
/// type = "postgres"    # postgres | mysql | mariadb | sqlserver | mssql
/// host = "localhost"
/// port = 5432
/// user = "cdc_user"
/// # ...
/// require_primary = true   # optional, default true
/// ```
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SourceConfig {
    /// When `true` (default), cdc-server validates at startup that the
    /// configured source database is a primary/writable node.
    ///
    /// Set to `false` only when you have explicitly validated replica-safety
    /// for your deployment topology.
    #[serde(default = "default_require_primary")]
    pub require_primary: bool,

    /// Source driver selection and connection parameters.
    #[serde(flatten)]
    pub driver: SourceDriver,
}

fn default_require_primary() -> bool {
    true
}

/// Tagged discriminator for the CDC source connector.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceDriver {
    /// PostgreSQL logical-replication source.
    Postgres(PostgresSourceConfig),

    /// MySQL binlog source.
    Mysql(MysqlSourceConfig),

    /// MariaDB binlog source.
    Mariadb(MariaDbSourceConfig),

    /// SQL Server CDC source.
    #[serde(alias = "mssql")]
    Sqlserver(SqlServerProfileConfig),
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct SqlServerProfileConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: SecretString,
    pub database: String,
    #[serde(default)]
    pub instance_name: Option<String>,
    pub transport: TransportConfig,
    pub conn_timeout_secs: u64,
    pub cdc_enabled: bool,
    pub cdc_schema: String,
    pub prereq_pool_size: usize,
    pub stream_poll_interval_ms: u64,
    pub max_events_per_poll: usize,
    #[serde(default)]
    pub capture_truncate_events: bool,
    #[serde(default)]
    pub table_include_list: Vec<String>,
    #[serde(default)]
    pub table_exclude_list: Vec<String>,
}

impl SqlServerProfileConfig {
    pub fn to_runtime_config(&self) -> SqlServerSourceConfig {
        SqlServerSourceConfig {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            database: self.database.clone(),
            instance_name: self.instance_name.clone(),
            transport: self.transport.clone(),
            conn_timeout_secs: self.conn_timeout_secs,
            cdc_enabled: self.cdc_enabled,
            cdc_schema: self.cdc_schema.clone(),
            prereq_pool_size: self.prereq_pool_size,
            stream_poll_interval_ms: self.stream_poll_interval_ms,
            max_events_per_poll: self.max_events_per_poll,
            capture_truncate_events: self.capture_truncate_events,
            table_include_list: self.table_include_list.clone(),
            table_exclude_list: self.table_exclude_list.clone(),
        }
    }
}
