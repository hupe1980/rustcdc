#[cfg(feature = "postgres")]
use rustcdc::PostgresSourceConfig;
#[cfg(feature = "mysql")]
use rustcdc::{MariaDbSourceConfig, MysqlSourceConfig};
#[cfg(feature = "sqlserver")]
use rustcdc::{SecretString, SqlServerSourceConfig, TransportConfig};
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
///
/// # Variants are compiled in, not always present
///
/// Each variant is gated on the cdc-server feature that provides its connector, because
/// the config types themselves come from `rustcdc` behind the same gates. A binary built
/// without `--features sqlserver` has no `Sqlserver` variant at all.
///
/// That would ordinarily make a config naming an uncompiled connector fail with serde's
/// `unknown variant \`sqlserver\`, expected \`postgres\`` — accurate and useless, since it
/// reads as "that connector does not exist" rather than "this binary was not built with
/// it". [`crate::config::loader::reject_uncompiled_source_driver`] inspects the `type`
/// discriminator before deserialization and produces the actionable message instead.
/// Every known driver name is listed there, whether or not it is compiled in, so the two
/// lists cannot drift apart silently — `every_source_driver_is_known_to_the_gate` fails
/// if a variant is added here without a matching entry.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceDriver {
    /// PostgreSQL logical-replication source.
    #[cfg(feature = "postgres")]
    Postgres(PostgresSourceConfig),

    /// MySQL binlog source.
    #[cfg(feature = "mysql")]
    Mysql(MysqlSourceConfig),

    /// MariaDB binlog source.
    #[cfg(feature = "mysql")]
    Mariadb(MariaDbSourceConfig),

    /// SQL Server CDC source.
    #[cfg(feature = "sqlserver")]
    #[serde(alias = "mssql")]
    Sqlserver(SqlServerProfileConfig),
}

/// Every source driver this project knows about, compiled in or not.
///
/// `(config name, aliases, cargo feature, compiled in this build)`. The single place the
/// two lists — what serde can parse and what the operator can ask for — are reconciled.
pub const KNOWN_SOURCE_DRIVERS: &[SourceDriverEntry] = &[
    SourceDriverEntry {
        name: "postgres",
        aliases: &[],
        feature: "postgres",
        compiled: cfg!(feature = "postgres"),
    },
    SourceDriverEntry {
        name: "mysql",
        aliases: &[],
        feature: "mysql",
        compiled: cfg!(feature = "mysql"),
    },
    SourceDriverEntry {
        name: "mariadb",
        aliases: &[],
        feature: "mysql",
        compiled: cfg!(feature = "mysql"),
    },
    SourceDriverEntry {
        name: "sqlserver",
        aliases: &["mssql"],
        feature: "sqlserver",
        compiled: cfg!(feature = "sqlserver"),
    },
];

/// One row of [`KNOWN_SOURCE_DRIVERS`].
#[derive(Debug, Clone, Copy)]
pub struct SourceDriverEntry {
    /// The `type` value in the config file.
    pub name: &'static str,
    /// Accepted spellings that are not the canonical name.
    pub aliases: &'static [&'static str],
    /// The cdc-server cargo feature that compiles this connector in.
    pub feature: &'static str,
    /// Whether *this* binary has it.
    pub compiled: bool,
}

impl SourceDriverEntry {
    /// Does `value` name this driver, under any accepted spelling?
    pub fn matches(&self, value: &str) -> bool {
        self.name == value || self.aliases.contains(&value)
    }
}

#[cfg(feature = "sqlserver")]
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

#[cfg(feature = "sqlserver")]
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
