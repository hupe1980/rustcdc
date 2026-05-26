//! MariaDB CDC facade — thin wrapper over the MySQL connector.
//!
//! MariaDB uses the same binlog wire protocol as MySQL and is supported by
//! the same `mysql_async` driver. This module provides a `MariaDbSourceConfig`
//! newtype and type aliases so that MariaDB users have a first-class,
//! self-describing API without duplicating any implementation.
//!
//! The only behavioural difference from `MysqlSourceConfig` is that
//! `server_flavor` defaults to `ServerFlavor::MariaDb`, which means:
//! - `source_type()` returns `"mariadb"` instead of `"mysql"`.
//! - Checkpoint files are named `checkpoint_mariadb.json` (separate namespace).
//! - Structured log labels use `"mariadb"` as the source identifier.
//!
//! Enable the `mariadb` Cargo feature to make this module available. The
//! feature is an alias for `mysql` — no extra compilation cost is incurred.

use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use super::mysql::incremental_snapshot::MysqlIncrementalSnapshotHandle;
use super::mysql::{
    MysqlConnection, MysqlSnapshotHandle, MysqlSourceConfig, MysqlStreamHandle, ServerFlavor,
};

/// Configuration for a MariaDB CDC connection.
///
/// A thin newtype over [`MysqlSourceConfig`] with `server_flavor` defaulting to
/// `ServerFlavor::MariaDb`. All fields are identical; access them via `Deref` or
/// destructure via [`MariaDbSourceConfig::into_inner`].
///
/// # Example
///
/// ```rust,no_run
/// # #[cfg(all(feature = "mysql", feature = "mariadb"))]
/// # {
/// use rustcdc::MariaDbSourceConfig;
///
/// let config = MariaDbSourceConfig::default()
///     .with_host("mariadb.example.com")
///     .with_port(3306)
///     .with_user("cdc_user".to_string())
///     .with_database("mydb".to_string());
/// # }
/// ```
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MariaDbSourceConfig(pub MysqlSourceConfig);

impl Default for MariaDbSourceConfig {
    fn default() -> Self {
        Self(MysqlSourceConfig {
            server_flavor: ServerFlavor::MariaDb,
            ..Default::default()
        })
    }
}

impl std::fmt::Debug for MariaDbSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Deref for MariaDbSourceConfig {
    type Target = MysqlSourceConfig;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MariaDbSourceConfig {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<MariaDbSourceConfig> for MysqlSourceConfig {
    fn from(c: MariaDbSourceConfig) -> Self {
        c.0
    }
}

impl From<MysqlSourceConfig> for MariaDbSourceConfig {
    fn from(mut c: MysqlSourceConfig) -> Self {
        c.server_flavor = ServerFlavor::MariaDb;
        Self(c)
    }
}

impl MariaDbSourceConfig {
    /// Unwrap into the inner [`MysqlSourceConfig`].
    pub fn into_inner(self) -> MysqlSourceConfig {
        self.0
    }

    /// Override the host name.
    #[must_use]
    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.0.host = host.into();
        self
    }

    /// Override the port.
    #[must_use]
    pub fn with_port(mut self, port: u16) -> Self {
        self.0.port = port;
        self
    }

    /// Override the user name.
    #[must_use]
    pub fn with_user(mut self, user: String) -> Self {
        self.0.user = user;
        self
    }

    /// Override the target database.
    #[must_use]
    pub fn with_database(mut self, database: String) -> Self {
        self.0.database = database;
        self
    }
}

/// MariaDB CDC connection type.
///
/// Type alias for [`MysqlConnection`]. The binlog protocol implementation is
/// identical. Pass a [`MariaDbSourceConfig`] (converted via `.into_inner()`) to
/// `MysqlConnection::new()`.
pub type MariaDbConnection = MysqlConnection;

/// Snapshot handle for a MariaDB bulk snapshot.
///
/// Type alias for [`MysqlSnapshotHandle`].
pub type MariaDbSnapshotHandle = MysqlSnapshotHandle;

/// Stream handle for a live MariaDB binlog stream.
///
/// Type alias for [`MysqlStreamHandle`].
pub type MariaDbStreamHandle = MysqlStreamHandle;

/// Incremental snapshot handle for MariaDB (DBLog watermark pattern).
///
/// Type alias for [`MysqlIncrementalSnapshotHandle`].
pub type MariaDbIncrementalSnapshotHandle = MysqlIncrementalSnapshotHandle;

#[cfg(test)]
mod tests {
    use super::{MariaDbSourceConfig, ServerFlavor};

    #[test]
    fn default_flavor_is_mariadb() {
        let config = MariaDbSourceConfig::default();
        assert_eq!(config.server_flavor, ServerFlavor::MariaDb);
        assert_eq!(config.source_type(), "mariadb");
    }

    #[test]
    fn source_type_mariadb() {
        let config = MariaDbSourceConfig::default();
        assert_eq!(config.source_type(), "mariadb");
        assert_eq!(
            config.server_flavor.snapshot_source_name(),
            "mariadb_snapshot"
        );
    }

    #[test]
    fn deref_gives_mysql_source_config() {
        let config = MariaDbSourceConfig::default();
        // Deref to inner config works
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 3306);
    }

    #[test]
    fn from_mysql_source_config_sets_mariadb_flavor() {
        use super::MysqlSourceConfig;
        let mysql_config = MysqlSourceConfig::default();
        assert_eq!(mysql_config.source_type(), "mysql");
        let mariadb_config = MariaDbSourceConfig::from(mysql_config);
        assert_eq!(mariadb_config.source_type(), "mariadb");
    }

    #[test]
    fn builder_methods_work() {
        let config = MariaDbSourceConfig::default()
            .with_host("db.example.com")
            .with_port(3307)
            .with_user("cdc_user".to_string())
            .with_database("events".to_string());
        assert_eq!(config.host, "db.example.com");
        assert_eq!(config.port, 3307);
        assert_eq!(config.user, "cdc_user");
        assert_eq!(config.database, "events");
        assert_eq!(config.source_type(), "mariadb");
    }
}
