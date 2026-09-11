use rustcdc::core::RuntimeSourceConfig;

use super::schema::{AppConfig, SourceDriver};
use crate::error::ConfigError;

pub fn validate_source_config(config: &AppConfig) -> Result<(), ConfigError> {
    match &config.source.driver {
        #[cfg(feature = "postgres")]
        SourceDriver::Postgres(pg) => {
            pg.validate()
                .map_err(|e| ConfigError::InvalidSource(e.to_string()))?;
            warn_on_non_loopback_plaintext(&pg.transport, &pg.host, "source");
            warn_on_sql_peek_wal_transport(pg);
            Ok(())
        }
        #[cfg(feature = "mysql")]
        SourceDriver::Mysql(mysql) => {
            mysql
                .validate()
                .map_err(|e| ConfigError::InvalidSource(e.to_string()))?;
            warn_on_non_loopback_plaintext(&mysql.transport, &mysql.host, "source");
            Ok(())
        }
        #[cfg(feature = "mysql")]
        SourceDriver::Mariadb(mariadb) => {
            mariadb
                .validate()
                .map_err(|e| ConfigError::InvalidSource(e.to_string()))?;
            warn_on_non_loopback_plaintext(&mariadb.transport, &mariadb.host, "source");
            Ok(())
        }
        #[cfg(feature = "sqlserver")]
        SourceDriver::Sqlserver(sqlserver) => {
            sqlserver
                .to_runtime_config()
                .validate()
                .map_err(|e| ConfigError::InvalidSource(e.to_string()))?;
            warn_on_non_loopback_plaintext(&sqlserver.transport, &sqlserver.host, "source");
            Ok(())
        }
    }
}

pub fn resolve_runtime_source_config(
    config: &AppConfig,
) -> Result<RuntimeSourceConfig, ConfigError> {
    validate_source_config(config)?;

    match &config.source.driver {
        #[cfg(feature = "postgres")]
        SourceDriver::Postgres(pg) => Ok(RuntimeSourceConfig::Postgres(pg.clone())),
        #[cfg(feature = "mysql")]
        SourceDriver::Mysql(mysql) => Ok(RuntimeSourceConfig::Mysql(mysql.clone())),
        #[cfg(feature = "mysql")]
        SourceDriver::Mariadb(mariadb) => Ok(RuntimeSourceConfig::MariaDb(mariadb.clone())),
        #[cfg(feature = "sqlserver")]
        SourceDriver::Sqlserver(sqlserver) => Ok(RuntimeSourceConfig::SqlServer(
            sqlserver.to_runtime_config(),
        )),
    }
}

/// Say at config time that the fallback WAL transport was selected.
///
/// rustcdc warns too, but only when the stream actually starts — so `validate-config` and
/// `dry-run`, which are where an operator reviews a change before shipping it, said
/// nothing. `sql_peek` is not a tuning preference: it re-decodes WAL from the slot's
/// `restart_lsn` on *every* poll, so its cost grows with the source's longest-running
/// transaction rather than staying constant, and it is easy to inherit from a copied
/// config without meaning to.
#[cfg(feature = "postgres")]
fn warn_on_sql_peek_wal_transport(pg: &rustcdc::PostgresSourceConfig) {
    if matches!(pg.wal_transport, rustcdc::WalTransport::SqlPeek) {
        tracing::warn!(
            slot = %pg.replication_slot_name,
            "source.postgres.wal_transport = \"sql_peek\": every poll re-decodes WAL from \
             the slot's restart_lsn, so one long-running transaction on the source makes \
             each poll re-read the gap to confirmed_flush_lsn, and latency is bounded by \
             stream_poll_interval_ms rather than pushed by the server. Prefer the default \
             \"streaming_replication\" unless the role lacks the REPLICATION attribute or \
             the connection must route through a transaction-pooling connection pooler."
        );
    }
}

fn warn_on_non_loopback_plaintext(
    transport: &rustcdc::TransportConfig,
    host: &str,
    source_label: &str,
) {
    if matches!(transport, rustcdc::TransportConfig::Plaintext) && !is_loopback_host(host) {
        tracing::warn!(
            source = source_label,
            host,
            "using plaintext source transport on non-loopback host; prefer tls for production deployments"
        );
    }
}

fn is_loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}
