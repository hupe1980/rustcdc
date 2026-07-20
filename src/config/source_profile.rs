use rustcdc::core::RuntimeSourceConfig;

use super::schema::{AppConfig, SourceDriver};
use crate::error::ConfigError;

pub fn validate_source_config(config: &AppConfig) -> Result<(), ConfigError> {
    match &config.source.driver {
        SourceDriver::Postgres(pg) => {
            pg.validate()
                .map_err(|e| ConfigError::InvalidSource(e.to_string()))?;
            warn_on_non_loopback_plaintext(&pg.transport, &pg.host, "source");
            Ok(())
        }
        SourceDriver::Mysql(mysql) => {
            mysql
                .validate()
                .map_err(|e| ConfigError::InvalidSource(e.to_string()))?;
            warn_on_non_loopback_plaintext(&mysql.transport, &mysql.host, "source");
            Ok(())
        }
        SourceDriver::Mariadb(mariadb) => {
            mariadb
                .validate()
                .map_err(|e| ConfigError::InvalidSource(e.to_string()))?;
            warn_on_non_loopback_plaintext(&mariadb.transport, &mariadb.host, "source");
            Ok(())
        }
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
        SourceDriver::Postgres(pg) => Ok(RuntimeSourceConfig::Postgres(pg.clone())),
        SourceDriver::Mysql(mysql) => Ok(RuntimeSourceConfig::Mysql(mysql.clone())),
        SourceDriver::Mariadb(mariadb) => Ok(RuntimeSourceConfig::MariaDb(mariadb.clone())),
        SourceDriver::Sqlserver(sqlserver) => Ok(RuntimeSourceConfig::SqlServer(
            sqlserver.to_runtime_config(),
        )),
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
