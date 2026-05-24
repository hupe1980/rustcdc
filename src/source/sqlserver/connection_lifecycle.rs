use std::time::Duration;

use tokio::task::JoinHandle;

use crate::core::{Error, Result, StructuredLogger};

use super::{
    query, SqlServerConnection, SqlServerPrereqProbe, SqlServerSourceConfig, HEARTBEAT_SECS,
};

pub(super) async fn connect_sqlserver_with_probe(connection: &SqlServerConnection) -> Result<()> {
    connection.config.validate()?;
    {
        let state = connection.state.lock().await;
        if state.connected {
            return Err(Error::StateError(
                "sqlserver connection already established".into(),
            ));
        }
    }

    #[cfg(not(feature = "tls"))]
    {
        return Err(Error::ConfigError(
            "sqlserver connector requires crate feature 'tls'; plaintext transport is disabled"
                .into(),
        ));
    }

    #[cfg(feature = "tls")]
    {
        let snapshot = connection.prereq_probe.probe(&connection.config).await?;
        query::validate_prereq_snapshot(&connection.config, &snapshot)?;

        let heartbeat_task = start_sqlserver_heartbeat(
            &connection.prereq_probe,
            &connection.config,
            connection.logger.clone(),
        );
        let mut state = connection.state.lock().await;
        state.connected = true;
        state.heartbeat_task = Some(heartbeat_task);
        connection.logger.source_connected();
        Ok(())
    }
}

pub(super) fn start_sqlserver_heartbeat(
    probe: &std::sync::Arc<dyn SqlServerPrereqProbe>,
    config: &SqlServerSourceConfig,
    logger: StructuredLogger,
) -> JoinHandle<()> {
    let probe = probe.clone();
    let config = config.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(HEARTBEAT_SECS));
        loop {
            interval.tick().await;
            if let Err(error) = probe.heartbeat(&config).await {
                logger.connection_error(&format!("sqlserver heartbeat failed: {error}"));
                break;
            }
        }
    })
}
