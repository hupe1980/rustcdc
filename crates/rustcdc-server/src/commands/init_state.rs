use std::path::Path;

use crate::cli::InitStateArgs;
use crate::config::{self, schema::StateBackend};
use crate::error::{AppError, ConfigError};

pub async fn execute(args: InitStateArgs, config_path: Option<&Path>) -> Result<(), AppError> {
    let config_path = config_path.ok_or(ConfigError::NoConfigFile)?;
    let config = config::load(config_path)?;

    let StateBackend::KafkaTopic(kafka_state) = &config.state.offset.backend else {
        return Err(AppError::Other(
            "init-state requires state.backend.type = \"kafka_topic\" in config".to_string(),
        ));
    };

    crate::state::initialize_kafka_topic_state(kafka_state, args.force).await?;

    println!(
        "initialized kafka_topic state artifacts in topic '{}' (force={})",
        kafka_state.topic, args.force
    );

    Ok(())
}
