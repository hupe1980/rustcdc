mod dry_run;
mod init;
mod init_state;
mod inspect_checkpoint;
mod migrate_state;
mod replay;
pub(crate) mod run;

/// A minimal valid `AppConfig`, for tests outside this module that need a real one
/// rather than a hand-written JSON fixture (see `crate::redaction`).
#[cfg(test)]
pub(crate) use run::tests::minimal_config as minimal_config_for_tests;
mod snapshot;
pub(crate) mod status;
mod validate_config;

use std::path::Path;

use crate::{cli::Command, error::AppError};

pub async fn dispatch(command: Command, config_path: Option<&Path>) -> Result<(), AppError> {
    match command {
        Command::Init(args) => init::execute(args).await,
        Command::Run(args) => run::execute(args, config_path).await,
        Command::ValidateConfig(args) => validate_config::execute(args, config_path).await,
        Command::Status(args) => status::execute(args).await,
        Command::DryRun(args) => dry_run::execute(args, config_path).await,
        Command::MigrateState(args) => migrate_state::execute(args, config_path).await,
        Command::InitState(args) => init_state::execute(args, config_path).await,
        Command::InspectCheckpoint(args) => inspect_checkpoint::execute(args, config_path).await,
        Command::Replay(args) => replay::execute(args, config_path).await,
        Command::Snapshot(args) => snapshot::execute(args).await,
    }
}
