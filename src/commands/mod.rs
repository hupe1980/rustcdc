mod dry_run;
mod init;
mod init_state;
mod inspect_checkpoint;
mod migrate_state;
mod replay;
mod run;
mod run_batch;
mod run_lifecycle;
mod run_loop;
mod run_loop_batch;
pub(crate) mod run_metrics;
mod run_reconciliation;
mod run_recovery;
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
    }
}
