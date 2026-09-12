use rustcdc::checkpoint::{Checkpoint, FileCheckpoint};
use rustcdc::schema_history::FileSchemaHistory;
use std::path::Path;

use crate::{
    cli::InspectCheckpointArgs,
    config::{self},
    error::AppError,
};

pub async fn execute(
    args: InspectCheckpointArgs,
    config_path: Option<&Path>,
) -> Result<(), AppError> {
    // Determine state dir: CLI arg > config file > error.
    let state_dir = if let Some(dir) = args.state_dir {
        dir
    } else {
        let cfg_path = config_path.ok_or(crate::error::ConfigError::NoConfigFile)?;
        let cfg = config::load(cfg_path)?;
        cfg.state.offset.dir
    };

    println!("State directory: {}", state_dir.display());

    // ── Checkpoint ────────────────────────────────────────────────────────
    let checkpoint = FileCheckpoint::new(state_dir.join("checkpoint"));
    match checkpoint.load().await {
        Ok(Some(offset)) => {
            println!("Checkpoint offset: {offset:?}");
        }
        Ok(None) => {
            println!("Checkpoint: no checkpoint saved yet");
        }
        Err(e) => {
            println!("Checkpoint error: {e}");
        }
    }
    match checkpoint.get_committed_count().await {
        Ok(count) => println!("Committed event count: {count}"),
        Err(e) => println!("Committed count error: {e}"),
    }

    // ── Schema history ────────────────────────────────────────────────────
    if args.schema_history {
        let sh_path = state_dir.join("schema_history");
        match FileSchemaHistory::new(&sh_path).await {
            Ok(sh) => {
                println!("Schema history: loaded from {}", sh_path.display());
                println!("{sh:?}");
            }
            Err(e) => println!("Schema history error: {e}"),
        }
    }

    Ok(())
}
