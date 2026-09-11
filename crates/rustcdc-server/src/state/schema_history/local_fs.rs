use std::path::Path;

use rustcdc::schema_history::FileSchemaHistory;

use crate::error::AppError;

pub(super) async fn build_schema_history(state_dir: &Path) -> Result<FileSchemaHistory, AppError> {
    std::fs::create_dir_all(state_dir)?;
    let schema_history_path = state_dir.join("schema_history");
    FileSchemaHistory::new(&schema_history_path)
        .await
        .map_err(|e| AppError::Other(format!("failed to open file schema history: {e}")))
}
