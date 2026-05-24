use std::{collections::HashSet, path::Path, time::{Duration, Instant}};

pub struct WorkerMarker {
    pub events: usize,
    pub acked: bool,
    pub ids: HashSet<String>,
}

pub fn wait_for_marker(path: &Path, timeout: Duration) -> cdc_rs::Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() && read_worker_marker(path).is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    Err(cdc_rs::Error::TimeoutError(format!(
        "timed out waiting for crash worker marker at {}",
        path.display()
    )))
}

pub fn read_worker_batch_len(path: &Path) -> cdc_rs::Result<usize> {
    Ok(read_worker_marker(path)?.events)
}

pub fn read_worker_marker(path: &Path) -> cdc_rs::Result<WorkerMarker> {
    let marker = std::fs::read_to_string(path).map_err(cdc_rs::Error::IoError)?;
    let mut events = None;
    let mut acked = false;
    let mut ids = HashSet::new();

    for line in marker.lines() {
        if let Some(value) = line.strip_prefix("events=") {
            if value.is_empty() {
                return Err(cdc_rs::Error::StateError(
                    "worker marker events field is empty".into(),
                ));
            }
            events = Some(value.parse::<usize>().map_err(|error| {
                cdc_rs::Error::StateError(format!("invalid worker marker events: {error}"))
            })?);
        } else if let Some(value) = line.strip_prefix("acked=") {
            acked = value == "1";
        } else if let Some(value) = line.strip_prefix("ids=") {
            for id in value.split(',').map(str::trim).filter(|id| !id.is_empty()) {
                ids.insert(id.to_string());
            }
        }
    }

    let events = events
        .ok_or_else(|| cdc_rs::Error::StateError("worker marker missing events field".into()))?;

    Ok(WorkerMarker { events, acked, ids })
}