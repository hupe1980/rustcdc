//! Seed a checkpoint file for disaster recovery.
//!
//! Checkpoint files carry an integrity checksum, so they cannot be written correctly by
//! hand. This tool builds a valid one: it writes atomically, applies the restrictive file
//! mode the runtime requires, and fsyncs the parent directory so the file survives a crash
//! immediately after seeding.
//!
//! Run it only while the connector is **stopped**. Seeding a checkpoint under a running
//! runtime races its own writes, and the runtime holds an owner lease that will reject the
//! result.
//!
//! ```bash
//! cargo run --example seed_checkpoint --features postgres -- \
//!   --dir /var/rustcdc/checkpoints \
//!   --source-type postgres \
//!   --committed-event-count 0 \
//!   --offset '{"lsn": 281474976711680, "slot_name": "rustcdc_postgres_new"}'
//! ```
//!
//! The `--offset` payload is the source-specific offset body:
//!
//! - `postgres`  — `{"lsn": <u64>, "slot_name": "<slot>"}`
//! - `mysql` / `mariadb` — `{"source_flavor": "mysql", "binlog_file": "...",
//!   "binlog_pos": <u64>, "gtid": "<set-or-null>"}`
//!
//! Seeding a position that is **ahead** of what was actually delivered downstream skips
//! every event in between, permanently. When in doubt, seed behind and accept duplicates:
//! the runtime's contract is at-least-once, so downstream must already tolerate them.

use std::path::PathBuf;

use rustcdc::checkpoint::FileCheckpoint;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut dir: Option<PathBuf> = None;
    let mut source_type: Option<String> = None;
    let mut committed_event_count: u64 = 0;
    let mut offset: Option<String> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || {
            args.next()
                .ok_or_else(|| format!("missing value for {arg}"))
        };
        match arg.as_str() {
            "--dir" => dir = Some(PathBuf::from(value()?)),
            "--source-type" => source_type = Some(value()?),
            "--committed-event-count" => committed_event_count = value()?.parse()?,
            "--offset" => offset = Some(value()?),
            "--help" | "-h" => {
                eprintln!("{}", USAGE);
                return Ok(());
            }
            other => return Err(format!("unknown argument '{other}'\n\n{USAGE}").into()),
        }
    }

    let (Some(dir), Some(source_type), Some(offset)) = (dir, source_type, offset) else {
        return Err(
            format!("--dir, --source-type and --offset are all required\n\n{USAGE}").into(),
        );
    };

    // Parse before writing so a malformed offset fails here rather than at connector
    // startup, when the outage is already in progress.
    let parsed: serde_json::Value = serde_json::from_str(&offset)
        .map_err(|error| format!("--offset is not valid JSON: {error}"))?;
    if !parsed.is_object() {
        return Err("--offset must be a JSON object".into());
    }

    FileCheckpoint::restore_from_record(
        &dir,
        &source_type,
        serde_json::to_vec(&parsed)?,
        committed_event_count,
    )?;

    println!(
        "wrote {}",
        dir.join(format!("checkpoint_{source_type}.json")).display()
    );
    println!("Start the connector to resume from it. It will refuse to load if the file is");
    println!("later modified: the integrity checksum is verified on every load.");
    Ok(())
}

const USAGE: &str = "\
seed_checkpoint --dir <path> --source-type <type> --offset <json> \
[--committed-event-count <n>]

Writes a valid, checksummed checkpoint file. Run only while the connector is stopped.";
