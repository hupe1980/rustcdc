//! End-to-end capture against a real MySQL server.
//!
//! # Why this exists
//!
//! Until this file, **MySQL had no end-to-end test in this repository**. It is on the
//! README's front table alongside PostgreSQL, and the whole binlog path — connect,
//! `binlog_format` verification, GTID handling, resume from a stored position — had never
//! been run against the database it names. A review scored that as the project's highest
//! risk, on the grounds that green CI was overstating coverage: PostgreSQL had a
//! container-managed suite and three of four connectors had nothing.
//!
//! The precedent is not hypothetical. `tests/integration_postgres.rs` found a real resume
//! defect on its first run, and later caught a primary-key defect it had been silently
//! reproducing for its entire existence. Integration tests here find real defects
//! immediately.
//!
//! # What MySQL makes harder than PostgreSQL, and why the assertions differ
//!
//! PostgreSQL's replication slot is server-side durable state: the server remembers the
//! consumer's position, so "resume without loss" is testable by stopping the pipeline,
//! writing, and restarting.
//!
//! MySQL has no slot. The binlog is a server-global log with its own retention, and the
//! *client* remembers its position — here, in the checkpoint. That makes the resume test
//! stronger in one way (it exercises this project's own checkpoint, not the server's
//! bookkeeping) and weaker in another (nothing on the server side prevents the position
//! from ageing out). The assertions below are written for what MySQL actually guarantees.
//!
//! # Running it
//!
//! ```text
//! RUSTCDC_INTEGRATION=1 cargo test --all-features --test integration_mysql
//! ```
//!
//! Requires a working Docker daemon; the test manages the container itself so the local
//! and CI paths are identical. Without `RUSTCDC_INTEGRATION=1` every test here returns
//! immediately — and `tests/architecture.rs` asserts that CI still runs it, so this suite
//! cannot quietly stop running.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Fixed rather than random, so a killed run leaves one thing to clean up.
const CONTAINER: &str = "rustcdc-integration-mysql";
const HOST_PORT: u16 = 15498;
const ROOT_PASSWORD: &str = "cdc_integration_root";
const CDC_PASSWORD: &str = "cdc_pw";
const DATABASE: &str = "cdc";

fn enabled() -> bool {
    std::env::var("RUSTCDC_INTEGRATION").as_deref() == Ok("1")
}

fn docker(args: &[&str]) -> std::process::Output {
    Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("failed to run `docker {}`: {e}", args.join(" ")))
}

/// Run SQL through `mysql` inside the container, failing on the first error.
fn mysql(sql: &str) -> String {
    use std::io::Write as _;

    let mut child = Command::new("docker")
        .args([
            "exec",
            "-i",
            CONTAINER,
            "mysql",
            "-uroot",
            &format!("-p{ROOT_PASSWORD}"),
            "--batch",
            "--skip-column-names",
            DATABASE,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mysql");

    child
        .stdin
        .as_mut()
        .expect("mysql stdin")
        .write_all(sql.as_bytes())
        .expect("write sql");

    let output = child.wait_with_output().expect("mysql output");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "mysql failed for:\n{sql}\nstderr:\n{stderr}"
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// Start MySQL with row-based binary logging and wait for it to accept **TCP** connections.
///
/// `--log-bin` and `--binlog-format=ROW` are server arguments rather than a custom image,
/// so the whole fixture is one `docker run` a developer can copy out of this file.
/// `binlog_row_image=FULL` is what makes a `DELETE` carry a usable before-image, which is
/// the MySQL analogue of PostgreSQL's `REPLICA IDENTITY FULL`.
fn start_mysql() {
    let _ = docker(&["rm", "-f", CONTAINER]);

    let out = docker(&[
        "run",
        "-d",
        "--name",
        CONTAINER,
        "-e",
        &format!("MYSQL_ROOT_PASSWORD={ROOT_PASSWORD}"),
        "-e",
        &format!("MYSQL_DATABASE={DATABASE}"),
        "-p",
        &format!("{HOST_PORT}:3306"),
        "mysql:8.0",
        "--log-bin=binlog",
        "--binlog-format=ROW",
        "--binlog-row-image=FULL",
        // **Two different settings, and both are required.**
        //
        // `binlog_row_image` decides which *columns* are logged; `binlog_row_metadata`
        // decides whether column names and primary-key flags travel with them. MySQL 8
        // defaults the latter to MINIMAL (MariaDB to NO_LOG), under which streamed events
        // carry positional placeholders (`@0`, `@1`, …) instead of column names and no
        // primary key at all — which in turn disables snapshot/stream duplicate
        // suppression. The connector refuses to start rather than emit that, with an error
        // that names the setting and the fix; this fixture was written against
        // `binlog_row_image` alone and hit it on the first run.
        "--binlog-row-metadata=FULL",
        "--server-id=1",
        "--gtid-mode=ON",
        "--enforce-gtid-consistency=ON",
    ]);
    assert!(
        out.status.success(),
        "failed to start mysql: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // **Probe over TCP, and require two consecutive successes.**
    //
    // The `mysql` image's entrypoint runs an initialisation pass with a temporary server
    // before starting the real one. Probing the socket, or accepting the first success,
    // can observe that temporary server and return while the real one is still coming up —
    // after which the next command fails with a connection error that looks like a broken
    // fixture. The PostgreSQL suite had exactly this defect and it failed one run in four.
    let deadline = Instant::now() + Duration::from_secs(180);
    let mut consecutive = 0;
    while Instant::now() < deadline {
        let ready = docker(&[
            "exec",
            CONTAINER,
            "mysqladmin",
            "ping",
            "-h",
            "127.0.0.1",
            "-P",
            "3306",
            "--protocol=TCP",
            "-uroot",
            &format!("-p{ROOT_PASSWORD}"),
        ]);
        if ready.status.success() {
            consecutive += 1;
            if consecutive >= 2 {
                return;
            }
        } else {
            consecutive = 0;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    panic!("mysql did not become ready within 180s");
}

fn stop_mysql() {
    let _ = docker(&["rm", "-f", CONTAINER]);
}

/// Schema and a least-privilege replication role.
///
/// `REPLICATION SLAVE` is what lets the connector read the binlog; `REPLICATION CLIENT`
/// lets it call `SHOW MASTER STATUS` to learn where the log currently is. `SELECT` is for
/// snapshots. A role missing any of the three fails in a different place, which is why the
/// grant is spelled out rather than using root.
fn provision_schema() {
    mysql(&format!(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, note VARCHAR(255) NOT NULL);
         CREATE USER 'cdc_user'@'%' IDENTIFIED BY '{CDC_PASSWORD}';
         GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
         GRANT SELECT ON {DATABASE}.* TO 'cdc_user'@'%';
         FLUSH PRIVILEGES;"
    ));
}

struct Fixture {
    dir: tempfile::TempDir,
    config: PathBuf,
    events: PathBuf,
}

impl Fixture {
    fn new(server_id: u32, gtid: bool) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let events = dir.path().join("events.jsonl");
        let state = dir.path().join("state");
        let config = dir.path().join("cdc.toml");

        std::fs::write(
            &config,
            format!(
                r#"api_version = "v1"

[source]
require_primary = false

[source.mysql]
host = "127.0.0.1"
port = {HOST_PORT}
user = "cdc_user"
password = {{ env = "RUSTCDC_IT_MYSQL_PASSWORD" }}
database = "{DATABASE}"
server_id = {server_id}
gtid_mode_enabled = {gtid}
binlog_format_check = true
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 500
table_include_list = ["{DATABASE}.orders"]
table_exclude_list = []

[source.mysql.transport]
mode = "plaintext"

[sink]
type = "file_jsonl"
path = "{events}"

[state]
dir = "{state}"

[runtime]
max_poll_wait_ms = 100
sink_flush_interval_events = 1

[admin]
enabled = false
"#,
                events = events.display(),
                state = state.display(),
            ),
        )
        .expect("write config");

        Self {
            dir,
            config,
            events,
        }
    }

    fn spawn(&self) -> Child {
        let log = std::fs::File::create(self.dir.path().join("pipeline.log")).expect("log file");
        Command::new(env!("CARGO_BIN_EXE_rustcdc"))
            .args(["run", "--config-file"])
            .arg(&self.config)
            .env("RUSTCDC_IT_MYSQL_PASSWORD", CDC_PASSWORD)
            .stdout(Stdio::null())
            // A file, not a pipe: a pipe nobody reads blocks the pipeline once it has
            // logged 64 KiB, turning a diagnostic into a hang.
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn rustcdc")
    }

    fn stderr(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("pipeline.log"))
            .unwrap_or_else(|e| format!("<could not read pipeline log: {e}>"))
    }

    /// Every captured event, as `(op, id, note)`.
    fn captured(&self) -> Vec<(String, String, String)> {
        let Ok(body) = std::fs::read_to_string(&self.events) else {
            return Vec::new();
        };
        body.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                let event: serde_json::Value =
                    serde_json::from_str(line).expect("each sink line is one JSON event");
                let row = event
                    .get("after")
                    .filter(|v| !v.is_null())
                    .or_else(|| event.get("before"))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let field = |name: &str| {
                    row.get(name)
                        .map(|v| {
                            v.as_str()
                                .map(str::to_string)
                                .unwrap_or_else(|| v.to_string())
                        })
                        .unwrap_or_default()
                };
                (
                    event["op"].as_str().unwrap_or_default().to_string(),
                    field("id"),
                    field("note"),
                )
            })
            .collect()
    }

    /// The `primary_key` array of every captured event.
    fn primary_keys(&self) -> Vec<Vec<String>> {
        let Ok(body) = std::fs::read_to_string(&self.events) else {
            return Vec::new();
        };
        body.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                let event: serde_json::Value =
                    serde_json::from_str(line).expect("each sink line is one JSON event");
                event["primary_key"]
                    .as_array()
                    .map(|cols| {
                        cols.iter()
                            .map(|c| c.as_str().unwrap_or_default().to_string())
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    fn wait_for(&self, expected: usize, within: Duration) -> Vec<(String, String, String)> {
        let deadline = Instant::now() + within;
        loop {
            let captured = self.captured();
            if captured.len() >= expected {
                return captured;
            }
            if Instant::now() >= deadline {
                panic!(
                    "expected at least {expected} events within {within:?}, got {}:\n{:#?}\n\n\
                     --- pipeline stderr ---\n{}",
                    captured.len(),
                    captured,
                    self.stderr(),
                );
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

/// Wait until the connector has registered as a replica.
///
/// The MySQL analogue of waiting for a replication slot to go active. A binlog reader shows
/// up in `SHOW PROCESSLIST` with `Command = 'Binlog Dump'` (or `Binlog Dump GTID`), which
/// is the server's own answer to "is a consumer attached right now". Waiting on a fixed
/// sleep instead is what made the PostgreSQL suite flaky under load: enough on an idle
/// machine, not enough on a busy one, and the DML that followed predated the connection and
/// was never captured.
fn wait_for_binlog_reader(within: Duration) {
    let deadline = Instant::now() + within;
    loop {
        let count = mysql(
            "SELECT COUNT(*) FROM information_schema.PROCESSLIST \
             WHERE COMMAND LIKE 'Binlog Dump%';",
        );
        if count.trim() != "0" && !count.trim().is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no binlog reader attached within {within:?} (PROCESSLIST reported {count:?}); \
             any DML from here would be invisible to the pipeline"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Terminate the child and wait for it, so the server drops the replica registration.
///
/// MySQL rejects a second connection with the same `server_id`, so a test that restarts the
/// pipeline has to actually wait for the first one to exit rather than merely signal it.
fn shutdown(mut child: Child) {
    #[cfg(unix)]
    {
        let _ = Command::new("kill").arg(child.id().to_string()).status();
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(200)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return;
            }
        }
    }
}

fn setup() -> bool {
    if !enabled() {
        eprintln!(
            "skipping: set RUSTCDC_INTEGRATION=1 (and have Docker running) to execute the \
             MySQL integration suite"
        );
        return false;
    }
    start_mysql();
    provision_schema();
    true
}

/// Insert, update and delete are captured, in order, with usable before-images.
///
/// The baseline: this is the first assertion in this repository that the MySQL connector
/// captures anything at all.
#[test]
fn insert_update_and_delete_are_captured_in_order() {
    if !setup() {
        return;
    }

    let fixture = Fixture::new(1001, true);
    let child = fixture.spawn();
    wait_for_binlog_reader(Duration::from_secs(60));

    mysql(
        "INSERT INTO orders VALUES (1,'one');
         UPDATE orders SET note='one-b' WHERE id=1;
         DELETE FROM orders WHERE id=1;",
    );

    let captured = fixture.wait_for(3, Duration::from_secs(60));
    shutdown(child);

    let ops: Vec<&str> = captured.iter().map(|(op, _, _)| op.as_str()).collect();
    assert_eq!(
        &ops[..3],
        &["insert", "update", "delete"],
        "operations must arrive in commit order: {captured:#?}"
    );
    assert_eq!(captured[1].2, "one-b", "the update's after-image");
    assert_eq!(
        captured[2].2, "one-b",
        "the delete must carry a before-image; without binlog_row_image=FULL it would be \
         key-only and a consumer could not tell which row was removed"
    );

    drop(fixture);
    stop_mysql();
}

/// The primary key is the table's real key, not every column.
///
/// The MySQL analogue of the PostgreSQL `REPLICA IDENTITY FULL` defect, and worth asserting
/// for the same reason: the Kafka message key is derived from these columns, so a key that
/// widens to the whole row means a compacted topic can never collapse a row's history and
/// one row's versions hash to different partitions. `binlog_row_image=FULL` sends every
/// column, which is exactly the condition under which the mistake is easy to make.
#[test]
fn the_primary_key_is_the_declared_key_not_every_column() {
    if !setup() {
        return;
    }

    let fixture = Fixture::new(1002, true);
    let child = fixture.spawn();
    wait_for_binlog_reader(Duration::from_secs(60));

    mysql(
        "INSERT INTO orders VALUES (7,'seven');
         UPDATE orders SET note='seven-b' WHERE id=7;
         DELETE FROM orders WHERE id=7;",
    );

    fixture.wait_for(3, Duration::from_secs(60));
    shutdown(child);

    for (index, key) in fixture.primary_keys().iter().enumerate() {
        assert_eq!(
            key,
            &vec!["id".to_string()],
            "event {index} reported primary key {key:?}; `orders` has one key column (`id`) \
             and binlog_row_image=FULL must not widen it to the whole row"
        );
    }

    drop(fixture);
    stop_mysql();
}

/// A restart loses nothing and replays nothing.
///
/// MySQL has no server-side slot: the binlog is a server-global log and the *client* owns
/// its position. So unlike the PostgreSQL equivalent, this exercises **this project's**
/// checkpoint rather than the server's bookkeeping — if the stored binlog coordinate or
/// GTID set is wrong, this is where it shows.
///
/// Both properties are asserted exactly:
///
/// * **no gaps** — every row committed while the pipeline was down is captured, in commit
///   order. Violating this is data loss.
/// * **no replay** — the event count is exactly the number of rows committed.
#[test]
fn the_resume_position_does_not_lose_events() {
    if !setup() {
        return;
    }

    let fixture = Fixture::new(1003, true);

    let first = fixture.spawn();
    wait_for_binlog_reader(Duration::from_secs(60));
    mysql("INSERT INTO orders VALUES (1,'before-restart');");
    fixture.wait_for(1, Duration::from_secs(60));
    shutdown(first);

    // Committed while nothing is capturing. The binlog retains these.
    mysql("INSERT INTO orders VALUES (2,'while-down-a'),(3,'while-down-b');");

    let second = fixture.spawn();
    wait_for_binlog_reader(Duration::from_secs(60));
    mysql("INSERT INTO orders VALUES (4,'after-restart');");

    let captured = fixture.wait_for(4, Duration::from_secs(60));
    // Settle, so a replayed event arriving late is caught rather than raced past.
    std::thread::sleep(Duration::from_secs(3));
    let captured_after_settle = fixture.captured();
    shutdown(second);

    let notes: Vec<&str> = captured_after_settle
        .iter()
        .map(|(_, _, note)| note.as_str())
        .collect();
    assert_eq!(
        notes,
        vec![
            "before-restart",
            "while-down-a",
            "while-down-b",
            "after-restart"
        ],
        "every row must be captured exactly once, in commit order: {captured:#?}"
    );

    drop(fixture);
    stop_mysql();
}

/// A clean restart with no new writes must redeliver nothing.
///
/// The sharpest form of the no-replay property, and the one that caught a real defect on
/// the PostgreSQL side: start, insert, idle, shut down cleanly, restart with **no** new
/// DML. Anything that appears is a redelivery of an already-checkpointed event.
#[test]
fn a_clean_restart_redelivers_nothing() {
    if !setup() {
        return;
    }

    let fixture = Fixture::new(1004, true);

    let first = fixture.spawn();
    wait_for_binlog_reader(Duration::from_secs(60));
    mysql("INSERT INTO orders VALUES (1,'only-row');");
    fixture.wait_for(1, Duration::from_secs(60));

    // Idle before shutdown, so a redelivery cannot be blamed on a checkpoint that had not
    // yet been written.
    std::thread::sleep(Duration::from_secs(4));
    shutdown(first);

    let second = fixture.spawn();
    // Wait for the reader to attach before the settle window, not just for time to pass:
    // this assertion is about an *absence*, and an absence is trivially satisfied by a
    // pipeline that never started.
    wait_for_binlog_reader(Duration::from_secs(60));
    std::thread::sleep(Duration::from_secs(10));
    shutdown(second);

    assert_eq!(
        fixture.captured().len(),
        1,
        "a clean restart with no new writes must redeliver nothing: {:#?}",
        fixture.captured()
    );

    drop(fixture);
    stop_mysql();
}

/// The connector works without GTID, on binlog file+position alone.
///
/// `gtid_mode_enabled = false` is the configuration for a server that predates GTID or has
/// it switched off, and it takes a different resume path — file+position rather than an
/// executed-GTID set. It is a supported configuration and had never been exercised.
#[test]
fn capture_works_without_gtid() {
    if !setup() {
        return;
    }

    let fixture = Fixture::new(1005, false);
    let child = fixture.spawn();
    wait_for_binlog_reader(Duration::from_secs(60));

    mysql("INSERT INTO orders VALUES (42,'no-gtid');");

    let captured = fixture.wait_for(1, Duration::from_secs(60));
    shutdown(child);

    assert_eq!(captured[0].1, "42");
    assert_eq!(captured[0].2, "no-gtid");

    drop(fixture);
    stop_mysql();
}

/// Silence the unused-field warning: `dir` is held to keep the temp directory alive.
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = &self.dir;
    }
}
