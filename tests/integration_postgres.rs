//! End-to-end capture against a real PostgreSQL server.
//!
//! # Why this exists
//!
//! Until this file, **nothing in this repository tested against a real database**. The
//! pipeline was covered end to end by a scripted in-memory source
//! (`src/commands/delivery_contract_tests.rs`), which is excellent for barrier and fault
//! semantics and proves nothing about a connector. Every database-touching test was
//! env-gated and therefore skipped, so the suite was green with the entire capture path
//! unexercised.
//!
//! That boundary — "rustcdc tests connectors, cdc-server tests the server" — is defensible
//! in principle and indefensible in practice, because the defects that matter are
//! *integration* defects between two independently versioned crates. rustcdc 0.10 replaced
//! the PostgreSQL WAL transport with a new ~900-line wire client and made it the default;
//! this file is the only thing in this repository that has ever run it.
//!
//! It found a real resume defect on its first run. See
//! `the_resume_position_does_not_lose_events` for what it asserts and why the bound is
//! written the way it is.
//!
//! # Running it
//!
//! ```text
//! RUSTCDC_INTEGRATION=1 cargo test --all-features --test integration_postgres
//! ```
//!
//! Requires a working Docker daemon; the test manages the container itself so the local
//! and CI paths are identical. Without `RUSTCDC_INTEGRATION=1` every test here returns
//! immediately — and `tests/architecture.rs` asserts that CI still sets it, so the suite
//! cannot quietly stop running.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Container name. Fixed rather than random so a killed run leaves one thing to clean up.
const CONTAINER: &str = "rustcdc-integration-postgres";
const HOST_PORT: u16 = 15499;
const PG_PASSWORD: &str = "cdc_integration_pw";
/// Write-scope bearer token for the fixtures that enable the admin surface.
const ADMIN_TOKEN: &str = "integration-write-token";

fn enabled() -> bool {
    std::env::var("RUSTCDC_INTEGRATION").as_deref() == Ok("1")
}

fn docker(args: &[&str]) -> std::process::Output {
    Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("failed to run `docker {}`: {e}", args.join(" ")))
}

/// Run SQL through `psql` inside the container, failing on the first error.
///
/// `-v ON_ERROR_STOP=1` matters: without it psql reports success for a script whose
/// statements all failed, and the test would assert against an empty database.
fn psql(sql: &str) -> String {
    use std::io::Write as _;

    let mut child = Command::new("docker")
        .args([
            "exec",
            "-i",
            CONTAINER,
            "psql",
            "-U",
            "postgres",
            "-d",
            "cdc",
            "-v",
            "ON_ERROR_STOP=1",
            "-tAq",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn psql");

    child
        .stdin
        .as_mut()
        .expect("psql stdin")
        .write_all(sql.as_bytes())
        .expect("write sql");

    let output = child.wait_with_output().expect("psql output");
    assert!(
        output.status.success(),
        "psql failed for:\n{sql}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// Start PostgreSQL with logical decoding enabled and wait for it to accept connections.
///
/// `wal_level=logical` is passed as a server argument rather than baked into an image so
/// the whole fixture is one `docker run` that a developer can copy out of this file.
fn start_postgres() {
    let _ = docker(&["rm", "-f", CONTAINER]);

    let out = docker(&[
        "run",
        "-d",
        "--name",
        CONTAINER,
        "-e",
        &format!("POSTGRES_PASSWORD={PG_PASSWORD}"),
        "-e",
        "POSTGRES_DB=cdc",
        "-p",
        &format!("{HOST_PORT}:5432"),
        "postgres:16",
        "-c",
        "wal_level=logical",
        "-c",
        "max_replication_slots=8",
        "-c",
        "max_wal_senders=8",
    ]);
    assert!(
        out.status.success(),
        "failed to start postgres: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // ── Wait for the *real* server, not the one initdb starts ────────────────
    //
    // The `postgres` image's entrypoint runs initdb, starts a **temporary** server to
    // apply initialisation, then shuts it down and starts the real one. That temporary
    // server is deliberately started with `listen_addresses=''`, so it accepts only the
    // Unix socket.
    //
    // `pg_isready -U postgres -d cdc` with no host therefore uses the socket and can
    // succeed against the temporary server — after which the socket disappears for the
    // restart. This suite's `provision_schema()` then failed with *"connection to server
    // on socket ... failed: No such file or directory"*, one run in four, and the
    // resulting panic pointed at whichever test drew the short straw.
    //
    // Probing over **TCP** is what distinguishes the two: only the real server listens
    // there. Two consecutive successes, because the first TCP accept can still land in
    // the window before the server finishes coming up.
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut consecutive = 0;
    while Instant::now() < deadline {
        let ready = docker(&[
            "exec",
            CONTAINER,
            "pg_isready",
            "-h",
            "127.0.0.1",
            "-p",
            "5432",
            "-U",
            "postgres",
            "-d",
            "cdc",
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
    panic!("postgres did not become ready within 90s");
}

fn stop_postgres() {
    let _ = docker(&["rm", "-f", CONTAINER]);
}

/// Schema, publication, and a least-privilege replication role.
///
/// `REPLICA IDENTITY FULL` so `DELETE` events carry a full before-image — otherwise the
/// assertions below could not identify which row was deleted, and neither could a real
/// consumer.
fn provision_schema() {
    psql(
        "CREATE TABLE public.orders (id BIGINT PRIMARY KEY, note TEXT NOT NULL);
         ALTER TABLE public.orders REPLICA IDENTITY FULL;
         CREATE PUBLICATION cdc_pub FOR TABLE public.orders;
         CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_pw';
         GRANT USAGE ON SCHEMA public TO cdc_user;
         GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;",
    );
}

struct Fixture {
    dir: tempfile::TempDir,
    config: PathBuf,
    events: PathBuf,
    /// `Some` when the fixture was built with an admin surface, for `POST /signals`.
    admin_port: Option<u16>,
}

/// Extra TOML appended to the generated config, so a test can opt into the admin
/// surface or an `[incremental_snapshot]` section without a second config template
/// drifting away from the first.
#[derive(Default)]
struct Extras {
    /// Bind an admin server on this port and issue `ADMIN_TOKEN` as the write token.
    admin_port: Option<u16>,
    /// Verbatim TOML appended after the generated sections.
    toml: String,
}

impl Fixture {
    fn new(wal_transport: &str, slot: &str) -> Self {
        Self::with_extras(wal_transport, slot, Extras::default())
    }

    fn with_extras(wal_transport: &str, slot: &str, extras: Extras) -> Self {
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

[source.postgres]
host = "127.0.0.1"
port = {HOST_PORT}
user = "cdc_user"
password = {{ env = "RUSTCDC_IT_PG_PASSWORD" }}
database = "cdc"
replication_slot_name = "{slot}"
publication_name = "cdc_pub"
create_replication_slot_if_missing = true
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 500
wal_transport = "{wal_transport}"
table_include_list = ["public.orders"]
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "file_jsonl"
path = "{events}"

[state]
dir = "{state}"

[runtime]
max_poll_wait_ms = 100
sink_flush_interval_events = 1

{admin}
{extra}
"#,
                events = events.display(),
                state = state.display(),
                admin = match extras.admin_port {
                    // `notification_log_file` is not optional decoration: the loader
                    // refuses write-capable signalling without a notification sink, so
                    // that an accepted signal always leaves a durable trace.
                    Some(port) => format!(
                        "[admin]\nenabled = true\nbind = \"127.0.0.1:{port}\"\n\
                         write_token_env = \"RUSTCDC_IT_ADMIN_TOKEN\"\n\
                         notification_log_file = \"{notifications}\"\n",
                        notifications = dir.path().join("notifications.jsonl").display(),
                    ),
                    None => "[admin]\nenabled = false\n".to_string(),
                },
                extra = extras.toml,
            ),
        )
        .expect("write config");

        Self {
            dir,
            config,
            events,
            admin_port: extras.admin_port,
        }
    }

    fn spawn(&self) -> Child {
        let log = std::fs::File::create(self.dir.path().join("pipeline.log")).expect("log file");
        Command::new(env!("CARGO_BIN_EXE_rustcdc"))
            .args(["run", "--config-file"])
            .arg(&self.config)
            .env("RUSTCDC_IT_PG_PASSWORD", "cdc_pw")
            .env("RUSTCDC_IT_ADMIN_TOKEN", ADMIN_TOKEN)
            .stdout(Stdio::null())
            // To a file rather than a pipe: a pipe nobody reads fills its buffer and
            // blocks the pipeline once it has logged 64 KiB, which would turn a
            // diagnostic into a hang. The file is in the fixture's temp dir and is read
            // back only when an assertion is about to fail.
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn rustcdc")
    }

    /// The `primary_key` array of every captured event, in order.
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
                    .map(|columns| {
                        columns
                            .iter()
                            .map(|c| c.as_str().unwrap_or_default().to_string())
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    /// Whatever the pipeline logged, for a failure message.
    fn stderr(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("pipeline.log"))
            .unwrap_or_else(|e| format!("<could not read pipeline log: {e}>"))
    }

    /// `POST /signals` with the write token, returning `(status, body)`.
    ///
    /// Goes over a real socket rather than through the in-process router, because the
    /// point of this file is the parts the unit suite cannot reach: the middleware stack,
    /// the token check, and — the thing being tested here — the handler actually reaching
    /// a live `RuntimeControl` instead of the `None` it holds in every unit test.
    fn post_signal(&self, body: serde_json::Value) -> (u16, String) {
        let port = self.admin_port.expect("fixture was built without admin");
        let response = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("signal runtime")
            .block_on(async move {
                reqwest::Client::new()
                    .post(format!("http://127.0.0.1:{port}/signals"))
                    .bearer_auth(ADMIN_TOKEN)
                    .json(&body)
                    .send()
                    .await
            })
            .expect("POST /signals");

        let status = response.status().as_u16();
        let body = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("body runtime")
            .block_on(response.text())
            .unwrap_or_default();
        (status, body)
    }

    /// Block until the admin server answers, so a signal is not raced against startup.
    fn wait_for_admin(&self, within: Duration) {
        let port = self.admin_port.expect("fixture was built without admin");
        let deadline = Instant::now() + within;
        loop {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the admin server did not bind 127.0.0.1:{port} within {within:?}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
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

    /// Wait until at least `expected` events have been written, or fail with what arrived.
    ///
    /// The panic includes the child's stderr. Without it a pipeline that refused to start —
    /// a config error, a slot conflict, a refused connection — reports only "got 0 events",
    /// which is the same message as a pipeline that started and captured nothing. Those
    /// have completely different causes and the distinction was being thrown away.
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

/// Terminate the child and wait for it, so the replication slot is released.
///
/// PostgreSQL refuses a second `START_REPLICATION` while a walsender still holds the slot,
/// so a test that restarts the pipeline has to actually wait for the first one to exit —
/// not merely signal it.
fn shutdown(mut child: Child) {
    #[cfg(unix)]
    unsafe {
        libc_kill(child.id() as i32);
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

/// `SIGTERM` via `kill(1)` — no libc dependency for one signal.
#[cfg(unix)]
unsafe fn libc_kill(pid: i32) {
    let _ = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status();
}

/// Block until the pipeline's replication slot is ready for the DML that follows.
///
/// This replaces a fixed `sleep(6s)` that stood in for "the stream is established by now".
/// Six seconds is enough on an idle machine and is not enough on a loaded one: when slot
/// creation and `START_REPLICATION` took longer, the DML that followed predated the slot,
/// was never captured, and the test failed with "expected at least 3 events, got 0" —
/// pointing at the pipeline rather than at the fixture.
///
/// # What "ready" means depends on the transport, and getting that wrong is subtle
///
/// Under `streaming_replication` a walsender attaches and holds the slot, so
/// `pg_replication_slots.active` goes true and *stays* true. That is a clean edge to wait
/// for.
///
/// Under `sql_peek` there is no walsender at all: the connector calls
/// `pg_logical_slot_peek_binary_changes` over an ordinary SQL connection, so the slot is
/// active only for the instant of each peek and reads `f` in between. Waiting for `active`
/// there is waiting for a transient — it passes when the poll happens to be sampled
/// mid-peek and times out otherwise, which is a flaky test that blames the pipeline.
///
/// So the precondition is: the slot **exists** (both transports), and additionally is
/// **active** only where something holds it open.
fn wait_for_slot_ready(slot: &str, wal_transport: &str, within: Duration) {
    let needs_walsender = wal_transport == "streaming_replication";
    let deadline = Instant::now() + within;
    loop {
        let state = psql(&format!(
            "SELECT active FROM pg_replication_slots WHERE slot_name = '{slot}';"
        ));
        let exists = !state.is_empty();
        let ready = if needs_walsender {
            state == "t"
        } else {
            exists
        };
        if ready {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "replication slot '{slot}' was not ready within {within:?} (transport \
             {wal_transport}, pg_replication_slots reported {state:?}). Under \
             streaming_replication this means no walsender attached; under sql_peek it \
             means the slot was never created. Either way the DML that follows would be \
             invisible to the pipeline."
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn setup() -> bool {
    if !enabled() {
        eprintln!(
            "skipping: set RUSTCDC_INTEGRATION=1 (and have Docker running) to execute the \
             PostgreSQL integration suite"
        );
        return false;
    }
    start_postgres();
    provision_schema();
    true
}

/// Insert, update and delete are captured, in order, with full before-images.
///
/// The baseline: this is the first assertion in this repository that the PostgreSQL
/// connector captures anything at all.
#[test]
fn insert_update_and_delete_are_captured_in_order() {
    if !setup() {
        return;
    }

    for transport in ["streaming_replication", "sql_peek"] {
        let fixture = Fixture::new(transport, &format!("it_basic_{transport}"));
        let child = fixture.spawn();

        // The stream has to be established before the DML, or the changes predate the
        // slot and there is nothing to capture.
        wait_for_slot_ready(
            &format!("it_basic_{transport}"),
            transport,
            Duration::from_secs(60),
        );

        psql(
            "INSERT INTO public.orders VALUES (1,'one');
             UPDATE public.orders SET note='one-b' WHERE id=1;
             DELETE FROM public.orders WHERE id=1;",
        );

        let captured = fixture.wait_for(3, Duration::from_secs(30));
        shutdown(child);

        let ops: Vec<&str> = captured.iter().map(|(op, _, _)| op.as_str()).collect();
        assert_eq!(
            &ops[..3],
            &["insert", "update", "delete"],
            "[{transport}] operations must arrive in commit order: {captured:#?}"
        );
        assert_eq!(captured[1].2, "one-b", "[{transport}] update after-image");
        assert_eq!(
            captured[2].2, "one-b",
            "[{transport}] REPLICA IDENTITY FULL must give the delete a before-image"
        );

        psql("DELETE FROM public.orders;");
        drop(fixture);
    }

    stop_postgres();
}

/// A restart loses nothing and replays nothing.
///
/// **This test found a real defect on its first run.** Against rustcdc 0.10 a *clean*
/// shutdown redelivered the last transaction on every restart — deterministically, with
/// seconds of idle time beforehand — and under `sql_peek` it was re-emitted on every poll.
///
/// The cause was not a missed checkpoint on this side: `rustcdc inspect-checkpoint` showed
/// the stored LSN was *exactly* the last delivered event's LSN with a matching
/// `committed_event_count`. We reported it diagnosing `START_REPLICATION` as inclusive; the
/// symptom was right and the mechanism was not. PostgreSQL logical decoding filters at
/// **transaction** granularity — a change's own LSN always precedes its transaction's
/// commit record, so resuming from that LSN, or from `lsn + 1` as we suggested, replays the
/// whole transaction either way. rustcdc 0.11 resumes from the position *after* the commit
/// record via `StreamHandle::resume_offset_for`.
///
/// Both properties are now asserted exactly:
///
/// * **no gaps** — every row committed while the pipeline was down is captured, in commit
///   order. Violating this is data loss.
/// * **no replay** — the event count is exactly the number of rows committed.
#[test]
fn the_resume_position_does_not_lose_events() {
    if !setup() {
        return;
    }

    let fixture = Fixture::new("streaming_replication", "it_resume");
    let first = fixture.spawn();
    wait_for_slot_ready(
        "it_resume",
        "streaming_replication",
        Duration::from_secs(60),
    );

    psql("INSERT INTO public.orders VALUES (1,'before-restart');");
    fixture.wait_for(1, Duration::from_secs(30));
    shutdown(first);

    // Committed while nothing is capturing. The slot must retain these.
    psql("INSERT INTO public.orders VALUES (2,'while-down-a'),(3,'while-down-b');");

    let second = fixture.spawn();
    wait_for_slot_ready(
        "it_resume",
        "streaming_replication",
        Duration::from_secs(60),
    );
    psql("INSERT INTO public.orders VALUES (4,'after-restart');");

    let captured = fixture.wait_for(4, Duration::from_secs(30));
    shutdown(second);

    let notes: Vec<&str> = captured.iter().map(|(_, _, note)| note.as_str()).collect();
    for expected in [
        "before-restart",
        "while-down-a",
        "while-down-b",
        "after-restart",
    ] {
        assert!(
            notes.contains(&expected),
            "row '{expected}' was never captured — a restart lost committed data: {captured:#?}"
        );
    }

    // Order is still commit order for the rows that were queued during the outage.
    let position = |note: &str| notes.iter().position(|n| *n == note).expect("present");
    assert!(
        position("while-down-a") < position("while-down-b")
            && position("while-down-b") < position("after-restart"),
        "commit order was not preserved across the restart: {captured:#?}"
    );

    // **Exact.** rustcdc 0.11 resumes from the transaction boundary
    // (`StreamHandle::resume_offset_for`), so a restart replays nothing at all. This bound
    // was `<= 6` while 0.10 redelivered the last transaction on every restart; tightening
    // it is the point of having found that, and a loose bound here would let the
    // regression back in silently.
    assert_eq!(
        captured.len(),
        4,
        "a restart must replay nothing: 4 rows committed, {} events captured: {captured:#?}",
        captured.len()
    );

    psql("DELETE FROM public.orders;");
    drop(fixture);
    stop_postgres();
}

/// The exact upstream reproduction, kept as its own test.
///
/// Start, insert one row, idle, shut down cleanly, restart with **no new writes**. Against
/// rustcdc 0.10 the row arrived a second time, every time. This is the smallest shape that
/// distinguishes "resumes from the last delivered position" from "resumes from the next
/// one", and it is deliberately separate from the resume test above so a failure says which
/// property broke.
#[test]
fn a_clean_restart_redelivers_nothing() {
    if !setup() {
        return;
    }

    let fixture = Fixture::new("streaming_replication", "it_no_replay");
    let first = fixture.spawn();
    wait_for_slot_ready(
        "it_no_replay",
        "streaming_replication",
        Duration::from_secs(60),
    );

    psql("INSERT INTO public.orders VALUES (1,'only-row');");
    let after_first = fixture.wait_for(1, Duration::from_secs(30));
    assert_eq!(after_first.len(), 1);

    // Idle before the shutdown, so a redelivery cannot be blamed on a checkpoint that had
    // not yet been written.
    std::thread::sleep(Duration::from_secs(4));
    shutdown(first);

    let second = fixture.spawn();
    // Wait for the slot to be *active* before the settle window, not just for time to
    // pass. This assertion is about an absence — no redelivery — and an absence is
    // trivially satisfied by a pipeline that never started. Without this the test would
    // pass just as happily against a binary that refused to connect.
    wait_for_slot_ready(
        "it_no_replay",
        "streaming_replication",
        Duration::from_secs(60),
    );
    std::thread::sleep(Duration::from_secs(12));
    shutdown(second);

    let after_restart = fixture.captured();
    assert_eq!(
        after_restart.len(),
        1,
        "a clean restart with no new writes replayed {} extra event(s): {after_restart:#?}",
        after_restart.len() - 1
    );

    psql("DELETE FROM public.orders;");
    drop(fixture);
    stop_postgres();
}

/// A TLS-configured connector must refuse a server without TLS rather than downgrade.
///
/// rustcdc 0.10 made `TransportConfig::Tls` enforcing — `tokio-postgres` defaults to
/// `sslmode=prefer`, which silently falls back to an unencrypted connection. This
/// container runs with `ssl = off`, so a TLS transport must fail to connect. Nothing else
/// in this repository exercises that: it is a negative property, and the only way to check
/// it is against a server that really does refuse TLS.
#[test]
fn a_tls_transport_refuses_a_server_without_tls() {
    if !setup() {
        return;
    }

    let fixture = Fixture::new("streaming_replication", "it_tls");
    let tls_config = std::fs::read_to_string(&fixture.config)
        .expect("read config")
        .replace("mode = \"plaintext\"", "mode = \"tls\"");
    std::fs::write(&fixture.config, tls_config).expect("write tls config");

    let mut child = fixture.spawn();
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => break status,
            None if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(250)),
            None => {
                shutdown(child);
                panic!(
                    "a TLS-configured connector kept running against a server with ssl=off; \
                     it must fail rather than silently downgrade to plaintext"
                );
            }
        }
    };

    assert!(
        !status.success(),
        "the process exited cleanly against a server that cannot do TLS"
    );

    drop(fixture);
    stop_postgres();
}

/// `POST /signals` with `execute_snapshot` really backfills, through a live runtime.
///
/// # Why this test is the one that matters for `execute_snapshot`
///
/// The unit suite drives `POST /signals` through the real router, but it cannot give the
/// handler a `RuntimeControl` — the type is not constructible outside a live runtime. So
/// every unit assertion about `execute_snapshot` stops at "the signal was accepted and the
/// audit trail recorded it", and the lifecycle ones had to be rewritten against the
/// *aborted* path once the stub bridge was deleted. Nothing checked that the signal
/// reaches the snapshot driver and that rows come out. This does.
///
/// # What makes the assertion sound
///
/// The rows are committed **before** the pipeline starts, so they sit behind the
/// replication slot's creation point and the live stream can never deliver them. A row
/// that reaches the sink therefore came from the snapshot and nowhere else.
#[test]
fn an_on_demand_snapshot_backfills_rows_the_stream_could_never_deliver() {
    if !setup() {
        return;
    }

    // Committed before anything is streaming: unreachable except by snapshot.
    psql(
        "INSERT INTO public.orders VALUES
           (901,'backfill-a'),
           (902,'backfill-b');",
    );

    let fixture = Fixture::with_extras(
        "streaming_replication",
        "it_snapshot",
        Extras {
            admin_port: Some(18499),
            toml: "\n[incremental_snapshot]\ntables = []\nchunk_size = 2\n".to_string(),
        },
    );

    let child = fixture.spawn();
    fixture.wait_for_admin(Duration::from_secs(60));
    // The slot has to be streaming before the signal, or the snapshot's watermarks have
    // no stream to bracket.
    wait_for_slot_ready(
        "it_snapshot",
        "streaming_replication",
        Duration::from_secs(60),
    );

    let (status, body) = fixture.post_signal(serde_json::json!({
        "action_type": "execute_snapshot",
        "tables": ["public.orders"],
    }));
    assert!(
        (200..300).contains(&status),
        "POST /signals must be accepted, got {status}: {body}"
    );

    let captured = fixture.wait_for(2, Duration::from_secs(60));
    shutdown(child);

    let mut notes: Vec<String> = captured.iter().map(|(_, _, note)| note.clone()).collect();
    notes.sort();
    assert_eq!(
        notes,
        vec!["backfill-a".to_string(), "backfill-b".to_string()],
        "the on-demand snapshot must deliver the pre-existing rows: {captured:#?}"
    );

    drop(fixture);
    stop_postgres();
}

/// A `table_conditions` filter restricts which rows a startup backfill reads.
///
/// # Why the startup path and not the on-demand one
///
/// `table_conditions` applies only to tables the driver resolves in
/// `IncrementalSnapshotDriver::new` — at startup, or when adopting an unfinished table
/// from a checkpoint. `enqueue_tables`, which services `execute_snapshot`, pushes the
/// resolved table with no condition, and the driver does not retain the config, so it
/// structurally cannot apply one. The first version of this test asserted the filter on
/// the on-demand path and failed with all three rows delivered; that is an upstream defect
/// (reported in `FEEDBACK_RUSTCDC.md`), and `IncrementalSnapshotConfig::validate` now
/// refuses the configuration that would walk into it.
///
/// # What makes the assertion sound
///
/// All three rows are committed before the pipeline starts, so the stream cannot deliver
/// any of them. Row 900 is excluded by the filter. Removing `with_table_condition` from
/// `src/commands/run.rs` delivers three rows and fails the final assertion — the wait
/// deliberately settles before counting, so a late third row is caught rather than missed
/// by a `>= 2` bound.
#[test]
fn a_startup_backfill_reads_only_the_rows_its_filter_selects() {
    if !setup() {
        return;
    }

    psql(
        "INSERT INTO public.orders VALUES
           (900,'excluded-by-filter'),
           (901,'backfill-a'),
           (902,'backfill-b');",
    );

    let fixture = Fixture::with_extras(
        "streaming_replication",
        "it_snapshot_filter",
        Extras {
            admin_port: None,
            toml: r#"
[incremental_snapshot]
tables = ["public.orders"]
chunk_size = 2

[incremental_snapshot.table_conditions]
"public.orders" = "t.id >= 901"
"#
            .to_string(),
        },
    );

    let child = fixture.spawn();
    fixture.wait_for(2, Duration::from_secs(60));
    // Settle, so an excluded row arriving late is caught rather than raced past.
    std::thread::sleep(Duration::from_secs(3));
    let captured = fixture.captured();
    shutdown(child);

    let mut notes: Vec<String> = captured.iter().map(|(_, _, note)| note.clone()).collect();
    notes.sort();
    assert_eq!(
        notes,
        vec!["backfill-a".to_string(), "backfill-b".to_string()],
        "the backfill must read exactly the filtered rows: {captured:#?}"
    );

    drop(fixture);
    stop_postgres();
}

/// The primary key of a `REPLICA IDENTITY FULL` table is its **primary key**, not every column.
///
/// # Why this needs a live server
///
/// pgoutput's `RELATION` message flags each column as part of the *replica identity*, and
/// under `REPLICA IDENTITY FULL` PostgreSQL sets that flag on every column — its own source
/// says "all columns are sent as part of key". rustcdc read the flag as "part of the primary
/// key" until 0.12, so every streamed event from a `FULL` table claimed a key of the whole
/// row. Nothing short of a real pgoutput stream produces that message.
///
/// This fixture has used `REPLICA IDENTITY FULL` from the start — it is what gives `DELETE`
/// a full before-image — so it was capturing the defect all along without asserting on it. A
/// manual run against 0.11 emitted `"primary_key":["id","note"]` for a table whose key is
/// `id`.
///
/// # What it costs when it is wrong
///
/// The Kafka message key is derived from these columns, so:
///
/// * the key changed whenever *any* column changed, which means a log-compacted topic could
///   never collapse a row's history, and one row's versions hashed to different partitions —
///   so per-key ordering, the property partitioning exists to provide, did not hold;
/// * it disagreed with the snapshot phase, which reads the real key from the catalog, so the
///   same row was keyed one way while being snapshotted and another way while streaming.
///
/// Both are silent. Nothing errors; the data just stops being addressable.
#[test]
fn a_replica_identity_full_table_reports_only_its_real_primary_key() {
    if !setup() {
        return;
    }

    let fixture = Fixture::new("streaming_replication", "it_pkey");
    let child = fixture.spawn();
    wait_for_slot_ready("it_pkey", "streaming_replication", Duration::from_secs(60));

    psql(
        "INSERT INTO public.orders VALUES (1,'one');
         UPDATE public.orders SET note='one-b' WHERE id=1;
         DELETE FROM public.orders WHERE id=1;",
    );

    fixture.wait_for(3, Duration::from_secs(30));
    shutdown(child);

    let keys = fixture.primary_keys();
    assert!(!keys.is_empty(), "no events captured");
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            key,
            &vec!["id".to_string()],
            "event {index} reported primary key {key:?}; `public.orders` has one key column \
             (`id`) and REPLICA IDENTITY FULL must not widen it to the whole row"
        );
    }

    drop(fixture);
    stop_postgres();
}

/// Silence the unused-field warning: `dir` is held to keep the temp directory alive.
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = &self.dir;
    }
}

/// Keep `Path` imported for the signature above without a stray warning.
#[allow(dead_code)]
fn _path_marker(_: &Path) {}
