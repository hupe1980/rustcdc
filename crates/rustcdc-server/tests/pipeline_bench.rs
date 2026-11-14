//! `cargo xtask pipeline-bench`: rows per second and latency from a real PostgreSQL, through the
//! release `rustcdc` binary, to a real Apache Kafka broker — and when one large transaction's
//! rows arrive relative to its commit.
//!
//! Ignored by default and run by nobody else: the numbers belong to the machine that produced
//! them, and the card printed at the end says which machine that was. It fails, rather than
//! reporting a rate, when any row never reaches the broker.
#![allow(clippy::print_stdout)]

#[path = "../../rustcdc/tests/support/artifacts.rs"]
mod artifacts;
#[path = "../../rustcdc/tests/support/matrix.rs"]
mod matrix;

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PG: &str = "rustcdc-bench-postgres";
const PG_PORT: u16 = 15434;
const BROKER: &str = "rustcdc-bench-kafka";
const BROKER_PORT: u16 = 19094;
const PG_PASSWORD: &str = "bench";
const STATE_TOPIC: &str = "__rustcdc_bench_state";

/// `cargo xtask pipeline-bench --smoke`: the harness as a correctness check.
fn smoke() -> bool {
    std::env::var_os("PIPELINE_BENCH_SMOKE").is_some()
}

fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as i64)
}

fn docker(args: &[&str]) -> std::process::Output {
    Command::new("docker").args(args).output().expect("docker")
}

fn psql(sql: &str) -> String {
    use std::io::Write as _;
    let mut child = Command::new("docker")
        .args([
            "exec",
            "-i",
            PG,
            "psql",
            "-U",
            "postgres",
            "-d",
            "bench",
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
        .expect("stdin")
        .write_all(sql.as_bytes())
        .expect("write sql");
    let out = child.wait_with_output().expect("psql");
    assert!(
        out.status.success(),
        "psql failed:\n{sql}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Removes the containers however the run ends, so a failed run does not hold the ports.
struct Containers;

impl Drop for Containers {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", PG, BROKER]);
    }
}

fn start_containers() -> Containers {
    let containers = Containers;
    let _ = docker(&["rm", "-f", PG, BROKER]);
    let pg = docker(&[
        "run",
        "-d",
        "--name",
        PG,
        "-e",
        &format!("POSTGRES_PASSWORD={PG_PASSWORD}"),
        "-e",
        "POSTGRES_DB=bench",
        "-p",
        &format!("{PG_PORT}:5432"),
        &format!("postgres:{}", matrix::full(matrix::Engine::Postgres)),
        "-c",
        "wal_level=logical",
        "-c",
        "max_replication_slots=8",
        "-c",
        "max_wal_senders=8",
    ]);
    assert!(
        pg.status.success(),
        "postgres: {}",
        String::from_utf8_lossy(&pg.stderr)
    );
    let port = BROKER_PORT.to_string();
    // Apache Kafka, not Redpanda: the `kafka_topic` state backend that effectively_once
    // needs reads `min.insync.replicas` from the topic's config, and Redpanda does not report it.
    let broker_run = docker(&[
        "run",
        "-d",
        "--name",
        BROKER,
        "-p",
        &format!("{port}:{port}"),
        "-e",
        "KAFKA_NODE_ID=1",
        "-e",
        "KAFKA_PROCESS_ROLES=broker,controller",
        "-e",
        "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
        "-e",
        &format!("KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:{port},CONTROLLER://0.0.0.0:9093"),
        "-e",
        &format!("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{port}"),
        "-e",
        "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "-e",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "-e",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
        "-e",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
        "-e",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
        &format!("apache/kafka:{}", matrix::full(matrix::Engine::Kafka)),
    ]);
    assert!(
        broker_run.status.success(),
        "kafka: {}",
        String::from_utf8_lossy(&broker_run.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(90);
    while !docker(&[
        "exec",
        PG,
        "pg_isready",
        "-h",
        "127.0.0.1",
        "-U",
        "postgres",
        "-d",
        "bench",
    ])
    .status
    .success()
    {
        assert!(Instant::now() < deadline, "postgres never became ready");
        std::thread::sleep(Duration::from_millis(500));
    }
    std::thread::sleep(Duration::from_secs(1));
    psql(
        "CREATE TABLE bench (id BIGINT PRIMARY KEY, written_us BIGINT NOT NULL, n INT NOT NULL, payload TEXT NOT NULL);
         CREATE PUBLICATION bench_pub FOR TABLE bench;
         CREATE ROLE cdc WITH REPLICATION LOGIN PASSWORD 'bench';
         GRANT SELECT ON bench TO cdc;
         -- One call writes every row, committing every `per_txn`: the writer is PostgreSQL itself,
         -- so the number is the pipeline's and not docker exec's.
         CREATE PROCEDURE load(first BIGINT, rows BIGINT, per_txn INT) LANGUAGE plpgsql AS $$
         DECLARE i BIGINT := first;
         BEGIN
           WHILE i < first + rows LOOP
             INSERT INTO bench SELECT g, (extract(epoch FROM clock_timestamp()) * 1e6)::bigint, 0, repeat('x', 200)
               FROM generate_series(i, least(i + per_txn, first + rows) - 1) g;
             COMMIT;
             i := i + per_txn;
           END LOOP;
         END $$;",
    );
    containers
}

fn broker() -> String {
    format!("127.0.0.1:{BROKER_PORT}")
}

fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(future)
}

fn wait_for_broker() {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let ok = block_on(async {
            let client = krafka::Kafka::builder(broker())
                .connect_timeout(Duration::from_secs(3))
                .connect()
                .await
                .ok()?;
            client
                .admin()
                .describe_cluster(krafka::admin::DescribeClusterOptions::default())
                .await
                .ok()
        });
        if ok.is_some() {
            return;
        }
        assert!(Instant::now() < deadline, "kafka never became ready");
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// The sink's preflight refuses a missing topic, as it should in production.
fn create_topic(topic: &str, configs: &[(&str, &str)]) {
    block_on(async {
        let admin = krafka::Kafka::builder(broker())
            .connect()
            .await
            .expect("admin client")
            .admin();
        let results = admin
            .create_topics(
                vec![{
                    let mut spec =
                        krafka::admin::NewTopic::new(topic, 1, 1).expect("valid topic spec");
                    spec.configs = configs
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                    spec
                }],
                krafka::admin::CreateTopicsOptions::default().timeout(Duration::from_secs(20)),
            )
            .await
            .expect("create_topics");
        for (name, result) in results {
            result.unwrap_or_else(|error| panic!("failed to create {name}: {error}"));
        }
        // Kafka acknowledges creation before the topic is in every broker's metadata.
        let deadline = Instant::now() + Duration::from_secs(30);
        while !admin
            .list_topics(krafka::admin::ListTopicsOptions::default())
            .await
            .is_ok_and(|topics| topics.iter().any(|t| t == topic))
        {
            assert!(Instant::now() < deadline, "{topic} never became visible");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
}

/// What-if settings: `;`-separated TOML lines for `[runtime]` and `[source.postgres]`. Unset
/// measures the defaults; the card prints whatever was set, so a tuned number is never mistaken
/// for the default one.
const OVERRIDES: [&str; 2] = ["PIPELINE_BENCH_RUNTIME", "PIPELINE_BENCH_POSTGRES"];

fn overrides(var: &str) -> String {
    std::env::var(var).unwrap_or_default().replace(';', "\n")
}

/// One delivery contract's run: the server's config, and how a consumer must read it.
struct Contract {
    name: &'static str,
    extra: String,
    read_committed: bool,
}

fn contracts() -> Vec<Contract> {
    vec![
        Contract {
            name: "at_least_once",
            extra: String::new(),
            read_committed: false,
        },
        Contract {
            name: "effectively_once",
            extra: format!(
                "delivery_mode = \"transactional\"\ntransactional_id = \"rustcdc-bench\"\n\n\
                 [state.backend.kafka_topic]\nbrokers = \"{}\"\ntopic = \"{STATE_TOPIC}\"\ndurability_profile = \"development\"\nmin_replication_factor = 1\nmin_insync_replicas = 1\n",
                broker()
            ),
            read_committed: true,
        },
    ]
}

fn spawn_server(binary: &str, dir: &std::path::Path, contract: &Contract, topic: &str) -> Child {
    let (sink_extra, state_extra) = match contract.extra.split_once("\n\n") {
        Some((sink, state)) => (sink.to_string(), state.to_string()),
        None => (contract.extra.clone(), String::new()),
    };
    let config = format!(
        r#"api_version = "v1"
delivery_contract = "{name}"

[source]
require_primary = false

[source.postgres]
host = "127.0.0.1"
port = {PG_PORT}
user = "cdc"
password = {{ env = "RUSTCDC_BENCH_PG_PASSWORD" }}
database = "bench"
replication_slot_name = "bench_{name}"
publication_name = "bench_pub"
create_replication_slot_if_missing = true
table_include_list = ["public.bench"]
{postgres}

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "{broker}"
topic = "{topic}"
{sink_extra}

[state]
dir = "{state}"
{state_extra}

[runtime]
{runtime}

[admin]
enabled = false
"#,
        name = contract.name,
        broker = broker(),
        state = dir.join("state").display(),
        runtime = overrides(OVERRIDES[0]),
        postgres = overrides(OVERRIDES[1]),
    );
    let path = dir.join("cdc.toml");
    std::fs::write(&path, config).expect("write config");
    if !contract.extra.is_empty() {
        create_topic(
            STATE_TOPIC,
            &[("cleanup.policy", "compact"), ("min.insync.replicas", "1")],
        );
        let seeded = Command::new(binary)
            .args(["init-state", "--config-file", path.to_str().unwrap()])
            .env("RUSTCDC_BENCH_PG_PASSWORD", PG_PASSWORD)
            .output()
            .expect("init-state");
        assert!(
            seeded.status.success(),
            "init-state: {}",
            String::from_utf8_lossy(&seeded.stderr)
        );
    }
    let log = std::fs::File::create(dir.join("server.log")).expect("log");
    Command::new(binary)
        .args(["run", "--config-file", path.to_str().unwrap()])
        .env("RUSTCDC_BENCH_PG_PASSWORD", PG_PASSWORD)
        .stdout(log.try_clone().expect("log"))
        .stderr(log)
        .spawn()
        .expect("spawn rustcdc")
}

/// The end of rustcdc's log for a failure message, with the whole log kept under `target/`:
/// the tempdir is gone once the test panics.
fn log_tail(dir: &std::path::Path) -> String {
    let log = std::fs::read_to_string(dir.join("server.log")).unwrap_or_default();
    let kept = artifacts::target_dir().join("pipeline-bench-server.log");
    let _ = std::fs::write(&kept, &log);
    let lines: Vec<&str> = log.lines().collect();
    format!(
        "{}\n(whole log: {})",
        lines[lines.len().saturating_sub(15)..].join("\n"),
        kept.display()
    )
}

fn wait_for_slot(server: &mut Child, dir: &std::path::Path, contract: &Contract) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let sql = format!(
        "SELECT active FROM pg_replication_slots WHERE slot_name = 'bench_{}'",
        contract.name
    );
    while psql(&sql) != "t" {
        if let Ok(Some(status)) = server.try_wait() {
            panic!(
                "rustcdc exited ({status}) before streaming for {}:\n{}",
                contract.name,
                log_tail(dir)
            );
        }
        assert!(
            Instant::now() < deadline,
            "rustcdc never started streaming for {}:\n{}",
            contract.name,
            log_tail(dir)
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// What the consumer saw: per id and operation, the row's write time (PostgreSQL clock, µs) and
/// its arrival (host clock, µs). The first arrival of each wins; at-least-once may repeat a row.
type Arrivals = HashMap<(i64, String), (i64, i64)>;

/// First arrivals per row, and how many arrived again: a duplicate at a `read_committed`
/// consumer is the failure `effectively_once` exists to prevent.
struct Consumed {
    arrivals: Arrivals,
    duplicates: usize,
}

fn consume(
    topic: String,
    read_committed: bool,
    wanted: usize,
    op: &'static str,
    ready: mpsc::Sender<()>,
) -> Consumed {
    block_on(async move {
        let mut builder = krafka::Kafka::builder(broker())
            .connect()
            .await
            .expect("consumer connects")
            .consumer(format!("bench-{topic}-{op}"))
            .auto_offset_reset(krafka::consumer::AutoOffsetReset::Earliest)
            .max_poll_records(5000);
        if read_committed {
            builder = builder.isolation_level(krafka::consumer::IsolationLevel::ReadCommitted);
        }
        let consumer = builder.build().await.expect("consumer builds");
        let deadline = Instant::now() + Duration::from_secs(60);
        while consumer.subscribe([topic.as_str()]).await.is_err() {
            assert!(Instant::now() < deadline, "subscribe never succeeded");
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        let _ = ready.send(());
        let mut seen: Arrivals = HashMap::new();
        let mut duplicates = 0;
        let mut last_progress = Instant::now();
        // Read on briefly once every row has arrived, so a duplicate sent after the last
        // first arrival is still counted.
        let mut complete_at: Option<Instant> = None;
        loop {
            if seen.len() >= wanted {
                let since = *complete_at.get_or_insert_with(Instant::now);
                if since.elapsed() > Duration::from_millis(1500) {
                    break;
                }
            } else if last_progress.elapsed() > Duration::from_secs(60) {
                break;
            }
            let records = match consumer.poll(Duration::from_millis(200)).await {
                Ok(records) => records,
                Err(error) if error.is_retriable() => continue,
                Err(error) => panic!("poll: {error}"),
            };
            let at = now_us();
            for record in records {
                let Some(value) = record.value else { continue };
                let Ok(event) = serde_json::from_slice::<serde_json::Value>(&value) else {
                    continue;
                };
                let row_op = event["op"].as_str().unwrap_or_default();
                if row_op != op {
                    continue;
                }
                let after = &event["after"];
                let id = after["id"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| after["id"].as_i64());
                let written = after["written_us"]
                    .as_i64()
                    .or_else(|| after["written_us"].as_str().and_then(|s| s.parse().ok()));
                // `n` counts a row's updates, so each pass of a long transaction is its own arrival.
                let n = after["n"]
                    .as_i64()
                    .or_else(|| after["n"].as_str().and_then(|s| s.parse().ok()))
                    .unwrap_or(0);
                if let (Some(id), Some(written)) = (id, written) {
                    match seen.entry((id, format!("{row_op}:{n}"))) {
                        std::collections::hash_map::Entry::Occupied(_) => duplicates += 1,
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            slot.insert((written, at));
                        }
                    }
                    last_progress = Instant::now();
                }
            }
        }
        Consumed {
            arrivals: seen,
            duplicates,
        }
    })
}

/// PostgreSQL's clock minus the host's, from the probe with the smallest round trip.
fn clock_offset() -> (i64, i64) {
    (0..5)
        .map(|_| {
            let before = now_us();
            let pg: f64 = psql("SELECT extract(epoch FROM clock_timestamp())")
                .parse()
                .expect("epoch");
            let after = now_us();
            ((pg * 1e6) as i64 - (before + after) / 2, after - before)
        })
        .min_by_key(|(_, rtt)| *rtt)
        .expect("a probe")
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

#[derive(serde::Serialize)]
struct Run {
    contract: &'static str,
    rows: u64,
    rows_per_second: f64,
    write_seconds: f64,
    latency_ms_p50: f64,
    latency_ms_p95: f64,
    latency_ms_p99: f64,
    duplicates: usize,
}

#[derive(serde::Serialize)]
struct Bulk {
    rows: u64,
    passes: u64,
    transaction_seconds: f64,
    first_after_commit_seconds: f64,
    last_after_commit_seconds: f64,
    primary_streamed_bytes: i64,
    primary_spilled_bytes: i64,
}

/// The primary's `(stream_bytes, spill_bytes)` for a slot, once decoding has reported them.
fn slot_bytes(slot: &str) -> (i64, i64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let row = psql(&format!(
            "SELECT stream_bytes || ' ' || spill_bytes FROM pg_stat_replication_slots WHERE slot_name = '{slot}'"
        ));
        let mut fields = row
            .split_whitespace()
            .map(|f| f.parse::<i64>().unwrap_or(0));
        let (streamed, spilled) = (fields.next().unwrap_or(0), fields.next().unwrap_or(0));
        if streamed + spilled > 0 || Instant::now() > deadline {
            return (streamed, spilled);
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn machine() -> String {
    let sh = |cmd: &str| {
        Command::new("sh")
            .args(["-c", cmd])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default()
    };
    let cpu = sh(
        "sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2",
    );
    let cores = sh("sysctl -n hw.ncpu 2>/dev/null || nproc");
    let os = sh("uname -sr");
    format!("{}, {cores} cores · {os}", cpu.trim())
}

#[test]
#[ignore = "run by `cargo xtask pipeline-bench`"]
fn pipeline_bench() {
    let Ok(binary) = std::env::var("PIPELINE_BENCH_BINARY") else {
        panic!(
            "run `cargo xtask pipeline-bench`: it builds the binary the image ships and points this at it"
        );
    };
    let rows = knob("PIPELINE_BENCH_ROWS", 200_000);
    let per_txn = knob("PIPELINE_BENCH_PER_TXN", 1_000);
    let bulk_rows = knob("PIPELINE_BENCH_BULK_ROWS", 100_000).min(rows);
    // Three passes over the bulk rows pass the default 64 MB `logical_decoding_work_mem`.
    // Zero skips the long transaction (the smoke run).
    let passes = knob("PIPELINE_BENCH_LONG_PASSES", 3);
    let long_seconds = knob("PIPELINE_BENCH_LONG_SECONDS", 20);

    let _containers = start_containers();
    wait_for_broker();
    let pg_version = psql("SHOW server_version");
    let (offset_us, probe_rtt_us) = clock_offset();

    let mut runs = Vec::new();
    let mut bulk = None;
    for (index, contract) in contracts().iter().enumerate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let topic = format!("bench-{}", contract.name.replace('_', "-"));
        create_topic(&topic, &[]);
        let mut server = spawn_server(&binary, dir.path(), contract, &topic);
        wait_for_slot(&mut server, dir.path(), contract);

        let first = 1 + index as i64 * rows as i64;
        let (ready_tx, ready_rx) = mpsc::channel();
        let consumer = {
            let topic = topic.clone();
            let read_committed = contract.read_committed;
            std::thread::spawn(move || {
                consume(topic, read_committed, rows as usize, "insert", ready_tx)
            })
        };
        ready_rx
            .recv_timeout(Duration::from_secs(90))
            .expect("consumer subscribed");

        let started = now_us();
        psql(&format!("CALL load({first}, {rows}, {per_txn})"));
        let written = now_us();
        let Consumed {
            arrivals: seen,
            duplicates,
        } = consumer.join().expect("consumer");
        assert!(
            !contract.read_committed || duplicates == 0,
            "{}: {duplicates} rows reached the read_committed consumer more than once. \
             rustcdc's log ends:\n{}",
            contract.name,
            log_tail(dir.path())
        );

        let mine: Vec<_> = seen
            .iter()
            .filter(|((id, _), _)| *id >= first && *id < first + rows as i64)
            .collect();
        let missing = rows as usize - mine.len();
        assert_eq!(
            missing,
            0,
            "{}: {missing} of {rows} rows never reached the broker. rustcdc's log ends:\n{}",
            contract.name,
            log_tail(dir.path())
        );
        let last_arrival = mine.iter().map(|(_, (_, at))| *at).max().expect("rows");
        let mut latency: Vec<f64> = mine
            .iter()
            .map(|(_, (w, at))| (*at - (*w - offset_us)) as f64 / 1000.0)
            .collect();
        latency.sort_by(f64::total_cmp);
        runs.push(Run {
            contract: contract.name,
            rows,
            rows_per_second: rows as f64 / ((last_arrival - started) as f64 / 1e6),
            write_seconds: (written - started) as f64 / 1e6,
            latency_ms_p50: percentile(&latency, 50.0),
            latency_ms_p95: percentile(&latency, 95.0),
            latency_ms_p99: percentile(&latency, 99.0),
            duplicates,
        });

        if index == 0 && passes > 0 {
            // One long transaction: every bulk row updated `passes` times, held open between
            // passes. Under v2 the primary streams it while it runs; under v1 it spills it.
            let slot = format!("bench_{}", contract.name);
            psql(&format!("SELECT pg_stat_reset_replication_slot('{slot}')"));
            let wanted = (bulk_rows * passes) as usize;
            let (ready_tx, ready_rx) = mpsc::channel();
            let consumer = {
                let topic = topic.clone();
                std::thread::spawn(move || consume(topic, false, wanted, "update", ready_tx))
            };
            ready_rx
                .recv_timeout(Duration::from_secs(90))
                .expect("consumer subscribed");
            let pause = long_seconds as f64 / passes as f64;
            let began = now_us();
            psql(&format!(
                "DO $$ BEGIN FOR p IN 1..{passes} LOOP \
                   UPDATE bench SET n = n + 1, written_us = (extract(epoch FROM clock_timestamp()) * 1e6)::bigint WHERE id < {}; \
                   PERFORM pg_sleep({pause}); \
                 END LOOP; END $$",
                first + bulk_rows as i64
            ));
            let committed = now_us();
            let seen = consumer.join().expect("consumer").arrivals;
            assert_eq!(seen.len(), wanted, "the long transaction lost rows");
            let (streamed, spilled) = slot_bytes(&slot);
            assert!(
                streamed + spilled > 0,
                "the long transaction neither streamed nor spilled on the primary, so it never \
                 passed logical_decoding_work_mem and measured nothing; raise \
                 PIPELINE_BENCH_BULK_ROWS or PIPELINE_BENCH_LONG_PASSES"
            );
            let arrivals: Vec<i64> = seen.values().map(|(_, at)| *at).collect();
            bulk = Some(Bulk {
                rows: bulk_rows,
                passes,
                transaction_seconds: (committed - began) as f64 / 1e6,
                first_after_commit_seconds: (arrivals.iter().min().unwrap() - committed) as f64
                    / 1e6,
                last_after_commit_seconds: (arrivals.iter().max().unwrap() - committed) as f64
                    / 1e6,
                primary_streamed_bytes: streamed,
                primary_spilled_bytes: spilled,
            });
        }
        let _ = server.kill();
        let _ = server.wait();
    }

    let card = card(
        &runs,
        bulk.as_ref(),
        &pg_version,
        rows,
        per_txn,
        probe_rtt_us,
    );
    println!("\n{card}");
    let json = serde_json::json!({
        "date": chrono::Utc::now().to_rfc3339(),
        "machine": machine(),
        "rustcdc": env!("CARGO_PKG_VERSION"),
        "postgres": pg_version,
        "broker": format!("Apache Kafka {}", matrix::full(matrix::Engine::Kafka)),
        "rows_per_transaction": per_txn,
        "overrides": OVERRIDES.map(|var| (var, overrides(var))).iter().filter(|(_, v)| !v.is_empty()).cloned().collect::<HashMap<_, _>>(),
        "clock_probe_rtt_ms": probe_rtt_us as f64 / 1000.0,
        "runs": runs,
        "long_transaction": bulk,
    });
    let path = artifacts::target_dir().join("pipeline-bench.json");
    std::fs::write(&path, serde_json::to_string_pretty(&json).unwrap()).expect("write json");
    println!("JSON: {}\n", path.display());
}

fn card(
    runs: &[Run],
    bulk: Option<&Bulk>,
    pg: &str,
    rows: u64,
    per_txn: u64,
    rtt_us: i64,
) -> String {
    let n = |v: f64| {
        let s = format!("{:.0}", v);
        let b = s.as_bytes();
        let mut out = String::new();
        for (i, c) in b.iter().enumerate() {
            if i > 0 && (b.len() - i) % 3 == 0 {
                out.push(' ');
            }
            out.push(*c as char);
        }
        out
    };
    let ms = |v: f64| {
        if v < 10.0 {
            format!("{v:.1} ms")
        } else {
            format!("{v:.0} ms")
        }
    };
    let mut out = format!(
        "rustcdc pipeline bench · {} · {} · rustcdc {}\n\
         PostgreSQL {pg} → rustcdc ({}) → Apache Kafka {} · {} rows, {} per transaction\n\n\
         {:<18}{:>10}{:>10}{:>10}{:>10}   write → broker\n",
        chrono::Utc::now().format("%Y-%m-%d"),
        machine(),
        env!("CARGO_PKG_VERSION"),
        if smoke() {
            "debug, smoke run: correctness, not a measurement"
        } else {
            "release"
        },
        matrix::full(matrix::Engine::Kafka),
        n(rows as f64),
        n(per_txn as f64),
        "",
        "rows/s",
        "p50",
        "p95",
        "p99",
    );
    for run in runs {
        out.push_str(&format!(
            "{:<18}{:>10}{:>10}{:>10}{:>10}\n",
            run.contract,
            n(run.rows_per_second),
            ms(run.latency_ms_p50),
            ms(run.latency_ms_p95),
            ms(run.latency_ms_p99),
        ));
    }
    out.push_str(&format!(
        "Rows that arrived more than once: {}\n",
        runs.iter()
            .map(|run| format!("{} {}", run.contract, run.duplicates))
            .collect::<Vec<_>>()
            .join(" · ")
    ));
    if let Some(b) = bulk {
        let mb = |bytes: i64| format!("{:.0} MB", bytes as f64 / 1e6);
        out.push_str(&format!(
            "\nOne transaction of {} updates ({} rows × {}), open {:.1} s\n\
             \x20 primary                  streamed {}, spilled {} to its own disk\n\
             \x20 first row at the broker  {:+.1} s after the commit   (rows are released at the commit)\n\
             \x20 last row at the broker   {:+.1} s after the commit\n",
            n((b.rows * b.passes) as f64),
            n(b.rows as f64),
            b.passes,
            b.transaction_seconds,
            mb(b.primary_streamed_bytes),
            mb(b.primary_spilled_bytes),
            b.first_after_commit_seconds,
            b.last_after_commit_seconds,
        ));
    }
    out.push_str(&format!(
        "\nLatency: PostgreSQL's write time to the consumer's arrival, clocks aligned within {:.1} ms.\n\
         Reproduce: cargo xtask pipeline-bench{}",
        rtt_us as f64 / 2000.0,
        if smoke() { " --smoke" } else { "" }
    ));
    let set: Vec<String> = OVERRIDES
        .iter()
        .filter_map(|var| {
            std::env::var(var)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .map(|v| format!("{var}=\"{v}\""))
        })
        .collect();
    out.push_str(&if set.is_empty() {
        "  (default settings)".to_string()
    } else {
        format!(" {}", set.join(" "))
    });
    out
}
