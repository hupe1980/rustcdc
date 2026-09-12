//! Connector-level tests, driven through a scripted executor.
//!
//! The transport is a trait precisely so this is possible: everything from statement text
//! to event envelope is exercised here with no account, no network and no warehouse. What
//! these tests cannot establish is that Snowflake's server agrees with the statements —
//! that belongs to whoever runs it against a live account, and is stated as a gap rather
//! than implied away.

use std::sync::Mutex;

use super::*;

/// An executor that answers from a script and records what it was asked.
#[derive(Debug, Default)]
struct ScriptedExecutor {
    /// `CURRENT_TIMESTAMP()` answers, consumed in order; the last one repeats.
    clock: Mutex<Vec<u64>>,
    /// Result sets for non-clock statements, consumed in order.
    responses: Mutex<Vec<Result<SnowflakeResultSet>>>,
    statements: Mutex<Vec<String>>,
}

impl ScriptedExecutor {
    fn new(clock: Vec<u64>, responses: Vec<Result<SnowflakeResultSet>>) -> Arc<Self> {
        Arc::new(Self {
            clock: Mutex::new(clock),
            responses: Mutex::new(responses),
            statements: Mutex::new(Vec::new()),
        })
    }

    fn statements(&self) -> Vec<String> {
        self.statements.lock().expect("statements").clone()
    }
}

#[async_trait]
impl SnowflakeQueryExecutor for ScriptedExecutor {
    async fn query(&self, statement: &str) -> Result<SnowflakeResultSet> {
        self.statements
            .lock()
            .expect("statements")
            .push(statement.to_string());

        if statement.contains("EPOCH_NANOSECOND") {
            let mut clock = self.clock.lock().expect("clock");
            let value = if clock.len() > 1 {
                clock.remove(0)
            } else {
                *clock.first().unwrap_or(&0)
            };
            return Ok(SnowflakeResultSet::new(
                vec!["NOW_NANOS".into()],
                vec![vec![Some(value.to_string())]],
            ));
        }

        let mut responses = self.responses.lock().expect("responses");
        if responses.is_empty() {
            return Ok(SnowflakeResultSet::default());
        }
        responses.remove(0)
    }
}

fn changes_columns() -> Vec<String> {
    vec![
        "ID".into(),
        "NAME".into(),
        "METADATA$ACTION".into(),
        "METADATA$ISUPDATE".into(),
        "METADATA$ROW_ID".into(),
    ]
}

fn config() -> SnowflakeSourceConfig {
    SnowflakeSourceConfig::new("ANALYTICS", "PUBLIC")
        .with_tables(["ORDERS"])
        .with_primary_key("ORDERS", ["ID"])
}

fn cell(value: &str) -> Option<String> {
    Some(value.to_string())
}

#[tokio::test]
async fn a_window_reads_from_the_committed_end_to_the_servers_current_instant() {
    // The window contract in one test: the lower bound is the checkpointed position, the
    // upper bound comes from the *server*, and the position only moves to the upper bound
    // once the rows have been handed to the caller.
    let executor = ScriptedExecutor::new(
        vec![2_000],
        vec![Ok(SnowflakeResultSet::new(
            changes_columns(),
            vec![vec![
                cell("1"),
                cell("a"),
                cell("INSERT"),
                cell("false"),
                cell("r1"),
            ]],
        ))],
    );

    let mut stream = SnowflakeStreamHandle::new(config(), executor.clone(), 1_000).expect("stream");
    let events = stream.next_events(0).await.expect("poll");

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].source.offset, "2000");
    let statements = executor.statements();
    assert!(
        statements[1].contains("AT(TIMESTAMP => TO_TIMESTAMP_LTZ(1000, 9))")
            && statements[1].contains("END(TIMESTAMP => TO_TIMESTAMP_LTZ(2000, 9))"),
        "got: {}",
        statements[1]
    );
}

#[tokio::test]
async fn consecutive_windows_join_without_a_gap_or_an_overlap() {
    // The previous window's END becomes the next window's AT. Any other arithmetic either
    // loses the changes in the seam or re-reads the boundary forever.
    let executor = ScriptedExecutor::new(
        vec![2_000, 3_000],
        vec![
            Ok(SnowflakeResultSet::new(changes_columns(), vec![])),
            Ok(SnowflakeResultSet::new(changes_columns(), vec![])),
        ],
    );

    let mut stream = SnowflakeStreamHandle::new(config(), executor.clone(), 1_000).expect("stream");
    stream.next_events(0).await.expect("first poll");
    stream.next_events(0).await.expect("second poll");

    let statements = executor.statements();
    let windows: Vec<&String> = statements
        .iter()
        .filter(|statement| statement.contains("CHANGES"))
        .collect();
    assert!(windows[0].contains("TO_TIMESTAMP_LTZ(1000, 9)"));
    assert!(windows[0].contains("TO_TIMESTAMP_LTZ(2000, 9)"));
    assert!(
        windows[1].contains("AT(TIMESTAMP => TO_TIMESTAMP_LTZ(2000, 9))"),
        "the second window must open exactly where the first closed; got: {}",
        windows[1]
    );
}

#[tokio::test]
async fn a_clock_that_has_not_advanced_yields_nothing_rather_than_a_backwards_window() {
    // Two polls inside the same nanosecond are possible on a fast loop, and a server whose
    // clock steps back would otherwise produce a window with END before AT — which
    // Snowflake would reject, and which would rewind the position if it did not.
    let executor = ScriptedExecutor::new(vec![500], vec![]);
    let mut stream = SnowflakeStreamHandle::new(config(), executor.clone(), 1_000).expect("stream");

    let events = stream.next_events(0).await.expect("poll");
    assert!(events.is_empty());
    assert!(
        !executor.statements().iter().any(|s| s.contains("CHANGES")),
        "no window may be issued when the clock has not advanced"
    );

    let offset = stream.position_offset().expect("an offset");
    let saved = crate::checkpoint::SnowflakeOffset::from_bytes(&offset.encode().unwrap()).unwrap();
    assert_eq!(
        saved.window_end_nanos, 1_000,
        "the position must not move backwards to the stale clock reading"
    );
}

#[tokio::test]
async fn the_position_advances_only_after_every_selected_table_has_been_read() {
    // Advancing per table would skip the tables that had not been read yet when the next
    // poll started from the new position.
    let config = SnowflakeSourceConfig::new("DB", "SC")
        .with_tables(["A", "B"])
        .with_primary_key("A", ["ID"])
        .with_primary_key("B", ["ID"]);

    let executor = ScriptedExecutor::new(
        vec![2_000],
        vec![
            Ok(SnowflakeResultSet::new(
                changes_columns(),
                vec![vec![
                    cell("1"),
                    cell("a"),
                    cell("INSERT"),
                    cell("false"),
                    cell("r1"),
                ]],
            )),
            Ok(SnowflakeResultSet::new(
                changes_columns(),
                vec![vec![
                    cell("2"),
                    cell("b"),
                    cell("INSERT"),
                    cell("false"),
                    cell("r2"),
                ]],
            )),
        ],
    );

    let mut stream = SnowflakeStreamHandle::new(config, executor.clone(), 1_000).expect("stream");
    let events = stream.next_events(0).await.expect("poll");

    assert_eq!(events.len(), 2, "both tables' changes belong to one window");
    let windows: Vec<String> = executor
        .statements()
        .into_iter()
        .filter(|statement| statement.contains("CHANGES"))
        .collect();
    assert_eq!(windows.len(), 2);
    assert!(
        windows
            .iter()
            .all(|window| window.contains("AT(TIMESTAMP => TO_TIMESTAMP_LTZ(1000, 9))")),
        "every table in a window shares the window's bounds"
    );
}

#[tokio::test]
async fn an_excluded_table_is_never_queried() {
    let config = SnowflakeSourceConfig::new("DB", "SC")
        .with_tables(["ORDERS", "SECRETS"])
        .with_primary_key("ORDERS", ["ID"]);
    let mut config = config;
    config.table_exclude_list = vec!["sc.secrets".into()];

    let executor = ScriptedExecutor::new(
        vec![2_000],
        vec![Ok(SnowflakeResultSet::new(changes_columns(), vec![]))],
    );
    let mut stream = SnowflakeStreamHandle::new(config, executor.clone(), 1_000).expect("stream");
    stream.next_events(0).await.expect("poll");

    assert!(
        !executor
            .statements()
            .iter()
            .any(|statement| statement.contains("SECRETS")),
        "an excluded table must not reach the warehouse at all — not even to be filtered \
         afterwards, which would still bill for the scan and still read the data"
    );
}

#[tokio::test]
async fn a_stream_over_no_tables_is_refused_rather_than_polling_forever() {
    let mut config = config();
    config.table_include_list = vec!["other.nothing".into()];
    let executor = ScriptedExecutor::new(vec![1], vec![]);

    let error = SnowflakeStreamHandle::new(config, executor, 0)
        .expect_err("a stream that can never deliver is a configuration error");
    assert_eq!(error.kind(), crate::core::ErrorKind::Configuration);
}

#[tokio::test]
async fn a_retention_failure_is_reported_as_data_loss_not_a_retry() {
    // The distinction an operator acts on: retrying cannot bring the window back, and
    // restarting from now would skip it silently.
    let executor = ScriptedExecutor::new(
        vec![2_000],
        vec![Err(Error::SourceError(
            "Time travel data is not available for table ORDERS".into(),
        ))],
    );
    let mut stream = SnowflakeStreamHandle::new(config(), executor, 1_000).expect("stream");

    let error = stream.next_events(0).await.expect_err("the window is gone");
    let text = error.to_string();
    assert!(text.contains("data loss"), "got: {text}");
    assert!(text.contains("DATA_RETENTION_TIME_IN_DAYS"), "got: {text}");
    assert!(text.contains("Re-snapshot"), "got: {text}");
}

#[tokio::test]
async fn a_missing_change_tracking_grant_names_the_alter_statement() {
    let executor = ScriptedExecutor::new(
        vec![2_000],
        vec![Err(Error::SourceError(
            "Change tracking is not enabled on table 'ORDERS'".into(),
        ))],
    );
    let mut stream = SnowflakeStreamHandle::new(config(), executor, 1_000).expect("stream");

    let error = stream.next_events(0).await.expect_err("tracking is off");
    assert!(
        error.to_string().contains("SET CHANGE_TRACKING = TRUE"),
        "got: {error}"
    );
}

#[tokio::test]
async fn the_snapshot_pins_every_chunk_to_one_instant_and_the_stream_opens_there() {
    // The property that removes the watermark bracket: the snapshot reads the table
    // version at T and the stream's first window opens at T, so there is no overlap to
    // deduplicate and no gap to lose changes in.
    let executor = ScriptedExecutor::new(
        vec![5_000],
        vec![
            Ok(SnowflakeResultSet::new(
                vec!["ID".into(), "NAME".into()],
                vec![vec![cell("1"), cell("a")], vec![cell("2"), cell("b")]],
            )),
            Ok(SnowflakeResultSet::new(
                vec!["ID".into(), "NAME".into()],
                vec![],
            )),
        ],
    );

    let mut source = SnowflakeSource::new(config(), executor.clone()).expect("source");
    let mut snapshot = source.start_snapshot(&[]).await.expect("snapshot starts");

    let chunk = snapshot.next_chunk(2).await.expect("first chunk");
    assert_eq!(chunk.len(), 2);
    assert_eq!(chunk[0].op, crate::core::Operation::Read);
    assert!(chunk[0].snapshot.is_some());

    let stream = source.start_stream(None).await.expect("stream starts");
    let offset = stream.position_offset().expect("an offset");
    let saved = crate::checkpoint::SnowflakeOffset::from_bytes(&offset.encode().unwrap()).unwrap();
    assert_eq!(
        saved.window_end_nanos, 5_000,
        "the stream must open exactly at the instant the snapshot was pinned to"
    );

    assert!(
        executor
            .statements()
            .iter()
            .filter(|statement| statement.contains("ORDER BY"))
            .all(|statement| statement.contains("AT(TIMESTAMP => TO_TIMESTAMP_LTZ(5000, 9))")),
        "every chunk reads the same table version, however long the snapshot takes"
    );
}

#[tokio::test]
async fn the_snapshot_keyset_advances_from_the_last_row_of_the_previous_chunk() {
    let executor = ScriptedExecutor::new(
        vec![5_000],
        vec![
            Ok(SnowflakeResultSet::new(
                vec!["ID".into()],
                vec![vec![cell("1")], vec![cell("2")]],
            )),
            Ok(SnowflakeResultSet::new(vec!["ID".into()], vec![])),
        ],
    );

    let mut source = SnowflakeSource::new(config(), executor.clone()).expect("source");
    let mut snapshot = source.start_snapshot(&[]).await.expect("snapshot");
    snapshot.next_chunk(2).await.expect("first chunk");
    snapshot.next_chunk(2).await.expect("second chunk");

    let chunk_statements: Vec<String> = executor
        .statements()
        .into_iter()
        .filter(|statement| statement.contains("ORDER BY"))
        .collect();
    assert!(!chunk_statements[0].contains("WHERE"));
    assert!(
        chunk_statements[1].contains(r#"WHERE ("ID") > ('2')"#),
        "got: {}",
        chunk_statements[1]
    );
}

#[tokio::test]
async fn a_snapshot_of_a_table_with_no_declared_key_is_refused_with_the_reason() {
    let config = SnowflakeSourceConfig::new("DB", "SC").with_tables(["ORDERS"]);
    let executor = ScriptedExecutor::new(vec![5_000], vec![]);
    let mut source = SnowflakeSource::new(config, executor).expect("source");

    let error = match source.start_snapshot(&[]).await {
        Err(error) => error,
        Ok(_) => panic!("keyset pagination needs a key"),
    };
    assert!(error.to_string().contains("primary_keys"), "got: {error}");
    assert_eq!(error.kind(), crate::core::ErrorKind::Configuration);
}

#[tokio::test]
async fn a_null_key_column_stops_the_snapshot_loudly_rather_than_silently() {
    // `(a) > (NULL)` is unknown, so the next chunk returns nothing: the table would end
    // early and the snapshot would report success over a partial copy.
    let executor = ScriptedExecutor::new(
        vec![5_000],
        vec![Ok(SnowflakeResultSet::new(
            vec!["ID".into()],
            vec![vec![cell("1")], vec![None]],
        ))],
    );
    let mut source = SnowflakeSource::new(config(), executor).expect("source");
    let mut snapshot = source.start_snapshot(&[]).await.expect("snapshot");

    let error = snapshot
        .next_chunk(2)
        .await
        .expect_err("a NULL key cannot be a cursor");
    assert!(error.to_string().contains("NOT NULL"), "got: {error}");
}

#[tokio::test]
async fn resuming_from_another_databases_checkpoint_is_refused() {
    let executor = ScriptedExecutor::new(vec![9_000], vec![]);
    let mut source = SnowflakeSource::new(config(), executor).expect("source");

    let foreign = crate::checkpoint::SnowflakeOffset::new(1_000, "OTHER", "PUBLIC");
    let error = match source.start_stream(Some(&foreign)).await {
        Err(error) => error,
        Ok(_) => panic!("a checkpoint from a different database is not a resume point"),
    };
    assert!(error.to_string().contains("OTHER"), "got: {error}");
}

#[tokio::test]
async fn resuming_from_a_checkpoint_opens_the_window_there() {
    let executor = ScriptedExecutor::new(vec![9_000], vec![]);
    let mut source = SnowflakeSource::new(config(), executor).expect("source");

    let saved = crate::checkpoint::SnowflakeOffset::new(1_234, "ANALYTICS", "PUBLIC");
    let stream = source.start_stream(Some(&saved)).await.expect("resumes");
    let offset = stream.position_offset().expect("an offset");
    let read_back =
        crate::checkpoint::SnowflakeOffset::from_bytes(&offset.encode().unwrap()).unwrap();
    assert_eq!(read_back.window_end_nanos, 1_234);
}

#[tokio::test]
async fn a_non_snowflake_offset_is_refused_rather_than_reinterpreted() {
    let executor = ScriptedExecutor::new(vec![9_000], vec![]);
    let mut source = SnowflakeSource::new(config(), executor).expect("source");

    let foreign = crate::checkpoint::PostgresOffset {
        lsn: 42,
        slot_name: "slot".into(),
        incremental_snapshot: None,
    };
    let error = match source.start_stream(Some(&foreign)).await {
        Err(error) => error,
        Ok(_) => panic!("a postgres offset is not a snowflake window"),
    };
    assert!(error.to_string().contains("postgres"), "got: {error}");
}

#[test]
fn the_clock_is_parsed_as_an_integer_because_a_float_would_quantise_it() {
    // Epoch nanoseconds pass f64's exact-integer range in 2033. Parsing the window
    // boundary through a float would round it, and a rounded *upper* bound that lands
    // ahead of the true instant skips whatever committed in the gap.
    let beyond_f64 = 1_893_456_000_123_456_789_u64;
    assert_ne!(
        beyond_f64,
        (beyond_f64 as f64) as u64,
        "this test is only meaningful while the value is outside f64's exact range"
    );
    assert_eq!(beyond_f64.to_string().parse::<u64>().unwrap(), beyond_f64);
}

#[test]
fn configuration_validation_catches_what_would_otherwise_fail_at_the_warehouse() {
    let mut empty = config();
    empty.tables = Vec::new();
    assert!(empty.validate().is_err(), "no tables is not a pipeline");

    let mut zero_interval = config();
    zero_interval.poll_interval_ms = 0;
    assert!(
        zero_interval.validate().is_err(),
        "a zero interval spins the warehouse and bills for it"
    );

    let mut empty_key = config();
    empty_key.primary_keys.insert("ORDERS".into(), Vec::new());
    assert!(
        empty_key.validate().is_err(),
        "an empty key reads as 'this table has a key' everywhere it is consulted"
    );

    config().validate().expect("the ordinary case is accepted");
}

#[tokio::test]
async fn connect_runs_one_statement_so_a_broken_transport_fails_at_startup() {
    #[derive(Debug)]
    struct Broken;

    #[async_trait]
    impl SnowflakeQueryExecutor for Broken {
        async fn query(&self, _statement: &str) -> Result<SnowflakeResultSet> {
            Err(Error::SourceError("401 Unauthorized".into()))
        }
    }

    let source = SnowflakeSource::new(config(), Arc::new(Broken)).expect("source");
    let error = source
        .connect()
        .await
        .expect_err("a pipeline must not report itself healthy and deliver nothing");
    assert!(error.to_string().contains("key-pair"), "got: {error}");
}

#[tokio::test]
async fn save_position_reports_a_monotonic_committed_count() {
    // `FileCheckpoint` refuses a write whose committed-event count moves backwards. A
    // handle that reported zero here would fail on shutdown for any stream that had
    // already committed something, turning an orderly stop into a checkpoint error —
    // which is how this was caught, by comparing against what every other connector
    // passes rather than by the tests above, all of which passed.
    use crate::checkpoint::{Checkpoint, InMemoryCheckpoint};

    let executor = ScriptedExecutor::new(
        vec![2_000],
        vec![Ok(SnowflakeResultSet::new(
            changes_columns(),
            vec![
                vec![
                    cell("1"),
                    cell("a"),
                    cell("INSERT"),
                    cell("false"),
                    cell("r1"),
                ],
                vec![
                    cell("2"),
                    cell("b"),
                    cell("INSERT"),
                    cell("false"),
                    cell("r2"),
                ],
            ],
        ))],
    );

    let mut stream = SnowflakeStreamHandle::new(config(), executor, 1_000).expect("stream");
    let events = stream.next_events(0).await.expect("poll");
    assert_eq!(events.len(), 2);

    let mut checkpoint = InMemoryCheckpoint::default();
    stream
        .save_position(&mut checkpoint)
        .await
        .expect("an orderly shutdown must be able to record its position");
    assert_eq!(
        checkpoint.get_committed_count().await.expect("count"),
        2,
        "the count must reflect what was delivered, not zero"
    );
}

#[tokio::test]
async fn the_event_timestamp_is_the_window_bound_not_the_decode_time() {
    // `Event::source.timestamp` is what the runtime's replication-lag metric measures
    // against now(), and that metric is the "capture has fallen behind" alert. Stamping it
    // with the decode time made it read ~0 forever: a pipeline a full poll interval behind,
    // or stalled outright, would report itself perfectly current.
    let window_end_nanos = 1_700_000_060_000_000_000_u64;
    let executor = ScriptedExecutor::new(
        vec![window_end_nanos],
        vec![Ok(SnowflakeResultSet::new(
            changes_columns(),
            vec![vec![
                cell("1"),
                cell("a"),
                cell("INSERT"),
                cell("false"),
                cell("r1"),
            ]],
        ))],
    );

    let mut stream =
        SnowflakeStreamHandle::new(config(), executor, 1_700_000_000_000_000_000).expect("stream");
    let events = stream.next_events(0).await.expect("poll");

    assert_eq!(
        events[0].source.timestamp,
        window_end_nanos / 1_000_000,
        "the timestamp must be the window's upper bound, the tightest bound CHANGES allows"
    );
    assert_eq!(events[0].ts, events[0].source.timestamp);
}

#[tokio::test]
async fn the_final_snapshot_chunk_is_marked_as_last() {
    // A consumer materialising a snapshot into a staging table watches `is_last_chunk` to
    // know when to swap it in. It was never set, so that consumer waited forever.
    let executor = ScriptedExecutor::new(
        vec![5_000],
        vec![
            Ok(SnowflakeResultSet::new(
                vec!["ID".into()],
                vec![vec![cell("1")], vec![cell("2")]],
            )),
            Ok(SnowflakeResultSet::new(
                vec!["ID".into()],
                vec![vec![cell("3")]],
            )),
        ],
    );

    let mut source = SnowflakeSource::new(config(), executor).expect("source");
    let mut snapshot = source.start_snapshot(&[]).await.expect("snapshot");

    let first = snapshot.next_chunk(2).await.expect("first chunk");
    assert!(
        first.iter().all(|event| !event
            .snapshot
            .as_ref()
            .expect("snapshot metadata")
            .is_last_chunk),
        "a full chunk cannot be the last one — there may be more rows"
    );

    let last = snapshot.next_chunk(2).await.expect("second chunk");
    assert!(
        last.iter().all(|event| event
            .snapshot
            .as_ref()
            .expect("snapshot metadata")
            .is_last_chunk),
        "a short chunk of the final table ends the snapshot"
    );
    assert!(
        snapshot.next_chunk(2).await.expect("drained").is_empty(),
        "and nothing follows it"
    );
}

#[tokio::test]
async fn a_snapshot_of_an_unconfigured_table_is_refused_rather_than_silently_empty() {
    // The failure mode two earlier audits of this crate kept finding: an operator action
    // that reports success and does nothing. Here it is worse than usual, because
    // "snapshot completed" is the signal that a backfill has finished — so a typo in a
    // table name would look like an instant, successful backfill of zero rows.
    let executor = ScriptedExecutor::new(vec![5_000], vec![]);
    let mut source = SnowflakeSource::new(config(), executor).expect("source");

    let error = match source.start_snapshot(&["TYPO"]).await {
        Err(error) => error,
        Ok(_) => panic!("a snapshot of a table that cannot be read must not report success"),
    };
    assert_eq!(error.kind(), crate::core::ErrorKind::Configuration);
    assert!(error.to_string().contains("TYPO"), "got: {error}");
    assert!(
        error.to_string().contains("ORDERS"),
        "the message must name what *is* selectable; got: {error}"
    );
}

#[tokio::test]
async fn an_excluded_table_cannot_be_snapshotted_by_naming_it_explicitly() {
    // The include/exclude lists bound what may leave the database. An explicit snapshot
    // request is not an exemption from that, and must not quietly become one.
    let mut config = config();
    config.tables = vec!["ORDERS".into(), "SECRETS".into()];
    config.table_exclude_list = vec!["public.secrets".into()];

    let executor = ScriptedExecutor::new(vec![5_000], vec![]);
    let mut source = SnowflakeSource::new(config, executor).expect("source");

    let error = match source.start_snapshot(&["SECRETS"]).await {
        Err(error) => error,
        Ok(_) => panic!("an excluded table must not be snapshottable on request"),
    };
    assert!(error.to_string().contains("SECRETS"), "got: {error}");
}
