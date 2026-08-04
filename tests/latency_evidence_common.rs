//! Shared harness for connector-backed latency evidence.
//!
//! # What this measures, and why the previous version did not
//!
//! The number an operator needs from a CDC pipeline is **capture latency**: the wall-clock
//! time from a row being committed in the source database to the corresponding event
//! reaching the consumer. Nothing else predicts whether a downstream view is stale.
//!
//! The earlier harness measured neither of the two things it named. It inserted every row
//! *before* the measurement loop started, so `poll_latency` timed draining an
//! already-populated in-process `VecDeque` and `commit_latency` timed one fsync. Both are
//! microbenchmarks of the runtime's own bookkeeping, executed against a pipeline that was
//! never under load, and neither could vary with anything a connector change would affect.
//! The gate thresholds (p95 ≤ 500 ms) sat two to four orders of magnitude above a
//! `VecDeque` drain, so **the gate could not fail for performance reasons.**
//!
//! # How capture latency is measured here
//!
//! Every row carries the writer's wall clock at insert time, embedded in the payload. On
//! delivery, the same process reads it back and subtracts. **One clock, no skew** — the
//! writer and the reader are the same process, so container/host clock drift cannot
//! contaminate the figure. The measurement therefore covers everything an operator cares
//! about: the database commit, the WAL/binlog flush, capture, decode, the transform
//! pipeline, and delivery to the consumer.
//!
//! Writes run **concurrently** with polling, so the pipeline is measured under live load
//! rather than draining a backlog accumulated in advance.
//!
//! `source_commit_skew_ms` reports the difference between this figure and the one derived
//! from the connector's own `SourceMetadata::timestamp`. It exists to make clock skew
//! *visible* rather than silently folded into the headline number: a large magnitude means
//! the database's clock and this process's clock disagree, and any latency figure sourced
//! from the event timestamp alone is wrong by that much.

#![allow(dead_code)] // Each connector suite uses a subset of this harness.

use std::{
    fs,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

/// Marker prefix identifying a payload that carries an embedded writer timestamp.
const STAMP_PREFIX: &str = "ts:";

/// Build a row payload carrying the writer's wall clock in microseconds.
///
/// Format: `ts:<epoch_micros>:<filler>`. The filler exists so payload size can be varied
/// independently of the timestamp.
pub fn stamped_payload(filler: &str) -> String {
    format!("{STAMP_PREFIX}{}:{}", now_micros(), filler)
}

/// Recover the writer timestamp from a payload built by [`stamped_payload`].
///
/// Returns `None` for a payload that does not carry one, so a connector emitting an
/// unexpected shape surfaces as a *missing sample* rather than as a plausible-looking wrong
/// latency.
pub fn payload_stamp_micros(payload: &str) -> Option<u64> {
    let rest = payload.strip_prefix(STAMP_PREFIX)?;
    let (micros, _) = rest.split_once(':')?;
    micros.parse().ok()
}

/// Extract the stamped payload from an event's after-image, whatever column it is in.
///
/// Connectors differ in how they render values — pgoutput sends text, SQL Server's
/// `FOR JSON PATH` sends typed JSON — so this scans the object's string values for the
/// marker rather than assuming a column name or a value type.
pub fn event_payload_stamp_micros(event: &rustcdc::Event) -> Option<u64> {
    let after = event.after.as_ref()?.as_object()?;
    after
        .values()
        .filter_map(|value| value.as_str())
        .find_map(payload_stamp_micros)
}

/// Wall clock in microseconds since the Unix epoch.
pub fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as u64)
        .unwrap_or_default()
}

fn micros_delta_ms(later_micros: u64, earlier_micros: u64) -> f64 {
    later_micros.saturating_sub(earlier_micros) as f64 / 1_000.0
}

/// Accumulates per-event capture latency and the mechanical runtime timings.
#[derive(Debug, Default)]
pub struct LatencyRecorder {
    capture_latency_ms: Vec<f64>,
    source_ts_latency_ms: Vec<f64>,
    poll_latency_ms: Vec<f64>,
    commit_latency_ms: Vec<f64>,
    batch_sizes: Vec<f64>,
    unstamped_events: u64,
}

impl LatencyRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the capture latency of every event in a delivered batch.
    ///
    /// `delivered_at_micros` is sampled once per batch, immediately after the poll
    /// returned — attributing the batch's own processing time to its last event would
    /// flatter the tail.
    pub fn observe_batch(&mut self, events: &[rustcdc::Event], delivered_at_micros: u64) {
        self.batch_sizes.push(events.len() as f64);
        for event in events {
            match event_payload_stamp_micros(event) {
                Some(written_at) => {
                    self.capture_latency_ms
                        .push(micros_delta_ms(delivered_at_micros, written_at));
                    // The connector's own commit timestamp, for the skew comparison.
                    // `SourceMetadata::timestamp` is milliseconds.
                    let source_micros = event.source.timestamp.saturating_mul(1_000);
                    if source_micros > 0 {
                        self.source_ts_latency_ms
                            .push(micros_delta_ms(delivered_at_micros, source_micros));
                    }
                }
                None => self.unstamped_events = self.unstamped_events.saturating_add(1),
            }
        }
    }

    pub fn observe_poll_ms(&mut self, ms: f64) {
        self.poll_latency_ms.push(ms);
    }

    pub fn observe_commit_ms(&mut self, ms: f64) {
        self.commit_latency_ms.push(ms);
    }

    /// Number of events whose payload carried no writer timestamp.
    ///
    /// Non-zero means those events could not be measured at all; the summary carries the
    /// count so a partially-measured run cannot be mistaken for a clean one.
    pub fn unstamped_events(&self) -> u64 {
        self.unstamped_events
    }

    pub fn measured_events(&self) -> usize {
        self.capture_latency_ms.len()
    }

    pub fn finish(
        &self,
        profile: &'static str,
        rows_inserted: u64,
        events_committed: u64,
        wall_clock_ms: u128,
    ) -> LatencySummary {
        let events_per_second = if wall_clock_ms > 0 {
            (events_committed as f64) * 1_000.0 / (wall_clock_ms as f64)
        } else {
            0.0
        };

        // Compared at the median so a single outlier batch cannot dominate the skew figure.
        let source_commit_skew_ms = if self.source_ts_latency_ms.is_empty() {
            0.0
        } else {
            percentile(&self.source_ts_latency_ms, 50.0)
                - percentile(&self.capture_latency_ms, 50.0)
        };

        LatencySummary {
            profile,
            rows_inserted,
            events_committed,
            events_measured: self.capture_latency_ms.len() as u64,
            unstamped_events: self.unstamped_events,
            batches: self.poll_latency_ms.len(),
            capture_latency_ms_p50: percentile(&self.capture_latency_ms, 50.0),
            capture_latency_ms_p95: percentile(&self.capture_latency_ms, 95.0),
            capture_latency_ms_p99: percentile(&self.capture_latency_ms, 99.0),
            capture_latency_ms_max: self
                .capture_latency_ms
                .iter()
                .copied()
                .fold(0.0_f64, f64::max),
            source_commit_skew_ms,
            poll_latency_ms_p50: percentile(&self.poll_latency_ms, 50.0),
            poll_latency_ms_p95: percentile(&self.poll_latency_ms, 95.0),
            poll_latency_ms_p99: percentile(&self.poll_latency_ms, 99.0),
            commit_latency_ms_p50: percentile(&self.commit_latency_ms, 50.0),
            commit_latency_ms_p95: percentile(&self.commit_latency_ms, 95.0),
            commit_latency_ms_p99: percentile(&self.commit_latency_ms, 99.0),
            batch_size_p50: percentile(&self.batch_sizes, 50.0),
            batch_size_p95: percentile(&self.batch_sizes, 95.0),
            batch_size_p99: percentile(&self.batch_sizes, 99.0),
            events_per_second,
            end_to_end_ms: wall_clock_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencySummary {
    pub profile: &'static str,
    pub rows_inserted: u64,
    pub events_committed: u64,
    /// Events whose capture latency was actually measured.
    pub events_measured: u64,
    /// Events whose payload carried no writer timestamp, and were therefore not measured.
    ///
    /// **Any non-zero value means the percentiles below cover only part of the run.**
    pub unstamped_events: u64,
    pub batches: usize,

    // ── The headline metric: source commit → consumer delivery ────────────────
    /// Wall-clock milliseconds from the writer committing the row to the event reaching
    /// the consumer. Measured against a single clock — see the module docs.
    pub capture_latency_ms_p50: f64,
    pub capture_latency_ms_p95: f64,
    pub capture_latency_ms_p99: f64,
    pub capture_latency_ms_max: f64,
    /// Median difference between latency derived from the connector's own
    /// `SourceMetadata::timestamp` and the single-clock measurement.
    ///
    /// A large magnitude means the database clock and this process's clock disagree, so any
    /// latency figure derived from event timestamps alone is wrong by that much.
    pub source_commit_skew_ms: f64,

    // ── Mechanical runtime timings (bookkeeping, not capture latency) ─────────
    /// Time spent inside `poll_event_batch`. Runtime bookkeeping only — it does **not**
    /// include the time a row spent waiting in the source log.
    pub poll_latency_ms_p50: f64,
    pub poll_latency_ms_p95: f64,
    pub poll_latency_ms_p99: f64,
    /// Time spent inside `commit_ack` — dominated by the checkpoint fsync.
    pub commit_latency_ms_p50: f64,
    pub commit_latency_ms_p95: f64,
    pub commit_latency_ms_p99: f64,

    pub batch_size_p50: f64,
    pub batch_size_p95: f64,
    pub batch_size_p99: f64,
    /// Sustained delivered-and-committed events per second over the measured window.
    pub events_per_second: f64,
    pub end_to_end_ms: u128,
}

pub fn percentile(values: &[f64], pct: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| {
        left.partial_cmp(right)
            .expect("latency values should be finite")
    });

    let rank = ((pct / 100.0) * ((sorted.len() - 1) as f64)).round() as usize;
    sorted[rank]
}

/// Assert the run produced evidence worth gating on.
///
/// A latency percentile over two samples is not a percentile, and a run where most events
/// carried no timestamp is not a measurement of the pipeline. Both used to pass silently:
/// the only assertion was `batches > 0`.
pub fn assert_sample_is_meaningful(summary: &LatencySummary, min_samples: u64) {
    assert!(
        summary.events_measured >= min_samples,
        "latency evidence needs at least {min_samples} measured events for percentiles to \
         mean anything; got {} (a p99 over a handful of samples is noise, not a tail)",
        summary.events_measured
    );
    assert_eq!(
        summary.unstamped_events, 0,
        "every delivered event must carry a writer timestamp; {} did not, so the reported \
         percentiles cover only part of the run",
        summary.unstamped_events
    );
    assert!(
        summary.capture_latency_ms_p50 > 0.0,
        "a zero median capture latency means the measurement is not wired up — a real \
         pipeline cannot deliver a row before it was written"
    );
}

pub fn write_latency_artifacts(prefix: &str, summary: &LatencySummary) -> rustcdc::Result<()> {
    let target_dir = Path::new("target");
    fs::create_dir_all(target_dir).map_err(rustcdc::Error::IoError)?;

    let json_path = target_dir.join(format!("{prefix}-latency-evidence.json"));
    let json = serde_json::to_string_pretty(summary)
        .map_err(|error| rustcdc::Error::SerializationError(error.to_string()))?;
    fs::write(&json_path, json).map_err(rustcdc::Error::IoError)?;

    let markdown_path = target_dir.join(format!("{prefix}-latency-evidence.md"));
    let markdown = format!(
        "# {} Latency Evidence\n\
         \n\
         Measured with writes running **concurrently** with polling. Capture latency is the\n\
         wall-clock time from the writer committing a row to the event reaching the consumer,\n\
         measured against a single clock (writer and reader are the same process), so it\n\
         includes the database commit, log flush, capture, decode, transforms, and delivery.\n\
         \n\
         - Profile: {}\n\
         - Rows inserted: {}\n\
         - Events committed: {}\n\
         - Events measured: {} (unstamped, unmeasured: {})\n\
         - Batches: {}\n\
         - Sustained throughput: {:.1} events/sec\n\
         - Wall clock (ms): {}\n\
         \n\
         ## Capture latency — source commit to consumer delivery (ms)\n\
         \n\
         **This is the operator-facing number.**\n\
         \n\
         - p50: {:.3}\n\
         - p95: {:.3}\n\
         - p99: {:.3}\n\
         - max: {:.3}\n\
         \n\
         Clock skew vs. the connector's own commit timestamp: {:+.3} ms (median).\n\
         A large magnitude means the database clock and this process's clock disagree.\n\
         \n\
         ## Runtime bookkeeping (not capture latency)\n\
         \n\
         `poll` is time inside `poll_event_batch`; `commit` is time inside `commit_ack`,\n\
         dominated by the checkpoint fsync. Neither includes time spent waiting in the\n\
         source log, so neither is a latency an operator can quote.\n\
         \n\
         | Stage | p50 | p95 | p99 |\n\
         |---|---:|---:|---:|\n\
         | poll (ms) | {:.3} | {:.3} | {:.3} |\n\
         | commit (ms) | {:.3} | {:.3} | {:.3} |\n\
         \n\
         ## Batch size\n\
         \n\
         - p50: {:.1}\n\
         - p95: {:.1}\n\
         - p99: {:.1}\n",
        prefix.to_ascii_uppercase(),
        summary.profile,
        summary.rows_inserted,
        summary.events_committed,
        summary.events_measured,
        summary.unstamped_events,
        summary.batches,
        summary.events_per_second,
        summary.end_to_end_ms,
        summary.capture_latency_ms_p50,
        summary.capture_latency_ms_p95,
        summary.capture_latency_ms_p99,
        summary.capture_latency_ms_max,
        summary.source_commit_skew_ms,
        summary.poll_latency_ms_p50,
        summary.poll_latency_ms_p95,
        summary.poll_latency_ms_p99,
        summary.commit_latency_ms_p50,
        summary.commit_latency_ms_p95,
        summary.commit_latency_ms_p99,
        summary.batch_size_p50,
        summary.batch_size_p95,
        summary.batch_size_p99,
    );
    fs::write(&markdown_path, markdown).map_err(rustcdc::Error::IoError)?;

    Ok(())
}

/// Shared status a concurrent writer task publishes to the collection loop.
///
/// The writer's `Result` is only observable by awaiting its `JoinHandle`, which the loop
/// does *after* it finishes — so a writer that dies mid-run leaves the loop waiting for
/// events that will never be produced, and its error stays invisible until the loop gives
/// up on its own. Publishing progress and the terminal error makes both visible while the
/// loop is still running.
#[derive(Debug, Default)]
pub struct WriterStatus {
    rows_written: AtomicU64,
    failure: std::sync::Mutex<Option<String>>,
}

impl WriterStatus {
    /// A fresh status, shared between the writer and the collection loop.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record one successfully committed row.
    pub fn record_row(&self) {
        self.rows_written.fetch_add(1, SeqCst);
    }

    /// Record the writer's terminal failure.
    pub fn record_failure(&self, error: &rustcdc::Error) {
        *self.failure.lock().expect("writer failure lock") = Some(error.to_string());
    }

    /// Rows committed to the source so far.
    pub fn rows_written(&self) -> u64 {
        self.rows_written.load(SeqCst)
    }

    /// The writer's terminal failure, if it has one.
    pub fn failure(&self) -> Option<String> {
        self.failure.lock().expect("writer failure lock").clone()
    }
}

/// Progress-based deadline for a latency collection loop.
///
/// # Why not a wall-clock deadline
///
/// These suites used a fixed total budget — "collect 2,000 events within 180 s". On a
/// loaded CI runner the same run that takes 5 s locally takes ~180 s, so the budget was
/// hit at 1,995 of 2,000 events: the pipeline was still delivering, and the test reported
/// a timeout. That failure says nothing about the pipeline, and a latency test that cannot
/// distinguish *slow machine* from *stuck pipeline* provides no evidence either way.
///
/// What is actually worth failing on is **a stall**: no new events for a sustained period
/// while the run is unfinished. That is the same signal the runtime's own `HealthVerdict`
/// treats as alertable, and it is machine-speed independent.
///
/// A generous absolute backstop remains, so a pathologically slow trickle cannot hang CI
/// forever.
pub struct ProgressDeadline {
    label: &'static str,
    expected: u64,
    stall_after: Duration,
    hard_stop: Instant,
    last_progress_at: Instant,
    last_count: u64,
    /// Rows the writer task reports having committed to the source, if it publishes them.
    ///
    /// Without this a stall message cannot distinguish "the pipeline stopped delivering"
    /// from "the writer never wrote the rows" — and the writer's own error is invisible
    /// until after the loop, which a stalled loop never reaches.
    writer: Option<Arc<WriterStatus>>,
}

impl ProgressDeadline {
    /// Fail after `stall_after` without progress, or after `hard_stop_after` overall.
    pub fn new(
        label: &'static str,
        expected: u64,
        stall_after: Duration,
        hard_stop_after: Duration,
    ) -> Self {
        let now = Instant::now();
        Self {
            label,
            expected,
            stall_after,
            hard_stop: now + hard_stop_after,
            last_progress_at: now,
            last_count: 0,
            writer: None,
        }
    }

    /// Observe the writer so a stall can name which side stopped — and so a writer that
    /// died is reported immediately with its own error rather than as a timeout.
    #[must_use]
    pub fn watching_writer(mut self, writer: Arc<WriterStatus>) -> Self {
        self.writer = Some(writer);
        self
    }

    /// Sensible defaults: fail after 90 s with no new events, 15 min overall.
    pub fn with_defaults(label: &'static str, expected: u64) -> Self {
        Self::new(
            label,
            expected,
            Duration::from_secs(90),
            Duration::from_secs(900),
        )
    }

    /// Record the current committed count and check both limits.
    ///
    /// Call once per loop iteration, including iterations that delivered nothing — an
    /// empty poll is exactly what a stall looks like.
    pub fn check(&mut self, committed: u64) -> rustcdc::Result<()> {
        // A dead writer is reported with its own error, immediately. Waiting out the stall
        // window would turn a clear "insert failed" into an ambiguous timeout.
        if let Some(failure) = self.writer.as_ref().and_then(|writer| writer.failure()) {
            return Err(rustcdc::Error::SourceError(format!(
                "{}: the writer task failed after {} rows, so the remaining events were \
                 never produced: {failure}",
                self.label,
                self.writer
                    .as_ref()
                    .map_or(0, |writer| writer.rows_written()),
            )));
        }

        if committed > self.last_count {
            self.last_count = committed;
            self.last_progress_at = Instant::now();
        }

        let stalled_for = self.last_progress_at.elapsed();
        if stalled_for >= self.stall_after {
            // Name which side stopped. A writer that died mid-run produces exactly the
            // same shape as a stalled pipeline, and blaming the pipeline for it sends the
            // reader looking in the wrong place.
            let attribution = match self.writer.as_ref().map(|writer| writer.rows_written()) {
                Some(written) if written < self.expected => format!(
                    " The writer had only committed {written}/{} rows to the source, so \
                     the missing events were never produced — look at the writer, not the \
                     pipeline.",
                    self.expected
                ),
                Some(written) => format!(
                    " The writer committed all {written} rows to the source, so the events \
                     exist and the pipeline stopped delivering them."
                ),
                None => " The pipeline stopped delivering.".to_string(),
            };
            return Err(rustcdc::Error::TimeoutError(format!(
                "{} stalled: no new events for {:.0}s at {}/{} committed.{attribution} \
                 This is a stall, not a slow machine.",
                self.label,
                stalled_for.as_secs_f64(),
                committed,
                self.expected,
            )));
        }

        if Instant::now() > self.hard_stop {
            return Err(rustcdc::Error::TimeoutError(format!(
                "{} exceeded its absolute backstop at {}/{} committed while still making \
                 progress. Either the runner is far slower than this budget assumes, or \
                 throughput has regressed by orders of magnitude.",
                self.label, committed, self.expected,
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod progress_deadline_tests {
    use super::ProgressDeadline;
    use std::time::Duration;

    #[test]
    fn progress_resets_the_stall_window() {
        // The failure this replaced: a run that was still delivering was reported as a
        // timeout because the *total* budget expired. Progress must keep it alive.
        let mut deadline = ProgressDeadline::new(
            "test",
            100,
            Duration::from_millis(120),
            Duration::from_secs(60),
        );
        for committed in 1..=5 {
            std::thread::sleep(Duration::from_millis(40));
            deadline
                .check(committed)
                .unwrap_or_else(|error| panic!("progress must not trip the stall: {error}"));
        }
    }

    #[test]
    fn no_progress_trips_the_stall_window() {
        let mut deadline = ProgressDeadline::new(
            "test",
            100,
            Duration::from_millis(60),
            Duration::from_secs(60),
        );
        deadline
            .check(10)
            .expect("first check establishes a baseline");
        std::thread::sleep(Duration::from_millis(90));
        let error = deadline
            .check(10)
            .expect_err("no progress must be reported as a stall");
        let message = error.to_string();
        assert!(message.contains("stalled"), "got: {message}");
        assert!(
            message.contains("10/100"),
            "the message must say how far it got: {message}"
        );
    }

    #[test]
    fn a_writer_failure_is_reported_immediately_with_its_own_error() {
        // Without this the loop waits out the whole stall window and then blames the
        // pipeline for events the writer never produced.
        let writer = super::WriterStatus::new();
        writer.record_row();
        writer.record_failure(&rustcdc::Error::SourceError("connection reset".into()));

        let mut deadline = ProgressDeadline::new(
            "test",
            100,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .watching_writer(std::sync::Arc::clone(&writer));

        let error = deadline
            .check(1)
            .expect_err("a dead writer must fail the run at once");
        let message = error.to_string();
        assert!(message.contains("connection reset"), "got: {message}");
        assert!(
            message.contains("writer task failed after 1 rows"),
            "the message must say how far the writer got: {message}"
        );
    }

    #[test]
    fn a_stall_names_the_writer_when_the_rows_were_never_written() {
        let writer = super::WriterStatus::new();
        writer.record_row();

        let mut deadline = ProgressDeadline::new(
            "test",
            100,
            Duration::from_millis(50),
            Duration::from_secs(600),
        )
        .watching_writer(std::sync::Arc::clone(&writer));
        deadline.check(1).expect("baseline");
        std::thread::sleep(Duration::from_millis(80));

        let message = deadline.check(1).expect_err("must stall").to_string();
        assert!(
            message.contains("look at the writer, not the pipeline"),
            "a short writer must not be blamed on the pipeline: {message}"
        );
    }

    #[test]
    fn a_stall_names_the_pipeline_when_every_row_was_written() {
        let writer = super::WriterStatus::new();
        for _ in 0..100 {
            writer.record_row();
        }

        let mut deadline = ProgressDeadline::new(
            "test",
            100,
            Duration::from_millis(50),
            Duration::from_secs(600),
        )
        .watching_writer(std::sync::Arc::clone(&writer));
        deadline.check(40).expect("baseline");
        std::thread::sleep(Duration::from_millis(80));

        let message = deadline.check(40).expect_err("must stall").to_string();
        assert!(
            message.contains("the pipeline stopped delivering them"),
            "with every row written, the pipeline is the right place to look: {message}"
        );
    }

    #[test]
    fn the_absolute_backstop_still_applies_while_progressing() {
        // A pathologically slow trickle must not hang CI forever.
        let mut deadline = ProgressDeadline::new(
            "test",
            1_000_000,
            Duration::from_secs(60),
            Duration::from_millis(50),
        );
        deadline
            .check(1)
            .expect("first check is inside the backstop");
        std::thread::sleep(Duration::from_millis(80));
        let error = deadline
            .check(2)
            .expect_err("the backstop must fire even while progressing");
        assert!(
            error.to_string().contains("backstop"),
            "the message must distinguish a backstop from a stall: {error}"
        );
    }
}
