//! Turning a `CHANGES` result set into canonical events.
//!
//! This is where the connector earns its place in the crate. Snowflake reports an update as
//! **two rows** — a `DELETE` and an `INSERT` sharing a `METADATA$ROW_ID`, each flagged
//! `METADATA$ISUPDATE = TRUE` — and reports them in no particular order. A consumer handed
//! those rows verbatim would delete a row and re-insert it, which is visible downstream as
//! a momentary absence and, on a compacted log, as a tombstone that outlives the re-insert
//! if the two land in different segments.

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::core::{Error, Event, Operation, Result, SourceMetadata, EVENT_ENVELOPE_VERSION};

/// Epoch nanoseconds to the epoch milliseconds the event envelope carries.
///
/// Saturating rather than wrapping: a nonsensical value from the server should not become a
/// plausible-looking timestamp somewhere else on the clock.
fn epoch_millis_from_nanos(nanos: u64) -> u64 {
    nanos / 1_000_000
}

/// The three columns Snowflake adds to a `CHANGES` result set.
pub(super) const METADATA_ACTION: &str = "METADATA$ACTION";
pub(super) const METADATA_ISUPDATE: &str = "METADATA$ISUPDATE";
pub(super) const METADATA_ROW_ID: &str = "METADATA$ROW_ID";

/// A result set as the executor hands it back: column names and rows of nullable text.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnowflakeResultSet {
    /// Column names, in the order the values appear in each row.
    pub columns: Vec<String>,
    /// One entry per row; `None` is SQL `NULL`, `Some` is the value's text form.
    pub rows: Vec<Vec<Option<String>>>,
}

impl SnowflakeResultSet {
    /// Build a result set from its parts.
    pub fn new(columns: Vec<String>, rows: Vec<Vec<Option<String>>>) -> Self {
        Self { columns, rows }
    }

    /// The single scalar of a one-row, one-column result, if that is what this is.
    pub(super) fn scalar(&self) -> Option<&str> {
        match self.rows.as_slice() {
            [row] => row.first().and_then(Option::as_deref),
            _ => None,
        }
    }

    fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|column| column == name)
    }
}

/// One change row, with the metadata split away from the payload.
struct ChangeRow {
    action: String,
    is_update: bool,
    row_id: String,
    payload: Value,
}

/// Parse the result set into change rows, rejecting a shape that is not a `CHANGES` output.
fn parse_rows(result: &SnowflakeResultSet, table: &str) -> Result<Vec<ChangeRow>> {
    let missing = |column: &str| {
        Error::SourceError(format!(
            "snowflake CHANGES result for '{table}' has no {column} column. Change tracking \
             must be enabled on the table (ALTER TABLE … SET CHANGE_TRACKING = TRUE) and the \
             query must use the CHANGES clause; a plain SELECT returns no change metadata."
        ))
    };

    let action_at = result
        .column_index(METADATA_ACTION)
        .ok_or_else(|| missing(METADATA_ACTION))?;
    let update_at = result
        .column_index(METADATA_ISUPDATE)
        .ok_or_else(|| missing(METADATA_ISUPDATE))?;
    let row_id_at = result
        .column_index(METADATA_ROW_ID)
        .ok_or_else(|| missing(METADATA_ROW_ID))?;

    let mut parsed = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        if row.len() != result.columns.len() {
            return Err(Error::SourceError(format!(
                "snowflake CHANGES result for '{table}' has a row of {} values against {} \
                 columns. The result set is inconsistent and the row cannot be attributed \
                 to its columns; emitting it would misalign every value after the gap.",
                row.len(),
                result.columns.len()
            )));
        }

        let action = row[action_at]
            .clone()
            .ok_or_else(|| {
                Error::SourceError(format!(
                    "snowflake CHANGES result for '{table}' has a NULL {METADATA_ACTION}"
                ))
            })?
            .to_ascii_uppercase();
        let row_id = row[row_id_at].clone().unwrap_or_default();
        // Snowflake renders a boolean as `true`/`false` through the REST API, but a
        // driver that hands back `TRUE` or `1` is not wrong either. Accept all three
        // rather than silently reading an unfamiliar spelling as "not an update", which
        // would split every update back into a delete and an insert.
        let is_update = matches!(
            row[update_at].as_deref().map(str::trim),
            Some("true" | "TRUE" | "True" | "1")
        );

        let mut payload = Map::with_capacity(result.columns.len().saturating_sub(3));
        for (index, column) in result.columns.iter().enumerate() {
            if index == action_at || index == update_at || index == row_id_at {
                continue;
            }
            // The crate's value contract: every scalar is a JSON string, SQL NULL is JSON
            // null. Snowflake's REST API already hands back text for every type, which is
            // the representation this contract wants — routing a NUMBER(38,4) through f64
            // to "restore" its type would lose the precision the contract exists to keep.
            payload.insert(
                column.clone(),
                row[index]
                    .as_ref()
                    .map_or(Value::Null, |text| Value::String(text.clone())),
            );
        }

        parsed.push(ChangeRow {
            action,
            is_update,
            row_id,
            payload: Value::Object(payload),
        });
    }

    Ok(parsed)
}

/// Convert a `CHANGES` result set into events, collapsing update pairs.
///
/// Ordering is deterministic but is **not** source order: `CHANGES` reports the net effect
/// of a window, not a sequence, so there is no source order to preserve. Rows are emitted
/// sorted by `METADATA$ROW_ID` so that two runs over the same window produce byte-identical
/// output — which is what makes a replay fixture and the idempotency guard meaningful.
pub(super) fn events_from_changes(
    result: &SnowflakeResultSet,
    source_name: &str,
    schema: &str,
    table: &str,
    primary_key: Option<&Vec<String>>,
    window_end_nanos: u64,
) -> Result<Vec<Event>> {
    let rows = parse_rows(result, table)?;

    // Group by row id so the two halves of an update meet. A `Vec` per id rather than a
    // pair: a malformed result set could carry three rows for one id, and silently keeping
    // the last is how a real change disappears.
    let mut grouped: HashMap<String, Vec<ChangeRow>> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for row in rows {
        let key = row.row_id.clone();
        if !grouped.contains_key(&key) {
            order.push(key.clone());
        }
        grouped.entry(key).or_default().push(row);
    }
    order.sort_unstable();

    let offset = window_end_nanos.to_string();
    // The window's upper bound, not the moment we decoded it.
    //
    // `Event::source.timestamp` is what the runtime's replication-lag metric measures
    // against `now()`, and it is the alert an operator wires up for "capture has fallen
    // behind". Stamping it with the decode time makes that metric read ~0 forever — a
    // pipeline a full poll interval behind, or stalled entirely, would report itself
    // perfectly current. `CHANGES` gives no per-row commit time, but a change reported in
    // `(from, to]` provably happened at or before `to`, so `to` is the tightest honest
    // bound available and it is exactly the offset being committed.
    let ts = epoch_millis_from_nanos(window_end_nanos);
    let mut events = Vec::with_capacity(order.len());

    for row_id in order {
        let group = grouped.remove(&row_id).unwrap_or_default();

        let (before, after, op) = if group.len() == 2 && group.iter().all(|row| row.is_update) {
            let delete = group.iter().find(|row| row.action == "DELETE");
            let insert = group.iter().find(|row| row.action == "INSERT");
            match (delete, insert) {
                (Some(delete), Some(insert)) => (
                    Some(delete.payload.clone()),
                    Some(insert.payload.clone()),
                    Operation::Update,
                ),
                // Two rows flagged as an update that are not one DELETE and one INSERT is
                // not a shape Snowflake documents. Guessing would fabricate a row image.
                _ => {
                    return Err(Error::SourceError(format!(
                        "snowflake CHANGES result for '{table}' has two rows for \
                         {METADATA_ROW_ID} '{row_id}' flagged as an update but they are not \
                         one DELETE and one INSERT. rustcdc cannot tell which image is the \
                         before and which the after, and picking one would fabricate a row."
                    )));
                }
            }
        } else if group.len() == 1 {
            let row = &group[0];
            match row.action.as_str() {
                "INSERT" => (None, Some(row.payload.clone()), Operation::Insert),
                "DELETE" => (Some(row.payload.clone()), None, Operation::Delete),
                other => {
                    return Err(Error::SourceError(format!(
                        "snowflake CHANGES result for '{table}' has {METADATA_ACTION} \
                         '{other}', which is neither INSERT nor DELETE. Snowflake documents \
                         only those two; a third would mean this decoder is out of date \
                         with the server."
                    )));
                }
            }
        } else {
            return Err(Error::SourceError(format!(
                "snowflake CHANGES result for '{table}' has {} rows for {METADATA_ROW_ID} \
                 '{row_id}'. A window reports the net effect per row, so it yields one row \
                 or an update's two; anything else means the result set is not what this \
                 decoder expects.",
                group.len()
            )));
        };

        events.push(Event {
            before,
            after,
            op,
            source: SourceMetadata {
                source_name: source_name.to_string(),
                offset: offset.clone(),
                timestamp: ts,
            },
            ts,
            schema: Some(schema.to_string()),
            table: table.to_string(),
            primary_key: primary_key.cloned(),
            snapshot: None,
            // `CHANGES` carries no transaction id and no commit grouping: it is the net
            // effect of an interval, not a log. Synthesising a transaction here would be a
            // fabrication, and `TransactionBoundaryPolicy` would then preserve a boundary
            // that never existed.
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            // Every column of the row image is present or explicitly NULL. Snowflake has
            // no equivalent of PostgreSQL's unchanged-TOAST omission.
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        });
    }

    Ok(events)
}

/// Everything a snapshot chunk needs to label its rows.
///
/// A struct rather than eight positional parameters: five of them are `&str`, and a
/// transposed pair would put the table name in the schema field with no type error and no
/// test that looks at both.
pub(super) struct SnapshotRowContext<'a> {
    pub(super) source_name: &'a str,
    pub(super) schema: &'a str,
    pub(super) table: &'a str,
    pub(super) primary_key: &'a [String],
    pub(super) at_nanos: u64,
    pub(super) snapshot_id: &'a str,
    pub(super) chunk_index: u32,
    /// Whether this is the final chunk of the whole snapshot.
    ///
    /// A consumer that materialises a snapshot into a staging table watches this to know
    /// when to swap it in. Never setting it — which this connector did at first — leaves
    /// that consumer waiting for an event that cannot arrive.
    pub(super) is_last_chunk: bool,
}

/// Convert a plain time-travel `SELECT` into snapshot read events.
pub(super) fn events_from_snapshot_rows(
    result: &SnowflakeResultSet,
    context: &SnapshotRowContext<'_>,
) -> Result<Vec<Event>> {
    let &SnapshotRowContext {
        source_name,
        schema,
        table,
        primary_key,
        at_nanos,
        snapshot_id,
        chunk_index,
        is_last_chunk,
    } = context;
    let offset = at_nanos.to_string();
    // The instant the snapshot is pinned to — see `events_from_changes` for why this is not
    // the decode time. A snapshot's rows are, by construction, the table as of `at_nanos`.
    let ts = epoch_millis_from_nanos(at_nanos);
    let mut events = Vec::with_capacity(result.rows.len());

    for row in &result.rows {
        if row.len() != result.columns.len() {
            return Err(Error::SourceError(format!(
                "snowflake snapshot result for '{table}' has a row of {} values against {} \
                 columns",
                row.len(),
                result.columns.len()
            )));
        }
        let mut payload = Map::with_capacity(result.columns.len());
        for (index, column) in result.columns.iter().enumerate() {
            payload.insert(
                column.clone(),
                row[index]
                    .as_ref()
                    .map_or(Value::Null, |text| Value::String(text.clone())),
            );
        }

        events.push(Event {
            before: None,
            after: Some(Value::Object(payload)),
            op: Operation::Read,
            source: SourceMetadata {
                source_name: source_name.to_string(),
                offset: offset.clone(),
                timestamp: ts,
            },
            ts,
            schema: Some(schema.to_string()),
            table: table.to_string(),
            primary_key: Some(primary_key.to_vec()),
            snapshot: Some(crate::core::SnapshotMetadata {
                snapshot_id: snapshot_id.to_string(),
                chunk_index,
                is_last_chunk,
            }),
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        });
    }

    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(rows: Vec<Vec<Option<String>>>) -> SnowflakeResultSet {
        SnowflakeResultSet::new(
            vec![
                "ID".into(),
                "NAME".into(),
                METADATA_ACTION.into(),
                METADATA_ISUPDATE.into(),
                METADATA_ROW_ID.into(),
            ],
            rows,
        )
    }

    fn cell(value: &str) -> Option<String> {
        Some(value.to_string())
    }

    #[test]
    fn an_update_pair_collapses_into_one_update_event() {
        // The whole reason this module exists. Handed through verbatim, these two rows
        // delete a row and re-insert it — visible downstream as a momentary absence, and
        // on a compacted log as a tombstone that can outlive the re-insert.
        let set = result(vec![
            vec![
                cell("1"),
                cell("old"),
                cell("DELETE"),
                cell("true"),
                cell("r1"),
            ],
            vec![
                cell("1"),
                cell("new"),
                cell("INSERT"),
                cell("true"),
                cell("r1"),
            ],
        ]);

        let events =
            events_from_changes(&set, "snowflake", "PUBLIC", "ORDERS", None, 42).expect("maps");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op, Operation::Update);
        assert_eq!(events[0].before.as_ref().unwrap()["NAME"], "old");
        assert_eq!(events[0].after.as_ref().unwrap()["NAME"], "new");
        assert_eq!(events[0].source.offset, "42");
    }

    #[test]
    fn the_pair_collapses_whichever_order_the_rows_arrive_in() {
        // A window's rows have no defined order, so the INSERT half may come first.
        let set = result(vec![
            vec![
                cell("1"),
                cell("new"),
                cell("INSERT"),
                cell("true"),
                cell("r1"),
            ],
            vec![
                cell("1"),
                cell("old"),
                cell("DELETE"),
                cell("true"),
                cell("r1"),
            ],
        ]);
        let events = events_from_changes(&set, "s", "PUBLIC", "T", None, 1).expect("maps");
        assert_eq!(events[0].before.as_ref().unwrap()["NAME"], "old");
        assert_eq!(events[0].after.as_ref().unwrap()["NAME"], "new");
    }

    #[test]
    fn a_plain_insert_and_a_plain_delete_stay_separate() {
        let set = result(vec![
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
                cell("DELETE"),
                cell("false"),
                cell("r2"),
            ],
        ]);
        let events = events_from_changes(&set, "s", "PUBLIC", "T", None, 1).expect("maps");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].op, Operation::Insert);
        assert!(events[0].before.is_none());
        assert_eq!(events[1].op, Operation::Delete);
        assert!(events[1].after.is_none());
    }

    #[test]
    fn metadata_columns_never_appear_in_the_payload() {
        // They are transport, not data. Leaking them makes every downstream schema carry
        // three columns the source table does not have.
        let set = result(vec![vec![
            cell("1"),
            cell("a"),
            cell("INSERT"),
            cell("false"),
            cell("r1"),
        ]]);
        let events = events_from_changes(&set, "s", "PUBLIC", "T", None, 1).expect("maps");
        let after = events[0].after.as_ref().unwrap();
        assert!(after.get(METADATA_ACTION).is_none());
        assert!(after.get(METADATA_ISUPDATE).is_none());
        assert!(after.get(METADATA_ROW_ID).is_none());
        assert_eq!(after.as_object().unwrap().len(), 2);
    }

    #[test]
    fn every_value_is_text_and_null_stays_null() {
        // The crate-wide contract: a JSON number is an IEEE-754 double by the time most
        // consumers see it, so NUMBER(38,4) would not survive one.
        let set = result(vec![vec![
            cell("9007199254740993"),
            None,
            cell("INSERT"),
            cell("false"),
            cell("r1"),
        ]]);
        let events = events_from_changes(&set, "s", "PUBLIC", "T", None, 1).expect("maps");
        let after = events[0].after.as_ref().unwrap();
        assert_eq!(after["ID"], Value::String("9007199254740993".into()));
        assert_eq!(after["NAME"], Value::Null);
    }

    #[test]
    fn a_result_set_without_change_metadata_is_refused_with_the_remedy() {
        // The overwhelmingly likely cause is CHANGE_TRACKING being off, which otherwise
        // surfaces as an empty or nonsensical stream.
        let set = SnowflakeResultSet::new(vec!["ID".into()], vec![vec![cell("1")]]);
        let error = events_from_changes(&set, "s", "PUBLIC", "T", None, 1)
            .expect_err("a plain SELECT is not a CHANGES result");
        assert!(
            error.to_string().contains("CHANGE_TRACKING"),
            "got: {error}"
        );
    }

    #[test]
    fn an_unexpected_row_count_for_one_row_id_is_an_error_not_a_guess() {
        let set = result(vec![
            vec![
                cell("1"),
                cell("a"),
                cell("INSERT"),
                cell("true"),
                cell("r1"),
            ],
            vec![
                cell("1"),
                cell("b"),
                cell("INSERT"),
                cell("true"),
                cell("r1"),
            ],
        ]);
        let error = events_from_changes(&set, "s", "PUBLIC", "T", None, 1)
            .expect_err("two INSERTs for one row id is not a documented shape");
        assert!(error.to_string().contains("before"), "got: {error}");
    }

    #[test]
    fn output_is_deterministic_across_runs_over_the_same_window() {
        // `CHANGES` has no source order to preserve, so the connector imposes one. Without
        // it, a replay fixture and the idempotency guard both compare against noise.
        let forward = result(vec![
            vec![
                cell("2"),
                cell("b"),
                cell("INSERT"),
                cell("false"),
                cell("r2"),
            ],
            vec![
                cell("1"),
                cell("a"),
                cell("INSERT"),
                cell("false"),
                cell("r1"),
            ],
        ]);
        let reverse = result(vec![
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
        ]);

        let left = events_from_changes(&forward, "s", "PUBLIC", "T", None, 1).expect("maps");
        let right = events_from_changes(&reverse, "s", "PUBLIC", "T", None, 1).expect("maps");
        let ids = |events: &[Event]| {
            events
                .iter()
                .map(|event| event.after.as_ref().unwrap()["ID"].clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&left), ids(&right));
    }

    #[test]
    fn a_declared_primary_key_reaches_the_event() {
        let set = result(vec![vec![
            cell("1"),
            cell("a"),
            cell("INSERT"),
            cell("false"),
            cell("r1"),
        ]]);
        let key = vec!["ID".to_string()];
        let events = events_from_changes(&set, "s", "PUBLIC", "T", Some(&key), 1).expect("maps");
        assert!(
            events[0].has_resolvable_key(),
            "a declared key must resolve against the payload, or every downstream write is unkeyed"
        );
    }
}
