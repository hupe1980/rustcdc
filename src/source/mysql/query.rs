use ahash::AHashMap as HashMap;

use mysql_common::{
    binlog::events::TableMapEvent,
    binlog::row::BinlogRow,
    constants::{ColumnFlags, ColumnType},
    row::Row as MysqlRow,
    value::Value as MysqlValue,
};

use crate::core::{Error, Event, Result};
pub(super) fn mysql_json_value_to_param(value: &serde_json::Value) -> Result<MysqlValue> {
    match value {
        serde_json::Value::Null => Err(Error::CheckpointError(
            "mysql snapshot cursor does not support NULL primary key values".into(),
        )),
        serde_json::Value::Bool(flag) => Ok(MysqlValue::Int(if *flag { 1 } else { 0 })),
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                Ok(MysqlValue::Int(value))
            } else if let Some(value) = number.as_u64() {
                Ok(MysqlValue::UInt(value))
            } else if let Some(value) = number.as_f64() {
                Ok(MysqlValue::Double(value))
            } else {
                Err(Error::CheckpointError(
                    "mysql snapshot cursor contains unsupported numeric value".into(),
                ))
            }
        }
        serde_json::Value::String(text) => Ok(MysqlValue::Bytes(text.clone().into_bytes())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Err(Error::CheckpointError(
            "mysql snapshot cursor contains unsupported composite value".into(),
        )),
    }
}

pub(super) fn mysql_event_pk_fingerprint(event: &Event) -> Option<String> {
    let pk_columns = event.primary_key.as_ref()?;
    if pk_columns.is_empty() {
        return None;
    }

    let row = event
        .after
        .as_ref()
        .or(event.before.as_ref())?
        .as_object()?;

    let mut fingerprint = String::with_capacity(64);
    fingerprint.push_str(&event.table);
    for column in pk_columns {
        let value = row.get(column)?;
        fingerprint.push('|');
        fingerprint.push_str(column);
        fingerprint.push('=');
        fingerprint.push_str(&value.to_string());
    }
    Some(fingerprint)
}

pub(super) fn dedup_overlap_events_by_pk(events: Vec<Event>) -> (Vec<Event>, u64) {
    let mut deduped = Vec::with_capacity(events.len());
    let mut last_index_by_pk: HashMap<String, usize> = HashMap::new();
    let mut duplicates = 0_u64;

    for event in events {
        if let Some(pk) = mysql_event_pk_fingerprint(&event) {
            if let Some(index) = last_index_by_pk.get(&pk).copied() {
                deduped[index] = event;
                duplicates = duplicates.saturating_add(1);
            } else {
                last_index_by_pk.insert(pk, deduped.len());
                deduped.push(event);
            }
        } else {
            deduped.push(event);
        }
    }

    (deduped, duplicates)
}

pub(super) fn format_gtid(sid: [u8; 16], gno: u64) -> String {
    let sid = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        sid[0],
        sid[1],
        sid[2],
        sid[3],
        sid[4],
        sid[5],
        sid[6],
        sid[7],
        sid[8],
        sid[9],
        sid[10],
        sid[11],
        sid[12],
        sid[13],
        sid[14],
        sid[15],
    );
    format!("{sid}:{gno}")
}

/// Merge a single GTID into a GTID **set**, returning the union.
///
/// This is the difference between a resumable checkpoint and a catastrophic one.
///
/// The stream previously did `stream.gtid = <single gtid>` on every `GtidEvent`,
/// **overwriting** the executed-set read at startup. So a connector that began at
/// `uuid:1-500` and processed one more transaction checkpointed `uuid:501` — discarding
/// all history. Resuming from that tells the server the replica has executed *only*
/// transaction 501, and it replays 1–500: a mass duplication of everything before the
/// restart point, silently.
///
/// Intervals are coalesced so the set stays compact over a long-running stream —
/// consecutive transactions collapse into one range rather than accumulating one entry
/// per transaction.
///
/// Renders in MySQL's canonical form: single values as `m`, ranges as `m-n`. MySQL
/// requires `n > m` strictly in the range form, so a one-transaction interval must be
/// written `11`, never `11-11`.
pub(super) fn merge_gtid_into_set(set: &str, gtid: &str) -> String {
    // `gtid` is parsed as a *set* rather than a single `uuid:gno`. A GtidEvent carries
    // one GTID, but accepting a set costs nothing and avoids a silent-loss failure mode:
    // a single-GTID parser handed `uuid:100-120` would fail to parse `100-120` as a
    // number and drop the whole thing, leaving the checkpoint empty.
    let mut parsed = parse_gtid_set_intervals(set);

    for (uuid, intervals) in parse_gtid_set_intervals(gtid) {
        parsed.entry(uuid).or_default().extend(intervals);
    }

    render_gtid_set(parsed)
}

/// Parse `uuid:m-n:p,uuid2:q` into `uuid -> [(start, end_inclusive)]`.
///
/// Unparseable fragments are skipped rather than failing: this runs on the hot path
/// during streaming, and a checkpoint that loses one malformed fragment is recoverable
/// (at worst a replay) whereas erroring here would stall the pipeline.
fn parse_gtid_set_intervals(set: &str) -> std::collections::BTreeMap<String, Vec<(u64, u64)>> {
    let mut parsed: std::collections::BTreeMap<String, Vec<(u64, u64)>> = Default::default();

    for uuid_set in set.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let mut parts = uuid_set.split(':').map(str::trim);
        let Some(uuid) = parts.next().filter(|uuid| !uuid.is_empty()) else {
            continue;
        };
        let entry = parsed.entry(uuid.to_ascii_lowercase()).or_default();
        for interval in parts.filter(|part| !part.is_empty()) {
            match interval.split_once('-') {
                Some((start, end)) => {
                    if let (Ok(start), Ok(end)) =
                        (start.trim().parse::<u64>(), end.trim().parse::<u64>())
                    {
                        entry.push((start, end.max(start)));
                    }
                }
                None => {
                    if let Ok(value) = interval.parse::<u64>() {
                        entry.push((value, value));
                    }
                }
            }
        }
    }

    parsed
}

/// Coalesce and render back to MySQL's canonical GTID-set text form.
fn render_gtid_set(parsed: std::collections::BTreeMap<String, Vec<(u64, u64)>>) -> String {
    let mut uuid_sets = Vec::with_capacity(parsed.len());

    for (uuid, mut intervals) in parsed {
        if intervals.is_empty() {
            continue;
        }
        intervals.sort_unstable();

        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(intervals.len());
        for (start, end) in intervals {
            match merged.last_mut() {
                // Overlapping *or adjacent* (`end + 1 == start`) — adjacency matters,
                // otherwise sequential transactions never coalesce and the set grows
                // one entry per transaction forever.
                Some(last) if start <= last.1.saturating_add(1) => {
                    last.1 = last.1.max(end);
                }
                _ => merged.push((start, end)),
            }
        }

        let rendered = merged
            .into_iter()
            .map(|(start, end)| {
                if start == end {
                    start.to_string()
                } else {
                    format!("{start}-{end}")
                }
            })
            .collect::<Vec<_>>()
            .join(":");
        uuid_sets.push(format!("{uuid}:{rendered}"));
    }

    uuid_sets.join(",")
}

pub(super) fn binlog_row_to_mysql_row(row: BinlogRow) -> Result<MysqlRow> {
    MysqlRow::try_from(row).map_err(|error| {
        Error::SourceError(format!(
            "failed converting mysql binlog row to row: {error}"
        ))
    })
}

pub(super) fn primary_key_columns_from_row(row: &MysqlRow) -> Option<Vec<String>> {
    let keys = row
        .columns_ref()
        .iter()
        .filter(|column| column.flags().contains(ColumnFlags::PRI_KEY_FLAG))
        .map(|column| column.name_str().to_string())
        .collect::<Vec<_>>();

    if keys.is_empty() {
        None
    } else {
        Some(keys)
    }
}

pub(super) fn mysql_row_to_json(row: &MysqlRow) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    for (index, column) in row.columns_ref().iter().enumerate() {
        let name = column.name_str().to_string();
        let value = row
            .as_ref(index)
            .map(|value| {
                mysql_value_to_json_for_column(
                    value,
                    column.column_type(),
                    column.character_set(),
                )
            })
            .unwrap_or(serde_json::Value::Null);
        object.insert(name, value);
    }
    serde_json::Value::Object(object)
}

pub(super) fn mysql_value_to_json(value: &MysqlValue) -> serde_json::Value {
    mysql_value_to_json_typed(value, None)
}

/// MySQL's collation id for the `binary` character set.
///
/// A column is binary — `BINARY`, `VARBINARY`, `TINYBLOB`…`LONGBLOB` — exactly when it is a
/// character-typed column carrying this charset. The *column type* cannot tell you: `BLOB`
/// and `TEXT` share `MYSQL_TYPE_BLOB`, `VARBINARY` and `VARCHAR` share
/// `MYSQL_TYPE_VAR_STRING`, `BINARY` and `CHAR` share `MYSQL_TYPE_STRING`. Only the charset
/// separates them.
const MYSQL_BINARY_CHARSET: u16 = 63;

/// Whether a column holds bytes rather than text.
///
/// `mysql_common` resolves each binlog column's charset from the table-map's optional
/// metadata — `DEFAULT_CHARSET` or `COLUMN_CHARSET`, whichever the server sent — and pairs it
/// with the column, using the same character-column indexing its own value parser uses. The
/// result-set path gets the collation id straight from the column-definition packet. So this
/// is a lookup, not arithmetic, and it cannot drift from the library's own reading.
///
/// A charset of `0` means the metadata was absent, which is treated as text — the behaviour
/// before this existed. `binlog_row_metadata = FULL` is already required for column names and
/// key flags, so present is the normal case.
fn is_binary_column(column_type: ColumnType, charset: u16) -> bool {
    column_type.is_character_type() && charset == MYSQL_BINARY_CHARSET
}

/// Convert a value, using the column's type **and charset**.
///
/// # Why the charset is load-bearing
///
/// `MysqlValue::Bytes` carries almost everything MySQL sends as a string: `VARCHAR`, `TEXT`,
/// `JSON`, `DECIMAL`, and also `VARBINARY` and `BLOB`. Deciding how to render it from the
/// bytes alone — text when they are valid UTF-8, hex when they are not — makes the
/// representation depend on the **value** rather than the column, and that is not a
/// representation a consumer can decode:
///
/// - A `BLOB` holding `hello` arrives as `"hello"`; the same column holding `0xDEADBEEF`
///   arrives as `"deadbeef"`. Nothing in the event says which happened.
/// - A consumer that hex-decodes corrupts the first row (or fails, if the text is not all hex
///   digits). One that reads text corrupts the second, silently.
/// - A `VARCHAR` containing the literal text `deadbeef` is indistinguishable from a
///   `VARBINARY` containing those four bytes.
///
/// The configuration reference promised "hex-encoded" for binary columns, which was true only
/// for values that happened not to be valid UTF-8. With the charset, a binary column is
/// **always** hex and a text column is **always** text, so the promise holds per column.
pub(super) fn mysql_value_to_json_for_column(
    value: &MysqlValue,
    column_type: ColumnType,
    charset: u16,
) -> serde_json::Value {
    if let MysqlValue::Bytes(bytes) = value {
        if is_binary_column(column_type, charset) {
            return serde_json::Value::String(hex_encode(bytes));
        }
    }
    mysql_value_to_json_typed(value, Some(column_type))
}

/// Lowercase hex, no prefix — the form the configuration reference documents for MySQL.
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Writing into a String cannot fail.
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// Convert a binlog value, using the column's declared type where the value alone is
/// ambiguous.
///
/// `mysql_common` collapses `DATE`, `DATETIME` and `TIMESTAMP` into a single
/// `Value::Date` variant carrying a full y/m/d/h/m/s/µs tuple, so rendering by value
/// alone reports a `DATE` as `2026-07-20T00:00:00.000000`. Truncating whenever the time
/// part is zero would be worse — it would strip the time from a `DATETIME` that genuinely
/// falls at midnight. The column type is the only thing that distinguishes them, and the
/// binlog table-map carries it.
///
/// `column_type` is `None` on paths where no column metadata is available; the value is
/// then rendered in the widest form, which is lossless but less precise about intent.
pub(super) fn mysql_value_to_json_typed(
    value: &MysqlValue,
    column_type: Option<ColumnType>,
) -> serde_json::Value {
    if let MysqlValue::Date(year, month, day, hour, minute, second, micros) = value {
        match column_type {
            // A calendar date with no time component. Emitting a midnight timestamp here
            // invents information the source never carried.
            //
            // `MYSQL_TYPE_NEWDATE` is what actually appears in the binlog: MySQL has
            // written the 3-byte packed form since 5.0 and reserves `MYSQL_TYPE_DATE`
            // for the legacy wire protocol. Matching only the latter is why the first
            // attempt at this fix changed nothing.
            Some(ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE) => {
                return serde_json::Value::String(format!("{year:04}-{month:02}-{day:02}"));
            }
            // Whole-second precision types: a fractional part would be fabricated.
            // The `2` spellings are the binlog's fractional-second-capable encodings.
            Some(
                ColumnType::MYSQL_TYPE_DATETIME
                | ColumnType::MYSQL_TYPE_TIMESTAMP
                | ColumnType::MYSQL_TYPE_DATETIME2
                | ColumnType::MYSQL_TYPE_TIMESTAMP2,
            ) if *micros == 0 => {
                return serde_json::Value::String(format!(
                    "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}"
                ));
            }
            _ => {}
        }
    }
    match value {
        MysqlValue::NULL => serde_json::Value::Null,
        // Reached only when the column's charset is unknown or the column is not binary.
        // A binary column is hex-encoded by `mysql_value_to_json_for_column` before it gets
        // here; this fallback exists for paths with no column metadata at all, where
        // guessing from the bytes is the only option left.
        MysqlValue::Bytes(bytes) => String::from_utf8(bytes.clone())
            .map(serde_json::Value::String)
            .unwrap_or_else(|_| serde_json::Value::String(hex_encode(bytes))),
        // Numerics render as text like every other value. A JSON number is an IEEE-754
        // double once it reaches most consumers, which rounds `BIGINT` past 2^53 and
        // `DOUBLE` at the last digits — and it disagreed with the connectors whose values
        // are text, so the same column changed JSON type depending on the capture path.
        MysqlValue::Int(value) => serde_json::Value::String(value.to_string()),
        MysqlValue::UInt(value) => serde_json::Value::String(value.to_string()),
        MysqlValue::Float(value) => serde_json::Value::String(value.to_string()),
        MysqlValue::Double(value) => serde_json::Value::String(value.to_string()),
        MysqlValue::Date(year, month, day, hour, minute, second, micros) => {
            serde_json::Value::String(format!(
                "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{:06}",
                micros
            ))
        }
        MysqlValue::Time(neg, days, hours, minutes, seconds, micros) => {
            let sign = if *neg { "-" } else { "" };
            serde_json::Value::String(format!(
                "{sign}{days}:{hours:02}:{minutes:02}:{seconds:02}.{:06}",
                micros
            ))
        }
    }
}

#[cfg(test)]
mod temporal_tests {
    use super::*;

    fn date(y: u16, mo: u8, d: u8, h: u8, mi: u8, s: u8, us: u32) -> MysqlValue {
        MysqlValue::Date(y, mo, d, h, mi, s, us)
    }

    #[test]
    fn the_binlogs_newdate_spelling_is_recognised_as_a_date() {
        // MySQL writes MYSQL_TYPE_NEWDATE in the binlog and reserves MYSQL_TYPE_DATE for
        // the legacy wire protocol. Handling only the latter silently does nothing.
        assert_eq!(
            mysql_value_to_json_typed(
                &date(2026, 7, 20, 0, 0, 0, 0),
                Some(ColumnType::MYSQL_TYPE_NEWDATE)
            ),
            serde_json::Value::String("2026-07-20".to_string())
        );
    }

    #[test]
    fn the_binlogs_datetime2_spelling_keeps_its_time() {
        assert_eq!(
            mysql_value_to_json_typed(
                &date(2026, 7, 20, 0, 0, 0, 0),
                Some(ColumnType::MYSQL_TYPE_DATETIME2)
            ),
            serde_json::Value::String("2026-07-20T00:00:00".to_string())
        );
    }

    #[test]
    fn a_date_column_renders_as_a_calendar_date_with_no_time() {
        // `mysql_common` hands back a full timestamp tuple for a DATE column. Emitting
        // the midnight time invents information the source never carried, and a consumer
        // writing into a DATE column then has to parse and truncate it back.
        assert_eq!(
            mysql_value_to_json_typed(
                &date(2026, 7, 20, 0, 0, 0, 0),
                Some(ColumnType::MYSQL_TYPE_DATE)
            ),
            serde_json::Value::String("2026-07-20".to_string())
        );
    }

    #[test]
    fn a_datetime_at_exactly_midnight_keeps_its_time_component() {
        // This is why the fix keys off the column type rather than "is the time zero".
        // Truncating on a zero time would silently turn a DATETIME into a DATE.
        assert_eq!(
            mysql_value_to_json_typed(
                &date(2026, 7, 20, 0, 0, 0, 0),
                Some(ColumnType::MYSQL_TYPE_DATETIME)
            ),
            serde_json::Value::String("2026-07-20T00:00:00".to_string())
        );
    }

    #[test]
    fn sub_second_precision_survives_when_the_column_has_it() {
        assert_eq!(
            mysql_value_to_json_typed(
                &date(2026, 7, 20, 12, 34, 56, 789_012),
                Some(ColumnType::MYSQL_TYPE_DATETIME)
            ),
            serde_json::Value::String("2026-07-20T12:34:56.789012".to_string())
        );
    }

    #[test]
    fn a_timestamp_without_fractional_seconds_does_not_gain_them() {
        assert_eq!(
            mysql_value_to_json_typed(
                &date(2026, 7, 20, 1, 2, 3, 0),
                Some(ColumnType::MYSQL_TYPE_TIMESTAMP)
            ),
            serde_json::Value::String("2026-07-20T01:02:03".to_string())
        );
    }

    #[test]
    fn without_column_metadata_the_widest_form_is_used() {
        // Lossless, just less precise about intent — better than guessing.
        assert_eq!(
            mysql_value_to_json_typed(&date(2026, 7, 20, 0, 0, 0, 0), None),
            serde_json::Value::String("2026-07-20T00:00:00.000000".to_string())
        );
    }

    #[test]
    fn non_temporal_values_are_unaffected_by_the_column_type() {
        assert_eq!(
            mysql_value_to_json_typed(&MysqlValue::Int(-7), Some(ColumnType::MYSQL_TYPE_DATE)),
            serde_json::json!("-7")
        );
    }

    #[test]
    fn every_value_is_rendered_as_text_so_no_precision_is_lost() {
        // The envelope contract: one JSON type for every column, on every connector and
        // every capture path. Numbers are the interesting case, because a JSON number is an
        // IEEE-754 double once it reaches most consumers — `BIGINT` past 2^53 and the low
        // digits of a `DOUBLE` do not survive it.
        assert_eq!(
            mysql_value_to_json(&MysqlValue::Int(i64::MAX)),
            serde_json::json!("9223372036854775807"),
        );
        assert_eq!(
            mysql_value_to_json(&MysqlValue::UInt(u64::MAX)),
            serde_json::json!("18446744073709551615"),
        );
        assert_eq!(
            mysql_value_to_json(&MysqlValue::Double(0.1 + 0.2)),
            serde_json::json!("0.30000000000000004"),
            "the exact double is preserved rather than re-rounded on the way out",
        );
        // NULL stays distinguishable from the string "NULL".
        assert_eq!(
            mysql_value_to_json(&MysqlValue::NULL),
            serde_json::Value::Null,
        );
    }
}

/// String labels for the ENUM and SET columns of one table, in column order.
///
/// # Why this is needed
///
/// The binlog stores an ENUM value as its **1-based ordinal** and a SET value as a
/// **bitmask** — never as text. A connector that forwards the raw value delivers `1`
/// where the row holds `'happy'`. That is not a decode failure a consumer can notice: it
/// is a plausible integer, it round-trips, and it silently means something different the
/// moment the enum's declaration order changes.
///
/// MySQL sends the labels in the table-map event's optional metadata, but only when
/// `binlog_row_metadata=FULL` — which rustcdc already requires and `connect()` already
/// enforces for unrelated reasons (column names and key flags). This reuses that.
#[derive(Debug, Default, Clone)]
pub(super) struct EnumSetLabels {
    /// Labels for each ENUM column, in the order ENUM columns appear in the table.
    enums: Vec<Vec<String>>,
    /// Labels for each SET column, in the order SET columns appear in the table.
    sets: Vec<Vec<String>>,
}

impl EnumSetLabels {
    /// Collect labels from a table-map event.
    ///
    /// Returns empty labels rather than an error when the metadata is absent: values are
    /// then passed through as ordinals, which is what the connector did before. Failing
    /// the stream over presentation metadata would be a worse trade.
    pub(super) fn from_table_map(table_map: &TableMapEvent<'_>) -> Self {
        use mysql_common::binlog::events::OptionalMetadataField;

        let mut labels = Self::default();
        for field in table_map.iter_optional_meta() {
            let Ok(field) = field else { continue };
            match field {
                OptionalMetadataField::EnumStrValue(values) => {
                    for column in values.iter_values() {
                        let Ok(column) = column else { continue };
                        labels.enums.push(
                            column
                                .values()
                                .iter()
                                .map(|value| value.value().into_owned())
                                .collect(),
                        );
                    }
                }
                OptionalMetadataField::SetStrValue(values) => {
                    for column in values.iter_values() {
                        let Ok(column) = column else { continue };
                        labels.sets.push(
                            column
                                .values()
                                .iter()
                                .map(|value| value.value().into_owned())
                                .collect(),
                        );
                    }
                }
                _ => {}
            }
        }
        labels
    }

    fn is_empty(&self) -> bool {
        self.enums.is_empty() && self.sets.is_empty()
    }

    /// Resolve an ENUM ordinal to its label.
    ///
    /// MySQL numbers ENUM variants from 1; `0` is the "invalid value" slot that a
    /// non-strict-mode insert can produce, and it maps to the empty string rather than to
    /// the first variant.
    fn enum_label(&self, enum_index: usize, ordinal: u64) -> Option<String> {
        let variants = self.enums.get(enum_index)?;
        if ordinal == 0 {
            return Some(String::new());
        }
        variants.get(usize::try_from(ordinal).ok()? - 1).cloned()
    }

    /// Expand a SET bitmask into its comma-joined labels, in declaration order.
    fn set_labels(&self, set_index: usize, mask: u64) -> Option<String> {
        let variants = self.sets.get(set_index)?;
        let selected: Vec<&str> = variants
            .iter()
            .enumerate()
            .filter(|(bit, _)| mask & (1u64 << bit) != 0)
            .map(|(_, label)| label.as_str())
            .collect();
        Some(selected.join(","))
    }
}

/// Convert a binlog row to JSON, resolving ENUM ordinals and SET bitmasks to labels.
pub(super) fn mysql_row_to_json_with_labels(
    row: &MysqlRow,
    labels: &EnumSetLabels,
) -> serde_json::Value {
    if labels.is_empty() {
        return mysql_row_to_json(row);
    }

    let mut object = serde_json::Map::new();
    // ENUM and SET labels are positional across *their own kind* of column, not across
    // all columns, so each needs its own running index.
    let (mut enum_index, mut set_index) = (0usize, 0usize);
    for (index, column) in row.columns_ref().iter().enumerate() {
        let name = column.name_str().to_string();
        let column_type = column.column_type();
        let raw = row.as_ref(index);

        let value = match column_type {
            ColumnType::MYSQL_TYPE_ENUM => {
                let resolved = raw
                    .and_then(numeric_value)
                    .and_then(|ordinal| labels.enum_label(enum_index, ordinal))
                    .map(serde_json::Value::String);
                enum_index += 1;
                resolved.or_else(|| {
                    raw.map(|value| mysql_value_to_json_typed(value, Some(column_type)))
                })
            }
            ColumnType::MYSQL_TYPE_SET => {
                let resolved = raw
                    .and_then(bitmask_value)
                    .and_then(|mask| labels.set_labels(set_index, mask))
                    .map(serde_json::Value::String);
                set_index += 1;
                resolved.or_else(|| {
                    raw.map(|value| mysql_value_to_json_typed(value, Some(column_type)))
                })
            }
            _ => raw.map(|value| {
                mysql_value_to_json_for_column(value, column_type, column.character_set())
            }),
        }
        .unwrap_or(serde_json::Value::Null);

        object.insert(name, value);
    }
    serde_json::Value::Object(object)
}

/// Read a value as an unsigned integer, whatever integral shape the binlog used.
fn numeric_value(value: &MysqlValue) -> Option<u64> {
    match value {
        MysqlValue::Int(number) => u64::try_from(*number).ok(),
        MysqlValue::UInt(number) => Some(*number),
        // Some server versions send the ordinal as text.
        MysqlValue::Bytes(bytes) => std::str::from_utf8(bytes).ok()?.trim().parse().ok(),
        _ => None,
    }
}

/// Read a SET value as a bitmask.
///
/// A SET arrives as 1-8 **raw little-endian bytes**, not as text. Parsing those bytes as
/// a string yields control characters that happen to be valid UTF-8, so the failure is
/// silent: the column is delivered as an unreadable one-character string rather than as
/// its labels. That is what the first version of this code did.
fn bitmask_value(value: &MysqlValue) -> Option<u64> {
    match value {
        MysqlValue::Int(number) => u64::try_from(*number).ok(),
        MysqlValue::UInt(number) => Some(*number),
        MysqlValue::Bytes(bytes) if !bytes.is_empty() && bytes.len() <= 8 => {
            let mut mask = 0u64;
            for (index, byte) in bytes.iter().enumerate() {
                mask |= u64::from(*byte) << (8 * index);
            }
            Some(mask)
        }
        _ => None,
    }
}

#[cfg(test)]
mod enum_set_tests {
    use super::*;

    fn labels() -> EnumSetLabels {
        EnumSetLabels {
            enums: vec![vec!["happy".into(), "sad".into(), "angry".into()]],
            sets: vec![vec!["read".into(), "write".into(), "admin".into()]],
        }
    }

    #[test]
    fn an_enum_ordinal_resolves_to_its_declared_label() {
        // The binlog carries `1`, the row holds `'happy'`. Forwarding the ordinal is a
        // plausible-looking wrong value, not a visible decode failure.
        assert_eq!(labels().enum_label(0, 1).as_deref(), Some("happy"));
        assert_eq!(labels().enum_label(0, 3).as_deref(), Some("angry"));
    }

    #[test]
    fn enum_ordinals_are_one_based() {
        // Off-by-one here would report every value as its neighbour — the worst possible
        // failure mode, because every value still looks valid.
        assert_ne!(labels().enum_label(0, 1).as_deref(), Some("sad"));
    }

    #[test]
    fn enum_ordinal_zero_is_the_invalid_value_slot_not_the_first_variant() {
        // A non-strict-mode insert of an unlisted value stores 0, which MySQL displays as
        // the empty string. Mapping it to the first variant would fabricate a real value.
        assert_eq!(labels().enum_label(0, 0).as_deref(), Some(""));
    }

    #[test]
    fn an_out_of_range_ordinal_does_not_resolve() {
        assert_eq!(labels().enum_label(0, 99), None);
    }

    #[test]
    fn a_set_bitmask_expands_in_declaration_order() {
        assert_eq!(labels().set_labels(0, 0b011).as_deref(), Some("read,write"));
        assert_eq!(labels().set_labels(0, 0b101).as_deref(), Some("read,admin"));
        assert_eq!(
            labels().set_labels(0, 0b111).as_deref(),
            Some("read,write,admin")
        );
    }

    #[test]
    fn an_empty_set_expands_to_the_empty_string() {
        assert_eq!(labels().set_labels(0, 0).as_deref(), Some(""));
    }

    #[test]
    fn a_column_with_no_recorded_labels_does_not_resolve() {
        // Absent metadata must fall through to the raw value rather than guess.
        assert_eq!(labels().enum_label(5, 1), None);
        assert_eq!(labels().set_labels(5, 1), None);
    }

    #[test]
    fn a_set_bitmask_is_read_from_raw_little_endian_bytes() {
        // A SET is 1-8 raw bytes, not text. Parsing them as a string produces control
        // characters that are valid UTF-8, so the wrong reading fails *silently* — the
        // column arrives as an unreadable one-character string instead of its labels.
        assert_eq!(
            bitmask_value(&MysqlValue::Bytes(vec![0b0000_0011])),
            Some(3)
        );
        assert_eq!(
            bitmask_value(&MysqlValue::Bytes(vec![0x00, 0x01])),
            Some(256),
            "byte order must be little-endian"
        );
        assert_eq!(bitmask_value(&MysqlValue::Int(5)), Some(5));
        assert_eq!(bitmask_value(&MysqlValue::Bytes(Vec::new())), None);
    }

    #[test]
    fn an_ordinal_sent_as_text_is_still_resolved() {
        assert_eq!(numeric_value(&MysqlValue::Bytes(b"2".to_vec())), Some(2));
        assert_eq!(numeric_value(&MysqlValue::Int(2)), Some(2));
        assert_eq!(numeric_value(&MysqlValue::UInt(2)), Some(2));
        assert_eq!(numeric_value(&MysqlValue::Bytes(b"happy".to_vec())), None);
    }
}

#[cfg(test)]
mod binary_column_tests {
    use super::*;

    fn bytes(raw: &[u8]) -> MysqlValue {
        MysqlValue::Bytes(raw.to_vec())
    }

    /// The bug this closes: rendering depended on the **value**, not the column.
    ///
    /// A `BLOB` holding `hello` came out as `"hello"` and the same column holding
    /// `0xDEADBEEF` came out as `"deadbeef"`, with nothing in the event saying which. No
    /// consumer can decode that: hex-decoding corrupts the first row, reading text corrupts
    /// the second, and a `VARCHAR` containing the literal text `deadbeef` is
    /// indistinguishable from a `VARBINARY` holding those four bytes.
    #[test]
    fn a_binary_column_is_hex_encoded_whatever_its_bytes_happen_to_be() {
        // Valid UTF-8 in a binary column: used to arrive as text.
        assert_eq!(
            mysql_value_to_json_for_column(
                &bytes(b"hello"),
                ColumnType::MYSQL_TYPE_BLOB,
                MYSQL_BINARY_CHARSET
            ),
            serde_json::json!("68656c6c6f")
        );
        // Not valid UTF-8: already hex before, and still is.
        assert_eq!(
            mysql_value_to_json_for_column(
                &bytes(&[0xDE, 0xAD, 0xBE, 0xEF]),
                ColumnType::MYSQL_TYPE_BLOB,
                MYSQL_BINARY_CHARSET
            ),
            serde_json::json!("deadbeef")
        );
    }

    /// `VARBINARY` and `VARCHAR` share a column type, and `BINARY`/`CHAR` share another.
    /// Only the charset separates them, which is why the type alone was never enough.
    #[test]
    fn the_charset_and_not_the_type_decides() {
        for column_type in [
            ColumnType::MYSQL_TYPE_BLOB,
            ColumnType::MYSQL_TYPE_VAR_STRING,
            ColumnType::MYSQL_TYPE_STRING,
            ColumnType::MYSQL_TYPE_VARCHAR,
        ] {
            assert_eq!(
                mysql_value_to_json_for_column(&bytes(b"ab"), column_type, MYSQL_BINARY_CHARSET),
                serde_json::json!("6162"),
                "charset 63 on {column_type:?} is a binary column"
            );
            // utf8mb4_general_ci
            assert_eq!(
                mysql_value_to_json_for_column(&bytes(b"ab"), column_type, 45),
                serde_json::json!("ab"),
                "a text charset on {column_type:?} must stay text"
            );
        }
    }

    /// Types that are not character columns carry no meaningful charset, and hex-encoding
    /// them would be catastrophic: `DECIMAL` and `JSON` both arrive as `Bytes`.
    #[test]
    fn non_character_columns_are_never_hex_encoded() {
        assert_eq!(
            mysql_value_to_json_for_column(
                &bytes(b"12345.6789"),
                ColumnType::MYSQL_TYPE_NEWDECIMAL,
                MYSQL_BINARY_CHARSET
            ),
            serde_json::json!("12345.6789"),
            "a DECIMAL rendered as hex would be unrecoverable garbage"
        );
        assert_eq!(
            mysql_value_to_json_for_column(
                &bytes(br#"{"a":1}"#),
                ColumnType::MYSQL_TYPE_JSON,
                MYSQL_BINARY_CHARSET
            ),
            serde_json::json!(r#"{"a":1}"#),
        );
    }

    /// Absent charset metadata means charset 0, which must behave as it did before rather
    /// than silently reclassifying every string column as binary.
    #[test]
    fn a_missing_charset_falls_back_to_the_previous_behaviour() {
        assert_eq!(
            mysql_value_to_json_for_column(&bytes(b"hello"), ColumnType::MYSQL_TYPE_BLOB, 0),
            serde_json::json!("hello")
        );
        assert_eq!(
            mysql_value_to_json_for_column(
                &bytes(&[0xDE, 0xAD, 0xBE, 0xEF]),
                ColumnType::MYSQL_TYPE_BLOB,
                0
            ),
            serde_json::json!("deadbeef"),
            "with no metadata, guessing from the bytes is the only option left"
        );
    }

    #[test]
    fn temporal_and_numeric_rendering_is_unchanged_by_the_charset_path() {
        assert_eq!(
            mysql_value_to_json_for_column(
                &MysqlValue::Int(-7),
                ColumnType::MYSQL_TYPE_LONG,
                MYSQL_BINARY_CHARSET
            ),
            serde_json::json!("-7")
        );
        assert_eq!(
            mysql_value_to_json_for_column(
                &MysqlValue::Date(2026, 7, 20, 0, 0, 0, 0),
                ColumnType::MYSQL_TYPE_NEWDATE,
                0
            ),
            serde_json::json!("2026-07-20")
        );
    }
}
