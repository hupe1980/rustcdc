use ahash::AHashMap as HashMap;

use mysql_common::{
    binlog::row::BinlogRow, constants::ColumnFlags, row::Row as MysqlRow,
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
            .map(mysql_value_to_json)
            .unwrap_or(serde_json::Value::Null);
        object.insert(name, value);
    }
    serde_json::Value::Object(object)
}

pub(super) fn mysql_value_to_json(value: &MysqlValue) -> serde_json::Value {
    match value {
        MysqlValue::NULL => serde_json::Value::Null,
        MysqlValue::Bytes(bytes) => String::from_utf8(bytes.clone())
            .map(serde_json::Value::String)
            .unwrap_or_else(|_| {
                let mut hex = String::with_capacity(bytes.len() * 2);
                for byte in bytes {
                    hex.push_str(&format!("{byte:02x}"));
                }
                serde_json::Value::String(hex)
            }),
        MysqlValue::Int(value) => serde_json::Value::Number((*value).into()),
        MysqlValue::UInt(value) => serde_json::Value::Number((*value).into()),
        MysqlValue::Float(value) => serde_json::Number::from_f64(f64::from(*value))
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(value.to_string())),
        MysqlValue::Double(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(value.to_string())),
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
