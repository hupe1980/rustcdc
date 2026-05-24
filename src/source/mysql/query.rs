use std::collections::HashMap;

use mysql_common::{
    binlog::row::BinlogRow,
    constants::ColumnFlags,
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

    let row = event.after.as_ref().or(event.before.as_ref())?.as_object()?;

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
