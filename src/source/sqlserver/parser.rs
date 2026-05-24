use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use crate::core::{Error, Event, Result};

pub(super) fn lsn_hex_to_bytes(lsn_hex: &str) -> Result<[u8; 10]> {
    let value = lsn_hex
        .strip_prefix("0x")
        .or_else(|| lsn_hex.strip_prefix("0X"))
        .unwrap_or(lsn_hex);
    if value.len() != 20 {
        return Err(Error::CheckpointError(format!(
            "invalid sqlserver LSN length: expected 20 hex chars, got {} ({lsn_hex})",
            value.len()
        )));
    }

    let mut bytes = [0_u8; 10];
    for (index, chunk) in value.as_bytes().chunks(2).enumerate() {
        let piece = std::str::from_utf8(chunk).map_err(|error| {
            Error::CheckpointError(format!("invalid sqlserver LSN encoding: {error}"))
        })?;
        bytes[index] = u8::from_str_radix(piece, 16).map_err(|error| {
            Error::CheckpointError(format!("invalid sqlserver LSN hex byte '{piece}': {error}"))
        })?;
    }
    Ok(bytes)
}

pub(super) fn lsn_bytes_to_hex(lsn: &[u8; 10]) -> String {
    let mut out = String::from("0x");
    for byte in lsn {
        out.push_str(&format!("{byte:02X}"));
    }
    out
}

pub(super) fn compare_lsn(left: &[u8; 10], right: &[u8; 10]) -> std::cmp::Ordering {
    left.cmp(right)
}

pub(super) fn tx_id_from_seqval(seqval_hex: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    seqval_hex.hash(&mut hasher);
    hasher.finish()
}

pub(super) fn lsn_from_source_offset(offset: &str) -> Option<[u8; 10]> {
    let candidate = offset.split(':').next().unwrap_or(offset);
    lsn_hex_to_bytes(candidate).ok()
}

pub(super) fn sqlserver_resume_lsn_from_offset_bytes(encoded: &[u8]) -> Result<[u8; 10]> {
    if let Ok(text) = serde_json::from_slice::<String>(encoded) {
        return lsn_from_source_offset(&text).ok_or_else(|| {
            Error::CheckpointError(format!(
                "invalid sqlserver checkpoint offset string: {text}"
            ))
        });
    }

    <[u8; 10]>::try_from(encoded).map_err(|_| {
        Error::CheckpointError("sqlserver checkpoint offset must contain exactly 10 bytes".into())
    })
}

pub(super) fn sqlserver_event_pk_fingerprint(event: &Event) -> Option<String> {
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
    let mut last_index_by_pk: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut duplicates = 0_u64;

    for event in events {
        if let Some(pk) = sqlserver_event_pk_fingerprint(&event) {
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

pub(super) fn validate_capture_instance_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::SourceError(
            "sqlserver capture_instance name must not be empty".into(),
        ));
    }
    if !name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(Error::SourceError(format!(
            "invalid sqlserver capture_instance name: {name}"
        )));
    }
    Ok(())
}

pub(super) fn validate_sql_identifier(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::ConfigError(
            "sqlserver identifier must not be empty".into(),
        ));
    }
    if !name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(Error::ConfigError(format!(
            "sqlserver identifier contains unsupported characters: {name}"
        )));
    }
    Ok(())
}

pub(super) fn parse_schema_table(name: &str) -> Result<(String, String)> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(Error::ConfigError(
            "sqlserver snapshot table name must not be empty".into(),
        ));
    }

    let parts = parse_sqlserver_identifier_path(trimmed)?;
    match parts.as_slice() {
        [table] => Ok(("dbo".to_string(), table.to_string())),
        [schema, table] => Ok((schema.to_string(), table.to_string())),
        _ => Err(Error::ConfigError(format!(
            "sqlserver snapshot table name is invalid: {trimmed}"
        ))),
    }
}

fn parse_sqlserver_identifier_path(input: &str) -> Result<Vec<String>> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_brackets = false;
    let mut segment_quoted = false;

    let chars: Vec<char> = input.chars().collect();
    let mut idx = 0;
    while idx < chars.len() {
        let ch = chars[idx];
        if in_brackets {
            if ch == ']' {
                if chars.get(idx + 1) == Some(&']') {
                    current.push(']');
                    idx += 2;
                    continue;
                }
                in_brackets = false;
                idx += 1;
                continue;
            }
            current.push(ch);
            idx += 1;
            continue;
        }

        match ch {
            '[' => {
                if !current.trim().is_empty() {
                    return Err(Error::ConfigError(format!(
                        "sqlserver snapshot table name is invalid: {input}"
                    )));
                }
                current.clear();
                in_brackets = true;
                segment_quoted = true;
                idx += 1;
            }
            '.' => {
                let segment =
                    finalize_sqlserver_identifier_segment(&current, segment_quoted, input)?;
                parts.push(segment);
                current.clear();
                segment_quoted = false;
                idx += 1;
            }
            _ => {
                current.push(ch);
                idx += 1;
            }
        }
    }

    if in_brackets {
        return Err(Error::ConfigError(format!(
            "sqlserver snapshot table name is invalid: {input}"
        )));
    }

    let last = finalize_sqlserver_identifier_segment(&current, segment_quoted, input)?;
    parts.push(last);
    Ok(parts)
}

fn finalize_sqlserver_identifier_segment(raw: &str, quoted: bool, full_input: &str) -> Result<String> {
    let segment = if quoted {
        raw.to_string()
    } else {
        raw.trim().to_string()
    };

    if segment.is_empty() {
        return Err(Error::ConfigError(format!(
            "sqlserver snapshot table name is invalid: {full_input}"
        )));
    }

    if quoted {
        if segment
            .chars()
            .any(|character| character == '\0' || character.is_control())
        {
            return Err(Error::ConfigError(format!(
                "sqlserver snapshot table name is invalid: {full_input}"
            )));
        }
        return Ok(segment);
    }

    validate_sql_identifier(&segment)?;
    Ok(segment)
}

pub(super) fn quoted_identifier(identifier: &str) -> String {
    format!("[{}]", identifier.replace(']', "]]"))
}

pub(super) fn qualified_table_name(schema: &str, table: &str) -> String {
    format!("{}.{}", quoted_identifier(schema), quoted_identifier(table))
}

fn build_prefixed_column_projection(columns: &[String], alias: &str) -> String {
    columns
        .iter()
        .map(|column| format!("{alias}.{}", quoted_identifier(column)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn build_seek_where_clause(primary_key_columns: &[String]) -> String {
    let mut predicates = Vec::with_capacity(primary_key_columns.len());
    for (index, column) in primary_key_columns.iter().enumerate() {
        let mut prefix = Vec::new();
        for (prev, previous_column) in primary_key_columns.iter().enumerate().take(index) {
            let left = quoted_identifier(previous_column);
            prefix.push(format!("t.{left} = @P{}", prev + 1));
        }
        let current = format!("t.{} > @P{}", quoted_identifier(column), index + 1);
        if prefix.is_empty() {
            predicates.push(format!("({current})"));
        } else {
            predicates.push(format!("({} AND {current})", prefix.join(" AND ")));
        }
    }
    format!("WHERE {}", predicates.join(" OR "))
}

pub(super) fn build_snapshot_fetch_sql(
    table_ref: &str,
    primary_key_columns: &[String],
    column_names: &[String],
    limit_param_index: usize,
    include_seek_where_clause: bool,
) -> String {
    let order_by = primary_key_columns
        .iter()
        .map(|column| quoted_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let cursor_projection = build_prefixed_column_projection(primary_key_columns, "t");
    let row_projection = build_prefixed_column_projection(column_names, "t");
    let where_clause = if include_seek_where_clause {
        build_seek_where_clause(primary_key_columns)
    } else {
        String::new()
    };

    format!(
        "SELECT TOP (@P{limit_param_index}) \
         (SELECT {cursor_projection} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS cursor_json, \
         (SELECT {row_projection} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS row_json \
         FROM {table_ref} AS t \
         {where_clause} \
         ORDER BY {order_by}"
    )
}

fn build_cdc_select_columns(columns: &[String]) -> String {
    let mut select_columns = String::from(
        "sys.fn_varbintohexstr(__$start_lsn) AS start_lsn_hex, sys.fn_varbintohexstr(__$seqval) AS seqval_hex, __$operation AS operation",
    );
    for column in columns {
        select_columns.push_str(", ");
        select_columns.push_str(&quoted_identifier(column));
    }
    select_columns
}

pub(super) fn build_cdc_poll_sql(
    capture_instance: &str,
    columns: &[String],
    max_events_per_poll: usize,
    start_lsn_hex: &str,
    end_lsn_hex: &str,
) -> String {
    let select_columns = build_cdc_select_columns(columns);
    format!(
        "SELECT TOP ({max_events_per_poll}) {select_columns}, DATEDIFF_BIG(MILLISECOND, '1970-01-01T00:00:00', SYSUTCDATETIME()) AS ts_ms \
         FROM cdc.fn_cdc_get_all_changes_{capture_instance}(CONVERT(binary(10), '{start_lsn_hex}', 1), CONVERT(binary(10), '{end_lsn_hex}', 1), 'all update old') \
         ORDER BY __$start_lsn, __$seqval, __$operation"
    )
}

pub(super) fn build_snapshot_row_count_sql(schema: &str, table: &str) -> String {
    format!("SELECT COUNT_BIG(1) FROM {}", qualified_table_name(schema, table))
}

pub(super) fn is_sqlserver_cdc_window_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    let mentions_cdc_fn = lower.contains("fn_cdc_get_all_changes_");
    let mentions_arg_shape =
        lower.contains("insufficient number of arguments") || lower.contains("expects parameter");
    mentions_cdc_fn && mentions_arg_shape
}
