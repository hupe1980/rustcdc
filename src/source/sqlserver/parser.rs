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

/// Returns `None` on parse failure; use in sort comparators where `Result`
/// propagation is not possible.
pub(super) fn lsn_hex_to_bytes_opt(lsn_hex: &str) -> Option<[u8; 10]> {
    lsn_hex_to_bytes(lsn_hex).ok()
}

pub(super) fn lsn_bytes_to_hex(lsn: &[u8; 10]) -> String {
    use std::fmt::Write as _;
    // Lowercase to match `sys.fn_varbintohexstr`, which is what every LSN read back
    // from the server looks like. Mixing cases breaks two things: the server-side
    // string comparison in the truncate query (meaningless under a case-sensitive or
    // binary collation, where 'a' > 'F'), and the client-side sort of the window
    // buffer by `source.offset` (where '0' < 'A' < 'a', so uppercase truncate offsets
    // sort before every lowercase DML offset that differs at a letter position).
    let mut out = String::with_capacity(2 + lsn.len() * 2);
    out.push_str("0x");
    for byte in lsn {
        let _ = write!(out, "{byte:02x}");
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

fn finalize_sqlserver_identifier_segment(
    raw: &str,
    quoted: bool,
    full_input: &str,
) -> Result<String> {
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

/// A resume point *within* a CDC window.
///
/// A window can contain more changes than `max_events_per_poll`, so a poll may stop
/// part-way through it. The cursor records exactly where, using the same triple the
/// `ORDER BY` sorts on, so the next poll resumes with no gap and no repeat.
///
/// `operation` is part of the key because `'all update old'` emits **two rows sharing
/// one `(start_lsn, seqval)`** — op=3 (before-image) and op=4 (after-image). A cursor
/// keyed only on `(lsn, seqval)` would skip the op=4 partner when a batch boundary
/// falls between them, silently turning an UPDATE into a lost after-image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlServerCdcCursor {
    pub(crate) lsn_hex: String,
    pub(crate) seqval_hex: String,
    pub(crate) operation: i32,
}

impl SqlServerCdcCursor {
    /// Encode as `"{lsn}:{seqval}:{op}"` for the checkpoint offset.
    pub(crate) fn encode(&self) -> String {
        format!("{}:{}:{}", self.lsn_hex, self.seqval_hex, self.operation)
    }

    /// Parse `"{lsn}:{seqval}:{op}"`. Returns `None` for a bare `"{lsn}"` offset,
    /// which means "start of window" and is the pre-cursor checkpoint format.
    pub(crate) fn decode(offset: &str) -> Option<Self> {
        let mut parts = offset.split(':');
        let lsn_hex = parts.next()?.to_string();
        let seqval_hex = parts.next()?.to_string();
        let operation = parts.next()?.parse::<i32>().ok()?;
        lsn_hex_to_bytes(&lsn_hex).ok()?;
        lsn_hex_to_bytes(&seqval_hex).ok()?;
        Some(Self {
            lsn_hex,
            seqval_hex,
            operation,
        })
    }
}

fn build_cdc_select_columns(columns: &[String]) -> String {
    // Project the captured columns through `FOR JSON PATH`, exactly as the snapshot
    // path does, and let SQL Server serialize every type.
    //
    // The previous approach decoded each cell client-side through a `try_get` ladder
    // over five Rust types (`&str`, `i64`, `i32`, `f64`, `bool`), discarding every
    // type mismatch. Because `tiberius` is pinned with `default-features = false` and
    // no `chrono`/`rust_decimal` features, that silently produced `null` for decimal,
    // numeric, money, datetime2, datetime, date, time, datetimeoffset, uniqueidentifier,
    // varbinary, rowversion, xml, smallint, tinyint and real — indistinguishable from a
    // genuine SQL NULL. It also meant the *same physical row* decoded correctly during
    // snapshot (which already used FOR JSON PATH) and as `null` during streaming.
    let mut projection = String::new();
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            projection.push_str(", ");
        }
        // Alias back to the original name so the JSON keys are the column names.
        projection.push_str(&format!(
            "c.{} AS {}",
            quoted_identifier(column),
            quoted_identifier(column)
        ));
    }
    projection
}

pub(super) fn build_cdc_poll_sql(
    capture_instance: &str,
    columns: &[String],
    max_events_per_poll: usize,
    start_lsn_hex: &str,
    end_lsn_hex: &str,
    cursor: Option<&SqlServerCdcCursor>,
) -> String {
    let row_projection = build_cdc_select_columns(columns);

    // Lexicographic strict-greater-than on the same triple as the ORDER BY, so a
    // resumed poll starts immediately after the last delivered row.
    let cursor_clause = match cursor {
        Some(c) => format!(
            " WHERE c.__$start_lsn > CONVERT(binary(10), '{lsn}', 1) \
              OR (c.__$start_lsn = CONVERT(binary(10), '{lsn}', 1) \
                  AND c.__$seqval > CONVERT(binary(10), '{seq}', 1)) \
              OR (c.__$start_lsn = CONVERT(binary(10), '{lsn}', 1) \
                  AND c.__$seqval = CONVERT(binary(10), '{seq}', 1) \
                  AND c.__$operation > {op})",
            lsn = c.lsn_hex,
            seq = c.seqval_hex,
            op = c.operation
        ),
        None => String::new(),
    };

    format!(
        "SELECT TOP ({max_events_per_poll}) \
         sys.fn_varbintohexstr(c.__$start_lsn) AS start_lsn_hex, \
         sys.fn_varbintohexstr(c.__$seqval) AS seqval_hex, \
         c.__$operation AS operation, \
         DATEDIFF_BIG(MILLISECOND, '1970-01-01T00:00:00', \
             COALESCE(sys.fn_cdc_map_lsn_to_time(c.__$start_lsn), SYSUTCDATETIME())) AS ts_ms, \
         (SELECT {row_projection} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS row_json \
         FROM cdc.fn_cdc_get_all_changes_{capture_instance}(\
             CONVERT(binary(10), '{start_lsn_hex}', 1), \
             CONVERT(binary(10), '{end_lsn_hex}', 1), 'all update old') AS c\
         {cursor_clause} \
         ORDER BY c.__$start_lsn, c.__$seqval, c.__$operation"
    )
}

pub(super) fn build_snapshot_row_count_sql(schema: &str, table: &str) -> String {
    format!(
        "SELECT COUNT_BIG(1) FROM {}",
        qualified_table_name(schema, table)
    )
}

pub(super) fn is_sqlserver_cdc_window_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    let mentions_cdc_fn = lower.contains("fn_cdc_get_all_changes_");
    let mentions_arg_shape =
        lower.contains("insufficient number of arguments") || lower.contains("expects parameter");
    mentions_cdc_fn && mentions_arg_shape
}
