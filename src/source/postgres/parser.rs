use crate::{
    checkpoint::PostgresOffset,
    core::{Error, Offset, Result},
};

pub(super) fn parse_table_reference(table: &str) -> Result<(String, String)> {
    let trimmed = table.trim();
    if trimmed.is_empty() {
        return Err(Error::ConfigError(
            "postgres snapshot table name must not be empty".into(),
        ));
    }

    let parts = parse_postgres_identifier_path(trimmed)?;
    match parts.as_slice() {
        [table_name] => Ok(("public".to_string(), table_name.to_string())),
        [schema, table_name] => Ok((schema.to_string(), table_name.to_string())),
        _ => Err(Error::ConfigError(format!(
            "postgres snapshot table name is invalid: {trimmed}"
        ))),
    }
}

fn parse_postgres_identifier_path(input: &str) -> Result<Vec<String>> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut segment_quoted = false;

    let chars: Vec<char> = input.chars().collect();
    let mut idx = 0;
    while idx < chars.len() {
        let ch = chars[idx];
        if in_quotes {
            if ch == '"' {
                if chars.get(idx + 1) == Some(&'"') {
                    current.push('"');
                    idx += 2;
                    continue;
                }
                in_quotes = false;
                idx += 1;
                continue;
            }
            current.push(ch);
            idx += 1;
            continue;
        }

        match ch {
            '"' => {
                if !current.trim().is_empty() {
                    return Err(Error::ConfigError(format!(
                        "postgres snapshot table name is invalid: {input}"
                    )));
                }
                current.clear();
                in_quotes = true;
                segment_quoted = true;
                idx += 1;
            }
            '.' => {
                let segment =
                    finalize_postgres_identifier_segment(&current, segment_quoted, input)?;
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

    if in_quotes {
        return Err(Error::ConfigError(format!(
            "postgres snapshot table name is invalid: {input}"
        )));
    }

    let last = finalize_postgres_identifier_segment(&current, segment_quoted, input)?;
    parts.push(last);
    Ok(parts)
}

fn finalize_postgres_identifier_segment(raw: &str, quoted: bool, full_input: &str) -> Result<String> {
    let segment = if quoted {
        raw.to_string()
    } else {
        raw.trim().to_string()
    };

    if segment.is_empty() {
        return Err(Error::ConfigError(format!(
            "postgres snapshot table name is invalid: {full_input}"
        )));
    }

    let valid = if quoted {
        segment.chars().all(|ch| ch != '\0' && !ch.is_control())
    } else {
        segment
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '$')
    };

    if !valid {
        return Err(Error::ConfigError(format!(
            "postgres snapshot table name is invalid: {full_input}"
        )));
    }

    Ok(segment)
}

pub(super) fn quote_pg_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(super) fn qualified_table_name(schema: &str, table: &str) -> String {
    format!(
        "{}.{}",
        quote_pg_identifier(schema),
        quote_pg_identifier(table)
    )
}

pub(super) fn parse_pg_lsn(value: &str) -> Result<u64> {
    let (high, low) = value.split_once('/').ok_or_else(|| {
        Error::SourceError(format!(
            "invalid postgres lsn format '{value}', expected HEX/HEX"
        ))
    })?;
    let high = u64::from_str_radix(high, 16)
        .map_err(|error| Error::SourceError(format!("invalid postgres lsn high bits: {error}")))?;
    let low = u64::from_str_radix(low, 16)
        .map_err(|error| Error::SourceError(format!("invalid postgres lsn low bits: {error}")))?;
    Ok((high << 32) | low)
}

pub(super) fn format_pg_lsn(lsn: u64) -> String {
    format!("{:X}/{:08X}", lsn >> 32, lsn as u32)
}

pub(super) fn pg_timestamp_to_millis(pg_us: i64) -> u64 {
    const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;
    let unix_us = pg_us.saturating_add(PG_EPOCH_OFFSET_US);
    if unix_us < 0 {
        0
    } else {
        (unix_us / 1_000) as u64
    }
}

pub(super) fn decode_stream_resume_lsn(
    source_type: &str,
    configured_slot_name: &str,
    resume_from: &dyn Offset,
) -> Result<u64> {
    if resume_from.source_type() != source_type {
        return Err(Error::CheckpointError(format!(
            "cannot resume postgres stream from offset source '{}'",
            resume_from.source_type()
        )));
    }

    let payload = resume_from.encode()?;
    let saved = PostgresOffset::from_bytes(&payload).map_err(|error| {
        Error::CheckpointError(format!(
            "failed decoding postgres stream checkpoint offset: {error}"
        ))
    })?;

    if saved.slot_name != configured_slot_name {
        return Err(Error::CheckpointError(format!(
            "checkpoint slot '{}' does not match configured slot '{}'",
            saved.slot_name, configured_slot_name
        )));
    }

    Ok(saved.lsn)
}

pub(super) fn reconcile_stream_resume_lsn(
    checkpoint_lsn: u64,
    slot_confirmed_lsn: u64,
    slot_name: &str,
) -> Result<u64> {
    if checkpoint_lsn <= slot_confirmed_lsn {
        return Ok(checkpoint_lsn);
    }

    Err(Error::CheckpointError(format!(
        "postgres checkpoint/slot divergence for slot '{slot_name}': checkpoint_lsn={} is ahead of slot_confirmed_lsn={}. operator intervention required",
        format_pg_lsn(checkpoint_lsn),
        format_pg_lsn(slot_confirmed_lsn),
    )))
}

pub(super) fn map_pgoutput_poll_error(slot_name: &str, error_message: &str) -> Error {
    let lower = error_message.to_ascii_lowercase();
    if lower.contains("required wal segment has been removed")
        || lower.contains("could not find record at")
        || lower.contains("replication slot") && lower.contains("invalid")
    {
        return Error::SourceError(format!(
            "replication slot '{slot_name}' appears stale or dead: {error_message}. \
             Recreate the slot or restart from a newer checkpoint/LSN."
        ));
    }

    Error::SourceError(format!(
        "pg_logical_slot_peek_binary_changes failed for slot '{slot_name}': {error_message}"
    ))
}
