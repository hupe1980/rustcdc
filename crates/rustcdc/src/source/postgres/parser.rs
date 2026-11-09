use crate::{
    checkpoint::PostgresOffset,
    core::{Error, Offset, Result, SlotWalStatus, SourceErrorKind},
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

fn finalize_postgres_identifier_segment(
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

/// Test helper: verifies the divergence-detection logic in isolation.
/// Production code self-heals via `query::reconcile_stream_resume_lsn_with_retry`.
#[cfg(test)]
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

/// What `pg_replication_slots` says about one slot, read once per sample.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct SlotState {
    /// `wal_status`; `None` when the server leaves it null or spells it in a way this
    /// crate does not know.
    pub(super) wal_status: Option<SlotWalStatus>,
    /// `safe_wal_size` in bytes; `None` when `max_slot_wal_keep_size` is unlimited.
    pub(super) safe_wal_size: Option<i64>,
    /// `invalidation_reason` (PostgreSQL 17+): `wal_removed`, `idle_timeout`, ….
    pub(super) invalidation_reason: Option<String>,
}

impl SlotState {
    fn is_invalidated(&self) -> bool {
        self.wal_status == Some(SlotWalStatus::Lost) || self.invalidation_reason.is_some()
    }
}

/// SQLSTATE `object_not_in_prerequisite_state`, which PostgreSQL raises for a slot it has
/// invalidated — and for much else, which is why the slot state has to confirm it.
pub(super) const SQLSTATE_OBJECT_NOT_IN_PREREQUISITE_STATE: &str = "55000";

/// The one rule for "the source has invalidated this slot", for every caller: the peek
/// poll, `START_REPLICATION` on the streaming transport, the periodic slot sample and
/// startup validation.
///
/// Decided by the slot's own state, never by the server's message: the wording differs
/// between PostgreSQL 16 and 18, and `55000` alone covers unrelated conditions. A caller
/// holding a server error passes its SQLSTATE so an unrelated error is not reclassified
/// because the slot happens to be lost; a caller with no error passes `None`.
pub(super) fn classify_slot_failure(
    slot_name: &str,
    sqlstate: Option<&str>,
    slot_state: Option<&SlotState>,
) -> Option<Error> {
    if sqlstate.is_some_and(|code| code != SQLSTATE_OBJECT_NOT_IN_PREREQUISITE_STATE) {
        return None;
    }
    let state = slot_state.filter(|state| state.is_invalidated())?;
    let why = match &state.invalidation_reason {
        Some(reason) => format!("invalidation_reason = {reason}"),
        None => "wal_status = lost".to_string(),
    };
    Some(Error::source_error(
        SourceErrorKind::SlotInvalidated,
        format!(
            "postgres invalidated replication slot '{slot_name}' ({why}): the WAL between \
             the last confirmed position and the invalidation is gone, and neither a retry \
             nor a restart can recover it. Drop and recreate the slot, then re-snapshot the \
             captured tables; a re-snapshot does not replay rows deleted during the gap."
        ),
    ))
}

/// The error for a failed `pg_logical_slot_peek_binary_changes`.
pub(super) fn map_pgoutput_poll_error(
    slot_name: &str,
    sqlstate: Option<&str>,
    slot_state: Option<&SlotState>,
    error_message: &str,
) -> Error {
    classify_slot_failure(slot_name, sqlstate, slot_state).unwrap_or_else(|| {
        Error::SourceError(format!(
            "pg_logical_slot_peek_binary_changes failed for slot '{slot_name}': {error_message}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(wal_status: Option<SlotWalStatus>, reason: Option<&str>) -> SlotState {
        SlotState {
            wal_status,
            safe_wal_size: None,
            invalidation_reason: reason.map(str::to_string),
        }
    }

    #[test]
    fn the_classifier_needs_the_slot_state_not_the_message() {
        let lost = state(Some(SlotWalStatus::Lost), None);
        let idle = state(Some(SlotWalStatus::Lost), Some("idle_timeout"));
        let reason_only = state(None, Some("wal_removed"));
        let reserved = state(Some(SlotWalStatus::Reserved), None);
        let unreserved = state(Some(SlotWalStatus::Unreserved), None);

        let kind = |error: Option<Error>| error.and_then(|e| e.source_kind());
        let invalidated = Some(SourceErrorKind::SlotInvalidated);

        assert_eq!(
            kind(classify_slot_failure("s1", Some("55000"), Some(&lost))),
            invalidated
        );
        assert_eq!(
            kind(classify_slot_failure("s1", None, Some(&lost))),
            invalidated
        );
        assert_eq!(
            kind(classify_slot_failure("s1", None, Some(&reason_only))),
            invalidated
        );

        let named = classify_slot_failure("s1", None, Some(&idle)).expect("invalidated");
        assert!(named.to_string().contains("'s1'"), "{named}");
        assert!(named.to_string().contains("idle_timeout"), "{named}");

        // A healthy or endangered slot is not lost, whatever the error said.
        assert!(classify_slot_failure("s1", Some("55000"), Some(&reserved)).is_none());
        assert!(classify_slot_failure("s1", Some("55000"), Some(&unreserved)).is_none());
        // An unrelated error is not reclassified because the slot happens to be lost.
        assert!(classify_slot_failure("s1", Some("57P01"), Some(&lost)).is_none());

        // The server's own words, PostgreSQL 16 and 18, without a slot state: not enough.
        for message in [
            "55000: can no longer get changes from replication slot \"s1\" (detail: This \
             slot has been invalidated because it exceeded the maximum reserved size.)",
            "55000: can no longer access replication slot \"s1\" (detail: This replication \
             slot has been invalidated due to \"wal_removed\".)",
        ] {
            let error = map_pgoutput_poll_error("s1", Some("55000"), None, message);
            assert_eq!(error.source_kind(), None, "{error}");
            assert!(error.is_recoverable(), "{error}");
        }
        let error = map_pgoutput_poll_error("s1", Some("55000"), Some(&lost), "db error");
        assert_eq!(error.source_kind(), invalidated);
    }
}
