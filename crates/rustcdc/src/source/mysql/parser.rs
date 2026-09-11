use crate::{
    checkpoint::MysqlOffset,
    core::{Error, Offset, Result},
};

pub(super) fn decode_stream_resume_position(
    expected_source: &str,
    offset: &dyn Offset,
) -> Result<MysqlOffset> {
    if offset.source_type() != expected_source {
        return Err(Error::CheckpointError(format!(
            "mysql stream offset source type mismatch: expected '{expected_source}', found '{}'",
            offset.source_type()
        )));
    }

    MysqlOffset::from_bytes(&offset.encode()?)
}

pub(super) fn parse_mysql_source_offset(offset: &str) -> Option<(&str, u32)> {
    let (file, rest) = offset.split_once(':')?;
    let pos = rest
        .split_once("#gtid=")
        .map_or(rest, |(position, _)| position)
        .parse::<u32>()
        .ok()?;
    Some((file, pos))
}

pub(super) fn format_mysql_source_offset(binlog_file: &str, binlog_pos: u32, gtid: &str) -> String {
    if gtid.is_empty() {
        format!("{binlog_file}:{binlog_pos}")
    } else {
        format!("{binlog_file}:{binlog_pos}#gtid={gtid}")
    }
}

pub(super) fn split_table_reference(table: &str) -> Result<(Option<String>, String)> {
    let trimmed = table.trim();
    if trimmed.is_empty() {
        return Err(Error::ConfigError(
            "mysql snapshot table name must not be empty".into(),
        ));
    }

    let parts = parse_mysql_identifier_path(trimmed)?;
    match parts.as_slice() {
        [table_name] => Ok((None, table_name.to_string())),
        [schema_name, table_name] => Ok((Some(schema_name.to_string()), table_name.to_string())),
        _ => Err(Error::ConfigError(format!(
            "mysql snapshot table name is invalid: {trimmed}"
        ))),
    }
}

fn parse_mysql_identifier_path(input: &str) -> Result<Vec<String>> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut segment_quoted = false;

    let chars: Vec<char> = input.chars().collect();
    let mut idx = 0;
    while idx < chars.len() {
        let ch = chars[idx];
        if in_quotes {
            if ch == '`' {
                if chars.get(idx + 1) == Some(&'`') {
                    current.push('`');
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
            '`' => {
                if !current.trim().is_empty() {
                    return Err(Error::ConfigError(format!(
                        "mysql snapshot table name is invalid: {input}"
                    )));
                }
                current.clear();
                in_quotes = true;
                segment_quoted = true;
                idx += 1;
            }
            '.' => {
                let segment = finalize_mysql_identifier_segment(&current, segment_quoted, input)?;
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
            "mysql snapshot table name is invalid: {input}"
        )));
    }

    let last = finalize_mysql_identifier_segment(&current, segment_quoted, input)?;
    parts.push(last);
    Ok(parts)
}

fn finalize_mysql_identifier_segment(raw: &str, quoted: bool, full_input: &str) -> Result<String> {
    let segment = if quoted {
        raw.to_string()
    } else {
        raw.trim().to_string()
    };

    if segment.is_empty() {
        return Err(Error::ConfigError(format!(
            "mysql snapshot table name is invalid: {full_input}"
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
            "mysql snapshot table name is invalid: {full_input}"
        )));
    }

    Ok(segment)
}

pub(super) fn quoted_mysql_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

pub(super) fn mysql_qualified_table_name(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(schema_name) => format!(
            "{}.{}",
            quoted_mysql_identifier(schema_name),
            quoted_mysql_identifier(table)
        ),
        None => quoted_mysql_identifier(table),
    }
}

pub(super) fn mysql_qualified_table_name_from_reference(table: &str) -> Result<String> {
    let (schema, name) = split_table_reference(table)?;
    Ok(mysql_qualified_table_name(schema.as_deref(), &name))
}

/// Parse the target of a `TRUNCATE [TABLE] ...` binlog query event.
///
/// Returns `Some((schema, table))` when the statement is a TRUNCATE and the
/// target name is parseable. Returns `None` for unrecognised statements.
///
/// MySQL logs `TRUNCATE TABLE` as a `Query_log_event`. Supported forms:
/// - `TRUNCATE TABLE \`schema\`.\`table\``
/// - `TRUNCATE TABLE table_name`
/// - `TRUNCATE \`schema\`.\`table\`` (`TABLE` keyword is optional per SQL standard)
pub(super) fn parse_truncate_target(statement: &str) -> Option<(Option<String>, String)> {
    let trimmed = statement.trim_start();
    // Keyword detection on uppercase; identifier extraction from original bytes.
    let upper = trimmed.to_uppercase();
    let after_kw = upper.strip_prefix("TRUNCATE")?;
    // Compute byte offset: len("TRUNCATE") plus however many leading spaces follow.
    let kw_end = trimmed.len() - after_kw.len();
    let after_kw_orig = &trimmed[kw_end..];
    let after_kw_upper = after_kw.trim_start();
    let spaces_after_kw = after_kw.len() - after_kw_upper.len();
    let trimmed_after_kw = &after_kw_orig[spaces_after_kw..];

    // Skip optional TABLE keyword.
    let table_ref_orig = if trimmed_after_kw.to_uppercase().starts_with("TABLE") {
        let s = trimmed_after_kw["TABLE".len()..].trim_start();
        // Must have at least one whitespace/backtick between TABLE and identifier.
        s
    } else {
        trimmed_after_kw
    };

    let cleaned = table_ref_orig.trim().trim_end_matches(';').trim();
    if cleaned.is_empty() {
        return None;
    }

    split_table_reference(cleaned).ok()
}

/// Strip anything a binlog file name cannot contain from a ROTATE event's name field.
///
/// # Why this is needed
///
/// A binlog file name is `<base>.<sequence>` and nothing else. What arrives on the wire can
/// be longer: the ROTATE event carries a trailing CRC32 when `binlog_checksum` is on, and the
/// *fake* rotate the server sends before the FORMAT_DESCRIPTION_EVENT is emitted before the
/// reader knows which checksum algorithm is in force. MariaDB appends it there; MySQL does
/// not, so only MariaDB shows the damage — and rustcdc claims MariaDB's protocol differences
/// are handled transparently, which makes this ours to absorb.
///
/// Measured against MariaDB 10.6, the name came through as
/// `mysql-bin.000002\x57\x07\x03\x52` — the four checksum bytes appended raw, two of them
/// printable. Two failures followed from that one string, and neither was loud:
///
/// * File+position resume failed outright — the server answers
///   *"Could not find first log file name in binary log index file"* and the stream never
///   starts. With GTID positioning enabled the resume still works, which is what kept this
///   hidden.
/// * The checkpoint rewind guard silently switched itself off.
///   [`binlog_coordinate`](crate::checkpoint) parses the sequence suffix with
///   `"000002WR".parse::<u64>()`, which fails, and a `None` there means "not comparable" —
///   so a genuinely regressed MariaDB coordinate would have been written without objection.
///
/// Truncating at the first byte that is not a filename byte is not enough: `0x57` is `W`.
/// The suffix after the final `.` must be digits, so that is what is enforced.
pub(super) fn sanitize_binlog_file_name(raw: &str) -> Result<String> {
    let (base, suffix) = raw.rsplit_once('.').ok_or_else(|| {
        Error::SourceError(format!(
            "mysql rotate event carried an unusable binlog file name {raw:?}: expected \
             '<base>.<sequence>'"
        ))
    })?;

    let digits: String = suffix.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        return Err(Error::SourceError(format!(
            "mysql rotate event carried an unusable binlog file name {raw:?}: the sequence \
             suffix after the final '.' is not numeric"
        )));
    }
    if base.is_empty() || base.chars().any(|c| c.is_control() || !c.is_ascii()) {
        return Err(Error::SourceError(format!(
            "mysql rotate event carried an unusable binlog file name {raw:?}: the base name \
             is empty or contains bytes a binlog file name cannot hold"
        )));
    }

    let sanitized = format!("{base}.{digits}");
    if sanitized.len() != raw.len() {
        tracing::debug!(
            target: "rustcdc::source::mysql",
            raw = ?raw,
            sanitized = %sanitized,
            dropped_bytes = raw.len() - sanitized.len(),
            "trimmed trailing bytes from a rotate event's binlog file name; these are the \
             event checksum, which the server appends before the reader has been told which \
             checksum algorithm is in force",
        );
    }
    Ok(sanitized)
}

#[cfg(test)]
mod tests {

    #[test]
    fn a_rotate_name_with_a_trailing_checksum_is_trimmed_to_the_file_name() {
        // Measured against MariaDB 10.6: the four CRC32 bytes arrive appended to the name,
        // and two of them are printable ASCII — so truncating at the first byte that "looks
        // wrong" does not work. The sequence suffix must be digits, and that is the rule.
        let raw = "mysql-bin.000002\u{57}\u{7}\u{3}\u{52}";
        assert_eq!(
            sanitize_binlog_file_name(raw).expect("trims to a usable name"),
            "mysql-bin.000002",
        );
    }

    #[test]
    fn a_clean_rotate_name_is_returned_unchanged() {
        assert_eq!(
            sanitize_binlog_file_name("binlog.001234").expect("already clean"),
            "binlog.001234",
        );
        assert_eq!(
            sanitize_binlog_file_name("mysql-bin.1000000").expect("seven digits"),
            "mysql-bin.1000000",
        );
    }

    #[test]
    fn a_name_with_no_numeric_suffix_is_rejected_rather_than_guessed_at() {
        // Writing a guess into a durable checkpoint is how the original defect stayed
        // invisible: a corrupted name disabled the rewind guard silently, because an
        // unparseable sequence reads as "not comparable" rather than as an error.
        for raw in ["mysql-bin", "mysql-bin.", "mysql-bin.abc"] {
            let error = sanitize_binlog_file_name(raw)
                .expect_err("an unusable binlog file name must not reach a checkpoint");
            assert!(
                error.to_string().contains("unusable binlog file name"),
                "got: {error}"
            );
        }
    }

    use super::*;

    #[test]
    fn parse_truncate_with_table_keyword_unqualified() {
        let result = parse_truncate_target("TRUNCATE TABLE orders");
        assert_eq!(result, Some((None, "orders".to_string())));
    }

    #[test]
    fn parse_truncate_without_table_keyword() {
        let result = parse_truncate_target("TRUNCATE orders");
        assert_eq!(result, Some((None, "orders".to_string())));
    }

    #[test]
    fn parse_truncate_qualified_backticks() {
        let result = parse_truncate_target("TRUNCATE TABLE `mydb`.`orders`");
        assert_eq!(
            result,
            Some((Some("mydb".to_string()), "orders".to_string()))
        );
    }

    #[test]
    fn parse_truncate_qualified_no_backticks() {
        let result = parse_truncate_target("TRUNCATE TABLE mydb.orders");
        assert_eq!(
            result,
            Some((Some("mydb".to_string()), "orders".to_string()))
        );
    }

    #[test]
    fn parse_truncate_mixed_case() {
        let result = parse_truncate_target("truncate table `shop`.`line_items`");
        assert_eq!(
            result,
            Some((Some("shop".to_string()), "line_items".to_string()))
        );
    }

    #[test]
    fn parse_truncate_with_trailing_semicolon() {
        let result = parse_truncate_target("TRUNCATE TABLE orders;");
        assert_eq!(result, Some((None, "orders".to_string())));
    }

    #[test]
    fn parse_truncate_returns_none_for_begin() {
        assert!(parse_truncate_target("BEGIN").is_none());
    }

    #[test]
    fn parse_truncate_returns_none_for_drop_table() {
        assert!(parse_truncate_target("DROP TABLE orders").is_none());
    }

    #[test]
    fn parse_truncate_returns_none_for_empty() {
        assert!(parse_truncate_target("").is_none());
    }

    #[test]
    fn format_mysql_source_offset_with_gtid() {
        let result = format_mysql_source_offset("mysql-bin.000001", 1234, "abc-def");
        assert_eq!(result, "mysql-bin.000001:1234#gtid=abc-def");
    }

    #[test]
    fn format_mysql_source_offset_no_gtid() {
        let result = format_mysql_source_offset("mysql-bin.000001", 1234, "");
        assert_eq!(result, "mysql-bin.000001:1234");
    }
}
