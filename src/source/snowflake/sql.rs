//! Statement construction for the Snowflake connector.
//!
//! Kept apart from the connector so the SQL can be read, reviewed and unit-tested as text.
//! Snowflake's SQL REST API takes bind parameters, but **not for identifiers** and not
//! inside the `CHANGES`/`AT`/`END` clause — those are parsed as part of the statement — so
//! every identifier here is validated and quoted rather than bound, and every time marker
//! is rendered from a `u64` that cannot carry a quote.

use crate::core::{Error, Result};

/// Quote an identifier for Snowflake, doubling any embedded quote.
///
/// Snowflake folds unquoted identifiers to **upper** case and preserves quoted ones
/// exactly, so quoting is not optional: a table created as `orders` is `ORDERS`, and one
/// created as `"orders"` is `orders`. Quoting whatever the operator wrote is the only
/// behaviour that round-trips both.
pub(super) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Reject an identifier that cannot be safely quoted into a statement.
///
/// Doubling handles the quote character itself, but a NUL or a control character has no
/// escape and would either truncate the statement or make it unparseable in a way that is
/// hard to attribute. An empty identifier is always a configuration mistake.
pub(super) fn validate_identifier(value: &str, what: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::ConfigError(format!(
            "snowflake {what} must not be empty"
        )));
    }
    if value.len() > 255 {
        return Err(Error::ConfigError(format!(
            "snowflake {what} '{value}' exceeds the 255-character identifier limit"
        )));
    }
    if value.chars().any(|ch| ch == '\0' || ch.is_control()) {
        return Err(Error::ConfigError(format!(
            "snowflake {what} '{value}' contains a control character. Identifiers are \
             quoted into the statement rather than bound — Snowflake takes no bind \
             parameter for an identifier — and a control character has no escape."
        )));
    }
    Ok(())
}

/// A fully qualified `"DB"."SCHEMA"."TABLE"`.
pub(super) fn qualified_name(database: &str, schema: &str, table: &str) -> String {
    format!(
        "{}.{}.{}",
        quote_identifier(database),
        quote_identifier(schema),
        quote_identifier(table)
    )
}

/// The statement that reads the current instant as epoch nanoseconds.
///
/// Read once per poll and used as the window's upper bound. Taking it from the server
/// rather than from the process clock is what makes the window well defined: the client's
/// clock has no defined relationship to the one Snowflake stamps its versions with, and a
/// client running even milliseconds fast would ask for a window ending in the future and
/// silently skip the changes that land in the gap.
pub(super) fn current_epoch_nanos_statement() -> String {
    "SELECT DATE_PART(EPOCH_NANOSECOND, CURRENT_TIMESTAMP())::VARCHAR AS NOW_NANOS".to_string()
}

/// Render an epoch-nanosecond instant as a Snowflake `TIMESTAMP_LTZ` expression.
///
/// `TO_TIMESTAMP_LTZ(<numeric>, 9)` reads the number as nanoseconds. The input is a `u64`,
/// so this cannot be an injection point.
pub(super) fn timestamp_expr(epoch_nanos: u64) -> String {
    format!("TO_TIMESTAMP_LTZ({epoch_nanos}, 9)")
}

/// The `CHANGES` query for one table over the half-open window `(from, to]`.
///
/// `AT` is the interval's start and `END` its inclusive finish, so passing the previous
/// window's `END` as the next window's `AT` joins consecutive windows without a gap and
/// without re-reading the boundary — which is exactly what the checkpointed offset holds.
pub(super) fn changes_statement(
    database: &str,
    schema: &str,
    table: &str,
    from_nanos: u64,
    to_nanos: u64,
    append_only: bool,
) -> String {
    let information = if append_only {
        "APPEND_ONLY"
    } else {
        "DEFAULT"
    };
    format!(
        "SELECT * FROM {} CHANGES(INFORMATION => {information}) AT(TIMESTAMP => {}) END(TIMESTAMP => {})",
        qualified_name(database, schema, table),
        timestamp_expr(from_nanos),
        timestamp_expr(to_nanos),
    )
}

/// One keyset-paginated chunk of a time-travel-consistent table read.
///
/// `AT(TIMESTAMP => …)` pins every chunk to the **same** instant, which is what makes the
/// snapshot consistent without holding a transaction open: Snowflake serves each chunk from
/// the table version at that instant, so concurrent writes cannot be half-seen across
/// chunk boundaries. It is also why the snapshot needs no watermark bracket at all — the
/// stream simply starts at the same instant the snapshot was pinned to.
pub(super) fn snapshot_chunk_statement(
    database: &str,
    schema: &str,
    table: &str,
    at_nanos: u64,
    primary_key: &[String],
    after: Option<&[String]>,
    chunk_size: usize,
) -> String {
    let columns: Vec<String> = primary_key
        .iter()
        .map(|key| quote_identifier(key))
        .collect();
    let order = columns.join(", ");

    let predicate = match after {
        // Row-value comparison: `(a, b) > (?, ?)` is the keyset predicate, and Snowflake
        // supports it directly. Expanding it into an OR-chain by hand is the usual source
        // of an off-by-one that skips or repeats a row at every chunk boundary.
        Some(values) if !values.is_empty() => {
            let literals: Vec<String> = values.iter().map(|value| quote_literal(value)).collect();
            format!(
                " WHERE ({}) > ({})",
                columns.join(", "),
                literals.join(", ")
            )
        }
        _ => String::new(),
    };

    format!(
        "SELECT * FROM {} AT(TIMESTAMP => {}){predicate} ORDER BY {order} LIMIT {chunk_size}",
        qualified_name(database, schema, table),
        timestamp_expr(at_nanos),
    )
}

/// Quote a key value as a string literal, doubling any embedded apostrophe.
///
/// Keyset cursor values come back from a previous chunk as text and go into the next
/// chunk's predicate. Comparing them as strings is deliberate: Snowflake compares a string
/// literal against a numeric column by coercing the literal, so a numeric key still orders
/// numerically, while a text key is compared with the collation the column declares.
pub(super) fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_identifier_quote_is_doubled_rather_than_escaped() {
        // Snowflake, like PostgreSQL, escapes a quote inside a quoted identifier by
        // doubling it. A backslash would end the identifier and leave the rest of the
        // name as syntax.
        assert_eq!(quote_identifier(r#"od"d"#), r#""od""d""#);
        assert_eq!(quote_identifier("orders"), r#""orders""#);
    }

    #[test]
    fn an_identifier_with_a_control_character_is_refused() {
        for hostile in ["a\0b", "a\nb", ""] {
            assert!(
                validate_identifier(hostile, "table").is_err(),
                "{hostile:?} must be refused"
            );
        }
        validate_identifier("orders", "table").expect("an ordinary name is accepted");
        validate_identifier(r#"od"d"#, "table").expect("a quote is escapable, so it is allowed");
    }

    #[test]
    fn the_changes_window_is_rendered_from_integers_only() {
        // The time markers are the one part of the statement an attacker-influenced value
        // could reach if they were strings. They are `u64`, and this pins that.
        let sql = changes_statement(
            "db",
            "sc",
            "orders",
            1_700_000_000_000_000_000,
            1_700_000_060_000_000_000,
            false,
        );
        assert!(sql.contains("AT(TIMESTAMP => TO_TIMESTAMP_LTZ(1700000000000000000, 9))"));
        assert!(sql.contains("END(TIMESTAMP => TO_TIMESTAMP_LTZ(1700000060000000000, 9))"));
        assert!(sql.contains(r#"FROM "db"."sc"."orders""#));
        assert!(sql.contains("INFORMATION => DEFAULT"));
    }

    #[test]
    fn append_only_selects_the_cheaper_information_mode() {
        let sql = changes_statement("db", "sc", "t", 1, 2, true);
        assert!(sql.contains("INFORMATION => APPEND_ONLY"));
    }

    #[test]
    fn the_first_snapshot_chunk_has_no_keyset_predicate() {
        let sql = snapshot_chunk_statement("db", "sc", "t", 5, &["id".into()], None, 100);
        assert!(!sql.contains("WHERE"), "got: {sql}");
        assert!(sql.contains(r#"ORDER BY "id" LIMIT 100"#), "got: {sql}");
        assert!(sql.contains("AT(TIMESTAMP => TO_TIMESTAMP_LTZ(5, 9))"));
    }

    #[test]
    fn a_composite_keyset_uses_a_row_value_comparison() {
        // `(a, b) > (x, y)` is one predicate. The hand-expanded OR-chain that replaces it
        // in most implementations is where the boundary row gets skipped or repeated.
        let sql = snapshot_chunk_statement(
            "db",
            "sc",
            "t",
            5,
            &["tenant".into(), "id".into()],
            Some(&["7".into(), "42".into()]),
            100,
        );
        assert!(
            sql.contains(r#"WHERE ("tenant", "id") > ('7', '42')"#),
            "got: {sql}"
        );
    }

    #[test]
    fn a_key_value_containing_an_apostrophe_is_escaped() {
        // Keyset cursors are row data, so they are arbitrary text.
        let sql = snapshot_chunk_statement(
            "db",
            "sc",
            "t",
            5,
            &["name".into()],
            Some(&["o'brien".into()]),
            10,
        );
        assert!(sql.contains(r#"> ('o''brien')"#), "got: {sql}");
    }
}
