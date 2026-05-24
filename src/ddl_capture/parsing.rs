//! Internal parsing helpers for DDL statement analysis.

use super::SchemaDiffOperation;
use crate::schema_history::ColumnDef;

pub(crate) fn parse_alter_clause(clause: &str) -> Option<SchemaDiffOperation> {
    let trimmed = clause.trim();
    if trimmed.is_empty() {
        return None;
    }
    let upper = trimmed.to_uppercase();

    if upper.starts_with("ADD COLUMN ") || upper.starts_with("ADD ") {
        let raw = if upper.starts_with("ADD COLUMN ") {
            &trimmed[11..]
        } else {
            &trimmed[4..]
        };
        let raw = strip_optional_keyword(raw.trim(), "IF NOT EXISTS");
        if !is_column_clause_candidate(raw) {
            return Some(SchemaDiffOperation::Unsupported {
                clause: normalize_clause_for_diff(trimmed),
            });
        }
        let column = parse_simple_column(raw)?;
        return Some(SchemaDiffOperation::AddColumn { column });
    }

    if upper.starts_with("DROP COLUMN ") || upper.starts_with("DROP ") {
        let raw = if upper.starts_with("DROP COLUMN ") {
            &trimmed[12..]
        } else {
            &trimmed[5..]
        };
        let raw = strip_optional_keyword(raw.trim(), "IF EXISTS");
        if !is_column_clause_candidate(raw) {
            return Some(SchemaDiffOperation::Unsupported {
                clause: normalize_clause_for_diff(trimmed),
            });
        }
        let name = raw
            .split_whitespace()
            .next()
            .map(normalize_identifier)
            .unwrap_or_default();
        if !name.is_empty() {
            return Some(SchemaDiffOperation::DropColumn { name });
        }
        return None;
    }

    if upper.starts_with("RENAME COLUMN ") {
        let raw = trimmed[14..].trim_start();
        let upper_raw = raw.to_uppercase();
        if let Some(to_pos) = upper_raw.find(" TO ") {
            let from = normalize_identifier(raw[..to_pos].trim());
            let to = normalize_identifier(raw[to_pos + 4..].trim());
            if !from.is_empty() && !to.is_empty() {
                return Some(SchemaDiffOperation::RenameColumn { from, to });
            }
        }
    }

    Some(SchemaDiffOperation::Unsupported {
        clause: normalize_clause_for_diff(trimmed),
    })
}

pub(crate) fn is_column_clause_candidate(raw: &str) -> bool {
    let first_token = raw.split_whitespace().next().unwrap_or("").trim();
    if first_token.is_empty() {
        return false;
    }

    // Quoted identifiers like "constraint" are valid column names.
    if first_token.starts_with('"') || first_token.starts_with('`') || first_token.starts_with('[')
    {
        return true;
    }

    let upper = first_token
        .trim_matches(|c: char| c == ',' || c == ';')
        .to_uppercase();

    !matches!(
        upper.as_str(),
        "CONSTRAINT"
            | "PRIMARY"
            | "FOREIGN"
            | "UNIQUE"
            | "CHECK"
            | "INDEX"
            | "KEY"
            | "FULLTEXT"
            | "SPATIAL"
            | "PARTITION"
            | "DEFAULT"
    )
}

pub(crate) fn normalize_clause_for_diff(clause: &str) -> String {
    let trimmed = clause.trim().trim_end_matches([';', ',']);
    let mut normalized = String::with_capacity(trimmed.len());
    let mut in_whitespace = false;
    let mut quote_state: Option<char> = None;

    let mut chars = trimmed.chars().peekable();
    while let Some(ch) = chars.next() {
        match quote_state {
            Some(']') => {
                normalized.push(ch);
                if ch == ']' {
                    if matches!(chars.peek(), Some(']')) {
                        if let Some(next) = chars.next() {
                            normalized.push(next);
                        }
                    } else {
                        quote_state = None;
                    }
                }
                continue;
            }
            Some(quote) => {
                normalized.push(ch);
                if ch == '\\' {
                    if let Some(next) = chars.next() {
                        normalized.push(next);
                    }
                    continue;
                }
                if ch == quote {
                    if matches!(chars.peek(), Some(next) if *next == quote) {
                        if let Some(next) = chars.next() {
                            normalized.push(next);
                        }
                    } else {
                        quote_state = None;
                    }
                }
                continue;
            }
            None => {}
        }

        if ch.is_whitespace() {
            if !in_whitespace {
                normalized.push(' ');
                in_whitespace = true;
            }
            continue;
        }

        in_whitespace = false;
        match ch {
            '[' => {
                quote_state = Some(']');
                normalized.push(ch);
            }
            '"' | '\'' | '`' => {
                quote_state = Some(ch);
                normalized.push(ch);
            }
            _ if ch.is_ascii_lowercase() => normalized.push(ch.to_ascii_uppercase()),
            _ => normalized.push(ch),
        }
    }

    normalized.trim().to_string()
}

pub(crate) fn split_sql_clauses(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut current = String::new();

    let mut quote_state: Option<char> = None;
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        match quote_state {
            Some(']') => {
                current.push(ch);
                if ch == ']' {
                    if matches!(chars.peek(), Some(']')) {
                        if let Some(next) = chars.next() {
                            current.push(next);
                        }
                    } else {
                        quote_state = None;
                    }
                }
                continue;
            }
            Some(quote) => {
                current.push(ch);
                if ch == '\\' {
                    if let Some(next) = chars.next() {
                        current.push(next);
                    }
                    continue;
                }
                if ch == quote {
                    if matches!(chars.peek(), Some(next) if *next == quote) {
                        if let Some(next) = chars.next() {
                            current.push(next);
                        }
                    } else {
                        quote_state = None;
                    }
                }
                continue;
            }
            None => {}
        }

        match ch {
            '"' | '\'' | '`' => {
                quote_state = Some(ch);
                current.push(ch);
            }
            '[' => {
                quote_state = Some(']');
                current.push(ch);
            }
            '(' => {
                depth = depth.saturating_add(1);
                current.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if depth == 0 => {
                if !current.trim().is_empty() {
                    out.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        out.push(current.trim().to_string());
    }

    out
}

pub(crate) fn split_alter_table_clauses(input: &str) -> Option<&str> {
    let mut in_quote: Option<char> = None;

    for (idx, ch) in input.char_indices() {
        match in_quote {
            Some('"') => {
                if ch == '"' {
                    in_quote = None;
                }
            }
            Some('`') => {
                if ch == '`' {
                    in_quote = None;
                }
            }
            Some(']') => {
                if ch == ']' {
                    in_quote = None;
                }
            }
            _ => match ch {
                '"' => in_quote = Some('"'),
                '`' => in_quote = Some('`'),
                '[' => in_quote = Some(']'),
                c if c.is_whitespace() => {
                    return Some(input[idx..].trim_start());
                }
                _ => {}
            },
        }
    }

    None
}

pub(crate) fn strip_optional_keyword<'a>(input: &'a str, keyword: &str) -> &'a str {
    let trimmed = input.trim_start();
    if trimmed.len() < keyword.len() {
        return trimmed;
    }
    let (candidate, rest) = trimmed.split_at(keyword.len());
    if candidate.eq_ignore_ascii_case(keyword)
        && (rest.is_empty()
            || rest
                .chars()
                .next()
                .map(char::is_whitespace)
                .unwrap_or(false))
    {
        rest.trim_start()
    } else {
        trimmed
    }
}

pub(crate) fn strip_alter_target_modifiers(mut input: &str) -> &str {
    loop {
        let stripped_only = strip_optional_keyword(input, "ONLY");
        let stripped_if_exists = strip_optional_keyword(stripped_only, "IF EXISTS");
        if stripped_if_exists == input {
            break input;
        }
        input = stripped_if_exists;
    }
}

/// Helper function to extract schema-qualified table name from a DDL statement.
///
/// Handles both quoted and unquoted identifiers across different databases.
pub fn extract_qualified_name(sql: &str) -> Option<(String, String)> {
    extract_qualified_name_with_default(sql, "public")
}

/// Extract schema-qualified table name and apply the given default schema.
pub fn extract_qualified_name_with_default(
    sql: &str,
    default_schema: &str,
) -> Option<(String, String)> {
    let sql = sql.trim();

    let parts = split_qualified_identifier_parts(sql);
    match parts.as_slice() {
        [] => None,
        [table] => Some((default_schema.to_string(), normalize_identifier(table))),
        _ => {
            let schema = normalize_identifier(&parts[parts.len() - 2]);
            let table = normalize_identifier(&parts[parts.len() - 1]);
            if schema.is_empty() || table.is_empty() {
                None
            } else {
                Some((schema, table))
            }
        }
    }
}

pub(crate) fn split_qualified_identifier_parts(sql: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quote_state: Option<char> = None;
    let mut chars = sql.trim().chars().peekable();

    while let Some(ch) = chars.next() {
        match quote_state {
            Some('"') => {
                current.push(ch);
                if ch == '"' {
                    if chars.peek() == Some(&'"') {
                        current.push(chars.next().unwrap());
                    } else {
                        quote_state = None;
                    }
                }
            }
            Some('`') => {
                current.push(ch);
                if ch == '`' {
                    if chars.peek() == Some(&'`') {
                        current.push(chars.next().unwrap());
                    } else {
                        quote_state = None;
                    }
                }
            }
            Some('[') => {
                current.push(ch);
                if ch == ']' {
                    if chars.peek() == Some(&']') {
                        current.push(chars.next().unwrap());
                    } else {
                        quote_state = None;
                    }
                }
            }
            _ => match ch {
                '"' | '`' => {
                    quote_state = Some(ch);
                    current.push(ch);
                }
                '[' => {
                    quote_state = Some('[');
                    current.push(ch);
                }
                '.' => {
                    let part = current.trim();
                    if part.is_empty() {
                        return Vec::new();
                    }
                    parts.push(part.to_string());
                    current.clear();
                }
                '(' | ',' | ';' if current.trim().is_empty() => break,
                ch if ch.is_whitespace() => {
                    if quote_state.is_none() {
                        break;
                    }
                    current.push(ch);
                }
                _ => current.push(ch),
            },
        }
    }

    let part = current.trim();
    if !part.is_empty() {
        parts.push(part.to_string());
    }

    parts
}

/// Extract primary keys from a CREATE TABLE statement.
pub fn extract_primary_keys(sql: &str) -> Vec<String> {
    let mut pks = Vec::new();
    let upper = sql.to_uppercase();

    if let Some(pk_start) = upper.find("PRIMARY KEY") {
        let after_pk = &sql[pk_start + 11..];
        let after_pk_trimmed = after_pk.trim_start();
        if after_pk_trimmed.starts_with('(') {
            if let Some(paren_end) = after_pk_trimmed.find(')') {
                let pk_cols = &after_pk_trimmed[1..paren_end];
                for col in pk_cols.split(',') {
                    let col_name = normalize_identifier(col.trim());
                    if !col_name.is_empty() {
                        pks.push(col_name);
                    }
                }
                return pks;
            }
        }
    }

    if let Some(start) = sql.find('(') {
        if let Some(end) = sql.rfind(')') {
            let col_defs = &sql[start + 1..end];
            let mut depth = 0;
            let mut current = String::new();

            for ch in col_defs.chars() {
                match ch {
                    '(' => {
                        depth += 1;
                        current.push(ch);
                    }
                    ')' => {
                        depth -= 1;
                        current.push(ch);
                    }
                    ',' if depth == 0 => {
                        maybe_push_inline_primary_key(&current, &mut pks);
                        current.clear();
                    }
                    _ => current.push(ch),
                }
            }

            maybe_push_inline_primary_key(&current, &mut pks);
        }
    }

    pks
}

pub(crate) fn maybe_push_inline_primary_key(column_def: &str, pks: &mut Vec<String>) {
    if !column_def.to_uppercase().contains("PRIMARY KEY") {
        return;
    }
    if let Some(col_name_raw) = column_def.split_whitespace().next() {
        let col_name = normalize_identifier(col_name_raw);
        if !col_name.is_empty() {
            pks.push(col_name);
        }
    }
}

/// Helper to normalize SQL identifiers (remove quotes, lowercase if unquoted).
pub fn normalize_identifier(ident: &str) -> String {
    let trimmed = ident.trim().trim_end_matches([';', ',']);

    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        return trimmed[1..trimmed.len() - 1]
            .replace("\"\"", "\"")
            .to_lowercase();
    }

    if trimmed.starts_with('`') && trimmed.ends_with('`') && trimmed.len() >= 2 {
        return trimmed[1..trimmed.len() - 1]
            .replace("``", "`")
            .to_lowercase();
    }

    if trimmed.starts_with('[') && trimmed.ends_with(']') && trimmed.len() >= 2 {
        return trimmed[1..trimmed.len() - 1]
            .replace("]]", "]")
            .to_lowercase();
    }

    trimmed.to_lowercase()
}

/// Helper to extract column definitions from a CREATE TABLE statement.
pub fn extract_columns_from_create(sql: &str) -> Vec<ColumnDef> {
    let mut columns = Vec::new();

    // Find content between first ( and last )
    if let Some(start) = sql.find('(') {
        if let Some(end) = sql.rfind(')') {
            let content = &sql[start + 1..end];

            for clause in split_sql_clauses(content) {
                let trimmed = clause.trim();
                if is_column_clause_candidate(trimmed) {
                    if let Some(col) = parse_enhanced_column(trimmed) {
                        columns.push(col);
                    }
                }
            }
        }
    }

    columns
}

pub(crate) fn parse_simple_column(def: &str) -> Option<ColumnDef> {
    let def = def.trim();
    let tokens: Vec<&str> = def.split_whitespace().collect();

    if tokens.len() < 2 {
        return None;
    }

    let name = normalize_identifier(tokens[0]);
    let data_type = tokens[1]
        .trim_matches(|c: char| c == ';' || c == ',')
        .to_uppercase();
    let nullable = !def.to_uppercase().contains("NOT NULL");

    Some(ColumnDef {
        name,
        data_type,
        nullable,
        constraints: vec![],
    })
}

pub(crate) fn parse_enhanced_column(def: &str) -> Option<ColumnDef> {
    let def = def.trim();

    // Extract column name (first quoted or unquoted token)
    let (name, rest) = extract_first_identifier(def)?;
    let name = normalize_identifier(&name);

    let upper_def = def.to_uppercase();

    // Check for generated/computed column indicators (stop parsing type for these)
    if upper_def.contains(" GENERATED ALWAYS AS ")
        || upper_def.contains(" AS ") && upper_def.contains("PERSISTED")
    {
        // For computed columns, just use a placeholder data type
        return Some(ColumnDef {
            name,
            data_type: "COMPUTED".to_string(),
            nullable: false, // computed columns are not nullable
            constraints: vec![],
        });
    }

    // Extract data type (second token or type(...) pattern)
    let rest = rest.trim_start();
    let (data_type, rest) = extract_column_type(rest)?;

    // Check for NOT NULL / UNIQUE / PRIMARY KEY
    let nullable = !rest.to_uppercase().contains("NOT NULL");

    Some(ColumnDef {
        name,
        data_type: data_type.to_uppercase(),
        nullable,
        constraints: vec![],
    })
}

pub(crate) fn extract_first_identifier(input: &str) -> Option<(String, String)> {
    let input = input.trim();

    if input.starts_with('"') {
        return extract_quoted_identifier(input, '"');
    } else if input.starts_with('`') {
        return extract_quoted_identifier(input, '`');
    } else if input.starts_with('[') {
        return extract_quoted_identifier(input, ']');
    } else {
        // Unquoted identifier: grab until whitespace/paren/comma
        let end_pos = input
            .char_indices()
            .find(|(_, c)| c.is_whitespace() || *c == '(' || *c == ',' || *c == ';')
            .map(|(idx, _)| idx)
            .unwrap_or(input.len());

        if end_pos > 0 {
            let identifier = input[..end_pos].to_string();
            let remaining = input[end_pos..].to_string();
            return Some((identifier, remaining));
        }
    }

    None
}

pub(crate) fn extract_quoted_identifier(input: &str, closing: char) -> Option<(String, String)> {
    let mut result = String::new();
    let mut chars = input.char_indices().peekable();

    if let Some((_, opening)) = chars.next() {
        result.push(opening);
    }

    while let Some((idx, ch)) = chars.next() {
        result.push(ch);
        if ch == closing {
            if matches!(chars.peek(), Some((_, next)) if *next == closing) {
                if let Some((_, escape_ch)) = chars.next() {
                    result.push(escape_ch);
                }
            } else {
                let remaining = input[idx + ch.len_utf8()..].to_string();
                return Some((result, remaining));
            }
        }
    }

    None
}

pub(crate) fn extract_column_type(input: &str) -> Option<(String, String)> {
    let input = input.trim_start();
    if input.is_empty() {
        return None;
    }

    let mut type_str = String::new();
    let mut paren_depth = 0;
    let mut end_idx = 0;

    for (i, ch) in input.chars().enumerate() {
        match ch {
            '(' => {
                paren_depth += 1;
                type_str.push(ch);
            }
            ')' => {
                paren_depth -= 1;
                type_str.push(ch);
            }
            ' ' if paren_depth == 0 => {
                end_idx = i;
                break;
            }
            ',' | ';' if paren_depth == 0 => {
                end_idx = i;
                break;
            }
            _ => type_str.push(ch),
        }
        end_idx = i + 1;
    }

    if type_str.is_empty() {
        return None;
    }

    let remaining = input[end_idx..].to_string();
    Some((type_str, remaining))
}
