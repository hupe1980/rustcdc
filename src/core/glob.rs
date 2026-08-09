//! Table-name glob matching, shared by every place that names tables by pattern.
//!
//! There used to be two: the sink router matched globs, and the connectors'
//! `table_include_list` / `table_exclude_list` matched exact strings only. Nothing said
//! so, so `table_include_list = ["public.audit_*"]` silently captured nothing — an
//! allowlist that matches no table is indistinguishable from a table that never changed.
//! One implementation, one documented semantics.

/// Returns `true` if `subject` matches the glob `pattern`.
///
/// Supported wildcards, both scoped to a single segment (they do not cross a `.`
/// boundary in [`table_matches`]):
///
/// - `*` — zero or more of any character.
/// - `?` — exactly one character.
///
/// Matching is over **bytes**, not characters, so a `?` against a multi-byte UTF-8
/// character matches one byte of it rather than the whole character. Identifiers in every
/// database this crate supports are ASCII in practice; a pattern with `?` against a
/// non-ASCII name is the one case where that distinction is observable.
pub(crate) fn glob_segment_matches(pattern: &str, subject: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if !pattern.contains(['*', '?']) {
        return pattern == subject;
    }
    glob_match(pattern.as_bytes(), subject.as_bytes())
}

/// Greedy glob match with a single backtrack point.
///
/// Linear in `subject.len() * (number of '*' in pattern)`, where the naive recursive form
/// — "try consuming nothing, else consume one byte, recurse both ways" — is exponential:
/// `a*a*a*a*a*b` against thirty `a`s explores every way of splitting the run and does not
/// return in any useful time. Patterns here come from operator configuration rather than
/// untrusted input, so this was a latency cliff rather than a vulnerability, but a config
/// typo should not be able to hang a pipeline.
///
/// The algorithm keeps one remembered `*` position and rewinds to it on a mismatch,
/// which is sufficient because `*` matches any run: whenever a later match fails, giving
/// the most recent `*` one more byte is the only move worth trying.
fn glob_match(pattern: &[u8], subject: &[u8]) -> bool {
    let mut p = 0usize;
    let mut s = 0usize;
    // Position in `pattern` just past the last `*` seen, and the position in `subject`
    // that `*` is currently matched up to.
    let mut star: Option<(usize, usize)> = None;

    loop {
        if p < pattern.len() {
            match pattern[p] {
                b'*' => {
                    star = Some((p + 1, s));
                    p += 1;
                    continue;
                }
                b'?' if s < subject.len() => {
                    p += 1;
                    s += 1;
                    continue;
                }
                byte if s < subject.len() && byte == subject[s] => {
                    p += 1;
                    s += 1;
                    continue;
                }
                _ => {}
            }
        } else if s == subject.len() {
            return true;
        }

        // Mismatch, or pattern exhausted with input left: extend the last `*` by one byte.
        match star {
            Some((star_p, star_s)) if star_s < subject.len() => {
                p = star_p;
                s = star_s + 1;
                star = Some((star_p, s));
            }
            _ => return false,
        }
    }
}

/// Match `pattern` against `table_key`, where `table_key` is `"schema.table"` or a bare
/// `"table"`.
///
/// | Pattern          | Matches                                                        |
/// |------------------|----------------------------------------------------------------|
/// | `"*"`            | anything, qualified or bare — a true catch-all                  |
/// | `"*.*"`          | any **qualified** `schema.table`, never a bare name             |
/// | `"schema.*"`     | any table in that schema                                       |
/// | `"*.table"`      | `table` in any schema                                          |
/// | `"schema.table"` | that table in that schema, and nothing bare                    |
/// | `"table"`        | `table` in **any** schema, and the bare name — see below        |
/// | `"pre*"`         | any table whose name starts with `pre`, in any schema           |
///
/// # An unqualified pattern is schema-agnostic
///
/// `"orders"` matches `public.orders` *and* `staging.orders`. That is deliberate — MySQL
/// callers name tables bare, and requiring a schema there would make every pattern
/// database-specific — but it is a widening, and worth stating rather than discovering.
/// In an allowlist it admits tables the author may not have meant to include; write
/// `"public.orders"` when the schema matters.
///
/// The first `.` splits the pattern, so a schema name containing a dot cannot be
/// expressed. No supported database permits one unquoted.
pub(crate) fn table_matches(pattern: &str, table_key: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    match (pattern.find('.'), table_key.find('.')) {
        // Both qualified: match schema against schema, table against table.
        (Some(pi), Some(ti)) => {
            let (pattern_schema, pattern_table) = pattern.split_at(pi);
            let (key_schema, key_table) = table_key.split_at(ti);
            glob_segment_matches(pattern_schema, key_schema)
                && glob_segment_matches(&pattern_table[1..], &key_table[1..])
        }
        // Pattern demands a schema the key does not have.
        (Some(_), None) => false,
        // Unqualified pattern against a qualified key: the table half only.
        (None, Some(ti)) => glob_segment_matches(pattern, &table_key[ti + 1..]),
        (None, None) => glob_segment_matches(pattern, table_key),
    }
}

/// `true` when `pattern` names no schema, and so matches its table in **every** schema.
///
/// Used to warn about a widened allowlist at configuration time rather than leaving the
/// operator to notice extra tables in the output.
pub(crate) fn is_schema_agnostic(pattern: &str) -> bool {
    pattern != "*" && !pattern.contains('.')
}

#[cfg(test)]
mod tests {
    use super::{glob_segment_matches, is_schema_agnostic, table_matches};

    #[test]
    fn literal_patterns_match_exactly() {
        assert!(glob_segment_matches("orders", "orders"));
        assert!(!glob_segment_matches("orders", "order"));
        assert!(!glob_segment_matches("orders", "orderss"));
        assert!(!glob_segment_matches("orders", ""));
    }

    #[test]
    fn star_matches_any_run_including_empty() {
        assert!(glob_segment_matches("*", ""));
        assert!(glob_segment_matches("pre*", "pre"));
        assert!(glob_segment_matches("pre*", "prefix"));
        assert!(glob_segment_matches("*fix", "prefix"));
        assert!(glob_segment_matches("p*f*x", "prefix"));
        assert!(!glob_segment_matches("pre*", "xpre"));
        assert!(glob_segment_matches("a*b*c", "abc"));
        assert!(!glob_segment_matches("a*b*c", "acb"));
    }

    #[test]
    fn question_mark_matches_exactly_one() {
        assert!(glob_segment_matches("t?ble", "table"));
        assert!(!glob_segment_matches("t?ble", "tble"));
        assert!(!glob_segment_matches("t?ble", "taable"));
    }

    /// The naive recursive matcher does not return on this input in any useful time.
    #[test]
    fn a_pathological_pattern_returns_promptly() {
        let subject = "a".repeat(64);
        assert!(!glob_segment_matches("a*a*a*a*a*a*a*a*a*a*b", &subject));
        assert!(glob_segment_matches("a*a*a*a*a*a*a*a*a*a*a", &subject));
    }

    #[test]
    fn qualified_and_unqualified_keys_follow_the_documented_table() {
        assert!(table_matches("*", "public.orders"));
        assert!(table_matches("*", "orders"));

        assert!(table_matches("*.*", "public.orders"));
        assert!(!table_matches("*.*", "orders"));

        assert!(table_matches("public.*", "public.orders"));
        assert!(!table_matches("public.*", "private.orders"));

        assert!(table_matches("*.orders", "public.orders"));
        assert!(!table_matches("*.orders", "public.products"));

        assert!(table_matches("public.orders", "public.orders"));
        assert!(!table_matches("public.orders", "public.products"));
        assert!(!table_matches("public.orders", "orders"));

        assert!(table_matches("audit_*", "public.audit_log"));
        assert!(!table_matches("audit_*", "public.orders"));
    }

    /// The behaviour the old doc table denied and the old code implemented.
    #[test]
    fn an_unqualified_pattern_matches_every_schema() {
        assert!(table_matches("orders", "orders"));
        assert!(table_matches("orders", "public.orders"));
        assert!(
            table_matches("orders", "staging.orders"),
            "documented as a widening: an unqualified pattern is schema-agnostic"
        );
        assert!(is_schema_agnostic("orders"));
        assert!(is_schema_agnostic("audit_*"));
        assert!(!is_schema_agnostic("public.orders"));
        assert!(!is_schema_agnostic("*"), "a catch-all is not a silent widening");
    }
}
