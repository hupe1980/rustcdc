//! Byte-bounded truncation that cannot panic and cannot produce invalid UTF-8.
//!
//! Two places cap a string at a byte budget: the audit-trail detail and the dead-letter
//! record's exception header. Both bound attacker-influenced text, so neither may use
//! `&text[..LIMIT]` — that panics when the limit lands inside a multi-byte character, and
//! in the signal-action worker one such panic silently ends all later signal processing.

use std::borrow::Cow;

/// Trim `text` to at most `max_bytes`, never splitting a character.
///
/// Borrows when the input already fits. Otherwise the result is the longest prefix that
/// fits plus `suffix`, so the value says on its face that it is incomplete. `suffix` is
/// appended beyond the budget: the budget bounds untrusted input, and a marker the caller
/// chose is not that.
pub(crate) fn truncate_utf8<'a>(text: &'a str, max_bytes: usize, suffix: &str) -> Cow<'a, str> {
    if text.len() <= max_bytes {
        return Cow::Borrowed(text);
    }

    // The largest index ≤ max_bytes that starts a character. `floor_char_boundary` is
    // still unstable; scanning back at most three bytes is not worth waiting for it.
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }

    let mut truncated = String::with_capacity(end + suffix.len());
    truncated.push_str(&text[..end]);
    truncated.push_str(suffix);
    Cow::Owned(truncated)
}

#[cfg(test)]
mod tests {
    use super::truncate_utf8;

    #[test]
    fn a_value_within_budget_is_returned_untouched() {
        assert_eq!(truncate_utf8("short", 64, " [truncated]"), "short");
        // Exactly at the budget is within it.
        assert_eq!(truncate_utf8("abcd", 4, "!"), "abcd");
    }

    /// The regression this module exists for.
    ///
    /// `&text[..limit]` panics here. Reached from the admin API by a signal whose message
    /// puts a multi-byte character across the audit detail's byte budget — which killed
    /// the signal worker, not just the request.
    #[test]
    fn truncating_inside_a_multi_byte_character_does_not_panic() {
        // 3 bytes each, so every budget from 1..3 lands mid-character.
        let text = "世界世界";
        for budget in 0..text.len() {
            let out = truncate_utf8(text, budget, "…");
            assert!(
                out.len() <= budget + "…".len(),
                "budget {budget} exceeded: {out:?}"
            );
            // The point: it is still a `str`, so it is still valid UTF-8 by construction.
            assert!(out.chars().all(|c| c == '世' || c == '界' || c == '…'));
        }
    }

    #[test]
    fn a_truncated_value_says_so() {
        let long = "a".repeat(100);
        assert_eq!(
            truncate_utf8(&long, 10, " [truncated]"),
            "aaaaaaaaaa [truncated]"
        );
    }

    /// A budget smaller than the first character yields only the marker.
    ///
    /// Returning the marker alone is right: an empty string would be indistinguishable
    /// from a value that genuinely was empty.
    #[test]
    fn a_budget_below_one_character_yields_the_marker_alone() {
        assert_eq!(truncate_utf8("世", 1, " [truncated]"), " [truncated]");
    }
}
