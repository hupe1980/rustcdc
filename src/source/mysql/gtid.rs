//! MySQL GTID sets, and why the incremental snapshot brackets chunk reads with them.
//!
//! # The problem a GTID set solves
//!
//! The DBLog watermark bracket asks one question of every live event: *could the chunk `SELECT`
//! have seen this?* A binlog file-and-position answers it **wrongly** on MySQL, and the reason
//! is in the commit pipeline rather than in this crate.
//!
//! With `binlog_order_commits = ON` (the default) a transaction is written to the binlog in the
//! **flush** stage and engine-committed afterwards. `SHOW MASTER STATUS`'s `File`/`Position`
//! advance at the flush, so a transaction can sit *below* a watermark taken from them and still
//! be invisible to a `SELECT` that starts next — the chunk then holds the row's pre-image, the
//! position test does not suppress it, and the stale value is emitted over the newer one.
//!
//! `Executed_Gtid_Set` is different: it is updated **after** the engine commit. A GTID present
//! in it therefore belongs to a transaction whose rows are already visible, which is the safe
//! direction and what makes the bracket sound:
//!
//! - low watermark = `Executed_Gtid_Set` read before the chunk read;
//! - high watermark = `Executed_Gtid_Set` read after it;
//! - an event is **inside** the bracket when its GTID is in `high` and not in `low`;
//! - an event whose GTID is **not in `high`** committed after the chunk read finished, so the
//!   chunk is emitted before it.
//!
//! Both bounds come from the set. Mixing them — a set-based lower bound with an ordinal upper
//! bound — is unsound in a way that is easy to reach: an event inside the ordinal high bound but
//! absent from `high`'s set committed *after* that read, and suppressing it would discard the
//! newer value.
//!
//! This is the mechanism Debezium's read-only incremental snapshot uses, and it requires
//! `gtid_mode = ON`. Without it the connector falls back to the ordinal test and its documented
//! residual window.
//!
//! # Wire format
//!
//! ```text
//! 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:11-13,
//! 8E11FA47-71CA-11E1-9E33-C80AA9429562:1-27
//! ```
//!
//! Comma-separated per source uuid, each with one or more colon-separated intervals that are a
//! single number or an inclusive `start-end` range. The server may fold the value across lines,
//! so whitespace is insignificant.

use std::collections::BTreeMap;

/// A parsed MySQL GTID set: per-source-uuid inclusive transaction-number intervals.
///
/// Intervals are kept sorted and coalesced, so [`GtidSet::contains`] is a binary search and two
/// sets built from equal contents compare equal however the server grouped them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct GtidSet {
    /// Uuid (lowercased) → sorted, non-overlapping, non-adjacent `(start, end)` intervals.
    sources: BTreeMap<String, Vec<(u64, u64)>>,
}

impl GtidSet {
    /// Parse `Executed_Gtid_Set` as the server renders it.
    ///
    /// An empty or whitespace-only value is the empty set — which is what a server with
    /// `gtid_mode = OFF` reports, and is how the connector detects that it must fall back to the
    /// ordinal bracket.
    ///
    /// # Errors
    ///
    /// Returns the offending fragment when a uuid has no intervals, an interval bound is not a
    /// number, or a range runs backwards. Parsing is strict on purpose: a silently-dropped
    /// interval shrinks a watermark, and a shrunken **low** watermark suppresses chunk rows it
    /// should not while a shrunken **high** watermark fails to suppress rows it must.
    pub(super) fn parse(raw: &str) -> Result<Self, String> {
        let mut sources: BTreeMap<String, Vec<(u64, u64)>> = BTreeMap::new();

        for entry in raw.split(',') {
            // The server folds long sets across lines; whitespace carries no meaning.
            let entry: String = entry.chars().filter(|c| !c.is_whitespace()).collect();
            if entry.is_empty() {
                continue;
            }

            let mut parts = entry.split(':');
            let uuid = parts
                .next()
                .filter(|uuid| !uuid.is_empty())
                .ok_or_else(|| format!("GTID set entry '{entry}' has no source uuid"))?
                .to_ascii_lowercase();

            let mut intervals = Vec::new();
            for interval in parts {
                intervals.push(parse_interval(interval, &entry)?);
            }
            if intervals.is_empty() {
                return Err(format!(
                    "GTID set entry '{entry}' names a source uuid with no transaction intervals"
                ));
            }

            sources.entry(uuid).or_default().extend(intervals);
        }

        for intervals in sources.values_mut() {
            coalesce(intervals);
        }

        Ok(Self { sources })
    }

    /// Whether this set contains the single GTID `uuid:seqno`.
    ///
    /// Returns `false` for anything that is not a single GTID, including the empty string. A
    /// caller must therefore establish that the event *has* a GTID before reading `false` as
    /// "outside the bracket" — see [`super::incremental_snapshot`], which falls back to the
    /// ordinal test for an event with no GTID rather than treating it as outside.
    pub(super) fn contains_gtid(&self, gtid: &str) -> bool {
        let Some((uuid, seqno)) = split_single_gtid(gtid) else {
            return false;
        };
        self.contains(&uuid, seqno)
    }

    /// Whether this set contains transaction `seqno` from `uuid`.
    pub(super) fn contains(&self, uuid: &str, seqno: u64) -> bool {
        let Some(intervals) = self.sources.get(&uuid.to_ascii_lowercase()) else {
            return false;
        };
        // Intervals are sorted and disjoint, so the candidate is the last one starting at or
        // below `seqno`.
        match intervals.binary_search_by(|(start, _)| start.cmp(&seqno)) {
            Ok(_) => true,
            Err(0) => false,
            Err(index) => {
                let (_, end) = intervals[index - 1];
                seqno <= end
            }
        }
    }

    /// `true` when the set names no transactions, which is what `gtid_mode = OFF` reports.
    pub(super) fn is_empty(&self) -> bool {
        self.sources.values().all(|intervals| intervals.is_empty())
    }
}

/// Split `uuid:seqno`, or `None` if it is not a single GTID.
///
/// Deliberately rejects a range (`uuid:1-5`) and a multi-interval GTID: an event carries one
/// transaction, and accepting a range would answer the membership question about whichever end
/// happened to parse.
fn split_single_gtid(gtid: &str) -> Option<(String, u64)> {
    let (uuid, seqno) = gtid.trim().rsplit_once(':')?;
    if uuid.is_empty() {
        return None;
    }
    Some((uuid.to_ascii_lowercase(), seqno.parse().ok()?))
}

fn parse_interval(raw: &str, entry: &str) -> Result<(u64, u64), String> {
    let parse = |value: &str| -> Result<u64, String> {
        value.parse::<u64>().map_err(|error| {
            format!("GTID set entry '{entry}' has a non-numeric interval bound '{value}': {error}")
        })
    };

    match raw.split_once('-') {
        Some((start, end)) => {
            let (start, end) = (parse(start)?, parse(end)?);
            if end < start {
                return Err(format!(
                    "GTID set entry '{entry}' has a backwards interval {start}-{end}"
                ));
            }
            Ok((start, end))
        }
        None => {
            let single = parse(raw)?;
            Ok((single, single))
        }
    }
}

/// Sort and merge overlapping or adjacent intervals in place.
///
/// Adjacency matters as well as overlap: `1-5` and `6-9` name the same transactions as `1-9`, and
/// leaving them separate would make two equal sets compare unequal depending on how the server
/// grouped them.
fn coalesce(intervals: &mut Vec<(u64, u64)>) {
    intervals.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(intervals.len());
    for (start, end) in intervals.drain(..) {
        match merged.last_mut() {
            Some((_, previous_end)) if start <= previous_end.saturating_add(1) => {
                *previous_end = (*previous_end).max(end);
            }
            _ => merged.push((start, end)),
        }
    }
    *intervals = merged;
}

#[cfg(test)]
mod tests {
    use super::GtidSet;

    fn set(raw: &str) -> GtidSet {
        GtidSet::parse(raw).expect("parses")
    }

    const A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const B: &str = "8e11fa47-71ca-11e1-9e33-c80aa9429562";

    #[test]
    fn an_empty_value_is_the_empty_set() {
        for raw in ["", "   ", "\n", ","] {
            let parsed = set(raw);
            assert!(parsed.is_empty(), "{raw:?} must parse as the empty set");
            assert!(!parsed.contains(A, 1));
        }
    }

    #[test]
    fn a_single_transaction_is_contained_and_its_neighbours_are_not() {
        let parsed = set(&format!("{A}:5"));
        assert!(parsed.contains(A, 5));
        assert!(!parsed.contains(A, 4));
        assert!(!parsed.contains(A, 6));
        assert!(!parsed.contains(B, 5), "another source must not match");
    }

    #[test]
    fn ranges_are_inclusive_at_both_ends() {
        let parsed = set(&format!("{A}:10-20"));
        assert!(!parsed.contains(A, 9));
        assert!(parsed.contains(A, 10));
        assert!(parsed.contains(A, 15));
        assert!(parsed.contains(A, 20));
        assert!(!parsed.contains(A, 21));
    }

    #[test]
    fn multiple_sources_and_intervals_parse_and_fold_across_lines() {
        // The server renders long sets with embedded newlines.
        let parsed = set(&format!("{A}:1-5:11-13,\n{B}:1-27"));
        assert!(parsed.contains(A, 3));
        assert!(
            !parsed.contains(A, 8),
            "the gap between intervals is not covered"
        );
        assert!(parsed.contains(A, 12));
        assert!(parsed.contains(B, 27));
        assert!(!parsed.contains(B, 28));
    }

    /// Two sets naming the same transactions must compare equal however the server grouped them,
    /// or a watermark comparison would depend on rendering rather than content.
    #[test]
    fn adjacent_and_overlapping_intervals_coalesce() {
        assert_eq!(set(&format!("{A}:1-5:6-9")), set(&format!("{A}:1-9")));
        assert_eq!(set(&format!("{A}:1-5:3-9")), set(&format!("{A}:1-9")));
        assert_eq!(set(&format!("{A}:7:1-5:6")), set(&format!("{A}:1-7")));
        // Grouped as two entries for one uuid rather than two intervals.
        assert_eq!(set(&format!("{A}:1-5,{A}:6-9")), set(&format!("{A}:1-9")));
    }

    #[test]
    fn uuid_case_does_not_matter() {
        let parsed = set(&format!("{}:1-5", A.to_ascii_uppercase()));
        assert!(parsed.contains(A, 3));
        assert!(parsed.contains(&A.to_ascii_uppercase(), 3));
    }

    #[test]
    fn contains_gtid_takes_a_single_event_gtid() {
        let parsed = set(&format!("{A}:1-5"));
        assert!(parsed.contains_gtid(&format!("{A}:3")));
        assert!(!parsed.contains_gtid(&format!("{A}:6")));
        assert!(!parsed.contains_gtid(&format!("{B}:3")));
    }

    /// An event carries one transaction. Accepting a range would answer the membership question
    /// about whichever end happened to parse.
    #[test]
    fn contains_gtid_rejects_anything_that_is_not_a_single_gtid() {
        let parsed = set(&format!("{A}:1-5"));
        for malformed in ["", "not-a-gtid", &format!("{A}:1-5"), &format!("{A}:"), ":3"] {
            assert!(
                !parsed.contains_gtid(malformed),
                "{malformed:?} is not a single GTID"
            );
        }
    }

    /// Strict parsing: a dropped interval shrinks a watermark, and a shrunken low watermark
    /// suppresses chunk rows it should not while a shrunken high watermark fails to suppress rows
    /// it must.
    #[test]
    fn a_malformed_set_is_refused_rather_than_partially_parsed() {
        for malformed in [
            A,                            // uuid with no intervals
            &format!("{A}:abc"),          // non-numeric bound
            &format!("{A}:9-1"),          // backwards range
            &format!("{A}:1-"),           // missing upper bound
            ":5",                         // no uuid
        ] {
            let error =
                GtidSet::parse(malformed).expect_err(&format!("{malformed:?} must be refused"));
            assert!(!error.is_empty(), "the error must name the fragment");
        }
    }

    /// The property the bracket rests on: inside = in `high` and not in `low`, with **both**
    /// bounds from the set.
    #[test]
    fn the_bracket_is_a_set_difference_on_both_bounds() {
        let low = set(&format!("{A}:1-10"));
        let high = set(&format!("{A}:1-15"));

        let classify = |seqno: u64| {
            let gtid = format!("{A}:{seqno}");
            (low.contains_gtid(&gtid), high.contains_gtid(&gtid))
        };

        assert_eq!(
            classify(5),
            (true, true),
            "committed before the chunk read: the chunk saw it, do not suppress"
        );
        assert_eq!(
            classify(11),
            (false, true),
            "committed during the chunk read: inside the bracket, suppress the chunk row"
        );
        assert_eq!(classify(15), (false, true), "the high bound is inclusive");
        assert_eq!(
            classify(16),
            (false, false),
            "past the high watermark: the chunk is emitted first, so suppressing would discard \
             the newer value"
        );
    }

    /// A transaction from a source the low watermark has never seen is inside the bracket as soon
    /// as the high watermark covers it — a replica promoted mid-snapshot produces this.
    #[test]
    fn a_new_source_uuid_is_handled_by_set_membership() {
        let low = set(&format!("{A}:1-10"));
        let high = set(&format!("{A}:1-10,{B}:1-3"));
        let gtid = format!("{B}:2");
        assert!(high.contains_gtid(&gtid) && !low.contains_gtid(&gtid));
    }
}
