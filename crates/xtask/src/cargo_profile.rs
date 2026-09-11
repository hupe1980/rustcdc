//! Reject `debug-assertions = true` in any Cargo profile but `dev` and `test`.
//!
//! # Why this one is in Rust
//!
//! It was in shell, and it did not work. The profile name was extracted with gawk's
//! three-argument `match($0, /…/, arr)`, which is a GNU extension: on BSD awk — that is,
//! on macOS — the whole program is a syntax error, awk exits immediately, `|| true`
//! swallows the failure, and the check reports success having read nothing.
//!
//! It was therefore silently vacuous for every developer not on Linux, and it stayed that
//! way through a change that added the exact `[profile.bench] debug-assertions = true`
//! it exists to reject. The bug was invisible precisely because the check was *about*
//! something rarely edited: nobody noticed a gate that never fired.
//!
//! A check whose correctness depends on which `awk` is installed cannot be trusted by the
//! people it is meant to protect. This version is compiled, runs identically everywhere,
//! and — unlike the shell — has tests, including one that plants the violation.
//!
//! # What it enforces
//!
//! `debug-assertions` changes what the compiled code *does*: overflow checks, `debug_assert!`
//! and the library's own `test-harnesses` guard all key off it. Leaving it on outside `dev`
//! and `test` means a benchmark measures different code than it claims to, and a release
//! build carries checks it was not supposed to.

use std::path::Path;

/// Manifests to scan, relative to the workspace root.
const MANIFESTS: &[&str] = &[
    "Cargo.toml",
    "crates/rustcdc/Cargo.toml",
    "crates/rustcdc-server/Cargo.toml",
    "crates/crash-workers/Cargo.toml",
    "crates/xtask/Cargo.toml",
];

/// Profiles allowed to enable debug assertions.
const ALLOWED: &[&str] = &["dev", "test"];

pub fn check(root: &Path) -> Result<(), String> {
    let mut offences = Vec::new();

    for manifest in MANIFESTS {
        let path = root.join(manifest);
        let Ok(text) = std::fs::read_to_string(&path) else {
            // A member may legitimately not exist in a partial checkout; a missing
            // manifest is not a policy violation.
            continue;
        };
        for (profile, line) in offending_profiles(&text) {
            offences.push(format!("{manifest}: [profile.{profile}] {line}"));
        }
    }

    if offences.is_empty() {
        println!(
            "Cargo profile safety check passed (no debug-assertions = true outside dev/test)."
        );
        return Ok(());
    }

    Err(format!(
        "debug-assertions = true found outside [profile.dev] / [profile.test]:\n  {}\n\n\
         These profiles must set it to false. `debug-assertions` changes the generated code, \
         so a benchmark or release build with it on is not measuring or shipping what it claims.",
        offences.join("\n  ")
    ))
}

/// Every `(profile, line)` pair that enables debug assertions outside the allowlist.
///
/// Tracks the current `[profile.<name>]` section. `[profile.foo.bar]` — an inherited or
/// per-package override — is attributed to `foo`, because that is the profile whose
/// behaviour changes.
fn offending_profiles(manifest: &str) -> Vec<(String, String)> {
    let mut found = Vec::new();
    let mut current: Option<String> = None;

    for raw in manifest.lines() {
        let line = raw.trim();
        if let Some(rest) = line.strip_prefix('[') {
            current = rest
                .strip_prefix("profile.")
                .map(|rest| rest.split(['.', ']']).next().unwrap_or_default().to_owned());
            continue;
        }
        if !enables_debug_assertions(line) {
            continue;
        }
        if let Some(profile) = &current
            && !ALLOWED.contains(&profile.as_str())
        {
            found.push((profile.clone(), line.to_owned()));
        }
    }

    found
}

/// `debug-assertions = true`, tolerating any spacing and an inline comment.
fn enables_debug_assertions(line: &str) -> bool {
    let Some((key, value)) = line.split_once('=') else {
        return false;
    };
    if key.trim() != "debug-assertions" {
        return false;
    }
    // `true  # because …` is still `true`.
    value
        .split('#')
        .next()
        .unwrap_or_default()
        .trim()
        .eq_ignore_ascii_case("true")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The case the shell version could not catch on a Mac.
    #[test]
    fn a_bench_profile_enabling_debug_assertions_is_an_offence() {
        let manifest = "[workspace]\n\n[profile.bench]\ndebug-assertions = true\n";
        let found = offending_profiles(manifest);
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].0, "bench");
    }

    #[test]
    fn dev_and_test_may_enable_them() {
        let manifest = "[profile.dev]\ndebug-assertions = true\n\n\
                        [profile.test]\ndebug-assertions = true\n";
        assert!(offending_profiles(manifest).is_empty());
    }

    /// `[profile.release.package.foo]` is still the release profile's behaviour.
    #[test]
    fn an_inherited_profile_is_attributed_to_its_base() {
        let manifest = "[profile.release.package.some-dep]\ndebug-assertions = true\n";
        let found = offending_profiles(manifest);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].0, "release");
    }

    /// The value is what matters, not the spacing or a trailing comment.
    #[test]
    fn spacing_and_comments_do_not_hide_the_setting() {
        for line in [
            "debug-assertions=true",
            "debug-assertions   =    true",
            "debug-assertions = true # needed by the harness",
        ] {
            let manifest = format!("[profile.bench]\n{line}\n");
            assert_eq!(offending_profiles(&manifest).len(), 1, "missed: {line}");
        }
    }

    #[test]
    fn false_and_unrelated_keys_are_not_offences() {
        let manifest = "[profile.bench]\ndebug-assertions = false\ndebug = true\nlto = \"thin\"\n";
        assert!(offending_profiles(manifest).is_empty());
    }

    /// A key outside any profile table is not a profile setting.
    #[test]
    fn a_setting_before_any_profile_table_is_ignored() {
        assert!(offending_profiles("debug-assertions = true\n").is_empty());
    }

    /// The real repository must pass its own gate.
    #[test]
    fn the_workspace_itself_is_clean() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("workspace root")
            .to_path_buf();
        check(&root).expect("the repository must satisfy its own profile policy");
    }
}
