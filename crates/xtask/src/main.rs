//! Repository automation: `cargo xtask <task>`.
//!
//! # Why this exists
//!
//! The gates were five shell scripts under `scripts/`, discoverable only by reading CI.
//! A contributor had to know that `ci-policy-gate.sh` is the one that must pass before a
//! pull request, that `ci-benchmark-gate.sh` needs a baseline first, and that two of them
//! are orchestration nobody runs by hand. `cargo xtask` with no arguments answers that.
//!
//! The workflows still invoke the scripts directly. Routing them through here would add a
//! `cargo build` to jobs that call `pull-images` eight times, and buy nothing: this is a
//! discoverability and portability layer for people, not an indirection for CI.
//!
//! # Why it is a dispatcher rather than a rewrite
//!
//! The scripts are ~2 000 lines of text munging over the repository: `sed` ranges across
//! Rust source, `rg` with PCRE2, `jq` over Avro schemas, `diff` of generated sets. That
//! is what shell is genuinely good at, and rewriting it wholesale would trade a working
//! tool for a large diff and a regression risk.
//!
//! What shell is *not* good at is being portable, and that is not hypothetical here: the
//! Cargo-profile check used gawk's three-argument `match()`, which is a syntax error on
//! BSD awk. On macOS the program aborted, `|| true` swallowed it, and the check printed
//! "passed" having examined nothing — silently vacuous for every developer on a Mac,
//! including through a change that added the exact profile it was meant to reject.
//!
//! So checks whose correctness depends on shell-tool dialects move here, where they are
//! compiled and unit-tested. [`cargo_profile`] is the first, and it carries the test that
//! the shell version could not have. The rest stay in `scripts/` and are invoked from
//! here until there is a reason to move them.

use std::process::{Command, ExitCode};

mod cargo_profile;

/// A task, its one-line description, and how to run it.
struct Task {
    name: &'static str,
    about: &'static str,
    run: fn(&[String]) -> Result<(), String>,
}

const TASKS: &[Task] = &[
    Task {
        name: "policy-gate",
        about: "Every repository gate a pull request must pass. Run this before pushing.",
        run: |args| script("ci-policy-gate.sh", args),
    },
    Task {
        name: "profile-check",
        about: "Reject debug-assertions in any profile but dev and test.",
        run: |_| cargo_profile::check(std::path::Path::new(".")),
    },
    Task {
        name: "latency-gate",
        about: "Compare recorded latency evidence against the documented budgets.",
        run: |args| script("ci-latency-gate.sh", args),
    },
    Task {
        name: "benchmark-gate",
        about: "Compare benchmark results against the committed baseline.",
        run: |args| script("ci-benchmark-gate.sh", args),
    },
    Task {
        name: "bench",
        about: "Build (--no-run) or run the workspace benchmarks. Extra args go to cargo.",
        run: bench,
    },
    Task {
        name: "pull-images",
        about: "Pre-pull the database images the integration matrix uses.",
        run: |args| script("ci-pull-relational-images.sh", args),
    },
    Task {
        name: "evidence",
        about: "Run the full integration matrix and collect release evidence (slow).",
        run: |args| script("run_full_integration_matrix_evidence.sh", args),
    },
];

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(name) = args.first() else {
        usage();
        return ExitCode::SUCCESS;
    };

    if matches!(name.as_str(), "-h" | "--help" | "help") {
        usage();
        return ExitCode::SUCCESS;
    }

    let Some(task) = TASKS.iter().find(|task| task.name == name) else {
        eprintln!("unknown task: {name}\n");
        usage();
        return ExitCode::FAILURE;
    };

    match (task.run)(&args[1..]) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xtask {name}: {error}");
            ExitCode::FAILURE
        }
    }
}

fn usage() {
    println!("cargo xtask <task> [args...]\n");
    let width = TASKS.iter().map(|t| t.name.len()).max().unwrap_or(0);
    for task in TASKS {
        println!("  {:width$}  {}", task.name, task.about, width = width);
    }
    println!("\nEvery task runs from the repository root, whatever directory you invoke it from.");
}

/// The cfg that opens the `test-harnesses` release guard for benchmark builds.
///
/// `src/fault_injection/mod.rs` refuses to compile when `test-harnesses` is on and
/// `debug_assertions` is off, so that fault injection cannot reach a shipped binary.
/// `cargo bench` is a release-profile build, and `rustcdc-server` dev-depends on
/// `rustcdc` with `test-harnesses`; cargo unifies features across the packages it
/// builds, so the feature arrives whether or not the bench wants it. A bench harness is
/// never shipped, so the guard has nothing to protect here.
const BENCH_HATCH: &str = "--cfg rustcdc_optimised_test_harnesses";

/// Benchmarks, with the release guard opened and the package scoping cargo needs.
///
/// This exists so the hatch has exactly one spelling. It previously lived only in a
/// comment in the root `Cargo.toml`, every caller was expected to retype it, and the
/// release-evidence CI job did not — which is how an unbuildable benchmark reached main.
fn bench(args: &[String]) -> Result<(), String> {
    let root = workspace_root()?;

    // RUSTFLAGS is one string, not a list: setting it in CI (`-D warnings`) and setting
    // it here would be mutually exclusive, so append rather than replace.
    let rustflags = match std::env::var("RUSTFLAGS") {
        Ok(existing) if !existing.trim().is_empty() => format!("{existing} {BENCH_HATCH}"),
        _ => BENCH_HATCH.to_string(),
    };

    // `--workspace --benches` alone silently skips targets with `required-features`, so
    // a bench could rot unnoticed; `--all-features` is what makes this a real check.
    let default_args = ["--workspace", "--benches", "--all-features"];
    let mut command = Command::new("cargo");
    command
        .arg("bench")
        .current_dir(&root)
        .env("RUSTFLAGS", &rustflags);
    if args.is_empty() {
        command.args(default_args);
    } else {
        command.args(args);
    }

    let status = command
        .status()
        .map_err(|error| format!("could not run cargo bench: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("cargo bench exited with {status}"))
    }
}

/// The repository root, whatever directory `cargo xtask` was invoked from.
fn workspace_root() -> Result<std::path::PathBuf, String> {
    Ok(std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .ok_or("cannot locate the workspace root")?
        .to_path_buf())
}

/// Run one of the shell gates, from the repository root.
///
/// `cargo` sets `CARGO_MANIFEST_DIR` to this crate, so the root is two levels up. That is
/// what makes `cargo xtask` work from anywhere in the tree — the scripts all assume the
/// root, and a contributor invoking one from `crates/rustcdc/` would otherwise get a
/// confusing "no such file" from inside the script rather than from the caller.
fn script(name: &str, args: &[String]) -> Result<(), String> {
    let root = workspace_root()?;
    let path = root.join("scripts").join(name);
    if !path.exists() {
        return Err(format!("{} does not exist", path.display()));
    }

    let status = Command::new("bash")
        .arg(&path)
        .args(args)
        .current_dir(&root)
        .status()
        .map_err(|error| format!("could not run {}: {error}", path.display()))?;

    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "{} exited with {}",
            name,
            status
                .code()
                .map_or_else(|| "a signal".to_owned(), |c| c.to_string())
        ))
    }
}
