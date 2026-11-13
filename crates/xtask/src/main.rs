//! Repository automation: `cargo xtask <task>`.
//!
//! One discoverable entry point for the gates: `cargo xtask` with no arguments lists them.
//! Most tasks dispatch to a shell script under `scripts/`, which is good at text munging
//! over the tree. A check whose correctness depends on a shell-tool dialect lives here
//! instead, compiled and unit-tested: BSD and GNU awk disagree, and a dialect error under
//! `|| true` reports a pass having checked nothing. [`cargo_profile`] is one.
//!
//! CI invokes the scripts directly, so its jobs do not pay for building this crate.

use std::process::{Command, ExitCode};

mod cargo_profile;
mod support_window;

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
        name: "support-window",
        about: "Is the tested database matrix still the one the vendors support? (network)",
        run: support_window::run,
    },
    Task {
        name: "console",
        about: "Build the operator console into crates/rustcdc-server/ui/dist (needs Node 24).",
        run: |_| npm(&["ci", "--no-audit", "--no-fund"]).and_then(|()| npm(&["run", "build"])),
    },
    Task {
        name: "console-dev",
        about: "Serve the console with live reload against `rustcdc run` (RUSTCDC_URL, default :8080).",
        run: |_| npm(&["run", "dev"]),
    },
    Task {
        name: "pipeline-bench",
        about: "Rows/s and latency, PostgreSQL → rustcdc → Kafka, both contracts (Docker). --smoke: both contracts end to end, no numbers.",
        run: pipeline_bench,
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
/// The hatch has exactly one spelling, here; a caller retyping it can get it wrong and
/// build a benchmark that does not compile anywhere else.
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

/// The pipeline bench, measuring the binary the image ships rather than the one cargo
/// builds for integration tests: that one carries `test-harnesses`, and the release
/// settings below are the Dockerfile's. Its own target directory keeps those settings from
/// invalidating the ordinary release cache.
fn pipeline_bench(args: &[String]) -> Result<(), String> {
    if args.iter().any(|arg| arg == "--smoke") {
        let rest: Vec<String> = args
            .iter()
            .filter(|arg| *arg != "--smoke")
            .cloned()
            .collect();
        return pipeline_smoke(&rest);
    }
    let root = workspace_root()?;
    let target = root.join("target").join("pipeline-bench");
    run(Command::new("cargo")
        .args([
            "build",
            "-p",
            "rustcdc-server",
            "--release",
            "--bin",
            "rustcdc",
            "--all-features",
            "--locked",
        ])
        .arg("--target-dir")
        .arg(&target)
        .env("CARGO_PROFILE_RELEASE_LTO", "thin")
        .env("CARGO_PROFILE_RELEASE_CODEGEN_UNITS", "1")
        .current_dir(&root))?;

    let rustflags = match std::env::var("RUSTFLAGS") {
        Ok(existing) if !existing.trim().is_empty() => format!("{existing} {BENCH_HATCH}"),
        _ => BENCH_HATCH.to_string(),
    };
    run(Command::new("cargo")
        .args([
            "test",
            "-p",
            "rustcdc-server",
            "--release",
            "--all-features",
            "--test",
            "pipeline_bench",
        ])
        .args(["--", "--ignored", "--nocapture"])
        .args(args)
        .env("RUSTFLAGS", rustflags)
        .env(
            "PIPELINE_BENCH_BINARY",
            target.join("release").join("rustcdc"),
        )
        .current_dir(&root))
}

/// The bench's harness as a correctness check: both contracts end to end, a small load, no long
/// transaction, a debug binary. What CI runs on every pull request; the card is not a measurement.
fn pipeline_smoke(args: &[String]) -> Result<(), String> {
    let root = workspace_root()?;
    run(Command::new("cargo")
        .args([
            "build",
            "-p",
            "rustcdc-server",
            "--bin",
            "rustcdc",
            "--all-features",
            "--locked",
        ])
        .current_dir(&root))?;
    run(Command::new("cargo")
        .args(["test", "-p", "rustcdc-server", "--all-features", "--locked"])
        .args(["--test", "pipeline_bench", "--", "--ignored", "--nocapture"])
        .args(args)
        .env(
            "PIPELINE_BENCH_BINARY",
            root.join("target").join("debug").join("rustcdc"),
        )
        .env("PIPELINE_BENCH_SMOKE", "1")
        .env("PIPELINE_BENCH_ROWS", "5000")
        .env("PIPELINE_BENCH_PER_TXN", "500")
        .env("PIPELINE_BENCH_BULK_ROWS", "0")
        .env("PIPELINE_BENCH_LONG_PASSES", "0")
        .current_dir(&root))
}

fn run(command: &mut Command) -> Result<(), String> {
    let status = command
        .status()
        .map_err(|error| format!("could not run cargo: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("cargo exited with {status}"))
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

/// Run `npm` in the console's directory. Node is needed only here; every other task and every
/// `cargo build` works without it.
fn npm(args: &[&str]) -> Result<(), String> {
    let dir = workspace_root()?.join("crates/rustcdc-server/ui");
    let status = Command::new("npm")
        .args(args)
        .current_dir(&dir)
        .status()
        .map_err(|error| format!("could not run npm (install Node 24): {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("npm {} failed with {status}", args.join(" ")))
    }
}
