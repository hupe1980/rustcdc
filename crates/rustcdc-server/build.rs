fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    // Cargo treats a watched path that does not exist as changed, and reruns this script, and
    // recompiles the crate, on every build. In a worktree `.git` is a file, so ask git.
    if let Some(head) =
        git(&["rev-parse", "--git-path", "HEAD"]).filter(|path| std::path::Path::new(path).exists())
    {
        println!("cargo:rerun-if-changed={head}");
    }

    // Git commit SHA (best-effort; absent in release tarballs)
    let sha = git(&["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_SHA={sha}");

    // Build profile (debug / release)
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=BUILD_PROFILE={profile}");

    // Target triple
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=BUILD_TARGET={target}");

    embed_console();
}

/// A git command's trimmed output; `None` outside a checkout (a release tarball).
fn git(args: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Embed the built console (`ui/dist`) when the `ui` feature is on.
///
/// Without a bundle, a build that ships (`RUSTCDC_REQUIRE_CONSOLE=1`: release, image) fails; any
/// other build warns and serves a page saying how to build it, so CI stays Node-free.
fn embed_console() {
    if std::env::var_os("CARGO_FEATURE_UI").is_none() {
        return;
    }
    let dist = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("ui/dist");
    // CI never builds the console, and a missing watched path reruns this script on every
    // build. `ui` always exists, and its scan still sees `dist` appear.
    let watched = if dist.exists() { "ui/dist" } else { "ui" };
    println!("cargo:rerun-if-changed={watched}");
    println!("cargo:rerun-if-env-changed=RUSTCDC_REQUIRE_CONSOLE");

    let mut files: Vec<String> = std::fs::read_dir(&dist)
        .map(|entries| {
            entries
                .flatten()
                .filter(|entry| entry.path().is_file())
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .filter(|name| !name.ends_with(".map"))
                .collect()
        })
        .unwrap_or_default();
    files.sort();

    if files.is_empty() {
        let how = "build it first (`cargo xtask console`), or build without the `ui` feature";
        if std::env::var_os("RUSTCDC_REQUIRE_CONSOLE").is_some_and(|value| value == "1") {
            panic!("the `ui` feature is on and ui/dist is empty: {how}");
        }
        println!("cargo:warning=the console is not built, so /ui explains how: {how}");
    }

    let mut src = String::from(
        "/// The built console, as (file name, bytes). Empty when it was not built.\n\
         pub static BUNDLE: &[(&str, &[u8])] = &[\n",
    );
    for file in &files {
        src.push_str(&format!(
            "    ({file:?}, include_bytes!({:?})),\n",
            dist.join(file).display().to_string()
        ));
    }
    src.push_str("];\n");
    let out =
        std::path::Path::new(&std::env::var("OUT_DIR").expect("OUT_DIR")).join("console_bundle.rs");
    std::fs::write(out, src).expect("write the console bundle");
}
