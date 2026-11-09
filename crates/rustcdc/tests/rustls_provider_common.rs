//! Installs a process-default rustls crypto provider for tests that need one.
//!
//! # Why a test needs this when the library does not
//!
//! `rustcdc` builds its own TLS clients through `ClientConfig::builder_with_provider`, and the
//! MySQL connector, whose driver reads only the process default, installs `ring` when nothing
//! is installed. `mysql_connection_integration` deliberately does not call this helper, so it
//! exercises that path.
//!
//! `sqlx` — which these suites use to *drive* the database, separately from the connector under
//! test — does rely on the default. rustls resolves that default automatically only when exactly
//! one provider feature is enabled, and under `--all-features` there are two: `ring`, from
//! rustcdc's own rustls, and `aws-lc-rs`, pulled in by `sqlserver` (tiberius) and the AWS SDK
//! behind `glue`. rustls cannot choose, so it panics:
//!
//! ```text
//! Could not automatically determine the process-level CryptoProvider from Rustls crate features.
//! ```
//!
//! CI runs each container-backed suite with a narrow feature set (`--features mysql,tls` and
//! friends), where only `ring` is present and the default resolves — which is why this went
//! unnoticed. But `README.md` documents `cargo test --all-features` as *the* development command,
//! and under it every sqlx-using suite aborted with a rustls error that has nothing to do with
//! whatever the developer was changing. Installing the provider explicitly makes the documented
//! command work and removes the feature-combination dependence entirely.
//!
//! `ring` is chosen to match what the library itself uses, so a test and the connector it
//! exercises agree on the crypto backend.

/// Install `ring` as the process-default rustls provider, once per test binary.
///
/// Idempotent and safe to call from every test in a binary: the first call wins and later ones
/// observe that a default is already installed. Call it before the first `sqlx` connection.
#[allow(dead_code)]
pub fn install_rustls_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // An error here means another provider was installed first, which is equally fine — the
        // point is only that *a* default exists before sqlx looks for one.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
