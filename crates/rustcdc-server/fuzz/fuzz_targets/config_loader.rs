#![no_main]

//! Fuzz the configuration loader.
//!
//! The invariant is that it **rejects rather than panics**. Rejecting any input is fine —
//! that is its job. Panicking is not: a panic in the loader is a crash loop that
//! `validate-config` cannot pre-empt, because running the validation is what crashes.
//!
//! The loader reads from a path rather than a string, so each input is written to a
//! temporary file. That is slower than an in-memory target would be, and it is the point:
//! it exercises the real entry point, including the layered figment sources and the
//! migration pass, rather than a parser called directly underneath them.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };

    let Ok(dir) = tempfile::tempdir() else {
        return;
    };
    let path = dir.path().join("cdc.toml");
    if std::fs::write(&path, text).is_err() {
        return;
    }

    // Accept or reject; both are correct outcomes. Only a panic fails.
    let _ = rustcdc_server::config::load(&path);
});
