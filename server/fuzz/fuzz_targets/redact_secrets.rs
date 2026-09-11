#![no_main]

//! Fuzz `redact_secrets`, the function standing between a config snapshot and any
//! read-scoped `/status` caller.
//!
//! Two invariants, in order of importance:
//!
//! 1. **Totality.** It must return for any input. A panic here is an admin-API crash
//!    reachable through configuration, and `/status` calls it on every request.
//! 2. **Structure preservation.** Valid JSON in must give valid JSON out, or a consumer of
//!    `/status` receives a body it cannot parse.
//!
//! The stronger secrecy property — no planted sentinel survives — is checked in
//! `tests/fuzz_properties.rs`, because it needs a generator that knows which keys are
//! meant to be secret, which arbitrary bytes cannot express.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };

    let redacted = rustcdc_server::redaction::redact_secrets(text);

    if serde_json::from_str::<serde_json::Value>(text).is_ok() {
        assert!(
            serde_json::from_str::<serde_json::Value>(&redacted).is_ok(),
            "redaction turned valid JSON into invalid JSON:\ninput:  {text}\noutput: {redacted}"
        );
    }
});
