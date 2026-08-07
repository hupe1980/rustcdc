pub mod admin;
pub mod cli;
pub mod codec;
pub mod commands;
pub mod config;
pub mod dlq;
pub mod error;
/// Secret redaction for the `/status` config snapshot.
///
/// Public so the randomised property suite in `tests/fuzz_properties.rs` and the
/// `fuzz/` targets can reach it. The crate is `publish = false`, so this widens no
/// external API — and the alternative, a test-only re-export, hides the one function
/// whose correctness a reader most needs to be able to find.
pub mod redaction;

pub mod pipeline;
pub mod sink;
pub mod state;
pub mod telemetry;
pub mod token_manifest_policy;
