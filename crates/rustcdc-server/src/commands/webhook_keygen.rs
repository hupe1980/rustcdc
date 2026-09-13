//! `rustcdc webhook-keygen` — mint a Standard Webhooks signing key.

use crate::{cli::WebhookKeygenArgs, error::AppError, webhook};

pub async fn execute(args: WebhookKeygenArgs) -> Result<(), AppError> {
    let scheme: webhook::WebhookSignatureScheme = args.scheme.into();
    let keys = webhook::generate_keys(scheme).map_err(AppError::Other)?;

    // stdout, not a log line: this is the command's output, and an operator pipes it.
    // The signing key goes to stdout too — it has to, that is what was asked for — but
    // the surrounding text says plainly that it does not belong in the config file.
    println!("# Standard Webhooks signing key ({scheme:?})");
    println!("#");
    println!("# Put the signing key in the environment, never in cdc.toml — a literal");
    println!("# `key` is rejected at load, because a key in the config file is a key in");
    println!("# every backup and every `/status` snapshot of it.");
    println!("#");
    println!(
        "#   export RUSTCDC_WEBHOOK_SIGNING_KEY='{}'",
        keys.signing_key
    );
    println!("#");
    println!("# [sink.signing]");
    println!(
        "# scheme = \"{}\"",
        match scheme {
            webhook::WebhookSignatureScheme::Ed25519 => "ed25519",
            webhook::WebhookSignatureScheme::HmacSha256 => "hmac_sha256",
        }
    );
    println!("# key    = {{ env = \"RUSTCDC_WEBHOOK_SIGNING_KEY\" }}");

    match &keys.public_key {
        Some(public) => {
            println!();
            println!("# Give this to the receiver. It is not a secret — publishing it is");
            println!("# the point, and it is the half that cannot forge anything.");
            println!("{public}");
        }
        None => {
            println!();
            println!("# hmac_sha256 has no public half: the receiver verifies with the same");
            println!("# secret it would forge with. Prefer `--scheme ed25519` whenever the");
            println!("# receiver is not you.");
        }
    }

    Ok(())
}
