//! `rustcdc snapshot` — backfill tables on a running instance.
//!
//! The runtime capability is otherwise reachable only by hand-writing a `POST /signals`
//! body, and this is the only command that uses `--admin-write-token` — `status` is the
//! only other one that talks to the admin API, and it only reads.
//!
//! It deliberately does **not** wait for the snapshot to finish. The signal is
//! asynchronous by design — the pipeline answers `STARTED`, services the request between
//! polls, and reports the outcome on the notification stream — and a CLI that blocked
//! would have to invent a timeout for an operation whose duration is the size of the
//! table.

use crate::{cli::SnapshotArgs, error::AppError};

use super::status::{AdminAuthClientConfig, AdminTlsClientConfig, http_post_json};

pub async fn execute(args: SnapshotArgs) -> Result<(), AppError> {
    let tables: Vec<String> = args
        .tables
        .iter()
        .map(|table| table.trim().to_string())
        .filter(|table| !table.is_empty())
        .collect();

    // Checked here as well as server-side so a typo costs a shell round trip rather than
    // an `ABORTED` notification the operator has to go looking for.
    if let Some(unqualified) = tables.iter().find(|table| !table.contains('.')) {
        return Err(AppError::Other(format!(
            "table '{unqualified}' must be fully qualified as \"schema.table\" — an \
             unqualified name cannot be resolved against the source catalog"
        )));
    }
    if tables.is_empty() {
        return Err(AppError::Other(
            "at least one table is required; a snapshot of nothing is not a request".to_string(),
        ));
    }

    let url = format!("{}/signals", args.admin_url.trim_end_matches('/'));
    let tls = AdminTlsClientConfig::from(&args.admin_tls);
    let auth = AdminAuthClientConfig::from(&args.admin_auth);

    let mut body = serde_json::json!({
        "action_type": "execute_snapshot",
        "tables": tables,
    });
    if let Some(signal_id) = args
        .signal_id
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        body["signal_id"] = serde_json::Value::String(signal_id.to_string());
    }
    if let Some(message) = args
        .message
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        body["message"] = serde_json::Value::String(message.to_string());
    }

    let response = http_post_json(&url, &body, &tls, &auth).await?;
    let parsed: serde_json::Value = serde_json::from_str(&response)
        .map_err(|e| AppError::Other(format!("invalid JSON from the signals endpoint: {e}")))?;

    let signal_id = parsed
        .get("signal_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("<unknown>");

    println!(
        "{}",
        serde_json::to_string_pretty(&parsed).unwrap_or(response)
    );
    println!();
    println!(
        "Requested a snapshot of {} table(s). The pipeline services this between polls and\n\
         reports the outcome — including how many tables it enqueued — on the notification\n\
         stream, not here:\n\
         \n\
         \x20 curl -N -H \"Authorization: Bearer $RUSTCDC_READ_TOKEN\" \\\n\
         \x20   {}/notifications/stream\n\
         \n\
         Look for signal_id={signal_id}.",
        tables.len(),
        args.admin_url.trim_end_matches('/'),
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::SnapshotArgs;

    fn args(tables: &[&str]) -> SnapshotArgs {
        SnapshotArgs {
            tables: tables.iter().map(|t| t.to_string()).collect(),
            admin_url: "http://127.0.0.1:1".to_string(),
            signal_id: None,
            message: None,
            admin_tls: Default::default(),
            admin_auth: Default::default(),
        }
    }

    /// An unqualified name fails before any request is sent.
    ///
    /// The server resolves every name against the catalog and refuses the whole request,
    /// but it does so asynchronously — the operator would see `STARTED` and have to go
    /// find the `ABORTED` notification to learn they typed `orders` instead of
    /// `public.orders`.
    #[tokio::test]
    async fn an_unqualified_table_name_is_refused_before_the_request() {
        let error = execute(args(&["orders"]))
            .await
            .expect_err("an unqualified name must be refused");
        assert!(
            error.to_string().contains("schema.table"),
            "the error must say what the name should look like: {error}"
        );
    }

    #[tokio::test]
    async fn a_blank_table_list_is_refused() {
        let error = execute(args(&["   "]))
            .await
            .expect_err("whitespace is not a table name");
        assert!(error.to_string().contains("at least one table"));
    }
}
