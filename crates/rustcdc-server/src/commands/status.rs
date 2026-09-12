use std::path::PathBuf;

use crate::{cli::StatusArgs, error::AppError};

pub async fn execute(args: StatusArgs) -> Result<(), AppError> {
    let url = format!("{}/status", args.admin_url.trim_end_matches('/'));
    let tls = AdminTlsClientConfig::from(&args.admin_tls);
    let auth = AdminAuthClientConfig::from(&args.admin_auth);

    let resp = http_get(&url, &tls, &auth).await?;

    let body: serde_json::Value = serde_json::from_str(&resp)
        .map_err(|e| AppError::Other(format!("invalid JSON from status endpoint: {e}")))?;

    println!("{}", serde_json::to_string_pretty(&body).unwrap_or(resp));

    if args.require_running {
        let state = body
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        if state != "running" {
            return Err(AppError::Other(format!(
                "instance state is '{state}', expected 'running'\n\
                 remediation: inspect `/status` slo.reasons or request graceful shutdown via SIGTERM/CTRL-C to the runtime process"
            )));
        }
    }

    Ok(())
}

// ── Admin HTTP client ─────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub(super) enum AdminClientError {
    #[error("{0}")]
    Setup(#[from] AppError),

    #[error("could not reach admin API at {url}: {source}")]
    Transport {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    #[error("failed to read admin API response body from {url}: {source}")]
    BodyRead {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    #[error("admin API request to {url} failed with status {status}: {body}")]
    Status {
        url: String,
        status: reqwest::StatusCode,
        body: String,
    },
}

impl From<AdminClientError> for AppError {
    fn from(value: AdminClientError) -> Self {
        AppError::Http(value.to_string())
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct AdminTlsClientConfig {
    admin_ca_file: Option<PathBuf>,
    admin_client_cert_file: Option<PathBuf>,
    admin_client_key_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct AdminAuthClientConfig {
    admin_read_token: Option<String>,
    admin_read_token_env: Option<String>,
    admin_write_token: Option<String>,
    admin_write_token_env: Option<String>,
}

impl From<&crate::cli::AdminTlsClientArgs> for AdminTlsClientConfig {
    fn from(value: &crate::cli::AdminTlsClientArgs) -> Self {
        Self {
            admin_ca_file: value.admin_ca_file.clone(),
            admin_client_cert_file: value.admin_client_cert_file.clone(),
            admin_client_key_file: value.admin_client_key_file.clone(),
        }
    }
}

impl From<&crate::cli::AdminAuthClientArgs> for AdminAuthClientConfig {
    fn from(value: &crate::cli::AdminAuthClientArgs) -> Self {
        Self {
            admin_read_token: value.admin_read_token.clone(),
            admin_read_token_env: value.admin_read_token_env.clone(),
            admin_write_token: value.admin_write_token.clone(),
            admin_write_token_env: value.admin_write_token_env.clone(),
        }
    }
}

/// POST a JSON body to a write-scoped admin endpoint.
///
/// Separate from `http_get` only in method, body and which token it presents — the write
/// token, which until now the CLI accepted and never used.
pub(super) async fn http_post_json(
    url: &str,
    body: &serde_json::Value,
    tls: &AdminTlsClientConfig,
    auth: &AdminAuthClientConfig,
) -> Result<String, AdminClientError> {
    let client = admin_http_client(std::time::Duration::from_secs(30), tls)?;

    let mut request = client.post(url).json(body);
    if let Some(token) = resolve_token(
        auth.admin_write_token.as_deref(),
        auth.admin_write_token_env.as_deref(),
    )? {
        request = request.bearer_auth(token);
    }

    send_and_read(request, url).await
}

async fn http_get(
    url: &str,
    tls: &AdminTlsClientConfig,
    auth: &AdminAuthClientConfig,
) -> Result<String, AdminClientError> {
    let client = admin_http_client(std::time::Duration::from_secs(5), tls)?;

    let mut request = client.get(url);
    if let Some(token) = resolve_token(
        auth.admin_read_token.as_deref(),
        auth.admin_read_token_env.as_deref(),
    )? {
        request = request.bearer_auth(token);
    }

    send_and_read(request, url).await
}

async fn send_and_read(
    request: reqwest::RequestBuilder,
    url: &str,
) -> Result<String, AdminClientError> {
    let response = request
        .send()
        .await
        .map_err(|e| AdminClientError::Transport {
            url: url.to_string(),
            source: e,
        })?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| AdminClientError::BodyRead {
            url: url.to_string(),
            source: e,
        })?;

    if !status.is_success() {
        return Err(AdminClientError::Status {
            url: url.to_string(),
            status,
            body,
        });
    }

    Ok(body)
}

fn resolve_token(
    explicit_token: Option<&str>,
    token_env_name: Option<&str>,
) -> Result<Option<String>, AdminClientError> {
    if let Some(token) = explicit_token {
        let token = token.trim();
        if token.is_empty() {
            return Err(AdminClientError::Setup(AppError::Http(
                "explicit admin token must not be empty".to_string(),
            )));
        }
        return Ok(Some(token.to_string()));
    }

    let Some(env_name) = token_env_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };

    let token = std::env::var(env_name).map_err(|_| {
        AdminClientError::Setup(AppError::Http(format!(
            "admin token env var '{env_name}' is not set"
        )))
    })?;

    let token = token.trim().to_string();
    if token.is_empty() {
        return Err(AdminClientError::Setup(AppError::Http(format!(
            "admin token env var '{env_name}' is empty"
        ))));
    }

    Ok(Some(token))
}

fn admin_http_client(
    timeout: std::time::Duration,
    tls: &AdminTlsClientConfig,
) -> Result<reqwest::Client, AppError> {
    let mut builder = reqwest::Client::builder().timeout(timeout);

    if let Some(ca_path) = &tls.admin_ca_file {
        let ca_bytes = std::fs::read(ca_path).map_err(|e| {
            AppError::Http(format!(
                "failed to read admin CA bundle {}: {e}",
                ca_path.display()
            ))
        })?;
        let ca = reqwest::Certificate::from_pem(&ca_bytes).map_err(|e| {
            AppError::Http(format!(
                "failed to parse admin CA bundle {} as PEM: {e}",
                ca_path.display()
            ))
        })?;
        builder = builder.add_root_certificate(ca);
    }

    match (&tls.admin_client_cert_file, &tls.admin_client_key_file) {
        (Some(cert_path), Some(key_path)) => {
            let cert_pem = std::fs::read(cert_path).map_err(|e| {
                AppError::Http(format!(
                    "failed to read admin client certificate {}: {e}",
                    cert_path.display()
                ))
            })?;
            let key_pem = std::fs::read(key_path).map_err(|e| {
                AppError::Http(format!(
                    "failed to read admin client key {}: {e}",
                    key_path.display()
                ))
            })?;
            let mut identity_pem = Vec::with_capacity(cert_pem.len() + key_pem.len() + 1);
            identity_pem.extend_from_slice(&cert_pem);
            identity_pem.push(b'\n');
            identity_pem.extend_from_slice(&key_pem);

            let identity = reqwest::Identity::from_pem(&identity_pem).map_err(|e| {
                AppError::Http(format!(
                    "failed to parse admin mTLS identity from {} and {}: {e}",
                    cert_path.display(),
                    key_path.display()
                ))
            })?;
            builder = builder.identity(identity);
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(AppError::Http(
                "both --admin-client-cert-file and --admin-client-key-file must be set for mTLS"
                    .to_string(),
            ));
        }
        _ => {}
    }

    builder
        .build()
        .map_err(|e| AppError::Http(format!("failed to build admin API client: {e}")))
}

#[cfg(test)]
mod tests {
    use super::{AdminTlsClientConfig, admin_http_client};
    use crate::cli::AdminTlsClientArgs;

    #[test]
    fn admin_http_client_requires_cert_and_key_pair_for_mtls() {
        let tls = AdminTlsClientArgs {
            admin_client_cert_file: Some("client-cert.pem".into()),
            ..Default::default()
        };

        let tls = AdminTlsClientConfig::from(&tls);
        let err = admin_http_client(std::time::Duration::from_secs(1), &tls)
            .expect_err("missing client key should fail");
        assert!(
            format!("{err}").contains("both --admin-client-cert-file and --admin-client-key-file")
        );
    }
}
