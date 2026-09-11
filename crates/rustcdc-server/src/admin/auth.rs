//! Admin API authentication: token sources, scopes, and the manifest loader.
//!
//! Split out of `admin/mod.rs` so the code that decides whether a request is allowed can
//! be read — and reviewed — without the several thousand lines of handlers and metrics it
//! used to sit among. Everything that answers "is this caller permitted?" is in this file
//! and nowhere else.

use std::path::Path;
use std::time::{Duration, Instant, SystemTime};

use axum::http::{HeaderMap, StatusCode, header::AUTHORIZATION, header::RETRY_AFTER};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use ed25519_dalek::VerifyingKey;
use sha2::{Digest, Sha256};

use crate::config::schema::AppConfig;
use crate::error::AppError;
use crate::token_manifest_policy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AdminScope {
    Read,
    #[allow(dead_code)]
    Write,
}

#[derive(Debug, Clone)]
pub(super) struct AuthToken {
    pub(super) id: String,
    pub(super) token_sha256_hex: String,
    pub(super) not_before: Option<DateTime<Utc>>,
    pub(super) expires_at: Option<DateTime<Utc>>,
    pub(super) revoked: bool,
}

#[derive(Debug, Clone)]
pub(super) struct AuthState {
    pub(super) read_tokens: Vec<AuthToken>,
    pub(super) write_tokens: Vec<AuthToken>,
    pub(super) source: AuthSource,
    pub(super) manifest_version: u64,
    pub(super) manifest_reload_ok: bool,
    pub(super) manifest_stale_blocked: bool,
    pub(super) last_reload_at: Option<SystemTime>,
    pub(super) last_reload_error: Option<String>,
    pub(super) last_observed_mtime: Option<SystemTime>,
    pub(super) last_refresh_attempt: Option<Instant>,
    pub(super) revoked_token_hits_total: u64,
}

#[derive(Debug, Clone)]
pub(super) enum AuthSource {
    Env,
    Manifest {
        path: std::path::PathBuf,
        trusted_public_keys: Vec<VerifyingKey>,
        refresh_interval: Duration,
        max_staleness: Option<Duration>,
    },
}

pub(super) fn load_token(
    var_name: Option<&str>,
    field_name: &str,
) -> Result<Option<String>, AppError> {
    let Some(var_name) = var_name else {
        return Ok(None);
    };

    let token = std::env::var(var_name).map_err(|_| {
        AppError::Other(format!(
            "{field_name} points to missing environment variable '{var_name}'"
        ))
    })?;

    if token.trim().is_empty() {
        return Err(AppError::Other(format!(
            "{field_name} references '{var_name}' but it is empty"
        )));
    }

    Ok(Some(token))
}

pub(super) fn load_auth_state(config: &AppConfig) -> Result<AuthState, AppError> {
    if let Some(path) = config.admin.token_manifest_file.as_deref() {
        let trusted_public_keys = token_manifest_policy::parse_trusted_manifest_keys(
            &config.admin.token_manifest_trusted_public_keys_hex,
        )
        .map_err(AppError::Other)?;
        let (read_tokens, write_tokens) =
            load_auth_tokens_from_manifest(path, &trusted_public_keys)?;
        return Ok(AuthState {
            read_tokens,
            write_tokens,
            source: AuthSource::Manifest {
                path: path.to_path_buf(),
                trusted_public_keys,
                refresh_interval: Duration::from_millis(config.admin.token_manifest_refresh_ms),
                max_staleness: config
                    .admin
                    .token_manifest_max_staleness_ms
                    .map(Duration::from_millis),
            },
            manifest_version: 1,
            manifest_reload_ok: true,
            manifest_stale_blocked: false,
            last_reload_at: Some(SystemTime::now()),
            last_reload_error: None,
            last_observed_mtime: file_modified_time(path).ok().flatten(),
            last_refresh_attempt: None,
            revoked_token_hits_total: 0,
        });
    }

    let mut read_tokens = Vec::new();
    let mut write_tokens = Vec::new();

    if let Some(read_token) = load_token(
        config.admin.read_token_env.as_deref(),
        "admin.read_token_env",
    )? {
        read_tokens.push(AuthToken {
            id: "read-env".to_string(),
            token_sha256_hex: token_sha256_hex(&read_token),
            not_before: None,
            expires_at: None,
            revoked: false,
        });
    }

    if let Some(write_token) = load_token(
        config.admin.write_token_env.as_deref(),
        "admin.write_token_env",
    )? {
        write_tokens.push(AuthToken {
            id: "write-env".to_string(),
            token_sha256_hex: token_sha256_hex(&write_token),
            not_before: None,
            expires_at: None,
            revoked: false,
        });
    }

    Ok(AuthState {
        read_tokens,
        write_tokens,
        source: AuthSource::Env,
        manifest_version: 0,
        manifest_reload_ok: true,
        manifest_stale_blocked: false,
        last_reload_at: None,
        last_reload_error: None,
        last_observed_mtime: None,
        last_refresh_attempt: None,
        revoked_token_hits_total: 0,
    })
}

pub(super) fn load_auth_tokens_from_manifest(
    path: &Path,
    trusted_public_keys: &[VerifyingKey],
) -> Result<(Vec<AuthToken>, Vec<AuthToken>), AppError> {
    let manifest = token_manifest_policy::load_signed_token_manifest(path, trusted_public_keys)
        .map_err(AppError::Other)?;

    let mut read_tokens = Vec::new();
    let mut write_tokens = Vec::new();

    for token in manifest.tokens {
        let auth = AuthToken {
            id: token.id,
            token_sha256_hex: token.token_sha256_hex.to_ascii_lowercase(),
            not_before: token.not_before,
            expires_at: token.expires_at,
            revoked: token.revoked,
        };

        let mut has_scope = false;
        for scope in token.scopes {
            match scope.trim().to_ascii_lowercase().as_str() {
                "read" => {
                    has_scope = true;
                    read_tokens.push(auth.clone());
                }
                "write" => {
                    has_scope = true;
                    write_tokens.push(auth.clone());
                    // Write-scoped tokens can also access read endpoints.
                    read_tokens.push(auth.clone());
                }
                _ => {}
            }
        }

        if !has_scope {
            return Err(AppError::Other(format!(
                "admin token '{}' has no recognized scopes in manifest {}",
                auth.id,
                path.display()
            )));
        }
    }

    if read_tokens.is_empty() {
        return Err(AppError::Other(format!(
            "admin token manifest {} must contain at least one token with a recognized read or write scope",
            path.display()
        )));
    }

    if write_tokens.is_empty() {
        return Err(AppError::Other(format!(
            "admin token manifest {} must contain at least one token with write scope",
            path.display()
        )));
    }

    Ok((read_tokens, write_tokens))
}

pub(super) fn token_sha256_hex(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    hex::encode(digest)
}

pub(super) fn constant_time_eq_str(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut diff = 0u8;
    for (a, b) in left.as_bytes().iter().zip(right.as_bytes().iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

pub(super) fn file_modified_time(path: &Path) -> Result<Option<SystemTime>, AppError> {
    let meta = std::fs::metadata(path).map_err(|e| {
        AppError::Other(format!(
            "failed to stat admin token manifest {}: {e}",
            path.display()
        ))
    })?;
    Ok(meta.modified().ok())
}

impl AuthState {
    pub(super) fn source_name(&self) -> &'static str {
        match self.source {
            AuthSource::Env => "env",
            AuthSource::Manifest { .. } => "manifest",
        }
    }

    pub(super) fn maybe_refresh_manifest(&mut self) {
        let AuthSource::Manifest {
            ref path,
            ref trusted_public_keys,
            refresh_interval,
            max_staleness,
        } = self.source
        else {
            return;
        };

        let now_instant = Instant::now();
        if let Some(last) = self.last_refresh_attempt
            && now_instant.duration_since(last) < refresh_interval
        {
            self.enforce_manifest_staleness(max_staleness);
            return;
        }
        self.last_refresh_attempt = Some(now_instant);

        let current_mtime = file_modified_time(path).ok().flatten();
        let reload_needed =
            self.last_observed_mtime != current_mtime || self.last_reload_at.is_none();

        if reload_needed {
            match load_auth_tokens_from_manifest(path, trusted_public_keys) {
                Ok((read_tokens, write_tokens)) => {
                    self.read_tokens = read_tokens;
                    self.write_tokens = write_tokens;
                    self.manifest_version = self.manifest_version.saturating_add(1);
                    self.manifest_reload_ok = true;
                    self.last_reload_at = Some(SystemTime::now());
                    self.last_reload_error = None;
                    self.last_observed_mtime = current_mtime;
                }
                Err(e) => {
                    self.manifest_reload_ok = false;
                    self.last_reload_error = Some(e.to_string());
                }
            }
        }

        self.enforce_manifest_staleness(max_staleness);
    }

    pub(super) fn enforce_manifest_staleness_from_source(&mut self) {
        let AuthSource::Manifest { max_staleness, .. } = self.source else {
            return;
        };

        self.enforce_manifest_staleness(max_staleness);
    }

    pub(super) fn enforce_manifest_staleness(&mut self, max_staleness: Option<Duration>) {
        self.manifest_stale_blocked = false;
        let Some(max_staleness) = max_staleness else {
            return;
        };

        let Some(last_reload_at) = self.last_reload_at else {
            self.manifest_stale_blocked = true;
            return;
        };

        match SystemTime::now().duration_since(last_reload_at) {
            Ok(age) => {
                if age > max_staleness {
                    self.manifest_stale_blocked = true;
                }
            }
            Err(_) => {
                self.manifest_stale_blocked = true;
            }
        }
    }
}

pub(super) fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let auth = headers.get(AUTHORIZATION)?.to_str().ok()?;
    auth.strip_prefix("Bearer ")
}

pub(super) fn unauthorized_response() -> Response {
    tracing::warn!("unauthorized admin API request");
    (
        StatusCode::UNAUTHORIZED,
        [(axum::http::header::WWW_AUTHENTICATE, "Bearer")],
        "unauthorized",
    )
        .into_response()
}

pub(super) fn rate_limited_response(endpoint: &str) -> Response {
    tracing::warn!(endpoint, "admin API request rate limited");
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(RETRY_AFTER, "1")],
        "too many requests",
    )
        .into_response()
}
