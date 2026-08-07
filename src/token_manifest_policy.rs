use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TokenManifestFile {
    pub tokens: Vec<TokenManifestToken>,
    pub signature: TokenManifestSignature,
}

#[derive(Debug, Serialize)]
pub struct TokenManifestUnsigned {
    pub tokens: Vec<TokenManifestToken>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TokenManifestToken {
    pub id: String,
    pub token_sha256_hex: String,
    pub scopes: Vec<String>,
    #[serde(default)]
    pub not_before: Option<DateTime<Utc>>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub revoked: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TokenManifestSignature {
    pub algorithm: String,
    pub public_key_hex: String,
    pub signature_hex: String,
}

pub fn parse_trusted_manifest_keys(keys_hex: &[String]) -> Result<Vec<VerifyingKey>, String> {
    if keys_hex.is_empty() {
        return Err(
            "admin.token_manifest_trusted_public_keys_hex must include at least one key when admin.token_manifest_file is configured"
                .to_string(),
        );
    }

    let mut keys = Vec::with_capacity(keys_hex.len());
    for key_hex in keys_hex {
        let trimmed = key_hex.trim();
        let key_bytes = hex::decode(trimmed).map_err(|e| {
            format!(
                "admin.token_manifest_trusted_public_keys_hex contains invalid hex key '{trimmed}': {e}"
            )
        })?;

        let key_bytes: [u8; 32] = key_bytes.try_into().map_err(|_| {
            format!("admin.token_manifest_trusted_public_keys_hex key '{trimmed}' must be 32 bytes")
        })?;

        let key = VerifyingKey::from_bytes(&key_bytes).map_err(|e| {
            format!("admin.token_manifest_trusted_public_keys_hex key '{trimmed}' is invalid: {e}")
        })?;
        keys.push(key);
    }

    Ok(keys)
}

pub fn load_signed_token_manifest(
    path: &Path,
    trusted_keys: &[VerifyingKey],
) -> Result<TokenManifestFile, String> {
    let bytes_first = std::fs::read(path).map_err(|e| {
        format!(
            "failed to read admin token manifest {}: {e}",
            path.display()
        )
    })?;

    match parse_and_verify_signed_manifest(path, &bytes_first, trusted_keys) {
        Ok(manifest) => Ok(manifest),
        Err(first_error) => {
            // Retry once to tolerate transient mid-write/read races during manifest rotation.
            let bytes_second = std::fs::read(path).map_err(|e| {
                format!(
                    "failed to read admin token manifest {} on retry after initial verification error '{}': {e}",
                    path.display(),
                    first_error
                )
            })?;

            parse_and_verify_signed_manifest(path, &bytes_second, trusted_keys).map_err(
                |second_error| {
                    format!(
                        "admin token manifest {} failed verification after retry; first error: {}; second error: {}",
                        path.display(),
                        first_error,
                        second_error
                    )
                },
            )
        }
    }
}

pub fn validate_token_manifest_file(
    path: &Path,
    trusted_keys: &[VerifyingKey],
) -> Result<(), String> {
    let _ = load_signed_token_manifest(path, trusted_keys)?;
    Ok(())
}

pub fn has_write_scope(tokens: &[TokenManifestToken]) -> bool {
    tokens.iter().any(|token| {
        token
            .scopes
            .iter()
            .any(|scope| scope.trim().eq_ignore_ascii_case("write"))
    })
}

fn verify_manifest_signature(
    path: &Path,
    manifest: &TokenManifestFile,
    trusted_keys: &[VerifyingKey],
) -> Result<(), String> {
    if !manifest
        .signature
        .algorithm
        .trim()
        .eq_ignore_ascii_case("ed25519")
    {
        return Err(format!(
            "admin token manifest {} uses unsupported signature algorithm '{}' (expected ed25519)",
            path.display(),
            manifest.signature.algorithm
        ));
    }

    let signer_key_bytes = hex::decode(manifest.signature.public_key_hex.trim()).map_err(|e| {
        format!(
            "admin token manifest {} has invalid signature.public_key_hex: {e}",
            path.display()
        )
    })?;
    let signer_key_bytes: [u8; 32] = signer_key_bytes.try_into().map_err(|_| {
        format!(
            "admin token manifest {} signature.public_key_hex must be 32 bytes",
            path.display()
        )
    })?;
    let signer_key = VerifyingKey::from_bytes(&signer_key_bytes).map_err(|e| {
        format!(
            "admin token manifest {} signature.public_key_hex is invalid: {e}",
            path.display()
        )
    })?;

    if !trusted_keys.iter().any(|key| key == &signer_key) {
        return Err(format!(
            "admin token manifest {} is signed by an untrusted key",
            path.display()
        ));
    }

    let signature_bytes = hex::decode(manifest.signature.signature_hex.trim()).map_err(|e| {
        format!(
            "admin token manifest {} has invalid signature.signature_hex: {e}",
            path.display()
        )
    })?;
    let signature = Signature::from_slice(&signature_bytes).map_err(|e| {
        format!(
            "admin token manifest {} signature bytes are invalid: {e}",
            path.display()
        )
    })?;

    let unsigned = TokenManifestUnsigned {
        tokens: manifest.tokens.clone(),
    };
    let unsigned_bytes = canonical_signing_payload(&unsigned).map_err(|e| {
        format!(
            "failed to serialize canonical signing payload for {}: {e}",
            path.display()
        )
    })?;

    signer_key
        .verify(&unsigned_bytes, &signature)
        .map_err(|e| {
            format!(
                "admin token manifest {} signature verification failed: {e}",
                path.display()
            )
        })?;

    Ok(())
}

/// Produce a deterministic, canonical JSON byte payload for signing/verification.
/// The exact bytes a manifest signature covers.
///
/// Public because signing a manifest is an operator task performed by external tooling:
/// without this, anyone building a signer has to reverse-engineer the canonical form,
/// and a mismatch produces a manifest the server silently refuses.
pub fn canonical_signing_payload(
    unsigned: &TokenManifestUnsigned,
) -> Result<Vec<u8>, serde_json::Error> {
    // Represent each token as a BTreeMap so field order is alphabetical.
    let tokens: Vec<Value> = unsigned
        .tokens
        .iter()
        .map(|t| {
            let mut m: BTreeMap<&str, Value> = BTreeMap::new();
            m.insert("id", Value::String(t.id.clone()));
            if let Some(nb) = t.not_before {
                m.insert("not_before", Value::String(nb.to_rfc3339()));
            }
            if let Some(ea) = t.expires_at {
                m.insert("expires_at", Value::String(ea.to_rfc3339()));
            }
            m.insert("revoked", Value::Bool(t.revoked));
            m.insert(
                "scopes",
                Value::Array(t.scopes.iter().map(|s| Value::String(s.clone())).collect()),
            );
            m.insert(
                "token_sha256_hex",
                Value::String(t.token_sha256_hex.clone()),
            );

            // BTreeMap<&str, Value> serializes keys in alphabetical order.
            let obj: Map<String, Value> = m.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
            Value::Object(obj)
        })
        .collect();

    let mut root: BTreeMap<&str, Value> = BTreeMap::new();
    root.insert("tokens", Value::Array(tokens));

    serde_json::to_vec(&root)
}

fn parse_and_verify_signed_manifest(
    path: &Path,
    bytes: &[u8],
    trusted_keys: &[VerifyingKey],
) -> Result<TokenManifestFile, String> {
    let manifest: TokenManifestFile = serde_json::from_slice(bytes).map_err(|e| {
        format!(
            "failed to parse admin token manifest {}: {e}",
            path.display()
        )
    })?;

    verify_manifest_signature(path, &manifest, trusted_keys)?;
    validate_manifest_tokens(path, &manifest.tokens)?;

    Ok(manifest)
}

fn validate_manifest_tokens(path: &Path, tokens: &[TokenManifestToken]) -> Result<(), String> {
    if tokens.is_empty() {
        return Err(format!(
            "admin token manifest {} must contain at least one token entry",
            path.display()
        ));
    }

    for token in tokens {
        if token.id.trim().is_empty() {
            return Err("admin token manifest token id must not be empty".to_string());
        }

        let hash = token.token_sha256_hex.trim();
        if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(format!(
                "admin token manifest token '{}' has invalid token_sha256_hex (expected 64 hex chars)",
                token.id
            ));
        }

        if token.scopes.is_empty() {
            return Err(format!(
                "admin token manifest token '{}' must include at least one scope",
                token.id
            ));
        }

        for scope in &token.scopes {
            match scope.trim().to_ascii_lowercase().as_str() {
                "read" | "write" => {}
                other => {
                    return Err(format!(
                        "admin token manifest token '{}' has unsupported scope '{}' (expected read|write)",
                        token.id, other
                    ));
                }
            }
        }

        if let (Some(not_before), Some(expires_at)) = (token.not_before, token.expires_at) {
            if expires_at <= not_before {
                return Err(format!(
                    "admin token manifest token '{}' has expires_at <= not_before",
                    token.id
                ));
            }
        }
    }

    Ok(())
}
