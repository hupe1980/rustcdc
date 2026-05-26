//! Sensitive data masking and hashing transform.

use ahash::AHashMap as HashMap;

use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};

#[cfg(feature = "encryption")]
use crate::core::{Error, SecretString};
use crate::core::{Event, Result};

use super::Transform;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaskRule {
    Hash,
    Redact(String),
    Null,
    Truncate(usize),
    /// Leave the field value unchanged.
    ///
    /// Use `Passthrough` to explicitly opt a field out of the `default_rule`.
    /// Without `Passthrough`, any field not in `mask_rules` is processed by
    /// `default_rule` (which defaults to [`MaskRule::Hash`]).
    Passthrough,
    #[cfg(feature = "encryption")]
    Encrypt(SecretString),
    #[cfg(feature = "encryption")]
    Decrypt(SecretString),
}

#[derive(Debug, Clone)]
pub struct MaskHashConfig {
    pub mask_rules: HashMap<String, MaskRule>,
    pub default_rule: MaskRule,
}

impl Default for MaskHashConfig {
    fn default() -> Self {
        Self {
            mask_rules: HashMap::new(),
            default_rule: MaskRule::Hash,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MaskHashTransform {
    pub config: MaskHashConfig,
}

impl MaskHashTransform {
    pub fn new(config: MaskHashConfig) -> Self {
        Self { config }
    }

    fn apply_payload(&self, payload: &mut Option<Value>) -> Result<()> {
        if let Some(value) = payload {
            let mut path_buf = String::new();
            self.walk_value(value, &mut path_buf)?;
        }
        Ok(())
    }

    fn walk_value(&self, value: &mut Value, path: &mut String) -> Result<()> {
        match value {
            Value::Object(map) => {
                for (key, child) in map.iter_mut() {
                    let prev = path.len();
                    if prev > 0 {
                        path.push('.');
                    }
                    path.push_str(key);
                    self.walk_value(child, path)?;
                    path.truncate(prev);
                }
            }
            Value::Array(values) => {
                use std::fmt::Write as _;
                for (index, child) in values.iter_mut().enumerate() {
                    let prev = path.len();
                    if prev > 0 {
                        path.push('.');
                    }
                    let _ = write!(path, "{index}");
                    self.walk_value(child, path)?;
                    path.truncate(prev);
                }
            }
            _ => {
                if !path.is_empty() {
                    let rule = self
                        .config
                        .mask_rules
                        .get(path.as_str())
                        .unwrap_or(&self.config.default_rule);
                    if !matches!(rule, MaskRule::Passthrough) {
                        *value = apply_rule(value, rule)?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Transform for MaskHashTransform {
    async fn apply(&self, event: &mut Event) -> Result<bool> {
        self.apply_payload(&mut event.before)?;
        self.apply_payload(&mut event.after)?;
        Ok(true)
    }

    fn name(&self) -> &str {
        "mask_hash"
    }
}

fn apply_rule(value: &Value, rule: &MaskRule) -> Result<Value> {
    Ok(match rule {
        MaskRule::Passthrough => unreachable!("Passthrough is handled before apply_rule"),
        MaskRule::Hash => {
            let digest = Sha256::digest(value.to_string().as_bytes());
            Value::String(format!("{digest:x}"))
        }
        MaskRule::Redact(mask) => Value::String(mask.clone()),
        MaskRule::Null => Value::Null,
        MaskRule::Truncate(count) => match value {
            Value::String(string) => Value::String(string.chars().take(*count).collect()),
            _ => value.clone(),
        },
        #[cfg(feature = "encryption")]
        MaskRule::Encrypt(secret) => encrypt_value(value, secret)?,
        #[cfg(feature = "encryption")]
        MaskRule::Decrypt(secret) => decrypt_value(value, secret)?,
    })
}

#[cfg(feature = "encryption")]
fn encrypt_value(value: &Value, secret: &SecretString) -> Result<Value> {
    use aes_gcm::{
        aead::{rand_core::RngCore, Aead, KeyInit, OsRng},
        Aes256Gcm, Nonce,
    };
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    let key = derive_encryption_key(secret)?;
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|error| Error::TransformError(format!("invalid encryption key: {error}")))?;

    let plaintext = serde_json::to_vec(value)?;
    let mut nonce = [0_u8; 12];
    OsRng.fill_bytes(&mut nonce);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext.as_ref())
        .map_err(|error| Error::TransformError(format!("encryption failed: {error}")))?;

    Ok(Value::String(format!(
        "enc:{}:{}",
        STANDARD.encode(nonce),
        STANDARD.encode(ciphertext)
    )))
}

#[cfg(feature = "encryption")]
fn decrypt_value(value: &Value, secret: &SecretString) -> Result<Value> {
    use aes_gcm::{aead::Aead, Aes256Gcm, KeyInit, Nonce};
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    let encoded = value.as_str().ok_or_else(|| {
        Error::TransformError("decrypt rule requires a string ciphertext payload".into())
    })?;
    let (nonce_b64, ciphertext_b64) = parse_encrypted_payload(encoded)?;
    let key = derive_encryption_key(secret)?;

    let nonce = STANDARD.decode(nonce_b64).map_err(|error| {
        Error::TransformError(format!("invalid encrypted payload nonce: {error}"))
    })?;
    if nonce.len() != 12 {
        return Err(Error::TransformError(format!(
            "invalid encrypted payload nonce length: {}",
            nonce.len()
        )));
    }
    let ciphertext = STANDARD.decode(ciphertext_b64).map_err(|error| {
        Error::TransformError(format!("invalid encrypted payload ciphertext: {error}"))
    })?;

    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|error| Error::TransformError(format!("invalid encryption key: {error}")))?;
    let plaintext = cipher
        .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
        .map_err(|error| Error::TransformError(format!("decryption failed: {error}")))?;

    serde_json::from_slice(&plaintext).map_err(|error| {
        Error::TransformError(format!("decrypted payload is not valid JSON: {error}"))
    })
}

/// HKDF-SHA-256 key derivation for AES-256-GCM field encryption.
///
/// Derives a 256-bit key from `secret` using HKDF (RFC 5869) with SHA-256 and
/// the domain-separation label `b"rustcdc-field-encryption"`. The label ensures
/// the derived key is independent of any other HKDF usage with the same secret.
///
/// Note: HKDF is an *extraction + expansion* function, not a password KDF. For
/// human-chosen passphrases, pre-hash with argon2 or bcrypt before using as the
/// HKDF input key material. For high-entropy machine secrets (e.g., 256-bit
/// random tokens), HKDF is sufficient.
#[cfg(feature = "encryption")]
fn derive_encryption_key(secret: &SecretString) -> Result<[u8; 32]> {
    use hkdf::Hkdf;
    use sha2::Sha256;

    let resolved = secret.resolve()?;
    let hk = Hkdf::<Sha256>::new(None, resolved.as_bytes());
    let mut key = [0_u8; 32];
    hk.expand(b"rustcdc-field-encryption", &mut key)
        .map_err(|_| Error::TransformError("HKDF expand failed (output too long)".into()))?;
    Ok(key)
}

/// Parses an encrypted field payload in the format `enc:<nonce_b64>:<ciphertext_b64>`.
/// Returns `(nonce_b64, ciphertext_b64)` on success.
#[cfg(feature = "encryption")]
fn parse_encrypted_payload(input: &str) -> Result<(&str, &str)> {
    let input = input.strip_prefix("enc:").ok_or_else(|| {
        Error::TransformError("encrypted payload must match format enc:<nonce>:<ciphertext>".into())
    })?;
    let sep = input.find(':').ok_or_else(|| {
        Error::TransformError("encrypted payload must match format enc:<nonce>:<ciphertext>".into())
    })?;
    let (nonce, rest) = input.split_at(sep);
    let ciphertext = &rest[1..];
    if nonce.is_empty() || ciphertext.is_empty() {
        return Err(Error::TransformError(
            "encrypted payload must match format enc:<nonce>:<ciphertext>".into(),
        ));
    }
    Ok((nonce, ciphertext))
}

#[cfg(test)]
mod tests {
    use ahash::AHashMap as HashMap;

    #[cfg(feature = "encryption")]
    use crate::core::SecretString;
    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    use crate::transform::Transform;
    use serde_json::json;

    use super::{MaskHashConfig, MaskHashTransform, MaskRule};

    fn event() -> Event {
        Event {
            before: Some(json!({"email": "old@example.com"})),
            after: Some(json!({
                "id": 1,
                "email": "alice@example.com",
                "profile": {"phone": "123456"}
            })),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "1".into(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".into()),
            table: "users".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    #[tokio::test]
    async fn hash_rule_is_applied() {
        let mut rules = HashMap::new();
        rules.insert("email".into(), MaskRule::Hash);
        let transform = MaskHashTransform::new(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Null,
        });

        let mut event = event();
        assert!(transform.apply(&mut event).await.unwrap());
        assert!(event.after.unwrap()["email"].as_str().unwrap().len() >= 64);
    }

    #[tokio::test]
    async fn redact_and_null_rules_are_applied() {
        let mut rules = HashMap::new();
        rules.insert("email".into(), MaskRule::Redact("***".into()));
        let transform = MaskHashTransform::new(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Null,
        });

        let mut event = event();
        assert!(transform.apply(&mut event).await.unwrap());
        let after = event.after.unwrap();
        assert_eq!(after["email"], "***");
        assert!(after["id"].is_null());
    }

    #[tokio::test]
    async fn truncate_rule_is_applied() {
        let mut rules = HashMap::new();
        rules.insert("email".into(), MaskRule::Truncate(5));
        let transform = MaskHashTransform::new(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Hash,
        });

        let mut event = event();
        assert!(transform.apply(&mut event).await.unwrap());
        assert_eq!(event.after.unwrap()["email"], "alice");
    }

    #[tokio::test]
    async fn nested_columns_can_be_masked() {
        let mut rules = HashMap::new();
        rules.insert("profile.phone".into(), MaskRule::Redact("hidden".into()));
        let transform = MaskHashTransform::new(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Hash,
        });

        let mut event = event();
        assert!(transform.apply(&mut event).await.unwrap());
        assert_eq!(event.after.unwrap()["profile"]["phone"], "hidden");
    }

    #[tokio::test]
    async fn mask_hash_is_deterministic() {
        let mut rules = HashMap::new();
        rules.insert("email".into(), MaskRule::Hash);
        let transform = MaskHashTransform::new(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Null,
        });

        let mut first = event();
        let mut second = event();
        assert!(transform.apply(&mut first).await.unwrap());
        assert!(transform.apply(&mut second).await.unwrap());
        assert_eq!(first.after, second.after);
    }

    #[cfg(feature = "encryption")]
    #[tokio::test]
    async fn encrypt_and_decrypt_rule_round_trip_json_values() {
        let mut encrypt_rules = HashMap::new();
        encrypt_rules.insert(
            "profile.phone".into(),
            MaskRule::Encrypt(SecretString::new("field-key")),
        );
        let encrypt = MaskHashTransform::new(MaskHashConfig {
            mask_rules: encrypt_rules,
            default_rule: MaskRule::Null,
        });

        let mut encrypted_event = event();
        assert!(encrypt.apply(&mut encrypted_event).await.unwrap());
        let ciphertext = encrypted_event.after.as_ref().unwrap()["profile"]["phone"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(ciphertext.starts_with("enc:"));
        assert_eq!(ciphertext.splitn(3, ':').count(), 3); // enc:<nonce>:<ciphertext>
        assert_ne!(ciphertext, "123456");

        let mut decrypt_rules = HashMap::new();
        decrypt_rules.insert(
            "profile.phone".into(),
            MaskRule::Decrypt(SecretString::new("field-key")),
        );
        let decrypt = MaskHashTransform::new(MaskHashConfig {
            mask_rules: decrypt_rules,
            default_rule: MaskRule::Null,
        });

        let mut decrypt_event = encrypted_event.clone();
        assert!(decrypt.apply(&mut decrypt_event).await.unwrap());
        assert_eq!(decrypt_event.after.unwrap()["profile"]["phone"], "123456");
    }

    #[cfg(feature = "encryption")]
    #[tokio::test]
    async fn encrypt_rule_is_non_deterministic_due_to_random_nonce() {
        let mut rules = HashMap::new();
        rules.insert(
            "email".into(),
            MaskRule::Encrypt(SecretString::new("field-key")),
        );
        let transform = MaskHashTransform::new(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Null,
        });

        let mut first = event();
        let mut second = event();
        assert!(transform.apply(&mut first).await.unwrap());
        assert!(transform.apply(&mut second).await.unwrap());
        assert_ne!(first.after, second.after);
    }

    #[cfg(feature = "encryption")]
    #[tokio::test]
    async fn decrypt_with_wrong_key_errors() {
        let mut encrypt_rules = HashMap::new();
        encrypt_rules.insert(
            "email".into(),
            MaskRule::Encrypt(SecretString::new("field-key")),
        );
        let encrypt = MaskHashTransform::new(MaskHashConfig {
            mask_rules: encrypt_rules,
            default_rule: MaskRule::Null,
        });

        let mut encrypted_event = event();
        assert!(encrypt.apply(&mut encrypted_event).await.unwrap());

        let mut decrypt_rules = HashMap::new();
        decrypt_rules.insert(
            "email".into(),
            MaskRule::Decrypt(SecretString::new("wrong-key")),
        );
        let decrypt = MaskHashTransform::new(MaskHashConfig {
            mask_rules: decrypt_rules,
            default_rule: MaskRule::Null,
        });

        let mut decrypt_event = encrypted_event;
        assert!(decrypt.apply(&mut decrypt_event).await.is_err());
    }
}
