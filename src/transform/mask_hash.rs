//! Sensitive data masking and hashing transform.
//!
//! # Security Note
//!
//! [`MaskRule::UnsaltedSha256`] provides **obfuscation**, not pseudonymization.
//! SHA-256 is a deterministic, fast hash: for low-cardinality fields (e.g. gender, country code)
//! or enumerable values, the original value can be recovered via brute-force lookup.
//! For GDPR-grade pseudonymization use `MaskRule::HmacSha256` or `MaskRule::Encrypt`
//! instead — both require the `encryption` feature, which is why they are named here
//! rather than linked.

use ahash::AHashMap as HashMap;

use serde_json::Value;
use sha2::{Digest, Sha256};

#[cfg(feature = "encryption")]
use crate::core::{Error, SecretString};
use crate::core::{Event, Result};

use super::{Transform, UnmatchedRule};

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
/// What to do with a field the mask configuration names.
///
/// Rules match by **exact dotted JSON path** against a `default_rule`, so a typo or an
/// upstream rename silently disables one. Every rule carries a hit counter for exactly
/// that reason — see [`Transform::unmatched_rules`].
pub enum MaskRule {
    /// Deterministic SHA-256 hash (no salt).
    ///
    /// Provides obfuscation only — **not** GDPR-safe pseudonymization for low-cardinality fields.
    /// Use `MaskRule::HmacSha256` or `MaskRule::Encrypt` (both `encryption`-gated) for keyed
    /// pseudonymization.
    UnsaltedSha256,
    /// Replace the value with a fixed placeholder.
    Redact(String),
    /// Replace the value with JSON `null`.
    ///
    /// Note this is indistinguishable downstream from a genuine `NULL`; use
    /// [`MaskRule::Redact`] when a consumer must be able to tell the two apart.
    Null,
    /// Keep the first `n` characters of a string value; leave non-strings unchanged.
    Truncate(usize),
    /// Leave the field value unchanged.
    ///
    /// This is the `default_rule` for [`MaskHashConfig`], meaning fields not
    /// explicitly listed in `mask_rules` pass through unmodified unless you
    /// call [`MaskHashConfig::hash_all`].
    Passthrough,
    /// HMAC-SHA256 keyed pseudonymization (requires `encryption` feature).
    ///
    /// Produces a deterministic, non-reversible 256-bit MAC tag using the supplied secret as the
    /// HMAC key. Safe to use for GDPR pseudonymization when the key is kept secret — unlike
    /// [`MaskRule::UnsaltedSha256`], a rainbow-table attack requires knowledge of the key.
    #[cfg(feature = "encryption")]
    HmacSha256(SecretString),
    #[cfg(feature = "encryption")]
    /// AES-256-GCM encrypt to `enc:v1:<nonce>:<ciphertext>`.
    ///
    /// Ciphertexts are bound to `table + JSON path` as associated data, so a value
    /// relocated to another column or row fails authentication instead of decrypting as
    /// authentic. **Non-deterministic** — a fresh nonce per call means the same input
    /// encrypts differently every time, so never apply this to a primary-key column or to
    /// any field a downstream deduplicates on.
    Encrypt(SecretString),
    #[cfg(feature = "encryption")]
    /// Reverse [`MaskRule::Encrypt`].
    ///
    /// Fails if the value is decrypted in a different context than it was encrypted in —
    /// a different table or field — because the associated data will not match.
    Decrypt(SecretString),
}

#[derive(Debug, Clone)]
/// Which fields to mask, and how.
pub struct MaskHashConfig {
    /// Rules keyed by dotted JSON path.
    ///
    /// A trailing `.*` matches every child one level down, which is what makes
    /// variable-length arrays coverable — `emails.*` masks every element, whereas
    /// enumerating `emails.0`, `emails.1`, … leaks whatever the operator did not guess.
    /// A rule on an object- or array-valued field masks the whole subtree.
    pub mask_rules: HashMap<String, MaskRule>,
    /// Rule applied to any field not present in `mask_rules`.
    ///
    /// **Default: [`MaskRule::Passthrough`]** — unlisted fields are left unchanged.
    /// To hash all unlisted fields use [`MaskHashConfig::hash_all`].
    pub default_rule: MaskRule,
}

impl Default for MaskHashConfig {
    /// Creates a configuration that **leaves all fields unchanged** unless
    /// they are explicitly listed in `mask_rules`.
    ///
    /// # Behaviour change
    /// Earlier versions defaulted `default_rule` to `MaskRule::UnsaltedSha256`, which
    /// silently hashed every field not mentioned in `mask_rules`.  This has
    /// been changed to `MaskRule::Passthrough` to eliminate unexpected data
    /// loss.  Use [`MaskHashConfig::hash_all`] to restore the old behaviour.
    fn default() -> Self {
        Self {
            mask_rules: HashMap::new(),
            default_rule: MaskRule::Passthrough,
        }
    }
}

impl MaskHashConfig {
    /// Create a configuration that **SHA-256 hashes every field** not
    /// explicitly listed in `mask_rules`.
    ///
    /// This is the opt-in "hash everything" mode.  Use [`Default::default`]
    /// when you only want to mask a specific set of fields and leave the rest
    /// untouched.
    pub fn hash_all() -> Self {
        Self {
            mask_rules: HashMap::new(),
            default_rule: MaskRule::UnsaltedSha256,
        }
    }

    /// Reject a configuration whose rules cannot do what they appear to.
    ///
    /// Called by [`MaskHashTransform::new`], so a bad rule fails at construction rather
    /// than producing plausible-looking output forever. Matches the validation
    /// [`FilterProjectionConfig::validate`](crate::transform::FilterProjectionConfig::validate)
    /// and [`RouteConfig::validate`](crate::transform::RouteConfig::validate) already do.
    ///
    /// # What is rejected, and why
    ///
    /// - **`Truncate(0)`** produces an empty string, which is indistinguishable
    ///   downstream from a genuinely empty column. It is almost always a typo for
    ///   [`MaskRule::Redact`] or [`MaskRule::Null`], and unlike those two it leaves a
    ///   consumer no way to tell that masking happened at all.
    /// - **`Redact("")`** has the same defect for the same reason.
    /// - **An empty rule path** can never match a field, so the rule silently does
    ///   nothing — the failure mode the hit counters exist to surface, caught earlier.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`](crate::core::Error::ConfigError) naming the offending
    /// path and the rule to use instead.
    pub fn validate(&self) -> Result<()> {
        let rules = self
            .mask_rules
            .iter()
            .map(|(path, rule)| (path.as_str(), rule))
            // The default rule reaches every unlisted field, so a bad one is worse, not
            // exempt.
            .chain([("<default_rule>", &self.default_rule)]);

        for (path, rule) in rules {
            if path.trim().is_empty() {
                return Err(crate::core::Error::ConfigError(
                    "mask rule path must not be empty: an empty path matches no field, so \
                     the rule would silently do nothing"
                        .into(),
                ));
            }
            match rule {
                MaskRule::Truncate(0) => {
                    return Err(crate::core::Error::ConfigError(format!(
                        "mask rule for '{path}' is Truncate(0), which produces an empty \
                         string — indistinguishable downstream from a genuinely empty \
                         column. Use MaskRule::Redact with a marker a consumer can \
                         recognise, or MaskRule::Null, both of which say masking happened."
                    )));
                }
                MaskRule::Redact(placeholder) if placeholder.is_empty() => {
                    return Err(crate::core::Error::ConfigError(format!(
                        "mask rule for '{path}' redacts to an empty string, which is \
                         indistinguishable downstream from a genuinely empty column. Give \
                         Redact a marker a consumer can recognise (\"***\"), or use \
                         MaskRule::Null."
                    )));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Applies masking, hashing and field encryption to event payloads.
///
/// # Masking fails open, so it reports
///
/// Rules match by exact dotted path against a `Passthrough` default. A typo, an
/// upstream column rename, or a path-mutating transform placed earlier in the
/// pipeline therefore disables the rule **silently** — the field flows through in
/// clear text and nothing anywhere says so. That is the failure mode that matters
/// most here, because the whole point of the transform is that the data never leaves
/// unmasked.
///
/// Fail-closed is not the right answer either: it would refuse to start a pipeline
/// over a rule for an optional column, and operators would work around it by removing
/// rules. Instead every rule carries a hit counter.
/// [`Transform::unmatched_rules`] names the rules that have never fired, the runtime
/// exposes them as `rustcdc_transform_rules_unmatched`, and
/// [`Transform::warn_on_unmatched_rules`] logs them. Alert on the metric, or wire
/// [`MaskHashTransform::unmatched_rule_paths`] into a health check — a rule with zero hits
/// after real traffic means the field is not being masked.
#[derive(Debug, Default)]
pub struct MaskHashTransform {
    /// Rule configuration.
    pub config: MaskHashConfig,
    /// Per-rule hit counters, parallel to `rule_paths`.
    rule_hits: Vec<std::sync::atomic::AtomicU64>,
    /// Rule paths in a stable order, so `rule_hits` can be a flat `Vec`.
    rule_paths: Vec<String>,
    /// Path → index into `rule_hits`.
    rule_slots: ahash::AHashMap<String, usize>,
}

impl Clone for MaskHashTransform {
    /// Clones the configuration; hit counters start fresh in the clone.
    fn clone(&self) -> Self {
        // The original's config was validated at construction, so this cannot fail.
        Self::new_unchecked(self.config.clone())
    }
}

impl MaskHashTransform {
    /// Build a transform from rule configuration, with all hit counters at zero.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`](crate::core::Error::ConfigError) when a rule cannot
    /// do what it appears to — see [`MaskHashConfig::validate`].
    pub fn new(config: MaskHashConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self::new_unchecked(config))
    }

    /// Build a transform without validating the configuration.
    ///
    /// Only for [`Clone`], which rebuilds from a config that was validated when the
    /// original was constructed.
    fn new_unchecked(config: MaskHashConfig) -> Self {
        let mut rule_paths: Vec<String> = config.mask_rules.keys().cloned().collect();
        // Deterministic order so `unmatched_rules()` output is stable across runs.
        rule_paths.sort();
        let rule_slots = rule_paths
            .iter()
            .enumerate()
            .map(|(index, path)| (path.clone(), index))
            .collect();
        let rule_hits = rule_paths
            .iter()
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect();

        Self {
            config,
            rule_hits,
            rule_paths,
            rule_slots,
        }
    }

    /// Configured rule paths that have never matched a field.
    ///
    /// **A non-empty result after real traffic means those fields are not being
    /// masked.** Empty before any events have flowed, so evaluate it against a
    /// meaningful sample rather than at startup.
    ///
    /// The same information reaches the runtime through
    /// [`Transform::unmatched_rules`], which is what feeds the
    /// `rustcdc_transform_rules_unmatched` metric. Prefer the metric for alerting;
    /// this accessor is for a caller that wants just the paths.
    pub fn unmatched_rule_paths(&self) -> Vec<&str> {
        use std::sync::atomic::Ordering;
        self.rule_paths
            .iter()
            .enumerate()
            .filter(|(index, _)| self.rule_hits[*index].load(Ordering::Relaxed) == 0)
            .map(|(_, path)| path.as_str())
            .collect()
    }

    /// Number of fields this rule path has masked.
    pub fn rule_hit_count(&self, path: &str) -> Option<u64> {
        use std::sync::atomic::Ordering;
        self.rule_slots
            .get(path)
            .map(|index| self.rule_hits[*index].load(Ordering::Relaxed))
    }

    /// Record that the rule registered at `path` fired.
    fn record_rule_hit(&self, path: &str) {
        use std::sync::atomic::Ordering;
        if let Some(index) = self.rule_slots.get(path) {
            self.rule_hits[*index].fetch_add(1, Ordering::Relaxed);
        }
    }

    fn apply_payload(&self, payload: &mut Option<Value>, table: &str) -> Result<()> {
        if let Some(value) = payload {
            let mut path_buf = String::new();
            self.walk_value(value, &mut path_buf, table)?;
        }
        Ok(())
    }

    /// Resolve the rule for a JSON path, honouring a trailing-segment `*` wildcard.
    ///
    /// Tries the exact path first, then the path with its final segment replaced by
    /// `*`. This is what makes variable-length arrays coverable: array elements address
    /// as `emails.0`, `emails.1`, … so without a wildcard an operator would have to
    /// enumerate indices they cannot know in advance, and any row with more elements
    /// than they guessed would leak the remainder.
    ///
    /// `emails.*` masks every element; `profile.*` masks every field of an object one
    /// level down.
    fn lookup_rule(&self, path: &str) -> Option<(&MaskRule, &str)> {
        if let Some(rule) = self.config.mask_rules.get_key_value(path) {
            return Some((rule.1, rule.0.as_str()));
        }
        let (prefix, _) = path.rsplit_once('.')?;
        self.config
            .mask_rules
            .get_key_value(&format!("{prefix}.*"))
            .map(|(matched, rule)| (rule, matched.as_str()))
    }

    fn walk_value(&self, value: &mut Value, path: &mut String, table: &str) -> Result<()> {
        // A rule targeting a container applies to the container as a whole.
        //
        // Previously only the scalar arm consulted `mask_rules`, so a rule on an
        // object- or array-valued field was accepted at construction and then did
        // nothing. That is a silent PII leak in exactly the case operators most expect
        // to be covered: a `jsonb` column such as `profile` holding `{"ssn": ...,
        // "dob": ...}` masked with a rule on `"profile"` passed straight through.
        //
        // Arrays were worse: elements address as `emails.0`, `emails.1`, … so covering
        // a variable-length array required enumerating indices the operator cannot know
        // in advance, and a row with three emails leaked the third.
        //
        // Checking here first means a container rule masks the whole subtree; without a
        // container rule the walk descends as before, so per-leaf rules still work.
        if !path.is_empty() && matches!(value, Value::Object(_) | Value::Array(_)) {
            if let Some(rule) = self.config.mask_rules.get(path.as_str()) {
                if !matches!(rule, MaskRule::Passthrough) {
                    *value = apply_rule(value, rule, &field_aad(table, path))?;
                    self.record_rule_hit(path.as_str());
                    return Ok(());
                }
            }
        }

        match value {
            Value::Object(map) => {
                for (key, child) in map.iter_mut() {
                    let prev = path.len();
                    if prev > 0 {
                        path.push('.');
                    }
                    path.push_str(key);
                    self.walk_value(child, path, table)?;
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
                    self.walk_value(child, path, table)?;
                    path.truncate(prev);
                }
            }
            _ => {
                if !path.is_empty() {
                    let matched = self.lookup_rule(path.as_str());
                    let rule = matched.map_or(&self.config.default_rule, |(rule, _)| rule);
                    if !matches!(rule, MaskRule::Passthrough) {
                        *value = apply_rule(value, rule, &field_aad(table, path))?;
                        if let Some((_, rule_path)) = matched {
                            // Record against the *configured* path, not the field path,
                            // so one `emails.*` rule counts every element it covered.
                            self.record_rule_hit(rule_path);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Associated data binding a ciphertext to the field it came from.
///
/// AES-GCM authenticates *integrity* but not *context*. Without AAD a ciphertext is
/// valid anywhere under the same key, so an attacker with write access to the sink — a
/// far weaker position than compromising the database — could copy the encrypted
/// `salary` blob from one row into another, or move an `ssn` ciphertext into the
/// `phone` column, and it would decrypt cleanly and be emitted as authentic. Binding
/// table + JSON path makes both substitutions fail authentication.
#[cfg(feature = "encryption")]
fn field_aad(table: &str, path: &str) -> String {
    format!("rustcdc/v1|{table}|{path}")
}

/// Non-encryption builds do not use the AAD, but the call sites are shared.
#[cfg(not(feature = "encryption"))]
fn field_aad(_table: &str, _path: &str) -> String {
    String::new()
}

impl Transform for MaskHashTransform {
    fn apply(&self, event: &mut Event) -> Result<bool> {
        // Bind ciphertexts to the table they came from — see `field_aad`.
        let table = event.qualified_table_name();
        self.apply_payload(&mut event.before, &table)?;
        self.apply_payload(&mut event.after, &table)?;
        Ok(true)
    }

    fn name(&self) -> &str {
        "mask_hash"
    }

    fn unmatched_rules(&self) -> Vec<UnmatchedRule> {
        self.unmatched_rule_paths()
            .into_iter()
            .map(|path| {
                UnmatchedRule::new(
                    self.name(),
                    "mask",
                    path,
                    "Those fields are NOT being masked and are shipping in clear text — \
                     check for a typo in the path, a column renamed upstream, or an earlier \
                     transform that moved the field.",
                )
            })
            .collect()
    }
}

fn apply_rule(value: &Value, rule: &MaskRule, aad: &str) -> Result<Value> {
    // Only the Encrypt/Decrypt rules consume the associated data, and those are gated
    // behind the `encryption` feature.
    #[cfg(not(feature = "encryption"))]
    let _ = aad;

    Ok(match rule {
        MaskRule::Passthrough => unreachable!("Passthrough is handled before apply_rule"),
        MaskRule::UnsaltedSha256 => {
            // See `fingerprint_event_stable`: RustCrypto 0.11 digests no longer implement
            // `LowerHex`, and the hex encoding here is externally visible data.
            let digest = Sha256::digest(value_as_hash_input(value).as_bytes());
            Value::String(digest.iter().map(|byte| format!("{byte:02x}")).collect())
        }
        #[cfg(feature = "encryption")]
        MaskRule::HmacSha256(secret) => {
            use hmac::{Hmac, KeyInit as _, Mac};
            type HmacSha256Instance = Hmac<Sha256>;
            let resolved = secret.resolve()?;
            let mut mac = HmacSha256Instance::new_from_slice(resolved.as_bytes())
                .map_err(|error| Error::TransformError(format!("HMAC key error: {error}")))?;
            mac.update(value_as_hash_input(value).as_bytes());
            let tag = mac.finalize().into_bytes();
            Value::String(tag.iter().map(|b| format!("{b:02x}")).collect())
        }
        MaskRule::Redact(mask) => Value::String(mask.clone()),
        MaskRule::Null => Value::Null,
        MaskRule::Truncate(count) => match value {
            Value::String(string) => Value::String(string.chars().take(*count).collect()),
            _ => value.clone(),
        },
        #[cfg(feature = "encryption")]
        MaskRule::Encrypt(secret) => encrypt_value(value, secret, aad)?,
        #[cfg(feature = "encryption")]
        MaskRule::Decrypt(secret) => decrypt_value(value, secret, aad)?,
    })
}

/// Encrypt/decrypt use the full JSON encoding intentionally so round-trips are
/// lossless across all value types.  Hash/HMAC callers use this helper instead
/// so that `UnsaltedSha256("alice")` hashes the bare string `alice`, not the
/// JSON-quoted form `"alice"`.
fn value_as_hash_input(value: &Value) -> std::borrow::Cow<'_, str> {
    match value {
        Value::String(s) => std::borrow::Cow::Borrowed(s.as_str()),
        other => std::borrow::Cow::Owned(other.to_string()),
    }
}

#[cfg(feature = "encryption")]
fn encrypt_value(value: &Value, secret: &SecretString, aad: &str) -> Result<Value> {
    use aes_gcm::{
        aead::{Aead, AeadCore, Generate as _, KeyInit},
        Aes256Gcm, Nonce,
    };
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    let key = derive_encryption_key(secret)?;
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|error| Error::TransformError(format!("invalid encryption key: {error}")))?;

    let plaintext = serde_json::to_vec(value)?;
    // `try_generate` rather than `generate`: the infallible variant panics if the OS
    // entropy source fails, and a panic inside a transform takes the pipeline down with a
    // backtrace instead of an actionable error. The fallible path also makes the stakes
    // explicit — a predictable or repeated AES-GCM nonce under the same key is a
    // key-recovery weakness, not a quality problem, so degrading is never an option.
    let nonce = Nonce::<<Aes256Gcm as AeadCore>::NonceSize>::try_generate().map_err(|error| {
        Error::TransformError(format!(
            "failed to draw a random AES-GCM nonce from the system entropy source: {error}. \
             Refusing to encrypt: reusing a nonce under the same key leaks the XOR of the \
             plaintexts and the GCM authentication subkey."
        ))
    })?;
    let ciphertext = cipher
        .encrypt(
            &nonce,
            aes_gcm::aead::Payload {
                msg: plaintext.as_ref(),
                aad: aad.as_bytes(),
            },
        )
        .map_err(|error| Error::TransformError(format!("encryption failed: {error}")))?;

    // `v1` is a key/format generation marker. Without it, rotating the secret makes
    // every existing ciphertext undecryptable rather than gracefully migratable,
    // because nothing in the payload says which key produced it.
    Ok(Value::String(format!(
        "enc:v1:{}:{}",
        STANDARD.encode(nonce),
        STANDARD.encode(ciphertext)
    )))
}

#[cfg(feature = "encryption")]
fn decrypt_value(value: &Value, secret: &SecretString, aad: &str) -> Result<Value> {
    use aes_gcm::{
        aead::{Aead, AeadCore},
        Aes256Gcm, KeyInit, Nonce,
    };
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
    // `TryFrom` rather than the deprecated `from_slice`, which panicked on a wrong length.
    // The length was already checked above, so this cannot fail — but a panic on attacker-
    // influenced input is not a failure mode worth keeping around for convenience.
    let nonce = Nonce::<<Aes256Gcm as AeadCore>::NonceSize>::try_from(nonce.as_slice())
        .map_err(|_| Error::TransformError("invalid encrypted payload nonce length".into()))?;
    let plaintext = cipher
        .decrypt(
            &nonce,
            aes_gcm::aead::Payload {
                msg: ciphertext.as_ref(),
                aad: aad.as_bytes(),
            },
        )
        .map_err(|error| {
            Error::TransformError(format!(
                "decryption failed: {error}. If the ciphertext is intact and the key is \
                 correct, this means the value is being decrypted in a different context \
                 than it was encrypted in — a different table or a different field. \
                 Ciphertexts are bound to their origin so they cannot be relocated."
            ))
        })?;

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

/// Parses an encrypted field payload in the format
/// `enc:v1:<nonce_b64>:<ciphertext_b64>`.
///
/// The `v1` generation marker exists so the key/format can be rotated: without it,
/// nothing in the payload identifies which key produced a given ciphertext, so
/// changing the secret orphans every existing value instead of allowing a migration.
///
/// Returns `(nonce_b64, ciphertext_b64)` on success.
#[cfg(feature = "encryption")]
fn parse_encrypted_payload(input: &str) -> Result<(&str, &str)> {
    const EXPECTED: &str = "encrypted payload must match format enc:v1:<nonce>:<ciphertext>";

    let rest = input
        .strip_prefix("enc:")
        .ok_or_else(|| Error::TransformError(EXPECTED.into()))?;

    let rest = rest.strip_prefix("v1:").ok_or_else(|| {
        // Name the unsupported generation rather than reporting a generic parse error,
        // so an operator who rotates the format gets an actionable message.
        let generation = rest.split(':').next().unwrap_or_default();
        Error::TransformError(format!(
            "unsupported encrypted payload generation '{generation}': this build understands \
             only 'v1'. {EXPECTED}"
        ))
    })?;

    let (nonce, ciphertext) = rest
        .split_once(':')
        .ok_or_else(|| Error::TransformError(EXPECTED.into()))?;
    if nonce.is_empty() || ciphertext.is_empty() {
        return Err(Error::TransformError(EXPECTED.into()));
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

    /// Every configuration in this module is deliberately valid; a construction failure
    /// is a test bug, not the behaviour under test. The validation itself is covered by
    /// its own tests below.
    fn mask_transform(config: MaskHashConfig) -> MaskHashTransform {
        MaskHashTransform::new(config).expect("test mask configuration must be valid")
    }

    /// A rule on an object- or array-valued field must actually apply.
    ///
    /// Previously only the scalar arm consulted `mask_rules`, so such a rule was
    /// accepted at construction and then silently did nothing — a PII leak in exactly
    /// the case operators most expect to be covered (a `jsonb` column of PII masked by
    /// naming the column).
    #[tokio::test]
    async fn mask_rule_on_a_container_field_is_applied() {
        let mut config = MaskHashConfig::default();
        config
            .mask_rules
            .insert("profile".into(), MaskRule::Redact("***".into()));
        let transform = mask_transform(config);

        let mut event = event();
        transform.apply(&mut event).unwrap();

        let after = event.after.as_ref().unwrap();
        assert_eq!(
            after.get("profile").unwrap(),
            "***",
            "a rule naming a container must mask the whole subtree"
        );
        // Untargeted fields are untouched.
        assert_eq!(after.get("email").unwrap(), "alice@example.com");
    }

    /// Array elements address as `field.0`, `field.1`, … so a variable-length array is
    /// uncoverable without a wildcard: any row with more elements than the operator
    /// enumerated leaks the remainder.
    #[tokio::test]
    async fn wildcard_rule_masks_every_array_element() {
        let mut config = MaskHashConfig::default();
        config
            .mask_rules
            .insert("emails.*".into(), MaskRule::Redact("***".into()));
        let transform = mask_transform(config);

        let mut event = event();
        event.after = Some(json!({
            "id": 1,
            "emails": ["a@example.com", "b@example.com", "c@example.com"],
        }));
        transform.apply(&mut event).unwrap();

        let emails = event.after.as_ref().unwrap().get("emails").unwrap();
        assert_eq!(
            emails,
            &json!(["***", "***", "***"]),
            "every element must be masked regardless of array length"
        );
    }

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
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }

    /// An event whose `after` payload is exactly `payload`, with no `before` image.
    fn event_with(payload: serde_json::Value) -> Event {
        let mut event = event();
        event.before = None;
        event.after = Some(payload);
        event
    }

    #[tokio::test]
    async fn hash_rule_is_applied() {
        let mut rules = HashMap::new();
        rules.insert("email".into(), MaskRule::UnsaltedSha256);
        let transform = mask_transform(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Null,
        });

        let mut event = event();
        assert!(transform.apply(&mut event).unwrap());
        assert!(event.after.unwrap()["email"].as_str().unwrap().len() >= 64);
    }

    #[tokio::test]
    async fn redact_and_null_rules_are_applied() {
        let mut rules = HashMap::new();
        rules.insert("email".into(), MaskRule::Redact("***".into()));
        let transform = mask_transform(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Null,
        });

        let mut event = event();
        assert!(transform.apply(&mut event).unwrap());
        let after = event.after.unwrap();
        assert_eq!(after["email"], "***");
        assert!(after["id"].is_null());
    }

    #[tokio::test]
    async fn truncate_rule_is_applied() {
        let mut rules = HashMap::new();
        rules.insert("email".into(), MaskRule::Truncate(5));
        let transform = mask_transform(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::UnsaltedSha256,
        });

        let mut event = event();
        assert!(transform.apply(&mut event).unwrap());
        assert_eq!(event.after.unwrap()["email"], "alice");
    }

    #[tokio::test]
    async fn nested_columns_can_be_masked() {
        let mut rules = HashMap::new();
        rules.insert("profile.phone".into(), MaskRule::Redact("hidden".into()));
        let transform = mask_transform(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::UnsaltedSha256,
        });

        let mut event = event();
        assert!(transform.apply(&mut event).unwrap());
        assert_eq!(event.after.unwrap()["profile"]["phone"], "hidden");
    }

    #[tokio::test]
    async fn mask_hash_is_deterministic() {
        let mut rules = HashMap::new();
        rules.insert("email".into(), MaskRule::UnsaltedSha256);
        let transform = mask_transform(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Null,
        });

        let mut first = event();
        let mut second = event();
        assert!(transform.apply(&mut first).unwrap());
        assert!(transform.apply(&mut second).unwrap());
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
        let encrypt = mask_transform(MaskHashConfig {
            mask_rules: encrypt_rules,
            default_rule: MaskRule::Null,
        });

        let mut encrypted_event = event();
        assert!(encrypt.apply(&mut encrypted_event).unwrap());
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
        let decrypt = mask_transform(MaskHashConfig {
            mask_rules: decrypt_rules,
            default_rule: MaskRule::Null,
        });

        let mut decrypt_event = encrypted_event.clone();
        assert!(decrypt.apply(&mut decrypt_event).unwrap());
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
        let transform = mask_transform(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Null,
        });

        let mut first = event();
        let mut second = event();
        assert!(transform.apply(&mut first).unwrap());
        assert!(transform.apply(&mut second).unwrap());
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
        let encrypt = mask_transform(MaskHashConfig {
            mask_rules: encrypt_rules,
            default_rule: MaskRule::Null,
        });

        let mut encrypted_event = event();
        assert!(encrypt.apply(&mut encrypted_event).unwrap());

        let mut decrypt_rules = HashMap::new();
        decrypt_rules.insert(
            "email".into(),
            MaskRule::Decrypt(SecretString::new("wrong-key")),
        );
        let decrypt = mask_transform(MaskHashConfig {
            mask_rules: decrypt_rules,
            default_rule: MaskRule::Null,
        });

        let mut decrypt_event = encrypted_event;
        assert!(decrypt.apply(&mut decrypt_event).is_err());
    }

    #[cfg(feature = "encryption")]
    #[tokio::test]
    async fn decrypt_rejects_invalid_unversioned_payload_format() {
        let mut decrypt_rules = HashMap::new();
        decrypt_rules.insert(
            "email".into(),
            MaskRule::Decrypt(SecretString::new("field-key")),
        );
        let decrypt = mask_transform(MaskHashConfig {
            mask_rules: decrypt_rules,
            default_rule: MaskRule::Null,
        });

        let mut malformed_event = event();
        malformed_event.after = Some(json!({
            "id": 1,
            "email": "enc:missing-separator",
            "profile": {"phone": "123456"}
        }));

        let error = decrypt.apply(&mut malformed_event).unwrap_err();
        let message = format!("{error}");
        assert!(message.contains("enc:v1:<nonce>:<ciphertext>"), "{message}");
    }

    /// A ciphertext must not be relocatable to another field or table.
    ///
    /// AES-GCM authenticates integrity but not context, so without associated data an
    /// attacker with write access to the *sink* — a far weaker position than
    /// compromising the database — could move an encrypted `salary` blob into another
    /// row, or an `ssn` ciphertext into the `phone` column, and it would decrypt
    /// cleanly and be emitted as authentic.
    #[cfg(feature = "encryption")]
    #[tokio::test]
    async fn ciphertext_cannot_be_moved_to_another_field_or_table() {
        let secret = || SecretString::new("field-key");

        let mut encrypt_rules = HashMap::new();
        encrypt_rules.insert("email".into(), MaskRule::Encrypt(secret()));
        let encrypt = mask_transform(MaskHashConfig {
            mask_rules: encrypt_rules,
            default_rule: MaskRule::Passthrough,
        });

        let mut source_event = event();
        source_event.before = None;
        encrypt.apply(&mut source_event).unwrap();
        let ciphertext = source_event
            .after
            .as_ref()
            .unwrap()
            .get("email")
            .unwrap()
            .clone();

        let mut decrypt_rules = HashMap::new();
        decrypt_rules.insert("email".into(), MaskRule::Decrypt(secret()));
        decrypt_rules.insert("phone".into(), MaskRule::Decrypt(secret()));
        let decrypt = mask_transform(MaskHashConfig {
            mask_rules: decrypt_rules,
            default_rule: MaskRule::Passthrough,
        });

        // Same field, same table: decrypts.
        let mut same = event();
        same.before = None;
        same.after = Some(json!({"id": 1, "email": ciphertext.clone()}));
        decrypt.apply(&mut same).unwrap();
        assert_eq!(
            same.after.as_ref().unwrap().get("email").unwrap(),
            "alice@example.com"
        );

        // Relocated to a different column: must fail authentication.
        let mut moved_field = event();
        moved_field.before = None;
        moved_field.after = Some(json!({"id": 1, "phone": ciphertext.clone()}));
        let error = decrypt
            .apply(&mut moved_field)
            .expect_err("a ciphertext moved to another column must not decrypt");
        assert!(format!("{error}").contains("different"), "{error}");

        // Same column, different table: must also fail.
        let mut moved_table = event();
        moved_table.before = None;
        moved_table.table = "audit".into();
        moved_table.after = Some(json!({"id": 1, "email": ciphertext}));
        decrypt
            .apply(&mut moved_table)
            .expect_err("a ciphertext moved to another table must not decrypt");
    }

    #[cfg(feature = "encryption")]
    #[tokio::test]
    async fn hmac_sha256_is_deterministic_and_keyed() {
        let secret = SecretString::new("my-secret-key");
        let mut rules = HashMap::new();
        rules.insert("email".into(), MaskRule::HmacSha256(secret.clone()));
        let transform = mask_transform(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Null,
        });

        let mut first = event();
        let mut second = event();
        assert!(transform.apply(&mut first).unwrap());
        assert!(transform.apply(&mut second).unwrap());
        // Deterministic with same key.
        assert_eq!(first.after, second.after);

        // Tag is 64 hex chars (256-bit HMAC-SHA256).
        let tag = first.after.unwrap()["email"].as_str().unwrap().to_string();
        assert_eq!(tag.len(), 64);
        assert!(tag.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[cfg(feature = "encryption")]
    #[tokio::test]
    async fn hmac_sha256_different_keys_produce_different_tags() {
        let make_transform = |key: &str| {
            let mut rules = HashMap::new();
            rules.insert("email".into(), MaskRule::HmacSha256(SecretString::new(key)));
            mask_transform(MaskHashConfig {
                mask_rules: rules,
                default_rule: MaskRule::Null,
            })
        };

        let t1 = make_transform("key-a");
        let t2 = make_transform("key-b");

        let mut e1 = event();
        let mut e2 = event();
        assert!(t1.apply(&mut e1).unwrap());
        assert!(t2.apply(&mut e2).unwrap());
        assert_ne!(
            e1.after, e2.after,
            "different keys must produce different tags"
        );
    }

    #[cfg(feature = "encryption")]
    #[tokio::test]
    async fn hmac_sha256_differs_from_unsalted_sha256() {
        let unsalted = {
            let mut rules = HashMap::new();
            rules.insert("email".into(), MaskRule::UnsaltedSha256);
            mask_transform(MaskHashConfig {
                mask_rules: rules,
                default_rule: MaskRule::Null,
            })
        };
        let keyed = {
            let mut rules = HashMap::new();
            rules.insert(
                "email".into(),
                MaskRule::HmacSha256(SecretString::new("key")),
            );
            mask_transform(MaskHashConfig {
                mask_rules: rules,
                default_rule: MaskRule::Null,
            })
        };

        let mut e1 = event();
        let mut e2 = event();
        assert!(unsalted.apply(&mut e1).unwrap());
        assert!(keyed.apply(&mut e2).unwrap());
        assert_ne!(
            e1.after, e2.after,
            "unsalted SHA-256 and HMAC-SHA256 must produce different output for same input"
        );
    }

    // ─── Fail-open reporting ─────────────────────────────────────────────────

    #[tokio::test]
    async fn a_rule_that_never_matches_is_reported_rather_than_silently_ignored() {
        // Rules match by exact dotted path against a `Passthrough` default, so a typo
        // or a renamed upstream column disables masking with no error and no log line.
        // The field then flows through in clear text indefinitely.
        let mut rules: HashMap<String, MaskRule> = HashMap::new();
        rules.insert("email".into(), MaskRule::Redact("***".into()));
        rules.insert("emial".into(), MaskRule::Redact("***".into())); // typo
        let transform = mask_transform(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Passthrough,
        });

        let mut event = event_with(json!({"id": 1, "email": "a@example.com"}));
        transform.apply(&mut event).unwrap();

        assert_eq!(
            transform.unmatched_rule_paths(),
            vec!["emial"],
            "the typo'd rule must be reported as never having matched"
        );
        assert_eq!(transform.rule_hit_count("email"), Some(1));
        assert_eq!(transform.warn_on_unmatched_rules(), 1);

        // And the same rule reaches the runtime through the trait, carrying the
        // consequence an operator needs to act on it.
        let reported = Transform::unmatched_rules(&transform);
        assert_eq!(reported.len(), 1);
        assert_eq!(reported[0].transform, "mask_hash");
        assert_eq!(reported[0].kind, "mask");
        assert_eq!(reported[0].rule, "emial");
        assert!(
            reported[0].consequence.contains("clear text"),
            "the consequence must name what is actually happening: {}",
            reported[0].consequence
        );
    }

    #[tokio::test]
    async fn a_wildcard_rule_counts_every_element_it_covered() {
        let mut rules: HashMap<String, MaskRule> = HashMap::new();
        rules.insert("emails.*".into(), MaskRule::Redact("***".into()));
        let transform = mask_transform(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Passthrough,
        });

        let mut event = event_with(json!({"emails": ["a@x.com", "b@x.com", "c@x.com"]}));
        transform.apply(&mut event).unwrap();

        assert_eq!(transform.rule_hit_count("emails.*"), Some(3));
        assert!(transform.unmatched_rule_paths().is_empty());
        assert!(Transform::unmatched_rules(&transform).is_empty());
    }

    // ─── Configuration validation ────────────────────────────────────────────

    #[test]
    fn truncate_zero_is_rejected_at_construction() {
        // `Truncate(0)` yields "", which downstream cannot tell apart from a genuinely
        // empty column — so the masking is invisible, not just useless. It is almost
        // always a typo for Redact or Null.
        let mut rules: HashMap<String, MaskRule> = HashMap::new();
        rules.insert("ssn".into(), MaskRule::Truncate(0));

        let error = MaskHashTransform::new(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Passthrough,
        })
        .expect_err("Truncate(0) must be rejected");

        let message = error.to_string();
        assert!(message.contains("ssn"), "must name the rule: {message}");
        assert!(
            message.contains("Redact") && message.contains("Null"),
            "must name the remedy: {message}"
        );
    }

    #[test]
    fn truncate_zero_is_rejected_as_the_default_rule_too() {
        // The default rule reaches every unlisted field, so a bad one is worse than a
        // bad named rule, not exempt from the check.
        let error = MaskHashTransform::new(MaskHashConfig {
            mask_rules: HashMap::new(),
            default_rule: MaskRule::Truncate(0),
        })
        .expect_err("Truncate(0) as the default must be rejected");
        assert!(error.to_string().contains("<default_rule>"));
    }

    #[test]
    fn truncate_of_at_least_one_is_accepted() {
        let mut rules: HashMap<String, MaskRule> = HashMap::new();
        rules.insert("ssn".into(), MaskRule::Truncate(1));
        MaskHashTransform::new(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Passthrough,
        })
        .expect("Truncate(1) keeps a character and is a legitimate rule");
    }

    #[test]
    fn redacting_to_an_empty_string_is_rejected() {
        // Same defect as Truncate(0), reached a different way.
        let mut rules: HashMap<String, MaskRule> = HashMap::new();
        rules.insert("ssn".into(), MaskRule::Redact(String::new()));
        let error = MaskHashTransform::new(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Passthrough,
        })
        .expect_err("an empty redaction marker must be rejected");
        assert!(error.to_string().contains("empty column"));
    }

    #[test]
    fn an_empty_rule_path_is_rejected() {
        let mut rules: HashMap<String, MaskRule> = HashMap::new();
        rules.insert(String::new(), MaskRule::Redact("***".into()));
        let error = MaskHashTransform::new(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::Passthrough,
        })
        .expect_err("a rule that can never match must be rejected");
        assert!(error.to_string().contains("matches no field"));
    }

    #[test]
    fn the_shipped_constructors_produce_valid_configurations() {
        MaskHashConfig::default()
            .validate()
            .expect("the default configuration must be valid");
        MaskHashConfig::hash_all()
            .validate()
            .expect("hash_all must be valid");
    }
}
