//! [Standard Webhooks](https://www.standardwebhooks.com/) request signing.
//!
//! `bearer_token` proves the sender holds a secret. It does not prove the body is
//! unaltered, and it is replayable by anyone who captures a request or reads a proxy log.
//! A per-request signature over the payload does both, and Standard Webhooks is the
//! interoperable spelling of it — a receiver built against Zapier, Twilio, ngrok, Supabase
//! or Svix verifies these with the same library.
//!
//! Three headers accompany every request: `webhook-id` (the message id, **stable across
//! retries** — the receiver's deduplication key), `webhook-timestamp` (Unix seconds,
//! **regenerated per attempt** — the replay window), and `webhook-signature` (a
//! space-delimited list of `<version>,<base64>`). The signed string is
//! `{id}.{timestamp}.{payload}`, byte for byte as sent.
//!
//! Both emphases matter, and getting either backwards breaks a receiver quietly. An id
//! that changed per attempt would defeat deduplication exactly when at-least-once delivery
//! redelivers. A timestamp frozen at the first attempt would fall outside the receiver's
//! tolerance — five minutes, typically — long before this sink's retry budget is spent, so
//! every late retry would be rejected as a replay.
//!
//! Two limits, stated because they are easy to assume away:
//!
//! * **Signature-compatible, not payload-compatible.** The spec *recommends* a
//!   `{type, timestamp, data}` body; this sink sends whatever `sink.http.codec` produces.
//!   Signatures cover bytes, so verification is unaffected.
//! * **One request is one batch.** The spec is written for one event per request. The batch
//!   is the message, so `webhook-id` identifies the batch, not a row change.

use base64::Engine as _;

use rustcdc::SecretString;

/// Prefix on a symmetric signing secret.
const HMAC_SECRET_PREFIX: &str = "whsec_";
/// Prefix on an asymmetric signing key (the private half).
const ED25519_SECRET_PREFIX: &str = "whsk_";
/// Prefix on an asymmetric verifying key (the public half), for the error that names it.
const ED25519_PUBLIC_PREFIX: &str = "whpk_";

/// Version tag on a symmetric signature.
const SIGNATURE_VERSION_HMAC: &str = "v1";
/// Version tag on an asymmetric signature.
const SIGNATURE_VERSION_ED25519: &str = "v1a";

/// `webhook-id`: the message identifier, and the receiver's idempotency key.
pub const HEADER_ID: &str = "webhook-id";
/// `webhook-timestamp`: Unix seconds, regenerated per attempt.
pub const HEADER_TIMESTAMP: &str = "webhook-timestamp";
/// `webhook-signature`: space-delimited `<version>,<base64>` list.
pub const HEADER_SIGNATURE: &str = "webhook-signature";

/// Which signature scheme a configured key produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookSignatureScheme {
    /// HMAC-SHA256, signature version `v1`, key prefixed `whsec_`.
    ///
    /// The receiver holds the same secret it verifies with, so anyone who can verify can
    /// also forge. That is fine when both ends are yours and a problem when they are not.
    HmacSha256,

    /// Ed25519, signature version `v1a`, private key prefixed `whsk_`.
    ///
    /// **Recommended by the specification, and the right default for a receiver you do not
    /// operate.** The receiver holds only the public half, so a compromised receiver cannot
    /// forge events from you — which a shared HMAC secret cannot promise.
    Ed25519,
}

impl WebhookSignatureScheme {
    fn version(self) -> &'static str {
        match self {
            Self::HmacSha256 => SIGNATURE_VERSION_HMAC,
            Self::Ed25519 => SIGNATURE_VERSION_ED25519,
        }
    }

    fn expected_prefix(self) -> &'static str {
        match self {
            Self::HmacSha256 => HMAC_SECRET_PREFIX,
            Self::Ed25519 => ED25519_SECRET_PREFIX,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::HmacSha256 => "hmac_sha256",
            Self::Ed25519 => "ed25519",
        }
    }
}

/// One signing key, parsed into the form the scheme actually signs with.
///
/// The HMAC variant holds an **already-keyed** `Hmac`, not the key bytes. Keying can fail
/// in principle — `new_from_slice` returns a `Result` — and doing it here means that
/// failure is a startup error like every other key problem, instead of an `.expect()` on
/// the signing path for a case that cannot arise. `Mac::finalize` consumes the instance,
/// so signing clones it; that is the documented RustCrypto pattern and it also skips
/// re-deriving the key schedule per request.
enum SigningKey {
    Hmac(Box<hmac::Hmac<sha2::Sha256>>),
    Ed25519(Box<ed25519_dalek::SigningKey>),
}

/// Signs outgoing requests, including every key currently in rotation.
///
/// `Debug` prints the scheme and the key *count*, never key material — this type is
/// reachable from the sink's own `Debug`, and a signing key rendered into a log or a panic
/// message is a signing key that has to be rotated.
pub struct WebhookSigner {
    scheme: WebhookSignatureScheme,
    /// The active key first, then any keys still being honoured during a rotation.
    ///
    /// Every one of them signs every request, and the signatures are concatenated into the
    /// header. That is what makes rotation zero-downtime: a receiver still holding the old
    /// key finds a signature it can verify, a receiver already updated finds the new one,
    /// and neither needs to change at the same instant as the sender.
    keys: Vec<SigningKey>,
}

impl WebhookSigner {
    /// Parse a primary key plus any keys still in rotation.
    ///
    /// Every failure here is a startup failure. A signing key that cannot be parsed would
    /// otherwise become a request every receiver rejects, and a pipeline that looks healthy
    /// from this side while delivering nothing.
    pub fn new(
        scheme: WebhookSignatureScheme,
        primary: &SecretString,
        rotating: &[SecretString],
    ) -> Result<Self, String> {
        let mut keys = Vec::with_capacity(1 + rotating.len());
        keys.push(parse_key(scheme, primary, "sink.http.signing.key")?);
        for (index, key) in rotating.iter().enumerate() {
            keys.push(parse_key(
                scheme,
                key,
                &format!("sink.http.signing.previous_keys[{index}]"),
            )?);
        }
        Ok(Self { scheme, keys })
    }

    /// The `webhook-signature` value for one attempt.
    ///
    /// `payload` is the exact bytes that will be sent. The specification is emphatic that
    /// the signed bytes and the transmitted bytes must be identical — a re-serialisation
    /// between signing and sending, even one that only changes whitespace, produces a
    /// signature the receiver cannot reproduce.
    pub fn sign(&self, id: &str, timestamp: i64, payload: &[u8]) -> String {
        // `{id}.{timestamp}.{payload}`, built over bytes rather than a `String` because
        // the payload is not required to be UTF-8: a Confluent-framed Avro or Protobuf
        // body is binary, and `String::from_utf8_lossy` would sign replacement characters
        // that are not what goes on the wire.
        let mut signed = Vec::with_capacity(id.len() + 24 + payload.len());
        signed.extend_from_slice(id.as_bytes());
        signed.push(b'.');
        signed.extend_from_slice(timestamp.to_string().as_bytes());
        signed.push(b'.');
        signed.extend_from_slice(payload);

        let version = self.scheme.version();
        self.keys
            .iter()
            .map(|key| format!("{version},{}", encode_signature(key, &signed)))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// How many keys sign each request — one, plus any in rotation.
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    pub fn scheme(&self) -> WebhookSignatureScheme {
        self.scheme
    }
}

impl std::fmt::Debug for WebhookSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookSigner")
            .field("scheme", &self.scheme)
            .field("keys", &self.keys.len())
            .finish()
    }
}

fn encode_signature(key: &SigningKey, signed: &[u8]) -> String {
    match key {
        SigningKey::Hmac(keyed) => {
            use hmac::Mac as _;
            let mut mac = (**keyed).clone();
            mac.update(signed);
            // Standard, padded base64 — what the reference implementation encodes with,
            // and therefore what every receiver library decodes with.
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
        }
        SigningKey::Ed25519(key) => {
            use ed25519_dalek::Signer as _;
            base64::engine::general_purpose::STANDARD.encode(key.sign(signed).to_bytes())
        }
    }
}

/// Decode one configured key, rejecting anything that would sign unverifiably.
fn parse_key(
    scheme: WebhookSignatureScheme,
    key: &SecretString,
    path: &str,
) -> Result<SigningKey, String> {
    let resolved = key
        .resolve()
        .map_err(|e| format!("{path} could not be resolved: {e}"))?;
    let resolved = resolved.trim();

    if resolved.is_empty() {
        return Err(format!("{path} must not be empty"));
    }

    // A key carrying the *other* scheme's prefix is the misconfiguration worth catching.
    // An ed25519 private key fed to HMAC is a perfectly valid HMAC secret: it signs, the
    // request is accepted by this sink, and no receiver on earth can verify it. The prefix
    // exists precisely so this is knowable, so it is checked rather than trimmed away.
    let wrong_prefix = match scheme {
        WebhookSignatureScheme::HmacSha256 => [ED25519_SECRET_PREFIX, ED25519_PUBLIC_PREFIX]
            .into_iter()
            .find(|p| resolved.starts_with(p)),
        WebhookSignatureScheme::Ed25519 => [HMAC_SECRET_PREFIX, ED25519_PUBLIC_PREFIX]
            .into_iter()
            .find(|p| resolved.starts_with(p)),
    };
    if let Some(prefix) = wrong_prefix {
        return Err(format!(
            "{path} is prefixed {prefix:?}, but scheme = \"{}\" expects {:?}. {}",
            scheme.name(),
            scheme.expected_prefix(),
            if prefix == ED25519_PUBLIC_PREFIX {
                "That is a *public* key — it verifies signatures, it does not make them."
            } else {
                "Signing with the wrong key type produces signatures no receiver can \
                 verify, and nothing downstream reports it."
            }
        ));
    }

    // The prefix is optional, matching the reference implementation, which strips it when
    // present and accepts the bare base64 when it is not.
    let body = resolved
        .strip_prefix(scheme.expected_prefix())
        .unwrap_or(resolved);
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(body)
        .map_err(|e| {
            format!(
                "{path} is not valid base64 after the {:?} prefix: {e}",
                scheme.expected_prefix()
            )
        })?;

    match scheme {
        WebhookSignatureScheme::HmacSha256 => {
            if bytes.is_empty() {
                return Err(format!("{path} decoded to zero bytes"));
            }
            // The specification calls for 24–64 bytes of entropy. Shorter is accepted
            // with a warning rather than refused: the key may have been minted by a
            // receiver this pipeline does not control, and refusing it would make an
            // interoperability problem out of a strength one. Silence would be worse.
            if bytes.len() < 24 {
                tracing::warn!(
                    path,
                    bytes = bytes.len(),
                    "webhook signing secret is shorter than the 24 bytes Standard \
                     Webhooks calls for; signatures are valid but weaker than the \
                     specification assumes"
                );
            }
            use hmac::KeyInit as _;
            let keyed = <hmac::Hmac<sha2::Sha256>>::new_from_slice(&bytes)
                .map_err(|e| format!("{path} was rejected as an HMAC key: {e}"))?;
            Ok(SigningKey::Hmac(Box::new(keyed)))
        }
        WebhookSignatureScheme::Ed25519 => {
            // 32 bytes is the ed25519 seed. A 64-byte value is the expanded keypair —
            // named explicitly because exporting one is a common mistake and "invalid
            // length" alone sends people to the wrong place.
            let seed: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                format!(
                    "{path} decoded to {} bytes; an ed25519 signing key is the 32-byte \
                     seed.{}",
                    bytes.len(),
                    if bytes.len() == 64 {
                        " 64 bytes is the expanded keypair — use its first 32 bytes."
                    } else {
                        ""
                    }
                )
            })?;
            Ok(SigningKey::Ed25519(Box::new(
                ed25519_dalek::SigningKey::from_bytes(&seed),
            )))
        }
    }
}

/// A freshly generated signing key pair, encoded the way the configuration expects.
pub struct GeneratedKeys {
    /// The value for `sink.http.signing.key` — `whsec_…` or `whsk_…`.
    pub signing_key: String,
    /// The `whpk_…` public key the receiver verifies with. `None` for HMAC, where the
    /// signing secret *is* the verification secret — which is the property that makes the
    /// asymmetric scheme worth preferring.
    pub public_key: Option<String>,
}

/// Generate a correctly-encoded signing key.
///
/// # Why this is a command rather than a documentation snippet
///
/// Because the encoding is the part people get wrong, and every way of getting it wrong
/// fails *late*. `openssl genpkey` emits PEM; exporting an ed25519 key from most libraries
/// gives the 64-byte expanded keypair rather than the 32-byte seed; `head -c 32
/// /dev/urandom | base64` produces a secret with no prefix, which is accepted, so the
/// mistake is invisible until a receiver cannot verify. [`parse_key`] has a specific error
/// for each of those, which is the wrong end of the problem to solve — this is the right
/// end.
///
/// Randomness comes from rustls's CSPRNG, which is already in the dependency graph and
/// already installed process-wide. Adding a `rand` dependency for this would be a second
/// secure random source to audit, which the project deliberately avoids.
pub fn generate_keys(scheme: WebhookSignatureScheme) -> Result<GeneratedKeys, String> {
    // 32 bytes for both schemes: the ed25519 seed length, and comfortably inside the
    // 24–64 bytes the specification asks of an HMAC secret.
    let mut seed = [0u8; 32];
    rustls::crypto::aws_lc_rs::default_provider()
        .secure_random
        .fill(&mut seed)
        .map_err(|_| "failed to draw key material from the system CSPRNG".to_string())?;

    let encoded = base64::engine::general_purpose::STANDARD.encode(seed);
    Ok(match scheme {
        WebhookSignatureScheme::HmacSha256 => GeneratedKeys {
            signing_key: format!("{HMAC_SECRET_PREFIX}{encoded}"),
            public_key: None,
        },
        WebhookSignatureScheme::Ed25519 => {
            let signing = ed25519_dalek::SigningKey::from_bytes(&seed);
            let public = base64::engine::general_purpose::STANDARD
                .encode(signing.verifying_key().to_bytes());
            GeneratedKeys {
                signing_key: format!("{ED25519_SECRET_PREFIX}{encoded}"),
                public_key: Some(format!("{ED25519_PUBLIC_PREFIX}{public}")),
            }
        }
    })
}

impl WebhookSigner {
    /// The `whpk_…` public key for the **active** ed25519 key, or `None` under HMAC.
    ///
    /// An operator who configures `scheme = "ed25519"` has to hand the receiver the public
    /// half, and until this existed there was no way to get it out of rustcdc at all — the
    /// configuration holds only the private seed. The sink logs this at startup for exactly
    /// that reason. It is not a secret; publishing it is the entire point.
    ///
    /// Only the active key, deliberately. During a rotation the previous key still signs,
    /// but the thing an operator is trying to do is distribute the *new* one.
    pub fn public_key(&self) -> Option<String> {
        match self.keys.first()? {
            SigningKey::Ed25519(key) => Some(format!(
                "{ED25519_PUBLIC_PREFIX}{}",
                base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes())
            )),
            SigningKey::Hmac(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret(value: &str) -> SecretString {
        SecretString::from(value.to_string())
    }

    /// A generated key must be one this crate's own parser accepts, and the public half
    /// must verify what the private half signs. Generating something `parse_key` rejects
    /// would be the worst possible bug in a command that exists to prevent encoding
    /// mistakes.
    #[test]
    fn a_generated_ed25519_pair_round_trips_through_the_parser_and_verifies() {
        use ed25519_dalek::{Signature, Verifier as _, VerifyingKey};

        let keys = generate_keys(WebhookSignatureScheme::Ed25519).expect("generate");
        assert!(
            keys.signing_key.starts_with("whsk_"),
            "{}",
            keys.signing_key
        );
        let public = keys.public_key.clone().expect("ed25519 has a public half");
        assert!(public.starts_with("whpk_"), "{public}");

        let signer = WebhookSigner::new(
            WebhookSignatureScheme::Ed25519,
            &secret(&keys.signing_key),
            &[],
        )
        .expect("a generated key must parse");

        // The reported public key must match the generated one.
        assert_eq!(signer.public_key().as_deref(), Some(public.as_str()));

        // And it must actually verify a signature the private half produced.
        let header = signer.sign("msg_1", 1_700_000_000, b"payload");
        let signature = Signature::from_slice(
            &base64::engine::general_purpose::STANDARD
                .decode(header.strip_prefix("v1a,").expect("v1a"))
                .expect("base64"),
        )
        .expect("signature");
        let key_bytes: [u8; 32] = base64::engine::general_purpose::STANDARD
            .decode(public.strip_prefix("whpk_").expect("whpk_"))
            .expect("base64")
            .try_into()
            .expect("32 bytes");
        VerifyingKey::from_bytes(&key_bytes)
            .expect("verifying key")
            .verify(b"msg_1.1700000000.payload", &signature)
            .expect("the published public key must verify the signature");
    }

    #[test]
    fn a_generated_hmac_secret_parses_and_has_no_public_half() {
        let keys = generate_keys(WebhookSignatureScheme::HmacSha256).expect("generate");
        assert!(
            keys.signing_key.starts_with("whsec_"),
            "{}",
            keys.signing_key
        );
        assert!(
            keys.public_key.is_none(),
            "a shared secret has no public half — that is the whole difference"
        );
        let signer = WebhookSigner::new(
            WebhookSignatureScheme::HmacSha256,
            &secret(&keys.signing_key),
            &[],
        )
        .expect("a generated secret must parse");
        assert!(signer.public_key().is_none());
    }

    #[test]
    fn two_generated_keys_differ() {
        let a = generate_keys(WebhookSignatureScheme::Ed25519).expect("generate");
        let b = generate_keys(WebhookSignatureScheme::Ed25519).expect("generate");
        assert_ne!(
            a.signing_key, b.signing_key,
            "a constant key would be catastrophic and silent"
        );
    }

    /// The published Standard Webhooks test vector.
    ///
    /// This is the only test here that proves *interoperability* rather than internal
    /// consistency: a round-trip against our own verifier would pass just as well with the
    /// delimiter, the base64 alphabet or the key decoding all wrong. Reproducing a
    /// signature computed by the reference implementation is what says a receiver library
    /// will accept ours.
    #[test]
    fn the_published_specification_test_vector_reproduces_exactly() {
        let signer = WebhookSigner::new(
            WebhookSignatureScheme::HmacSha256,
            &secret("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"),
            &[],
        )
        .expect("valid secret");

        let signature = signer.sign(
            "msg_p5jXN8AQM9LWM0D4loKWxJek",
            1_614_265_330,
            br#"{"test": 2432232314}"#,
        );

        assert_eq!(signature, "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=");
    }

    #[test]
    fn the_prefix_is_optional_exactly_as_the_reference_implementation_has_it() {
        let with = WebhookSigner::new(
            WebhookSignatureScheme::HmacSha256,
            &secret("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"),
            &[],
        )
        .expect("prefixed");
        let without = WebhookSigner::new(
            WebhookSignatureScheme::HmacSha256,
            &secret("MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"),
            &[],
        )
        .expect("bare");

        assert_eq!(
            with.sign("id", 1, b"body"),
            without.sign("id", 1, b"body"),
            "stripping the prefix must not change the key"
        );
    }

    /// The misconfiguration that would otherwise succeed silently: an ed25519 private key
    /// is a perfectly good HMAC secret, so it signs, the sink reports success, and no
    /// receiver can verify a single request.
    #[test]
    fn a_key_carrying_the_other_schemes_prefix_is_refused() {
        let error = WebhookSigner::new(
            WebhookSignatureScheme::HmacSha256,
            &secret("whsk_K5oZfzN95Z9UVu1EsfQmfVNQhnkZ2pj9o9NDN/H/pI4="),
            &[],
        )
        .expect_err("an ed25519 key is not an HMAC secret");
        assert!(error.contains("whsk_"), "{error}");
        assert!(error.contains("hmac_sha256"), "{error}");
    }

    /// Handing the sink a *public* key is the other half of the same mistake, and deserves
    /// its own sentence rather than "wrong prefix".
    #[test]
    fn a_public_key_is_refused_with_the_reason_spelled_out() {
        let error = WebhookSigner::new(
            WebhookSignatureScheme::Ed25519,
            &secret("whpk_K5oZfzN95Z9UVu1EsfQmfVNQhnkZ2pj9o9NDN/H/pI4="),
            &[],
        )
        .expect_err("a public key does not sign");
        assert!(error.contains("public"), "{error}");
    }

    #[test]
    fn an_unparseable_key_fails_at_construction_rather_than_per_request() {
        for bad in ["whsec_not base64!!", "whsec_", "   "] {
            assert!(
                WebhookSigner::new(WebhookSignatureScheme::HmacSha256, &secret(bad), &[]).is_err(),
                "{bad:?} should be refused"
            );
        }
    }

    /// The ed25519 signature must verify under the public half of the same seed — the only
    /// check that proves the seed was interpreted as a seed.
    #[test]
    fn an_ed25519_signature_verifies_under_the_matching_public_key() {
        use ed25519_dalek::{Signature, Verifier as _};

        let seed = [7u8; 32];
        let encoded = base64::engine::general_purpose::STANDARD.encode(seed);
        let signer = WebhookSigner::new(
            WebhookSignatureScheme::Ed25519,
            &secret(&format!("whsk_{encoded}")),
            &[],
        )
        .expect("valid seed");

        let header = signer.sign("msg_1", 1_700_000_000, b"payload");
        let encoded_signature = header
            .strip_prefix("v1a,")
            .expect("asymmetric signatures use the v1a tag");
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded_signature)
            .expect("base64");
        let signature = Signature::from_slice(&bytes).expect("ed25519 signatures are 64 bytes");

        let verifying = ed25519_dalek::SigningKey::from_bytes(&seed).verifying_key();
        verifying
            .verify(b"msg_1.1700000000.payload", &signature)
            .expect("the receiver must be able to verify with the public half alone");
    }

    #[test]
    fn an_ed25519_key_of_the_wrong_length_names_the_expanded_keypair_case() {
        let encoded = base64::engine::general_purpose::STANDARD.encode([1u8; 64]);
        let error = WebhookSigner::new(
            WebhookSignatureScheme::Ed25519,
            &secret(&format!("whsk_{encoded}")),
            &[],
        )
        .expect_err("64 bytes is the keypair, not the seed");
        assert!(error.contains("expanded keypair"), "{error}");
    }

    /// Zero-downtime rotation: every key signs, and the receiver needs only one of them.
    #[test]
    fn every_rotating_key_contributes_a_signature() {
        let signer = WebhookSigner::new(
            WebhookSignatureScheme::HmacSha256,
            &secret("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"),
            &[secret("whsec_cHJldmlvdXMta2V5LWZvci1yb3RhdGlvbg==")],
        )
        .expect("valid keys");
        assert_eq!(signer.key_count(), 2);

        let header = signer.sign("msg_1", 1_614_265_330, b"body");
        let signatures: Vec<&str> = header.split(' ').collect();
        assert_eq!(signatures.len(), 2, "one signature per key: {header}");
        assert!(signatures.iter().all(|s| s.starts_with("v1,")), "{header}");
        assert_ne!(
            signatures[0], signatures[1],
            "two different keys must produce two different signatures"
        );
        // The active key signs first, so a receiver that stops at the first match uses it.
        let active = WebhookSigner::new(
            WebhookSignatureScheme::HmacSha256,
            &secret("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"),
            &[],
        )
        .expect("valid key");
        assert_eq!(signatures[0], active.sign("msg_1", 1_614_265_330, b"body"));
    }

    /// A binary payload — Confluent-framed Avro, say — must be signed as bytes. Routing it
    /// through a `String` would sign U+FFFD replacement characters instead of what is sent.
    #[test]
    fn a_non_utf8_payload_is_signed_as_the_bytes_that_are_sent() {
        let signer = WebhookSigner::new(
            WebhookSignatureScheme::HmacSha256,
            &secret("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"),
            &[],
        )
        .expect("valid secret");

        let avro_framed = [0x00, 0x00, 0x00, 0x00, 0x01, 0xff, 0xfe, 0x80];
        assert!(
            String::from_utf8(avro_framed.to_vec()).is_err(),
            "precondition: this payload is not UTF-8"
        );
        let signature = signer.sign("msg_1", 1, &avro_framed);
        assert!(signature.starts_with("v1,"));
        assert_ne!(
            signature,
            signer.sign("msg_1", 1, String::from_utf8_lossy(&avro_framed).as_bytes()),
            "a lossy conversion must not produce the same signature as the real bytes"
        );
    }

    #[test]
    fn the_timestamp_is_part_of_the_signature() {
        let signer = WebhookSigner::new(
            WebhookSignatureScheme::HmacSha256,
            &secret("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"),
            &[],
        )
        .expect("valid secret");
        assert_ne!(
            signer.sign("msg_1", 1_614_265_330, b"body"),
            signer.sign("msg_1", 1_614_265_331, b"body"),
            "a retry one second later must re-sign, or replay protection is decorative"
        );
        assert_ne!(
            signer.sign("msg_1", 1, b"body"),
            signer.sign("msg_2", 1, b"body"),
            "the id is signed too"
        );
    }
}
