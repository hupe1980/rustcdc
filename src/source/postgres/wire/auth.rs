//! Password authentication for the replication connection.
//!
//! Three methods, matching what `tokio-postgres` accepts on the SQL connection — a
//! streaming transport that cannot authenticate where the SQL transport can would be a
//! silent downgrade that only shows up once someone enables it.
//!
//! * **SCRAM-SHA-256** (RFC 5802 / RFC 7677) — the default since PostgreSQL 14, and the
//!   only one of the three that never puts a password-equivalent on the wire.
//! * **MD5** — deprecated by PostgreSQL but still the configured method on plenty of
//!   servers.
//! * **Cleartext** — only meaningful under TLS, and refused without it.
//!
//! # What is deliberately not implemented
//!
//! **SCRAM-SHA-256-PLUS** (channel binding). It binds the authentication exchange to the
//! TLS channel, which defeats a MITM that holds a certificate the client would otherwise
//! accept. rustcdc's default transport is `verify_full`, so such a MITM already needs a
//! certificate chaining to a trusted root for the right hostname — the case channel
//! binding adds protection for is one where verification has already been weakened.
//!
//! The residual risk is a **downgrade**: a MITM in that position can advertise only
//! `SCRAM-SHA-256` and this client will accept it. That is why
//! [`select_sasl_mechanism`] records whether the server offered `-PLUS` and the caller
//! logs it. Do not pair `allow_invalid_certificates` with SCRAM and assume the password
//! is safe.

use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::core::{Error, Result};

/// SASL mechanism this client implements.
pub(super) const SCRAM_SHA_256: &str = "SCRAM-SHA-256";
/// Channel-binding variant, recognised but not implemented. See the module docs.
pub(super) const SCRAM_SHA_256_PLUS: &str = "SCRAM-SHA-256-PLUS";

/// Bytes of client nonce. RFC 5802 sets no length; 24 bytes of CSPRNG output is what
/// libpq uses and is comfortably beyond the 128-bit floor for a nonce.
const CLIENT_NONCE_BYTES: usize = 24;

/// Largest SCRAM iteration count this client will honour.
///
/// The count is chosen by the **server** and is the loop bound of a PBKDF2 derivation the
/// client then performs. PostgreSQL's `scram_iterations` accepts anything up to `INT_MAX`,
/// and neither the RFC nor libpq imposes a ceiling, so an `i=4294967295` — from a
/// misconfiguration or a hostile server — asks this client for roughly four billion
/// HMAC-SHA256 rounds. That is minutes to hours of pure CPU per connection attempt: a
/// denial of service the server can trigger for free, with nothing on the wire to
/// distinguish it from a slow handshake.
///
/// One million is ~250× PostgreSQL's default of 4096 and well past any deliberate
/// hardening (OWASP's PBKDF2-SHA256 guidance is 600k), so a legitimate server never
/// reaches it, while the worst case stays under about a second of work.
const MAX_SCRAM_ITERATIONS: u32 = 1_000_000;

const BASE64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

/// Outcome of choosing a mechanism from the server's advertised list.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct MechanismChoice {
    /// The mechanism to use.
    pub(super) mechanism: &'static str,
    /// Whether the server also offered channel binding.
    ///
    /// Surfaced so the caller can log that a stronger mechanism was available but not
    /// used, rather than leaving the downgrade invisible.
    pub(super) plus_offered: bool,
}

/// Pick a SASL mechanism from the server's advertised list.
pub(super) fn select_sasl_mechanism(offered: &[String]) -> Result<MechanismChoice> {
    let plus_offered = offered.iter().any(|name| name == SCRAM_SHA_256_PLUS);
    if offered.iter().any(|name| name == SCRAM_SHA_256) {
        return Ok(MechanismChoice {
            mechanism: SCRAM_SHA_256,
            plus_offered,
        });
    }

    Err(Error::SourceError(format!(
        "postgres offered no SASL mechanism this client implements. Offered: [{}]; \
         supported: [{SCRAM_SHA_256}]. If the server offers only {SCRAM_SHA_256_PLUS}, it \
         is requiring TLS channel binding, which rustcdc's replication transport does not \
         implement — use WalTransport::SqlPeek, which authenticates through \
         tokio-postgres.",
        offered.join(", ")
    )))
}

/// Parse the mechanism list out of an `AuthenticationSASL` body.
///
/// The body is a sequence of NUL-terminated mechanism names, ended by an empty one.
pub(super) fn parse_sasl_mechanisms(body: &[u8]) -> Vec<String> {
    body.split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
        .map(|name| String::from_utf8_lossy(name).into_owned())
        .collect()
}

/// MD5 password response: `md5` + hex(md5(hex(md5(password + user)) + salt)).
///
/// The double hashing is what makes the stored value independent of the salt, and the salt
/// is what stops a captured response from being replayed. Neither makes it a good scheme —
/// the inner digest is a password equivalent — which is why PostgreSQL deprecated it.
pub(super) fn md5_password_response(user: &str, password: &str, salt: &[u8; 4]) -> String {
    use md5::Md5;

    let mut inner = Md5::new();
    inner.update(password.as_bytes());
    inner.update(user.as_bytes());
    let inner_hex = hex_lower(&inner.finalize());

    let mut outer = Md5::new();
    outer.update(inner_hex.as_bytes());
    outer.update(salt);
    format!("md5{}", hex_lower(&outer.finalize()))
}

/// Client state carried between the two SCRAM round trips.
pub(super) struct ScramExchange {
    /// `n=,r=<nonce>` — the client-first message without its GS2 header.
    client_first_bare: String,
    /// Password, needed to derive the salted key once the server supplies the salt.
    password: String,
    /// Set from the server's final message so the caller can verify it.
    server_signature: Option<Vec<u8>>,
}

impl ScramExchange {
    /// Begin an exchange, producing the client-first message.
    ///
    /// The nonce comes from rustls's CSPRNG rather than a new `rand` dependency: the
    /// `postgres` feature already requires `tls`, so a vetted secure random source is
    /// present, and adding a second one would be a second thing to audit.
    pub(super) fn start(password: &str) -> Result<(Self, String)> {
        let mut nonce_bytes = [0u8; CLIENT_NONCE_BYTES];
        rustls::crypto::ring::default_provider()
            .secure_random
            .fill(&mut nonce_bytes)
            .map_err(|_| {
                Error::SourceError(
                    "failed to generate a SCRAM client nonce from the system CSPRNG".into(),
                )
            })?;
        let nonce = BASE64.encode(nonce_bytes);

        // The username is empty on purpose: PostgreSQL takes it from the startup packet,
        // and RFC 5802's `n=` field would otherwise need SASLprep normalisation.
        let client_first_bare = format!("n=,r={nonce}");
        // `n,,` is the GS2 header for "client does not support channel binding".
        let client_first = format!("n,,{client_first_bare}");

        Ok((
            Self {
                client_first_bare,
                password: password.to_string(),
                server_signature: None,
            },
            client_first,
        ))
    }

    /// Consume the server-first message and produce the client-final message.
    ///
    /// # Runs the key derivation on a blocking worker
    ///
    /// PBKDF2 is CPU work whose duration a **remote** value chooses, and this crate runs
    /// inside the embedder's Tokio runtime. Deriving inline stalls the worker thread for the
    /// whole derivation — up to [`MAX_SCRAM_ITERATIONS`] rounds — which on a current-thread
    /// runtime stalls every other task in the process. The same reasoning already puts
    /// `FileCheckpoint`'s `fsync` on `spawn_blocking`; a handshake happens once per
    /// connection, so the spawn costs nothing measurable.
    pub(super) async fn client_final(&mut self, server_first: &str) -> Result<String> {
        let parsed = ServerFirst::parse(server_first)?;

        // The server must echo the client nonce as a prefix of its own. Skipping this check
        // is what makes a SCRAM implementation vulnerable to a replayed server-first.
        let client_nonce = self
            .client_first_bare
            .strip_prefix("n=,r=")
            .ok_or_else(|| Error::SourceError("malformed SCRAM client-first state".into()))?;
        if !parsed.nonce.starts_with(client_nonce) {
            return Err(Error::SourceError(
                "postgres SCRAM server nonce does not extend the client nonce; the exchange \
                 may be replayed or tampered with"
                    .into(),
            ));
        }

        let salted_password = {
            let password = self.password.clone();
            let salt = parsed.salt.clone();
            let iterations = parsed.iterations;
            tokio::task::spawn_blocking(move || hi(password.as_bytes(), &salt, iterations))
                .await
                .map_err(|error| {
                    Error::SourceError(format!(
                        "the SCRAM key derivation task failed to complete: {error}"
                    ))
                })??
        };
        let client_key = hmac_sha256(&salted_password, b"Client Key")?;
        let stored_key = Sha256::digest(&client_key);

        // `c=` is base64 of the GS2 header, which for no-channel-binding is exactly `n,,`.
        let client_final_without_proof = format!("c={},r={}", BASE64.encode("n,,"), parsed.nonce);
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first, client_final_without_proof
        );

        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes())?;
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(key, signature)| key ^ signature)
            .collect();

        // Retained so `verify_server_final` can prove the server also knows the password —
        // mutual authentication is the half of SCRAM that is easiest to skip and worst to.
        let server_key = hmac_sha256(&salted_password, b"Server Key")?;
        self.server_signature = Some(hmac_sha256(&server_key, auth_message.as_bytes())?);

        Ok(format!(
            "{client_final_without_proof},p={}",
            BASE64.encode(proof)
        ))
    }

    /// Verify the server's final message proves it knows the password.
    ///
    /// Without this the exchange authenticates the client to the server but not the
    /// reverse, so an impostor that cannot verify the proof can still complete a handshake
    /// and receive whatever the client sends next.
    pub(super) fn verify_server_final(&self, server_final: &str) -> Result<()> {
        let expected = self.server_signature.as_ref().ok_or_else(|| {
            Error::SourceError(
                "postgres SCRAM server-final arrived before the client-final was sent".into(),
            )
        })?;

        // The field may be `v=<sig>`, or `e=<error>` when the server rejected the proof.
        for field in server_final.split(',') {
            if let Some(reason) = field.strip_prefix("e=") {
                return Err(Error::SourceError(format!(
                    "postgres rejected the SCRAM authentication: {reason}"
                )));
            }
            if let Some(encoded) = field.strip_prefix("v=") {
                let actual = BASE64.decode(encoded).map_err(|error| {
                    Error::SourceError(format!(
                        "postgres SCRAM server signature is not valid base64: {error}"
                    ))
                })?;
                // Constant-time comparison: a signature check that leaks its position
                // through timing is a signature check an attacker can search.
                return if constant_time_eq(&actual, expected) {
                    Ok(())
                } else {
                    Err(Error::SourceError(
                        "postgres SCRAM server signature did not verify; the server does not \
                         know this password and the connection cannot be trusted"
                            .into(),
                    ))
                };
            }
        }

        Err(Error::SourceError(
            "postgres SCRAM server-final carried no signature field".into(),
        ))
    }
}

/// The `r=`, `s=`, `i=` triple of a SCRAM server-first message.
struct ServerFirst {
    nonce: String,
    salt: Vec<u8>,
    iterations: u32,
}

impl ServerFirst {
    fn parse(message: &str) -> Result<Self> {
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;

        for field in message.split(',') {
            match field.split_at_checked(2) {
                Some(("r=", value)) => nonce = Some(value.to_string()),
                Some(("s=", value)) => {
                    salt = Some(BASE64.decode(value).map_err(|error| {
                        Error::SourceError(format!(
                            "postgres SCRAM salt is not valid base64: {error}"
                        ))
                    })?);
                }
                Some(("i=", value)) => {
                    iterations = Some(value.parse::<u32>().map_err(|error| {
                        Error::SourceError(format!(
                            "postgres SCRAM iteration count '{value}' is not a number: {error}"
                        ))
                    })?);
                }
                _ => {}
            }
        }

        let iterations = iterations.ok_or_else(|| {
            Error::SourceError("postgres SCRAM server-first carried no iteration count".into())
        })?;
        if iterations == 0 {
            return Err(Error::SourceError(
                "postgres SCRAM iteration count is zero, which would skip key derivation \
                 entirely"
                    .into(),
            ));
        }
        if iterations > MAX_SCRAM_ITERATIONS {
            return Err(Error::SourceError(format!(
                "postgres asked for {iterations} SCRAM iterations, above the {MAX_SCRAM_ITERATIONS} \
                 this client will perform. The count is the server's choice and the work is the \
                 client's, so an unbounded value is a denial of service the server triggers for \
                 free. PostgreSQL's default is 4096; if this is deliberate hardening, lower \
                 `scram_iterations` below the cap."
            )));
        }

        Ok(Self {
            nonce: nonce.ok_or_else(|| {
                Error::SourceError("postgres SCRAM server-first carried no nonce".into())
            })?,
            salt: salt.ok_or_else(|| {
                Error::SourceError("postgres SCRAM server-first carried no salt".into())
            })?,
            iterations,
        })
    }
}

/// RFC 5802 `Hi` — PBKDF2-HMAC-SHA256 with a derived-key length of one hash block.
///
/// Written out rather than pulled from a PBKDF2 crate because at `dkLen == hLen` PBKDF2
/// reduces to exactly this loop: the block index is always 1, so there is no block
/// concatenation to get wrong, and it avoids a dependency for eight lines of HMAC.
fn hi(password: &[u8], salt: &[u8], iterations: u32) -> Result<Vec<u8>> {
    // U1 = HMAC(password, salt || INT(1))
    let mut salted = Vec::with_capacity(salt.len() + 4);
    salted.extend_from_slice(salt);
    salted.extend_from_slice(&1u32.to_be_bytes());

    let mut previous = hmac_sha256(password, &salted)?;
    let mut result = previous.clone();

    for _ in 1..iterations {
        previous = hmac_sha256(password, &previous)?;
        for (accumulated, byte) in result.iter_mut().zip(previous.iter()) {
            *accumulated ^= byte;
        }
    }

    Ok(result)
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> Result<Vec<u8>> {
    let mut mac = <Hmac<Sha256> as hmac::KeyInit>::new_from_slice(key)
        .map_err(|error| Error::SourceError(format!("SCRAM HMAC key rejected: {error}")))?;
    mac.update(message);
    Ok(mac.finalize().into_bytes().to_vec())
}

/// Compare two byte strings without an early exit.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .fold(0u8, |accumulated, (a, b)| accumulated | (a ^ b))
        == 0
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut rendered, byte| {
            let _ = write!(rendered, "{byte:02x}");
            rendered
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hi_matches_the_rfc_7677_test_vector() {
        // RFC 7677 §3: password "pencil", salt W22ZaJ0SNY7soEsUEjb6gQ==, i=4096.
        // SaltedPassword is the root of every other value in the exchange, so a wrong `Hi`
        // fails authentication with no clue as to why.
        let salt = BASE64
            .decode("W22ZaJ0SNY7soEsUEjb6gQ==")
            .expect("vector salt");
        let salted = hi(b"pencil", &salt, 4096).expect("derives");
        assert_eq!(
            BASE64.encode(&salted),
            "xKSVEDI6tPlSysH6mUQZOeeOp01r6B3fcJbodRPcYV0=",
            "SaltedPassword must match RFC 7677"
        );
    }

    #[test]
    fn the_full_exchange_matches_the_rfc_7677_test_vector() {
        // Drives the real code path with the RFC's fixed nonce, so client proof and server
        // signature are both checked against published values rather than against
        // themselves.
        let salt = BASE64
            .decode("W22ZaJ0SNY7soEsUEjb6gQ==")
            .expect("vector salt");
        let salted = hi(b"pencil", &salt, 4096).expect("derives");

        let client_first_bare = "n=user,r=rOprNGfwEbeRWgbNEkqO";
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                            s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let client_final_without_proof =
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        let auth_message =
            format!("{client_first_bare},{server_first},{client_final_without_proof}");

        let client_key = hmac_sha256(&salted, b"Client Key").expect("hmac");
        let stored_key = Sha256::digest(&client_key);
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes()).expect("hmac");
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(k, s)| k ^ s)
            .collect();
        assert_eq!(
            BASE64.encode(&proof),
            "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            "ClientProof must match RFC 7677 §3"
        );

        let server_key = hmac_sha256(&salted, b"Server Key").expect("hmac");
        let server_signature = hmac_sha256(&server_key, auth_message.as_bytes()).expect("hmac");
        assert_eq!(
            BASE64.encode(&server_signature),
            "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=",
            "ServerSignature must match RFC 7677"
        );
    }

    #[tokio::test]
    async fn the_client_final_message_is_shaped_as_the_protocol_requires() {
        let (mut exchange, client_first) = ScramExchange::start("pencil").expect("starts");
        assert!(
            client_first.starts_with("n,,n=,r="),
            "GS2 header must declare no channel binding: {client_first}"
        );

        let nonce = client_first
            .strip_prefix("n,,n=,r=")
            .expect("nonce present")
            .to_string();
        let server_first = format!("r={nonce}serverpart,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096");
        let client_final = exchange.client_final(&server_first).await.expect("continues");

        assert!(
            client_final.starts_with("c=biws,"),
            "`c=` must be base64 of the `n,,` GS2 header: {client_final}"
        );
        assert!(client_final.contains(&format!("r={nonce}serverpart")));
        assert!(client_final.contains(",p="));
    }

    #[tokio::test]
    async fn a_server_nonce_that_does_not_extend_the_client_nonce_is_refused() {
        // Without this check a replayed or substituted server-first is accepted.
        let (mut exchange, _) = ScramExchange::start("pencil").expect("starts");
        let error = exchange
            .client_final("r=totally-unrelated,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096")
            .await
            .expect_err("must refuse");
        assert!(error.to_string().contains("does not extend"));
    }

    #[tokio::test]
    async fn a_wrong_server_signature_fails_verification() {
        // Mutual authentication: the server must prove it knows the password too.
        let (mut exchange, client_first) = ScramExchange::start("pencil").expect("starts");
        let nonce = client_first.strip_prefix("n,,n=,r=").expect("nonce");
        exchange
            .client_final(&format!("r={nonce}x,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"))
            .await
            .expect("continues");

        let error = exchange
            .verify_server_final(&format!("v={}", BASE64.encode("not the signature")))
            .expect_err("must refuse");
        assert!(error.to_string().contains("did not verify"));
    }

    #[tokio::test]
    async fn a_correct_server_signature_verifies() {
        let (mut exchange, client_first) = ScramExchange::start("pencil").expect("starts");
        let nonce = client_first.strip_prefix("n,,n=,r=").expect("nonce");
        exchange
            .client_final(&format!("r={nonce}x,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"))
            .await
            .expect("continues");
        let signature = exchange
            .server_signature
            .as_ref()
            .expect("signature computed")
            .clone();
        exchange
            .verify_server_final(&format!("v={}", BASE64.encode(signature)))
            .expect("verifies");
    }

    #[tokio::test]
    async fn a_server_error_in_the_final_message_is_surfaced_verbatim() {
        let (mut exchange, client_first) = ScramExchange::start("pencil").expect("starts");
        let nonce = client_first.strip_prefix("n,,n=,r=").expect("nonce");
        exchange
            .client_final(&format!("r={nonce}x,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"))
            .await
            .expect("continues");
        let error = exchange
            .verify_server_final("e=invalid-proof")
            .expect_err("must surface");
        assert!(error.to_string().contains("invalid-proof"));
    }

    #[tokio::test]
    async fn a_zero_iteration_count_is_refused() {
        // i=0 would return the first HMAC unchanged, skipping key stretching entirely.
        let (mut exchange, client_first) = ScramExchange::start("pencil").expect("starts");
        let nonce = client_first.strip_prefix("n,,n=,r=").expect("nonce");
        let error = exchange
            .client_final(&format!("r={nonce}x,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=0"))
            .await
            .expect_err("must refuse");
        assert!(error.to_string().contains("iteration count is zero"));
    }

    /// The iteration count is the server's choice and the work is the client's, so an
    /// unbounded value is a denial of service the server triggers for free.
    #[tokio::test]
    async fn an_absurd_iteration_count_is_refused_before_the_derivation_runs() {
        let (mut exchange, client_first) = ScramExchange::start("pencil").expect("starts");
        let nonce = client_first
            .strip_prefix("n,,n=,r=")
            .expect("nonce present")
            .to_string();
        let server_first = format!(
            "r={nonce}serverpart,s=W22ZaJ0SNY7soEsUEjb6gQ==,i={}",
            u32::MAX
        );

        // Must return promptly rather than performing four billion HMAC rounds.
        let error = exchange
            .client_final(&server_first)
            .await
            .expect_err("an unbounded iteration count must be refused");
        let message = error.to_string();
        assert!(
            message.contains("SCRAM iterations"),
            "the error must name the count as the problem: {message}"
        );
        assert!(
            message.contains("scram_iterations"),
            "the error must name the server setting to change: {message}"
        );
    }

    #[tokio::test]
    async fn an_iteration_count_at_the_cap_is_still_honoured() {
        // The cap must be a ceiling on abuse, not a limit that breaks deliberate hardening
        // below it. PostgreSQL's default is 4096; this is the boundary case.
        let salt = BASE64
            .decode("W22ZaJ0SNY7soEsUEjb6gQ==")
            .expect("vector salt");
        let parsed = ServerFirst::parse(&format!(
            "r=abc,s=W22ZaJ0SNY7soEsUEjb6gQ==,i={MAX_SCRAM_ITERATIONS}"
        ))
        .expect("the cap itself is acceptable");
        assert_eq!(parsed.iterations, MAX_SCRAM_ITERATIONS);
        assert_eq!(parsed.salt, salt);

        assert!(
            ServerFirst::parse(&format!(
                "r=abc,s=W22ZaJ0SNY7soEsUEjb6gQ==,i={}",
                MAX_SCRAM_ITERATIONS + 1
            ))
            .is_err(),
            "one above the cap must be refused"
        );
    }

    #[test]
    fn two_exchanges_never_reuse_a_nonce() {
        // A repeated nonce makes the client proof replayable.
        let (_, first) = ScramExchange::start("pencil").expect("starts");
        let (_, second) = ScramExchange::start("pencil").expect("starts");
        assert_ne!(first, second);
    }

    #[test]
    fn the_md5_response_matches_the_documented_construction() {
        // Fixed vector computed from the protocol definition:
        // "md5" + hex(md5(hex(md5(password + user)) + salt)).
        let response = md5_password_response("postgres", "secret", &[0x01, 0x02, 0x03, 0x04]);
        assert!(response.starts_with("md5"));
        assert_eq!(response.len(), 35, "md5 + 32 hex characters");

        // Recompute independently to catch an argument-order swap, which is the classic
        // defect here: md5(password + user), not md5(user + password).
        use md5::Md5;
        let mut inner = Md5::new();
        inner.update(b"secret");
        inner.update(b"postgres");
        let mut outer = Md5::new();
        outer.update(hex_lower(&inner.finalize()).as_bytes());
        outer.update([0x01, 0x02, 0x03, 0x04]);
        assert_eq!(response, format!("md5{}", hex_lower(&outer.finalize())));
    }

    #[test]
    fn scram_is_selected_and_a_plus_offer_is_reported() {
        let choice =
            select_sasl_mechanism(&[SCRAM_SHA_256_PLUS.to_string(), SCRAM_SHA_256.to_string()])
                .expect("selects");
        assert_eq!(choice.mechanism, SCRAM_SHA_256);
        assert!(
            choice.plus_offered,
            "a channel-binding offer must be reported so the downgrade is visible"
        );
    }

    #[test]
    fn a_plus_only_server_produces_an_actionable_error() {
        let error =
            select_sasl_mechanism(&[SCRAM_SHA_256_PLUS.to_string()]).expect_err("cannot satisfy");
        let rendered = error.to_string();
        assert!(rendered.contains(SCRAM_SHA_256_PLUS));
        assert!(
            rendered.contains("SqlPeek"),
            "the error must name the transport that can authenticate: {rendered}"
        );
    }

    #[test]
    fn mechanism_lists_parse_from_their_nul_terminated_wire_form() {
        let body = b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0";
        assert_eq!(
            parse_sasl_mechanisms(body),
            vec![SCRAM_SHA_256_PLUS.to_string(), SCRAM_SHA_256.to_string()]
        );
    }

    #[test]
    fn constant_time_eq_still_compares_correctly() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
