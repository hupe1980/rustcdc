//! Randomised property tests for the two parsers that take untrusted-shaped input:
//! `redact_secrets` and the configuration loader.
//!
//! # Why these exist, and why they are not only a `fuzz/` crate
//!
//! Round 5's adversarial suite (`tests/security_negative.rs`) probes *chosen* inputs: 16
//! attacks somebody thought of. It found what it was pointed at and, by construction,
//! nothing else. The residual gap it named was arbitrary input.
//!
//! `fuzz/` covers that properly, but `cargo fuzz` needs a nightly toolchain and unbounded
//! time, so it runs on demand rather than on every commit. A fuzz target CI never executes
//! protects nothing. These tests close that half: a fixed corpus of adversarial shapes
//! plus a deterministic PRNG, cheap enough to run on every build.
//!
//! # What is actually asserted
//!
//! Not "does not panic" — that is the weakest interesting property and the one a fuzzer
//! finds unaided. The load-bearing assertions are the two guarantees redaction actually
//! makes, stated separately because they have different scopes:
//!
//! * a value under a **secret-named key** is replaced entirely, at any depth; and
//! * a **URL carrying userinfo** loses it under *any* key, named sensitive or not.
//!
//! The second is the shape of a real past defect: `sink.http.url` was neither named like a secret nor
//! enumerated, and `/status` returned `user:pass@` to any read-scoped token. Keeping the
//! two apart matters — an earlier draft of this suite planted secrets under benign keys
//! and "failed", which was the test asserting a guarantee the code never offered.

use std::fmt::Write as _;

/// xorshift64*. Deterministic and dependency-free: a failure here must reproduce from the
/// printed seed, which a thread-seeded RNG would not give.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

/// The marker planted as every secret value. Chosen so no legitimate output could contain
/// it by chance, and so a partial redaction (prefix kept, suffix replaced) still trips.
const SENTINEL: &str = "S3CRET-c4nary-value";

const SECRET_KEY_NAMES: &[&str] = &[
    "password",
    "sasl_password",
    "bearer_token",
    "token",
    "credential",
    "api_key",
    "apikey",
    "private_key",
    "ssl_key_password",
    "ssl_keystore_password",
    "authorization",
    "PASSWORD",
    "Sasl_Password",
    "some_secret",
    "nested_token_field",
];

const BENIGN_KEY_NAMES: &[&str] = &[
    "topic", "brokers", "mode", "enabled", "count", "url", "headers", "name", "type", "dir",
];

/// Values that have historically caused trouble: URL userinfo, control characters,
/// unicode, and shapes that look like a scheme but are not.
fn adversarial_values() -> Vec<String> {
    vec![
        format!("postgres://user:{SENTINEL}@db.internal:5432/app"),
        format!("https://svc:{SENTINEL}@api.example.com/v1?x=1#frag"),
        format!("kafka+ssl://u:{SENTINEL}@broker:9093"),
        format!("not-a-url-just-an-@-sign-{SENTINEL}"),
        format!("mailto:someone@example.com {SENTINEL}"),
        format!("{SENTINEL}"),
        format!("  {SENTINEL}  "),
        format!("{SENTINEL}\n{SENTINEL}"),
        format!("://{SENTINEL}@host"),
        format!("a b://user:{SENTINEL}@host"),
        format!("ünïcøde-{SENTINEL}-🔑"),
        format!("{SENTINEL}\u{0000}embedded-nul"),
    ]
}

/// Values with no credential in them, for the benign keys. Deliberately includes an `@`
/// and an email so the URL rule's narrowness is exercised from the other side too.
const BENIGN_VALUES: &[&str] = &[
    "cdc-events",
    "broker-1:9092,broker-2:9092",
    "someone@example.com",
    "https://api.example.com/v1",
    "",
    "🔑",
];

/// Build a random JSON document.
///
/// `SENTINEL` is planted **only** under secret-named keys. That restriction is the
/// contract, not a convenience: redaction is key-driven, so a bare credential stored under
/// a key named `enabled` is not something it claims — or could — detect. An earlier
/// version of this generator planted sentinels under benign keys as well and "failed",
/// which was the test asserting a guarantee the code never offered. URL-shaped values are
/// the one value-driven rule, and they get their own property below.
fn random_secret_bearing_json(rng: &mut Rng, depth: usize) -> serde_json::Value {
    let mut object = serde_json::Map::new();

    for _ in 0..1 + rng.below(4) {
        let key = rng.pick(SECRET_KEY_NAMES).to_string();
        object.insert(
            key,
            serde_json::Value::String(rng.pick(&adversarial_values()).clone()),
        );
    }

    for _ in 0..rng.below(3) {
        let key = rng.pick(BENIGN_KEY_NAMES).to_string();
        let value = if depth == 0 {
            serde_json::Value::String(rng.pick(BENIGN_VALUES).to_string())
        } else {
            random_secret_bearing_json(rng, depth - 1)
        };
        object.insert(key, value);
    }

    serde_json::Value::Object(object)
}

/// **The property `/status` rests on.** Every value reachable under a secret-named key is
/// replaced, at any depth, under any key casing, for any of the adversarial value shapes.
///
/// A past credential leak was exactly this property failing for one field nobody had
/// listed. An
/// enumerated test passes for the fields it enumerates; this one fails for the next field
/// somebody adds without listing it.
#[test]
fn no_secret_under_a_secret_named_key_survives_redaction() {
    for seed in 1..400u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let document = random_secret_bearing_json(&mut rng, 3);
        let input = serde_json::to_string(&document).expect("serialise fixture");

        let redacted = rustcdc_server::redaction::redact_secrets(&input);

        assert!(
            !redacted.contains(SENTINEL),
            "seed {seed}: a planted secret survived redaction.\n\
             input:    {input}\n\
             redacted: {redacted}"
        );
    }
}

/// The one **value**-driven rule, checked as a property.
///
/// A URL carrying userinfo must lose it wherever it appears, including under keys nobody
/// listed as sensitive — that is precisely the past defect, where `sink.http.url` was
/// neither named like a secret nor enumerated, and `/status` returned `user:pass@` to any
/// read-scoped token.
#[test]
fn url_userinfo_is_stripped_under_any_key() {
    for seed in 1..300u64 {
        let mut rng = Rng(seed.wrapping_mul(0xC2B2_AE3D_27D4_EB4F) | 1);

        let mut object = serde_json::Map::new();
        for _ in 0..1 + rng.below(5) {
            // Deliberately a *benign* key: the rule must not depend on the name.
            let key = rng.pick(BENIGN_KEY_NAMES).to_string();
            let scheme = rng.pick(&["postgres", "https", "kafka+ssl", "mysql", "s3"]);
            object.insert(
                key,
                serde_json::Value::String(format!("{scheme}://user:{SENTINEL}@host:5432/db")),
            );
        }

        let input = serde_json::to_string(&serde_json::Value::Object(object)).expect("serialise");
        let redacted = rustcdc_server::redaction::redact_secrets(&input);

        assert!(
            !redacted.contains(SENTINEL),
            "seed {seed}: URL userinfo survived redaction under a non-sensitive key.\n\
             input:    {input}\n\
             redacted: {redacted}"
        );
    }
}

/// Redaction must be total: any input at all, valid JSON or not, returns rather than
/// panicking. `/status` calls this on a config snapshot, so a panic here is an admin-API
/// crash reachable by configuration.
#[test]
fn redaction_is_total_over_arbitrary_bytes() {
    let fragments = [
        "{",
        "}",
        "[",
        "]",
        ":",
        ",",
        "\"",
        "\\",
        "null",
        "true",
        "0",
        "-",
        "e",
        "\u{0}",
        "\u{7f}",
        "🔑",
        "\"a\":",
        "{\"a\":{\"b\":",
        "1e999999",
        "\"\\u",
        "\\ud800",
        &"[".repeat(64),
        &"{\"a\":".repeat(64),
    ];

    for seed in 1..600u64 {
        let mut rng = Rng(seed.wrapping_mul(0xD1B5_4A32_D192_ED03) | 1);
        let mut input = String::new();
        for _ in 0..rng.below(24) {
            let _ = write!(input, "{}", rng.pick(&fragments));
        }

        // The assertion is that this returns at all. A panic fails the test by unwinding.
        let output = rustcdc_server::redaction::redact_secrets(&input);

        // Valid JSON in must give valid JSON out, or a downstream consumer of `/status`
        // receives a body it cannot parse.
        if serde_json::from_str::<serde_json::Value>(&input).is_ok() {
            assert!(
                serde_json::from_str::<serde_json::Value>(&output).is_ok(),
                "seed {seed}: redaction turned valid JSON into invalid JSON.\n\
                 input:  {input}\n\
                 output: {output}"
            );
        }
    }
}

/// The config loader parses operator-supplied TOML. It is allowed to reject anything, but
/// it must reject rather than panic — a panic in a loader is a crash loop that no
/// `validate-config` run can pre-empt, because the crash is the validation.
#[test]
fn the_config_loader_rejects_rather_than_panics() {
    let fragments = [
        "[source]\n",
        "[sink]\n",
        "[runtime]\n",
        "[[sinks]]\n",
        "type = \"kafka\"\n",
        "type = \"\"\n",
        "brokers = \"\"\n",
        "max_event_bytes = -1\n",
        "max_event_bytes = 99999999999999999999\n",
        "linger_ms = 1e400\n",
        "password = \"literal\"\n",
        "url = \"postgres://a:b@c/d\"\n",
        "[[",
        "]]",
        "= 1\n",
        "\"\" = \"\"\n",
        "x = [1, 2,\n",
        "x = {a = }\n",
        "\u{0}\n",
        "🔑 = \"🔑\"\n",
        "a.b.c.d.e.f.g = 1\n",
    ];

    let dir = tempfile::tempdir().expect("tempdir");

    for seed in 1..400u64 {
        let mut rng = Rng(seed.wrapping_mul(0xA24B_AED4_963E_E407) | 1);
        let mut toml = String::new();
        for _ in 0..rng.below(16) {
            // Dereferenced explicitly: `pick` returns `&T`, and letting inference pick
            // `T` from `push_str`'s `&str` parameter resolves to the unsized `str` on
            // older compilers within the supported range.
            toml.push_str(*rng.pick(&fragments));
        }

        let path = dir.path().join(format!("fuzz-{seed}.toml"));
        std::fs::write(&path, &toml).expect("write fixture");

        // Accept or reject; both are correct. Panicking is not.
        let _ = rustcdc_server::config::load(&path);
    }
}
