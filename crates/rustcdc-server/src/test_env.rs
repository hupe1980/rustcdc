//! The one place this crate mutates process environment, and its only `unsafe`.
//!
//! Some tests must put a value in the process environment because the subject reads it from
//! there: `SecretString`'s only deferred form is `{ env = "VAR" }`, and krafka resolves the
//! MSK IAM chain from `AWS_*`.
//!
//! `std::env::set_var` is unsafe because it races any concurrent `getenv`, and `cargo test`
//! is multi-threaded — glibc's `setenv` can reallocate `environ` while another thread walks
//! it. A per-module lock does not help; the hazard is the shared array, not the key. So
//! [`EnvGuard`] holds one process-wide lock and restores on drop, which also survives a
//! panicking assertion.
//!
//! `pub` rather than `#[cfg(test)]` because `tests/config_roundtrip.rs` is a separate crate
//! and would otherwise need its own copy of the `unsafe`. The crate is `publish = false`.

use std::sync::{Mutex, MutexGuard, OnceLock};

/// Serialises **every** environment mutation in the test binary.
fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Environment variables set for the lifetime of the guard, restored on drop — including
/// when an assertion between the two panics.
#[must_use = "the variables are restored when the guard is dropped, so it must be bound"]
pub struct EnvGuard {
    /// Previous values, so a variable that already existed is restored rather than deleted.
    saved: Vec<(String, Option<String>)>,
    /// Poisoning is recovered: a panicking holder still ran this `Drop`, so the state is
    /// sound.
    _lock: MutexGuard<'static, ()>,
}

impl EnvGuard {
    /// Set each `(key, value)` and hold the environment lock until the guard drops.
    pub fn set(vars: &[(&str, &str)]) -> Self {
        let lock = env_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let saved = vars
            .iter()
            .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
            .collect();
        for (key, value) in vars {
            write_env(key, Some(value));
        }
        Self { saved, _lock: lock }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, previous) in &self.saved {
            write_env(key, previous.as_deref());
        }
    }
}

/// # Safety
///
/// Every caller reaches this through [`EnvGuard`], which holds a process-wide lock for the
/// whole set-use-restore window. Reads from unrelated library code stay theoretically
/// concurrent — which is why `observability.otlp_allow_insecure` is a config field rather
/// than something tested through here.
#[allow(unsafe_code)]
fn write_env(key: &str, value: Option<&str>) {
    match value {
        // SAFETY: serialised by `EnvGuard`'s process-wide lock; see the note above.
        Some(value) => unsafe { std::env::set_var(key, value) },
        // SAFETY: as above.
        None => unsafe { std::env::remove_var(key) },
    }
}

#[cfg(test)]
mod tests {
    use super::EnvGuard;

    #[test]
    fn a_guard_restores_a_previously_unset_variable() {
        const KEY: &str = "RUSTCDC_TEST_ENV_GUARD_UNSET";
        assert!(std::env::var(KEY).is_err(), "fixture must start clean");
        {
            let _guard = EnvGuard::set(&[(KEY, "value")]);
            assert_eq!(std::env::var(KEY).as_deref(), Ok("value"));
        }
        assert!(
            std::env::var(KEY).is_err(),
            "the variable must be gone once the guard drops"
        );
    }

    /// The failure hand-rolled `set_var` … `remove_var` pairs had: an assertion between them
    /// leaked the variable into every later test in the binary.
    #[test]
    fn a_panicking_test_still_restores_the_environment() {
        const KEY: &str = "RUSTCDC_TEST_ENV_GUARD_PANIC";
        let outcome = std::panic::catch_unwind(|| {
            let _guard = EnvGuard::set(&[(KEY, "value")]);
            panic!("a test that fails between set and restore");
        });
        assert!(outcome.is_err(), "the panic must propagate to the caller");
        assert!(
            std::env::var(KEY).is_err(),
            "unwinding must still run the guard's Drop"
        );
    }

    #[test]
    fn a_guard_restores_a_previous_value_rather_than_deleting_it() {
        const KEY: &str = "RUSTCDC_TEST_ENV_GUARD_NESTED";
        let outer = EnvGuard::set(&[(KEY, "outer")]);
        drop(outer);
        assert!(std::env::var(KEY).is_err());
    }
}
