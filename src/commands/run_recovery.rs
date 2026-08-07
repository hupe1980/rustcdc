use std::time::{SystemTime, UNIX_EPOCH};

pub(super) struct RecoveryPolicyConfig {
    pub(super) initial_backoff_ms: u64,
    pub(super) max_backoff_ms: u64,
    pub(super) backoff_multiplier: f64,
    pub(super) jitter_ratio: f64,
    pub(super) breaker_consecutive_threshold: u64,
    pub(super) breaker_max_open_cycles: u64,
    pub(super) breaker_cooldown_ms: u64,
    /// Consecutive clean-success polls required to reset `breaker_open_consecutive`.
    /// Prevents a single successful poll from clearing escalation state when
    /// the source is flapping.  Default should be >= 10.
    pub(super) breaker_clean_window_successes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RecoverableErrorSnapshot {
    pub(super) total: u64,
    pub(super) consecutive: u64,
    pub(super) backoff_ms: u64,
    pub(super) backoff_last_ms: u64,
    pub(super) breaker_open_total: u64,
    pub(super) breaker_open_consecutive: u64,
    /// Monotonic count of circuit-breaker open events across the full session.
    /// Never resets; used for long-running liveness monitoring.
    pub(super) lifetime_breaker_open_total: u64,
}

pub(super) enum RecoveryAction {
    Retry {
        consecutive: u64,
        backoff_base_ms: u64,
        backoff_ms: u64,
    },
    BreakerCooldown {
        consecutive: u64,
        breaker_open_consecutive: u64,
        cooldown_ms: u64,
    },
    Escalate {
        message: String,
    },
}

pub(super) struct RecoverableErrorState {
    total: u64,
    consecutive: u64,
    backoff_ms: u64,
    backoff_last_ms: u64,
    breaker_open_total: u64,
    breaker_open_consecutive: u64,
    /// Number of consecutive successful polls since the last recoverable error.
    /// Resets to 0 on any new error.  Used as the clean-window gate for
    /// `breaker_open_consecutive` resets so that a single successful poll does
    /// not mask persistent flapping.
    consecutive_successes: u64,
    /// Monotonic count of circuit-breaker open events.  Never resets.
    lifetime_breaker_open_total: u64,
}

impl RecoverableErrorState {
    pub(super) fn new(initial_backoff_ms: u64) -> Self {
        Self {
            total: 0,
            consecutive: 0,
            backoff_ms: initial_backoff_ms,
            backoff_last_ms: 0,
            breaker_open_total: 0,
            breaker_open_consecutive: 0,
            consecutive_successes: 0,
            lifetime_breaker_open_total: 0,
        }
    }

    pub(super) fn mark_success(&mut self, initial_backoff_ms: u64, policy: &RecoveryPolicyConfig) {
        self.backoff_ms = initial_backoff_ms;
        self.consecutive = 0;
        self.consecutive_successes = self.consecutive_successes.saturating_add(1);
        // Only reset the breaker escalation counter after a sustained clean window.
        // A single success must not clear the open-consecutive counter so that
        // a source alternating success/error can still escalate to terminal error.
        if self.consecutive_successes >= policy.breaker_clean_window_successes {
            self.breaker_open_consecutive = 0;
            self.consecutive_successes = 0;
        }
    }

    pub(super) fn snapshot(&self) -> RecoverableErrorSnapshot {
        RecoverableErrorSnapshot {
            total: self.total,
            consecutive: self.consecutive,
            backoff_ms: self.backoff_ms,
            backoff_last_ms: self.backoff_last_ms,
            breaker_open_total: self.breaker_open_total,
            breaker_open_consecutive: self.breaker_open_consecutive,
            lifetime_breaker_open_total: self.lifetime_breaker_open_total,
        }
    }

    pub(super) fn on_recoverable_error(
        &mut self,
        policy: &RecoveryPolicyConfig,
        seed: u64,
    ) -> RecoveryAction {
        self.total = self.total.saturating_add(1);
        self.consecutive = self.consecutive.saturating_add(1);
        // Any error resets the clean-success window so that breaker_open_consecutive
        // can only be cleared by a sustained run of consecutive successes.
        self.consecutive_successes = 0;

        let backoff_base_ms = self.backoff_ms;
        let backoff_with_jitter_ms =
            with_recoverable_error_jitter_ms(backoff_base_ms, policy.jitter_ratio, seed);
        self.backoff_last_ms = backoff_with_jitter_ms;

        self.backoff_ms = next_recoverable_error_backoff_ms(
            self.backoff_ms,
            policy.max_backoff_ms,
            policy.backoff_multiplier,
        );

        if self.consecutive >= policy.breaker_consecutive_threshold {
            self.breaker_open_total = self.breaker_open_total.saturating_add(1);
            self.breaker_open_consecutive = self.breaker_open_consecutive.saturating_add(1);
            self.lifetime_breaker_open_total = self.lifetime_breaker_open_total.saturating_add(1);

            if self.breaker_open_consecutive >= policy.breaker_max_open_cycles {
                return RecoveryAction::Escalate {
                    message: format!(
                        "recoverable error circuit-breaker opened {} consecutive times (max {}); escalating to terminal error",
                        self.breaker_open_consecutive,
                        policy.breaker_max_open_cycles
                    ),
                };
            }

            let action = RecoveryAction::BreakerCooldown {
                consecutive: self.consecutive,
                breaker_open_consecutive: self.breaker_open_consecutive,
                cooldown_ms: policy.breaker_cooldown_ms,
            };

            self.consecutive = 0;
            self.backoff_ms = policy.initial_backoff_ms;
            return action;
        }

        RecoveryAction::Retry {
            consecutive: self.consecutive,
            backoff_base_ms,
            backoff_ms: backoff_with_jitter_ms,
        }
    }
}

fn next_recoverable_error_backoff_ms(current_ms: u64, max_ms: u64, multiplier: f64) -> u64 {
    let next = ((current_ms as f64) * multiplier).round() as u64;
    next.min(max_ms).max(current_ms)
}

pub(super) fn with_recoverable_error_jitter_ms(base_ms: u64, jitter_ratio: f64, seed: u64) -> u64 {
    if base_ms <= 1 || jitter_ratio <= 0.0 {
        return base_ms;
    }

    let jitter_span = ((base_ms as f64) * jitter_ratio).round() as u64;
    if jitter_span == 0 {
        return base_ms;
    }

    let spread = jitter_span.saturating_mul(2).saturating_add(1);
    let offset = (seed % spread) as i64 - jitter_span as i64;
    (base_ms as i64).saturating_add(offset).max(1) as u64
}

/// Mix in a monotonic counter so the seed is non-zero even when the
/// system clock is behind UNIX_EPOCH (NTP step, container cold-start, etc.).
pub(super) fn jitter_seed() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    // Knuth multiplicative hash to spread low-entropy seeds.
    nanos
        .wrapping_add(seq)
        .wrapping_mul(6_364_136_223_846_793_005)
}
