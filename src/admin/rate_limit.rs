use axum::http::HeaderMap;
use dashmap::DashMap;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use crate::config::schema::AdminConfig;

/// Maximum distinct client keys tracked before LRU eviction.
pub(super) const RATE_LIMIT_MAX_CLIENT_KEYS: usize = 4096;
/// How long a client entry may be idle before being swept.
pub(super) const RATE_LIMIT_STALE_CLIENT_TTL: Duration = Duration::from_secs(600);

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub(super) enum AbuseLimitScope {
    Readyz,
    Status,
    Metrics,
}

#[derive(Debug)]
pub(super) struct AdminAbuseGuard {
    readyz_limiter: EndpointRateLimiter,
    status_limiter: EndpointRateLimiter,
    metrics_limiter: EndpointRateLimiter,
    trusted_proxy_ips: HashSet<IpAddr>,
}

#[derive(Debug)]
pub(super) struct EndpointRateLimiter {
    tokens_per_second: f64,
    burst_capacity: f64,
    clients: DashMap<String, ClientTokenBucket>,
    /// Epoch-ms of the last stale-entry TTL sweep.  Stored as an atomic so
    /// concurrent `allow()` callers can cooperatively throttle the O(n)
    /// `retain()` scan to at most once per 1000 ms.
    last_stale_eviction_epoch_ms: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Clone, Copy)]
struct ClientTokenBucket {
    tokens: f64,
    last_refill: Instant,
    last_seen: Instant,
}

// ─────────────────────────────────────────────────────────────────────────────
// Implementations
// ─────────────────────────────────────────────────────────────────────────────

impl AdminAbuseGuard {
    pub(super) fn new(config: &AdminConfig) -> Self {
        Self {
            readyz_limiter: EndpointRateLimiter::new(
                config.readyz_rate_limit_rps,
                config.readyz_rate_limit_burst,
            ),
            status_limiter: EndpointRateLimiter::new(
                config.status_rate_limit_rps,
                config.status_rate_limit_burst,
            ),
            metrics_limiter: EndpointRateLimiter::new(
                config.metrics_rate_limit_rps,
                config.metrics_rate_limit_burst,
            ),
            trusted_proxy_ips: config
                .trusted_proxy_ips
                .iter()
                .filter_map(|entry| entry.parse::<IpAddr>().ok())
                .collect(),
        }
    }

    pub(super) fn allow(
        &self,
        scope: AbuseLimitScope,
        headers: &HeaderMap,
        peer_addr: Option<SocketAddr>,
    ) -> (bool, Duration) {
        let started = Instant::now();
        let client_key = self.client_key(headers, peer_addr);
        let allowed = match scope {
            AbuseLimitScope::Readyz => self.readyz_limiter.allow(&client_key),
            AbuseLimitScope::Status => self.status_limiter.allow(&client_key),
            AbuseLimitScope::Metrics => self.metrics_limiter.allow(&client_key),
        };

        (allowed, started.elapsed())
    }

    /// Evict entries that have been idle longer than `RATE_LIMIT_STALE_CLIENT_TTL`.
    /// Called by the background sweep thread approximately every
    /// `RATE_LIMIT_STALE_CLIENT_TTL / 2`.
    pub(super) fn sweep_stale_entries(&self) {
        let now = Instant::now();
        let retain = |_: &String, bucket: &mut ClientTokenBucket| {
            now.duration_since(bucket.last_seen) < RATE_LIMIT_STALE_CLIENT_TTL
        };
        self.readyz_limiter.clients.retain(retain);
        self.status_limiter.clients.retain(retain);
        self.metrics_limiter.clients.retain(retain);
    }

    pub(super) fn client_key(&self, headers: &HeaderMap, peer_addr: Option<SocketAddr>) -> String {
        let Some(peer_addr) = peer_addr else {
            return "peer:unknown".to_string();
        };

        let peer_ip = peer_addr.ip();
        if !self.trusted_proxy_ips.contains(&peer_ip) {
            return format!("peer:{peer_ip}");
        }

        if let Some(forwarded) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
            for candidate in forwarded.split(',').map(str::trim) {
                if let Ok(ip) = candidate.parse::<IpAddr>() {
                    // Only accept globally-routable unicast addresses from
                    // X-Forwarded-For.  Loopback, link-local, private, and
                    // unspecified addresses can be spoofed by an attacker
                    // behind a trusted proxy to exhaust unrelated rate-limit
                    // buckets or claim a "trusted" identity (CR-010).
                    if is_globally_routable(ip) {
                        return format!("xff:{ip}");
                    }
                }
            }
        }

        if let Some(real_ip) = headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
            if let Ok(ip) = real_ip.trim().parse::<IpAddr>() {
                if is_globally_routable(ip) {
                    return format!("xri:{ip}");
                }
            }
        }

        format!("proxy:{peer_ip}")
    }
}

/// Returns `true` when `ip` is a globally-routable unicast address that can
/// safely serve as a per-client rate-limit key extracted from a proxy header.
///
/// Rejects: loopback, unspecified, link-local, private (RFC 1918 / RFC 4193),
/// documentation ranges, and IPv4-in-IPv6 mapped addresses.
fn is_globally_routable(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            !v4.is_loopback()
                && !v4.is_unspecified()
                && !v4.is_link_local()
                && !v4.is_private()
                && !v4.is_broadcast()
                && !v4.is_documentation()
                && !v4.is_multicast()
        }
        IpAddr::V6(v6) => {
            !v6.is_loopback()
                && !v6.is_unspecified()
                && !v6.is_multicast()
                // Link-local: fe80::/10
                && (v6.segments()[0] & 0xffc0) != 0xfe80
                // Unique local: fc00::/7 (RFC 4193)
                && (v6.segments()[0] & 0xfe00) != 0xfc00
                // IPv4-mapped: ::ffff:0:0/96
                && !v6.is_loopback()
                && v6.to_ipv4_mapped().is_none_or(|v4| {
                    !v4.is_private() && !v4.is_loopback() && !v4.is_link_local()
                })
        }
    }
}

impl EndpointRateLimiter {
    pub(super) fn new(tokens_per_second: u32, burst_capacity: u32) -> Self {
        Self {
            tokens_per_second: f64::from(tokens_per_second),
            burst_capacity: f64::from(burst_capacity),
            clients: DashMap::new(),
            last_stale_eviction_epoch_ms: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub(super) fn allow(&self, client_key: &str) -> bool {
        let now = Instant::now();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        if self.clients.len() >= RATE_LIMIT_MAX_CLIENT_KEYS
            && !self.clients.contains_key(client_key)
        {
            // Rate-limit the O(n) retain() sweep to at most once per 1000 ms so a
            // sustained flood of distinct IPs cannot DoS on the scan itself.
            let last = self
                .last_stale_eviction_epoch_ms
                .load(std::sync::atomic::Ordering::Relaxed);
            if now_ms.saturating_sub(last) >= 1000 {
                self.last_stale_eviction_epoch_ms
                    .store(now_ms, std::sync::atomic::Ordering::Relaxed);
                self.clients.retain(|_, bucket| {
                    now.duration_since(bucket.last_seen) < RATE_LIMIT_STALE_CLIENT_TTL
                });
            }

            // If still at capacity after the TTL sweep, evict the oldest 25 % of
            // entries (by `last_seen`) rather than a single arbitrary one.  This
            // prevents an attacker from being immediately admitted after eviction.
            if self.clients.len() >= RATE_LIMIT_MAX_CLIENT_KEYS {
                let evict_count = RATE_LIMIT_MAX_CLIENT_KEYS / 4; // 1024

                let mut entries: Vec<(String, Instant)> = self
                    .clients
                    .iter()
                    .map(|e| (e.key().clone(), e.value().last_seen))
                    .collect();
                entries.sort_unstable_by_key(|(_, last_seen)| *last_seen);

                for (key, _) in entries.into_iter().take(evict_count) {
                    self.clients.remove(&key);
                }
            }

            // If still full (e.g. all entries are brand-new), drop this request
            // to protect memory bounds.
            if self.clients.len() >= RATE_LIMIT_MAX_CLIENT_KEYS {
                return false;
            }
        }

        // New clients start with a single token, not full burst, to prevent
        // burst amplification by rapid IP rotation under load.
        let mut bucket = self
            .clients
            .entry(client_key.to_string())
            .or_insert(ClientTokenBucket {
                tokens: 1.0_f64.min(self.burst_capacity),
                last_refill: now,
                last_seen: now,
            });

        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.tokens_per_second).min(self.burst_capacity);
        bucket.last_refill = now;
        bucket.last_seen = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_guard(rps: u32, burst: u32) -> AdminAbuseGuard {
        AdminAbuseGuard {
            readyz_limiter: EndpointRateLimiter::new(rps, burst),
            status_limiter: EndpointRateLimiter::new(rps, burst),
            metrics_limiter: EndpointRateLimiter::new(rps, burst),
            trusted_proxy_ips: HashSet::new(),
        }
    }

    #[test]
    fn burst_allows_initial_requests() {
        let guard = make_guard(1, 5);
        // New clients start with exactly 1 token (not full burst) to prevent
        // burst amplification via rapid IP rotation.
        assert!(guard.readyz_limiter.allow("client1"));
        // Immediate second request (no time elapsed) exceeds the single initial token.
        assert!(!guard.readyz_limiter.allow("client1"));
    }

    #[test]
    fn new_client_gets_single_token_not_full_burst() {
        let guard = make_guard(1, 100);
        // First call should be allowed (1 token).
        assert!(guard.readyz_limiter.allow("new_client"));
        // Second call on the same Instant-tick (no refill time) should fail.
        assert!(!guard.readyz_limiter.allow("new_client"));
    }

    #[test]
    fn capacity_cap_enforced() {
        let guard = make_guard(1, 1);
        // Fill capacity with distinct clients.
        for i in 0..RATE_LIMIT_MAX_CLIENT_KEYS {
            guard.readyz_limiter.allow(&format!("c{i}"));
        }
        assert_eq!(
            guard.readyz_limiter.clients.len(),
            RATE_LIMIT_MAX_CLIENT_KEYS
        );
    }

    #[test]
    fn sweep_removes_stale_entries() {
        let guard = make_guard(1, 1);
        // Insert a client entry.
        guard.readyz_limiter.allow("client_a");
        assert_eq!(guard.readyz_limiter.clients.len(), 1);
        // Manually backdate `last_seen` to exceed TTL.
        if let Some(mut b) = guard.readyz_limiter.clients.get_mut("client_a") {
            b.last_seen = Instant::now()
                .checked_sub(RATE_LIMIT_STALE_CLIENT_TTL + Duration::from_secs(1))
                .unwrap_or(b.last_seen);
        }
        guard.sweep_stale_entries();
        assert_eq!(guard.readyz_limiter.clients.len(), 0);
    }
}
