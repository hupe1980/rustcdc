use axum::http::HeaderMap;
use dashmap::DashMap;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use crate::config::schema::AdminConfig;

/// Maximum distinct client keys tracked before LRU eviction.
pub(super) const RATE_LIMIT_MAX_CLIENT_KEYS: usize = 4096;
/// Distinct client keys above which a new client is admitted with a single token
/// rather than its configured burst.
///
/// A rapidly-rotating attacker mints a fresh key per request, so a full burst per new
/// key would multiply their allowance by the burst size. Starting new clients at one
/// token defeats that — but applied *unconditionally* it also means an ordinary client
/// gets one request rather than the burst the operator configured, which made
/// `*_rate_limit_burst` inert for anyone who had not been seen before.
///
/// A filling key table is the signature of the attack, so the restriction is applied
/// only under that pressure. In steady state (a handful of scrapers and probes) the
/// configured burst is honoured; under a rotation flood the old behaviour returns.
pub(super) const RATE_LIMIT_NEW_CLIENT_BURST_PRESSURE: usize = RATE_LIMIT_MAX_CLIENT_KEYS / 2;
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

        // `X-Forwarded-For` is read **right to left**, skipping addresses that belong to
        // trusted proxies, and the first remaining address is the client.
        //
        // Reading it left to right — the previous behaviour — hands the rate limiter to
        // the caller. The header is append-only: a proxy adds the address it saw to the
        // *end*, so everything to the left of that is whatever the client sent. A client
        // that sets `X-Forwarded-For: 203.0.113.<random>` on every request gets a fresh
        // bucket per request and is never limited at all, which is the entire protection
        // this guard provides for `/status`, `/metrics`, `/readyz` and `/openapi.json`.
        //
        // Only the rightmost non-proxy entry is attested by something we trust. This is
        // the same rule as nginx's `real_ip_recursive on` and the Forwarded-header
        // guidance in RFC 7239 §7.1.
        if let Some(forwarded) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
            for candidate in forwarded.rsplit(',').map(str::trim) {
                let Ok(ip) = candidate.parse::<IpAddr>() else {
                    // An unparseable hop is not evidence of anything, and skipping past it
                    // would let a client inject `garbage, 203.0.113.9` to move the cursor.
                    // Stop and fall back to the peer address.
                    break;
                };
                if self.trusted_proxy_ips.contains(&ip) {
                    // Our own infrastructure, appended by the hop in front of it. Keep
                    // walking left.
                    continue;
                }
                // Only accept globally-routable unicast addresses. Loopback, link-local,
                // private and unspecified addresses can be spoofed by an attacker behind a
                // trusted proxy to exhaust unrelated rate-limit buckets or claim a
                // "trusted" identity.
                if is_globally_routable(ip) {
                    return format!("xff:{ip}");
                }
                break;
            }
        }

        if let Some(real_ip) = headers.get("x-real-ip").and_then(|v| v.to_str().ok())
            && let Ok(ip) = real_ip.trim().parse::<IpAddr>()
            && is_globally_routable(ip)
        {
            return format!("xri:{ip}");
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
                // Carrier-grade NAT, 100.64.0.0/10 (RFC 6598). Shared between subscribers
                // and not globally unique, so it identifies a carrier rather than a
                // client — and `Ipv4Addr::is_private` does not cover it.
                && !(v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 0x40)
        }
        IpAddr::V6(v6) => {
            !v6.is_loopback()
                && !v6.is_unspecified()
                && !v6.is_multicast()
                // Link-local: fe80::/10
                && (v6.segments()[0] & 0xffc0) != 0xfe80
                // Unique local: fc00::/7 (RFC 4193)
                && (v6.segments()[0] & 0xfe00) != 0xfc00
                // An IPv4-mapped address (::ffff:0:0/96) is an IPv4 address wearing a
                // different notation; judge it by the IPv4 rules above rather than
                // letting `::ffff:127.0.0.1` through as "not IPv6-loopback".
                && v6
                    .to_ipv4_mapped()
                    .is_none_or(|v4| is_globally_routable(IpAddr::V4(v4)))
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

        // A new client gets its configured burst, unless the key table is under the
        // pressure that makes burst amplification by IP rotation possible — see
        // `RATE_LIMIT_NEW_CLIENT_BURST_PRESSURE`.
        let initial_tokens = if self.clients.len() >= RATE_LIMIT_NEW_CLIENT_BURST_PRESSURE {
            1.0_f64.min(self.burst_capacity)
        } else {
            self.burst_capacity
        };

        let mut bucket = self
            .clients
            .entry(client_key.to_string())
            .or_insert(ClientTokenBucket {
                tokens: initial_tokens,
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
        // In steady state a new client gets the burst the operator configured. It used
        // to get exactly one token regardless, which made `*_rate_limit_burst` inert
        // for anyone not already in the table — an operator setting 40 got 1.
        for attempt in 0..5 {
            assert!(
                guard.readyz_limiter.allow("client1"),
                "request {attempt} must be within the configured burst of 5"
            );
        }
        assert!(
            !guard.readyz_limiter.allow("client1"),
            "the sixth request exceeds the burst and must be refused"
        );
    }

    #[test]
    fn new_client_gets_single_token_not_full_burst_under_key_table_pressure() {
        let guard = make_guard(1, 100);

        // Simulate the attack this rule exists for: enough distinct keys to put the
        // table under pressure. Only then does a new key drop to a single token.
        for index in 0..RATE_LIMIT_NEW_CLIENT_BURST_PRESSURE {
            guard.readyz_limiter.allow(&format!("rotating-{index}"));
        }

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

    fn proxied_guard(proxies: &[&str]) -> AdminAbuseGuard {
        AdminAbuseGuard {
            readyz_limiter: EndpointRateLimiter::new(1, 1),
            status_limiter: EndpointRateLimiter::new(1, 1),
            metrics_limiter: EndpointRateLimiter::new(1, 1),
            trusted_proxy_ips: proxies.iter().filter_map(|p| p.parse().ok()).collect(),
        }
    }

    fn xff(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", value.parse().expect("header value"));
        headers
    }

    fn from_proxy(proxy: &str) -> Option<SocketAddr> {
        Some(SocketAddr::new(proxy.parse().expect("proxy ip"), 4444))
    }

    /// The defect this rule exists for.
    ///
    /// `X-Forwarded-For` is append-only: the trusted proxy adds the address it observed to
    /// the **end**, and everything to its left is whatever the client typed. Reading the
    /// header left to right therefore keyed the rate limiter on an attacker-chosen string,
    /// so rotating the value gave a fresh token bucket on every request and the limiter
    /// stopped limiting.
    #[test]
    fn a_client_supplied_forwarded_for_prefix_cannot_choose_the_rate_limit_key() {
        let guard = proxied_guard(&["10.0.0.1"]);

        let spoofed = guard.client_key(&xff("8.8.8.8, 9.9.9.9"), from_proxy("10.0.0.1"));
        assert_eq!(
            spoofed, "xff:9.9.9.9",
            "the rightmost non-proxy hop is the one the trusted proxy attested"
        );

        // Rotating the prefix must not move the key — that rotation was the bypass.
        let rotated = guard.client_key(&xff("8.8.4.4, 9.9.9.9"), from_proxy("10.0.0.1"));
        assert_eq!(spoofed, rotated);
    }

    /// A chain of trusted proxies is walked through, not stopped at.
    #[test]
    fn trusted_proxy_hops_are_skipped_to_reach_the_real_client() {
        let guard = proxied_guard(&["10.0.0.1", "10.0.0.2"]);
        let key = guard.client_key(&xff("9.9.9.9, 10.0.0.2"), from_proxy("10.0.0.1"));
        assert_eq!(key, "xff:9.9.9.9");
    }

    /// A private or unroutable rightmost hop is not a usable identity, and must not cause
    /// the walk to continue leftwards into client-controlled text.
    #[test]
    fn an_unroutable_rightmost_hop_falls_back_to_the_peer_address() {
        let guard = proxied_guard(&["10.0.0.1"]);
        let key = guard.client_key(&xff("9.9.9.9, 192.168.5.5"), from_proxy("10.0.0.1"));
        assert_eq!(key, "proxy:10.0.0.1");
    }

    /// Injected garbage must not act as a cursor that skips past the attested hop.
    #[test]
    fn an_unparseable_hop_stops_the_walk() {
        let guard = proxied_guard(&["10.0.0.1"]);
        let key = guard.client_key(&xff("9.9.9.9, not-an-ip"), from_proxy("10.0.0.1"));
        assert_eq!(key, "proxy:10.0.0.1");
    }

    /// An untrusted peer's header is never read at all.
    #[test]
    fn forwarded_headers_from_an_untrusted_peer_are_ignored() {
        let guard = proxied_guard(&["10.0.0.1"]);
        let key = guard.client_key(&xff("9.9.9.9"), from_proxy("1.1.1.1"));
        assert_eq!(key, "peer:1.1.1.1");
    }

    #[test]
    fn ipv4_mapped_loopback_is_not_globally_routable() {
        assert!(!is_globally_routable(
            "::ffff:127.0.0.1".parse().expect("mapped loopback")
        ));
        assert!(!is_globally_routable(
            "100.64.0.1".parse().expect("cgnat address")
        ));
        assert!(is_globally_routable(
            "9.9.9.9".parse().expect("public address")
        ));
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
