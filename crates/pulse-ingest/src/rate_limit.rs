//! Per-project ingest rate limit: a token bucket in events per second.
//!
//! Keyed by project, not API key, so a tenant with several keys gets one quota. A request is
//! admitted while the bucket is positive and may overdraw it (a 250k-event batch still goes
//! through); the overdraft is paid back before the next request, which gets a 429 with
//! `Retry-After` until then. One node's view only: behind a load balancer each node enforces its
//! own share.

use std::time::Instant;

use dashmap::DashMap;
use parking_lot::Mutex;

/// Sustained events/second per project (`PULSE_INGEST_EVENTS_PER_SEC`).
pub const DEFAULT_EVENTS_PER_SEC: f64 = 20_000.0;
/// Seconds of traffic a project may burst above the rate (`PULSE_INGEST_BURST_SECS`).
pub const DEFAULT_BURST_SECS: f64 = 5.0;

struct Bucket {
    tokens: f64,
    last: Instant,
}

pub struct RateLimiter {
    rate: f64,
    burst: f64,
    buckets: DashMap<String, Mutex<Bucket>>,
}

impl RateLimiter {
    /// `rate <= 0` disables limiting.
    pub fn new(events_per_sec: f64, burst_secs: f64) -> Self {
        Self {
            rate: events_per_sec,
            burst: (events_per_sec * burst_secs.max(1.0)).max(1.0),
            buckets: DashMap::new(),
        }
    }

    pub fn from_env() -> Self {
        let env = |k: &str, d: f64| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|v| v.is_finite())
                .unwrap_or(d)
        };
        Self::new(
            env("PULSE_INGEST_EVENTS_PER_SEC", DEFAULT_EVENTS_PER_SEC),
            env("PULSE_INGEST_BURST_SECS", DEFAULT_BURST_SECS),
        )
    }

    /// Charges `units` to `project_id`. `Err(retry_after_secs)` when the project is over its rate.
    pub fn charge(&self, project_id: &str, units: u64) -> Result<(), u64> {
        self.charge_at(project_id, units, Instant::now())
    }

    fn charge_at(&self, project_id: &str, units: u64, now: Instant) -> Result<(), u64> {
        if self.rate <= 0.0 {
            return Ok(());
        }
        // ponytail: one bucket per project ever seen, never evicted; ~100 B each.
        let entry = self
            .buckets
            .entry(project_id.to_string())
            .or_insert_with(|| {
                Mutex::new(Bucket {
                    tokens: self.burst,
                    last: now,
                })
            });
        let mut b = entry.lock();
        let elapsed = now.saturating_duration_since(b.last).as_secs_f64();
        b.tokens = (b.tokens + elapsed * self.rate).min(self.burst);
        b.last = now;
        if b.tokens <= 0.0 {
            return Err(((-b.tokens) / self.rate).ceil().max(1.0) as u64);
        }
        b.tokens -= units as f64;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn bursts_overdraw_then_wait_and_projects_are_independent() {
        let rl = RateLimiter::new(100.0, 2.0); // 100/s, burst 200
        let t0 = Instant::now();
        assert!(rl.charge_at("p", 150, t0).is_ok());
        // Still positive (50 left): a batch larger than what's left is admitted and overdraws.
        assert!(rl.charge_at("p", 350, t0).is_ok()); // -300
        assert_eq!(rl.charge_at("p", 1, t0), Err(3)); // 300 tokens at 100/s
                                                      // Another tenant is unaffected.
        assert!(rl.charge_at("q", 10, t0).is_ok());
        // Refills at the rate: after 3.01 s the debt is paid.
        assert!(rl
            .charge_at("p", 1, t0 + Duration::from_millis(3_010))
            .is_ok());
        // Never refills past the burst.
        assert!(rl
            .charge_at("q", 205, t0 + Duration::from_secs(3_600))
            .is_ok());
        assert!(rl
            .charge_at("q", 1, t0 + Duration::from_secs(3_600))
            .is_err());
    }

    #[test]
    fn zero_rate_disables() {
        let rl = RateLimiter::new(0.0, 5.0);
        assert!(rl.charge("p", u64::MAX).is_ok());
        assert!(rl.charge("p", u64::MAX).is_ok());
    }
}
