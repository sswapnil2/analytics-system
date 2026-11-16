use std::collections::HashSet;

use dashmap::DashMap;
use parking_lot::Mutex;

pub const DEDUP_WINDOW_MS: i64 = 15 * 60 * 1_000;
/// Hard per-bucket window cap preventing unbounded RAM growth under UUID flood (`P3-03`).
pub const MAX_DEDUP_WINDOW_ENTRIES: usize = 524_288;

struct RotatingRing {
    window_epoch: i64,
    current: HashSet<u64>,
    previous: HashSet<u64>,
}

impl RotatingRing {
    fn new(now_ms: i64) -> Self {
        Self {
            window_epoch: now_ms.div_euclid(DEDUP_WINDOW_MS),
            current: HashSet::with_capacity(4096),
            previous: HashSet::new(),
        }
    }

    fn rotate_if_needed(&mut self, now_ms: i64) {
        let ep = now_ms.div_euclid(DEDUP_WINDOW_MS);
        if ep > self.window_epoch {
            if ep == self.window_epoch + 1 {
                self.previous = std::mem::take(&mut self.current);
            } else {
                self.previous.clear();
                self.current.clear();
            }
            self.window_epoch = ep;
        }
    }

    fn contains(&mut self, fp: u64, now_ms: i64) -> bool {
        self.rotate_if_needed(now_ms);
        self.current.contains(&fp) || self.previous.contains(&fp)
    }

    fn insert_bounded(&mut self, fp: u64, now_ms: i64) {
        self.rotate_if_needed(now_ms);
        if self.current.len() >= MAX_DEDUP_WINDOW_ENTRIES {
            // Rotate early under memory pressure rather than growing without bound (`P3-03`).
            self.previous = std::mem::take(&mut self.current);
        }
        self.current.insert(fp);
    }

    /// Returns `true` if `fp` is new and inserted; `false` if already present in current or previous 15-min window.
    fn check_and_insert(&mut self, fp: u64, now_ms: i64) -> bool {
        if self.contains(fp, now_ms) {
            return false;
        }
        self.insert_bounded(fp, now_ms);
        true
    }

    fn seed(&mut self, fp: u64) {
        if self.current.len() < MAX_DEDUP_WINDOW_ENTRIES {
            self.current.insert(fp);
        }
    }
}

/// Per-`(project_id, bucket_id)` rotating two-window deterministic deduplication filter.
#[derive(Default)]
pub struct DedupRegistry {
    rings: DashMap<(String, u8), Mutex<RotatingRing>>,
}

impl DedupRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Non-mutating check used BEFORE durable WAL append (`P1-01`).
    pub fn contains(&self, project_id: &str, bucket_id: u8, fp: u64, now_ms: i64) -> bool {
        let key = (project_id.to_string(), bucket_id);
        let entry = self
            .rings
            .entry(key)
            .or_insert_with(|| Mutex::new(RotatingRing::new(now_ms)));
        let found = entry.lock().contains(fp, now_ms);
        found
    }

    /// Commits fingerprints ONLY AFTER durable WAL + MemBucket append succeeds (`P1-01`).
    pub fn commit_batch(&self, project_id: &str, bucket_id: u8, fps: &[u64], now_ms: i64) {
        if fps.is_empty() {
            return;
        }
        let key = (project_id.to_string(), bucket_id);
        let entry = self
            .rings
            .entry(key)
            .or_insert_with(|| Mutex::new(RotatingRing::new(now_ms)));
        let mut ring = entry.lock();
        for &fp in fps {
            ring.insert_bounded(fp, now_ms);
        }
    }

    pub fn check_and_insert(&self, project_id: &str, bucket_id: u8, fp: u64, now_ms: i64) -> bool {
        let key = (project_id.to_string(), bucket_id);
        let entry = self
            .rings
            .entry(key)
            .or_insert_with(|| Mutex::new(RotatingRing::new(now_ms)));
        let is_new = entry.lock().check_and_insert(fp, now_ms);
        is_new
    }

    pub fn seed_recovered(&self, project_id: &str, items: &[(u8, u64)], now_ms: i64) {
        for &(bucket_id, fp) in items {
            let key = (project_id.to_string(), bucket_id);
            let entry = self
                .rings
                .entry(key)
                .or_insert_with(|| Mutex::new(RotatingRing::new(now_ms)));
            entry.lock().seed(fp);
        }
    }
}

/// Clock-skew normalizer implementing Pulse's mobile clock-drift correction law (`P1-13`).
///
/// Only adjusts when `client_sent_ts_ms > 0` (present on SDK transmissions, `0` on server-side/backfill JSON)
/// and `client_event_ts_ms <= client_sent_ts_ms`, shifting by `(server_now_ms - client_sent_ts_ms)` when
/// device clock skew exceeds 5 seconds. Uses `i128` arithmetic so extreme `i64` values never overflow.
#[inline]
pub fn normalize_event_timestamp(
    client_event_ts_ms: i64,
    client_sent_ts_ms: i64,
    server_now_ms: i64,
) -> i64 {
    if client_sent_ts_ms <= 0 || client_event_ts_ms > client_sent_ts_ms {
        return client_event_ts_ms;
    }
    let skew = (server_now_ms as i128) - (client_sent_ts_ms as i128);
    if skew.abs() > 5_000 {
        let shifted = (client_event_ts_ms as i128) + skew;
        shifted.clamp(i64::MIN as i128, i64::MAX as i128) as i64
    } else {
        client_event_ts_ms
    }
}
