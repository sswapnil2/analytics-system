use std::collections::HashSet;

use dashmap::DashMap;
use parking_lot::Mutex;

pub const DEDUP_WINDOW_MS: i64 = 15 * 60 * 1_000;

struct RotatingRing {
    window_epoch: i64,
    current: HashSet<u64>,
    previous: HashSet<u64>,
}

impl RotatingRing {
    fn new(now_ms: i64) -> Self {
        Self {
            window_epoch: now_ms / DEDUP_WINDOW_MS,
            current: HashSet::with_capacity(4096),
            previous: HashSet::new(),
        }
    }

    fn rotate_if_needed(&mut self, now_ms: i64) {
        let ep = now_ms / DEDUP_WINDOW_MS;
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

    /// Returns `true` if `fp` is new and inserted; `false` if already present in current or previous 15-min window.
    fn check_and_insert(&mut self, fp: u64, now_ms: i64) -> bool {
        self.rotate_if_needed(now_ms);
        if self.current.contains(&fp) || self.previous.contains(&fp) {
            return false;
        }
        self.current.insert(fp);
        true
    }

    fn seed(&mut self, fp: u64) {
        self.current.insert(fp);
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

/// Clock-skew normalizer implementing Pulse's mobile clock-drift correction law.
#[inline]
pub fn normalize_event_timestamp(
    client_event_ts_ms: i64,
    client_sent_ts_ms: i64,
    server_now_ms: i64,
) -> i64 {
    if (client_sent_ts_ms - server_now_ms).abs() > 5_000 || client_event_ts_ms > client_sent_ts_ms {
        let age = (client_sent_ts_ms - client_event_ts_ms).max(0);
        server_now_ms - age
    } else {
        client_event_ts_ms
    }
}
