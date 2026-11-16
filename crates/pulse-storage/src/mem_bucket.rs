use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, NaiveDate, Utc};
use parking_lot::RwLock;
use pulse_wire::PropValue;

pub const SESSION_FLAG_RAGE_TAP: u16 = 0b001;
pub const SESSION_FLAG_DEAD_TAP: u16 = 0b010;
pub const SESSION_FLAG_ERROR: u16 = 0b100;

/// Canonical in-memory analytical event row (L0 and segment builder representation).
#[derive(Debug, Clone)]
pub struct StoredRow {
    pub l0_seq: u64,
    pub wal_seq: u64,
    pub event_uuid_fp: u64,
    pub uid_internal: u32,
    pub session_hash: u64,
    pub session_id: String,
    pub event_id: u16,
    pub event_ts_ms: i64,
    pub replay_offset_kb: u32,
    pub session_flags: u16,
    pub props: Vec<(u16, PropValue)>,
}

impl StoredRow {
    #[inline]
    pub fn event_date(&self) -> NaiveDate {
        DateTime::<Utc>::from_timestamp_millis(self.event_ts_ms)
            .map(|dt| dt.date_naive())
            .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
    }
}

struct BucketInner {
    rows: Vec<Arc<StoredRow>>,
    cached_slice: Option<Arc<[Arc<StoredRow>]>>,
    oldest_unflushed_at: Option<Instant>,
}

/// L0 hot in-memory buffer per `(project_id, bucket_id)`.
pub struct MemBucket {
    bucket_id: u8,
    inner: RwLock<BucketInner>,
}

impl MemBucket {
    pub fn new(bucket_id: u8) -> Self {
        Self {
            bucket_id,
            inner: RwLock::new(BucketInner {
                rows: Vec::new(),
                cached_slice: Some(Arc::from(&[][..])),
                oldest_unflushed_at: None,
            }),
        }
    }

    pub fn bucket_id(&self) -> u8 {
        self.bucket_id
    }

    pub fn append_batch(&self, rows: Vec<StoredRow>) -> usize {
        if rows.is_empty() {
            return 0;
        }
        let mut guard = self.inner.write();
        if guard.oldest_unflushed_at.is_none() {
            guard.oldest_unflushed_at = Some(Instant::now());
        }
        guard.rows.extend(rows.into_iter().map(Arc::new));
        guard.cached_slice = None;
        guard.rows.len()
    }

    pub fn len(&self) -> usize {
        self.inner.read().rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().rows.is_empty()
    }

    pub fn should_flush(&self, row_threshold: usize, max_age: Duration) -> bool {
        let guard = self.inner.read();
        if guard.rows.is_empty() {
            return false;
        }
        if guard.rows.len() >= row_threshold {
            return true;
        }
        if let Some(ts) = guard.oldest_unflushed_at {
            if ts.elapsed() >= max_age {
                return true;
            }
        }
        false
    }

    /// Captures an immutable `Arc<[Arc<StoredRow>]>` snapshot of this bucket's L0 rows (`P1-07`, `P3-04`).
    /// Cached across consecutive readers so repeated snapshots take O(1) atomic refcount increments!
    pub fn arc_snapshot(&self) -> Arc<[Arc<StoredRow>]> {
        {
            let guard = self.inner.read();
            if let Some(cached) = &guard.cached_slice {
                return cached.clone();
            }
        }
        let mut guard = self.inner.write();
        if let Some(cached) = &guard.cached_slice {
            return cached.clone();
        }
        let snap: Arc<[Arc<StoredRow>]> = Arc::from(guard.rows.as_slice());
        guard.cached_slice = Some(snap.clone());
        snap
    }

    /// Reads all rows strictly inside the MVCC window `(floor_l0_seq, max_l0_seq]`.
    pub fn snapshot_rows(&self, floor_l0_seq: u64, max_l0_seq: u64) -> Vec<StoredRow> {
        let guard = self.inner.read();
        guard
            .rows
            .iter()
            .filter(|r| r.l0_seq > floor_l0_seq && r.l0_seq <= max_l0_seq)
            .map(|r| (**r).clone())
            .collect()
    }

    /// Updates session frustration flags on in-memory events matching `session_hash` using copy-on-write `Arc::make_mut`.
    pub fn add_session_flags(&self, session_hash: u64, flags: u16) {
        let mut guard = self.inner.write();
        let mut changed = false;
        for row in &mut guard.rows {
            if row.session_hash == session_hash && (row.session_flags & flags) != flags {
                Arc::make_mut(row).session_flags |= flags;
                changed = true;
            }
        }
        if changed {
            guard.cached_slice = None;
        }
    }

    /// Clones rows eligible for L0 -> L1 flush (`l0_seq > floor_l0_seq`) without removing them yet.
    pub fn stage_flush(&self, floor_l0_seq: u64) -> Option<(u64, u64, Vec<StoredRow>)> {
        let guard = self.inner.read();
        let mut staged: Vec<StoredRow> = guard
            .rows
            .iter()
            .filter(|r| r.l0_seq > floor_l0_seq)
            .map(|r| (**r).clone())
            .collect();
        if staged.is_empty() {
            return None;
        }
        let max_l0 = staged
            .iter()
            .map(|r| r.l0_seq)
            .max()
            .unwrap_or(floor_l0_seq);
        let max_wal = staged.iter().map(|r| r.wal_seq).max().unwrap_or(0);
        staged.shrink_to_fit();
        Some((max_l0, max_wal, staged))
    }

    /// Evicts rows with `l0_seq <= committed_l0_seq` AFTER the new MVCC Manifest epoch is durable.
    pub fn complete_flush(&self, committed_l0_seq: u64) {
        let mut guard = self.inner.write();
        let prev_len = guard.rows.len();
        guard.rows.retain(|r| r.l0_seq > committed_l0_seq);
        if guard.rows.len() != prev_len {
            guard.cached_slice = None;
        }
        if guard.rows.is_empty() {
            guard.oldest_unflushed_at = None;
        } else {
            guard.oldest_unflushed_at = Some(Instant::now());
        }
    }
}
