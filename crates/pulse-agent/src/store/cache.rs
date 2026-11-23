use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use dashmap::DashMap;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, PsyncIoEngineConfig, S3FifoConfig,
};
use moka::sync::Cache;
use pulse_storage::EncryptedObjectStore;

use crate::store::error::{StoreError, StoreResult};
use crate::store::segment::{
    SegmentStringTable, SegmentTail, SessionRollupRow, SpanMetricRow, TraceSummaryRow,
};

const MAX_HYBRID_DISK_ENTRY_BYTES: usize = 3 * 1024 * 1024; // well under 4 MiB BlockEngineConfig block size

/// Hybrid RAM + NVMe Block Cache (`foyer`) + byte-bounded decoded section caches (`moka`).
pub struct AgentSegmentCache {
    hybrid: HybridCache<String, Vec<u8>>,
    enc_store: Option<Arc<EncryptedObjectStore>>,
    tail_cache: Cache<String, Arc<SegmentTail>>,
    strings_cache: Cache<String, Arc<SegmentStringTable>>,
    summaries_cache: Cache<String, Arc<Vec<TraceSummaryRow>>>,
    span_metrics_cache: Cache<String, Arc<Vec<SpanMetricRow>>>,
    session_rollups_cache: Cache<String, Arc<Vec<SessionRollupRow>>>,
    segment_hybrid_keys: DashMap<String, HashSet<String>>,
}

impl AgentSegmentCache {
    pub async fn open(
        enc_store: Option<Arc<EncryptedObjectStore>>,
        memory_bytes: usize,
        cache_dir: Option<&Path>,
        disk_bytes: usize,
    ) -> StoreResult<Arc<Self>> {
        let policy = if cache_dir.is_some() {
            HybridCachePolicy::WriteOnInsertion
        } else {
            HybridCachePolicy::WriteOnEviction
        };
        let mem_budget = memory_bytes.max(4 * 1024 * 1024);

        let mem_builder = HybridCacheBuilder::new()
            .with_name("pulse-agent-cache")
            .with_policy(policy)
            .with_flush_on_close(false)
            .memory(mem_budget)
            .with_eviction_config(S3FifoConfig::default())
            .with_weighter(|k: &String, v: &Vec<u8>| k.len().saturating_add(v.len()));

        let hybrid = if let Some(dir) = cache_dir {
            std::fs::create_dir_all(dir)?;
            let device = FsDeviceBuilder::new(dir)
                .with_capacity(disk_bytes.max(32 * 1024 * 1024))
                .build()
                .map_err(|e| StoreError::Cache(e.to_string()))?;
            mem_builder
                .storage()
                .with_io_engine_config(PsyncIoEngineConfig::new())
                .with_engine_config(BlockEngineConfig::new(device).with_block_size(4 * 1024 * 1024))
                .build()
                .await
                .map_err(|e| StoreError::Cache(e.to_string()))?
        } else {
            mem_builder
                .storage()
                .build()
                .await
                .map_err(|e| StoreError::Cache(e.to_string()))?
        };

        let q_budget = (mem_budget as u64 / 4).max(8 * 1024 * 1024);
        let s_budget = (mem_budget as u64 / 8).max(4 * 1024 * 1024);

        let tail_cache = Cache::builder()
            .max_capacity(q_budget)
            .weigher(|k: &String, v: &Arc<SegmentTail>| {
                k.len()
                    .saturating_add(v.approx_bytes())
                    .clamp(64, u32::MAX as usize) as u32
            })
            .build();

        let strings_cache = Cache::builder()
            .max_capacity(q_budget)
            .weigher(|k: &String, v: &Arc<SegmentStringTable>| {
                k.len()
                    .saturating_add(v.approx_bytes())
                    .clamp(64, u32::MAX as usize) as u32
            })
            .build();

        let summaries_cache = Cache::builder()
            .max_capacity(q_budget)
            .weigher(|k: &String, v: &Arc<Vec<TraceSummaryRow>>| {
                let rows_bytes: usize = v.iter().map(TraceSummaryRow::approx_bytes).sum();
                k.len()
                    .saturating_add(rows_bytes)
                    .clamp(64, u32::MAX as usize) as u32
            })
            .build();

        let span_metrics_cache = Cache::builder()
            .max_capacity(q_budget)
            .weigher(|k: &String, v: &Arc<Vec<SpanMetricRow>>| {
                let rows_bytes = v.len().saturating_mul(std::mem::size_of::<SpanMetricRow>());
                k.len()
                    .saturating_add(rows_bytes)
                    .clamp(64, u32::MAX as usize) as u32
            })
            .build();

        let session_rollups_cache = Cache::builder()
            .max_capacity(s_budget)
            .weigher(|k: &String, v: &Arc<Vec<SessionRollupRow>>| {
                let rows_bytes: usize = v.iter().map(SessionRollupRow::approx_bytes).sum();
                k.len()
                    .saturating_add(rows_bytes)
                    .clamp(64, u32::MAX as usize) as u32
            })
            .build();

        Ok(Arc::new(Self {
            hybrid,
            enc_store,
            tail_cache,
            strings_cache,
            summaries_cache,
            span_metrics_cache,
            session_rollups_cache,
            segment_hybrid_keys: DashMap::new(),
        }))
    }

    pub(crate) async fn hybrid_get_unsealed(&self, key: &str) -> Option<Vec<u8>> {
        let key_owned = key.to_string();
        let Ok(Some(entry)) = self.hybrid.get(&key_owned).await else {
            return None;
        };
        let raw = entry.value();
        match &self.enc_store {
            Some(es) => es.unseal_cache_blob(raw).ok(),
            None => Some(raw.clone()),
        }
    }

    pub(crate) fn hybrid_insert_sealed(&self, key: String, val: &[u8]) {
        if val.len() > MAX_HYBRID_DISK_ENTRY_BYTES {
            return;
        }
        if let Some(seg_id) = key.split(':').nth(1) {
            self.segment_hybrid_keys
                .entry(seg_id.to_string())
                .or_default()
                .insert(key.clone());
        }
        if let Some(es) = &self.enc_store {
            if let Ok(sealed) = es.seal_cache_blob(val) {
                self.hybrid.insert(key, sealed);
            }
        } else {
            self.hybrid.insert(key, val.to_vec());
        }
    }

    /// Invalidates all RAM (`moka`) and hybrid disk/memory (`foyer`) entries for `segment_id`.
    pub(crate) fn invalidate_segment(&self, segment_id: &str) {
        self.tail_cache.invalidate(segment_id);
        self.strings_cache.invalidate(segment_id);
        self.summaries_cache.invalidate(segment_id);
        self.span_metrics_cache.invalidate(segment_id);
        self.session_rollups_cache.invalidate(segment_id);

        self.hybrid.remove(&format!("eof:{segment_id}"));
        self.hybrid.remove(&format!("p4idx:{segment_id}"));
        self.hybrid.remove(&format!("p5dir:{segment_id}"));
        for tag in 1u16..=7u16 {
            self.hybrid.remove(&format!("sec:{segment_id}:{tag:02x}"));
        }
        if let Some((_, keys)) = self.segment_hybrid_keys.remove(segment_id) {
            for k in keys {
                self.hybrid.remove(&k);
            }
        }
    }

    pub(crate) fn tail_cache(&self) -> &Cache<String, Arc<SegmentTail>> {
        &self.tail_cache
    }

    pub(crate) fn strings_cache(&self) -> &Cache<String, Arc<SegmentStringTable>> {
        &self.strings_cache
    }

    pub(crate) fn summaries_cache(&self) -> &Cache<String, Arc<Vec<TraceSummaryRow>>> {
        &self.summaries_cache
    }

    pub(crate) fn span_metrics_cache(&self) -> &Cache<String, Arc<Vec<SpanMetricRow>>> {
        &self.span_metrics_cache
    }

    pub(crate) fn session_rollups_cache(&self) -> &Cache<String, Arc<Vec<SessionRollupRow>>> {
        &self.session_rollups_cache
    }

    pub async fn close(&self) -> StoreResult<()> {
        self.hybrid
            .close()
            .await
            .map_err(|e| StoreError::Cache(e.to_string()))
    }
}
