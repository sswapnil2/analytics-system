use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, UInt16Type, UInt32Type, UInt64Type};
use arrow_array::{Array as ArrowArray, RecordBatch};
use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, PsyncIoEngineConfig, S3FifoConfig,
};
use moka::sync::Cache;
use pulse_storage::{
    decode_session_index, decode_tier_c_row, extract_arrow_str, tier_c_key_bit,
    EncryptedObjectStore, SegmentInvertedIndex, SegmentMeta, SegmentSessionIndex, StorageError,
    TierBColumnEntry, VortexSegmentReader,
};
use pulse_wire::PropValue;

/// Dense contiguous Tier A columns decoded from an immutable Vortex segment.
#[derive(Debug)]
pub struct TierAColumns {
    pub uid_internal: Vec<u32>,
    pub session_hash: Vec<u64>,
    pub event_id: Vec<u16>,
    pub event_ts_ms: Vec<i64>,
    pub replay_offset_kb: Vec<u32>,
    pub session_index: Arc<SegmentSessionIndex>,
}

impl TierAColumns {
    #[inline]
    pub fn len(&self) -> usize {
        self.uid_internal.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.uid_internal.is_empty()
    }

    fn approx_bytes(&self) -> u32 {
        let row_bytes = self.len().saturating_mul(4 + 8 + 2 + 8 + 4);
        let sidx_bytes = self
            .session_index
            .headers
            .len()
            .saturating_mul(std::mem::size_of::<pulse_storage::SessionHeader>());
        row_bytes
            .saturating_add(sidx_bytes)
            .clamp(64, u32::MAX as usize) as u32
    }
}

/// Hybrid RAM + NVMe Block Cache (`foyer`) + byte-bounded decoded columnar caches (`moka`) (`P3-03`).
pub struct SegmentBlockCache {
    hybrid: HybridCache<String, Vec<u8>>,
    reader: Arc<VortexSegmentReader>,
    enc_store: Option<Arc<EncryptedObjectStore>>,
    tier_a_cache: Cache<String, Arc<TierAColumns>>,
    inverted_cache: Cache<String, Arc<SegmentInvertedIndex>>,
    prop_col_cache: Cache<(String, u16), Arc<Vec<PropValue>>>,
}

impl SegmentBlockCache {
    /// Opens a `foyer` HybridCache backed by RAM + optional NVMe directory (`cache_dir`),
    /// sealing NVMe disk cache entries whenever encryption-at-rest is enabled (`H2`).
    pub async fn open(
        reader: Arc<VortexSegmentReader>,
        memory_bytes: usize,
        disk_cache_dir: Option<&Path>,
        disk_bytes: usize,
    ) -> Result<Arc<Self>, String> {
        Self::open_with_store(reader, None, memory_bytes, disk_cache_dir, disk_bytes).await
    }

    pub async fn open_with_store(
        reader: Arc<VortexSegmentReader>,
        enc_store: Option<Arc<EncryptedObjectStore>>,
        memory_bytes: usize,
        disk_cache_dir: Option<&Path>,
        disk_bytes: usize,
    ) -> Result<Arc<Self>, String> {
        let policy = if disk_cache_dir.is_some() {
            HybridCachePolicy::WriteOnInsertion
        } else {
            HybridCachePolicy::WriteOnEviction
        };
        let mem_builder = HybridCacheBuilder::new()
            .with_name("pulse-vortex-cache")
            .with_policy(policy)
            .with_flush_on_close(false)
            .memory(memory_bytes.max(4 * 1024 * 1024))
            .with_eviction_config(S3FifoConfig::default())
            .with_weighter(|k: &String, v: &Vec<u8>| k.len() + v.len());

        let hybrid = if let Some(dir) = disk_cache_dir {
            std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
            let device = FsDeviceBuilder::new(dir)
                .with_capacity(disk_bytes.max(32 * 1024 * 1024))
                .build()
                .map_err(|e| e.to_string())?;
            mem_builder
                .storage()
                .with_io_engine_config(PsyncIoEngineConfig::new())
                .with_engine_config(BlockEngineConfig::new(device).with_block_size(4 * 1024 * 1024))
                .build()
                .await
                .map_err(|e| e.to_string())?
        } else {
            mem_builder
                .storage()
                .build()
                .await
                .map_err(|e| e.to_string())?
        };

        let tier_a_budget = (memory_bytes as u64 / 2).max(16 * 1024 * 1024);
        let inv_budget = (memory_bytes as u64 / 4).max(8 * 1024 * 1024);
        let prop_budget = (memory_bytes as u64 / 2).max(16 * 1024 * 1024);

        let tier_a_cache = Cache::builder()
            .max_capacity(tier_a_budget)
            .weigher(|k: &String, v: &Arc<TierAColumns>| {
                (k.len() as u32).saturating_add(v.approx_bytes())
            })
            .build();

        let inverted_cache = Cache::builder()
            .max_capacity(inv_budget)
            .weigher(|k: &String, v: &Arc<SegmentInvertedIndex>| {
                let bm_bytes: usize = v
                    .bitmaps
                    .iter()
                    .map(|((_, _, s), bm)| 24 + s.len() + bm.serialized_size())
                    .sum();
                (k.len().saturating_add(bm_bytes)).clamp(64, u32::MAX as usize) as u32
            })
            .build();

        let prop_col_cache = Cache::builder()
            .max_capacity(prop_budget)
            .weigher(|(k, _): &(String, u16), v: &Arc<Vec<PropValue>>| {
                let elem_bytes = v.len().saturating_mul(std::mem::size_of::<PropValue>());
                (k.len().saturating_add(elem_bytes)).clamp(64, u32::MAX as usize) as u32
            })
            .build();

        Ok(Arc::new(Self {
            hybrid,
            reader,
            enc_store,
            tier_a_cache,
            inverted_cache,
            prop_col_cache,
        }))
    }

    async fn hybrid_get_unsealed(&self, key: &String) -> Option<Vec<u8>> {
        let Ok(Some(entry)) = self.hybrid.get(key).await else {
            return None;
        };
        let raw = entry.value();
        match &self.enc_store {
            Some(es) => es.unseal_cache_blob(raw).ok(),
            None => Some(raw.clone()),
        }
    }

    fn hybrid_insert_sealed(&self, key: String, val: &[u8]) {
        if let Some(es) = &self.enc_store {
            if let Ok(sealed) = es.seal_cache_blob(val) {
                self.hybrid.insert(key, sealed);
            }
        } else {
            self.hybrid.insert(key, val.to_vec());
        }
    }

    pub fn reader(&self) -> &Arc<VortexSegmentReader> {
        &self.reader
    }

    pub async fn get_vortex_bytes(&self, meta: &SegmentMeta) -> Result<Bytes, StorageError> {
        let key = format!("vtx:{}", meta.segment_id);
        if let Some(plain) = self.hybrid_get_unsealed(&key).await {
            return Ok(Bytes::from(plain));
        }
        let fetched = self.reader.fetch_vortex_bytes(meta).await?;
        self.hybrid_insert_sealed(key, &fetched);
        Ok(fetched)
    }

    pub async fn get_session_index(
        &self,
        meta: &SegmentMeta,
    ) -> Result<Arc<SegmentSessionIndex>, StorageError> {
        if let Some(ta) = self.tier_a_cache.get(&meta.segment_id) {
            return Ok(ta.session_index.clone());
        }
        let key = format!("sidx:{}", meta.segment_id);
        if let Some(plain) = self.hybrid_get_unsealed(&key).await {
            let decoded = decode_session_index(&plain)?;
            return Ok(Arc::new(decoded));
        }
        let path = object_store::path::Path::from(meta.object_path.as_str());
        let raw = self
            .reader
            .object_store()
            .get_range(&path, meta.session_idx_offset..meta.file_size)
            .await?;
        self.hybrid_insert_sealed(key, &raw);
        Ok(Arc::new(decode_session_index(&raw)?))
    }

    pub async fn get_inverted_index(
        &self,
        meta: &SegmentMeta,
    ) -> Result<Arc<SegmentInvertedIndex>, StorageError> {
        if let Some(inv) = self.inverted_cache.get(&meta.segment_id) {
            return Ok(inv);
        }
        let key = format!("iidx:{}", meta.segment_id);
        let inv = if let Some(plain) = self.hybrid_get_unsealed(&key).await {
            Arc::new(SegmentInvertedIndex::decode(&plain)?)
        } else {
            let path = object_store::path::Path::from(meta.object_path.as_str());
            let raw = self
                .reader
                .object_store()
                .get_range(&path, meta.inverted_idx_offset..meta.session_idx_offset)
                .await?;
            self.hybrid_insert_sealed(key, &raw);
            Arc::new(SegmentInvertedIndex::decode(&raw)?)
        };
        self.inverted_cache
            .insert(meta.segment_id.clone(), inv.clone());
        Ok(inv)
    }

    pub async fn get_tier_a(&self, meta: &SegmentMeta) -> Result<Arc<TierAColumns>, StorageError> {
        if let Some(cached) = self.tier_a_cache.get(&meta.segment_id) {
            return Ok(cached);
        }
        let sidx = self.get_session_index(meta).await?;
        let vbytes = self.get_vortex_bytes(meta).await?;
        let batch = self.reader.project_from_vortex_bytes_sync(
            vbytes,
            &[
                "uid_internal",
                "session_hash",
                "event_id",
                "event_ts_ms",
                "replay_offset_kb",
            ],
        )?;

        let uid_col = batch
            .column_by_name("uid_internal")
            .ok_or_else(|| StorageError::Corrupted("missing uid_internal".into()))?
            .as_primitive::<UInt32Type>();
        let shash_col = batch
            .column_by_name("session_hash")
            .ok_or_else(|| StorageError::Corrupted("missing session_hash".into()))?
            .as_primitive::<UInt64Type>();
        let ev_col = batch
            .column_by_name("event_id")
            .ok_or_else(|| StorageError::Corrupted("missing event_id".into()))?
            .as_primitive::<UInt16Type>();
        let ts_col = batch
            .column_by_name("event_ts_ms")
            .ok_or_else(|| StorageError::Corrupted("missing event_ts_ms".into()))?
            .as_primitive::<Int64Type>();
        let rep_col = batch
            .column_by_name("replay_offset_kb")
            .ok_or_else(|| StorageError::Corrupted("missing replay_offset_kb".into()))?
            .as_primitive::<UInt32Type>();

        let ta = Arc::new(TierAColumns {
            uid_internal: uid_col.values().to_vec(),
            session_hash: shash_col.values().to_vec(),
            event_id: ev_col.values().to_vec(),
            event_ts_ms: ts_col.values().to_vec(),
            replay_offset_kb: rep_col.values().to_vec(),
            session_index: sidx,
        });
        self.tier_a_cache
            .insert(meta.segment_id.clone(), ta.clone());
        Ok(ta)
    }

    /// Projects and coalesces a polymorphic property column (`Tier B` + `Tier C` Bloom check) into `Arc<Vec<PropValue>>`.
    pub async fn get_property_column(
        &self,
        meta: &SegmentMeta,
        prop_key_id: u16,
    ) -> Result<Arc<Vec<PropValue>>, StorageError> {
        let cache_key = (meta.segment_id.clone(), prop_key_id);
        if let Some(col) = self.prop_col_cache.get(&cache_key) {
            return Ok(col);
        }

        let n = meta.footer.row_count as usize;
        let tb_entry = meta
            .footer
            .tier_b_columns
            .iter()
            .find(|c| c.prop_key_id == prop_key_id);

        if let Some(entry) = tb_entry {
            let mut cols_to_read = Vec::with_capacity(4);
            let n_i64 = TierBColumnEntry::col_i64_name(prop_key_id);
            let n_f64 = TierBColumnEntry::col_f64_name(prop_key_id);
            let n_bool = TierBColumnEntry::col_bool_name(prop_key_id);
            let n_str = TierBColumnEntry::col_str_name(prop_key_id);

            if entry.has_i64 {
                cols_to_read.push(n_i64.as_str());
            }
            if entry.has_f64 {
                cols_to_read.push(n_f64.as_str());
            }
            if entry.has_bool {
                cols_to_read.push(n_bool.as_str());
            }
            if entry.has_str {
                cols_to_read.push(n_str.as_str());
            }

            if cols_to_read.is_empty() {
                // Only `has_null` was present in Tier B (`P2-09l`).
                let empty = Arc::new(vec![PropValue::Null; n]);
                self.prop_col_cache.insert(cache_key, empty.clone());
                return Ok(empty);
            }

            let vbytes = self.get_vortex_bytes(meta).await?;
            let batch = self
                .reader
                .project_from_vortex_bytes_sync(vbytes, &cols_to_read)?;
            let c_i64 = entry
                .has_i64
                .then(|| batch.column_by_name(&n_i64))
                .flatten();
            let c_f64 = entry
                .has_f64
                .then(|| batch.column_by_name(&n_f64))
                .flatten();
            let c_bool = entry
                .has_bool
                .then(|| batch.column_by_name(&n_bool))
                .flatten();
            let c_str = entry
                .has_str
                .then(|| batch.column_by_name(&n_str))
                .flatten();

            let mut vals = Vec::with_capacity(n);
            for row in 0..n {
                if let Some(c) = c_i64 {
                    if !c.is_null(row) {
                        vals.push(PropValue::Int(c.as_primitive::<Int64Type>().value(row)));
                        continue;
                    }
                }
                if let Some(c) = c_f64 {
                    if !c.is_null(row) {
                        vals.push(PropValue::Float(c.as_primitive::<Float64Type>().value(row)));
                        continue;
                    }
                }
                if let Some(c) = c_bool {
                    if !c.is_null(row) {
                        vals.push(PropValue::Bool(c.as_boolean().value(row)));
                        continue;
                    }
                }
                if let Some(c) = c_str {
                    if let Some(s) = extract_arrow_str(c.as_ref(), row) {
                        vals.push(PropValue::Str(s.to_string()));
                        continue;
                    }
                }
                vals.push(PropValue::Null);
            }
            let arc = Arc::new(vals);
            self.prop_col_cache.insert(cache_key, arc.clone());
            return Ok(arc);
        }

        // Not in Tier B: check 64-bit Tier C Bloom filter in 1 bitwise instruction!
        let bit = tier_c_key_bit(prop_key_id);
        if !meta.footer.has_tier_c_column || (meta.footer.tier_c_bloom_64 & bit) == 0 {
            let empty = Arc::new(vec![PropValue::Null; n]);
            self.prop_col_cache.insert(cache_key, empty.clone());
            return Ok(empty);
        }

        let vbytes = self.get_vortex_bytes(meta).await?;
        let batch: RecordBatch = self
            .reader
            .project_from_vortex_bytes_sync(vbytes, &["tier_c_overflow"])?;
        let mut vals = Vec::with_capacity(n);
        if let Some(tc_col) = batch.column_by_name("tier_c_overflow") {
            let bin = tc_col.as_binary::<i32>();
            for row in 0..n {
                if bin.is_null(row) {
                    vals.push(PropValue::Null);
                } else {
                    let kv = decode_tier_c_row(bin.value(row))?;
                    let found = kv
                        .into_iter()
                        .find(|(k, _)| *k == prop_key_id)
                        .map(|(_, v)| v)
                        .unwrap_or(PropValue::Null);
                    vals.push(found);
                }
            }
        } else {
            vals.resize(n, PropValue::Null);
        }

        let arc = Arc::new(vals);
        self.prop_col_cache.insert(cache_key, arc.clone());
        Ok(arc)
    }

    pub async fn project_raw_batch(
        &self,
        meta: &SegmentMeta,
        cols: &[&str],
    ) -> Result<RecordBatch, StorageError> {
        let vbytes = self.get_vortex_bytes(meta).await?;
        self.reader.project_from_vortex_bytes_sync(vbytes, cols)
    }

    pub fn cached_segment_ids(&self) -> HashMap<String, usize> {
        self.tier_a_cache
            .iter()
            .map(|(k, v)| ((*k).clone(), v.len()))
            .collect()
    }

    /// Gracefully shuts down the underlying `foyer` hybrid cache before runtime teardown.
    pub async fn close(&self) {
        let _ = self.hybrid.close().await;
    }
}
