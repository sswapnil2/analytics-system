use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, UInt16Type, UInt32Type, UInt64Type};
use arrow_array::{Array as ArrowArray, RecordBatch};
use bytes::Bytes;
use dashmap::DashMap;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, NoopDeviceBuilder, PsyncIoEngineConfig,
};
use pulse_storage::{
    decode_session_index, decode_tier_c_row, extract_arrow_str, tier_c_key_bit,
    SegmentInvertedIndex, SegmentMeta, SegmentSessionIndex, StorageError, TierBColumnEntry,
    VortexSegmentReader,
};
use pulse_wire::PropValue;

/// Dense contiguous Tier A columns decoded from an immutable Vortex segment.
#[derive(Debug)]
pub struct TierAColumns {
    pub uid_internal: Vec<u32>,
    pub session_seq: Vec<u32>,
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
}

/// Hybrid RAM + NVMe Block Cache (`foyer`) + decoded columnar vector cache for sub-50ms analytical scans.
pub struct SegmentBlockCache {
    hybrid: HybridCache<String, Vec<u8>>,
    reader: Arc<VortexSegmentReader>,
    tier_a_cache: DashMap<String, Arc<TierAColumns>>,
    inverted_cache: DashMap<String, Arc<SegmentInvertedIndex>>,
    prop_col_cache: DashMap<(String, u16), Arc<Vec<PropValue>>>,
}

impl SegmentBlockCache {
    /// Opens a `foyer` HybridCache backed by RAM + optional NVMe directory (`cache_dir`).
    pub async fn open(
        reader: Arc<VortexSegmentReader>,
        memory_bytes: usize,
        disk_cache_dir: Option<&Path>,
        disk_bytes: usize,
    ) -> Result<Arc<Self>, String> {
        let mem_builder = HybridCacheBuilder::new()
            .with_name("pulse-vortex-cache")
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .with_flush_on_close(false)
            .memory(memory_bytes.max(4 * 1024 * 1024))
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
            let device = NoopDeviceBuilder::default()
                .build()
                .map_err(|e| e.to_string())?;
            mem_builder
                .storage()
                .with_io_engine_config(PsyncIoEngineConfig::new())
                .with_engine_config(BlockEngineConfig::new(device))
                .build()
                .await
                .map_err(|e| e.to_string())?
        };

        Ok(Arc::new(Self {
            hybrid,
            reader,
            tier_a_cache: DashMap::new(),
            inverted_cache: DashMap::new(),
            prop_col_cache: DashMap::new(),
        }))
    }

    pub fn reader(&self) -> &Arc<VortexSegmentReader> {
        &self.reader
    }

    pub async fn get_vortex_bytes(&self, meta: &SegmentMeta) -> Result<Bytes, StorageError> {
        let key = format!("vtx:{}", meta.segment_id);
        if let Ok(Some(entry)) = self.hybrid.get(&key).await {
            return Ok(Bytes::copy_from_slice(entry.value()));
        }
        let fetched = self.reader.fetch_vortex_bytes(meta).await?;
        self.hybrid.insert(key, fetched.to_vec());
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
        if let Ok(Some(entry)) = self.hybrid.get(&key).await {
            let decoded = decode_session_index(entry.value())?;
            return Ok(Arc::new(decoded));
        }
        let path = object_store::path::Path::from(meta.object_path.as_str());
        let raw = self
            .reader
            .object_store()
            .get_range(&path, meta.session_idx_offset..meta.footer_meta_offset)
            .await?;
        self.hybrid.insert(key, raw.to_vec());
        Ok(Arc::new(decode_session_index(&raw)?))
    }

    pub async fn get_inverted_index(
        &self,
        meta: &SegmentMeta,
    ) -> Result<Arc<SegmentInvertedIndex>, StorageError> {
        if let Some(inv) = self.inverted_cache.get(&meta.segment_id) {
            return Ok(inv.clone());
        }
        let key = format!("iidx:{}", meta.segment_id);
        let inv = if let Ok(Some(entry)) = self.hybrid.get(&key).await {
            Arc::new(SegmentInvertedIndex::decode(entry.value())?)
        } else {
            let path = object_store::path::Path::from(meta.object_path.as_str());
            let raw = self
                .reader
                .object_store()
                .get_range(&path, meta.inverted_idx_offset..meta.session_idx_offset)
                .await?;
            self.hybrid.insert(key, raw.to_vec());
            Arc::new(SegmentInvertedIndex::decode(&raw)?)
        };
        self.inverted_cache
            .insert(meta.segment_id.clone(), inv.clone());
        Ok(inv)
    }

    pub async fn get_tier_a(&self, meta: &SegmentMeta) -> Result<Arc<TierAColumns>, StorageError> {
        if let Some(cached) = self.tier_a_cache.get(&meta.segment_id) {
            return Ok(cached.clone());
        }
        let sidx = self.get_session_index(meta).await?;
        let vbytes = self.get_vortex_bytes(meta).await?;
        let batch = self.reader.project_from_vortex_bytes_sync(
            vbytes,
            &[
                "uid_internal",
                "session_seq",
                "session_hash",
                "event_id",
                "event_ts_ms",
                "replay_offset_kb",
            ],
        )?;

        let uid_col = batch
            .column_by_name("uid_internal")
            .unwrap()
            .as_primitive::<UInt32Type>();
        let sseq_col = batch
            .column_by_name("session_seq")
            .unwrap()
            .as_primitive::<UInt32Type>();
        let shash_col = batch
            .column_by_name("session_hash")
            .unwrap()
            .as_primitive::<UInt64Type>();
        let ev_col = batch
            .column_by_name("event_id")
            .unwrap()
            .as_primitive::<UInt16Type>();
        let ts_col = batch
            .column_by_name("event_ts_ms")
            .unwrap()
            .as_primitive::<Int64Type>();
        let rep_col = batch
            .column_by_name("replay_offset_kb")
            .unwrap()
            .as_primitive::<UInt32Type>();

        let ta = Arc::new(TierAColumns {
            uid_internal: uid_col.values().to_vec(),
            session_seq: sseq_col.values().to_vec(),
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
            return Ok(col.clone());
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
            .map(|e| (e.key().clone(), e.value().len()))
            .collect()
    }
}
