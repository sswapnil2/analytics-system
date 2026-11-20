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
}

/// Selective projection of only the Tier A columns needed by a specific query (and optionally
/// only the surviving rows selected by late materialization).
#[derive(Debug, Default)]
pub struct TierASubset {
    pub len: usize,
    pub uid_internal: Option<Vec<u32>>,
    pub session_hash: Option<Vec<u64>>,
    pub event_id: Option<Vec<u16>>,
    pub event_ts_ms: Option<Vec<i64>>,
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

    /// Approximate heap bytes owned by these Tier A arrays.
    #[inline]
    pub fn approx_bytes(&self) -> usize {
        self.len().saturating_mul(4 + 8 + 2 + 8 + 4)
    }

    fn cache_weight(&self) -> u32 {
        self.approx_bytes().clamp(64, u32::MAX as usize) as u32
    }
}

/// Approximate heap bytes owned by a materialized property column, including
/// heap-allocated `String` payloads inside `PropValue::Str`.
pub fn prop_col_approx_bytes(v: &[PropValue]) -> usize {
    let base = v.len().saturating_mul(std::mem::size_of::<PropValue>());
    let heap: usize = v
        .iter()
        .map(|pv| match pv {
            PropValue::Str(s) => s.capacity(),
            _ => 0,
        })
        .sum();
    base.saturating_add(heap)
}

/// Hybrid RAM + NVMe Block Cache (`foyer`) + byte-bounded decoded columnar caches (`moka`).
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
        // Partition `memory_bytes` strictly across all four in-memory caches so their combined
        // footprint stays within the configured budget (30% foyer + 35% tier_a + 25% prop + 10% inv).
        let total_budget = memory_bytes.max(16 * 1024 * 1024);
        let hybrid_budget = ((total_budget * 30) / 100).max(4 * 1024 * 1024);
        let tier_a_budget = ((total_budget as u64 * 35) / 100).max(4 * 1024 * 1024);
        let prop_budget = ((total_budget as u64 * 25) / 100).max(4 * 1024 * 1024);
        let inv_budget = (total_budget as u64)
            .saturating_sub(hybrid_budget as u64 + tier_a_budget + prop_budget)
            .max(1024 * 1024);

        let policy = if disk_cache_dir.is_some() {
            HybridCachePolicy::WriteOnInsertion
        } else {
            HybridCachePolicy::WriteOnEviction
        };
        let mem_builder = HybridCacheBuilder::new()
            .with_name("pulse-vortex-cache")
            .with_policy(policy)
            .with_flush_on_close(false)
            .memory(hybrid_budget)
            .with_eviction_config(S3FifoConfig::default())
            .with_weighter(|k: &String, v: &Vec<u8>| k.len() + v.capacity());

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

        let tier_a_cache = Cache::builder()
            .max_capacity(tier_a_budget)
            .weigher(|k: &String, v: &Arc<TierAColumns>| {
                (k.len() as u32).saturating_add(v.cache_weight())
            })
            .build();

        let inverted_cache = Cache::builder()
            .max_capacity(inv_budget)
            .weigher(|k: &String, v: &Arc<SegmentInvertedIndex>| {
                let bm_bytes: usize = v
                    .bitmaps
                    .iter()
                    .map(|((_, _, s), bm)| 48 + s.capacity() + bm.serialized_size())
                    .sum();
                (k.len().saturating_add(bm_bytes)).clamp(64, u32::MAX as usize) as u32
            })
            .build();

        let prop_col_cache = Cache::builder()
            .max_capacity(prop_budget)
            .weigher(|(k, _): &(String, u16), v: &Arc<Vec<PropValue>>| {
                (k.len().saturating_add(prop_col_approx_bytes(v))).clamp(64, u32::MAX as usize)
                    as u32
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
        let key = format!("sidx:{}", meta.segment_id);
        let raw_bytes: Vec<u8> = if let Some(plain) = self.hybrid_get_unsealed(&key).await {
            plain
        } else {
            let path = object_store::path::Path::from(meta.object_path.as_str());
            let raw = self
                .reader
                .object_store()
                .get_range(&path, meta.session_idx_offset..meta.file_size)
                .await?;
            self.hybrid_insert_sealed(key, &raw);
            raw.to_vec()
        };
        let decoded = tokio::task::spawn_blocking(move || decode_session_index(&raw_bytes))
            .await
            .map_err(|e| {
                StorageError::Corrupted(format!("session index decode panicked: {e}"))
            })??;
        Ok(Arc::new(decoded))
    }

    pub async fn get_inverted_index(
        &self,
        meta: &SegmentMeta,
    ) -> Result<Arc<SegmentInvertedIndex>, StorageError> {
        if let Some(inv) = self.inverted_cache.get(&meta.segment_id) {
            return Ok(inv);
        }
        let key = format!("iidx:{}", meta.segment_id);
        let raw_bytes: Vec<u8> = if let Some(plain) = self.hybrid_get_unsealed(&key).await {
            plain
        } else {
            let path = object_store::path::Path::from(meta.object_path.as_str());
            let raw = self
                .reader
                .object_store()
                .get_range(&path, meta.inverted_idx_offset..meta.session_idx_offset)
                .await?;
            self.hybrid_insert_sealed(key, &raw);
            raw.to_vec()
        };
        let decoded = tokio::task::spawn_blocking(move || SegmentInvertedIndex::decode(&raw_bytes))
            .await
            .map_err(|e| {
                StorageError::Corrupted(format!("inverted index decode panicked: {e}"))
            })??;
        let inv = Arc::new(decoded);
        self.inverted_cache
            .insert(meta.segment_id.clone(), inv.clone());
        Ok(inv)
    }

    pub async fn get_tier_a(&self, meta: &SegmentMeta) -> Result<Arc<TierAColumns>, StorageError> {
        if let Some(cached) = self.tier_a_cache.get(&meta.segment_id) {
            return Ok(cached);
        }
        let vbytes = self.get_vortex_bytes(meta).await?;
        let reader = self.reader.clone();
        let ta = tokio::task::spawn_blocking(move || -> Result<TierAColumns, StorageError> {
            let batch = reader.project_from_vortex_bytes_sync(
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

            Ok(TierAColumns {
                uid_internal: uid_col.values().to_vec(),
                session_hash: shash_col.values().to_vec(),
                event_id: ev_col.values().to_vec(),
                event_ts_ms: ts_col.values().to_vec(),
                replay_offset_kb: rep_col.values().to_vec(),
            })
        })
        .await
        .map_err(|e| StorageError::Corrupted(format!("tier_a decode panicked: {e}")))??;

        let ta = Arc::new(ta);
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
            .find(|c| c.prop_key_id == prop_key_id)
            .cloned();

        if let Some(entry) = tb_entry {
            if !entry.has_i64 && !entry.has_f64 && !entry.has_bool && !entry.has_str {
                // Only `has_null` was present in Tier B (`P2-09l`).
                let empty = Arc::new(vec![PropValue::Null; n]);
                self.prop_col_cache.insert(cache_key, empty.clone());
                return Ok(empty);
            }

            let vbytes = self.get_vortex_bytes(meta).await?;
            let reader = self.reader.clone();
            let vals =
                tokio::task::spawn_blocking(move || -> Result<Vec<PropValue>, StorageError> {
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

                    let batch = reader.project_from_vortex_bytes_sync(vbytes, &cols_to_read)?;
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
                                vals.push(PropValue::Float(
                                    c.as_primitive::<Float64Type>().value(row),
                                ));
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
                    Ok(vals)
                })
                .await
                .map_err(|e| StorageError::Corrupted(format!("tier_b decode panicked: {e}")))??;

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
        let reader = self.reader.clone();
        let vals = tokio::task::spawn_blocking(move || -> Result<Vec<PropValue>, StorageError> {
            let batch: RecordBatch =
                reader.project_from_vortex_bytes_sync(vbytes, &["tier_c_overflow"])?;
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
            Ok(vals)
        })
        .await
        .map_err(|e| StorageError::Corrupted(format!("tier_c decode panicked: {e}")))??;

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
        let reader = self.reader.clone();
        let owned_cols: Vec<String> = cols.iter().map(|s| (*s).to_string()).collect();
        tokio::task::spawn_blocking(move || {
            let col_refs: Vec<&str> = owned_cols.iter().map(|s| s.as_str()).collect();
            reader.project_from_vortex_bytes_sync(vbytes, &col_refs)
        })
        .await
        .map_err(|e| StorageError::Corrupted(format!("raw batch decode panicked: {e}")))?
    }

    /// Projects only the requested Tier A columns (and optionally only sorted `row_indices` via
    /// Vortex `Selection::IncludeByIndex` late materialization when selective).
    pub async fn get_tier_a_subset(
        &self,
        meta: &SegmentMeta,
        need_uid: bool,
        need_session: bool,
        need_event: bool,
        need_ts: bool,
        row_indices: Option<&[u64]>,
    ) -> Result<TierASubset, StorageError> {
        let total_rows = meta.footer.row_count as usize;
        if let Some(indices) = row_indices {
            if indices.is_empty() {
                return Ok(TierASubset::default());
            }
        }

        // Fast path: if full Tier A is already warm in cache, slice/gather without touching Vortex.
        if let Some(cached) = self.tier_a_cache.get(&meta.segment_id) {
            return Ok(match row_indices {
                Some(indices) => TierASubset {
                    len: indices.len(),
                    uid_internal: need_uid.then(|| {
                        indices
                            .iter()
                            .map(|&i| cached.uid_internal[i as usize])
                            .collect()
                    }),
                    session_hash: need_session.then(|| {
                        indices
                            .iter()
                            .map(|&i| cached.session_hash[i as usize])
                            .collect()
                    }),
                    event_id: need_event.then(|| {
                        indices
                            .iter()
                            .map(|&i| cached.event_id[i as usize])
                            .collect()
                    }),
                    event_ts_ms: need_ts.then(|| {
                        indices
                            .iter()
                            .map(|&i| cached.event_ts_ms[i as usize])
                            .collect()
                    }),
                },
                None => TierASubset {
                    len: cached.len(),
                    uid_internal: need_uid.then(|| cached.uid_internal.clone()),
                    session_hash: need_session.then(|| cached.session_hash.clone()),
                    event_id: need_event.then(|| cached.event_id.clone()),
                    event_ts_ms: need_ts.then(|| cached.event_ts_ms.clone()),
                },
            });
        }

        let vbytes = self.get_vortex_bytes(meta).await?;
        let reader = self.reader.clone();
        // Use Vortex Selection::IncludeByIndex when selective (< 50% of rows); otherwise scan full projected columns.
        let use_vortex_sel = row_indices.is_some_and(|idx| idx.len() * 2 < total_rows);
        let owned_indices: Option<Vec<u64>> = row_indices.map(|s| s.to_vec());

        tokio::task::spawn_blocking(move || -> Result<TierASubset, StorageError> {
            let mut cols: Vec<&str> = Vec::with_capacity(4);
            if need_uid {
                cols.push("uid_internal");
            }
            if need_session {
                cols.push("session_hash");
            }
            if need_event {
                cols.push("event_id");
            }
            if need_ts {
                cols.push("event_ts_ms");
            }
            if cols.is_empty() {
                let len = owned_indices.as_ref().map_or(total_rows, Vec::len);
                return Ok(TierASubset {
                    len,
                    ..Default::default()
                });
            }

            let sel_slice = if use_vortex_sel {
                owned_indices.as_deref()
            } else {
                None
            };
            let batch =
                reader.project_from_vortex_bytes_with_selection_sync(vbytes, &cols, sel_slice)?;

            let gather_u32 = |name: &str| -> Result<Vec<u32>, StorageError> {
                let arr = batch
                    .column_by_name(name)
                    .ok_or_else(|| StorageError::Corrupted(format!("missing {name}")))?
                    .as_primitive::<UInt32Type>();
                let vals = arr.values();
                Ok(if !use_vortex_sel {
                    if let Some(ref idx) = owned_indices {
                        idx.iter().map(|&i| vals[i as usize]).collect()
                    } else {
                        vals.to_vec()
                    }
                } else {
                    vals.to_vec()
                })
            };
            let gather_u64 = |name: &str| -> Result<Vec<u64>, StorageError> {
                let arr = batch
                    .column_by_name(name)
                    .ok_or_else(|| StorageError::Corrupted(format!("missing {name}")))?
                    .as_primitive::<UInt64Type>();
                let vals = arr.values();
                Ok(if !use_vortex_sel {
                    if let Some(ref idx) = owned_indices {
                        idx.iter().map(|&i| vals[i as usize]).collect()
                    } else {
                        vals.to_vec()
                    }
                } else {
                    vals.to_vec()
                })
            };
            let gather_u16 = |name: &str| -> Result<Vec<u16>, StorageError> {
                let arr = batch
                    .column_by_name(name)
                    .ok_or_else(|| StorageError::Corrupted(format!("missing {name}")))?
                    .as_primitive::<UInt16Type>();
                let vals = arr.values();
                Ok(if !use_vortex_sel {
                    if let Some(ref idx) = owned_indices {
                        idx.iter().map(|&i| vals[i as usize]).collect()
                    } else {
                        vals.to_vec()
                    }
                } else {
                    vals.to_vec()
                })
            };
            let gather_i64 = |name: &str| -> Result<Vec<i64>, StorageError> {
                let arr = batch
                    .column_by_name(name)
                    .ok_or_else(|| StorageError::Corrupted(format!("missing {name}")))?
                    .as_primitive::<Int64Type>();
                let vals = arr.values();
                Ok(if !use_vortex_sel {
                    if let Some(ref idx) = owned_indices {
                        idx.iter().map(|&i| vals[i as usize]).collect()
                    } else {
                        vals.to_vec()
                    }
                } else {
                    vals.to_vec()
                })
            };

            let uid_internal = need_uid.then(|| gather_u32("uid_internal")).transpose()?;
            let session_hash = need_session
                .then(|| gather_u64("session_hash"))
                .transpose()?;
            let event_id = need_event.then(|| gather_u16("event_id")).transpose()?;
            let event_ts_ms = need_ts.then(|| gather_i64("event_ts_ms")).transpose()?;
            let len = owned_indices.as_ref().map_or(batch.num_rows(), Vec::len);

            Ok(TierASubset {
                len,
                uid_internal,
                session_hash,
                event_id,
                event_ts_ms,
            })
        })
        .await
        .map_err(|e| StorageError::Corrupted(format!("tier_a subset decode panicked: {e}")))?
    }

    /// Projects a property column for only the sorted `row_indices` using Vortex
    /// `Selection::IncludeByIndex` when selective (< 50% of rows), avoiding whole-block decode.
    pub async fn get_property_column_selected(
        &self,
        meta: &SegmentMeta,
        prop_key_id: u16,
        row_indices: &[u64],
    ) -> Result<Vec<PropValue>, StorageError> {
        if row_indices.is_empty() {
            return Ok(Vec::new());
        }
        let cache_key = (meta.segment_id.clone(), prop_key_id);
        if let Some(cached) = self.prop_col_cache.get(&cache_key) {
            return Ok(row_indices
                .iter()
                .map(|&i| cached.get(i as usize).cloned().unwrap_or(PropValue::Null))
                .collect());
        }

        let total_rows = meta.footer.row_count as usize;
        if row_indices.len() * 2 >= total_rows {
            let full = self.get_property_column(meta, prop_key_id).await?;
            return Ok(row_indices
                .iter()
                .map(|&i| full.get(i as usize).cloned().unwrap_or(PropValue::Null))
                .collect());
        }

        let tb_entry = meta
            .footer
            .tier_b_columns
            .iter()
            .find(|c| c.prop_key_id == prop_key_id)
            .cloned();

        if let Some(entry) = tb_entry {
            if !entry.has_i64 && !entry.has_f64 && !entry.has_bool && !entry.has_str {
                return Ok(vec![PropValue::Null; row_indices.len()]);
            }
            let vbytes = self.get_vortex_bytes(meta).await?;
            let reader = self.reader.clone();
            let owned_idx = row_indices.to_vec();
            return tokio::task::spawn_blocking(move || -> Result<Vec<PropValue>, StorageError> {
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

                let batch = reader.project_from_vortex_bytes_with_selection_sync(
                    vbytes,
                    &cols_to_read,
                    Some(&owned_idx),
                )?;
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

                let n = batch.num_rows();
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
                Ok(vals)
            })
            .await
            .map_err(|e| StorageError::Corrupted(format!("selected prop decode panicked: {e}")))?;
        }

        let bit = tier_c_key_bit(prop_key_id);
        if !meta.footer.has_tier_c_column || (meta.footer.tier_c_bloom_64 & bit) == 0 {
            return Ok(vec![PropValue::Null; row_indices.len()]);
        }

        let vbytes = self.get_vortex_bytes(meta).await?;
        let reader = self.reader.clone();
        let owned_idx = row_indices.to_vec();
        tokio::task::spawn_blocking(move || -> Result<Vec<PropValue>, StorageError> {
            let batch = reader.project_from_vortex_bytes_with_selection_sync(
                vbytes,
                &["tier_c_overflow"],
                Some(&owned_idx),
            )?;
            let n = batch.num_rows();
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
            Ok(vals)
        })
        .await
        .map_err(|e| StorageError::Corrupted(format!("selected tier_c decode panicked: {e}")))?
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
