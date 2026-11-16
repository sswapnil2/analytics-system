use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use parking_lot::Mutex;
use pulse_storage::{event_uuid_fingerprint, route_bucket, StorageEngine, StorageError};
use pulse_wire::{
    mask_input_value, DictionaryRegistry, PropValue, ReplayChunk, WireBatch, WireError, WireEvent,
    WireframeOp,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::dedup::{normalize_event_timestamp, DedupRegistry};
use crate::governance::{DiscoveredEventSchema, GovernanceProvider, ProjectLexiconRules};

/// Per-batch and per-project schema cardinality caps preventing dictionary exhaustion (`P1-12`).
pub const MAX_NEW_EVENTS_PER_BATCH: usize = 128;
pub const MAX_NEW_PROP_KEYS_PER_BATCH: usize = 256;
pub const MAX_EVENTS_PER_PROJECT: usize = 8_192;
pub const MAX_PROP_KEYS_PER_PROJECT: usize = 16_384;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("missing or invalid x-pulse-key API key")]
    Unauthorized,

    #[error("wire decode error: {0}")]
    Wire(#[from] WireError),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("identity error: {0}")]
    Identity(#[from] pulse_identity::IdentityError),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("schema governance limit exceeded: {0}")]
    SchemaGovernanceLimit(String),

    #[error("quic transport error: {0}")]
    Quic(String),
}

pub type Result<T> = std::result::Result<T, IngestError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestResponse {
    pub status: String,
    pub accepted: usize,
    pub deduped: usize,
    pub dropped: usize,
    pub server_ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentifyRequest {
    pub anon_id: String,
    pub user_id: String,
    #[serde(default)]
    pub routing_seed: Option<String>,
    #[serde(default)]
    pub anon_routing_seed: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentifyResponse {
    pub status: String,
    pub canonical_uid: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayIngestResponse {
    pub status: String,
    pub object_path: String,
    pub byte_offset: u64,
    pub replay_offset_kb: u32,
    pub chunk_bytes: usize,
}

#[derive(Default)]
pub struct IngestMetrics {
    pub accepted_total: AtomicU64,
    pub deduped_total: AtomicU64,
    pub dropped_total: AtomicU64,
    pub batches_total: AtomicU64,
    eps_buckets: Mutex<[(i64, u64); 10]>,
}

impl IngestMetrics {
    fn record(&self, accepted: u64, deduped: u64, dropped: u64, now_ms: i64) {
        self.accepted_total.fetch_add(accepted, Ordering::Relaxed);
        self.deduped_total.fetch_add(deduped, Ordering::Relaxed);
        self.dropped_total.fetch_add(dropped, Ordering::Relaxed);
        self.batches_total.fetch_add(1, Ordering::Relaxed);

        let sec = now_ms / 1_000;
        let idx = (sec.rem_euclid(10)) as usize;
        let mut guard = self.eps_buckets.lock();
        if guard[idx].0 == sec {
            guard[idx].1 += accepted;
        } else {
            guard[idx] = (sec, accepted);
        }
    }

    pub fn current_eps(&self) -> u64 {
        let now_sec = Utc::now().timestamp();
        let guard = self.eps_buckets.lock();
        let mut sum = 0u64;
        let mut active_secs = 0u64;
        for &(sec, count) in guard.iter() {
            if sec >= now_sec - 5 && sec <= now_sec {
                sum += count;
                active_secs += 1;
            }
        }
        if active_secs == 0 {
            0
        } else {
            sum / active_secs.max(1)
        }
    }
}

/// Core transport-agnostic Ingestion Pipeline shared by HTTP/1.1, HTTP/2, QUIC, and Seeder.
pub struct IngestService {
    storage: Arc<StorageEngine>,
    governance: Arc<dyn GovernanceProvider>,
    dedup: DedupRegistry,
    seeded_projects: DashMap<String, bool>,
    dict_registry: Arc<DictionaryRegistry>,
    metrics: IngestMetrics,
}

impl IngestService {
    pub fn new(storage: Arc<StorageEngine>, governance: Arc<dyn GovernanceProvider>) -> Arc<Self> {
        Arc::new(Self {
            storage,
            governance,
            dedup: DedupRegistry::new(),
            seeded_projects: DashMap::new(),
            dict_registry: Arc::new(DictionaryRegistry::default()),
            metrics: IngestMetrics::default(),
        })
    }

    pub fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    pub fn governance(&self) -> &Arc<dyn GovernanceProvider> {
        &self.governance
    }

    pub fn dict_registry(&self) -> &Arc<DictionaryRegistry> {
        &self.dict_registry
    }

    pub fn metrics(&self) -> &IngestMetrics {
        &self.metrics
    }

    async fn ensure_project_ready(
        &self,
        project_id: &str,
        now_ms: i64,
    ) -> Result<Arc<pulse_storage::ProjectStorage>> {
        let ps = self.storage.get_or_open_project(project_id).await?;
        if !self.seeded_projects.contains_key(project_id) {
            let recovered = ps.take_recovered_uuid_fps();
            if !recovered.is_empty() {
                self.dedup.seed_recovered(project_id, &recovered, now_ms);
            }
            self.seeded_projects.insert(project_id.to_string(), true);
        }
        Ok(ps)
    }

    pub async fn ingest_raw(
        &self,
        api_key: &str,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Result<IngestResponse> {
        let project_id = self
            .governance
            .authenticate_api_key(api_key)
            .await
            .ok_or(IngestError::Unauthorized)?;
        let batch = WireBatch::decode_auto(body, content_type, &self.dict_registry)?;
        self.ingest_for_project(&project_id, batch, Utc::now().timestamp_millis())
            .await
    }

    pub async fn ingest_for_project(
        &self,
        project_id: &str,
        batch: WireBatch,
        server_now_ms: i64,
    ) -> Result<IngestResponse> {
        let ps = self.ensure_project_ready(project_id, server_now_ms).await?;
        let id_graph = self
            .storage
            .identity_router()
            .get_or_open(project_id)
            .await?;
        let rules = self.governance.lexicon_rules(project_id);
        let num_buckets = ps.config().num_buckets;
        let dict = ps.dictionary();

        let mut accepted = 0usize;
        let mut deduped = 0usize;
        let mut dropped = 0usize;

        type PropTypeList = Vec<(String, &'static str)>;
        let mut by_bucket: BTreeMap<u8, Vec<(WireEvent, u32, u64)>> = BTreeMap::new();
        let mut intra_batch_seen: HashSet<(u8, u64)> = HashSet::with_capacity(batch.len());
        let mut new_events_in_batch: HashSet<String> = HashSet::new();
        let mut new_prop_keys_in_batch: HashSet<String> = HashSet::new();

        for mut ev in batch.events {
            // 1. Lexicon Governance Check
            if !rules.apply(&mut ev) {
                dropped += 1;
                continue;
            }

            // 2. Deterministic Bucket Routing & Non-Mutating Dedup Pre-Check (`P1-01`)
            let seed = if ev.routing_seed.is_empty() {
                ev.distinct_id.as_str()
            } else {
                ev.routing_seed.as_str()
            };
            let bucket_id = route_bucket(seed, num_buckets);
            let fp = event_uuid_fingerprint(&ev.event_uuid);

            if !intra_batch_seen.insert((bucket_id, fp))
                || self
                    .dedup
                    .contains(project_id, bucket_id, fp, server_now_ms)
            {
                deduped += 1;
                continue;
            }

            // 3. Enforce per-batch and per-project schema cardinality governance (`P1-12`)
            if dict.lookup_event(&ev.event_name).is_none() {
                new_events_in_batch.insert(ev.event_name.clone());
                if new_events_in_batch.len() > MAX_NEW_EVENTS_PER_BATCH
                    || dict.event_count() + new_events_in_batch.len() > MAX_EVENTS_PER_PROJECT
                {
                    return Err(IngestError::SchemaGovernanceLimit(format!(
                        "batch exceeds maximum new or total event names ({MAX_NEW_EVENTS_PER_BATCH} new/batch, {MAX_EVENTS_PER_PROJECT}/project)"
                    )));
                }
            }
            for (k, _) in &ev.props {
                if dict.lookup_prop_key(k).is_none() {
                    new_prop_keys_in_batch.insert(k.clone());
                    if new_prop_keys_in_batch.len() > MAX_NEW_PROP_KEYS_PER_BATCH
                        || dict.prop_key_count() + new_prop_keys_in_batch.len()
                            > MAX_PROP_KEYS_PER_PROJECT
                    {
                        return Err(IngestError::SchemaGovernanceLimit(format!(
                            "batch exceeds maximum new or total property keys ({MAX_NEW_PROP_KEYS_PER_BATCH} new/batch, {MAX_PROP_KEYS_PER_PROJECT}/project)"
                        )));
                    }
                }
            }

            // 4. Mobile Clock-Skew Normalization (`P1-13`)
            ev.client_event_ts_ms = normalize_event_timestamp(
                ev.client_event_ts_ms,
                ev.client_sent_ts_ms,
                server_now_ms,
            );

            // 5. Dense u32 Identity Interning
            let uid = id_graph.get_or_intern(&ev.distinct_id, bucket_id);
            by_bucket.entry(bucket_id).or_default().push((ev, uid, fp));
        }

        // 6. Flush interned u32 identities durably BEFORE writing WAL or inline L1 segments (`P1-10`)!
        if !by_bucket.is_empty() {
            id_graph.flush().await?;
        }

        // 7. Under per-bucket append lock: re-check dedup -> durable WAL append -> L0 append -> commit dedup (`P1-01`, `P1-02`)!
        let mut discovery_map: HashMap<String, (u64, PropTypeList)> = HashMap::new();
        for (bucket_id, candidates) in by_bucket {
            let should_flush = {
                let append_guard = ps.lock_bucket_append(bucket_id).await;
                let mut to_append = Vec::with_capacity(candidates.len());
                let mut committed_fps = Vec::with_capacity(candidates.len());

                for (ev, uid, fp) in candidates {
                    if self
                        .dedup
                        .contains(project_id, bucket_id, fp, server_now_ms)
                    {
                        deduped += 1;
                        continue;
                    }
                    committed_fps.push(fp);
                    to_append.push((ev, uid));
                }

                if !to_append.is_empty() {
                    ps.append_bucket_events_locked(&append_guard, bucket_id, to_append.clone())
                        .await?;
                    self.dedup
                        .commit_batch(project_id, bucket_id, &committed_fps, server_now_ms);
                    accepted += to_append.len();

                    for (ev, _) in &to_append {
                        let disc =
                            discovery_map
                                .entry(ev.event_name.clone())
                                .or_insert_with(|| {
                                    let ptypes = ev
                                        .props
                                        .iter()
                                        .map(|(k, v)| {
                                            let t = match v {
                                                PropValue::Null => "null",
                                                PropValue::Bool(_) => "bool",
                                                PropValue::Int(_) => "int",
                                                PropValue::Float(_) => "float",
                                                PropValue::Str(_) => "string",
                                            };
                                            (k.clone(), t)
                                        })
                                        .collect();
                                    (0, ptypes)
                                });
                        disc.0 += 1;
                    }
                }
                ps.should_flush_bucket(bucket_id)
            };

            if should_flush {
                ps.flush_bucket(bucket_id).await?;
            }
        }

        if !discovery_map.is_empty() {
            let items = discovery_map
                .into_iter()
                .map(|(event_name, (count, props))| DiscoveredEventSchema {
                    project_id: project_id.to_string(),
                    event_name,
                    count,
                    props,
                })
                .collect();
            self.governance.record_discovery(items);
        }

        self.metrics.record(
            accepted as u64,
            deduped as u64,
            dropped as u64,
            server_now_ms,
        );

        Ok(IngestResponse {
            status: "ok".to_string(),
            accepted,
            deduped,
            dropped,
            server_ts_ms: server_now_ms,
        })
    }

    pub async fn identify(&self, api_key: &str, req: IdentifyRequest) -> Result<IdentifyResponse> {
        let project_id = self
            .governance
            .authenticate_api_key(api_key)
            .await
            .ok_or(IngestError::Unauthorized)?;
        self.identify_for_project(&project_id, req).await
    }

    pub async fn identify_for_project(
        &self,
        project_id: &str,
        req: IdentifyRequest,
    ) -> Result<IdentifyResponse> {
        let ps = self
            .ensure_project_ready(project_id, Utc::now().timestamp_millis())
            .await?;
        let num_buckets = ps.config().num_buckets;
        let id_graph = self
            .storage
            .identity_router()
            .get_or_open(project_id)
            .await?;

        let anon_seed = req
            .anon_routing_seed
            .as_deref()
            .or(req.routing_seed.as_deref())
            .unwrap_or(&req.anon_id);
        let user_seed = req.routing_seed.as_deref().unwrap_or(&req.user_id);

        let anon_bucket = route_bucket(anon_seed, num_buckets);
        let user_bucket = route_bucket(user_seed, num_buckets);

        let canonical_uid = id_graph
            .alias_durable(&req.anon_id, &req.user_id, anon_bucket, user_bucket)
            .await?;

        Ok(IdentifyResponse {
            status: "ok".to_string(),
            canonical_uid,
        })
    }

    pub async fn ingest_replay(
        &self,
        api_key: &str,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Result<ReplayIngestResponse> {
        let project_id = self
            .governance
            .authenticate_api_key(api_key)
            .await
            .ok_or(IngestError::Unauthorized)?;
        self.ingest_replay_for_project(&project_id, content_type, body)
            .await
    }

    pub async fn ingest_replay_for_project(
        &self,
        project_id: &str,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Result<ReplayIngestResponse> {
        let mut chunk = ReplayChunk::decode_auto(body, content_type)?;
        chunk.project_id = project_id.to_string();
        let rules = self.governance.lexicon_rules(project_id);
        apply_lexicon_to_replay_chunk(&mut chunk, &rules);
        let res = self.storage.append_replay_chunk(&chunk).await?;
        Ok(ReplayIngestResponse {
            status: "ok".to_string(),
            object_path: res.object_path,
            byte_offset: res.byte_offset,
            replay_offset_kb: res.replay_offset_kb,
            chunk_bytes: res.chunk_bytes,
        })
    }
}

/// Applies project Lexicon PII rules and input masking to wireframe replay labels on ingest (`P3-01`).
fn apply_lexicon_to_replay_chunk(chunk: &mut ReplayChunk, rules: &ProjectLexiconRules) {
    let mask_label = |label: &mut String, kind: pulse_wire::NodeKind| {
        if label.is_empty() || label == "████" {
            return;
        }
        if kind == pulse_wire::NodeKind::MaskedInput {
            *label = mask_input_value(label);
            return;
        }
        let lower = label.to_ascii_lowercase();
        for prop in &rules.pii_masked_properties {
            if !prop.is_empty() && lower.contains(&prop.to_ascii_lowercase()) {
                *label = mask_input_value(label);
                return;
            }
        }
        for dropped in &rules.dropped_properties {
            if !dropped.is_empty() && lower.contains(&dropped.to_ascii_lowercase()) {
                label.clear();
                return;
            }
        }
    };

    for op in &mut chunk.ops {
        match op {
            WireframeOp::FullSnapshot { nodes, .. } => {
                for node in nodes {
                    mask_label(&mut node.masked_label, node.kind);
                }
            }
            WireframeOp::NodeUpsert(node) => {
                mask_label(&mut node.masked_label, node.kind);
            }
            _ => {}
        }
    }
}
