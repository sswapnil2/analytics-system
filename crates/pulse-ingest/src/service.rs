use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
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

/// Per-batch and per-project schema cardinality caps preventing dictionary exhaustion.
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

    #[error("ingest rate limit exceeded for this project; retry in {retry_after_secs}s")]
    RateLimited { retry_after_secs: u64 },
}

pub type Result<T> = std::result::Result<T, IngestError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestResponse {
    pub status: String,
    pub accepted: usize,
    pub deduped: usize,
    pub dropped: usize,
    pub server_ts_ms: i64,
    /// Events of the request that were refused, by index; the rest were processed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rejected: Vec<pulse_wire::RejectedEvent>,
    /// Segment `identify` / `alias` calls in the batch that were applied to profiles.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub profiles_updated: usize,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IdentifyRequest {
    pub anon_id: String,
    pub user_id: String,
    #[serde(default)]
    pub routing_seed: Option<String>,
    #[serde(default)]
    pub anon_routing_seed: Option<String>,
    #[serde(default)]
    pub user_properties: Option<serde_json::Value>,
    #[serde(default)]
    pub operations: Option<serde_json::Value>,
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

/// One WAL record holds at most about this many bytes of events. Requests beyond it wait for
/// the next record, so a single PUT stays bounded however many requests are queued.
const WAL_GROUP_MAX_BYTES: usize = 8 * 1024 * 1024;

/// A routed event waiting for its bucket's WAL: `(event, uuid fingerprint)`.
type Candidate = (WireEvent, u64);
/// What one request got from a bucket: `(accepted, deduped)`.
type BucketOutcome = (usize, usize);

/// One durable WAL record on its way from the appender (the request path) to the project's
/// processor task, which turns it into L0 rows.
struct LoggedRecord {
    bucket_id: u8,
    wal_seq: u64,
    events: Vec<WireEvent>,
    fps: Vec<u64>,
    bytes: usize,
}

/// The processor of one open project: the task consuming `tx`, tied to one `ProjectStorage`
/// instance (a project re-opened after a writer-lease takeover gets a new one).
struct Processor {
    ps: Arc<pulse_storage::ProjectStorage>,
    tx: tokio::sync::mpsc::UnboundedSender<LoggedRecord>,
}

/// One request's events for one bucket, queued until a WAL record that includes them is
/// durable. `Err` hands the events back after a failed write so the request retries itself.
struct GroupWaiter {
    candidates: Vec<Candidate>,
    bytes: usize,
    tx: tokio::sync::oneshot::Sender<std::result::Result<BucketOutcome, Vec<Candidate>>>,
}

type BucketQueues = Arc<Vec<Mutex<VecDeque<GroupWaiter>>>>;

fn approx_event_bytes(ev: &WireEvent) -> usize {
    64 + ev.distinct_id.len()
        + ev.session_id.len()
        + ev.event_name.len()
        + ev.routing_seed.len()
        + ev.props
            .iter()
            .map(|(k, v)| {
                16 + k.len()
                    + match v {
                        PropValue::Str(s) => s.len(),
                        _ => 8,
                    }
            })
            .sum::<usize>()
}

fn spawn_processor(
    ps: Arc<pulse_storage::ProjectStorage>,
    id_graph: Arc<pulse_identity::ProjectIdentityGraph>,
    governance: Arc<dyn GovernanceProvider>,
) -> Processor {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<LoggedRecord>();
    let task_ps = ps.clone();
    tokio::spawn(async move {
        let ps = task_ps;
        // ponytail: one task per project applies records one at a time. Split per bucket if a
        // single project's apply rate becomes the limit.
        while let Some(rec) = rx.recv().await {
            // Nothing below is lost by repeating it, and the record must not be skipped: it was
            // acknowledged. The only failure is the object store refusing a dead-letter write.
            while let Err(e) = apply_record(&ps, &id_graph, &rec).await {
                tracing::error!(
                    project_id = %ps.project_id(),
                    bucket_id = rec.bucket_id,
                    wal_seq = rec.wal_seq,
                    error = %e,
                    "applying a logged record failed; retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            record_discovery(governance.as_ref(), ps.project_id(), &rec.events);
            if ps.should_flush_bucket(rec.bucket_id) {
                ps.schedule_background_flush(rec.bucket_id);
            }
        }
    });
    Processor { ps, tx }
}

/// Interns ids and turns one logged record into L0 rows. Events over the project's schema caps
/// (the request path only checks those against the dictionary it sees at the time) go to the
/// dead-letter path instead of growing the dictionary.
async fn apply_record(
    ps: &Arc<pulse_storage::ProjectStorage>,
    id_graph: &pulse_identity::ProjectIdentityGraph,
    rec: &LoggedRecord,
) -> std::result::Result<(), StorageError> {
    let dict = ps.dictionary();
    let mut new_events: HashSet<&str> = HashSet::new();
    let mut new_keys: HashSet<&str> = HashSet::new();
    let mut events = Vec::with_capacity(rec.events.len());
    for (ev, &fp) in rec.events.iter().zip(&rec.fps) {
        let event_is_new = dict.lookup_event(&ev.event_name).is_none()
            && !new_events.contains(ev.event_name.as_str());
        let keys_new: Vec<&str> = ev
            .props
            .iter()
            .map(|(k, _)| k.as_str())
            .filter(|k| dict.lookup_prop_key(k).is_none() && !new_keys.contains(k))
            .collect();
        if (event_is_new && dict.event_count() + new_events.len() >= MAX_EVENTS_PER_PROJECT)
            || (!keys_new.is_empty()
                && dict.prop_key_count() + new_keys.len() + keys_new.len()
                    > MAX_PROP_KEYS_PER_PROJECT)
        {
            ps.dead_letter(
                rec.bucket_id,
                rec.wal_seq,
                ev.clone(),
                fp,
                &"project schema cap reached (event names or property keys)",
            )
            .await?;
            continue;
        }
        new_events.insert(&ev.event_name);
        new_keys.extend(keys_new);
        let uid = id_graph.get_or_intern(&ev.distinct_id, rec.bucket_id);
        events.push((ev, uid, fp));
    }
    ps.apply_logged(rec.bucket_id, rec.wal_seq, &events, rec.bytes)
        .await?;
    Ok(())
}

fn record_discovery(governance: &dyn GovernanceProvider, project_id: &str, events: &[WireEvent]) {
    type Seen = (u64, Vec<(String, &'static str)>);
    let mut discovery_map: HashMap<&str, Seen> = HashMap::new();
    for ev in events {
        let disc = discovery_map.entry(&ev.event_name).or_insert_with(|| {
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
    if !discovery_map.is_empty() {
        governance.record_discovery(
            discovery_map
                .into_iter()
                .map(|(event_name, (count, props))| DiscoveredEventSchema {
                    project_id: project_id.to_string(),
                    event_name: event_name.to_string(),
                    count,
                    props,
                })
                .collect(),
        );
    }
}

/// Core transport-agnostic Ingestion Pipeline shared by HTTP/1.1, HTTP/2, QUIC, and Seeder.
pub struct IngestService {
    storage: Arc<StorageEngine>,
    governance: Arc<dyn GovernanceProvider>,
    dedup: DedupRegistry,
    processors: DashMap<String, Processor>,
    dict_registry: Arc<DictionaryRegistry>,
    metrics: IngestMetrics,
    rate_limit: crate::rate_limit::RateLimiter,
    /// Per project, per bucket: requests waiting to share the next WAL record.
    wal_groups: DashMap<String, BucketQueues>,
}

impl IngestService {
    pub fn new(storage: Arc<StorageEngine>, governance: Arc<dyn GovernanceProvider>) -> Arc<Self> {
        Self::with_rate_limit(
            storage,
            governance,
            crate::rate_limit::RateLimiter::from_env(),
        )
    }

    pub fn with_rate_limit(
        storage: Arc<StorageEngine>,
        governance: Arc<dyn GovernanceProvider>,
        rate_limit: crate::rate_limit::RateLimiter,
    ) -> Arc<Self> {
        Arc::new(Self {
            storage,
            governance,
            dedup: DedupRegistry::new(),
            processors: DashMap::new(),
            dict_registry: Arc::new(DictionaryRegistry::default()),
            metrics: IngestMetrics::default(),
            rate_limit,
            wal_groups: DashMap::new(),
        })
    }

    /// Charges a request from the public SDK endpoints against its project's rate limit.
    fn charge(&self, project_id: &str, units: u64) -> Result<()> {
        self.rate_limit
            .charge(project_id, units)
            .map_err(|retry_after_secs| IngestError::RateLimited { retry_after_secs })
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
        // Empty except right after an open, including a re-open after a writer-lease takeover.
        let recovered = ps.take_recovered_uuid_fps();
        if !recovered.is_empty() {
            self.dedup.seed_recovered(project_id, &recovered, now_ms);
        }
        Ok(ps)
    }

    /// The processor for `ps`, started on first use and restarted when the project was re-opened.
    async fn processor_for(
        &self,
        project_id: &str,
        ps: &Arc<pulse_storage::ProjectStorage>,
    ) -> Result<tokio::sync::mpsc::UnboundedSender<LoggedRecord>> {
        if let Some(p) = self.processors.get(project_id) {
            if Arc::ptr_eq(&p.ps, ps) {
                return Ok(p.tx.clone());
            }
        }
        let id_graph = self
            .storage
            .identity_router()
            .get_or_open(project_id)
            .await?;
        let mut entry = self
            .processors
            .entry(project_id.to_string())
            .or_insert_with(|| {
                spawn_processor(ps.clone(), id_graph.clone(), self.governance.clone())
            });
        if !Arc::ptr_eq(&entry.ps, ps) {
            // Dropping the old sender ends the old task once its queue is drained.
            *entry = spawn_processor(ps.clone(), id_graph, self.governance.clone());
        }
        Ok(entry.tx.clone())
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
        self.charge(&project_id, batch.events.len() as u64)?;
        self.ingest_for_project(&project_id, batch, Utc::now().timestamp_millis())
            .await
    }

    pub async fn ingest_for_project(
        &self,
        project_id: &str,
        mut batch: WireBatch,
        server_now_ms: i64,
    ) -> Result<IngestResponse> {
        let ps = self.ensure_project_ready(project_id, server_now_ms).await?;
        let rules = self.governance.lexicon_rules(project_id);
        let num_buckets = ps.config().num_buckets;
        let dict = ps.dictionary();

        let mut accepted = 0usize;
        let mut deduped = 0usize;
        let mut dropped = 0usize;
        let rejected = std::mem::take(&mut batch.rejected);
        let profile_calls = std::mem::take(&mut batch.profile_calls);

        let mut by_bucket: BTreeMap<u8, Vec<Candidate>> = BTreeMap::new();
        let mut intra_batch_seen: HashSet<(u8, u64)> = HashSet::with_capacity(batch.len());
        let mut new_events_in_batch: HashSet<String> = HashSet::new();
        let mut new_prop_keys_in_batch: HashSet<String> = HashSet::new();

        for mut ev in batch.events {
            // 1. Lexicon Governance Check
            if !rules.apply(&mut ev) {
                dropped += 1;
                continue;
            }

            // 2. Deterministic Bucket Routing & Non-Mutating Dedup Pre-Check
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

            // 3. Enforce per-batch and per-project schema cardinality governance
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

            // 4. Mobile Clock-Skew Normalization
            ev.client_event_ts_ms = normalize_event_timestamp(
                ev.client_event_ts_ms,
                ev.client_sent_ts_ms,
                server_now_ms,
            );

            // 5. Identity interning, row building and L0 insertion happen in the processor.
            by_bucket.entry(bucket_id).or_default().push((ev, fp));
        }

        // 6. Enforce bounded L0 high-water backpressure BEFORE flushing identities or appending WALs.
        for &bucket_id in by_bucket.keys() {
            ps.check_l0_backpressure(bucket_id)?;
        }

        // 8. Group commit per bucket (`P0-2`). Each request queues its events, then takes the bucket
        //    append lock. Whoever holds the lock writes ONE WAL record for everything queued (up to
        //    `WAL_GROUP_MAX_BYTES`), so there is one PUT in flight per bucket log and requests that
        //    arrive during it share the next one. Under the lock (`P1-01`, `P1-02`): re-check dedup
        //    -> durable WAL append -> commit dedup -> hand the record to the processor. A request
        //    is answered once the record holding its events is durable; the processor makes it
        //    queryable (interning, rows, L0, flush scheduling) in WAL order, off the request path.
        let processor = self.processor_for(project_id, &ps).await?;
        let queues = self
            .wal_groups
            .entry(project_id.to_string())
            .or_insert_with(|| {
                Arc::new(
                    (0..num_buckets)
                        .map(|_| Mutex::new(VecDeque::new()))
                        .collect(),
                )
            })
            .clone();
        let bucket_results =
            futures::future::try_join_all(by_bucket.into_iter().map(|(bucket_id, candidates)| {
                let ps = ps.clone();
                let queues = queues.clone();
                let processor = processor.clone();
                async move {
                    let queue = &queues[bucket_id as usize];
                    let enqueue = |candidates: Vec<Candidate>| {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        let bytes = candidates
                            .iter()
                            .map(|(ev, _)| approx_event_bytes(ev))
                            .sum();
                        queue.lock().push_back(GroupWaiter {
                            candidates,
                            bytes,
                            tx,
                        });
                        rx
                    };
                    let mut rx = enqueue(candidates);
                    let append_guard = ps.lock_bucket_append(bucket_id).await;
                    let outcome = loop {
                        use tokio::sync::oneshot::error::TryRecvError;
                        match rx.try_recv() {
                            // An earlier lock holder wrote our events.
                            Ok(Ok(outcome)) => break outcome,
                            // The record carrying our events failed: write them ourselves.
                            Ok(Err(candidates)) => rx = enqueue(candidates),
                            Err(TryRecvError::Empty) => {}
                            Err(TryRecvError::Closed) => {
                                return Err(IngestError::Storage(StorageError::Io(
                                    std::io::Error::other(
                                        "WAL group write was abandoned before completing; retry",
                                    ),
                                )))
                            }
                        }

                        let mut group = Vec::new();
                        let mut group_bytes = 0usize;
                        {
                            let mut q = queue.lock();
                            while let Some(w) = q.front() {
                                if !group.is_empty() && group_bytes + w.bytes > WAL_GROUP_MAX_BYTES
                                {
                                    break;
                                }
                                group_bytes += w.bytes;
                                group.extend(q.pop_front());
                            }
                        }

                        let mut seen_in_group: HashSet<u64> = HashSet::new();
                        let mut to_append = Vec::new();
                        let mut committed_fps = Vec::new();
                        // Per waiter: where its events start in `to_append`, and its dedup count.
                        let mut spans = Vec::with_capacity(group.len());
                        for w in &group {
                            let start = to_append.len();
                            let mut waiter_deduped = 0usize;
                            for (ev, fp) in &w.candidates {
                                if self
                                    .dedup
                                    .contains(project_id, bucket_id, *fp, server_now_ms)
                                    || !seen_in_group.insert(*fp)
                                {
                                    waiter_deduped += 1;
                                    continue;
                                }
                                committed_fps.push(*fp);
                                to_append.push(ev.clone());
                            }
                            spans.push((start, waiter_deduped));
                        }

                        let appended = to_append.len();
                        let written = if to_append.is_empty() {
                            Ok(None)
                        } else {
                            let batch = WireBatch::new(to_append);
                            ps.log_events_locked(&append_guard, bucket_id, &batch, group_bytes)
                                .await
                                .map(|wal_seq| Some((wal_seq, batch.events)))
                        };
                        // No await between the durable append and the replies: a request
                        // dropped mid-write leaves either nothing or a fully answered group.
                        match written {
                            Ok(logged) => {
                                self.dedup.commit_batch(
                                    project_id,
                                    bucket_id,
                                    &committed_fps,
                                    server_now_ms,
                                );
                                if let Some((wal_seq, events)) = logged {
                                    // Sent under the bucket lock, so the processor sees a
                                    // bucket's records in `wal_seq` order. If the processor is
                                    // gone the record is still durable and is applied by WAL
                                    // replay on the next open.
                                    let _ = processor.send(LoggedRecord {
                                        bucket_id,
                                        wal_seq,
                                        events,
                                        fps: committed_fps,
                                        bytes: group_bytes,
                                    });
                                }
                                for (i, w) in group.into_iter().enumerate() {
                                    let (start, waiter_deduped) = spans[i];
                                    let end = spans.get(i + 1).map_or(appended, |s| s.0);
                                    let _ = w.tx.send(Ok((end - start, waiter_deduped)));
                                }
                            }
                            Err(e) => {
                                for w in group {
                                    let _ = w.tx.send(Err(w.candidates));
                                }
                                // Ours was in the failed record: report the failure. Otherwise
                                // it is still queued; keep going.
                                if matches!(rx.try_recv(), Ok(Err(_))) {
                                    return Err(IngestError::Storage(e));
                                }
                            }
                        }
                    };
                    drop(append_guard);
                    Ok::<_, IngestError>(outcome)
                }
            }))
            .await?;

        for (local_accepted, local_deduped) in bucket_results {
            accepted += local_accepted;
            deduped += local_deduped;
        }

        self.metrics.record(
            accepted as u64,
            deduped as u64,
            dropped as u64,
            server_now_ms,
        );

        if !profile_calls.is_empty() {
            // `identify` / `alias` in a batch act on that batch's events, as before.
            ps.wait_applied(std::time::Duration::from_secs(1)).await;
        }
        Ok(IngestResponse {
            status: "ok".to_string(),
            accepted,
            deduped,
            dropped,
            server_ts_ms: server_now_ms,
            rejected,
            profiles_updated: self
                .apply_segment_profile_calls(project_id, profile_calls)
                .await?,
        })
    }

    /// Applies Segment calls that travel in event batches:
    /// - `identify` with `userId` + `anonymousId` links the two ids and sets `traits` on the user;
    ///   with only one id it sets `traits` on that id.
    /// - `alias` links `previousId` to `userId`.
    async fn apply_segment_profile_calls(
        &self,
        project_id: &str,
        calls: Vec<serde_json::Value>,
    ) -> Result<usize> {
        let s = |v: &serde_json::Value, keys: &[&str]| {
            keys.iter()
                .find_map(|k| v.get(*k).and_then(|x| x.as_str()))
                .map(str::trim)
                .filter(|x| !x.is_empty())
                .map(str::to_string)
        };
        let mut applied = 0usize;
        for call in calls {
            let kind = s(&call, &["type"]).unwrap_or_default().to_ascii_lowercase();
            let user = s(&call, &["userId", "user_id"]);
            let traits = call
                .get("traits")
                .or_else(|| call.get("context").and_then(|c| c.get("traits")))
                .filter(|t| t.as_object().is_some_and(|o| !o.is_empty()))
                .cloned();
            let other = if kind == "alias" {
                s(&call, &["previousId", "previous_id"])
            } else {
                s(&call, &["anonymousId", "anonymous_id"])
            };
            match (user, other) {
                (Some(user_id), Some(anon_id)) if user_id != anon_id => {
                    self.identify_for_project(
                        project_id,
                        IdentifyRequest {
                            anon_id,
                            user_id,
                            user_properties: traits,
                            ..Default::default()
                        },
                    )
                    .await?;
                }
                (Some(id), _) | (None, Some(id)) => {
                    let Some(t) = traits else { continue };
                    self.engage_for_project(
                        project_id,
                        serde_json::json!({ "distinct_id": id, "$set": t }),
                    )
                    .await?;
                }
                (None, None) => continue,
            }
            applied += 1;
        }
        Ok(applied)
    }

    pub async fn identify(&self, api_key: &str, req: IdentifyRequest) -> Result<IdentifyResponse> {
        let project_id = self
            .governance
            .authenticate_api_key(api_key)
            .await
            .ok_or(IngestError::Unauthorized)?;
        self.charge(&project_id, 1)?;
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
        // Keep the old order: events acknowledged before this call get their ids first.
        ps.wait_applied(std::time::Duration::from_secs(1)).await;
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
        self.governance
            .on_identify(project_id, &req.anon_id, &req.user_id, canonical_uid)
            .await;

        if let Some(up) = req.user_properties {
            let payload = serde_json::json!({ "$set": up });
            self.governance
                .apply_engage(project_id, &req.user_id, canonical_uid, &payload)
                .await;
        }
        if let Some(ops) = req.operations {
            self.governance
                .apply_engage(project_id, &req.user_id, canonical_uid, &ops)
                .await;
        }

        Ok(IdentifyResponse {
            status: "ok".to_string(),
            canonical_uid,
        })
    }

    /// Processes `/engage` or Identify user profile mutations (`Gap 1`).
    pub async fn engage(&self, api_key: &str, body: &[u8]) -> Result<serde_json::Value> {
        let project_id = self
            .governance
            .authenticate_api_key(api_key)
            .await
            .ok_or(IngestError::Unauthorized)?;
        self.charge(&project_id, 1)?;
        let val: serde_json::Value = serde_json::from_slice(body)?;
        let updated = self.engage_for_project(&project_id, val).await?;
        Ok(serde_json::json!({
            "status": "ok",
            "updated": updated,
        }))
    }

    pub async fn engage_for_project(
        &self,
        project_id: &str,
        payload: serde_json::Value,
    ) -> Result<usize> {
        let ps = self
            .ensure_project_ready(project_id, Utc::now().timestamp_millis())
            .await?;
        let id_graph = self
            .storage
            .identity_router()
            .get_or_open(project_id)
            .await?;
        let num_buckets = ps.config().num_buckets;

        let items: Vec<serde_json::Value> = match payload {
            serde_json::Value::Array(arr) => arr,
            serde_json::Value::Object(mut obj) => {
                if let Some(serde_json::Value::Array(arr)) =
                    obj.remove("profiles").or_else(|| obj.remove("batch"))
                {
                    arr
                } else {
                    vec![serde_json::Value::Object(obj)]
                }
            }
            _ => {
                return Err(IngestError::Wire(WireError::InvalidPayload(
                    "expected a JSON profile object or array".to_string(),
                )));
            }
        };

        let mut updated = 0usize;
        let mut need_flush = false;
        for item in &items {
            let distinct_id = [
                "distinct_id",
                "$distinct_id",
                "user_id",
                "$user_id",
                "userId",
            ]
            .iter()
            .find_map(|k| item.get(*k).and_then(|v| v.as_str()))
            .map(str::trim)
            .filter(|s| !s.is_empty());
            let Some(did) = distinct_id else {
                continue;
            };
            let seed = item
                .get("routing_seed")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
                .unwrap_or(did);
            let bucket_id = route_bucket(seed, num_buckets);
            let raw_uid = id_graph.get_or_intern(did, bucket_id);
            let canon_uid = id_graph.lookup_canonical(did).unwrap_or(raw_uid);
            need_flush = true;
            self.governance
                .apply_engage(project_id, did, canon_uid, item)
                .await;
            updated += 1;
        }
        if need_flush {
            id_graph.flush().await?;
        }
        Ok(updated)
    }

    /// Processes Group Analytics `/groups` property updates (`Gap 8`).
    pub async fn group_engage(&self, api_key: &str, body: &[u8]) -> Result<serde_json::Value> {
        let project_id = self
            .governance
            .authenticate_api_key(api_key)
            .await
            .ok_or(IngestError::Unauthorized)?;
        self.charge(&project_id, 1)?;
        let val: serde_json::Value = serde_json::from_slice(body)?;
        let updated = self.group_engage_for_project(&project_id, val).await?;
        Ok(serde_json::json!({
            "status": "ok",
            "updated": updated,
        }))
    }

    pub async fn group_engage_for_project(
        &self,
        project_id: &str,
        payload: serde_json::Value,
    ) -> Result<usize> {
        let items: Vec<serde_json::Value> = match payload {
            serde_json::Value::Array(arr) => arr,
            serde_json::Value::Object(_) => vec![payload],
            _ => {
                return Err(IngestError::Wire(WireError::InvalidPayload(
                    "expected a JSON group object or array".to_string(),
                )));
            }
        };
        let mut updated = 0usize;
        for item in &items {
            let group_type = ["group_type", "$group_key", "type"]
                .iter()
                .find_map(|k| item.get(*k).and_then(|v| v.as_str()))
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let group_id = ["group_id", "$group_id", "id"]
                .iter()
                .find_map(|k| item.get(*k).and_then(|v| v.as_str()))
                .map(str::trim)
                .filter(|s| !s.is_empty());
            if let (Some(gt), Some(gid)) = (group_type, group_id) {
                self.governance
                    .apply_group_engage(project_id, gt, gid, item)
                    .await;
                updated += 1;
            }
        }
        Ok(updated)
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
        self.charge(&project_id, 1)?;
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

/// Applies project Lexicon PII rules and input masking to wireframe replay labels on ingest.
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
            WireframeOp::NodeUpsert { node, .. } => {
                mask_label(&mut node.masked_label, node.kind);
            }
            _ => {}
        }
    }
}
