use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use parking_lot::{Mutex, RwLock};
use pulse_wire::{
    mask_input_value, InteractionKind, NodeKind, PropValue, ReplayChunk, WireBatch, WireEvent,
    WireNode, WireframeOp,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

uniffi::setup_scaffolding!();

pub const MAX_DISK_QUEUE_BYTES: usize = 10 * 1024 * 1024; // 10 MB circular disk WAL
pub const SESSION_INACTIVITY_TIMEOUT_MS: i64 = 30 * 60 * 1_000; // 30 minutes
pub const RAGE_TAP_WINDOW_MS: i64 = 800;
pub const RAGE_TAP_RADIUS_PX: i32 = 40;

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum SdkError {
    #[error("io error: {0}")]
    Io(String),
    #[error("network error: {0}")]
    Network(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct SdkConfig {
    pub endpoint_url: String,
    pub api_key: String,
    pub storage_dir: String,
    pub flush_batch_size: u32,
    pub flush_interval_ms: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct SdkWireNode {
    pub node_id: u32,
    pub parent_id: u32,
    /// `"screen" | "container" | "button" | "masked_input" | "text" | "image_placeholder" | "error_banner"`
    pub kind: String,
    pub x: u16,
    pub y: u16,
    pub w: u16,
    pub h: u16,
    pub bg_rgba: u32,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum QueuedPacket {
    Events(Vec<u8>),
    Identify {
        anon_id: String,
        user_id: String,
        routing_seed: String,
    },
    Replay(Vec<u8>),
}

impl QueuedPacket {
    fn byte_len(&self) -> usize {
        match self {
            QueuedPacket::Events(b) | QueuedPacket::Replay(b) => b.len() + 16,
            QueuedPacket::Identify {
                anon_id,
                user_id,
                routing_seed,
            } => anon_id.len() + user_id.len() + routing_seed.len() + 24,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedIdentityState {
    routing_seed: String,
    anon_id: String,
    identified_user_id: Option<String>,
    session_id: String,
    last_activity_ts_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
struct EdgeVariant {
    key: String,
    weight_bps: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct EdgeRule {
    key: String,
    enabled: bool,
    rollout_bps: u32,
    variants: Vec<EdgeVariant>,
}

#[derive(Debug, Clone, Deserialize)]
struct EdgeBundleResp {
    etag: String,
    flags: Vec<EdgeRule>,
}

#[derive(Default)]
struct FlagCache {
    etag: Option<String>,
    rules: HashMap<String, EdgeRule>,
}

#[derive(Clone, Copy)]
struct TapPoint {
    ts_ms: i64,
    x: i32,
    y: i32,
}

/// Cross-platform thread-safe Rust SDK Core (UniFFI-ready for Kotlin & Swift).
#[derive(uniffi::Object)]
pub struct PulseClient {
    config: SdkConfig,
    state_path: PathBuf,
    wal_path: PathBuf,
    identity: RwLock<PersistedIdentityState>,
    event_ring: Mutex<Vec<WireEvent>>,
    replay_ops_ring: Mutex<(Option<i64>, Vec<WireframeOp>)>,
    disk_queue: Mutex<(VecDeque<QueuedPacket>, usize)>,
    tap_history: Mutex<VecDeque<TapPoint>>,
    flag_cache: RwLock<FlagCache>,
    replay_offset_kb: AtomicU32,
    seq_counter: AtomicU32,
    shutdown_flag: Arc<AtomicBool>,
    http_client: reqwest::Client,
}

#[uniffi::export]
impl PulseClient {
    #[uniffi::constructor]
    pub fn new(config: SdkConfig) -> Result<Arc<Self>, SdkError> {
        let dir = PathBuf::from(&config.storage_dir);
        fs::create_dir_all(&dir).map_err(|e| SdkError::Io(e.to_string()))?;

        let state_path = dir.join("sdk_identity.json");
        let wal_path = dir.join("sdk_circular_wal.json");
        let now_ms = Utc::now().timestamp_millis();

        let identity = if let Ok(bytes) = fs::read(&state_path) {
            serde_json::from_slice::<PersistedIdentityState>(&bytes)
                .unwrap_or_else(|_| Self::fresh_identity(now_ms))
        } else {
            let fresh = Self::fresh_identity(now_ms);
            if let Ok(b) = serde_json::to_vec(&fresh) {
                let _ = fs::write(&state_path, b);
            }
            fresh
        };

        let mut loaded_queue = VecDeque::new();
        let mut total_bytes = 0usize;
        if let Ok(bytes) = fs::read(&wal_path) {
            if let Ok(items) = serde_json::from_slice::<Vec<QueuedPacket>>(&bytes) {
                for pkt in items {
                    total_bytes += pkt.byte_len();
                    loaded_queue.push_back(pkt);
                }
            }
        }

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| SdkError::Network(e.to_string()))?;

        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let client = Arc::new(Self {
            config: config.clone(),
            state_path,
            wal_path,
            identity: RwLock::new(identity),
            event_ring: Mutex::new(Vec::with_capacity(config.flush_batch_size as usize)),
            replay_ops_ring: Mutex::new((None, Vec::new())),
            disk_queue: Mutex::new((loaded_queue, total_bytes)),
            tap_history: Mutex::new(VecDeque::with_capacity(16)),
            flag_cache: RwLock::new(FlagCache::default()),
            replay_offset_kb: AtomicU32::new(0),
            seq_counter: AtomicU32::new(1),
            shutdown_flag: shutdown_flag.clone(),
            http_client,
        });

        // Spawn background flush worker if inside an active Tokio runtime and interval > 0
        if config.flush_interval_ms > 0 {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let weak = Arc::downgrade(&client);
                let interval_ms = config.flush_interval_ms.max(200);
                handle.spawn(async move {
                    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
                    loop {
                        ticker.tick().await;
                        if shutdown_flag.load(Ordering::Acquire) {
                            break;
                        }
                        let Some(strong) = weak.upgrade() else {
                            break;
                        };
                        let _ = strong.flush().await;
                    }
                });
            }
        }

        Ok(client)
    }

    pub fn distinct_id(&self) -> String {
        let g = self.identity.read();
        g.identified_user_id
            .clone()
            .unwrap_or_else(|| g.anon_id.clone())
    }

    pub fn session_id(&self) -> String {
        self.identity.read().session_id.clone()
    }

    pub fn queued_packet_count(&self) -> u32 {
        self.disk_queue.lock().0.len() as u32
    }

    /// Tracks an analytical event with a JSON object of properties.
    pub fn track(&self, event_name: String, props_json: String) -> Result<(), SdkError> {
        let props = if props_json.trim().is_empty() {
            Vec::new()
        } else {
            let map: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&props_json)
                .map_err(|e| SdkError::Serialization(e.to_string()))?;
            map.into_iter()
                .map(|(k, v)| (k, PropValue::from_json_value(v)))
                .collect()
        };
        self.track_at(event_name, props, Utc::now().timestamp_millis())
    }

    /// Identifies a logged-in user, preserving `routing_seed` (`device_install_id`) and queueing retroactive `alias`.
    pub fn identify(&self, user_id: String) -> Result<(), SdkError> {
        let now_ms = Utc::now().timestamp_millis();
        let (anon_id, routing_seed) = {
            let mut id = self.identity.write();
            self.rotate_session_if_inactive(&mut id, now_ms);
            let anon = id.anon_id.clone();
            let seed = id.routing_seed.clone();
            id.identified_user_id = Some(user_id.clone());
            self.persist_identity(&id);
            (anon, seed)
        };

        self.enqueue_packet(QueuedPacket::Identify {
            anon_id,
            user_id,
            routing_seed,
        })?;
        Ok(())
    }

    /// Records a wireframe layout snapshot strictly masking all `masked_input` labels into `████` on client!
    pub fn record_wireframe_snapshot(
        &self,
        width: u16,
        height: u16,
        nodes: Vec<SdkWireNode>,
    ) -> Result<(), SdkError> {
        let now_ms = Utc::now().timestamp_millis();
        {
            let mut id = self.identity.write();
            self.rotate_session_if_inactive(&mut id, now_ms);
        }

        let wire_nodes: Vec<WireNode> = nodes
            .into_iter()
            .map(|n| {
                let kind = match n.kind.as_str() {
                    "screen" => NodeKind::Screen,
                    "container" => NodeKind::Container,
                    "button" => NodeKind::Button,
                    "masked_input" => NodeKind::MaskedInput,
                    "image_placeholder" => NodeKind::ImagePlaceholder,
                    "error_banner" => NodeKind::ErrorBanner,
                    _ => NodeKind::Text,
                };
                let masked_label = if kind == NodeKind::MaskedInput {
                    mask_input_value(&n.label)
                } else {
                    n.label
                };
                WireNode {
                    node_id: n.node_id,
                    parent_id: n.parent_id,
                    kind,
                    x: n.x,
                    y: n.y,
                    w: n.w,
                    h: n.h,
                    bg_rgba: n.bg_rgba,
                    masked_label,
                }
            })
            .collect();

        let mut ring = self.replay_ops_ring.lock();
        if ring.0.is_none() {
            ring.0 = Some(now_ms);
        }
        ring.1.push(WireframeOp::FullSnapshot {
            width,
            height,
            nodes: wire_nodes,
        });
        Ok(())
    }

    /// Records a touch interaction and runs automatic Rage Tap (3+ taps <=800ms in 40px radius) & Dead Tap detection!
    pub fn record_touch(
        &self,
        x: u16,
        y: u16,
        target_is_interactive: bool,
        ts_ms: i64,
    ) -> Result<String, SdkError> {
        {
            let mut id = self.identity.write();
            self.rotate_session_if_inactive(&mut id, ts_ms);
        }

        let kind = {
            let mut taps = self.tap_history.lock();
            while let Some(front) = taps.front() {
                if ts_ms - front.ts_ms > RAGE_TAP_WINDOW_MS {
                    taps.pop_front();
                } else {
                    break;
                }
            }
            taps.push_back(TapPoint {
                ts_ms,
                x: x as i32,
                y: y as i32,
            });

            let nearby_count = taps
                .iter()
                .filter(|p| {
                    let dx = p.x - (x as i32);
                    let dy = p.y - (y as i32);
                    dx * dx + dy * dy <= RAGE_TAP_RADIUS_PX * RAGE_TAP_RADIUS_PX
                })
                .count();

            if nearby_count >= 3 {
                InteractionKind::RageTap {
                    count: nearby_count.min(255) as u8,
                }
            } else if !target_is_interactive {
                InteractionKind::DeadTap
            } else {
                InteractionKind::Tap
            }
        };

        {
            let mut ring = self.replay_ops_ring.lock();
            let base = *ring.0.get_or_insert(ts_ms);
            let ts_offset_ms = (ts_ms - base).max(0) as u32;
            ring.1.push(WireframeOp::PointerInteraction {
                ts_offset_ms,
                x,
                y,
                kind,
            });
        }

        let label = match kind {
            InteractionKind::RageTap { count } => {
                self.track_at(
                    "$rage_tap".to_string(),
                    vec![
                        ("x".to_string(), PropValue::Int(x as i64)),
                        ("y".to_string(), PropValue::Int(y as i64)),
                        ("tap_count".to_string(), PropValue::Int(count as i64)),
                    ],
                    ts_ms,
                )?;
                "rage_tap"
            }
            InteractionKind::DeadTap => "dead_tap",
            InteractionKind::Tap => "tap",
            InteractionKind::Scroll => "scroll",
        };
        Ok(label.to_string())
    }

    /// Synchronizes the Edge Flag bundle (with ETag caching) and evaluates `flag_key` locally in < 1ms.
    pub async fn evaluate_flag(&self, flag_key: String) -> Result<Option<String>, SdkError> {
        if self.flag_cache.read().rules.is_empty() {
            let _ = self.refresh_flags().await;
        }
        let did = self.distinct_id();
        let cache = self.flag_cache.read();
        let Some(rule) = cache.rules.get(&flag_key) else {
            return Ok(None);
        };
        if !rule.enabled || rule.rollout_bps == 0 {
            return Ok(None);
        }
        let slot = (xxh3_64(format!("{}:{did}", rule.key).as_bytes()) % 10_000) as u32;
        if slot >= rule.rollout_bps {
            return Ok(None);
        }
        if rule.variants.is_empty() {
            return Ok(Some("on".to_string()));
        }
        let total_w: u32 = rule.variants.iter().map(|v| v.weight_bps).sum();
        if total_w == 0 {
            return Ok(rule.variants.first().map(|v| v.key.clone()));
        }
        let v_slot =
            (xxh3_64(format!("var:{}:{did}", rule.key).as_bytes()) % (total_w as u64)) as u32;
        let mut acc = 0u32;
        for v in &rule.variants {
            acc += v.weight_bps;
            if v_slot < acc {
                return Ok(Some(v.key.clone()));
            }
        }
        Ok(rule.variants.last().map(|v| v.key.clone()))
    }

    pub async fn refresh_flags(&self) -> Result<(), SdkError> {
        let base = self.config.endpoint_url.trim_end_matches('/');
        let url = format!("{base}/v1/flags/{}", self.config.api_key);
        let mut req = self.http_client.get(&url);
        if let Some(etag) = self.flag_cache.read().etag.clone() {
            req = req.header("If-None-Match", etag);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| SdkError::Network(e.to_string()))?;
        if resp.status().as_u16() == 304 {
            return Ok(());
        }
        if !resp.status().is_success() {
            return Err(SdkError::Network(format!(
                "flag fetch HTTP {}",
                resp.status()
            )));
        }
        let bundle: EdgeBundleResp = resp
            .json()
            .await
            .map_err(|e| SdkError::Serialization(e.to_string()))?;
        let mut guard = self.flag_cache.write();
        guard.etag = Some(bundle.etag);
        guard.rules = bundle
            .flags
            .into_iter()
            .map(|r| (r.key.clone(), r))
            .collect();
        Ok(())
    }

    /// Seals active in-memory rings into the bounded 10 MB circular disk WAL and transmits all queued packets.
    pub async fn flush(&self) -> Result<u32, SdkError> {
        self.seal_rings_to_disk_queue()?;

        let packets: Vec<QueuedPacket> = {
            let guard = self.disk_queue.lock();
            guard.0.iter().cloned().collect()
        };
        if packets.is_empty() {
            return Ok(0);
        }

        let base = self.config.endpoint_url.trim_end_matches('/');
        let mut sent_count = 0usize;

        for pkt in &packets {
            let res = match pkt {
                QueuedPacket::Events(bin) => {
                    self.http_client
                        .post(format!("{base}/v1/ingest"))
                        .header("x-pulse-key", &self.config.api_key)
                        .header("content-type", "application/x-pulse-frame")
                        .body(bin.clone())
                        .send()
                        .await
                }
                QueuedPacket::Identify {
                    anon_id,
                    user_id,
                    routing_seed,
                } => {
                    self.http_client
                        .post(format!("{base}/v1/identify"))
                        .header("x-pulse-key", &self.config.api_key)
                        .json(&serde_json::json!({
                            "anon_id": anon_id,
                            "user_id": user_id,
                            "routing_seed": routing_seed
                        }))
                        .send()
                        .await
                }
                QueuedPacket::Replay(bin) => {
                    self.http_client
                        .post(format!("{base}/v1/replay/chunk"))
                        .header("x-pulse-key", &self.config.api_key)
                        .header("content-type", "application/x-pulse-frame")
                        .body(bin.clone())
                        .send()
                        .await
                }
            };

            match res {
                Ok(resp) if resp.status().is_success() => {
                    if matches!(pkt, QueuedPacket::Replay(_)) {
                        if let Ok(v) = resp.json::<serde_json::Value>().await {
                            if let Some(kb) = v.get("replay_offset_kb").and_then(|x| x.as_u64()) {
                                self.replay_offset_kb.store(kb as u32, Ordering::Release);
                            }
                        }
                    }
                    sent_count += 1;
                }
                Ok(resp) => {
                    // Keep remaining packets safely in circular disk WAL for retry!
                    let status = resp.status();
                    self.pop_sent_from_disk_queue(sent_count)?;
                    return Err(SdkError::Network(format!("HTTP {status}")));
                }
                Err(err) => {
                    self.pop_sent_from_disk_queue(sent_count)?;
                    return Err(SdkError::Network(err.to_string()));
                }
            }
        }

        self.pop_sent_from_disk_queue(sent_count)?;
        Ok(sent_count as u32)
    }

    pub async fn shutdown(&self) -> Result<(), SdkError> {
        self.shutdown_flag.store(true, Ordering::Release);
        let _ = self.flush().await;
        Ok(())
    }
}

impl PulseClient {
    fn fresh_identity(now_ms: i64) -> PersistedIdentityState {
        let seed = format!("dev_{}", Uuid::new_v4().simple());
        let anon = format!("anon_{}", &Uuid::new_v4().simple().to_string()[..12]);
        let sess = format!("sess_{}", &Uuid::new_v4().simple().to_string()[..12]);
        PersistedIdentityState {
            routing_seed: seed,
            anon_id: anon,
            identified_user_id: None,
            session_id: sess,
            last_activity_ts_ms: now_ms,
        }
    }

    fn rotate_session_if_inactive(&self, state: &mut PersistedIdentityState, now_ms: i64) {
        if now_ms - state.last_activity_ts_ms > SESSION_INACTIVITY_TIMEOUT_MS {
            state.session_id = format!("sess_{}", &Uuid::new_v4().simple().to_string()[..12]);
            self.replay_offset_kb.store(0, Ordering::Release);
        }
        state.last_activity_ts_ms = now_ms;
    }

    fn persist_identity(&self, state: &PersistedIdentityState) {
        if let Ok(bytes) = serde_json::to_vec(state) {
            let _ = fs::write(&self.state_path, bytes);
        }
    }

    pub fn track_at(
        &self,
        event_name: String,
        props: Vec<(String, PropValue)>,
        event_ts_ms: i64,
    ) -> Result<(), SdkError> {
        let (routing_seed, distinct_id, session_id) = {
            let mut id = self.identity.write();
            self.rotate_session_if_inactive(&mut id, event_ts_ms);
            let did = id
                .identified_user_id
                .clone()
                .unwrap_or_else(|| id.anon_id.clone());
            self.persist_identity(&id);
            (id.routing_seed.clone(), did, id.session_id.clone())
        };

        let seq = self.seq_counter.fetch_add(1, Ordering::Relaxed) as u64;
        let ev = WireEvent {
            event_uuid: WireEvent::deterministic_uuid(
                &distinct_id,
                &session_id,
                &event_name,
                event_ts_ms,
                seq,
            ),
            routing_seed,
            distinct_id,
            session_id,
            event_name,
            client_event_ts_ms: event_ts_ms,
            client_sent_ts_ms: Utc::now().timestamp_millis(),
            props,
            replay_offset_kb: self.replay_offset_kb.load(Ordering::Acquire),
        };

        let should_seal = {
            let mut ring = self.event_ring.lock();
            ring.push(ev);
            ring.len() >= (self.config.flush_batch_size.max(1) as usize)
        };
        if should_seal {
            self.seal_rings_to_disk_queue()?;
        }
        Ok(())
    }

    fn seal_rings_to_disk_queue(&self) -> Result<(), SdkError> {
        // Seal replay ops first so `replay_offset_kb` updates before events
        let replay_ops = {
            let mut rr = self.replay_ops_ring.lock();
            if rr.1.is_empty() {
                None
            } else {
                let start_ts = rr.0.take().unwrap_or_else(|| Utc::now().timestamp_millis());
                let ops = std::mem::take(&mut rr.1);
                Some((start_ts, ops))
            }
        };

        if let Some((chunk_start_ts_ms, ops)) = replay_ops {
            let chunk = ReplayChunk {
                project_id: String::new(),
                session_id: self.session_id(),
                chunk_start_ts_ms,
                ops,
            };
            let bin = chunk
                .encode_binary(3)
                .map_err(|e| SdkError::Serialization(e.to_string()))?;
            self.enqueue_packet(QueuedPacket::Replay(bin))?;
        }

        let events = {
            let mut er = self.event_ring.lock();
            if er.is_empty() {
                Vec::new()
            } else {
                let now_sent = Utc::now().timestamp_millis();
                for e in er.iter_mut() {
                    e.client_sent_ts_ms = now_sent;
                }
                std::mem::take(&mut *er)
            }
        };

        if !events.is_empty() {
            let batch = WireBatch::new(events);
            let bin = batch
                .encode_binary(3)
                .map_err(|e| SdkError::Serialization(e.to_string()))?;
            self.enqueue_packet(QueuedPacket::Events(bin))?;
        }
        Ok(())
    }

    fn enqueue_packet(&self, pkt: QueuedPacket) -> Result<(), SdkError> {
        let mut guard = self.disk_queue.lock();
        let pkt_bytes = pkt.byte_len();
        // Enforce bounded 10 MB circular disk WAL by dropping oldest packets if full
        while guard.1 + pkt_bytes > MAX_DISK_QUEUE_BYTES && !guard.0.is_empty() {
            if let Some(removed) = guard.0.pop_front() {
                guard.1 = guard.1.saturating_sub(removed.byte_len());
            }
        }
        guard.1 += pkt_bytes;
        guard.0.push_back(pkt);
        self.persist_wal_locked(&guard.0)
    }

    fn pop_sent_from_disk_queue(&self, count: usize) -> Result<(), SdkError> {
        if count == 0 {
            return Ok(());
        }
        let mut guard = self.disk_queue.lock();
        for _ in 0..count {
            if let Some(removed) = guard.0.pop_front() {
                guard.1 = guard.1.saturating_sub(removed.byte_len());
            }
        }
        self.persist_wal_locked(&guard.0)
    }

    fn persist_wal_locked(&self, queue: &VecDeque<QueuedPacket>) -> Result<(), SdkError> {
        let vec: Vec<&QueuedPacket> = queue.iter().collect();
        let bytes = serde_json::to_vec(&vec).map_err(|e| SdkError::Serialization(e.to_string()))?;
        let crc = crc32fast::hash(&bytes);
        let _ = crc;
        fs::write(&self.wal_path, bytes).map_err(|e| SdkError::Io(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rage_tap_session_rotation_and_offline_disk_wal_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = SdkConfig {
            endpoint_url: "http://127.0.0.1:9".to_string(), // Unreachable port -> triggers offline disk WAL retention!
            api_key: "pk_test".to_string(),
            storage_dir: dir.path().to_string_lossy().to_string(),
            flush_batch_size: 10,
            flush_interval_ms: 0,
        };

        let client = PulseClient::new(cfg.clone()).unwrap();
        let s1 = client.session_id();
        client
            .track_at("App_Open".into(), vec![], 1_760_000_000_000)
            .unwrap();
        assert_eq!(client.session_id(), s1);

        // 31 minutes later -> session must deterministically rotate!
        client
            .track_at(
                "Browse_Credit_Card".into(),
                vec![],
                1_760_000_000_000 + 31 * 60 * 1000,
            )
            .unwrap();
        let s2 = client.session_id();
        assert_ne!(s1, s2, "session must rotate after 30-min inactivity");

        // 3 rapid taps within 800ms inside 40px -> Rage Tap!
        let t_tap = 1_760_000_000_000 + 31 * 60 * 1000 + 1_000;
        assert_eq!(client.record_touch(200, 300, true, t_tap).unwrap(), "tap");
        assert_eq!(
            client.record_touch(210, 305, true, t_tap + 200).unwrap(),
            "tap"
        );
        assert_eq!(
            client.record_touch(195, 298, true, t_tap + 450).unwrap(),
            "rage_tap"
        );

        // Flush while offline -> must persist in circular disk WAL
        assert!(client.flush().await.is_err());
        let queued = client.queued_packet_count();
        assert!(queued >= 2);
        drop(client);

        // Restart SDK from same directory -> disk WAL survives restart!
        let recovered = PulseClient::new(cfg).unwrap();
        assert_eq!(recovered.queued_packet_count(), queued);
    }
}
