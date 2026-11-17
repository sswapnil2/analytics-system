use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::Utc;
use parking_lot::{Mutex, RwLock};
use pulse_wire::{
    evaluate_flag_rule, mask_input_value, InteractionKind, NodeKind, PropValue, ReplayChunk,
    WireBatch, WireEvent, WireNode, WireframeOp, FIXED_MASK_PLACEHOLDER,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use uuid::Uuid;

uniffi::setup_scaffolding!();

/// Strict 10 MiB bound measured on actual on-disk WAL frame bytes (`P2-07`).
pub const MAX_DISK_QUEUE_BYTES: usize = 10 * 1024 * 1024;
pub const SESSION_INACTIVITY_TIMEOUT_MS: i64 = 30 * 60 * 1_000; // 30 minutes
pub const RAGE_TAP_WINDOW_MS: i64 = 800;
pub const RAGE_TAP_RADIUS_PX: i32 = 40;

const SDK_WAL_MAGIC: [u8; 4] = *b"PSDK";
const SDK_WAL_HEADER_LEN: usize = 20; // 4B magic + 8B packet_id + 4B len + 4B crc32

/// Shared background Tokio runtime ensuring `PulseClient` works even when invoked from
/// bare OS / Kotlin / Swift threads that have no ambient Tokio runtime (`P2-08`).
fn sdk_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("pulse-sdk-bg")
            .enable_all()
            .build()
            .expect("failed to initialize pulse-sdk-core Tokio runtime")
    })
}

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
    /// Default-deny privacy (`P3-01`): unless `unmasked == true` (and `kind != "masked_input"`),
    /// any non-empty `label` is replaced on client with fixed-length `"████"`.
    #[uniffi(default = false)]
    pub unmasked: bool,
}

#[derive(Debug, Clone)]
enum QueuedPacket {
    Events(Vec<u8>),
    Identify {
        anon_id: String,
        user_id: String,
        routing_seed: String,
    },
    Replay(Vec<u8>),
    Engage(Vec<u8>),
}

impl QueuedPacket {
    const TAG_EVENTS: u8 = 1;
    const TAG_IDENTIFY: u8 = 2;
    const TAG_REPLAY: u8 = 3;
    const TAG_ENGAGE: u8 = 4;

    fn encode_payload(&self) -> Vec<u8> {
        match self {
            QueuedPacket::Events(bin) => {
                let mut out = Vec::with_capacity(1 + bin.len());
                out.push(Self::TAG_EVENTS);
                out.extend_from_slice(bin);
                out
            }
            QueuedPacket::Identify {
                anon_id,
                user_id,
                routing_seed,
            } => {
                let mut out =
                    Vec::with_capacity(1 + 12 + anon_id.len() + user_id.len() + routing_seed.len());
                out.push(Self::TAG_IDENTIFY);
                for s in [anon_id.as_str(), user_id.as_str(), routing_seed.as_str()] {
                    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    out.extend_from_slice(s.as_bytes());
                }
                out
            }
            QueuedPacket::Replay(bin) => {
                let mut out = Vec::with_capacity(1 + bin.len());
                out.push(Self::TAG_REPLAY);
                out.extend_from_slice(bin);
                out
            }
            QueuedPacket::Engage(json) => {
                let mut out = Vec::with_capacity(1 + json.len());
                out.push(Self::TAG_ENGAGE);
                out.extend_from_slice(json);
                out
            }
        }
    }

    fn decode_payload(bytes: &[u8]) -> Option<Self> {
        let (&tag, rest) = bytes.split_first()?;
        match tag {
            Self::TAG_EVENTS => Some(QueuedPacket::Events(rest.to_vec())),
            Self::TAG_REPLAY => Some(QueuedPacket::Replay(rest.to_vec())),
            Self::TAG_ENGAGE => Some(QueuedPacket::Engage(rest.to_vec())),
            Self::TAG_IDENTIFY => {
                let mut pos = 0usize;
                let mut parts = Vec::with_capacity(3);
                for _ in 0..3 {
                    if pos + 4 > rest.len() {
                        return None;
                    }
                    let len = u32::from_le_bytes([
                        rest[pos],
                        rest[pos + 1],
                        rest[pos + 2],
                        rest[pos + 3],
                    ]) as usize;
                    pos += 4;
                    if pos + len > rest.len() {
                        return None;
                    }
                    let s = std::str::from_utf8(&rest[pos..pos + len]).ok()?.to_string();
                    pos += len;
                    parts.push(s);
                }
                if pos != rest.len() {
                    return None;
                }
                Some(QueuedPacket::Identify {
                    anon_id: parts.remove(0),
                    user_id: parts.remove(0),
                    routing_seed: parts.remove(0),
                })
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct WalQueueEntry {
    id: u64,
    pkt: QueuedPacket,
    frame_bytes: Vec<u8>,
}

impl WalQueueEntry {
    fn new(id: u64, pkt: QueuedPacket) -> Self {
        let payload = pkt.encode_payload();
        let len_u32 = payload.len() as u32;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&id.to_le_bytes());
        hasher.update(&len_u32.to_le_bytes());
        hasher.update(&payload);
        let crc = hasher.finalize();

        let mut frame = Vec::with_capacity(SDK_WAL_HEADER_LEN + payload.len());
        frame.extend_from_slice(&SDK_WAL_MAGIC);
        frame.extend_from_slice(&id.to_le_bytes());
        frame.extend_from_slice(&len_u32.to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(&payload);

        Self {
            id,
            pkt,
            frame_bytes: frame,
        }
    }

    #[inline]
    fn disk_len(&self) -> usize {
        self.frame_bytes.len()
    }
}

enum WalIoCmd {
    Append(Vec<u8>),
    Compact(Vec<u8>),
    PersistIdentity(Vec<u8>),
    Sync(oneshot::Sender<Result<(), String>>),
}

fn load_wal_entries(wal_path: &Path) -> (VecDeque<WalQueueEntry>, usize, u64, bool) {
    let Ok(raw) = fs::read(wal_path) else {
        return (VecDeque::new(), 0, 0, false);
    };
    let mut pos = 0usize;
    let mut queue = VecDeque::new();
    let mut total_bytes = 0usize;
    let mut max_id = 0u64;
    let mut had_corruption = false;

    while pos < raw.len() {
        if pos + SDK_WAL_HEADER_LEN > raw.len() {
            had_corruption = true;
            break;
        }
        if raw[pos..pos + 4] != SDK_WAL_MAGIC {
            had_corruption = true;
            break;
        }
        let mut id_arr = [0u8; 8];
        id_arr.copy_from_slice(&raw[pos + 4..pos + 12]);
        let id = u64::from_le_bytes(id_arr);
        let len = u32::from_le_bytes([raw[pos + 12], raw[pos + 13], raw[pos + 14], raw[pos + 15]])
            as usize;
        let expected_crc =
            u32::from_le_bytes([raw[pos + 16], raw[pos + 17], raw[pos + 18], raw[pos + 19]]);
        let frame_end = pos.saturating_add(SDK_WAL_HEADER_LEN).saturating_add(len);
        if frame_end > raw.len() {
            had_corruption = true;
            break;
        }
        let payload = &raw[pos + SDK_WAL_HEADER_LEN..frame_end];
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&id.to_le_bytes());
        hasher.update(&(len as u32).to_le_bytes());
        hasher.update(payload);
        if hasher.finalize() != expected_crc {
            had_corruption = true;
            break;
        }
        let Some(pkt) = QueuedPacket::decode_payload(payload) else {
            had_corruption = true;
            break;
        };
        let frame_bytes = raw[pos..frame_end].to_vec();
        total_bytes += frame_bytes.len();
        max_id = max_id.max(id);
        queue.push_back(WalQueueEntry {
            id,
            pkt,
            frame_bytes,
        });
        pos = frame_end;
    }

    // Enforce 10 MiB bound on startup too if file exceeded budget
    while total_bytes > MAX_DISK_QUEUE_BYTES && !queue.is_empty() {
        if let Some(evicted) = queue.pop_front() {
            total_bytes = total_bytes.saturating_sub(evicted.disk_len());
            had_corruption = true;
        }
    }

    (queue, total_bytes, max_id, had_corruption)
}

fn write_atomic_file(path: &Path, data: &[u8]) -> std::io::Result<()> {
    static TMP_SEQ: AtomicU64 = AtomicU64::new(1);
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = path.with_extension(format!("tmp.{seq}"));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_data()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

fn spawn_wal_io_worker(wal_path: PathBuf, state_path: PathBuf) -> mpsc::UnboundedSender<WalIoCmd> {
    let (tx, mut rx) = mpsc::unbounded_channel::<WalIoCmd>();
    sdk_runtime().spawn(async move {
        while let Some(cmd) = rx.recv().await {
            let path = wal_path.clone();
            let id_path = state_path.clone();
            match cmd {
                WalIoCmd::Append(frame) => {
                    let _ = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                        let mut f = OpenOptions::new().create(true).append(true).open(&path)?;
                        f.write_all(&frame)?;
                        f.sync_data()
                    })
                    .await;
                }
                WalIoCmd::Compact(snapshot) => {
                    let _ =
                        tokio::task::spawn_blocking(move || write_atomic_file(&path, &snapshot))
                            .await;
                }
                WalIoCmd::PersistIdentity(bytes) => {
                    let _ =
                        tokio::task::spawn_blocking(move || write_atomic_file(&id_path, &bytes))
                            .await;
                }
                WalIoCmd::Sync(reply) => {
                    let res = tokio::task::spawn_blocking(move || -> Result<(), String> {
                        if path.exists() {
                            let f = File::open(&path).map_err(|e| e.to_string())?;
                            f.sync_data().map_err(|e| e.to_string())?;
                        }
                        if id_path.exists() {
                            let f = File::open(&id_path).map_err(|e| e.to_string())?;
                            f.sync_data().map_err(|e| e.to_string())?;
                        }
                        Ok(())
                    })
                    .await
                    .unwrap_or_else(|e| Err(e.to_string()));
                    let _ = reply.send(res);
                }
            }
        }
    });
    tx
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedIdentityState {
    routing_seed: String,
    anon_id: String,
    identified_user_id: Option<String>,
    session_id: String,
    last_activity_ts_ms: i64,
    #[serde(default)]
    opted_out: bool,
    #[serde(default)]
    super_props: Vec<(String, PropValue)>,
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
    #[serde(default)]
    cohort_members: Option<Vec<String>>,
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
    wal_path: PathBuf,
    identity: RwLock<PersistedIdentityState>,
    event_ring: Mutex<Vec<WireEvent>>,
    /// `(chunk_start_ts_ms, session_id_for_ops, ops)`
    replay_ops_ring: Mutex<(Option<i64>, Option<String>, Vec<WireframeOp>)>,
    disk_queue: Mutex<(VecDeque<WalQueueEntry>, usize)>,
    wal_io_tx: mpsc::UnboundedSender<WalIoCmd>,
    flush_lock: AsyncMutex<()>,
    next_packet_id: AtomicU64,
    consecutive_failures: AtomicU32,
    backoff_until_ms: AtomicI64,
    tap_history: Mutex<VecDeque<TapPoint>>,
    timed_events: Mutex<HashMap<String, i64>>,
    flag_cache: RwLock<FlagCache>,
    replay_offset_kb: AtomicU32,
    seq_counter: AtomicU32,
    shutdown_flag: Arc<AtomicBool>,
    http_client: reqwest::Client,
}

#[uniffi::export(async_runtime = "tokio")]
impl PulseClient {
    #[uniffi::constructor]
    pub fn new(config: SdkConfig) -> Result<Arc<Self>, SdkError> {
        let _rt_guard = sdk_runtime().enter();
        let dir = PathBuf::from(&config.storage_dir);
        fs::create_dir_all(&dir).map_err(|e| SdkError::Io(e.to_string()))?;

        let state_path = dir.join("sdk_identity.json");
        let wal_path = dir.join("sdk_wal.bin");
        let identity = if let Ok(bytes) = fs::read(&state_path) {
            serde_json::from_slice::<PersistedIdentityState>(&bytes)
                .unwrap_or_else(|_| Self::fresh_identity())
        } else {
            let fresh = Self::fresh_identity();
            if let Ok(b) = serde_json::to_vec(&fresh) {
                let _ = write_atomic_file(&state_path, &b);
            }
            fresh
        };

        let (loaded_queue, total_bytes, max_id, needs_compact) = load_wal_entries(&wal_path);
        if needs_compact {
            let mut compacted = Vec::with_capacity(total_bytes);
            for e in &loaded_queue {
                compacted.extend_from_slice(&e.frame_bytes);
            }
            let _ = write_atomic_file(&wal_path, &compacted);
        }

        let wal_io_tx = spawn_wal_io_worker(wal_path.clone(), state_path.clone());

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| SdkError::Network(e.to_string()))?;

        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let client = Arc::new(Self {
            config: config.clone(),
            wal_path,
            identity: RwLock::new(identity),
            event_ring: Mutex::new(Vec::with_capacity(config.flush_batch_size as usize)),
            replay_ops_ring: Mutex::new((None, None, Vec::new())),
            disk_queue: Mutex::new((loaded_queue, total_bytes)),
            wal_io_tx,
            flush_lock: AsyncMutex::new(()),
            next_packet_id: AtomicU64::new(max_id + 1),
            consecutive_failures: AtomicU32::new(0),
            backoff_until_ms: AtomicI64::new(0),
            tap_history: Mutex::new(VecDeque::with_capacity(16)),
            timed_events: Mutex::new(HashMap::new()),
            flag_cache: RwLock::new(FlagCache::default()),
            replay_offset_kb: AtomicU32::new(0),
            seq_counter: AtomicU32::new(1),
            shutdown_flag: shutdown_flag.clone(),
            http_client,
        });

        // Always start background flush worker on `sdk_runtime()` when `flush_interval_ms > 0`,
        // even if the caller thread has no Tokio runtime (`P2-08`)!
        if config.flush_interval_ms > 0 {
            let weak = Arc::downgrade(&client);
            let interval_ms = config.flush_interval_ms.max(200);
            sdk_runtime().spawn(async move {
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

    pub fn opt_out(&self) -> Result<(), SdkError> {
        let snap = {
            let mut id = self.identity.write();
            id.opted_out = true;
            id.clone()
        };
        self.event_ring.lock().clear();
        {
            let mut r = self.replay_ops_ring.lock();
            r.0 = None;
            r.1 = None;
            r.2.clear();
        }
        {
            let mut dq = self.disk_queue.lock();
            dq.0.clear();
            dq.1 = 0;
        }
        self.timed_events.lock().clear();
        let _ = self.wal_io_tx.send(WalIoCmd::Compact(Vec::new()));
        self.persist_identity_async(snap);
        Ok(())
    }

    pub fn opt_in(&self) -> Result<(), SdkError> {
        let snap = {
            let mut id = self.identity.write();
            id.opted_out = false;
            id.clone()
        };
        self.persist_identity_async(snap);
        Ok(())
    }

    pub fn is_opted_out(&self) -> bool {
        self.identity.read().opted_out
    }

    pub fn register_super_props(&self, props_json: String) -> Result<(), SdkError> {
        let map: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&props_json)
            .map_err(|e| SdkError::Serialization(e.to_string()))?;
        let snap = {
            let mut id = self.identity.write();
            for (k, v) in map {
                let pv = PropValue::from_json_value(v);
                if let Some(pos) = id.super_props.iter().position(|(ek, _)| ek == &k) {
                    id.super_props[pos].1 = pv;
                } else {
                    id.super_props.push((k, pv));
                }
            }
            id.clone()
        };
        self.persist_identity_async(snap);
        Ok(())
    }

    pub fn time_event(&self, event_name: String) -> Result<(), SdkError> {
        if self.is_opted_out() {
            return Ok(());
        }
        self.timed_events
            .lock()
            .insert(event_name, Utc::now().timestamp_millis());
        Ok(())
    }

    pub fn set_user_properties(&self, props_json: String) -> Result<(), SdkError> {
        if self.is_opted_out() {
            return Ok(());
        }
        let val: serde_json::Value = serde_json::from_str(&props_json)
            .map_err(|e| SdkError::Serialization(e.to_string()))?;
        if !val.is_object() {
            return Err(SdkError::Serialization(
                "user properties payload must be a JSON object".into(),
            ));
        }
        let payload = serde_json::json!({
            "$distinct_id": self.distinct_id(),
            "$set": val,
        });
        let bytes =
            serde_json::to_vec(&payload).map_err(|e| SdkError::Serialization(e.to_string()))?;
        self.enqueue_packet(QueuedPacket::Engage(bytes));
        Ok(())
    }

    pub fn track_revenue(
        &self,
        amount: f64,
        currency: String,
        product_id: Option<String>,
    ) -> Result<(), SdkError> {
        if self.is_opted_out() {
            return Ok(());
        }
        let mut props = vec![
            ("amount".to_string(), PropValue::Float(amount)),
            ("currency".to_string(), PropValue::Str(currency.clone())),
        ];
        if let Some(pid) = &product_id {
            props.push(("product_id".to_string(), PropValue::Str(pid.clone())));
        }
        self.track_at("$revenue".to_string(), props, Utc::now().timestamp_millis())?;

        let mut charge = serde_json::json!({
            "$amount": amount,
            "currency": currency,
        });
        if let Some(pid) = product_id {
            charge["product_id"] = serde_json::Value::String(pid);
        }
        let payload = serde_json::json!({
            "$distinct_id": self.distinct_id(),
            "$track_charge": charge,
        });
        let bytes =
            serde_json::to_vec(&payload).map_err(|e| SdkError::Serialization(e.to_string()))?;
        self.enqueue_packet(QueuedPacket::Engage(bytes));
        Ok(())
    }

    /// Identifies a logged-in user, preserving `routing_seed` (`device_install_id`) and queueing retroactive `alias`.
    pub fn identify(&self, user_id: String) -> Result<(), SdkError> {
        if self.is_opted_out() {
            return Ok(());
        }
        let now_ms = Utc::now().timestamp_millis();
        self.touch_activity_and_maybe_rotate(now_ms)?;
        let (anon_id, routing_seed) = {
            let mut id = self.identity.write();
            let anon = id.anon_id.clone();
            let seed = id.routing_seed.clone();
            id.identified_user_id = Some(user_id.clone());
            let snap = id.clone();
            drop(id);
            self.persist_identity_async(snap);
            (anon, seed)
        };

        self.enqueue_packet(QueuedPacket::Identify {
            anon_id,
            user_id,
            routing_seed,
        });
        Ok(())
    }

    /// Resets identity on logout (`P1-08`), sealing pending replay/event rings for the outgoing user
    /// before rotating `anon_id` and `session_id` so a subsequent login on the same device never merges two users!
    pub fn reset(&self) -> Result<(), SdkError> {
        self.seal_rings_to_disk_queue()?;
        let now_ms = Utc::now().timestamp_millis();
        let snap = {
            let mut id = self.identity.write();
            id.anon_id = format!("anon_{}", &Uuid::new_v4().simple().to_string()[..12]);
            id.identified_user_id = None;
            id.session_id = format!("sess_{}", &Uuid::new_v4().simple().to_string()[..12]);
            id.last_activity_ts_ms = now_ms;
            self.replay_offset_kb.store(0, Ordering::Release);
            id.clone()
        };
        self.tap_history.lock().clear();
        self.persist_identity_async(snap);
        Ok(())
    }

    /// Records a wireframe layout snapshot with default-deny fixed-length `"████"` PII masking (`P3-01`).
    pub fn record_wireframe_snapshot(
        &self,
        width: u16,
        height: u16,
        nodes: Vec<SdkWireNode>,
    ) -> Result<(), SdkError> {
        if self.is_opted_out() {
            return Ok(());
        }
        let now_ms = Utc::now().timestamp_millis();
        let cur_session = self.touch_activity_and_maybe_rotate(now_ms)?;

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
                let masked_label = if n.label.is_empty() {
                    String::new()
                } else if kind == NodeKind::MaskedInput || !n.unmasked {
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
                    masked_label: if kind == NodeKind::MaskedInput && masked_label.is_empty() {
                        FIXED_MASK_PLACEHOLDER.to_string()
                    } else {
                        masked_label
                    },
                }
            })
            .collect();

        let mut ring = self.replay_ops_ring.lock();
        if ring.0.is_none() {
            ring.0 = Some(now_ms);
        }
        if ring.1.is_none() {
            ring.1 = Some(cur_session);
        }
        ring.2.push(WireframeOp::FullSnapshot {
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
        if self.is_opted_out() {
            return Ok("opted_out".to_string());
        }
        let cur_session = self.touch_activity_and_maybe_rotate(ts_ms)?;

        let kind = {
            let mut taps = self.tap_history.lock();
            while let Some(front) = taps.front() {
                if ts_ms.saturating_sub(front.ts_ms) > RAGE_TAP_WINDOW_MS {
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
                // Clear tap history immediately after detecting a rage-tap cluster (`P3-05`)
                // so clicks 4, 5, ... do not emit duplicate `$rage_tap` events on every tap!
                taps.clear();
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
            if ring.1.is_none() {
                ring.1 = Some(cur_session);
            }
            let ts_offset_ms = ts_ms.saturating_sub(base).max(0) as u32;
            ring.2.push(WireframeOp::PointerInteraction {
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

    /// Synchronizes the Edge Flag bundle (with ETag caching) and evaluates `flag_key` locally in < 1ms (`P2-02`, `P2-03`).
    pub async fn evaluate_flag(&self, flag_key: String) -> Result<Option<String>, SdkError> {
        if self.flag_cache.read().rules.is_empty() {
            let _ = self.refresh_flags().await;
        }
        let did = self.distinct_id();
        let cache = self.flag_cache.read();
        let Some(rule) = cache.rules.get(&flag_key) else {
            return Ok(None);
        };
        Ok(evaluate_flag_rule(
            &rule.key,
            rule.enabled,
            rule.rollout_bps,
            rule.cohort_members.as_deref(),
            rule.variants.iter().map(|v| (v.key.as_str(), v.weight_bps)),
            &did,
        ))
    }

    pub async fn refresh_flags(&self) -> Result<(), SdkError> {
        let base = self.config.endpoint_url.trim_end_matches('/');
        let url = format!("{base}/v1/flags/{}", self.config.api_key);
        let etag = self.flag_cache.read().etag.clone();
        let client = self.http_client.clone();

        let bundle_opt = sdk_runtime()
            .spawn(async move {
                let mut req = client.get(&url);
                if let Some(tag) = etag {
                    req = req.header("If-None-Match", tag);
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| SdkError::Network(e.to_string()))?;
                if resp.status().as_u16() == 304 {
                    return Ok(None);
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
                Ok(Some(bundle))
            })
            .await
            .map_err(|e| SdkError::Network(e.to_string()))??;

        if let Some(bundle) = bundle_opt {
            let mut guard = self.flag_cache.write();
            guard.etag = Some(bundle.etag);
            guard.rules = bundle
                .flags
                .into_iter()
                .map(|r| (r.key.clone(), r))
                .collect();
        }
        Ok(())
    }

    /// Seals active in-memory rings into the bounded 10 MB binary disk WAL and transmits all queued packets
    /// under a single flush mutex, removing packets strictly by ID (`P2-06`, `P2-07`, `P1-13`).
    pub async fn flush(&self) -> Result<u32, SdkError> {
        let _flush_guard = self.flush_lock.lock().await;
        self.seal_rings_to_disk_queue()?;
        self.sync_wal_io().await?;

        let now_ms = Utc::now().timestamp_millis();
        let backoff_until = self.backoff_until_ms.load(Ordering::Acquire);
        if backoff_until > now_ms {
            return Err(SdkError::Network(format!(
                "backing off for {}ms after transient server error",
                backoff_until - now_ms
            )));
        }

        let snapshot: Vec<WalQueueEntry> = {
            let guard = self.disk_queue.lock();
            guard.0.iter().cloned().collect()
        };
        if snapshot.is_empty() {
            return Ok(0);
        }

        let base = self.config.endpoint_url.trim_end_matches('/').to_string();
        let api_key = self.config.api_key.clone();
        let http = self.http_client.clone();

        let mut remove_ids: HashSet<u64> = HashSet::new();
        let mut sent_count = 0u32;
        let mut transient_err: Option<SdkError> = None;

        for entry in &snapshot {
            let http_c = http.clone();
            let base_c = base.clone();
            let key_c = api_key.clone();
            let pkt = entry.pkt.clone();

            let send_res = sdk_runtime()
                .spawn(async move {
                    match pkt {
                        QueuedPacket::Events(raw_bin) => {
                            // Stamp `client_sent_ts_ms` at actual transmit time (`P1-13`)!
                            let transmit_body = match WireBatch::decode_binary(&raw_bin) {
                                Ok(mut batch) => {
                                    let tx_now = Utc::now().timestamp_millis();
                                    for ev in &mut batch.events {
                                        ev.client_sent_ts_ms = tx_now;
                                    }
                                    batch.encode_binary(3).unwrap_or(raw_bin)
                                }
                                Err(_) => raw_bin,
                            };
                            http_c
                                .post(format!("{base_c}/v1/ingest"))
                                .header("x-pulse-key", &key_c)
                                .header("content-type", "application/x-pulse-frame")
                                .body(transmit_body)
                                .send()
                                .await
                        }
                        QueuedPacket::Identify {
                            anon_id,
                            user_id,
                            routing_seed,
                        } => {
                            http_c
                                .post(format!("{base_c}/v1/identify"))
                                .header("x-pulse-key", &key_c)
                                .json(&serde_json::json!({
                                    "anon_id": anon_id,
                                    "user_id": user_id,
                                    "routing_seed": routing_seed
                                }))
                                .send()
                                .await
                        }
                        QueuedPacket::Replay(bin) => {
                            http_c
                                .post(format!("{base_c}/v1/replay/chunk"))
                                .header("x-pulse-key", &key_c)
                                .header("content-type", "application/x-pulse-frame")
                                .body(bin)
                                .send()
                                .await
                        }
                        QueuedPacket::Engage(json_bytes) => {
                            http_c
                                .post(format!("{base_c}/v1/engage"))
                                .header("x-pulse-key", &key_c)
                                .header("content-type", "application/json")
                                .body(json_bytes)
                                .send()
                                .await
                        }
                    }
                })
                .await
                .map_err(|e| SdkError::Network(e.to_string()))?;

            match send_res {
                Ok(resp) if resp.status().is_success() => {
                    if matches!(entry.pkt, QueuedPacket::Replay(_)) {
                        if let Ok(Ok(v)) = sdk_runtime()
                            .spawn(async move { resp.json::<serde_json::Value>().await })
                            .await
                        {
                            if let Some(kb) = v.get("replay_offset_kb").and_then(|x| x.as_u64()) {
                                self.replay_offset_kb.store(kb as u32, Ordering::Release);
                            }
                        }
                    }
                    remove_ids.insert(entry.id);
                    sent_count += 1;
                    self.consecutive_failures.store(0, Ordering::Release);
                    self.backoff_until_ms.store(0, Ordering::Release);
                }
                Ok(resp) if matches!(resp.status().as_u16(), 400 | 413 | 422) => {
                    // Permanent client/schema rejection (`P2-06`): drop poison packet so queue head never stalls forever!
                    remove_ids.insert(entry.id);
                }
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_server_error() || status.as_u16() == 429 {
                        self.record_transient_backoff();
                    }
                    transient_err = Some(SdkError::Network(format!("HTTP {status}")));
                    break;
                }
                Err(err) => {
                    self.record_transient_backoff();
                    transient_err = Some(SdkError::Network(err.to_string()));
                    break;
                }
            }
        }

        if !remove_ids.is_empty() {
            self.remove_packets_by_id(&remove_ids);
            self.sync_wal_io().await?;
        }

        if let Some(err) = transient_err {
            Err(err)
        } else {
            Ok(sent_count)
        }
    }

    pub async fn shutdown(&self) -> Result<(), SdkError> {
        self.shutdown_flag.store(true, Ordering::Release);
        let _ = self.flush().await;
        let _ = self.sync_wal_io().await;
        Ok(())
    }
}

impl PulseClient {
    fn fresh_identity() -> PersistedIdentityState {
        let seed = format!("dev_{}", Uuid::new_v4().simple());
        let anon = format!("anon_{}", &Uuid::new_v4().simple().to_string()[..12]);
        let sess = format!("sess_{}", &Uuid::new_v4().simple().to_string()[..12]);
        PersistedIdentityState {
            routing_seed: seed,
            anon_id: anon,
            identified_user_id: None,
            session_id: sess,
            last_activity_ts_ms: 0,
            opted_out: false,
            super_props: Vec::new(),
        }
    }

    /// Checks session inactivity, sealing any pending replay ops under the OLD `session_id` BEFORE
    /// rotating `session_id`, and updates `last_activity_ts_ms` monotonically so backdated `track_at`
    /// timestamps never trigger spurious session rotations (`P3-05`).
    fn touch_activity_and_maybe_rotate(&self, activity_ts_ms: i64) -> Result<String, SdkError> {
        let needs_rotation = {
            let id = self.identity.read();
            id.last_activity_ts_ms > 0
                && activity_ts_ms > id.last_activity_ts_ms
                && activity_ts_ms.saturating_sub(id.last_activity_ts_ms)
                    > SESSION_INACTIVITY_TIMEOUT_MS
        };
        if needs_rotation {
            // Seal pending replay ops attributed to the outgoing session BEFORE rotating (`P3-05`)!
            self.seal_replay_ring_only()?;
        }

        let mut id = self.identity.write();
        if id.last_activity_ts_ms > 0
            && activity_ts_ms > id.last_activity_ts_ms
            && activity_ts_ms.saturating_sub(id.last_activity_ts_ms) > SESSION_INACTIVITY_TIMEOUT_MS
        {
            id.session_id = format!("sess_{}", &Uuid::new_v4().simple().to_string()[..12]);
            self.replay_offset_kb.store(0, Ordering::Release);
        }
        if id.last_activity_ts_ms == 0 || activity_ts_ms > id.last_activity_ts_ms {
            id.last_activity_ts_ms = activity_ts_ms;
            let snap = id.clone();
            let cur = id.session_id.clone();
            drop(id);
            self.persist_identity_async(snap);
            Ok(cur)
        } else {
            Ok(id.session_id.clone())
        }
    }

    fn persist_identity_async(&self, state: PersistedIdentityState) {
        if let Ok(bytes) = serde_json::to_vec(&state) {
            let _ = self.wal_io_tx.send(WalIoCmd::PersistIdentity(bytes));
        }
    }

    fn record_transient_backoff(&self) {
        let exp = self
            .consecutive_failures
            .fetch_add(1, Ordering::AcqRel)
            .min(6);
        let delay_ms = (500i64 << exp).min(30_000);
        let until = Utc::now().timestamp_millis().saturating_add(delay_ms);
        self.backoff_until_ms.store(until, Ordering::Release);
    }

    pub fn clear_backoff_for_retry(&self) {
        self.consecutive_failures.store(0, Ordering::Release);
        self.backoff_until_ms.store(0, Ordering::Release);
    }

    pub fn track_at(
        &self,
        event_name: String,
        props: Vec<(String, PropValue)>,
        event_ts_ms: i64,
    ) -> Result<(), SdkError> {
        if self.is_opted_out() {
            return Ok(());
        }
        let session_id = self.touch_activity_and_maybe_rotate(event_ts_ms)?;
        let (routing_seed, distinct_id, super_props) = {
            let id = self.identity.read();
            let did = id
                .identified_user_id
                .clone()
                .unwrap_or_else(|| id.anon_id.clone());
            (id.routing_seed.clone(), did, id.super_props.clone())
        };

        let mut merged_props = super_props;
        if let Some(started_ms) = self.timed_events.lock().remove(&event_name) {
            let dur_ms = (Utc::now().timestamp_millis() - started_ms).max(0);
            merged_props.push(("$duration_ms".to_string(), PropValue::Int(dur_ms)));
            merged_props.push((
                "$duration".to_string(),
                PropValue::Float((dur_ms as f64) / 1000.0),
            ));
        }
        for (k, v) in props {
            if let Some(pos) = merged_props.iter().position(|(ek, _)| ek == &k) {
                merged_props[pos].1 = v;
            } else {
                merged_props.push((k, v));
            }
        }

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
            // Overwritten with actual transmission timestamp in `flush()` (`P1-13`).
            client_sent_ts_ms: 0,
            props: merged_props,
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

    fn seal_replay_ring_only(&self) -> Result<(), SdkError> {
        let replay_ops = {
            let mut rr = self.replay_ops_ring.lock();
            if rr.2.is_empty() {
                None
            } else {
                let start_ts = rr.0.take().unwrap_or_else(|| Utc::now().timestamp_millis());
                let sess = rr.1.take().unwrap_or_else(|| self.session_id());
                let ops = std::mem::take(&mut rr.2);
                Some((start_ts, sess, ops))
            }
        };

        if let Some((chunk_start_ts_ms, session_id, ops)) = replay_ops {
            let chunk = ReplayChunk {
                project_id: String::new(),
                session_id,
                chunk_start_ts_ms,
                ops,
            };
            let bin = chunk
                .encode_binary(3)
                .map_err(|e| SdkError::Serialization(e.to_string()))?;
            self.enqueue_packet(QueuedPacket::Replay(bin));
        }
        Ok(())
    }

    fn seal_rings_to_disk_queue(&self) -> Result<(), SdkError> {
        self.seal_replay_ring_only()?;

        let events = {
            let mut er = self.event_ring.lock();
            if er.is_empty() {
                Vec::new()
            } else {
                std::mem::take(&mut *er)
            }
        };

        if !events.is_empty() {
            let batch = WireBatch::new(events);
            let bin = batch
                .encode_binary(1)
                .map_err(|e| SdkError::Serialization(e.to_string()))?;
            self.enqueue_packet(QueuedPacket::Events(bin));
        }
        Ok(())
    }

    fn enqueue_packet(&self, pkt: QueuedPacket) {
        let id = self.next_packet_id.fetch_add(1, Ordering::Relaxed);
        let entry = WalQueueEntry::new(id, pkt);
        let entry_disk_len = entry.disk_len();
        if entry_disk_len > MAX_DISK_QUEUE_BYTES {
            return;
        }

        let mut guard = self.disk_queue.lock();
        let mut evicted = false;
        while guard.1 + entry_disk_len > MAX_DISK_QUEUE_BYTES && !guard.0.is_empty() {
            if let Some(removed) = guard.0.pop_front() {
                guard.1 = guard.1.saturating_sub(removed.disk_len());
                evicted = true;
            }
        }
        let frame_clone = entry.frame_bytes.clone();
        guard.1 += entry_disk_len;
        guard.0.push_back(entry);

        if evicted {
            let snapshot = Self::concat_frames(&guard.0, guard.1);
            let _ = self.wal_io_tx.send(WalIoCmd::Compact(snapshot));
        } else {
            let _ = self.wal_io_tx.send(WalIoCmd::Append(frame_clone));
        }
    }

    fn remove_packets_by_id(&self, ids: &HashSet<u64>) {
        let mut guard = self.disk_queue.lock();
        guard.0.retain(|e| !ids.contains(&e.id));
        guard.1 = guard.0.iter().map(|e| e.disk_len()).sum();
        let snapshot = Self::concat_frames(&guard.0, guard.1);
        let _ = self.wal_io_tx.send(WalIoCmd::Compact(snapshot));
    }

    fn concat_frames(queue: &VecDeque<WalQueueEntry>, cap: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(cap);
        for e in queue {
            out.extend_from_slice(&e.frame_bytes);
        }
        out
    }

    async fn sync_wal_io(&self) -> Result<(), SdkError> {
        let (tx, rx) = oneshot::channel();
        if self.wal_io_tx.send(WalIoCmd::Sync(tx)).is_ok() {
            let _ = rx.await;
        }
        let _ = &self.wal_path;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use axum::body::Bytes;
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::Router;

    use super::*;

    #[tokio::test]
    async fn rage_tap_session_rotation_and_offline_disk_wal_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = SdkConfig {
            endpoint_url: "http://127.0.0.1:9".to_string(),
            api_key: "pk_test".to_string(),
            storage_dir: dir.path().to_string_lossy().to_string(),
            flush_batch_size: 10,
            flush_interval_ms: 0,
        };

        let client = PulseClient::new(cfg.clone()).unwrap();
        let s1 = client.session_id();
        let base_ts = Utc::now().timestamp_millis();
        client.track_at("App_Open".into(), vec![], base_ts).unwrap();
        assert_eq!(client.session_id(), s1);

        // Backdated `track_at` must NOT regress `last_activity_ts_ms` or trigger spurious rotation (`P3-05`)!
        client
            .track_at("Backdated_Import".into(), vec![], base_ts - 3_600_000)
            .unwrap();
        assert_eq!(client.session_id(), s1);
        client
            .track_at("Still_Same_Session".into(), vec![], base_ts + 5_000)
            .unwrap();
        assert_eq!(client.session_id(), s1);

        // Record wireframe on session s1 BEFORE 31-minute inactivity gap (`P3-05`)
        client
            .record_wireframe_snapshot(
                390,
                844,
                vec![SdkWireNode {
                    node_id: 1,
                    parent_id: 0,
                    kind: "text".into(),
                    x: 0,
                    y: 0,
                    w: 100,
                    h: 20,
                    bg_rgba: 0xFF,
                    label: "John Doe Balance ₹99,500".into(),
                    unmasked: false,
                }],
            )
            .unwrap();

        // 31 minutes later -> pending replay must seal under `s1`, and session rotates to `s2`!
        let t_later = Utc::now().timestamp_millis() + 31 * 60 * 1000;
        client
            .track_at("Browse_Credit_Card".into(), vec![], t_later)
            .unwrap();
        let s2 = client.session_id();
        assert_ne!(s1, s2, "session must rotate after 30-min inactivity");

        // 3 rapid taps -> Rage Tap; 4th tap must NOT emit a second Rage Tap (`P3-05`)!
        let t_tap = t_later + 1_000;
        assert_eq!(client.record_touch(200, 300, true, t_tap).unwrap(), "tap");
        assert_eq!(
            client.record_touch(210, 305, true, t_tap + 200).unwrap(),
            "tap"
        );
        assert_eq!(
            client.record_touch(195, 298, true, t_tap + 450).unwrap(),
            "rage_tap"
        );
        assert_eq!(
            client.record_touch(196, 299, true, t_tap + 550).unwrap(),
            "tap",
            "4th click immediately after rage_tap must not re-fire rage_tap!"
        );

        // Flush while offline -> must persist in binary CRC-checked `sdk_wal.bin`
        assert!(client.flush().await.is_err());
        let queued = client.queued_packet_count();
        assert!(queued >= 2);
        drop(client);

        // Restart SDK from same directory -> disk WAL survives restart!
        let recovered = PulseClient::new(cfg).unwrap();
        assert_eq!(recovered.queued_packet_count(), queued);
    }

    #[tokio::test]
    async fn p2_06_p2_07_p1_13_concurrent_flush_id_removal_4xx_drop_and_disk_bound() {
        let ingest_calls = Arc::new(AtomicUsize::new(0));
        let last_sent_ts = Arc::new(AtomicI64::new(0));
        let return_422_first = Arc::new(AtomicBool::new(true));

        let ic = ingest_calls.clone();
        let st = last_sent_ts.clone();
        let r422 = return_422_first.clone();

        let app = Router::new().route(
            "/v1/ingest",
            post(move |body: Bytes| {
                let ic = ic.clone();
                let st = st.clone();
                let r422 = r422.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    ic.fetch_add(1, Ordering::SeqCst);
                    if r422.swap(false, Ordering::SeqCst) {
                        return StatusCode::UNPROCESSABLE_ENTITY;
                    }
                    let batch = WireBatch::decode_binary(&body).unwrap();
                    if let Some(ev) = batch.events.first() {
                        st.store(ev.client_sent_ts_ms, Ordering::SeqCst);
                    }
                    StatusCode::OK
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let dir = tempfile::tempdir().unwrap();
        let client = PulseClient::new(SdkConfig {
            endpoint_url: format!("http://{addr}"),
            api_key: "pk_sdk".into(),
            storage_dir: dir.path().to_string_lossy().to_string(),
            flush_batch_size: 1,
            flush_interval_ms: 0,
        })
        .unwrap();

        // Packet 1 (poison -> 422 dropped) and Packet 2 (valid -> 200 OK with fresh transmit `client_sent_ts_ms`)
        let old_event_ts = 1_700_000_000_000i64;
        client
            .track_at("Poison_Batch".into(), vec![], old_event_ts)
            .unwrap();
        client
            .track_at("Valid_Offline_Batch".into(), vec![], old_event_ts + 1000)
            .unwrap();

        // Race two concurrent `flush()` calls (`P2-06`): single mutex must prevent double-sending or over-popping!
        let c1 = client.clone();
        let c2 = client.clone();
        let (f1, f2) = tokio::join!(c1.flush(), c2.flush());
        assert!(f1.is_ok() && f2.is_ok());
        assert_eq!(client.queued_packet_count(), 0);
        assert_eq!(
            ingest_calls.load(Ordering::SeqCst),
            2,
            "concurrent flushes must not send duplicate packets"
        );
        let transmitted_sent_ts = last_sent_ts.load(Ordering::SeqCst);
        assert!(
            transmitted_sent_ts > 1_760_000_000_000,
            "SDK must stamp client_sent_ts_ms at transmit time, got {transmitted_sent_ts}"
        );

        // Verify `reset()` rotates anon_id and session_id (`P1-08`)
        client.identify("alice@pulse.in".into()).unwrap();
        assert_eq!(client.distinct_id(), "alice@pulse.in");
        let prev_sess = client.session_id();
        client.reset().unwrap();
        assert_ne!(client.distinct_id(), "alice@pulse.in");
        assert!(client.distinct_id().starts_with("anon_"));
        assert_ne!(client.session_id(), prev_sess);

        // Verify `sdk_wal.bin` CRC corruption truncation & 10 MiB on-disk byte bound (`P2-07`)
        let wal_file = dir.path().join("sdk_wal.bin");
        let offline = PulseClient::new(SdkConfig {
            endpoint_url: "http://127.0.0.1:9".into(),
            api_key: "pk_sdk".into(),
            storage_dir: dir.path().to_string_lossy().to_string(),
            flush_batch_size: 1,
            flush_interval_ms: 0,
        })
        .unwrap();
        let big_str = "x".repeat(40_000);
        // Enqueue > 12 MB of uncompressible random-ish payloads and verify on-disk file <= 10 MiB!
        for i in 0..400 {
            let mut noisy = String::with_capacity(32_000);
            for j in 0..4000 {
                noisy.push_str(&format!("{:08x}", (i * 4099 + j * 131071) as u32));
            }
            let _ = &big_str;
            offline
                .track_at(
                    "Heavy".into(),
                    vec![("blob".into(), PropValue::Str(noisy))],
                    1_760_000_000_000 + i,
                )
                .unwrap();
        }
        offline.sync_wal_io().await.unwrap();
        let on_disk_len = fs::metadata(&wal_file).unwrap().len() as usize;
        assert!(
            on_disk_len <= MAX_DISK_QUEUE_BYTES,
            "on-disk WAL size {on_disk_len} exceeded 10 MiB bound!"
        );
    }

    #[test]
    fn p2_08_sdk_works_from_plain_os_thread_without_tokio_runtime() {
        // Spawn a bare OS thread with ZERO ambient Tokio runtime!
        let handle = std::thread::spawn(|| {
            assert!(
                tokio::runtime::Handle::try_current().is_err(),
                "test thread must have no ambient Tokio runtime"
            );
            let dir = tempfile::tempdir().unwrap();
            let client = PulseClient::new(SdkConfig {
                endpoint_url: "http://127.0.0.1:9".into(),
                api_key: "pk_no_rt".into(),
                storage_dir: dir.path().to_string_lossy().to_string(),
                flush_batch_size: 1,
                flush_interval_ms: 250,
            })
            .expect("PulseClient::new must succeed without ambient Tokio runtime");

            client
                .track("BareThreadEvent".into(), r#"{"ok":true}"#.into())
                .unwrap();
            // Poll async `flush()` using `futures::executor::block_on` (non-Tokio executor!)
            let res = futures::executor::block_on(client.flush());
            assert!(res.is_err()); // Port 9 unreachable, but MUST NOT panic with "no reactor running"!
            assert_eq!(client.queued_packet_count(), 1);
            futures::executor::block_on(client.shutdown()).unwrap();
        });
        handle.join().expect("bare OS thread must not panic");
    }

    #[tokio::test]
    async fn en1_rapid_identity_persistence_never_races_or_rotates_session_id() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = SdkConfig {
            endpoint_url: "http://127.0.0.1:9".to_string(),
            api_key: "pk_en1".to_string(),
            storage_dir: dir.path().to_string_lossy().to_string(),
            flush_batch_size: 50,
            flush_interval_ms: 0,
        };

        let expected_session_id = {
            let client = PulseClient::new(cfg.clone()).unwrap();
            let sid = client.session_id();
            client
                .record_wireframe_snapshot(
                    390,
                    844,
                    vec![SdkWireNode {
                        node_id: 1,
                        parent_id: 0,
                        kind: "masked_input".into(),
                        x: 10,
                        y: 10,
                        w: 100,
                        h: 40,
                        bg_rgba: 0x1122_33FF,
                        label: "secret".into(),
                        unmasked: false,
                    }],
                )
                .unwrap();
            let now = Utc::now().timestamp_millis();
            for i in 0..50 {
                client.record_touch(100, 100, true, now + i * 20).unwrap();
            }
            client.identify("+919900099999".into()).unwrap();
            client
                .track("Quick_Event".into(), r#"{"ok":true}"#.into())
                .unwrap();
            let _ = client.flush().await;
            sid
        };

        let reopened = PulseClient::new(cfg).unwrap();
        assert_eq!(
            reopened.session_id(),
            expected_session_id,
            "rapid concurrent identity updates must preserve session_id across restart"
        );
        assert_eq!(reopened.distinct_id(), "+919900099999");
    }

    #[tokio::test]
    async fn phase3_consent_super_props_timed_events_profiles_and_revenue() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = SdkConfig {
            endpoint_url: "http://127.0.0.1:9".to_string(),
            api_key: "pk_phase3".to_string(),
            storage_dir: dir.path().to_string_lossy().to_string(),
            flush_batch_size: 1,
            flush_interval_ms: 0,
        };
        let client = PulseClient::new(cfg.clone()).unwrap();
        assert!(!client.is_opted_out());

        client
            .register_super_props(r#"{"app_tier":"fintech","env":"prod"}"#.into())
            .unwrap();
        client.time_event("KYC_Submit".into()).unwrap();
        client
            .track("KYC_Submit".into(), r#"{"step":1}"#.into())
            .unwrap();
        client
            .set_user_properties(r#"{"plan":"credit_pro"}"#.into())
            .unwrap();
        client
            .track_revenue(999.0, "INR".into(), Some("pro_sub".into()))
            .unwrap();

        // Events, engage, and revenue packets queued in WAL
        assert!(client.queued_packet_count() >= 4);

        // Verify super props and duration were stamped in the queued WireBatch
        let has_super_and_dur = {
            let guard = client.disk_queue.lock();
            guard.0.iter().any(|entry| {
                if let QueuedPacket::Events(bin) = &entry.pkt {
                    let batch = WireBatch::decode_binary(bin).unwrap();
                    batch.events.iter().any(|e| {
                        e.event_name == "KYC_Submit"
                            && e.get_prop("app_tier") == Some(&PropValue::Str("fintech".into()))
                            && e.get_prop("$duration_ms").is_some()
                    })
                } else {
                    false
                }
            })
        };
        assert!(
            has_super_and_dur,
            "super props + $duration_ms must be stamped on event"
        );

        // Opt-out clears queues and blocks further tracking/profile updates
        client.opt_out().unwrap();
        assert!(client.is_opted_out());
        assert_eq!(client.queued_packet_count(), 0);
        client.track("Blocked_Event".into(), "{}".into()).unwrap();
        client
            .set_user_properties(r#"{"blocked":true}"#.into())
            .unwrap();
        assert_eq!(client.queued_packet_count(), 0);

        client.sync_wal_io().await.unwrap();
        drop(client);

        // Opted-out state survives SDK restart
        let reopened = PulseClient::new(cfg).unwrap();
        assert!(reopened.is_opted_out());
        reopened.opt_in().unwrap();
        assert!(!reopened.is_opted_out());
    }
}
