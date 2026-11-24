use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use dashmap::DashMap;
use parking_lot::Mutex;
use pulse_storage::{validate_replay_identifier, StorageError};
use pulse_wire::WireBatch;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use uuid::Uuid;

pub const WRITER_LOG_MAGIC: [u8; 4] = *b"PWLF";
pub const WRITER_LOG_VERSION: u8 = 1;
pub const WRITER_LOG_HEADER_LEN: usize = 29;

pub const DEFAULT_WRITER_LOG_SEGMENT_MAX_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_WRITER_LOG_IDLE_CLOSE_MS: u64 = 5_000;
/// Hard max batch age strictly `< T_expire` (7 days) so expired batches are dropped on the writer
/// before the storage node's `(writer_id, bucket_id)` offset window expires.
pub const DEFAULT_WRITER_LOG_MAX_AGE_SECS: u64 = 6 * 86_400;
pub const DEFAULT_WRITER_LOG_PROJECT_MAX_MB: usize = 512;
pub const DEFAULT_WRITER_LOG_NODE_MAX_MB: usize = 4096;

#[derive(Debug, Clone, Copy, Default)]
pub struct WriterLogStats {
    pub bytes: usize,
    pub capacity_bytes: usize,
    pub unforwarded_batches: usize,
    pub unforwarded_events: usize,
    pub forwarding_lag_seconds: f64,
    pub open_handles: usize,
    pub expired_batches_dropped_total: u64,
    pub expired_events_dropped_total: u64,
}

#[derive(Debug, Clone)]
pub struct WriterLogBatch {
    pub batch_offset: u64,
    pub enqueued_at_ms: i64,
    pub event_count: usize,
    pub plsb_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CursorFile {
    acked_offset: Option<u64>,
    updated_at_ms: i64,
}

#[derive(Debug, Clone)]
struct FrameIndex {
    offset: u64,
    file_offset: u64,
    payload_len: u32,
    enqueued_at_ms: i64,
    event_count: usize,
}

#[derive(Debug)]
struct SegmentIndex {
    path: PathBuf,
    last_offset: u64,
    file_bytes: usize,
    frames: VecDeque<FrameIndex>,
}

struct ProjectLogState {
    proj_dir: PathBuf,
    open_write_file: Option<(PathBuf, File)>,
    last_io_at: Instant,
    segments: VecDeque<SegmentIndex>,
    acked_offset: Option<u64>,
    next_offset: u64,
    disk_bytes: usize,
    unforwarded_batches: usize,
    unforwarded_events: usize,
    oldest_unforwarded_ts_ms: Option<i64>,
}

struct ProjectLogHandle {
    state: Mutex<ProjectLogState>,
    notify: Notify,
}

pub struct WriterLogManager {
    root_dir: PathBuf,
    writer_id: String,
    segment_max_bytes: AtomicUsize,
    idle_close_ms: AtomicU64,
    max_age_ms: AtomicI64,
    project_max_bytes: AtomicUsize,
    node_max_bytes: AtomicUsize,
    node_disk_bytes: AtomicUsize,
    node_unforwarded_batches: AtomicUsize,
    node_unforwarded_events: AtomicUsize,
    open_handles: AtomicUsize,
    expired_batches_dropped_total: AtomicU64,
    expired_events_dropped_total: AtomicU64,
    sticky_io_error: Mutex<Option<String>>,
    projects: DashMap<String, Arc<ProjectLogHandle>>,
}

fn env_usize_bytes(bytes_var: &str, mb_var: &str, default_mb: usize) -> usize {
    if let Ok(raw) = std::env::var(bytes_var) {
        if let Ok(v) = raw.trim().parse::<usize>() {
            if v > 0 {
                return v;
            }
        }
    }
    std::env::var(mb_var)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&mb| mb > 0)
        .unwrap_or(default_mb)
        .saturating_mul(1024 * 1024)
}

fn max_age_ms_from_env() -> i64 {
    if let Ok(raw) = std::env::var("PULSE_WRITER_LOG_MAX_AGE_MS") {
        if let Ok(ms) = raw.trim().parse::<i64>() {
            if ms > 0 {
                return ms;
            }
        }
    }
    let secs = std::env::var("PULSE_WRITER_LOG_MAX_AGE_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&s| s > 0)
        .unwrap_or(DEFAULT_WRITER_LOG_MAX_AGE_SECS);
    (secs.saturating_mul(1_000)).min(i64::MAX as u64) as i64
}

fn idle_close_ms_from_env() -> u64 {
    std::env::var("PULSE_WRITER_LOG_IDLE_CLOSE_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .unwrap_or(DEFAULT_WRITER_LOG_IDLE_CLOSE_MS)
}

fn segment_max_bytes_from_env() -> usize {
    std::env::var("PULSE_WRITER_LOG_SEGMENT_MAX_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&b| b >= 1024)
        .unwrap_or(DEFAULT_WRITER_LOG_SEGMENT_MAX_BYTES)
}

fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    let f = File::open(dir)?;
    f.sync_all()
}

fn atomic_write_file(dir: &Path, filename: &str, bytes: &[u8]) -> std::io::Result<()> {
    fs::create_dir_all(dir)?;
    let tmp_name = format!(".{filename}.tmp.{}", Uuid::new_v4().simple());
    let tmp_path = dir.join(tmp_name);
    let final_path = dir.join(filename);
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)?;
        f.write_all(bytes)?;
        f.sync_data()?;
    }
    fs::rename(&tmp_path, &final_path)?;
    fsync_dir(dir)?;
    Ok(())
}

fn read_or_create_writer_id(root_dir: &Path) -> std::result::Result<String, StorageError> {
    fs::create_dir_all(root_dir)?;
    let id_path = root_dir.join("writer_id");
    match fs::read_to_string(&id_path) {
        Ok(raw) => {
            let trimmed = raw.trim().to_string();
            if !trimmed.is_empty() {
                return Ok(trimmed);
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(StorageError::Io(e)),
    }
    let fresh = Uuid::new_v4().to_string();
    atomic_write_file(root_dir, "writer_id", fresh.as_bytes())?;
    Ok(fresh)
}

fn parse_segment_first_offset(name: &str) -> Option<u64> {
    let body = name.strip_prefix("seg_")?.strip_suffix(".log")?;
    body.parse::<u64>().ok()
}

fn compute_frame_crc(header_without_magic_or_crc: &[u8], payload: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(header_without_magic_or_crc);
    hasher.update(payload);
    hasher.finalize()
}

fn encode_frame(
    batch_offset: u64,
    enqueued_at_ms: i64,
    payload: &[u8],
) -> Result<Vec<u8>, StorageError> {
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| StorageError::Corrupted("writer_log frame exceeds 4 GiB".into()))?;
    let mut out = Vec::with_capacity(WRITER_LOG_HEADER_LEN + payload.len());
    out.extend_from_slice(&WRITER_LOG_MAGIC);
    out.push(WRITER_LOG_VERSION);
    out.extend_from_slice(&batch_offset.to_le_bytes());
    out.extend_from_slice(&enqueued_at_ms.to_le_bytes());
    out.extend_from_slice(&payload_len.to_le_bytes());
    let crc = compute_frame_crc(&out[4..25], payload);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

fn load_cursor(proj_dir: &Path) -> Result<Option<u64>, StorageError> {
    let cursor_path = proj_dir.join("cursor.json");
    match fs::read(&cursor_path) {
        Ok(bytes) => {
            let parsed: CursorFile = serde_json::from_slice(&bytes)?;
            Ok(parsed.acked_offset)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StorageError::Io(e)),
    }
}

fn write_cursor(
    proj_dir: &Path,
    acked_offset: Option<u64>,
    now_ms: i64,
) -> Result<(), StorageError> {
    let cursor = CursorFile {
        acked_offset,
        updated_at_ms: now_ms,
    };
    let bytes = serde_json::to_vec(&cursor)?;
    atomic_write_file(proj_dir, "cursor.json", &bytes)?;
    Ok(())
}

fn recover_project_state(proj_dir: &Path) -> Result<ProjectLogState, StorageError> {
    fs::create_dir_all(proj_dir)?;
    let acked_offset = load_cursor(proj_dir)?;

    let mut seg_files: Vec<(u64, PathBuf)> = Vec::new();
    for entry in fs::read_dir(proj_dir)? {
        let entry = entry?;
        let fname = entry.file_name();
        let fname_str = fname.to_string_lossy();
        if fname_str.starts_with('.') && fname_str.contains(".tmp.") {
            let _ = fs::remove_file(entry.path());
            continue;
        }
        if let Some(first_off) = parse_segment_first_offset(&fname_str) {
            seg_files.push((first_off, entry.path()));
        }
    }
    seg_files.sort_unstable_by_key(|(off, _)| *off);

    let mut segments = VecDeque::new();
    let mut max_offset_seen = acked_offset;
    let mut disk_bytes = 0usize;
    let mut unforwarded_batches = 0usize;
    let mut unforwarded_events = 0usize;
    let mut oldest_unforwarded_ts_ms: Option<i64> = None;

    for (_first_offset, path) in seg_files {
        let mut f = OpenOptions::new().read(true).write(true).open(&path)?;
        let file_len = f.metadata()?.len();
        let mut pos = 0u64;
        let mut frames = VecDeque::new();
        let mut last_offset = 0u64;

        while pos + (WRITER_LOG_HEADER_LEN as u64) <= file_len {
            let mut hdr = [0u8; WRITER_LOG_HEADER_LEN];
            if f.seek(SeekFrom::Start(pos)).is_err() || f.read_exact(&mut hdr).is_err() {
                break;
            }
            if hdr[0..4] != WRITER_LOG_MAGIC || hdr[4] != WRITER_LOG_VERSION {
                break;
            }
            let offset = u64::from_le_bytes(hdr[5..13].try_into().unwrap());
            let enqueued_at_ms = i64::from_le_bytes(hdr[13..21].try_into().unwrap());
            let payload_len = u32::from_le_bytes(hdr[21..25].try_into().unwrap());
            let expected_crc = u32::from_le_bytes(hdr[25..29].try_into().unwrap());
            let frame_end = pos + (WRITER_LOG_HEADER_LEN as u64) + u64::from(payload_len);
            if frame_end > file_len {
                break;
            }
            let mut payload = vec![0u8; payload_len as usize];
            if f.read_exact(&mut payload).is_err() {
                break;
            }
            let actual_crc = compute_frame_crc(&hdr[4..25], &payload);
            if actual_crc != expected_crc {
                break;
            }
            let Ok(batch) = WireBatch::decode_binary(&payload) else {
                break;
            };
            let event_count = batch.events.len();
            max_offset_seen = Some(max_offset_seen.map_or(offset, |m| m.max(offset)));
            last_offset = offset;
            if acked_offset.is_none_or(|a| offset > a) {
                unforwarded_batches += 1;
                unforwarded_events += event_count;
                oldest_unforwarded_ts_ms = Some(
                    oldest_unforwarded_ts_ms.map_or(enqueued_at_ms, |m| m.min(enqueued_at_ms)),
                );
                frames.push_back(FrameIndex {
                    offset,
                    file_offset: pos,
                    payload_len,
                    enqueued_at_ms,
                    event_count,
                });
            }
            pos = frame_end;
        }

        if pos < file_len {
            tracing::warn!(
                path = %path.display(),
                from_len = file_len,
                valid_len = pos,
                "truncating incomplete trailing writer_log frame after crash"
            );
            f.set_len(pos)?;
            f.sync_data()?;
        }
        drop(f);

        if pos == 0 || (acked_offset.is_some_and(|a| last_offset <= a) && frames.is_empty()) {
            let _ = fs::remove_file(&path);
            continue;
        }

        let seg_bytes = pos as usize;
        disk_bytes += seg_bytes;
        segments.push_back(SegmentIndex {
            path,
            last_offset,
            file_bytes: seg_bytes,
            frames,
        });
    }

    let next_offset = max_offset_seen.map_or(0, |m| m.saturating_add(1));
    Ok(ProjectLogState {
        proj_dir: proj_dir.to_path_buf(),
        open_write_file: None,
        last_io_at: Instant::now(),
        segments,
        acked_offset,
        next_offset,
        disk_bytes,
        unforwarded_batches,
        unforwarded_events,
        oldest_unforwarded_ts_ms,
    })
}

impl WriterLogManager {
    pub fn open(data_dir: &Path) -> Result<Arc<Self>, StorageError> {
        let root_dir = data_dir.join("writer_log");
        let writer_id = read_or_create_writer_id(&root_dir)?;
        let projects = DashMap::new();
        let mut node_disk_bytes = 0usize;
        let mut node_unforwarded_batches = 0usize;
        let mut node_unforwarded_events = 0usize;

        for entry in fs::read_dir(&root_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let pid = entry.file_name().to_string_lossy().to_string();
            if validate_replay_identifier("project_id", &pid).is_err() {
                continue;
            }
            let st = recover_project_state(&entry.path())?;
            node_disk_bytes += st.disk_bytes;
            node_unforwarded_batches += st.unforwarded_batches;
            node_unforwarded_events += st.unforwarded_events;
            projects.insert(
                pid,
                Arc::new(ProjectLogHandle {
                    state: Mutex::new(st),
                    notify: Notify::new(),
                }),
            );
        }

        Ok(Arc::new(Self {
            root_dir,
            writer_id,
            segment_max_bytes: AtomicUsize::new(segment_max_bytes_from_env()),
            idle_close_ms: AtomicU64::new(idle_close_ms_from_env()),
            max_age_ms: AtomicI64::new(max_age_ms_from_env()),
            project_max_bytes: AtomicUsize::new(env_usize_bytes(
                "PULSE_WRITER_LOG_PROJECT_MAX_BYTES",
                "PULSE_WRITER_LOG_PROJECT_MAX_MB",
                DEFAULT_WRITER_LOG_PROJECT_MAX_MB,
            )),
            node_max_bytes: AtomicUsize::new(env_usize_bytes(
                "PULSE_WRITER_LOG_MAX_BYTES",
                "PULSE_WRITER_LOG_MAX_MB",
                DEFAULT_WRITER_LOG_NODE_MAX_MB,
            )),
            node_disk_bytes: AtomicUsize::new(node_disk_bytes),
            node_unforwarded_batches: AtomicUsize::new(node_unforwarded_batches),
            node_unforwarded_events: AtomicUsize::new(node_unforwarded_events),
            open_handles: AtomicUsize::new(0),
            expired_batches_dropped_total: AtomicU64::new(0),
            expired_events_dropped_total: AtomicU64::new(0),
            sticky_io_error: Mutex::new(None),
            projects,
        }))
    }

    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub fn idle_close_duration(&self) -> Duration {
        Duration::from_millis(self.idle_close_ms.load(Ordering::Relaxed).max(1))
    }

    #[doc(hidden)]
    pub fn set_idle_close_for_test(&self, d: Duration) {
        self.idle_close_ms
            .store(d.as_millis().max(1) as u64, Ordering::Relaxed);
    }

    #[doc(hidden)]
    pub fn set_max_batch_age_for_test(&self, d: Duration) {
        let ms = (d.as_millis() as i64).max(1);
        self.max_age_ms.store(ms, Ordering::Relaxed);
    }

    #[doc(hidden)]
    pub fn set_disk_limits_for_test(&self, project_max_bytes: usize, node_max_bytes: usize) {
        self.project_max_bytes
            .store(project_max_bytes.max(1), Ordering::Relaxed);
        self.node_max_bytes
            .store(node_max_bytes.max(1), Ordering::Relaxed);
    }

    #[doc(hidden)]
    pub fn set_segment_max_bytes_for_test(&self, segment_max_bytes: usize) {
        self.segment_max_bytes
            .store(segment_max_bytes.max(1), Ordering::Relaxed);
    }

    pub fn health_check(&self) -> Result<(), String> {
        if let Some(ref err) = *self.sticky_io_error.lock() {
            return Err(err.clone());
        }
        if !self.root_dir.exists() {
            return Err("writer_log root directory missing".to_string());
        }
        Ok(())
    }

    fn record_io_error(&self, err: &StorageError) -> StorageError {
        let msg = err.to_string();
        *self.sticky_io_error.lock() = Some(msg.clone());
        StorageError::Io(std::io::Error::other(msg))
    }

    fn get_or_create_project(
        &self,
        project_id: &str,
    ) -> Result<Arc<ProjectLogHandle>, StorageError> {
        validate_replay_identifier("project_id", project_id)?;
        if let Some(existing) = self.projects.get(project_id) {
            return Ok(existing.clone());
        }
        let proj_dir = self.root_dir.join(project_id);
        let st = recover_project_state(&proj_dir).map_err(|err| {
            if matches!(err, StorageError::Io(_)) {
                self.record_io_error(&err)
            } else {
                err
            }
        })?;
        let mut created = false;
        let handle = self
            .projects
            .entry(project_id.to_string())
            .or_insert_with(|| {
                created = true;
                Arc::new(ProjectLogHandle {
                    state: Mutex::new(st),
                    notify: Notify::new(),
                })
            })
            .clone();
        if created {
            let st = handle.state.lock();
            self.node_disk_bytes
                .fetch_add(st.disk_bytes, Ordering::AcqRel);
            self.node_unforwarded_batches
                .fetch_add(st.unforwarded_batches, Ordering::AcqRel);
            self.node_unforwarded_events
                .fetch_add(st.unforwarded_events, Ordering::AcqRel);
        }
        Ok(handle)
    }

    /// Verifies disk capacity limits and sticky I/O health before admitting events into memory.
    pub fn check_admission(
        &self,
        project_id: &str,
        proj_buffered_bytes: usize,
        node_buffered_bytes: usize,
        incoming_bytes: usize,
    ) -> Result<(), StorageError> {
        if let Some(ref err) = *self.sticky_io_error.lock() {
            return Err(StorageError::Io(std::io::Error::other(err.clone())));
        }
        validate_replay_identifier("project_id", project_id)?;
        if incoming_bytes == 0 {
            return Ok(());
        }
        let proj_disk = self
            .projects
            .get(project_id)
            .map(|h| h.state.lock().disk_bytes)
            .unwrap_or(0);
        let proj_max = self.project_max_bytes.load(Ordering::Relaxed);
        let proj_total = proj_disk
            .saturating_add(proj_buffered_bytes)
            .saturating_add(incoming_bytes);
        if proj_total > proj_max {
            return Err(StorageError::Backpressure {
                project_id: project_id.to_string(),
                what: "writer_log",
                bytes: proj_total,
                max_bytes: proj_max,
            });
        }
        let node_disk = self.node_disk_bytes.load(Ordering::Acquire);
        let node_max = self.node_max_bytes.load(Ordering::Relaxed);
        let node_total = node_disk
            .saturating_add(node_buffered_bytes)
            .saturating_add(incoming_bytes);
        if node_total > node_max {
            return Err(StorageError::Backpressure {
                project_id: project_id.to_string(),
                what: "writer_log",
                bytes: node_total,
                max_bytes: node_max,
            });
        }
        Ok(())
    }

    pub fn append_batch(
        &self,
        project_id: &str,
        batch: &WireBatch,
        enqueued_at_ms: i64,
    ) -> Result<u64, StorageError> {
        if let Some(ref err) = *self.sticky_io_error.lock() {
            return Err(StorageError::Io(std::io::Error::other(err.clone())));
        }
        let payload = batch.encode_binary(1)?;
        let event_count = batch.events.len();
        let handle = self.get_or_create_project(project_id)?;
        let seg_max = self.segment_max_bytes.load(Ordering::Relaxed);

        let offset = {
            let mut st = handle.state.lock();
            let offset = st.next_offset;
            let frame_bytes = encode_frame(offset, enqueued_at_ms, &payload)?;

            let need_new_segment = match st.segments.back() {
                Some(seg) => seg.file_bytes >= seg_max,
                None => true,
            };

            if need_new_segment {
                if st.open_write_file.take().is_some() {
                    self.open_handles.fetch_sub(1, Ordering::AcqRel);
                }
                let seg_name = format!("seg_{offset:016}.log");
                let seg_path = st.proj_dir.join(&seg_name);
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&seg_path)
                    .map_err(|e| self.record_io_error(&StorageError::Io(e)))?;
                fsync_dir(&st.proj_dir).map_err(|e| self.record_io_error(&StorageError::Io(e)))?;
                st.open_write_file = Some((seg_path.clone(), file));
                self.open_handles.fetch_add(1, Ordering::AcqRel);
                st.segments.push_back(SegmentIndex {
                    path: seg_path,
                    last_offset: offset,
                    file_bytes: 0,
                    frames: VecDeque::new(),
                });
            } else if st.open_write_file.is_none() {
                let seg_path = st.segments.back().unwrap().path.clone();
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&seg_path)
                    .map_err(|e| self.record_io_error(&StorageError::Io(e)))?;
                st.open_write_file = Some((seg_path, file));
                self.open_handles.fetch_add(1, Ordering::AcqRel);
            }

            let file_offset = st.segments.back().map_or(0, |s| s.file_bytes as u64);
            {
                let (_, ref mut file) = st.open_write_file.as_mut().unwrap();
                file.write_all(&frame_bytes)
                    .and_then(|_| file.sync_data())
                    .map_err(|e| self.record_io_error(&StorageError::Io(e)))?;
            }

            let seg = st.segments.back_mut().unwrap();
            seg.last_offset = offset;
            seg.file_bytes += frame_bytes.len();
            seg.frames.push_back(FrameIndex {
                offset,
                file_offset,
                payload_len: payload.len() as u32,
                enqueued_at_ms,
                event_count,
            });
            st.next_offset = offset.saturating_add(1);
            st.disk_bytes += frame_bytes.len();
            st.unforwarded_batches += 1;
            st.unforwarded_events += event_count;
            st.oldest_unforwarded_ts_ms = Some(
                st.oldest_unforwarded_ts_ms
                    .map_or(enqueued_at_ms, |m| m.min(enqueued_at_ms)),
            );
            st.last_io_at = Instant::now();
            self.node_disk_bytes
                .fetch_add(frame_bytes.len(), Ordering::AcqRel);
            self.node_unforwarded_batches.fetch_add(1, Ordering::AcqRel);
            self.node_unforwarded_events
                .fetch_add(event_count, Ordering::AcqRel);
            offset
        };

        handle.notify.notify_one();
        Ok(offset)
    }

    pub fn list_active_projects(&self) -> Vec<String> {
        self.projects
            .iter()
            .filter(|e| e.value().state.lock().unforwarded_batches > 0)
            .map(|e| e.key().clone())
            .collect()
    }

    pub async fn wait_for_project_data(&self, project_id: &str) {
        if let Ok(handle) = self.get_or_create_project(project_id) {
            let notified = handle.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if handle.state.lock().unforwarded_batches > 0 {
                return;
            }
            notified.await;
        }
    }

    fn apply_ack_locked(
        &self,
        st: &mut ProjectLogState,
        batch_offset: u64,
        now_ms: i64,
    ) -> Result<(), StorageError> {
        if st.acked_offset.is_some_and(|a| batch_offset <= a) {
            return Ok(());
        }
        write_cursor(&st.proj_dir, Some(batch_offset), now_ms)
            .map_err(|e| self.record_io_error(&e))?;
        st.acked_offset = Some(batch_offset);

        let mut removed_batches = 0usize;
        let mut removed_events = 0usize;
        let mut freed_bytes = 0usize;

        while let Some(seg) = st.segments.front_mut() {
            while let Some(fr) = seg.frames.front() {
                if fr.offset <= batch_offset {
                    removed_batches += 1;
                    removed_events += fr.event_count;
                    seg.frames.pop_front();
                } else {
                    break;
                }
            }
            if seg.frames.is_empty() && seg.last_offset <= batch_offset {
                let seg_path = seg.path.clone();
                let seg_bytes = seg.file_bytes;
                if st
                    .open_write_file
                    .as_ref()
                    .is_some_and(|(open_p, _)| *open_p == seg_path)
                {
                    st.open_write_file = None;
                    self.open_handles.fetch_sub(1, Ordering::AcqRel);
                }
                let _ = fs::remove_file(&seg_path);
                freed_bytes += seg_bytes;
                st.segments.pop_front();
            } else {
                break;
            }
        }

        st.disk_bytes = st.disk_bytes.saturating_sub(freed_bytes);
        st.unforwarded_batches = st.unforwarded_batches.saturating_sub(removed_batches);
        st.unforwarded_events = st.unforwarded_events.saturating_sub(removed_events);
        st.oldest_unforwarded_ts_ms = st
            .segments
            .front()
            .and_then(|s| s.frames.front())
            .map(|f| f.enqueued_at_ms);

        self.node_disk_bytes
            .fetch_sub(freed_bytes, Ordering::AcqRel);
        self.node_unforwarded_batches
            .fetch_sub(removed_batches, Ordering::AcqRel);
        self.node_unforwarded_events
            .fetch_sub(removed_events, Ordering::AcqRel);
        Ok(())
    }

    fn expire_head_frames_locked(
        &self,
        project_id: &str,
        st: &mut ProjectLogState,
        now_ms: i64,
    ) -> Result<usize, StorageError> {
        let max_age_ms = self.max_age_ms.load(Ordering::Relaxed);
        let mut dropped_batches = 0usize;
        while let Some(front) = st.segments.front().and_then(|s| s.frames.front()).cloned() {
            let age_ms = now_ms.saturating_sub(front.enqueued_at_ms);
            if age_ms < max_age_ms {
                break;
            }
            tracing::error!(
                project_id,
                writer_id = %self.writer_id,
                batch_offset = front.offset,
                age_ms,
                max_age_ms,
                event_count = front.event_count,
                "dropping expired writer_log batch exceeding max age (< storage offset T_expire)"
            );
            self.apply_ack_locked(st, front.offset, now_ms)?;
            self.expired_batches_dropped_total
                .fetch_add(1, Ordering::Relaxed);
            self.expired_events_dropped_total
                .fetch_add(front.event_count as u64, Ordering::Relaxed);
            dropped_batches += 1;
        }
        Ok(dropped_batches)
    }

    /// Reads the next unforwarded batch for `project_id`, dropping expired batches (`>= max_age_ms`)
    /// first. Uses a short-lived read handle so no idle file descriptors stay open.
    pub fn peek_next_batch(
        &self,
        project_id: &str,
        now_ms: i64,
    ) -> Result<Option<WriterLogBatch>, StorageError> {
        let Some(handle) = self.projects.get(project_id).map(|e| e.clone()) else {
            return Ok(None);
        };
        let (seg_path, frame) = {
            let mut st = handle.state.lock();
            self.expire_head_frames_locked(project_id, &mut st, now_ms)?;
            let (seg_path, fr) = {
                let Some(seg) = st.segments.front() else {
                    return Ok(None);
                };
                let Some(fr) = seg.frames.front().cloned() else {
                    return Ok(None);
                };
                (seg.path.clone(), fr)
            };
            (seg_path, fr)
        };

        let mut f =
            File::open(&seg_path).map_err(|e| self.record_io_error(&StorageError::Io(e)))?;
        f.seek(SeekFrom::Start(frame.file_offset))
            .map_err(|e| self.record_io_error(&StorageError::Io(e)))?;
        let mut hdr = [0u8; WRITER_LOG_HEADER_LEN];
        f.read_exact(&mut hdr)
            .map_err(|e| self.record_io_error(&StorageError::Io(e)))?;
        if hdr[0..4] != WRITER_LOG_MAGIC || hdr[4] != WRITER_LOG_VERSION {
            return Err(StorageError::Corrupted(
                "invalid writer_log frame header".into(),
            ));
        }
        let expected_crc = u32::from_le_bytes(hdr[25..29].try_into().unwrap());
        let mut payload = vec![0u8; frame.payload_len as usize];
        f.read_exact(&mut payload)
            .map_err(|e| self.record_io_error(&StorageError::Io(e)))?;
        let actual_crc = compute_frame_crc(&hdr[4..25], &payload);
        if actual_crc != expected_crc {
            return Err(StorageError::Corrupted(format!(
                "writer_log crc mismatch: expected {expected_crc:#x}, got {actual_crc:#x}"
            )));
        }

        Ok(Some(WriterLogBatch {
            batch_offset: frame.offset,
            enqueued_at_ms: frame.enqueued_at_ms,
            event_count: frame.event_count,
            plsb_bytes: payload,
        }))
    }

    pub fn ack_batch(&self, project_id: &str, batch_offset: u64) -> Result<(), StorageError> {
        let Some(handle) = self.projects.get(project_id).map(|e| e.clone()) else {
            return Ok(());
        };
        let mut st = handle.state.lock();
        self.apply_ack_locked(&mut st, batch_offset, Utc::now().timestamp_millis())
    }

    pub fn close_idle_handles(&self) -> usize {
        let idle_dur = self.idle_close_duration();
        let mut closed = 0usize;
        for entry in self.projects.iter() {
            let mut st = entry.value().state.lock();
            if st.open_write_file.is_some() && st.last_io_at.elapsed() >= idle_dur {
                st.open_write_file = None;
                self.open_handles.fetch_sub(1, Ordering::AcqRel);
                closed += 1;
            }
        }
        closed
    }

    pub fn sweep_expired_batches(&self, now_ms: i64) -> usize {
        let mut dropped = 0usize;
        let pids: Vec<String> = self.projects.iter().map(|e| e.key().clone()).collect();
        for pid in pids {
            if let Some(handle) = self.projects.get(&pid).map(|e| e.clone()) {
                let mut st = handle.state.lock();
                if let Ok(n) = self.expire_head_frames_locked(&pid, &mut st, now_ms) {
                    dropped += n;
                }
            }
        }
        dropped
    }

    pub fn project_disk_bytes(&self, project_id: &str) -> usize {
        self.projects
            .get(project_id)
            .map(|h| h.state.lock().disk_bytes)
            .unwrap_or(0)
    }

    pub fn project_acked_offset(&self, project_id: &str) -> Option<u64> {
        self.projects
            .get(project_id)
            .and_then(|h| h.state.lock().acked_offset)
    }

    pub fn stats(&self) -> WriterLogStats {
        let now_ms = Utc::now().timestamp_millis();
        let mut oldest_ms: Option<i64> = None;
        for entry in self.projects.iter() {
            if let Some(ts) = entry.value().state.lock().oldest_unforwarded_ts_ms {
                oldest_ms = Some(oldest_ms.map_or(ts, |m| m.min(ts)));
            }
        }
        let lag_secs = oldest_ms
            .map(|ts| (now_ms.saturating_sub(ts).max(0) as f64) / 1_000.0)
            .unwrap_or(0.0);
        WriterLogStats {
            bytes: self.node_disk_bytes.load(Ordering::Relaxed),
            capacity_bytes: self.node_max_bytes.load(Ordering::Relaxed),
            unforwarded_batches: self.node_unforwarded_batches.load(Ordering::Relaxed),
            unforwarded_events: self.node_unforwarded_events.load(Ordering::Relaxed),
            forwarding_lag_seconds: lag_secs,
            open_handles: self.open_handles.load(Ordering::Relaxed),
            expired_batches_dropped_total: self
                .expired_batches_dropped_total
                .load(Ordering::Relaxed),
            expired_events_dropped_total: self.expired_events_dropped_total.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulse_wire::WireEvent;

    fn make_ev(id: u64) -> WireEvent {
        WireEvent {
            event_uuid: WireEvent::deterministic_uuid("u1", "s1", "Ev", 1_790_000_000_000, id),
            routing_seed: "u1".into(),
            distinct_id: "u1".into(),
            session_id: "s1".into(),
            event_name: "Ev".into(),
            client_event_ts_ms: 1_790_000_000_000,
            client_sent_ts_ms: 0,
            props: vec![],
            replay_offset_kb: 0,
        }
    }

    #[test]
    fn writer_id_persists_across_reopen_and_wipe_resets_to_offset_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path();

        let wl1 = WriterLogManager::open(data_dir).unwrap();
        let id1 = wl1.writer_id().to_string();
        assert_eq!(
            wl1.append_batch("proj_a", &WireBatch::new(vec![make_ev(1)]), 1_000)
                .unwrap(),
            0
        );
        assert_eq!(
            wl1.append_batch("proj_a", &WireBatch::new(vec![make_ev(2)]), 1_001)
                .unwrap(),
            1
        );
        drop(wl1);

        let wl2 = WriterLogManager::open(data_dir).unwrap();
        assert_eq!(wl2.writer_id(), id1);
        assert_eq!(
            wl2.append_batch("proj_a", &WireBatch::new(vec![make_ev(3)]), 1_002)
                .unwrap(),
            2
        );
        drop(wl2);

        // Wiping writer_log generates a fresh writer_id starting at offset 0.
        std::fs::remove_dir_all(data_dir.join("writer_log")).unwrap();
        let wl3 = WriterLogManager::open(data_dir).unwrap();
        assert_ne!(wl3.writer_id(), id1);
        assert_eq!(
            wl3.append_batch("proj_a", &WireBatch::new(vec![make_ev(4)]), 2_000)
                .unwrap(),
            0
        );
    }

    #[test]
    fn crash_truncates_incomplete_trailing_frame_and_preserves_valid_frames() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path();
        let wl = WriterLogManager::open(data_dir).unwrap();
        let now = Utc::now().timestamp_millis();
        wl.append_batch("proj_c", &WireBatch::new(vec![make_ev(1)]), now)
            .unwrap();
        wl.append_batch("proj_c", &WireBatch::new(vec![make_ev(2)]), now)
            .unwrap();
        drop(wl);

        // Append truncated partial frame bytes at EOF.
        let seg_path = data_dir
            .join("writer_log")
            .join("proj_c")
            .join("seg_0000000000000000.log");
        let valid_len = std::fs::metadata(&seg_path).unwrap().len();
        {
            let mut f = OpenOptions::new().append(true).open(&seg_path).unwrap();
            f.write_all(b"PWLF\x01partial_garbage").unwrap();
        }

        let wl2 = WriterLogManager::open(data_dir).unwrap();
        assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), valid_len);
        let b0 = wl2.peek_next_batch("proj_c", now).unwrap().unwrap();
        assert_eq!(b0.batch_offset, 0);
        wl2.ack_batch("proj_c", 0).unwrap();
        let b1 = wl2.peek_next_batch("proj_c", now).unwrap().unwrap();
        assert_eq!(b1.batch_offset, 1);
    }

    #[test]
    fn idle_handle_close_and_expiry_and_per_project_disk_caps() {
        let tmp = tempfile::tempdir().unwrap();
        let wl = WriterLogManager::open(tmp.path()).unwrap();
        wl.set_idle_close_for_test(Duration::from_millis(20));
        wl.set_max_batch_age_for_test(Duration::from_millis(100));
        wl.set_disk_limits_for_test(512, 2048);

        let now = Utc::now().timestamp_millis();
        wl.append_batch("proj_a", &WireBatch::new(vec![make_ev(1)]), now)
            .unwrap();
        assert_eq!(wl.stats().open_handles, 1);

        std::thread::sleep(Duration::from_millis(35));
        assert_eq!(wl.close_idle_handles(), 1);
        assert_eq!(wl.stats().open_handles, 0);
        assert!(tmp.path().join("writer_log/proj_a").exists());

        // Per-project disk cap rejects proj_a while proj_b succeeds (zero cross-project HOL blocking).
        let err = wl
            .check_admission("proj_a", 0, 0, 512)
            .expect_err("proj_a exceeds cap");
        assert!(matches!(
            err,
            StorageError::Backpressure {
                what: "writer_log",
                ..
            }
        ));
        assert!(wl.check_admission("proj_b", 0, 0, 128).is_ok());

        // Advance past max_batch_age -> expired batch dropped & counted, cursor advanced.
        let future_ms = now + 200;
        assert_eq!(wl.sweep_expired_batches(future_ms), 1);
        assert_eq!(wl.stats().expired_batches_dropped_total, 1);
        assert_eq!(wl.stats().expired_events_dropped_total, 1);
        assert_eq!(wl.project_acked_offset("proj_a"), Some(0));
        assert!(wl.peek_next_batch("proj_a", future_ms).unwrap().is_none());
    }

    #[test]
    fn idle_handle_closes_even_while_peek_next_batch_runs_repeatedly() {
        let tmp = tempfile::tempdir().unwrap();
        let wl = WriterLogManager::open(tmp.path()).unwrap();
        wl.set_idle_close_for_test(Duration::from_millis(30));

        let now = Utc::now().timestamp_millis();
        wl.append_batch("proj_peek", &WireBatch::new(vec![make_ev(1)]), now)
            .unwrap();
        assert_eq!(wl.stats().open_handles, 1);

        // Simulate WriterForwarder polling peek_next_batch repeatedly during a storage outage.
        for _ in 0..5 {
            std::thread::sleep(Duration::from_millis(12));
            let batch = wl.peek_next_batch("proj_peek", now).unwrap().unwrap();
            assert_eq!(batch.batch_offset, 0);
        }

        assert_eq!(wl.close_idle_handles(), 1);
        assert_eq!(wl.stats().open_handles, 0);
        assert!(wl.peek_next_batch("proj_peek", now).unwrap().is_some());
    }

    #[cfg(unix)]
    #[test]
    fn get_or_create_project_io_error_trips_sticky_health_check() {
        use std::os::unix::fs::PermissionsExt;

        struct RestorePerms(PathBuf, std::fs::Permissions);
        impl Drop for RestorePerms {
            fn drop(&mut self) {
                let _ = std::fs::set_permissions(&self.0, self.1.clone());
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let wl = WriterLogManager::open(tmp.path()).unwrap();
        assert!(wl.health_check().is_ok());

        let orig_perms = std::fs::metadata(wl.root_dir()).unwrap().permissions();
        let _restore = RestorePerms(wl.root_dir().to_path_buf(), orig_perms);
        std::fs::set_permissions(wl.root_dir(), std::fs::Permissions::from_mode(0o555)).unwrap();

        let now = Utc::now().timestamp_millis();
        let err = wl
            .append_batch("proj_unwritable", &WireBatch::new(vec![make_ev(1)]), now)
            .expect_err("unwritable writer_log dir must fail append_batch");
        assert!(matches!(err, StorageError::Io(_)));
        assert!(
            wl.health_check().is_err(),
            "health_check() must trip sticky_io_error so /readyz returns 503"
        );
        assert!(matches!(
            wl.check_admission("proj_unwritable", 0, 0, 64),
            Err(StorageError::Io(_))
        ));
    }
}
