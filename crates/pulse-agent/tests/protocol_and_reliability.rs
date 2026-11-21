use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::NaiveDate;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use parking_lot::Mutex;
use pulse_agent::evaluators::{
    parse_llm_judge_response, run_deterministic_evaluator, run_deterministic_evaluator_for_target,
    run_deterministic_evaluator_on_session, run_deterministic_evaluator_on_span, EvalContext,
    EvalTarget, SessionEvalContext, SpanEvalContext,
};
use pulse_agent::model::{
    AgentErasureTombstone, AgentEvalRollup, AgentPrivacyPolicy, AgentSpan, AgentTrace,
    AnalyticsFilter, CompareFilter, ErasureLevel, EvalTargetType, EvaluatorConfig, EvaluatorType,
    FailureSource, SessionListFilter, SpanEvent, SpanPhase, SpanType, TraceIngestBatch,
    TraceListFilter, TraceStatus,
};
use pulse_agent::otlp::{normalize_otlp_traces, OtlpExportTraceRequest};
use pulse_agent::privacy::sanitize_batch;
use pulse_agent::protocol::{
    agent_zstd_dict, decode_plst_batch, encode_plst_batch, encode_plst_batch_uncompressed,
    ProtocolError, AGENT_DICT_ID, FLAG_DICT_ZSTD, FLAG_RAW_ZSTD, MAX_PLST_UNCOMP_BYTES, PLST_MAGIC,
    PLST_VERSION,
};
use pulse_agent::stats::{compare_continuous_metric, detect_execution_loop};
use pulse_agent::store::manifest::{ManifestStore, ReaderPinTracker};
use pulse_agent::store::segment::{
    PlssFooter, SectionDirEntry, SectionTag, SegmentWriter, PLSS_DIR_ENTRY_LEN, SEC_FLAG_CRDT_RANKS,
};
use pulse_agent::store::wal::{decode_ptwl_frames_tolerant, encode_ptwl_frame, wal_object_path};
use pulse_agent::store::{AgentTraceStore, AgentTraceStoreConfig, StoreError};
use pulse_agent::{
    MAX_PAYLOAD_BYTES, MAX_SPANS_PER_TRACE, MAX_SPAN_DEPTH, MAX_TAGS_PER_TRACE,
    MIN_SIGNIFICANCE_SAMPLES,
};
use pulse_storage::EncryptedObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{json, Value};
use tempfile::TempDir;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Test Helpers
// ---------------------------------------------------------------------------

fn make_span(trace_id: &str, span_id: &str, parent: Option<&str>, start_ts_ms: i64) -> AgentSpan {
    AgentSpan {
        span_id: span_id.to_string(),
        trace_id: trace_id.to_string(),
        project_id: String::new(),
        parent_span_id: parent.map(str::to_string),
        linked_span_ids: Vec::new(),
        span_phase: SpanPhase::Final,
        depth: 0,
        name: format!("span_{span_id}"),
        span_type: SpanType::Llm,
        status: TraceStatus::Ok,
        start_ts_ms,
        end_ts_ms: Some(start_ts_ms + 120),
        duration_ms: 120,
        provider: Some("openai".to_string()),
        model: Some("gpt-4o".to_string()),
        model_id: Some("gpt-4o".to_string()),
        model_version: None,
        prompt_template_id: None,
        prompt_template_version: None,
        input_tokens: 150,
        output_tokens: 60,
        cached_tokens: 20,
        total_tokens: 210,
        cost_nanos: 1_250_000,
        cost_usd: 0.00125,
        temperature: Some(0.2),
        finish_reason: Some("stop".to_string()),
        model_params: json!({"temperature": 0.2}),
        tool_name: None,
        tool_version: None,
        tool_retry_count: 0,
        input: json!({"messages": [{"role": "user", "content": "hello"}]}),
        output: json!({"choices": [{"message": {"content": "world"}}]}),
        attributes: json!({"gen_ai.system": "openai"}),
        events: vec![SpanEvent {
            name: "token.first".to_string(),
            timestamp_ms: start_ts_ms + 25,
            attributes: json!({"ttft_ms": 25}),
        }],
        metadata: Value::Null,
        error_type: None,
        error_message: None,
        error_stack: None,
    }
}

fn make_trace(
    trace_id: &str,
    agent_name: &str,
    agent_version: &str,
    session_id: Option<&str>,
    user_id: Option<&str>,
    start_ts_ms: i64,
    status: TraceStatus,
) -> AgentTrace {
    AgentTrace {
        trace_id: trace_id.to_string(),
        project_id: String::new(),
        agent_name: agent_name.to_string(),
        agent_version: agent_version.to_string(),
        prompt_version: Some("p1".to_string()),
        model_config_version: None,
        tool_config_version: None,
        app_release: None,
        environment: "production".to_string(),
        session_id: session_id.map(str::to_string),
        user_id: user_id.map(str::to_string),
        workflow_id: None,
        parent_trace_id: None,
        status,
        start_ts_ms,
        end_ts_ms: Some(start_ts_ms + 200),
        duration_ms: 200,
        input: json!({"query": "status?"}),
        output: json!({"answer": "ok"}),
        error_type: (status == TraceStatus::Error).then(|| "ToolFailure".to_string()),
        error_message: (status == TraceStatus::Error).then(|| "failed".to_string()),
        failure_source: None,
        metadata: json!({"env": "prod"}),
        tags: vec!["ci".to_string()],
        span_count: 1,
        llm_call_count: 1,
        tool_call_count: 0,
        error_span_count: i32::from(status == TraceStatus::Error),
        input_tokens: 150,
        output_tokens: 60,
        cached_tokens: 20,
        total_tokens: 210,
        total_cost_nanos: 1_250_000,
        total_cost_usd: 0.00125,
        primary_model: Some("gpt-4o".to_string()),
        primary_provider: Some("openai".to_string()),
        avg_eval_score: None,
        eval_pass_rate: None,
        has_loop_warning: false,
    }
}

fn make_batch(
    trace_id: &str,
    session_id: Option<&str>,
    user_id: Option<&str>,
    ts: i64,
) -> TraceIngestBatch {
    let trace = make_trace(
        trace_id,
        "planner",
        "1.0.0",
        session_id,
        user_id,
        ts,
        TraceStatus::Ok,
    );
    let span = make_span(trace_id, &format!("{trace_id}_sp1"), None, ts);
    TraceIngestBatch {
        traces: vec![trace],
        spans: vec![span],
    }
}

fn test_store_config(read_only: bool, gc_grace_secs: i64) -> AgentTraceStoreConfig {
    AgentTraceStoreConfig {
        cache_dir: None,
        writer_id: Uuid::now_v7(),
        wal_group_commit_ms: 5,
        l0_flush_age_secs: 4,
        l0_shards: 4,
        max_segment_bytes: 16 * 1024 * 1024,
        gc_grace_secs,
        read_only,
        cache_memory_bytes: 8 * 1024 * 1024,
        cache_disk_bytes: 0,
        l0_flush_max_traces: 4096,
        l0_flush_max_bytes: 32 * 1024 * 1024,
        wal_max_group_items: 64,
        wal_max_group_bytes: 8 * 1024 * 1024,
    }
}

#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    get_range_calls: AtomicUsize,
    ranges: Mutex<Vec<(String, Range<u64>)>>,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            get_range_calls: AtomicUsize::new(0),
            ranges: Mutex::new(Vec::new()),
        })
    }

    fn reset(&self) {
        self.get_range_calls.store(0, AtomicOrdering::SeqCst);
        self.ranges.lock().clear();
    }

    fn recorded_ranges(&self) -> Vec<(String, Range<u64>)> {
        self.ranges.lock().clone()
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &ObjectPath, range: Range<u64>) -> OsResult<Bytes> {
        self.get_range_calls.fetch_add(1, AtomicOrdering::SeqCst);
        self.ranges
            .lock()
            .push((location.as_ref().to_string(), range.clone()));
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &ObjectPath) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &ObjectPath) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

// ---------------------------------------------------------------------------
// 1. Hostile Input Fuzzing (PLST, PTWL, PLSS + Tag 0x04 bounds check)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hostile_fuzz_plst_ptwl_plss_inputs() {
    let base_batch = make_batch(
        "tr_fuzz_1",
        Some("sess_fuzz"),
        Some("user_fuzz"),
        1_700_000_000_000,
    );
    let valid_dict = encode_plst_batch(&base_batch, true).unwrap();
    let valid_raw = encode_plst_batch(&base_batch, false).unwrap();
    let valid_uncomp = encode_plst_batch_uncompressed(&base_batch).unwrap();

    // Bad magic
    let mut f = valid_dict.clone();
    f[0..4].copy_from_slice(b"NOPE");
    assert!(matches!(
        decode_plst_batch(&f),
        Err(ProtocolError::InvalidMagic { .. })
    ));

    // Unsupported version
    let mut f = valid_dict.clone();
    f[4] = PLST_VERSION + 7;
    assert!(matches!(
        decode_plst_batch(&f),
        Err(ProtocolError::UnsupportedVersion(_))
    ));

    // Unknown flags + mutually exclusive compression flags
    let mut f = valid_dict.clone();
    f[5] = 0x80;
    assert!(matches!(
        decode_plst_batch(&f),
        Err(ProtocolError::UnsupportedFlags(0x80))
    ));
    let mut f = valid_dict.clone();
    f[5] = FLAG_RAW_ZSTD | FLAG_DICT_ZSTD;
    assert!(matches!(
        decode_plst_batch(&f),
        Err(ProtocolError::UnsupportedFlags(_))
    ));

    // Wrong DictID
    let mut f = valid_dict.clone();
    f[6..8].copy_from_slice(&999u16.to_le_bytes());
    assert!(matches!(
        decode_plst_batch(&f),
        Err(ProtocolError::UnknownDictionary(999))
    ));
    let mut f = valid_raw.clone();
    f[6..8].copy_from_slice(&AGENT_DICT_ID.to_le_bytes());
    assert!(matches!(
        decode_plst_batch(&f),
        Err(ProtocolError::InvalidHeader(_))
    ));

    // UncompLen > 64 MiB
    let mut f = valid_dict.clone();
    let huge = (MAX_PLST_UNCOMP_BYTES as u32) + 1;
    f[8..12].copy_from_slice(&huge.to_le_bytes());
    assert!(matches!(
        decode_plst_batch(&f),
        Err(ProtocolError::ResourceLimitExceeded(_))
    ));

    // Zstd bomb: compressed payload expands beyond 64 MiB while header claims small uncomp_len
    let bomb_plain = vec![0u8; MAX_PLST_UNCOMP_BYTES + 4096];
    let bomb_comp = zstd::bulk::compress(&bomb_plain, 1).unwrap();
    let bomb_crc = crc32fast::hash(&bomb_comp);
    let mut bomb_frame = Vec::with_capacity(20 + bomb_comp.len());
    bomb_frame.extend_from_slice(&PLST_MAGIC);
    bomb_frame.push(PLST_VERSION);
    bomb_frame.push(FLAG_RAW_ZSTD);
    bomb_frame.extend_from_slice(&0u16.to_le_bytes());
    bomb_frame.extend_from_slice(&128u32.to_le_bytes());
    bomb_frame.extend_from_slice(&(bomb_comp.len() as u32).to_le_bytes());
    bomb_frame.extend_from_slice(&bomb_crc.to_le_bytes());
    bomb_frame.extend_from_slice(&bomb_comp);
    assert!(matches!(
        decode_plst_batch(&bomb_frame),
        Err(ProtocolError::ResourceLimitExceeded(_))
    ));

    // CRC32 mismatch + truncation + trailing bytes
    let mut f = valid_dict.clone();
    f[20] ^= 0xFF;
    assert!(matches!(
        decode_plst_batch(&f),
        Err(ProtocolError::CrcMismatch { .. })
    ));
    assert!(matches!(
        decode_plst_batch(&valid_dict[..19]),
        Err(ProtocolError::TruncatedFrame(_))
    ));
    let mut f = valid_dict.clone();
    f.push(0xAA);
    assert!(matches!(
        decode_plst_batch(&f),
        Err(ProtocolError::TrailingBytes(1))
    ));

    // Forged uncomp payload with out-of-bounds string index (re-compressed with valid CRC)
    let mut bad_body = valid_uncomp[20..].to_vec();
    if bad_body.len() > 24 {
        let mid = bad_body.len() / 2;
        bad_body[mid..mid + 4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        let crc = crc32fast::hash(&bad_body);
        let mut forged = Vec::with_capacity(20 + bad_body.len());
        forged.extend_from_slice(&PLST_MAGIC);
        forged.push(PLST_VERSION);
        forged.push(0x00);
        forged.extend_from_slice(&0u16.to_le_bytes());
        forged.extend_from_slice(&(bad_body.len() as u32).to_le_bytes());
        forged.extend_from_slice(&(bad_body.len() as u32).to_le_bytes());
        forged.extend_from_slice(&crc.to_le_bytes());
        forged.extend_from_slice(&bad_body);
        assert!(decode_plst_batch(&forged).is_err());
    }

    // 550 deterministic PRNG-mutated frames must never panic
    let mut rng = StdRng::seed_from_u64(0x5EED_CAFE);
    for _ in 0..550 {
        let mut mutated = valid_dict.clone();
        let ops = rng.gen_range(1..=6);
        for _ in 0..ops {
            if mutated.is_empty() {
                break;
            }
            let idx = rng.gen_range(0..mutated.len());
            mutated[idx] = rng.gen();
        }
        if rng.gen_bool(0.25) && mutated.len() > 4 {
            let cut = rng.gen_range(0..mutated.len());
            mutated.truncate(cut);
        }
        let _ = decode_plst_batch(&mutated);
    }

    // PTWL tolerant decoder fuzzing
    let ptwl_ok = encode_ptwl_frame(1, 1_700_000_000_000, &base_batch).unwrap();
    let mut ptwl_corrupt_tail = ptwl_ok.clone();
    ptwl_corrupt_tail.extend_from_slice(b"PTWL_TRUNC");
    let (frames, err_msg) = decode_ptwl_frames_tolerant(&ptwl_corrupt_tail);
    assert_eq!(frames.len(), 1);
    assert!(err_msg.is_some());

    let mut ptwl_bad_crc = ptwl_ok.clone();
    let last = ptwl_bad_crc.len() - 1;
    ptwl_bad_crc[last] ^= 0x55;
    let (frames, err_msg) = decode_ptwl_frames_tolerant(&ptwl_bad_crc);
    assert!(frames.is_empty());
    assert!(err_msg.is_some());

    let mut ptwl_huge_len = ptwl_ok;
    ptwl_huge_len[24..28].copy_from_slice(&u32::MAX.to_le_bytes());
    let (frames, err_msg) = decode_ptwl_frames_tolerant(&ptwl_huge_len);
    assert!(frames.is_empty());
    assert!(err_msg.is_some());

    // PLSS corruption + Tag04Index::decode block_count = u32::MAX bounds check regression
    let mem = Arc::new(InMemory::new());
    let obj_store: Arc<dyn ObjectStore> = mem.clone();
    let store = AgentTraceStore::open(obj_store.clone(), test_store_config(false, 900))
        .await
        .unwrap();
    let trace_id = "tr_tag04_bounds";
    store
        .ingest_batch(
            "proj_bounds",
            make_batch(trace_id, Some("s1"), Some("u1"), 1_700_000_000_000),
        )
        .await
        .unwrap();
    store.flush_all_l0().await.unwrap();

    // Locate the written .plss segment
    let man = ManifestStore::new("proj_bounds", obj_store.clone(), 900)
        .unwrap()
        .load_latest()
        .await
        .unwrap();
    assert_eq!(man.segments.len(), 1);
    let seg_path = ObjectPath::from(man.segments[0].object_path.as_str());
    let orig_bytes = obj_store
        .get(&seg_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec();

    // Patch Tag 0x04 trace_to_block[0].block_count = u32::MAX and recompute section + directory CRCs
    let mut patched = orig_bytes.clone();
    let tail_start = patched.len() - 24;
    let footer = PlssFooter::decode(&patched[tail_start..]).unwrap();
    let dir_start = footer.dir_offset as usize;
    let dir_end = dir_start + footer.dir_len as usize;
    let mut entries = Vec::new();
    for chunk in patched[dir_start..dir_end].chunks_exact(20) {
        entries.push(SectionDirEntry::decode(chunk).unwrap());
    }
    let sec04_entry_idx = entries
        .iter()
        .position(|e| e.tag == SectionTag::SpanTree as u16)
        .unwrap();
    let sec04_off = entries[sec04_entry_idx].offset as usize;
    let sec04_len = entries[sec04_entry_idx].len as usize;
    // Tag 0x04 layout: block_count(4) | trace_count(4) | index_bytes(4) | trace_to_block[0](hash:8, block_start:4, block_count:4)
    let tc_pos = sec04_off + 12 + 8 + 4;
    patched[tc_pos..tc_pos + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    let new_sec04_crc = crc32fast::hash(&patched[sec04_off..sec04_off + sec04_len]);
    entries[sec04_entry_idx].crc32 = new_sec04_crc;

    let mut new_dir = Vec::new();
    for e in &entries {
        e.encode_into(&mut new_dir);
    }
    patched[dir_start..dir_end].copy_from_slice(&new_dir);
    let new_dir_crc = crc32fast::hash(&patched[dir_start..dir_end]);
    let mut new_footer = footer;
    new_footer.dir_crc32 = new_dir_crc;
    let mut footer_bytes = Vec::with_capacity(24);
    new_footer.encode_into(&mut footer_bytes);
    patched[tail_start..].copy_from_slice(&footer_bytes);

    obj_store
        .put(&seg_path, PutPayload::from_bytes(Bytes::from(patched)))
        .await
        .unwrap();

    // Use a fresh read_only store (empty cache) so it reads the patched Tag 0x04 from ObjectStore
    let reader_store = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 900))
        .await
        .unwrap();
    let err = reader_store
        .get_trace_detail("proj_bounds", trace_id, &[])
        .await
        .unwrap_err();
    assert!(
        matches!(err, StoreError::Corrupted(_)),
        "expected Corrupted from Tag04Index bounds check, got {err:?}"
    );

    // Corrupt footer magic
    let mut bad_footer = orig_bytes;
    let flen = bad_footer.len();
    bad_footer[flen - 4..flen].copy_from_slice(b"DEAD");
    obj_store
        .put(&seg_path, PutPayload::from_bytes(Bytes::from(bad_footer)))
        .await
        .unwrap();
    let reader_store2 = AgentTraceStore::open(obj_store, test_store_config(true, 900))
        .await
        .unwrap();
    assert!(reader_store2
        .list_traces(
            "proj_bounds",
            &TraceListFilter::default(),
            &HashMap::new(),
            &[]
        )
        .await
        .is_err());

    store.close().await.unwrap();
    reader_store.close().await.unwrap();
    reader_store2.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// 2. Payload Truncation, PII Redaction & Negative Token Clamping (T2-F02-04)
// ---------------------------------------------------------------------------

#[test]
fn truncation_redaction_and_negative_token_clamping() {
    let huge_mb = "🚀é中\"\\".repeat(25_000); // > 200 KiB multi-byte UTF-8 + escaped quotes
    let mut trace = make_trace(
        "tr_priv",
        "agent",
        "1.0.0",
        Some("sess_priv"),
        Some("usr_secret_123"),
        1_700_000_000_000,
        TraceStatus::Ok,
    );
    trace.input = json!({
        "prompt": huge_mb,
        "password": "hunter2",
        "contact": "alice@example.com",
        "api_key": "sk-test-secret-1234567890abcdef",
        "ssn": "123-45-6789"
    });
    trace.input_tokens = -42;
    trace.output_tokens = -17;
    trace.cached_tokens = -9;
    trace.total_tokens = -100;

    let mut span = make_span("tr_priv", "sp_priv", None, 1_700_000_000_000);
    span.input = json!({"secret": "top-secret", "text": "call pk_live_abcdef1234567890"});
    span.attributes = json!({"drop_me": "bye", "keep_me": "ok"});
    span.input_tokens = -50;
    span.output_tokens = -20;
    span.cached_tokens = -5;
    span.total_tokens = -75;

    let mut batch = TraceIngestBatch {
        traces: vec![trace],
        spans: vec![span],
    };
    let mut policy = AgentPrivacyPolicy::default_for_project("proj_test");
    policy.exclude_attributes = vec!["drop_me".to_string()];

    let stats = sanitize_batch(&mut batch, &policy);
    assert!(stats.truncated_payloads >= 1);
    assert!(stats.redacted_fields >= 2);

    let t = &batch.traces[0];
    assert!(
        t.user_id.is_none(),
        "capture_user_id = false must strip user_id"
    );
    assert_eq!(t.input["_truncated"], Value::Bool(true));
    assert!(serde_json::to_vec(&t.input).unwrap().len() <= MAX_PAYLOAD_BYTES);
    assert!(t.input["_preview"].as_str().is_some());
    assert_eq!(t.input_tokens, 0);
    assert_eq!(t.output_tokens, 0);
    assert_eq!(t.cached_tokens, 0);
    assert_eq!(t.total_tokens, 0);

    let s = &batch.spans[0];
    assert_eq!(s.input["secret"], "[REDACTED]");
    assert!(!s.input["text"].as_str().unwrap().contains("pk_live_"));
    assert!(s.attributes.get("drop_me").is_none());
    assert_eq!(s.attributes["keep_me"], "ok");
    assert_eq!(s.input_tokens, 0);
    assert_eq!(s.output_tokens, 0);
    assert_eq!(s.cached_tokens, 0);
    assert_eq!(s.total_tokens, 0);

    // OTLP negative token clamping (T2-F02-04)
    let otlp_json = json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [
                    { "key": "service.name", "value": { "stringValue": "otlp-bot" } }
                ]
            },
            "scopeSpans": [{
                "scope": { "name": "pulse", "version": "1.0.0" },
                "spans": [{
                    "traceId": "otlp_tr_neg",
                    "spanId": "otlp_sp_neg",
                    "name": "llm",
                    "startTimeUnixNano": "1700000000000000000",
                    "endTimeUnixNano": "1700000000500000000",
                    "attributes": [
                        { "key": "gen_ai.usage.input_tokens", "value": { "intValue": -50 } },
                        { "key": "gen_ai.usage.output_tokens", "value": { "intValue": -25 } },
                        { "key": "gen_ai.usage.cache_read_input_tokens", "value": { "intValue": -10 } },
                        { "key": "gen_ai.usage.total_tokens", "value": { "intValue": -75 } }
                    ],
                    "status": { "code": 1 }
                }]
            }]
        }]
    });
    let otlp_req: OtlpExportTraceRequest = serde_json::from_value(otlp_json).unwrap();
    let otlp_batch = normalize_otlp_traces(otlp_req);
    assert_eq!(otlp_batch.spans.len(), 1);
    assert_eq!(otlp_batch.spans[0].input_tokens, 0);
    assert_eq!(otlp_batch.spans[0].output_tokens, 0);
    assert_eq!(otlp_batch.spans[0].cached_tokens, 0);
    assert_eq!(otlp_batch.spans[0].total_tokens, 0);
    assert_eq!(otlp_batch.traces[0].input_tokens, 0);
    assert_eq!(otlp_batch.traces[0].output_tokens, 0);
    assert_eq!(otlp_batch.traces[0].cached_tokens, 0);
    assert_eq!(otlp_batch.traces[0].total_tokens, 0);
}

// ---------------------------------------------------------------------------
// 3. Out-of-Order SpanPhase CRDT Convergence Across L0, PLSS & Compaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn out_of_order_span_phase_crdt_convergence() {
    let ts = 1_700_000_000_000i64;
    let tid = "tr_ooo";

    let mut sp_start = make_span(tid, "sp_1", None, ts);
    sp_start.span_phase = SpanPhase::Start;
    sp_start.status = TraceStatus::Running;
    sp_start.end_ts_ms = None;
    sp_start.duration_ms = 0;
    sp_start.input = json!({"prompt": "initial prompt payload"});
    sp_start.output = Value::Null;
    sp_start.input_tokens = 100;
    sp_start.output_tokens = 0;
    sp_start.total_tokens = 100;
    sp_start.cost_nanos = 0;
    sp_start.cost_usd = 0.0;

    let mut sp_update = sp_start.clone();
    sp_update.span_phase = SpanPhase::Update;
    sp_update.output = json!({"partial": "streaming chunk"});
    sp_update.output_tokens = 25;
    sp_update.total_tokens = 125;

    let mut sp_final = sp_start.clone();
    sp_final.span_phase = SpanPhase::Final;
    sp_final.status = TraceStatus::Ok;
    sp_final.end_ts_ms = Some(ts + 350);
    sp_final.duration_ms = 350;
    sp_final.input = Value::Null; // Final arrives without input payload -> must backfill from Start!
    sp_final.output = json!({"answer": "complete"});
    sp_final.input_tokens = 100;
    sp_final.output_tokens = 80;
    sp_final.total_tokens = 180;
    sp_final.cost_nanos = 2_000_000;
    sp_final.cost_usd = 0.002;

    let mut tr_start = make_trace(
        tid,
        "agent",
        "1.0.0",
        Some("s_ooo"),
        Some("u1"),
        ts,
        TraceStatus::Running,
    );
    tr_start.end_ts_ms = None;
    tr_start.duration_ms = 0;
    tr_start.total_tokens = 0;
    tr_start.total_cost_nanos = 0;
    tr_start.total_cost_usd = 0.0;

    let mut tr_final = make_trace(
        tid,
        "agent",
        "1.0.0",
        Some("s_ooo"),
        Some("u1"),
        ts,
        TraceStatus::Ok,
    );
    tr_final.total_tokens = 0;
    tr_final.total_cost_nanos = 0;
    tr_final.total_cost_usd = 0.0;

    // Store A: Final -> flush -> Update -> flush -> Start (left in L0)
    let store_a = AgentTraceStore::open(Arc::new(InMemory::new()), test_store_config(false, 0))
        .await
        .unwrap();
    store_a
        .ingest_batch(
            "p1",
            TraceIngestBatch {
                traces: vec![tr_final.clone()],
                spans: vec![sp_final.clone()],
            },
        )
        .await
        .unwrap();
    store_a.flush_all_l0().await.unwrap();
    store_a
        .ingest_batch(
            "p1",
            TraceIngestBatch {
                traces: vec![],
                spans: vec![sp_update.clone()],
            },
        )
        .await
        .unwrap();
    store_a.flush_all_l0().await.unwrap();
    store_a
        .ingest_batch(
            "p1",
            TraceIngestBatch {
                traces: vec![tr_start.clone()],
                spans: vec![sp_start.clone()],
            },
        )
        .await
        .unwrap();

    // Store B: Start -> flush -> Update -> flush -> Final (flushed)
    let store_b = AgentTraceStore::open(Arc::new(InMemory::new()), test_store_config(false, 0))
        .await
        .unwrap();
    store_b
        .ingest_batch(
            "p1",
            TraceIngestBatch {
                traces: vec![tr_start],
                spans: vec![sp_start],
            },
        )
        .await
        .unwrap();
    store_b.flush_all_l0().await.unwrap();
    store_b
        .ingest_batch(
            "p1",
            TraceIngestBatch {
                traces: vec![],
                spans: vec![sp_update],
            },
        )
        .await
        .unwrap();
    store_b.flush_all_l0().await.unwrap();
    store_b
        .ingest_batch(
            "p1",
            TraceIngestBatch {
                traces: vec![tr_final],
                spans: vec![sp_final],
            },
        )
        .await
        .unwrap();
    store_b.flush_all_l0().await.unwrap();

    let det_a = store_a
        .get_trace_detail("p1", tid, &[])
        .await
        .unwrap()
        .unwrap();
    let det_b = store_b
        .get_trace_detail("p1", tid, &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(det_a.spans.len(), 1);
    assert_eq!(det_b.spans.len(), 1);
    assert_eq!(det_a.spans[0].span_phase, SpanPhase::Final);
    assert_eq!(det_b.spans[0].span_phase, SpanPhase::Final);
    assert_eq!(
        det_a.spans[0].input,
        json!({"prompt": "initial prompt payload"})
    );
    assert_eq!(
        det_b.spans[0].input,
        json!({"prompt": "initial prompt payload"})
    );
    assert_eq!(det_a.spans[0].output, json!({"answer": "complete"}));
    assert_eq!(det_b.spans[0].output, json!({"answer": "complete"}));
    assert_eq!(det_a.trace.status, TraceStatus::Ok);
    assert_eq!(det_b.trace.status, TraceStatus::Ok);
    assert_eq!(det_a.trace.total_tokens, 180);
    assert_eq!(det_b.trace.total_tokens, 180);
    assert_eq!(det_a.trace.total_cost_nanos, 2_000_000);
    assert_eq!(det_b.trace.total_cost_nanos, 2_000_000);

    // Compact Store B and verify identical state
    store_b.compact_and_gc(Some("p1"), &[]).await.unwrap();
    let det_b_comp = store_b
        .get_trace_detail("p1", tid, &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(det_b_comp.trace.total_tokens, 180);
    assert_eq!(det_b_comp.trace.total_cost_nanos, 2_000_000);
    assert_eq!(
        det_b_comp.spans[0].input,
        json!({"prompt": "initial prompt payload"})
    );

    let list_a = store_a
        .list_traces("p1", &TraceListFilter::default(), &HashMap::new(), &[])
        .await
        .unwrap();
    let list_b = store_b
        .list_traces("p1", &TraceListFilter::default(), &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(list_a.traces.len(), 1);
    assert_eq!(list_b.traces.len(), 1);
    assert_eq!(list_a.traces[0].total_tokens, list_b.traces[0].total_tokens);
    assert_eq!(list_a.traces[0].status, list_b.traces[0].status);

    store_a.close().await.unwrap();
    store_b.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// 4. Loop Detection, Cyclic & Deep Span Trees
// ---------------------------------------------------------------------------

#[test]
fn loop_cycle_and_depth_limits() {
    // > 512 spans dropped, depth > 32 clamped, indirect cycle C -> A <-> B broken cleanly
    let ts = 1_700_000_000_000i64;
    let tid = "tr_graph";
    let trace = make_trace(tid, "bot", "1.0.0", None, None, ts, TraceStatus::Ok);

    // Build C -> A <-> B plus chain of 45 nodes + overflow > 512
    let mut spans = Vec::new();
    let mut sp_c = make_span(tid, "sp_c", None, ts);
    sp_c.linked_span_ids = (0..80).map(|i| format!("link_{i}")).collect();
    spans.push(sp_c);
    spans.push(make_span(tid, "sp_a", Some("sp_b"), ts + 1));
    spans.push(make_span(tid, "sp_b", Some("sp_a"), ts + 2));

    let mut prev = "sp_c".to_string();
    for i in 0..45 {
        let sid = format!("chain_{i}");
        spans.push(make_span(tid, &sid, Some(&prev), ts + 10 + i));
        prev = sid;
    }
    for i in 0..500 {
        spans.push(make_span(
            tid,
            &format!("extra_{i}"),
            Some("sp_c"),
            ts + 100 + i,
        ));
    }

    let mut batch = TraceIngestBatch {
        traces: vec![trace],
        spans,
    };
    let stats = sanitize_batch(
        &mut batch,
        &AgentPrivacyPolicy::default_for_project("proj_test"),
    );
    assert!(stats.dropped_spans > 0);
    assert_eq!(batch.spans.len(), MAX_SPANS_PER_TRACE);
    assert!(batch.spans.iter().all(|s| s.depth <= MAX_SPAN_DEPTH as i32));
    assert_eq!(batch.spans[0].linked_span_ids.len(), 64);

    // Verify chain_40 clamped to MAX_SPAN_DEPTH (32) and C->A<->B did not crash/infinite-loop
    let max_depth_seen = batch.spans.iter().map(|s| s.depth).max().unwrap();
    assert_eq!(max_depth_seen, MAX_SPAN_DEPTH as i32);

    // Loop detection: 10 LLM spans emitted across 3 SpanPhases each = 30 span rows -> NO loop warning!
    let mut non_loop_spans = Vec::new();
    for i in 0..10 {
        for phase in [SpanPhase::Start, SpanPhase::Update, SpanPhase::Final] {
            let mut s = make_span("tr_noloop", &format!("llm_{i}"), None, ts + i * 10);
            s.span_phase = phase;
            non_loop_spans.push(s);
        }
    }
    assert!(detect_execution_loop(&non_loop_spans).is_none());

    // 4 consecutive same tool calls -> triggers loop warning
    let mut tool_loop = Vec::new();
    for i in 0..4 {
        let mut s = make_span("tr_tloop", &format!("t_{i}"), None, ts + i * 10);
        s.span_type = SpanType::Tool;
        s.tool_name = Some("search_docs".to_string());
        tool_loop.push(s);
    }
    assert!(detect_execution_loop(&tool_loop).is_some());

    // 12 unique LLM spans -> triggers loop warning
    let llm_loop: Vec<AgentSpan> = (0..12)
        .map(|i| make_span("tr_lloop", &format!("llm_{i}"), None, ts + i * 10))
        .collect();
    assert!(detect_execution_loop(&llm_loop).is_some());
}

// ---------------------------------------------------------------------------
// 5. Multi-Level Deterministic Evaluators & Error Isolation
// ---------------------------------------------------------------------------

#[test]
fn deterministic_evaluators_multi_level_and_failure_isolation() {
    let ts = 1_700_000_000_000i64;
    let trace = make_trace(
        "tr_ev",
        "bot",
        "1.0.0",
        Some("sess_ev"),
        None,
        ts,
        TraceStatus::Ok,
    );
    let mut tool_sp = make_span("tr_ev", "sp_tool", None, ts);
    tool_sp.span_type = SpanType::Tool;
    tool_sp.tool_name = Some("lookup".to_string());
    let spans = vec![tool_sp.clone()];

    let mk_cfg = |kind: &str, cfg: Value| EvaluatorConfig {
        id: format!("ev_{kind}"),
        project_id: "proj_test".to_string(),
        name: kind.to_string(),
        description: String::new(),
        target_type: EvalTargetType::Trace,
        evaluator_type: EvaluatorType::Deterministic,
        kind: kind.to_string(),
        version: "1.0.0".to_string(),
        enabled: true,
        agent_name: None,
        environment: None,
        sample_rate: 1.0,
        config: cfg,
        created_at_ms: ts,
        updated_at_ms: ts,
    };

    // Trace-level deterministic checks
    let ok = run_deterministic_evaluator(
        &mk_cfg("tool_sequence", json!({"required_tools": ["lookup"]})),
        &EvalContext {
            trace: &trace,
            spans: &spans,
            expected_output: None,
            expected_behavior: None,
        },
    )
    .unwrap();
    assert_eq!(ok.passed, Some(true));

    // Invalid regex returns graceful error outcome
    let bad_rx = run_deterministic_evaluator(
        &mk_cfg("regex_match", json!({"pattern": "[unclosed("})),
        &EvalContext {
            trace: &trace,
            spans: &spans,
            expected_output: None,
            expected_behavior: None,
        },
    )
    .unwrap();
    assert_eq!(bad_rx.passed, Some(false));
    assert!(bad_rx.error.is_some());

    // exact_match skipped when expected_output is None
    let skipped = run_deterministic_evaluator(
        &mk_cfg("exact_match", json!({})),
        &EvalContext {
            trace: &trace,
            spans: &spans,
            expected_output: None,
            expected_behavior: None,
        },
    )
    .unwrap();
    assert!(skipped.passed.is_none());
    assert_eq!(skipped.label.as_deref(), Some("skipped"));

    // Span-level evaluator
    let mut sp_cfg = mk_cfg("latency_budget", json!({"max_ms": 50}));
    sp_cfg.target_type = EvalTargetType::Span;
    let sp_out = run_deterministic_evaluator_on_span(
        &sp_cfg,
        &SpanEvalContext {
            span: &tool_sp,
            trace: Some(&trace),
            expected_output: None,
            expected_behavior: None,
        },
    )
    .unwrap();
    assert_eq!(sp_out.passed, Some(false));

    // Session-level no_loop across 15 traces with 1 LLM span each must NOT false positive
    let mut sess_traces = Vec::new();
    let mut sess_spans = Vec::new();
    for i in 0..15 {
        let tid = format!("tr_s_{i}");
        sess_traces.push(make_trace(
            &tid,
            "bot",
            "1.0.0",
            Some("sess_ev"),
            None,
            ts + i * 100,
            TraceStatus::Ok,
        ));
        sess_spans.push(make_span(&tid, &format!("sp_{i}"), None, ts + i * 100));
    }
    let mut sess_cfg = mk_cfg("no_loop", json!({}));
    sess_cfg.target_type = EvalTargetType::Session;
    let sess_out = run_deterministic_evaluator_on_session(
        &sess_cfg,
        &SessionEvalContext {
            project_id: "proj_test",
            session_id: "sess_ev",
            traces: &sess_traces,
            spans: &sess_spans,
            expected_output: None,
            expected_behavior: None,
        },
    )
    .unwrap();
    assert_eq!(sess_out.passed, Some(true));

    // Unknown kind returns error outcome; llm_judge returns None
    let unk = run_deterministic_evaluator(
        &mk_cfg("unknown_metric", json!({})),
        &EvalContext {
            trace: &trace,
            spans: &spans,
            expected_output: None,
            expected_behavior: None,
        },
    )
    .unwrap();
    assert!(unk.error.is_some());

    let mut llm_cfg = mk_cfg("llm_judge", json!({"rubric": "be accurate"}));
    llm_cfg.evaluator_type = EvaluatorType::LlmJudge;
    assert!(run_deterministic_evaluator_for_target(
        &llm_cfg,
        &EvalTarget::Trace(EvalContext {
            trace: &trace,
            spans: &spans,
            expected_output: None,
            expected_behavior: None,
        })
    )
    .is_none());

    // parse_llm_judge_response handles markdown fences and invalid JSON safely
    let parsed = parse_llm_judge_response(
        &llm_cfg,
        &trace,
        "```json\n{\"score\": 0.9, \"passed\": true, \"label\": \"good\", \"reasoning\": \"ok\"}\n```",
        15,
    );
    assert_eq!(parsed.passed, Some(true));
    assert!((parsed.score - 0.9).abs() < 1e-6);

    let bad_llm = parse_llm_judge_response(&llm_cfg, &trace, "not json at all", 15);
    assert_eq!(bad_llm.passed, Some(false));
    assert!(bad_llm.error.is_some());
}

// ---------------------------------------------------------------------------
// 6. Multi-Writer WAL Isolation & Corrupt Tail Quarantine vs Read-Only
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_writer_wal_group_commit_and_crash_recovery() {
    let mem = Arc::new(InMemory::new());
    let obj_store: Arc<dyn ObjectStore> = mem.clone();

    let mut cfg_a = test_store_config(false, 0);
    cfg_a.writer_id = Uuid::now_v7();
    let mut cfg_b = test_store_config(false, 0);
    cfg_b.writer_id = Uuid::now_v7();

    let writer_a = AgentTraceStore::open(obj_store.clone(), cfg_a.clone())
        .await
        .unwrap();
    let writer_b = AgentTraceStore::open(obj_store.clone(), cfg_b.clone())
        .await
        .unwrap();

    let ts = 1_700_000_000_000i64;
    let (r1, r2) = tokio::join!(
        writer_a.ingest_batch("proj_wal", make_batch("tr_wa_1", Some("s1"), None, ts)),
        writer_b.ingest_batch("proj_wal", make_batch("tr_wb_1", Some("s1"), None, ts + 10)),
    );
    r1.unwrap();
    r2.unwrap();

    // Both writer WAL files exist under distinct writer_ids
    let w_a_path = wal_object_path("proj_wal", &cfg_a.writer_id.to_string(), 1).unwrap();
    let w_b_path = wal_object_path("proj_wal", &cfg_b.writer_id.to_string(), 1).unwrap();
    assert!(obj_store.head(&w_a_path).await.is_ok());
    assert!(obj_store.head(&w_b_path).await.is_ok());

    // Inject a corrupt WAL file for an old writer_id
    let dead_wid = Uuid::now_v7().to_string();
    let bad_wal = wal_object_path("proj_wal", &dead_wid, 1).unwrap();
    obj_store
        .put(&bad_wal, PutPayload::from_static(b"PTWL_BAD_TAIL_BYTES"))
        .await
        .unwrap();

    // Read-only store must NOT quarantine/delete the corrupt WAL file
    let ro = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
        .await
        .unwrap();
    assert!(obj_store.head(&bad_wal).await.is_ok());
    let listed = ro
        .list_traces(
            "proj_wal",
            &TraceListFilter::default(),
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(listed.traces.len(), 2);

    // Writable store recovery MUST quarantine the corrupt WAL file
    let rw_recover = AgentTraceStore::open(obj_store.clone(), test_store_config(false, 0))
        .await
        .unwrap();
    assert!(obj_store.head(&bad_wal).await.is_err());

    writer_a.close().await.unwrap();
    writer_b.close().await.unwrap();
    ro.close().await.unwrap();
    rw_recover.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// 7. L0 Sharded Flush & --role query Real-Time Visibility
// ---------------------------------------------------------------------------

#[tokio::test]
async fn l0_sharded_flush_and_query_node_realtime_visibility() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = AgentTraceStore::open(obj_store.clone(), test_store_config(false, 0))
        .await
        .unwrap();
    let query = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
        .await
        .unwrap();

    let ts = 1_700_000_000_000i64;
    writer
        .ingest_batch(
            "proj_rt",
            make_batch("tr_rt_1", Some("sess_rt"), Some("usr_rt"), ts),
        )
        .await
        .unwrap();

    // Unflushed in writer L0, query node sees it immediately via refresh_from_wal
    let list_before = query
        .list_traces("proj_rt", &TraceListFilter::default(), &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(list_before.total, 1);
    assert!(query
        .get_trace_detail("proj_rt", "tr_rt_1", &[])
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        query
            .list_sessions("proj_rt", &SessionListFilter::default(), &[])
            .await
            .unwrap()
            .total,
        1
    );

    // Flush writer L0 -> PLSS, then query node reads from PLSS + evicts L0 without duplicates
    writer.flush_all_l0().await.unwrap();
    let list_after = query
        .list_traces("proj_rt", &TraceListFilter::default(), &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(list_after.total, 1);
    let analytics = query
        .query_analytics("proj_rt", &AnalyticsFilter::default(), &[])
        .await
        .unwrap();
    assert_eq!(analytics.total_executions, 1);

    writer.close().await.unwrap();
    query.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// 8. Compaction Merges Segments & Respects Size Cap
// ---------------------------------------------------------------------------

#[tokio::test]
async fn compaction_merges_segments_and_respects_size_cap() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut cfg = test_store_config(false, 0);
    cfg.l0_shards = 1; // force same (date, shard_id) bucket for deterministic tier merge
    cfg.max_segment_bytes = 512 * 1024;
    let store = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();

    let ts = 1_700_000_000_000i64;
    for i in 0..5 {
        let tid = format!("tr_cmp_{i}");
        store
            .ingest_batch(
                "proj_cmp",
                make_batch(&tid, Some("sess_cmp"), Some("u1"), ts + i * 100),
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
    }
    // Also send an update delta for tr_cmp_0 in a 6th flush
    let mut sp_up = make_span("tr_cmp_0", "tr_cmp_0_sp2", Some("tr_cmp_0_sp1"), ts + 50);
    sp_up.cost_nanos = 500_000;
    sp_up.cost_usd = 0.0005;
    store
        .ingest_batch(
            "proj_cmp",
            TraceIngestBatch {
                traces: vec![],
                spans: vec![sp_up],
            },
        )
        .await
        .unwrap();
    store.flush_all_l0().await.unwrap();

    let man_before = ManifestStore::new("proj_cmp", obj_store.clone(), 0)
        .unwrap()
        .load_latest()
        .await
        .unwrap();
    assert_eq!(man_before.segments.len(), 6);

    store.compact_and_gc(Some("proj_cmp"), &[]).await.unwrap();

    let man_after = ManifestStore::new("proj_cmp", obj_store.clone(), 0)
        .unwrap()
        .load_latest()
        .await
        .unwrap();
    assert!(man_after.segments.len() < man_before.segments.len());
    assert!(man_after.segments.iter().all(|s| s.level >= 1));
    assert!(man_after.segments.iter().all(|s| s.file_size <= 512 * 1024));

    let det = store
        .get_trace_detail("proj_cmp", "tr_cmp_0", &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(det.spans.len(), 2);
    assert_eq!(det.trace.total_cost_nanos, 1_750_000);

    store.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// 9. Compaction 15-Minute GC Grace Period & ReaderPinTracker Protection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn compaction_gc_15m_grace_and_reader_pinning() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut cfg = test_store_config(false, 900); // 15m grace
    cfg.l0_shards = 1;
    let store = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();

    let ts = 1_700_000_000_000i64;
    for i in 0..4 {
        store
            .ingest_batch(
                "proj_gc",
                make_batch(&format!("tr_gc_{i}"), Some("s_gc"), None, ts + i * 10),
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
    }

    let man_before = ManifestStore::new("proj_gc", obj_store.clone(), 900_000)
        .unwrap()
        .load_latest()
        .await
        .unwrap();
    let old_paths: Vec<String> = man_before
        .segments
        .iter()
        .map(|s| s.object_path.clone())
        .collect();
    assert_eq!(old_paths.len(), 4);

    store.compact_and_gc(Some("proj_gc"), &[]).await.unwrap();

    // Within 15-minute grace: old segments MUST still exist in ObjectStore & pending_gc
    let man_store = ManifestStore::new("proj_gc", obj_store.clone(), 900_000).unwrap();
    let man_after = man_store.load_latest().await.unwrap();
    assert_eq!(man_after.pending_gc.len(), 4);
    for entry in &man_after.pending_gc {
        assert_eq!(entry.eligible_after_ms - entry.superseded_at_ms, 900_000);
        assert!(obj_store
            .head(&ObjectPath::from(entry.object_path.as_str()))
            .await
            .is_ok());
    }

    // Simulated clock < 900s: run_gc deletes nothing
    let before_grace_ms = man_after.pending_gc[0].eligible_after_ms - 1;
    let pins = ReaderPinTracker::default();
    assert_eq!(man_store.run_gc(before_grace_ms, &pins).await.unwrap(), 0);

    // Simulated clock >= 900s while pinned: pinned path MUST NOT be deleted
    let after_grace_ms = man_after.pending_gc[0].eligible_after_ms + 1;
    let pinned_one = vec![old_paths[0].clone()];
    pins.pin_all(&pinned_one);
    let deleted_unpinned = man_store.run_gc(after_grace_ms, &pins).await.unwrap();
    assert_eq!(deleted_unpinned, 3);
    assert!(obj_store
        .head(&ObjectPath::from(old_paths[0].as_str()))
        .await
        .is_ok());
    assert!(obj_store
        .head(&ObjectPath::from(old_paths[1].as_str()))
        .await
        .is_err());

    // Unpin and re-run GC: remaining path deleted and pending_gc cleared
    pins.unpin_all(&pinned_one);
    assert_eq!(man_store.run_gc(after_grace_ms, &pins).await.unwrap(), 1);
    let final_man = man_store.load_latest().await.unwrap();
    assert!(final_man.pending_gc.is_empty());

    store.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// 10. O(1) Daily .plss Retention Pruning
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retention_pruning_o1_daily_segments() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = AgentTraceStore::open(obj_store.clone(), test_store_config(false, 0))
        .await
        .unwrap();

    // 2026-10-01, 2026-10-15, 2026-11-19
    let ts_oct1 = chrono::NaiveDate::from_ymd_opt(2026, 10, 1)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();
    let ts_oct15 = chrono::NaiveDate::from_ymd_opt(2026, 10, 15)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();
    let ts_nov19 = chrono::NaiveDate::from_ymd_opt(2026, 11, 19)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();

    store
        .ingest_batch("proj_ret", make_batch("tr_oct1", Some("s1"), None, ts_oct1))
        .await
        .unwrap();
    store
        .ingest_batch(
            "proj_ret",
            make_batch("tr_oct15", Some("s2"), None, ts_oct15),
        )
        .await
        .unwrap();
    store
        .ingest_batch(
            "proj_ret",
            make_batch("tr_nov19", Some("s3"), None, ts_nov19),
        )
        .await
        .unwrap();
    store.flush_all_l0().await.unwrap();

    let man_before = ManifestStore::new("proj_ret", obj_store.clone(), 0)
        .unwrap()
        .load_latest()
        .await
        .unwrap();
    assert_eq!(man_before.segments.len(), 3);
    let nov_seg = man_before
        .segments
        .iter()
        .find(|s| s.date == "2026-11-19")
        .unwrap()
        .clone();

    let cutoff = NaiveDate::from_ymd_opt(2026, 11, 1).unwrap();
    let pruned = store
        .prune_expired_segments("proj_ret", cutoff)
        .await
        .unwrap();
    assert_eq!(pruned, 2);

    let man_after = ManifestStore::new("proj_ret", obj_store.clone(), 0)
        .unwrap()
        .load_latest()
        .await
        .unwrap();
    assert_eq!(man_after.segments.len(), 1);
    assert_eq!(man_after.segments[0].segment_id, nov_seg.segment_id);
    assert_eq!(man_after.segments[0].object_path, nov_seg.object_path);

    // Expired .plss objects deleted, Nov 19 segment intact
    for old in man_before
        .segments
        .iter()
        .filter(|s| s.date != "2026-11-19")
    {
        assert!(obj_store
            .head(&ObjectPath::from(old.object_path.as_str()))
            .await
            .is_err());
    }
    assert!(obj_store
        .head(&ObjectPath::from(nov_seg.object_path.as_str()))
        .await
        .is_ok());

    let remaining = store
        .list_traces(
            "proj_ret",
            &TraceListFilter::default(),
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(remaining.total, 1);
    assert_eq!(remaining.traces[0].trace_id, "tr_nov19");

    store.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// 11. 3-Level Erasure Tombstones: Query Filter + Expired Ignore + Compaction Strip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn three_level_erasure_tombstones_immediate_filter_and_compaction_strip() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = AgentTraceStore::open(obj_store.clone(), test_store_config(false, 0))
        .await
        .unwrap();

    let ts = 1_700_000_000_000i64;
    // Flushed to PLSS
    store
        .ingest_batch(
            "proj_er",
            make_batch("tr_keep", Some("sess_keep"), Some("usr_keep"), ts),
        )
        .await
        .unwrap();
    store
        .ingest_batch(
            "proj_er",
            make_batch("tr_del_trace", Some("sess_keep"), Some("usr_keep"), ts + 10),
        )
        .await
        .unwrap();
    store
        .ingest_batch(
            "proj_er",
            make_batch("tr_del_sess", Some("sess_bye"), Some("usr_keep"), ts + 20),
        )
        .await
        .unwrap();
    store.flush_all_l0().await.unwrap();

    // Still in L0
    store
        .ingest_batch(
            "proj_er",
            make_batch("tr_del_user_l0", Some("sess_u"), Some("usr_bye"), ts + 30),
        )
        .await
        .unwrap();

    let now_ms = chrono::Utc::now().timestamp_millis();
    let tombstones = vec![
        AgentErasureTombstone {
            id: "tb_1".into(),
            project_id: "proj_er".into(),
            level: ErasureLevel::Trace,
            target_id: "tr_del_trace".into(),
            created_at: now_ms - 1000,
            expires_at: None,
        },
        AgentErasureTombstone {
            id: "tb_2".into(),
            project_id: "proj_er".into(),
            level: ErasureLevel::Session,
            target_id: "sess_bye".into(),
            created_at: now_ms - 1000,
            expires_at: Some(now_ms + 60_000),
        },
        AgentErasureTombstone {
            id: "tb_3".into(),
            project_id: "proj_er".into(),
            level: ErasureLevel::User,
            target_id: "usr_bye".into(),
            created_at: now_ms - 1000,
            expires_at: None,
        },
        // Expired tombstone targeting tr_keep -> MUST NOT hide tr_keep!
        AgentErasureTombstone {
            id: "tb_expired".into(),
            project_id: "proj_er".into(),
            level: ErasureLevel::Trace,
            target_id: "tr_keep".into(),
            created_at: now_ms - 10_000,
            expires_at: Some(now_ms - 1),
        },
    ];

    // Immediate query-time hiding across all query APIs
    let traces = store
        .list_traces(
            "proj_er",
            &TraceListFilter::default(),
            &HashMap::new(),
            &tombstones,
        )
        .await
        .unwrap();
    assert_eq!(traces.total, 1);
    assert_eq!(traces.traces[0].trace_id, "tr_keep");

    assert!(store
        .get_trace_detail("proj_er", "tr_del_trace", &tombstones)
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_trace_detail("proj_er", "tr_del_sess", &tombstones)
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_trace_detail("proj_er", "tr_del_user_l0", &tombstones)
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_trace_detail("proj_er", "tr_keep", &tombstones)
        .await
        .unwrap()
        .is_some());

    let sessions = store
        .list_sessions("proj_er", &SessionListFilter::default(), &tombstones)
        .await
        .unwrap();
    assert_eq!(sessions.total, 1);
    assert_eq!(sessions.sessions[0].session_id, "sess_keep");
    assert_eq!(sessions.sessions[0].trace_count, 1);
    assert!(store
        .get_session_detail("proj_er", "sess_bye", &tombstones)
        .await
        .unwrap()
        .is_none());

    let analytics = store
        .query_analytics("proj_er", &AnalyticsFilter::default(), &tombstones)
        .await
        .unwrap();
    assert_eq!(analytics.total_executions, 1);

    let cmp = store
        .compare_groups("proj_er", &CompareFilter::default(), &tombstones)
        .await
        .unwrap();
    assert_eq!(cmp.baseline_sample_size, 1);

    // Compact with tombstones -> physical rewrite strips erased traces permanently
    store.flush_all_l0().await.unwrap();
    store
        .compact_and_gc(Some("proj_er"), &tombstones)
        .await
        .unwrap();

    // Querying with EMPTY tombstones still only finds tr_keep!
    let post_compact = store
        .list_traces("proj_er", &TraceListFilter::default(), &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(post_compact.total, 1);
    assert_eq!(post_compact.traces[0].trace_id, "tr_keep");

    store.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// 12. EncryptedObjectStore BYOK + Foyer Cache Close & Reopen Roundtrip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn encrypted_object_store_byok_and_foyer_close_roundtrip() {
    let raw_mem = Arc::new(InMemory::new());
    let enc = Arc::new(EncryptedObjectStore::new(raw_mem.clone()));
    enc.register_master_key("key-2026", [0x5Au8; 32], true)
        .unwrap();

    let tmp_dir = TempDir::new().unwrap();
    let mut cfg = test_store_config(false, 0);
    cfg.cache_dir = Some(tmp_dir.path().join("agent_foyer_cache"));
    cfg.cache_disk_bytes = 32 * 1024 * 1024;

    let store1 =
        AgentTraceStore::open_with_encrypted(enc.as_object_store(), Some(enc.clone()), cfg.clone())
            .await
            .unwrap();

    let ts = 1_700_000_000_000i64;
    store1
        .ingest_batch(
            "proj_byok",
            make_batch("tr_byok_1", Some("s_byok"), Some("u_byok"), ts),
        )
        .await
        .unwrap();
    store1.flush_all_l0().await.unwrap();

    // Warm the foyer cache before close
    let det1 = store1
        .get_trace_detail("proj_byok", "tr_byok_1", &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(det1.trace.trace_id, "tr_byok_1");
    store1.close().await.unwrap();

    // Verify raw object store contains PLSEAGM2 encrypted envelope, not plaintext PLSS
    let man = ManifestStore::new("proj_byok", enc.as_object_store(), 0)
        .unwrap()
        .load_latest()
        .await
        .unwrap();
    let raw_seg = raw_mem
        .get(&ObjectPath::from(man.segments[0].object_path.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(raw_seg.starts_with(b"PLSEAGM2"));
    assert!(enc.status().ops_encrypted > 0);

    // Reopen fresh AgentTraceStore with same EncryptedObjectStore & foyer cache_dir
    let store2 = AgentTraceStore::open_with_encrypted(enc.as_object_store(), Some(enc), cfg)
        .await
        .unwrap();
    let det2 = store2
        .get_trace_detail("proj_byok", "tr_byok_1", &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(det2.trace.trace_id, "tr_byok_1");
    assert_eq!(det2.spans.len(), 1);
    let ov2 = store2
        .query_analytics("proj_byok", &AnalyticsFilter::default(), &[])
        .await
        .unwrap();
    assert_eq!(ov2.total_executions, 1);

    store2.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// 13. Keyset Pagination, eval_rollups Join & Tag 0x06 Pruning Efficiency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn keyset_pagination_eval_rollup_join_and_tag06_pruning_efficiency() {
    let raw_mem = Arc::new(InMemory::new());
    let counting = CountingStore::new(raw_mem);
    let obj_store: Arc<dyn ObjectStore> = counting.clone();

    let mut cfg = test_store_config(false, 0);
    cfg.l0_shards = 1;
    let writer = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();

    let ts = 1_700_000_000_000i64;
    // Create 4 disjoint segments with distinct agent_name / status / session_id
    for (idx, (agent, st, sess)) in [
        ("alpha", TraceStatus::Ok, "sess_a"),
        ("beta", TraceStatus::Error, "sess_b"),
        ("gamma", TraceStatus::Ok, "sess_c"),
        ("delta", TraceStatus::Ok, "sess_d"),
    ]
    .iter()
    .enumerate()
    {
        for j in 0..2 {
            let tid = format!("tr_{agent}_{j}");
            let tr = make_trace(
                &tid,
                agent,
                "1.0.0",
                Some(sess),
                Some("u1"),
                ts + (idx as i64) * 10_000 + j * 100,
                *st,
            );
            let sp = make_span(&tid, &format!("sp_{agent}_{j}"), None, tr.start_ts_ms);
            writer
                .ingest_batch(
                    "proj_prune",
                    TraceIngestBatch {
                        traces: vec![tr],
                        spans: vec![sp],
                    },
                )
                .await
                .unwrap();
        }
        writer.flush_all_l0().await.unwrap();
    }

    // Add 1 more matching trace in L0 to verify L0 + segment cursor pagination
    writer
        .ingest_batch(
            "proj_prune",
            make_batch("tr_beta_l0", Some("sess_b"), Some("u1"), ts + 50_000),
        )
        .await
        .unwrap();

    // Open a cold read-only query store so caches start empty
    let reader = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
        .await
        .unwrap();

    let man = ManifestStore::new("proj_prune", obj_store.clone(), 0)
        .unwrap()
        .load_latest()
        .await
        .unwrap();
    assert_eq!(man.segments.len(), 4);
    let seg_by_path: HashMap<String, _> = man
        .segments
        .iter()
        .map(|s| (s.object_path.clone(), s.clone()))
        .collect();

    counting.reset();
    let filter = TraceListFilter {
        agent_name: Some("beta".to_string()),
        status: Some("error".to_string()),
        session_id: Some("sess_b".to_string()),
        ..Default::default()
    };
    let res = reader
        .list_traces("proj_prune", &filter, &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(res.total, 2);

    // Verify Tag 0x06 pruned the other 3 segments (no Tag 0x01/0x02 reads outside tail_offset..file_size)
    // and ZERO Tag 0x04 or Tag 0x05 reads across ANY segment!
    let mut non_tail_segments = std::collections::HashSet::new();
    for (path, range) in counting.recorded_ranges() {
        if let Some(seg) = seg_by_path.get(&path) {
            if range.start < seg.tail_offset {
                non_tail_segments.insert(path.clone());
                // Also assert we didn't read beyond Tag 0x01/0x02/0x07 (i.e. never touched Tag 0x04 or Tag 0x05)
                let sec04_start = seg.tail_offset; // Tag 0x04/0x05 sit right before tail, wait: verify via PlssReader tail
                let _ = sec04_start;
            }
        }
    }
    assert_eq!(
        non_tail_segments.len(),
        1,
        "only the matching beta segment should have non-tail section reads, got {non_tail_segments:?}"
    );

    // Explicitly verify Tag 0x04 / Tag 0x05 byte ranges were never touched during list_traces
    let plss_reader = pulse_agent::store::segment::PlssReader::new(
        obj_store.clone(),
        pulse_agent::store::cache::AgentSegmentCache::open(None, 1024 * 1024, None, 0)
            .await
            .unwrap(),
    );
    let beta_seg_path = non_tail_segments.iter().next().unwrap();
    let beta_seg = &seg_by_path[beta_seg_path];
    let beta_tail = plss_reader.read_eof_tail(beta_seg).await.unwrap();
    let sec04 = beta_tail.find_section(SectionTag::SpanTree).unwrap();
    let sec05 = beta_tail.find_section(SectionTag::PayloadPack).unwrap();
    for (path, range) in counting.recorded_ranges() {
        if path == *beta_seg_path {
            let touches_04 =
                range.start < sec04.offset + sec04.len as u64 && range.end > sec04.offset;
            let touches_05 =
                range.start < sec05.offset + sec05.len as u64 && range.end > sec05.offset;
            assert!(!touches_04, "list_traces must NEVER read Tag 0x04");
            assert!(!touches_05, "list_traces must NEVER read Tag 0x05");
        }
    }

    // Eval rollup join + score filter + score sort + keyset cursor pagination across all 9 traces
    let mut rollups = HashMap::new();
    for (idx, tid) in [
        "tr_alpha_0",
        "tr_alpha_1",
        "tr_beta_0",
        "tr_beta_1",
        "tr_gamma_0",
        "tr_gamma_1",
        "tr_delta_0",
        "tr_delta_1",
        "tr_beta_l0",
    ]
    .iter()
    .enumerate()
    {
        rollups.insert(
            (EvalTargetType::Trace, (*tid).to_string()),
            AgentEvalRollup {
                project_id: "proj_prune".to_string(),
                target_type: EvalTargetType::Trace,
                target_id: (*tid).to_string(),
                avg_score: (idx as f64 + 1.0) / 10.0,
                pass_rate: Some(if idx >= 4 { 1.0 } else { 0.0 }),
                eval_count: 1,
                updated_at: ts,
            },
        );
    }

    let mut page_filter = TraceListFilter {
        min_eval_score: Some(0.35),
        sort_by: Some("avg_eval_score".to_string()),
        sort_dir: Some("desc".to_string()),
        limit: Some(2),
        ..Default::default()
    };
    let mut collected = Vec::new();
    loop {
        let page = reader
            .list_traces("proj_prune", &page_filter, &rollups, &[])
            .await
            .unwrap();
        assert_eq!(page.total, 6);
        collected.extend(page.traces);
        match page.next_cursor {
            Some(c) => page_filter.cursor = Some(c),
            None => break,
        }
    }
    assert_eq!(collected.len(), 6);
    let uniq: std::collections::HashSet<&str> =
        collected.iter().map(|t| t.trace_id.as_str()).collect();
    assert_eq!(uniq.len(), 6);
    for w in collected.windows(2) {
        assert!(w[0].avg_eval_score.unwrap() >= w[1].avg_eval_score.unwrap());
    }

    // Cold get_trace_detail coalesced get_range check (<= 2 calls after EOF tail, 0 on warm hit)
    let cold_reader = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
        .await
        .unwrap();
    counting.reset();
    let det = cold_reader
        .get_trace_detail("proj_prune", "tr_alpha_0", &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(det.trace.trace_id, "tr_alpha_0");
    let cold_calls = counting.recorded_ranges().len();
    // 4 read_eof_tail checks (at most 4) + <= 2 coalesced Tag 0x04/0x05 calls on matching segment
    let alpha_seg = man
        .segments
        .iter()
        .find(|s| {
            counting
                .recorded_ranges()
                .iter()
                .any(|(p, r)| p == &s.object_path && r.start < s.tail_offset)
        })
        .unwrap();
    let alpha_non_tail_calls = counting
        .recorded_ranges()
        .iter()
        .filter(|(p, r)| p == &alpha_seg.object_path && r.start < alpha_seg.tail_offset)
        .count();
    assert!(
        alpha_non_tail_calls <= 2,
        "cold get_trace_detail must hydrate in <= 2 coalesced get_range calls (got {alpha_non_tail_calls}, total={cold_calls})"
    );

    // Warm cache hit -> 0 ObjectStore get_range calls
    counting.reset();
    let det_warm = cold_reader
        .get_trace_detail("proj_prune", "tr_alpha_0", &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(det_warm.trace.trace_id, "tr_alpha_0");
    assert_eq!(counting.recorded_ranges().len(), 0);

    writer.close().await.unwrap();
    reader.close().await.unwrap();
    cold_reader.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// 14. compare_groups MIN_SIGNIFICANCE_SAMPLES = 20 Guard
// ---------------------------------------------------------------------------

#[tokio::test]
async fn compare_groups_min_20_sample_guard() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = AgentTraceStore::open(obj_store, test_store_config(false, 0))
        .await
        .unwrap();
    let ts = 1_700_000_000_000i64;

    // 19 baseline v1 traces (fast, cheap)
    for i in 0..(MIN_SIGNIFICANCE_SAMPLES - 1) {
        let tid = format!("tr_v1_{i}");
        let mut tr = make_trace(
            &tid,
            "qa",
            "v1",
            None,
            None,
            ts + i as i64 * 10,
            TraceStatus::Ok,
        );
        tr.duration_ms = 100 + (i as i64 % 5);
        let mut sp = make_span(&tid, &format!("sp_v1_{i}"), None, tr.start_ts_ms);
        sp.duration_ms = tr.duration_ms;
        sp.end_ts_ms = Some(tr.start_ts_ms + tr.duration_ms);
        sp.cost_nanos = 1_000_000;
        store
            .ingest_batch(
                "proj_cmp20",
                TraceIngestBatch {
                    traces: vec![tr],
                    spans: vec![sp],
                },
            )
            .await
            .unwrap();
    }
    // 25 candidate v2 traces (significantly slower & costlier)
    for i in 0..25 {
        let tid = format!("tr_v2_{i}");
        let mut tr = make_trace(
            &tid,
            "qa",
            "v2",
            None,
            None,
            ts + 10_000 + i * 10,
            TraceStatus::Ok,
        );
        tr.duration_ms = 800 + (i % 10);
        let mut sp = make_span(&tid, &format!("sp_v2_{i}"), None, tr.start_ts_ms);
        sp.duration_ms = tr.duration_ms;
        sp.end_ts_ms = Some(tr.start_ts_ms + tr.duration_ms);
        sp.cost_nanos = 5_000_000;
        store
            .ingest_batch(
                "proj_cmp20",
                TraceIngestBatch {
                    traces: vec![tr],
                    spans: vec![sp],
                },
            )
            .await
            .unwrap();
    }
    store.flush_all_l0().await.unwrap();

    let cmp_filter = CompareFilter {
        baseline_agent_version: Some("v1".to_string()),
        candidate_agent_version: Some("v2".to_string()),
        ..Default::default()
    };

    let guard_res = store
        .compare_groups("proj_cmp20", &cmp_filter, &[])
        .await
        .unwrap();
    assert!(!guard_res.sufficient_samples);
    assert_eq!(guard_res.overall_verdict, "insufficient_data");
    assert_eq!(guard_res.latency_ms.verdict, "insufficient_data");
    assert!(guard_res.latency_ms.p_value.is_none());

    // Direct stats guard with NaN/Inf filtering
    let nan_guard = compare_continuous_metric(&[f64::NAN; 25], &[1.0; 25], false);
    assert_eq!(nan_guard.verdict, "insufficient_data");

    // Add 20th v1 trace -> reaches MIN_SIGNIFICANCE_SAMPLES = 20 -> significant_regression!
    let mut tr20 = make_trace(
        "tr_v1_19",
        "qa",
        "v1",
        None,
        None,
        ts + 999,
        TraceStatus::Ok,
    );
    tr20.duration_ms = 102;
    let mut sp20 = make_span("tr_v1_19", "sp_v1_19", None, tr20.start_ts_ms);
    sp20.duration_ms = 102;
    sp20.end_ts_ms = Some(tr20.start_ts_ms + 102);
    sp20.cost_nanos = 1_000_000;
    store
        .ingest_batch(
            "proj_cmp20",
            TraceIngestBatch {
                traces: vec![tr20],
                spans: vec![sp20],
            },
        )
        .await
        .unwrap();

    let sig_res = store
        .compare_groups("proj_cmp20", &cmp_filter, &[])
        .await
        .unwrap();
    assert!(sig_res.sufficient_samples);
    assert_eq!(sig_res.overall_verdict, "significant_regression");
    assert_eq!(sig_res.latency_ms.verdict, "significant_regression");
    assert!(sig_res.latency_ms.p_value.unwrap() < 0.05);
    assert_eq!(sig_res.cost_usd.verdict, "significant_regression");

    store.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// 15. Benchmark: PLST v1 (Dict + Blob Dedup) vs OTLP/JSON
// ---------------------------------------------------------------------------

#[test]
fn bench_plst_vs_otlp() {
    let sys_prompt = "You are Pulse Assistant, an expert autonomous engineering agent. Always cite file paths, verify invariants, and avoid destructive commands without confirmation. ".repeat(4);
    let mut resource_spans = Vec::with_capacity(100);
    let base_ns: u64 = 1_700_000_000_000_000_000;

    for t in 0..100u64 {
        let trace_id = format!("trace_{t:04}");
        let mut spans = Vec::with_capacity(8);
        for s in 0..8u64 {
            let span_id = format!("span_{t:04}_{s:02}");
            let st_ns = base_ns + (t * 1_000 + s * 50) * 1_000_000;
            let en_ns = st_ns + 45_000_000;
            let span_type = match s % 4 {
                0 => "agent",
                1 => "llm",
                2 => "tool",
                _ => "guardrail",
            };
            let mut attrs = vec![
                json!({ "key": "pulse.span_type", "value": { "stringValue": span_type } }),
                json!({ "key": "pulse.span_phase", "value": { "stringValue": "final" } }),
                json!({ "key": "gen_ai.system", "value": { "stringValue": "openai" } }),
                json!({ "key": "gen_ai.request.model", "value": { "stringValue": "gpt-4o" } }),
                json!({ "key": "gen_ai.usage.input_tokens", "value": { "intValue": 420 } }),
                json!({ "key": "gen_ai.usage.output_tokens", "value": { "intValue": 128 } }),
                json!({
                    "key": "gen_ai.prompt",
                    "value": { "stringValue": format!("{{\"messages\":[{{\"role\":\"system\",\"content\":\"{sys_prompt}\"}},{{\"role\":\"user\",\"content\":\"Analyze step {s} for ticket #{t}\"}}]}}") }
                }),
                json!({
                    "key": "gen_ai.completion",
                    "value": { "stringValue": format!("{{\"choices\":[{{\"index\":0,\"message\":{{\"role\":\"assistant\",\"content\":\"Completed analysis step {s} cleanly.\"}},\"finish_reason\":\"stop\"}}]}}") }
                }),
            ];
            if span_type == "tool" {
                attrs.push(
                    json!({ "key": "gen_ai.tool.name", "value": { "stringValue": "grep_search" } }),
                );
            }
            let mut sp_obj = json!({
                "traceId": trace_id,
                "spanId": span_id,
                "name": format!("{span_type}_op_{s}"),
                "startTimeUnixNano": st_ns.to_string(),
                "endTimeUnixNano": en_ns.to_string(),
                "attributes": attrs,
                "status": { "code": 1 }
            });
            if s > 0 {
                sp_obj["parentSpanId"] = json!(format!("span_{t:04}_00"));
            }
            spans.push(sp_obj);
        }
        resource_spans.push(json!({
            "resource": {
                "attributes": [
                    { "key": "service.name", "value": { "stringValue": "pulse-demo-agent" } },
                    { "key": "deployment.environment", "value": { "stringValue": "production" } }
                ]
            },
            "scopeSpans": [{
                "scope": { "name": "pulse-sdk", "version": "1.0.0" },
                "spans": spans
            }]
        }));
    }

    let otlp_json_val = json!({ "resourceSpans": resource_spans });
    let otlp_json_bytes = serde_json::to_vec(&otlp_json_val).unwrap();
    let otlp_req: OtlpExportTraceRequest = serde_json::from_slice(&otlp_json_bytes).unwrap();
    let canonical_batch = normalize_otlp_traces(otlp_req);
    assert_eq!(canonical_batch.traces.len(), 100);
    assert_eq!(canonical_batch.spans.len(), 800);

    let plst_raw_bytes = encode_plst_batch(&canonical_batch, false).unwrap();
    let plst_dict_bytes = encode_plst_batch(&canonical_batch, true).unwrap();
    assert!(!agent_zstd_dict().is_empty());

    let iters = 50u32;
    let t_otlp = Instant::now();
    for _ in 0..iters {
        let parsed: OtlpExportTraceRequest = serde_json::from_slice(&otlp_json_bytes).unwrap();
        let norm = normalize_otlp_traces(parsed);
        assert_eq!(norm.spans.len(), 800);
    }
    let otlp_elapsed = t_otlp.elapsed();

    let t_plst = Instant::now();
    for _ in 0..iters {
        let decoded = decode_plst_batch(&plst_dict_bytes).unwrap();
        assert_eq!(decoded.traces.len(), 100);
        assert_eq!(decoded.spans.len(), 800);
    }
    let plst_elapsed = t_plst.elapsed();

    let otlp_ns_per_iter = otlp_elapsed.as_nanos() / u128::from(iters);
    let plst_ns_per_iter = plst_elapsed.as_nanos() / u128::from(iters);
    let size_ratio = otlp_json_bytes.len() as f64 / plst_dict_bytes.len() as f64;
    let speedup = otlp_ns_per_iter as f64 / plst_ns_per_iter.max(1) as f64;

    println!(
        "bench_plst_vs_otlp (100 traces x 8 spans = 800 spans): OTLP/JSON={} B, PLST_raw={} B, PLST_dict={} B ({:.2}x smaller); OTLP_decode={} ns/batch, PLST_decode={} ns/batch ({:.2}x faster)",
        otlp_json_bytes.len(),
        plst_raw_bytes.len(),
        plst_dict_bytes.len(),
        size_ratio,
        otlp_ns_per_iter,
        plst_ns_per_iter,
        speedup
    );

    assert!(
        plst_dict_bytes.len() < otlp_json_bytes.len(),
        "PLST dict ({}) must be smaller than OTLP/JSON ({})",
        plst_dict_bytes.len(),
        otlp_json_bytes.len()
    );
    assert!(
        plst_ns_per_iter < otlp_ns_per_iter,
        "PLST decode ({plst_ns_per_iter} ns) must be faster than OTLP/JSON decode ({otlp_ns_per_iter} ns)"
    );
}

// ---------------------------------------------------------------------------
// 16–21. Iteration 2 Regression Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn regression_cross_tenant_l0_erasure_and_post_merge_partial_compaction_strip() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut cfg = test_store_config(false, 0);
    cfg.l0_shards = 1;
    let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
    let ts = 1_700_000_000_000i64;

    // Part A: Cross-tenant L0 erasure isolation during compact_and_gc
    store
        .ingest_batch(
            "proj_a",
            make_batch("tr_shared", Some("sess_a"), Some("u_keep"), ts),
        )
        .await
        .unwrap();
    store
        .ingest_batch(
            "proj_a",
            make_batch("tr_a_victim", Some("sess_v"), Some("u_erase"), ts + 100),
        )
        .await
        .unwrap();

    let tb_proj_a_user = AgentErasureTombstone {
        id: "tb_a_u".to_string(),
        project_id: "proj_a".to_string(),
        level: ErasureLevel::User,
        target_id: "u_erase".to_string(),
        created_at: ts,
        expires_at: None,
    };
    let tb_proj_b_trace = AgentErasureTombstone {
        id: "tb_b_tr".to_string(),
        project_id: "proj_b".to_string(),
        level: ErasureLevel::Trace,
        target_id: "tr_shared".to_string(),
        created_at: ts,
        expires_at: None,
    };

    store
        .compact_and_gc(Some("proj_a"), &[tb_proj_a_user, tb_proj_b_trace])
        .await
        .unwrap();

    let listed = store
        .list_traces("proj_a", &TraceListFilter::default(), &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(listed.traces.len(), 1);
    assert_eq!(listed.traces[0].trace_id, "tr_shared");
    assert!(store
        .get_trace_detail("proj_a", "tr_a_victim", &[])
        .await
        .unwrap()
        .is_none());

    // Part B: Post-merge Session/User erasure on partial span-only segments (both orders)
    for (pid, span_only_first, level) in [
        ("proj_pc_fwd", false, ErasureLevel::Session),
        ("proj_pc_rev", true, ErasureLevel::Session),
        ("proj_pc_user", true, ErasureLevel::User),
    ] {
        let mut hdr_tr = make_trace(
            "tr_part",
            "planner",
            "1.0",
            Some("sess_bye"),
            Some("u_bye"),
            ts,
            TraceStatus::Running,
        );
        hdr_tr.span_count = 1;
        let sp_1 = make_span("tr_part", "sp_1", None, ts);
        let sp_2 = make_span("tr_part", "sp_2", Some("sp_1"), ts + 50);

        if span_only_first {
            store
                .ingest_batch(
                    pid,
                    TraceIngestBatch {
                        traces: vec![],
                        spans: vec![sp_2.clone()],
                    },
                )
                .await
                .unwrap();
            store.flush_all_l0().await.unwrap();

            store
                .ingest_batch(
                    pid,
                    TraceIngestBatch {
                        traces: vec![hdr_tr.clone()],
                        spans: vec![sp_1.clone()],
                    },
                )
                .await
                .unwrap();
            store.flush_all_l0().await.unwrap();
        } else {
            store
                .ingest_batch(
                    pid,
                    TraceIngestBatch {
                        traces: vec![hdr_tr.clone()],
                        spans: vec![sp_1.clone()],
                    },
                )
                .await
                .unwrap();
            store.flush_all_l0().await.unwrap();

            store
                .ingest_batch(
                    pid,
                    TraceIngestBatch {
                        traces: vec![],
                        spans: vec![sp_2.clone()],
                    },
                )
                .await
                .unwrap();
            store.flush_all_l0().await.unwrap();
        }

        store
            .ingest_batch(
                pid,
                make_batch("tr_keep", Some("sess_keep"), Some("u_keep"), ts + 200),
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let tb = AgentErasureTombstone {
            id: format!("tb_{pid}"),
            project_id: pid.to_string(),
            level,
            target_id: match level {
                ErasureLevel::Session => "sess_bye".to_string(),
                ErasureLevel::User => "u_bye".to_string(),
                ErasureLevel::Trace => unreachable!(),
            },
            created_at: ts,
            expires_at: None,
        };
        store.compact_and_gc(Some(pid), &[tb]).await.unwrap();

        // Query WITHOUT tombstones to prove physical removal of even the span-only partial segment
        let after = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(
            after.traces.len(),
            1,
            "partial span-only segment survived compaction erasure for {pid}: {:?}",
            after.traces
        );
        assert_eq!(after.traces[0].trace_id, "tr_keep");
        assert!(store
            .get_trace_detail(pid, "tr_part", &[])
            .await
            .unwrap()
            .is_none());
    }

    store.close().await.unwrap();
}

#[tokio::test]
async fn regression_commutative_merge_trace_headers_late_start_partial_output() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut cfg = test_store_config(false, 0);
    cfg.l0_shards = 1;
    let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
    let ts = 1_700_000_000_000i64;

    for (pid, final_first) in [("proj_ooo_comm", true), ("proj_in_comm", false)] {
        let mut tr_final = make_trace(
            "tr_comm",
            "planner",
            "1.0",
            Some("sess_c"),
            Some("u_c"),
            ts,
            TraceStatus::Ok,
        );
        tr_final.output = json!({"final_answer": "complete"});
        tr_final.metadata = json!({"stage": "final"});
        let sp_final = make_span("tr_comm", "sp_1", None, ts);

        let mut tr_start = make_trace(
            "tr_comm",
            "planner",
            "1.0",
            Some("sess_c"),
            Some("u_c"),
            ts,
            TraceStatus::Running,
        );
        tr_start.output = json!({"partial": "streaming..."});
        tr_start.metadata = json!({"stage": "start"});
        let mut sp_start = make_span("tr_comm", "sp_1", None, ts);
        sp_start.span_phase = SpanPhase::Start;
        sp_start.status = TraceStatus::Running;

        if final_first {
            store
                .ingest_batch(
                    pid,
                    TraceIngestBatch {
                        traces: vec![tr_final],
                        spans: vec![sp_final],
                    },
                )
                .await
                .unwrap();
            store.flush_all_l0().await.unwrap();
            store
                .ingest_batch(
                    pid,
                    TraceIngestBatch {
                        traces: vec![tr_start],
                        spans: vec![sp_start],
                    },
                )
                .await
                .unwrap();
            store.flush_all_l0().await.unwrap();
        } else {
            store
                .ingest_batch(
                    pid,
                    TraceIngestBatch {
                        traces: vec![tr_start],
                        spans: vec![sp_start],
                    },
                )
                .await
                .unwrap();
            store.flush_all_l0().await.unwrap();
            store
                .ingest_batch(
                    pid,
                    TraceIngestBatch {
                        traces: vec![tr_final],
                        spans: vec![sp_final],
                    },
                )
                .await
                .unwrap();
            store.flush_all_l0().await.unwrap();
        }

        let before = store
            .get_trace_detail(pid, "tr_comm", &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before.trace.status, TraceStatus::Ok);
        assert_eq!(before.trace.output, json!({"final_answer": "complete"}));
        assert_eq!(before.trace.metadata, json!({"stage": "final"}));

        store.compact_and_gc(Some(pid), &[]).await.unwrap();

        let after = store
            .get_trace_detail(pid, "tr_comm", &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.trace.status, TraceStatus::Ok);
        assert_eq!(after.trace.output, json!({"final_answer": "complete"}));
        assert_eq!(after.trace.metadata, json!({"stage": "final"}));
    }

    store.close().await.unwrap();
}

#[tokio::test]
async fn regression_list_sessions_user_tombstone_via_l0_update() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut cfg = test_store_config(false, 0);
    cfg.l0_shards = 1;
    let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
    let ts = 1_700_000_000_000i64;
    let pid = "proj_sess_u_l0";

    // Segment 1: tr_anon in sess_anon with user_id = None
    let tr_seg = make_trace(
        "tr_anon",
        "ag1",
        "1.0",
        Some("sess_anon"),
        None,
        ts,
        TraceStatus::Running,
    );
    let sp_seg = make_span("tr_anon", "sp_1", None, ts);
    store
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr_seg],
                spans: vec![sp_seg],
            },
        )
        .await
        .unwrap();
    store.flush_all_l0().await.unwrap();

    // Also Segment 1 has sess_multi with tr_m1 (user_id = None) and Segment 2 has tr_m2 (user_id = u_erased)
    store
        .ingest_batch(pid, make_batch("tr_m1", Some("sess_multi"), None, ts + 10))
        .await
        .unwrap();
    store.flush_all_l0().await.unwrap();
    store
        .ingest_batch(
            pid,
            make_batch("tr_m2", Some("sess_multi"), Some("u_erased"), ts + 20),
        )
        .await
        .unwrap();
    store.flush_all_l0().await.unwrap();

    // Surviving session for another user
    store
        .ingest_batch(
            pid,
            make_batch("tr_ok", Some("sess_ok"), Some("u_keep"), ts + 30),
        )
        .await
        .unwrap();
    store.flush_all_l0().await.unwrap();

    // Unflushed L0 update attaches user_id = Some("u_erased") to tr_anon
    let tr_l0 = make_trace(
        "tr_anon",
        "ag1",
        "1.0",
        Some("sess_anon"),
        Some("u_erased"),
        ts,
        TraceStatus::Ok,
    );
    store
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr_l0],
                spans: vec![],
            },
        )
        .await
        .unwrap();

    let tb_user = AgentErasureTombstone {
        id: "tb_u".to_string(),
        project_id: pid.to_string(),
        level: ErasureLevel::User,
        target_id: "u_erased".to_string(),
        created_at: ts,
        expires_at: None,
    };

    assert!(store
        .get_session_detail(pid, "sess_anon", std::slice::from_ref(&tb_user))
        .await
        .unwrap()
        .is_none());

    let listed = store
        .list_sessions(pid, &SessionListFilter::default(), &[tb_user])
        .await
        .unwrap();
    assert_eq!(listed.sessions.len(), 1, "got {:?}", listed.sessions);
    assert_eq!(listed.sessions[0].session_id, "sess_ok");

    store.close().await.unwrap();
}

#[tokio::test]
async fn regression_list_sessions_multi_segment_filter_no_truncation() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut cfg = test_store_config(false, 0);
    cfg.l0_shards = 1;
    let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
    let ts = 1_700_000_000_000i64;
    let pid = "proj_ms_sess";

    // Segment 1: Ok trace (100 tokens, 1_000_000 nanos, agent = "planner")
    let mut tr1 = make_trace(
        "tr_ms_1",
        "planner",
        "1.0",
        Some("sess_split"),
        Some("u1"),
        ts,
        TraceStatus::Ok,
    );
    tr1.total_tokens = 100;
    tr1.total_cost_nanos = 1_000_000;
    let mut sp1 = make_span("tr_ms_1", "sp_1", None, ts);
    sp1.input_tokens = 80;
    sp1.output_tokens = 20;
    sp1.total_tokens = 100;
    sp1.cost_nanos = 1_000_000;
    store
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr1],
                spans: vec![sp1],
            },
        )
        .await
        .unwrap();
    store.flush_all_l0().await.unwrap();

    // Segment 2: Error trace (200 tokens, 2_000_000 nanos, agent = "executor")
    let mut tr2 = make_trace(
        "tr_ms_2",
        "executor",
        "1.0",
        Some("sess_split"),
        Some("u1"),
        ts + 5_000,
        TraceStatus::Error,
    );
    tr2.total_tokens = 200;
    tr2.total_cost_nanos = 2_000_000;
    let mut sp2 = make_span("tr_ms_2", "sp_2", None, ts + 5_000);
    sp2.status = TraceStatus::Error;
    sp2.input_tokens = 150;
    sp2.output_tokens = 50;
    sp2.total_tokens = 200;
    sp2.cost_nanos = 2_000_000;
    store
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr2],
                spans: vec![sp2],
            },
        )
        .await
        .unwrap();
    store.flush_all_l0().await.unwrap();

    let detail = store
        .get_session_detail(pid, "sess_split", &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detail.summary.trace_count, 2);
    assert_eq!(detail.summary.total_tokens, 300);
    assert_eq!(detail.summary.total_cost_nanos, 3_000_000);

    for filter in [
        SessionListFilter {
            has_error: Some(true),
            ..Default::default()
        },
        SessionListFilter {
            start_ts_ms: Some(ts + 4_000),
            ..Default::default()
        },
        SessionListFilter {
            agent_name: Some("executor".to_string()),
            ..Default::default()
        },
    ] {
        let res = store.list_sessions(pid, &filter, &[]).await.unwrap();
        assert_eq!(res.sessions.len(), 1);
        let s = &res.sessions[0];
        assert_eq!(s.session_id, "sess_split");
        assert_eq!(s.trace_count, detail.summary.trace_count);
        assert_eq!(s.error_count, detail.summary.error_count);
        assert_eq!(s.total_tokens, detail.summary.total_tokens);
        assert_eq!(s.total_cost_nanos, detail.summary.total_cost_nanos);
        assert_eq!(s.first_ts_ms, detail.summary.first_ts_ms);
        assert_eq!(s.last_ts_ms, detail.summary.last_ts_ms);
        assert_eq!(s.agent_names, detail.summary.agent_names);
    }

    store.close().await.unwrap();
}

#[tokio::test]
async fn regression_session_endpoints_include_l0_span_only_updates() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut cfg = test_store_config(false, 0);
    cfg.l0_shards = 1;
    let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
    let ts = 1_700_000_000_000i64;
    let pid = "proj_l0_span_only";

    let mut tr = make_trace(
        "tr_sess",
        "planner",
        "1.0",
        Some("sess_l0_sp"),
        Some("u1"),
        ts,
        TraceStatus::Running,
    );
    tr.total_tokens = 100;
    tr.total_cost_nanos = 1_000_000;
    let mut sp1 = make_span("tr_sess", "sp_1", None, ts);
    sp1.span_phase = SpanPhase::Start;
    sp1.status = TraceStatus::Running;
    sp1.input_tokens = 80;
    sp1.output_tokens = 20;
    sp1.total_tokens = 100;
    sp1.cost_nanos = 1_000_000;
    store
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr],
                spans: vec![sp1],
            },
        )
        .await
        .unwrap();
    store.flush_all_l0().await.unwrap();

    // Unflushed L0 span-only update (synthesizes L0 entry with session_id = None)
    let mut sp2 = make_span("tr_sess", "sp_2", Some("sp_1"), ts + 100);
    sp2.status = TraceStatus::Error;
    sp2.input_tokens = 200;
    sp2.output_tokens = 50;
    sp2.total_tokens = 250;
    sp2.cost_nanos = 2_500_000;
    store
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![],
                spans: vec![sp2],
            },
        )
        .await
        .unwrap();

    let listed = store
        .list_sessions(pid, &SessionListFilter::default(), &[])
        .await
        .unwrap();
    assert_eq!(listed.sessions.len(), 1);
    assert_eq!(listed.sessions[0].trace_count, 1);
    assert_eq!(listed.sessions[0].error_count, 1);
    assert_eq!(listed.sessions[0].total_tokens, 350);
    assert_eq!(listed.sessions[0].total_cost_nanos, 3_500_000);

    let detail = store
        .get_session_detail(pid, "sess_l0_sp", &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detail.summary.error_count, 1);
    assert_eq!(detail.summary.total_tokens, 350);
    assert_eq!(detail.summary.total_cost_nanos, 3_500_000);
    assert_eq!(detail.traces.len(), 1);
    assert_eq!(detail.traces[0].status, TraceStatus::Error);

    store.close().await.unwrap();
}

#[tokio::test]
async fn regression_cross_segment_split_trace_tag06_pruning_and_counting_store() {
    let raw_mem = Arc::new(InMemory::new());
    let counting = CountingStore::new(raw_mem);
    let obj_store: Arc<dyn ObjectStore> = counting.clone();

    let mut cfg = test_store_config(false, 0);
    cfg.l0_shards = 1;
    let writer = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();
    let ts = 1_700_000_000_000i64;
    let pid = "proj_split_tag06";

    // Segment 1: tr_split part 1 (model = gpt-4o, status = Ok) + disjoint tr_ok_only
    let mut tr_s1 = make_trace(
        "tr_split",
        "planner",
        "1.0",
        Some("sess_sp"),
        Some("u1"),
        ts,
        TraceStatus::Ok,
    );
    tr_s1.total_tokens = 100;
    tr_s1.total_cost_nanos = 1_000_000;
    let mut sp_llm = make_span("tr_split", "sp_llm", None, ts);
    sp_llm.span_type = SpanType::Llm;
    sp_llm.model = Some("gpt-4o".to_string());
    sp_llm.input_tokens = 80;
    sp_llm.output_tokens = 20;
    sp_llm.total_tokens = 100;
    sp_llm.cost_nanos = 1_000_000;

    let tr_ok_only = make_trace(
        "tr_ok_only",
        "ok_agent",
        "1.0",
        Some("sess_ok"),
        Some("u2"),
        ts + 10,
        TraceStatus::Ok,
    );
    let sp_ok = make_span("tr_ok_only", "sp_ok", None, ts + 10);

    writer
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr_s1, tr_ok_only],
                spans: vec![sp_llm, sp_ok],
            },
        )
        .await
        .unwrap();
    writer.flush_all_l0().await.unwrap();

    // Segment 2: tr_split part 2 (tool span, model = None, status = Error)
    let mut tr_s2 = make_trace(
        "tr_split",
        "planner",
        "1.0",
        Some("sess_sp"),
        Some("u1"),
        ts + 50,
        TraceStatus::Error,
    );
    tr_s2.primary_model = None;
    tr_s2.primary_provider = None;
    tr_s2.total_tokens = 200;
    tr_s2.total_cost_nanos = 2_000_000;
    let mut sp_tool = make_span("tr_split", "sp_tool", Some("sp_llm"), ts + 50);
    sp_tool.span_type = SpanType::Tool;
    sp_tool.status = TraceStatus::Error;
    sp_tool.model = None;
    sp_tool.provider = None;
    sp_tool.tool_name = Some("bad_tool".to_string());
    sp_tool.input_tokens = 150;
    sp_tool.output_tokens = 50;
    sp_tool.total_tokens = 200;
    sp_tool.cost_nanos = 2_000_000;

    writer
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr_s2],
                spans: vec![sp_tool],
            },
        )
        .await
        .unwrap();
    writer.flush_all_l0().await.unwrap();

    // Segment 3: disjoint tr_disjoint
    let mut tr_dis = make_trace(
        "tr_disjoint",
        "disjoint_agent",
        "1.0",
        Some("sess_dis"),
        Some("u3"),
        ts + 5_000,
        TraceStatus::Ok,
    );
    tr_dis.primary_model = Some("claude-sonnet-4".to_string());
    let mut sp_dis = make_span("tr_disjoint", "sp_dis", None, ts + 5_000);
    sp_dis.model = Some("claude-sonnet-4".to_string());
    writer
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr_dis],
                spans: vec![sp_dis],
            },
        )
        .await
        .unwrap();
    writer.flush_all_l0().await.unwrap();

    let reader = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
        .await
        .unwrap();

    // 1) list_traces(status = "ok") must NOT return tr_split (merged CRDT status is Error)
    let ok_res = reader
        .list_traces(
            pid,
            &TraceListFilter {
                status: Some("ok".to_string()),
                ..Default::default()
            },
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    let ok_ids: Vec<&str> = ok_res.traces.iter().map(|t| t.trace_id.as_str()).collect();
    assert!(!ok_ids.contains(&"tr_split"), "got {ok_ids:?}");
    assert!(ok_ids.contains(&"tr_ok_only"));
    assert!(ok_ids.contains(&"tr_disjoint"));

    // 2) list_traces(status = "error") must complete tr_split with span_count = 2 and full totals
    let err_res = reader
        .list_traces(
            pid,
            &TraceListFilter {
                status: Some("error".to_string()),
                ..Default::default()
            },
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(err_res.traces.len(), 1);
    assert_eq!(err_res.traces[0].trace_id, "tr_split");
    assert_eq!(err_res.traces[0].span_count, 2);
    assert_eq!(err_res.traces[0].total_tokens, 300);
    assert_eq!(err_res.traces[0].total_cost_nanos, 3_000_000);

    // 3) query_analytics(model = "gpt-4o", agent_name = "planner") must include Seg 2's Error & cost & tool
    let qa = reader
        .query_analytics(
            pid,
            &AnalyticsFilter {
                model: Some("gpt-4o".to_string()),
                agent_name: Some("planner".to_string()),
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();
    assert_eq!(qa.total_executions, 1);
    assert_eq!(qa.failed_executions, 1);
    assert!((qa.total_cost_usd - 0.003).abs() < 1e-9);
    assert_eq!(qa.by_tool.len(), 1);
    assert_eq!(qa.by_tool[0].tool_name, "bad_tool");
    assert_eq!(qa.by_tool[0].failures, 1);

    // 4) Cold CountingStore disjoint Tag 0x06 pruning check: 0 non-tail reads on Seg 1 & Seg 2
    let cold = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
        .await
        .unwrap();
    let man = ManifestStore::new(pid, obj_store.clone(), 0)
        .unwrap()
        .load_latest()
        .await
        .unwrap();
    let seg_by_path: HashMap<String, _> = man
        .segments
        .iter()
        .map(|s| (s.object_path.clone(), s.clone()))
        .collect();

    counting.reset();
    let dis_res = cold
        .list_traces(
            pid,
            &TraceListFilter {
                agent_name: Some("disjoint_agent".to_string()),
                status: Some("ok".to_string()),
                ..Default::default()
            },
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(dis_res.traces.len(), 1);
    assert_eq!(dis_res.traces[0].trace_id, "tr_disjoint");

    let mut non_tail_segs = std::collections::HashSet::new();
    for (path, range) in counting.recorded_ranges() {
        if let Some(seg) = seg_by_path.get(&path) {
            if range.start < seg.tail_offset {
                non_tail_segs.insert(path);
            }
        }
    }
    assert_eq!(
        non_tail_segs.len(),
        1,
        "pruned disjoint segments must incur 0 non-tail reads, got {non_tail_segs:?}"
    );

    writer.close().await.unwrap();
    reader.close().await.unwrap();
    cold.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// 22–25. Iteration 3 Regression Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn regression_unforced_cross_date_and_l0_partial_erasure_no_zombie() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut cfg = test_store_config(false, 0);
    cfg.l0_shards = 1;
    let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
    let ts = 1_700_000_000_000i64;

    // A) 2-segment unforced layout [seg_sp2_and_keep, seg_hdr] (probe_3e)
    {
        let pid = "proj_r3_3e";
        let sp_2 = make_span("tr_split", "sp_2", Some("sp_1"), ts + 50);
        let mut tr_keep = make_trace(
            "tr_keep",
            "planner",
            "1.0",
            Some("sess_keep"),
            Some("u_keep"),
            ts + 60,
            TraceStatus::Ok,
        );
        tr_keep.span_count = 1;
        let sp_keep = make_span("tr_keep", "sp_k", None, ts + 60);
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr_keep],
                    spans: vec![sp_2, sp_keep],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let mut hdr_tr = make_trace(
            "tr_split",
            "planner",
            "1.0",
            Some("sess_bye"),
            Some("u_bye"),
            ts,
            TraceStatus::Running,
        );
        hdr_tr.span_count = 1;
        let sp_1 = make_span("tr_split", "sp_1", None, ts);
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![hdr_tr],
                    spans: vec![sp_1],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let tb = AgentErasureTombstone {
            id: "tb_3e".to_string(),
            project_id: pid.to_string(),
            level: ErasureLevel::Session,
            target_id: "sess_bye".to_string(),
            created_at: ts,
            expires_at: None,
        };
        store.compact_and_gc(Some(pid), &[tb]).await.unwrap();

        let after = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(after.traces.len(), 1, "got {:?}", after.traces);
        assert_eq!(after.traces[0].trace_id, "tr_keep");
        assert!(store
            .get_trace_detail(pid, "tr_split", &[])
            .await
            .unwrap()
            .is_none());
    }

    // B) Cross-date earlier-plan span-only partial trace (probe_3f) for both Session and User
    for (pid, level) in [
        ("proj_r3_3f_sess", ErasureLevel::Session),
        ("proj_r3_3f_user", ErasureLevel::User),
    ] {
        let day1_ts = ts;
        let day2_ts = ts + 86_400_000;
        let sp_2 = make_span("tr_cross", "sp_2", Some("sp_1"), day1_ts + 50);
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![sp_2],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let hdr = make_trace(
            "tr_cross",
            "planner",
            "1.0",
            Some("sess_bye"),
            Some("u_bye"),
            day2_ts,
            TraceStatus::Ok,
        );
        let sp_1 = make_span("tr_cross", "sp_1", None, day2_ts);
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![hdr],
                    spans: vec![sp_1],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let tb = AgentErasureTombstone {
            id: format!("tb_{pid}"),
            project_id: pid.to_string(),
            level,
            target_id: match level {
                ErasureLevel::Session => "sess_bye".to_string(),
                ErasureLevel::User => "u_bye".to_string(),
                ErasureLevel::Trace => unreachable!(),
            },
            created_at: day2_ts,
            expires_at: None,
        };
        store.compact_and_gc(Some(pid), &[tb]).await.unwrap();

        let after = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert!(
            after.traces.is_empty(),
            "cross-date span-only zombie leaked for {pid}: {:?}",
            after.traces
        );
        assert!(store
            .get_trace_detail(pid, "tr_cross", &[])
            .await
            .unwrap()
            .is_none());
    }

    // C) Unflushed L0 span-only partial trace after .plss header erased (probe_3g)
    {
        let pid = "proj_r3_3g";
        store
            .ingest_batch(
                pid,
                make_batch("tr_l0p", Some("sess_bye"), Some("u_bye"), ts),
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let sp_l0 = make_span("tr_l0p", "sp_l0", None, ts + 20);
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![sp_l0],
                },
            )
            .await
            .unwrap();

        let tb = AgentErasureTombstone {
            id: "tb_3g".to_string(),
            project_id: pid.to_string(),
            level: ErasureLevel::Session,
            target_id: "sess_bye".to_string(),
            created_at: ts,
            expires_at: None,
        };
        store.compact_and_gc(Some(pid), &[tb]).await.unwrap();

        let after = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert!(
            after.traces.is_empty(),
            "L0 span-only entry survived header erasure: {:?}",
            after.traces
        );
        assert!(store
            .get_trace_detail(pid, "tr_l0p", &[])
            .await
            .unwrap()
            .is_none());
    }

    // D) Flushed span-only partial trace + unflushed L0 header
    {
        let pid = "proj_r3_l0_hdr";
        let sp_seg = make_span("tr_l0h", "sp_seg", None, ts);
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![sp_seg],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        store
            .ingest_batch(
                pid,
                make_batch("tr_l0h", Some("sess_bye"), Some("u_bye"), ts + 10),
            )
            .await
            .unwrap();

        let tb = AgentErasureTombstone {
            id: "tb_l0h".to_string(),
            project_id: pid.to_string(),
            level: ErasureLevel::User,
            target_id: "u_bye".to_string(),
            created_at: ts,
            expires_at: None,
        };
        store.compact_and_gc(Some(pid), &[tb]).await.unwrap();

        let after = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert!(
            after.traces.is_empty(),
            "flushed span-only zombie survived L0 header erasure: {:?}",
            after.traces
        );
    }

    store.close().await.unwrap();
}

#[tokio::test]
async fn regression_get_session_detail_reverse_user_tombstone_consistency() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut cfg = test_store_config(false, 0);
    cfg.l0_shards = 1;
    let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
    let ts = 1_700_000_000_000i64;
    let pid = "proj_r3_1b";

    // sess_rev: tr_r1 (u_erased) in Seg 1, tr_r2 (user=None) in Seg 2, tr_r3 (user=None) in L0
    store
        .ingest_batch(
            pid,
            make_batch("tr_r1", Some("sess_rev"), Some("u_erased"), ts),
        )
        .await
        .unwrap();
    store.flush_all_l0().await.unwrap();

    store
        .ingest_batch(pid, make_batch("tr_r2", Some("sess_rev"), None, ts + 100))
        .await
        .unwrap();
    store.flush_all_l0().await.unwrap();

    store
        .ingest_batch(pid, make_batch("tr_r3", Some("sess_rev"), None, ts + 200))
        .await
        .unwrap();

    // sess_trace_only: tr_t1 (Trace-erased) + tr_t2 (kept)
    store
        .ingest_batch(
            pid,
            make_batch("tr_t1", Some("sess_tr"), Some("u_keep"), ts + 300),
        )
        .await
        .unwrap();
    store
        .ingest_batch(
            pid,
            make_batch("tr_t2", Some("sess_tr"), Some("u_keep"), ts + 400),
        )
        .await
        .unwrap();

    let tbs = vec![
        AgentErasureTombstone {
            id: "tb_u".to_string(),
            project_id: pid.to_string(),
            level: ErasureLevel::User,
            target_id: "u_erased".to_string(),
            created_at: ts,
            expires_at: None,
        },
        AgentErasureTombstone {
            id: "tb_tr".to_string(),
            project_id: pid.to_string(),
            level: ErasureLevel::Trace,
            target_id: "tr_t1".to_string(),
            created_at: ts,
            expires_at: None,
        },
    ];

    let listed = store
        .list_sessions(pid, &SessionListFilter::default(), &tbs)
        .await
        .unwrap();
    assert_eq!(listed.sessions.len(), 1);
    assert_eq!(listed.sessions[0].session_id, "sess_tr");
    assert_eq!(listed.sessions[0].trace_count, 1);

    assert!(
        store
            .get_session_detail(pid, "sess_rev", &tbs)
            .await
            .unwrap()
            .is_none(),
        "get_session_detail must hide sess_rev consistently with list_sessions"
    );

    let tr_only = store
        .get_session_detail(pid, "sess_tr", &tbs)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tr_only.summary.trace_count, 1);
    assert_eq!(tr_only.traces[0].trace_id, "tr_t2");

    store.close().await.unwrap();
}

#[tokio::test]
async fn regression_session_endpoints_include_flushed_sessionless_segments() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut cfg = test_store_config(false, 0);
    cfg.l0_shards = 1;
    let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
    let ts = 1_700_000_000_000i64;

    // Part A (probe_1c): sessionless Seg 2 carries user_id = Some("u_erased")
    {
        let pid = "proj_r3_1c";
        store
            .ingest_batch(pid, make_batch("tr_1", Some("sess_p1c"), None, ts))
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let tr_upd = make_trace(
            "tr_1",
            "ag1",
            "1.0",
            None,
            Some("u_erased"),
            ts + 10,
            TraceStatus::Ok,
        );
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr_upd],
                    spans: vec![],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let tb_user = AgentErasureTombstone {
            id: "tb_1c".to_string(),
            project_id: pid.to_string(),
            level: ErasureLevel::User,
            target_id: "u_erased".to_string(),
            created_at: ts,
            expires_at: None,
        };

        let listed = store
            .list_sessions(
                pid,
                &SessionListFilter::default(),
                std::slice::from_ref(&tb_user),
            )
            .await
            .unwrap();
        assert!(
            listed.sessions.is_empty(),
            "list_sessions leaked user_id from sessionless segment: {:?}",
            listed.sessions
        );
        assert!(
            store
                .get_session_detail(pid, "sess_p1c", &[tb_user])
                .await
                .unwrap()
                .is_none(),
            "get_session_detail leaked user_id from sessionless segment"
        );
    }

    // Part B (probe_3b): flushed span-only Seg 2 carries Error + tokens
    {
        let pid = "proj_r3_3b";
        let mut tr = make_trace(
            "tr_s",
            "planner",
            "1.0",
            Some("sess_fl"),
            Some("u1"),
            ts,
            TraceStatus::Running,
        );
        tr.total_tokens = 100;
        tr.total_cost_nanos = 1_000_000;
        let mut sp1 = make_span("tr_s", "sp_1", None, ts);
        sp1.span_phase = SpanPhase::Start;
        sp1.status = TraceStatus::Running;
        sp1.input_tokens = 80;
        sp1.output_tokens = 20;
        sp1.total_tokens = 100;
        sp1.cost_nanos = 1_000_000;
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr],
                    spans: vec![sp1],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let mut sp2 = make_span("tr_s", "sp_2", Some("sp_1"), ts + 100);
        sp2.status = TraceStatus::Error;
        sp2.input_tokens = 200;
        sp2.output_tokens = 50;
        sp2.total_tokens = 250;
        sp2.cost_nanos = 2_500_000;
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![sp2],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let listed = store
            .list_sessions(pid, &SessionListFilter::default(), &[])
            .await
            .unwrap();
        assert_eq!(listed.sessions.len(), 1);
        assert_eq!(listed.sessions[0].error_count, 1);
        assert_eq!(listed.sessions[0].total_tokens, 350);
        assert_eq!(listed.sessions[0].total_cost_nanos, 3_500_000);

        let detail = store
            .get_session_detail(pid, "sess_fl", &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(detail.summary.error_count, 1);
        assert_eq!(detail.summary.total_tokens, 350);
        assert_eq!(detail.summary.total_cost_nanos, 3_500_000);
        assert_eq!(detail.traces[0].status, TraceStatus::Error);
    }

    store.close().await.unwrap();
}

#[tokio::test]
async fn regression_combined_predicate_split_trace_and_counting_store() {
    let raw_mem = Arc::new(InMemory::new());
    let counting = CountingStore::new(raw_mem);
    let obj_store: Arc<dyn ObjectStore> = counting.clone();

    let mut cfg = test_store_config(false, 0);
    cfg.l0_shards = 1;
    let writer = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();
    let ts = 1_700_000_000_000i64;
    let pid = "proj_r3_4b";

    // Seg 1: tr_cross (model = gpt-4o, status = Ok, cost = $0.001)
    let mut tr1 = make_trace(
        "tr_cross",
        "planner",
        "1.0",
        Some("sess_c"),
        Some("u1"),
        ts,
        TraceStatus::Ok,
    );
    tr1.primary_model = Some("gpt-4o".to_string());
    tr1.total_tokens = 100;
    tr1.total_cost_nanos = 1_000_000;
    let mut sp1 = make_span("tr_cross", "sp_1", None, ts);
    sp1.span_type = SpanType::Llm;
    sp1.model = Some("gpt-4o".to_string());
    sp1.input_tokens = 80;
    sp1.output_tokens = 20;
    sp1.total_tokens = 100;
    sp1.cost_nanos = 1_000_000;
    writer
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr1],
                spans: vec![sp1],
            },
        )
        .await
        .unwrap();
    writer.flush_all_l0().await.unwrap();

    // Seg 2: tr_cross (tool_name = bad_tool, model = None, status = Error, cost = $0.002)
    let mut tr2 = make_trace(
        "tr_cross",
        "planner",
        "1.0",
        Some("sess_c"),
        Some("u1"),
        ts + 50,
        TraceStatus::Error,
    );
    tr2.primary_model = None;
    tr2.primary_provider = None;
    tr2.total_tokens = 200;
    tr2.total_cost_nanos = 2_000_000;
    let mut sp2 = make_span("tr_cross", "sp_2", Some("sp_1"), ts + 50);
    sp2.span_type = SpanType::Tool;
    sp2.status = TraceStatus::Error;
    sp2.model = None;
    sp2.provider = None;
    sp2.tool_name = Some("bad_tool".to_string());
    sp2.input_tokens = 150;
    sp2.output_tokens = 50;
    sp2.total_tokens = 200;
    sp2.cost_nanos = 2_000_000;
    writer
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr2],
                spans: vec![sp2],
            },
        )
        .await
        .unwrap();
    writer.flush_all_l0().await.unwrap();

    // Seg 3: disjoint tr_disjoint (claude-sonnet-4, good_tool, Ok, cost = $0.000)
    let mut tr3 = make_trace(
        "tr_disjoint",
        "disjoint_agent",
        "1.0",
        Some("sess_d"),
        Some("u2"),
        ts + 1_000,
        TraceStatus::Ok,
    );
    tr3.primary_model = Some("claude-sonnet-4".to_string());
    tr3.total_tokens = 0;
    tr3.total_cost_nanos = 0;
    tr3.total_cost_usd = 0.0;
    let mut sp3 = make_span("tr_disjoint", "sp_3", None, ts + 1_000);
    sp3.span_type = SpanType::Tool;
    sp3.model = Some("claude-sonnet-4".to_string());
    sp3.tool_name = Some("good_tool".to_string());
    sp3.input_tokens = 0;
    sp3.output_tokens = 0;
    sp3.total_tokens = 0;
    sp3.cost_nanos = 0;
    sp3.cost_usd = 0.0;
    writer
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr3],
                spans: vec![sp3],
            },
        )
        .await
        .unwrap();
    writer.flush_all_l0().await.unwrap();

    let man = ManifestStore::new(pid, obj_store.clone(), 0)
        .unwrap()
        .load_latest()
        .await
        .unwrap();
    assert_eq!(man.segments.len(), 3);
    let seg_by_path: HashMap<String, _> = man
        .segments
        .iter()
        .map(|s| (s.object_path.clone(), s.clone()))
        .collect();

    // 1) Cold list_traces(model="gpt-4o", status="error")
    let cold1 = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
        .await
        .unwrap();
    counting.reset();
    let res1 = cold1
        .list_traces(
            pid,
            &TraceListFilter {
                model: Some("gpt-4o".to_string()),
                status: Some("error".to_string()),
                ..Default::default()
            },
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(res1.traces.len(), 1);
    assert_eq!(res1.traces[0].trace_id, "tr_cross");
    assert_eq!(res1.traces[0].total_cost_nanos, 3_000_000);
    let non_tail_1: std::collections::HashSet<String> = counting
        .recorded_ranges()
        .into_iter()
        .filter(|(p, r)| seg_by_path.get(p).is_some_and(|s| r.start < s.tail_offset))
        .map(|(p, _)| p)
        .collect();
    assert_eq!(
        non_tail_1.len(),
        2,
        "Seg 3 (claude-sonnet-4, Ok) must be pruned with 0 non-tail reads, got {non_tail_1:?}"
    );

    // Verify 0 Tag 0x04 / Tag 0x05 reads in list_traces
    let plss_reader = pulse_agent::store::segment::PlssReader::new(
        obj_store.clone(),
        pulse_agent::store::cache::AgentSegmentCache::open(None, 1024 * 1024, None, 0)
            .await
            .unwrap(),
    );
    for path in &non_tail_1 {
        let seg = &seg_by_path[path];
        let tail = plss_reader.read_eof_tail(seg).await.unwrap();
        let sec04 = tail.find_section(SectionTag::SpanTree).unwrap();
        let sec05 = tail.find_section(SectionTag::PayloadPack).unwrap();
        for (p, r) in counting.recorded_ranges() {
            if &p == path {
                assert!(!(r.start < sec04.offset + sec04.len as u64 && r.end > sec04.offset));
                assert!(!(r.start < sec05.offset + sec05.len as u64 && r.end > sec05.offset));
            }
        }
    }

    // 2) Cold list_traces(min_cost_usd = 0.0025)
    let cold2 = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
        .await
        .unwrap();
    counting.reset();
    let res2 = cold2
        .list_traces(
            pid,
            &TraceListFilter {
                min_cost_usd: Some(0.0025),
                ..Default::default()
            },
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(res2.traces.len(), 1);
    assert_eq!(res2.traces[0].trace_id, "tr_cross");
    let non_tail_2: std::collections::HashSet<String> = counting
        .recorded_ranges()
        .into_iter()
        .filter(|(p, r)| seg_by_path.get(p).is_some_and(|s| r.start < s.tail_offset))
        .map(|(p, _)| p)
        .collect();
    assert_eq!(
        non_tail_2.len(),
        2,
        "Zero-cost Seg 3 must be pruned with 0 non-tail reads, got {non_tail_2:?}"
    );

    // 3) Cold query_analytics(model="gpt-4o", tool_name="bad_tool")
    let cold3 = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
        .await
        .unwrap();
    counting.reset();
    let qa = cold3
        .query_analytics(
            pid,
            &AnalyticsFilter {
                model: Some("gpt-4o".to_string()),
                tool_name: Some("bad_tool".to_string()),
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();
    assert_eq!(qa.total_executions, 1);
    assert_eq!(qa.failed_executions, 1);
    assert_eq!(qa.by_tool.len(), 1);
    assert_eq!(qa.by_tool[0].tool_name, "bad_tool");
    let non_tail_3: std::collections::HashSet<String> = counting
        .recorded_ranges()
        .into_iter()
        .filter(|(p, r)| seg_by_path.get(p).is_some_and(|s| r.start < s.tail_offset))
        .map(|(p, _)| p)
        .collect();
    assert_eq!(
        non_tail_3.len(),
        2,
        "Seg 3 (claude-sonnet-4, good_tool) must be pruned with 0 non-tail reads, got {non_tail_3:?}"
    );

    writer.close().await.unwrap();
    cold1.close().await.unwrap();
    cold2.close().await.unwrap();
    cold3.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// 26–29. Iteration 4 Regression Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn regression_r4_uncommitted_wal_erasure_query_node_and_reopen() {
    let ts = 1_700_000_000_000i64;

    // 1. probe_3h_a Variant A1: `--role query` opened and warmed BEFORE compact_and_gc
    {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 900);
        cfg.l0_shards = 1;
        let writer = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();
        let query_node = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 900))
            .await
            .unwrap();
        let pid = "proj_r4_3h_a1";

        writer
            .ingest_batch(
                pid,
                make_batch("tr_l0p", Some("sess_bye"), Some("u_bye"), ts),
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();

        // Uncommitted WAL seq 2: span-only for tr_l0p + surviving tr_keep (shared WAL batch)
        let sp_l0 = make_span("tr_l0p", "sp_l0", None, ts + 20);
        let mut batch_keep = make_batch("tr_keep", Some("sess_keep"), Some("u_keep"), ts + 30);
        batch_keep.spans.push(sp_l0);
        writer.ingest_batch(pid, batch_keep).await.unwrap();

        // Warm query_node BEFORE compaction so query_node.memtable holds seq 2 tr_l0p
        let pre = query_node
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(pre.traces.len(), 2);

        let tb_sess = AgentErasureTombstone {
            id: "tb_a1".to_string(),
            project_id: pid.to_string(),
            level: ErasureLevel::Session,
            target_id: "sess_bye".to_string(),
            created_at: ts,
            expires_at: None,
        };
        writer
            .compact_and_gc(Some(pid), std::slice::from_ref(&tb_sess))
            .await
            .unwrap();

        for tbs in [vec![tb_sess.clone()], Vec::new()] {
            let listed = query_node
                .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &tbs)
                .await
                .unwrap();
            assert_eq!(
                listed.traces.len(),
                1,
                "query_node leaked tr_l0p in A1: {:?}",
                listed.traces
            );
            assert_eq!(listed.traces[0].trace_id, "tr_keep");
            assert!(query_node
                .get_trace_detail(pid, "tr_l0p", &tbs)
                .await
                .unwrap()
                .is_none());
            let sess = query_node
                .list_sessions(pid, &SessionListFilter::default(), &tbs)
                .await
                .unwrap();
            assert_eq!(sess.sessions.len(), 1);
            assert_eq!(sess.sessions[0].session_id, "sess_keep");
            assert!(query_node
                .get_session_detail(pid, "sess_bye", &tbs)
                .await
                .unwrap()
                .is_none());
            let qa = query_node
                .query_analytics(pid, &AnalyticsFilter::default(), &tbs)
                .await
                .unwrap();
            assert_eq!(qa.total_executions, 1);
            let cmp = query_node
                .compare_groups(pid, &CompareFilter::default(), &tbs)
                .await
                .unwrap();
            assert_eq!(cmp.baseline_sample_size, 1);
        }

        query_node.close().await.unwrap();
        writer.close().await.unwrap();
    }

    // 2. probe_3h_a Variant A2: fresh `--role query` opened AFTER compact_and_gc with shared seq 2
    {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 900);
        cfg.l0_shards = 1;
        let writer = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();
        let pid = "proj_r4_3h_a2";

        writer
            .ingest_batch(
                pid,
                make_batch("tr_l0p", Some("sess_bye"), Some("u_bye"), ts),
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();

        let sp_l0 = make_span("tr_l0p", "sp_l0", None, ts + 20);
        let mut batch_keep = make_batch("tr_keep", Some("sess_keep"), Some("u_keep"), ts + 30);
        batch_keep.spans.push(sp_l0);
        writer.ingest_batch(pid, batch_keep).await.unwrap();

        let tb_sess = AgentErasureTombstone {
            id: "tb_a2".to_string(),
            project_id: pid.to_string(),
            level: ErasureLevel::Session,
            target_id: "sess_bye".to_string(),
            created_at: ts,
            expires_at: None,
        };
        writer
            .compact_and_gc(Some(pid), std::slice::from_ref(&tb_sess))
            .await
            .unwrap();

        // seq 2 still on ObjectStore (because tr_keep is unflushed)
        let wal_prefix = ObjectPath::from(format!("agent_wal/{pid}/"));
        let wal_objs: Vec<_> = obj_store
            .list(Some(&wal_prefix))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(wal_objs.len(), 1);

        let query_node = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 900))
            .await
            .unwrap();
        for tbs in [vec![tb_sess.clone()], Vec::new()] {
            let listed = query_node
                .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &tbs)
                .await
                .unwrap();
            assert_eq!(listed.traces.len(), 1);
            assert_eq!(listed.traces[0].trace_id, "tr_keep");
            assert!(query_node
                .get_trace_detail(pid, "tr_l0p", &tbs)
                .await
                .unwrap()
                .is_none());
        }
        query_node.close().await.unwrap();
        writer.close().await.unwrap();
    }

    // 3. probe_3h_b: clean close() + open() (single-trace seq 2 AND shared seq 2)
    {
        // B1: single-trace seq 2 -> WAL purged + reopen sees 0 traces
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 900);
        cfg.l0_shards = 1;
        let writer = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();
        let pid = "proj_r4_3h_b1";

        writer
            .ingest_batch(
                pid,
                make_batch("tr_l0p", Some("sess_bye"), Some("u_bye"), ts),
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();
        writer
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![make_span("tr_l0p", "sp_l0", None, ts + 20)],
                },
            )
            .await
            .unwrap();

        let tb_sess = AgentErasureTombstone {
            id: "tb_b1".to_string(),
            project_id: pid.to_string(),
            level: ErasureLevel::Session,
            target_id: "sess_bye".to_string(),
            created_at: ts,
            expires_at: None,
        };
        writer.compact_and_gc(Some(pid), &[tb_sess]).await.unwrap();
        writer.close().await.unwrap();

        let wal_prefix = ObjectPath::from(format!("agent_wal/{pid}/"));
        let wal_objs: Vec<_> = obj_store
            .list(Some(&wal_prefix))
            .try_collect()
            .await
            .unwrap();
        assert!(
            wal_objs.is_empty(),
            "single-trace erased WAL seq 2 must be purged: {wal_objs:?}"
        );

        let reopened = AgentTraceStore::open(obj_store.clone(), test_store_config(false, 900))
            .await
            .unwrap();
        let after = reopened
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert!(
            after.traces.is_empty(),
            "zombie resurrected: {:?}",
            after.traces
        );
        assert!(reopened
            .get_trace_detail(pid, "tr_l0p", &[])
            .await
            .unwrap()
            .is_none());
        reopened.close().await.unwrap();
    }

    {
        // B2: shared seq 2 (sp_l0 + tr_keep) -> close() + open() recovers tr_keep without tr_l0p
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 900);
        cfg.l0_shards = 1;
        let writer = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();
        let pid = "proj_r4_3h_b2";

        writer
            .ingest_batch(
                pid,
                make_batch("tr_l0p", Some("sess_bye"), Some("u_bye"), ts),
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();
        let mut batch_keep = make_batch("tr_keep", Some("sess_keep"), Some("u_keep"), ts + 30);
        batch_keep
            .spans
            .push(make_span("tr_l0p", "sp_l0", None, ts + 20));
        writer.ingest_batch(pid, batch_keep).await.unwrap();

        let tb_sess = AgentErasureTombstone {
            id: "tb_b2".to_string(),
            project_id: pid.to_string(),
            level: ErasureLevel::Session,
            target_id: "sess_bye".to_string(),
            created_at: ts,
            expires_at: None,
        };
        writer.compact_and_gc(Some(pid), &[tb_sess]).await.unwrap();
        writer.close().await.unwrap();

        let reopened = AgentTraceStore::open(obj_store.clone(), test_store_config(false, 900))
            .await
            .unwrap();
        let after = reopened
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(after.traces.len(), 1);
        assert_eq!(after.traces[0].trace_id, "tr_keep");
        assert!(reopened
            .get_trace_detail(pid, "tr_l0p", &[])
            .await
            .unwrap()
            .is_none());
        reopened.close().await.unwrap();
    }

    // 4. L0-only erasure (zero .plss segments) + close() + open() + query_node
    {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 900);
        cfg.l0_shards = 1;
        let writer = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();
        let pid = "proj_r4_l0_only";

        writer
            .ingest_batch(
                pid,
                make_batch("tr_a_victim", Some("sess_v"), Some("u_erase"), ts),
            )
            .await
            .unwrap();
        writer
            .ingest_batch(
                pid,
                make_batch("tr_shared", Some("sess_s"), Some("u_keep"), ts + 10),
            )
            .await
            .unwrap();

        let tb_user = AgentErasureTombstone {
            id: "tb_l0_u".to_string(),
            project_id: pid.to_string(),
            level: ErasureLevel::User,
            target_id: "u_erase".to_string(),
            created_at: ts,
            expires_at: None,
        };
        writer.compact_and_gc(Some(pid), &[tb_user]).await.unwrap();

        let q = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 900))
            .await
            .unwrap();
        let q_res = q
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(q_res.traces.len(), 1);
        assert_eq!(q_res.traces[0].trace_id, "tr_shared");
        q.close().await.unwrap();
        writer.close().await.unwrap();

        let reopened = AgentTraceStore::open(obj_store.clone(), test_store_config(false, 900))
            .await
            .unwrap();
        let r_res = reopened
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(r_res.traces.len(), 1);
        assert_eq!(r_res.traces[0].trace_id, "tr_shared");
        assert!(reopened
            .get_trace_detail(pid, "tr_a_victim", &[])
            .await
            .unwrap()
            .is_none());
        reopened.close().await.unwrap();
    }
}

#[tokio::test]
async fn regression_r4_multi_writer_ingest_compactor_and_erased_traces_gc() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ts = 1_700_000_000_000i64;
    let pid = "proj_r4_mw_gc";

    let mut cfg_ingest = test_store_config(false, 0);
    cfg_ingest.l0_shards = 1;
    let ingest_store = AgentTraceStore::open(obj_store.clone(), cfg_ingest)
        .await
        .unwrap();

    // Ingest seg_1 with tr_l0p + tr_keep
    ingest_store
        .ingest_batch(
            pid,
            make_batch("tr_l0p", Some("sess_bye"), Some("u_bye"), ts),
        )
        .await
        .unwrap();
    ingest_store
        .ingest_batch(
            pid,
            make_batch("tr_keep", Some("sess_keep"), Some("u_keep"), ts + 10),
        )
        .await
        .unwrap();
    ingest_store.flush_all_l0().await.unwrap();

    // Ingest uncommitted seq 2 with sp_l0 for tr_l0p
    ingest_store
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![],
                spans: vec![make_span("tr_l0p", "sp_l0", None, ts + 20)],
            },
        )
        .await
        .unwrap();

    // Separate compactor node runs compact_and_gc
    let mut cfg_comp = test_store_config(false, 0);
    cfg_comp.l0_shards = 1;
    let compactor_store = AgentTraceStore::open(obj_store.clone(), cfg_comp)
        .await
        .unwrap();

    let tb_sess = AgentErasureTombstone {
        id: "tb_mw".to_string(),
        project_id: pid.to_string(),
        level: ErasureLevel::Session,
        target_id: "sess_bye".to_string(),
        created_at: ts,
        expires_at: None,
    };
    compactor_store
        .compact_and_gc(Some(pid), &[tb_sess])
        .await
        .unwrap();

    let ms = ManifestStore::new(pid, obj_store.clone(), 0).unwrap();
    let m1 = ms.load_latest().await.unwrap();
    assert!(
        m1.erased_traces.contains_key("tr_l0p"),
        "manifest.erased_traces must persist tr_l0p: {:?}",
        m1.erased_traces
    );

    // ingest_store flushes L0 -> must NOT write tr_l0p to any new .plss segment, and must purge seq 2
    ingest_store.flush_all_l0().await.unwrap();
    let wal_prefix = ObjectPath::from(format!("agent_wal/{pid}/"));
    let wal_objs: Vec<_> = obj_store
        .list(Some(&wal_prefix))
        .try_collect()
        .await
        .unwrap();
    assert!(wal_objs.is_empty(), "seq 2 should be purged: {wal_objs:?}");

    for st in [&ingest_store, &compactor_store] {
        let listed = st
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(listed.traces.len(), 1);
        assert_eq!(listed.traces[0].trace_id, "tr_keep");
    }

    // Subsequent compact_and_gc with gc_grace_secs=0 and no remaining segment/WAL hits GCs erased_traces
    compactor_store
        .compact_and_gc(Some(pid), &[])
        .await
        .unwrap();
    let m2 = ms.load_latest().await.unwrap();
    assert!(
        m2.erased_traces.is_empty(),
        "manifest.erased_traces should be GC'd once grace elapsed and no segment/WAL contains tr_l0p: {:?}",
        m2.erased_traces
    );

    ingest_store.close().await.unwrap();
    compactor_store.close().await.unwrap();

    let cold = AgentTraceStore::open(obj_store, test_store_config(true, 0))
        .await
        .unwrap();
    let final_res = cold
        .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(final_res.traces.len(), 1);
    assert_eq!(final_res.traces[0].trace_id, "tr_keep");
    cold.close().await.unwrap();
}

#[tokio::test]
async fn regression_r4_sound_pass1_multi_segment_pruning_and_session_tiebreak() {
    let ts = 1_700_000_000_000i64;

    async fn assert_no_tag04_05_and_pruned_disjoint(
        counting: &CountingStore,
        obj_store: &Arc<dyn ObjectStore>,
        pid: &str,
        expected_non_tail_count: usize,
        disjoint_seg_path: &str,
    ) {
        let man = ManifestStore::new(pid, obj_store.clone(), 0)
            .unwrap()
            .load_latest()
            .await
            .unwrap();
        let seg_by_path: HashMap<String, _> = man
            .segments
            .iter()
            .map(|s| (s.object_path.clone(), s.clone()))
            .collect();
        let plss_reader = pulse_agent::store::segment::PlssReader::new(
            obj_store.clone(),
            pulse_agent::store::cache::AgentSegmentCache::open(None, 1024 * 1024, None, 0)
                .await
                .unwrap(),
        );
        let ranges = counting.recorded_ranges();
        let mut non_tail_paths = std::collections::HashSet::new();
        for (p, r) in &ranges {
            if let Some(seg) = seg_by_path.get(p) {
                if r.start < seg.tail_offset {
                    non_tail_paths.insert(p.clone());
                }
                let tail = plss_reader.read_eof_tail(seg).await.unwrap();
                let sec04 = tail.find_section(SectionTag::SpanTree).unwrap();
                let sec05 = tail.find_section(SectionTag::PayloadPack).unwrap();
                assert!(!(r.start < sec04.offset + sec04.len as u64 && r.end > sec04.offset));
                assert!(!(r.start < sec05.offset + sec05.len as u64 && r.end > sec05.offset));
            }
        }
        assert!(
            !non_tail_paths.contains(disjoint_seg_path),
            "disjoint segment {disjoint_seg_path} had non-tail reads: {non_tail_paths:?}"
        );
        assert_eq!(
            non_tail_paths.len(),
            expected_non_tail_count,
            "expected {expected_non_tail_count} non-tail segments, got {non_tail_paths:?}"
        );
    }

    // 1. probe_6a / Finding 3: split session_id vs user_id + parent_trace_id
    {
        let raw_mem = Arc::new(InMemory::new());
        let counting = CountingStore::new(raw_mem);
        let obj_store: Arc<dyn ObjectStore> = counting.clone();
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let writer = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();
        let pid = "proj_r4_6a";

        let mut tr1 = make_trace("tr_a", "ag", "1.0", Some("s1"), None, ts, TraceStatus::Ok);
        tr1.parent_trace_id = None;
        writer
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr1],
                    spans: vec![make_span("tr_a", "sp1", None, ts)],
                },
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();

        let mut tr2 = make_trace(
            "tr_a",
            "ag",
            "1.0",
            None,
            Some("u1"),
            ts + 10,
            TraceStatus::Ok,
        );
        tr2.parent_trace_id = Some("p1".to_string());
        writer
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr2],
                    spans: vec![make_span("tr_a", "sp2", None, ts + 10)],
                },
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();

        let tr3 = make_trace(
            "tr_dis",
            "ag",
            "1.0",
            Some("s2"),
            Some("u1"),
            ts + 20,
            TraceStatus::Ok,
        );
        writer
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr3],
                    spans: vec![make_span("tr_dis", "sp3", None, ts + 20)],
                },
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();
        let seg3_path = ManifestStore::new(pid, obj_store.clone(), 0)
            .unwrap()
            .load_latest()
            .await
            .unwrap()
            .segments[2]
            .object_path
            .clone();

        let cold = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
            .await
            .unwrap();
        counting.reset();
        let r1 = cold
            .list_traces(
                pid,
                &TraceListFilter {
                    session_id: Some("s1".to_string()),
                    user_id: Some("u1".to_string()),
                    ..Default::default()
                },
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(r1.traces.len(), 1);
        assert_eq!(r1.traces[0].trace_id, "tr_a");
        assert_no_tag04_05_and_pruned_disjoint(&counting, &obj_store, pid, 2, &seg3_path).await;

        let cold2 = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
            .await
            .unwrap();
        counting.reset();
        let r2 = cold2
            .list_traces(
                pid,
                &TraceListFilter {
                    session_id: Some("s1".to_string()),
                    parent_trace_id: Some("p1".to_string()),
                    ..Default::default()
                },
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(r2.traces.len(), 1);
        assert_no_tag04_05_and_pruned_disjoint(&counting, &obj_store, pid, 2, &seg3_path).await;

        let qa = cold2
            .query_analytics(
                pid,
                &AnalyticsFilter {
                    session_id: Some("s1".to_string()),
                    user_id: Some("u1".to_string()),
                    ..Default::default()
                },
                &[],
            )
            .await
            .unwrap();
        assert_eq!(qa.total_executions, 1);

        writer.close().await.unwrap();
        cold.close().await.unwrap();
        cold2.close().await.unwrap();
    }

    // 2. probe_6b / Finding 3 variant: environment="production" in Seg 1 + "staging" in Seg 2
    {
        let raw_mem = Arc::new(InMemory::new());
        let counting = CountingStore::new(raw_mem);
        let obj_store: Arc<dyn ObjectStore> = counting.clone();
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let writer = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();
        let pid = "proj_r4_6b";

        let mut tr1 = make_trace("tr_b", "ag1", "1.0", None, None, ts, TraceStatus::Ok);
        tr1.environment = "production".to_string();
        tr1.primary_model = Some("gpt-4o".to_string());
        writer
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr1],
                    spans: vec![make_span("tr_b", "sp1", None, ts)],
                },
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();

        let mut tr2 = make_trace(
            "tr_b",
            "ag1",
            "1.0",
            None,
            None,
            ts + 10,
            TraceStatus::Error,
        );
        tr2.environment = "staging".to_string();
        tr2.primary_model = None;
        let mut sp2 = make_span("tr_b", "sp2", None, ts + 10);
        sp2.model = None;
        sp2.status = TraceStatus::Error;
        writer
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr2],
                    spans: vec![sp2],
                },
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();

        let mut tr3 = make_trace("tr_d", "ag2", "1.0", None, None, ts + 20, TraceStatus::Ok);
        tr3.environment = "production".to_string();
        tr3.primary_model = Some("claude-sonnet-4".to_string());
        let mut sp3 = make_span("tr_d", "sp3", None, ts + 20);
        sp3.model = Some("claude-sonnet-4".to_string());
        writer
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr3],
                    spans: vec![sp3],
                },
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();
        let seg3_path = ManifestStore::new(pid, obj_store.clone(), 0)
            .unwrap()
            .load_latest()
            .await
            .unwrap()
            .segments[2]
            .object_path
            .clone();

        let cold = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
            .await
            .unwrap();
        counting.reset();
        let r1 = cold
            .list_traces(
                pid,
                &TraceListFilter {
                    environment: Some("staging".to_string()),
                    model: Some("gpt-4o".to_string()),
                    ..Default::default()
                },
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(r1.traces.len(), 1);
        assert_no_tag04_05_and_pruned_disjoint(&counting, &obj_store, pid, 2, &seg3_path).await;

        let cold2 = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
            .await
            .unwrap();
        counting.reset();
        let r2 = cold2
            .list_traces(
                pid,
                &TraceListFilter {
                    environment: Some("staging".to_string()),
                    status: Some("error".to_string()),
                    ..Default::default()
                },
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(r2.traces.len(), 1);
        assert_no_tag04_05_and_pruned_disjoint(&counting, &obj_store, pid, 2, &seg3_path).await;

        writer.close().await.unwrap();
        cold.close().await.unwrap();
        cold2.close().await.unwrap();
    }

    // 3. probe_6c + probe_6g + Finding 2 + probe_6h (early span-only + late header + end_ts_ms + compare_groups)
    {
        let raw_mem = Arc::new(InMemory::new());
        let counting = CountingStore::new(raw_mem);
        let obj_store: Arc<dyn ObjectStore> = counting.clone();
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let writer = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();
        let pid = "proj_r4_6cg";

        // Seg 1: early span-only start_ts_ms=ts, status=Ok, cost=0
        let mut sp1 = make_span("tr_cg", "sp1", None, ts);
        sp1.model = None;
        sp1.cost_nanos = 0;
        sp1.cost_usd = 0.0;
        writer
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![sp1],
                },
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();

        // Seg 2: late header start_ts_ms=ts+5000, session_id="s1", env="staging", v1, gpt-4o, bad_tool, Error, $0.005
        let mut tr2 = make_trace(
            "tr_cg",
            "ag",
            "v1",
            Some("s1"),
            Some("u1"),
            ts + 5000,
            TraceStatus::Error,
        );
        tr2.environment = "staging".to_string();
        tr2.primary_model = Some("gpt-4o".to_string());
        tr2.total_cost_nanos = 5_000_000;
        let mut sp2 = make_span("tr_cg", "sp2", None, ts + 5000);
        sp2.span_type = SpanType::Tool;
        sp2.status = TraceStatus::Error;
        sp2.model = Some("gpt-4o".to_string());
        sp2.tool_name = Some("bad_tool".to_string());
        sp2.cost_nanos = 5_000_000;
        writer
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr2],
                    spans: vec![sp2],
                },
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();

        // Seg 3: disjoint at ts (inside end_ts_ms = ts+500!)
        let mut tr3 = make_trace(
            "tr_dis",
            "ag",
            "v1",
            Some("s2"),
            Some("u2"),
            ts,
            TraceStatus::Ok,
        );
        tr3.environment = "production".to_string();
        tr3.primary_model = Some("claude-sonnet-4".to_string());
        let mut sp3 = make_span("tr_dis", "sp3", None, ts);
        sp3.span_type = SpanType::Tool;
        sp3.model = Some("claude-sonnet-4".to_string());
        sp3.tool_name = Some("good_tool".to_string());
        writer
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr3],
                    spans: vec![sp3],
                },
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();
        let seg3_path = ManifestStore::new(pid, obj_store.clone(), 0)
            .unwrap()
            .load_latest()
            .await
            .unwrap()
            .segments[2]
            .object_path
            .clone();

        for (label, f) in [
            (
                "end+model",
                TraceListFilter {
                    end_ts_ms: Some(ts + 500),
                    model: Some("gpt-4o".to_string()),
                    ..Default::default()
                },
            ),
            (
                "end+sid",
                TraceListFilter {
                    end_ts_ms: Some(ts + 500),
                    session_id: Some("s1".to_string()),
                    ..Default::default()
                },
            ),
            (
                "end+status",
                TraceListFilter {
                    end_ts_ms: Some(ts + 500),
                    status: Some("error".to_string()),
                    ..Default::default()
                },
            ),
        ] {
            let cold = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
                .await
                .unwrap();
            counting.reset();
            let res = cold
                .list_traces(pid, &f, &HashMap::new(), &[])
                .await
                .unwrap();
            assert_eq!(res.traces.len(), 1, "failed case {label}");
            assert_eq!(res.traces[0].trace_id, "tr_cg");
            assert_no_tag04_05_and_pruned_disjoint(&counting, &obj_store, pid, 2, &seg3_path).await;
            cold.close().await.unwrap();
        }

        let cold_qa = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
            .await
            .unwrap();
        counting.reset();
        let qa = cold_qa
            .query_analytics(
                pid,
                &AnalyticsFilter {
                    end_ts_ms: Some(ts + 500),
                    tool_name: Some("bad_tool".to_string()),
                    ..Default::default()
                },
                &[],
            )
            .await
            .unwrap();
        assert_eq!(qa.total_executions, 1);
        assert_no_tag04_05_and_pruned_disjoint(&counting, &obj_store, pid, 2, &seg3_path).await;
        cold_qa.close().await.unwrap();

        // probe_6h: compare_groups bound_filter
        let cmp = writer
            .compare_groups(
                pid,
                &CompareFilter {
                    start_ts_ms: Some(ts - 100),
                    end_ts_ms: Some(ts + 500),
                    environment: Some("staging".to_string()),
                    baseline_agent_version: Some("v1".to_string()),
                    candidate_agent_version: Some("v2".to_string()),
                    ..Default::default()
                },
                &[],
            )
            .await
            .unwrap();
        assert_eq!(cmp.baseline_sample_size, 1);

        writer.close().await.unwrap();
    }

    // 4. Finding 1 (min_cost_usd + status / session_id) + Monotonic Bound Pruning
    {
        let raw_mem = Arc::new(InMemory::new());
        let counting = CountingStore::new(raw_mem);
        let obj_store: Arc<dyn ObjectStore> = counting.clone();
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let writer = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();
        let pid = "proj_r4_f1_mono";

        // Seg 1: cost=$0.005, status=Ok, session_id="s1", start=ts+2000, dur=500
        let mut tr1 = make_trace(
            "tr_f1",
            "ag",
            "1.0",
            Some("s1"),
            None,
            ts + 2000,
            TraceStatus::Ok,
        );
        tr1.duration_ms = 500;
        tr1.end_ts_ms = Some(ts + 2500);
        tr1.total_cost_nanos = 5_000_000;
        let mut sp1 = make_span("tr_f1", "sp1", None, ts + 2000);
        sp1.duration_ms = 500;
        sp1.end_ts_ms = Some(ts + 2500);
        sp1.cost_nanos = 5_000_000;
        writer
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr1],
                    spans: vec![sp1],
                },
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();

        // Seg 2: cost=$0, status=Error, session_id=None, start=ts+2100, dur=600
        let mut tr2 = make_trace(
            "tr_f1",
            "ag",
            "1.0",
            None,
            None,
            ts + 2100,
            TraceStatus::Error,
        );
        tr2.duration_ms = 600;
        tr2.end_ts_ms = Some(ts + 2700);
        tr2.total_cost_nanos = 0;
        tr2.total_cost_usd = 0.0;
        let mut sp2 = make_span("tr_f1", "sp2", None, ts + 2100);
        sp2.status = TraceStatus::Error;
        sp2.duration_ms = 600;
        sp2.end_ts_ms = Some(ts + 2700);
        sp2.cost_nanos = 0;
        sp2.cost_usd = 0.0;
        writer
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr2],
                    spans: vec![sp2],
                },
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();

        // Seg 3: disjoint cost=$0.005, status=Ok, session_id="s2", model="gpt-4o", start=ts, dur=100
        let mut tr3 = make_trace("tr_d", "ag", "1.0", Some("s2"), None, ts, TraceStatus::Ok);
        tr3.duration_ms = 100;
        tr3.end_ts_ms = Some(ts + 100);
        tr3.total_cost_nanos = 5_000_000;
        let mut sp3 = make_span("tr_d", "sp3", None, ts);
        sp3.duration_ms = 100;
        sp3.end_ts_ms = Some(ts + 100);
        sp3.cost_nanos = 5_000_000;
        writer
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr3],
                    spans: vec![sp3],
                },
            )
            .await
            .unwrap();
        writer.flush_all_l0().await.unwrap();
        let seg3_path = ManifestStore::new(pid, obj_store.clone(), 0)
            .unwrap()
            .load_latest()
            .await
            .unwrap()
            .segments[2]
            .object_path
            .clone();

        for f in [
            TraceListFilter {
                min_cost_usd: Some(0.002),
                status: Some("error".to_string()),
                ..Default::default()
            },
            TraceListFilter {
                min_cost_usd: Some(0.002),
                session_id: Some("s1".to_string()),
                ..Default::default()
            },
            // Monotonic start_ts_ms bound prunes Seg 3 even though Seg 3 has model="gpt-4o"
            TraceListFilter {
                start_ts_ms: Some(ts + 1000),
                model: Some("gpt-4o".to_string()),
                ..Default::default()
            },
        ] {
            let cold = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
                .await
                .unwrap();
            counting.reset();
            let res = cold
                .list_traces(pid, &f, &HashMap::new(), &[])
                .await
                .unwrap();
            assert_eq!(res.traces.len(), 1);
            assert_eq!(res.traces[0].trace_id, "tr_f1");
            assert_no_tag04_05_and_pruned_disjoint(&counting, &obj_store, pid, 2, &seg3_path).await;
            cold.close().await.unwrap();
        }

        // Monotonic max_duration_ms and max_cost_usd prune Seg 1 & Seg 2? Here Seg 3 has dur=100, cost=$0.005 while Seg 1/2 have dur>=500;
        // verify max_duration_ms=200 prunes Seg 1 & Seg 2 (leaving only Seg 3) with 0 non-tail reads on Seg 1 & Seg 2!
        let cold_dur = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
            .await
            .unwrap();
        counting.reset();
        let res_dur = cold_dur
            .list_traces(
                pid,
                &TraceListFilter {
                    max_duration_ms: Some(200),
                    model: Some("gpt-4o".to_string()),
                    ..Default::default()
                },
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(res_dur.traces.len(), 1);
        assert_eq!(res_dur.traces[0].trace_id, "tr_d");
        let man = ManifestStore::new(pid, obj_store.clone(), 0)
            .unwrap()
            .load_latest()
            .await
            .unwrap();
        let non_tail_dur: std::collections::HashSet<String> = counting
            .recorded_ranges()
            .into_iter()
            .filter(|(p, r)| {
                man.segments
                    .iter()
                    .any(|s| &s.object_path == p && r.start < s.tail_offset)
            })
            .map(|(p, _)| p)
            .collect();
        assert_eq!(non_tail_dur.len(), 1);
        assert!(non_tail_dur.contains(&seg3_path));
        cold_dur.close().await.unwrap();

        writer.close().await.unwrap();
    }

    // 5. Minor Finding 4: deterministic list_sessions vs get_session_detail user_id/environment
    {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
        let pid = "proj_r4_sess_det";

        let mut tr1 = make_trace(
            "tr_1",
            "ag",
            "1.0",
            Some("sess_det"),
            Some("u_alpha"),
            ts,
            TraceStatus::Ok,
        );
        tr1.environment = "production".to_string();
        let mut tr2 = make_trace(
            "tr_2",
            "ag",
            "1.0",
            Some("sess_det"),
            Some("u_zeta"),
            ts + 100,
            TraceStatus::Ok,
        );
        tr2.environment = "staging".to_string();
        let tr3 = make_trace(
            "tr_3",
            "ag",
            "1.0",
            Some("sess_det"),
            Some("u_mid"),
            ts + 200,
            TraceStatus::Ok,
        );
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr1],
                    spans: vec![make_span("tr_1", "sp1", None, ts)],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr2, tr3],
                    spans: vec![
                        make_span("tr_2", "sp2", None, ts + 100),
                        make_span("tr_3", "sp3", None, ts + 200),
                    ],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        // Force recompute via Trace tombstone on tr_3
        let tb_tr3 = AgentErasureTombstone {
            id: "tb_f4".to_string(),
            project_id: pid.to_string(),
            level: ErasureLevel::Trace,
            target_id: "tr_3".to_string(),
            created_at: ts,
            expires_at: None,
        };

        for _ in 0..5 {
            let listed = store
                .list_sessions(
                    pid,
                    &SessionListFilter::default(),
                    std::slice::from_ref(&tb_tr3),
                )
                .await
                .unwrap();
            let detail = store
                .get_session_detail(pid, "sess_det", std::slice::from_ref(&tb_tr3))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(listed.sessions.len(), 1);
            assert_eq!(listed.sessions[0].user_id, detail.summary.user_id);
            assert_eq!(listed.sessions[0].environment, detail.summary.environment);
            assert_eq!(listed.sessions[0].user_id.as_deref(), Some("u_zeta"));
            assert_eq!(listed.sessions[0].environment.as_deref(), Some("staging"));
        }

        store.close().await.unwrap();
    }
}

#[tokio::test]
async fn regression_r4_crdt_commutativity_and_associativity_invariants() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ts = 1_700_000_000_000i64;

    // 1. probe_1e (environment + failure_source) across L0 and .plss segments in both orders
    for (pid, flush_segments, reverse) in [
        ("proj_r4_1e_l0_fwd", false, false),
        ("proj_r4_1e_l0_rev", false, true),
        ("proj_r4_1e_plss_fwd", true, false),
        ("proj_r4_1e_plss_rev", true, true),
    ] {
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();

        let mut tr_start = make_trace(
            "tr_1e",
            "unknown",
            "unversioned",
            Some("sess_1e"),
            Some("u_1e"),
            ts,
            TraceStatus::Running,
        );
        tr_start.environment = "staging".to_string();
        tr_start.failure_source = Some(FailureSource::Application);
        let mut sp_start = make_span("tr_1e", "sp_1", None, ts);
        sp_start.span_phase = SpanPhase::Start;
        sp_start.status = TraceStatus::Running;

        let mut tr_final = make_trace(
            "tr_1e",
            "planner",
            "2.0",
            Some("sess_1e"),
            Some("u_1e"),
            ts,
            TraceStatus::Ok,
        );
        tr_final.environment = "production".to_string();
        tr_final.failure_source = Some(FailureSource::Model);
        let sp_final = make_span("tr_1e", "sp_1", None, ts);

        let first = TraceIngestBatch {
            traces: vec![tr_start],
            spans: vec![sp_start],
        };
        let second = TraceIngestBatch {
            traces: vec![tr_final],
            spans: vec![sp_final],
        };

        let (b1, b2) = if reverse {
            (second, first)
        } else {
            (first, second)
        };
        store.ingest_batch(pid, b1).await.unwrap();
        if flush_segments {
            store.flush_all_l0().await.unwrap();
        }
        store.ingest_batch(pid, b2).await.unwrap();
        if flush_segments {
            store.flush_all_l0().await.unwrap();
        }

        let det = store
            .get_trace_detail(pid, "tr_1e", &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(det.trace.environment, "staging", "failed for {pid}");
        assert_eq!(
            det.trace.failure_source,
            Some(FailureSource::Model),
            "failed for {pid}"
        );
        assert_eq!(det.trace.agent_name, "planner");
        assert_eq!(det.trace.agent_version, "2.0");

        let listed = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(listed.traces[0].environment, "staging");
        assert_eq!(listed.traces[0].failure_source, Some(FailureSource::Model));

        if flush_segments {
            store.compact_and_gc(Some(pid), &[]).await.unwrap();
            let det_after = store
                .get_trace_detail(pid, "tr_1e", &[])
                .await
                .unwrap()
                .unwrap();
            assert_eq!(det_after.trace.environment, "staging");
            assert_eq!(det_after.trace.failure_source, Some(FailureSource::Model));
        }

        store.close().await.unwrap();
    }

    // 2. Equal-rank (status, start_ts_ms) trace headers + equal-rank spans + tags>64 + L0 span-only symmetry
    let mut results = Vec::new();
    for reverse in [false, true] {
        let local_os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let pid = "proj_r4_eq";
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(local_os, cfg).await.unwrap();

        let mut tr_a = make_trace(
            "tr_eq",
            "ag_alpha",
            "v1",
            Some("sess_a"),
            Some("u_a"),
            ts,
            TraceStatus::Ok,
        );
        tr_a.environment = "canary".to_string();
        tr_a.output = json!({"v": "alpha"});
        tr_a.metadata = json!({"k": "alpha"});
        tr_a.error_type = Some("ErrA".to_string());
        tr_a.error_message = Some("msgA".to_string());
        tr_a.tags = (0..70).map(|i| format!("ta_{i:03}")).collect();
        let mut sp_a = make_span("tr_eq", "sp_eq", None, ts);
        sp_a.span_phase = SpanPhase::Update;
        sp_a.status = TraceStatus::Ok;
        sp_a.duration_ms = 100;
        sp_a.cost_nanos = 1_000_000;
        sp_a.total_tokens = 50;
        sp_a.model = Some("claude-sonnet-4".to_string());
        sp_a.tool_name = Some("tool_a".to_string());
        sp_a.linked_span_ids = vec!["l_a".to_string()];
        sp_a.events = vec![SpanEvent {
            name: "ev_a".to_string(),
            timestamp_ms: ts + 1,
            attributes: json!({"n": 1}),
        }];

        let mut tr_b = make_trace(
            "tr_eq",
            "ag_zeta",
            "v2",
            Some("sess_b"),
            Some("u_b"),
            ts,
            TraceStatus::Ok,
        );
        tr_b.environment = "staging".to_string();
        tr_b.output = json!({"v": "zeta"});
        tr_b.metadata = json!({"k": "zeta"});
        tr_b.error_type = Some("ErrZ".to_string());
        tr_b.error_message = Some("msgZ".to_string());
        tr_b.tags = (0..70).map(|i| format!("tb_{i:03}")).collect();
        let mut sp_b = make_span("tr_eq", "sp_eq", None, ts);
        sp_b.span_phase = SpanPhase::Update;
        sp_b.status = TraceStatus::Error;
        sp_b.duration_ms = 250;
        sp_b.cost_nanos = 3_000_000;
        sp_b.total_tokens = 120;
        sp_b.model = Some("gpt-4o".to_string());
        sp_b.tool_name = Some("tool_b".to_string());
        sp_b.linked_span_ids = vec!["l_b".to_string()];
        sp_b.events = vec![SpanEvent {
            name: "ev_b".to_string(),
            timestamp_ms: ts + 2,
            attributes: json!({"n": 2}),
        }];

        let ba = TraceIngestBatch {
            traces: vec![tr_a],
            spans: vec![sp_a],
        };
        let bb = TraceIngestBatch {
            traces: vec![tr_b],
            spans: vec![sp_b],
        };
        let (b1, b2) = if reverse { (bb, ba) } else { (ba, bb) };

        store.ingest_batch(pid, b1).await.unwrap();
        store.flush_all_l0().await.unwrap();
        store.ingest_batch(pid, b2).await.unwrap();
        store.flush_all_l0().await.unwrap();

        let det = store
            .get_trace_detail(pid, "tr_eq", &[])
            .await
            .unwrap()
            .unwrap();
        let listed = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        let qa = store
            .query_analytics(pid, &AnalyticsFilter::default(), &[])
            .await
            .unwrap();
        assert_eq!(det.trace.tags.len(), MAX_TAGS_PER_TRACE);
        assert_eq!(det.spans[0].linked_span_ids, vec!["l_a", "l_b"]);
        assert_eq!(det.spans[0].events.len(), 2);

        results.push((
            det.trace,
            det.spans,
            listed.traces[0].clone(),
            qa.total_cost_usd,
        ));
        store.close().await.unwrap();
    }
    assert_eq!(
        results[0].0, results[1].0,
        "equal-rank TraceDetail diverged"
    );
    assert_eq!(results[0].1, results[1].1, "equal-rank spans diverged");
    assert_eq!(
        results[0].2, results[1].2,
        "equal-rank list_traces diverged"
    );
    assert!((results[0].3 - results[1].3).abs() < 1e-12);

    // 3. L0 span-only -> header vs header -> span-only symmetry
    let mut l0_sym = Vec::new();
    for span_first in [true, false] {
        let local_os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let pid = "proj_r4_sym";
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(local_os, cfg).await.unwrap();

        let mut sp = make_span("tr_sym", "sp_1", None, ts);
        sp.status = TraceStatus::Error;
        sp.model = Some("claude-sonnet-4".to_string());
        sp.provider = Some("anthropic".to_string());

        let mut hdr = make_trace(
            "tr_sym",
            "planner",
            "1.0",
            Some("sess_sym"),
            Some("u_sym"),
            ts,
            TraceStatus::Ok,
        );
        hdr.environment = "staging".to_string();
        hdr.primary_model = Some("gpt-4o".to_string());
        hdr.primary_provider = Some("openai".to_string());

        let b_sp = TraceIngestBatch {
            traces: vec![],
            spans: vec![sp],
        };
        let b_hdr = TraceIngestBatch {
            traces: vec![hdr],
            spans: vec![],
        };
        if span_first {
            store.ingest_batch(pid, b_sp).await.unwrap();
            store.ingest_batch(pid, b_hdr).await.unwrap();
        } else {
            store.ingest_batch(pid, b_hdr).await.unwrap();
            store.ingest_batch(pid, b_sp).await.unwrap();
        }
        let det = store
            .get_trace_detail(pid, "tr_sym", &[])
            .await
            .unwrap()
            .unwrap();
        l0_sym.push(det.trace);
        store.close().await.unwrap();
    }
    assert_eq!(
        l0_sym[0], l0_sym[1],
        "L0 span-only -> header and header -> span-only must converge identically"
    );

    // 4. detect_execution_loop equal-rank commutativity
    let mut s1 = make_span("tr_loop", "sp_1", None, ts);
    s1.span_type = SpanType::Tool;
    s1.tool_name = Some("t1".to_string());
    let mut s2 = make_span("tr_loop", "sp_1", None, ts);
    s2.span_type = SpanType::Llm;
    s2.tool_name = None;
    assert_eq!(
        detect_execution_loop(&[s1.clone(), s2.clone()]),
        detect_execution_loop(&[s2, s1])
    );
}

// ---------------------------------------------------------------------------
// 26–29. Iteration 5 Regression Tests (probe_3h_d, probe_1f B/C, probe_7b.2, probe_7e)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn regression_r5_probe_3h_d_multi_writer_wal_and_erased_gc() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ts = 1_700_000_000_000i64;
    let pid = "proj_r5_3hd";

    let mut cfg_ingest = test_store_config(false, 0);
    cfg_ingest.l0_shards = 1;
    let ingest_wid = cfg_ingest.writer_id.to_string();
    let ingest_store = AgentTraceStore::open(obj_store.clone(), cfg_ingest)
        .await
        .unwrap();

    // Seg 1 (WAL seq 1): tr_l0p + tr_keep
    let b_l0p = make_batch("tr_l0p", Some("sess_bye"), Some("u_bye"), ts);
    let b_keep = make_batch("tr_keep", Some("sess_keep"), Some("u_keep"), ts + 10);
    ingest_store
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: [b_l0p.traces, b_keep.traces].concat(),
                spans: [b_l0p.spans, b_keep.spans].concat(),
            },
        )
        .await
        .unwrap();
    ingest_store.flush_all_l0().await.unwrap();

    // Uncommitted seq 2 on ingest_store holding sp_l0(tr_l0p) in live L0 memtable
    ingest_store
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![],
                spans: vec![make_span("tr_l0p", "sp_l0", None, ts + 20)],
            },
        )
        .await
        .unwrap();

    let wal_prefix = ObjectPath::from(format!("agent_wal/{pid}/"));
    let wal_before: Vec<_> = obj_store
        .list(Some(&wal_prefix))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(wal_before.len(), 1);
    let seq2_path = wal_before[0].location.to_string();
    assert!(seq2_path.contains(&ingest_wid));

    // Separate compactor opens pid AFTER seq 2 exists and runs compact_and_gc
    let mut cfg_comp = test_store_config(false, 0);
    cfg_comp.l0_shards = 1;
    let compactor_store = AgentTraceStore::open(obj_store.clone(), cfg_comp)
        .await
        .unwrap();
    let tb_sess = AgentErasureTombstone {
        id: "tb_3hd".to_string(),
        project_id: pid.to_string(),
        level: ErasureLevel::Session,
        target_id: "sess_bye".to_string(),
        created_at: ts,
        expires_at: None,
    };
    compactor_store
        .compact_and_gc(Some(pid), &[tb_sess])
        .await
        .unwrap();

    // Compactor MUST NOT purge foreign ingest_wid's WAL seq 2 or advance committed_wal_seq[ingest_wid]
    let ms = ManifestStore::new(pid, obj_store.clone(), 0).unwrap();
    let m1 = ms.load_latest().await.unwrap();
    assert_eq!(
        m1.committed_wal_seq.get(&ingest_wid).copied(),
        Some(1),
        "compactor must not advance foreign writer committed_wal_seq: {:?}",
        m1.committed_wal_seq
    );
    assert!(
        m1.erased_traces.contains_key("tr_l0p"),
        "manifest.erased_traces must contain tr_l0p"
    );
    let wal_after_comp1: Vec<_> = obj_store
        .list(Some(&wal_prefix))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        wal_after_comp1.len(),
        1,
        "compactor must not delete foreign writer WAL seq 2: {wal_after_comp1:?}"
    );

    // Second compact_and_gc after gc_grace_ms (gc_grace_secs=0) BEFORE ingest flushes MUST NOT prematurely GC tr_l0p
    compactor_store
        .compact_and_gc(Some(pid), &[])
        .await
        .unwrap();
    let m2 = ms.load_latest().await.unwrap();
    assert!(
        m2.erased_traces.contains_key("tr_l0p"),
        "tr_l0p must NOT be GC'd while ingest's uncommitted WAL seq 2 still references it: {:?}",
        m2.erased_traces
    );

    // Closing compactor_store also must NOT purge foreign ingest_wid's WAL seq 2
    compactor_store.close().await.unwrap();
    let wal_after_comp_close: Vec<_> = obj_store
        .list(Some(&wal_prefix))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        wal_after_comp_close.len(),
        1,
        "compactor.close() must not delete foreign writer WAL seq 2"
    );

    // Now ingest_store flushes L0 -> purges sp_l0(tr_l0p), does NOT write tr_l0p to .plss, advances committed_wal_seq[ingest_wid]=2, deletes seq 2
    ingest_store.flush_all_l0().await.unwrap();
    let wal_after_ingest_flush: Vec<_> = obj_store
        .list(Some(&wal_prefix))
        .try_collect()
        .await
        .unwrap();
    assert!(
        wal_after_ingest_flush.is_empty(),
        "ingest flush must purge seq 2: {wal_after_ingest_flush:?}"
    );
    let m3 = ms.load_latest().await.unwrap();
    assert_eq!(m3.committed_wal_seq.get(&ingest_wid).copied(), Some(2));

    // Verify tr_l0p is NOT resurrected (even queried with &[])
    let listed_no_tb = ingest_store
        .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(
        listed_no_tb.traces.len(),
        1,
        "tr_l0p resurrected! {:?}",
        listed_no_tb.traces
    );
    assert_eq!(listed_no_tb.traces[0].trace_id, "tr_keep");
    assert!(ingest_store
        .get_trace_detail(pid, "tr_l0p", &[])
        .await
        .unwrap()
        .is_none());

    // Subsequent compact_and_gc on a fresh compactor safely GCs tr_l0p from erased_traces
    let mut cfg_comp2 = test_store_config(false, 0);
    cfg_comp2.l0_shards = 1;
    let compactor2 = AgentTraceStore::open(obj_store.clone(), cfg_comp2)
        .await
        .unwrap();
    compactor2.compact_and_gc(Some(pid), &[]).await.unwrap();
    let m4 = ms.load_latest().await.unwrap();
    assert!(
        m4.erased_traces.is_empty(),
        "tr_l0p should now be safely GC'd: {:?}",
        m4.erased_traces
    );

    ingest_store.close().await.unwrap();
    compactor2.close().await.unwrap();

    let cold = AgentTraceStore::open(obj_store, test_store_config(true, 0))
        .await
        .unwrap();
    let final_traces = cold
        .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(final_traces.traces.len(), 1);
    assert_eq!(final_traces.traces[0].trace_id, "tr_keep");
    cold.close().await.unwrap();
}

#[tokio::test]
async fn regression_r5_probe_1f_l0_recompute_and_3way_associativity() {
    let ts = 1_700_000_000_000i64;

    // 1. Part B: L0 recompute_trace_from_spans + merge_trace_headers commutativity
    // (hdr(None) + [ToolErr, Llm("zzz")]) + hdr("aaa", Application) == hdr("aaa", Application) + (hdr(None) + [ToolErr, Llm("zzz")])
    let mut part_b_results = Vec::new();
    for reverse in [false, true] {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
        let pid = "proj_r5_1fb";

        let mut hdr_none = make_trace("tr_1fb", "planner", "1.0", None, None, ts, TraceStatus::Ok);
        hdr_none.primary_model = None;
        hdr_none.primary_provider = None;
        hdr_none.failure_source = None;

        let mut sp_tool_err = make_span("tr_1fb", "sp_tool_err", None, ts);
        sp_tool_err.span_type = SpanType::Tool;
        sp_tool_err.tool_name = Some("search".to_string());
        sp_tool_err.model = None;
        sp_tool_err.provider = None;
        sp_tool_err.status = TraceStatus::Error;

        let mut sp_llm_zzz = make_span("tr_1fb", "sp_llm_zzz", None, ts + 10);
        sp_llm_zzz.span_type = SpanType::Llm;
        sp_llm_zzz.model = Some("zzz-model".to_string());
        sp_llm_zzz.provider = Some("zzz-prov".to_string());

        let mut hdr_aaa = make_trace(
            "tr_1fb",
            "planner",
            "1.0",
            None,
            None,
            ts,
            TraceStatus::Error,
        );
        hdr_aaa.primary_model = Some("aaa-model".to_string());
        hdr_aaa.primary_provider = Some("aaa-prov".to_string());
        hdr_aaa.failure_source = Some(FailureSource::Application);

        let b_span = TraceIngestBatch {
            traces: vec![hdr_none],
            spans: vec![sp_tool_err, sp_llm_zzz],
        };
        let b_hdr = TraceIngestBatch {
            traces: vec![hdr_aaa],
            spans: vec![],
        };

        if reverse {
            store.ingest_batch(pid, b_hdr).await.unwrap();
            store.ingest_batch(pid, b_span).await.unwrap();
        } else {
            store.ingest_batch(pid, b_span).await.unwrap();
            store.ingest_batch(pid, b_hdr).await.unwrap();
        }

        let det = store
            .get_trace_detail(pid, "tr_1fb", &[])
            .await
            .unwrap()
            .unwrap();
        let listed = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(det.trace.primary_model.as_deref(), Some("zzz-model"));
        assert_eq!(det.trace.primary_provider.as_deref(), Some("zzz-prov"));
        assert_eq!(det.trace.failure_source, Some(FailureSource::Tool));
        part_b_results.push((det.trace, listed.traces[0].clone()));
        store.close().await.unwrap();
    }
    assert_eq!(part_b_results[0].0, part_b_results[1].0);
    assert_eq!(part_b_results[0].1, part_b_results[1].1);

    // Also verify depth-based classify_failure_source commutativity in L0
    let mut depth_results = Vec::new();
    for reverse in [false, true] {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
        let pid = "proj_r5_depth";

        let mut sp_shallow_llm = make_span("tr_d", "sp_d0", None, ts);
        sp_shallow_llm.depth = 0;
        sp_shallow_llm.span_type = SpanType::Llm;
        sp_shallow_llm.status = TraceStatus::Error;

        let mut sp_deep_tool = make_span("tr_d", "sp_d1", Some("sp_d0"), ts + 10);
        sp_deep_tool.depth = 1;
        sp_deep_tool.span_type = SpanType::Tool;
        sp_deep_tool.tool_name = Some("t".to_string());
        sp_deep_tool.model = None;
        sp_deep_tool.provider = None;
        sp_deep_tool.status = TraceStatus::Error;

        let (b1, b2) = if reverse {
            (
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![sp_deep_tool],
                },
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![sp_shallow_llm],
                },
            )
        } else {
            (
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![sp_shallow_llm],
                },
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![sp_deep_tool],
                },
            )
        };
        store.ingest_batch(pid, b1).await.unwrap();
        store.ingest_batch(pid, b2).await.unwrap();
        let det = store
            .get_trace_detail(pid, "tr_d", &[])
            .await
            .unwrap()
            .unwrap();
        depth_results.push(det.trace);
        store.close().await.unwrap();
    }
    assert_eq!(depth_results[0], depth_results[1]);
    assert_eq!(depth_results[0].failure_source, Some(FailureSource::Tool));

    // 2. Part C: 3-way associativity across 3 segments AND L0 (all 6 permutations)
    let mut hdr_a = make_trace(
        "tr_3w",
        "planner",
        "1.0",
        Some("s_3w"),
        Some("u_3w"),
        ts,
        TraceStatus::Running,
    );
    hdr_a.prompt_version = Some("p_running_zzz".to_string());
    hdr_a.model_config_version = Some("mc_zzz".to_string());
    hdr_a.tool_config_version = Some("tc_zzz".to_string());
    hdr_a.app_release = Some("rel_zzz".to_string());
    hdr_a.primary_model = Some("zzz-model".to_string());
    hdr_a.primary_provider = Some("zzz-prov".to_string());
    hdr_a.input = json!({"step": "zzz_running"});
    hdr_a.output = json!({"step": "zzz_running"});
    hdr_a.metadata = json!({"m": "zzz_running"});
    hdr_a.error_type = Some("ErrRunningZzz".to_string());
    hdr_a.error_message = Some("msgRunningZzz".to_string());

    let mut sp_a = make_span("tr_3w", "sp_3p", None, ts);
    sp_a.span_phase = SpanPhase::Start;
    sp_a.status = TraceStatus::Running;
    sp_a.model = Some("zzz-model".to_string());
    sp_a.provider = Some("zzz-prov".to_string());
    sp_a.input = json!({"span_step": "s"});
    sp_a.output = json!({"span_out": "s"});
    sp_a.metadata = json!({"span_meta": "s"});

    let mut hdr_b = make_trace(
        "tr_3w",
        "planner",
        "1.0",
        Some("s_3w"),
        Some("u_3w"),
        ts,
        TraceStatus::Ok,
    );
    hdr_b.prompt_version = None;
    hdr_b.model_config_version = None;
    hdr_b.tool_config_version = None;
    hdr_b.app_release = None;
    hdr_b.primary_model = None;
    hdr_b.primary_provider = None;
    hdr_b.input = Value::Null;
    hdr_b.output = Value::Null;
    hdr_b.metadata = Value::Null;
    hdr_b.error_type = None;
    hdr_b.error_message = None;

    let mut sp_b = make_span("tr_3w", "sp_3p", None, ts);
    sp_b.span_phase = SpanPhase::Final;
    sp_b.status = TraceStatus::Ok;
    sp_b.model = None;
    sp_b.provider = None;
    sp_b.input = Value::Null;
    sp_b.output = Value::Null;
    sp_b.metadata = Value::Null;

    let mut hdr_c = make_trace(
        "tr_3w",
        "planner",
        "1.0",
        Some("s_3w"),
        Some("u_3w"),
        ts,
        TraceStatus::Ok,
    );
    hdr_c.prompt_version = Some("p_ok_aaa".to_string());
    hdr_c.model_config_version = Some("mc_aaa".to_string());
    hdr_c.tool_config_version = Some("tc_aaa".to_string());
    hdr_c.app_release = Some("rel_aaa".to_string());
    hdr_c.primary_model = Some("aaa-model".to_string());
    hdr_c.primary_provider = Some("aaa-prov".to_string());
    hdr_c.input = json!({"step": "aaa_ok"});
    hdr_c.output = json!({"step": "aaa_ok"});
    hdr_c.metadata = json!({"m": "aaa_ok"});
    hdr_c.error_type = Some("ErrOkAaa".to_string());
    hdr_c.error_message = Some("msgOkAaa".to_string());

    let mut sp_c = make_span("tr_3w", "sp_3p", None, ts);
    sp_c.span_phase = SpanPhase::Update;
    sp_c.status = TraceStatus::Running;
    sp_c.model = Some("aaa-model".to_string());
    sp_c.provider = Some("aaa-prov".to_string());
    sp_c.input = json!({"span_step": "u"});
    sp_c.output = json!({"span_out": "u"});
    sp_c.metadata = json!({"span_meta": "u"});

    let batches = [
        TraceIngestBatch {
            traces: vec![hdr_a],
            spans: vec![sp_a],
        },
        TraceIngestBatch {
            traces: vec![hdr_b],
            spans: vec![sp_b],
        },
        TraceIngestBatch {
            traces: vec![hdr_c],
            spans: vec![sp_c],
        },
    ];

    let perms = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];

    let mut expected_detail: Option<(AgentTrace, Vec<AgentSpan>)> = None;
    let mut expected_listed: Option<AgentTrace> = None;

    for flush_each in [true, false] {
        for perm in perms {
            let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut cfg = test_store_config(false, 0);
            cfg.l0_shards = 1;
            let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
            let pid = "proj_r5_3way";
            for idx in perm {
                store.ingest_batch(pid, batches[idx].clone()).await.unwrap();
                if flush_each {
                    store.flush_all_l0().await.unwrap();
                }
            }

            let det = store
                .get_trace_detail(pid, "tr_3w", &[])
                .await
                .unwrap()
                .unwrap();
            let listed = store
                .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
                .await
                .unwrap();

            assert_eq!(det.trace.prompt_version.as_deref(), Some("p_running_zzz"));
            assert_eq!(det.trace.model_config_version.as_deref(), Some("mc_zzz"));
            assert_eq!(det.trace.tool_config_version.as_deref(), Some("tc_zzz"));
            assert_eq!(det.trace.app_release.as_deref(), Some("rel_zzz"));
            assert_eq!(det.trace.primary_model.as_deref(), Some("zzz-model"));
            assert_eq!(det.trace.primary_provider.as_deref(), Some("zzz-prov"));
            assert_eq!(det.trace.input, json!({"step": "zzz_running"}));
            assert_eq!(det.trace.output, json!({"step": "aaa_ok"}));
            assert_eq!(det.trace.metadata, json!({"m": "aaa_ok"}));
            assert_eq!(det.trace.error_type.as_deref(), Some("ErrOkAaa"));
            assert_eq!(det.trace.error_message.as_deref(), Some("msgOkAaa"));

            assert_eq!(det.spans.len(), 1);
            assert_eq!(det.spans[0].span_phase, SpanPhase::Final);
            assert_eq!(det.spans[0].input, json!({"span_step": "u"}));
            assert_eq!(det.spans[0].output, json!({"span_out": "u"}));
            assert_eq!(det.spans[0].metadata, json!({"span_meta": "u"}));
            assert_eq!(det.spans[0].model.as_deref(), Some("zzz-model"));

            assert_eq!(
                listed.traces[0].prompt_version.as_deref(),
                Some("p_running_zzz")
            );
            assert_eq!(listed.traces[0].error_type.as_deref(), Some("ErrOkAaa"));

            if let Some((ref exp_tr, ref exp_sp)) = expected_detail {
                assert_eq!(
                    &det.trace, exp_tr,
                    "3-way trace divergence (flush={flush_each}, perm={perm:?})"
                );
                assert_eq!(
                    &det.spans, exp_sp,
                    "3-way span divergence (flush={flush_each}, perm={perm:?})"
                );
            } else {
                expected_detail = Some((det.trace, det.spans));
            }
            if let Some(ref exp_l) = expected_listed {
                assert_eq!(
                    &listed.traces[0], exp_l,
                    "3-way list_traces divergence (flush={flush_each}, perm={perm:?})"
                );
            } else {
                expected_listed = Some(listed.traces[0].clone());
            }

            store.close().await.unwrap();
        }
    }

    // Also test mixed L0 + Segment 3-way associativity: (A+B in L0) + C in Seg vs A in Seg + (B+C in L0)
    let mut mixed_res = Vec::new();
    for a_b_in_l0 in [true, false] {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
        let pid = "proj_r5_3way";
        if a_b_in_l0 {
            store.ingest_batch(pid, batches[2].clone()).await.unwrap();
            store.flush_all_l0().await.unwrap();
            store.ingest_batch(pid, batches[0].clone()).await.unwrap();
            store.ingest_batch(pid, batches[1].clone()).await.unwrap();
        } else {
            store.ingest_batch(pid, batches[0].clone()).await.unwrap();
            store.flush_all_l0().await.unwrap();
            store.ingest_batch(pid, batches[1].clone()).await.unwrap();
            store.ingest_batch(pid, batches[2].clone()).await.unwrap();
        }
        let det = store
            .get_trace_detail(pid, "tr_3w", &[])
            .await
            .unwrap()
            .unwrap();
        let listed = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        mixed_res.push((det.trace, det.spans, listed.traces[0].clone()));
        store.close().await.unwrap();
    }
    assert_eq!(mixed_res[0].0, mixed_res[1].0);
    assert_eq!(mixed_res[0].1, mixed_res[1].1);
    assert_eq!(mixed_res[0].2, mixed_res[1].2);
    assert_eq!(mixed_res[0].0, expected_detail.unwrap().0);
}

#[tokio::test]
async fn regression_r5_probe_7b2_single_segment_max_cost_nan() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut cfg = test_store_config(false, 0);
    cfg.l0_shards = 1;
    let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
    let ts = 1_700_000_000_000i64;
    let pid = "proj_r5_7b2";

    let mut tr = make_trace("tr_nan", "ag", "1.0", None, None, ts, TraceStatus::Ok);
    tr.total_cost_nanos = 5_000_000;
    let mut sp = make_span("tr_nan", "sp_1", None, ts);
    sp.cost_nanos = 5_000_000;

    // L0 check
    store
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr],
                spans: vec![sp],
            },
        )
        .await
        .unwrap();
    let nan_filter = TraceListFilter {
        max_cost_usd: Some(f64::NAN),
        ..Default::default()
    };
    let l0_res = store
        .list_traces(pid, &nan_filter, &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(l0_res.traces.len(), 1);

    // Single-segment check (multi_segment == false)
    store.flush_all_l0().await.unwrap();
    let single_res = store
        .list_traces(pid, &nan_filter, &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(
        single_res.traces.len(),
        1,
        "single-segment max_cost_usd=NaN must match L0 and multi-segment"
    );

    // Multi-segment check (multi_segment == true)
    let mut tr2 = make_trace(
        "tr_nan_2",
        "ag",
        "1.0",
        None,
        None,
        ts + 10,
        TraceStatus::Ok,
    );
    tr2.total_cost_nanos = 7_000_000;
    let mut sp2 = make_span("tr_nan_2", "sp_2", None, ts + 10);
    sp2.cost_nanos = 7_000_000;
    store
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr2],
                spans: vec![sp2],
            },
        )
        .await
        .unwrap();
    store.flush_all_l0().await.unwrap();
    let multi_res = store
        .list_traces(pid, &nan_filter, &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(multi_res.traces.len(), 2);

    store.close().await.unwrap();
}

#[tokio::test]
async fn regression_r5_probe_7e_cross_segment_split_loop_detection() {
    let raw_mem = Arc::new(InMemory::new());
    let counting = CountingStore::new(raw_mem);
    let obj_store: Arc<dyn ObjectStore> = counting.clone();

    let mut cfg = test_store_config(false, 0);
    cfg.l0_shards = 1;
    let writer = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();
    let ts = 1_700_000_000_000i64;
    let pid = "proj_r5_7e";

    // Segment 1: 6 unique LLM spans for tr_loop_split (loop_count = 0 in Seg 1)
    let mut seg1_spans = Vec::new();
    for i in 0..6 {
        let mut sp = make_span("tr_loop_split", &format!("sp_llm_{i}"), None, ts + i * 10);
        sp.span_type = SpanType::Llm;
        sp.model = Some("gpt-4o".to_string());
        seg1_spans.push(sp);
    }
    let mut tr1 = make_trace(
        "tr_loop_split",
        "planner",
        "1.0",
        Some("sess_loop"),
        Some("u1"),
        ts,
        TraceStatus::Ok,
    );
    tr1.primary_model = Some("gpt-4o".to_string());
    writer
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr1],
                spans: seg1_spans,
            },
        )
        .await
        .unwrap();
    writer.flush_all_l0().await.unwrap();

    // Segment 2: 6 more unique LLM spans for tr_loop_split (loop_count = 0 in Seg 2, 12 total!)
    let mut seg2_spans = Vec::new();
    for i in 6..12 {
        let mut sp = make_span("tr_loop_split", &format!("sp_llm_{i}"), None, ts + i * 10);
        sp.span_type = SpanType::Llm;
        sp.model = Some("gpt-4o".to_string());
        seg2_spans.push(sp);
    }
    let mut tr2 = make_trace(
        "tr_loop_split",
        "planner",
        "1.0",
        Some("sess_loop"),
        Some("u1"),
        ts + 60,
        TraceStatus::Ok,
    );
    tr2.primary_model = Some("gpt-4o".to_string());
    writer
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr2],
                spans: seg2_spans,
            },
        )
        .await
        .unwrap();
    writer.flush_all_l0().await.unwrap();

    // Segment 3: disjoint non-loop trace tr_normal
    let mut tr_norm = make_trace(
        "tr_normal",
        "other_agent",
        "1.0",
        Some("sess_norm"),
        Some("u2"),
        ts + 500,
        TraceStatus::Ok,
    );
    tr_norm.primary_model = Some("claude-sonnet-4".to_string());
    let mut sp_norm = make_span("tr_normal", "sp_norm", None, ts + 500);
    sp_norm.model = Some("claude-sonnet-4".to_string());
    writer
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![tr_norm],
                spans: vec![sp_norm],
            },
        )
        .await
        .unwrap();
    writer.flush_all_l0().await.unwrap();

    let det = writer
        .get_trace_detail(pid, "tr_loop_split", &[])
        .await
        .unwrap()
        .unwrap();
    assert!(det.trace.has_loop_warning);
    assert_eq!(det.trace.llm_call_count, 12);

    let loop_only = writer
        .list_traces(
            pid,
            &TraceListFilter {
                has_loop_warning: Some(true),
                ..Default::default()
            },
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(loop_only.traces.len(), 1);
    assert_eq!(loop_only.traces[0].trace_id, "tr_loop_split");
    assert!(loop_only.traces[0].has_loop_warning);
    assert_eq!(loop_only.traces[0].llm_call_count, 12);

    let no_loop = writer
        .list_traces(
            pid,
            &TraceListFilter {
                has_loop_warning: Some(false),
                ..Default::default()
            },
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(no_loop.traces.len(), 1);
    assert_eq!(no_loop.traces[0].trace_id, "tr_normal");

    let qa = writer
        .query_analytics(pid, &AnalyticsFilter::default(), &[])
        .await
        .unwrap();
    assert_eq!(qa.total_executions, 2);
    assert_eq!(qa.loop_warning_count, 1);

    let cmp = writer
        .compare_groups(
            pid,
            &CompareFilter {
                baseline_agent_version: Some("1.0".to_string()),
                candidate_agent_version: Some("2.0".to_string()),
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();
    assert_eq!(cmp.baseline.loop_warning_count, 1);

    // Cold CountingStore: has_loop_warning=true + higher-selectivity witness (model="gpt-4o")
    // prunes disjoint Segment 3 with 0 non-tail reads and 0 Tag 0x04/0x05 reads
    let cold = AgentTraceStore::open(obj_store.clone(), test_store_config(true, 0))
        .await
        .unwrap();
    let man = ManifestStore::new(pid, obj_store.clone(), 0)
        .unwrap()
        .load_latest()
        .await
        .unwrap();
    let seg3_path = man.segments[2].object_path.clone();
    let seg_by_path: HashMap<String, _> = man
        .segments
        .iter()
        .map(|s| (s.object_path.clone(), s.clone()))
        .collect();

    counting.reset();
    let filtered_loop = cold
        .list_traces(
            pid,
            &TraceListFilter {
                has_loop_warning: Some(true),
                model: Some("gpt-4o".to_string()),
                ..Default::default()
            },
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(filtered_loop.traces.len(), 1);
    assert_eq!(filtered_loop.traces[0].trace_id, "tr_loop_split");

    let plss_reader = pulse_agent::store::segment::PlssReader::new(
        obj_store,
        pulse_agent::store::cache::AgentSegmentCache::open(None, 1024 * 1024, None, 0)
            .await
            .unwrap(),
    );
    let mut non_tail_paths = std::collections::HashSet::new();
    for (p, r) in counting.recorded_ranges() {
        if let Some(seg) = seg_by_path.get(&p) {
            if r.start < seg.tail_offset {
                non_tail_paths.insert(p.clone());
            }
            let tail = plss_reader.read_eof_tail(seg).await.unwrap();
            let sec04 = tail.find_section(SectionTag::SpanTree).unwrap();
            let sec05 = tail.find_section(SectionTag::PayloadPack).unwrap();
            assert!(!(r.start < sec04.offset + sec04.len as u64 && r.end > sec04.offset));
            assert!(!(r.start < sec05.offset + sec05.len as u64 && r.end > sec05.offset));
        }
    }
    assert!(
        !non_tail_paths.contains(&seg3_path),
        "disjoint Segment 3 must have 0 non-tail reads: {non_tail_paths:?}"
    );
    assert_eq!(non_tail_paths.len(), 2);

    cold.close().await.unwrap();
    writer.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// 30–35. Iteration 6 Regression Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn regression_r6_compactor_close_nonempty_staged_preserves_foreign_erased_wal() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ts = 1_700_000_000_000i64;
    let pid = "proj_r6_1a";

    let mut cfg_ingest = test_store_config(false, 0);
    cfg_ingest.l0_shards = 1;
    let ingest_wid = cfg_ingest.writer_id.to_string();
    let ingest_a = AgentTraceStore::open(obj_store.clone(), cfg_ingest)
        .await
        .unwrap();

    // WAL seq 1: tr_l0p (sess_bye) flushed to Seg 1
    ingest_a
        .ingest_batch(
            pid,
            make_batch("tr_l0p", Some("sess_bye"), Some("u_bye"), ts),
        )
        .await
        .unwrap();
    ingest_a.flush_all_l0().await.unwrap();

    // WAL seq 2: shared batch with surviving tr_keep + sp_l0(tr_l0p)
    let b_keep = make_batch("tr_keep", Some("sess_keep"), Some("u_keep"), ts + 10);
    let sp_l0 = make_span("tr_l0p", "sp_l0", None, ts + 20);
    ingest_a
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: b_keep.traces,
                spans: [b_keep.spans, vec![sp_l0]].concat(),
            },
        )
        .await
        .unwrap();

    let wal_prefix = ObjectPath::from(format!("agent_wal/{pid}/"));
    let wal_before: Vec<_> = obj_store
        .list(Some(&wal_prefix))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(wal_before.len(), 1);
    assert!(wal_before[0].location.as_ref().contains(&ingest_wid));

    // Compactor opens pid, erases sess_bye, and closes (non-empty staged_list containing tr_keep)
    let mut cfg_comp = test_store_config(false, 0);
    cfg_comp.l0_shards = 1;
    let compactor = AgentTraceStore::open(obj_store.clone(), cfg_comp)
        .await
        .unwrap();
    let tb_sess = AgentErasureTombstone {
        id: "tb_r6_1a".to_string(),
        project_id: pid.to_string(),
        level: ErasureLevel::Session,
        target_id: "sess_bye".to_string(),
        created_at: ts,
        expires_at: None,
    };
    compactor
        .compact_and_gc(Some(pid), &[tb_sess])
        .await
        .unwrap();
    compactor.close().await.unwrap();

    let ms = ManifestStore::new(pid, obj_store.clone(), 0).unwrap();
    let m1 = ms.load_latest().await.unwrap();
    assert_eq!(
        m1.committed_wal_seq.get(&ingest_wid).copied(),
        Some(1),
        "compactor.close() with non-empty staged_list must NOT advance foreign committed_wal_seq past blocked seq 2: {:?}",
        m1.committed_wal_seq
    );
    let wal_after_comp_close: Vec<_> = obj_store
        .list(Some(&wal_prefix))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        wal_after_comp_close.len(),
        1,
        "compactor.close() must preserve ingest_a seq 2 WAL"
    );

    // Second compactor with gc_grace_secs=0 must NOT GC tr_l0p while seq 2 exists
    let mut cfg_comp2 = test_store_config(false, 0);
    cfg_comp2.l0_shards = 1;
    let compactor2 = AgentTraceStore::open(obj_store.clone(), cfg_comp2)
        .await
        .unwrap();
    compactor2.compact_and_gc(Some(pid), &[]).await.unwrap();
    let m2 = ms.load_latest().await.unwrap();
    assert!(
        m2.erased_traces.contains_key("tr_l0p"),
        "tr_l0p must remain in erased_traces until ingest_a flushes seq 2"
    );
    compactor2.close().await.unwrap();

    // Now ingest_a flushes: seq 2 is purged, tr_keep survives, tr_l0p is NOT resurrected
    ingest_a.flush_all_l0().await.unwrap();
    let wal_after_ingest: Vec<_> = obj_store
        .list(Some(&wal_prefix))
        .try_collect()
        .await
        .unwrap();
    assert!(wal_after_ingest.is_empty());
    let listed = ingest_a
        .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(listed.traces.len(), 1);
    assert_eq!(listed.traces[0].trace_id, "tr_keep");
    assert!(ingest_a
        .get_trace_detail(pid, "tr_l0p", &[])
        .await
        .unwrap()
        .is_none());

    // Subsequent compactor safely GCs tr_l0p
    let mut cfg_comp3 = test_store_config(false, 0);
    cfg_comp3.l0_shards = 1;
    let compactor3 = AgentTraceStore::open(obj_store.clone(), cfg_comp3)
        .await
        .unwrap();
    compactor3.compact_and_gc(Some(pid), &[]).await.unwrap();
    let m3 = ms.load_latest().await.unwrap();
    assert!(m3.erased_traces.is_empty());
    compactor3.close().await.unwrap();
    ingest_a.close().await.unwrap();
}

#[tokio::test]
async fn regression_r6_second_ingest_writer_flush_preserves_first_writers_erased_wal() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ts = 1_700_000_000_000i64;
    let pid = "proj_r6_a2";

    let mut cfg_a = test_store_config(false, 0);
    cfg_a.l0_shards = 1;
    let wid_a = cfg_a.writer_id.to_string();
    let ingest_a = AgentTraceStore::open(obj_store.clone(), cfg_a)
        .await
        .unwrap();

    // Seg 1 (seq 1): tr_l0p + tr_keep
    let b_l0p = make_batch("tr_l0p", Some("sess_bye"), Some("u_bye"), ts);
    let b_keep = make_batch("tr_keep", Some("sess_keep"), Some("u_keep"), ts + 10);
    ingest_a
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: [b_l0p.traces, b_keep.traces].concat(),
                spans: [b_l0p.spans, b_keep.spans].concat(),
            },
        )
        .await
        .unwrap();
    ingest_a.flush_all_l0().await.unwrap();

    // Uncommitted seq 2 on ingest_a holding sp_l0(tr_l0p)
    ingest_a
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: vec![],
                spans: vec![make_span("tr_l0p", "sp_l0", None, ts + 20)],
            },
        )
        .await
        .unwrap();

    // Compactor erases sess_bye
    let mut cfg_comp = test_store_config(false, 0);
    cfg_comp.l0_shards = 1;
    let compactor = AgentTraceStore::open(obj_store.clone(), cfg_comp)
        .await
        .unwrap();
    let tb_sess = AgentErasureTombstone {
        id: "tb_r6_a2".to_string(),
        project_id: pid.to_string(),
        level: ErasureLevel::Session,
        target_id: "sess_bye".to_string(),
        created_at: ts,
        expires_at: None,
    };
    compactor
        .compact_and_gc(Some(pid), &[tb_sess])
        .await
        .unwrap();
    compactor.close().await.unwrap();

    // Second writer ingest_b opens pid (drops sp_l0 on replay), ingests tr_b, flushes
    let mut cfg_b = test_store_config(false, 0);
    cfg_b.l0_shards = 1;
    let ingest_b = AgentTraceStore::open(obj_store.clone(), cfg_b)
        .await
        .unwrap();
    ingest_b
        .ingest_batch(
            pid,
            make_batch("tr_b", Some("sess_b"), Some("u_b"), ts + 30),
        )
        .await
        .unwrap();
    ingest_b.flush_all_l0().await.unwrap();

    let ms = ManifestStore::new(pid, obj_store.clone(), 0).unwrap();
    let m1 = ms.load_latest().await.unwrap();
    assert_eq!(m1.committed_wal_seq.get(&wid_a).copied(), Some(1));
    assert!(obj_store
        .head(&wal_object_path(pid, &wid_a, 2).unwrap())
        .await
        .is_ok());

    // Compactor2 with gc_grace_secs=0 must NOT GC tr_l0p while ingest_a seq 2 still exists
    let mut cfg_comp2 = test_store_config(false, 0);
    cfg_comp2.l0_shards = 1;
    let compactor2 = AgentTraceStore::open(obj_store.clone(), cfg_comp2)
        .await
        .unwrap();
    compactor2.compact_and_gc(Some(pid), &[]).await.unwrap();
    let m2 = ms.load_latest().await.unwrap();
    assert!(m2.erased_traces.contains_key("tr_l0p"));
    compactor2.close().await.unwrap();

    // ingest_a flushes -> no zombie tr_l0p
    ingest_a.flush_all_l0().await.unwrap();
    let listed = ingest_a
        .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
        .await
        .unwrap();
    let mut ids: Vec<&str> = listed.traces.iter().map(|t| t.trace_id.as_str()).collect();
    ids.sort();
    assert_eq!(ids, vec!["tr_b", "tr_keep"]);

    ingest_b.close().await.unwrap();
    ingest_a.close().await.unwrap();
}

#[tokio::test]
async fn regression_r6_compactor_close_then_ingest_crash_recovers_keep_without_zombie() {
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ts = 1_700_000_000_000i64;
    let pid = "proj_r6_1b";

    let mut cfg_a = test_store_config(false, 0);
    cfg_a.l0_shards = 1;
    let ingest_a = AgentTraceStore::open(obj_store.clone(), cfg_a)
        .await
        .unwrap();

    ingest_a
        .ingest_batch(
            pid,
            make_batch("tr_l0p", Some("sess_bye"), Some("u_bye"), ts),
        )
        .await
        .unwrap();
    ingest_a.flush_all_l0().await.unwrap();

    let b_keep = make_batch("tr_keep", Some("sess_keep"), Some("u_keep"), ts + 10);
    let sp_l0 = make_span("tr_l0p", "sp_l0", None, ts + 20);
    ingest_a
        .ingest_batch(
            pid,
            TraceIngestBatch {
                traces: b_keep.traces,
                spans: [b_keep.spans, vec![sp_l0]].concat(),
            },
        )
        .await
        .unwrap();

    let mut cfg_comp = test_store_config(false, 0);
    cfg_comp.l0_shards = 1;
    let compactor = AgentTraceStore::open(obj_store.clone(), cfg_comp)
        .await
        .unwrap();
    let tb_sess = AgentErasureTombstone {
        id: "tb_r6_1b".to_string(),
        project_id: pid.to_string(),
        level: ErasureLevel::Session,
        target_id: "sess_bye".to_string(),
        created_at: ts,
        expires_at: None,
    };
    compactor
        .compact_and_gc(Some(pid), &[tb_sess])
        .await
        .unwrap();
    compactor.close().await.unwrap();

    // Simulate ingest_a crash without calling close() or flush_all_l0()
    drop(ingest_a);

    let mut cfg_2 = test_store_config(false, 0);
    cfg_2.l0_shards = 1;
    let ingest2 = AgentTraceStore::open(obj_store.clone(), cfg_2)
        .await
        .unwrap();
    let listed = ingest2
        .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(listed.traces.len(), 1);
    assert_eq!(listed.traces[0].trace_id, "tr_keep");

    ingest2.compact_and_gc(Some(pid), &[]).await.unwrap();
    ingest2.flush_all_l0().await.unwrap();
    ingest2.close().await.unwrap();

    let cold = AgentTraceStore::open(obj_store, test_store_config(true, 0))
        .await
        .unwrap();
    let final_list = cold
        .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(final_list.traces.len(), 1);
    assert_eq!(final_list.traces[0].trace_id, "tr_keep");
    assert!(cold
        .get_trace_detail(pid, "tr_l0p", &[])
        .await
        .unwrap()
        .is_none());
    cold.close().await.unwrap();
}

#[tokio::test]
async fn regression_r6_recompute_rank_non_inflation_and_3way_crdt_associativity() {
    let ts = 1_700_000_000_000i64;

    // 1. Reviewer 1 Finding 2: span-promoted Running -> Error must NOT inflate TraceFieldRanks on flush
    {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(obj_store.clone(), cfg).await.unwrap();
        let pid = "proj_r6_non_inflate";

        let mut hdr_a = make_trace(
            "tr_ni",
            "planner",
            "1.0",
            Some("s_ni"),
            Some("u_ni"),
            ts,
            TraceStatus::Running,
        );
        hdr_a.output = json!({"r": "zzz"});
        hdr_a.metadata = json!({"m": "zzz"});
        hdr_a.error_type = Some("ErrZzz".to_string());
        hdr_a.error_message = Some("msgZzz".to_string());
        let mut sp_a = make_span("tr_ni", "sp_err", None, ts);
        sp_a.status = TraceStatus::Error;
        sp_a.span_type = SpanType::Tool;

        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![hdr_a],
                    spans: vec![sp_a],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let mut hdr_c = make_trace(
            "tr_ni",
            "planner",
            "1.0",
            Some("s_ni"),
            Some("u_ni"),
            ts,
            TraceStatus::Ok,
        );
        hdr_c.output = json!({"r": "aaa"});
        hdr_c.metadata = json!({"m": "aaa"});
        hdr_c.error_type = Some("ErrAaa".to_string());
        hdr_c.error_message = Some("msgAaa".to_string());

        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![hdr_c],
                    spans: vec![],
                },
            )
            .await
            .unwrap();

        // Check mixed Seg + L0, then all-flushed Seg + Seg, then compacted
        for step in 0..3 {
            if step == 1 {
                store.flush_all_l0().await.unwrap();
            } else if step == 2 {
                store.compact_and_gc(Some(pid), &[]).await.unwrap();
            }
            let det = store
                .get_trace_detail(pid, "tr_ni", &[])
                .await
                .unwrap()
                .unwrap();
            assert_eq!(det.trace.status, TraceStatus::Error);
            assert_eq!(det.trace.output, json!({"r": "aaa"}), "step {step}");
            assert_eq!(det.trace.metadata, json!({"m": "aaa"}), "step {step}");
            assert_eq!(
                det.trace.error_type.as_deref(),
                Some("ErrAaa"),
                "step {step}"
            );
            assert_eq!(
                det.trace.error_message.as_deref(),
                Some("msgAaa"),
                "step {step}"
            );
            let listed = store
                .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
                .await
                .unwrap();
            assert_eq!(
                listed.traces[0].error_type.as_deref(),
                Some("ErrAaa"),
                "list_traces step {step}"
            );
        }
        store.close().await.unwrap();
    }

    // 2. probe_b1 & probe_r5_2b: 3-way associativity with intermediate Cancelled/Null & Final/Null
    // across uncompacted segments, mixed L0+segment, AND post-compaction
    let mut hdr_a = make_trace(
        "tr_b1",
        "planner",
        "1.0",
        Some("s_b1"),
        Some("u_b1"),
        ts,
        TraceStatus::Running,
    );
    hdr_a.output = json!({"v": "zzz_running"});
    hdr_a.metadata = json!({"m": "zzz_running"});
    hdr_a.error_type = Some("ErrZzz".to_string());
    hdr_a.error_message = Some("msgZzz".to_string());

    let mut sp_a = make_span("tr_b1", "sp_b1", None, ts);
    sp_a.span_phase = SpanPhase::Start;
    sp_a.status = TraceStatus::Running;
    sp_a.output = json!({"s": "zzz_start"});
    sp_a.metadata = json!({"sm": "zzz_start"});
    sp_a.attributes = json!({"sa": "zzz_start"});
    sp_a.model_params = json!({"mp": "zzz_start"});
    sp_a.finish_reason = Some("fr_zzz".to_string());
    sp_a.error_type = Some("SpErrZzz".to_string());
    sp_a.error_message = Some("sp_msg_zzz".to_string());
    sp_a.error_stack = Some("sp_stk_zzz".to_string());

    let mut hdr_b = make_trace(
        "tr_b1",
        "planner",
        "1.0",
        Some("s_b1"),
        Some("u_b1"),
        ts,
        TraceStatus::Cancelled,
    );
    hdr_b.output = Value::Null;
    hdr_b.metadata = Value::Null;
    hdr_b.error_type = None;
    hdr_b.error_message = None;

    let mut sp_b = make_span("tr_b1", "sp_b1", None, ts);
    sp_b.span_phase = SpanPhase::Final;
    sp_b.status = TraceStatus::Cancelled;
    sp_b.output = Value::Null;
    sp_b.metadata = Value::Null;
    sp_b.attributes = Value::Null;
    sp_b.model_params = Value::Null;
    sp_b.finish_reason = None;
    sp_b.error_type = None;
    sp_b.error_message = None;
    sp_b.error_stack = None;

    let mut hdr_c = make_trace(
        "tr_b1",
        "planner",
        "1.0",
        Some("s_b1"),
        Some("u_b1"),
        ts,
        TraceStatus::Ok,
    );
    hdr_c.output = json!({"v": "aaa_ok"});
    hdr_c.metadata = json!({"m": "aaa_ok"});
    hdr_c.error_type = Some("ErrAaa".to_string());
    hdr_c.error_message = Some("msgAaa".to_string());

    let mut sp_c = make_span("tr_b1", "sp_b1", None, ts);
    sp_c.span_phase = SpanPhase::Update;
    sp_c.status = TraceStatus::Ok;
    sp_c.output = json!({"s": "aaa_update"});
    sp_c.metadata = json!({"sm": "aaa_update"});
    sp_c.attributes = json!({"sa": "aaa_update"});
    sp_c.model_params = json!({"mp": "aaa_update"});
    sp_c.finish_reason = Some("fr_aaa".to_string());
    sp_c.error_type = Some("SpErrAaa".to_string());
    sp_c.error_message = Some("sp_msg_aaa".to_string());
    sp_c.error_stack = Some("sp_stk_aaa".to_string());

    let batches = [
        TraceIngestBatch {
            traces: vec![hdr_a],
            spans: vec![sp_a],
        },
        TraceIngestBatch {
            traces: vec![hdr_b],
            spans: vec![sp_b],
        },
        TraceIngestBatch {
            traces: vec![hdr_c],
            spans: vec![sp_c],
        },
    ];

    // (A+B) flushed/compacted + C vs A + (B+C) flushed/compacted
    let mut results = Vec::new();
    for (first_pair, last_one, compact_pair) in [
        ([0, 1], 2, false),
        ([1, 2], 0, false),
        ([0, 1], 2, true),
        ([1, 2], 0, true),
    ] {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
        let pid = "proj_r6_b1";

        for idx in first_pair {
            store.ingest_batch(pid, batches[idx].clone()).await.unwrap();
            store.flush_all_l0().await.unwrap();
        }
        if compact_pair {
            store.compact_and_gc(Some(pid), &[]).await.unwrap();
        }
        // Check with C in L0 (mixed L0+Seg) AND after flushing C (Seg+Seg) AND after full compaction
        store
            .ingest_batch(pid, batches[last_one].clone())
            .await
            .unwrap();
        let det_l0 = store
            .get_trace_detail(pid, "tr_b1", &[])
            .await
            .unwrap()
            .unwrap();
        let list_l0 = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();

        store.flush_all_l0().await.unwrap();
        let det_seg = store
            .get_trace_detail(pid, "tr_b1", &[])
            .await
            .unwrap()
            .unwrap();
        let list_seg = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();

        store.compact_and_gc(Some(pid), &[]).await.unwrap();
        let det_comp = store
            .get_trace_detail(pid, "tr_b1", &[])
            .await
            .unwrap()
            .unwrap();
        let list_comp = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();

        assert_eq!(det_l0.trace, det_seg.trace);
        assert_eq!(det_seg.trace, det_comp.trace);
        assert_eq!(det_l0.spans, det_seg.spans);
        assert_eq!(det_seg.spans, det_comp.spans);
        assert_eq!(list_l0.traces[0], list_seg.traces[0]);
        assert_eq!(list_seg.traces[0], list_comp.traces[0]);

        assert_eq!(det_comp.trace.status, TraceStatus::Cancelled);
        assert_eq!(det_comp.trace.output, json!({"v": "aaa_ok"}));
        assert_eq!(det_comp.trace.metadata, json!({"m": "aaa_ok"}));
        assert_eq!(det_comp.trace.error_type.as_deref(), Some("ErrAaa"));
        assert_eq!(det_comp.trace.error_message.as_deref(), Some("msgAaa"));
        assert_eq!(list_comp.traces[0].error_type.as_deref(), Some("ErrAaa"));

        let sp = &det_comp.spans[0];
        assert_eq!(sp.span_phase, SpanPhase::Final);
        assert_eq!(sp.output, json!({"s": "aaa_update"}));
        assert_eq!(sp.metadata, json!({"sm": "aaa_update"}));
        assert_eq!(sp.attributes, json!({"sa": "aaa_update"}));
        assert_eq!(sp.model_params, json!({"mp": "aaa_update"}));
        assert_eq!(sp.finish_reason.as_deref(), Some("fr_aaa"));
        assert_eq!(sp.error_type.as_deref(), Some("SpErrAaa"));
        assert_eq!(sp.error_message.as_deref(), Some("sp_msg_aaa"));
        assert_eq!(sp.error_stack.as_deref(), Some("sp_stk_aaa"));

        results.push((det_comp.trace, det_comp.spans, list_comp.traces[0].clone()));
        store.close().await.unwrap();
    }
    for r in &results[1..] {
        assert_eq!(r, &results[0]);
    }
}

#[tokio::test]
async fn regression_r6_failure_source_depth_split_across_segments_l0_and_compaction() {
    let ts = 1_700_000_000_000i64;

    let mut sp_shallow_llm = make_span("tr_b2", "sp_llm", None, ts);
    sp_shallow_llm.depth = 0;
    sp_shallow_llm.span_type = SpanType::Llm;
    sp_shallow_llm.status = TraceStatus::Error;

    let mut sp_deep_tool = make_span("tr_b2", "sp_tool", Some("sp_llm"), ts + 10);
    sp_deep_tool.depth = 1;
    sp_deep_tool.span_type = SpanType::Tool;
    sp_deep_tool.tool_name = Some("calc".to_string());
    sp_deep_tool.model = None;
    sp_deep_tool.provider = None;
    sp_deep_tool.status = TraceStatus::Error;

    let hdr_base = make_trace(
        "tr_b2",
        "planner",
        "1.0",
        Some("sess_b2"),
        Some("u_b2"),
        ts,
        TraceStatus::Ok,
    );

    async fn check_all_apis(store: &AgentTraceStore, pid: &str, expected: FailureSource) {
        let det = store
            .get_trace_detail(pid, "tr_b2", &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(det.trace.failure_source, Some(expected), "get_trace_detail");

        let listed = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(
            listed.traces[0].failure_source,
            Some(expected),
            "list_traces"
        );

        let qa = store
            .query_analytics(pid, &AnalyticsFilter::default(), &[])
            .await
            .unwrap();
        assert_eq!(
            qa.failure_sources.get(expected.as_str()).copied(),
            Some(1),
            "query_analytics: {:?}",
            qa.failure_sources
        );

        let sess = store
            .get_session_detail(pid, "sess_b2", &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            sess.traces[0].failure_source,
            Some(expected),
            "get_session_detail"
        );

        let cmp = store
            .compare_groups(
                pid,
                &CompareFilter {
                    baseline_agent_version: Some("1.0".to_string()),
                    candidate_agent_version: Some("2.0".to_string()),
                    ..Default::default()
                },
                &[],
            )
            .await
            .unwrap();
        assert_eq!(cmp.baseline.failures, 1);
    }

    // Test: L0-only, Seg+Seg (both orders), Seg+L0 (both orders), and post-compaction
    for (first_llm, flush_second) in [(true, false), (false, false), (true, true), (false, true)] {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
        let pid = "proj_r6_b2";

        let (first_sp, second_sp) = if first_llm {
            (sp_shallow_llm.clone(), sp_deep_tool.clone())
        } else {
            (sp_deep_tool.clone(), sp_shallow_llm.clone())
        };
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![hdr_base.clone()],
                    spans: vec![first_sp],
                },
            )
            .await
            .unwrap();
        // Verify L0-only after adding second span if !flush_second, or Seg + L0 after flushing first
        if flush_second {
            store.flush_all_l0().await.unwrap();
        }
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![second_sp],
                },
            )
            .await
            .unwrap();
        // Checks L0-only (when !flush_second) or Seg + L0 (when flush_second)
        check_all_apis(&store, pid, FailureSource::Tool).await;

        store.flush_all_l0().await.unwrap();
        // Checks Seg + Seg
        check_all_apis(&store, pid, FailureSource::Tool).await;

        store.compact_and_gc(Some(pid), &[]).await.unwrap();
        // Checks post-compaction
        check_all_apis(&store, pid, FailureSource::Tool).await;

        store.close().await.unwrap();
    }

    // Explicit failure_source override tests across split segments:
    // (1) Explicit Prompt (rank 3) + split error spans (Tool rank 5) -> Tool
    // (2) Explicit Model (rank 6) + split error spans (Tool rank 5) -> Model
    for (explicit_fs, expected_fs) in [
        (FailureSource::Prompt, FailureSource::Tool),
        (FailureSource::Model, FailureSource::Model),
    ] {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
        let pid = "proj_r6_b2_exp";

        let mut hdr_exp = hdr_base.clone();
        hdr_exp.failure_source = Some(explicit_fs);
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![hdr_exp],
                    spans: vec![sp_deep_tool.clone()],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![],
                },
            )
            .await
            .unwrap();
        check_all_apis(&store, pid, expected_fs).await;
        store.compact_and_gc(Some(pid), &[]).await.unwrap();
        check_all_apis(&store, pid, expected_fs).await;
        store.close().await.unwrap();
    }

    // Equal-(depth, start_ts_ms) tie-break determinism in classify_failure_source
    let mut sp_tie_a = make_span("tr_tie", "sp_a", None, ts);
    sp_tie_a.depth = 1;
    sp_tie_a.span_type = SpanType::Tool;
    sp_tie_a.status = TraceStatus::Error;
    let mut sp_tie_b = make_span("tr_tie", "sp_b", None, ts);
    sp_tie_b.depth = 1;
    sp_tie_b.span_type = SpanType::Llm;
    sp_tie_b.status = TraceStatus::Error;
    assert_eq!(
        pulse_agent::stats::classify_failure_source(&[sp_tie_a.clone(), sp_tie_b.clone()]),
        FailureSource::Tool
    );
    assert_eq!(
        pulse_agent::stats::classify_failure_source(&[sp_tie_b, sp_tie_a]),
        FailureSource::Tool
    );
}

#[tokio::test]
async fn regression_r6_plss_crdt_ranks_hostile_and_v1_compat() {
    let ts = 1_700_000_000_000i64;
    let tr = make_trace(
        "tr_hostile_ranks",
        "planner",
        "1.0",
        Some("sess_h"),
        Some("u_h"),
        ts,
        TraceStatus::Ok,
    );
    let sp = make_span("tr_hostile_ranks", "sp_h", None, ts);
    let (base_bytes, tail_offset, zone) =
        SegmentWriter::build_plss_bytes(vec![(tr.clone(), vec![sp])]).unwrap();

    fn patch_section_and_dir(
        buf: &mut [u8],
        target_tag: SectionTag,
        clear_flags: u16,
        new_section_crc: Option<u32>,
    ) {
        let footer = PlssFooter::decode(&buf[buf.len() - 24..]).unwrap();
        let dir_start = footer.dir_offset as usize;
        let dir_end = dir_start + footer.dir_len as usize;
        let mut entries = Vec::new();
        for c in buf[dir_start..dir_end].chunks_exact(PLSS_DIR_ENTRY_LEN) {
            entries.push(SectionDirEntry::decode(c).unwrap());
        }
        for e in &mut entries {
            if e.tag == target_tag as u16 {
                e.flags &= !clear_flags;
                if let Some(crc) = new_section_crc {
                    e.crc32 = crc;
                }
            }
        }
        let mut new_dir = Vec::new();
        for e in &entries {
            e.encode_into(&mut new_dir);
        }
        buf[dir_start..dir_end].copy_from_slice(&new_dir);
        let new_crc = crc32fast::hash(&buf[dir_start..dir_end]);
        let blen = buf.len();
        buf[blen - 12..blen - 8].copy_from_slice(&new_crc.to_le_bytes());
    }

    // 1. Clearing SEC_FLAG_CRDT_RANKS on Tag 0x02 or Tag 0x04 in a v2 segment must error cleanly
    for tag in [SectionTag::TraceSummary, SectionTag::SpanTree] {
        let mut mutated = base_bytes.clone();
        patch_section_and_dir(&mut mutated, tag, SEC_FLAG_CRDT_RANKS, None);
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = ObjectPath::from("agent_segments/proj_hr/2023-11-14/shard_00/seg_h.plss");
        os.put(&path, PutPayload::from_bytes(Bytes::from(mutated.clone())))
            .await
            .unwrap();
        let cache =
            pulse_agent::store::cache::AgentSegmentCache::open(None, 4 * 1024 * 1024, None, 0)
                .await
                .unwrap();
        let reader = pulse_agent::store::segment::PlssReader::new(os, cache.clone());
        let seg = pulse_agent::store::manifest::SegmentDescriptor {
            segment_id: format!("seg_h_{}", tag as u16),
            object_path: path.as_ref().to_string(),
            date: "2023-11-14".into(),
            shard_id: 0,
            level: 0,
            file_size: mutated.len() as u64,
            tail_offset,
            zone_map: zone.clone(),
            trace_count: 1,
            created_at_ms: 1_000,
        };
        let tail = reader.read_eof_tail(&seg).await.unwrap();
        if tag == SectionTag::TraceSummary {
            assert!(reader.read_trace_summaries(&seg, &tail).await.is_err());
        } else {
            assert!(reader
                .read_trace_ranges(&seg, &tail, "tr_hostile_ranks")
                .await
                .is_err());
            assert!(reader.read_all_traces_and_spans(&seg).await.is_err());
        }
        cache.close().await.unwrap();
    }

    fn replace_section_bytes(
        base: &[u8],
        old_tail_offset: u64,
        target_tag: SectionTag,
        new_sec: &[u8],
        clear_flags: u16,
    ) -> (Vec<u8>, u64) {
        let footer = PlssFooter::decode(&base[base.len() - 24..]).unwrap();
        let dir_start = footer.dir_offset as usize;
        let dir_end = dir_start + footer.dir_len as usize;
        let mut entries: Vec<_> = base[dir_start..dir_end]
            .chunks_exact(PLSS_DIR_ENTRY_LEN)
            .map(|c| SectionDirEntry::decode(c).unwrap())
            .collect();
        let idx = entries
            .iter()
            .position(|e| e.tag == target_tag as u16)
            .unwrap();
        let old_off = entries[idx].offset as usize;
        let old_len = entries[idx].len as usize;
        let delta = new_sec.len() as isize - old_len as isize;

        let mut out = Vec::with_capacity((base.len() as isize + delta) as usize);
        out.extend_from_slice(&base[..old_off]);
        out.extend_from_slice(new_sec);
        out.extend_from_slice(&base[old_off + old_len..dir_start]);

        entries[idx].len = new_sec.len() as u32;
        entries[idx].crc32 = crc32fast::hash(new_sec);
        entries[idx].flags &= !clear_flags;
        for e in entries.iter_mut().skip(idx + 1) {
            e.offset = (e.offset as isize + delta) as u64;
        }

        let new_dir_offset = out.len() as u64;
        let mut new_dir = Vec::new();
        for e in &entries {
            e.encode_into(&mut new_dir);
        }
        out.extend_from_slice(&new_dir);
        let new_footer = PlssFooter {
            dir_offset: new_dir_offset,
            dir_len: footer.dir_len,
            dir_crc32: crc32fast::hash(&new_dir),
            version_flags: footer.version_flags,
            magic: footer.magic,
        };
        new_footer.encode_into(&mut out);
        let new_tail_offset = (old_tail_offset as isize + delta) as u64;
        (out, new_tail_offset)
    }

    // 2. Hostile enum / rank bounds (> 4 / > 6 / > 2) in Tag 0x02 & Tag 0x04 rejected
    {
        let footer = PlssFooter::decode(&base_bytes[base_bytes.len() - 24..]).unwrap();
        let dir_start = footer.dir_offset as usize;
        let dir_end = dir_start + footer.dir_len as usize;
        let entries: Vec<_> = base_bytes[dir_start..dir_end]
            .chunks_exact(PLSS_DIR_ENTRY_LEN)
            .map(|c| SectionDirEntry::decode(c).unwrap())
            .collect();
        let sec02 = entries
            .iter()
            .find(|e| e.tag == SectionTag::TraceSummary as u16)
            .unwrap();
        // 2a. Corrupt Tag 0x02 output_rank = 99
        let s02 = &base_bytes[sec02.offset as usize..(sec02.offset as usize + sec02.len as usize)];
        let comp_len = u32::from_le_bytes(s02[8..12].try_into().unwrap()) as usize;
        let mut raw_chunk = zstd::stream::decode_all(&s02[20..20 + comp_len]).unwrap();
        let rank_off = raw_chunk.len() - 101 - 4 - 13; // output_rank (+2 in 15-byte block before crdt_cnt=1*101B)
        assert_eq!(raw_chunk[rank_off], 1);
        raw_chunk[rank_off] = 99;
        let new_comp = zstd::stream::encode_all(raw_chunk.as_slice(), 3).unwrap();
        let mut new_sec02 = Vec::new();
        new_sec02.extend_from_slice(&1u32.to_le_bytes());
        new_sec02.extend_from_slice(&(raw_chunk.len() as u32).to_le_bytes());
        new_sec02.extend_from_slice(&(new_comp.len() as u32).to_le_bytes());
        new_sec02.extend_from_slice(&crc32fast::hash(&new_comp).to_le_bytes());
        new_sec02.push(1); // CODEC_ZSTD_RAW
        new_sec02.extend_from_slice(&[0u8; 3]);
        new_sec02.extend_from_slice(&new_comp);
        let (mutated, new_tail_off) = replace_section_bytes(
            &base_bytes,
            tail_offset,
            SectionTag::TraceSummary,
            &new_sec02,
            0,
        );
        {
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path = ObjectPath::from("agent_segments/proj_hr/2023-11-14/shard_00/bad_rank.plss");
            os.put(&path, PutPayload::from_bytes(Bytes::from(mutated.clone())))
                .await
                .unwrap();
            let cache =
                pulse_agent::store::cache::AgentSegmentCache::open(None, 4 * 1024 * 1024, None, 0)
                    .await
                    .unwrap();
            let reader = pulse_agent::store::segment::PlssReader::new(os, cache.clone());
            let seg = pulse_agent::store::manifest::SegmentDescriptor {
                segment_id: "bad_rank".into(),
                object_path: path.as_ref().to_string(),
                date: "2023-11-14".into(),
                shard_id: 0,
                level: 0,
                file_size: mutated.len() as u64,
                tail_offset: new_tail_off,
                zone_map: zone.clone(),
                trace_count: 1,
                created_at_ms: 1_000,
            };
            let tail = reader.read_eof_tail(&seg).await.unwrap();
            let err = reader.read_trace_summaries(&seg, &tail).await.unwrap_err();
            assert!(matches!(err, StoreError::Corrupted(_)), "got {err:?}");
            cache.close().await.unwrap();
        }

        // 2b. Corrupt Tag 0x04 SpanFieldRanks phase (> 2) -> rejected as Corrupted
        let sec04 = entries
            .iter()
            .find(|e| e.tag == SectionTag::SpanTree as u16)
            .unwrap();
        let s04 = &base_bytes[sec04.offset as usize..(sec04.offset as usize + sec04.len as usize)];
        let idx_bytes = u32::from_le_bytes(s04[8..12].try_into().unwrap()) as usize;
        let dir04_base = 12 + idx_bytes - 24;
        let comp_len4 =
            u32::from_le_bytes(s04[dir04_base + 8..dir04_base + 12].try_into().unwrap()) as usize;
        let blk_off = 12 + idx_bytes;
        let mut raw4 = {
            let mut dec = zstd::stream::Decoder::with_dictionary(
                &s04[blk_off..blk_off + comp_len4],
                agent_zstd_dict(),
            )
            .unwrap();
            let mut out = Vec::new();
            std::io::Read::read_to_end(&mut dec, &mut out).unwrap();
            out
        };
        // Last span's SpanFieldRanks has 4 set bits (output, attributes, model_params, finish_reason):
        // 2B rank_mask + 4 * 9B = 38B at end of raw4; first slot phase byte is at raw4.len() - 36.
        let orig_mask = u16::from_le_bytes([raw4[raw4.len() - 38], raw4[raw4.len() - 37]]);
        assert_eq!(orig_mask, 0x001B);
        let phase_off = raw4.len() - 36;
        assert_eq!(raw4[phase_off], SpanPhase::Final.to_u8());
        raw4[phase_off] = 9; // invalid SpanPhase > 2
        let new_comp4 = {
            let mut enc =
                zstd::stream::Encoder::with_dictionary(Vec::new(), 3, agent_zstd_dict()).unwrap();
            std::io::Write::write_all(&mut enc, &raw4).unwrap();
            enc.finish().unwrap()
        };
        let mut new_sec04 = s04[..12 + idx_bytes].to_vec();
        new_sec04[dir04_base + 8..dir04_base + 12]
            .copy_from_slice(&(new_comp4.len() as u32).to_le_bytes());
        new_sec04[dir04_base + 12..dir04_base + 16]
            .copy_from_slice(&(raw4.len() as u32).to_le_bytes());
        new_sec04[dir04_base + 16..dir04_base + 20]
            .copy_from_slice(&crc32fast::hash(&new_comp4).to_le_bytes());
        new_sec04.extend_from_slice(&new_comp4);
        let (mutated4, new_tail_off4) = replace_section_bytes(
            &base_bytes,
            tail_offset,
            SectionTag::SpanTree,
            &new_sec04,
            0,
        );
        {
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path =
                ObjectPath::from("agent_segments/proj_hr/2023-11-14/shard_00/bad_sp_rank.plss");
            os.put(&path, PutPayload::from_bytes(Bytes::from(mutated4.clone())))
                .await
                .unwrap();
            let cache =
                pulse_agent::store::cache::AgentSegmentCache::open(None, 4 * 1024 * 1024, None, 0)
                    .await
                    .unwrap();
            let reader = pulse_agent::store::segment::PlssReader::new(os, cache.clone());
            let seg = pulse_agent::store::manifest::SegmentDescriptor {
                segment_id: "bad_sp_rank".into(),
                object_path: path.as_ref().to_string(),
                date: "2023-11-14".into(),
                shard_id: 0,
                level: 0,
                file_size: mutated4.len() as u64,
                tail_offset: new_tail_off4,
                zone_map: zone.clone(),
                trace_count: 1,
                created_at_ms: 1_000,
            };
            let tail = reader.read_eof_tail(&seg).await.unwrap();
            let err = reader
                .read_trace_ranges(&seg, &tail, "tr_hostile_ranks")
                .await
                .unwrap_err();
            assert!(matches!(err, StoreError::Corrupted(_)), "got {err:?}");
            cache.close().await.unwrap();
        }

        // 2c. Corrupt Tag 0x04 rank_mask unused bit (0x0200) -> rejected as Corrupted
        raw4[phase_off] = SpanPhase::Final.to_u8();
        let bad_mask = (orig_mask | 0x0200).to_le_bytes();
        let mask_off = raw4.len() - 38;
        raw4[mask_off] = bad_mask[0];
        raw4[mask_off + 1] = bad_mask[1];
        let bad_mask_comp4 = {
            let mut enc =
                zstd::stream::Encoder::with_dictionary(Vec::new(), 3, agent_zstd_dict()).unwrap();
            std::io::Write::write_all(&mut enc, &raw4).unwrap();
            enc.finish().unwrap()
        };
        let mut bad_mask_sec04 = s04[..12 + idx_bytes].to_vec();
        bad_mask_sec04[dir04_base + 8..dir04_base + 12]
            .copy_from_slice(&(bad_mask_comp4.len() as u32).to_le_bytes());
        bad_mask_sec04[dir04_base + 12..dir04_base + 16]
            .copy_from_slice(&(raw4.len() as u32).to_le_bytes());
        bad_mask_sec04[dir04_base + 16..dir04_base + 20]
            .copy_from_slice(&crc32fast::hash(&bad_mask_comp4).to_le_bytes());
        bad_mask_sec04.extend_from_slice(&bad_mask_comp4);
        let (mutated_mask4, new_tail_mask4) = replace_section_bytes(
            &base_bytes,
            tail_offset,
            SectionTag::SpanTree,
            &bad_mask_sec04,
            0,
        );
        {
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path =
                ObjectPath::from("agent_segments/proj_hr/2023-11-14/shard_00/bad_mask4.plss");
            os.put(
                &path,
                PutPayload::from_bytes(Bytes::from(mutated_mask4.clone())),
            )
            .await
            .unwrap();
            let cache =
                pulse_agent::store::cache::AgentSegmentCache::open(None, 4 * 1024 * 1024, None, 0)
                    .await
                    .unwrap();
            let reader = pulse_agent::store::segment::PlssReader::new(os, cache.clone());
            let seg = pulse_agent::store::manifest::SegmentDescriptor {
                segment_id: "bad_mask4".into(),
                object_path: path.as_ref().to_string(),
                date: "2023-11-14".into(),
                shard_id: 0,
                level: 0,
                file_size: mutated_mask4.len() as u64,
                tail_offset: new_tail_mask4,
                zone_map: zone.clone(),
                trace_count: 1,
                created_at_ms: 1_000,
            };
            let tail = reader.read_eof_tail(&seg).await.unwrap();
            let err = reader
                .read_trace_ranges(&seg, &tail, "tr_hostile_ranks")
                .await
                .unwrap_err();
            assert!(matches!(err, StoreError::Corrupted(_)), "got {err:?}");
            cache.close().await.unwrap();
        }
    }

    // 3. Forged v1 compatibility: 0-span trace with 6 rank bytes stripped & SEC_FLAG_CRDT_RANKS unset
    {
        let mut tr_v1 = tr.clone();
        tr_v1.input = Value::Null;
        tr_v1.output = Value::Null;
        tr_v1.metadata = Value::Null;
        tr_v1.error_message = None;
        let (v2_bytes, v2_tail_off, v2_zone) =
            SegmentWriter::build_plss_bytes(vec![(tr_v1, vec![])]).unwrap();
        let footer = PlssFooter::decode(&v2_bytes[v2_bytes.len() - 24..]).unwrap();
        let dir_start = footer.dir_offset as usize;
        let dir_end = dir_start + footer.dir_len as usize;
        let entries: Vec<_> = v2_bytes[dir_start..dir_end]
            .chunks_exact(PLSS_DIR_ENTRY_LEN)
            .map(|c| SectionDirEntry::decode(c).unwrap())
            .collect();

        // Strip 15 rank bytes from Tag 0x02 (located immediately before trailing 4-byte crdt_cnt=0)
        let new_sec02 = {
            let e02 = entries
                .iter()
                .find(|e| e.tag == SectionTag::TraceSummary as u16)
                .unwrap();
            let s02 = &v2_bytes[e02.offset as usize..(e02.offset as usize + e02.len as usize)];
            let comp_len = u32::from_le_bytes(s02[8..12].try_into().unwrap()) as usize;
            let mut raw = zstd::stream::decode_all(&s02[20..20 + comp_len]).unwrap();
            let cut = raw.len() - 19;
            raw.drain(cut..cut + 15);
            let new_comp = zstd::stream::encode_all(raw.as_slice(), 3).unwrap();
            let mut new_sec = Vec::new();
            new_sec.extend_from_slice(&1u32.to_le_bytes());
            new_sec.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            new_sec.extend_from_slice(&(new_comp.len() as u32).to_le_bytes());
            new_sec.extend_from_slice(&crc32fast::hash(&new_comp).to_le_bytes());
            new_sec.push(1);
            new_sec.extend_from_slice(&[0u8; 3]);
            new_sec.extend_from_slice(&new_comp);
            new_sec
        };
        let (v1_step1, v1_tail1) = replace_section_bytes(
            &v2_bytes,
            v2_tail_off,
            SectionTag::TraceSummary,
            &new_sec02,
            SEC_FLAG_CRDT_RANKS,
        );

        // Strip 29 explicit meta bytes (7B ranks + 11B "gpt-4o" + 11B "openai") from Tag 0x04 (before trailing 4B span_cnt=0)
        let new_sec04 = {
            let e04 = entries
                .iter()
                .find(|e| e.tag == SectionTag::SpanTree as u16)
                .unwrap();
            let s04 = &v2_bytes[e04.offset as usize..(e04.offset as usize + e04.len as usize)];
            let idx_bytes = u32::from_le_bytes(s04[8..12].try_into().unwrap()) as usize;
            let dir04_base = 12 + idx_bytes - 24;
            let comp_len =
                u32::from_le_bytes(s04[dir04_base + 8..dir04_base + 12].try_into().unwrap())
                    as usize;
            let blk_off = 12 + idx_bytes;
            let mut raw = {
                let mut dec = zstd::stream::Decoder::with_dictionary(
                    &s04[blk_off..blk_off + comp_len],
                    agent_zstd_dict(),
                )
                .unwrap();
                let mut out = Vec::new();
                std::io::Read::read_to_end(&mut dec, &mut out).unwrap();
                out
            };
            let cut = raw.len() - 33;
            raw.drain(cut..cut + 29);
            let new_comp = {
                let mut enc =
                    zstd::stream::Encoder::with_dictionary(Vec::new(), 3, agent_zstd_dict())
                        .unwrap();
                std::io::Write::write_all(&mut enc, &raw).unwrap();
                enc.finish().unwrap()
            };
            let mut new_sec = s04[..12 + idx_bytes].to_vec();
            new_sec[dir04_base + 8..dir04_base + 12]
                .copy_from_slice(&(new_comp.len() as u32).to_le_bytes());
            new_sec[dir04_base + 12..dir04_base + 16]
                .copy_from_slice(&(raw.len() as u32).to_le_bytes());
            new_sec[dir04_base + 16..dir04_base + 20]
                .copy_from_slice(&crc32fast::hash(&new_comp).to_le_bytes());
            new_sec.extend_from_slice(&new_comp);
            new_sec
        };
        let (v1_bytes, v1_tail_off) = replace_section_bytes(
            &v1_step1,
            v1_tail1,
            SectionTag::SpanTree,
            &new_sec04,
            SEC_FLAG_CRDT_RANKS,
        );

        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let seg_path = ObjectPath::from("agent_segments/proj_v1/2023-11-14/shard_00/v1.plss");
        os.put(
            &seg_path,
            PutPayload::from_bytes(Bytes::from(v1_bytes.clone())),
        )
        .await
        .unwrap();
        let ms = ManifestStore::new("proj_v1", os.clone(), 0).unwrap();
        ms.commit_with_cas(|m| {
            m.segments
                .push(pulse_agent::store::manifest::SegmentDescriptor {
                    segment_id: "v1".into(),
                    object_path: seg_path.as_ref().to_string(),
                    date: "2023-11-14".into(),
                    shard_id: 0,
                    level: 0,
                    file_size: v1_bytes.len() as u64,
                    tail_offset: v1_tail_off,
                    zone_map: v2_zone.clone(),
                    trace_count: 1,
                    created_at_ms: 1_000,
                });
            Ok(true)
        })
        .await
        .unwrap();

        let store = AgentTraceStore::open(os, test_store_config(false, 0))
            .await
            .unwrap();
        let listed = store
            .list_traces("proj_v1", &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(listed.traces.len(), 1);
        assert_eq!(listed.traces[0].trace_id, "tr_hostile_ranks");
        let det = store
            .get_trace_detail("proj_v1", "tr_hostile_ranks", &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(det.trace.trace_id, "tr_hostile_ranks");
        store.compact_and_gc(Some("proj_v1"), &[]).await.unwrap();
        store.close().await.unwrap();
    }

    // 3b. Forged 1-span v1 compatibility (SEC_FLAG_CRDT_RANKS unset, crdt_cnt == 1 at 84B)
    {
        let mut tr_v1 = tr.clone();
        tr_v1.input = Value::Null;
        tr_v1.output = Value::Null;
        tr_v1.metadata = Value::Null;
        tr_v1.error_message = None;
        let mut sp_v1 = make_span("tr_hostile_ranks", "sp_h", None, ts);
        sp_v1.input = Value::Null;
        sp_v1.output = Value::Null;
        sp_v1.attributes = Value::Null;
        sp_v1.metadata = Value::Null;
        sp_v1.model_params = Value::Null;
        sp_v1.events.clear();
        sp_v1.finish_reason = None;
        sp_v1.error_type = None;
        sp_v1.error_message = None;
        sp_v1.error_stack = None;
        sp_v1.tool_name = None;
        let (v2_1sp_bytes, v2_1sp_tail, v2_1sp_zone) =
            SegmentWriter::build_plss_bytes(vec![(tr_v1, vec![sp_v1])]).unwrap();

        let footer = PlssFooter::decode(&v2_1sp_bytes[v2_1sp_bytes.len() - 24..]).unwrap();
        let dir_start = footer.dir_offset as usize;
        let dir_end = dir_start + footer.dir_len as usize;
        let entries: Vec<_> = v2_1sp_bytes[dir_start..dir_end]
            .chunks_exact(PLSS_DIR_ENTRY_LEN)
            .map(|c| SectionDirEntry::decode(c).unwrap())
            .collect();

        // Strip trailing 17B from SpanCrdtDelta (101B -> 84B) and 15 trace rank bytes before crdt_cnt=1
        let new_sec02 = {
            let e02 = entries
                .iter()
                .find(|e| e.tag == SectionTag::TraceSummary as u16)
                .unwrap();
            let s02 = &v2_1sp_bytes[e02.offset as usize..(e02.offset as usize + e02.len as usize)];
            let comp_len = u32::from_le_bytes(s02[8..12].try_into().unwrap()) as usize;
            let mut raw = zstd::stream::decode_all(&s02[20..20 + comp_len]).unwrap();
            raw.truncate(raw.len() - 17); // strip tool_name_str + phase + ts + span_name_str
            let cut = raw.len() - 84 - 4 - 15;
            assert_eq!(&raw[cut + 15..cut + 19], &1u32.to_le_bytes());
            raw.drain(cut..cut + 15);
            let new_comp = zstd::stream::encode_all(raw.as_slice(), 3).unwrap();
            let mut new_sec = Vec::new();
            new_sec.extend_from_slice(&1u32.to_le_bytes());
            new_sec.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            new_sec.extend_from_slice(&(new_comp.len() as u32).to_le_bytes());
            new_sec.extend_from_slice(&crc32fast::hash(&new_comp).to_le_bytes());
            new_sec.push(1);
            new_sec.extend_from_slice(&[0u8; 3]);
            new_sec.extend_from_slice(&new_comp);
            new_sec
        };
        let (v1_step1, v1_tail1) = replace_section_bytes(
            &v2_1sp_bytes,
            v2_1sp_tail,
            SectionTag::TraceSummary,
            &new_sec02,
            SEC_FLAG_CRDT_RANKS,
        );

        // Strip 2B u16 rank_mask (0 set bits) from end of raw04 and 29 trace rank+explicit bytes before span_cnt=1 + "sp_h"
        let new_sec04 = {
            let e04 = entries
                .iter()
                .find(|e| e.tag == SectionTag::SpanTree as u16)
                .unwrap();
            let s04 = &v2_1sp_bytes[e04.offset as usize..(e04.offset as usize + e04.len as usize)];
            let idx_bytes = u32::from_le_bytes(s04[8..12].try_into().unwrap()) as usize;
            let dir04_base = 12 + idx_bytes - 24;
            let comp_len =
                u32::from_le_bytes(s04[dir04_base + 8..dir04_base + 12].try_into().unwrap())
                    as usize;
            let blk_off = 12 + idx_bytes;
            let mut raw = {
                let mut dec = zstd::stream::Decoder::with_dictionary(
                    &s04[blk_off..blk_off + comp_len],
                    agent_zstd_dict(),
                )
                .unwrap();
                let mut out = Vec::new();
                std::io::Read::read_to_end(&mut dec, &mut out).unwrap();
                out
            };
            assert_eq!(&raw[raw.len() - 2..], &[0u8, 0u8]);
            raw.truncate(raw.len() - 2); // strip 2B u16 rank_mask
            let marker: [u8; 12] = [1, 0, 0, 0, 4, 0, 0, 0, b's', b'p', b'_', b'h'];
            let marker_pos = raw
                .windows(marker.len())
                .position(|w| w == marker)
                .expect("span_cnt=1 + sp_h marker");
            raw.drain(marker_pos - 29..marker_pos);
            let new_comp = {
                let mut enc =
                    zstd::stream::Encoder::with_dictionary(Vec::new(), 3, agent_zstd_dict())
                        .unwrap();
                std::io::Write::write_all(&mut enc, &raw).unwrap();
                enc.finish().unwrap()
            };
            let mut new_sec = s04[..12 + idx_bytes].to_vec();
            new_sec[dir04_base + 8..dir04_base + 12]
                .copy_from_slice(&(new_comp.len() as u32).to_le_bytes());
            new_sec[dir04_base + 12..dir04_base + 16]
                .copy_from_slice(&(raw.len() as u32).to_le_bytes());
            new_sec[dir04_base + 16..dir04_base + 20]
                .copy_from_slice(&crc32fast::hash(&new_comp).to_le_bytes());
            new_sec.extend_from_slice(&new_comp);
            new_sec
        };
        let (v1_bytes, v1_tail_off) = replace_section_bytes(
            &v1_step1,
            v1_tail1,
            SectionTag::SpanTree,
            &new_sec04,
            SEC_FLAG_CRDT_RANKS,
        );

        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let seg_path = ObjectPath::from("agent_segments/proj_v1_1sp/2023-11-14/shard_00/v1.plss");
        os.put(
            &seg_path,
            PutPayload::from_bytes(Bytes::from(v1_bytes.clone())),
        )
        .await
        .unwrap();
        let ms = ManifestStore::new("proj_v1_1sp", os.clone(), 0).unwrap();
        ms.commit_with_cas(|m| {
            m.segments
                .push(pulse_agent::store::manifest::SegmentDescriptor {
                    segment_id: "v1_1sp".into(),
                    object_path: seg_path.as_ref().to_string(),
                    date: "2023-11-14".into(),
                    shard_id: 0,
                    level: 0,
                    file_size: v1_bytes.len() as u64,
                    tail_offset: v1_tail_off,
                    zone_map: v2_1sp_zone.clone(),
                    trace_count: 1,
                    created_at_ms: 1_000,
                });
            Ok(true)
        })
        .await
        .unwrap();

        let store = AgentTraceStore::open(os, test_store_config(false, 0))
            .await
            .unwrap();
        let listed = store
            .list_traces(
                "proj_v1_1sp",
                &TraceListFilter::default(),
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(listed.traces.len(), 1);
        assert_eq!(listed.traces[0].span_count, 1);
        let det = store
            .get_trace_detail("proj_v1_1sp", "tr_hostile_ranks", &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(det.spans.len(), 1);
        assert_eq!(det.spans[0].span_id, "sp_h");
        let qa = store
            .query_analytics("proj_v1_1sp", &AnalyticsFilter::default(), &[])
            .await
            .unwrap();
        assert_eq!(qa.total_executions, 1);
        store
            .compact_and_gc(Some("proj_v1_1sp"), &[])
            .await
            .unwrap();
        store.close().await.unwrap();
    }

    // 4. min_cost_usd = f64::NAN and max_cost_usd = f64::NAN across L0, single-segment, and multi-segment
    {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(obj_store, cfg).await.unwrap();
        let pid = "proj_r6_nan";

        let mut tr1 = make_trace("tr_n1", "ag", "1.0", None, None, ts, TraceStatus::Ok);
        tr1.total_cost_nanos = 5_000_000;
        let mut sp1 = make_span("tr_n1", "sp_1", None, ts);
        sp1.cost_nanos = 5_000_000;
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr1],
                    spans: vec![sp1],
                },
            )
            .await
            .unwrap();

        let nan_min_filter = TraceListFilter {
            min_cost_usd: Some(f64::NAN),
            max_cost_usd: Some(f64::NAN),
            ..Default::default()
        };
        assert_eq!(
            store
                .list_traces(pid, &nan_min_filter, &HashMap::new(), &[])
                .await
                .unwrap()
                .traces
                .len(),
            1
        );
        store.flush_all_l0().await.unwrap();
        assert_eq!(
            store
                .list_traces(pid, &nan_min_filter, &HashMap::new(), &[])
                .await
                .unwrap()
                .traces
                .len(),
            1
        );

        let mut tr2 = make_trace("tr_n2", "ag", "1.0", None, None, ts + 10, TraceStatus::Ok);
        tr2.total_cost_nanos = 7_000_000;
        let mut sp2 = make_span("tr_n2", "sp_2", None, ts + 10);
        sp2.cost_nanos = 7_000_000;
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr2],
                    spans: vec![sp2],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
        assert_eq!(
            store
                .list_traces(pid, &nan_min_filter, &HashMap::new(), &[])
                .await
                .unwrap()
                .traces
                .len(),
            2
        );
        store.close().await.unwrap();
    }
}

#[tokio::test]
async fn regression_r7_loop_warning_flush_boundary_determinism() {
    let ts = 1_700_000_000_000i64;

    let make_12_start_llm = |tid: &str, sess: &str| -> (AgentTrace, Vec<AgentSpan>) {
        let mut tr = make_trace(
            tid,
            "planner",
            "1.0",
            Some(sess),
            Some("u_r7"),
            ts,
            TraceStatus::Ok,
        );
        tr.primary_model = Some("gpt-4o".to_string());
        let spans = (0..12)
            .map(|i| {
                let mut sp = make_span(tid, &format!("sp_{i}"), None, ts + i * 10);
                sp.span_phase = SpanPhase::Start;
                sp.span_type = SpanType::Llm;
                sp.model = Some("gpt-4o".to_string());
                sp
            })
            .collect();
        (tr, spans)
    };

    let make_sp11_final_tool = |tid: &str| -> AgentSpan {
        let mut sp = make_span(tid, "sp_11", None, ts + 11 * 10);
        sp.span_phase = SpanPhase::Final;
        sp.span_type = SpanType::Tool;
        sp.model = None;
        sp.tool_name = Some("search".to_string());
        sp
    };

    async fn assert_healed_all_apis(store: &AgentTraceStore, pid: &str, tid: &str, sess: &str) {
        let det = store
            .get_trace_detail(pid, tid, &[])
            .await
            .unwrap()
            .unwrap();
        assert!(
            !det.trace.has_loop_warning,
            "{pid}: detail has_loop_warning"
        );
        assert_eq!(det.trace.llm_call_count, 11, "{pid}: detail llm_call_count");
        assert_eq!(
            det.trace.tool_call_count, 1,
            "{pid}: detail tool_call_count"
        );

        let all = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(all.traces.len(), 1, "{pid}: list(None) len");
        assert!(!all.traces[0].has_loop_warning, "{pid}: list(None) loop");
        assert_eq!(all.traces[0].llm_call_count, 11, "{pid}: list(None) llm");

        let no_loop = store
            .list_traces(
                pid,
                &TraceListFilter {
                    has_loop_warning: Some(false),
                    ..Default::default()
                },
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(no_loop.traces.len(), 1, "{pid}: list(false) len");
        assert!(!no_loop.traces[0].has_loop_warning);
        assert_eq!(no_loop.traces[0].llm_call_count, 11);

        let with_loop = store
            .list_traces(
                pid,
                &TraceListFilter {
                    has_loop_warning: Some(true),
                    ..Default::default()
                },
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert!(
            with_loop.traces.is_empty(),
            "{pid}: list(true) must be empty"
        );

        let qa = store
            .query_analytics(pid, &AnalyticsFilter::default(), &[])
            .await
            .unwrap();
        assert_eq!(qa.total_executions, 1, "{pid}: qa total");
        assert_eq!(qa.loop_warning_count, 0, "{pid}: qa loop_warning_count");

        let cmp = store
            .compare_groups(
                pid,
                &CompareFilter {
                    baseline_agent_version: Some("1.0".to_string()),
                    candidate_agent_version: Some("2.0".to_string()),
                    ..Default::default()
                },
                &[],
            )
            .await
            .unwrap();
        assert_eq!(cmp.baseline.executions, 1);
        assert_eq!(cmp.baseline.loop_warning_count, 0, "{pid}: cmp loop_count");

        let sdet = store
            .get_session_detail(pid, sess, &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sdet.traces.len(), 1);
        assert!(
            !sdet.traces[0].has_loop_warning,
            "{pid}: session detail loop"
        );
        assert_eq!(sdet.traces[0].llm_call_count, 11);
    }

    // 1. L0-only vs Seg1+Seg2 (both orders) vs Seg1+L0 vs post-compaction
    {
        // 1a. L0-only
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(os, cfg).await.unwrap();
        let pid = "proj_r7_l0";
        let (tr1, sp1) = make_12_start_llm("tr_heal", "sess_r7");
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr1],
                    spans: sp1,
                },
            )
            .await
            .unwrap();
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![make_sp11_final_tool("tr_heal")],
                },
            )
            .await
            .unwrap();
        assert_healed_all_apis(&store, pid, "tr_heal", "sess_r7").await;
        store.close().await.unwrap();
    }

    {
        // 1b. Seg 1 (12 Start(Llm)) + L0 (sp_11 Final(Tool)), then Seg 1 + Seg 2, then post-compaction
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(os, cfg).await.unwrap();
        let pid = "proj_r7_seg_l0_compact";
        let (tr1, sp1) = make_12_start_llm("tr_heal", "sess_r7");
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr1],
                    spans: sp1,
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![make_sp11_final_tool("tr_heal")],
                },
            )
            .await
            .unwrap();
        // Seg 1 + L0
        assert_healed_all_apis(&store, pid, "tr_heal", "sess_r7").await;

        // Seg 1 + Seg 2
        store.flush_all_l0().await.unwrap();
        assert_healed_all_apis(&store, pid, "tr_heal", "sess_r7").await;

        // Post-compaction
        store.compact_and_gc(Some(pid), &[]).await.unwrap();
        assert_healed_all_apis(&store, pid, "tr_heal", "sess_r7").await;
        store.close().await.unwrap();
    }

    {
        // 1c. Reverse flush order: Seg 1 (sp_11 Final(Tool)) + Seg 2 (12 Start(Llm)) + Seg1+L0
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(os, cfg).await.unwrap();
        let pid = "proj_r7_rev";
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![make_sp11_final_tool("tr_heal")],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
        let (tr1, sp1) = make_12_start_llm("tr_heal", "sess_r7");
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr1],
                    spans: sp1,
                },
            )
            .await
            .unwrap();
        // Seg 1 (Final Tool) + L0 (12 Start Llm)
        assert_healed_all_apis(&store, pid, "tr_heal", "sess_r7").await;
        // Seg 1 + Seg 2
        store.flush_all_l0().await.unwrap();
        assert_healed_all_apis(&store, pid, "tr_heal", "sess_r7").await;
        store.close().await.unwrap();
    }

    // 2. Pass 1 pruning guard: Seg 1 (12 Start(Llm), loop_count >= trace_count) + Seg 2 (sp_11 Final(Tool) + tr_other)
    {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(os, cfg).await.unwrap();
        let pid = "proj_r7_prune_guard";
        let (tr1, sp1) = make_12_start_llm("tr_heal", "sess_heal");
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr1],
                    spans: sp1,
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let (mut tr_other, sp_other) = make_12_start_llm("tr_other", "sess_other");
        tr_other.primary_model = Some("claude-sonnet-4".to_string());
        let sp_other: Vec<AgentSpan> = sp_other
            .into_iter()
            .map(|mut s| {
                s.model = Some("claude-sonnet-4".to_string());
                s
            })
            .collect();
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr_other],
                    spans: [vec![make_sp11_final_tool("tr_heal")], sp_other].concat(),
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let no_loop = store
            .list_traces(
                pid,
                &TraceListFilter {
                    has_loop_warning: Some(false),
                    ..Default::default()
                },
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(no_loop.traces.len(), 1);
        assert_eq!(no_loop.traces[0].trace_id, "tr_heal");
        assert!(!no_loop.traces[0].has_loop_warning);

        let no_loop_model = store
            .list_traces(
                pid,
                &TraceListFilter {
                    has_loop_warning: Some(false),
                    model: Some("gpt-4o".to_string()),
                    ..Default::default()
                },
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(no_loop_model.traces.len(), 1);
        assert_eq!(no_loop_model.traces[0].trace_id, "tr_heal");

        let only_loop = store
            .list_traces(
                pid,
                &TraceListFilter {
                    has_loop_warning: Some(true),
                    ..Default::default()
                },
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(only_loop.traces.len(), 1);
        assert_eq!(only_loop.traces[0].trace_id, "tr_other");
        store.close().await.unwrap();
    }

    // 3. Tool-loop flush-boundary determinism (4 consecutive Tool("search") vs upgraded sp_3 -> Final(Llm))
    {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(os, cfg).await.unwrap();
        let pid = "proj_r7_tool_loop";

        let tr_tool_loop = make_trace(
            "tr_tloop",
            "planner",
            "1.0",
            Some("sess_tl"),
            Some("u_tl"),
            ts,
            TraceStatus::Ok,
        );
        let sp_tool_loop: Vec<AgentSpan> = (0..4)
            .map(|i| {
                let mut s = make_span("tr_tloop", &format!("spt_{i}"), None, ts + i * 10);
                s.span_type = SpanType::Tool;
                s.span_phase = SpanPhase::Final;
                s.tool_name = Some("search".to_string());
                s
            })
            .collect();

        let tr_healed_tool = make_trace(
            "tr_theal",
            "planner",
            "1.0",
            Some("sess_th"),
            Some("u_th"),
            ts + 100,
            TraceStatus::Ok,
        );
        let sp_healed_start: Vec<AgentSpan> = (0..4)
            .map(|i| {
                let mut s = make_span("tr_theal", &format!("sph_{i}"), None, ts + 100 + i * 10);
                s.span_type = SpanType::Tool;
                s.span_phase = SpanPhase::Start;
                s.tool_name = Some("search".to_string());
                s
            })
            .collect();

        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr_tool_loop, tr_healed_tool],
                    spans: [sp_tool_loop, sp_healed_start].concat(),
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let mut sp_fix = make_span("tr_theal", "sph_3", None, ts + 130);
        sp_fix.span_type = SpanType::Llm;
        sp_fix.span_phase = SpanPhase::Final;
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![],
                    spans: vec![sp_fix],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let det_loop = store
            .get_trace_detail(pid, "tr_tloop", &[])
            .await
            .unwrap()
            .unwrap();
        assert!(det_loop.trace.has_loop_warning);
        let det_heal = store
            .get_trace_detail(pid, "tr_theal", &[])
            .await
            .unwrap()
            .unwrap();
        assert!(!det_heal.trace.has_loop_warning);

        let listed_true = store
            .list_traces(
                pid,
                &TraceListFilter {
                    has_loop_warning: Some(true),
                    ..Default::default()
                },
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(listed_true.traces.len(), 1);
        assert_eq!(listed_true.traces[0].trace_id, "tr_tloop");

        let listed_false = store
            .list_traces(
                pid,
                &TraceListFilter {
                    has_loop_warning: Some(false),
                    ..Default::default()
                },
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(listed_false.traces.len(), 1);
        assert_eq!(listed_false.traces[0].trace_id, "tr_theal");

        let qa = store
            .query_analytics(pid, &AnalyticsFilter::default(), &[])
            .await
            .unwrap();
        assert_eq!(qa.loop_warning_count, 1);
        store.close().await.unwrap();
    }

    // 4. Explicit has_loop_warning == true preserved with < 12 LLM spans across L0, Segs, Compaction
    {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(os, cfg).await.unwrap();
        let pid = "proj_r7_explicit_true";
        let mut tr1 = make_trace(
            "tr_exp",
            "planner",
            "1.0",
            Some("sess_exp"),
            Some("u_exp"),
            ts,
            TraceStatus::Ok,
        );
        tr1.has_loop_warning = true;
        let sp1 = make_span("tr_exp", "sp_1", None, ts);
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr1],
                    spans: vec![sp1],
                },
            )
            .await
            .unwrap();
        assert!(
            store
                .get_trace_detail(pid, "tr_exp", &[])
                .await
                .unwrap()
                .unwrap()
                .trace
                .has_loop_warning
        );
        store.flush_all_l0().await.unwrap();

        let mut tr2 = make_trace(
            "tr_exp",
            "planner",
            "1.0",
            Some("sess_exp"),
            Some("u_exp"),
            ts + 10,
            TraceStatus::Ok,
        );
        tr2.has_loop_warning = false;
        let sp2 = make_span("tr_exp", "sp_2", None, ts + 10);
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr2],
                    spans: vec![sp2],
                },
            )
            .await
            .unwrap();
        assert!(
            store
                .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
                .await
                .unwrap()
                .traces[0]
                .has_loop_warning
        );
        store.flush_all_l0().await.unwrap();
        store.compact_and_gc(Some(pid), &[]).await.unwrap();
        assert!(
            store
                .get_trace_detail(pid, "tr_exp", &[])
                .await
                .unwrap()
                .unwrap()
                .trace
                .has_loop_warning
        );
        assert_eq!(
            store
                .query_analytics(pid, &AnalyticsFilter::default(), &[])
                .await
                .unwrap()
                .loop_warning_count,
            1
        );
        store.close().await.unwrap();
    }

    // 5. Hostile explicit_has_loop_warning = 2 (> 1) rejected as StoreError::Corrupted in Tag 0x02 & Tag 0x04
    {
        let mut tr = make_trace(
            "tr_hostile_ehlw",
            "planner",
            "1.0",
            Some("sess_h"),
            Some("u_h"),
            ts,
            TraceStatus::Ok,
        );
        tr.input = Value::Null;
        tr.output = Value::Null;
        tr.metadata = Value::Null;
        tr.error_message = None;
        let (base_bytes, tail_offset, zone) =
            SegmentWriter::build_plss_bytes(vec![(tr, vec![])]).unwrap();

        fn replace_sec(
            base: &[u8],
            old_tail_offset: u64,
            target_tag: SectionTag,
            new_sec: &[u8],
        ) -> (Vec<u8>, u64) {
            let footer = PlssFooter::decode(&base[base.len() - 24..]).unwrap();
            let dir_start = footer.dir_offset as usize;
            let dir_end = dir_start + footer.dir_len as usize;
            let mut entries: Vec<_> = base[dir_start..dir_end]
                .chunks_exact(PLSS_DIR_ENTRY_LEN)
                .map(|c| SectionDirEntry::decode(c).unwrap())
                .collect();
            let idx = entries
                .iter()
                .position(|e| e.tag == target_tag as u16)
                .unwrap();
            let old_off = entries[idx].offset as usize;
            let old_len = entries[idx].len as usize;
            let delta = new_sec.len() as isize - old_len as isize;
            let mut out = Vec::with_capacity((base.len() as isize + delta) as usize);
            out.extend_from_slice(&base[..old_off]);
            out.extend_from_slice(new_sec);
            out.extend_from_slice(&base[old_off + old_len..dir_start]);
            entries[idx].len = new_sec.len() as u32;
            entries[idx].crc32 = crc32fast::hash(new_sec);
            for e in entries.iter_mut().skip(idx + 1) {
                e.offset = (e.offset as isize + delta) as u64;
            }
            let new_dir_offset = out.len() as u64;
            let mut new_dir = Vec::new();
            for e in &entries {
                e.encode_into(&mut new_dir);
            }
            out.extend_from_slice(&new_dir);
            PlssFooter {
                dir_offset: new_dir_offset,
                dir_len: footer.dir_len,
                dir_crc32: crc32fast::hash(&new_dir),
                version_flags: footer.version_flags,
                magic: footer.magic,
            }
            .encode_into(&mut out);
            (out, (old_tail_offset as isize + delta) as u64)
        }

        let footer = PlssFooter::decode(&base_bytes[base_bytes.len() - 24..]).unwrap();
        let dir_start = footer.dir_offset as usize;
        let dir_end = dir_start + footer.dir_len as usize;
        let entries: Vec<_> = base_bytes[dir_start..dir_end]
            .chunks_exact(PLSS_DIR_ENTRY_LEN)
            .map(|c| SectionDirEntry::decode(c).unwrap())
            .collect();

        // 5a. Corrupt Tag 0x02 explicit_has_loop_warning = 2 (at -13 before 8B explicit_primary_* str IDs + 4-byte crdt_cnt=0)
        {
            let e02 = entries
                .iter()
                .find(|e| e.tag == SectionTag::TraceSummary as u16)
                .unwrap();
            let s02 = &base_bytes[e02.offset as usize..(e02.offset as usize + e02.len as usize)];
            let comp_len = u32::from_le_bytes(s02[8..12].try_into().unwrap()) as usize;
            let mut raw = zstd::stream::decode_all(&s02[20..20 + comp_len]).unwrap();
            let off = raw.len() - 13;
            assert_eq!(raw[off], 0);
            raw[off] = 2;
            let new_comp = zstd::stream::encode_all(raw.as_slice(), 3).unwrap();
            let mut new_sec = Vec::new();
            new_sec.extend_from_slice(&1u32.to_le_bytes());
            new_sec.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            new_sec.extend_from_slice(&(new_comp.len() as u32).to_le_bytes());
            new_sec.extend_from_slice(&crc32fast::hash(&new_comp).to_le_bytes());
            new_sec.push(1);
            new_sec.extend_from_slice(&[0u8; 3]);
            new_sec.extend_from_slice(&new_comp);
            let (mutated, new_tail_off) =
                replace_sec(&base_bytes, tail_offset, SectionTag::TraceSummary, &new_sec);
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path = ObjectPath::from("agent_segments/proj_h2/2023-11-14/shard_00/bad_02.plss");
            os.put(&path, PutPayload::from_bytes(Bytes::from(mutated.clone())))
                .await
                .unwrap();
            let cache =
                pulse_agent::store::cache::AgentSegmentCache::open(None, 4 * 1024 * 1024, None, 0)
                    .await
                    .unwrap();
            let reader = pulse_agent::store::segment::PlssReader::new(os, cache.clone());
            let seg = pulse_agent::store::manifest::SegmentDescriptor {
                segment_id: "bad_02_ehlw".into(),
                object_path: path.as_ref().to_string(),
                date: "2023-11-14".into(),
                shard_id: 0,
                level: 0,
                file_size: mutated.len() as u64,
                tail_offset: new_tail_off,
                zone_map: zone.clone(),
                trace_count: 1,
                created_at_ms: 1_000,
            };
            let tail = reader.read_eof_tail(&seg).await.unwrap();
            let err = reader.read_trace_summaries(&seg, &tail).await.unwrap_err();
            assert!(matches!(err, StoreError::Corrupted(_)), "got {err:?}");
            cache.close().await.unwrap();
        }

        // 5b. Corrupt Tag 0x04 explicit_has_loop_warning = 2 (at -27 before 11B "gpt-4o" + 11B "openai" + 4-byte span_cnt=0)
        {
            let e04 = entries
                .iter()
                .find(|e| e.tag == SectionTag::SpanTree as u16)
                .unwrap();
            let s04 = &base_bytes[e04.offset as usize..(e04.offset as usize + e04.len as usize)];
            let idx_bytes = u32::from_le_bytes(s04[8..12].try_into().unwrap()) as usize;
            let dir04_base = 12 + idx_bytes - 24;
            let comp_len =
                u32::from_le_bytes(s04[dir04_base + 8..dir04_base + 12].try_into().unwrap())
                    as usize;
            let blk_off = 12 + idx_bytes;
            let mut raw = {
                let mut dec = zstd::stream::Decoder::with_dictionary(
                    &s04[blk_off..blk_off + comp_len],
                    agent_zstd_dict(),
                )
                .unwrap();
                let mut out = Vec::new();
                std::io::Read::read_to_end(&mut dec, &mut out).unwrap();
                out
            };
            let off = raw.len() - 27;
            assert_eq!(raw[off], 0);
            raw[off] = 2;
            let new_comp = {
                let mut enc =
                    zstd::stream::Encoder::with_dictionary(Vec::new(), 3, agent_zstd_dict())
                        .unwrap();
                std::io::Write::write_all(&mut enc, &raw).unwrap();
                enc.finish().unwrap()
            };
            let mut new_sec = s04[..12 + idx_bytes].to_vec();
            new_sec[dir04_base + 8..dir04_base + 12]
                .copy_from_slice(&(new_comp.len() as u32).to_le_bytes());
            new_sec[dir04_base + 12..dir04_base + 16]
                .copy_from_slice(&(raw.len() as u32).to_le_bytes());
            new_sec[dir04_base + 16..dir04_base + 20]
                .copy_from_slice(&crc32fast::hash(&new_comp).to_le_bytes());
            new_sec.extend_from_slice(&new_comp);
            let (mutated, new_tail_off) =
                replace_sec(&base_bytes, tail_offset, SectionTag::SpanTree, &new_sec);
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path = ObjectPath::from("agent_segments/proj_h2/2023-11-14/shard_00/bad_04.plss");
            os.put(&path, PutPayload::from_bytes(Bytes::from(mutated.clone())))
                .await
                .unwrap();
            let cache =
                pulse_agent::store::cache::AgentSegmentCache::open(None, 4 * 1024 * 1024, None, 0)
                    .await
                    .unwrap();
            let reader = pulse_agent::store::segment::PlssReader::new(os, cache.clone());
            let seg = pulse_agent::store::manifest::SegmentDescriptor {
                segment_id: "bad_04_ehlw".into(),
                object_path: path.as_ref().to_string(),
                date: "2023-11-14".into(),
                shard_id: 0,
                level: 0,
                file_size: mutated.len() as u64,
                tail_offset: new_tail_off,
                zone_map: zone,
                trace_count: 1,
                created_at_ms: 1_000,
            };
            let tail = reader.read_eof_tail(&seg).await.unwrap();
            let err = reader
                .read_trace_ranges(&seg, &tail, "tr_hostile_ehlw")
                .await
                .unwrap_err();
            assert!(matches!(err, StoreError::Corrupted(_)), "got {err:?}");
            cache.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn regression_r8_tool_name_phase_crdt_and_oob_string_id() {
    use pulse_agent::store::segment::{
        PlssFooter, SectionDirEntry, SectionTag, SegmentWriter, PLSS_DIR_ENTRY_LEN,
    };

    let ts = 1_700_000_000_000i64;

    async fn assert_all_5_apis(
        store: &AgentTraceStore,
        pid: &str,
        tid: &str,
        sess: &str,
        expected_loop: bool,
        ctx: &str,
    ) {
        let det = store
            .get_trace_detail(pid, tid, &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(det.trace.has_loop_warning, expected_loop, "{ctx}: detail");

        let all = store
            .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(all.traces.len(), 1, "{ctx}: list len");
        assert_eq!(
            all.traces[0].has_loop_warning, expected_loop,
            "{ctx}: list(None)"
        );

        let filtered = store
            .list_traces(
                pid,
                &TraceListFilter {
                    has_loop_warning: Some(expected_loop),
                    ..Default::default()
                },
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(filtered.traces.len(), 1, "{ctx}: list(expected)");

        let opposite = store
            .list_traces(
                pid,
                &TraceListFilter {
                    has_loop_warning: Some(!expected_loop),
                    ..Default::default()
                },
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert!(opposite.traces.is_empty(), "{ctx}: list(!expected)");

        let qa = store
            .query_analytics(pid, &AnalyticsFilter::default(), &[])
            .await
            .unwrap();
        assert_eq!(
            qa.loop_warning_count,
            i64::from(expected_loop),
            "{ctx}: query_analytics"
        );

        let cmp = store
            .compare_groups(
                pid,
                &CompareFilter {
                    baseline_agent_version: Some("1.0".to_string()),
                    candidate_agent_version: Some("2.0".to_string()),
                    ..Default::default()
                },
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            cmp.baseline.loop_warning_count,
            i64::from(expected_loop),
            "{ctx}: compare_groups"
        );

        let sdet = store
            .get_session_detail(pid, sess, &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sdet.traces.len(), 1);
        assert_eq!(
            sdet.traces[0].has_loop_warning, expected_loop,
            "{ctx}: get_session_detail"
        );
    }

    fn mk_tool_span(
        tid: &str,
        sid: &str,
        phase: SpanPhase,
        ts_ms: i64,
        name: &str,
        tool_name: Option<&str>,
    ) -> AgentSpan {
        let mut sp = make_span(tid, sid, None, ts_ms);
        sp.span_phase = phase;
        sp.span_type = SpanType::Tool;
        sp.name = name.to_string();
        sp.tool_name = tool_name.map(str::to_string);
        sp.model = None;
        sp.provider = None;
        sp.status = if phase == SpanPhase::Start {
            TraceStatus::Running
        } else {
            TraceStatus::Ok
        };
        sp
    }

    // 1. Start(Tool, "search") -> Final(Tool, "other") (B1):
    //    "other" < "search" lexicographically, so Final must heal the loop across ALL layouts & APIs.
    {
        let prefix: Vec<AgentSpan> = (0..3)
            .map(|i| {
                mk_tool_span(
                    "tr_b1",
                    &format!("sp_{i}"),
                    SpanPhase::Final,
                    ts + (i as i64) * 10,
                    "step",
                    Some("search"),
                )
            })
            .collect();
        let sp3_start = mk_tool_span(
            "tr_b1",
            "sp_3",
            SpanPhase::Start,
            ts + 30,
            "step_3",
            Some("search"),
        );
        let sp3_final = mk_tool_span(
            "tr_b1",
            "sp_3",
            SpanPhase::Final,
            ts + 30,
            "step_3",
            Some("other"),
        );

        let mut direct_spans = prefix.clone();
        direct_spans.push(sp3_start.clone());
        direct_spans.push(sp3_final.clone());
        assert!(
            detect_execution_loop(&direct_spans).is_none(),
            "stats::detect_execution_loop must return None for B1"
        );
        let mut rev_direct = prefix.clone();
        rev_direct.push(sp3_final.clone());
        rev_direct.push(sp3_start.clone());
        assert!(
            detect_execution_loop(&rev_direct).is_none(),
            "stats::detect_execution_loop must be commutative for B1"
        );

        let tr_start = make_trace(
            "tr_b1",
            "ag",
            "1.0",
            Some("sess_b1"),
            None,
            ts,
            TraceStatus::Running,
        );
        let tr_final = make_trace(
            "tr_b1",
            "ag",
            "1.0",
            Some("sess_b1"),
            None,
            ts,
            TraceStatus::Ok,
        );
        let b_a = TraceIngestBatch {
            traces: vec![tr_start],
            spans: [prefix.as_slice(), &[sp3_start]].concat(),
        };
        let b_b = TraceIngestBatch {
            traces: vec![tr_final],
            spans: vec![sp3_final],
        };

        for (mode, flush1, flush2, reverse) in [
            ("b1_l0_only", false, false, false),
            ("b1_seg_plus_l0", true, false, false),
            ("b1_seg1_seg2_fwd", true, true, false),
            ("b1_seg1_seg2_rev", true, true, true),
        ] {
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut cfg = test_store_config(false, 0);
            cfg.l0_shards = 1;
            let store = AgentTraceStore::open(os, cfg).await.unwrap();
            let (first, second) = if reverse {
                (b_b.clone(), b_a.clone())
            } else {
                (b_a.clone(), b_b.clone())
            };
            store.ingest_batch(mode, first).await.unwrap();
            if flush1 {
                store.flush_all_l0().await.unwrap();
            }
            store.ingest_batch(mode, second).await.unwrap();
            if flush2 {
                store.flush_all_l0().await.unwrap();
            }
            assert_all_5_apis(&store, mode, "tr_b1", "sess_b1", false, mode).await;
            let qa = store
                .query_analytics(mode, &AnalyticsFilter::default(), &[])
                .await
                .unwrap();
            assert!(
                qa.by_tool.iter().any(|t| t.tool_name == "other"),
                "{mode}: by_tool should reflect healed tool_name 'other'"
            );
            store.compact_and_gc(Some(mode), &[]).await.unwrap();
            assert_all_5_apis(
                &store,
                mode,
                "tr_b1",
                "sess_b1",
                false,
                &format!("{mode}_compacted"),
            )
            .await;
            store.close().await.unwrap();
        }
    }

    // 2. Start(Tool, tool_name=Some("search"), name="step_i") -> Final(Tool, tool_name=None, name="final_step"):
    //    Lower-phase explicit tool_name must backfill Final(None), producing true across all layouts + 3-way associativity.
    {
        let start_spans: Vec<AgentSpan> = (0..4)
            .map(|i| {
                mk_tool_span(
                    "tr_bf",
                    &format!("sp_{i}"),
                    SpanPhase::Start,
                    ts + (i as i64) * 10,
                    &format!("step_{i}"),
                    Some("search"),
                )
            })
            .collect();
        let final_spans: Vec<AgentSpan> = (0..4)
            .map(|i| {
                mk_tool_span(
                    "tr_bf",
                    &format!("sp_{i}"),
                    SpanPhase::Final,
                    ts + (i as i64) * 10,
                    &format!("final_step_{i}"),
                    None,
                )
            })
            .collect();

        let mut all_fwd = start_spans.clone();
        all_fwd.extend(final_spans.clone());
        assert!(detect_execution_loop(&all_fwd).is_some());
        let mut all_rev = final_spans.clone();
        all_rev.extend(start_spans.clone());
        assert!(detect_execution_loop(&all_rev).is_some());

        let tr_start = make_trace(
            "tr_bf",
            "ag",
            "1.0",
            Some("sess_bf"),
            None,
            ts,
            TraceStatus::Running,
        );
        let tr_final = make_trace(
            "tr_bf",
            "ag",
            "1.0",
            Some("sess_bf"),
            None,
            ts,
            TraceStatus::Ok,
        );
        let b_a = TraceIngestBatch {
            traces: vec![tr_start],
            spans: start_spans,
        };
        let b_b = TraceIngestBatch {
            traces: vec![tr_final],
            spans: final_spans,
        };

        for (mode, flush1, flush2, reverse) in [
            ("bf_l0_only", false, false, false),
            ("bf_seg_plus_l0", true, false, false),
            ("bf_seg1_seg2_fwd", true, true, false),
            ("bf_seg1_seg2_rev", true, true, true),
        ] {
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut cfg = test_store_config(false, 0);
            cfg.l0_shards = 1;
            let store = AgentTraceStore::open(os, cfg).await.unwrap();
            let (first, second) = if reverse {
                (b_b.clone(), b_a.clone())
            } else {
                (b_a.clone(), b_b.clone())
            };
            store.ingest_batch(mode, first).await.unwrap();
            if flush1 {
                store.flush_all_l0().await.unwrap();
            }
            store.ingest_batch(mode, second).await.unwrap();
            if flush2 {
                store.flush_all_l0().await.unwrap();
            }
            assert_all_5_apis(&store, mode, "tr_bf", "sess_bf", true, mode).await;
            store.compact_and_gc(Some(mode), &[]).await.unwrap();
            assert_all_5_apis(
                &store,
                mode,
                "tr_bf",
                "sess_bf",
                true,
                &format!("{mode}_compacted"),
            )
            .await;
            store.close().await.unwrap();
        }

        // 3-way associativity: compact(A, B) + C == compact(B, C) + A
        // A = Start(Some("search")), B = Update(None), C = Final(None) -> true in both associations
        // Also test A = Start(Some("search")), B = Update(None), C = Final(Some("other")) on sp_3 -> false in both associations
        for (tag, sp3_c_tool, expected_loop) in [
            ("3way_true", None, true),
            ("3way_false", Some("other"), false),
        ] {
            let batch_a = TraceIngestBatch {
                traces: vec![make_trace(
                    "tr_3w",
                    "ag",
                    "1.0",
                    Some("sess_3w"),
                    None,
                    ts,
                    TraceStatus::Running,
                )],
                spans: (0..4)
                    .map(|i| {
                        mk_tool_span(
                            "tr_3w",
                            &format!("sp_{i}"),
                            SpanPhase::Start,
                            ts + (i as i64) * 10,
                            &format!("s_{i}"),
                            Some("search"),
                        )
                    })
                    .collect(),
            };
            let batch_b = TraceIngestBatch {
                traces: vec![make_trace(
                    "tr_3w",
                    "ag",
                    "1.0",
                    Some("sess_3w"),
                    None,
                    ts,
                    TraceStatus::Running,
                )],
                spans: (0..4)
                    .map(|i| {
                        mk_tool_span(
                            "tr_3w",
                            &format!("sp_{i}"),
                            SpanPhase::Update,
                            ts + (i as i64) * 10,
                            &format!("u_{i}"),
                            None,
                        )
                    })
                    .collect(),
            };
            let batch_c = TraceIngestBatch {
                traces: vec![make_trace(
                    "tr_3w",
                    "ag",
                    "1.0",
                    Some("sess_3w"),
                    None,
                    ts,
                    TraceStatus::Ok,
                )],
                spans: (0..4)
                    .map(|i| {
                        mk_tool_span(
                            "tr_3w",
                            &format!("sp_{i}"),
                            SpanPhase::Final,
                            ts + (i as i64) * 10,
                            &format!("f_{i}"),
                            if i == 3 { sp3_c_tool } else { None },
                        )
                    })
                    .collect(),
            };

            for (assoc_name, compact_first, third) in [
                (
                    "ab_c",
                    vec![batch_a.clone(), batch_b.clone()],
                    batch_c.clone(),
                ),
                (
                    "bc_a",
                    vec![batch_b.clone(), batch_c.clone()],
                    batch_a.clone(),
                ),
            ] {
                let pid = format!("{tag}_{assoc_name}");
                let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
                let mut cfg = test_store_config(false, 0);
                cfg.l0_shards = 1;
                let store = AgentTraceStore::open(os, cfg).await.unwrap();
                for b in compact_first {
                    store.ingest_batch(&pid, b).await.unwrap();
                    store.flush_all_l0().await.unwrap();
                }
                store.compact_and_gc(Some(&pid), &[]).await.unwrap();
                store.ingest_batch(&pid, third).await.unwrap();
                assert_all_5_apis(
                    &store,
                    &pid,
                    "tr_3w",
                    "sess_3w",
                    expected_loop,
                    &format!("{pid}_l0"),
                )
                .await;
                store.flush_all_l0().await.unwrap();
                assert_all_5_apis(
                    &store,
                    &pid,
                    "tr_3w",
                    "sess_3w",
                    expected_loop,
                    &format!("{pid}_seg"),
                )
                .await;
                store.close().await.unwrap();
            }
        }
    }

    // 3. Equal-rank Tool spans with tool_name=None, name="" AND tool_name=Some(""), name="x" across Seg 1 + Seg 2
    for (case_name, tn, sname) in [
        ("empty_span_name", None, ""),
        ("empty_tool_name", Some(""), "x"),
    ] {
        let pid = format!("proj_empty_{case_name}");
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(os, cfg).await.unwrap();
        let tr = make_trace(
            "tr_emp",
            "ag",
            "1.0",
            Some("sess_emp"),
            None,
            ts,
            TraceStatus::Ok,
        );
        let spans: Vec<AgentSpan> = (0..4)
            .map(|i| {
                mk_tool_span(
                    "tr_emp",
                    &format!("sp_{i}"),
                    SpanPhase::Final,
                    ts + (i as i64) * 10,
                    sname,
                    tn,
                )
            })
            .collect();
        store
            .ingest_batch(
                &pid,
                TraceIngestBatch {
                    traces: vec![tr.clone()],
                    spans: spans.clone(),
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
        store
            .ingest_batch(
                &pid,
                TraceIngestBatch {
                    traces: vec![tr],
                    spans,
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
        assert_all_5_apis(&store, &pid, "tr_emp", "sess_emp", true, case_name).await;
        store.close().await.unwrap();
    }

    // 4. Out-of-bounds tool_name_str & span_name_str (999_999) in Tag 0x02 rejected with Err(StoreError::Corrupted)
    {
        let tr = make_trace(
            "tr_oob",
            "planner",
            "1.0",
            Some("sess_oob"),
            Some("u_oob"),
            ts,
            TraceStatus::Ok,
        );
        let mut sp = mk_tool_span(
            "tr_oob",
            "sp_oob",
            SpanPhase::Final,
            ts,
            "search",
            Some("search"),
        );
        sp.status = TraceStatus::Ok;
        let (base_bytes, tail_offset, zone) =
            SegmentWriter::build_plss_bytes(vec![(tr, vec![sp])]).unwrap();

        let footer = PlssFooter::decode(&base_bytes[base_bytes.len() - 24..]).unwrap();
        let dir_start = footer.dir_offset as usize;
        let dir_end = dir_start + footer.dir_len as usize;
        let entries: Vec<_> = base_bytes[dir_start..dir_end]
            .chunks_exact(PLSS_DIR_ENTRY_LEN)
            .map(|c| SectionDirEntry::decode(c).unwrap())
            .collect();
        let e02 = entries
            .iter()
            .find(|e| e.tag == SectionTag::TraceSummary as u16)
            .unwrap();
        let s02 = &base_bytes[e02.offset as usize..(e02.offset as usize + e02.len as usize)];
        let comp_len = u32::from_le_bytes(s02[8..12].try_into().unwrap()) as usize;
        let raw_orig = zstd::stream::decode_all(&s02[20..20 + comp_len]).unwrap();

        for (field_name, off_from_end) in [("tool_name_str", 17usize), ("span_name_str", 4usize)] {
            let mut raw = raw_orig.clone();
            let pos = raw.len() - off_from_end;
            raw[pos..pos + 4].copy_from_slice(&999_999u32.to_le_bytes());
            let new_comp = zstd::stream::encode_all(raw.as_slice(), 3).unwrap();
            let mut new_sec02 = Vec::new();
            new_sec02.extend_from_slice(&1u32.to_le_bytes());
            new_sec02.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            new_sec02.extend_from_slice(&(new_comp.len() as u32).to_le_bytes());
            new_sec02.extend_from_slice(&crc32fast::hash(&new_comp).to_le_bytes());
            new_sec02.push(1);
            new_sec02.extend_from_slice(&[0u8; 3]);
            new_sec02.extend_from_slice(&new_comp);

            let mut mod_entries = entries.clone();
            let idx = mod_entries
                .iter()
                .position(|e| e.tag == SectionTag::TraceSummary as u16)
                .unwrap();
            let old_off = mod_entries[idx].offset as usize;
            let old_len = mod_entries[idx].len as usize;
            let delta = new_sec02.len() as isize - old_len as isize;
            let mut mutated = Vec::with_capacity((base_bytes.len() as isize + delta) as usize);
            mutated.extend_from_slice(&base_bytes[..old_off]);
            mutated.extend_from_slice(&new_sec02);
            mutated.extend_from_slice(&base_bytes[old_off + old_len..dir_start]);
            mod_entries[idx].len = new_sec02.len() as u32;
            mod_entries[idx].crc32 = crc32fast::hash(&new_sec02);
            for e in mod_entries.iter_mut().skip(idx + 1) {
                e.offset = (e.offset as isize + delta) as u64;
            }
            let new_dir_offset = mutated.len() as u64;
            let mut new_dir = Vec::new();
            for e in &mod_entries {
                e.encode_into(&mut new_dir);
            }
            mutated.extend_from_slice(&new_dir);
            PlssFooter {
                dir_offset: new_dir_offset,
                dir_len: footer.dir_len,
                dir_crc32: crc32fast::hash(&new_dir),
                version_flags: footer.version_flags,
                magic: footer.magic,
            }
            .encode_into(&mut mutated);
            let new_tail_off = (tail_offset as isize + delta) as u64;

            let pid = format!("proj_oob_{field_name}");
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let seg_path =
                ObjectPath::from(format!("agent_segments/{pid}/2023-11-14/shard_00/oob.plss"));
            os.put(
                &seg_path,
                PutPayload::from_bytes(Bytes::from(mutated.clone())),
            )
            .await
            .unwrap();
            let ms = ManifestStore::new(&pid, os.clone(), 0).unwrap();
            ms.commit_with_cas(|m| {
                m.segments
                    .push(pulse_agent::store::manifest::SegmentDescriptor {
                        segment_id: "oob".into(),
                        object_path: seg_path.as_ref().to_string(),
                        date: "2023-11-14".into(),
                        shard_id: 0,
                        level: 0,
                        file_size: mutated.len() as u64,
                        tail_offset: new_tail_off,
                        zone_map: zone.clone(),
                        trace_count: 1,
                        created_at_ms: 1_000,
                    });
                Ok(true)
            })
            .await
            .unwrap();

            let store = AgentTraceStore::open(os, test_store_config(false, 0))
                .await
                .unwrap();
            let err_list = store
                .list_traces(&pid, &TraceListFilter::default(), &HashMap::new(), &[])
                .await
                .unwrap_err();
            assert!(
                matches!(err_list, StoreError::Corrupted(_)),
                "{field_name} list_traces expected Corrupted, got {err_list:?}"
            );
            let err_qa = store
                .query_analytics(&pid, &AnalyticsFilter::default(), &[])
                .await
                .unwrap_err();
            assert!(
                matches!(err_qa, StoreError::Corrupted(_)),
                "{field_name} query_analytics expected Corrupted, got {err_qa:?}"
            );
            let err_cmp = store
                .compare_groups(&pid, &CompareFilter::default(), &[])
                .await
                .unwrap_err();
            assert!(
                matches!(err_cmp, StoreError::Corrupted(_)),
                "{field_name} compare_groups expected Corrupted, got {err_cmp:?}"
            );
            let err_sdet = store
                .get_session_detail(&pid, "sess_oob", &[])
                .await
                .unwrap_err();
            assert!(
                matches!(err_sdet, StoreError::Corrupted(_)),
                "{field_name} get_session_detail expected Corrupted, got {err_sdet:?}"
            );
            store.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn regression_r9_tag03_phase_rank_empty_tool_name_and_filter_crdt_parity() {
    use pulse_agent::model::{AnalyticsFilter, CompareFilter, TraceListFilter};
    use pulse_agent::store::segment::SegmentWriter;
    use pulse_agent::store::AgentTraceStore;

    let ts = 1_700_000_000_000i64;

    let mk_tool = |tid: &str,
                   sid: &str,
                   phase: SpanPhase,
                   ts_ms: i64,
                   name: &str,
                   tool_name: Option<&str>| {
        let mut sp = make_span(tid, sid, None, ts_ms);
        sp.span_phase = phase;
        sp.span_type = SpanType::Tool;
        sp.name = name.to_string();
        sp.tool_name = tool_name.map(str::to_string);
        sp.model = None;
        sp.provider = None;
        sp.status = if phase == SpanPhase::Start {
            TraceStatus::Running
        } else {
            TraceStatus::Ok
        };
        sp
    };

    // 1. 3-phase split: Start("search") + Final(None) in Seg 1 + Update("other") in L0 / Seg 2
    //    Also verify model/provider 3-phase Start("gpt-4","openai")+Final(None) + Update("claude","anthropic")
    //    and cross-type Start(Tool "search") -> Final(Llm "gpt-4") across all layouts + compaction.
    {
        let mut sp_llm_start = make_span("tr_3ph", "sp_llm", None, ts + 50);
        sp_llm_start.span_phase = SpanPhase::Start;
        sp_llm_start.span_type = SpanType::Llm;
        sp_llm_start.model = Some("gpt-4".to_string());
        sp_llm_start.provider = Some("openai".to_string());
        sp_llm_start.status = TraceStatus::Running;

        let mut sp_llm_upd = make_span("tr_3ph", "sp_llm", None, ts + 50);
        sp_llm_upd.span_phase = SpanPhase::Update;
        sp_llm_upd.span_type = SpanType::Llm;
        sp_llm_upd.model = Some("claude".to_string());
        sp_llm_upd.provider = Some("anthropic".to_string());
        sp_llm_upd.status = TraceStatus::Running;

        let mut sp_llm_fin = make_span("tr_3ph", "sp_llm", None, ts + 50);
        sp_llm_fin.span_phase = SpanPhase::Final;
        sp_llm_fin.span_type = SpanType::Llm;
        sp_llm_fin.model = None;
        sp_llm_fin.provider = None;
        sp_llm_fin.status = TraceStatus::Ok;

        let sp_xtype_start = mk_tool(
            "tr_3ph",
            "sp_x",
            SpanPhase::Start,
            ts + 60,
            "x",
            Some("stale_tool"),
        );
        let mut sp_xtype_final = make_span("tr_3ph", "sp_x", None, ts + 60);
        sp_xtype_final.span_phase = SpanPhase::Final;
        sp_xtype_final.span_type = SpanType::Llm;
        sp_xtype_final.model = Some("gpt-4o-mini".to_string());
        sp_xtype_final.provider = Some("openai".to_string());
        sp_xtype_final.tool_name = None;
        sp_xtype_final.status = TraceStatus::Ok;

        let seg1_batch_a = TraceIngestBatch {
            traces: vec![make_trace(
                "tr_3ph",
                "ag",
                "1.0",
                Some("sess_3ph"),
                None,
                ts,
                TraceStatus::Running,
            )],
            spans: vec![
                mk_tool(
                    "tr_3ph",
                    "sp_t",
                    SpanPhase::Start,
                    ts + 10,
                    "step",
                    Some("search"),
                ),
                sp_llm_start,
                sp_xtype_start,
            ],
        };
        let seg1_batch_c = TraceIngestBatch {
            traces: vec![make_trace(
                "tr_3ph",
                "ag",
                "1.0",
                Some("sess_3ph"),
                None,
                ts,
                TraceStatus::Ok,
            )],
            spans: vec![
                mk_tool("tr_3ph", "sp_t", SpanPhase::Final, ts + 10, "step", None),
                sp_llm_fin,
                sp_xtype_final,
            ],
        };
        let batch_b = TraceIngestBatch {
            traces: vec![make_trace(
                "tr_3ph",
                "ag",
                "1.0",
                Some("sess_3ph"),
                None,
                ts,
                TraceStatus::Running,
            )],
            spans: vec![
                mk_tool(
                    "tr_3ph",
                    "sp_t",
                    SpanPhase::Update,
                    ts + 10,
                    "step",
                    Some("other"),
                ),
                sp_llm_upd,
            ],
        };

        async fn assert_3ph_state(store: &AgentTraceStore, pid: &str, ctx: &str) {
            let det = store
                .get_trace_detail(pid, "tr_3ph", &[])
                .await
                .unwrap()
                .unwrap();
            let sp_t = det.spans.iter().find(|s| s.span_id == "sp_t").unwrap();
            assert_eq!(sp_t.tool_name.as_deref(), Some("other"), "{ctx}: detail");

            let lt_model_gpt4 = store
                .list_traces(
                    pid,
                    &TraceListFilter {
                        model: Some("gpt-4".into()),
                        ..Default::default()
                    },
                    &HashMap::new(),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(lt_model_gpt4.traces.len(), 1, "{ctx}: list model=gpt-4");

            let qa = store
                .query_analytics(pid, &AnalyticsFilter::default(), &[])
                .await
                .unwrap();
            let tool_names: Vec<(&str, i64)> = qa
                .by_tool
                .iter()
                .map(|b| (b.tool_name.as_str(), b.invocations))
                .collect();
            assert_eq!(tool_names, vec![("other", 1)], "{ctx}: by_tool");
            let model_names: Vec<&str> = qa.by_model.iter().map(|b| b.key.as_str()).collect();
            assert!(
                model_names.contains(&"gpt-4"),
                "{ctx}: by_model missing gpt-4"
            );
            assert!(
                model_names.contains(&"gpt-4o-mini"),
                "{ctx}: by_model missing gpt-4o-mini"
            );

            for (tn, expected) in [("search", 0i64), ("stale_tool", 0i64), ("other", 1i64)] {
                let qa_f = store
                    .query_analytics(
                        pid,
                        &AnalyticsFilter {
                            tool_name: Some(tn.into()),
                            ..Default::default()
                        },
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    qa_f.total_executions, expected,
                    "{ctx}: query_analytics(tool_name={tn})"
                );
                let cmp = store
                    .compare_groups(
                        pid,
                        &CompareFilter {
                            baseline: Some(AnalyticsFilter {
                                tool_name: Some(tn.into()),
                                ..Default::default()
                            }),
                            candidate: Some(AnalyticsFilter {
                                tool_name: Some("other".into()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    cmp.baseline.executions, expected,
                    "{ctx}: compare_groups(tool_name={tn})"
                );
                assert_eq!(cmp.candidate.executions, 1, "{ctx}: compare_groups(other)");
            }
            let sdet = store
                .get_session_detail(pid, "sess_3ph", &[])
                .await
                .unwrap()
                .unwrap();
            assert_eq!(sdet.traces.len(), 1, "{ctx}: session_detail");
        }

        for (mode, flush_ac, flush_b, b_first) in [
            ("3ph_l0", false, false, false),
            ("3ph_seg_l0", true, false, false),
            ("3ph_seg_seg_fwd", true, true, false),
            ("3ph_seg_seg_rev", true, true, true),
        ] {
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut cfg = test_store_config(false, 0);
            cfg.l0_shards = 1;
            let store = AgentTraceStore::open(os, cfg).await.unwrap();
            if b_first {
                store.ingest_batch(mode, batch_b.clone()).await.unwrap();
                if flush_b {
                    store.flush_all_l0().await.unwrap();
                }
                store
                    .ingest_batch(mode, seg1_batch_a.clone())
                    .await
                    .unwrap();
                store
                    .ingest_batch(mode, seg1_batch_c.clone())
                    .await
                    .unwrap();
                if flush_ac {
                    store.flush_all_l0().await.unwrap();
                }
            } else {
                store
                    .ingest_batch(mode, seg1_batch_a.clone())
                    .await
                    .unwrap();
                store
                    .ingest_batch(mode, seg1_batch_c.clone())
                    .await
                    .unwrap();
                if flush_ac {
                    store.flush_all_l0().await.unwrap();
                }
                store.ingest_batch(mode, batch_b.clone()).await.unwrap();
                if flush_b {
                    store.flush_all_l0().await.unwrap();
                }
            }
            assert_3ph_state(&store, mode, mode).await;
            store.compact_and_gc(Some(mode), &[]).await.unwrap();
            assert_3ph_state(&store, mode, &format!("{mode}_comp")).await;
            store.close().await.unwrap();
        }
    }

    // 2. Start("search") -> Final(Some("")) & 3-way associativity (A+B)+C == A+(B+C)
    //    across L0, Seg+L0, Seg1+Seg2 (fwd/rev), and compact_and_gc for query_analytics & compare_groups
    {
        let prefix: Vec<AgentSpan> = (0..3)
            .map(|i| {
                mk_tool(
                    "tr_emp3",
                    &format!("sp_{i}"),
                    SpanPhase::Final,
                    ts + (i as i64) * 10,
                    "step",
                    Some("search"),
                )
            })
            .collect();

        let batch_a = TraceIngestBatch {
            traces: vec![make_trace(
                "tr_emp3",
                "ag",
                "1.0",
                Some("sess_emp3"),
                None,
                ts,
                TraceStatus::Running,
            )],
            spans: [
                prefix.as_slice(),
                &[mk_tool(
                    "tr_emp3",
                    "sp_3",
                    SpanPhase::Start,
                    ts + 30,
                    "n_a",
                    Some("search"),
                )],
            ]
            .concat(),
        };
        let batch_b = TraceIngestBatch {
            traces: vec![make_trace(
                "tr_emp3",
                "ag",
                "1.0",
                Some("sess_emp3"),
                None,
                ts,
                TraceStatus::Running,
            )],
            spans: vec![mk_tool(
                "tr_emp3",
                "sp_3",
                SpanPhase::Update,
                ts + 30,
                "n_b",
                Some("other"),
            )],
        };
        let batch_c = TraceIngestBatch {
            traces: vec![make_trace(
                "tr_emp3",
                "ag",
                "1.0",
                Some("sess_emp3"),
                None,
                ts,
                TraceStatus::Ok,
            )],
            spans: vec![mk_tool(
                "tr_emp3",
                "sp_3",
                SpanPhase::Final,
                ts + 30,
                "n_c",
                Some(""),
            )],
        };

        async fn assert_emp3(store: &AgentTraceStore, pid: &str, ctx: &str) {
            let det = store
                .get_trace_detail(pid, "tr_emp3", &[])
                .await
                .unwrap()
                .unwrap();
            let sp3 = det.spans.iter().find(|s| s.span_id == "sp_3").unwrap();
            assert_eq!(sp3.tool_name.as_deref(), Some(""), "{ctx}: detail");
            assert!(!det.trace.has_loop_warning, "{ctx}: loop=false");

            let qa = store
                .query_analytics(pid, &AnalyticsFilter::default(), &[])
                .await
                .unwrap();
            let mut by_tool: Vec<(String, i64)> = qa
                .by_tool
                .iter()
                .map(|b| (b.tool_name.clone(), b.invocations))
                .collect();
            by_tool.sort();
            assert_eq!(
                by_tool,
                vec![("search".to_string(), 3), ("unnamed_tool".to_string(), 1)],
                "{ctx}: by_tool"
            );

            let qa_other = store
                .query_analytics(
                    pid,
                    &AnalyticsFilter {
                        tool_name: Some("other".into()),
                        ..Default::default()
                    },
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(qa_other.total_executions, 0, "{ctx}: filter other");

            let cmp_other = store
                .compare_groups(
                    pid,
                    &CompareFilter {
                        baseline: Some(AnalyticsFilter {
                            tool_name: Some("other".into()),
                            ..Default::default()
                        }),
                        candidate: Some(AnalyticsFilter {
                            tool_name: Some("search".into()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(cmp_other.baseline.executions, 0, "{ctx}: cmp other");
            assert_eq!(cmp_other.candidate.executions, 1, "{ctx}: cmp search");
        }

        for (assoc_name, compact_pair, third) in [
            (
                "ab_c",
                vec![batch_a.clone(), batch_b.clone()],
                batch_c.clone(),
            ),
            (
                "bc_a",
                vec![batch_b.clone(), batch_c.clone()],
                batch_a.clone(),
            ),
        ] {
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut cfg = test_store_config(false, 0);
            cfg.l0_shards = 1;
            let store = AgentTraceStore::open(os, cfg).await.unwrap();
            for b in compact_pair {
                store.ingest_batch(assoc_name, b).await.unwrap();
                store.flush_all_l0().await.unwrap();
            }
            store.compact_and_gc(Some(assoc_name), &[]).await.unwrap();
            store.ingest_batch(assoc_name, third).await.unwrap();
            assert_emp3(&store, assoc_name, &format!("{assoc_name}_l0")).await;
            store.flush_all_l0().await.unwrap();
            assert_emp3(&store, assoc_name, &format!("{assoc_name}_seg")).await;
            store.compact_and_gc(Some(assoc_name), &[]).await.unwrap();
            assert_emp3(&store, assoc_name, &format!("{assoc_name}_comp")).await;
            store.close().await.unwrap();
        }

        // Direct 2-phase Start("search") -> Final(Some("")) single-span trace across all layouts
        let b1 = TraceIngestBatch {
            traces: vec![make_trace(
                "tr_s_emp",
                "ag",
                "1.0",
                Some("sess_se"),
                None,
                ts,
                TraceStatus::Running,
            )],
            spans: vec![mk_tool(
                "tr_s_emp",
                "sp_1",
                SpanPhase::Start,
                ts + 10,
                "step",
                Some("search"),
            )],
        };
        let b2 = TraceIngestBatch {
            traces: vec![make_trace(
                "tr_s_emp",
                "ag",
                "1.0",
                Some("sess_se"),
                None,
                ts,
                TraceStatus::Ok,
            )],
            spans: vec![mk_tool(
                "tr_s_emp",
                "sp_1",
                SpanPhase::Final,
                ts + 10,
                "step",
                Some(""),
            )],
        };
        for (mode, f1, f2, rev) in [
            ("se_l0", false, false, false),
            ("se_seg_l0", true, false, false),
            ("se_seg_seg_fwd", true, true, false),
            ("se_seg_seg_rev", true, true, true),
        ] {
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut cfg = test_store_config(false, 0);
            cfg.l0_shards = 1;
            let store = AgentTraceStore::open(os, cfg).await.unwrap();
            let (first, second) = if rev {
                (b2.clone(), b1.clone())
            } else {
                (b1.clone(), b2.clone())
            };
            store.ingest_batch(mode, first).await.unwrap();
            if f1 {
                store.flush_all_l0().await.unwrap();
            }
            store.ingest_batch(mode, second).await.unwrap();
            if f2 {
                store.flush_all_l0().await.unwrap();
            }
            for step in ["pre", "post"] {
                let qa = store
                    .query_analytics(mode, &AnalyticsFilter::default(), &[])
                    .await
                    .unwrap();
                assert_eq!(qa.by_tool.len(), 1, "{mode}/{step}");
                assert_eq!(qa.by_tool[0].tool_name, "unnamed_tool", "{mode}/{step}");
                let qa_s = store
                    .query_analytics(
                        mode,
                        &AnalyticsFilter {
                            tool_name: Some("search".into()),
                            ..Default::default()
                        },
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(qa_s.total_executions, 0, "{mode}/{step} filter search");
                assert!(
                    qa_s.by_tool.is_empty(),
                    "{mode}/{step} filter search by_tool"
                );
                store.compact_and_gc(Some(mode), &[]).await.unwrap();
            }
            store.close().await.unwrap();
        }
    }

    // 3. Pass 1 Tag 0x06 Bloom pruning invariant with CountingStore:
    //    Seg 1: tr_split Start("search"), Seg 2: tr_split Final("other"), Seg 3: tr_disjoint Final("other")
    //    query_analytics(tool_name="search") warms EOF tails, then must do 0 non-tail reads on Seg 3!
    {
        let raw_mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let counting = CountingStore::new(raw_mem.clone());
        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(counting.clone(), cfg).await.unwrap();
        let pid = "proj_cnt_r9";
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![make_trace(
                        "tr_split",
                        "ag",
                        "1.0",
                        None,
                        None,
                        ts,
                        TraceStatus::Running,
                    )],
                    spans: vec![mk_tool(
                        "tr_split",
                        "sp_1",
                        SpanPhase::Start,
                        ts,
                        "step",
                        Some("search"),
                    )],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![make_trace(
                        "tr_split",
                        "ag",
                        "1.0",
                        None,
                        None,
                        ts,
                        TraceStatus::Ok,
                    )],
                    spans: vec![mk_tool(
                        "tr_split",
                        "sp_1",
                        SpanPhase::Final,
                        ts,
                        "step",
                        Some("other"),
                    )],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![make_trace(
                        "tr_disjoint",
                        "ag",
                        "1.0",
                        None,
                        None,
                        ts,
                        TraceStatus::Ok,
                    )],
                    spans: vec![mk_tool(
                        "tr_disjoint",
                        "sp_d",
                        SpanPhase::Final,
                        ts,
                        "step",
                        Some("other"),
                    )],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let segs = ManifestStore::new(pid, raw_mem, 0)
            .unwrap()
            .load_latest()
            .await
            .unwrap()
            .segments;
        assert_eq!(segs.len(), 3);
        let seg3_path = segs[2].object_path.clone();

        // Cold query warms EOF tails; second query must do 0 non-tail reads on Seg 3
        let _ = store
            .query_analytics(
                pid,
                &AnalyticsFilter {
                    tool_name: Some("search".into()),
                    ..Default::default()
                },
                &[],
            )
            .await
            .unwrap();
        counting.reset();
        let qa = store
            .query_analytics(
                pid,
                &AnalyticsFilter {
                    tool_name: Some("search".into()),
                    ..Default::default()
                },
                &[],
            )
            .await
            .unwrap();
        assert_eq!(qa.total_executions, 0);
        let seg3_reads: Vec<_> = counting
            .recorded_ranges()
            .into_iter()
            .filter(|(p, _)| p == &seg3_path)
            .collect();
        assert!(
            seg3_reads.is_empty(),
            "disjoint Seg 3 should have 0 non-tail reads after EOF cache warm: {seg3_reads:?}"
        );
        store.close().await.unwrap();
    }

    // 4. Hostile OOB string IDs (999_999) for ALL 12 optional/tag TraceSummaryRow fields,
    //    OOB SpanMetricRow string fields, and invalid SpanCrdtDelta enum bytes in Tag 0x02
    {
        let replace_sec = |base: &[u8], tag: SectionTag, new_sec: &[u8]| -> (Vec<u8>, u64) {
            let footer = PlssFooter::decode(&base[base.len() - 24..]).unwrap();
            let dir_start = footer.dir_offset as usize;
            let dir_end = dir_start + footer.dir_len as usize;
            let mut entries: Vec<_> = base[dir_start..dir_end]
                .chunks_exact(PLSS_DIR_ENTRY_LEN)
                .map(|c| SectionDirEntry::decode(c).unwrap())
                .collect();
            let idx = entries.iter().position(|e| e.tag == tag as u16).unwrap();
            let old_off = entries[idx].offset as usize;
            let old_len = entries[idx].len as usize;
            let delta = new_sec.len() as isize - old_len as isize;
            let mut out = Vec::with_capacity((base.len() as isize + delta) as usize);
            out.extend_from_slice(&base[..old_off]);
            out.extend_from_slice(new_sec);
            out.extend_from_slice(&base[old_off + old_len..dir_start]);
            entries[idx].len = new_sec.len() as u32;
            entries[idx].crc32 = crc32fast::hash(new_sec);
            for e in entries.iter_mut().skip(idx + 1) {
                e.offset = (e.offset as isize + delta) as u64;
            }
            let new_dir_off = out.len() as u64;
            let mut new_dir = Vec::new();
            for e in &entries {
                e.encode_into(&mut new_dir);
            }
            out.extend_from_slice(&new_dir);
            PlssFooter {
                dir_offset: new_dir_off,
                dir_len: footer.dir_len,
                dir_crc32: crc32fast::hash(&new_dir),
                version_flags: footer.version_flags,
                magic: footer.magic,
            }
            .encode_into(&mut out);
            let old_tail = entries
                .iter()
                .find(|e| e.tag == SectionTag::IndexBloom as u16)
                .unwrap()
                .offset;
            (out, old_tail)
        };

        let recompress_chunk = |raw: &[u8]| -> Vec<u8> {
            let new_comp = zstd::stream::encode_all(raw, 3).unwrap();
            let mut sec = Vec::new();
            sec.extend_from_slice(&1u32.to_le_bytes());
            sec.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            sec.extend_from_slice(&(new_comp.len() as u32).to_le_bytes());
            sec.extend_from_slice(&crc32fast::hash(&new_comp).to_le_bytes());
            sec.push(1);
            sec.extend_from_slice(&[0u8; 3]);
            sec.extend_from_slice(&new_comp);
            sec
        };

        let mut tr = make_trace(
            "tr_h9",
            "planner",
            "1.0",
            Some("sess_h9"),
            Some("user_h9"),
            ts,
            TraceStatus::Error,
        );
        tr.workflow_id = Some("wf_h9".into());
        tr.parent_trace_id = Some("parent_h9".into());
        tr.prompt_version = Some("pv_h9".into());
        tr.model_config_version = Some("mcv_h9".into());
        tr.tool_config_version = Some("tcv_h9".into());
        tr.app_release = Some("rel_h9".into());
        tr.primary_model = Some("gpt-4".into());
        tr.primary_provider = Some("openai".into());
        tr.error_type = Some("err_h9".into());
        tr.tags = vec!["tag_h9".into()];

        let mut sp = mk_tool(
            "tr_h9",
            "sp_h9",
            SpanPhase::Final,
            ts,
            "search",
            Some("search"),
        );
        sp.model = Some("gpt-4".into());
        sp.provider = Some("openai".into());
        sp.status = TraceStatus::Ok;

        let (base_bytes, _tail_off, zone) =
            SegmentWriter::build_plss_bytes(vec![(tr, vec![sp])]).unwrap();
        let footer = PlssFooter::decode(&base_bytes[base_bytes.len() - 24..]).unwrap();
        let entries: Vec<_> = base_bytes
            [footer.dir_offset as usize..(footer.dir_offset as usize + footer.dir_len as usize)]
            .chunks_exact(PLSS_DIR_ENTRY_LEN)
            .map(|c| SectionDirEntry::decode(c).unwrap())
            .collect();

        let e02 = entries
            .iter()
            .find(|e| e.tag == SectionTag::TraceSummary as u16)
            .unwrap();
        let s02 = &base_bytes[e02.offset as usize..(e02.offset as usize + e02.len as usize)];
        let comp02 = u32::from_le_bytes(s02[8..12].try_into().unwrap()) as usize;
        let raw02 = zstd::stream::decode_all(&s02[20..20 + comp02]).unwrap();

        let oob_tag02_u32_fields: [(&str, usize); 12] = [
            ("session_id_str", 8),
            ("user_id_str", 12),
            ("workflow_id_str", 16),
            ("parent_trace_id_str", 20),
            ("prompt_version_str", 32),
            ("model_config_version_str", 36),
            ("tool_config_version_str", 40),
            ("app_release_str", 44),
            ("primary_model_str", 52),
            ("primary_provider_str", 56),
            ("error_type_str", 60),
            ("tags_str[0]", 66),
        ];

        for (fname, off) in oob_tag02_u32_fields {
            let mut raw = raw02.clone();
            raw[off..off + 4].copy_from_slice(&999_999u32.to_le_bytes());
            let (mutated, new_tail) = replace_sec(
                &base_bytes,
                SectionTag::TraceSummary,
                &recompress_chunk(&raw),
            );
            let pid = format!("p_oob02_{off}");
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let seg_path =
                ObjectPath::from(format!("agent_segments/{pid}/2023-11-14/shard_00/h.plss"));
            os.put(
                &seg_path,
                PutPayload::from_bytes(Bytes::from(mutated.clone())),
            )
            .await
            .unwrap();
            let ms = ManifestStore::new(&pid, os.clone(), 0).unwrap();
            ms.commit_with_cas(|m| {
                m.segments
                    .push(pulse_agent::store::manifest::SegmentDescriptor {
                        segment_id: "h".into(),
                        object_path: seg_path.as_ref().to_string(),
                        date: "2023-11-14".into(),
                        shard_id: 0,
                        level: 0,
                        file_size: mutated.len() as u64,
                        tail_offset: new_tail,
                        zone_map: zone.clone(),
                        trace_count: 1,
                        created_at_ms: 1_000,
                    });
                Ok(true)
            })
            .await
            .unwrap();
            let store = AgentTraceStore::open(os, test_store_config(false, 0))
                .await
                .unwrap();
            let err_l = store
                .list_traces(&pid, &TraceListFilter::default(), &HashMap::new(), &[])
                .await
                .unwrap_err();
            assert!(
                matches!(err_l, StoreError::Corrupted(_)),
                "{fname} list: {err_l:?}"
            );
            let err_q = store
                .query_analytics(&pid, &AnalyticsFilter::default(), &[])
                .await
                .unwrap_err();
            assert!(
                matches!(err_q, StoreError::Corrupted(_)),
                "{fname} qa: {err_q:?}"
            );
            let err_c = store
                .compare_groups(&pid, &CompareFilter::default(), &[])
                .await
                .unwrap_err();
            assert!(
                matches!(err_c, StoreError::Corrupted(_)),
                "{fname} cmp: {err_c:?}"
            );
            store.close().await.unwrap();
        }

        // Invalid SpanCrdtDelta enum bytes: span_type=99, span_phase=3, span_phase=0xFE, sp_status=99
        let crdt_base = raw02.len() - 101;
        for (ename, rel, val) in [
            ("span_type_99", 4usize, 99u8),
            ("span_phase_3", 5usize, 3u8),
            ("span_phase_fe", 5usize, 0xFEu8),
            ("sp_status_99", 6usize, 99u8),
        ] {
            let mut raw = raw02.clone();
            raw[crdt_base + rel] = val;
            let (mutated, new_tail) = replace_sec(
                &base_bytes,
                SectionTag::TraceSummary,
                &recompress_chunk(&raw),
            );
            let pid = format!("p_enum_{ename}");
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let seg_path =
                ObjectPath::from(format!("agent_segments/{pid}/2023-11-14/shard_00/h.plss"));
            os.put(
                &seg_path,
                PutPayload::from_bytes(Bytes::from(mutated.clone())),
            )
            .await
            .unwrap();
            let ms = ManifestStore::new(&pid, os.clone(), 0).unwrap();
            ms.commit_with_cas(|m| {
                m.segments
                    .push(pulse_agent::store::manifest::SegmentDescriptor {
                        segment_id: "h".into(),
                        object_path: seg_path.as_ref().to_string(),
                        date: "2023-11-14".into(),
                        shard_id: 0,
                        level: 0,
                        file_size: mutated.len() as u64,
                        tail_offset: new_tail,
                        zone_map: zone.clone(),
                        trace_count: 1,
                        created_at_ms: 1_000,
                    });
                Ok(true)
            })
            .await
            .unwrap();
            let store = AgentTraceStore::open(os, test_store_config(false, 0))
                .await
                .unwrap();
            let err_l = store
                .list_traces(&pid, &TraceListFilter::default(), &HashMap::new(), &[])
                .await
                .unwrap_err();
            assert!(
                matches!(err_l, StoreError::Corrupted(_)),
                "{ename} list: {err_l:?}"
            );
            let err_q = store
                .query_analytics(&pid, &AnalyticsFilter::default(), &[])
                .await
                .unwrap_err();
            assert!(
                matches!(err_q, StoreError::Corrupted(_)),
                "{ename} qa: {err_q:?}"
            );
            store.close().await.unwrap();
        }

        // OOB SpanMetricRow string fields in Tag 0x03: span_id_str, provider_str, model_str, tool_name_str
        let e03 = entries
            .iter()
            .find(|e| e.tag == SectionTag::SpanMetrics as u16)
            .unwrap();
        let s03 = &base_bytes[e03.offset as usize..(e03.offset as usize + e03.len as usize)];
        let comp03 = u32::from_le_bytes(s03[8..12].try_into().unwrap()) as usize;
        let raw03 = zstd::stream::decode_all(&s03[20..20 + comp03]).unwrap();
        assert_eq!(raw03.len(), 4 + 87);
        for (fname, off) in [
            ("sm_span_id_str", 12usize),
            ("sm_provider_str", 35usize),
            ("sm_model_str", 39usize),
            ("sm_tool_name_str", 43usize),
        ] {
            let mut raw = raw03.clone();
            raw[off..off + 4].copy_from_slice(&999_999u32.to_le_bytes());
            let (mutated, new_tail) = replace_sec(
                &base_bytes,
                SectionTag::SpanMetrics,
                &recompress_chunk(&raw),
            );
            let pid = format!("p_oob03_{off}");
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let seg_path =
                ObjectPath::from(format!("agent_segments/{pid}/2023-11-14/shard_00/h.plss"));
            os.put(
                &seg_path,
                PutPayload::from_bytes(Bytes::from(mutated.clone())),
            )
            .await
            .unwrap();
            let ms = ManifestStore::new(&pid, os.clone(), 0).unwrap();
            ms.commit_with_cas(|m| {
                m.segments
                    .push(pulse_agent::store::manifest::SegmentDescriptor {
                        segment_id: "h".into(),
                        object_path: seg_path.as_ref().to_string(),
                        date: "2023-11-14".into(),
                        shard_id: 0,
                        level: 0,
                        file_size: mutated.len() as u64,
                        tail_offset: new_tail,
                        zone_map: zone.clone(),
                        trace_count: 1,
                        created_at_ms: 1_000,
                    });
                Ok(true)
            })
            .await
            .unwrap();
            let store = AgentTraceStore::open(os, test_store_config(false, 0))
                .await
                .unwrap();
            let err_q = store
                .query_analytics(&pid, &AnalyticsFilter::default(), &[])
                .await
                .unwrap_err();
            assert!(
                matches!(err_q, StoreError::Corrupted(_)),
                "{fname} qa: {err_q:?}"
            );
            store.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn regression_r10_explicit_primary_model_llm_filter_and_mixed_v1_v2_tool_parity() {
    use pulse_agent::model::{AnalyticsFilter, CompareFilter, TraceListFilter};
    use pulse_agent::store::segment::{
        PlssFooter, SectionDirEntry, SectionTag, SegmentWriter, PLSS_DIR_ENTRY_LEN,
        SEC_FLAG_CRDT_RANKS,
    };
    use pulse_agent::store::AgentTraceStore;

    let ts = 1_700_000_000_000i64;

    // 1. Start(Llm,"gpt-4o","openai") + Update(Llm,"claude-3","anthropic") + Final(Tool,"calc",None)
    //    3-way associativity across l0_all, (A+B)+C, (A+C)+B, (B+C)+A across Seg+L0, Seg+Seg (fwd/rev),
    //    and compact_and_gc for ALL 5 APIs, BOTH with primary_model=None AND explicit primary_model=Some("gpt-4o").
    for (case_tag, explicit_hdr_model, explicit_hdr_provider) in [
        ("non_explicit", None, None),
        ("explicit_hdr", Some("gpt-4o"), Some("openai")),
    ] {
        let mk_tr = |phase_st: TraceStatus| {
            let mut tr = make_trace(
                "tr_llm_tool",
                "planner",
                "1.0",
                Some("sess_lt"),
                Some("u_lt"),
                ts,
                phase_st,
            );
            tr.primary_model = explicit_hdr_model.map(str::to_string);
            tr.primary_provider = explicit_hdr_provider.map(str::to_string);
            tr
        };
        let mut sp_a = make_span("tr_llm_tool", "sp_1", None, ts);
        sp_a.span_phase = SpanPhase::Start;
        sp_a.span_type = SpanType::Llm;
        sp_a.model = Some("gpt-4o".into());
        sp_a.provider = Some("openai".into());
        sp_a.status = TraceStatus::Running;

        let mut sp_b = make_span("tr_llm_tool", "sp_1", None, ts);
        sp_b.span_phase = SpanPhase::Update;
        sp_b.span_type = SpanType::Llm;
        sp_b.model = Some("claude-3".into());
        sp_b.provider = Some("anthropic".into());
        sp_b.status = TraceStatus::Running;

        let mut sp_c = make_span("tr_llm_tool", "sp_1", None, ts);
        sp_c.span_phase = SpanPhase::Final;
        sp_c.span_type = SpanType::Tool;
        sp_c.model = None;
        sp_c.provider = None;
        sp_c.tool_name = Some("calc".into());
        sp_c.status = TraceStatus::Ok;

        let b_a = TraceIngestBatch {
            traces: vec![mk_tr(TraceStatus::Running)],
            spans: vec![sp_a],
        };
        let b_b = TraceIngestBatch {
            traces: vec![{
                let mut t = make_trace(
                    "tr_llm_tool",
                    "planner",
                    "1.0",
                    Some("sess_lt"),
                    Some("u_lt"),
                    ts,
                    TraceStatus::Running,
                );
                t.primary_model = None;
                t.primary_provider = None;
                t
            }],
            spans: vec![sp_b],
        };
        let b_c = TraceIngestBatch {
            traces: vec![{
                let mut t = make_trace(
                    "tr_llm_tool",
                    "planner",
                    "1.0",
                    Some("sess_lt"),
                    Some("u_lt"),
                    ts,
                    TraceStatus::Ok,
                );
                t.primary_model = None;
                t.primary_provider = None;
                t
            }],
            spans: vec![sp_c],
        };

        let expected_pm = explicit_hdr_model.map(str::to_string);
        let expected_pp = explicit_hdr_provider.map(str::to_string);
        let expected_gpt4_match = usize::from(explicit_hdr_model.is_some());
        let expected_openai_match = usize::from(explicit_hdr_provider.is_some());

        async fn check_all_5(
            store: &AgentTraceStore,
            pid: &str,
            expected_pm: &Option<String>,
            expected_pp: &Option<String>,
            expected_gpt4_match: usize,
            expected_openai_match: usize,
            ctx: &str,
        ) {
            let det = store
                .get_trace_detail(pid, "tr_llm_tool", &[])
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&det.trace.primary_model, expected_pm, "{ctx}: detail pm");
            assert_eq!(&det.trace.primary_provider, expected_pp, "{ctx}: detail pp");
            assert_eq!(det.spans[0].span_type, SpanType::Tool, "{ctx}: span_type");
            assert_eq!(
                det.spans[0].tool_name.as_deref(),
                Some("calc"),
                "{ctx}: tool_name"
            );

            let lt_all = store
                .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
                .await
                .unwrap();
            assert_eq!(lt_all.traces.len(), 1);
            assert_eq!(
                &lt_all.traces[0].primary_model, expected_pm,
                "{ctx}: list pm"
            );
            assert_eq!(
                &lt_all.traces[0].primary_provider, expected_pp,
                "{ctx}: list pp"
            );

            for (m, exp) in [("gpt-4o", expected_gpt4_match), ("claude-3", 0)] {
                let lt = store
                    .list_traces(
                        pid,
                        &TraceListFilter {
                            model: Some(m.into()),
                            ..Default::default()
                        },
                        &HashMap::new(),
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(lt.traces.len(), exp, "{ctx}: list(model={m})");

                let qa = store
                    .query_analytics(
                        pid,
                        &AnalyticsFilter {
                            model: Some(m.into()),
                            ..Default::default()
                        },
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    qa.total_executions, exp as i64,
                    "{ctx}: qa(model={m}) execs"
                );
                assert!(qa.by_model.is_empty(), "{ctx}: qa(model={m}) by_model");

                let cmp = store
                    .compare_groups(
                        pid,
                        &CompareFilter {
                            baseline: Some(AnalyticsFilter {
                                model: Some(m.into()),
                                ..Default::default()
                            }),
                            candidate: Some(AnalyticsFilter {
                                tool_name: Some("calc".into()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(cmp.baseline.executions, exp as i64, "{ctx}: cmp(model={m})");
                assert_eq!(cmp.candidate.executions, 1, "{ctx}: cmp(tool=calc)");
            }

            for (p, exp) in [("openai", expected_openai_match), ("anthropic", 0)] {
                let lt_p = store
                    .list_traces(
                        pid,
                        &TraceListFilter {
                            provider: Some(p.into()),
                            ..Default::default()
                        },
                        &HashMap::new(),
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(lt_p.traces.len(), exp, "{ctx}: list(provider={p})");
                let qa_p = store
                    .query_analytics(
                        pid,
                        &AnalyticsFilter {
                            provider: Some(p.into()),
                            ..Default::default()
                        },
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(qa_p.total_executions, exp as i64, "{ctx}: qa(provider={p})");
            }

            let qa_all = store
                .query_analytics(pid, &AnalyticsFilter::default(), &[])
                .await
                .unwrap();
            assert_eq!(qa_all.total_executions, 1, "{ctx}: qa_all");
            assert!(qa_all.by_model.is_empty(), "{ctx}: qa_all by_model=[]");
            assert_eq!(qa_all.by_tool.len(), 1, "{ctx}: qa_all by_tool len");
            assert_eq!(qa_all.by_tool[0].tool_name, "calc", "{ctx}: by_tool[0]");

            let sdet = store
                .get_session_detail(pid, "sess_lt", &[])
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                &sdet.traces[0].primary_model, expected_pm,
                "{ctx}: session pm"
            );
            assert_eq!(
                &sdet.traces[0].primary_provider, expected_pp,
                "{ctx}: session pp"
            );
        }

        // l0_all
        {
            let pid = format!("r10_{case_tag}_l0_all");
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut cfg = test_store_config(false, 0);
            cfg.l0_shards = 1;
            let store = AgentTraceStore::open(os, cfg).await.unwrap();
            store.ingest_batch(&pid, b_a.clone()).await.unwrap();
            store.ingest_batch(&pid, b_b.clone()).await.unwrap();
            store.ingest_batch(&pid, b_c.clone()).await.unwrap();
            check_all_5(
                &store,
                &pid,
                &expected_pm,
                &expected_pp,
                expected_gpt4_match,
                expected_openai_match,
                &format!("{pid}/l0"),
            )
            .await;
            store.flush_all_l0().await.unwrap();
            check_all_5(
                &store,
                &pid,
                &expected_pm,
                &expected_pp,
                expected_gpt4_match,
                expected_openai_match,
                &format!("{pid}/flushed"),
            )
            .await;
            store.close().await.unwrap();
        }

        // 3-way associations: (A+B)+C, (A+C)+B, (B+C)+A across Seg+L0, Seg+Seg, and compact_and_gc
        for (assoc, pair, third) in [
            ("ab_c", vec![b_a.clone(), b_b.clone()], b_c.clone()),
            ("ac_b", vec![b_a.clone(), b_c.clone()], b_b.clone()),
            ("bc_a", vec![b_b.clone(), b_c.clone()], b_a.clone()),
        ] {
            let pid = format!("r10_{case_tag}_{assoc}");
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut cfg = test_store_config(false, 0);
            cfg.l0_shards = 1;
            let store = AgentTraceStore::open(os, cfg).await.unwrap();
            for b in pair {
                store.ingest_batch(&pid, b).await.unwrap();
                store.flush_all_l0().await.unwrap();
            }
            store.compact_and_gc(Some(&pid), &[]).await.unwrap();
            store.ingest_batch(&pid, third).await.unwrap();
            check_all_5(
                &store,
                &pid,
                &expected_pm,
                &expected_pp,
                expected_gpt4_match,
                expected_openai_match,
                &format!("{pid}/seg_l0"),
            )
            .await;
            store.flush_all_l0().await.unwrap();
            check_all_5(
                &store,
                &pid,
                &expected_pm,
                &expected_pp,
                expected_gpt4_match,
                expected_openai_match,
                &format!("{pid}/seg_seg"),
            )
            .await;
            store.compact_and_gc(Some(&pid), &[]).await.unwrap();
            check_all_5(
                &store,
                &pid,
                &expected_pm,
                &expected_pp,
                expected_gpt4_match,
                expected_openai_match,
                &format!("{pid}/compacted"),
            )
            .await;
            store.close().await.unwrap();
        }
    }

    // 2. Mixed legacy v1 Final(Tool,"search") + v2 Start(Tool,"other") + disjoint v2 Final(Tool,"other")
    //    verifying query_analytics(tool_name), compare_groups(tool_name), by_tool parity, and CountingStore 0 non-tail reads.
    {
        let replace_sec = |base: &[u8],
                           old_tail: u64,
                           tag: SectionTag,
                           new_sec: &[u8],
                           clear_flags: u16|
         -> (Vec<u8>, u64) {
            let footer = PlssFooter::decode(&base[base.len() - 24..]).unwrap();
            let dir_start = footer.dir_offset as usize;
            let dir_end = dir_start + footer.dir_len as usize;
            let mut entries: Vec<_> = base[dir_start..dir_end]
                .chunks_exact(PLSS_DIR_ENTRY_LEN)
                .map(|c| SectionDirEntry::decode(c).unwrap())
                .collect();
            let idx = entries.iter().position(|e| e.tag == tag as u16).unwrap();
            let old_off = entries[idx].offset as usize;
            let old_len = entries[idx].len as usize;
            let delta = new_sec.len() as isize - old_len as isize;
            let mut out = Vec::with_capacity((base.len() as isize + delta) as usize);
            out.extend_from_slice(&base[..old_off]);
            out.extend_from_slice(new_sec);
            out.extend_from_slice(&base[old_off + old_len..dir_start]);
            entries[idx].len = new_sec.len() as u32;
            entries[idx].crc32 = crc32fast::hash(new_sec);
            entries[idx].flags &= !clear_flags;
            for e in entries.iter_mut().skip(idx + 1) {
                e.offset = (e.offset as isize + delta) as u64;
            }
            let new_dir_offset = out.len() as u64;
            let mut new_dir = Vec::new();
            for e in &entries {
                e.encode_into(&mut new_dir);
            }
            out.extend_from_slice(&new_dir);
            PlssFooter {
                dir_offset: new_dir_offset,
                dir_len: footer.dir_len,
                dir_crc32: crc32fast::hash(&new_dir),
                version_flags: footer.version_flags,
                magic: footer.magic,
            }
            .encode_into(&mut out);
            (out, (old_tail as isize + delta) as u64)
        };

        let mut tr_v1 = make_trace(
            "tr_mix",
            "planner",
            "1.0",
            Some("sess_mix"),
            None,
            ts,
            TraceStatus::Ok,
        );
        tr_v1.input = Value::Null;
        tr_v1.output = Value::Null;
        tr_v1.metadata = Value::Null;
        tr_v1.error_message = None;
        tr_v1.primary_model = None;
        tr_v1.primary_provider = None;

        let mut sp_v1 = make_span("tr_mix", "sp_mix", None, ts);
        sp_v1.span_phase = SpanPhase::Final;
        sp_v1.span_type = SpanType::Tool;
        sp_v1.tool_name = Some("search".into());
        sp_v1.model = None;
        sp_v1.provider = None;
        sp_v1.input = Value::Null;
        sp_v1.output = Value::Null;
        sp_v1.attributes = Value::Null;
        sp_v1.metadata = Value::Null;
        sp_v1.model_params = Value::Null;
        sp_v1.events.clear();
        sp_v1.finish_reason = None;
        sp_v1.error_type = None;
        sp_v1.error_message = None;
        sp_v1.error_stack = None;

        let (v2_bytes, v2_tail, v1_zone) =
            SegmentWriter::build_plss_bytes(vec![(tr_v1, vec![sp_v1])]).unwrap();
        let footer = PlssFooter::decode(&v2_bytes[v2_bytes.len() - 24..]).unwrap();
        let entries: Vec<_> = v2_bytes
            [footer.dir_offset as usize..(footer.dir_offset as usize + footer.dir_len as usize)]
            .chunks_exact(PLSS_DIR_ENTRY_LEN)
            .map(|c| SectionDirEntry::decode(c).unwrap())
            .collect();

        // Convert to legacy v1 (strip 17B SpanCrdtDelta tail + 15B trace ranks in Tag 0x02, 2B rank_mask + 9B trace ranks in Tag 0x04)
        let new_sec02 = {
            let e02 = entries
                .iter()
                .find(|e| e.tag == SectionTag::TraceSummary as u16)
                .unwrap();
            let s02 = &v2_bytes[e02.offset as usize..(e02.offset as usize + e02.len as usize)];
            let comp_len = u32::from_le_bytes(s02[8..12].try_into().unwrap()) as usize;
            let mut raw = zstd::stream::decode_all(&s02[20..20 + comp_len]).unwrap();
            raw.truncate(raw.len() - 17);
            let cut = raw.len() - 84 - 4 - 15;
            assert_eq!(&raw[cut + 15..cut + 19], &1u32.to_le_bytes());
            raw.drain(cut..cut + 15);
            let new_comp = zstd::stream::encode_all(raw.as_slice(), 3).unwrap();
            let mut sec = Vec::new();
            sec.extend_from_slice(&1u32.to_le_bytes());
            sec.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            sec.extend_from_slice(&(new_comp.len() as u32).to_le_bytes());
            sec.extend_from_slice(&crc32fast::hash(&new_comp).to_le_bytes());
            sec.push(1);
            sec.extend_from_slice(&[0u8; 3]);
            sec.extend_from_slice(&new_comp);
            sec
        };
        let (v1_s1, v1_t1) = replace_sec(
            &v2_bytes,
            v2_tail,
            SectionTag::TraceSummary,
            &new_sec02,
            SEC_FLAG_CRDT_RANKS,
        );
        let new_sec04 = {
            let e04 = entries
                .iter()
                .find(|e| e.tag == SectionTag::SpanTree as u16)
                .unwrap();
            let s04 = &v2_bytes[e04.offset as usize..(e04.offset as usize + e04.len as usize)];
            let idx_bytes = u32::from_le_bytes(s04[8..12].try_into().unwrap()) as usize;
            let dir04_base = 12 + idx_bytes - 24;
            let comp_len =
                u32::from_le_bytes(s04[dir04_base + 8..dir04_base + 12].try_into().unwrap())
                    as usize;
            let blk_off = 12 + idx_bytes;
            let mut raw = {
                let mut dec = zstd::stream::Decoder::with_dictionary(
                    &s04[blk_off..blk_off + comp_len],
                    agent_zstd_dict(),
                )
                .unwrap();
                let mut out = Vec::new();
                std::io::Read::read_to_end(&mut dec, &mut out).unwrap();
                out
            };
            // tool_name_rank bit is set (0x0100): 2B rank_mask + 9B rank slot = 11B at end
            assert_eq!(
                &raw[raw.len() - 11..raw.len() - 9],
                &0x0100u16.to_le_bytes()
            );
            raw.truncate(raw.len() - 11);
            let marker: [u8; 14] = [1, 0, 0, 0, 6, 0, 0, 0, b's', b'p', b'_', b'm', b'i', b'x'];
            let marker_pos = raw
                .windows(marker.len())
                .position(|w| w == marker)
                .expect("span_cnt=1 + sp_mix marker");
            raw.drain(marker_pos - 29..marker_pos);
            let new_comp = {
                let mut enc =
                    zstd::stream::Encoder::with_dictionary(Vec::new(), 3, agent_zstd_dict())
                        .unwrap();
                std::io::Write::write_all(&mut enc, &raw).unwrap();
                enc.finish().unwrap()
            };
            let mut new_sec = s04[..12 + idx_bytes].to_vec();
            new_sec[dir04_base + 8..dir04_base + 12]
                .copy_from_slice(&(new_comp.len() as u32).to_le_bytes());
            new_sec[dir04_base + 12..dir04_base + 16]
                .copy_from_slice(&(raw.len() as u32).to_le_bytes());
            new_sec[dir04_base + 16..dir04_base + 20]
                .copy_from_slice(&crc32fast::hash(&new_comp).to_le_bytes());
            new_sec.extend_from_slice(&new_comp);
            new_sec
        };
        let (v1_bytes, v1_tail) = replace_sec(
            &v1_s1,
            v1_t1,
            SectionTag::SpanTree,
            &new_sec04,
            SEC_FLAG_CRDT_RANKS,
        );

        let pid = "proj_r10_mixed_v1_v2";
        let raw_mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let counting = CountingStore::new(raw_mem.clone());
        let v1_path = ObjectPath::from(format!(
            "agent_segments/{pid}/2023-11-14/shard_00/v1_seg.plss"
        ));
        counting
            .put(
                &v1_path,
                PutPayload::from_bytes(Bytes::from(v1_bytes.clone())),
            )
            .await
            .unwrap();
        let ms = ManifestStore::new(pid, raw_mem.clone(), 0).unwrap();
        ms.commit_with_cas(|m| {
            m.segments
                .push(pulse_agent::store::manifest::SegmentDescriptor {
                    segment_id: "v1_seg".into(),
                    object_path: v1_path.as_ref().to_string(),
                    date: "2023-11-14".into(),
                    shard_id: 0,
                    level: 0,
                    file_size: v1_bytes.len() as u64,
                    tail_offset: v1_tail,
                    zone_map: v1_zone.clone(),
                    trace_count: 1,
                    created_at_ms: 1_000,
                });
            Ok(true)
        })
        .await
        .unwrap();

        let mut cfg = test_store_config(false, 0);
        cfg.l0_shards = 1;
        let store = AgentTraceStore::open(counting.clone(), cfg).await.unwrap();

        // v2 Seg 2: tr_mix Start(Tool, "other")
        let mut tr_v2 = make_trace(
            "tr_mix",
            "planner",
            "1.0",
            Some("sess_mix"),
            None,
            ts,
            TraceStatus::Running,
        );
        tr_v2.primary_model = None;
        tr_v2.primary_provider = None;
        let mut sp_v2 = make_span("tr_mix", "sp_mix", None, ts);
        sp_v2.span_phase = SpanPhase::Start;
        sp_v2.span_type = SpanType::Tool;
        sp_v2.tool_name = Some("other".into());
        sp_v2.model = None;
        sp_v2.provider = None;
        sp_v2.status = TraceStatus::Running;
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr_v2],
                    spans: vec![sp_v2],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        // v2 Seg 3: disjoint tr_other Final(Tool, "unrelated") for CountingStore pruning check
        let mut tr_dis = make_trace(
            "tr_dis_mix",
            "planner",
            "1.0",
            None,
            None,
            ts + 100,
            TraceStatus::Ok,
        );
        tr_dis.primary_model = None;
        tr_dis.primary_provider = None;
        let mut sp_dis = make_span("tr_dis_mix", "sp_dis", None, ts + 100);
        sp_dis.span_phase = SpanPhase::Final;
        sp_dis.span_type = SpanType::Tool;
        sp_dis.tool_name = Some("unrelated".into());
        sp_dis.model = None;
        sp_dis.provider = None;
        store
            .ingest_batch(
                pid,
                TraceIngestBatch {
                    traces: vec![tr_dis],
                    spans: vec![sp_dis],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        let segs = ManifestStore::new(pid, raw_mem, 0)
            .unwrap()
            .load_latest()
            .await
            .unwrap()
            .segments;
        assert_eq!(segs.len(), 3);
        let dis_path = segs[2].object_path.clone();

        // Warm EOF tails, then check CountingStore 0 non-tail reads on disjoint Seg 3
        let _ = store
            .query_analytics(
                pid,
                &AnalyticsFilter {
                    tool_name: Some("search".into()),
                    ..Default::default()
                },
                &[],
            )
            .await
            .unwrap();
        counting.reset();

        let qa_search = store
            .query_analytics(
                pid,
                &AnalyticsFilter {
                    tool_name: Some("search".into()),
                    ..Default::default()
                },
                &[],
            )
            .await
            .unwrap();
        assert_eq!(qa_search.total_executions, 1);
        assert_eq!(qa_search.by_tool.len(), 1);
        assert_eq!(qa_search.by_tool[0].tool_name, "search");
        assert_eq!(qa_search.by_tool[0].invocations, 1);
        assert!(
            counting
                .recorded_ranges()
                .iter()
                .all(|(p, _)| p != &dis_path),
            "disjoint Seg 3 must incur 0 reads after EOF cache warm"
        );

        let qa_other = store
            .query_analytics(
                pid,
                &AnalyticsFilter {
                    tool_name: Some("other".into()),
                    ..Default::default()
                },
                &[],
            )
            .await
            .unwrap();
        assert_eq!(qa_other.total_executions, 0);
        assert!(qa_other.by_tool.is_empty());

        let cmp = store
            .compare_groups(
                pid,
                &CompareFilter {
                    baseline: Some(AnalyticsFilter {
                        tool_name: Some("search".into()),
                        ..Default::default()
                    }),
                    candidate: Some(AnalyticsFilter {
                        tool_name: Some("other".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                &[],
            )
            .await
            .unwrap();
        assert_eq!(cmp.baseline.executions, 1);
        assert_eq!(cmp.candidate.executions, 0);

        store.close().await.unwrap();
    }

    // 3. Hostile explicit_primary_model_str / explicit_primary_provider_str OOB string IDs in Tag 0x02
    //    and hostile opt_str presence/UTF-8/truncation in Tag 0x04 rejected with StoreError::Corrupted
    {
        let mut tr = make_trace(
            "tr_hostile_epm",
            "planner",
            "1.0",
            Some("sess_epm"),
            Some("u_epm"),
            ts,
            TraceStatus::Ok,
        );
        tr.input = Value::Null;
        tr.output = Value::Null;
        tr.metadata = Value::Null;
        tr.error_message = None;
        let (base_bytes, tail_offset, zone) =
            SegmentWriter::build_plss_bytes(vec![(tr, vec![])]).unwrap();

        let replace_sec = |base: &[u8],
                           old_tail: u64,
                           target_tag: SectionTag,
                           new_sec: &[u8]|
         -> (Vec<u8>, u64) {
            let footer = PlssFooter::decode(&base[base.len() - 24..]).unwrap();
            let dir_start = footer.dir_offset as usize;
            let dir_end = dir_start + footer.dir_len as usize;
            let mut entries: Vec<_> = base[dir_start..dir_end]
                .chunks_exact(PLSS_DIR_ENTRY_LEN)
                .map(|c| SectionDirEntry::decode(c).unwrap())
                .collect();
            let idx = entries
                .iter()
                .position(|e| e.tag == target_tag as u16)
                .unwrap();
            let old_off = entries[idx].offset as usize;
            let old_len = entries[idx].len as usize;
            let delta = new_sec.len() as isize - old_len as isize;
            let mut out = Vec::with_capacity((base.len() as isize + delta) as usize);
            out.extend_from_slice(&base[..old_off]);
            out.extend_from_slice(new_sec);
            out.extend_from_slice(&base[old_off + old_len..dir_start]);
            entries[idx].len = new_sec.len() as u32;
            entries[idx].crc32 = crc32fast::hash(new_sec);
            for e in entries.iter_mut().skip(idx + 1) {
                e.offset = (e.offset as isize + delta) as u64;
            }
            let new_dir_offset = out.len() as u64;
            let mut new_dir = Vec::new();
            for e in &entries {
                e.encode_into(&mut new_dir);
            }
            out.extend_from_slice(&new_dir);
            PlssFooter {
                dir_offset: new_dir_offset,
                dir_len: footer.dir_len,
                dir_crc32: crc32fast::hash(&new_dir),
                version_flags: footer.version_flags,
                magic: footer.magic,
            }
            .encode_into(&mut out);
            (out, (old_tail as isize + delta) as u64)
        };

        let footer = PlssFooter::decode(&base_bytes[base_bytes.len() - 24..]).unwrap();
        let entries: Vec<_> = base_bytes
            [footer.dir_offset as usize..(footer.dir_offset as usize + footer.dir_len as usize)]
            .chunks_exact(PLSS_DIR_ENTRY_LEN)
            .map(|c| SectionDirEntry::decode(c).unwrap())
            .collect();

        // Tag 0x02: explicit_primary_model_str at len-12, explicit_primary_provider_str at len-8 (before 4B crdt_cnt=0)
        for (fname, off_from_end) in [("epm_oob", 12usize), ("epp_oob", 8usize)] {
            let e02 = entries
                .iter()
                .find(|e| e.tag == SectionTag::TraceSummary as u16)
                .unwrap();
            let s02 = &base_bytes[e02.offset as usize..(e02.offset as usize + e02.len as usize)];
            let comp_len = u32::from_le_bytes(s02[8..12].try_into().unwrap()) as usize;
            let mut raw = zstd::stream::decode_all(&s02[20..20 + comp_len]).unwrap();
            let off = raw.len() - off_from_end;
            raw[off..off + 4].copy_from_slice(&999_999u32.to_le_bytes());
            let new_comp = zstd::stream::encode_all(raw.as_slice(), 3).unwrap();
            let mut new_sec = Vec::new();
            new_sec.extend_from_slice(&1u32.to_le_bytes());
            new_sec.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            new_sec.extend_from_slice(&(new_comp.len() as u32).to_le_bytes());
            new_sec.extend_from_slice(&crc32fast::hash(&new_comp).to_le_bytes());
            new_sec.push(1);
            new_sec.extend_from_slice(&[0u8; 3]);
            new_sec.extend_from_slice(&new_comp);
            let (mutated, new_tail_off) =
                replace_sec(&base_bytes, tail_offset, SectionTag::TraceSummary, &new_sec);
            let pid = format!("p_02_{fname}");
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path = ObjectPath::from(format!("agent_segments/{pid}/2023-11-14/shard_00/h.plss"));
            os.put(&path, PutPayload::from_bytes(Bytes::from(mutated.clone())))
                .await
                .unwrap();
            let seg = pulse_agent::store::manifest::SegmentDescriptor {
                segment_id: format!("bad02_{fname}"),
                object_path: path.as_ref().to_string(),
                date: "2023-11-14".into(),
                shard_id: 0,
                level: 0,
                file_size: mutated.len() as u64,
                tail_offset: new_tail_off,
                zone_map: zone.clone(),
                trace_count: 1,
                created_at_ms: 1_000,
            };
            {
                let cache = pulse_agent::store::cache::AgentSegmentCache::open(
                    None,
                    4 * 1024 * 1024,
                    None,
                    0,
                )
                .await
                .unwrap();
                let reader =
                    pulse_agent::store::segment::PlssReader::new(os.clone(), cache.clone());
                let tail = reader.read_eof_tail(&seg).await.unwrap();
                let strings = reader.read_string_table(&seg, &tail).await.unwrap();
                let rows = reader.read_trace_summaries(&seg, &tail).await.unwrap();
                assert!(matches!(
                    rows[0].to_agent_trace(&pid, &strings),
                    Err(StoreError::Corrupted(_))
                ));
                cache.close().await.unwrap();
            }
            let ms = ManifestStore::new(&pid, os.clone(), 0).unwrap();
            ms.commit_with_cas(|m| {
                m.segments.push(seg.clone());
                Ok(true)
            })
            .await
            .unwrap();
            let store = AgentTraceStore::open(os, test_store_config(false, 0))
                .await
                .unwrap();
            assert!(matches!(
                store
                    .list_traces(&pid, &TraceListFilter::default(), &HashMap::new(), &[])
                    .await,
                Err(StoreError::Corrupted(_))
            ));
            assert!(matches!(
                store
                    .query_analytics(&pid, &AnalyticsFilter::default(), &[])
                    .await,
                Err(StoreError::Corrupted(_))
            ));
            assert!(matches!(
                store
                    .compare_groups(&pid, &CompareFilter::default(), &[])
                    .await,
                Err(StoreError::Corrupted(_))
            ));
            assert!(matches!(
                store.get_session_detail(&pid, "sess_epm", &[]).await,
                Err(StoreError::Corrupted(_))
            ));
            store.close().await.unwrap();
        }

        // Tag 0x04: explicit_primary_model at len-26 (presence=1 + u32 len=6 + "gpt-4o"),
        //           explicit_primary_provider at len-15 (presence=1 + u32 len=6 + "openai"), before 4B span_cnt=0
        for (fname, off_from_end, bad_val) in [
            ("epm_pres_2", 26usize, 2u8),
            ("epp_pres_2", 15usize, 2u8),
            ("epm_pres_255", 26usize, 255u8),
            ("epp_pres_255", 15usize, 255u8),
            ("epm_utf8", 21usize, 0xFFu8),
            ("epp_utf8", 10usize, 0xFFu8),
            ("epm_trunc_len", 25usize, 250u8),
        ] {
            let e04 = entries
                .iter()
                .find(|e| e.tag == SectionTag::SpanTree as u16)
                .unwrap();
            let s04 = &base_bytes[e04.offset as usize..(e04.offset as usize + e04.len as usize)];
            let idx_bytes = u32::from_le_bytes(s04[8..12].try_into().unwrap()) as usize;
            let dir04_base = 12 + idx_bytes - 24;
            let comp_len =
                u32::from_le_bytes(s04[dir04_base + 8..dir04_base + 12].try_into().unwrap())
                    as usize;
            let blk_off = 12 + idx_bytes;
            let mut raw = {
                let mut dec = zstd::stream::Decoder::with_dictionary(
                    &s04[blk_off..blk_off + comp_len],
                    agent_zstd_dict(),
                )
                .unwrap();
                let mut out = Vec::new();
                std::io::Read::read_to_end(&mut dec, &mut out).unwrap();
                out
            };
            let off = raw.len() - off_from_end;
            raw[off] = bad_val;
            let new_comp = {
                let mut enc =
                    zstd::stream::Encoder::with_dictionary(Vec::new(), 3, agent_zstd_dict())
                        .unwrap();
                std::io::Write::write_all(&mut enc, &raw).unwrap();
                enc.finish().unwrap()
            };
            let mut new_sec = s04[..12 + idx_bytes].to_vec();
            new_sec[dir04_base + 8..dir04_base + 12]
                .copy_from_slice(&(new_comp.len() as u32).to_le_bytes());
            new_sec[dir04_base + 12..dir04_base + 16]
                .copy_from_slice(&(raw.len() as u32).to_le_bytes());
            new_sec[dir04_base + 16..dir04_base + 20]
                .copy_from_slice(&crc32fast::hash(&new_comp).to_le_bytes());
            new_sec.extend_from_slice(&new_comp);
            let (mutated, new_tail_off) =
                replace_sec(&base_bytes, tail_offset, SectionTag::SpanTree, &new_sec);
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path = ObjectPath::from(format!(
                "agent_segments/p_04_{fname}/2023-11-14/shard_00/h.plss"
            ));
            os.put(&path, PutPayload::from_bytes(Bytes::from(mutated.clone())))
                .await
                .unwrap();
            let cache =
                pulse_agent::store::cache::AgentSegmentCache::open(None, 4 * 1024 * 1024, None, 0)
                    .await
                    .unwrap();
            let reader = pulse_agent::store::segment::PlssReader::new(os, cache.clone());
            let seg = pulse_agent::store::manifest::SegmentDescriptor {
                segment_id: format!("bad04_{fname}"),
                object_path: path.as_ref().to_string(),
                date: "2023-11-14".into(),
                shard_id: 0,
                level: 0,
                file_size: mutated.len() as u64,
                tail_offset: new_tail_off,
                zone_map: zone.clone(),
                trace_count: 1,
                created_at_ms: 1_000,
            };
            let tail = reader.read_eof_tail(&seg).await.unwrap();
            let err = reader
                .read_trace_ranges(&seg, &tail, "tr_hostile_epm")
                .await
                .unwrap_err();
            assert!(matches!(err, StoreError::Corrupted(_)), "{fname}: {err:?}");
            cache.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn regression_r11_explicit_smaller_than_transient_llm_and_zero_span_model_order_independence()
{
    use pulse_agent::model::{AnalyticsFilter, CompareFilter, TraceListFilter};
    use pulse_agent::store::AgentTraceStore;

    let ts = 1_700_000_000_000i64;

    // 1. Explicit primary_model="claude-3", primary_provider="anthropic" (< transient Start(Llm,"gpt-4o","openai"))
    //    followed by Final(Tool,"calc",None) across l0_all, (A+B)+C, (A+C)+B, (B+C)+A, Seg1(A)+L0(B),
    //    Seg1(B)+L0(A), Seg1+Seg2, and compact_and_gc across all 5 query APIs + CountingStore pruning.
    {
        let mut tr_a = make_trace(
            "tr_caveat2",
            "planner",
            "1.0",
            Some("sess_c2"),
            Some("u_c2"),
            ts,
            TraceStatus::Running,
        );
        tr_a.primary_model = Some("claude-3".into());
        tr_a.primary_provider = Some("anthropic".into());

        let mut sp_a = make_span("tr_caveat2", "sp_1", None, ts);
        sp_a.span_phase = SpanPhase::Start;
        sp_a.span_type = SpanType::Llm;
        sp_a.model = Some("gpt-4o".into());
        sp_a.provider = Some("openai".into());
        sp_a.status = TraceStatus::Running;

        let mut tr_b = make_trace(
            "tr_caveat2",
            "planner",
            "1.0",
            Some("sess_c2"),
            Some("u_c2"),
            ts,
            TraceStatus::Running,
        );
        tr_b.primary_model = None;
        tr_b.primary_provider = None;

        let mut sp_b = make_span("tr_caveat2", "sp_1", None, ts);
        sp_b.span_phase = SpanPhase::Update;
        sp_b.span_type = SpanType::Llm;
        sp_b.model = Some("gpt-4o".into());
        sp_b.provider = Some("openai".into());
        sp_b.status = TraceStatus::Running;

        let mut tr_c = make_trace(
            "tr_caveat2",
            "planner",
            "1.0",
            Some("sess_c2"),
            Some("u_c2"),
            ts,
            TraceStatus::Ok,
        );
        tr_c.primary_model = None;
        tr_c.primary_provider = None;

        let mut sp_c = make_span("tr_caveat2", "sp_1", None, ts);
        sp_c.span_phase = SpanPhase::Final;
        sp_c.span_type = SpanType::Tool;
        sp_c.model = None;
        sp_c.provider = None;
        sp_c.tool_name = Some("calc".into());
        sp_c.status = TraceStatus::Ok;

        let b_a = TraceIngestBatch {
            traces: vec![tr_a],
            spans: vec![sp_a],
        };
        let b_b = TraceIngestBatch {
            traces: vec![tr_b],
            spans: vec![sp_b],
        };
        let b_c = TraceIngestBatch {
            traces: vec![tr_c],
            spans: vec![sp_c],
        };

        // Also add a disjoint trace in Seg 3 in the CountingStore check
        let mut tr_disjoint = make_trace(
            "tr_disjoint",
            "other_agent",
            "1.0",
            Some("sess_other"),
            Some("u_other"),
            ts + 10_000,
            TraceStatus::Ok,
        );
        tr_disjoint.primary_model = Some("llama-3".into());
        tr_disjoint.primary_provider = Some("meta".into());
        let mut sp_disjoint = make_span("tr_disjoint", "sp_d", None, ts + 10_000);
        sp_disjoint.span_phase = SpanPhase::Final;
        sp_disjoint.span_type = SpanType::Llm;
        sp_disjoint.model = Some("llama-3".into());
        sp_disjoint.provider = Some("meta".into());
        let b_disjoint = TraceIngestBatch {
            traces: vec![tr_disjoint],
            spans: vec![sp_disjoint],
        };

        async fn verify_all_5(store: &AgentTraceStore, pid: &str, ctx: &str) {
            let det = store
                .get_trace_detail(pid, "tr_caveat2", &[])
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                det.trace.primary_model.as_deref(),
                Some("claude-3"),
                "{ctx}: detail pm"
            );
            assert_eq!(
                det.trace.primary_provider.as_deref(),
                Some("anthropic"),
                "{ctx}: detail pp"
            );

            let lt_all = store
                .list_traces(pid, &TraceListFilter::default(), &HashMap::new(), &[])
                .await
                .unwrap();
            let tr_row = lt_all
                .traces
                .iter()
                .find(|t| t.trace_id == "tr_caveat2")
                .unwrap();
            assert_eq!(
                tr_row.primary_model.as_deref(),
                Some("claude-3"),
                "{ctx}: list pm"
            );
            assert_eq!(
                tr_row.primary_provider.as_deref(),
                Some("anthropic"),
                "{ctx}: list pp"
            );

            for (m, exp) in [("claude-3", 1usize), ("gpt-4o", 0usize)] {
                let lt = store
                    .list_traces(
                        pid,
                        &TraceListFilter {
                            model: Some(m.into()),
                            ..Default::default()
                        },
                        &HashMap::new(),
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(lt.traces.len(), exp, "{ctx}: list(model={m})");

                let qa = store
                    .query_analytics(
                        pid,
                        &AnalyticsFilter {
                            model: Some(m.into()),
                            ..Default::default()
                        },
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(qa.total_executions, exp as i64, "{ctx}: qa(model={m})");
                assert!(qa.by_model.is_empty(), "{ctx}: qa(model={m}) by_model");

                let cmp = store
                    .compare_groups(
                        pid,
                        &CompareFilter {
                            baseline: Some(AnalyticsFilter {
                                model: Some(m.into()),
                                ..Default::default()
                            }),
                            candidate: Some(AnalyticsFilter {
                                tool_name: Some("calc".into()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(cmp.baseline.executions, exp as i64, "{ctx}: cmp(model={m})");
                assert_eq!(cmp.candidate.executions, 1, "{ctx}: cmp(tool=calc)");
            }

            for (p, exp) in [("anthropic", 1usize), ("openai", 0usize)] {
                let lt = store
                    .list_traces(
                        pid,
                        &TraceListFilter {
                            provider: Some(p.into()),
                            ..Default::default()
                        },
                        &HashMap::new(),
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(lt.traces.len(), exp, "{ctx}: list(provider={p})");

                let qa = store
                    .query_analytics(
                        pid,
                        &AnalyticsFilter {
                            provider: Some(p.into()),
                            ..Default::default()
                        },
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(qa.total_executions, exp as i64, "{ctx}: qa(provider={p})");

                let cmp = store
                    .compare_groups(
                        pid,
                        &CompareFilter {
                            baseline: Some(AnalyticsFilter {
                                provider: Some(p.into()),
                                ..Default::default()
                            }),
                            candidate: Some(AnalyticsFilter {
                                tool_name: Some("calc".into()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    cmp.baseline.executions, exp as i64,
                    "{ctx}: cmp(provider={p})"
                );
            }

            let sdet = store
                .get_session_detail(pid, "sess_c2", &[])
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                sdet.traces[0].primary_model.as_deref(),
                Some("claude-3"),
                "{ctx}: session pm"
            );
            assert_eq!(
                sdet.traces[0].primary_provider.as_deref(),
                Some("anthropic"),
                "{ctx}: session pp"
            );
        }

        // l0_all
        {
            let pid = "r11_c2_l0";
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut cfg = test_store_config(false, 0);
            cfg.l0_shards = 1;
            let store = AgentTraceStore::open(os, cfg).await.unwrap();
            store.ingest_batch(pid, b_a.clone()).await.unwrap();
            store.ingest_batch(pid, b_b.clone()).await.unwrap();
            store.ingest_batch(pid, b_c.clone()).await.unwrap();
            verify_all_5(&store, pid, "l0_all").await;
            store.flush_all_l0().await.unwrap();
            verify_all_5(&store, pid, "l0_flushed").await;
            store.close().await.unwrap();
        }

        // 2-way Seg1(A)+L0(C), Seg1(C)+L0(A), Seg1(A)+Seg2(C), Seg1(C)+Seg2(A), compact_and_gc,
        // plus CountingStore pruning with disjoint Seg3("llama-3")
        for (name, first, second) in [
            ("a_then_c", b_a.clone(), b_c.clone()),
            ("c_then_a", b_c.clone(), b_a.clone()),
        ] {
            let pid = format!("r11_c2_{name}");
            let counting = CountingStore::new(Arc::new(InMemory::new()));
            let os: Arc<dyn ObjectStore> = counting.clone();
            let mut cfg = test_store_config(false, 0);
            cfg.l0_shards = 1;
            let store = AgentTraceStore::open(os, cfg).await.unwrap();
            store.ingest_batch(&pid, b_disjoint.clone()).await.unwrap();
            store.flush_all_l0().await.unwrap();
            store.ingest_batch(&pid, first).await.unwrap();
            store.flush_all_l0().await.unwrap();
            store.ingest_batch(&pid, second).await.unwrap();
            verify_all_5(&store, &pid, &format!("{name}/seg_l0")).await;
            store.flush_all_l0().await.unwrap();
            verify_all_5(&store, &pid, &format!("{name}/seg_seg")).await;

            // Warm EOF tails, then ensure disjoint Seg3("llama-3") has 0 non-tail reads on model="claude-3"
            let _ = store
                .list_traces(
                    &pid,
                    &TraceListFilter {
                        model: Some("claude-3".into()),
                        ..Default::default()
                    },
                    &HashMap::new(),
                    &[],
                )
                .await
                .unwrap();
            let manifest = ManifestStore::new(&pid, counting.clone(), 0)
                .unwrap()
                .load_latest()
                .await
                .unwrap();
            let disjoint_seg_path = manifest
                .segments
                .iter()
                .find(|s| s.zone_map.min_start_ts_ms == ts + 10_000)
                .unwrap()
                .object_path
                .clone();
            counting.reset();
            let lt = store
                .list_traces(
                    &pid,
                    &TraceListFilter {
                        model: Some("claude-3".into()),
                        ..Default::default()
                    },
                    &HashMap::new(),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(lt.traces.len(), 1);
            let seg3_reads: Vec<_> = counting
                .recorded_ranges()
                .into_iter()
                .filter(|(p, _)| p == &disjoint_seg_path)
                .collect();
            assert!(
                seg3_reads.is_empty(),
                "{name}: expected 0 non-tail reads on disjoint Seg3, got {seg3_reads:?}"
            );

            store.compact_and_gc(Some(&pid), &[]).await.unwrap();
            verify_all_5(&store, &pid, &format!("{name}/compacted")).await;
            store.close().await.unwrap();
        }

        // 3-way associations: (A+B)+C, (A+C)+B, (B+C)+A across Seg+L0, Seg+Seg, and compact_and_gc
        for (assoc, pair, third) in [
            ("ab_c", vec![b_a.clone(), b_b.clone()], b_c.clone()),
            ("ac_b", vec![b_a.clone(), b_c.clone()], b_b.clone()),
            ("bc_a", vec![b_b.clone(), b_c.clone()], b_a.clone()),
        ] {
            let pid = format!("r11_c2_3way_{assoc}");
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut cfg = test_store_config(false, 0);
            cfg.l0_shards = 1;
            let store = AgentTraceStore::open(os, cfg).await.unwrap();
            for b in pair {
                store.ingest_batch(&pid, b).await.unwrap();
                store.flush_all_l0().await.unwrap();
            }
            store.compact_and_gc(Some(&pid), &[]).await.unwrap();
            store.ingest_batch(&pid, third).await.unwrap();
            verify_all_5(&store, &pid, &format!("3way_{assoc}/seg_l0")).await;
            store.flush_all_l0().await.unwrap();
            verify_all_5(&store, &pid, &format!("3way_{assoc}/seg_seg")).await;
            store.compact_and_gc(Some(&pid), &[]).await.unwrap();
            verify_all_5(&store, &pid, &format!("3way_{assoc}/compacted")).await;
            store.close().await.unwrap();
        }
    }

    // 2. 0-span trace explicit primary_model / primary_provider update ("claude-3"/"anthropic" -> "gpt-4o"/"openai")
    //    across L0, Seg1+Seg2, Seg2+Seg1, Seg1+L0, L0+Seg2, and compact_and_gc:
    //    stale "claude-3"/"anthropic" MUST return 0 and "gpt-4o"/"openai" MUST return 1 in ALL orders.
    {
        let mut tr1 = make_trace(
            "tr_z0",
            "planner",
            "1.0",
            Some("sess_z0"),
            Some("u_z0"),
            ts,
            TraceStatus::Running,
        );
        tr1.span_count = 0;
        tr1.llm_call_count = 0;
        tr1.primary_model = Some("claude-3".into());
        tr1.primary_provider = Some("anthropic".into());

        let mut tr2 = make_trace(
            "tr_z0",
            "planner",
            "1.0",
            Some("sess_z0"),
            Some("u_z0"),
            ts,
            TraceStatus::Ok,
        );
        tr2.span_count = 0;
        tr2.llm_call_count = 0;
        tr2.primary_model = Some("gpt-4o".into());
        tr2.primary_provider = Some("openai".into());

        let b1 = TraceIngestBatch {
            traces: vec![tr1],
            spans: vec![],
        };
        let b2 = TraceIngestBatch {
            traces: vec![tr2],
            spans: vec![],
        };

        async fn check_zero_span(store: &AgentTraceStore, pid: &str, ctx: &str) {
            let det = store
                .get_trace_detail(pid, "tr_z0", &[])
                .await
                .unwrap()
                .unwrap();
            assert_eq!(det.trace.primary_model.as_deref(), Some("gpt-4o"), "{ctx}");
            assert_eq!(
                det.trace.primary_provider.as_deref(),
                Some("openai"),
                "{ctx}"
            );

            let sdet = store
                .get_session_detail(pid, "sess_z0", &[])
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                sdet.traces[0].primary_model.as_deref(),
                Some("gpt-4o"),
                "{ctx}"
            );

            for (m, exp) in [("claude-3", 0usize), ("gpt-4o", 1usize)] {
                let lt = store
                    .list_traces(
                        pid,
                        &TraceListFilter {
                            model: Some(m.into()),
                            ..Default::default()
                        },
                        &HashMap::new(),
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(lt.traces.len(), exp, "{ctx}: list(model={m})");

                let qa = store
                    .query_analytics(
                        pid,
                        &AnalyticsFilter {
                            model: Some(m.into()),
                            ..Default::default()
                        },
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(qa.total_executions, exp as i64, "{ctx}: qa(model={m})");

                let cmp = store
                    .compare_groups(
                        pid,
                        &CompareFilter {
                            baseline: Some(AnalyticsFilter {
                                model: Some(m.into()),
                                ..Default::default()
                            }),
                            candidate: Some(AnalyticsFilter {
                                model: Some("gpt-4o".into()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(cmp.baseline.executions, exp as i64, "{ctx}: cmp(model={m})");
                assert_eq!(cmp.candidate.executions, 1, "{ctx}: cmp(gpt-4o)");
            }

            for (p, exp) in [("anthropic", 0usize), ("openai", 1usize)] {
                let lt = store
                    .list_traces(
                        pid,
                        &TraceListFilter {
                            provider: Some(p.into()),
                            ..Default::default()
                        },
                        &HashMap::new(),
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(lt.traces.len(), exp, "{ctx}: list(provider={p})");

                let qa = store
                    .query_analytics(
                        pid,
                        &AnalyticsFilter {
                            provider: Some(p.into()),
                            ..Default::default()
                        },
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(qa.total_executions, exp as i64, "{ctx}: qa(provider={p})");
            }
        }

        // L0-only (both orders)
        for (name, first, second) in [
            ("l0_12", b1.clone(), b2.clone()),
            ("l0_21", b2.clone(), b1.clone()),
        ] {
            let pid = format!("r11_z0_{name}");
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let store = AgentTraceStore::open(os, test_store_config(false, 0))
                .await
                .unwrap();
            store.ingest_batch(&pid, first).await.unwrap();
            store.ingest_batch(&pid, second).await.unwrap();
            check_zero_span(&store, &pid, name).await;
            store.close().await.unwrap();
        }

        // Seg1+L0, L0+Seg2, Seg1+Seg2, Seg2+Seg1, and compact_and_gc
        for (name, seg_b, l0_b) in [
            ("seg1_l02", b1.clone(), b2.clone()),
            ("seg2_l01", b2.clone(), b1.clone()),
        ] {
            let pid = format!("r11_z0_{name}");
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut cfg = test_store_config(false, 0);
            cfg.l0_shards = 1;
            let store = AgentTraceStore::open(os, cfg).await.unwrap();
            store.ingest_batch(&pid, seg_b).await.unwrap();
            store.flush_all_l0().await.unwrap();
            store.ingest_batch(&pid, l0_b).await.unwrap();
            check_zero_span(&store, &pid, &format!("{name}/seg_l0")).await;
            store.flush_all_l0().await.unwrap();
            check_zero_span(&store, &pid, &format!("{name}/seg_seg")).await;
            store.compact_and_gc(Some(&pid), &[]).await.unwrap();
            check_zero_span(&store, &pid, &format!("{name}/compacted")).await;
            store.close().await.unwrap();
        }
    }
}
