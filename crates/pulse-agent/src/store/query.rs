use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::{
    AgentAnalyticsOverview, AgentErasureTombstone, AgentEvalRollup, AgentSpan, AgentTrace,
    AnalyticsFilter, AnalyticsTimeBucket, CompareFilter, CompareGroupMetrics, CompareResponse,
    DimensionBreakdown, ErasureLevel, EvalTargetType, FailureSource, SessionDetail,
    SessionListFilter, SessionListResponse, SessionSummary, SpanPhase, SpanType, ToolBreakdown,
    TraceDetail, TraceListFilter, TraceListResponse, TraceStatus,
};
use crate::pricing::{cost_nanos_to_usd, usd_to_cost_nanos};
use crate::stats::{
    compare_continuous_metric, compare_cost_nanos_metric, compute_latency_percentiles, MetricDelta,
};
use crate::store::bloom::SegmentIndexSection;
use crate::store::error::{validate_identifier, StoreError, StoreResult};
use crate::store::manifest::SegmentDescriptor;
use crate::store::memtable::{
    env_rank, merge_opt_str_max, merge_opt_str_phase_ranked_preserve_empty,
    merge_spans_crdt_with_ranks, merge_trace_headers_with_meta,
    recompute_trace_from_spans_with_explicit, status_rank, SpanFieldRanks, TraceCrdtMeta,
};
use crate::store::segment::{
    merge_trace_summary_rows_with_meta, ResolvedSpanCrdt, SegmentStringTable, SegmentTail,
    SpanMetricRow, TraceSummaryRow,
};
use crate::store::AgentTraceStore;
use crate::{MAX_TRACE_LIST_LIMIT, MIN_SIGNIFICANCE_SAMPLES};

/// Reads a segment tail, skipping (with a warning) a segment whose file is gone so one lost
/// object doesn't fail every query in the project. Other errors, like corruption, still fail.
async fn tail_or_skip(
    reader: &crate::store::segment::PlssReader,
    seg: &SegmentDescriptor,
) -> StoreResult<Option<Arc<SegmentTail>>> {
    match reader.read_eof_tail(seg).await {
        Ok(tail) => Ok(Some(tail)),
        Err(StoreError::ObjectStore(object_store::Error::NotFound { .. })) => {
            tracing::warn!(segment = %seg.object_path, "agent segment missing from object store; skipping it");
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// Active 3-Level Erasure Index
// ---------------------------------------------------------------------------

struct ActiveTombstones<'a> {
    erased_traces: HashSet<&'a str>,
    erased_sessions: HashSet<&'a str>,
    erased_users: HashSet<&'a str>,
}

impl<'a> ActiveTombstones<'a> {
    fn new(
        project_id: &str,
        tombstones: &'a [AgentErasureTombstone],
        manifest_erased_traces: &'a BTreeMap<String, i64>,
        now_ms: i64,
    ) -> Self {
        let mut out = Self::new_tombstones_only(project_id, tombstones, now_ms);
        out.erased_traces
            .extend(manifest_erased_traces.keys().map(String::as_str));
        out
    }

    fn new_tombstones_only(
        project_id: &str,
        tombstones: &'a [AgentErasureTombstone],
        now_ms: i64,
    ) -> Self {
        let mut erased_traces = HashSet::new();
        let mut erased_sessions = HashSet::new();
        let mut erased_users = HashSet::new();
        for tb in tombstones {
            if tb.is_expired(now_ms) {
                continue;
            }
            if !tb.project_id.is_empty() && tb.project_id != project_id {
                continue;
            }
            match tb.level {
                ErasureLevel::Trace => {
                    erased_traces.insert(tb.target_id.as_str());
                }
                ErasureLevel::Session => {
                    erased_sessions.insert(tb.target_id.as_str());
                }
                ErasureLevel::User => {
                    erased_users.insert(tb.target_id.as_str());
                }
            }
        }
        Self {
            erased_traces,
            erased_sessions,
            erased_users,
        }
    }

    #[inline]
    fn is_trace_erased(
        &self,
        trace_id: &str,
        session_id: Option<&str>,
        user_id: Option<&str>,
    ) -> bool {
        if self.erased_traces.contains(trace_id) {
            return true;
        }
        if let Some(sid) = session_id.filter(|s| !s.is_empty()) {
            if self.erased_sessions.contains(sid) {
                return true;
            }
        }
        if let Some(uid) = user_id.filter(|s| !s.is_empty()) {
            if self.erased_users.contains(uid) {
                return true;
            }
        }
        false
    }

    #[inline]
    fn is_session_erased(&self, session_id: &str, user_id: Option<&str>) -> bool {
        if self.erased_sessions.contains(session_id) {
            return true;
        }
        if let Some(uid) = user_id.filter(|s| !s.is_empty()) {
            if self.erased_users.contains(uid) {
                return true;
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Zero-Dependency URL-Safe Base64 Keyset Cursor Codec
// ---------------------------------------------------------------------------

const B64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn base64url_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity((input.len() * 4).div_ceil(3));
    let mut i = 0;
    while i + 3 <= input.len() {
        let n = ((input[i] as u32) << 16) | ((input[i + 1] as u32) << 8) | (input[i + 2] as u32);
        out.push(B64URL[((n >> 18) & 0x3f) as usize] as char);
        out.push(B64URL[((n >> 12) & 0x3f) as usize] as char);
        out.push(B64URL[((n >> 6) & 0x3f) as usize] as char);
        out.push(B64URL[(n & 0x3f) as usize] as char);
        i += 3;
    }
    let rem = input.len() - i;
    if rem == 1 {
        let n = (input[i] as u32) << 16;
        out.push(B64URL[((n >> 18) & 0x3f) as usize] as char);
        out.push(B64URL[((n >> 12) & 0x3f) as usize] as char);
    } else if rem == 2 {
        let n = ((input[i] as u32) << 16) | ((input[i + 1] as u32) << 8);
        out.push(B64URL[((n >> 18) & 0x3f) as usize] as char);
        out.push(B64URL[((n >> 12) & 0x3f) as usize] as char);
        out.push(B64URL[((n >> 6) & 0x3f) as usize] as char);
    }
    out
}

fn b64_val(b: u8) -> Option<u32> {
    match b {
        b'A'..=b'Z' => Some((b - b'A') as u32),
        b'a'..=b'z' => Some((b - b'a' + 26) as u32),
        b'0'..=b'9' => Some((b - b'0' + 52) as u32),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    }
}

fn base64url_decode(s: &str) -> StoreResult<Vec<u8>> {
    let bytes = s.trim_end_matches('=').as_bytes();
    if bytes.len() % 4 == 1 {
        return Err(StoreError::InvalidIdentifier(
            "invalid cursor encoding".into(),
        ));
    }
    let mut out = Vec::with_capacity((bytes.len() * 3) / 4);
    let mut i = 0;
    while i + 4 <= bytes.len() {
        let a = b64_val(bytes[i])
            .ok_or_else(|| StoreError::InvalidIdentifier("invalid cursor".into()))?;
        let b = b64_val(bytes[i + 1])
            .ok_or_else(|| StoreError::InvalidIdentifier("invalid cursor".into()))?;
        let c = b64_val(bytes[i + 2])
            .ok_or_else(|| StoreError::InvalidIdentifier("invalid cursor".into()))?;
        let d = b64_val(bytes[i + 3])
            .ok_or_else(|| StoreError::InvalidIdentifier("invalid cursor".into()))?;
        let n = (a << 18) | (b << 12) | (c << 6) | d;
        out.push(((n >> 16) & 0xff) as u8);
        out.push(((n >> 8) & 0xff) as u8);
        out.push((n & 0xff) as u8);
        i += 4;
    }
    let rem = bytes.len() - i;
    if rem == 2 {
        let a = b64_val(bytes[i])
            .ok_or_else(|| StoreError::InvalidIdentifier("invalid cursor".into()))?;
        let b = b64_val(bytes[i + 1])
            .ok_or_else(|| StoreError::InvalidIdentifier("invalid cursor".into()))?;
        let n = (a << 18) | (b << 12);
        out.push(((n >> 16) & 0xff) as u8);
    } else if rem == 3 {
        let a = b64_val(bytes[i])
            .ok_or_else(|| StoreError::InvalidIdentifier("invalid cursor".into()))?;
        let b = b64_val(bytes[i + 1])
            .ok_or_else(|| StoreError::InvalidIdentifier("invalid cursor".into()))?;
        let c = b64_val(bytes[i + 2])
            .ok_or_else(|| StoreError::InvalidIdentifier("invalid cursor".into()))?;
        let n = (a << 18) | (b << 12) | (c << 6);
        out.push(((n >> 16) & 0xff) as u8);
        out.push(((n >> 8) & 0xff) as u8);
    }
    Ok(out)
}

#[derive(Debug, Serialize, Deserialize)]
struct KeysetCursor {
    sort_by: String,
    sort_dir: String,
    i64_val: Option<i64>,
    f64_bits: Option<u64>,
    tie_id: String,
}

fn encode_cursor(c: &KeysetCursor) -> StoreResult<String> {
    let raw = serde_json::to_vec(c)?;
    Ok(base64url_encode(&raw))
}

fn decode_cursor(
    raw_cursor: &str,
    expected_sort_by: &str,
    expected_sort_dir: &str,
) -> StoreResult<KeysetCursor> {
    let bytes = base64url_decode(raw_cursor)?;
    let cur: KeysetCursor = serde_json::from_slice(&bytes)
        .map_err(|_| StoreError::InvalidIdentifier("malformed keyset cursor".into()))?;
    if cur.sort_by != expected_sort_by || cur.sort_dir != expected_sort_dir {
        return Err(StoreError::InvalidIdentifier(
            "keyset cursor does not match sort parameters".into(),
        ));
    }
    Ok(cur)
}

// ---------------------------------------------------------------------------
// CRDT Summary Merge Helpers
// ---------------------------------------------------------------------------

struct MergedTraceEntry {
    trace: AgentTrace,
    meta: TraceCrdtMeta,
    crdts: HashMap<String, ResolvedSpanCrdt>,
    models: HashSet<String>,
    providers: HashSet<String>,
}

type V1SpanToolsEntry = HashMap<String, (Option<String>, Option<(u8, i64)>)>;
type V1SpanToolsMap = HashMap<String, V1SpanToolsEntry>;

fn record_v1_span_tool(map: &mut V1SpanToolsMap, resolved: &ResolvedSpanMetric) {
    if let Some(ref tn) = resolved.tool_name {
        let entry = map
            .entry(resolved.trace_id.clone())
            .or_default()
            .entry(resolved.span_id.clone())
            .or_insert((None, None));
        merge_opt_str_phase_ranked_preserve_empty(
            &mut entry.0,
            &mut entry.1,
            Some(tn.clone()),
            resolved.tool_name_rank,
        );
    }
}

fn apply_v1_span_tools(crdts: &mut [ResolvedSpanCrdt], span_tools: &V1SpanToolsEntry) {
    for c in crdts {
        if let Some((tn, rank)) = span_tools.get(&c.span_id) {
            merge_opt_str_phase_ranked_preserve_empty(
                &mut c.tool_name,
                &mut c.tool_name_rank,
                tn.clone(),
                *rank,
            );
        }
    }
}

fn resolve_span_crdts(
    row: &TraceSummaryRow,
    strings: &SegmentStringTable,
    has_ranks: bool,
) -> StoreResult<(Vec<ResolvedSpanCrdt>, HashSet<String>, HashSet<String>)> {
    let mut out = Vec::with_capacity(row.span_crdts.len());
    let mut models = HashSet::new();
    let mut providers = HashSet::new();
    let opt_req = |id: u32, ctx: &'static str| -> StoreResult<Option<String>> {
        strings.get_opt(id, ctx).map(|o| o.map(str::to_string))
    };
    for d in &row.span_crdts {
        let span_id = strings.get_required(d.span_id_str, "span_id")?.to_string();
        let model = opt_req(d.model_str, "span model_str")?.filter(|s| !s.is_empty());
        if d.span_type == SpanType::Llm.to_u8() {
            if let Some(ref m) = model {
                models.insert(m.clone());
            }
        }
        let provider = opt_req(d.provider_str, "span provider_str")?.filter(|s| !s.is_empty());
        if d.span_type == SpanType::Llm.to_u8() {
            if let Some(ref p) = provider {
                providers.insert(p.clone());
            }
        }
        let tool_name = opt_req(d.tool_name_str, "span tool_name_str")?;
        let span_name = opt_req(d.span_name_str, "span span_name_str")?;
        let tool_name_rank =
            (d.tool_name_phase != 0xFF).then_some((d.tool_name_phase, d.tool_name_ts));
        out.push(ResolvedSpanCrdt {
            span_id,
            span_type: d.span_type,
            span_phase: d.span_phase,
            status: d.status,
            depth: d.depth,
            start_ts_ms: d.start_ts_ms,
            input_tokens: d.input_tokens,
            output_tokens: d.output_tokens,
            cached_tokens: d.cached_tokens,
            total_tokens: d.total_tokens,
            cost_nanos: d.cost_nanos,
            model,
            provider,
            tool_name,
            tool_name_rank,
            span_name,
            has_v1_gap: !has_ranks,
        });
    }
    Ok((out, models, providers))
}

fn lightweight_l0_summary(
    tr: &AgentTrace,
    l0_meta: Option<&TraceCrdtMeta>,
    l0_span_ranks: &HashMap<(String, String), SpanFieldRanks>,
    spans: &[AgentSpan],
) -> (
    AgentTrace,
    TraceCrdtMeta,
    Vec<ResolvedSpanCrdt>,
    HashSet<String>,
    HashSet<String>,
) {
    let mut summary = tr.clone();
    summary.input = Value::Null;
    summary.output = Value::Null;
    summary.metadata = Value::Null;
    summary.error_message = None;

    let mut meta = l0_meta
        .cloned()
        .unwrap_or_else(|| TraceCrdtMeta::from_trace(tr));
    meta.field_ranks.output_rank = 0;
    meta.field_ranks.metadata_rank = 0;
    meta.field_ranks.error_message_rank = 0;

    let mut crdts = Vec::with_capacity(spans.len());
    let mut models = HashSet::new();
    let mut providers = HashSet::new();

    for sp in spans {
        let model = sp.model.clone().filter(|s| !s.is_empty());
        if sp.span_type == SpanType::Llm {
            if let Some(ref m) = model {
                models.insert(m.clone());
            }
        }
        let provider = sp.provider.clone().filter(|s| !s.is_empty());
        if sp.span_type == SpanType::Llm {
            if let Some(ref p) = provider {
                providers.insert(p.clone());
            }
        }
        let tool_name_rank = l0_span_ranks
            .get(&(tr.trace_id.clone(), sp.span_id.clone()))
            .and_then(|sr| sr.tool_name_rank)
            .or_else(|| {
                sp.tool_name
                    .as_ref()
                    .map(|_| (sp.span_phase.priority(), sp.start_ts_ms))
            });
        crdts.push(ResolvedSpanCrdt {
            span_id: sp.span_id.clone(),
            span_type: sp.span_type.to_u8(),
            span_phase: sp.span_phase.to_u8(),
            status: sp.status.to_u8(),
            depth: sp.depth,
            start_ts_ms: sp.start_ts_ms,
            input_tokens: sp.input_tokens,
            output_tokens: sp.output_tokens,
            cached_tokens: sp.cached_tokens,
            total_tokens: sp.total_tokens,
            cost_nanos: sp.cost_nanos,
            model,
            provider,
            tool_name: sp.tool_name.clone(),
            tool_name_rank,
            span_name: Some(sp.name.clone()),
            has_v1_gap: false,
        });
    }
    (summary, meta, crdts, models, providers)
}

fn merge_into_map(
    merged: &mut HashMap<String, MergedTraceEntry>,
    src_trace: AgentTrace,
    src_meta: TraceCrdtMeta,
    src_crdts: Vec<ResolvedSpanCrdt>,
    models: HashSet<String>,
    providers: HashSet<String>,
) {
    let tid = src_trace.trace_id.clone();
    if let Some(existing) = merged.get_mut(&tid) {
        merge_trace_summary_rows_with_meta(
            &mut existing.trace,
            &mut existing.meta,
            &mut existing.crdts,
            src_trace,
            src_meta,
            src_crdts,
        );
        existing.models.extend(models);
        existing.providers.extend(providers);
    } else {
        let mut crdts = HashMap::with_capacity(src_crdts.len());
        let mut dummy = src_trace.clone();
        let mut meta = src_meta.clone();
        // Normalize CRDT map and rollups via merge_trace_summary_rows_with_meta
        merge_trace_summary_rows_with_meta(
            &mut dummy, &mut meta, &mut crdts, src_trace, src_meta, src_crdts,
        );
        merged.insert(
            tid,
            MergedTraceEntry {
                trace: dummy,
                meta,
                crdts,
                models,
                providers,
            },
        );
    }
}

fn is_error_status(st: TraceStatus) -> bool {
    matches!(st, TraceStatus::Error | TraceStatus::Timeout)
}

// ---------------------------------------------------------------------------
// 1. list_sessions
// ---------------------------------------------------------------------------

struct SessionAgg {
    session_id: String,
    project_id: String,
    user_id: Option<String>,
    environment: Option<String>,
    agent_names: BTreeSet<String>,
    first_ts_ms: i64,
    last_ts_ms: i64,
    trace_count: i64,
    error_count: i64,
    total_tokens: i64,
    total_cost_nanos: i64,
    seen_trace_ids: HashSet<String>,
}

impl SessionAgg {
    fn into_summary(self) -> SessionSummary {
        SessionSummary {
            session_id: self.session_id,
            project_id: self.project_id,
            user_id: self.user_id,
            agent_names: self.agent_names.into_iter().collect(),
            environment: self.environment,
            first_ts_ms: self.first_ts_ms,
            last_ts_ms: self.last_ts_ms,
            trace_count: self.trace_count,
            error_count: self.error_count,
            total_tokens: self.total_tokens,
            total_cost_nanos: self.total_cost_nanos,
            total_cost_usd: cost_nanos_to_usd(self.total_cost_nanos),
            avg_eval_score: None,
            eval_pass_rate: None,
        }
    }
}

fn merge_session_env(dst: &mut Option<String>, src: Option<String>) {
    if dst.as_ref().is_some_and(|d| d.is_empty()) {
        *dst = None;
    }
    if let Some(s) = src.filter(|v| !v.is_empty()) {
        let take = match dst.as_deref() {
            None => true,
            Some(d) => (env_rank(&s), s.as_str()) > (env_rank(d), d),
        };
        if take {
            *dst = Some(s);
        }
    }
}

pub(crate) async fn list_sessions_impl(
    store: &AgentTraceStore,
    project_id: &str,
    filter: &SessionListFilter,
    tombstones: &[AgentErasureTombstone],
) -> StoreResult<SessionListResponse> {
    if store.closed.load(Ordering::Acquire) {
        return Err(StoreError::Shutdown);
    }
    validate_identifier("project_id", project_id)?;

    let snap = store.acquire_snapshot(project_id).await?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let tomb_idx =
        ActiveTombstones::new(project_id, tombstones, &snap.manifest.erased_traces, now_ms);
    let reader = store.plss_reader();

    let mut sessions: HashMap<String, SessionAgg> = HashMap::new();
    let mut recompute_sessions: HashSet<String> = HashSet::new();
    let mut excluded_sessions: HashSet<String> = HashSet::new();
    let mut trace_to_session: HashMap<String, String> = HashMap::new();
    let mut all_segments: Vec<(&SegmentDescriptor, Arc<SegmentTail>, HashSet<String>)> =
        Vec::with_capacity(snap.manifest.segments.len());

    for seg in &snap.manifest.segments {
        let Some(tail) = tail_or_skip(&reader, seg).await? else {
            continue;
        };
        if seg.zone_map.session_count == 0 {
            all_segments.push((seg, tail, HashSet::new()));
            continue;
        }

        let seg_may_have_erased_user = tomb_idx
            .erased_users
            .iter()
            .any(|uid| tail.index.may_contain_user(uid));
        let strings = reader.read_string_table(seg, &tail).await?;
        let rollups = reader.read_session_rollups(seg, &tail).await?;
        let mut accounted_tids = HashSet::new();

        for row in rollups.iter() {
            let sid = strings.get_required(row.session_id_str, "session_id")?;
            let uid = strings
                .get_opt(row.user_id_str, "session user_id")?
                .filter(|s| !s.is_empty())
                .map(str::to_string);

            let mut has_erased_trace = false;
            let mut row_tids = Vec::with_capacity(row.trace_id_strs.len());
            for &tid_str in &row.trace_id_strs {
                let tid = strings.get_required(tid_str, "session trace_id")?;
                trace_to_session.insert(tid.to_string(), sid.to_string());
                accounted_tids.insert(tid.to_string());
                if tomb_idx.erased_traces.contains(tid) {
                    has_erased_trace = true;
                }
                row_tids.push(tid.to_string());
            }

            if tomb_idx.is_session_erased(sid, uid.as_deref()) {
                excluded_sessions.insert(sid.to_string());
                continue;
            }
            if has_erased_trace || seg_may_have_erased_user {
                recompute_sessions.insert(sid.to_string());
            }

            let env = strings
                .get_opt(row.environment_str, "session environment")?
                .filter(|s| !s.is_empty())
                .map(str::to_string);

            let agg = sessions
                .entry(sid.to_string())
                .or_insert_with(|| SessionAgg {
                    session_id: sid.to_string(),
                    project_id: project_id.to_string(),
                    user_id: uid.clone(),
                    environment: env.clone(),
                    agent_names: BTreeSet::new(),
                    first_ts_ms: row.first_ts_ms,
                    last_ts_ms: row.last_ts_ms,
                    trace_count: 0,
                    error_count: 0,
                    total_tokens: 0,
                    total_cost_nanos: 0,
                    seen_trace_ids: HashSet::new(),
                });
            merge_opt_str_max(&mut agg.user_id, uid);
            merge_session_env(&mut agg.environment, env);
            for &aid in &row.agent_name_strs {
                let a = strings.get_required(aid, "session agent_name")?;
                if !a.is_empty() {
                    agg.agent_names.insert(a.to_string());
                }
            }
            agg.first_ts_ms = agg.first_ts_ms.min(row.first_ts_ms);
            agg.last_ts_ms = agg.last_ts_ms.max(row.last_ts_ms);
            agg.trace_count = agg.trace_count.saturating_add(row.trace_count);
            agg.error_count = agg.error_count.saturating_add(row.error_count);
            agg.total_tokens = agg.total_tokens.saturating_add(row.total_tokens.max(0));
            agg.total_cost_nanos = agg
                .total_cost_nanos
                .saturating_add(row.total_cost_nanos.max(0));
            for tid in row_tids {
                if !agg.seen_trace_ids.insert(tid) {
                    recompute_sessions.insert(sid.to_string());
                }
            }
        }
        all_segments.push((seg, tail, accounted_tids));
    }

    // Overlay L0 entries in deterministic `(start_ts_ms, trace_id)` order
    let mut sorted_l0: Vec<&(AgentTrace, Vec<AgentSpan>)> = snap.l0_entries.iter().collect();
    sorted_l0.sort_by(|a, b| {
        a.0.start_ts_ms
            .cmp(&b.0.start_ts_ms)
            .then_with(|| a.0.trace_id.cmp(&b.0.trace_id))
    });
    for (tr, _spans) in sorted_l0 {
        let explicit_sid = tr.session_id.as_deref().filter(|s| !s.is_empty());
        if let Some(sid) = explicit_sid {
            trace_to_session
                .entry(tr.trace_id.clone())
                .or_insert_with(|| sid.to_string());
        }
        let resolved_sid =
            explicit_sid.or_else(|| trace_to_session.get(&tr.trace_id).map(String::as_str));
        let Some(sid) = resolved_sid else {
            continue;
        };
        if tomb_idx.is_session_erased(sid, tr.user_id.as_deref()) {
            excluded_sessions.insert(sid.to_string());
            continue;
        }
        if tomb_idx.is_trace_erased(&tr.trace_id, Some(sid), tr.user_id.as_deref()) {
            if trace_to_session.contains_key(&tr.trace_id)
                || sessions
                    .get(sid)
                    .is_some_and(|agg| agg.seen_trace_ids.contains(&tr.trace_id))
            {
                recompute_sessions.insert(sid.to_string());
            }
            continue;
        }
        if explicit_sid.is_none() {
            recompute_sessions.insert(sid.to_string());
            continue;
        }
        if let Some(agg) = sessions.get_mut(sid) {
            if agg.seen_trace_ids.contains(&tr.trace_id) {
                recompute_sessions.insert(sid.to_string());
                continue;
            }
            merge_opt_str_max(&mut agg.user_id, tr.user_id.clone());
            merge_session_env(&mut agg.environment, Some(tr.environment.clone()));
            if !tr.agent_name.is_empty() {
                agg.agent_names.insert(tr.agent_name.clone());
            }
            agg.first_ts_ms = agg.first_ts_ms.min(tr.start_ts_ms);
            agg.last_ts_ms = agg.last_ts_ms.max(tr.start_ts_ms);
            agg.trace_count = agg.trace_count.saturating_add(1);
            if is_error_status(tr.status) {
                agg.error_count = agg.error_count.saturating_add(1);
            }
            agg.total_tokens = agg.total_tokens.saturating_add(tr.total_tokens.max(0));
            agg.total_cost_nanos = agg
                .total_cost_nanos
                .saturating_add(tr.total_cost_nanos.max(0));
            agg.seen_trace_ids.insert(tr.trace_id.clone());
        } else {
            let mut agent_names = BTreeSet::new();
            if !tr.agent_name.is_empty() {
                agent_names.insert(tr.agent_name.clone());
            }
            let mut seen = HashSet::new();
            seen.insert(tr.trace_id.clone());
            sessions.insert(
                sid.to_string(),
                SessionAgg {
                    session_id: sid.to_string(),
                    project_id: project_id.to_string(),
                    user_id: tr.user_id.clone().filter(|s| !s.is_empty()),
                    environment: (!tr.environment.is_empty()).then(|| tr.environment.clone()),
                    agent_names,
                    first_ts_ms: tr.start_ts_ms,
                    last_ts_ms: tr.start_ts_ms,
                    trace_count: 1,
                    error_count: i64::from(is_error_status(tr.status)),
                    total_tokens: tr.total_tokens.max(0),
                    total_cost_nanos: tr.total_cost_nanos.max(0),
                    seen_trace_ids: seen,
                },
            );
        }
    }

    // Check whether any segment (including sessionless segments with session_count == 0)
    // contains an unaccounted partial/span-only update for a trace belonging to a known session.
    let mut recompute_seg_indices: HashSet<usize> = HashSet::new();
    for (seg_idx, (_seg, tail, accounted_tids)) in all_segments.iter().enumerate() {
        for (tid, sid) in &trace_to_session {
            if excluded_sessions.contains(sid) || accounted_tids.contains(tid) {
                continue;
            }
            if tail.index.may_contain_trace(tid) {
                recompute_sessions.insert(sid.clone());
                recompute_seg_indices.insert(seg_idx);
            }
        }
    }

    recompute_sessions.retain(|sid| !excluded_sessions.contains(sid));

    // Recompute affected sessions cleanly via Tag 0x02 + L0 CRDT merge when trace erasure
    // or cross-segment/L0 trace overlap occurred.
    if !recompute_sessions.is_empty() {
        let mut merged: HashMap<String, MergedTraceEntry> = HashMap::new();
        for (seg_idx, (seg, tail, _accounted_tids)) in all_segments.into_iter().enumerate() {
            let matches_seg = recompute_seg_indices.contains(&seg_idx)
                || recompute_sessions
                    .iter()
                    .any(|sid| tail.index.may_contain_session(sid));
            if !matches_seg {
                continue;
            }
            let strings = reader.read_string_table(seg, &tail).await?;
            let summaries = reader.read_trace_summaries(seg, &tail).await?;
            let has_ranks = tail.tag02_has_ranks();
            for row in summaries.iter() {
                let row_tid = strings.get_required(row.trace_id_str, "trace_id")?;
                let row_sid = strings
                    .get_opt(row.session_id_str, "session_id")?
                    .filter(|s| !s.is_empty());
                let belongs = row_sid.is_some_and(|sid| recompute_sessions.contains(sid))
                    || trace_to_session
                        .get(row_tid)
                        .is_some_and(|sid| recompute_sessions.contains(sid));
                if !belongs {
                    continue;
                }
                let tr = row.to_agent_trace(project_id, &strings)?;
                let tr_meta = row.crdt_meta(&strings)?;
                let (crdts, models, providers) = resolve_span_crdts(row, &strings, has_ranks)?;
                merge_into_map(&mut merged, tr, tr_meta, crdts, models, providers);
            }
        }
        for (tr, spans) in &snap.l0_entries {
            let resolved_sid = tr
                .session_id
                .as_deref()
                .filter(|s| !s.is_empty())
                .or_else(|| trace_to_session.get(&tr.trace_id).map(String::as_str));
            if resolved_sid.is_some_and(|sid| recompute_sessions.contains(sid))
                || merged.contains_key(&tr.trace_id)
            {
                let (summary, l0_meta, crdts, models, providers) = lightweight_l0_summary(
                    tr,
                    snap.l0_meta.get(&tr.trace_id),
                    &snap.l0_span_ranks,
                    spans,
                );
                merge_into_map(&mut merged, summary, l0_meta, crdts, models, providers);
            }
        }
        for sid in &recompute_sessions {
            sessions.remove(sid);
        }
        let mut merged_traces: Vec<AgentTrace> = merged.into_values().map(|e| e.trace).collect();
        merged_traces.sort_by(|a, b| {
            a.start_ts_ms
                .cmp(&b.start_ts_ms)
                .then_with(|| a.trace_id.cmp(&b.trace_id))
        });
        for tr in merged_traces {
            let Some(sid) = tr.session_id.as_deref().filter(|s| !s.is_empty()) else {
                continue;
            };
            if tomb_idx.is_session_erased(sid, tr.user_id.as_deref()) {
                excluded_sessions.insert(sid.to_string());
                continue;
            }
            if tomb_idx.is_trace_erased(&tr.trace_id, Some(sid), tr.user_id.as_deref()) {
                continue;
            }
            let agg = sessions
                .entry(sid.to_string())
                .or_insert_with(|| SessionAgg {
                    session_id: sid.to_string(),
                    project_id: project_id.to_string(),
                    user_id: tr.user_id.clone().filter(|s| !s.is_empty()),
                    environment: (!tr.environment.is_empty()).then(|| tr.environment.clone()),
                    agent_names: BTreeSet::new(),
                    first_ts_ms: tr.start_ts_ms,
                    last_ts_ms: tr.start_ts_ms,
                    trace_count: 0,
                    error_count: 0,
                    total_tokens: 0,
                    total_cost_nanos: 0,
                    seen_trace_ids: HashSet::new(),
                });
            merge_opt_str_max(&mut agg.user_id, tr.user_id.clone());
            merge_session_env(&mut agg.environment, Some(tr.environment.clone()));
            if !tr.agent_name.is_empty() {
                agg.agent_names.insert(tr.agent_name.clone());
            }
            agg.first_ts_ms = agg.first_ts_ms.min(tr.start_ts_ms);
            agg.last_ts_ms = agg.last_ts_ms.max(tr.start_ts_ms);
            agg.trace_count = agg.trace_count.saturating_add(1);
            if is_error_status(tr.status) {
                agg.error_count = agg.error_count.saturating_add(1);
            }
            agg.total_tokens = agg.total_tokens.saturating_add(tr.total_tokens.max(0));
            agg.total_cost_nanos = agg
                .total_cost_nanos
                .saturating_add(tr.total_cost_nanos.max(0));
        }
    }

    for sid in &excluded_sessions {
        sessions.remove(sid);
    }

    let search_lower = filter
        .search
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_lowercase);

    let mut matched: Vec<SessionSummary> = sessions
        .into_values()
        .map(SessionAgg::into_summary)
        .filter(|s| {
            if let Some(uid) = filter.user_id.as_deref().filter(|v| !v.is_empty()) {
                if s.user_id.as_deref() != Some(uid) {
                    return false;
                }
            }
            if let Some(agent) = filter.agent_name.as_deref().filter(|v| !v.is_empty()) {
                if !s.agent_names.iter().any(|a| a == agent) {
                    return false;
                }
            }
            if let Some(env) = filter.environment.as_deref().filter(|v| !v.is_empty()) {
                if s.environment.as_deref() != Some(env) {
                    return false;
                }
            }
            if let Some(has_err) = filter.has_error {
                if has_err && s.error_count == 0 {
                    return false;
                }
                if !has_err && s.error_count > 0 {
                    return false;
                }
            }
            if let Some(start) = filter.start_ts_ms {
                if s.last_ts_ms < start {
                    return false;
                }
            }
            if let Some(end) = filter.end_ts_ms {
                if s.first_ts_ms > end {
                    return false;
                }
            }
            if let Some(ref q) = search_lower {
                let in_sid = s.session_id.to_ascii_lowercase().contains(q);
                let in_uid = s
                    .user_id
                    .as_deref()
                    .is_some_and(|u| u.to_ascii_lowercase().contains(q));
                let in_agent = s
                    .agent_names
                    .iter()
                    .any(|a| a.to_ascii_lowercase().contains(q));
                if !in_sid && !in_uid && !in_agent {
                    return false;
                }
            }
            true
        })
        .collect();

    let sort_by = match filter.sort_by.as_deref().unwrap_or("last_ts_ms") {
        "first_ts_ms" => "first_ts_ms",
        "trace_count" => "trace_count",
        "total_cost_usd" | "total_cost_nanos" | "cost" => "total_cost_nanos",
        "total_tokens" | "tokens" => "total_tokens",
        _ => "last_ts_ms",
    };
    let sort_dir = match filter.sort_dir.as_deref() {
        Some(d) if d.eq_ignore_ascii_case("asc") => "asc",
        _ => "desc",
    };
    let asc = sort_dir == "asc";

    let session_key_i64 = |s: &SessionSummary| -> i64 {
        match sort_by {
            "first_ts_ms" => s.first_ts_ms,
            "trace_count" => s.trace_count,
            "total_cost_nanos" => s.total_cost_nanos,
            "total_tokens" => s.total_tokens,
            _ => s.last_ts_ms,
        }
    };

    matched.sort_by(|a, b| {
        let ord = session_key_i64(a)
            .cmp(&session_key_i64(b))
            .then_with(|| a.session_id.cmp(&b.session_id));
        if asc {
            ord
        } else {
            ord.reverse()
        }
    });

    let total = matched.len() as i64;
    let limit = filter.limit.unwrap_or(50).clamp(1, MAX_TRACE_LIST_LIMIT);
    let offset = filter.offset.unwrap_or(0);

    let sliced: Vec<SessionSummary> = if let Some(ref raw_cur) = filter.cursor {
        let cur = decode_cursor(raw_cur, sort_by, sort_dir)?;
        let cur_val = cur.i64_val.unwrap_or(0);
        matched
            .into_iter()
            .filter(|s| {
                let ord = session_key_i64(s)
                    .cmp(&cur_val)
                    .then_with(|| s.session_id.cmp(&cur.tie_id));
                if asc {
                    ord == std::cmp::Ordering::Greater
                } else {
                    ord == std::cmp::Ordering::Less
                }
            })
            .collect()
    } else {
        matched.into_iter().skip(offset).collect()
    };

    let has_more = sliced.len() > limit;
    let page: Vec<SessionSummary> = sliced.into_iter().take(limit).collect();
    let next_cursor = if has_more {
        page.last().and_then(|last| {
            encode_cursor(&KeysetCursor {
                sort_by: sort_by.to_string(),
                sort_dir: sort_dir.to_string(),
                i64_val: Some(session_key_i64(last)),
                f64_bits: None,
                tie_id: last.session_id.clone(),
            })
            .ok()
        })
    } else {
        None
    };

    Ok(SessionListResponse {
        sessions: page,
        total,
        limit,
        offset: if filter.cursor.is_some() { 0 } else { offset },
        next_cursor,
    })
}

// ---------------------------------------------------------------------------
// 2. get_session_detail
// ---------------------------------------------------------------------------

pub(crate) async fn get_session_detail_impl(
    store: &AgentTraceStore,
    project_id: &str,
    session_id: &str,
    tombstones: &[AgentErasureTombstone],
) -> StoreResult<Option<SessionDetail>> {
    if store.closed.load(Ordering::Acquire) {
        return Err(StoreError::Shutdown);
    }
    validate_identifier("project_id", project_id)?;
    if session_id.trim().is_empty() {
        return Ok(None);
    }

    let now_ms = chrono::Utc::now().timestamp_millis();
    let pre_tomb_idx = ActiveTombstones::new_tombstones_only(project_id, tombstones, now_ms);
    if pre_tomb_idx.is_session_erased(session_id, None) {
        return Ok(None);
    }

    let snap = store.acquire_snapshot(project_id).await?;
    let tomb_idx =
        ActiveTombstones::new(project_id, tombstones, &snap.manifest.erased_traces, now_ms);
    let reader = store.plss_reader();
    let mut merged: HashMap<String, MergedTraceEntry> = HashMap::new();
    let mut remaining_segments: Vec<(&SegmentDescriptor, Arc<SegmentTail>, HashSet<String>)> =
        Vec::new();

    for seg in &snap.manifest.segments {
        let Some(tail) = tail_or_skip(&reader, seg).await? else {
            continue;
        };
        if seg.zone_map.session_count == 0 || !tail.index.may_contain_session(session_id) {
            remaining_segments.push((seg, tail, HashSet::new()));
            continue;
        }
        let strings = reader.read_string_table(seg, &tail).await?;
        let summaries = reader.read_trace_summaries(seg, &tail).await?;
        let has_ranks = tail.tag02_has_ranks();
        let mut read_tids = HashSet::new();
        for row in summaries.iter() {
            if strings.get_opt(row.session_id_str, "session_id")? != Some(session_id) {
                continue;
            }
            let tr = row.to_agent_trace(project_id, &strings)?;
            let tr_meta = row.crdt_meta(&strings)?;
            read_tids.insert(tr.trace_id.clone());
            let (crdts, models, providers) = resolve_span_crdts(row, &strings, has_ranks)?;
            merge_into_map(&mut merged, tr, tr_meta, crdts, models, providers);
        }
        if read_tids.len() < summaries.len() {
            remaining_segments.push((seg, tail, read_tids));
        }
    }

    for (tr, spans) in &snap.l0_entries {
        if tr.session_id.as_deref() == Some(session_id) || merged.contains_key(&tr.trace_id) {
            let (summary, l0_meta, crdts, models, providers) = lightweight_l0_summary(
                tr,
                snap.l0_meta.get(&tr.trace_id),
                &snap.l0_span_ranks,
                spans,
            );
            merge_into_map(&mut merged, summary, l0_meta, crdts, models, providers);
        }
    }

    // Pass 2: complete split/sessionless updates (session_count == 0 or row.session_id_str == NONE)
    // from remaining segments ONLY when Tag 0x06 Bloom hits a trace_id belonging to this session.
    if !merged.is_empty() && !remaining_segments.is_empty() {
        let candidate_tids: HashSet<String> = merged.keys().cloned().collect();
        for (seg, tail, read_tids) in remaining_segments {
            let may_hit = candidate_tids
                .iter()
                .any(|tid| !read_tids.contains(tid) && tail.index.may_contain_trace(tid));
            if !may_hit {
                continue;
            }
            let strings = reader.read_string_table(seg, &tail).await?;
            let summaries = reader.read_trace_summaries(seg, &tail).await?;
            let has_ranks = tail.tag02_has_ranks();
            for row in summaries.iter() {
                let tid = strings.get_required(row.trace_id_str, "trace_id")?;
                if read_tids.contains(tid) || !candidate_tids.contains(tid) {
                    continue;
                }
                let tr = row.to_agent_trace(project_id, &strings)?;
                let tr_meta = row.crdt_meta(&strings)?;
                let (crdts, models, providers) = resolve_span_crdts(row, &strings, has_ranks)?;
                merge_into_map(&mut merged, tr, tr_meta, crdts, models, providers);
            }
        }
    }

    // Match list_sessions_impl: if ANY trace belonging to this session matches a Session or User
    // tombstone, exclude the entire session even if sibling traces have user_id = None.
    if merged.values().any(|e| {
        tomb_idx.is_session_erased(session_id, e.trace.user_id.as_deref())
            || e.trace
                .session_id
                .as_deref()
                .is_some_and(|sid| tomb_idx.is_session_erased(sid, e.trace.user_id.as_deref()))
    }) {
        return Ok(None);
    }

    let mut traces: Vec<AgentTrace> = merged
        .into_values()
        .map(|e| e.trace)
        .filter(|tr| {
            !tomb_idx.is_trace_erased(
                &tr.trace_id,
                tr.session_id.as_deref(),
                tr.user_id.as_deref(),
            )
        })
        .collect();

    if traces.is_empty() {
        return Ok(None);
    }

    traces.sort_by(|a, b| {
        a.start_ts_ms
            .cmp(&b.start_ts_ms)
            .then_with(|| a.trace_id.cmp(&b.trace_id))
    });

    let mut agent_names = BTreeSet::new();
    let mut user_id = None;
    let mut environment = None;
    let mut first_ts_ms = i64::MAX;
    let mut last_ts_ms = i64::MIN;
    let mut error_count = 0i64;
    let mut total_tokens = 0i64;
    let mut total_cost_nanos = 0i64;

    for tr in &traces {
        if !tr.agent_name.is_empty() {
            agent_names.insert(tr.agent_name.clone());
        }
        merge_opt_str_max(&mut user_id, tr.user_id.clone());
        merge_session_env(&mut environment, Some(tr.environment.clone()));
        first_ts_ms = first_ts_ms.min(tr.start_ts_ms);
        last_ts_ms = last_ts_ms.max(tr.start_ts_ms);
        if is_error_status(tr.status) {
            error_count += 1;
        }
        total_tokens = total_tokens.saturating_add(tr.total_tokens.max(0));
        total_cost_nanos = total_cost_nanos.saturating_add(tr.total_cost_nanos.max(0));
    }

    let summary = SessionSummary {
        session_id: session_id.to_string(),
        project_id: project_id.to_string(),
        user_id,
        agent_names: agent_names.into_iter().collect(),
        environment,
        first_ts_ms,
        last_ts_ms,
        trace_count: traces.len() as i64,
        error_count,
        total_tokens,
        total_cost_nanos,
        total_cost_usd: cost_nanos_to_usd(total_cost_nanos),
        avg_eval_score: None,
        eval_pass_rate: None,
    };

    Ok(Some(SessionDetail {
        summary,
        traces,
        evaluations: Vec::new(),
    }))
}

// ---------------------------------------------------------------------------
// 3. list_traces
// ---------------------------------------------------------------------------

fn matches_trace_list_filter(entry: &MergedTraceEntry, filter: &TraceListFilter) -> bool {
    let t = &entry.trace;
    if let Some(a) = filter.agent_name.as_deref().filter(|s| !s.is_empty()) {
        if t.agent_name != a {
            return false;
        }
    }
    if let Some(av) = filter.agent_version.as_deref().filter(|s| !s.is_empty()) {
        if t.agent_version != av {
            return false;
        }
    }
    if let Some(pv) = filter.prompt_version.as_deref().filter(|s| !s.is_empty()) {
        if t.prompt_version.as_deref() != Some(pv) {
            return false;
        }
    }
    if let Some(env) = filter.environment.as_deref().filter(|s| !s.is_empty()) {
        if t.environment != env {
            return false;
        }
    }
    if let Some(st) = filter.status.as_deref().filter(|s| !s.is_empty()) {
        if t.status != TraceStatus::parse(st) {
            return false;
        }
    }
    if let Some(m) = filter.model.as_deref().filter(|s| !s.is_empty()) {
        let matched_crdt = if entry.crdts.is_empty() {
            entry.models.contains(m)
        } else {
            entry
                .crdts
                .values()
                .any(|c| c.span_type == SpanType::Llm.to_u8() && c.model.as_deref() == Some(m))
        };
        if t.primary_model.as_deref() != Some(m) && !matched_crdt {
            return false;
        }
    }
    if let Some(p) = filter.provider.as_deref().filter(|s| !s.is_empty()) {
        let matched_crdt = if entry.crdts.is_empty() {
            entry.providers.contains(p)
        } else {
            entry
                .crdts
                .values()
                .any(|c| c.span_type == SpanType::Llm.to_u8() && c.provider.as_deref() == Some(p))
        };
        if t.primary_provider.as_deref() != Some(p) && !matched_crdt {
            return false;
        }
    }
    if let Some(tag) = filter.tag.as_deref().filter(|s| !s.is_empty()) {
        if !t.tags.iter().any(|tg| tg == tag) {
            return false;
        }
    }
    if let Some(uid) = filter.user_id.as_deref().filter(|s| !s.is_empty()) {
        if t.user_id.as_deref() != Some(uid) {
            return false;
        }
    }
    if let Some(sid) = filter.session_id.as_deref().filter(|s| !s.is_empty()) {
        if t.session_id.as_deref() != Some(sid) {
            return false;
        }
    }
    if let Some(ptid) = filter.parent_trace_id.as_deref().filter(|s| !s.is_empty()) {
        if t.parent_trace_id.as_deref() != Some(ptid) {
            return false;
        }
    }
    if let Some(has_err) = filter.has_error {
        let is_err = is_error_status(t.status);
        if has_err != is_err {
            return false;
        }
    }
    if let Some(has_loop) = filter.has_loop_warning {
        if t.has_loop_warning != has_loop {
            return false;
        }
    }
    if let Some(min_d) = filter.min_duration_ms {
        if t.duration_ms < min_d {
            return false;
        }
    }
    if let Some(max_d) = filter.max_duration_ms {
        if t.duration_ms > max_d {
            return false;
        }
    }
    if let Some(min_c) = filter.min_cost_usd {
        if t.total_cost_usd < min_c {
            return false;
        }
    }
    if let Some(max_c) = filter.max_cost_usd {
        if t.total_cost_usd > max_c {
            return false;
        }
    }
    if let Some(min_s) = filter.min_eval_score {
        if t.avg_eval_score.is_none_or(|s| s < min_s) {
            return false;
        }
    }
    if let Some(max_s) = filter.max_eval_score {
        if t.avg_eval_score.is_none_or(|s| s > max_s) {
            return false;
        }
    }
    if let Some(start) = filter.start_ts_ms {
        if t.start_ts_ms < start {
            return false;
        }
    }
    if let Some(end) = filter.end_ts_ms {
        if t.start_ts_ms > end {
            return false;
        }
    }
    if let Some(q) = filter
        .search
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let ql = q.to_ascii_lowercase();
        let in_tid = t.trace_id.to_ascii_lowercase().contains(&ql);
        let in_agent = t.agent_name.to_ascii_lowercase().contains(&ql);
        let in_ver = t.agent_version.to_ascii_lowercase().contains(&ql);
        let in_err = t
            .error_type
            .as_deref()
            .is_some_and(|e| e.to_ascii_lowercase().contains(&ql));
        let in_model = t
            .primary_model
            .as_deref()
            .is_some_and(|m| m.to_ascii_lowercase().contains(&ql));
        if !in_tid && !in_agent && !in_ver && !in_err && !in_model {
            return false;
        }
    }
    true
}

enum PrunedSegment<'a> {
    Skipped {
        seg: &'a SegmentDescriptor,
        tail: Arc<SegmentTail>,
    },
    Partial {
        seg: &'a SegmentDescriptor,
        tail: Arc<SegmentTail>,
        read_indices: RoaringBitmap,
        read_tids: HashSet<String>,
    },
}

impl<'a> PrunedSegment<'a> {
    fn seg(&self) -> &'a SegmentDescriptor {
        match self {
            Self::Skipped { seg, .. } | Self::Partial { seg, .. } => seg,
        }
    }

    fn tail(&self) -> &SegmentTail {
        match self {
            Self::Skipped { tail, .. } | Self::Partial { tail, .. } => tail,
        }
    }

    fn read_indices(&self) -> Option<&RoaringBitmap> {
        match self {
            Self::Skipped { .. } => None,
            Self::Partial { read_indices, .. } => Some(read_indices),
        }
    }

    fn any_missing_candidate_may_exist(&self, candidate_tids: &HashSet<String>) -> bool {
        let tail = self.tail();
        match self {
            Self::Skipped { .. } => candidate_tids
                .iter()
                .any(|tid| tail.index.may_contain_trace(tid)),
            Self::Partial { read_tids, .. } => candidate_tids
                .iter()
                .any(|tid| !read_tids.contains(tid) && tail.index.may_contain_trace(tid)),
        }
    }
}

fn pass1_segment_candidates(
    index: &SegmentIndexSection,
    filter: &TraceListFilter,
    tool_name: Option<&str>,
    multi_segment: bool,
    l0_empty: bool,
) -> (bool, Option<RoaringBitmap>) {
    if !multi_segment {
        if !index.zone_map_matches_trace_filter(filter) {
            return (false, None);
        }
        if l0_empty
            && filter.has_loop_warning == Some(false)
            && index.zone_map.trace_count > 0
            && index.zone_map.loop_count >= index.zone_map.trace_count
        {
            return (false, None);
        }
        let mut cand_bm = index.candidate_trace_indices(filter);
        if let Some(tn) = tool_name.filter(|s| !s.is_empty()) {
            if let Some(tool_bm) = index.candidate_traces_for_tool(tn) {
                match &mut cand_bm {
                    Some(cur) => *cur &= tool_bm,
                    None => cand_bm = Some(tool_bm),
                }
            }
        }
        if cand_bm.as_ref().is_some_and(|bm| bm.is_empty()) {
            return (false, None);
        }
        return (true, cand_bm);
    }

    if index.zone_map.trace_count == 0 {
        return (false, None);
    }

    // 1. Strictly monotonic per-segment hard bounds (sound across arbitrary CRDT splits).
    // Note: `has_loop_warning == Some(false)` is NOT monotonic across segments/L0 because
    // a span-phase upgrade (e.g., Start(Llm) -> Final(Tool)) can clear a transient loop.
    if let Some(start) = filter.start_ts_ms {
        if index.zone_map.min_start_ts_ms > 0 && index.zone_map.max_start_ts_ms < start {
            return (false, None);
        }
    }
    if let Some(max_dur) = filter.max_duration_ms {
        if index.zone_map.min_duration_ms > max_dur {
            return (false, None);
        }
    }
    if let Some(max_cost) = filter.max_cost_usd.filter(|c| !c.is_nan()) {
        let max_nanos = usd_to_cost_nanos(max_cost).saturating_add(1);
        if index.zone_map.min_cost_nanos > max_nanos {
            return (false, None);
        }
    }

    // 2. Single Fixed Witness selection (in selectivity order).
    // Every witness branch MUST return directly (never falling through to another witness)
    // so that every segment evaluates the exact same witness predicate.
    let eval_dict_witness = |map: &BTreeMap<String, RoaringBitmap>,
                             truncated: bool,
                             key: &str|
     -> (bool, Option<RoaringBitmap>) {
        if let Some(bm) = map.get(key) {
            if bm.is_empty() {
                (false, None)
            } else {
                (true, Some(bm.clone()))
            }
        } else if !truncated {
            (false, None)
        } else {
            (true, None)
        }
    };

    if let Some(tn) = tool_name.filter(|s| !s.is_empty()) {
        return match index.candidate_traces_for_tool(tn) {
            Some(bm) if bm.is_empty() => (false, None),
            Some(bm) => (true, Some(bm)),
            None => (true, None),
        };
    }

    if let Some(model) = filter.model.as_deref().filter(|s| !s.is_empty()) {
        return eval_dict_witness(&index.model_bitmaps, index.model_truncated, model);
    }

    if let Some(sid) = filter.session_id.as_deref().filter(|s| !s.is_empty()) {
        return (index.may_contain_session(sid), None);
    }

    if let Some(uid) = filter.user_id.as_deref().filter(|s| !s.is_empty()) {
        return (index.may_contain_user(uid), None);
    }

    if let Some(ptid) = filter.parent_trace_id.as_deref().filter(|s| !s.is_empty()) {
        return (index.may_contain_parent_trace(ptid), None);
    }

    if let Some(agent) = filter.agent_name.as_deref().filter(|s| !s.is_empty()) {
        return eval_dict_witness(&index.agent_name_bitmaps, index.agent_name_truncated, agent);
    }

    if let Some(status_str) = filter.status.as_deref().filter(|s| !s.is_empty()) {
        let st = TraceStatus::parse(status_str);
        if st != TraceStatus::Ok {
            return match index.status_bitmaps.get(&st.to_u8()) {
                Some(bm) if !bm.is_empty() => (true, Some(bm.clone())),
                _ => (false, None),
            };
        }
    }

    if filter.has_error == Some(true) {
        let mut bm = RoaringBitmap::new();
        if let Some(e) = index.status_bitmaps.get(&TraceStatus::Error.to_u8()) {
            bm |= e;
        }
        if let Some(t) = index.status_bitmaps.get(&TraceStatus::Timeout.to_u8()) {
            bm |= t;
        }
        return if bm.is_empty() {
            (false, None)
        } else {
            (true, Some(bm))
        };
    }

    if let Some(env) = filter.environment.as_deref().filter(|s| !s.is_empty()) {
        return eval_dict_witness(&index.environment_bitmaps, index.environment_truncated, env);
    }

    if let Some(status_str) = filter.status.as_deref().filter(|s| !s.is_empty()) {
        let st = TraceStatus::parse(status_str);
        return match index.status_bitmaps.get(&st.to_u8()) {
            Some(bm) if !bm.is_empty() => (true, Some(bm.clone())),
            _ => (false, None),
        };
    }

    if filter.has_error == Some(false) {
        let mut bm = RoaringBitmap::new();
        for st in [
            TraceStatus::Ok,
            TraceStatus::Running,
            TraceStatus::Cancelled,
        ] {
            if let Some(b) = index.status_bitmaps.get(&st.to_u8()) {
                bm |= b;
            }
        }
        return if bm.is_empty() {
            (false, None)
        } else {
            (true, Some(bm))
        };
    }

    if filter.has_loop_warning == Some(true) {
        return (
            index.zone_map.loop_count > 0 || index.zone_map.span_count > 0,
            None,
        );
    }

    if let Some(min_cost) = filter.min_cost_usd {
        if min_cost > 0.0 {
            return (index.zone_map.max_cost_nanos > 0, None);
        }
    }

    if let Some(end) = filter.end_ts_ms {
        return (index.zone_map.min_start_ts_ms <= end, None);
    }

    (true, None)
}

pub(crate) async fn list_traces_impl(
    store: &AgentTraceStore,
    project_id: &str,
    filter: &TraceListFilter,
    rollups: &HashMap<(EvalTargetType, String), AgentEvalRollup>,
    tombstones: &[AgentErasureTombstone],
) -> StoreResult<TraceListResponse> {
    if store.closed.load(Ordering::Acquire) {
        return Err(StoreError::Shutdown);
    }
    validate_identifier("project_id", project_id)?;

    let snap = store.acquire_snapshot(project_id).await?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let tomb_idx =
        ActiveTombstones::new(project_id, tombstones, &snap.manifest.erased_traces, now_ms);
    let reader = store.plss_reader();

    let mut merged: HashMap<String, MergedTraceEntry> = HashMap::new();
    let mut pruned_segments: Vec<PrunedSegment<'_>> = Vec::new();
    let multi_segment = snap.manifest.segments.len() > 1;
    let l0_empty = snap.l0_entries.is_empty();

    // Never touch Tag 0x04 or Tag 0x05 in list_traces!
    for seg in &snap.manifest.segments {
        let Some(tail) = tail_or_skip(&reader, seg).await? else {
            continue;
        };
        let (matched, cand_bm) =
            pass1_segment_candidates(&tail.index, filter, None, multi_segment, l0_empty);
        if !matched {
            pruned_segments.push(PrunedSegment::Skipped { seg, tail });
            continue;
        }

        let strings = reader.read_string_table(seg, &tail).await?;
        let summaries = reader.read_trace_summaries(seg, &tail).await?;
        let has_ranks = tail.tag02_has_ranks();

        if let Some(bm) = cand_bm {
            let mut read_tids = HashSet::with_capacity(bm.len() as usize);
            for idx in &bm {
                if let Some(row) = summaries.get(idx as usize) {
                    let tr = row.to_agent_trace(project_id, &strings)?;
                    let tr_meta = row.crdt_meta(&strings)?;
                    read_tids.insert(tr.trace_id.clone());
                    let (crdts, models, providers) = resolve_span_crdts(row, &strings, has_ranks)?;
                    merge_into_map(&mut merged, tr, tr_meta, crdts, models, providers);
                }
            }
            if (bm.len() as usize) < summaries.len() {
                pruned_segments.push(PrunedSegment::Partial {
                    seg,
                    tail,
                    read_indices: bm,
                    read_tids,
                });
            }
        } else {
            for row in summaries.iter() {
                let tr = row.to_agent_trace(project_id, &strings)?;
                let tr_meta = row.crdt_meta(&strings)?;
                let (crdts, models, providers) = resolve_span_crdts(row, &strings, has_ranks)?;
                merge_into_map(&mut merged, tr, tr_meta, crdts, models, providers);
            }
        }
    }

    for (tr, spans) in &snap.l0_entries {
        let (summary, l0_meta, crdts, models, providers) = lightweight_l0_summary(
            tr,
            snap.l0_meta.get(&tr.trace_id),
            &snap.l0_span_ranks,
            spans,
        );
        merge_into_map(&mut merged, summary, l0_meta, crdts, models, providers);
    }

    // Complete split traces from pruned segments ONLY when Tag 0x06 Bloom hits a candidate trace_id.
    if !pruned_segments.is_empty() && !merged.is_empty() {
        let candidate_tids: HashSet<String> = merged
            .keys()
            .filter(|tid| !tomb_idx.erased_traces.contains(tid.as_str()))
            .cloned()
            .collect();
        if !candidate_tids.is_empty() {
            for ps in &pruned_segments {
                if !ps.any_missing_candidate_may_exist(&candidate_tids) {
                    continue;
                }
                let seg = ps.seg();
                let tail = ps.tail();
                let read_indices = ps.read_indices();
                let strings = reader.read_string_table(seg, tail).await?;
                let summaries = reader.read_trace_summaries(seg, tail).await?;
                let has_ranks = tail.tag02_has_ranks();
                for (idx, row) in summaries.iter().enumerate() {
                    if read_indices.is_some_and(|bm| bm.contains(idx as u32)) {
                        continue;
                    }
                    let tid = strings.get_required(row.trace_id_str, "trace_id")?;
                    if !candidate_tids.contains(tid) {
                        continue;
                    }
                    let tr = row.to_agent_trace(project_id, &strings)?;
                    let tr_meta = row.crdt_meta(&strings)?;
                    let (crdts, models, providers) = resolve_span_crdts(row, &strings, has_ranks)?;
                    merge_into_map(&mut merged, tr, tr_meta, crdts, models, providers);
                }
            }
        }
    }

    let mut matched: Vec<AgentTrace> = merged
        .into_values()
        .filter(|e| {
            !tomb_idx.is_trace_erased(
                &e.trace.trace_id,
                e.trace.session_id.as_deref(),
                e.trace.user_id.as_deref(),
            )
        })
        .map(|mut e| {
            if let Some(r) = rollups.get(&(EvalTargetType::Trace, e.trace.trace_id.clone())) {
                e.trace.avg_eval_score = Some(r.avg_score);
                e.trace.eval_pass_rate = r.pass_rate;
            }
            e
        })
        .filter(|e| matches_trace_list_filter(e, filter))
        .map(|e| e.trace)
        .collect();

    let sort_by = match filter.sort_by.as_deref().unwrap_or("start_ts_ms") {
        "duration_ms" => "duration_ms",
        "total_cost_usd" | "total_cost_nanos" | "cost" => "total_cost_nanos",
        "total_tokens" | "tokens" => "total_tokens",
        "avg_eval_score" | "score" => "avg_eval_score",
        _ => "start_ts_ms",
    };
    let sort_dir = match filter.sort_dir.as_deref() {
        Some(d) if d.eq_ignore_ascii_case("asc") => "asc",
        _ => "desc",
    };
    let asc = sort_dir == "asc";

    let cmp_traces = |a: &AgentTrace, b: &AgentTrace| -> std::cmp::Ordering {
        let ord = match sort_by {
            "duration_ms" => a.duration_ms.cmp(&b.duration_ms),
            "total_cost_nanos" => a.total_cost_nanos.cmp(&b.total_cost_nanos),
            "total_tokens" => a.total_tokens.cmp(&b.total_tokens),
            "avg_eval_score" => {
                let sa = a.avg_eval_score.unwrap_or(if asc {
                    f64::INFINITY
                } else {
                    f64::NEG_INFINITY
                });
                let sb = b.avg_eval_score.unwrap_or(if asc {
                    f64::INFINITY
                } else {
                    f64::NEG_INFINITY
                });
                sa.total_cmp(&sb)
            }
            _ => a.start_ts_ms.cmp(&b.start_ts_ms),
        }
        .then_with(|| a.trace_id.cmp(&b.trace_id));
        if asc {
            ord
        } else {
            ord.reverse()
        }
    };

    matched.sort_by(cmp_traces);

    let total = matched.len() as i64;
    let limit = filter.limit.unwrap_or(50).clamp(1, MAX_TRACE_LIST_LIMIT);
    let offset = filter.offset.unwrap_or(0);

    let sliced: Vec<AgentTrace> = if let Some(ref raw_cur) = filter.cursor {
        let cur = decode_cursor(raw_cur, sort_by, sort_dir)?;
        matched
            .into_iter()
            .filter(|t| {
                let ord = match sort_by {
                    "duration_ms" => t.duration_ms.cmp(&cur.i64_val.unwrap_or(0)),
                    "total_cost_nanos" => t.total_cost_nanos.cmp(&cur.i64_val.unwrap_or(0)),
                    "total_tokens" => t.total_tokens.cmp(&cur.i64_val.unwrap_or(0)),
                    "avg_eval_score" => {
                        let st = t.avg_eval_score.unwrap_or(if asc {
                            f64::INFINITY
                        } else {
                            f64::NEG_INFINITY
                        });
                        let sc = cur.f64_bits.map(f64::from_bits).unwrap_or(if asc {
                            f64::INFINITY
                        } else {
                            f64::NEG_INFINITY
                        });
                        st.total_cmp(&sc)
                    }
                    _ => t.start_ts_ms.cmp(&cur.i64_val.unwrap_or(0)),
                }
                .then_with(|| t.trace_id.cmp(&cur.tie_id));
                let cmp = if asc { ord } else { ord.reverse() };
                cmp == std::cmp::Ordering::Greater
            })
            .collect()
    } else {
        matched.into_iter().skip(offset).collect()
    };

    let has_more = sliced.len() > limit;
    let page: Vec<AgentTrace> = sliced.into_iter().take(limit).collect();
    let next_cursor = if has_more {
        page.last().and_then(|last| {
            let (i64_val, f64_bits) = match sort_by {
                "duration_ms" => (Some(last.duration_ms), None),
                "total_cost_nanos" => (Some(last.total_cost_nanos), None),
                "total_tokens" => (Some(last.total_tokens), None),
                "avg_eval_score" => (None, last.avg_eval_score.map(f64::to_bits)),
                _ => (Some(last.start_ts_ms), None),
            };
            encode_cursor(&KeysetCursor {
                sort_by: sort_by.to_string(),
                sort_dir: sort_dir.to_string(),
                i64_val,
                f64_bits,
                tie_id: last.trace_id.clone(),
            })
            .ok()
        })
    } else {
        None
    };

    Ok(TraceListResponse {
        traces: page,
        total,
        limit,
        offset: if filter.cursor.is_some() { 0 } else { offset },
        next_cursor,
    })
}

// ---------------------------------------------------------------------------
// 4. get_trace_detail
// ---------------------------------------------------------------------------

pub(crate) async fn get_trace_detail_impl(
    store: &AgentTraceStore,
    project_id: &str,
    trace_id: &str,
    tombstones: &[AgentErasureTombstone],
) -> StoreResult<Option<TraceDetail>> {
    if store.closed.load(Ordering::Acquire) {
        return Err(StoreError::Shutdown);
    }
    validate_identifier("project_id", project_id)?;
    if trace_id.trim().is_empty() {
        return Ok(None);
    }

    let now_ms = chrono::Utc::now().timestamp_millis();
    let pre_tomb_idx = ActiveTombstones::new_tombstones_only(project_id, tombstones, now_ms);
    if pre_tomb_idx.is_trace_erased(trace_id, None, None) {
        return Ok(None);
    }

    let snap = store.acquire_snapshot(project_id).await?;
    let tomb_idx =
        ActiveTombstones::new(project_id, tombstones, &snap.manifest.erased_traces, now_ms);
    if tomb_idx.is_trace_erased(trace_id, None, None) {
        return Ok(None);
    }
    let reader = store.plss_reader();

    struct RankedTracePart {
        trace: AgentTrace,
        meta: TraceCrdtMeta,
        spans: Vec<(AgentSpan, SpanFieldRanks)>,
    }

    let mut parts: Vec<RankedTracePart> = Vec::new();
    for seg in &snap.manifest.segments {
        let Some(tail) = tail_or_skip(&reader, seg).await? else {
            continue;
        };
        if !tail.index.may_contain_trace(trace_id) {
            continue;
        }
        if let Some((tr, tr_meta, ranked_spans)) = reader
            .read_trace_ranges_ranked(seg, &tail, trace_id)
            .await?
        {
            parts.push(RankedTracePart {
                trace: tr,
                meta: tr_meta,
                spans: ranked_spans,
            });
        }
    }

    for (tr, spans) in &snap.l0_entries {
        if tr.trace_id == trace_id {
            let tr_meta = snap
                .l0_meta
                .get(&tr.trace_id)
                .cloned()
                .unwrap_or_else(|| TraceCrdtMeta::from_trace(tr));
            let ranked_spans = spans
                .iter()
                .map(|s| {
                    let sr = snap
                        .l0_span_ranks
                        .get(&(tr.trace_id.clone(), s.span_id.clone()))
                        .copied()
                        .unwrap_or_else(|| SpanFieldRanks::from_span(s));
                    (s.clone(), sr)
                })
                .collect();
            parts.push(RankedTracePart {
                trace: tr.clone(),
                meta: tr_meta,
                spans: ranked_spans,
            });
        }
    }

    if parts.is_empty() {
        return Ok(None);
    }

    let mut iter = parts.into_iter();
    let first = iter.next().unwrap();
    let mut merged_trace = first.trace;
    let mut merged_meta = first.meta;
    let mut span_map: HashMap<String, (AgentSpan, SpanFieldRanks)> =
        HashMap::with_capacity(first.spans.len());
    merge_spans_crdt_with_ranks(&mut span_map, first.spans);

    for part in iter {
        merge_trace_headers_with_meta(&mut merged_trace, &mut merged_meta, part.trace, part.meta);
        merge_spans_crdt_with_ranks(&mut span_map, part.spans);
    }

    let mut spans: Vec<AgentSpan> = span_map.into_values().map(|(sp, _)| sp).collect();
    spans.sort_by(|a, b| {
        a.start_ts_ms
            .cmp(&b.start_ts_ms)
            .then_with(|| a.span_id.cmp(&b.span_id))
    });
    recompute_trace_from_spans_with_explicit(
        &mut merged_trace,
        merged_meta.explicit_status,
        merged_meta.explicit_failure_source,
        merged_meta.explicit_has_loop_warning,
        merged_meta.explicit_primary_model.clone(),
        merged_meta.explicit_primary_provider.clone(),
        &spans,
    );

    if tomb_idx.is_trace_erased(
        &merged_trace.trace_id,
        merged_trace.session_id.as_deref(),
        merged_trace.user_id.as_deref(),
    ) {
        return Ok(None);
    }

    Ok(Some(TraceDetail {
        trace: merged_trace,
        spans,
        evaluations: Vec::new(),
    }))
}

// ---------------------------------------------------------------------------
// 5. query_analytics
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ResolvedSpanMetric {
    trace_id: String,
    span_id: String,
    span_type: SpanType,
    span_phase: u8,
    status: TraceStatus,
    start_ts_ms: i64,
    duration_ms: i64,
    provider: Option<String>,
    model: Option<String>,
    tool_name: Option<String>,
    tool_name_rank: Option<(u8, i64)>,
    tool_retry_count: i32,
    total_tokens: i64,
    cost_nanos: i64,
}

fn merge_span_metric(
    map: &mut HashMap<(String, String), ResolvedSpanMetric>,
    cand: ResolvedSpanMetric,
) {
    let key = (cand.trace_id.clone(), cand.span_id.clone());
    map.entry(key)
        .and_modify(|cur| {
            let new_rank = (cand.span_phase, cand.start_ts_ms);
            let cur_rank = (cur.span_phase, cur.start_ts_ms);
            if new_rank > cur_rank {
                let mut next = cand.clone();
                if next.span_phase == cur.span_phase
                    && status_rank(cur.status) > status_rank(next.status)
                {
                    next.status = cur.status;
                }
                merge_opt_str_max(&mut next.model, cur.model.clone());
                merge_opt_str_max(&mut next.provider, cur.provider.clone());
                merge_opt_str_phase_ranked_preserve_empty(
                    &mut next.tool_name,
                    &mut next.tool_name_rank,
                    cur.tool_name.clone(),
                    cur.tool_name_rank,
                );
                *cur = next;
            } else if new_rank < cur_rank {
                if cand.span_phase == cur.span_phase
                    && status_rank(cand.status) > status_rank(cur.status)
                {
                    cur.status = cand.status;
                }
                merge_opt_str_max(&mut cur.model, cand.model.clone());
                merge_opt_str_max(&mut cur.provider, cand.provider.clone());
                merge_opt_str_phase_ranked_preserve_empty(
                    &mut cur.tool_name,
                    &mut cur.tool_name_rank,
                    cand.tool_name.clone(),
                    cand.tool_name_rank,
                );
            } else {
                if status_rank(cand.status) > status_rank(cur.status) {
                    cur.status = cand.status;
                }
                let cur_st_rank = match cur.span_type {
                    SpanType::Custom => 0u8,
                    other => other.to_u8().saturating_add(1),
                };
                let new_st_rank = match cand.span_type {
                    SpanType::Custom => 0u8,
                    other => other.to_u8().saturating_add(1),
                };
                if new_st_rank > cur_st_rank {
                    cur.span_type = cand.span_type;
                }
                cur.duration_ms = cur.duration_ms.max(cand.duration_ms).max(0);
                cur.tool_retry_count = cur.tool_retry_count.max(cand.tool_retry_count).max(0);
                cur.total_tokens = cur.total_tokens.max(cand.total_tokens).max(0);
                cur.cost_nanos = cur.cost_nanos.max(cand.cost_nanos).max(0);
                merge_opt_str_max(&mut cur.model, cand.model.clone());
                merge_opt_str_max(&mut cur.provider, cand.provider.clone());
                merge_opt_str_phase_ranked_preserve_empty(
                    &mut cur.tool_name,
                    &mut cur.tool_name_rank,
                    cand.tool_name.clone(),
                    cand.tool_name_rank,
                );
            }
        })
        .or_insert(cand);
}

fn span_metric_from_row(
    r: &SpanMetricRow,
    strings: &SegmentStringTable,
) -> StoreResult<ResolvedSpanMetric> {
    let trace_id = strings
        .get_required(r.trace_id_str, "span_metric trace_id")?
        .to_string();
    let span_id = strings
        .get_required(r.span_id_str, "span_metric span_id")?
        .to_string();
    let span_type = SpanType::from_u8(r.span_type).ok_or_else(|| {
        StoreError::Corrupted(format!("invalid Tag 0x03 span_type {}", r.span_type))
    })?;
    if SpanPhase::from_u8(r.span_phase).is_none() {
        return Err(StoreError::Corrupted(format!(
            "invalid Tag 0x03 span_phase {}",
            r.span_phase
        )));
    }
    let status = TraceStatus::from_u8(r.status)
        .ok_or_else(|| StoreError::Corrupted(format!("invalid Tag 0x03 status {}", r.status)))?;
    let tool_name = strings
        .get_opt(r.tool_name_str, "span_metric tool_name_str")?
        .map(str::to_string);
    let tool_name_rank = tool_name.as_ref().map(|_| (r.span_phase, r.start_ts_ms));
    let provider = strings
        .get_opt(r.provider_str, "span_metric provider_str")?
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let model = strings
        .get_opt(r.model_str, "span_metric model_str")?
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    Ok(ResolvedSpanMetric {
        trace_id,
        span_id,
        span_type,
        span_phase: r.span_phase,
        status,
        start_ts_ms: r.start_ts_ms,
        duration_ms: r.duration_ms.max(0),
        provider,
        model,
        tool_name,
        tool_name_rank,
        tool_retry_count: r.tool_retry_count.max(0),
        // Senders often omit total_tokens; same rule as the trace rollup.
        total_tokens: r.total_tokens.max(r.input_tokens.saturating_add(r.output_tokens)).max(0),
        cost_nanos: r.cost_nanos.max(0),
    })
}

fn matches_analytics_filter(entry: &MergedTraceEntry, filter: &AnalyticsFilter) -> bool {
    let t = &entry.trace;
    if let Some(s) = filter.start_ts_ms {
        if t.start_ts_ms < s {
            return false;
        }
    }
    if let Some(e) = filter.end_ts_ms {
        if t.start_ts_ms > e {
            return false;
        }
    }
    if let Some(a) = filter.agent_name.as_deref().filter(|s| !s.is_empty()) {
        if t.agent_name != a {
            return false;
        }
    }
    if let Some(av) = filter.agent_version.as_deref().filter(|s| !s.is_empty()) {
        if t.agent_version != av {
            return false;
        }
    }
    if let Some(pv) = filter.prompt_version.as_deref().filter(|s| !s.is_empty()) {
        if t.prompt_version.as_deref() != Some(pv) {
            return false;
        }
    }
    if let Some(m) = filter.model.as_deref().filter(|s| !s.is_empty()) {
        let matched_crdt = if entry.crdts.is_empty() {
            entry.models.contains(m)
        } else {
            entry
                .crdts
                .values()
                .any(|c| c.span_type == SpanType::Llm.to_u8() && c.model.as_deref() == Some(m))
        };
        if t.primary_model.as_deref() != Some(m) && !matched_crdt {
            return false;
        }
    }
    if let Some(p) = filter.provider.as_deref().filter(|s| !s.is_empty()) {
        let matched_crdt = if entry.crdts.is_empty() {
            entry.providers.contains(p)
        } else {
            entry
                .crdts
                .values()
                .any(|c| c.span_type == SpanType::Llm.to_u8() && c.provider.as_deref() == Some(p))
        };
        if t.primary_provider.as_deref() != Some(p) && !matched_crdt {
            return false;
        }
    }
    if let Some(tn) = filter.tool_name.as_deref().filter(|s| !s.is_empty()) {
        let matched_crdt = entry.crdts.values().any(|c| {
            c.span_type == SpanType::Tool.to_u8()
                && c.tool_name.as_deref().filter(|s| !s.is_empty()) == Some(tn)
        });
        if !matched_crdt {
            return false;
        }
    }
    if let Some(env) = filter.environment.as_deref().filter(|s| !s.is_empty()) {
        if t.environment != env {
            return false;
        }
    }
    if let Some(st) = filter.status.as_deref().filter(|s| !s.is_empty()) {
        if t.status != TraceStatus::parse(st) {
            return false;
        }
    }
    if let Some(uid) = filter.user_id.as_deref().filter(|s| !s.is_empty()) {
        if t.user_id.as_deref() != Some(uid) {
            return false;
        }
    }
    if let Some(sid) = filter.session_id.as_deref().filter(|s| !s.is_empty()) {
        if t.session_id.as_deref() != Some(sid) {
            return false;
        }
    }
    if let Some(tag) = filter.tag.as_deref().filter(|s| !s.is_empty()) {
        if !t.tags.iter().any(|tg| tg == tag) {
            return false;
        }
    }
    true
}

fn group_trace_breakdown<F>(traces: &[AgentTrace], key_fn: F) -> Vec<DimensionBreakdown>
where
    F: Fn(&AgentTrace) -> (String, Option<String>),
{
    let mut map: HashMap<(String, Option<String>), Vec<&AgentTrace>> = HashMap::new();
    for t in traces {
        map.entry(key_fn(t)).or_default().push(t);
    }
    let mut out: Vec<DimensionBreakdown> = map
        .into_iter()
        .map(|((key, secondary), items)| breakdown_row(key, secondary, &items))
        .collect();
    out.sort_by(|a, b| {
        b.executions
            .cmp(&a.executions)
            .then_with(|| a.key.cmp(&b.key))
    });
    out
}

/// One breakdown row over a set of runs (traces).
fn breakdown_row(key: String, secondary: Option<String>, items: &[&AgentTrace]) -> DimensionBreakdown {
    let execs = items.len() as i64;
    let fails = items.iter().filter(|t| is_error_status(t.status)).count() as i64;
    let durs: Vec<i64> = items.iter().map(|t| t.duration_ms).collect();
    let pct = compute_latency_percentiles(&durs);
    let toks: i64 = items
        .iter()
        .fold(0i64, |acc, t| acc.saturating_add(t.total_tokens.max(0)));
    let nanos: i64 = items
        .iter()
        .fold(0i64, |acc, t| acc.saturating_add(t.total_cost_nanos.max(0)));
    let cost = cost_nanos_to_usd(nanos);
    let scores: Vec<f64> = items.iter().filter_map(|t| t.avg_eval_score).collect();
    DimensionBreakdown {
        key,
        secondary,
        executions: execs,
        successes: execs - fails,
        failures: fails,
        error_rate: if execs > 0 {
            fails as f64 / execs as f64
        } else {
            0.0
        },
        avg_latency_ms: pct.avg_ms,
        p95_latency_ms: pct.p95_ms,
        total_tokens: toks,
        total_cost_usd: cost,
        cost_per_execution_usd: if execs > 0 { cost / execs as f64 } else { 0.0 },
        avg_eval_score: if scores.is_empty() {
            None
        } else {
            Some(scores.iter().sum::<f64>() / scores.len() as f64)
        },
    }
}

pub(crate) async fn query_analytics_impl(
    store: &AgentTraceStore,
    project_id: &str,
    filter: &AnalyticsFilter,
    tombstones: &[AgentErasureTombstone],
    rollups: &HashMap<(EvalTargetType, String), AgentEvalRollup>,
) -> StoreResult<AgentAnalyticsOverview> {
    if store.closed.load(Ordering::Acquire) {
        return Err(StoreError::Shutdown);
    }
    validate_identifier("project_id", project_id)?;

    let snap = store.acquire_snapshot(project_id).await?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let tomb_idx =
        ActiveTombstones::new(project_id, tombstones, &snap.manifest.erased_traces, now_ms);
    let reader = store.plss_reader();

    let trace_filter = filter.to_trace_list_filter();
    let mut merged_traces: HashMap<String, MergedTraceEntry> = HashMap::new();
    let mut span_metrics: HashMap<(String, String), ResolvedSpanMetric> = HashMap::new();
    let mut pruned_segments: Vec<PrunedSegment<'_>> = Vec::new();
    let multi_segment = snap.manifest.segments.len() > 1;
    let l0_empty = snap.l0_entries.is_empty();

    // Never touch Tag 0x04 or Tag 0x05!
    for seg in &snap.manifest.segments {
        let Some(tail) = tail_or_skip(&reader, seg).await? else {
            continue;
        };
        let (matched, cand_bm) = pass1_segment_candidates(
            &tail.index,
            &trace_filter,
            filter.tool_name.as_deref(),
            multi_segment,
            l0_empty,
        );
        if !matched {
            pruned_segments.push(PrunedSegment::Skipped { seg, tail });
            continue;
        }

        let strings = reader.read_string_table(seg, &tail).await?;
        let summaries = reader.read_trace_summaries(seg, &tail).await?;
        let metrics = reader.read_span_metrics(seg, &tail).await?;
        let has_ranks = tail.tag02_has_ranks();

        let mut v1_span_tools: V1SpanToolsMap = HashMap::new();
        for sm in metrics.iter() {
            if let Some(ref bm) = cand_bm {
                if !bm.contains(sm.trace_idx) {
                    continue;
                }
            }
            let resolved = span_metric_from_row(sm, &strings)?;
            if !has_ranks {
                record_v1_span_tool(&mut v1_span_tools, &resolved);
            }
            merge_span_metric(&mut span_metrics, resolved);
        }

        if let Some(bm) = cand_bm {
            let mut read_tids = HashSet::with_capacity(bm.len() as usize);
            for idx in &bm {
                if let Some(row) = summaries.get(idx as usize) {
                    let tr = row.to_agent_trace(project_id, &strings)?;
                    let tr_meta = row.crdt_meta(&strings)?;
                    read_tids.insert(tr.trace_id.clone());
                    let (mut crdts, models, providers) =
                        resolve_span_crdts(row, &strings, has_ranks)?;
                    if let Some(span_tools) = v1_span_tools.remove(&tr.trace_id) {
                        apply_v1_span_tools(&mut crdts, &span_tools);
                    }
                    merge_into_map(&mut merged_traces, tr, tr_meta, crdts, models, providers);
                }
            }
            if (bm.len() as usize) < summaries.len() {
                pruned_segments.push(PrunedSegment::Partial {
                    seg,
                    tail,
                    read_indices: bm,
                    read_tids,
                });
            }
        } else {
            for row in summaries.iter() {
                let tr = row.to_agent_trace(project_id, &strings)?;
                let tr_meta = row.crdt_meta(&strings)?;
                let (mut crdts, models, providers) = resolve_span_crdts(row, &strings, has_ranks)?;
                if let Some(span_tools) = v1_span_tools.remove(&tr.trace_id) {
                    apply_v1_span_tools(&mut crdts, &span_tools);
                }
                merge_into_map(&mut merged_traces, tr, tr_meta, crdts, models, providers);
            }
        }
    }

    for (tr, spans) in &snap.l0_entries {
        let (summary, l0_meta, crdts, models, providers) = lightweight_l0_summary(
            tr,
            snap.l0_meta.get(&tr.trace_id),
            &snap.l0_span_ranks,
            spans,
        );
        merge_into_map(
            &mut merged_traces,
            summary,
            l0_meta,
            crdts,
            models,
            providers,
        );
        for sp in spans {
            let l0_tn = sp.tool_name.clone();
            let l0_tn_rank = l0_tn.as_ref().and_then(|_| {
                snap.l0_span_ranks
                    .get(&(sp.trace_id.clone(), sp.span_id.clone()))
                    .and_then(|sr| sr.tool_name_rank)
                    .or_else(|| Some((sp.span_phase.priority(), sp.start_ts_ms)))
            });
            merge_span_metric(
                &mut span_metrics,
                ResolvedSpanMetric {
                    trace_id: sp.trace_id.clone(),
                    span_id: sp.span_id.clone(),
                    span_type: sp.span_type,
                    span_phase: sp.span_phase.to_u8(),
                    status: sp.status,
                    start_ts_ms: sp.start_ts_ms,
                    duration_ms: sp.duration_ms.max(0),
                    provider: sp.provider.clone().filter(|s| !s.is_empty()),
                    model: sp.model.clone().filter(|s| !s.is_empty()),
                    tool_name: l0_tn,
                    tool_name_rank: l0_tn_rank,
                    tool_retry_count: sp.tool_retry_count.max(0),
                    total_tokens: sp
                        .total_tokens
                        .max(sp.input_tokens.saturating_add(sp.output_tokens))
                        .max(0),
                    cost_nanos: sp.cost_nanos.max(0),
                },
            );
        }
    }

    // Complete split traces from pruned segments ONLY when Tag 0x06 Bloom hits a candidate trace_id.
    if !pruned_segments.is_empty() && !merged_traces.is_empty() {
        let candidate_tids: HashSet<String> = merged_traces
            .keys()
            .filter(|tid| !tomb_idx.erased_traces.contains(tid.as_str()))
            .cloned()
            .collect();
        if !candidate_tids.is_empty() {
            for ps in &pruned_segments {
                if !ps.any_missing_candidate_may_exist(&candidate_tids) {
                    continue;
                }
                let seg = ps.seg();
                let tail = ps.tail();
                let read_indices = ps.read_indices();
                let strings = reader.read_string_table(seg, tail).await?;
                let summaries = reader.read_trace_summaries(seg, tail).await?;
                let metrics = reader.read_span_metrics(seg, tail).await?;
                let has_ranks = tail.tag02_has_ranks();

                let mut matched_indices: HashSet<u32> = HashSet::new();
                let mut matched_rows: Vec<(String, &TraceSummaryRow)> = Vec::new();
                for (idx, row) in summaries.iter().enumerate() {
                    let idx_u32 = idx as u32;
                    if read_indices.is_some_and(|bm| bm.contains(idx_u32)) {
                        continue;
                    }
                    let tid = strings.get_required(row.trace_id_str, "trace_id")?;
                    if candidate_tids.contains(tid) {
                        matched_indices.insert(idx_u32);
                        matched_rows.push((tid.to_string(), row));
                    }
                }
                if matched_rows.is_empty() {
                    continue;
                }

                let mut v1_span_tools: V1SpanToolsMap = HashMap::new();
                for sm in metrics.iter() {
                    if !matched_indices.contains(&sm.trace_idx) {
                        continue;
                    }
                    let resolved = span_metric_from_row(sm, &strings)?;
                    if !has_ranks {
                        record_v1_span_tool(&mut v1_span_tools, &resolved);
                    }
                    merge_span_metric(&mut span_metrics, resolved);
                }

                for (tid, row) in matched_rows {
                    let tr = row.to_agent_trace(project_id, &strings)?;
                    let tr_meta = row.crdt_meta(&strings)?;
                    let (mut crdts, models, providers) =
                        resolve_span_crdts(row, &strings, has_ranks)?;
                    if let Some(span_tools) = v1_span_tools.remove(&tid) {
                        apply_v1_span_tools(&mut crdts, &span_tools);
                    }
                    merge_into_map(&mut merged_traces, tr, tr_meta, crdts, models, providers);
                }
            }
        }
    }

    // Reconcile Tag 0x03 span metrics against CRDT-merged Tag 0x02 + L0 span metadata
    // so 3-phase splits (`Start + Final(None)` + `Update`), `Some("")`, and cross-phase
    // `span_type` / `model` / `provider` changes match `get_trace_detail` & `list_traces`.
    for sm in span_metrics.values_mut() {
        if let Some(crdt) = merged_traces
            .get(&sm.trace_id)
            .and_then(|e| e.crdts.get(&sm.span_id))
        {
            if let Some(st) = SpanType::from_u8(crdt.span_type) {
                sm.span_type = st;
            }
            sm.span_phase = crdt.span_phase;
            if let Some(st) = TraceStatus::from_u8(crdt.status) {
                sm.status = st;
            }
            merge_opt_str_max(&mut sm.model, crdt.model.clone());
            merge_opt_str_max(&mut sm.provider, crdt.provider.clone());
            sm.tool_name = crdt.tool_name.clone();
            sm.tool_name_rank = crdt.tool_name_rank;
            sm.total_tokens = crdt
                .total_tokens
                .max(crdt.input_tokens.saturating_add(crdt.output_tokens))
                .max(0);
            sm.cost_nanos = crdt.cost_nanos.max(0);
        }
    }

    let mut traces: Vec<AgentTrace> = merged_traces
        .into_values()
        .filter(|e| {
            !tomb_idx.is_trace_erased(
                &e.trace.trace_id,
                e.trace.session_id.as_deref(),
                e.trace.user_id.as_deref(),
            ) && matches_analytics_filter(e, filter)
        })
        .map(|mut e| {
            // Eval scores live in the control DB; apply them here so every breakdown
            // (including by model) gets a score from the same runs it counts.
            if let Some(r) = rollups.get(&(EvalTargetType::Trace, e.trace.trace_id.clone())) {
                e.trace.avg_eval_score = Some(r.avg_score);
                e.trace.eval_pass_rate = r.pass_rate;
            }
            e.trace
        })
        .collect();
    traces.sort_by(|a, b| {
        a.start_ts_ms
            .cmp(&b.start_ts_ms)
            .then_with(|| a.trace_id.cmp(&b.trace_id))
    });

    let surviving_ids: HashSet<String> = traces.iter().map(|t| t.trace_id.clone()).collect();
    let surviving_spans: Vec<ResolvedSpanMetric> = span_metrics
        .into_values()
        .filter(|s| {
            if !surviving_ids.contains(&s.trace_id) {
                return false;
            }
            if s.span_type == SpanType::Llm {
                if let Some(m) = filter.model.as_deref().filter(|v| !v.is_empty()) {
                    if s.model.as_deref() != Some(m) {
                        return false;
                    }
                }
                if let Some(p) = filter.provider.as_deref().filter(|v| !v.is_empty()) {
                    if s.provider.as_deref() != Some(p) {
                        return false;
                    }
                }
            } else if s.span_type == SpanType::Tool {
                if let Some(tn) = filter.tool_name.as_deref().filter(|v| !v.is_empty()) {
                    if s.tool_name.as_deref().filter(|v| !v.is_empty()) != Some(tn) {
                        return false;
                    }
                }
            }
            true
        })
        .collect();

    let total = traces.len() as i64;
    let failed = traces.iter().filter(|t| is_error_status(t.status)).count() as i64;
    let succeeded = (total - failed).max(0);
    let loop_warning_count = traces.iter().filter(|t| t.has_loop_warning).count() as i64;

    let durations: Vec<i64> = traces.iter().map(|t| t.duration_ms).collect();
    let model_durations: Vec<i64> = surviving_spans
        .iter()
        .filter(|s| s.span_type == SpanType::Llm)
        .map(|s| s.duration_ms)
        .collect();
    let tool_durations: Vec<i64> = surviving_spans
        .iter()
        .filter(|s| s.span_type == SpanType::Tool)
        .map(|s| s.duration_ms)
        .collect();

    let input_tokens: i64 = traces
        .iter()
        .fold(0i64, |acc, t| acc.saturating_add(t.input_tokens.max(0)));
    let output_tokens: i64 = traces
        .iter()
        .fold(0i64, |acc, t| acc.saturating_add(t.output_tokens.max(0)));
    let cached_tokens: i64 = traces
        .iter()
        .fold(0i64, |acc, t| acc.saturating_add(t.cached_tokens.max(0)));
    let total_tokens: i64 = traces
        .iter()
        .fold(0i64, |acc, t| acc.saturating_add(t.total_tokens.max(0)));
    let total_cost_nanos: i64 = traces
        .iter()
        .fold(0i64, |acc, t| acc.saturating_add(t.total_cost_nanos.max(0)));
    let total_cost_usd = cost_nanos_to_usd(total_cost_nanos);

    let eval_scores: Vec<f64> = traces.iter().filter_map(|t| t.avg_eval_score).collect();
    let avg_eval_score = if eval_scores.is_empty() {
        None
    } else {
        Some(eval_scores.iter().sum::<f64>() / eval_scores.len() as f64)
    };
    let eval_passes: Vec<f64> = traces.iter().filter_map(|t| t.eval_pass_rate).collect();
    let eval_pass_rate = if eval_passes.is_empty() {
        None
    } else {
        Some(eval_passes.iter().sum::<f64>() / eval_passes.len() as f64)
    };

    let mut failure_sources: BTreeMap<String, i64> = BTreeMap::new();
    for t in &traces {
        if is_error_status(t.status) {
            let src = t
                .failure_source
                .unwrap_or(FailureSource::Unknown)
                .as_str()
                .to_string();
            *failure_sources.entry(src).or_insert(0) += 1;
        }
    }

    let win_start = filter
        .start_ts_ms
        .or_else(|| traces.first().map(|t| t.start_ts_ms))
        .unwrap_or(0);
    let win_end = filter
        .end_ts_ms
        .or_else(|| traces.last().map(|t| t.start_ts_ms))
        .unwrap_or(win_start);
    let span_ms = win_end.saturating_sub(win_start).max(1);
    let bucket_ms: i64 = if span_ms <= 48 * 3600 * 1000 {
        3600 * 1000
    } else {
        24 * 3600 * 1000
    };

    let mut ts_groups: BTreeMap<i64, Vec<&AgentTrace>> = BTreeMap::new();
    for t in &traces {
        let b = t
            .start_ts_ms
            .div_euclid(bucket_ms)
            .saturating_mul(bucket_ms);
        ts_groups.entry(b).or_default().push(t);
    }
    let timeseries: Vec<AnalyticsTimeBucket> = ts_groups
        .into_iter()
        .map(|(bucket_ts_ms, items)| {
            let execs = items.len() as i64;
            let errs = items.iter().filter(|t| is_error_status(t.status)).count() as i64;
            let durs: Vec<i64> = items.iter().map(|t| t.duration_ms).collect();
            let pct = compute_latency_percentiles(&durs);
            let nanos: i64 = items
                .iter()
                .fold(0i64, |acc, t| acc.saturating_add(t.total_cost_nanos.max(0)));
            let toks: i64 = items
                .iter()
                .fold(0i64, |acc, t| acc.saturating_add(t.total_tokens.max(0)));
            let scores: Vec<f64> = items.iter().filter_map(|t| t.avg_eval_score).collect();
            AnalyticsTimeBucket {
                bucket_ts_ms,
                executions: execs,
                errors: errs,
                error_rate: if execs > 0 {
                    errs as f64 / execs as f64
                } else {
                    0.0
                },
                avg_latency_ms: pct.avg_ms,
                p95_latency_ms: pct.p95_ms,
                total_cost_usd: cost_nanos_to_usd(nanos),
                total_tokens: toks,
                avg_eval_score: if scores.is_empty() {
                    None
                } else {
                    Some(scores.iter().sum::<f64>() / scores.len() as f64)
                },
            }
        })
        .collect();

    let by_agent = group_trace_breakdown(&traces, |t| (t.agent_name.clone(), None));
    let by_version = group_trace_breakdown(&traces, |t| {
        (
            format!("{}@{}", t.agent_name, t.agent_version),
            t.prompt_version.clone(),
        )
    });
    let by_environment = group_trace_breakdown(&traces, |t| (t.environment.clone(), None));

    // By model: the runs that called each model, so runs, failure rate, latency and eval score
    // compare like the other breakdowns. Tokens and cost are that model's own LLM calls, so a run
    // that used two models isn't counted twice.
    let trace_by_id: HashMap<&str, &AgentTrace> =
        traces.iter().map(|t| (t.trace_id.as_str(), t)).collect();
    let mut model_map: HashMap<(String, Option<String>), Vec<&ResolvedSpanMetric>> = HashMap::new();
    for sp in surviving_spans
        .iter()
        .filter(|s| s.span_type == SpanType::Llm)
    {
        let m = sp.model.clone().unwrap_or_else(|| "unknown".into());
        model_map
            .entry((m, sp.provider.clone()))
            .or_default()
            .push(sp);
    }
    let mut by_model: Vec<DimensionBreakdown> = model_map
        .into_iter()
        .map(|((model, provider), calls)| {
            let mut seen = HashSet::new();
            let runs: Vec<&AgentTrace> = calls
                .iter()
                .filter(|c| seen.insert(c.trace_id.as_str()))
                .filter_map(|c| trace_by_id.get(c.trace_id.as_str()).copied())
                .collect();
            let mut row = breakdown_row(model, provider, &runs);
            row.total_tokens = calls
                .iter()
                .fold(0i64, |acc, c| acc.saturating_add(c.total_tokens.max(0)));
            let nanos = calls
                .iter()
                .fold(0i64, |acc, c| acc.saturating_add(c.cost_nanos.max(0)));
            row.total_cost_usd = cost_nanos_to_usd(nanos);
            row.cost_per_execution_usd = if row.executions > 0 {
                row.total_cost_usd / row.executions as f64
            } else {
                0.0
            };
            row
        })
        .collect();
    by_model.sort_by(|a, b| {
        b.total_cost_usd
            .total_cmp(&a.total_cost_usd)
            .then_with(|| a.key.cmp(&b.key))
    });

    // By Tool (from Tool spans)
    let mut tool_map: HashMap<String, Vec<&ResolvedSpanMetric>> = HashMap::new();
    for sp in surviving_spans
        .iter()
        .filter(|s| s.span_type == SpanType::Tool)
    {
        let name = sp
            .tool_name
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| "unnamed_tool".into());
        tool_map.entry(name).or_default().push(sp);
    }
    let mut by_tool: Vec<ToolBreakdown> = tool_map
        .into_iter()
        .map(|(tool_name, items)| {
            let inv = items.len() as i64;
            let fails = items.iter().filter(|s| is_error_status(s.status)).count() as i64;
            let retries: i64 = items
                .iter()
                .fold(0i64, |acc, s| acc.saturating_add(s.tool_retry_count as i64));
            let durs: Vec<i64> = items.iter().map(|s| s.duration_ms).collect();
            let pct = compute_latency_percentiles(&durs);
            let nanos: i64 = items
                .iter()
                .fold(0i64, |acc, s| acc.saturating_add(s.cost_nanos.max(0)));
            ToolBreakdown {
                tool_name,
                invocations: inv,
                failures: fails,
                failure_rate: if inv > 0 {
                    fails as f64 / inv as f64
                } else {
                    0.0
                },
                total_retries: retries,
                avg_latency_ms: pct.avg_ms,
                p95_latency_ms: pct.p95_ms,
                total_cost_usd: cost_nanos_to_usd(nanos),
            }
        })
        .collect();
    by_tool.sort_by(|a, b| {
        b.invocations
            .cmp(&a.invocations)
            .then_with(|| a.tool_name.cmp(&b.tool_name))
    });

    Ok(AgentAnalyticsOverview {
        total_executions: total,
        successful_executions: succeeded,
        failed_executions: failed,
        error_rate: if total > 0 {
            failed as f64 / total as f64
        } else {
            0.0
        },
        success_rate: if total > 0 {
            succeeded as f64 / total as f64
        } else {
            0.0
        },
        loop_warning_count,
        latency: compute_latency_percentiles(&durations),
        model_latency: compute_latency_percentiles(&model_durations),
        tool_latency: compute_latency_percentiles(&tool_durations),
        total_tokens,
        input_tokens,
        output_tokens,
        cached_tokens,
        total_cost_usd,
        cost_per_execution_usd: if total > 0 {
            total_cost_usd / total as f64
        } else {
            0.0
        },
        cost_per_successful_execution_usd: if succeeded > 0 {
            total_cost_usd / succeeded as f64
        } else {
            0.0
        },
        avg_eval_score,
        eval_pass_rate,
        failure_sources,
        timeseries,
        by_agent,
        by_version,
        by_model,
        by_tool,
        by_environment,
    })
}

// ---------------------------------------------------------------------------
// 6. compare_groups
// ---------------------------------------------------------------------------

fn derive_group_label(explicit: Option<&str>, f: &AnalyticsFilter, fallback: &str) -> String {
    if let Some(lbl) = explicit.map(str::trim).filter(|s| !s.is_empty()) {
        return lbl.to_string();
    }
    if let Some(av) = f.agent_version.as_deref().filter(|s| !s.is_empty()) {
        return av.to_string();
    }
    if let Some(pv) = f.prompt_version.as_deref().filter(|s| !s.is_empty()) {
        return pv.to_string();
    }
    if let Some(m) = f.model.as_deref().filter(|s| !s.is_empty()) {
        return m.to_string();
    }
    if let Some(env) = f.environment.as_deref().filter(|s| !s.is_empty()) {
        return env.to_string();
    }
    fallback.to_string()
}

fn build_compare_group_metrics(traces: &[&AgentTrace]) -> CompareGroupMetrics {
    let sample_size = traces.len();
    let executions = sample_size as i64;
    let failures = traces.iter().filter(|t| is_error_status(t.status)).count() as i64;
    let successes = (executions - failures).max(0);
    let loop_warning_count = traces.iter().filter(|t| t.has_loop_warning).count() as i64;
    let durations: Vec<i64> = traces.iter().map(|t| t.duration_ms).collect();
    let total_tokens: i64 = traces
        .iter()
        .fold(0i64, |acc, t| acc.saturating_add(t.total_tokens.max(0)));
    let total_cost_nanos: i64 = traces
        .iter()
        .fold(0i64, |acc, t| acc.saturating_add(t.total_cost_nanos.max(0)));
    let total_cost_usd = cost_nanos_to_usd(total_cost_nanos);
    let scores: Vec<f64> = traces.iter().filter_map(|t| t.avg_eval_score).collect();

    CompareGroupMetrics {
        sample_size,
        executions,
        successes,
        failures,
        success_rate: if executions > 0 {
            successes as f64 / executions as f64
        } else {
            0.0
        },
        error_rate: if executions > 0 {
            failures as f64 / executions as f64
        } else {
            0.0
        },
        loop_warning_count,
        loop_rate: if executions > 0 {
            loop_warning_count as f64 / executions as f64
        } else {
            0.0
        },
        latency: compute_latency_percentiles(&durations),
        total_tokens,
        avg_tokens: if executions > 0 {
            total_tokens as f64 / executions as f64
        } else {
            0.0
        },
        total_cost_nanos,
        total_cost_usd,
        avg_cost_usd: if executions > 0 {
            total_cost_usd / executions as f64
        } else {
            0.0
        },
        avg_eval_score: if scores.is_empty() {
            None
        } else {
            Some(scores.iter().sum::<f64>() / scores.len() as f64)
        },
    }
}

pub(crate) async fn compare_groups_impl(
    store: &AgentTraceStore,
    project_id: &str,
    filter: &CompareFilter,
    tombstones: &[AgentErasureTombstone],
) -> StoreResult<CompareResponse> {
    if store.closed.load(Ordering::Acquire) {
        return Err(StoreError::Shutdown);
    }
    validate_identifier("project_id", project_id)?;

    let (base_filter, cand_filter) = filter.resolve_groups();
    let bound_start = match (base_filter.start_ts_ms, cand_filter.start_ts_ms) {
        (Some(a), Some(b)) => Some(a.min(b)),
        _ => None,
    };
    let bound_end = match (base_filter.end_ts_ms, cand_filter.end_ts_ms) {
        (Some(a), Some(b)) => Some(a.max(b)),
        _ => None,
    };
    let bound_filter = TraceListFilter {
        start_ts_ms: bound_start,
        end_ts_ms: bound_end,
        agent_name: if base_filter.agent_name == cand_filter.agent_name {
            base_filter.agent_name.clone()
        } else {
            None
        },
        environment: if base_filter.environment == cand_filter.environment {
            base_filter.environment.clone()
        } else {
            None
        },
        ..Default::default()
    };

    let snap = store.acquire_snapshot(project_id).await?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let tomb_idx =
        ActiveTombstones::new(project_id, tombstones, &snap.manifest.erased_traces, now_ms);
    let reader = store.plss_reader();

    let need_tools = base_filter.tool_name.is_some() || cand_filter.tool_name.is_some();
    let mut merged: HashMap<String, MergedTraceEntry> = HashMap::new();
    let mut pruned_segments: Vec<PrunedSegment<'_>> = Vec::new();
    let multi_segment = snap.manifest.segments.len() > 1;
    let l0_empty = snap.l0_entries.is_empty();

    for seg in &snap.manifest.segments {
        let Some(tail) = tail_or_skip(&reader, seg).await? else {
            continue;
        };
        let (matched, cand_bm) =
            pass1_segment_candidates(&tail.index, &bound_filter, None, multi_segment, l0_empty);
        if !matched {
            pruned_segments.push(PrunedSegment::Skipped { seg, tail });
            continue;
        }

        let strings = reader.read_string_table(seg, &tail).await?;
        let summaries = reader.read_trace_summaries(seg, &tail).await?;
        let has_ranks = tail.tag02_has_ranks();
        let mut v1_span_tools: V1SpanToolsMap = HashMap::new();
        if need_tools && !has_ranks {
            let metrics = reader.read_span_metrics(seg, &tail).await?;
            for sm in metrics.iter() {
                if let Some(ref bm) = cand_bm {
                    if !bm.contains(sm.trace_idx) {
                        continue;
                    }
                }
                let resolved = span_metric_from_row(sm, &strings)?;
                record_v1_span_tool(&mut v1_span_tools, &resolved);
            }
        }

        if let Some(bm) = cand_bm {
            let mut read_tids = HashSet::with_capacity(bm.len() as usize);
            for idx in &bm {
                if let Some(row) = summaries.get(idx as usize) {
                    let tr = row.to_agent_trace(project_id, &strings)?;
                    let tr_meta = row.crdt_meta(&strings)?;
                    read_tids.insert(tr.trace_id.clone());
                    let (mut crdts, models, providers) =
                        resolve_span_crdts(row, &strings, has_ranks)?;
                    if let Some(span_tools) = v1_span_tools.remove(&tr.trace_id) {
                        apply_v1_span_tools(&mut crdts, &span_tools);
                    }
                    merge_into_map(&mut merged, tr, tr_meta, crdts, models, providers);
                }
            }
            if (bm.len() as usize) < summaries.len() {
                pruned_segments.push(PrunedSegment::Partial {
                    seg,
                    tail,
                    read_indices: bm,
                    read_tids,
                });
            }
        } else {
            for row in summaries.iter() {
                let tr = row.to_agent_trace(project_id, &strings)?;
                let tr_meta = row.crdt_meta(&strings)?;
                let (mut crdts, models, providers) = resolve_span_crdts(row, &strings, has_ranks)?;
                if let Some(span_tools) = v1_span_tools.remove(&tr.trace_id) {
                    apply_v1_span_tools(&mut crdts, &span_tools);
                }
                merge_into_map(&mut merged, tr, tr_meta, crdts, models, providers);
            }
        }
    }

    for (tr, spans) in &snap.l0_entries {
        let (summary, l0_meta, crdts, models, providers) = lightweight_l0_summary(
            tr,
            snap.l0_meta.get(&tr.trace_id),
            &snap.l0_span_ranks,
            spans,
        );
        merge_into_map(&mut merged, summary, l0_meta, crdts, models, providers);
    }

    if !pruned_segments.is_empty() && !merged.is_empty() {
        let candidate_tids: HashSet<String> = merged
            .keys()
            .filter(|tid| !tomb_idx.erased_traces.contains(tid.as_str()))
            .cloned()
            .collect();
        if !candidate_tids.is_empty() {
            for ps in &pruned_segments {
                if !ps.any_missing_candidate_may_exist(&candidate_tids) {
                    continue;
                }
                let seg = ps.seg();
                let tail = ps.tail();
                let read_indices = ps.read_indices();
                let strings = reader.read_string_table(seg, tail).await?;
                let summaries = reader.read_trace_summaries(seg, tail).await?;
                let has_ranks = tail.tag02_has_ranks();
                let mut matched_indices: HashSet<u32> = HashSet::new();
                let mut matched_rows: Vec<(String, &TraceSummaryRow)> = Vec::new();
                for (idx, row) in summaries.iter().enumerate() {
                    let idx_u32 = idx as u32;
                    if read_indices.is_some_and(|bm| bm.contains(idx_u32)) {
                        continue;
                    }
                    let tid = strings.get_required(row.trace_id_str, "trace_id")?;
                    if candidate_tids.contains(tid) {
                        matched_indices.insert(idx_u32);
                        matched_rows.push((tid.to_string(), row));
                    }
                }
                if matched_rows.is_empty() {
                    continue;
                }
                let mut v1_span_tools: V1SpanToolsMap = HashMap::new();
                if need_tools && !has_ranks {
                    let metrics = reader.read_span_metrics(seg, tail).await?;
                    for sm in metrics.iter() {
                        if !matched_indices.contains(&sm.trace_idx) {
                            continue;
                        }
                        let resolved = span_metric_from_row(sm, &strings)?;
                        record_v1_span_tool(&mut v1_span_tools, &resolved);
                    }
                }
                for (tid, row) in matched_rows {
                    let tr = row.to_agent_trace(project_id, &strings)?;
                    let tr_meta = row.crdt_meta(&strings)?;
                    let (mut crdts, models, providers) =
                        resolve_span_crdts(row, &strings, has_ranks)?;
                    if let Some(span_tools) = v1_span_tools.remove(&tid) {
                        apply_v1_span_tools(&mut crdts, &span_tools);
                    }
                    merge_into_map(&mut merged, tr, tr_meta, crdts, models, providers);
                }
            }
        }
    }

    let entries: Vec<MergedTraceEntry> = merged
        .into_values()
        .filter(|e| {
            !tomb_idx.is_trace_erased(
                &e.trace.trace_id,
                e.trace.session_id.as_deref(),
                e.trace.user_id.as_deref(),
            )
        })
        .collect();

    let base_traces: Vec<&AgentTrace> = entries
        .iter()
        .filter(|e| matches_analytics_filter(e, &base_filter))
        .map(|e| &e.trace)
        .collect();
    let cand_traces: Vec<&AgentTrace> = entries
        .iter()
        .filter(|e| matches_analytics_filter(e, &cand_filter))
        .map(|e| &e.trace)
        .collect();

    let baseline = build_compare_group_metrics(&base_traces);
    let candidate = build_compare_group_metrics(&cand_traces);

    let base_lat: Vec<f64> = base_traces
        .iter()
        .map(|t| t.duration_ms.max(0) as f64)
        .collect();
    let cand_lat: Vec<f64> = cand_traces
        .iter()
        .map(|t| t.duration_ms.max(0) as f64)
        .collect();
    let latency_ms = compare_continuous_metric(&base_lat, &cand_lat, false);

    let base_cost: Vec<i64> = base_traces.iter().map(|t| t.total_cost_nanos).collect();
    let cand_cost: Vec<i64> = cand_traces.iter().map(|t| t.total_cost_nanos).collect();
    let cost_usd = compare_cost_nanos_metric(&base_cost, &cand_cost);

    let base_tok: Vec<f64> = base_traces
        .iter()
        .map(|t| t.total_tokens.max(0) as f64)
        .collect();
    let cand_tok: Vec<f64> = cand_traces
        .iter()
        .map(|t| t.total_tokens.max(0) as f64)
        .collect();
    let total_tokens = compare_continuous_metric(&base_tok, &cand_tok, false);

    let base_err: Vec<f64> = base_traces
        .iter()
        .map(|t| if is_error_status(t.status) { 1.0 } else { 0.0 })
        .collect();
    let cand_err: Vec<f64> = cand_traces
        .iter()
        .map(|t| if is_error_status(t.status) { 1.0 } else { 0.0 })
        .collect();
    let error_rate = compare_continuous_metric(&base_err, &cand_err, false);

    let base_succ: Vec<f64> = base_traces
        .iter()
        .map(|t| if is_error_status(t.status) { 0.0 } else { 1.0 })
        .collect();
    let cand_succ: Vec<f64> = cand_traces
        .iter()
        .map(|t| if is_error_status(t.status) { 0.0 } else { 1.0 })
        .collect();
    let success_rate = compare_continuous_metric(&base_succ, &cand_succ, true);

    let base_scores: Vec<f64> = base_traces
        .iter()
        .filter_map(|t| t.avg_eval_score)
        .collect();
    let cand_scores: Vec<f64> = cand_traces
        .iter()
        .filter_map(|t| t.avg_eval_score)
        .collect();
    let eval_score = compare_continuous_metric(&base_scores, &cand_scores, true);

    let baseline_sample_size = base_traces.len();
    let candidate_sample_size = cand_traces.len();
    let sufficient_samples = baseline_sample_size >= MIN_SIGNIFICANCE_SAMPLES
        && candidate_sample_size >= MIN_SIGNIFICANCE_SAMPLES;

    let deltas: [&MetricDelta; 6] = [
        &latency_ms,
        &cost_usd,
        &total_tokens,
        &error_rate,
        &success_rate,
        &eval_score,
    ];
    let overall_verdict = if !sufficient_samples {
        "insufficient_data".to_string()
    } else if deltas.iter().any(|d| d.verdict == "significant_regression") {
        "significant_regression".to_string()
    } else if deltas
        .iter()
        .any(|d| d.verdict == "significant_improvement")
    {
        "significant_improvement".to_string()
    } else {
        "no_significant_change".to_string()
    };

    Ok(CompareResponse {
        baseline_label: derive_group_label(
            filter.baseline_label.as_deref(),
            &base_filter,
            "baseline",
        ),
        candidate_label: derive_group_label(
            filter.candidate_label.as_deref(),
            &cand_filter,
            "candidate",
        ),
        baseline_sample_size,
        candidate_sample_size,
        min_significance_samples: MIN_SIGNIFICANCE_SAMPLES,
        sufficient_samples,
        overall_verdict,
        baseline,
        candidate,
        latency_ms,
        cost_usd,
        total_tokens,
        error_rate,
        success_rate,
        eval_score,
    })
}
