use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use chrono::NaiveDate;
use object_store::path::Path as ObjectPath;
use object_store::PutMode;

use crate::model::{AgentErasureTombstone, AgentSpan, AgentTrace, ErasureLevel};
use crate::store::error::{validate_identifier, StoreError, StoreResult};
use crate::store::manifest::{
    compute_urgent_grace_ms, partition_upload_key, PendingAgentS3Delete, PendingGcEntry,
    ReaderPinTracker, SegmentDescriptor,
};
use crate::store::memtable::{
    merge_spans_crdt_with_ranks, merge_trace_headers_with_meta,
    recompute_trace_from_spans_with_explicit, RankedTraceEntry, SpanFieldRanks, TraceCrdtMeta,
};
use crate::store::segment::SegmentWriter;
use crate::store::{AgentTraceStore, ProjectAgentState};

pub const DEFAULT_L0_COMPACT_THRESHOLD: usize = 4;
pub const DEFAULT_TIER_COMPACT_FANIN: usize = 8;

struct SegmentPinGuard {
    pin_tracker: Arc<ReaderPinTracker>,
    paths: Vec<String>,
}

impl SegmentPinGuard {
    fn new(pin_tracker: Arc<ReaderPinTracker>, paths: Vec<String>) -> Self {
        if !paths.is_empty() {
            pin_tracker.pin_all(&paths);
        }
        Self { pin_tracker, paths }
    }
}

impl Drop for SegmentPinGuard {
    fn drop(&mut self) {
        if !self.paths.is_empty() {
            self.pin_tracker.unpin_all(&self.paths);
        }
    }
}

pub(crate) fn trace_matches_tombstones(
    trace: &AgentTrace,
    active_tombstones: &[&AgentErasureTombstone],
) -> bool {
    active_tombstones.iter().any(|tb| match tb.level {
        ErasureLevel::Trace => tb.target_id == trace.trace_id,
        ErasureLevel::Session => trace.session_id.as_deref() == Some(tb.target_id.as_str()),
        ErasureLevel::User => trace.user_id.as_deref() == Some(tb.target_id.as_str()),
    })
}

pub(crate) fn span_matches_trace_tombstones(
    span: &AgentSpan,
    active_tombstones: &[&AgentErasureTombstone],
) -> bool {
    active_tombstones
        .iter()
        .any(|tb| tb.level == ErasureLevel::Trace && tb.target_id == span.trace_id)
}

pub(crate) async fn compact_and_gc_impl(
    store: &AgentTraceStore,
    project_id: Option<&str>,
    tombstones: &[AgentErasureTombstone],
) -> StoreResult<()> {
    if store.closed.load(Ordering::Acquire) {
        return Err(StoreError::Shutdown);
    }
    if store.config.read_only {
        return Ok(());
    }

    let targets: Vec<String> = match project_id {
        Some(pid) => {
            validate_identifier("project_id", pid)?;
            vec![pid.to_string()]
        }
        None => store.list_known_projects().await?,
    };

    for pid in targets {
        let state = store.get_or_open_project(&pid).await?;
        let _pub_guard = state.publish_lock.lock().await;
        compact_project_locked(
            store,
            &state,
            tombstones,
            None,
            None,
            chrono::Utc::now().timestamp_millis(),
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn compact_project_locked(
    store: &AgentTraceStore,
    state: &Arc<ProjectAgentState>,
    tombstones: &[AgentErasureTombstone],
    urgent_deadlines: Option<&HashMap<String, Option<i64>>>,
    extra_erased_trace_ids: Option<&HashSet<String>>,
    now_ms: i64,
) -> StoreResult<()> {
    if state.fenced.load(Ordering::Acquire) {
        return Err(StoreError::NotWriter {
            project_id: state.project_id.clone(),
        });
    }
    let pid = state.project_id.as_str();
    let is_local_hot = store.is_local_hot_tier();
    let upload_interval_ms = store.upload_interval_ms.load(Ordering::Acquire);
    let cold_merge_idle_ms = store.cold_merge_idle_ms.load(Ordering::Acquire) as i64;
    let upload_max_input = store
        .upload_merge_max_input_bytes
        .load(Ordering::Acquire)
        .max(1);
    let today_utc = chrono::DateTime::from_timestamp_millis(now_ms)
        .unwrap_or_default()
        .date_naive();

    store.drain_pending_local_deletes(state).await;

    let manifest = state.manifest_store.load_latest().await?;
    let loaded_epoch = manifest.epoch;
    state
        .memtable
        .evict_committed_wal(&manifest.committed_wal_seq);
    if !manifest.erased_traces.is_empty() {
        state.memtable.purge_erased_ids(&manifest.erased_traces);
    }

    let active_tombstones: Vec<&AgentErasureTombstone> = tombstones
        .iter()
        .filter(|tb| !tb.is_expired(now_ms) && (tb.project_id.is_empty() || tb.project_id == pid))
        .collect();

    let tb_deadline = |tb: &AgentErasureTombstone| -> Option<i64> {
        match urgent_deadlines {
            Some(map) => map.get(&tb.id).copied().flatten(),
            None => tb.expires_at,
        }
    };

    let mut erased_trace_ids: HashSet<String> = manifest.erased_traces.keys().cloned().collect();
    let mut newly_erased_ids: HashSet<String> = HashSet::new();
    if let Some(extra) = extra_erased_trace_ids {
        for tid in extra {
            erased_trace_ids.insert(tid.clone());
            if !manifest.erased_traces.contains_key(tid) {
                newly_erased_ids.insert(tid.clone());
            }
        }
    }

    if !active_tombstones.is_empty() {
        let active_owned: Vec<AgentErasureTombstone> =
            active_tombstones.iter().copied().cloned().collect();
        for tid in state.memtable.purge_erased_with_ids(&active_owned) {
            erased_trace_ids.insert(tid.clone());
            if !manifest.erased_traces.contains_key(&tid) {
                newly_erased_ids.insert(tid);
            }
        }
        for tb in &active_tombstones {
            if tb.level == ErasureLevel::Trace {
                erased_trace_ids.insert(tb.target_id.clone());
                if !manifest.erased_traces.contains_key(&tb.target_id) {
                    newly_erased_ids.insert(tb.target_id.clone());
                }
            }
        }

        // Scan uncommitted WALs read-only so `--role compactor` also resolves headers written
        // by separate `--role ingest` nodes that have not yet flushed to `.plss`.
        let mut dummy_applied = HashMap::new();
        let wal_batches = state
            .wal_store
            .replay_uncommitted(&manifest.committed_wal_seq, &mut dummy_applied, false)
            .await?;
        for (_wid, _seq, batch) in wal_batches {
            for tr in &batch.traces {
                if trace_matches_tombstones(tr, &active_tombstones) {
                    erased_trace_ids.insert(tr.trace_id.clone());
                    if !manifest.erased_traces.contains_key(&tr.trace_id) {
                        newly_erased_ids.insert(tr.trace_id.clone());
                    }
                }
            }
        }
    }

    let grace_ms = store.gc_grace_ms();
    let has_ready_erased_gc = manifest
        .erased_traces
        .values()
        .any(|&ts| now_ms.saturating_sub(ts) >= grace_ms);
    let has_ready_pending_gc = manifest
        .pending_gc
        .iter()
        .any(|e| now_ms >= e.eligible_after_ms && !store.pin_tracker().is_pinned(&e.object_path));
    let has_ready_pending_s3 = manifest.pending_s3_deletes.iter().any(|e| {
        e.eligible_after_ms.is_some_and(|t| now_ms >= t)
            && !store.pin_tracker().is_pinned(&e.object_path)
    });

    // Zero-S3-call idle fast-path when nothing in this project requires compaction, erasure, or GC.
    if active_tombstones.is_empty()
        && newly_erased_ids.is_empty()
        && !has_ready_erased_gc
        && !has_ready_pending_gc
        && !has_ready_pending_s3
        && !state.memtable.has_purged_wal()
    {
        let mut has_compactable = false;
        let mut by_part: BTreeMap<(String, u16), (usize, usize, i64)> = BTreeMap::new();
        for seg in &manifest.segments {
            let is_local_seg = seg.object_path.starts_with("hot/agent_segments/");
            let entry = by_part.entry((seg.date.clone(), seg.shard_id)).or_insert((
                0,
                0,
                seg.created_at_ms,
            ));
            if is_local_seg {
                if seg.level == 0 {
                    entry.0 += 1;
                }
            } else {
                entry.1 += 1;
                entry.2 = entry.2.max(seg.created_at_ms);
            }
        }
        for ((date, shard_id), (local_l0s, s3_cnt, max_s3_created)) in by_part {
            if is_local_hot {
                if upload_interval_ms == 0 && local_l0s >= DEFAULT_L0_COMPACT_THRESHOLD {
                    has_compactable = true;
                    break;
                }
                if s3_cnt >= 2 {
                    let closed =
                        NaiveDate::parse_from_str(&date, "%Y-%m-%d").is_ok_and(|d| d < today_utc);
                    if closed {
                        let last_up = manifest
                            .s3_partition_last_upload_ms
                            .get(&partition_upload_key(&date, shard_id))
                            .copied()
                            .unwrap_or(max_s3_created);
                        if now_ms.saturating_sub(last_up) >= cold_merge_idle_ms {
                            has_compactable = true;
                            break;
                        }
                    }
                }
            } else if local_l0s >= DEFAULT_L0_COMPACT_THRESHOLD || s3_cnt >= 2 {
                has_compactable = true;
                break;
            }
        }
        if !has_compactable {
            let safe_wal = state.memtable.compute_safe_committed_wal_seq(
                &manifest.committed_wal_seq,
                &[],
                &store.writer_id_str,
            );
            let wal_advances = safe_wal
                .iter()
                .any(|(wid, &seq)| seq > manifest.committed_wal_seq.get(wid).copied().unwrap_or(0));
            if !wal_advances {
                state.dirty_since_compact.store(false, Ordering::Release);
                return Ok(());
            }
        }
    }

    let reader = store.plss_reader();
    let mut forced_ids: HashSet<String> = HashSet::new();
    let mut seg_matching_tbs: HashMap<String, Vec<&AgentErasureTombstone>> = HashMap::new();
    let mut seg_tails = Vec::with_capacity(manifest.segments.len());

    if !active_tombstones.is_empty() || !erased_trace_ids.is_empty() {
        for seg in &manifest.segments {
            let tail = reader.read_eof_tail(seg).await?;
            let matching_tbs: Vec<&AgentErasureTombstone> = active_tombstones
                .iter()
                .copied()
                .filter(|tb| match tb.level {
                    ErasureLevel::Trace => tail.index.may_contain_trace(&tb.target_id),
                    ErasureLevel::Session => tail.index.may_contain_session(&tb.target_id),
                    ErasureLevel::User => tail.index.may_contain_user(&tb.target_id),
                })
                .collect();
            let hit = !matching_tbs.is_empty()
                || erased_trace_ids
                    .iter()
                    .any(|tid| tail.index.may_contain_trace(tid));
            if hit {
                forced_ids.insert(seg.segment_id.clone());
                if !matching_tbs.is_empty() {
                    seg_matching_tbs.insert(seg.segment_id.clone(), matching_tbs);
                }
            }
            seg_tails.push((seg, tail));
        }

        // Pre-scan forced segments (Tag 0x01 + Tag 0x02 only) to resolve all erased trace_ids
        // from Session/User tombstones before building CompactionPlans, then expand forced_ids
        // to any span-only partial segments and purge matching unflushed L0 entries.
        if !active_tombstones.is_empty() && !forced_ids.is_empty() {
            for (seg, tail) in &seg_tails {
                if !forced_ids.contains(&seg.segment_id) {
                    continue;
                }
                let strings = reader.read_string_table(seg, tail).await?;
                let summaries = reader.read_trace_summaries(seg, tail).await?;
                for row in summaries.iter() {
                    let tid = strings.get_required(row.trace_id_str, "trace_id")?;
                    let sid = strings
                        .get_opt(row.session_id_str, "session_id")?
                        .filter(|s| !s.is_empty());
                    let uid = strings
                        .get_opt(row.user_id_str, "user_id")?
                        .filter(|s| !s.is_empty());
                    for tb in &active_tombstones {
                        let matched = match tb.level {
                            ErasureLevel::Trace => tb.target_id == tid,
                            ErasureLevel::Session => sid == Some(tb.target_id.as_str()),
                            ErasureLevel::User => uid == Some(tb.target_id.as_str()),
                        };
                        if matched {
                            seg_matching_tbs
                                .entry(seg.segment_id.clone())
                                .or_default()
                                .push(*tb);
                            if !erased_trace_ids.contains(tid) {
                                erased_trace_ids.insert(tid.to_string());
                                if !manifest.erased_traces.contains_key(tid) {
                                    newly_erased_ids.insert(tid.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }

        if !erased_trace_ids.is_empty() {
            for (seg, tail) in &seg_tails {
                if forced_ids.contains(&seg.segment_id) {
                    continue;
                }
                if erased_trace_ids
                    .iter()
                    .any(|tid| tail.index.may_contain_trace(tid))
                {
                    forced_ids.insert(seg.segment_id.clone());
                }
            }
            state.memtable.purge_erased_id_set(&erased_trace_ids);
        }
    }

    // Group segments by `(date, shard_id, is_local_tier)` so local `hot/agent_segments/` and
    // S3 `agent_segments/` are NEVER merged into the same segment file.
    let mut buckets: BTreeMap<(String, u16, bool), Vec<&SegmentDescriptor>> = BTreeMap::new();
    for seg in &manifest.segments {
        let seg_is_local = is_local_hot && seg.object_path.starts_with("hot/agent_segments/");
        buckets
            .entry((seg.date.clone(), seg.shard_id, seg_is_local))
            .or_default()
            .push(seg);
    }

    let max_seg_bytes = store.max_segment_bytes() as u64;
    let max_batch_input_bytes = max_seg_bytes.saturating_mul(2).max(2048);

    struct CompactionPlan<'a> {
        date: String,
        shard_id: u16,
        is_local: bool,
        target_level: u8,
        segs: Vec<&'a SegmentDescriptor>,
    }

    let mut plans: Vec<CompactionPlan<'_>> = Vec::new();
    for ((date, shard_id, seg_is_local), mut segs) in buckets {
        segs.sort_by(|a, b| {
            a.level
                .cmp(&b.level)
                .then_with(|| a.created_at_ms.cmp(&b.created_at_ms))
                .then_with(|| a.segment_id.cmp(&b.segment_id))
        });

        let mut chosen_ids: HashSet<String> = HashSet::new();
        let mut chosen: Vec<&SegmentDescriptor> = Vec::new();

        for &seg in &segs {
            if forced_ids.contains(&seg.segment_id) && chosen_ids.insert(seg.segment_id.clone()) {
                chosen.push(seg);
            }
        }

        let non_forced: Vec<&SegmentDescriptor> = segs
            .iter()
            .copied()
            .filter(|s| !forced_ids.contains(&s.segment_id))
            .collect();

        if is_local_hot {
            if seg_is_local {
                // Local hot tier merges once at upload; count-based compaction runs only when
                // `PULSE_UPLOAD_INTERVAL_SECS=0`.
                if upload_interval_ms == 0 {
                    let l0s: Vec<&SegmentDescriptor> = non_forced
                        .iter()
                        .copied()
                        .filter(|s| s.level == 0)
                        .collect();
                    if l0s.len() >= DEFAULT_L0_COMPACT_THRESHOLD {
                        let mut acc_bytes: u64 = 0;
                        for (idx, &seg) in l0s.iter().enumerate() {
                            if chosen.len() >= DEFAULT_TIER_COMPACT_FANIN {
                                break;
                            }
                            if idx >= 2
                                && acc_bytes.saturating_add(seg.file_size) > max_batch_input_bytes
                            {
                                break;
                            }
                            if chosen_ids.insert(seg.segment_id.clone()) {
                                acc_bytes = acc_bytes.saturating_add(seg.file_size);
                                chosen.push(seg);
                            }
                        }
                    }
                }
            } else if non_forced.len() >= 2 {
                // Cold S3 merge: only closed dates idle for >= `cold_merge_idle_ms`.
                let is_closed =
                    NaiveDate::parse_from_str(&date, "%Y-%m-%d").is_ok_and(|d| d < today_utc);
                if is_closed {
                    let max_created = non_forced
                        .iter()
                        .map(|s| s.created_at_ms)
                        .max()
                        .unwrap_or(0);
                    let last_up = manifest
                        .s3_partition_last_upload_ms
                        .get(&partition_upload_key(&date, shard_id))
                        .copied()
                        .unwrap_or(max_created);
                    if now_ms.saturating_sub(last_up) >= cold_merge_idle_ms {
                        let input_cap = upload_max_input
                            .min(max_seg_bytes.saturating_mul(2))
                            .max(2048);
                        let mut acc_bytes: u64 = 0;
                        let mut cold_group = Vec::new();
                        for &seg in &non_forced {
                            if !cold_group.is_empty()
                                && acc_bytes.saturating_add(seg.file_size) > input_cap
                            {
                                break;
                            }
                            acc_bytes = acc_bytes.saturating_add(seg.file_size);
                            cold_group.push(seg);
                        }
                        if cold_group.len() >= 2 {
                            for seg in cold_group {
                                if chosen_ids.insert(seg.segment_id.clone()) {
                                    chosen.push(seg);
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // `object` mode preserves original L0 threshold & small-segment coalescing.
            let l0s: Vec<&SegmentDescriptor> = non_forced
                .iter()
                .copied()
                .filter(|s| s.level == 0)
                .collect();

            if l0s.len() >= DEFAULT_L0_COMPACT_THRESHOLD {
                let mut acc_bytes: u64 = 0;
                for (idx, &seg) in l0s.iter().enumerate() {
                    if chosen.len() >= DEFAULT_TIER_COMPACT_FANIN {
                        break;
                    }
                    if idx >= 2 && acc_bytes.saturating_add(seg.file_size) > max_batch_input_bytes {
                        break;
                    }
                    if chosen_ids.insert(seg.segment_id.clone()) {
                        acc_bytes = acc_bytes.saturating_add(seg.file_size);
                        chosen.push(seg);
                    }
                }
            } else if non_forced.len() >= 2 {
                // Coalesce >= 2 small segments whose combined size stays <= max_segment_bytes
                let mut candidates: Vec<&SegmentDescriptor> = non_forced;
                candidates.sort_by(|a, b| {
                    a.file_size
                        .cmp(&b.file_size)
                        .then_with(|| a.level.cmp(&b.level))
                        .then_with(|| a.created_at_ms.cmp(&b.created_at_ms))
                });
                let mut small_group = Vec::new();
                let mut sum_bytes: u64 = 0;
                for seg in candidates {
                    if small_group.len() >= DEFAULT_TIER_COMPACT_FANIN {
                        break;
                    }
                    if sum_bytes.saturating_add(seg.file_size) <= max_seg_bytes {
                        sum_bytes = sum_bytes.saturating_add(seg.file_size);
                        small_group.push(seg);
                    }
                }
                if small_group.len() >= 2 {
                    for seg in small_group {
                        if chosen_ids.insert(seg.segment_id.clone()) {
                            chosen.push(seg);
                        }
                    }
                }
            }
        }

        if !chosen.is_empty() {
            let target_level = chosen
                .iter()
                .map(|s| s.level)
                .max()
                .unwrap_or(0)
                .saturating_add(1);
            plans.push(CompactionPlan {
                date,
                shard_id,
                is_local: seg_is_local,
                target_level,
                segs: chosen,
            });
        }
    }

    let mut superseded_map: HashMap<String, &SegmentDescriptor> = HashMap::new();
    let mut new_segments: Vec<SegmentDescriptor> = Vec::new();
    let mut cold_merged_parts: HashSet<(String, u16)> = HashSet::new();

    if !plans.is_empty() {
        let selected_paths: Vec<String> = plans
            .iter()
            .flat_map(|p| p.segs.iter().map(|s| s.object_path.clone()))
            .collect();
        let _pin_guard = SegmentPinGuard::new(store.pin_tracker().clone(), selected_paths);

        struct CompactedTraceAcc {
            trace: AgentTrace,
            meta: TraceCrdtMeta,
            spans: HashMap<String, (AgentSpan, SpanFieldRanks)>,
        }

        for plan in &plans {
            let mut merged_traces: HashMap<String, CompactedTraceAcc> = HashMap::new();

            for &seg in &plan.segs {
                superseded_map.insert(seg.segment_id.clone(), seg);
                let entries = reader.read_all_traces_and_spans_ranked(seg).await?;
                for (tr, tr_meta, spans) in entries {
                    if erased_trace_ids.contains(&tr.trace_id)
                        || (!active_tombstones.is_empty()
                            && trace_matches_tombstones(&tr, &active_tombstones))
                    {
                        if !manifest.erased_traces.contains_key(&tr.trace_id) {
                            newly_erased_ids.insert(tr.trace_id.clone());
                        }
                        erased_trace_ids.insert(tr.trace_id);
                        continue;
                    }
                    let surviving_spans: Vec<(AgentSpan, SpanFieldRanks)> =
                        if active_tombstones.is_empty() && erased_trace_ids.is_empty() {
                            spans
                        } else {
                            spans
                                .into_iter()
                                .filter(|(sp, _)| {
                                    !erased_trace_ids.contains(&sp.trace_id)
                                        && !span_matches_trace_tombstones(sp, &active_tombstones)
                                })
                                .collect()
                        };
                    match merged_traces.get_mut(&tr.trace_id) {
                        Some(acc) => {
                            merge_trace_headers_with_meta(
                                &mut acc.trace,
                                &mut acc.meta,
                                tr,
                                tr_meta,
                            );
                            merge_spans_crdt_with_ranks(&mut acc.spans, surviving_spans);
                        }
                        None => {
                            let tid = tr.trace_id.clone();
                            let mut dst_spans = HashMap::with_capacity(surviving_spans.len());
                            merge_spans_crdt_with_ranks(&mut dst_spans, surviving_spans);
                            merged_traces.insert(
                                tid,
                                CompactedTraceAcc {
                                    trace: tr,
                                    meta: tr_meta,
                                    spans: dst_spans,
                                },
                            );
                        }
                    }
                }
            }

            let mut merged_entries: Vec<RankedTraceEntry> = Vec::with_capacity(merged_traces.len());
            for (_tid, acc) in merged_traces {
                let mut tr = acc.trace;
                let mut ranked_spans: Vec<(AgentSpan, SpanFieldRanks)> =
                    acc.spans.into_values().collect();
                ranked_spans.sort_by(|a, b| {
                    a.0.start_ts_ms
                        .cmp(&b.0.start_ts_ms)
                        .then_with(|| a.0.span_id.cmp(&b.0.span_id))
                });
                let sp_vec: Vec<AgentSpan> = ranked_spans.iter().map(|(s, _)| s.clone()).collect();
                recompute_trace_from_spans_with_explicit(
                    &mut tr,
                    acc.meta.explicit_status,
                    acc.meta.explicit_failure_source,
                    acc.meta.explicit_has_loop_warning,
                    acc.meta.explicit_primary_model.clone(),
                    acc.meta.explicit_primary_provider.clone(),
                    &sp_vec,
                );
                if erased_trace_ids.contains(&tr.trace_id)
                    || (!active_tombstones.is_empty()
                        && trace_matches_tombstones(&tr, &active_tombstones))
                {
                    if !manifest.erased_traces.contains_key(&tr.trace_id) {
                        newly_erased_ids.insert(tr.trace_id.clone());
                    }
                    erased_trace_ids.insert(tr.trace_id);
                    continue;
                }
                merged_entries.push((tr, acc.meta, ranked_spans));
            }

            if !merged_entries.is_empty() {
                let (prefix_root, put_mode) = if is_local_hot && plan.is_local {
                    ("hot/agent_segments", PutMode::Overwrite)
                } else if is_local_hot {
                    cold_merged_parts.insert((plan.date.clone(), plan.shard_id));
                    ("agent_segments", PutMode::Create)
                } else {
                    ("agent_segments", PutMode::Overwrite)
                };
                let written = SegmentWriter::write_segments_split_ranked_with_prefix_and_mode(
                    prefix_root,
                    pid,
                    &plan.date,
                    plan.shard_id,
                    plan.target_level,
                    merged_entries,
                    store.max_segment_bytes(),
                    store.object_store(),
                    put_mode,
                    None,
                )
                .await;
                match written {
                    Ok(segs) => new_segments.extend(segs),
                    Err(e) => {
                        cleanup_orphan_segments(store, &new_segments).await;
                        return Err(e);
                    }
                }
            }
        }
    }

    // Determine pre-existing `manifest.erased_traces` entries eligible for safe GC:
    // 1. Not newly erased in this call and not targeted by an active Trace tombstone
    // 2. Grace period (`gc_grace_ms`) has elapsed since `erased_at_ms`
    // 3. No surviving segment's Bloom filter may contain `tid`
    // 4. No uncommitted WAL (`seq > committed_wal_seq[wid]`) contains `tid`
    let commit_ts_ms = chrono::Utc::now().timestamp_millis();
    let mut gc_erased_ids: HashSet<String> = HashSet::new();
    if !manifest.erased_traces.is_empty() {
        let mut gc_candidates: Vec<String> = Vec::new();
        for (tid, &erased_at_ms) in &manifest.erased_traces {
            if newly_erased_ids.contains(tid) {
                continue;
            }
            if active_tombstones
                .iter()
                .any(|tb| tb.level == ErasureLevel::Trace && tb.target_id == *tid)
            {
                continue;
            }
            if commit_ts_ms.saturating_sub(erased_at_ms) < grace_ms {
                continue;
            }
            let any_surviving_seg_hit = seg_tails.iter().any(|(seg, tail)| {
                !superseded_map.contains_key(&seg.segment_id) && tail.index.may_contain_trace(tid)
            });
            if !any_surviving_seg_hit {
                gc_candidates.push(tid.clone());
            }
        }
        if !gc_candidates.is_empty() {
            let wal_tids = state
                .wal_store
                .uncommitted_trace_ids(&manifest.committed_wal_seq)
                .await?;
            for tid in gc_candidates {
                if !wal_tids.contains(&tid) {
                    gc_erased_ids.insert(tid);
                }
            }
        }
    }

    let finite_tombstones: Vec<(&AgentErasureTombstone, i64)> = active_tombstones
        .iter()
        .filter_map(|&tb| tb_deadline(tb).map(|dl| (tb, dl)))
        .collect();
    let normal_s3_grace_ms = if is_local_hot {
        (upload_interval_ms as i64).saturating_mul(2)
    } else {
        grace_ms
    };

    let mut aborted_due_to_missing = false;
    let mut manifest_changed = false;
    let mut superseded_local_paths: Vec<String> = Vec::new();
    let mut advanced_wids: Vec<(String, u64)> = Vec::new();

    let commit_res = state
        .manifest_store
        .commit_with_cas(|m| {
            superseded_local_paths.clear();
            advanced_wids.clear();
            if !superseded_map.is_empty() {
                let present_ids: HashSet<&str> =
                    m.segments.iter().map(|s| s.segment_id.as_str()).collect();
                if superseded_map
                    .keys()
                    .any(|sid| !present_ids.contains(sid.as_str()))
                {
                    aborted_due_to_missing = true;
                    return Ok(false);
                }
            }

            let mut changed = !plans.is_empty();
            if !superseded_map.is_empty() || !new_segments.is_empty() {
                let mut pending_gc_paths: HashSet<String> =
                    m.pending_gc.iter().map(|p| p.object_path.clone()).collect();
                let mut pending_s3_paths: HashSet<String> = m
                    .pending_s3_deletes
                    .iter()
                    .map(|p| p.object_path.clone())
                    .collect();

                m.segments
                    .retain(|s| !superseded_map.contains_key(&s.segment_id));
                m.segments.extend(new_segments.clone());

                for (date, shard_id) in &cold_merged_parts {
                    m.s3_partition_last_upload_ms
                        .insert(partition_upload_key(date, *shard_id), commit_ts_ms);
                }

                let next_pub_seq = m.last_published_seq.map_or(0, |s| s.saturating_add(1));

                for seg in superseded_map.values() {
                    let matching = seg_matching_tbs.get(&seg.segment_id);
                    let mut tb_ids: Vec<String> = matching
                        .map(|v| v.iter().map(|tb| tb.id.clone()).collect())
                        .unwrap_or_default();
                    tb_ids.sort();
                    tb_ids.dedup();
                    let urgent_dl =
                        matching.and_then(|v| v.iter().filter_map(|tb| tb_deadline(tb)).min());

                    if is_local_hot && seg.object_path.starts_with("hot/agent_segments/") {
                        superseded_local_paths.push(seg.object_path.clone());
                    } else if is_local_hot {
                        if pending_s3_paths.insert(seg.object_path.clone()) {
                            m.pending_s3_deletes.push(PendingAgentS3Delete {
                                object_path: seg.object_path.clone(),
                                segment_id: seg.segment_id.clone(),
                                superseding_pub_seq: next_pub_seq,
                                superseded_at_ms: None,
                                eligible_after_ms: None,
                                urgent_erasure_deadline_ms: urgent_dl,
                                erasure_tombstone_ids: tb_ids,
                            });
                        }
                    } else if pending_gc_paths.insert(seg.object_path.clone()) {
                        let eff_grace = match urgent_dl {
                            Some(dl) => compute_urgent_grace_ms(commit_ts_ms, dl, grace_ms),
                            None => grace_ms,
                        };
                        m.pending_gc.push(PendingGcEntry {
                            object_path: seg.object_path.clone(),
                            segment_id: seg.segment_id.clone(),
                            superseded_at_ms: commit_ts_ms,
                            eligible_after_ms: commit_ts_ms.saturating_add(eff_grace),
                            urgent_erasure_deadline_ms: urgent_dl,
                            erasure_tombstone_ids: tb_ids,
                        });
                    }
                }
                changed = true;
            }

            // Attach finite-TTL tombstones to any pre-existing un-deleted pending GC entries from
            // <= tb.created_at so pre-existing superseded segments cannot outlive `expires_at`.
            if !finite_tombstones.is_empty() {
                for entry in &mut m.pending_gc {
                    for &(tb, dl) in &finite_tombstones {
                        if entry.superseded_at_ms <= tb.created_at {
                            if !entry.erasure_tombstone_ids.contains(&tb.id) {
                                entry.erasure_tombstone_ids.push(tb.id.clone());
                                changed = true;
                            }
                            let next_dl = entry
                                .urgent_erasure_deadline_ms
                                .map_or(dl, |cur| cur.min(dl));
                            if entry.urgent_erasure_deadline_ms != Some(next_dl) {
                                entry.urgent_erasure_deadline_ms = Some(next_dl);
                                changed = true;
                            }
                            let eff_grace =
                                compute_urgent_grace_ms(entry.superseded_at_ms, next_dl, grace_ms);
                            let capped_elig = entry.superseded_at_ms.saturating_add(eff_grace);
                            if capped_elig < entry.eligible_after_ms {
                                entry.eligible_after_ms = capped_elig;
                                changed = true;
                            }
                        }
                    }
                }
                for entry in &mut m.pending_s3_deletes {
                    for &(tb, dl) in &finite_tombstones {
                        if entry.superseded_at_ms.is_none_or(|s| s <= tb.created_at)
                            || entry.eligible_after_ms.is_none()
                            || entry.erasure_tombstone_ids.contains(&tb.id)
                        {
                            if !entry.erasure_tombstone_ids.contains(&tb.id) {
                                entry.erasure_tombstone_ids.push(tb.id.clone());
                                changed = true;
                            }
                            let next_dl = entry
                                .urgent_erasure_deadline_ms
                                .map_or(dl, |cur| cur.min(dl));
                            if entry.urgent_erasure_deadline_ms != Some(next_dl) {
                                entry.urgent_erasure_deadline_ms = Some(next_dl);
                                changed = true;
                            }
                            if let (Some(sup_at), Some(cur_elig)) =
                                (entry.superseded_at_ms, entry.eligible_after_ms)
                            {
                                let eff_grace =
                                    compute_urgent_grace_ms(sup_at, next_dl, normal_s3_grace_ms);
                                let capped_elig = sup_at.saturating_add(eff_grace);
                                if capped_elig < cur_elig {
                                    entry.eligible_after_ms = Some(capped_elig);
                                    changed = true;
                                }
                            }
                        }
                    }
                }
            }

            for tid in &newly_erased_ids {
                if !m.erased_traces.contains_key(tid) {
                    m.erased_traces.insert(tid.clone(), commit_ts_ms);
                    changed = true;
                }
            }
            if m.epoch == loaded_epoch && !gc_erased_ids.is_empty() {
                let before = m.erased_traces.len();
                m.erased_traces
                    .retain(|tid, _| !gc_erased_ids.contains(tid));
                if m.erased_traces.len() != before {
                    changed = true;
                }
            }

            // Never advance `committed_wal_seq` or purge `.ptwl` files for live foreign writers whose
            // in-memory L0 this compactor does not own (only recovered dead local writers in local tier).
            let safe_wal = state.memtable.compute_safe_committed_wal_seq(
                &m.committed_wal_seq,
                &[],
                &store.writer_id_str,
            );
            for (wid, safe_seq) in safe_wal {
                let prev = m.committed_wal_seq.get(&wid).copied().unwrap_or(0);
                if safe_seq > prev {
                    m.committed_wal_seq.insert(wid.clone(), safe_seq);
                    advanced_wids.push((wid, safe_seq));
                    changed = true;
                }
            }

            if changed {
                m.updated_at_ms = commit_ts_ms;
            }
            manifest_changed = changed;
            Ok(changed)
        })
        .await;

    if aborted_due_to_missing || commit_res.is_err() {
        cleanup_orphan_segments(store, &new_segments).await;
    }
    let committed_manifest = commit_res?;

    for seg_id in superseded_map.keys() {
        store.cache().invalidate_segment(seg_id);
    }
    if !superseded_local_paths.is_empty() {
        state
            .pending_local_deletes
            .lock()
            .extend(superseded_local_paths);
    }
    store.drain_pending_local_deletes(state).await;

    // Step C: Execute pending GC respecting grace period & reader pins + clean old manifest epochs
    let gc_now_ms = chrono::Utc::now().timestamp_millis();
    if !committed_manifest.pending_gc.is_empty()
        || !committed_manifest.pending_s3_deletes.is_empty()
    {
        state
            .manifest_store
            .run_gc(gc_now_ms, store.pin_tracker())
            .await?;
    }
    if manifest_changed || !is_local_hot {
        state.manifest_store.gc_old_epochs(gc_now_ms).await?;
    }

    // Step D: Purge WAL files safely committed in the manifest
    for (wid, safe_seq) in &advanced_wids {
        if *safe_seq > 0 {
            let _ = state.wal_store.purge_up_to(wid, *safe_seq).await?;
        }
    }
    if let Some(&safe_seq) = committed_manifest
        .committed_wal_seq
        .get(&store.writer_id_str)
    {
        if safe_seq > 0 && !advanced_wids.iter().any(|(w, _)| w == &store.writer_id_str) {
            let _ = state
                .wal_store
                .purge_up_to(&store.writer_id_str, safe_seq)
                .await?;
        }
    }
    state.memtable.clear_purged_wal_flag();
    state.dirty_since_compact.store(false, Ordering::Release);

    Ok(())
}

async fn cleanup_orphan_segments(store: &AgentTraceStore, new_segments: &[SegmentDescriptor]) {
    for ns in new_segments {
        let loc = ObjectPath::from(ns.object_path.as_str());
        let _ = store.object_store().delete(&loc).await;
    }
}

pub(crate) async fn prune_expired_segments_impl(
    store: &AgentTraceStore,
    project_id: &str,
    cutoff_date: NaiveDate,
) -> StoreResult<usize> {
    if store.closed.load(Ordering::Acquire) {
        return Err(StoreError::Shutdown);
    }
    validate_identifier("project_id", project_id)?;
    if store.config.read_only {
        return Ok(0);
    }
    let is_local_hot = store.is_local_hot_tier();
    if is_local_hot && !store.local_project_has_manifest_or_wal(project_id).await? {
        return Ok(0);
    }

    let state = store.get_or_open_project(project_id).await?;
    let _pub_guard = state.publish_lock.lock().await;
    let _l0_pruned = state.memtable.prune_before_date(cutoff_date);

    let now_ms = chrono::Utc::now().timestamp_millis();
    let grace_ms = store.gc_grace_ms();
    let cur_mf = state.manifest_store.load_latest().await?;
    let has_expired_seg = cur_mf
        .segments
        .iter()
        .any(|seg| NaiveDate::parse_from_str(&seg.date, "%Y-%m-%d").is_ok_and(|d| d < cutoff_date));
    let has_gc = !cur_mf.pending_gc.is_empty()
        || cur_mf
            .pending_s3_deletes
            .iter()
            .any(|e| e.eligible_after_ms.is_some_and(|t| now_ms >= t));
    if !has_expired_seg && !has_gc {
        store.drain_pending_local_deletes(&state).await;
        return Ok(0);
    }

    let mut pruned_count = 0usize;
    let mut expired_local_paths: Vec<String> = Vec::new();
    let mut expired_seg_ids: Vec<String> = Vec::new();

    let committed = state
        .manifest_store
        .commit_with_cas(|m| {
            expired_local_paths.clear();
            expired_seg_ids.clear();
            let mut expired = Vec::new();
            let mut kept = Vec::with_capacity(m.segments.len());
            for seg in m.segments.drain(..) {
                match NaiveDate::parse_from_str(&seg.date, "%Y-%m-%d") {
                    Ok(d) if d < cutoff_date => expired.push(seg),
                    _ => kept.push(seg),
                }
            }
            if expired.is_empty() {
                m.segments = kept;
                pruned_count = 0;
                return Ok(false);
            }
            pruned_count = expired.len();
            m.segments = kept;
            let next_pub_seq = m.last_published_seq.map_or(0, |s| s.saturating_add(1));
            let mut pending_gc_paths: HashSet<String> =
                m.pending_gc.iter().map(|p| p.object_path.clone()).collect();
            let mut pending_s3_paths: HashSet<String> = m
                .pending_s3_deletes
                .iter()
                .map(|p| p.object_path.clone())
                .collect();

            for seg in expired {
                expired_seg_ids.push(seg.segment_id.clone());
                if is_local_hot && seg.object_path.starts_with("hot/agent_segments/") {
                    expired_local_paths.push(seg.object_path);
                } else if is_local_hot {
                    if pending_s3_paths.insert(seg.object_path.clone()) {
                        m.pending_s3_deletes.push(PendingAgentS3Delete {
                            object_path: seg.object_path,
                            segment_id: seg.segment_id,
                            superseding_pub_seq: next_pub_seq,
                            superseded_at_ms: None,
                            eligible_after_ms: None,
                            urgent_erasure_deadline_ms: None,
                            erasure_tombstone_ids: Vec::new(),
                        });
                    }
                } else if pending_gc_paths.insert(seg.object_path.clone()) {
                    m.pending_gc.push(PendingGcEntry {
                        object_path: seg.object_path,
                        segment_id: seg.segment_id,
                        superseded_at_ms: now_ms,
                        eligible_after_ms: now_ms.saturating_add(grace_ms),
                        urgent_erasure_deadline_ms: None,
                        erasure_tombstone_ids: Vec::new(),
                    });
                }
            }
            m.updated_at_ms = now_ms;
            Ok(true)
        })
        .await?;

    for sid in &expired_seg_ids {
        store.cache().invalidate_segment(sid);
    }
    if !expired_local_paths.is_empty() {
        state
            .pending_local_deletes
            .lock()
            .extend(expired_local_paths);
    }
    store.drain_pending_local_deletes(&state).await;

    if !committed.pending_gc.is_empty() || !committed.pending_s3_deletes.is_empty() {
        let _ = state
            .manifest_store
            .run_gc(now_ms, store.pin_tracker())
            .await?;
    }

    Ok(pruned_count)
}
