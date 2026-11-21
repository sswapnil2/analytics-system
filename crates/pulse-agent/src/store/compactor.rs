use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use chrono::NaiveDate;
use object_store::path::Path as ObjectPath;

use crate::model::{AgentErasureTombstone, AgentSpan, AgentTrace, ErasureLevel};
use crate::store::error::{validate_identifier, StoreError, StoreResult};
use crate::store::manifest::{PendingGcEntry, ReaderPinTracker, SegmentDescriptor};
use crate::store::memtable::{
    merge_spans_crdt_with_ranks, merge_trace_headers_with_meta,
    recompute_trace_from_spans_with_explicit, RankedTraceEntry, SpanFieldRanks, TraceCrdtMeta,
};
use crate::store::segment::SegmentWriter;
use crate::store::AgentTraceStore;

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

fn is_trace_erased(trace: &AgentTrace, active_tombstones: &[&AgentErasureTombstone]) -> bool {
    active_tombstones.iter().any(|tb| match tb.level {
        ErasureLevel::Trace => tb.target_id == trace.trace_id,
        ErasureLevel::Session => trace.session_id.as_deref() == Some(tb.target_id.as_str()),
        ErasureLevel::User => trace.user_id.as_deref() == Some(tb.target_id.as_str()),
    })
}

fn is_span_erased_by_trace_id(
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
        compact_project(store, &pid, tombstones).await?;
    }
    Ok(())
}

async fn compact_project(
    store: &AgentTraceStore,
    pid: &str,
    tombstones: &[AgentErasureTombstone],
) -> StoreResult<()> {
    let state = store.get_or_open_project(pid).await?;
    let now_ms = chrono::Utc::now().timestamp_millis();

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

    let mut erased_trace_ids: HashSet<String> = manifest.erased_traces.keys().cloned().collect();
    let mut newly_erased_ids: HashSet<String> = HashSet::new();

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
                if is_trace_erased(tr, &active_tombstones) {
                    erased_trace_ids.insert(tr.trace_id.clone());
                    if !manifest.erased_traces.contains_key(&tr.trace_id) {
                        newly_erased_ids.insert(tr.trace_id.clone());
                    }
                }
            }
        }
    }

    if manifest.segments.is_empty()
        && manifest.pending_gc.is_empty()
        && manifest.committed_wal_seq.is_empty()
        && manifest.erased_traces.is_empty()
        && newly_erased_ids.is_empty()
        && !state.memtable.has_purged_wal()
    {
        return Ok(());
    }

    let reader = store.plss_reader();
    let mut forced_ids: HashSet<String> = HashSet::new();
    let mut seg_tails = Vec::with_capacity(manifest.segments.len());

    if !active_tombstones.is_empty() || !erased_trace_ids.is_empty() {
        for seg in &manifest.segments {
            let tail = reader.read_eof_tail(seg).await?;
            let hit = active_tombstones.iter().any(|tb| match tb.level {
                ErasureLevel::Trace => tail.index.may_contain_trace(&tb.target_id),
                ErasureLevel::Session => tail.index.may_contain_session(&tb.target_id),
                ErasureLevel::User => tail.index.may_contain_user(&tb.target_id),
            }) || erased_trace_ids
                .iter()
                .any(|tid| tail.index.may_contain_trace(tid));
            if hit {
                forced_ids.insert(seg.segment_id.clone());
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
                    if erased_trace_ids.contains(tid) {
                        continue;
                    }
                    let sid = strings
                        .get_opt(row.session_id_str, "session_id")?
                        .filter(|s| !s.is_empty());
                    let uid = strings
                        .get_opt(row.user_id_str, "user_id")?
                        .filter(|s| !s.is_empty());
                    let matched = active_tombstones.iter().any(|tb| match tb.level {
                        ErasureLevel::Trace => tb.target_id == tid,
                        ErasureLevel::Session => sid == Some(tb.target_id.as_str()),
                        ErasureLevel::User => uid == Some(tb.target_id.as_str()),
                    });
                    if matched {
                        erased_trace_ids.insert(tid.to_string());
                        if !manifest.erased_traces.contains_key(tid) {
                            newly_erased_ids.insert(tid.to_string());
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

    // Group segments by (date, shard_id)
    let mut buckets: BTreeMap<(String, u16), Vec<&SegmentDescriptor>> = BTreeMap::new();
    for seg in &manifest.segments {
        buckets
            .entry((seg.date.clone(), seg.shard_id))
            .or_default()
            .push(seg);
    }

    let max_seg_bytes = store.max_segment_bytes() as u64;
    let max_batch_input_bytes = max_seg_bytes.saturating_mul(2).max(2048);

    struct CompactionPlan<'a> {
        date: String,
        shard_id: u16,
        target_level: u8,
        segs: Vec<&'a SegmentDescriptor>,
    }

    let mut plans: Vec<CompactionPlan<'_>> = Vec::new();
    for ((date, shard_id), mut segs) in buckets {
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
                target_level,
                segs: chosen,
            });
        }
    }

    let mut superseded_map: HashMap<String, &SegmentDescriptor> = HashMap::new();
    let mut new_segments: Vec<SegmentDescriptor> = Vec::new();

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
                            && is_trace_erased(&tr, &active_tombstones))
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
                                        && !is_span_erased_by_trace_id(sp, &active_tombstones)
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
                    || (!active_tombstones.is_empty() && is_trace_erased(&tr, &active_tombstones))
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
                let written = SegmentWriter::write_segments_split_ranked(
                    pid,
                    &plan.date,
                    plan.shard_id,
                    plan.target_level,
                    merged_entries,
                    store.max_segment_bytes(),
                    store.object_store(),
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
    let grace_ms = store.gc_grace_ms();
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

    let mut aborted_due_to_missing = false;
    let commit_res = state
        .manifest_store
        .commit_with_cas(|m| {
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
                let mut pending_paths: HashSet<String> =
                    m.pending_gc.iter().map(|p| p.object_path.clone()).collect();

                m.segments
                    .retain(|s| !superseded_map.contains_key(&s.segment_id));
                m.segments.extend(new_segments.clone());

                for seg in superseded_map.values() {
                    if pending_paths.insert(seg.object_path.clone()) {
                        m.pending_gc.push(PendingGcEntry {
                            object_path: seg.object_path.clone(),
                            segment_id: seg.segment_id.clone(),
                            superseded_at_ms: commit_ts_ms,
                            eligible_after_ms: commit_ts_ms.saturating_add(grace_ms),
                        });
                    }
                }
                changed = true;
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

            // Never advance `committed_wal_seq` or purge `.ptwl` files for foreign writers whose
            // live in-memory L0 this compactor does not own (`wid != store.writer_id_str`).
            let safe_wal = state.memtable.compute_safe_committed_wal_seq(
                &m.committed_wal_seq,
                &[],
                &store.writer_id_str,
            );
            if let Some(&self_safe_seq) = safe_wal.get(&store.writer_id_str) {
                let prev_self = m
                    .committed_wal_seq
                    .get(&store.writer_id_str)
                    .copied()
                    .unwrap_or(0);
                if self_safe_seq > prev_self {
                    m.committed_wal_seq
                        .insert(store.writer_id_str.clone(), self_safe_seq);
                    changed = true;
                }
            }

            if changed {
                m.updated_at_ms = commit_ts_ms;
            }
            Ok(changed)
        })
        .await;

    if aborted_due_to_missing || commit_res.is_err() {
        cleanup_orphan_segments(store, &new_segments).await;
    }
    let committed_manifest = commit_res?;

    // Step C: Execute pending GC respecting grace period & reader pins + clean old manifest epochs
    let gc_now_ms = chrono::Utc::now().timestamp_millis();
    state
        .manifest_store
        .run_gc(gc_now_ms, store.pin_tracker())
        .await?;
    state.manifest_store.gc_old_epochs(gc_now_ms).await?;

    // Step D: Purge WAL files owned by this writer_id safely committed in the manifest
    if let Some(&safe_seq) = committed_manifest
        .committed_wal_seq
        .get(&store.writer_id_str)
    {
        if safe_seq > 0 {
            let _ = state
                .wal_store
                .purge_up_to(&store.writer_id_str, safe_seq)
                .await?;
        }
    }
    state.memtable.clear_purged_wal_flag();

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

    let state = store.get_or_open_project(project_id).await?;
    let _l0_pruned = state.memtable.prune_before_date(cutoff_date);

    let now_ms = chrono::Utc::now().timestamp_millis();
    let grace_ms = store.gc_grace_ms();
    let mut pruned_count = 0usize;

    state
        .manifest_store
        .commit_with_cas(|m| {
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
            let mut pending_paths: HashSet<String> =
                m.pending_gc.iter().map(|p| p.object_path.clone()).collect();
            for seg in expired {
                if pending_paths.insert(seg.object_path.clone()) {
                    m.pending_gc.push(PendingGcEntry {
                        object_path: seg.object_path,
                        segment_id: seg.segment_id,
                        superseded_at_ms: now_ms,
                        eligible_after_ms: now_ms.saturating_add(grace_ms),
                    });
                }
            }
            m.updated_at_ms = now_ms;
            Ok(true)
        })
        .await?;

    let _ = state
        .manifest_store
        .run_gc(now_ms, store.pin_tracker())
        .await?;

    Ok(pruned_count)
}
