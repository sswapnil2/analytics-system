use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use pulse_identity::IdentitySnapshot;
use pulse_storage::{ProjectStorage, StorageSnapshot};
use rayon::prelude::*;
use roaring::RoaringBitmap;

use crate::ast::{FlowDirection, FlowLink, FlowNode, FlowsQuery, FlowsResult};
use crate::cache::SegmentBlockCache;
use crate::catalog::VirtualCatalog;
use crate::scanner::{
    load_project_chunks_sampled, resolve_event_target, resolve_global_cohort_bitmap,
    row_matches_predicates, ExecError, Result, SamplingPlan,
};

const MAX_FLOWS_SCANNED_EVENTS: usize = 2_000_000;
const MAX_FLOWS_EVENTS_PER_USER: usize = 4_096;

pub struct FlowsEngine;

impl FlowsEngine {
    pub async fn execute(
        query: &FlowsQuery,
        ps: &ProjectStorage,
        snap: &StorageSnapshot,
        id_snap: &IdentitySnapshot,
        cache: &SegmentBlockCache,
        catalog: &VirtualCatalog,
    ) -> Result<FlowsResult> {
        let t0 = Instant::now();
        if query.time_range.start_ts_ms > query.time_range.end_ts_ms {
            return Err(ExecError::InvalidQuery(format!(
                "invalid time_range: start_ts_ms ({}) > end_ts_ms ({})",
                query.time_range.start_ts_ms, query.time_range.end_ts_ms
            )));
        }

        let steps = query.steps.clamp(1, 10);
        let top_n = query.top_n_per_step.unwrap_or(25).clamp(1, 50);

        let eligible_cohort = resolve_global_cohort_bitmap(
            &query.global_filter,
            query.time_range,
            ps,
            snap,
            id_snap,
            cache,
            catalog,
        )
        .await?;

        let dict = ps.dictionary();
        let anchor_tgt = resolve_event_target(&query.anchor_event, dict, catalog);
        let mut props_needed: Vec<String> = query
            .global_filter
            .predicates
            .iter()
            .map(|p| p.prop_name.clone())
            .collect();
        props_needed.extend(anchor_tgt.required_props());

        let sampling_plan = SamplingPlan::resolve(query.sampling, snap, ps, query.time_range);
        let chunks = load_project_chunks_sampled(
            ps,
            snap,
            cache,
            catalog,
            Some(query.time_range),
            &props_needed,
            None,
            Some(id_snap),
            sampling_plan.bucket_limit(),
        )
        .await?;

        // Map `event_id -> event_name` once and identify `$`-prefixed system events
        let id_to_name: HashMap<u16, String> = dict.all_events().into_iter().collect();
        let include_system_events = query.anchor_event.starts_with('$');
        let system_eids: HashSet<u16> = id_to_name
            .iter()
            .filter_map(|(&eid, name)| name.starts_with('$').then_some(eid))
            .collect();

        // Parallelize per-chunk event filtering, then assemble user timelines in chunk order with resource bounds.
        let chunk_batches: Vec<Vec<(u32, i64, u16, bool)>> =
            crate::scanner::run_rayon_compute(|stop| {
                chunks
                    .par_iter()
                    .map(|chunk| {
                        let ta = &chunk.tier_a;
                        let mut local = Vec::new();
                        for r in 0..chunk.len() {
                            if !stop.live_every(r) {
                                break;
                            }
                            let ts = ta.event_ts_ms[r];
                            if !query.time_range.contains(ts) {
                                continue;
                            }
                            let canon_uid = id_snap.resolve(ta.uid_internal[r]);
                            if let Some(ec) = &eligible_cohort {
                                if !ec.contains(canon_uid) {
                                    continue;
                                }
                            }
                            if !row_matches_predicates(chunk, r, &query.global_filter.predicates) {
                                continue;
                            }

                            let eid = ta.event_id[r];
                            let is_anchor = anchor_tgt.matches_id(eid)
                                && anchor_tgt.extra_filter.as_ref().is_none_or(|ef| {
                                    row_matches_predicates(chunk, r, std::slice::from_ref(ef))
                                });
                            if !include_system_events && !is_anchor && system_eids.contains(&eid) {
                                continue;
                            }

                            local.push((canon_uid, ts, eid, is_anchor));
                        }
                        local
                    })
                    .collect()
            });

        crate::budget::check_current()?;
        let mut timelines: HashMap<u32, Vec<(i64, u16, bool)>> = HashMap::new();
        let mut total_collected = 0usize;
        for batch in chunk_batches {
            crate::budget::check_current()?;
            for (canon_uid, ts, eid, is_anchor) in batch {
                total_collected += 1;
                if total_collected > MAX_FLOWS_SCANNED_EVENTS {
                    return Err(ExecError::InvalidQuery(format!(
                        "flows query exceeded maximum scanned event budget ({MAX_FLOWS_SCANNED_EVENTS}); narrow time_range or add global_filter"
                    )));
                }

                let user_tl = timelines.entry(canon_uid).or_default();
                if user_tl.len() < MAX_FLOWS_EVENTS_PER_USER || is_anchor {
                    user_tl.push((ts, eid, is_anchor));
                }
            }
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        enum FlowToken {
            Anchor,
            EventId(u16),
            DropOff,
            SessionStart,
            Other,
        }

        let to_token = |eid: u16, is_anchor: bool| -> FlowToken {
            if is_anchor
                || id_to_name
                    .get(&eid)
                    .is_some_and(|n| n == &query.anchor_event)
            {
                FlowToken::Anchor
            } else {
                FlowToken::EventId(eid)
            }
        };

        let resolve_token = |tok: FlowToken| -> String {
            match tok {
                FlowToken::Anchor => query.anchor_event.clone(),
                FlowToken::EventId(eid) => id_to_name
                    .get(&eid)
                    .cloned()
                    .unwrap_or_else(|| format!("event_{eid}")),
                FlowToken::DropOff => "$Drop_Off".to_string(),
                FlowToken::SessionStart => "$Session_Start".to_string(),
                FlowToken::Other => "$Other".to_string(),
            }
        };

        // Parallel per-user chronological sort, zero-allocation consecutive-event collapsing, and path construction (`Section 6`, `P3-02`).
        let user_paths: Vec<(u32, Vec<(i32, FlowToken)>)> =
            crate::scanner::run_rayon_compute(|stop| {
                timelines
                    .into_par_iter()
                    .filter_map(|(uid, mut evs)| {
                        if !stop.live() {
                            return None;
                        }
                        evs.sort_by_key(|(ts, eid, _)| (*ts, *eid));

                        let mut collapsed: Vec<(FlowToken, bool)> = Vec::with_capacity(evs.len());
                        for (_, eid, is_anchor) in evs {
                            let tok = to_token(eid, is_anchor);
                            if let Some((last_tok, last_anchor)) = collapsed.last_mut() {
                                if *last_tok == tok {
                                    *last_anchor |= is_anchor;
                                    continue;
                                }
                            }
                            collapsed.push((tok, is_anchor));
                        }

                        let anchor_idx = collapsed.iter().position(|(_, is_a)| *is_a)?;
                        let mut path: Vec<(i32, FlowToken)> = Vec::with_capacity(steps + 1);
                        path.push((0, FlowToken::Anchor));

                        match query.direction {
                            FlowDirection::Forward => {
                                for offset in 1..=steps {
                                    let pos = anchor_idx + offset;
                                    let step_off = offset as i32;
                                    if let Some(&(tok, _)) = collapsed.get(pos) {
                                        path.push((step_off, tok));
                                    } else {
                                        path.push((step_off, FlowToken::DropOff));
                                        break;
                                    }
                                }
                            }
                            FlowDirection::Reverse => {
                                for offset in 1..=steps {
                                    let step_off = -(offset as i32);
                                    if anchor_idx >= offset {
                                        let (tok, _) = collapsed[anchor_idx - offset];
                                        path.push((step_off, tok));
                                    } else {
                                        path.push((step_off, FlowToken::SessionStart));
                                        break;
                                    }
                                }
                            }
                        }

                        Some((uid, path))
                    })
                    .collect()
            });

        let mut raw_step_counts: HashMap<i32, HashMap<FlowToken, u64>> = HashMap::new();
        for (_uid, path) in &user_paths {
            for &(step_off, tok) in path {
                if step_off != 0 && tok != FlowToken::DropOff && tok != FlowToken::SessionStart {
                    *raw_step_counts
                        .entry(step_off)
                        .or_default()
                        .entry(tok)
                        .or_insert(0) += 1;
                }
            }
        }

        // Determine top-`top_n` event names at each non-zero `step_offset`
        let mut allowed_per_step: HashMap<i32, HashSet<FlowToken>> = HashMap::new();
        for (step_off, counts) in raw_step_counts {
            let mut ranked: Vec<(FlowToken, u64)> = counts.into_iter().collect();
            ranked.sort_by(|a, b| {
                b.1.cmp(&a.1)
                    .then_with(|| resolve_token(a.0).cmp(&resolve_token(b.0)))
            });
            let keep: HashSet<FlowToken> =
                ranked.into_iter().take(top_n).map(|(tok, _)| tok).collect();
            allowed_per_step.insert(step_off, keep);
        }

        type FlowNodeKey = (i32, FlowToken);
        let mut node_users: HashMap<FlowNodeKey, RoaringBitmap> = HashMap::new();
        let mut link_users: HashMap<(FlowNodeKey, FlowNodeKey), RoaringBitmap> = HashMap::new();
        let mut mapped_nodes: Vec<FlowNodeKey> = Vec::with_capacity(steps + 1);

        for (uid, path) in user_paths {
            mapped_nodes.clear();
            for (step_off, raw_tok) in path {
                let final_tok = if step_off == 0
                    || raw_tok == FlowToken::DropOff
                    || raw_tok == FlowToken::SessionStart
                    || allowed_per_step
                        .get(&step_off)
                        .is_some_and(|set| set.contains(&raw_tok))
                {
                    raw_tok
                } else {
                    FlowToken::Other
                };
                node_users
                    .entry((step_off, final_tok))
                    .or_default()
                    .insert(uid);
                mapped_nodes.push((step_off, final_tok));
            }

            match query.direction {
                FlowDirection::Forward => {
                    for pair in mapped_nodes.windows(2) {
                        link_users
                            .entry((pair[0], pair[1]))
                            .or_default()
                            .insert(uid);
                    }
                }
                FlowDirection::Reverse => {
                    for pair in mapped_nodes.windows(2) {
                        // `path` runs from `0` -> `-1` -> `-2`; links point chronologically (`-1 -> 0`, `-2 -> -1`)
                        link_users
                            .entry((pair[1], pair[0]))
                            .or_default()
                            .insert(uid);
                    }
                }
            }
        }

        let mut merged_nodes: BTreeMap<(i32, String), RoaringBitmap> = BTreeMap::new();
        for ((step_offset, tok), bm) in node_users {
            *merged_nodes
                .entry((step_offset, resolve_token(tok)))
                .or_default() |= bm;
        }

        let mut merged_links: BTreeMap<(String, String), RoaringBitmap> = BTreeMap::new();
        for (((src_step, src_tok), (dst_step, dst_tok)), bm) in link_users {
            let src_id = format_node_id(src_step, &resolve_token(src_tok));
            let dst_id = format_node_id(dst_step, &resolve_token(dst_tok));
            *merged_links.entry((src_id, dst_id)).or_default() |= bm;
        }

        let mut sample_basis = 0u64;
        let mut nodes: Vec<FlowNode> = merged_nodes
            .into_iter()
            .map(|((step_offset, event_name), bm)| {
                let raw = bm.len();
                if step_offset == 0 {
                    sample_basis = sample_basis.saturating_add(raw);
                }
                FlowNode {
                    id: format_node_id(step_offset, &event_name),
                    step_offset,
                    event_name,
                    user_count: sampling_plan.scale_u64(raw),
                }
            })
            .collect();
        nodes.sort_by(|a, b| {
            a.step_offset
                .cmp(&b.step_offset)
                .then_with(|| b.user_count.cmp(&a.user_count))
                .then_with(|| a.event_name.cmp(&b.event_name))
        });

        let mut links: Vec<FlowLink> = merged_links
            .into_iter()
            .map(|((source, target), bm)| FlowLink {
                source,
                target,
                user_count: sampling_plan.scale_u64(bm.len()),
            })
            .collect();
        links.sort_by(|a, b| {
            b.user_count
                .cmp(&a.user_count)
                .then_with(|| a.source.cmp(&b.source))
                .then_with(|| a.target.cmp(&b.target))
        });

        Ok(FlowsResult {
            snapshot_epoch: snap.epoch(),
            exec_time_us: t0.elapsed().as_micros() as u64,
            nodes,
            links,
            sampling: Some(sampling_plan.to_info(sample_basis)),
        })
    }
}

#[inline]
fn format_node_id(step_offset: i32, event_name: &str) -> String {
    format!("{step_offset}:{event_name}")
}
