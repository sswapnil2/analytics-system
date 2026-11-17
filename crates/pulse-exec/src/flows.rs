use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use pulse_identity::IdentitySnapshot;
use pulse_storage::{ProjectStorage, StorageSnapshot};
use roaring::RoaringBitmap;

use crate::ast::{FlowDirection, FlowLink, FlowNode, FlowsQuery, FlowsResult};
use crate::cache::SegmentBlockCache;
use crate::catalog::VirtualCatalog;
use crate::scanner::{
    load_project_chunks_with_id, resolve_event_target, resolve_global_cohort_bitmap,
    row_matches_predicates, ExecError, Result,
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

        let chunks = load_project_chunks_with_id(
            ps,
            snap,
            cache,
            catalog,
            Some(query.time_range),
            &props_needed,
            None,
            Some(id_snap),
        )
        .await?;

        // Map `event_id -> event_name` once and identify `$`-prefixed system events (`PR-10`)
        let id_to_name: HashMap<u16, String> = dict.all_events().into_iter().collect();
        let include_system_events = query.anchor_event.starts_with('$');
        let system_eids: HashSet<u16> = id_to_name
            .iter()
            .filter_map(|(&eid, name)| name.starts_with('$').then_some(eid))
            .collect();

        // Collect chronological `(ts_ms, event_id, is_anchor)` per canonical user with resource bounds (`P3-02`)
        let mut timelines: HashMap<u32, Vec<(i64, u16, bool)>> = HashMap::new();
        let mut total_collected = 0usize;

        for chunk in &chunks {
            let ta = &chunk.tier_a;
            for r in 0..chunk.len() {
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

        // First pass: extract each anchored user's step sequence `(step_offset, event_name)`
        // and count raw unique users per `(step_offset, event_name)` so `top_n_per_step` can rank them (`Section 6`, `P3-02`).
        let mut user_paths: Vec<(u32, Vec<(i32, String)>)> = Vec::new();
        let mut raw_step_counts: HashMap<i32, HashMap<String, u64>> = HashMap::new();

        for (uid, mut evs) in timelines {
            evs.sort_by_key(|(ts, eid, _)| (*ts, *eid));

            // Collapse consecutive identical events (`Screen_A -> Screen_A => Screen_A`)
            let mut collapsed: Vec<(String, bool)> = Vec::with_capacity(evs.len());
            for (_, eid, is_anchor) in evs {
                let name = if is_anchor {
                    query.anchor_event.clone()
                } else {
                    id_to_name
                        .get(&eid)
                        .cloned()
                        .unwrap_or_else(|| format!("event_{eid}"))
                };
                if let Some(last) = collapsed.last_mut() {
                    if last.0 == name {
                        last.1 |= is_anchor;
                        continue;
                    }
                }
                collapsed.push((name, is_anchor));
            }

            let Some(anchor_idx) = collapsed.iter().position(|(_, is_a)| *is_a) else {
                continue;
            };

            let mut path: Vec<(i32, String)> = Vec::with_capacity(steps + 1);
            path.push((0, query.anchor_event.clone()));

            match query.direction {
                FlowDirection::Forward => {
                    for offset in 1..=steps {
                        let pos = anchor_idx + offset;
                        let step_off = offset as i32;
                        if let Some((ev_name, _)) = collapsed.get(pos) {
                            *raw_step_counts
                                .entry(step_off)
                                .or_default()
                                .entry(ev_name.clone())
                                .or_insert(0) += 1;
                            path.push((step_off, ev_name.clone()));
                        } else {
                            path.push((step_off, "$Drop_Off".to_string()));
                            break;
                        }
                    }
                }
                FlowDirection::Reverse => {
                    for offset in 1..=steps {
                        let step_off = -(offset as i32);
                        if anchor_idx >= offset {
                            let ev_name = collapsed[anchor_idx - offset].0.clone();
                            *raw_step_counts
                                .entry(step_off)
                                .or_default()
                                .entry(ev_name.clone())
                                .or_insert(0) += 1;
                            path.push((step_off, ev_name));
                        } else {
                            path.push((step_off, "$Session_Start".to_string()));
                            break;
                        }
                    }
                }
            }

            user_paths.push((uid, path));
        }

        // Determine top-`top_n` event names at each non-zero `step_offset`
        let mut allowed_per_step: HashMap<i32, HashSet<String>> = HashMap::new();
        for (step_off, counts) in raw_step_counts {
            let mut ranked: Vec<(String, u64)> = counts.into_iter().collect();
            ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let keep: HashSet<String> = ranked
                .into_iter()
                .take(top_n)
                .map(|(name, _)| name)
                .collect();
            allowed_per_step.insert(step_off, keep);
        }

        let mut node_users: BTreeMap<(i32, String), RoaringBitmap> = BTreeMap::new();
        let mut link_users: BTreeMap<(String, String), RoaringBitmap> = BTreeMap::new();

        for (uid, path) in user_paths {
            let mut mapped_ids: Vec<String> = Vec::with_capacity(path.len());
            for (step_off, raw_name) in path {
                let final_name = if step_off == 0
                    || raw_name == "$Drop_Off"
                    || raw_name == "$Session_Start"
                    || allowed_per_step
                        .get(&step_off)
                        .is_some_and(|set| set.contains(&raw_name))
                {
                    raw_name
                } else {
                    "$Other".to_string()
                };
                let nid = format_node_id(step_off, &final_name);
                node_users
                    .entry((step_off, final_name))
                    .or_default()
                    .insert(uid);
                mapped_ids.push(nid);
            }

            match query.direction {
                FlowDirection::Forward => {
                    for pair in mapped_ids.windows(2) {
                        link_users
                            .entry((pair[0].clone(), pair[1].clone()))
                            .or_default()
                            .insert(uid);
                    }
                }
                FlowDirection::Reverse => {
                    for pair in mapped_ids.windows(2) {
                        // `path` runs from `0` -> `-1` -> `-2`; links point chronologically (`-1 -> 0`, `-2 -> -1`)
                        link_users
                            .entry((pair[1].clone(), pair[0].clone()))
                            .or_default()
                            .insert(uid);
                    }
                }
            }
        }

        let mut nodes: Vec<FlowNode> = node_users
            .into_iter()
            .map(|((step_offset, event_name), bm)| FlowNode {
                id: format_node_id(step_offset, &event_name),
                step_offset,
                event_name,
                user_count: bm.len(),
            })
            .collect();
        nodes.sort_by(|a, b| {
            a.step_offset
                .cmp(&b.step_offset)
                .then_with(|| b.user_count.cmp(&a.user_count))
                .then_with(|| a.event_name.cmp(&b.event_name))
        });

        let mut links: Vec<FlowLink> = link_users
            .into_iter()
            .map(|((source, target), bm)| FlowLink {
                source,
                target,
                user_count: bm.len(),
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
        })
    }
}

#[inline]
fn format_node_id(step_offset: i32, event_name: &str) -> String {
    format!("{step_offset}:{event_name}")
}
