use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use pulse_identity::IdentitySnapshot;
use pulse_storage::{ProjectStorage, StorageSnapshot};
use roaring::RoaringBitmap;

use crate::ast::{FlowDirection, FlowLink, FlowNode, FlowsQuery, FlowsResult};
use crate::cache::SegmentBlockCache;
use crate::catalog::VirtualCatalog;
use crate::scanner::{
    load_project_chunks, resolve_event_target, resolve_global_cohort_bitmap,
    row_matches_predicates, Result,
};

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
        let steps = query.steps.clamp(1, 10);

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

        let chunks = load_project_chunks(
            ps,
            snap,
            cache,
            catalog,
            Some(query.time_range),
            &props_needed,
            None,
        )
        .await?;

        // Map `event_id -> event_name` once
        let id_to_name: HashMap<u16, String> = dict.all_events().into_iter().collect();

        // Collect chronological `(ts_ms, event_id, is_anchor)` per canonical user
        let mut timelines: HashMap<u32, Vec<(i64, u16, bool)>> = HashMap::new();

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

                timelines
                    .entry(canon_uid)
                    .or_default()
                    .push((ts, eid, is_anchor));
            }
        }

        let mut node_users: BTreeMap<(i32, String), RoaringBitmap> = BTreeMap::new();
        let mut link_users: BTreeMap<(String, String), RoaringBitmap> = BTreeMap::new();

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

            let anchor_node_id = format_node_id(0, &query.anchor_event);
            node_users
                .entry((0, query.anchor_event.clone()))
                .or_default()
                .insert(uid);

            match query.direction {
                FlowDirection::Forward => {
                    let mut prev_id = anchor_node_id;
                    for offset in 1..=steps {
                        let pos = anchor_idx + offset;
                        let step_off = offset as i32;
                        if let Some((ev_name, _)) = collapsed.get(pos) {
                            let cur_id = format_node_id(step_off, ev_name);
                            node_users
                                .entry((step_off, ev_name.clone()))
                                .or_default()
                                .insert(uid);
                            link_users
                                .entry((prev_id, cur_id.clone()))
                                .or_default()
                                .insert(uid);
                            prev_id = cur_id;
                        } else {
                            let drop_name = "$Drop_Off".to_string();
                            let drop_id = format_node_id(step_off, &drop_name);
                            node_users
                                .entry((step_off, drop_name))
                                .or_default()
                                .insert(uid);
                            link_users
                                .entry((prev_id, drop_id))
                                .or_default()
                                .insert(uid);
                            break;
                        }
                    }
                }
                FlowDirection::Reverse => {
                    let mut next_id = anchor_node_id;
                    for offset in 1..=steps {
                        let step_off = -(offset as i32);
                        if anchor_idx >= offset {
                            let ev_name = &collapsed[anchor_idx - offset].0;
                            let cur_id = format_node_id(step_off, ev_name);
                            node_users
                                .entry((step_off, ev_name.clone()))
                                .or_default()
                                .insert(uid);
                            link_users
                                .entry((cur_id.clone(), next_id))
                                .or_default()
                                .insert(uid);
                            next_id = cur_id;
                        } else {
                            let entry_name = "$Session_Start".to_string();
                            let entry_id = format_node_id(step_off, &entry_name);
                            node_users
                                .entry((step_off, entry_name))
                                .or_default()
                                .insert(uid);
                            link_users
                                .entry((entry_id, next_id))
                                .or_default()
                                .insert(uid);
                            break;
                        }
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
