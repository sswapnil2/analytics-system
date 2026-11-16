pub mod ast;
pub mod cache;
pub mod catalog;
pub mod engine;
pub mod flows;
pub mod funnel;
pub mod insights;
pub mod retention;
pub mod scanner;

pub use ast::*;
pub use cache::{SegmentBlockCache, TierAColumns};
pub use catalog::{
    arrow_array_to_prop_values, evaluate_custom_property, extract_sql_identifiers, CatalogRegistry,
    CustomEventDef, CustomPropertyDef, LookupTableDef, VirtualCatalog,
};
pub use engine::QueryEngine;
pub use flows::FlowsEngine;
pub use funnel::FunnelEngine;
pub use insights::{eval_math_expr, InsightsEngine};
pub use retention::RetentionEngine;
pub use scanner::{ExecError, Result};

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use pulse_identity::IdentityRouter;
    use pulse_ingest::{IdentifyRequest, InMemoryGovernance, IngestService};
    use pulse_storage::{StorageConfig, StorageEngine};
    use pulse_wire::{PropValue, WireBatch, WireEvent};

    use super::*;

    /// Independent brute-force reference oracle operating directly on raw `(WireEvent, alias_map)` history.
    struct BruteForceOracle {
        events: Vec<WireEvent>,
        aliases: HashMap<String, String>,
    }

    impl BruteForceOracle {
        fn canonical_id<'a>(&'a self, mut id: &'a str) -> &'a str {
            while let Some(next) = self.aliases.get(id) {
                if next == id {
                    break;
                }
                id = next.as_str();
            }
            id
        }

        fn user_timelines(&self) -> HashMap<String, Vec<WireEvent>> {
            let mut by_user: HashMap<String, Vec<WireEvent>> = HashMap::new();
            for ev in &self.events {
                let cid = self.canonical_id(&ev.distinct_id).to_string();
                by_user.entry(cid).or_default().push(ev.clone());
            }
            for list in by_user.values_mut() {
                list.sort_by_key(|e| e.client_event_ts_ms);
            }
            by_user
        }

        fn eval_funnel(&self, query: &FunnelQuery) -> Vec<u64> {
            let k = query.steps.len();
            let window_ms = query.conversion_window_ms;
            let timelines = self.user_timelines();
            let mut step_counts = vec![0u64; k];

            for evs in timelines.values() {
                // Apply `first_time_ever` per step on full user history
                let mut first_idx_for_step: Vec<Option<usize>> = vec![None; k];
                for (s_idx, step) in query.steps.iter().enumerate() {
                    if step.first_time_ever {
                        first_idx_for_step[s_idx] = evs.iter().position(|e| {
                            e.event_name == step.event_name
                                && step
                                    .filters
                                    .iter()
                                    .all(|f| f.matches(e.get_prop(&f.prop_name)))
                        });
                    }
                }

                let matches_step = |ev_idx: usize, s_idx: usize| -> bool {
                    let ev = &evs[ev_idx];
                    let spec = &query.steps[s_idx];
                    if ev.event_name != spec.event_name {
                        return false;
                    }
                    if !spec
                        .filters
                        .iter()
                        .all(|f| f.matches(ev.get_prop(&f.prop_name)))
                    {
                        return false;
                    }
                    if spec.first_time_ever && first_idx_for_step[s_idx] != Some(ev_idx) {
                        return false;
                    }
                    true
                };

                let hold_tuple = |ev: &WireEvent| -> Option<Vec<String>> {
                    if query.hold_constant_props.is_empty() {
                        return Some(Vec::new());
                    }
                    let mut out = Vec::with_capacity(query.hold_constant_props.len());
                    for hp in &query.hold_constant_props {
                        match ev.get_prop(hp) {
                            None | Some(PropValue::Null) => return None,
                            Some(pv) => out.push(pv.as_str_cow().into_owned()),
                        }
                    }
                    Some(out)
                };

                let mut max_reached: Option<usize> = None;

                match query.order_mode {
                    FunnelOrderMode::LooseOrder | FunnelOrderMode::StrictOrder => {
                        let strict = query.order_mode == FunnelOrderMode::StrictOrder;
                        for start_i in 0..evs.len() {
                            if !query.time_range.contains(evs[start_i].client_event_ts_ms)
                                || !matches_step(start_i, 0)
                            {
                                continue;
                            }
                            let Some(h0) = hold_tuple(&evs[start_i]) else {
                                continue;
                            };
                            let mut cur = 0usize;
                            for j in (start_i + 1)..evs.len() {
                                if evs[j].client_event_ts_ms - evs[start_i].client_event_ts_ms
                                    > window_ms
                                {
                                    break;
                                }
                                let excluded = query.exclusions.iter().any(|ex| {
                                    ex.between_steps.0 <= cur
                                        && cur < ex.between_steps.1
                                        && evs[j].event_name == ex.event_name
                                });
                                if excluded {
                                    break;
                                }
                                if cur + 1 < k
                                    && matches_step(j, cur + 1)
                                    && hold_tuple(&evs[j]).as_ref() == Some(&h0)
                                {
                                    cur += 1;
                                    if cur == k - 1 {
                                        break;
                                    }
                                } else if strict {
                                    break;
                                }
                            }
                            max_reached = Some(max_reached.map_or(cur, |prev| prev.max(cur)));
                        }
                    }
                    FunnelOrderMode::AnyOrder => {
                        for start_i in 0..evs.len() {
                            if !query.time_range.contains(evs[start_i].client_event_ts_ms) {
                                continue;
                            }
                            let init_steps: HashSet<usize> =
                                (0..k).filter(|&s| matches_step(start_i, s)).collect();
                            if init_steps.is_empty() {
                                continue;
                            }
                            let Some(h0) = hold_tuple(&evs[start_i]) else {
                                continue;
                            };
                            let mut matched = init_steps;
                            for j in (start_i + 1)..evs.len() {
                                if evs[j].client_event_ts_ms - evs[start_i].client_event_ts_ms
                                    > window_ms
                                {
                                    break;
                                }
                                let excluded = query.exclusions.iter().any(|ex| {
                                    matched.contains(&ex.between_steps.0)
                                        && !matched.contains(&ex.between_steps.1)
                                        && evs[j].event_name == ex.event_name
                                });
                                if excluded {
                                    break;
                                }
                                if hold_tuple(&evs[j]).as_ref() == Some(&h0) {
                                    for s in 0..k {
                                        if matches_step(j, s) {
                                            matched.insert(s);
                                        }
                                    }
                                    if matched.len() == k {
                                        break;
                                    }
                                }
                            }
                            let reached = matched.len() - 1;
                            max_reached =
                                Some(max_reached.map_or(reached, |prev| prev.max(reached)));
                        }
                    }
                }

                if let Some(mr) = max_reached {
                    for slot in step_counts.iter_mut().take(mr + 1) {
                        *slot += 1;
                    }
                }
            }

            step_counts
        }

        fn eval_retention(&self, query: &RetentionQuery) -> Vec<(i64, Vec<u64>)> {
            let step_ms = query.unit.step_ms();
            let timelines = self.user_timelines();
            let mut cohorts: BTreeMap<i64, Vec<&Vec<WireEvent>>> = BTreeMap::new();

            for evs in timelines.values() {
                if let Some(first_start) = evs
                    .iter()
                    .filter(|e| {
                        query.time_range.contains(e.client_event_ts_ms)
                            && e.event_name == query.start_event
                    })
                    .map(|e| e.client_event_ts_ms)
                    .min()
                {
                    let c_ts = query.unit.align_floor(first_start);
                    cohorts.entry(c_ts).or_default().push(evs);
                }
            }

            let mut out = Vec::new();
            for (c_ts, users) in cohorts {
                let mut counts = vec![0u64; query.num_periods + 1];
                match query.mode {
                    RetentionMode::NDay => {
                        counts[0] = users.len() as u64;
                        for (p, slot) in counts
                            .iter_mut()
                            .enumerate()
                            .take(query.num_periods + 1)
                            .skip(1)
                        {
                            let target_bucket = c_ts + (p as i64) * step_ms;
                            *slot = users
                                .iter()
                                .filter(|evs| {
                                    evs.iter().any(|e| {
                                        e.event_name == query.return_event
                                            && query.unit.align_floor(e.client_event_ts_ms)
                                                == target_bucket
                                    })
                                })
                                .count() as u64;
                        }
                    }
                    RetentionMode::Unbounded => {
                        counts[0] = users.len() as u64;
                        for (p, slot) in counts
                            .iter_mut()
                            .enumerate()
                            .take(query.num_periods + 1)
                            .skip(1)
                        {
                            let min_bucket = c_ts + (p as i64) * step_ms;
                            let max_bucket = c_ts + (query.num_periods as i64) * step_ms;
                            *slot = users
                                .iter()
                                .filter(|evs| {
                                    evs.iter().any(|e| {
                                        if e.event_name != query.return_event {
                                            return false;
                                        }
                                        let b = query.unit.align_floor(e.client_event_ts_ms);
                                        b >= min_bucket && b <= max_bucket
                                    })
                                })
                                .count() as u64;
                        }
                    }
                    RetentionMode::FrequencyStickiness => {
                        for evs in &users {
                            let mut active_buckets = HashSet::new();
                            for e in *evs {
                                if e.event_name == query.return_event {
                                    let b = query.unit.align_floor(e.client_event_ts_ms);
                                    let max_b = c_ts + (query.num_periods as i64) * step_ms;
                                    if b >= c_ts && b <= max_b {
                                        active_buckets.insert(b);
                                    }
                                }
                            }
                            for (idx, slot) in counts.iter_mut().enumerate() {
                                if active_buckets.len() > idx {
                                    *slot += 1;
                                }
                            }
                        }
                    }
                }
                out.push((c_ts, counts));
            }
            out
        }

        fn eval_flows(&self, query: &FlowsQuery) -> (Vec<FlowNode>, Vec<FlowLink>) {
            let timelines = self.user_timelines();
            let mut node_sets: BTreeMap<(i32, String), HashSet<String>> = BTreeMap::new();
            let mut link_sets: BTreeMap<(String, String), HashSet<String>> = BTreeMap::new();

            for (uid, evs) in timelines {
                let mut collapsed: Vec<String> = Vec::new();
                for e in evs {
                    if !query.time_range.contains(e.client_event_ts_ms) {
                        continue;
                    }
                    if collapsed.last() != Some(&e.event_name) {
                        collapsed.push(e.event_name.clone());
                    }
                }
                let Some(anchor_pos) = collapsed.iter().position(|n| n == &query.anchor_event)
                else {
                    continue;
                };

                node_sets
                    .entry((0, query.anchor_event.clone()))
                    .or_default()
                    .insert(uid.clone());
                let anchor_id = format!("0:{}", query.anchor_event);

                match query.direction {
                    FlowDirection::Forward => {
                        let mut prev = anchor_id;
                        for off in 1..=query.steps {
                            let name = collapsed
                                .get(anchor_pos + off)
                                .cloned()
                                .unwrap_or_else(|| "$Drop_Off".to_string());
                            let is_drop = name == "$Drop_Off";
                            let cur_id = format!("{off}:{name}");
                            node_sets
                                .entry((off as i32, name))
                                .or_default()
                                .insert(uid.clone());
                            link_sets
                                .entry((prev, cur_id.clone()))
                                .or_default()
                                .insert(uid.clone());
                            prev = cur_id;
                            if is_drop {
                                break;
                            }
                        }
                    }
                    FlowDirection::Reverse => {
                        let mut next = anchor_id;
                        for off in 1..=query.steps {
                            let step_i = -(off as i32);
                            let (name, is_start) = if anchor_pos >= off {
                                (collapsed[anchor_pos - off].clone(), false)
                            } else {
                                ("$Session_Start".to_string(), true)
                            };
                            let cur_id = format!("{step_i}:{name}");
                            node_sets
                                .entry((step_i, name))
                                .or_default()
                                .insert(uid.clone());
                            link_sets
                                .entry((cur_id.clone(), next))
                                .or_default()
                                .insert(uid.clone());
                            next = cur_id;
                            if is_start {
                                break;
                            }
                        }
                    }
                }
            }

            let mut nodes: Vec<FlowNode> = node_sets
                .into_iter()
                .map(|((step_offset, event_name), set)| FlowNode {
                    id: format!("{step_offset}:{event_name}"),
                    step_offset,
                    event_name,
                    user_count: set.len() as u64,
                })
                .collect();
            nodes.sort_by(|a, b| {
                a.step_offset
                    .cmp(&b.step_offset)
                    .then_with(|| b.user_count.cmp(&a.user_count))
                    .then_with(|| a.event_name.cmp(&b.event_name))
            });

            let mut links: Vec<FlowLink> = link_sets
                .into_iter()
                .map(|((source, target), set)| FlowLink {
                    source,
                    target,
                    user_count: set.len() as u64,
                })
                .collect();
            links.sort_by(|a, b| {
                b.user_count
                    .cmp(&a.user_count)
                    .then_with(|| a.source.cmp(&b.source))
                    .then_with(|| a.target.cmp(&b.target))
            });

            (nodes, links)
        }
    }

    fn make_ev(
        seq: u64,
        routing_seed: &str,
        distinct_id: &str,
        session_id: &str,
        event_name: &str,
        ts_ms: i64,
        props: Vec<(&str, PropValue)>,
    ) -> WireEvent {
        WireEvent {
            event_uuid: WireEvent::deterministic_uuid(
                distinct_id,
                session_id,
                event_name,
                ts_ms,
                seq,
            ),
            routing_seed: routing_seed.to_string(),
            distinct_id: distinct_id.to_string(),
            session_id: session_id.to_string(),
            event_name: event_name.to_string(),
            client_event_ts_ms: ts_ms,
            client_sent_ts_ms: ts_ms + 50,
            props: props.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            replay_offset_kb: (seq % 64) as u32,
        }
    }

    #[tokio::test]
    async fn gate3_differential_correctness_against_bruteforce_oracle() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).expect("local fs"));
        let id_router = Arc::new(IdentityRouter::new(store.clone()));

        let cfg = StorageConfig {
            num_buckets: 16,
            l0_flush_row_threshold: 50_000,
            l0_flush_max_age: Duration::from_secs(600),
            compaction_min_segments: 4,
        };
        let storage = StorageEngine::new(cfg, store, id_router.clone());
        let gov = InMemoryGovernance::new();
        gov.register_api_key("pk_diff", "proj_diff");
        let ingest = IngestService::new(storage.clone(), gov);

        let catalogs = CatalogRegistry::new();
        let qengine = QueryEngine::new(
            storage.clone(),
            32 * 1024 * 1024,
            Some(&tmp_dir.path().join("foyer_cache")),
            64 * 1024 * 1024,
            catalogs.clone(),
        )
        .await
        .expect("open QueryEngine");

        let day0 = TimeInterval::Day.align_floor(1_762_000_000_000);
        let server_now = day0 + 5 * 86_400_000;
        let mut raw_events: Vec<WireEvent> = Vec::new();
        let mut aliases: HashMap<String, String> = HashMap::new();
        let mut seq = 1u64;

        // 1. Tricky Case A: Anonymous-to-login mid-funnel (same-bucket routing_seed)
        for u in 0..20 {
            let seed = format!("seed_same_{u}");
            let anon = format!("anon_same_{u}");
            let user = format!("user_same_{u}");
            let sess = format!("sess_same_{u}");
            let t = day0 + 10_000 + u * 1_000;

            raw_events.push(make_ev(
                seq,
                &seed,
                &anon,
                &sess,
                "Step_0",
                t,
                vec![
                    ("payment_method", PropValue::Str("upi".into())),
                    ("amount", PropValue::Int(1500)),
                    ("merchant_id", PropValue::Str("m_swiggy".into())),
                ],
            ));
            seq += 1;
            raw_events.push(make_ev(
                seq,
                &seed,
                &anon,
                &sess,
                "Step_1",
                t + 5_000,
                vec![("payment_method", PropValue::Str("upi".into()))],
            ));
            seq += 1;
            // Login happens mid-funnel!
            aliases.insert(anon.clone(), user.clone());
            ingest
                .identify_for_project(
                    "proj_diff",
                    IdentifyRequest {
                        anon_id: anon,
                        user_id: user.clone(),
                        routing_seed: Some(seed.clone()),
                        anon_routing_seed: Some(seed.clone()),
                    },
                )
                .await
                .unwrap();

            raw_events.push(make_ev(
                seq,
                &seed,
                &user,
                &sess,
                "Step_2",
                t + 12_000,
                vec![("payment_method", PropValue::Str("upi".into()))],
            ));
            seq += 1;
            raw_events.push(make_ev(
                seq,
                &seed,
                &user,
                &sess,
                "Step_3",
                t + 20_000,
                vec![("payment_method", PropValue::Str("upi".into()))],
            ));
            seq += 1;
        }

        // 2. Tricky Case B: Cross-bucket second device!
        //    Steps 0 & 1 happen on iPhone (`seed_ios_*`), Steps 2 & 3 happen on Web (`seed_web_*` in a DIFFERENT bucket!)
        for u in 0..15 {
            let seed_a = format!("seed_ios_device_{u}_alpha");
            let seed_b = format!("seed_web_laptop_{u}_omega");
            let anon_a = format!("anon_ios_{u}");
            let user = format!("user_cross_{u}");
            let t = day0 + 50_000 + u * 1_000;

            raw_events.push(make_ev(
                seq,
                &seed_a,
                &anon_a,
                &format!("s_ios_{u}"),
                "Step_0",
                t,
                vec![("payment_method", PropValue::Str("upi".into()))],
            ));
            seq += 1;
            raw_events.push(make_ev(
                seq,
                &seed_a,
                &anon_a,
                &format!("s_ios_{u}"),
                "Step_1",
                t + 4_000,
                vec![("payment_method", PropValue::Str("upi".into()))],
            ));
            seq += 1;

            aliases.insert(anon_a.clone(), user.clone());
            ingest
                .identify_for_project(
                    "proj_diff",
                    IdentifyRequest {
                        anon_id: anon_a,
                        user_id: user.clone(),
                        routing_seed: Some(seed_b.clone()),
                        anon_routing_seed: Some(seed_a.clone()),
                    },
                )
                .await
                .unwrap();

            raw_events.push(make_ev(
                seq,
                &seed_b,
                &user,
                &format!("s_web_{u}"),
                "Step_2",
                t + 15_000,
                vec![("payment_method", PropValue::Str("upi".into()))],
            ));
            seq += 1;
            raw_events.push(make_ev(
                seq,
                &seed_b,
                &user,
                &format!("s_web_{u}"),
                "Step_3",
                t + 25_000,
                vec![("payment_method", PropValue::Str("upi".into()))],
            ));
            seq += 1;
        }

        // 3. Tricky Case C: Excluded error event (`NSDL_Gateway_504_Timeout`) between Step 1 & Step 2!
        for u in 0..12 {
            let uid = format!("user_excl_{u}");
            let t = day0 + 100_000 + u * 1_000;
            for (dt, ename) in [
                (0, "Step_0"),
                (2_000, "Step_1"),
                (4_000, "NSDL_Gateway_504_Timeout"),
                (6_000, "Step_2"),
                (8_000, "Step_3"),
            ] {
                raw_events.push(make_ev(
                    seq,
                    &uid,
                    &uid,
                    &format!("s_ex_{u}"),
                    ename,
                    t + dt,
                    vec![("payment_method", PropValue::Str("upi".into()))],
                ));
                seq += 1;
            }
        }

        // 4. Tricky Case D: Held `payment_method` changing mid-funnel (`upi` -> `card`)!
        for u in 0..18 {
            let uid = format!("user_hold_switch_{u}");
            let t = day0 + 200_000 + u * 1_000;
            for (dt, ename, pm) in [
                (0, "Step_0", "upi"),
                (3_000, "Step_1", "upi"),
                (6_000, "Step_2", "card"), // switched mid-funnel!
                (9_000, "Step_3", "card"),
            ] {
                raw_events.push(make_ev(
                    seq,
                    &uid,
                    &uid,
                    &format!("s_hold_{u}"),
                    ename,
                    t + dt,
                    vec![
                        ("payment_method", PropValue::Str(pm.into())),
                        ("amount", PropValue::Int(500)),
                        ("merchant_id", PropValue::Str("m_zepto".into())),
                    ],
                ));
                seq += 1;
            }
        }

        // 5. Tricky Case E: Intervening noise events (passes LooseOrder, fails StrictOrder) + consecutive duplicate events for Flows + multi-day returns for Retention!
        for u in 0..25 {
            let uid = format!("user_mixed_{u}");
            let t = day0 + 300_000 + u * 1_000;
            // Consecutive duplicate `Step_0 -> Step_0` + intervening `Browse_Help`
            for (dt, ename) in [
                (0, "Step_0"),
                (500, "Step_0"),
                (1_500, "Browse_Help"),
                (3_000, "Step_1"),
                (4_500, "Step_2"),
            ] {
                raw_events.push(make_ev(
                    seq,
                    &uid,
                    &uid,
                    &format!("s_mix_{u}"),
                    ename,
                    t + dt,
                    vec![("payment_method", PropValue::Str("card".into()))],
                ));
                seq += 1;
            }
            // Multi-day Retention return events across Day +1, +2, +4
            for &ret_day in &[1i64, 2, 4] {
                if (u + ret_day) % 2 == 0 {
                    raw_events.push(make_ev(
                        seq,
                        &uid,
                        &uid,
                        &format!("s_ret_{u}_{ret_day}"),
                        "Step_1",
                        day0 + ret_day * 86_400_000 + 3_600_000,
                        vec![("payment_method", PropValue::Str("card".into()))],
                    ));
                    seq += 1;
                }
            }
        }

        for ev in &mut raw_events {
            ev.client_sent_ts_ms = server_now;
        }

        // Flush first 70% of events into Vortex L1 segments and leave remaining 30% hot in L0 MemBucket
        // so queries exercise unified MVCC L1 + L0 execution simultaneously!
        let split_at = (raw_events.len() * 7) / 10;
        ingest
            .ingest_for_project(
                "proj_diff",
                WireBatch::new(raw_events[..split_at].to_vec()),
                server_now,
            )
            .await
            .unwrap();
        storage.flush_all_projects().await.unwrap();

        ingest
            .ingest_for_project(
                "proj_diff",
                WireBatch::new(raw_events[split_at..].to_vec()),
                server_now,
            )
            .await
            .unwrap();

        let oracle = BruteForceOracle {
            events: raw_events,
            aliases,
        };

        let full_window = TimeRange {
            start_ts_ms: day0,
            end_ts_ms: day0 + 86_400_000 - 1,
        };
        let steps_4: Vec<FunnelStepSpec> = ["Step_0", "Step_1", "Step_2", "Step_3"]
            .into_iter()
            .map(|name| FunnelStepSpec {
                event_name: name.to_string(),
                filters: Vec::new(),
                first_time_ever: false,
            })
            .collect();

        // --- Differential Funnel Check 1: LooseOrder + Exclusion(1,2) + HoldConstant(payment_method) ---
        let f_query_1 = FunnelQuery {
            time_range: full_window,
            steps: steps_4.clone(),
            order_mode: FunnelOrderMode::LooseOrder,
            conversion_window_ms: 3_600_000,
            exclusions: vec![FunnelExclusion {
                event_name: "NSDL_Gateway_504_Timeout".to_string(),
                between_steps: (1, 2),
            }],
            hold_constant_props: vec!["payment_method".to_string()],
            breakdown_by: Some("payment_method".to_string()),
            global_filter: GlobalFilter::default(),
        };
        let f_res_1 = qengine
            .query_funnel("proj_diff", &f_query_1)
            .await
            .expect("funnel 1");
        let actual_1: Vec<u64> = f_res_1
            .overall_steps
            .iter()
            .map(|s| s.entered_users)
            .collect();
        let expected_1 = oracle.eval_funnel(&f_query_1);
        assert_eq!(
            actual_1, expected_1,
            "LooseOrder + Exclusion + HoldConstant + CrossBucket mismatch vs oracle"
        );
        assert!(!f_res_1.dropped_sample_sessions.is_empty());

        // --- Differential Funnel Check 2: StrictOrder ---
        let f_query_strict = FunnelQuery {
            order_mode: FunnelOrderMode::StrictOrder,
            ..f_query_1.clone()
        };
        let f_res_strict = qengine
            .query_funnel("proj_diff", &f_query_strict)
            .await
            .expect("funnel strict");
        let actual_strict: Vec<u64> = f_res_strict
            .overall_steps
            .iter()
            .map(|s| s.entered_users)
            .collect();
        assert_eq!(actual_strict, oracle.eval_funnel(&f_query_strict));

        // --- Differential Funnel Check 3: AnyOrder ---
        let f_query_any = FunnelQuery {
            order_mode: FunnelOrderMode::AnyOrder,
            ..f_query_1.clone()
        };
        let f_res_any = qengine
            .query_funnel("proj_diff", &f_query_any)
            .await
            .expect("funnel any");
        let actual_any: Vec<u64> = f_res_any
            .overall_steps
            .iter()
            .map(|s| s.entered_users)
            .collect();
        assert_eq!(actual_any, oracle.eval_funnel(&f_query_any));

        // --- Differential Retention Checks: NDay, Unbounded, FrequencyStickiness ---
        for mode in [
            RetentionMode::NDay,
            RetentionMode::Unbounded,
            RetentionMode::FrequencyStickiness,
        ] {
            let r_query = RetentionQuery {
                time_range: full_window,
                start_event: "Step_0".to_string(),
                return_event: "Step_1".to_string(),
                mode,
                unit: TimeInterval::Day,
                num_periods: 5,
                global_filter: GlobalFilter::default(),
            };
            let r_res = qengine
                .query_retention("proj_diff", &r_query)
                .await
                .expect("retention");
            let actual_ret: Vec<(i64, Vec<u64>)> = r_res
                .cohorts
                .into_iter()
                .map(|c| (c.cohort_start_ts_ms, c.retained_counts))
                .collect();
            let expected_ret = oracle.eval_retention(&r_query);
            assert_eq!(
                actual_ret, expected_ret,
                "Retention mode {mode:?} mismatch vs oracle"
            );
        }

        // --- Differential Flows Checks: Forward & Reverse (with consecutive collapse!) ---
        for direction in [FlowDirection::Forward, FlowDirection::Reverse] {
            let fl_query = FlowsQuery {
                time_range: full_window,
                anchor_event: "Step_1".to_string(),
                direction,
                steps: 3,
                top_n_per_step: None,
                global_filter: GlobalFilter::default(),
            };
            let fl_res = qengine
                .query_flows("proj_diff", &fl_query)
                .await
                .expect("flows");
            let (exp_nodes, exp_links) = oracle.eval_flows(&fl_query);
            assert_eq!(
                fl_res.nodes, exp_nodes,
                "Flows {direction:?} nodes mismatch vs oracle"
            );
            assert_eq!(
                fl_res.links, exp_links,
                "Flows {direction:?} links mismatch vs oracle"
            );
        }

        // --- Verify DataFusion Custom Properties, Custom Events & CSV Lookup Table Joins in InsightsEngine ---
        let cat = catalogs.for_project("proj_diff");
        cat.custom_properties.insert(
            "value_tier".to_string(),
            CustomPropertyDef {
                name: "value_tier".to_string(),
                sql_expr: "CASE WHEN amount > 1000 THEN 'High' ELSE 'Standard' END".to_string(),
                referenced_props: vec!["amount".to_string()],
            },
        );
        cat.custom_properties.insert(
            "gst_fee".to_string(),
            CustomPropertyDef {
                name: "gst_fee".to_string(),
                sql_expr: "amount * 0.018".to_string(),
                referenced_props: vec!["amount".to_string()],
            },
        );
        let csv_lookup = b"merchant_id,category\nm_swiggy,Food_Delivery\nm_zepto,Quick_Commerce\n";
        cat.lookup_tables.insert(
            "merchants".to_string(),
            LookupTableDef::from_csv_bytes("merchants", "merchant_id", csv_lookup).unwrap(),
        );

        let ins_query = InsightsQuery {
            time_range: full_window,
            interval: TimeInterval::Day,
            series: vec![
                InsightsSeriesSpec {
                    label: "A".to_string(),
                    event_name: "Step_0".to_string(),
                    metric: InsightsMetric::Uniques,
                    filters: Vec::new(),
                },
                InsightsSeriesSpec {
                    label: "B".to_string(),
                    event_name: "Step_3".to_string(),
                    metric: InsightsMetric::Uniques,
                    filters: Vec::new(),
                },
            ],
            breakdown_by: vec!["value_tier".to_string(), "category".to_string()],
            global_filter: GlobalFilter::default(),
            compare_previous_period: true,
            formula: Some("(B / A) * 100".to_string()),
        };
        let ins_res = qengine
            .query_insights("proj_diff", &ins_query)
            .await
            .expect("insights");
        assert!(ins_res
            .series
            .iter()
            .any(|s| s.breakdown_key == "High | Food_Delivery" && s.overall_value == 20.0));
        assert!(!ins_res.formula_series.is_empty());

        id_router.close_all().await.unwrap();
    }

    #[tokio::test]
    async fn gate3_benchmark_1m_events_4step_funnel_under_50ms() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).expect("local fs"));
        let id_router = Arc::new(IdentityRouter::new(store.clone()));

        let cfg = StorageConfig {
            num_buckets: 16,
            l0_flush_row_threshold: 250_000,
            l0_flush_max_age: Duration::from_secs(600),
            compaction_min_segments: 8,
        };
        let storage = StorageEngine::new(cfg, store, id_router.clone());
        let gov = InMemoryGovernance::new();
        let ingest = IngestService::new(storage.clone(), gov);
        let catalogs = CatalogRegistry::new();
        let qengine = QueryEngine::new(
            storage.clone(),
            128 * 1024 * 1024,
            None,
            64 * 1024 * 1024,
            catalogs,
        )
        .await
        .unwrap();

        let base_ts = TimeInterval::Day.align_floor(1_763_000_000_000);
        let step_names = [
            "App_Open",
            "KYC_PAN_Submitted",
            "UPI_Autopay_Mandate_Approved",
            "First_Disbursal_Success",
            "Feed_Scroll",
            "View_Offer",
        ];

        // Ingest 1,000,000 events across 25,000 users in 20 batches of 50,000 events
        let batch_len = 50_000usize;
        for b_idx in 0..20usize {
            let mut batch_events = Vec::with_capacity(batch_len);
            let offset_base = b_idx * batch_len;
            for j in 0..batch_len {
                let i = offset_base + j;
                let uid = i % 25_000;
                let stage = (i / 25_000) % 6;
                // Drop off progressively so realistic funnel numbers emerge
                let ev_name = if stage == 1 && uid % 10 >= 7 {
                    "Feed_Scroll"
                } else if stage == 2 && uid % 10 >= 5 {
                    "View_Offer"
                } else if stage == 3 && uid % 10 >= 3 {
                    "Feed_Scroll"
                } else {
                    step_names[stage]
                };

                let ts = base_ts + ((i as i64) * 40);
                let distinct_id = format!("u_{uid:05}");
                batch_events.push(WireEvent {
                    event_uuid: [
                        (i & 0xff) as u8,
                        ((i >> 8) & 0xff) as u8,
                        ((i >> 16) & 0xff) as u8,
                        ((i >> 24) & 0xff) as u8,
                        b_idx as u8,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9,
                        10,
                        11,
                    ],
                    routing_seed: distinct_id.clone(),
                    distinct_id,
                    session_id: format!("s_{uid:05}"),
                    event_name: ev_name.to_string(),
                    client_event_ts_ms: ts,
                    client_sent_ts_ms: ts + 10,
                    props: Vec::new(),
                    replay_offset_kb: 0,
                });
            }
            ingest
                .ingest_for_project(
                    "proj_bench_1m",
                    WireBatch::new(batch_events),
                    base_ts + 50_000_000,
                )
                .await
                .unwrap();
        }
        storage.flush_all_projects().await.unwrap();

        let ps = storage.get_or_open_project("proj_bench_1m").await.unwrap();
        assert_eq!(ps.snapshot().manifest.total_l1_rows(), 1_000_000);

        let query = FunnelQuery {
            time_range: TimeRange {
                start_ts_ms: base_ts,
                end_ts_ms: base_ts + 86_400_000,
            },
            steps: [
                "App_Open",
                "KYC_PAN_Submitted",
                "UPI_Autopay_Mandate_Approved",
                "First_Disbursal_Success",
            ]
            .into_iter()
            .map(|s| FunnelStepSpec {
                event_name: s.to_string(),
                filters: Vec::new(),
                first_time_ever: false,
            })
            .collect(),
            order_mode: FunnelOrderMode::LooseOrder,
            conversion_window_ms: 86_400_000,
            exclusions: Vec::new(),
            hold_constant_props: Vec::new(),
            breakdown_by: None,
            global_filter: GlobalFilter::default(),
        };

        // Prime `foyer` NVMe/memory cache once (stateless compute node hot block cache) and measure scan!
        let warmup = qengine.query_funnel("proj_bench_1m", &query).await.unwrap();
        assert_eq!(warmup.overall_steps[0].entered_users, 25_000);
        assert_eq!(warmup.overall_steps[1].entered_users, 17_500);
        assert_eq!(warmup.overall_steps[2].entered_users, 12_500);
        assert_eq!(warmup.overall_steps[3].entered_users, 7_500);

        let t0 = Instant::now();
        let res = qengine.query_funnel("proj_bench_1m", &query).await.unwrap();
        let elapsed = t0.elapsed();
        println!(
            "[BENCHMARK] 1,000,000 stored events 4-step Funnel latency: {:?} (cold={:.2}ms)",
            elapsed,
            warmup.exec_time_us as f64 / 1000.0
        );

        #[cfg(not(debug_assertions))]
        assert!(
            elapsed < Duration::from_millis(50),
            "expected < 50ms for 1,000,000-event 4-step Funnel in --release, took {elapsed:?}"
        );
        assert_eq!(res.overall_steps[3].entered_users, 7_500);

        id_router.close_all().await.unwrap();
    }
}
