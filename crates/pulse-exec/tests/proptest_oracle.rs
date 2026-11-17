//! Independent, zero-shared-code naive reference oracle driven by `proptest` (`REVIEW.md` Section 5, `P2-01`, `P2-09`).
//!
//! `mod naive_oracle` intentionally imports NOTHING from `pulse_*` crates so zero filter, bucketing,
//! cohort, funnel, insights, retention, or flows code is shared with the execution engine.

mod naive_oracle {
    use std::collections::{BTreeMap, HashMap, HashSet};

    #[derive(Debug, Clone, PartialEq)]
    pub enum NVal {
        Null,
        Bool(bool),
        Int(i64),
        Float(f64),
        Str(String),
    }

    impl NVal {
        pub fn as_num(&self) -> Option<f64> {
            match self {
                NVal::Int(i) => Some(*i as f64),
                NVal::Float(f) => Some(*f),
                NVal::Str(s) => s.parse::<f64>().ok(),
                _ => None,
            }
        }

        pub fn as_text(&self) -> String {
            match self {
                NVal::Null => "null".to_string(),
                NVal::Bool(b) => {
                    if *b {
                        "true".to_string()
                    } else {
                        "false".to_string()
                    }
                }
                NVal::Int(i) => i.to_string(),
                NVal::Float(f) => f.to_string(),
                NVal::Str(s) => s.clone(),
            }
        }
    }

    fn vals_coalesced_eq(a: &NVal, b: &NVal) -> bool {
        if a == b {
            return true;
        }
        if let (Some(na), Some(nb)) = (a.as_num(), b.as_num()) {
            return (na - nb).abs() < 1e-9;
        }
        a.as_text() == b.as_text()
    }

    #[derive(Debug, Clone)]
    pub enum NFilterOp {
        Eq(NVal),
        Neq(NVal),
        Gt(f64),
        Lte(f64),
        Contains(String),
        In(Vec<String>),
        IsSet,
    }

    #[derive(Debug, Clone)]
    pub struct NPred {
        pub prop: String,
        pub op: NFilterOp,
    }

    impl NPred {
        pub fn eval(&self, actual: Option<&NVal>) -> bool {
            match (&self.op, actual) {
                (NFilterOp::IsSet, Some(v)) => !matches!(v, NVal::Null),
                (NFilterOp::IsSet, None) => false,
                (NFilterOp::Eq(NVal::Null), None | Some(NVal::Null)) => true,
                (NFilterOp::Eq(NVal::Null), Some(_)) => false,
                (NFilterOp::Neq(target), None | Some(NVal::Null)) => !matches!(target, NVal::Null),
                (_, None | Some(NVal::Null)) => false,
                (NFilterOp::Eq(target), Some(v)) => vals_coalesced_eq(v, target),
                (NFilterOp::Neq(target), Some(v)) => !vals_coalesced_eq(v, target),
                (NFilterOp::Gt(bound), Some(v)) => v.as_num().is_some_and(|x| x > *bound),
                (NFilterOp::Lte(bound), Some(v)) => v.as_num().is_some_and(|x| x <= *bound),
                (NFilterOp::Contains(sub), Some(v)) => v.as_text().contains(sub),
                (NFilterOp::In(opts), Some(v)) => {
                    let t = v.as_text();
                    opts.iter().any(|o| o == &t)
                }
            }
        }
    }

    #[derive(Debug, Clone)]
    pub struct NEvent {
        pub distinct_id: String,
        pub session_id: String,
        pub event_name: String,
        pub ts_ms: i64,
        pub props: HashMap<String, NVal>,
    }

    #[derive(Debug, Clone)]
    pub enum NCohort {
        Performed {
            event_name: String,
            min_count: u32,
            range: Option<(i64, i64)>,
            preds: Vec<NPred>,
        },
        NotPerformed {
            event_name: String,
            range: Option<(i64, i64)>,
        },
        PropMatch {
            pred: NPred,
            range: Option<(i64, i64)>,
        },
        Ref(String),
        And(Vec<NCohort>),
        Or(Vec<NCohort>),
    }

    #[derive(Debug, Clone)]
    pub struct NWorld {
        pub events: Vec<NEvent>,
        /// `anon_id -> user_id`
        pub merges: Vec<(String, String)>,
        pub saved_cohorts: HashMap<String, NCohort>,
    }

    impl NWorld {
        /// Naive transitive closure over undirected connected components, picking a deterministic canonical ID per component.
        pub fn canonical_map(&self) -> HashMap<String, String> {
            let mut all_ids: HashSet<String> = HashSet::new();
            for e in &self.events {
                all_ids.insert(e.distinct_id.clone());
            }
            for (a, u) in &self.merges {
                all_ids.insert(a.clone());
                all_ids.insert(u.clone());
            }
            let mut parent: HashMap<String, String> =
                all_ids.iter().map(|id| (id.clone(), id.clone())).collect();

            fn find(map: &HashMap<String, String>, mut k: String) -> String {
                while let Some(p) = map.get(&k) {
                    if p == &k {
                        break;
                    }
                    k = p.clone();
                }
                k
            }

            for (a, u) in &self.merges {
                let ra = find(&parent, a.clone());
                let ru = find(&parent, u.clone());
                if ra != ru {
                    // Orient anon root -> user root
                    parent.insert(ra, ru);
                }
            }

            let mut resolved = HashMap::new();
            for id in all_ids {
                let root = find(&parent, id.clone());
                resolved.insert(id, root);
            }
            resolved
        }

        pub fn timelines_by_user(&self) -> BTreeMap<String, Vec<NEvent>> {
            let cmap = self.canonical_map();
            let mut out: BTreeMap<String, Vec<NEvent>> = BTreeMap::new();
            // Ensure every known canonical user exists in the universe even if they have 0 events
            for canon in cmap.values() {
                out.entry(canon.clone()).or_default();
            }
            for ev in &self.events {
                let canon = cmap
                    .get(&ev.distinct_id)
                    .cloned()
                    .unwrap_or_else(|| ev.distinct_id.clone());
                out.entry(canon).or_default().push(ev.clone());
            }
            for list in out.values_mut() {
                list.sort_by(|a, b| {
                    a.ts_ms
                        .cmp(&b.ts_ms)
                        .then_with(|| a.event_name.cmp(&b.event_name))
                });
            }
            out
        }

        pub fn eval_cohort(
            &self,
            cohort: &NCohort,
            default_range: (i64, i64),
            timelines: &BTreeMap<String, Vec<NEvent>>,
        ) -> HashSet<String> {
            match cohort {
                NCohort::Performed {
                    event_name,
                    min_count,
                    range,
                    preds,
                } => {
                    let (lo, hi) = range.unwrap_or(default_range);
                    let need = (*min_count).max(1) as usize;
                    timelines
                        .iter()
                        .filter(|(_, evs)| {
                            let cnt = evs
                                .iter()
                                .filter(|e| {
                                    e.ts_ms >= lo
                                        && e.ts_ms <= hi
                                        && &e.event_name == event_name
                                        && preds.iter().all(|p| p.eval(e.props.get(&p.prop)))
                                })
                                .count();
                            cnt >= need
                        })
                        .map(|(u, _)| u.clone())
                        .collect()
                }
                NCohort::NotPerformed { event_name, range } => {
                    let (lo, hi) = range.unwrap_or(default_range);
                    // Matches engine: universe of users active in `[lo, hi]` minus those who performed `event_name` in `[lo, hi]`
                    timelines
                        .iter()
                        .filter(|(_, evs)| {
                            let active_in_window =
                                evs.iter().any(|e| e.ts_ms >= lo && e.ts_ms <= hi);
                            let did_event = evs.iter().any(|e| {
                                e.ts_ms >= lo && e.ts_ms <= hi && &e.event_name == event_name
                            });
                            active_in_window && !did_event
                        })
                        .map(|(u, _)| u.clone())
                        .collect()
                }
                NCohort::PropMatch { pred, range } => {
                    let (lo, hi) = range.unwrap_or(default_range);
                    timelines
                        .iter()
                        .filter(|(_, evs)| {
                            evs.iter().any(|e| {
                                e.ts_ms >= lo && e.ts_ms <= hi && pred.eval(e.props.get(&pred.prop))
                            })
                        })
                        .map(|(u, _)| u.clone())
                        .collect()
                }
                NCohort::Ref(id) => self
                    .saved_cohorts
                    .get(id)
                    .map(|c| self.eval_cohort(c, default_range, timelines))
                    .unwrap_or_default(),
                NCohort::And(clauses) => {
                    let Some((first, rest)) = clauses.split_first() else {
                        return HashSet::new();
                    };
                    let mut acc = self.eval_cohort(first, default_range, timelines);
                    for c in rest {
                        let rhs = self.eval_cohort(c, default_range, timelines);
                        acc.retain(|u| rhs.contains(u));
                    }
                    acc
                }
                NCohort::Or(clauses) => {
                    let mut acc = HashSet::new();
                    for c in clauses {
                        acc.extend(self.eval_cohort(c, default_range, timelines));
                    }
                    acc
                }
            }
        }

        #[allow(clippy::too_many_arguments)]
        pub fn eval_funnel(
            &self,
            range: (i64, i64),
            steps: &[(String, Vec<NPred>, bool)],
            order: &str,
            window_ms: i64,
            exclusions: &[(String, usize, usize)],
            hold_props: &[String],
            global_preds: &[NPred],
            cohort: Option<&NCohort>,
        ) -> Vec<u64> {
            let timelines = self.timelines_by_user();
            let eligible = cohort.map(|c| self.eval_cohort(c, range, &timelines));
            let k = steps.len();
            let mut counts = vec![0u64; k];

            for (uid, raw_evs) in &timelines {
                if let Some(el) = &eligible {
                    if !el.contains(uid) {
                        continue;
                    }
                }
                // Filter stream by global predicates first
                let evs: Vec<&NEvent> = raw_evs
                    .iter()
                    .filter(|e| global_preds.iter().all(|gp| gp.eval(e.props.get(&gp.prop))))
                    .collect();
                if evs.is_empty() {
                    continue;
                }

                // For each step `s` with `first_time_ever = true`, find the earliest index across ALL history matching `step[s]`
                let mut first_ever_idx: Vec<Option<usize>> = vec![None; k];
                for (s, (ename, spreds, first_ever)) in steps.iter().enumerate() {
                    if *first_ever {
                        first_ever_idx[s] = evs.iter().position(|e| {
                            &e.event_name == ename
                                && spreds.iter().all(|p| p.eval(e.props.get(&p.prop)))
                        });
                    }
                }

                let step_matches = |idx: usize, s: usize| -> bool {
                    let e = evs[idx];
                    let (ename, spreds, first_ever) = &steps[s];
                    if &e.event_name != ename {
                        return false;
                    }
                    if !spreds.iter().all(|p| p.eval(e.props.get(&p.prop))) {
                        return false;
                    }
                    if *first_ever && first_ever_idx[s] != Some(idx) {
                        return false;
                    }
                    true
                };

                let hold_vals = |idx: usize| -> Option<Vec<String>> {
                    if hold_props.is_empty() {
                        return Some(Vec::new());
                    }
                    let mut v = Vec::with_capacity(hold_props.len());
                    for hp in hold_props {
                        match evs[idx].props.get(hp) {
                            None | Some(NVal::Null) => return None,
                            Some(val) => v.push(val.as_text()),
                        }
                    }
                    Some(v)
                };

                let mut best_step: Option<usize> = None;

                match order {
                    "strict" => {
                        // StrictOrder: contiguous slice `i0, i0+1, ..., i0+r` in the user's global filtered timeline!
                        for i0 in 0..evs.len() {
                            if evs[i0].ts_ms < range.0
                                || evs[i0].ts_ms > range.1
                                || !step_matches(i0, 0)
                            {
                                continue;
                            }
                            let Some(h0) = hold_vals(i0) else {
                                continue;
                            };
                            let mut reached = 0usize;
                            for s in 1..k {
                                let idx = i0 + s;
                                if idx >= evs.len() {
                                    break;
                                }
                                if evs[idx].ts_ms - evs[i0].ts_ms > window_ms {
                                    break;
                                }
                                let hit_excl = exclusions.iter().any(|(ex_name, lo, hi)| {
                                    *lo <= reached
                                        && reached < *hi
                                        && &evs[idx].event_name == ex_name
                                });
                                if hit_excl {
                                    break;
                                }
                                if step_matches(idx, s) && hold_vals(idx).as_ref() == Some(&h0) {
                                    reached = s;
                                } else {
                                    break;
                                }
                            }
                            best_step = Some(best_step.map_or(reached, |b| b.max(reached)));
                        }
                    }
                    "loose" => {
                        // LooseOrder: recursive earliest-valid-step progression + exhaustive subsequence sanity check when exclusions.is_empty()
                        for i0 in 0..evs.len() {
                            if evs[i0].ts_ms < range.0
                                || evs[i0].ts_ms > range.1
                                || !step_matches(i0, 0)
                            {
                                continue;
                            }
                            let Some(h0) = hold_vals(i0) else {
                                continue;
                            };
                            let mut reached = 0usize;
                            let mut prev_idx = i0;
                            while reached + 1 < k {
                                let mut next_found = None;
                                let mut aborted_by_excl = false;
                                for cand in (prev_idx + 1)..evs.len() {
                                    if evs[cand].ts_ms - evs[i0].ts_ms > window_ms {
                                        break;
                                    }
                                    let hit_excl = exclusions.iter().any(|(ex_name, lo, hi)| {
                                        *lo <= reached
                                            && reached < *hi
                                            && &evs[cand].event_name == ex_name
                                    });
                                    if hit_excl {
                                        aborted_by_excl = true;
                                        break;
                                    }
                                    if step_matches(cand, reached + 1)
                                        && hold_vals(cand).as_ref() == Some(&h0)
                                    {
                                        next_found = Some(cand);
                                        break;
                                    }
                                }
                                if aborted_by_excl {
                                    break;
                                }
                                if let Some(nxt) = next_found {
                                    reached += 1;
                                    prev_idx = nxt;
                                } else {
                                    break;
                                }
                            }

                            // When no exclusions exist and k <= 3, double-check against exhaustive O(n^3) tuple enumeration!
                            if exclusions.is_empty() && k <= 3 {
                                let mut tuple_best = 0usize;
                                for i1 in (i0 + 1)..evs.len() {
                                    if evs[i1].ts_ms - evs[i0].ts_ms <= window_ms
                                        && k >= 2
                                        && step_matches(i1, 1)
                                        && hold_vals(i1).as_ref() == Some(&h0)
                                    {
                                        tuple_best = tuple_best.max(1);
                                        for i2 in (i1 + 1)..evs.len() {
                                            if evs[i2].ts_ms - evs[i0].ts_ms <= window_ms
                                                && k >= 3
                                                && step_matches(i2, 2)
                                                && hold_vals(i2).as_ref() == Some(&h0)
                                            {
                                                tuple_best = tuple_best.max(2);
                                            }
                                        }
                                    }
                                }
                                assert_eq!(reached, tuple_best);
                            }

                            best_step = Some(best_step.map_or(reached, |b| b.max(reached)));
                        }
                    }
                    "any" => {
                        for i0 in 0..evs.len() {
                            if evs[i0].ts_ms < range.0 || evs[i0].ts_ms > range.1 {
                                continue;
                            }
                            let mut done: BTreeMap<usize, ()> = BTreeMap::new();
                            for s in 0..k {
                                if step_matches(i0, s) {
                                    done.insert(s, ());
                                }
                            }
                            if done.is_empty() {
                                continue;
                            }
                            let Some(h0) = hold_vals(i0) else {
                                continue;
                            };
                            for j in (i0 + 1)..evs.len() {
                                if evs[j].ts_ms - evs[i0].ts_ms > window_ms {
                                    break;
                                }
                                let hit_excl = exclusions.iter().any(|(ex_name, lo, hi)| {
                                    done.contains_key(lo)
                                        && !done.contains_key(hi)
                                        && &evs[j].event_name == ex_name
                                });
                                if hit_excl {
                                    break;
                                }
                                if hold_vals(j).as_ref() == Some(&h0) {
                                    for s in 0..k {
                                        if step_matches(j, s) {
                                            done.insert(s, ());
                                        }
                                    }
                                    if done.len() == k {
                                        break;
                                    }
                                }
                            }
                            let reached = done.len() - 1;
                            best_step = Some(best_step.map_or(reached, |b| b.max(reached)));
                        }
                    }
                    _ => unreachable!(),
                }

                if let Some(mr) = best_step {
                    for slot in counts.iter_mut().take(mr + 1) {
                        *slot += 1;
                    }
                }
            }

            counts
        }

        pub fn utc_day_floor(ts_ms: i64) -> i64 {
            const DAY_MS: i64 = 86_400_000;
            ts_ms.div_euclid(DAY_MS) * DAY_MS
        }

        #[allow(clippy::too_many_arguments)]
        pub fn eval_insights_daily(
            &self,
            range: (i64, i64),
            event_name: &str,
            filters: &[NPred],
            global_preds: &[NPred],
            cohort: Option<&NCohort>,
            prop_name: &str,
        ) -> (u64, u64, f64, Vec<f64>, Vec<(i64, u64)>) {
            let timelines = self.timelines_by_user();
            let eligible = cohort.map(|c| self.eval_cohort(c, range, &timelines));

            let mut unique_users: HashSet<String> = HashSet::new();
            let mut totals: u64 = 0;
            let mut prop_sum: f64 = 0.0;
            let mut prop_vals: Vec<f64> = Vec::new();

            // Also track users per UTC day for end-anchored 7-day WAU verification
            let mut users_by_day: BTreeMap<i64, HashSet<String>> = BTreeMap::new();

            let wau_scan_lo = Self::utc_day_floor(range.0) - 7 * 86_400_000;
            for (uid, evs) in &timelines {
                if let Some(el) = &eligible {
                    if !el.contains(uid) {
                        continue;
                    }
                }
                for e in evs {
                    if e.event_name != event_name {
                        continue;
                    }
                    if !global_preds.iter().all(|p| p.eval(e.props.get(&p.prop))) {
                        continue;
                    }
                    if !filters.iter().all(|p| p.eval(e.props.get(&p.prop))) {
                        continue;
                    }
                    if e.ts_ms >= wau_scan_lo && e.ts_ms <= range.1 {
                        let d = Self::utc_day_floor(e.ts_ms);
                        users_by_day.entry(d).or_default().insert(uid.clone());
                    }
                    if e.ts_ms >= range.0 && e.ts_ms <= range.1 {
                        unique_users.insert(uid.clone());
                        totals += 1;
                        if let Some(v) = e.props.get(prop_name).and_then(|x| x.as_num()) {
                            prop_sum += v;
                            prop_vals.push(v);
                        }
                    }
                }
            }
            prop_vals.sort_by(|a, b| a.total_cmp(b));

            let mut wau_points = Vec::new();
            let mut b = Self::utc_day_floor(range.0);
            let b_end = Self::utc_day_floor(range.1);
            while b <= b_end {
                let mut rolling: HashSet<String> = HashSet::new();
                for back in 0..7i64 {
                    let day_k = b - back * 86_400_000;
                    if let Some(set) = users_by_day.get(&day_k) {
                        rolling.extend(set.iter().cloned());
                    }
                }
                wau_points.push((b, rolling.len() as u64));
                b += 86_400_000;
            }

            (
                unique_users.len() as u64,
                totals,
                prop_sum,
                prop_vals,
                wau_points,
            )
        }

        pub fn eval_retention_daily(
            &self,
            range: (i64, i64),
            start_ev: &str,
            ret_ev: &str,
            num_periods: usize,
            mode: &str,
        ) -> Vec<(i64, Vec<u64>)> {
            let timelines = self.timelines_by_user();
            let mut cohorts: BTreeMap<i64, Vec<&Vec<NEvent>>> = BTreeMap::new();
            for evs in timelines.values() {
                if let Some(t0) = evs
                    .iter()
                    .filter(|e| {
                        e.ts_ms >= range.0 && e.ts_ms <= range.1 && e.event_name == start_ev
                    })
                    .map(|e| e.ts_ms)
                    .min()
                {
                    cohorts
                        .entry(Self::utc_day_floor(t0))
                        .or_default()
                        .push(evs);
                }
            }

            let mut rows = Vec::new();
            for (c_day, users) in cohorts {
                let mut counts = vec![0u64; num_periods + 1];
                match mode {
                    "n_day" => {
                        counts[0] = users.len() as u64;
                        for (k, slot) in counts.iter_mut().enumerate().take(num_periods + 1).skip(1)
                        {
                            let target_day = c_day + (k as i64) * 86_400_000;
                            *slot = users
                                .iter()
                                .filter(|evs| {
                                    evs.iter().any(|e| {
                                        e.event_name == ret_ev
                                            && Self::utc_day_floor(e.ts_ms) == target_day
                                    })
                                })
                                .count() as u64;
                        }
                    }
                    "unbounded" => {
                        counts[0] = users.len() as u64;
                        let max_day = c_day + (num_periods as i64) * 86_400_000;
                        for (k, slot) in counts.iter_mut().enumerate().take(num_periods + 1).skip(1)
                        {
                            let min_day = c_day + (k as i64) * 86_400_000;
                            *slot = users
                                .iter()
                                .filter(|evs| {
                                    evs.iter().any(|e| {
                                        let d = Self::utc_day_floor(e.ts_ms);
                                        e.event_name == ret_ev && d >= min_day && d <= max_day
                                    })
                                })
                                .count() as u64;
                        }
                    }
                    "stickiness" => {
                        let max_day = c_day + (num_periods as i64) * 86_400_000;
                        for evs in &users {
                            let mut active_days = HashSet::new();
                            for e in *evs {
                                if e.event_name == ret_ev {
                                    let d = Self::utc_day_floor(e.ts_ms);
                                    if d >= c_day && d <= max_day {
                                        active_days.insert(d);
                                    }
                                }
                            }
                            for (idx, slot) in counts.iter_mut().enumerate() {
                                if active_days.len() > idx {
                                    *slot += 1;
                                }
                            }
                        }
                    }
                    _ => unreachable!(),
                }
                rows.push((c_day, counts));
            }
            rows
        }
    }
}

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use naive_oracle::{NCohort, NEvent, NFilterOp, NPred, NVal, NWorld};
use object_store::memory::InMemory;
use proptest::prelude::*;
use pulse_exec::{
    BehavioralCohortDef, CatalogRegistry, FunnelExclusion, FunnelOrderMode, FunnelQuery,
    FunnelStepSpec, GlobalFilter, InsightsMetric, InsightsQuery, InsightsSeriesSpec, PropFilterOp,
    PropPredicate, QueryEngine, RetentionMode, RetentionQuery, TimeInterval, TimeRange,
};
use pulse_identity::IdentityRouter;
use pulse_ingest::{IdentifyRequest, InMemoryGovernance, IngestService};
use pulse_storage::{StorageConfig, StorageEngine};
use pulse_wire::{PropValue, WireBatch, WireEvent};

fn to_wire_val(v: &NVal) -> PropValue {
    match v {
        NVal::Null => PropValue::Null,
        NVal::Bool(b) => PropValue::Bool(*b),
        NVal::Int(i) => PropValue::Int(*i),
        NVal::Float(f) => PropValue::Float(*f),
        NVal::Str(s) => PropValue::Str(s.clone()),
    }
}

fn to_exec_pred(p: &NPred) -> PropPredicate {
    let filter = match &p.op {
        NFilterOp::Eq(v) => PropFilterOp::Eq(to_wire_val(v)),
        NFilterOp::Neq(v) => PropFilterOp::Neq(to_wire_val(v)),
        NFilterOp::Gt(x) => PropFilterOp::Gt(*x),
        NFilterOp::Lte(x) => PropFilterOp::Lte(*x),
        NFilterOp::Contains(s) => PropFilterOp::Contains(s.clone()),
        NFilterOp::In(v) => PropFilterOp::In(v.clone()),
        NFilterOp::IsSet => PropFilterOp::IsSet,
    };
    PropPredicate {
        prop_name: p.prop.clone(),
        filter,
    }
}

fn to_exec_cohort(c: &NCohort) -> BehavioralCohortDef {
    match c {
        NCohort::Performed {
            event_name,
            min_count,
            range,
            preds,
        } => BehavioralCohortDef::PerformedEvent {
            event_name: event_name.clone(),
            min_count: *min_count,
            time_range: range.map(|(s, e)| TimeRange {
                start_ts_ms: s,
                end_ts_ms: e,
            }),
            where_clauses: preds.iter().map(to_exec_pred).collect(),
        },
        NCohort::NotPerformed { event_name, range } => BehavioralCohortDef::NotPerformedEvent {
            event_name: event_name.clone(),
            time_range: range.map(|(s, e)| TimeRange {
                start_ts_ms: s,
                end_ts_ms: e,
            }),
        },
        NCohort::PropMatch { pred, range } => BehavioralCohortDef::PropertyMatch {
            predicate: to_exec_pred(pred),
            time_range: range.map(|(s, e)| TimeRange {
                start_ts_ms: s,
                end_ts_ms: e,
            }),
        },
        NCohort::Ref(id) => BehavioralCohortDef::CohortRef {
            cohort_id: id.clone(),
        },
        NCohort::And(v) => BehavioralCohortDef::And {
            clauses: v.iter().map(to_exec_cohort).collect(),
        },
        NCohort::Or(v) => BehavioralCohortDef::Or {
            clauses: v.iter().map(to_exec_cohort).collect(),
        },
    }
}

#[derive(Debug, Clone)]
struct GeneratedCase {
    world: NWorld,
    flush_split_pct: usize,
    window_ms: i64,
    first_time_s0: bool,
    first_time_s1: bool,
    use_exclusion: bool,
    use_hold_prop: bool,
    step0_filter_kind: u8,
    global_filter_kind: u8,
    cohort_kind: u8,
    quantile: f64,
}

fn arb_country_opt() -> impl Strategy<Value = Option<NVal>> {
    prop_oneof![
        2 => Just(None),
        2 => Just(Some(NVal::Null)),
        3 => Just(Some(NVal::Str("IN".to_string()))),
        3 => Just(Some(NVal::Str("US".to_string()))),
        2 => Just(Some(NVal::Str("SG".to_string()))),
    ]
}

fn arb_tier_opt() -> impl Strategy<Value = Option<NVal>> {
    prop_oneof![
        1 => Just(None),
        1 => Just(Some(NVal::Null)),
        3 => Just(Some(NVal::Int(1))),
        3 => Just(Some(NVal::Str("1".to_string()))), // exercises Int(1) vs Str("1") coalescing (`P2-01`)
        3 => Just(Some(NVal::Int(2))),
        2 => Just(Some(NVal::Bool(true))),
        2 => Just(Some(NVal::Str("true".to_string()))), // exercises Bool(true) vs Str("true") (`P2-01`)
    ]
}

fn arb_case() -> impl Strategy<Value = GeneratedCase> {
    let base_day = 1_762_041_600_000i64; // 2025-11-02 00:00:00 UTC

    (
        prop::collection::vec(
            (
                0usize..8usize, // user index
                any::<bool>(),  // emitted under anon_id vs user_id
                prop_oneof![
                    Just("E0"),
                    Just("E1"),
                    Just("E2"),
                    Just("X_ERR"),
                    Just("NOISE"),
                ],
                -1i64..4i64, // day offset (-1 exercises pre-window history for `first_time_ever`!)
                0i64..6_000i64, // intra-day base ms
                arb_country_opt(),
                arb_tier_opt(),
                -250i64..250i64, // signed delta (negative, zero, positive for `P2-09a`!)
            ),
            25..90,
        ),
        prop::collection::vec(0usize..8usize, 0..6), // which users merge anon_u -> user_u
        20usize..85usize,                            // L1 flush split percentage
        prop_oneof![Just(500i64), Just(1_500i64), Just(3_000i64)], // conversion_window_ms
        any::<bool>(),                               // first_time_s0
        any::<bool>(),                               // first_time_s1
        any::<bool>(),                               // use_exclusion
        any::<bool>(),                               // use_hold_prop
        0u8..7u8,                                    // step0_filter_kind
        0u8..4u8,                                    // global_filter_kind
        0u8..5u8,                                    // cohort_kind
        prop_oneof![Just(0.10), Just(0.50), Just(0.90)], // quantile
    )
        .prop_map(
            move |(
                raw_specs,
                merge_users,
                flush_split_pct,
                window_ms,
                first_time_s0,
                first_time_s1,
                use_exclusion,
                use_hold_prop,
                step0_filter_kind,
                global_filter_kind,
                cohort_kind,
                quantile,
            )| {
                let mut events = Vec::with_capacity(raw_specs.len() + 6);
                let mut unique_tick = 1i64;

                for (u, use_anon, ename, day_off, sub_ms, country_opt, tier_opt, delta_i) in
                    raw_specs
                {
                    unique_tick += 7;
                    let did = if use_anon {
                        format!("anon_{u}")
                    } else {
                        format!("user_{u}")
                    };
                    let ts_ms = base_day + day_off * 86_400_000 + sub_ms + unique_tick;
                    let mut props = HashMap::new();
                    if let Some(c) = country_opt {
                        props.insert("country".to_string(), c);
                    }
                    if let Some(t) = tier_opt {
                        props.insert("tier".to_string(), t);
                    }
                    props.insert("delta".to_string(), NVal::Float(delta_i as f64));

                    events.push(NEvent {
                        distinct_id: did,
                        session_id: format!("sess_{u}_{}", day_off + 2),
                        event_name: ename.to_string(),
                        ts_ms,
                        props,
                    });
                }

                // Inject exact conversion-window boundary pair on `user_8` & `user_9` (`t0 + window_ms` vs `t0 + window_ms + 1`)!
                let t_bound = base_day + 20_000;
                for (uid, dt, tier_v) in [
                    ("user_8", 0i64, "1"),
                    ("user_8", window_ms, "1"), // exact boundary: MUST convert!
                    ("user_9", 100i64, "1"),
                    ("user_9", 100 + window_ms + 1, "1"), // 1 ms past boundary: MUST NOT convert!
                ] {
                    let mut props = HashMap::new();
                    props.insert("country".to_string(), NVal::Str("IN".to_string()));
                    props.insert("tier".to_string(), NVal::Str(tier_v.to_string()));
                    props.insert("delta".to_string(), NVal::Float(-42.5));
                    events.push(NEvent {
                        distinct_id: uid.to_string(),
                        session_id: format!("sb_{uid}"),
                        event_name: if dt == 0 || dt == 100 {
                            "E0".to_string()
                        } else {
                            "E1".to_string()
                        },
                        ts_ms: t_bound + dt,
                        props,
                    });
                }

                let mut merges_dedup = Vec::new();
                let mut seen_u = std::collections::HashSet::new();
                for u in merge_users {
                    if seen_u.insert(u) {
                        merges_dedup.push((format!("anon_{u}"), format!("user_{u}")));
                    }
                }

                let mut saved_cohorts = HashMap::new();
                saved_cohorts.insert(
                    "coh_active_e0".to_string(),
                    NCohort::Performed {
                        event_name: "E0".to_string(),
                        min_count: 1,
                        range: None,
                        preds: Vec::new(),
                    },
                );

                GeneratedCase {
                    world: NWorld {
                        events,
                        merges: merges_dedup,
                        saved_cohorts,
                    },
                    flush_split_pct,
                    window_ms,
                    first_time_s0,
                    first_time_s1,
                    use_exclusion,
                    use_hold_prop,
                    step0_filter_kind,
                    global_filter_kind,
                    cohort_kind,
                    quantile,
                }
            },
        )
}

async fn assert_case_matches_oracle(case: GeneratedCase) {
    let base_day = 1_762_041_600_000i64;
    let query_range = (base_day, base_day + 3 * 86_400_000 - 1);
    let exec_range = TimeRange {
        start_ts_ms: query_range.0,
        end_ts_ms: query_range.1,
    };
    let server_now = base_day + 6 * 86_400_000;

    let store = Arc::new(InMemory::new());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 8,
            l0_flush_row_threshold: 100_000,
            l0_flush_max_age: Duration::from_secs(600),
            compaction_min_segments: 4,
        },
        store,
        id_router.clone(),
    );
    let gov = InMemoryGovernance::new();
    let ingest = IngestService::new(storage.clone(), gov);
    let catalogs = CatalogRegistry::new();
    let cat = catalogs.for_project("proj_prop");
    for (cid, cdef) in &case.world.saved_cohorts {
        cat.saved_cohorts.insert(cid.clone(), to_exec_cohort(cdef));
    }

    let qengine = QueryEngine::new(storage.clone(), 16 * 1024 * 1024, None, 0, catalogs)
        .await
        .unwrap();

    // Register cross-bucket & same-bucket identity merges
    for (idx, (anon_id, user_id)) in case.world.merges.iter().enumerate() {
        let anon_seed = format!("seed_a_{anon_id}");
        let user_seed = if idx % 2 == 0 {
            anon_seed.clone()
        } else {
            format!("seed_b_{user_id}_cross")
        };
        ingest
            .identify_for_project(
                "proj_prop",
                IdentifyRequest {
                    anon_id: anon_id.clone(),
                    user_id: user_id.clone(),
                    routing_seed: Some(user_seed),
                    anon_routing_seed: Some(anon_seed),
                    user_properties: None,
                    operations: None,
                },
            )
            .await
            .unwrap();
    }

    let wire_events: Vec<WireEvent> = case
        .world
        .events
        .iter()
        .enumerate()
        .map(|(seq, ne)| {
            let seed = if ne.distinct_id.starts_with("anon_") {
                format!("seed_a_{}", ne.distinct_id)
            } else {
                format!("seed_b_{}_cross", ne.distinct_id)
            };
            WireEvent {
                event_uuid: WireEvent::deterministic_uuid(
                    &ne.distinct_id,
                    &ne.session_id,
                    &ne.event_name,
                    ne.ts_ms,
                    seq as u64 + 1,
                ),
                routing_seed: seed,
                distinct_id: ne.distinct_id.clone(),
                session_id: ne.session_id.clone(),
                event_name: ne.event_name.clone(),
                client_event_ts_ms: ne.ts_ms,
                client_sent_ts_ms: server_now,
                props: ne
                    .props
                    .iter()
                    .map(|(k, v)| (k.clone(), to_wire_val(v)))
                    .collect(),
                replay_offset_kb: 0,
            }
        })
        .collect();

    let split_idx = (wire_events.len() * case.flush_split_pct) / 100;
    if split_idx > 0 {
        ingest
            .ingest_for_project(
                "proj_prop",
                WireBatch::new(wire_events[..split_idx].to_vec()),
                server_now,
            )
            .await
            .unwrap();
        storage.flush_all_projects().await.unwrap();
    }
    if split_idx < wire_events.len() {
        ingest
            .ingest_for_project(
                "proj_prop",
                WireBatch::new(wire_events[split_idx..].to_vec()),
                server_now,
            )
            .await
            .unwrap();
    }

    let step0_preds: Vec<NPred> = match case.step0_filter_kind {
        0 => Vec::new(),
        1 => vec![NPred {
            prop: "country".into(),
            op: NFilterOp::Neq(NVal::Str("US".into())), // exercises Neq on missing, Null, and non-US!
        }],
        2 => vec![NPred {
            prop: "tier".into(),
            op: NFilterOp::Eq(NVal::Int(1)), // exercises Int(1) == Str("1") across inverted index!
        }],
        3 => vec![NPred {
            prop: "tier".into(),
            op: NFilterOp::Eq(NVal::Bool(true)), // exercises Bool(true) == Str("true") across inverted index!
        }],
        4 => vec![NPred {
            prop: "delta".into(),
            op: NFilterOp::Lte(100.0),
        }],
        5 => vec![NPred {
            prop: "country".into(),
            op: NFilterOp::Contains("I".into()),
        }],
        _ => vec![NPred {
            prop: "country".into(),
            op: NFilterOp::IsSet,
        }],
    };

    let global_preds: Vec<NPred> = match case.global_filter_kind {
        0 => Vec::new(),
        1 => vec![NPred {
            prop: "country".into(),
            op: NFilterOp::Neq(NVal::Str("US".into())),
        }],
        2 => vec![NPred {
            prop: "delta".into(),
            op: NFilterOp::Gt(-200.0),
        }],
        _ => vec![NPred {
            prop: "country".into(),
            op: NFilterOp::In(vec!["IN".into(), "SG".into()]),
        }],
    };

    let cohort_opt: Option<NCohort> = match case.cohort_kind {
        0 => None,
        1 => Some(NCohort::Ref("coh_active_e0".into())),
        2 => Some(NCohort::NotPerformed {
            event_name: "X_ERR".into(),
            range: None,
        }),
        3 => Some(NCohort::And(vec![
            NCohort::Ref("coh_active_e0".into()),
            NCohort::PropMatch {
                pred: NPred {
                    prop: "country".into(),
                    op: NFilterOp::Neq(NVal::Str("US".into())),
                },
                range: None,
            },
        ])),
        _ => Some(NCohort::Or(vec![
            NCohort::Performed {
                event_name: "E0".into(),
                min_count: 2,
                range: None,
                preds: Vec::new(),
            },
            NCohort::Performed {
                event_name: "E1".into(),
                min_count: 1,
                range: None,
                preds: Vec::new(),
            },
        ])),
    };

    let exec_global = GlobalFilter {
        predicates: global_preds.iter().map(to_exec_pred).collect(),
        cohort: cohort_opt.as_ref().map(to_exec_cohort),
    };

    let funnel_steps_n = vec![
        ("E0".to_string(), step0_preds.clone(), case.first_time_s0),
        ("E1".to_string(), Vec::new(), case.first_time_s1),
        ("E2".to_string(), Vec::new(), false),
    ];
    let funnel_steps_exec: Vec<FunnelStepSpec> = funnel_steps_n
        .iter()
        .map(|(n, ps, ft)| FunnelStepSpec {
            event_name: n.clone(),
            filters: ps.iter().map(to_exec_pred).collect(),
            first_time_ever: *ft,
        })
        .collect();

    let exclusions_n: Vec<(String, usize, usize)> = if case.use_exclusion {
        vec![("X_ERR".to_string(), 0, 1)]
    } else {
        Vec::new()
    };
    let exclusions_exec: Vec<FunnelExclusion> = exclusions_n
        .iter()
        .map(|(n, lo, hi)| FunnelExclusion {
            event_name: n.clone(),
            between_steps: (*lo, *hi),
        })
        .collect();

    let hold_props: Vec<String> = if case.use_hold_prop {
        vec!["country".to_string()]
    } else {
        Vec::new()
    };

    // 1. Differential Funnel checks across LooseOrder, StrictOrder, and AnyOrder
    for (mode_str, mode_enum) in [
        ("loose", FunnelOrderMode::LooseOrder),
        ("strict", FunnelOrderMode::StrictOrder),
        ("any", FunnelOrderMode::AnyOrder),
    ] {
        let expected = case.world.eval_funnel(
            query_range,
            &funnel_steps_n,
            mode_str,
            case.window_ms,
            &exclusions_n,
            &hold_props,
            &global_preds,
            cohort_opt.as_ref(),
        );
        let fq = FunnelQuery {
            time_range: exec_range,
            steps: funnel_steps_exec.clone(),
            order_mode: mode_enum,
            conversion_window_ms: case.window_ms,
            exclusions: exclusions_exec.clone(),
            hold_constant_props: hold_props.clone(),
            breakdown_by: None,
            global_filter: exec_global.clone(),
        };
        let actual_res = qengine.query_funnel("proj_prop", &fq).await.unwrap();
        let actual: Vec<u64> = actual_res
            .overall_steps
            .iter()
            .map(|s| s.entered_users)
            .collect();
        assert_eq!(
            actual, expected,
            "Funnel {mode_str} mismatch vs zero-shared-code naive oracle"
        );
    }

    // 2. Differential Insights checks: Uniques (including P2-01 masked vs unmasked), Totals, PropSum, signed PropPercentile (`P2-09a`), and end-anchored WAU (`P2-09b`)
    let (exp_uniques, exp_totals, exp_sum, exp_vals, exp_wau) = case.world.eval_insights_daily(
        query_range,
        "E0",
        &step0_preds,
        &global_preds,
        cohort_opt.as_ref(),
        "delta",
    );

    let ins_res = qengine
        .query_insights(
            "proj_prop",
            &InsightsQuery {
                time_range: exec_range,
                interval: TimeInterval::Day,
                series: vec![
                    InsightsSeriesSpec {
                        label: "U".into(),
                        event_name: "E0".into(),
                        metric: InsightsMetric::Uniques,
                        filters: step0_preds.iter().map(to_exec_pred).collect(),
                    },
                    InsightsSeriesSpec {
                        label: "T".into(),
                        event_name: "E0".into(),
                        metric: InsightsMetric::Totals,
                        filters: step0_preds.iter().map(to_exec_pred).collect(),
                    },
                    InsightsSeriesSpec {
                        label: "S".into(),
                        event_name: "E0".into(),
                        metric: InsightsMetric::PropSum {
                            prop: "delta".into(),
                        },
                        filters: step0_preds.iter().map(to_exec_pred).collect(),
                    },
                    InsightsSeriesSpec {
                        label: "P".into(),
                        event_name: "E0".into(),
                        metric: InsightsMetric::PropPercentile {
                            prop: "delta".into(),
                            quantile: case.quantile,
                        },
                        filters: step0_preds.iter().map(to_exec_pred).collect(),
                    },
                    InsightsSeriesSpec {
                        label: "W".into(),
                        event_name: "E0".into(),
                        metric: InsightsMetric::Wau,
                        filters: step0_preds.iter().map(to_exec_pred).collect(),
                    },
                ],
                breakdown_by: Vec::new(),
                global_filter: exec_global,
                compare_previous_period: false,
                formula: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(ins_res.series[0].overall_value, Some(exp_uniques as f64));
    assert_eq!(ins_res.series[1].overall_value, Some(exp_totals as f64));
    let got_sum = ins_res.series[2].overall_value.unwrap_or(0.0);
    assert!(
        (got_sum - exp_sum).abs() < 1e-6,
        "PropSum mismatch: got {got_sum}, expected {exp_sum}"
    );

    if exp_vals.is_empty() {
        assert_eq!(ins_res.series[3].overall_value, None);
    } else {
        let est = ins_res.series[3].overall_value.expect("percentile value");
        let n = exp_vals.len();
        let lo_rank =
            (((case.quantile * n as f64).floor() as isize) - 1).clamp(0, n as isize - 1) as usize;
        let hi_rank = ((case.quantile * n as f64).ceil() as usize).min(n - 1);
        let lo_v = exp_vals[lo_rank];
        let hi_v = exp_vals[hi_rank];
        let tol = 0.05 * lo_v.abs().max(hi_v.abs()) + 1e-4;
        assert!(
            est >= lo_v - tol && est <= hi_v + tol,
            "PropPercentile({}) = {est} outside DDSketch envelope [{}, {}] (tol={tol}) for vals={exp_vals:?}",
            case.quantile,
            lo_v - tol,
            hi_v + tol
        );
    }

    let got_wau: Vec<(i64, u64)> = ins_res.series[4]
        .points
        .iter()
        .map(|pt| (pt.bucket_ts_ms, pt.value.unwrap_or(0.0) as u64))
        .collect();
    assert_eq!(
        got_wau, exp_wau,
        "End-anchored WAU mismatch vs naive oracle"
    );

    // 3. Differential Retention checks (NDay, Unbounded, FrequencyStickiness)
    for (mode_str, mode_enum) in [
        ("n_day", RetentionMode::NDay),
        ("unbounded", RetentionMode::Unbounded),
        ("stickiness", RetentionMode::FrequencyStickiness),
    ] {
        let exp_ret = case
            .world
            .eval_retention_daily(query_range, "E0", "E1", 2, mode_str);
        let got_ret = qengine
            .query_retention(
                "proj_prop",
                &RetentionQuery {
                    time_range: exec_range,
                    start_event: "E0".into(),
                    return_event: "E1".into(),
                    mode: mode_enum,
                    unit: TimeInterval::Day,
                    num_periods: 2,
                    global_filter: GlobalFilter::default(),
                },
            )
            .await
            .unwrap();
        let actual_ret: Vec<(i64, Vec<u64>)> = got_ret
            .cohorts
            .into_iter()
            .map(|c| (c.cohort_start_ts_ms, c.retained_counts))
            .collect();
        assert_eq!(
            actual_ret, exp_ret,
            "Retention {mode_str} mismatch vs naive oracle"
        );
    }

    id_router.close_all().await.unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn section5_randomized_differential_oracle_proptest(case in arb_case()) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(assert_case_matches_oracle(case));
    }
}
