//! Release gate for analytics correctness: boots a full node, loads a dataset with
//! hand-computed answers (the product review's e-commerce fixture plus 1,200 user profiles),
//! and checks every answer before and after a restart. Each assertion names the review
//! finding it guards (B-n / I-n in product-improvements/validated-findings.md).

use chrono::{TimeZone, Utc};
use pulse_node::{start_pulse_node, NodeConfig, NodeRole, RunningNode};
use serde_json::{json, Value};
use std::path::Path;

const ADMIN: &str = "sk_root_correctnesstest00000000000000000000000000";
const D: i64 = 86_400_000;
const SOURCES: [&str; 4] = ["google", "facebook", "organic", "referral"];
const MIN: i64 = 60_000;
const STEPS: [&str; 5] = [
    "User Signed Up",
    "Product Viewed",
    "Product Added to Cart",
    "Checkout Started",
    "Payment Completed",
];

fn ms(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> i64 {
    Utc.with_ymd_and_hms(y, mo, d, h, mi, 0)
        .unwrap()
        .timestamp_millis()
}

async fn boot(dir: &Path) -> RunningNode {
    start_pulse_node(NodeConfig {
        role: NodeRole::All,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        quic_addr: None,
        data_dir: dir.join("node"),
        web_dist_dir: dir.join("no_web"),
        default_project_id: "proj_default".into(),
        default_api_key: String::new(),
        admin_token: Some(ADMIN.into()),
        allowed_origins: vec![],
        auto_seed: false,
        num_buckets: 8,
    })
    .await
    .expect("start node")
}

/// The review's fixture (product-review/fixtures/gen_ecommerce.py), generated in Rust.
fn ecommerce_events() -> Vec<Value> {
    let base = ms(2026, 11, 1, 10, 0);
    let mut out = Vec::new();
    let mut ev = |name: &str, uid: &str, t: i64, props: Value, extra: Value| {
        let mut e = json!({"event": name, "userId": uid, "timestamp": t, "properties": props});
        if let Value::Object(x) = extra {
            e.as_object_mut().unwrap().extend(x);
        }
        out.push(e);
    };
    let common = |i: usize| {
        json!({
            "plan": if i % 2 == 0 { "premium" } else { "free" },
            "utm_source": SOURCES[i % 4],
        })
    };
    let with = |mut a: Value, b: Value| {
        a.as_object_mut()
            .unwrap()
            .extend(b.as_object().unwrap().clone());
        a
    };
    for i in 0..100usize {
        let u = format!("u{i:03}");
        let t0 = base + (i % 10) as i64 * D;
        ev("User Signed Up", &u, t0, common(i), json!({}));
        if i < 80 {
            let p = with(common(i), json!({"sku": format!("sku_{}", i % 5)}));
            ev("Product Viewed", &u, t0 + 5 * MIN, p, json!({}));
        }
        if i < 50 {
            ev(
                "Product Added to Cart",
                &u,
                t0 + 10 * MIN,
                common(i),
                json!({}),
            );
        }
        if i < 30 {
            let p = match i {
                0..=9 => with(common(i), json!({"coupon": "SAVE10"})),
                10..=14 => with(common(i), json!({"coupon": null})),
                _ => common(i),
            };
            ev("Checkout Started", &u, t0 + 15 * MIN, p, json!({}));
        }
        if i < 20 {
            let amt = if i >= 3 { json!(499) } else { json!("499") };
            let p = with(common(i), json!({"amount": amt}));
            let id = json!({"messageId": format!("pay-{u}")});
            ev(
                "Payment Completed",
                &u,
                t0 + 20 * MIN,
                p.clone(),
                id.clone(),
            );
            if i < 10 {
                ev("Payment Completed", &u, t0 + 20 * MIN, p, id); // retry, same id
            }
        }
        if (20..25).contains(&i) {
            let p = with(common(i), json!({"amount": 499}));
            ev("Payment Completed", &u, t0 + 8 * D, p, json!({}));
        }
        if (90..95).contains(&i) {
            let p = with(common(i), json!({"amount": 499}));
            ev("Payment Completed", &u, t0 + 30 * MIN, p, json!({}));
        }
        if i < 40 {
            ev("App Opened", &u, t0 + D, json!({}), json!({}));
        }
        if i < 20 {
            ev("App Opened", &u, t0 + 7 * D, json!({}), json!({}));
        }
        if (10..15).contains(&i) {
            // identical retry without an id: content-hash dedup
            let p = with(common(i), json!({"sku": format!("sku_{}", i % 5)}));
            ev("Product Viewed", &u, t0 + 5 * MIN, p, json!({}));
        }
        if i < 10 {
            // 20:00 UTC Nov 12 == 01:30 IST Nov 13
            ev(
                "TZ Probe",
                &u,
                ms(2026, 11, 12, 20, 0),
                json!({}),
                json!({}),
            );
        }
    }
    for i in 60..70usize {
        let t0 = base + (i % 10) as i64 * D;
        out.push(
            json!({"event": "Product Viewed", "anonymousId": format!("anon_{i:03}"),
            "timestamp": t0 - MIN, "properties": {"sku": "sku_anon"}}),
        );
    }
    out
}

struct Api {
    http: reqwest::Client,
    base: String,
    pid: String,
    key: String,
}

impl Api {
    async fn admin(&self, path: &str, body: Value) -> (u16, Value) {
        let r = self
            .http
            .post(format!("{}{path}", self.base))
            .bearer_auth(ADMIN)
            .json(&body)
            .send()
            .await
            .unwrap();
        let st = r.status().as_u16();
        (st, r.json().await.unwrap_or(Value::Null))
    }
    async fn ingest(&self, path: &str, body: Value) -> (u16, Value) {
        let r = self
            .http
            .post(format!("{}{path}", self.base))
            .header("x-pulse-key", &self.key)
            .json(&body)
            .send()
            .await
            .unwrap();
        let st = r.status().as_u16();
        (st, r.json().await.unwrap_or(Value::Null))
    }
    fn tr() -> Value {
        json!({"start_ts_ms": ms(2026, 10, 25, 0, 0), "end_ts_ms": ms(2026, 11, 25, 0, 0)})
    }
    async fn funnel(&self, window_days: i64, extra: Value) -> Value {
        let mut q = json!({"time_range": Self::tr(),
            "steps": STEPS.iter().map(|s| json!({"event_name": s})).collect::<Vec<_>>(),
            "conversion_window_ms": window_days * D, "order_mode": "loose_order"});
        q.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let (st, v) = self
            .admin(&format!("/v1/projects/{}/query/funnel", self.pid), q)
            .await;
        assert_eq!(st, 200, "funnel: {v}");
        v
    }
    async fn insight(
        &self,
        event: &str,
        metric: Value,
        series_extra: Value,
        extra: Value,
    ) -> Value {
        let mut series = json!({"label": "A", "event_name": event, "metric": metric});
        series
            .as_object_mut()
            .unwrap()
            .extend(series_extra.as_object().unwrap().clone());
        let mut q = json!({"time_range": Self::tr(), "interval": "day", "series": [series]});
        q.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let (st, v) = self
            .admin(&format!("/v1/projects/{}/query/insights", self.pid), q)
            .await;
        assert_eq!(st, 200, "insights: {v}");
        v
    }
    /// Calls an MCP tool and returns its parsed JSON text; panics on an MCP error.
    async fn mcp(&self, tool: &str, args: Value) -> Value {
        let v: Value = self
            .http
            .post(format!("{}/v1/mcp", self.base))
            .bearer_auth(ADMIN)
            .header("x-pulse-project", &self.pid)
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {"name": tool, "arguments": args}}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_ne!(v["result"]["isError"], json!(true), "{tool}: {v}");
        serde_json::from_str(v["result"]["content"][0]["text"].as_str().unwrap()).unwrap()
    }
    async fn total(&self, event: &str, metric: Value, filters: Value) -> f64 {
        let v = self
            .insight(event, metric, json!({"filters": filters}), json!({}))
            .await;
        v["series"][0]["overall_value"].as_f64().unwrap_or(-1.0)
    }
}

fn entered(steps: &Value) -> Vec<u64> {
    steps
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["entered_users"].as_u64().unwrap())
        .collect()
}

async fn check_answers(api: &Api, phase: &str) {
    // Funnels
    let f7 = api.funnel(7, json!({})).await;
    assert_eq!(
        entered(&f7["overall_steps"]),
        [100, 80, 50, 30, 20],
        "{phase}: funnel 7d"
    );
    // P1-3: the full drop-off list has exactly the users the funnel counts as lost.
    let q = json!({"time_range": Api::tr(),
        "steps": STEPS.iter().map(|s| json!({"event_name": s})).collect::<Vec<_>>(),
        "conversion_window_ms": 7 * D, "order_mode": "loose_order"});
    let (st, dropped) = api
        .admin(
            &format!(
                "/v1/projects/{}/query/funnel/dropped?step=2&format=json",
                api.pid
            ),
            q,
        )
        .await;
    assert_eq!(st, 200, "{dropped}");
    let mut ids: Vec<&str> = dropped["users"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u.as_str().unwrap())
        .collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 30, "{phase}: dropped before step 3: {dropped}");
    let f14 = api.funnel(14, json!({})).await;
    assert_eq!(
        entered(&f14["overall_steps"])[4],
        25,
        "{phase}: funnel 14d last step"
    );
    let fb = api.funnel(7, json!({"breakdown_by": "plan"})).await;
    for g in fb["breakdowns"].as_array().unwrap() {
        assert_eq!(
            entered(&g["steps"]),
            [50, 40, 25, 15, 10],
            "{phase}: funnel by plan"
        );
    }

    // Totals, dedup, identity merge
    let pc = api
        .total("Payment Completed", json!("totals"), json!([]))
        .await;
    assert_eq!(pc, 30.0, "{phase}: messageId dedup");
    let pv = api
        .total("Product Viewed", json!("totals"), json!([]))
        .await;
    assert_eq!(pv, 90.0, "{phase}: content-hash dedup");
    let pvu = api
        .total("Product Viewed", json!("uniques"), json!([]))
        .await;
    assert_eq!(pvu, 80.0, "{phase}: anonymous views merged into users");

    // Null / missing semantics
    let neq = json!([{"prop_name": "coupon", "filter": {"op": "neq", "val": "SAVE10"}}]);
    let c = api.total("Checkout Started", json!("totals"), neq).await;
    assert_eq!(c, 20.0, "{phase}: neq counts missing and null");

    // B-5: equality coerces the same way on every code path
    for val in [json!(499), json!("499"), json!("499.0")] {
        let f = json!([{"prop_name": "amount", "filter": {"op": "eq", "val": val}}]);
        let n = api.total("Payment Completed", json!("totals"), f).await;
        assert_eq!(n, 30.0, "{phase}: amount eq {val}");
    }

    // Retention
    let (st, ret) = api
        .admin(
            &format!("/v1/projects/{}/query/retention", api.pid),
            json!({"time_range": Api::tr(), "start_event": "User Signed Up",
                   "return_event": "App Opened", "num_periods": 8, "mode": "n_day", "unit": "day"}),
        )
        .await;
    assert_eq!(st, 200, "{ret}");
    let rows = ret["cohorts"].as_array().unwrap();
    let sum_at = |i: usize| -> u64 {
        rows.iter()
            .filter_map(|c| c["retained_counts"].get(i).and_then(Value::as_u64))
            .sum()
    };
    let size: u64 = rows
        .iter()
        .map(|c| c["cohort_size"].as_u64().unwrap())
        .sum();
    assert_eq!(
        (size, sum_at(1), sum_at(7)),
        (100, 40, 20),
        "{phase}: retention"
    );

    // Project time zone (Asia/Kolkata): day bucket and, B-10, hour bucket on the local clock
    let day = api
        .insight("TZ Probe", json!("totals"), json!({}), json!({}))
        .await;
    let day_buckets: Vec<i64> = nonzero_buckets(&day);
    assert_eq!(
        day_buckets,
        [ms(2026, 11, 12, 18, 30)],
        "{phase}: IST day bucket"
    );
    let hour = api
        .insight(
            "TZ Probe",
            json!("totals"),
            json!({}),
            json!({"interval": "hour"}),
        )
        .await;
    assert_eq!(
        nonzero_buckets(&hour),
        [ms(2026, 11, 12, 19, 30)],
        "{phase}: IST hour bucket starts at local :00"
    );

    // B-1: user properties across all 1,200 profiles
    let gold = json!([{"prop_name": "user.tier", "filter": {"op": "eq", "val": "gold"}}]);
    let n = api.total("Login", json!("uniques"), gold).await;
    assert_eq!(n, 100.0, "{phase}: user.tier = gold");
    let bd = api
        .insight(
            "Login",
            json!("uniques"),
            json!({}),
            json!({"breakdown_by": ["user.tier"]}),
        )
        .await;
    let mut groups: Vec<(String, f64)> = bd["series"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| {
            (
                s["breakdown_key"].as_str().unwrap_or("?").to_string(),
                s["overall_value"].as_f64().unwrap_or(0.0),
            )
        })
        .collect();
    groups.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        groups,
        [
            ("basic".to_string(), 1099.0),
            ("gold".to_string(), 100.0),
            ("platinum".to_string(), 1.0)
        ],
        "{phase}: breakdown by user.tier has no (none) bucket"
    );
    // B-3: newest write wins
    let upg = json!([{"prop_name": "user.tier", "filter": {"op": "eq", "val": "platinum"}}]);
    assert_eq!(
        api.total("Login", json!("uniques"), upg).await,
        1.0,
        "{phase}: later $set overrides earlier"
    );
    // B-3: anonymous traits follow the user after identify
    let refd = json!([{"prop_name": "user.referrer", "filter": {"op": "eq", "val": "podcast"}}]);
    assert_eq!(
        api.total("Login", json!("uniques"), refd).await,
        1.0,
        "{phase}: anonymous $set merged on identify"
    );

    // B-2: search covers every profile
    let r: Value = api
        .http
        .get(format!(
            "{}/v1/projects/{}/users?search=p0005",
            api.base, api.pid
        ))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ids: Vec<&str> = r
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["distinct_id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"p0005"), "{phase}: search found {ids:?}");

    // Drop-off lists only carry masked ids; `?uid=` must still find the user.
    let get = |path: String| {
        let req = api
            .http
            .get(format!("{}{path}", api.base))
            .bearer_auth(ADMIN);
        async move { req.send().await.unwrap().json::<Value>().await.unwrap() }
    };
    let by_id = get(format!("/v1/projects/{}/users/p0700", api.pid)).await;
    let uid = by_id["canonical_uid"].as_u64().unwrap();
    assert!(uid > 0, "{phase}: {by_id}");
    let by_uid = get(format!("/v1/projects/{}/users/masked?uid={uid}", api.pid)).await;
    assert_eq!(by_uid["canonical_uid"].as_u64(), Some(uid), "{phase}");
    assert_eq!(
        by_uid["events"].as_array().map(|e| e.len()),
        by_id["events"].as_array().map(|e| e.len()),
        "{phase}: user detail by uid"
    );
    assert!(!by_uid["events"].as_array().unwrap().is_empty(), "{phase}");

    // B-4: imported history keeps its own timestamp
    let imp = api
        .insight(
            "Legacy Purchase",
            json!("totals"),
            json!({}),
            json!({"time_range": {"start_ts_ms": ms(2026, 11, 2, 0, 0), "end_ts_ms": ms(2026, 11, 3, 0, 0)}}),
        )
        .await;
    assert_eq!(
        imp["series"][0]["overall_value"].as_f64(),
        Some(1.0),
        "{phase}: import with sentAt not restamped"
    );

    // M-7: MCP tools an agent needs to answer follow-ups.
    let range = json!({"start_ts_ms": ms(2026, 10, 25, 0, 0), "end_ts_ms": ms(2026, 11, 25, 0, 0)});
    let mut a = json!({"property": "user.tier", "event_name": "Login"});
    a.as_object_mut()
        .unwrap()
        .extend(range.as_object().unwrap().clone());
    let vals = api.mcp("list_property_values", a).await;
    let got: Vec<(String, f64)> = vals["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            (
                v["value"].as_str().unwrap().to_string(),
                v["events"].as_f64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        got,
        [
            ("basic".into(), 1099.0),
            ("gold".into(), 100.0),
            ("platinum".into(), 1.0)
        ],
        "{phase}: list_property_values"
    );
    let user = api.mcp("get_user", json!({"distinct_id": "p0700"})).await;
    assert_eq!(
        user["properties"]["tier"],
        json!("basic"),
        "{phase}: {user}"
    );
    assert_eq!(
        user["properties"]["referrer"],
        json!("podcast"),
        "{phase}: {user}"
    );
    assert_eq!(
        user["recent_events"].as_array().map(|e| e.len()),
        Some(1),
        "{phase}: {user}"
    );
    let mut a = json!({"anchor_event": "User Signed Up", "steps": 2});
    a.as_object_mut()
        .unwrap()
        .extend(range.as_object().unwrap().clone());
    let flows = api.mcp("query_flows", a).await;
    assert!(
        flows.to_string().contains("Product Viewed"),
        "{phase}: flows after signup include Product Viewed: {flows}"
    );
    let cohorts = api.mcp("list_cohorts", json!({})).await;
    assert!(cohorts["cohorts"].is_array(), "{phase}: {cohorts}");

    // B-12: er1's pre-erasure event is hidden; their event after signing up again counts.
    assert_eq!(
        api.total("Erase Probe", json!("totals"), json!([])).await,
        2.0,
        "{phase}: erased history hidden, re-signup and control counted"
    );
}

fn nonzero_buckets(v: &Value) -> Vec<i64> {
    v["series"][0]["points"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|p| p["value"].as_f64().unwrap_or(0.0) > 0.0)
        .map(|p| p["bucket_ts_ms"].as_i64().unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn analytics_answers_match_known_dataset_before_and_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let node = boot(tmp.path()).await;
    let http = reqwest::Client::new();
    let base = format!("http://{}", node.http_addr);

    // B-9: unknown time zones are refused
    let bad = http
        .post(format!("{base}/v1/projects"))
        .bearer_auth(ADMIN)
        .json(&json!({"name": "Mars", "timezone": "Mars/Olympus"}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status().as_u16(), 400);
    let created: Value = http
        .post(format!("{base}/v1/projects"))
        .bearer_auth(ADMIN)
        .json(&json!({"name": "Shop", "timezone": "Asia/Kolkata"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let api = Api {
        http: http.clone(),
        base: base.clone(),
        pid: created["project"]["id"].as_str().unwrap().to_string(),
        key: created["api_key"]["plaintext_key"]
            .as_str()
            .unwrap_or_else(|| panic!("no key in {created}"))
            .to_string(),
    };

    let import_path = format!("/v1/projects/{}/import", api.pid);

    // Ingest the fixture and link anonymous browsing to the users.
    let (st, res) = api
        .ingest("/v1/ingest", json!({"events": ecommerce_events()}))
        .await;
    assert_eq!(st, 200, "{res}");
    assert_eq!(res["deduped"].as_u64(), Some(15), "{res}");
    for i in 60..70 {
        let (st, r) = api
            .ingest(
                "/v1/identify",
                json!({"anon_id": format!("anon_{i:03}"), "user_id": format!("u{i:03}")}),
            )
            .await;
        assert_eq!(st, 200, "{r}");
    }

    // 1,200 profiles: the first 100 written are gold, so a newest-1,000 window misses them.
    let t = ms(2026, 11, 14, 9, 0);
    for chunk in (0..1200).collect::<Vec<_>>().chunks(200) {
        let profiles: Vec<Value> = chunk
            .iter()
            .map(|i| {
                json!({"distinct_id": format!("p{i:04}"),
                "$set": {"tier": if *i < 100 { "gold" } else { "basic" }}})
            })
            .collect();
        let (st, r) = api.ingest("/v1/engage", json!(profiles)).await;
        assert_eq!(st, 200, "{r}");
        let logins: Vec<Value> = chunk
            .iter()
            .map(|i| json!({"event": "Login", "userId": format!("p{i:04}"), "timestamp": t}))
            .collect();
        let (st, r) = api.ingest("/v1/ingest", json!({"events": logins})).await;
        assert_eq!(st, 200, "{r}");
    }
    // A later $set on one basic user wins over the earlier value.
    api.ingest(
        "/v1/engage",
        json!({"distinct_id": "p0500", "$set": {"tier": "platinum"}}),
    )
    .await;
    // Traits set while anonymous, then identify: they must show on the user (p0700, basic).
    api.ingest(
        "/v1/engage",
        json!({"distinct_id": "anon_p0700", "$set": {"referrer": "podcast"}}),
    )
    .await;
    let (st, r) = api
        .ingest(
            "/v1/identify",
            json!({"anon_id": "anon_p0700", "user_id": "p0700"}),
        )
        .await;
    assert_eq!(st, 200, "{r}");

    // B-4: a backfill whose sentAt is also in the past keeps its timestamp.
    let (st, r) = api.admin(&import_path, legacy_import()).await;
    assert_eq!(st, 200, "{r}");
    assert_eq!(r["accepted"].as_u64(), Some(1), "{r}");

    // B-12: erasure hides what the person did before it, not what they do after signing up
    // again with the same id. er2 is a control that is never erased.
    let (st, r) = api
        .ingest(
            "/v1/ingest",
            json!({"events": [
                {"event": "Erase Probe", "userId": "er1", "timestamp": t},
                {"event": "Erase Probe", "userId": "er2", "timestamp": t},
            ]}),
        )
        .await;
    assert_eq!(st, 200, "{r}");
    let (st, r) = api.admin("/v1/admin/flush", json!({})).await;
    assert_eq!(st, 200, "flush: {r}");
    let del = http
        .delete(format!("{base}/v1/projects/{}/users/er1", api.pid))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap();
    assert_eq!(del.status().as_u16(), 200);
    let resignup = Utc::now().timestamp_millis() + 1;
    let (st, r) = api
        .ingest(
            "/v1/ingest",
            json!({"events": [{"event": "Erase Probe", "userId": "er1", "timestamp": resignup}]}),
        )
        .await;
    assert_eq!(st, 200, "{r}");

    // I-8 / I-11: partial success, and Segment calls in a batch.
    let (st, r) = api
        .ingest(
            "/v1/ingest",
            json!({"batch": [
                {"event": "Batch OK", "userId": "b1", "timestamp": t},
                {"userId": "b2", "timestamp": t},
                {"type": "identify", "userId": "seg_user", "anonymousId": "seg_anon", "traits": {"segment_plan": "pro"}},
                {"type": "page", "name": "Home", "userId": "b4", "timestamp": t},
                {"event": "x".repeat(300), "userId": "b5", "timestamp": t},
            ]}),
        )
        .await;
    assert_eq!(st, 200, "{r}");
    assert_eq!(r["accepted"].as_u64(), Some(2), "{r}");
    let rejected: Vec<u64> = r["rejected"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["index"].as_u64().unwrap())
        .collect();
    assert_eq!(rejected, [1, 4], "{r}");
    assert_eq!(r["profiles_updated"].as_u64(), Some(1), "{r}");
    // The Segment identify linked seg_anon to seg_user and set its traits: an event the
    // anonymous id sends is counted for a user with segment_plan = pro.
    api.ingest(
        "/v1/ingest",
        json!({"events": [{"event": "Seg Probe", "anonymousId": "seg_anon", "timestamp": t}]}),
    )
    .await;
    let seg = json!([{"prop_name": "user.segment_plan", "filter": {"op": "eq", "val": "pro"}}]);
    assert_eq!(
        api.total("Seg Probe", json!("uniques"), seg).await,
        1.0,
        "Segment identify in a batch applies traits and the merge"
    );
    let pv = api.total("$pageview", json!("totals"), json!([])).await;
    assert_eq!(pv, 1.0, "Segment page becomes $pageview");

    // B-8: MCP refuses unknown event names and suggests the right one.
    let mcp: Value = http
        .post(format!("{base}/v1/mcp"))
        .bearer_auth(ADMIN)
        .header("x-pulse-project", &api.pid)
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "query_insights", "arguments": {"event_name": "Payment Complete"}}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mcp["result"]["isError"], json!(true), "{mcp}");
    let msg = mcp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(msg.contains("Payment Completed"), "{msg}");

    // P1-5: annotations are stored, listed by range, and deleted.
    let (st, ann) = api
        .admin(
            &format!("/v1/projects/{}/annotations", api.pid),
            json!({"ts_ms": ms(2026, 11, 10, 12, 0), "text": "v2.3 released"}),
        )
        .await;
    assert_eq!(st, 200, "{ann}");
    let list = |start: i64, end: i64| {
        let http = http.clone();
        let url = format!(
            "{base}/v1/projects/{}/annotations?start_ts_ms={start}&end_ts_ms={end}",
            api.pid
        );
        async move {
            let v: Value = http
                .get(url)
                .bearer_auth(ADMIN)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            v.as_array().unwrap().len()
        }
    };
    assert_eq!(list(ms(2026, 11, 9, 0, 0), ms(2026, 11, 11, 0, 0)).await, 1);
    assert_eq!(
        list(ms(2026, 11, 11, 0, 0), ms(2026, 11, 12, 0, 0)).await,
        0
    );
    let del = http
        .delete(format!(
            "{base}/v1/projects/{}/annotations/{}",
            api.pid,
            ann["id"].as_str().unwrap()
        ))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap();
    assert_eq!(del.status().as_u16(), 200);
    assert_eq!(list(ms(2026, 11, 9, 0, 0), ms(2026, 11, 11, 0, 0)).await, 0);

    check_answers(&api, "live").await;

    // Restart on the same data: everything must still hold from disk.
    node.shutdown().await;
    let node = boot(tmp.path()).await;
    let api = Api {
        base: format!("http://{}", node.http_addr),
        ..api
    };
    check_answers(&api, "after restart").await;

    // I-18: re-running the same backfill adds nothing, even after a restart cleared the
    // in-memory dedup window.
    let (st, r) = api.admin(&import_path, legacy_import()).await;
    assert_eq!(st, 200, "{r}");
    assert_eq!(
        r["accepted"].as_u64(),
        Some(0),
        "re-import after restart: {r}"
    );
    assert_eq!(
        r["deduped"].as_u64(),
        Some(1),
        "re-import after restart: {r}"
    );
    check_answers(&api, "after re-import").await;
    node.shutdown().await;
}

fn legacy_import() -> Value {
    json!({"events": [{"event": "Legacy Purchase", "userId": "u001",
        "timestamp": ms(2026, 11, 2, 12, 0), "sentAt": ms(2026, 11, 2, 12, 0) + 1000}]})
}
