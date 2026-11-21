//! Closed/open-loop HTTP load generator for a running `pulse-node`.
//!
//!   cargo run --release -p pulse-node --example loadgen -- events \
//!       --url http://127.0.0.1:8080 --key pk_live_... --concurrency 32 --batch 100 \
//!       --duration 20 --rate 10000 --payload-bytes 300
//!   ... -- traces --spans 12 --payload-bytes 2000      (one agent trace per request)
//!   ... -- query  --token sk_root_... --project proj_x (insights query, 30-day range)
//!   ... -- query-cold ...                              (same, but defeats the result cache)
//!
//! `--rate` is the target in events (or traces, or queries) per second across all workers;
//! 0 means as fast as the server answers. Prints one JSON line of results.
//!
//! ponytail: open-loop pacing is per worker (sleep to the next slot), so under saturation it
//! degrades to closed-loop and under-reports queueing delay. Use a coordinated-omission-free
//! generator if you need exact tail latency at overload.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

#[derive(Clone)]
struct Opts {
    mode: String,
    url: String,
    key: String,
    token: String,
    project: String,
    concurrency: usize,
    batch: usize,
    duration: u64,
    rate: u64,
    payload_bytes: usize,
    spans: usize,
    users: u64,
}

fn parse() -> Opts {
    let mut args = std::env::args().skip(1);
    let mut o = Opts {
        mode: args.next().unwrap_or_else(|| "events".into()),
        url: "http://127.0.0.1:8080".into(),
        key: String::new(),
        token: String::new(),
        project: "proj_default".into(),
        concurrency: 16,
        batch: 100,
        duration: 10,
        rate: 0,
        payload_bytes: 300,
        spans: 12,
        users: 100_000,
    };
    while let Some(flag) = args.next() {
        let v = args
            .next()
            .unwrap_or_else(|| panic!("missing value for {flag}"));
        let n = || {
            v.parse::<u64>()
                .unwrap_or_else(|_| panic!("bad number for {flag}"))
        };
        match flag.as_str() {
            "--url" => o.url = v.clone(),
            "--key" => o.key = v.clone(),
            "--token" => o.token = v.clone(),
            "--project" => o.project = v.clone(),
            "--concurrency" => o.concurrency = n() as usize,
            "--batch" => o.batch = n() as usize,
            "--duration" => o.duration = n(),
            "--rate" => o.rate = n(),
            "--payload-bytes" => o.payload_bytes = n() as usize,
            "--spans" => o.spans = n() as usize,
            "--users" => o.users = n(),
            other => panic!("unknown flag {other}"),
        }
    }
    o
}

/// Filler that does not compress to nothing: realistic-ish mixed text.
fn filler(seed: u64, bytes: usize) -> String {
    const WORDS: [&str; 12] = [
        "checkout", "retry", "customer", "invoice", "the", "model", "returned", "tool", "search",
        "result", "error", "context",
    ];
    let mut s = String::with_capacity(bytes + 12);
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    while s.len() < bytes {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.push_str(WORDS[(x % 12) as usize]);
        s.push(' ');
    }
    s
}

fn events_body(o: &Opts, seq: u64) -> (Value, u64) {
    let now = chrono::Utc::now().timestamp_millis();
    const NAMES: [&str; 6] = [
        "Page_View",
        "Search",
        "Add_To_Cart",
        "Checkout",
        "Purchase",
        "App_Open",
    ];
    let events: Vec<Value> = (0..o.batch as u64)
        .map(|i| {
            let n = seq * o.batch as u64 + i;
            let user = n.wrapping_mul(2_654_435_761) % o.users;
            let plan = ["free", "pro", "team"][(n % 3) as usize];
            let country = ["IN", "US", "DE", "BR"][(n % 4) as usize];
            json!({
                "distinct_id": format!("user_{user}"),
                "session_id": format!("s_{user}_{}", now / 1_800_000),
                "event_name": NAMES[(n % 6) as usize],
                "client_event_ts_ms": now - (n % 1000) as i64,
                "properties": {
                    "plan": plan,
                    "country": country,
                    "amount": n % 5000,
                    "seq": n,
                    "note": filler(n, o.payload_bytes),
                }
            })
        })
        .collect();
    (json!({ "events": events }), o.batch as u64)
}

fn trace_body(o: &Opts, seq: u64, worker: usize) -> (Value, u64) {
    let now = chrono::Utc::now().timestamp_millis();
    let trace_id = format!("tr_{worker}_{seq}_{now}");
    let spans: Vec<Value> = (0..o.spans)
        .map(|i| {
            let llm = i % 3 == 1;
            let tool = i % 3 == 2;
            json!({
                "span_id": format!("{trace_id}_sp{i}"),
                "trace_id": trace_id,
                "parent_span_id": if i == 0 { Value::Null } else { json!(format!("{trace_id}_sp0")) },
                "name": if llm { "chat" } else if tool { "search_orders" } else { "plan" },
                "span_type": if llm { "llm" } else if tool { "tool" } else { "agent" },
                "status": "ok",
                "start_ts_ms": now - 4000 + (i as i64) * 300,
                "end_ts_ms": now - 3800 + (i as i64) * 300,
                "duration_ms": 200,
                "provider": if llm { json!("anthropic") } else { Value::Null },
                "model": if llm { json!("claude-sonnet-5-5") } else { Value::Null },
                "input_tokens": if llm { 1800 } else { 0 },
                "output_tokens": if llm { 350 } else { 0 },
                "tool_name": if tool { json!("search_orders") } else { Value::Null },
                "input": { "messages": [{ "role": "user", "content": filler(seq + i as u64, o.payload_bytes / 2) }] },
                "output": { "content": filler(seq * 31 + i as u64, o.payload_bytes / 2) },
                "attributes": { "step": i, "tenant": format!("acct_{}", seq % 50) },
            })
        })
        .collect();
    let body = json!({
        "traces": [{
            "trace_id": trace_id,
            "agent_name": "support-agent",
            "agent_version": format!("v{}", seq % 3),
            "environment": "production",
            "session_id": format!("sess_{}", seq % 5000),
            "user_id": format!("user_{}", seq % o.users),
            "status": "ok",
            "start_ts_ms": now - 4000,
            "end_ts_ms": now,
            "duration_ms": 4000,
            "input": { "question": filler(seq, o.payload_bytes / 2) },
            "output": { "answer": filler(seq + 7, o.payload_bytes / 2) },
            "span_count": o.spans,
        }],
        "spans": spans,
    });
    (body, 1)
}

/// `cold` shifts the range start per request so the result cache cannot answer.
fn query_body(o: &Opts, seq: u64, cold: bool) -> (Value, u64) {
    let now = chrono::Utc::now().timestamp_millis() / 60_000 * 60_000;
    let shift = if cold {
        (seq % 100_000) as i64 * 1000
    } else {
        0
    };
    let body = json!({
        "project_id": o.project,
        "kind": "insights",
        "query": {
            "time_range": { "start_ts_ms": now - 30 * 86_400_000_i64 - shift, "end_ts_ms": now + 60_000 },
            "interval": "day",
            "series": [{ "label": "A", "event_name": "Purchase", "metric": "uniques" }],
            "breakdown_by": ["country"]
        }
    });
    (body, 1)
}

#[tokio::main]
async fn main() {
    let o = parse();
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(o.concurrency)
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    let units = Arc::new(AtomicU64::new(0)); // events / traces / queries acknowledged 2xx
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let per_req = match o.mode.as_str() {
        "events" => o.batch as u64,
        _ => 1,
    };
    // Per-worker gap between requests for the target rate.
    let gap = (o.rate > 0)
        .then(|| Duration::from_secs_f64(o.concurrency as f64 * per_req as f64 / o.rate as f64));
    let deadline = Instant::now() + Duration::from_secs(o.duration);
    let started = Instant::now();

    let mut tasks = Vec::new();
    for w in 0..o.concurrency {
        let (o, client, units, bytes_sent) =
            (o.clone(), client.clone(), units.clone(), bytes_sent.clone());
        tasks.push(tokio::spawn(async move {
            let mut lat_us: Vec<u64> = Vec::new();
            let mut statuses: std::collections::BTreeMap<u16, u64> = Default::default();
            let mut seq = (w as u64) << 40;
            let mut next = Instant::now();
            while Instant::now() < deadline {
                if let Some(g) = gap {
                    tokio::time::sleep_until(next.into()).await;
                    next += g;
                }
                seq += 1;
                let (body, n) = match o.mode.as_str() {
                    "events" => events_body(&o, seq),
                    "traces" => trace_body(&o, seq, w),
                    "query" => query_body(&o, seq, false),
                    "query-cold" => query_body(&o, seq, true),
                    m => panic!("unknown mode {m} (events | traces | query | query-cold)"),
                };
                let raw = serde_json::to_vec(&body).unwrap();
                bytes_sent.fetch_add(raw.len() as u64, Ordering::Relaxed);
                let req = match o.mode.as_str() {
                    "events" => client
                        .post(format!("{}/v1/ingest", o.url))
                        .header("x-pulse-key", &o.key),
                    "traces" => client
                        .post(format!("{}/v1/agent/traces", o.url))
                        .header("x-pulse-key", &o.key),
                    _ => client
                        .post(format!("{}/v1/query", o.url))
                        .bearer_auth(&o.token),
                };
                let t0 = Instant::now();
                let status = match req
                    .header("content-type", "application/json")
                    .body(raw)
                    .send()
                    .await
                {
                    Ok(resp) => {
                        let s = resp.status().as_u16();
                        let _ = resp.bytes().await;
                        s
                    }
                    Err(_) => 0, // connect error / timeout
                };
                lat_us.push(t0.elapsed().as_micros() as u64);
                *statuses.entry(status).or_default() += 1;
                if (200..300).contains(&status) {
                    units.fetch_add(n, Ordering::Relaxed);
                }
            }
            (lat_us, statuses)
        }));
    }

    let mut lat: Vec<u64> = Vec::new();
    let mut statuses: std::collections::BTreeMap<u16, u64> = Default::default();
    for t in tasks {
        let (l, s) = t.await.unwrap();
        lat.extend(l);
        for (k, v) in s {
            *statuses.entry(k).or_default() += v;
        }
    }
    let secs = started.elapsed().as_secs_f64();
    lat.sort_unstable();
    let pct = |p: f64| -> f64 {
        if lat.is_empty() {
            return 0.0;
        }
        lat[((lat.len() - 1) as f64 * p) as usize] as f64 / 1000.0
    };
    let ok = units.load(Ordering::Relaxed);
    println!(
        "{}",
        json!({
            "mode": o.mode, "concurrency": o.concurrency, "batch": per_req, "target_rate": o.rate,
            "payload_bytes": o.payload_bytes, "spans": if o.mode == "traces" { o.spans } else { 0 },
            "secs": (secs * 10.0).round() / 10.0,
            "requests": lat.len(),
            "ok_units": ok,
            "units_per_sec": (ok as f64 / secs).round(),
            "mb_per_sec": ((bytes_sent.load(Ordering::Relaxed) as f64 / secs / 1e6) * 100.0).round() / 100.0,
            "avg_req_bytes": bytes_sent.load(Ordering::Relaxed) / lat.len().max(1) as u64,
            "latency_ms": { "p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99), "max": pct(1.0) },
            "status": statuses.iter().map(|(k, v)| (k.to_string(), json!(v))).collect::<serde_json::Map<_, _>>(),
        })
    );
}
