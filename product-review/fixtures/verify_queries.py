"""Run the review's correctness checks against a Pulse node loaded with gen_ecommerce.py data.
Usage: python3 verify_queries.py <base_url> <admin_token> <project_id>
Prints one PASS/FAIL line per check plus the raw numbers.
"""
import json, sys, urllib.request
from datetime import datetime, timezone

B, TOK, P = sys.argv[1:4]
def post(path, body):
    r = urllib.request.Request(B + path, json.dumps(body).encode(),
        {"Authorization": f"Bearer {TOK}", "Content-Type": "application/json"})
    try:
        return json.load(urllib.request.urlopen(r))
    except urllib.error.HTTPError as e:
        return {"__error__": e.code, "body": e.read().decode()[:300]}

ms = lambda *a: int(datetime(*a, tzinfo=timezone.utc).timestamp() * 1000)
TR = {"start_ts_ms": ms(2026, 10, 25), "end_ts_ms": ms(2026, 11, 19)}
STEPS = ["User Signed Up", "Product Viewed", "Product Added to Cart", "Checkout Started", "Payment Completed"]
results = []
def check(name, got, want):
    ok = got == want
    results.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name}: got {got!r}, expected {want!r}")

def funnel(window_d, **kw):
    q = {"time_range": TR, "steps": [{"event_name": s} for s in STEPS],
         "conversion_window_ms": window_d * 86400000, "order_mode": "loose_order", **kw}
    return post(f"/v1/projects/{P}/query/funnel", q)

f7 = funnel(7)
steps = f7.get("overall_steps")
if steps is None: print("RAW funnel:", json.dumps(f7)[:600])
check("loose funnel 7d step counts", [s["entered_users"] for s in steps], [100, 80, 50, 30, 20])
f14 = funnel(14)
s14 = f14.get("overall_steps")
check("loose funnel 14d last step", s14[-1]["entered_users"], 25)
fs = funnel(7, order_mode="strict_order")
print("INFO  strict funnel:", [s["entered_users"] for s in (fs.get("overall_steps") or [])])
fb = funnel(7, breakdown_by="plan")
groups = {g["breakdown_value"]: [s["entered_users"] for s in g["steps"]] for g in fb.get("breakdowns") or []}
if not groups: print("RAW breakdown keys:", list(fb.keys()))
check("funnel by plan", groups, {"premium": [50, 40, 25, 15, 10], "free": [50, 40, 25, 15, 10]})

def insight(event, metric, **kw):
    q = {"time_range": TR, "interval": "day", "series": [{"label": "A", "event_name": event, "metric": metric, **kw.pop("series_kw", {})}], **kw}
    return post(f"/v1/projects/{P}/query/insights", q)

def total(res):
    s = (res.get("series") or [{}])[0]
    for k in ("overall_value",):
        if k in s: return s[k]
    pts = s.get("points") or s.get("data") or s.get("values") or []
    return sum((p.get("value", 0) if isinstance(p, dict) else p[1]) for p in pts)

r = insight("Payment Completed", "totals"); check("Payment Completed total events", total(r), 30)
r = insight("Payment Completed", "uniques"); check("Payment Completed uniques (sum of daily)", total(r), 30)
r = insight("Product Viewed", "totals"); check("Product Viewed total events (after dedup)", total(r), 90)
r = insight("Product Viewed", "uniques"); print("INFO  Product Viewed daily-uniques sum:", total(r), "(80 if anon merged; anon views are 1 min earlier same day)")
r = insight("Checkout Started", "totals", series_kw={"filters": [{"prop_name": "coupon", "filter": {"op": "neq", "val": "SAVE10"}}]})
check("coupon != SAVE10 incl. missing/null", total(r), 20)
r = insight("Checkout Started", "totals", series_kw={"filters": [{"prop_name": "coupon", "filter": {"op": "is_set"}}]})
print("INFO  coupon is_set count (10 SAVE10 + 5 explicit null):", total(r))
r = insight("Payment Completed", {"type": "prop_sum", "prop": "amount"})
print("INFO  sum(amount) expected 30*499=14970 (3 sent as strings):", total(r))
r = insight("TZ Probe", "totals")
pts = [p for p in (r.get("series") or [{}])[0].get("points", []) if (p.get("value") if isinstance(p, dict) else p[1])]
print("INFO  TZ Probe non-zero buckets (expect 2026-11-13 in IST):", json.dumps(pts)[:300])
r = insight("Future Event", "totals", time_range={"start_ts_ms": ms(2027, 5, 30), "end_ts_ms": ms(2027, 6, 2)})
print("INFO  2027 future event queryable:", total(r))
r = insight("Payment Completed", "uniques", breakdown_by=["utm_source"])
print("INFO  breakdown utm_source:", json.dumps(r)[:400])

ret = post(f"/v1/projects/{P}/query/retention", {"time_range": TR, "start_event": "User Signed Up",
      "return_event": "App Opened", "num_periods": 8, "mode": "n_day", "unit": "day"})
rows = ret.get("cohorts") or ret.get("rows") or []
if not rows: print("RAW retention:", json.dumps(ret)[:500])
size = sum(c["cohort_size"] for c in rows)
d1 = sum(c["retained_counts"][1] for c in rows if len(c["retained_counts"]) > 1)
d7 = sum(c["retained_counts"][7] for c in rows if len(c["retained_counts"]) > 7)
check("retention cohort size", size, 100)
check("retention day1 returners", d1, 40)
check("retention day7 returners (cohorts with 7 days elapsed)", d7, 20)
print(f"\n{sum(results)}/{len(results)} checks passed")
