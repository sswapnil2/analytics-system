"""Deterministic e-commerce test dataset for the Pulse product review.

Writes events.json (a Pulse {"events": [...]} batch), identify.json (anon->user merges)
and expected.json (hand-computed answers the queries must reproduce).
Usage: python3 gen_ecommerce.py <out_dir>
"""
import json, sys
from datetime import datetime, timedelta, timezone

out = sys.argv[1]
H = 3600_000
D = 24 * H
def ts(dt): return int(dt.timestamp() * 1000)
base = datetime(2026, 11, 1, 10, 0, tzinfo=timezone.utc)

events, identify = [], []
def ev(name, uid, t, props=None, **extra):
    e = {"event": name, "userId": uid, "timestamp": t, "properties": props or {}}
    e.update(extra)
    events.append(e)

for i in range(100):
    u = f"u{i:03d}"
    plan = "premium" if i % 2 == 0 else "free"
    src = ["google", "facebook", "organic", "referral"][i % 4]
    t0 = ts(base + timedelta(days=i % 10))
    common = {"plan": plan, "utm_source": src}
    ev("User Signed Up", u, t0, common)
    if i < 80: ev("Product Viewed", u, t0 + 5 * 60_000, {**common, "sku": f"sku_{i%5}"})
    if i < 50: ev("Product Added to Cart", u, t0 + 10 * 60_000, common)
    if i < 30:
        p = dict(common)
        if i < 10: p["coupon"] = "SAVE10"
        elif i < 15: p["coupon"] = None
        ev("Checkout Started", u, t0 + 15 * 60_000, p)
    if i < 20:
        amt = 499 if i >= 3 else "499"  # 3 events send amount as a string
        ev("Payment Completed", u, t0 + 20 * 60_000, {**common, "amount": amt}, messageId=f"pay-{u}")
    # late payers: checkout-only users who pay 8 days later (outside a 7d window)
    if 20 <= i < 25: ev("Payment Completed", u, t0 + 8 * D, {**common, "amount": 499})
    # out-of-order users: pay without ever viewing / carting / checking out
    if 90 <= i < 95: ev("Payment Completed", u, t0 + 30 * 60_000, {**common, "amount": 499})
    # retention return event
    if i < 40: ev("App Opened", u, t0 + 1 * D)
    if i < 20: ev("App Opened", u, t0 + 7 * D)

# duplicate retries WITH messageId (should be dropped)
for i in range(10):
    u = f"u{i:03d}"; t0 = ts(base + timedelta(days=i % 10))
    ev("Payment Completed", u, t0 + 20 * 60_000, {"plan": "premium" if i % 2 == 0 else "free",
       "utm_source": ["google", "facebook", "organic", "referral"][i % 4],
       "amount": 499 if i >= 3 else "499"}, messageId=f"pay-{u}")
# duplicate retries WITHOUT messageId, identical content (content-hash dedup)
for i in range(10, 15):
    u = f"u{i:03d}"; t0 = ts(base + timedelta(days=i % 10))
    plan = "premium" if i % 2 == 0 else "free"; src = ["google", "facebook", "organic", "referral"][i % 4]
    ev("Product Viewed", u, t0 + 5 * 60_000, {"plan": plan, "utm_source": src, "sku": f"sku_{i%5}"})

# anonymous pre-signup browsing for u060..u069, merged later via /v1/identify
for i in range(60, 70):
    t0 = ts(base + timedelta(days=i % 10))
    events.append({"event": "Product Viewed", "anonymousId": f"anon_{i:03d}", "timestamp": t0 - 60_000,
                   "properties": {"sku": "sku_anon"}})
    identify.append({"anon_id": f"anon_{i:03d}", "user_id": f"u{i:03d}"})

# timezone probe: 20:00 UTC on Nov 12 == 01:30 IST on Nov 13
for i in range(10):
    ev("TZ Probe", f"u{i:03d}", ts(datetime(2026, 11, 12, 20, 0, tzinfo=timezone.utc)))
# far-future timestamp
ev("Future Event", "u000", ts(datetime(2027, 6, 1, tzinfo=timezone.utc)))

expected = {
    "funnel_loose_7d": [100, 80, 50, 30, 20],
    "funnel_loose_14d_last_step": 25,
    "payment_completed_total_events": 30,
    "payment_completed_uniques": 30,
    "product_viewed_total_events_after_dedup": 90,
    "product_viewed_uniques_if_merged": 80,
    "product_viewed_uniques_if_not_merged": 90,
    "checkout_coupon_neq_SAVE10_including_missing": 20,
    "checkout_coupon_neq_SAVE10_excluding_missing": 0,
    "retention_d1_signed_up_to_app_opened": 0.40,
    "retention_d7": 0.20,
    "funnel_by_plan": {"premium": [50, 40, 25, 15, 10], "free": [50, 40, 25, 15, 10]},
    "tz_probe_day_ist": "2026-11-13", "tz_probe_day_utc": "2026-11-12",
}
json.dump({"events": events}, open(f"{out}/events.json", "w"))
json.dump(identify, open(f"{out}/identify.json", "w"))
json.dump(expected, open(f"{out}/expected.json", "w"), indent=2)
print(len(events), "events written")
