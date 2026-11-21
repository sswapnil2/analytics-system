#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PORT="${PULSE_VERIFY_PORT:-18080}"
BASE_URL="${PULSE_BASE_URL:-http://127.0.0.1:${PORT}}"
PROJECT_ID="${PULSE_PROJECT_ID:-proj_fintech_in}"
API_KEY="${PULSE_API_KEY:-}"
ADMIN_TOKEN="${PULSE_ADMIN_TOKEN:-}"

NODE_PID=""
TMP_DATA_DIR=""

cleanup() {
  if [[ -n "$NODE_PID" ]] && kill -0 "$NODE_PID" 2>/dev/null; then
    kill "$NODE_PID" 2>/dev/null || true
    wait "$NODE_PID" 2>/dev/null || true
  fi
  if [[ -n "$TMP_DATA_DIR" && -d "$TMP_DATA_DIR" ]]; then
    rm -rf "$TMP_DATA_DIR"
  fi
}
trap cleanup EXIT

echo "=== Pulse Analytics End-to-End Verification ==="

if ! curl -fsS "${BASE_URL}/healthz" >/dev/null 2>&1; then
  echo "[boot] No live pulse-node at ${BASE_URL}; booting isolated release instance with --seed..."
  if [[ ! -f "${ROOT_DIR}/web/dist/index.html" ]]; then
    npm --prefix "${ROOT_DIR}/web" run build
  fi
  cargo build -p pulse-node --release
  TMP_DATA_DIR="$(mktemp -d "${TMP_DIR:-/tmp}/pulse_e2e.XXXXXX")"
  PORT="$PORT" \
  PULSE_DEFAULT_PROJECT="$PROJECT_ID" \
  PULSE_DATA_DIR="$TMP_DATA_DIR" \
  PULSE_WEB_DIST_DIR="${ROOT_DIR}/web/dist" \
  "${ROOT_DIR}/target/release/pulse-node" --seed --no-quic >"${TMP_DATA_DIR}/node.log" 2>&1 &
  NODE_PID=$!

  for _ in $(seq 1 240); do
    if curl -fsS "${BASE_URL}/healthz" >/dev/null 2>&1; then
      break
    fi
    if ! kill -0 "$NODE_PID" 2>/dev/null; then
      echo "[boot] pulse-node exited early; log:" >&2
      cat "${TMP_DATA_DIR}/node.log" >&2
      exit 1
    fi
    sleep 0.25
  done
  if ! curl -fsS "${BASE_URL}/healthz" >/dev/null 2>&1; then
    echo "[boot] Timed out waiting for pulse-node; log:" >&2
    cat "${TMP_DATA_DIR}/node.log" >&2
    exit 1
  fi

  if [[ -z "$ADMIN_TOKEN" ]]; then
    ADMIN_TOKEN="$(tr -d '[:space:]' <"${TMP_DATA_DIR}/root_admin_token")"
  fi
  if [[ -z "$API_KEY" ]]; then
    API_KEY="$(python3 - "${TMP_DATA_DIR}/node.log" <<'PY'
import re, sys
text = open(sys.argv[1], "r", encoding="utf-8", errors="replace").read()
m = re.search(r"(pk_live_[A-Za-z0-9_]+)", text)
assert m, f"failed to find generated pk_live_ key in boot log:\n{text}"
print(m.group(1))
PY
)"
  fi
  echo "[boot] pulse-node healthy at ${BASE_URL} (pid=${NODE_PID})"
else
  echo "[boot] Using live pulse-node at ${BASE_URL}"
  if [[ -z "$ADMIN_TOKEN" || -z "$API_KEY" ]]; then
    echo "When targeting an already-running pulse-node, export PULSE_ADMIN_TOKEN and PULSE_API_KEY." >&2
    exit 1
  fi
fi

AUTH_HDR="Authorization: Bearer ${ADMIN_TOKEN}"

NOW_MS="$(python3 -c 'import time; print(int(time.time() * 1000))')"
START_MS="$((NOW_MS - 15 * 86400 * 1000))"
END_MS="$((NOW_MS + 3600 * 1000))"

# 1. Verify Admin Authentication Enforcement (`P0-01`) & Seeded 100k+ Dataset
echo "[1/9] Checking GET /v1/status (401 unauthenticated -> 200 with Bearer token)..."
UNAUTH_CODE="$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/v1/status?project_id=${PROJECT_ID}")"
if [[ "$UNAUTH_CODE" != "401" ]]; then
  echo "Expected HTTP 401 on unauthenticated /v1/status, got ${UNAUTH_CODE}" >&2
  exit 1
fi
STATUS_JSON="$(curl -fsS -H "${AUTH_HDR}" "${BASE_URL}/v1/status?project_id=${PROJECT_ID}")"
python3 - "$STATUS_JSON" <<'PY'
import json, sys
s = json.loads(sys.argv[1])
total_rows = int(s["l0_rows"]) + int(s["l1_rows"])
assert s["status"] == "ok", f"unexpected status: {s}"
assert "default_api_key" not in s, f"/v1/status must never leak API keys: {s}"
assert total_rows >= 100_000, f"expected >= 100,000 seeded rows, got {total_rows}"
assert int(s["interned_users"]) >= 7_000, f"expected >= 7,000 interned u32 users, got {s['interned_users']}"
print(f"      ✓ Unauth=401, Auth=200 | Stored rows={total_rows:,}, Vortex segments={s['segment_count']}, u32 users={s['interned_users']:,}")
PY

# 2. Verify /v1/ingest + Deterministic Retry Deduplication + /v1/identify
echo "[2/9] Checking POST /v1/ingest & deterministic deduplication..."
INGEST_PAYLOAD="$(cat <<EOF
{
  "events": [
    {
      "event_uuid": "11111111-2222-3333-4444-555555555501",
      "routing_seed": "dev_e2e_verify_01",
      "distinct_id": "anon_e2e_verify_01",
      "session_id": "sess_e2e_verify_01",
      "event_name": "App_Open",
      "client_event_ts_ms": $((NOW_MS - 2000)),
      "client_sent_ts_ms": ${NOW_MS},
      "props": { "city": "Bengaluru", "platform": "Android", "amount": 1499 }
    },
    {
      "event_uuid": "11111111-2222-3333-4444-555555555502",
      "routing_seed": "dev_e2e_verify_01",
      "distinct_id": "anon_e2e_verify_01",
      "session_id": "sess_e2e_verify_01",
      "event_name": "KYC_PAN_Submitted",
      "client_event_ts_ms": $((NOW_MS - 1000)),
      "client_sent_ts_ms": ${NOW_MS},
      "props": { "city": "Bengaluru", "platform": "Android", "amount": 1499 }
    }
  ]
}
EOF
)"

INGEST_1="$(curl -fsS -X POST "${BASE_URL}/v1/ingest" \
  -H "Content-Type: application/json" \
  -H "x-pulse-key: ${API_KEY}" \
  -d "${INGEST_PAYLOAD}")"

INGEST_2="$(curl -fsS -X POST "${BASE_URL}/v1/ingest" \
  -H "Content-Type: application/json" \
  -H "x-pulse-key: ${API_KEY}" \
  -d "${INGEST_PAYLOAD}")"

IDENTIFY_RES="$(curl -fsS -X POST "${BASE_URL}/v1/identify" \
  -H "Content-Type: application/json" \
  -H "x-pulse-key: ${API_KEY}" \
  -d '{"anon_id":"anon_e2e_verify_01","user_id":"+919899900001","routing_seed":"dev_e2e_verify_01"}')"

python3 - "$INGEST_1" "$INGEST_2" "$IDENTIFY_RES" <<'PY'
import json, sys
r1, r2, ident = json.loads(sys.argv[1]), json.loads(sys.argv[2]), json.loads(sys.argv[3])
assert r1["accepted"] == 2 and r1["deduped"] == 0, f"expected 2 accepted on first send, got {r1}"
assert r2["accepted"] == 0 and r2["deduped"] == 2, f"expected 2 deduped on retry, got {r2}"
assert ident["status"] == "ok" and int(ident["canonical_uid"]) >= 0, f"identify failed: {ident}"
print(f"      ✓ First batch accepted=2, retry deduped=2, identify canonical_uid={ident['canonical_uid']}")
PY

# 3. Verify POST /v1/query across all 4 core report types (Insights, Funnel, Retention, Flows)
echo "[3/9] Checking POST /v1/query (Insights, Funnel, Retention, Flows)..."

INSIGHTS_RES="$(curl -fsS -X POST "${BASE_URL}/v1/query" \
  -H "${AUTH_HDR}" \
  -H "Content-Type: application/json" \
  -d "{
    \"project_id\": \"${PROJECT_ID}\",
    \"kind\": \"insights\",
    \"time_range\": { \"start_ts_ms\": ${START_MS}, \"end_ts_ms\": ${END_MS} },
    \"interval\": \"day\",
    \"series\": [
      { \"label\": \"A\", \"event_name\": \"App_Open\", \"metric\": { \"type\": \"uniques\" }, \"filters\": [] },
      { \"label\": \"B\", \"event_name\": \"First_Disbursal_Success\", \"metric\": { \"type\": \"uniques\" }, \"filters\": [] }
    ],
    \"breakdown_by\": [\"city\"],
    \"formula\": \"(B / A) * 100\"
  }")"

FUNNEL_RES="$(curl -fsS -X POST "${BASE_URL}/v1/query" \
  -H "${AUTH_HDR}" \
  -H "Content-Type: application/json" \
  -d "{
    \"project_id\": \"${PROJECT_ID}\",
    \"kind\": \"funnel\",
    \"time_range\": { \"start_ts_ms\": ${START_MS}, \"end_ts_ms\": ${END_MS} },
    \"steps\": [
      { \"event_name\": \"App_Open\", \"filters\": [] },
      { \"event_name\": \"KYC_PAN_Submitted\", \"filters\": [] },
      { \"event_name\": \"Video_KYC_Completed\", \"filters\": [] },
      { \"event_name\": \"UPI_Autopay_Mandate_Approved\", \"filters\": [] },
      { \"event_name\": \"First_Disbursal_Success\", \"filters\": [] }
    ],
    \"order_mode\": \"loose_order\",
    \"conversion_window_ms\": 604800000,
    \"breakdown_by\": \"platform\"
  }")"

RETENTION_RES="$(curl -fsS -X POST "${BASE_URL}/v1/query" \
  -H "${AUTH_HDR}" \
  -H "Content-Type: application/json" \
  -d "{
    \"project_id\": \"${PROJECT_ID}\",
    \"kind\": \"retention\",
    \"time_range\": { \"start_ts_ms\": ${START_MS}, \"end_ts_ms\": ${END_MS} },
    \"start_event\": \"App_Open\",
    \"return_event\": \"Quick_Grocery_Browse\",
    \"mode\": \"n_day\",
    \"unit\": \"day\",
    \"num_periods\": 7
  }")"

FLOWS_RES="$(curl -fsS -X POST "${BASE_URL}/v1/query" \
  -H "${AUTH_HDR}" \
  -H "Content-Type: application/json" \
  -d "{
    \"project_id\": \"${PROJECT_ID}\",
    \"kind\": \"flows\",
    \"time_range\": { \"start_ts_ms\": ${START_MS}, \"end_ts_ms\": ${END_MS} },
    \"anchor_event\": \"KYC_PAN_Submitted\",
    \"direction\": \"forward\",
    \"steps\": 3
  }")"

python3 - "$INSIGHTS_RES" "$FUNNEL_RES" "$RETENTION_RES" "$FLOWS_RES" <<'PY'
import json, sys
ins, fun, ret, flw = [json.loads(x) for x in sys.argv[1:5]]
assert len(ins["series"]) > 0 and len(ins["formula_series"]) >= 1, f"insights series unexpected: {ins}"
steps = fun["overall_steps"]
assert len(steps) == 5, f"expected 5 funnel steps, got {len(steps)}"
assert steps[0]["entered_users"] >= steps[-1]["entered_users"] > 0, f"invalid funnel monotonicity: {steps}"
assert fun["time_to_convert"]["p50_ms"] > 0, "expected positive TTC p50_ms"
assert len(ret["cohorts"]) >= 5, f"expected >= 5 retention cohorts, got {len(ret['cohorts'])}"
assert len(flw["nodes"]) >= 3 and len(flw["links"]) >= 2, f"expected Sankey nodes & links, got {flw}"
print(
    f"      ✓ Insights={ins['exec_time_us']}µs | "
    f"Funnel(5-step: {steps[0]['entered_users']:,} -> {steps[-1]['entered_users']:,})={fun['exec_time_us']}µs | "
    f"Retention={ret['exec_time_us']}µs | Flows={flw['exec_time_us']}µs"
)
PY

# 4. Verify SessionIndex 64-bit Bitmask Pruning on GET /v1/sessions
echo "[4/9] Checking GET /v1/sessions (SessionIndex 64-bit Bloom pruning)..."
SESSIONS_JSON="$(curl -fsS -H "${AUTH_HDR}" "${BASE_URL}/v1/sessions?project_id=${PROJECT_ID}&event=NSDL_Gateway_504_Timeout&rage_tap=true&limit=10")"
TARGET_SESSION_ID="$(python3 - "$SESSIONS_JSON" <<'PY'
import json, sys
res = json.loads(sys.argv[1])
assert res["sessions_pruned_by_bloom"] > 0, f"expected bloom pruning > 0, got {res}"
assert len(res["sessions"]) > 0, f"expected frustrated sessions, got {res}"
s0 = res["sessions"][0]
assert s0["has_rage_tap"] is True
print(s0["session_id"])
PY
)"
echo "      ✓ SessionIndex pruned non-matching sessions via 64-bit bitmask; matched session=${TARGET_SESSION_ID}"

# 5. Verify Byte-Range Wireframe Replay (`206 Partial Content` + `PLSR` magic + Masked PII, `P3-01`, `P3-05`)
echo "[5/9] Checking GET /v1/projects/${PROJECT_ID}/replay/${TARGET_SESSION_ID} (HTTP 206 Range + PII masking)..."
TMP_RANGE_HEADERS="$(mktemp)"
TMP_RANGE_BODY="$(mktemp)"
curl -fsS -D "$TMP_RANGE_HEADERS" \
  -H "${AUTH_HDR}" \
  -H "Range: bytes=0-511" \
  "${BASE_URL}/v1/projects/${PROJECT_ID}/replay/${TARGET_SESSION_ID}" -o "$TMP_RANGE_BODY"

head -n 1 "$TMP_RANGE_HEADERS" | tr -d '\r' | grep -Eq '^HTTP/[0-9.]+ 206([[:space:]]|$)'
MAGIC_HEX="$(head -c 4 "$TMP_RANGE_BODY" | xxd -p)"
rm -f "$TMP_RANGE_HEADERS" "$TMP_RANGE_BODY"
if [[ "$MAGIC_HEX" != "504c5352" ]]; then
  echo "Expected PLSR magic (504c5352), got ${MAGIC_HEX}" >&2
  exit 1
fi

REPLAY_JSON="$(curl -fsS -H "${AUTH_HDR}" "${BASE_URL}/v1/projects/${PROJECT_ID}/replay/${TARGET_SESSION_ID}")"
python3 - "$REPLAY_JSON" <<'PY'
import json, sys
raw = sys.argv[1]
data = json.loads(raw)
assert len(data["chunks"]) >= 1, "expected >= 1 replay chunk"
assert "████" in raw, "expected fixed-length client-masked placeholder in wireframe stream"
assert "rage_tap" in raw, "expected rage_tap interaction in wireframe stream"
print(f"      ✓ HTTP 206 Partial Content (status line & PLSR magic verified) & decoded {len(data['chunks'])} masked wireframe chunk(s)")
PY

# 6. Verify Experimentation Engine (Frequentist Z-test + SRM + Holm + Bayesian Monte Carlo, `P0-04`, `P2-10`)
echo "[6/9] Checking GET /v1/projects/${PROJECT_ID}/experiments/exp_one_tap_upi_mandate/results..."
EXP_JSON="$(curl -fsS -H "${AUTH_HDR}" "${BASE_URL}/v1/projects/${PROJECT_ID}/experiments/exp_one_tap_upi_mandate/results")"
python3 - "$EXP_JSON" <<'PY'
import json, sys
exp = json.loads(sys.argv[1])
assert exp["sample_ratio_mismatch"] is False, f"unexpected SRM in balanced synthetic experiment: {exp}"
assert exp["sequential"] is True and "always-valid" in exp["statistical_methodology_note"], exp["statistical_methodology_note"]
variants = {v["variant_key"]: v for v in exp["variants"]}
treat = variants["one_tap_sheet"]
assert treat["relative_lift_pct"] > 5.0, f"expected positive lift, got {treat}"
assert treat["p_value"] < 0.05, f"expected Holm-adjusted p_value < 0.05, got {treat}"
assert treat["bayesian_prob_to_beat_control"] > 0.95, f"expected Bayesian prob > 0.95, got {treat}"
assert treat["statistically_significant"] is True
print(
    f"      ✓ Lift=+{treat['relative_lift_pct']:.2f}%, "
    f"adj_p={treat['p_value']:.4g}, SRM_p={exp['srm_p_value']:.3f}, "
    f"Bayesian P(beat control)={treat['bayesian_prob_to_beat_control']*100:.2f}%"
)
PY

# 7. Verify Edge Flag Bundle ETag Caching (`304 Not Modified`)
echo "[7/9] Checking GET /v1/flags/${API_KEY} ETag caching..."
FLAG_HDRS="$(mktemp)"
curl -fsS -D "$FLAG_HDRS" "${BASE_URL}/v1/flags/${API_KEY}" >/dev/null
ETAG_VAL="$(awk 'tolower($1)=="etag:" {print $2}' "$FLAG_HDRS" | tr -d '\r')"
rm -f "$FLAG_HDRS"
HTTP_304="$(curl -s -o /dev/null -w "%{http_code}" -H "If-None-Match: ${ETAG_VAL}" "${BASE_URL}/v1/flags/${API_KEY}")"
if [[ "$HTTP_304" != "304" ]]; then
  echo "Expected HTTP 304 for matching ETag ${ETAG_VAL}, got ${HTTP_304}" >&2
  exit 1
fi
echo "      ✓ Edge Flag bundle ETag=${ETAG_VAL} -> HTTP 304 Not Modified"

# 8. Verify Principal-Bound Built-in MCP JSON-RPC 2.0 Server (`POST /v1/mcp`, `P0-04`)
echo "[8/9] Checking POST /v1/mcp JSON-RPC 2.0 server..."
MCP_LIST="$(curl -fsS -X POST "${BASE_URL}/v1/mcp" \
  -H "${AUTH_HDR}" \
  -H "x-pulse-project: ${PROJECT_ID}" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}')"
MCP_CALL="$(curl -fsS -X POST "${BASE_URL}/v1/mcp" \
  -H "${AUTH_HDR}" \
  -H "x-pulse-project: ${PROJECT_ID}" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"query_funnel","arguments":{"steps":["App_Open","KYC_PAN_Submitted","First_Disbursal_Success"],"days":14}}}')"
python3 - "$MCP_LIST" "$MCP_CALL" <<'PY'
import json, sys
tl, tc = json.loads(sys.argv[1]), json.loads(sys.argv[2])
tools = tl["result"]["tools"]
assert len(tools) >= 5, f"expected >= 5 MCP tools, got {len(tools)}"
assert tc["result"].get("isError", False) is False and len(tc["result"]["content"]) >= 1
print(f"      ✓ MCP registered {len(tools)} tools & executed principal-bound tools/call(query_funnel)")
PY

# 9. Verify Static Analytics Studio Delivery (`GET /`)
echo "[9/9] Checking GET / static Analytics Studio bundle..."
INDEX_HTML="$(curl -fsS "${BASE_URL}/")"
if ! grep -q '<div id="root"></div>' <<<"$INDEX_HTML"; then
  echo "Static Analytics Studio root mount missing in GET / response!" >&2
  exit 1
fi
JS_ASSET="$(python3 - "$INDEX_HTML" <<'PY'
import re, sys
m = re.search(r'src="(/assets/[^"]+\.js)"', sys.argv[1])
assert m, "bundled JS asset script tag not found in index.html"
print(m.group(1))
PY
)"
ASSET_CODE="$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}${JS_ASSET}")"
if [[ "$ASSET_CODE" != "200" ]]; then
  echo "Expected 200 for ${JS_ASSET}, got ${ASSET_CODE}" >&2
  exit 1
fi
echo "      ✓ Static Analytics Studio HTML & ${JS_ASSET} served with HTTP 200"

echo "=== ALL 9 E2E VERIFICATION GATES PASSED ==="
