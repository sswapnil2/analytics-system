# Pulse sample apps

Each app runs the same onboarding flow (welcome → details form with a masked PAN field → plan
screen with a broken "Apply promo code" button to rage-tap → done) with session replay on.

Start a node first and copy the `pk_live_...` ingest key from its log:

```bash
cargo build --release -p pulse-node && npm --prefix web ci && npm --prefix web run build
PORT=18190 ./target/release/pulse-node --seed --no-quic
```

## Web (`web-demo/`)

```bash
npm --prefix sdks/ts-web ci && npm --prefix sdks/ts-web run build
python3 -m http.server 5190     # from the repo root
open "http://127.0.0.1:5190/examples/web-demo/?key=pk_live_..."
```

## iOS (`ios-demo/`)

```bash
examples/ios-demo/build-rust.sh   # static lib + fresh Swift bindings into ios-demo/PulseSDK/
xcodebuild -project examples/ios-demo/PulseDemo.xcodeproj -scheme PulseDemo -sdk iphonesimulator \
  -destination 'platform=iOS Simulator,name=iPhone 17' -derivedDataPath build/ios build
xcrun simctl install booted build/ios/Build/Products/Debug-iphonesimulator/PulseDemo.app
xcrun simctl launch booted dev.pulse.demo -pulseKey pk_live_...
```

## Android (`android-demo/`) — not yet verified

Needs the Android SDK + NDK and `cargo install cargo-ndk`.

```bash
examples/android-demo/build-rust.sh
cd examples/android-demo && gradle :app:installDebug -PpulseKey=pk_live_...
```

## AI Agent Observability (`agent-demo/`)

Zero-dependency TypeScript/Node ESM ([`index.mjs`](agent-demo/index.mjs)) and Python ([`main.py`](agent-demo/main.py)) demo agents (see [`package.json`](agent-demo/package.json)) demonstrating multi-turn sessions, parent/child trace lineage (`parent_trace_id`), RAG retrieval with `linked_span_ids`, OpenAI streaming (`first_token` TTFT), explicit `span_phase` (`start` -> `update` -> `final`) updates, tool failure/recovery, guardrails with client-side PII/API-key redaction, and `PLST` v1 binary transport with retry recovery.

Build the local TypeScript Agent SDK ([`sdks/ts-agent`](../sdks/ts-agent/package.json)) once before running the Node demo (the Python demo automatically locates [`sdks/py-agent`](../sdks/py-agent/pyproject.toml) when `pulse_agent` is not installed):

```bash
npm --prefix sdks/ts-agent ci && npm --prefix sdks/ts-agent run build
```

Run against a live `pulse-node` (`PULSE_ENDPOINT` / `--endpoint`, `PULSE_API_KEY` / `--api-key`):

```bash
PULSE_ENDPOINT=http://127.0.0.1:18190 PULSE_API_KEY=pk_live_... npm --prefix examples/agent-demo start
# or invoke directly:
node examples/agent-demo/index.mjs --endpoint http://127.0.0.1:18190 --api-key pk_live_...
python3 examples/agent-demo/main.py --endpoint http://127.0.0.1:18190 --api-key pk_live_...
```

Run the offline `--self-test` verification (starts an ephemeral loopback server injecting transient HTTP `429`/`503` responses and decodes every `PLST` frame):

```bash
npm --prefix examples/agent-demo run self-test
# or individually:
node examples/agent-demo/index.mjs --self-test
python3 examples/agent-demo/main.py --self-test
```

