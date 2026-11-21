# Mobile core (beta): `crates/pulse-sdk-core`

There is no packaged iOS or Android SDK yet. What exists is a Rust client core exported through
[UniFFI](https://mozilla.github.io/uniffi-rs/), so Swift and Kotlin can call it. You build the native
library, generate bindings, link them into your app, and write the platform glue (view-tree walking
for replay, touch hooks) yourself. Expect an afternoon for a first integration.

## What the core does

- Persists a disk-backed event queue in `storage_dir`, batches, retries with exponential backoff, and
  sends to the same ingest routes as the web SDK (`/v1/ingest`, `/v1/identify`, `/v1/engage`, `/v1/replay/chunk`).
- Evaluates feature flags locally from `/v1/flags/:apiKey`, with the same deterministic bucketing as the web SDK.
- Encodes wireframe snapshots and touches for replay, masking labels unless a node is explicitly unmasked.

## API (`PulseClient`)

```text
PulseClient(config: SdkConfig)        // endpoint_url, api_key (pk_), storage_dir, flush_batch_size, flush_interval_ms,
                                      // flags_ttl_ms (default 300000: stale flags re-fetch in the background)
distinct_id() -> String               session_id() -> String          queued_packet_count() -> u32
track(event_name, props_json)         identify(user_id)               reset()
register_super_props(props_json)      time_event(event_name)          set_user_properties(props_json)
track_revenue(amount, currency, product_id?)
opt_out()   opt_in()   is_opted_out() -> bool
record_wireframe_snapshot(width, height, nodes: [SdkWireNode])
record_touch(x, y, target_is_interactive, ts_ms) -> String
async evaluate_flag(flag_key) -> String?    async refresh_flags()
async get_variant(flag_key, track_exposure) -> String?   // track_exposure sends $experiment_started once
async flush() -> u32                        async shutdown()
clear_backoff_for_retry()
```

Properties are passed as a JSON object string. Errors come back as `SdkError` (`Io`, `Network`, `Serialization`).
`SdkWireNode.kind` is one of `screen`, `container`, `button`, `masked_input`, `text`,
`image_placeholder`, `error_banner`; labels are replaced with `████` unless `unmasked = true`
(and never for `masked_input`).

Experiment results count users who sent `$experiment_started` with `flag_key` and `variant`
properties: call `get_variant(key, true)` where the user first sees the variant.

In Swift and Kotlin the names are camel-cased (`registerSuperProps`, `recordWireframeSnapshot`, …).

## Build and bind (iOS example)

```bash
rustup target add aarch64-apple-ios-sim aarch64-apple-ios
IPHONEOS_DEPLOYMENT_TARGET=16.0 cargo build --release -p pulse-sdk-core --target aarch64-apple-ios-sim
cargo build --release -p pulse-sdk-core          # host dylib, needed by the binding generator
cargo run --release -p bindings-gen -- generate \
  --library target/release/libpulse_sdk_core.dylib --language swift --out-dir sdks/bindings-gen/generated/swift
```

Then add the generated `.swift` file to your app, rename `pulse_sdk_coreFFI.modulemap` to
`module.modulemap` and put its folder on `SWIFT_INCLUDE_PATHS`, and link the static library
(`LIBRARY_SEARCH_PATHS` to the target dir, `OTHER_LDFLAGS = -lpulse_sdk_core`).

For Android, build for your ABIs with the NDK (for example via `cargo ndk`), generate with
`--language kotlin` (on Linux the host library is `libpulse_sdk_core.so`), and load the `.so` with JNA,
which UniFFI's Kotlin bindings use.

## Known gaps

- The bindings checked in under `sdks/bindings-gen/generated/` are older than the core; regenerate them rather than using them as-is.
- No SwiftPM package, xcframework, AAR or Maven artifact.
- Replay needs your own view-tree walker that turns the screen into `SdkWireNode`s, and your own touch hook calling `record_touch`.
