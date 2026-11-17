#!/usr/bin/env bash
# Builds pulse-sdk-core for the iOS Simulator and regenerates the Swift bindings
# (the checked-in ones in sdks/bindings-gen/generated are stale).
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=examples/ios-demo/PulseSDK
TARGET=aarch64-apple-ios-sim   # use aarch64-apple-ios for a device build

rustup target add "$TARGET"
cargo build --release -p pulse-sdk-core --target "$TARGET"
cargo build --release -p pulse-sdk-core            # host dylib, needed by uniffi-bindgen
cargo run --release -q -p bindings-gen -- generate \
  --library target/release/libpulse_sdk_core.dylib --language swift --out-dir "$OUT"

mv "$OUT/pulse_sdk_coreFFI.modulemap" "$OUT/module.modulemap"   # Swift wants this exact name
cp "target/$TARGET/release/libpulse_sdk_core.a" "$OUT/"
echo "OK: $OUT"
