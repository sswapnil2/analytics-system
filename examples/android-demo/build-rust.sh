#!/usr/bin/env bash
# Cross-compiles pulse-sdk-core for Android and regenerates the Kotlin bindings.
# Needs: Android NDK (ANDROID_NDK_HOME), `cargo install cargo-ndk`, rustup targets below.
set -euo pipefail
cd "$(dirname "$0")/../.."
APP=examples/android-demo/app/src/main
rustup target add aarch64-linux-android x86_64-linux-android
cargo ndk -t arm64-v8a -t x86_64 -o "$APP/jniLibs" build --release -p pulse-sdk-core
cargo build --release -p pulse-sdk-core   # host dylib for uniffi-bindgen
cargo run --release -q -p bindings-gen -- generate \
  --library target/release/libpulse_sdk_core.dylib --language kotlin --out-dir "$APP/java"
echo "OK"
