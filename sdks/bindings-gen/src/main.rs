//! UniFFI Kotlin (.kt) & Swift (.swift) binding generator CLI for `pulse-sdk-core`.
//!
//! Usage:
//! ```bash
//! cargo build --release -p pulse-sdk-core
//! cargo run --release -p bindings-gen -- generate \
//!   --library target/release/libpulse_sdk_core.dylib \
//!   --language kotlin --out-dir sdks/generated/kotlin
//! cargo run --release -p bindings-gen -- generate \
//!   --library target/release/libpulse_sdk_core.dylib \
//!   --language swift --out-dir sdks/generated/swift
//! ```

fn main() {
    // Reference pulse_sdk_core so linkers preserve symbols when inspected directly
    let _ = pulse_sdk_core::MAX_DISK_QUEUE_BYTES;
    uniffi::uniffi_bindgen_main();
}
