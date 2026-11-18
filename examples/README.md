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
