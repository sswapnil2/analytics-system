# Pulse SDK integration report — web, iOS, Android

Date: 2026-11-17. Tested on a fresh node built from this branch (`pulse-node --seed --no-quic`, port 18190),
Studio served from a fresh `web/dist`. Sample apps live in [`examples/`](examples/README.md).

Every sample runs the same flow: identify a user → welcome → details form (name + **PAN in a text field
that must be masked**) → plan screen with a deliberately broken "Apply promo code" button (one error per tap,
rage-tapped 4×) → choose plan → done.

## Summary

| | Web | iOS | Android |
|---|---|---|---|
| Ran end to end | ✅ Chrome (built-in browser) | ✅ iPhone 17 simulator, iOS 26.4 | ❌ No Android SDK/NDK/emulator on this machine |
| Time to first event (SDK already in repo, node running) | ~5 min | ~25 min | not measured (est. 45+ min incl. NDK) |
| Integration code | ~16 lines | ~64 lines of glue + 11 call sites | ~52 lines of glue + 12 call sites (unbuilt) |
| Events / identify / user profile | ✅ | ✅ | — |
| Funnel (4 steps, split by `platform`) | ✅ 1/1 converted | ✅ 1/1 converted | — |
| Replay plays in Studio | ✅ (clipping + banner bugs fixed on this branch) | ✅ clean | — |
| Masked text field in replay | ✅ `████` | ✅ `████` | — |
| Rage tap detected | ✅ ×3, "What happened" entry | ✅ ×3, "What happened" entry | — |
| Error shown in replay | ✅ after fix (was stacked ×4 in the wrong place) | ✅ at the real position | — |

Bottom line: **web is easy; native mobile is a do-it-yourself job.** There is no iOS or Android SDK as
such — only a Rust core with UniFFI bindings. A mobile team has to cross-compile Rust, regenerate
bindings (the checked-in ones are stale), set up linking by hand, and write its own view-tree walker
and touch hook before replay shows anything.

One-time cost that applies to every platform: `cargo build --release -p pulse-node` took **15 min** on
this machine.

---

## Getting a project key (all platforms)

- The only way to get an ingest key on a fresh node is to read it from the startup log
  (`Generated one-time Public Ingest Key ... pk_live_...`). The Studio has **no screen for ingest keys**;
  only the API route `/v1/projects/:id/api-keys` exists (`crates/pulse-control/src/api.rs:809`).
  If you miss the log line, you're stuck.
- The in-app "Send your first event" panel shows only a curl snippet with a placeholder
  `YOUR_INGEST_KEY` (`web/src/App.tsx:296`). It doesn't show the real key, and has no web or mobile
  SDK snippet.
- The generated `sk_root_` admin token is **re-minted on every node restart** and the old one stops
  working, which also signs you out of the Studio, unless `PULSE_ADMIN_TOKEN` is set. Nothing tells
  you this until the API returns `invalid admin bearer token`.
- `--seed` put my events in `proj_fintech_in` even though I didn't set `PULSE_DEFAULT_PROJECT`
  (the code default is `proj_default`). The Studio labels that project "Default Project". This is
  confusing when you call the REST API by project id.

---

## Web (`examples/web-demo/index.html`)

**Steps to first event**
1. `npm --prefix sdks/ts-web ci && npm run build`. The SDK isn't on npm and has no bundle or CDN
   build, so you import `sdks/ts-web/dist/index.js` as an ES module (or use `npm install <path>`).
2. `new PulseWebClient({ endpointUrl, apiKey, enableWireframeReplay: true, autocapture: true })`.
3. `identify()`, `track()`. Events, identify and replay chunks all returned 200 on the first try.
   Ingest CORS is permissive, so a separate origin (`:5190`) worked without config.

**API friction**
- Replay is enabled but **records nothing until you call `captureDomWireframeSnapshot()` yourself**.
  It doesn't snapshot on load, on DOM mutation, or on route change, so every screen change needs a
  manual call. This is the biggest web gap.
- Errors aren't captured automatically: there's no `window.onerror` / `unhandledrejection` hook, so
  you must call `recordConsoleError()`. Network breadcrumbs aren't automatic either (no fetch/XHR
  wrapper). Autocapture pageviews listen only to `popstate`, so SPA `pushState` navigations are missed.
- Labels are masked by default, so the app needs `data-pulse-unmask` on headings and buttons to get a
  readable replay. That's reasonable, but the docs don't cover it.

**Bugs found**
1. **[Fixed on this branch]** `recordConsoleError` added a **new** error-banner node on every call, with id `99_000 + replayOps.length`,
   always at a hard-coded `x:16, y:16` (`sdks/ts-web/src/index.ts:943-960`). Four taps produced four
   overlapping banners in the corner instead of the real alert box. It also recorded replay ops when
   replay was off. Now it upserts one stable node over the visible `[role=alert]` box (falling back
   to the corner toast), and only while recording. Covered by `sdks/ts-web/test/sdk.test.mjs`.
   Remaining limit: the page isn't re-snapshotted, so content the alert pushed down (here
   "Choose Pro") is still drawn at its old position, under the banner.
2. Snapshot viewport is clamped to a minimum of 320×480, and elements are capped at the **first 120 in
   DOM order before** filtering hidden ones (`index.ts:974-990`). On a large page, visible content past
   element 120 is silently dropped. In my run the first snapshot came out 320×480 with only the button,
   because the tab was opened in the background (innerWidth was 0 at load). The SDK doesn't re-snapshot
   on resize or visibility.
3. `getSessionReplayUrl()` returns `/#/replays?session=…` (`index.ts:919`), but the replays page
   ignores the `session` param (`web/src/workspaces/SessionsReplayWorkspace.tsx`), so the deep link
   doesn't open the recording.
4. Autocapture's `$current_url` stores the full URL with its query string (`index.ts:220`). Anything
   sensitive in a query string (tokens, emails, my demo's `?key=`) lands in event props.

**Replay result** (session `sess_b24eca5e86a44c0b`, 6 chunks, 22 s). I checked frames by reading the
player DOM at different times:
- 0.5 s welcome (button only, see bug 2) → 6.2 s "Your details" with both inputs `████` →
  10.5 s "Pick a plan" → 16.1 s error banners + "Rage tap ×3" marker → 22.5 s "You're all set". ✅
- **[Fixed on this branch]** The player drew a 1024 px desktop viewport inside a phone frame at a
  fixed `scale = 0.72`, so web recordings overflowed and got **clipped**. It now measures the
  available width (ResizeObserver) and scales down to fit, and draws landscape recordings in a
  browser-style frame stacked above the controls. Re-verified: a 1024×768 recording renders 597 px
  wide in a 597 px column with no horizontal overflow, and iOS replays are unchanged.

---

## iOS (`examples/ios-demo/`)

**Steps to first event**
1. `rustup target add aarch64-apple-ios-sim` (aarch64-apple-ios for devices).
2. `cargo build --release -p pulse-sdk-core --target aarch64-apple-ios-sim` → a **46 MB** static lib.
3. Regenerate bindings with `bindings-gen`. The checked-in `sdks/bindings-gen/generated/swift` is
   **stale**: it lacks `optOut`, `optIn`, `registerSuperProps`, `timeEvent`, `setUserProperties`,
   `trackRevenue` and others (100 funcs vs 116). The usage comment points at the wrong output dir
   (`sdks/generated/...`, `sdks/bindings-gen/src/main.rs:8,11`). Bindgen also needs a *host* dylib
   build, and warns that `swiftformat` is missing.
4. Rename `pulse_sdk_coreFFI.modulemap` → `module.modulemap` so `SWIFT_INCLUDE_PATHS` picks it up.
   Then set `LIBRARY_SEARCH_PATHS` and `OTHER_LDFLAGS=-lpulse_sdk_core`. There's no SwiftPM package
   and no xcframework.
5. Link warning: the static lib is built for iOS 26.4 while the app targets 16.0 (set
   `IPHONEOS_DEPLOYMENT_TARGET` when building with cargo).

`examples/ios-demo/build-rust.sh` automates steps 1–4. After that, the hand-written 100-line
`project.pbxproj` built on the first try. HTTP to `127.0.0.1` needed no ATS exception, because the
SDK uses reqwest/rustls instead of URLSession.

**API friction**
- `SdkConfig` has no defaults. You must pass `storageDir`, `flushBatchSize` and `flushIntervalMs`
  yourself, and choose an app-support path.
- `track(eventName:propsJson:)` takes a **JSON string**, not a dictionary. Every app needs a
  `JSONSerialization` wrapper. Every call `throws`.
- **No automatic context**: no `$os`, `$app_version`, `$device`, or screen events. I only got a
  `platform` breakdown because I set it as a user property.
- **Replay is entirely manual.** You build `[SdkWireNode]` yourself (kind strings, `UInt16` boxes,
  RGBA ints, node ids). That meant writing a UIKit view walker (~35 lines) and a `UIWindow.sendEvent`
  override to feed `recordTouch` with `targetIsInteractive`. SwiftUI apps would get nothing useful
  from a UIKit walk.
- Labels are masked unless you set `unmasked: true` per node. That's good default-deny, but the replay
  is unreadable until you opt in.
- There's no `recordError` API on mobile. I tracked `$error` and re-snapshotted with an
  `error_banner` node.

**Replay result** (session `sess_3b71ef68df73`, 6 chunks, 37 s). Checked in the Studio player:
- 0.5 s welcome (title, subtitle, button) → 7 s "Your details", **both fields `████`** →
  21.5 s "Pick a plan" → 29.4 s "PromoService unavailable" banner at the real position →
  29.6 s "Rage tap ×3" → 37.2 s "You're all set". ✅ All screens, taps, the rage tap, masking and the
  error played correctly.
- **[Fixed on this branch]** `FullSnapshot` ops carried **no timestamp**, so the player pinned every
  snapshot to its chunk's start: several screens captured inside one flush interval all jumped to the
  same instant and the last one won. Snapshots now carry `ts_offset_ms` (wire format, Rust and web
  SDKs, player). The binary format writes a new op tag (6) only for non-zero offsets, and JSON
  defaults a missing field to 0, so old recordings and old clients keep working. Upgrade the server
  before the SDKs: an old server rejects the new tag. Re-verified with a fast run where three screens
  landed in one batch: iOS "Your details" at +195 ms and "Pick a plan" at +676 ms; web details at +0,
  plan at +1011 ms and done at +3058 ms. Both play in order in Studio.
- Still open: `node_upsert` (used for the web error banner) has no timestamp either. The player
  places it at the last timed op, so the web banner appears when the plan screen loads (11.3 s)
  rather than at the first failing tap (12.3 s).
- Node `bg_rgba` colours are drawn literally on the dark Studio theme. My light-grey text boxes from
  iOS came out as near-white blocks, which are hard to read. The player should style by `kind` and
  ignore `bg` for text.

---

## Android (`examples/android-demo/`) — written, NOT built or run

There's no Android SDK, NDK, emulator, `adb`, `gradle`, `kotlinc` or `cargo-ndk` on this machine.
I didn't install them (several GB), as instructed. The sample is a straight port of the iOS one: a
programmatic `Activity`, a `View` tree walker, and a `dispatchTouchEvent` hook. It also has
`build-rust.sh` (cargo-ndk → `jniLibs/`, Kotlin bindings regenerated) and a JNA dependency. Expected
friction, from reading the code:
- Same stale-bindings problem: `generated/kotlin` also lacks `setUserProperties` and `optOut`.
- UniFFI Kotlin needs JNA (`net.java.dev.jna:jna:…@aar`) and per-ABI `.so` files from cargo-ndk.
  None of this is documented.
- Unsigned types (`UShort`, `UInt`, `ULong`) leak into the Kotlin API (`flushBatchSize = 20u`,
  `x.toUShort()`), which makes for awkward call sites.
- Android touch coordinates are physical px while the snapshot uses window px. These match here, but
  density isn't handled anywhere, so replays will be 2–3× larger than the iOS ones.
- There's no Gradle wrapper in the sample. Add one once someone with the SDK can run `gradle wrapper`.

**To finish:** install Android Studio (SDK + NDK + an AVD) and run `cargo install cargo-ndk`. Then run
`examples/android-demo/build-rust.sh` and `gradle :app:installDebug -PpulseKey=…` on an emulator,
which reaches the host at `10.0.2.2:18190`.

---

## Studio checks (web + iOS)

- **Events**: all custom events plus `$autocapture`, `$error` and `$rage_tap` show up. The new event
  names appear in the Studio's event pickers.
- **Users**: `/v1/projects/:id/users/{web,ios}_demo_user_1` return profiles with full event history;
  the user properties (`platform`, `plan`) were merged.
- **Funnel** Onboarding Started → Profile Submitted → Plan Selected → Onboarding Completed:
  2 users, 100% at every step. The `platform` breakdown shows `ios` and `web` separately.
- **Session replays page**: both sessions show up with "Rage taps" and "Error" badges, the right
  duration and event counts, and a "What happened" entry for the rage tap. But the page **opens on a
  hard-coded seed event filter** ("NSDL Gateway 504 Timeout") with "Rage taps" toggled on
  (`SessionsReplayWorkspace.tsx:39-40`). A new customer lands on an empty list until they change the
  filter.
- Screenshot note: the browser pane's screenshots lagged behind the player state, so frame contents
  above were verified by reading the player DOM after seeking. The iOS simulator screenshots (welcome,
  details, plan + error) were taken live.

---

## Top fixes, ranked by impact on ease of integration

1. **Ship real mobile SDKs**: a Swift Package (xcframework, device + sim, correct deployment target)
   and an Android AAR (all ABIs, JNA bundled), each with idiomatic wrappers: `track(_:properties:)`
   with dictionaries, default config, auto context props (`$os`, `$app_version`, `$device`), and
   screen tracking.
2. **Automatic mobile replay capture**: a built-in UIKit/SwiftUI and Android View walker plus a touch
   hook, so "enable replay" is one flag, as on web. Today every team rewrites ~50 lines of fragile code.
3. **Automatic web snapshots**: snapshot on load, on `MutationObserver` changes (throttled) and on
   history `pushState`/`popstate`. Hook `window.onerror`/`unhandledrejection` and fetch/XHR for errors
   and breadcrumbs.
4. **Timestamp `node_upsert` too.** `full_snapshot` now has `ts_offset_ms` (fixed here), but upserts
   still borrow the previous op's time, so error banners appear early.
5. **Ingest keys in the Studio**: a Project settings page to view and create `pk_live_` keys, and put
   the real key into the "Send your first event" panel with web/iOS/Android snippets.
6. **Fix the stale bindings and the bindings-gen docs**: regenerate in CI, or stop committing them, and
   fix the output path in `sdks/bindings-gen/src/main.rs`.
7. **Replay page defaults and deep link**: default to "Any event" with no toggles, and honour
   `?session=` so `getSessionReplayUrl()` works.
8. **Web replay polish**: re-snapshot after errors and layout changes, and don't cap elements
   before filtering for visibility. (Banner stacking and player clipping are already fixed here.)
9. **Publish `@pulse-analytics/ts-web`** (npm + a UMD/CDN build) and strip query strings from
   `$current_url` by default, or make that configurable.
10. **Docs**: one "Integrate Pulse" page per platform covering keys, CORS, the masking/unmask model,
    replay calls and offline queueing. Today the README covers only curl and the node.
