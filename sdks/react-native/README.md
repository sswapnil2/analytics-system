# `@pulse-analytics/react-native`

First-class React Native SDK for Pulse Analytics with:

- **Fetch timeouts (default 10s)** so hung mobile connections never stall the send queue.
- **Pluggable storage** (`createAsyncStorageAdapter`, `createMmkvStorageAdapter`, or in-memory fallback) with `await pulse.ready()` hydration.
- **Native CSPRNG** (`SecRandomCopyBytes` on iOS, `SecureRandom` on Android) so IDs never fall back to `Math.random` on Hermes.
- **Hermes error & network capture** via `ErrorUtils.setGlobalHandler` and global `fetch` wrapping (with query strings stripped).
- **`AppState` lifecycle integration** that automatically flushes queued events and replay chunks on background.
- **Native iOS & Android wireframe recorder** (`PulseNativeRecorder`) that walks all visible windows (including modals and bottom sheets), applies ancestor clipping rects, extracts real sRGB background colors, and streams incremental diffs (`node_upsert` / `node_remove`) instead of full snapshots.

## Quick start

```ts
import { AppState, Dimensions, NativeModules, Platform } from "react-native";
import AsyncStorage from "@react-native-async-storage/async-storage";
import {
  PulseReactNativeClient,
  createAsyncStorageAdapter,
  createNavigationTracker,
} from "@pulse-analytics/react-native";

export const pulse = new PulseReactNativeClient({
  endpointUrl: "https://pulse.example.com",
  apiKey: "pk_live_...",
  fetchTimeoutMs: 10_000,
  enableWireframeReplay: true,
  storage: createAsyncStorageAdapter(AsyncStorage),
  appState: AppState,
  platform: Platform,
  dimensions: Dimensions,
  nativeRecorder: NativeModules.PulseNativeRecorder,
  appVersion: "1.0.0",
});

await pulse.ready();
```
