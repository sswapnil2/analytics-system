import assert from "node:assert/strict";
import test from "node:test";
import {
  PulseReactNativeClient,
  createAsyncStorageAdapter,
  createMmkvStorageAdapter,
  createNavigationTracker,
} from "../dist/index.js";

test("PulseReactNativeClient: AsyncStorage hydration, native CSPRNG, AppState flush, ErrorUtils, fetch breadcrumbs, navigation, and native incremental replay", async () => {
  const savedFetch = globalThis.fetch;
  const savedErrorUtils = globalThis.ErrorUtils;
  const savedWindow = globalThis.window;
  const savedDocument = globalThis.document;
  const savedLocalStorage = globalThis.localStorage;

  // Simulate Hermes / React Native globals: no document, no localStorage, window is plain object.
  globalThis.window = {};
  globalThis.document = undefined;
  globalThis.localStorage = undefined;

  const asyncBacking = new Map();
  const mockAsyncStorage = {
    async getItem(k) {
      await new Promise((r) => setTimeout(r, 3));
      return asyncBacking.get(k) ?? null;
    },
    async setItem(k, v) {
      asyncBacking.set(k, v);
    },
    async removeItem(k) {
      asyncBacking.delete(k);
    },
  };

  let prevErrorCalled = false;
  let registeredErrorHandler = null;
  globalThis.ErrorUtils = {
    getGlobalHandler: () => () => {
      prevErrorCalled = true;
    },
    setGlobalHandler: (h) => {
      registeredErrorHandler = h;
    },
  };

  let appStateListener = null;
  const mockAppState = {
    currentState: "active",
    addEventListener(_type, listener) {
      appStateListener = listener;
      return {
        remove() {
          appStateListener = null;
        },
      };
    },
  };

  const sentEvents = [];
  const sentReplays = [];
  globalThis.fetch = async (url, init) => {
    const s = String(url);
    if (s.endsWith("/v1/ingest")) {
      sentEvents.push(...JSON.parse(init.body).events);
      return { ok: true, status: 200, json: async () => ({}), headers: { get: () => null } };
    }
    if (s.endsWith("/v1/replay/chunk")) {
      sentReplays.push(JSON.parse(init.body));
      return { ok: true, status: 200, json: async () => ({ replay_offset_kb: 2 }), headers: { get: () => null } };
    }
    return { ok: true, status: 201, json: async () => ({ ok: true }), headers: { get: () => null } };
  };

  let captureCall = 0;
  let randSeed = 10;
  const mockNativeRecorder = {
    randomBytesSync(len) {
      return Array.from({ length: len }, () => (randSeed += 7) & 0xff);
    },
    async captureWireframe() {
      captureCall += 1;
      const baseNodes = [
        { node_id: 1, parent_id: 0, kind: "screen", x: 0, y: 0, w: 390, h: 844, bg_rgba: 0x090d16ff, masked_label: "████" },
        { node_id: 10, parent_id: 1, kind: "container", x: 16, y: 80, w: 358, h: 120, bg_rgba: 0x1e293bff, masked_label: "" },
        { node_id: 11, parent_id: 10, kind: "text", x: 24, y: 96, w: 200, h: 24, bg_rgba: 0x00000000, masked_label: "████" },
        { node_id: 12, parent_id: 1, kind: "button", x: 16, y: 720, w: 358, h: 52, bg_rgba: 0x2563ebff, masked_label: "Pay" },
      ];
      if (captureCall >= 2) {
        // Second capture adds a bottom sheet window node (node_id 99) -> should emit incremental node_upsert!
        baseNodes.push({
          node_id: 99,
          parent_id: 1,
          kind: "container",
          x: 0,
          y: 480,
          w: 390,
          h: 364,
          bg_rgba: 0xffffffff,
          masked_label: "",
        });
      }
      return { width: 390, height: 844, nodes: baseNodes };
    },
  };

  try {
    const pulse = new PulseReactNativeClient({
      endpointUrl: "http://pulse.test",
      apiKey: "pk_rn_full",
      flushIntervalMs: 0,
      enableWireframeReplay: true,
      replayIntervalMs: 0,
      storage: createAsyncStorageAdapter(mockAsyncStorage),
      appState: mockAppState,
      platform: { OS: "android", Version: 34 },
      dimensions: { get: () => ({ width: 390, height: 844 }) },
      appVersion: "3.1.0",
      buildNumber: "310",
      nativeRecorder: mockNativeRecorder,
    });
    await pulse.ready();

    // 1. Native CSPRNG IDs (16 hex chars)
    assert.match(pulse.getDistinctId(), /^anon_[0-9a-f]{16}$/);

    // 2. React Navigation tracker + native wireframe snapshot (initial full + incremental diff on sheet)
    let currentRoute = "Home";
    const nav = createNavigationTracker(pulse, () => currentRoute);
    nav.onReady();
    await pulse.captureNativeSnapshot(); // triggers 2nd capture with bottom sheet -> incremental diff!

    // 3. Global fetch breadcrumb on Hermes (query string stripped!)
    await globalThis.fetch("https://api.myapp.in/v1/upi/status?token=secret_upi_123", { method: "POST" });

    // 4. Hermes ErrorUtils global error capture
    assert.ok(typeof registeredErrorHandler === "function");
    registeredErrorHandler(new TypeError("Cannot read property 'amount' of undefined"), false);
    assert.equal(prevErrorCalled, true, "previous ErrorUtils handler must be chained");

    // 5. Touch / rage-tap via handleTouchStart
    pulse.handleTouchStart({ nativeEvent: { pageX: 200, pageY: 740 } }, true);
    pulse.handleTouchStart({ nativeEvent: { pageX: 201, pageY: 741 } }, true);
    pulse.handleTouchStart({ nativeEvent: { pageX: 199, pageY: 739 } }, true);

    // 6. AppState background triggers automatic flush + $app_backgrounded
    appStateListener("background");
    await new Promise((r) => setTimeout(r, 20));
    await pulse.flush();

    // Assertions
    const screenEv = sentEvents.find((e) => e.event_name === "$screen");
    assert.ok(screenEv, "$screen event should be tracked");
    assert.equal(screenEv.props.$os, "Android");
    assert.equal(screenEv.props.$os_version, "34");
    assert.equal(screenEv.props.$app_version, "3.1.0");
    assert.equal(screenEv.props.$screen_name, "Home");

    assert.ok(sentEvents.some((e) => e.event_name === "$error"), "Hermes ErrorUtils error should be tracked");
    assert.ok(sentEvents.some((e) => e.event_name === "$rage_tap"), "touch rage tap should be tracked");
    assert.ok(sentEvents.some((e) => e.event_name === "$app_backgrounded"), "AppState background should be tracked");

    const allOps = sentReplays.flatMap((c) => c.ops);
    assert.equal(allOps.filter((o) => o.op === "full_snapshot").length, 1, "only 1 full_snapshot should be emitted");
    assert.ok(
      allOps.some((o) => o.op === "node_upsert" && o.node_id === 99 && o.bg_rgba === 0xffffffff),
      "bottom sheet container should be emitted as incremental node_upsert with real color",
    );
    const netOp = allOps.find((o) => o.op === "network_breadcrumb");
    assert.ok(netOp, "global fetch breadcrumb should be recorded");
    assert.equal(netOp.path, "https://api.myapp.in/v1/upi/status");
    assert.ok(!JSON.stringify(allOps).includes("secret_upi_123"), "query string must be stripped");

    pulse.shutdown();
  } finally {
    globalThis.fetch = savedFetch;
    globalThis.ErrorUtils = savedErrorUtils;
    globalThis.window = savedWindow;
    globalThis.document = savedDocument;
    globalThis.localStorage = savedLocalStorage;
  }
});

test("createMmkvStorageAdapter adapts synchronous MMKV API cleanly", () => {
  const store = new Map();
  const mmkv = {
    getString: (k) => store.get(k),
    set: (k, v) => store.set(k, v),
    delete: (k) => store.delete(k),
  };
  const adapter = createMmkvStorageAdapter(mmkv);
  assert.equal(adapter.getItem("k"), null);
  adapter.setItem("k", "val");
  assert.equal(adapter.getItem("k"), "val");
  adapter.removeItem("k");
  assert.equal(adapter.getItem("k"), null);
});
