import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  FIXED_MASK,
  PulseWebClient,
  evaluateEdgeFlag,
  flagRolloutSlot,
  flagVariantSlot,
  maskPiiText,
  xxh3_64,
} from "../dist/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

test("P2-03: TypeScript xxh3_64 and bucketing match shared Rust test vectors 100%", () => {
  const vectorsPath = path.resolve(__dirname, "../../bucketing_vectors.json");
  const vectors = JSON.parse(fs.readFileSync(vectorsPath, "utf8"));
  assert.ok(vectors.length >= 10, "expected >= 10 shared vectors");

  for (const v of vectors) {
    const hHex = xxh3_64(`${v.flag_key}:${v.distinct_id}`).toString(16).padStart(16, "0");
    assert.equal(hHex, v.xxh3_rollout_hex, `xxh3_64 mismatch for ${v.flag_key}:${v.distinct_id}`);
    assert.equal(flagRolloutSlot(v.flag_key, v.distinct_id), v.rollout_slot);
    assert.equal(flagVariantSlot(v.flag_key, v.distinct_id, 10000), v.variant_slot_10000);
    const chosen = evaluateEdgeFlag(
      {
        key: v.flag_key,
        enabled: true,
        rollout_bps: 10000,
        variants: [
          { key: "control", weight_bps: 5000 },
          { key: "treatment", weight_bps: 5000 },
        ],
      },
      v.distinct_id,
    );
    assert.equal(chosen, v.variant_50_50);
  }
});

test("P2-02: evaluateEdgeFlag enforces cohort_members allowlist", () => {
  const rule = {
    key: "beta_feature",
    enabled: true,
    rollout_bps: 10000,
    targeting_cohort_id: "coh_beta",
    cohort_members: ["beta_user_1", "beta_user_2"],
    variants: [{ key: "treatment", weight_bps: 10000 }],
  };
  assert.equal(evaluateEdgeFlag(rule, "beta_user_1"), "treatment");
  assert.equal(evaluateEdgeFlag(rule, "non_member_user"), null);
});

test("P3-01: maskPiiText uses default-deny fixed-length mask and never leaks prefix or length", () => {
  assert.equal(maskPiiText("John Smith: 9876543210"), FIXED_MASK);
  assert.equal(maskPiiText("A"), FIXED_MASK);
  assert.equal(maskPiiText("4532-0151-1283-0366"), FIXED_MASK);
  assert.equal(maskPiiText("₹1,42,950.00"), FIXED_MASK);
  assert.equal(maskPiiText("   "), "");
  assert.equal(maskPiiText("Verify PAN", true), "Verify PAN");
});

test("P2-04, P2-05, P1-08, P3-05: localStorage persistence, reset(), failure requeue, rage-tap clear, and listener cleanup", async () => {
  const store = new Map();
  globalThis.localStorage = {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => store.set(k, String(v)),
    removeItem: (k) => store.delete(k),
  };

  const listeners = new Map();
  globalThis.window = {
    innerWidth: 1280,
    innerHeight: 800,
    addEventListener: (evt, fn) => {
      if (!listeners.has(evt)) listeners.set(evt, new Set());
      listeners.get(evt).add(fn);
    },
    removeEventListener: (evt, fn) => {
      listeners.get(evt)?.delete(fn);
    },
  };
  globalThis.document = {
    visibilityState: "visible",
    title: "John Smith Account Statement",
    documentElement: { hasAttribute: () => false },
    body: { querySelectorAll: () => [] },
  };

  let shouldFailFetch = true;
  const receivedBatches = [];
  const receivedReplays = [];
  globalThis.fetch = async (url, init) => {
    if (shouldFailFetch) {
      throw new Error("simulated offline network failure");
    }
    const body = JSON.parse(init.body);
    if (String(url).endsWith("/v1/ingest")) {
      receivedBatches.push(body);
      return { ok: true, status: 200, json: async () => ({ status: "ok" }) };
    }
    if (String(url).endsWith("/v1/replay/chunk")) {
      receivedReplays.push(body);
      return { ok: true, status: 200, json: async () => ({ status: "ok", replay_offset_kb: 4 }) };
    }
    return { ok: true, status: 200, json: async () => ({ flags: [] }), headers: { get: () => null } };
  };

  // 1. Visibility listener registered even when enableWireframeReplay is false (`P2-05`)
  const c1 = new PulseWebClient({
    endpointUrl: "http://127.0.0.1:18080",
    apiKey: "pk_live_persist_test",
    flushIntervalMs: 0,
    enableWireframeReplay: false,
  });
  assert.equal(listeners.get("visibilitychange")?.size, 1);
  const anon1 = c1.getDistinctId();
  const sess1 = c1.getSessionId();

  // 2. Reload client -> persists anonId and sessionId via localStorage (`P2-04`)
  c1.shutdown();
  assert.equal(listeners.get("visibilitychange")?.size, 0, "shutdown() must remove visibilitychange listener");

  const c2 = new PulseWebClient({
    endpointUrl: "http://127.0.0.1:18080",
    apiKey: "pk_live_persist_test",
    flushIntervalMs: 0,
    enableWireframeReplay: true,
  });
  assert.equal(c2.getDistinctId(), anon1, "anonId must persist across page loads");
  assert.equal(c2.getSessionId(), sess1, "sessionId must persist within 30m window");

  // 3. Failed flush requeues events instead of dropping them (`P2-05`)
  c2.track("Checkout_Click", { amount: 999 });
  c2.captureDomWireframeSnapshot();
  await c2.flush();
  assert.equal(c2.getQueuedEventCount(), 1, "failed fetch must requeue events");

  shouldFailFetch = false;
  c2.clearBackoffForRetry();
  await c2.flush();
  assert.equal(c2.getQueuedEventCount(), 0);
  assert.equal(receivedBatches.length, 1);
  assert.equal(receivedReplays.length, 1);
  // Title "John Smith Account Statement" MUST be masked to FIXED_MASK (`P3-01`)
  assert.equal(receivedReplays[0].ops[0].nodes[0].masked_label, FIXED_MASK);
  assert.equal(receivedReplays[0].ops[0].ts_offset_ms, 0, "first snapshot starts its chunk");

  // 4. 4 rapid clicks -> only ONE `$rage_tap` emitted (`P3-05`)
  const clickFn = Array.from(listeners.get("click"))[0];
  for (let i = 0; i < 4; i++) {
    clickFn({ clientX: 100, clientY: 100, target: { closest: () => true } });
  }
  await c2.flush();
  const rageEvents = receivedBatches
    .flatMap((b) => b.events)
    .filter((e) => e.event_name === "$rage_tap");
  assert.equal(rageEvents.length, 1, "4 clicks must emit exactly 1 $rage_tap, not 2");

  // 5. reset() rotates anonId and sessionId (`P1-08`)
  c2.reset();
  assert.notEqual(c2.getDistinctId(), anon1);
  assert.notEqual(c2.getSessionId(), sess1);

  c2.shutdown();
  assert.equal(listeners.get("click")?.size, 0, "shutdown() must remove click listener");
});

test("Gaps 1-5, 8-11, 13: consent opt-out, super props, timeEvent, UTMs, autocapture, engage, groups, revenue, replay controls, and exposures", async () => {
  const store = new Map();
  globalThis.localStorage = {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => store.set(k, String(v)),
    removeItem: (k) => store.delete(k),
  };

  const listeners = new Map();
  globalThis.window = {
    innerWidth: 1440,
    innerHeight: 900,
    location: {
      href: "https://app.example.in/kyc?utm_source=google_ads&utm_campaign=diwali_2026&gclid=g_123",
      pathname: "/kyc",
      search: "?utm_source=google_ads&utm_campaign=diwali_2026&gclid=g_123",
    },
    addEventListener: (evt, fn) => {
      if (!listeners.has(evt)) listeners.set(evt, new Set());
      listeners.get(evt).add(fn);
    },
    removeEventListener: (evt, fn) => {
      listeners.get(evt)?.delete(fn);
    },
  };
  Object.defineProperty(globalThis, "navigator", {
    value: {
      userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) Chrome/126.0.0.0",
      language: "en-IN",
    },
    configurable: true,
  });
  globalThis.document = {
    visibilityState: "visible",
    title: "Pulse KYC",
    referrer: "https://google.com/",
    documentElement: { hasAttribute: () => false },
    body: { querySelectorAll: () => [] },
  };

  const calls = [];
  globalThis.fetch = async (url, init) => {
    const u = String(url);
    const body = init?.body ? JSON.parse(init.body) : null;
    calls.push({ url: u, body });
    if (u.includes("/v1/flags/")) {
      return {
        ok: true,
        status: 200,
        headers: { get: () => '"etag_1"' },
        json: async () => ({
          flags: [
            {
              key: "one_tap_upi_mandate",
              enabled: true,
              rollout_bps: 10000,
              variants: [
                { key: "one_tap_sheet", weight_bps: 10000, payload: { theme: "bottom_sheet" } },
              ],
            },
          ],
        }),
      };
    }
    return { ok: true, status: 200, json: async () => ({ status: "ok" }), headers: { get: () => null } };
  };

  const sdk = new PulseWebClient({
    endpointUrl: "http://127.0.0.1:18080",
    apiKey: "pk_live_feature_parity",
    flushIntervalMs: 0,
    autocapture: { pageviews: true, clicks: false, forms: false },
    propertyBlacklist: ["secret_note"],
    replayTriggerEvents: ["Error_Hit"],
    beforeSend: (ev) => (ev.event_name === "DropMe" ? null : ev),
  });

  // Triggered `$pageview` captures default context + UTM attribution
  sdk.register({ app_version: "2.4.0" });
  sdk.registerOnce({ app_version: "1.0.0", install_channel: "play_store" });
  sdk.setGroup("merchant", "m_zepto");
  sdk.timeEvent("KYC_Step");
  sdk.track("DropMe", { a: 1 });
  sdk.track("KYC_Step", { step: 2, secret_note: "ignore_me" });
  assert.equal(sdk.getQueuedEventCount(), 2, "DropMe dropped by beforeSend, only $pageview + KYC_Step queued");

  await sdk.flush();
  const ingestPayloads = calls.filter((c) => c.url.endsWith("/v1/ingest")).flatMap((c) => c.body.events);
  const kycEv = ingestPayloads.find((e) => e.event_name === "KYC_Step");
  assert.equal(kycEv.props.app_version, "2.4.0");
  assert.equal(kycEv.props.install_channel, "play_store");
  assert.equal(kycEv.props.$group_merchant, "m_zepto");
  assert.equal(kycEv.props.utm_source, "google_ads");
  assert.equal(kycEv.props.initial_utm_source, "google_ads");
  assert.equal(kycEv.props.secret_note, undefined, "propertyBlacklist must strip keys");
  assert.equal(typeof kycEv.props.$duration_ms, "number");

  // Engage, Group Props & Revenue
  await sdk.setUserProperties({ plan: "credit_pro" });
  await sdk.incrementUserProperty("logins", 2);
  await sdk.setGroupProperties("merchant", "m_zepto", { tier: "enterprise" });
  await sdk.trackRevenue({ amount: 499, currency: "INR", productId: "pro_plan" });
  assert.ok(calls.some((c) => c.url.endsWith("/v1/engage") && c.body.$set?.plan === "credit_pro"));
  assert.ok(calls.some((c) => c.url.endsWith("/v1/groups") && c.body.group_id === "m_zepto"));
  assert.ok(calls.some((c) => c.url.endsWith("/v1/engage") && c.body.$track_charge?.$amount === 499));

  // Replay trigger + pause/resume + console/network breadcrumbs
  assert.equal(sdk.isRecordingSession(), false, "paused until Error_Hit trigger fires");
  sdk.track("Error_Hit");
  assert.equal(sdk.isRecordingSession(), true);
  sdk.pauseSessionRecording();
  assert.equal(sdk.isRecordingSession(), false);
  sdk.resumeSessionRecording();
  assert.equal(sdk.isRecordingSession(), true);
  sdk.recordNetworkBreadcrumb("POST", "/v1/pay", 504, 1200);
  sdk.recordConsoleError("Unhandled payment timeout");
  sdk.recordConsoleError("Unhandled payment timeout");
  assert.ok(sdk.getSessionReplayUrl().includes(sdk.getSessionId()));

  // Experiment exposure deduplication & remote config payload
  const v1 = await sdk.getVariant("one_tap_upi_mandate", { trackExposure: true });
  const v2 = await sdk.getVariant("one_tap_upi_mandate", { trackExposure: true });
  assert.equal(v1, "one_tap_sheet");
  assert.equal(v2, "one_tap_sheet");
  assert.equal(sdk.isEnabled("one_tap_upi_mandate"), true);
  assert.deepEqual(sdk.getVariantPayload("one_tap_upi_mandate"), { theme: "bottom_sheet" });
  await sdk.flush();
  // Repeated errors upsert one stable banner node instead of stacking copies.
  const bannerIds = calls
    .filter((c) => c.url.endsWith("/v1/replay/chunk"))
    .flatMap((c) => c.body.ops)
    .filter((op) => op.kind === "error_banner")
    .map((op) => op.node_id);
  assert.equal(bannerIds.length, 2);
  assert.equal(new Set(bannerIds).size, 1);
  const banners = calls
    .filter((c) => c.url.endsWith("/v1/replay/chunk"))
    .flatMap((c) => c.body.ops)
    .filter((op) => op.kind === "error_banner");
  assert.ok(banners.every((op) => typeof op.ts_offset_ms === "number"), "banner upserts carry a timestamp");
  const exposureEvents = calls
    .filter((c) => c.url.endsWith("/v1/ingest"))
    .flatMap((c) => c.body.events)
    .filter((e) => e.event_name === "$experiment_started");
  assert.equal(exposureEvents.length, 1, "exposure event must deduplicate per session");

  // Consent opt-out blocks further tracking & clears queue
  sdk.optOut();
  assert.equal(sdk.hasOptedOut(), true);
  sdk.track("Should_Not_Track", {});
  assert.equal(sdk.getQueuedEventCount(), 0);
  sdk.optIn();
  assert.equal(sdk.hasOptedOut(), false);

  sdk.shutdown();
});
