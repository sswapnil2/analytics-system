import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  FIXED_MASK,
  PulseWebClient,
  cohortMemberHash,
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

test("P2-02: evaluateEdgeFlag gates on hashed cohort members, never raw ids", () => {
  const hashes = ["beta_user_1", "beta_user_2"].map((id) => cohortMemberHash("coh_beta", id)).sort();
  const rule = {
    key: "beta_feature",
    enabled: true,
    rollout_bps: 10000,
    targeting_cohort_id: "coh_beta",
    cohort_members: [],
    cohort_member_hashes: hashes,
    variants: [{ key: "treatment", weight_bps: 10000 }],
  };
  assert.equal(evaluateEdgeFlag(rule, "beta_user_1"), "treatment");
  assert.equal(evaluateEdgeFlag(rule, "non_member_user"), null);
  // Targeted with no hashes: fail closed.
  assert.equal(evaluateEdgeFlag({ ...rule, cohort_member_hashes: undefined }, "beta_user_1"), null);
  // Same bytes as pulse_wire::cohort_member_hash (pinned in crates/pulse-wire/src/bucketing.rs).
  assert.equal(cohortMemberHash("coh_beta", "beta_user_1"), "4270f71e9f59a803");
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

  // 1. Visibility listener registered even when enableWireframeReplay is false
  const c1 = new PulseWebClient({
    endpointUrl: "http://127.0.0.1:18080",
    apiKey: "pk_live_persist_test",
    flushIntervalMs: 0,
    enableWireframeReplay: false,
  });
  assert.equal(listeners.get("visibilitychange")?.size, 1);
  const anon1 = c1.getDistinctId();
  const sess1 = c1.getSessionId();

  // 2. Reload client -> persists anonId and sessionId via localStorage
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

  // 3. Failed flush requeues events instead of dropping them
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
  // Title "John Smith Account Statement" MUST be masked to FIXED_MASK
  assert.equal(receivedReplays[0].ops[0].nodes[0].masked_label, FIXED_MASK);
  assert.equal(receivedReplays[0].ops[0].ts_offset_ms, 0, "first snapshot starts its chunk");

  // 4. 4 rapid clicks -> only ONE `$rage_tap` emitted
  const clickFn = Array.from(listeners.get("click"))[0];
  for (let i = 0; i < 4; i++) {
    clickFn({ clientX: 100, clientY: 100, target: { closest: () => true } });
  }
  await c2.flush();
  const rageEvents = receivedBatches
    .flatMap((b) => b.events)
    .filter((e) => e.event_name === "$rage_tap");
  assert.equal(rageEvents.length, 1, "4 clicks must emit exactly 1 $rage_tap, not 2");

  // 5. reset() rotates anonId and sessionId
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
  await sdk.trackRevenue({ amount: 499, productId: "pro_plan" });
  assert.ok(calls.some((c) => c.url.endsWith("/v1/engage") && c.body.$set?.plan === "credit_pro"));
  assert.ok(calls.some((c) => c.url.endsWith("/v1/groups") && c.body.group_id === "m_zepto"));
  assert.ok(
    calls.some(
      (c) =>
        c.url.endsWith("/v1/engage") &&
        c.body.$track_charge?.$amount === 499 &&
        c.body.$track_charge?.currency === "USD",
    ),
  );

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

test("SPA navigations: pushState/replaceState emit one $pageview per new path, and teardown restores history", async () => {
  globalThis.localStorage = { getItem: () => null, setItem: () => {}, removeItem: () => {} };
  const location = { href: "https://app.example.com/", pathname: "/", search: "" };
  const history = {
    pushState(_s, _t, url) { const u = new URL(url, "https://app.example.com"); location.pathname = u.pathname; location.search = u.search; },
    replaceState(_s, _t, url) { if (url) this.pushState(_s, _t, url); },
  };
  const origPush = history.pushState;
  globalThis.window = { innerWidth: 1, innerHeight: 1, location, history, addEventListener: () => {}, removeEventListener: () => {} };
  globalThis.document = { visibilityState: "visible", title: "t", referrer: "", documentElement: { hasAttribute: () => false }, body: { querySelectorAll: () => [] } };
  globalThis.fetch = async () => ({ ok: true, status: 200, json: async () => ({}), headers: { get: () => null } });

  const sdk = new PulseWebClient({
    endpointUrl: "http://127.0.0.1:1",
    apiKey: "pk_test",
    flushIntervalMs: 0,
    autocapture: { pageviews: true, clicks: false, forms: false },
  });
  assert.equal(sdk.getQueuedEventCount(), 1, "initial pageview");
  window.history.pushState({}, "", "/pricing");
  assert.equal(sdk.getQueuedEventCount(), 2, "pushState to a new path");
  window.history.replaceState({ scroll: 10 }, "");
  assert.equal(sdk.getQueuedEventCount(), 2, "same-URL replaceState is not a page");
  window.history.pushState({}, "", "/pricing?plan=pro");
  assert.equal(sdk.getQueuedEventCount(), 3, "query change is a new page");
  sdk.shutdown();
  assert.equal(window.history.pushState, origPush, "history restored on teardown");
});

function browserEnv({ fetchImpl }) {
  const store = new Map();
  const listeners = new Map();
  globalThis.localStorage = {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => store.set(k, String(v)),
    removeItem: (k) => store.delete(k),
  };
  globalThis.window = {
    innerWidth: 1, innerHeight: 1,
    location: { href: "https://app.example.com/", pathname: "/", search: "" },
    addEventListener: (e, f) => { if (!listeners.has(e)) listeners.set(e, new Set()); listeners.get(e).add(f); },
    removeEventListener: (e, f) => listeners.get(e)?.delete(f),
    fetch: fetchImpl,
  };
  globalThis.document = { visibilityState: "visible", title: "t", referrer: "", documentElement: { hasAttribute: () => false }, body: { querySelectorAll: () => [] } };
  globalThis.fetch = (...a) => globalThis.window.fetch(...a);
  return { store, listeners };
}

test("Persisted queue: events tracked offline survive a reload and are sent by the next page", async () => {
  let online = false;
  const sent = [];
  const env = browserEnv({
    fetchImpl: async (url, init) => {
      if (!online) throw new Error("offline");
      if (String(url).endsWith("/v1/ingest")) sent.push(...JSON.parse(init.body).events);
      return { ok: true, status: 200, json: async () => ({}), text: async () => "", headers: { get: () => null } };
    },
  });
  const cfg = { endpointUrl: "http://pulse.test", apiKey: "pk_q", flushIntervalMs: 0, captureErrors: false };
  const a = new PulseWebClient(cfg);
  a.track("Offline_Event_1");
  a.track("Offline_Event_2");
  await a.flush(); // fails: offline
  a.shutdown();
  assert.equal(sent.length, 0);

  online = true;
  const b = new PulseWebClient(cfg); // a "reload": same storage, new instance
  assert.equal(b.getQueuedEventCount(), 2, "queue restored from localStorage");
  b.clearBackoffForRetry();
  await b.flush();
  assert.deepEqual(sent.map((e) => e.event_name), ["Offline_Event_1", "Offline_Event_2"]);
  assert.equal(b.getQueuedEventCount(), 0);
  const c = new PulseWebClient(cfg);
  assert.equal(c.getQueuedEventCount(), 0, "acknowledged events are not resent");
  b.shutdown();
  c.shutdown();
  assert.ok(env.store.size >= 1);
});

test("Error and network capture: uncaught errors become $error; fetch calls land on the replay timeline without query strings", async () => {
  const sent = [];
  const env = browserEnv({
    fetchImpl: async (url, init) => {
      if (String(url).endsWith("/v1/ingest")) sent.push(...JSON.parse(init.body).events);
      if (String(url).includes("/api/kyc")) return { ok: false, status: 504, json: async () => ({}), headers: { get: () => null } };
      return { ok: true, status: 200, json: async () => ({}), text: async () => "", headers: { get: () => null } };
    },
  });
  const sdk = new PulseWebClient({ endpointUrl: "http://pulse.test", apiKey: "pk_e", flushIntervalMs: 0, captureNetwork: true });
  for (const f of env.listeners.get("error") ?? []) f({ message: "boom", filename: "https://app.example.com/main.js?v=3", lineno: 7 });
  for (const f of env.listeners.get("unhandledrejection") ?? []) f({ reason: new Error("nope") });
  await window.fetch("https://api.example.com/api/kyc?pan=ABCDE1234F", { method: "POST" });
  const ops = sdk["replayOps"];
  const crumb = ops.find((o) => o.op === "network_breadcrumb");
  assert.ok(crumb, "breadcrumb recorded");
  assert.equal(crumb.path, "https://api.example.com/api/kyc");
  assert.equal(crumb.status_code, 504);
  assert.equal(crumb.method, "POST");
  await sdk.flush();
  const errors = sent.filter((e) => e.event_name === "$error").map((e) => e.props.message);
  assert.deepEqual(errors, ["boom (https://app.example.com/main.js:7)", "Unhandled rejection: nope"]);
  assert.ok(!sent.some((e) => JSON.stringify(e).includes("ABCDE1234F")), "query string never leaves the page");
  const wrapped = window.fetch;
  sdk.shutdown();
  assert.notEqual(window.fetch, wrapped, "fetch restored on shutdown");
});

test("URL privacy: strips query strings, fragments, userinfo, and mailto/tel from context and autocapture", async () => {
  const sent = [];
  const env = browserEnv({
    fetchImpl: async (url, init) => {
      if (String(url).endsWith("/v1/ingest")) sent.push(...JSON.parse(init.body).events);
      return { ok: true, status: 200, json: async () => ({}), headers: { get: () => null } };
    },
  });
  window.location.href = "https://alice:secret@app.example.com/reset?token=reset_999&utm_source=email#frag";
  window.location.search = "?token=reset_999&utm_source=email";
  document.referrer = "https://idp.example.com/oauth?code=oauth_secret_123";
  const sdk = new PulseWebClient({
    endpointUrl: "http://pulse.test",
    apiKey: "pk_u",
    flushIntervalMs: 0,
    autocapture: { pageviews: false, clicks: true, forms: false },
  });
  sdk.track("Visited");
  const fakeAnchor = {
    tagName: "A",
    textContent: "Email Support",
    href: "mailto:private@example.com?subject=help",
    closest: (sel) => (sel.includes("a") ? fakeAnchor : null),
  };
  for (const f of env.listeners.get("click") ?? []) f({ target: fakeAnchor });
  await sdk.flush();

  const visited = sent.find((e) => e.event_name === "Visited");
  assert.equal(visited.props.$current_url, "https://app.example.com/reset");
  assert.equal(visited.props.$referrer, "https://idp.example.com/oauth");
  assert.equal(visited.props.utm_source, "email");
  const click = sent.find((e) => e.event_name === "$autocapture");
  assert.equal(click.props.href, "mailto:[redacted]");
  const raw = JSON.stringify(sent);
  assert.ok(!raw.includes("reset_999") && !raw.includes("oauth_secret_123") && !raw.includes("private@example.com") && !raw.includes("alice:secret"));
  sdk.shutdown();
});

test("Fetch timeout: hung requests abort, requeue events, and never stall subsequent flushes", async () => {
  let attempt = 0;
  let aborted = false;
  const delivered = [];
  browserEnv({
    fetchImpl: (url, init) => {
      attempt += 1;
      if (attempt === 1) {
        // Simulate a hung mobile socket that never resolves unless aborted.
        return new Promise((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => {
            aborted = true;
            reject(new Error("aborted"));
          });
        });
      }
      if (String(url).endsWith("/v1/ingest")) {
        delivered.push(...JSON.parse(init.body).events);
      }
      return Promise.resolve({ ok: true, status: 200, json: async () => ({}), headers: { get: () => null } });
    },
  });

  const sdk = new PulseWebClient({
    endpointUrl: "http://pulse.test",
    apiKey: "pk_timeout",
    flushIntervalMs: 0,
    fetchTimeoutMs: 40,
  });

  sdk.track("CheckoutStarted");
  const t0 = Date.now();
  await sdk.flush();
  const elapsed = Date.now() - t0;

  assert.ok(elapsed < 1000, `flush should abort promptly, took ${elapsed}ms`);
  assert.equal(aborted, true, "AbortController signal should be aborted on timeout");
  assert.equal(sdk.getQueuedEventCount(), 1, "timed-out batch must be re-queued");

  // Clear backoff and verify the next flush succeeds cleanly without wedging.
  sdk.clearBackoffForRetry();
  await sdk.flush();
  assert.equal(sdk.getQueuedEventCount(), 0);
  assert.equal(delivered.length, 1);
  assert.equal(delivered[0].event_name, "CheckoutStarted");
  sdk.shutdown();
});

test("Pluggable async storage, custom randomBytes, and non-browser safety", async () => {
  const savedWindow = globalThis.window;
  const savedDocument = globalThis.document;
  const savedLocalStorage = globalThis.localStorage;
  const savedFetch = globalThis.fetch;

  const backing = new Map();
  const asyncStorage = {
    async getItem(k) {
      await new Promise((r) => setTimeout(r, 5));
      return backing.get(k) ?? null;
    },
    async setItem(k, v) {
      backing.set(k, v);
    },
    async removeItem(k) {
      backing.delete(k);
    },
  };

  let randCounter = 1;
  const deterministicBytes = (len) => {
    const out = new Uint8Array(len);
    for (let i = 0; i < len; i++) out[i] = (randCounter++ * 17) & 0xff;
    return out;
  };

  const sent = [];
  // Simulate a non-browser / React Native global where document & localStorage do not exist
  // and window is an object without addEventListener.
  globalThis.window = {};
  globalThis.document = undefined;
  globalThis.localStorage = undefined;
  globalThis.fetch = async (url, init) => {
    if (String(url).endsWith("/v1/ingest")) sent.push(...JSON.parse(init.body).events);
    return { ok: true, status: 200, json: async () => ({}), headers: { get: () => null } };
  };

  try {
    const first = new PulseWebClient({
      endpointUrl: "http://pulse.test",
      apiKey: "pk_rn_storage",
      flushIntervalMs: 0,
      storage: asyncStorage,
      randomBytes: deterministicBytes,
      contextProvider: () => ({ $os: "iOS", $app_version: "2.4.0" }),
    });
    await first.ready();
    const anon1 = first.getDistinctId();
    assert.ok(anon1.startsWith("anon_") && anon1.length === 21);
    first.track("AppOpened");
    await first.flush();
    assert.equal(sent.length, 1);
    assert.equal(sent[0].props.$os, "iOS");
    assert.equal(sent[0].props.$app_version, "2.4.0");
    first.shutdown();

    // Second instance hydrates the same anonId from async storage.
    const second = new PulseWebClient({
      endpointUrl: "http://pulse.test",
      apiKey: "pk_rn_storage",
      flushIntervalMs: 0,
      storage: asyncStorage,
      randomBytes: deterministicBytes,
    });
    await second.ready();
    assert.equal(second.getDistinctId(), anon1);
    second.shutdown();
  } finally {
    globalThis.window = savedWindow;
    globalThis.document = savedDocument;
    globalThis.localStorage = savedLocalStorage;
    globalThis.fetch = savedFetch;
  }
});

test("Public replay API and incremental wireframe snapshot diffing", async () => {
  const replayChunks = [];
  const events = [];
  browserEnv({
    fetchImpl: async (url, init) => {
      if (String(url).endsWith("/v1/replay/chunk")) {
        replayChunks.push(JSON.parse(init.body));
        return { ok: true, status: 200, json: async () => ({ replay_offset_kb: 1 }), headers: { get: () => null } };
      }
      if (String(url).endsWith("/v1/ingest")) {
        events.push(...JSON.parse(init.body).events);
      }
      return { ok: true, status: 200, json: async () => ({}), headers: { get: () => null } };
    },
  });

  const sdk = new PulseWebClient({
    endpointUrl: "http://pulse.test",
    apiKey: "pk_replay_diff",
    flushIntervalMs: 0,
    enableWireframeReplay: true,
  });

  const makeTree = (buttonY = 400, includeFooter = true, extraNode = null) => {
    const nodes = [
      { node_id: 1, parent_id: 0, kind: "screen", x: 0, y: 0, w: 390, h: 844, bg_rgba: 0x090d16ff, masked_label: "Home" },
    ];
    for (let i = 2; i <= 15; i++) {
      nodes.push({
        node_id: i,
        parent_id: 1,
        kind: i === 5 ? "masked_input" : "text",
        x: 16,
        y: i * 32,
        w: 358,
        h: 24,
        bg_rgba: 0x162033ff,
        masked_label: i === 5 ? "secret-pin-1234" : `row-${i}`,
      });
    }
    nodes.push({
      node_id: 20,
      parent_id: 1,
      kind: "button",
      x: 16,
      y: buttonY,
      w: 358,
      h: 48,
      bg_rgba: 0x3b82f6ff,
      masked_label: "Pay",
    });
    if (includeFooter) {
      nodes.push({
        node_id: 21,
        parent_id: 1,
        kind: "text",
        x: 16,
        y: 780,
        w: 358,
        h: 24,
        bg_rgba: 0x162033ff,
        masked_label: "Footer",
      });
    }
    if (extraNode) nodes.push(extraNode);
    return nodes;
  };

  // 1. Initial snapshot -> full_snapshot (with masked_input sanitized!)
  sdk.recordWireframeSnapshot(390, 844, makeTree(400, true));
  // 2. Identical snapshot -> 0 ops emitted!
  sdk.recordWireframeSnapshot(390, 844, makeTree(400, true));
  // 3. Small change: move button, remove footer, add bottom sheet container -> incremental diff!
  sdk.recordWireframeSnapshot(
    390,
    844,
    makeTree(420, false, {
      node_id: 30,
      parent_id: 1,
      kind: "container",
      x: 0,
      y: 500,
      w: 390,
      h: 344,
      bg_rgba: 0xffffffff,
      masked_label: "",
    }),
  );
  // 4. Viewport resize -> full_snapshot
  sdk.recordWireframeSnapshot(844, 390, makeTree(200, true));

  // 5. Public tap + rage-tap API
  sdk.recordTap(120, 420, true);
  sdk.recordTap(122, 421, true);
  sdk.recordTap(119, 419, true); // triggers rage_tap + $rage_tap event

  await sdk.flush();

  assert.equal(replayChunks.length, 1);
  const ops = replayChunks[0].ops;
  const fullSnapshots = ops.filter((o) => o.op === "full_snapshot");
  const initialCustom = fullSnapshots.find((o) => o.width === 390 && o.height === 844);
  assert.ok(initialCustom, "initial custom full_snapshot should be present");
  const maskedInput = initialCustom.nodes.find((n) => n.node_id === 5);
  assert.equal(maskedInput.masked_label, "████", "masked_input must be sanitized unconditionally");

  // Incremental diff ops (2 upserts + 1 remove, NO second full_snapshot for the small change)
  const diffUpserts = ops.filter((o) => o.op === "node_upsert");
  const diffRemoves = ops.filter((o) => o.op === "node_remove");
  assert.equal(diffUpserts.length, 2, "only moved button + added sheet should be upserted");
  assert.deepEqual(
    diffUpserts.map((o) => o.node_id).sort((a, b) => a - b),
    [20, 30],
  );
  assert.equal(diffRemoves.length, 1, "only removed footer should be emitted");
  assert.equal(diffRemoves[0].node_id, 21);

  // Only 1 full_snapshot for 390x844 and 1 for 844x390 (not 4!)
  assert.equal(fullSnapshots.filter((o) => o.width === 390 && o.height === 844).length, 1);
  assert.equal(fullSnapshots.filter((o) => o.width === 844 && o.height === 390).length, 1);

  // Rage tap emitted via public recordTap
  const rageOp = ops.find((o) => o.op === "pointer_interaction" && o.kind.type === "rage_tap");
  assert.ok(rageOp, "recordTap should detect rage tap");
  assert.ok(events.some((e) => e.event_name === "$rage_tap"));

  sdk.shutdown();
});

test("Flag TTL: stale flags refresh in the background, so a rollback reaches a running app", async () => {
  let weights = [5000, 5000];
  let flagFetches = 0;
  browserEnv({
    fetchImpl: (url) => {
      if (String(url).includes("/v1/flags/")) {
        flagFetches += 1;
        const body = {
          flags: [
            {
              key: "checkout",
              enabled: true,
              rollout_bps: 10000,
              variants: [
                { key: "control", weight_bps: weights[0] },
                { key: "treatment", weight_bps: weights[1] },
              ],
            },
          ],
        };
        return Promise.resolve({ ok: true, status: 200, json: async () => body, headers: { get: () => null } });
      }
      return Promise.resolve({ ok: true, status: 200, json: async () => ({}), headers: { get: () => null } });
    },
  });
  const sdk = new PulseWebClient({ endpointUrl: "http://pulse.test", apiKey: "pk_ttl", flushIntervalMs: 0, flagsTtlMs: 30 });

  await Promise.all([sdk.getVariant("checkout"), sdk.getVariant("checkout")]);
  assert.equal(flagFetches, 1, "concurrent first calls share one request");
  sdk.getVariantSync("checkout");
  assert.equal(flagFetches, 1, "fresh flags are not refetched");

  weights = [10000, 0];
  await new Promise((r) => setTimeout(r, 40));
  sdk.getVariantSync("checkout");
  assert.equal(flagFetches, 2, "stale flags trigger a background refresh");
  await sdk.refreshFlags();
  assert.equal(sdk.getVariantSync("checkout"), "control", "rolled-back weights apply");
  sdk.shutdown();
});
