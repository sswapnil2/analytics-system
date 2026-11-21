# Web SDK (`sdks/ts-web`)

A zero-dependency TypeScript client (`PulseWebClient`) for browsers. It batches events and persists identity,
session, super properties and the unsent event queue in `localStorage`, so events tracked offline or
just before the tab closes are sent by the next page load (they keep their ids, so ones that already
arrived are deduplicated). It also captures uncaught errors, evaluates feature flags locally, and can
record privacy-masked DOM wireframes for session replay.

## Install

Every [GitHub release](https://github.com/sswapnil2/analytics-system/releases/latest) attaches the
packed SDK. Install it straight from the release (replace `0.1.2` with the release you want):

```bash
npm install https://github.com/sswapnil2/analytics-system/releases/download/v0.1.2/pulse-analytics-ts-web-0.1.2.tgz
```

It is not on the npm registry yet. To build it from source instead:

```bash
npm --prefix sdks/ts-web ci && npm --prefix sdks/ts-web run build   # -> sdks/ts-web/dist/index.js
```

Then either `npm install ../path/to/sdks/ts-web` in your app, or serve `dist/` and
`import { PulseWebClient } from "./pulse/index.js"`.

## Set up

```ts
import { PulseWebClient } from "@pulse-analytics/ts-web";

const pulse = new PulseWebClient({
  endpointUrl: "http://127.0.0.1:8080", // your node
  apiKey: "pk_live_…",                  // an ingest key, never an sk_ key
  autocapture: true,                     // pageviews (incl. SPA pushState/replaceState), clicks, form submits
  enableWireframeReplay: true,
});
```

Ingest routes allow any origin (permissive CORS), so the app can live on a different host from the node.

| Option | Default | Meaning |
|---|---|---|
| `endpointUrl`, `apiKey` | required | Node URL and `pk_` ingest key |
| `flushBatchSize` / `flushIntervalMs` | 20 / 5000 ms | Send when the queue reaches this size, or on the interval |
| `maxQueuedEvents` | built-in cap | Oldest events are dropped beyond this |
| `optOutByDefault` | `false` | Start opted out until `optIn()` |
| `propertyBlacklist` | `[]` | Property names stripped from every event |
| `beforeSend(event)` | none | Edit an event, or return `null` to drop it |
| `autocapture` | off | `true` or `{ pageviews, clicks, forms, allowSelectors, blockSelectors }` |
| `enableWireframeReplay`, `replaySampleRate`, `replayTriggerEvents` | off | Session replay and when to record |
| `maskSelectors` / `blockSelectors` / `unmaskSelectors` | | Replay privacy controls (text is masked by default) |
| `captureErrors` | `true` | `window` errors and unhandled promise rejections become `$error` events (message and script path, no query string) |
| `captureNetwork` | same as `enableWireframeReplay` | Every `fetch`/XHR call (method, URL without query string, status, duration) goes on the replay timeline; calls to Pulse itself are skipped |
| `debug` | `false` | Logs each tracked event and each send result, including events the server rejected |

## Track and identify

```ts
pulse.track("Checkout_Started", { cart_value: 99, currency: "USD" });
await pulse.identify("usr_1042", { plan: "pro" });  // merges the anonymous id into the user
pulse.register({ app_version: "2.3.0" });           // super property, sent with every event
pulse.timeEvent("Video_Watched");                   // next track() of this event gets $duration / $duration_ms
await pulse.trackRevenue({ amount: 49, currency: "USD", productId: "pro_monthly" });
pulse.reset();                                      // on logout: new anonymous id and session
```

User and group profiles: `setUserProperties`, `setUserPropertiesOnce`, `incrementUserProperty`,
`unsetUserProperties`, `appendUserProperty`, `unionUserProperty`, `removeUserProperty`, `deleteUser`,
`setGroup(type, id)`, `setGroupProperties`.

`deleteUser()` only forgets the user on this device. The ingest key is public, so it can't erase
server-side data; erase a user with the admin API, `DELETE /v1/projects/:id/users/:distinct_id`.

Consent: `optOut()`, `optIn()`, `hasOptedOut()`, `clearOptInOut()`.

Delivery: `flush()` sends now; `flushBeacon()` sends with `keepalive` on page hide (wired up for you);
`shutdown()` stops timers and listeners.

## Feature flags

```ts
await pulse.refreshFlags();                 // GET /v1/flags/:apiKey (ETag-cached)
if (pulse.isEnabled("new_checkout")) { … }
const variant = pulse.getVariantSync("pricing_test");  // or await pulse.getVariant(key)
const payload = pulse.getVariantPayload("pricing_test");
```

Assignment is deterministic: `xxh3_64("{flag_key}:{distinct_id}")` picks the rollout slot, so the client, the
other SDKs and the server's experiment analysis agree. It uses the *current* distinct id, so a user can
land in a different variant after `identify()` switches them from the anonymous id to their user id;
call `identify()` before the first flag check when that matters. Record that a user saw a variant with `trackExposure(key, variant)`
or `getVariant(key, { trackExposure: true })`; repeat exposures of the same variant are deduplicated.

## Session replay

Replay records wireframes (element boxes and masked labels), not pixels or HTML. Current limits to know:

- With `enableWireframeReplay`, a snapshot is taken on load and whenever the DOM changes (at most once a second). `captureDomWireframeSnapshot()` is still available for a manual snapshot.
- Text is masked to `████` unless an element (or its ancestor) has `data-pulse-unmask` or matches `unmaskSelectors`. Inputs and `contenteditable` content stay masked regardless.
- Errors and network calls are captured automatically (see `captureErrors` / `captureNetwork`). `recordConsoleError(message)` and `recordNetworkBreadcrumb(...)` remain for errors you catch yourself.
- Autocapture pageviews follow `history.pushState`, `replaceState` and `popstate`, and send one `$pageview` per new path + query string (hash changes and same-URL `replaceState` calls are not pages).
- `$current_url` and `$referrer` are sent without their query string, fragment or embedded credentials. UTM parameters and ad click ids are read from the URL and kept as separate properties.

Controls: `startSessionRecording()`, `stopSessionRecording()`, `pauseSessionRecording()`,
`resumeSessionRecording()`, `isRecordingSession()`. Rage taps and dead taps are detected from clicks.
