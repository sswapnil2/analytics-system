# Web SDK (`sdks/ts-web`)

A zero-dependency TypeScript client (`PulseWebClient`) for browsers. It batches events, persists its
queue and identity in `localStorage`, evaluates feature flags locally, and can record privacy-masked
DOM wireframes for session replay.

## Install

The SDK is not published to npm yet. Build it from the repo and import the ES module:

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
  autocapture: true,                     // pageviews (popstate), clicks, form submits
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

Assignment is deterministic (xxh3 bucketing on the user's routing seed), so the client and the
server's experiment analysis agree. Record that a user saw a variant with `trackExposure(key, variant)`
or `getVariant(key, { trackExposure: true })`; repeat exposures of the same variant are deduplicated.

## Session replay

Replay records wireframes (element boxes and masked labels), not pixels or HTML. Current limits to know:

- Nothing is recorded until you call `pulse.captureDomWireframeSnapshot()`. Call it after load and after each screen change.
- Text is masked to `████` unless an element (or its ancestor) has `data-pulse-unmask` or matches `unmaskSelectors`. Inputs and `contenteditable` content stay masked regardless.
- Errors and network calls are not captured automatically: call `recordConsoleError(message)` and `recordNetworkBreadcrumb(...)` yourself.
- Autocapture pageviews listen to `popstate` only, so single-page-app `pushState` navigations need a manual `track`.
- `$current_url` includes the query string; strip sensitive parameters with `beforeSend`.

Controls: `startSessionRecording()`, `stopSessionRecording()`, `pauseSessionRecording()`,
`resumeSessionRecording()`, `isRecordingSession()`. Rage taps and dead taps are detected from clicks.
