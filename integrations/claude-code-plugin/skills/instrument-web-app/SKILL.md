---
name: instrument-web-app
description: Add Pulse analytics to a web app with the Pulse web SDK (sdks/ts-web): install, initialise with an ingest key, track events, identify users, feature flags and privacy-masked session replay. Use when asked to add Pulse tracking or analytics events to a browser app.
---

# Instrument a web app with the Pulse web SDK

The SDK is `@pulse-analytics/ts-web`, in `sdks/ts-web` of the Pulse repo. It is **not on npm**: build
it (`npm --prefix sdks/ts-web ci && npm --prefix sdks/ts-web run build`) and install it by path
(`npm install <path-to-pulse>/sdks/ts-web`) or copy `dist/` into the app.

1. **Get an ingest key** (`pk_live_…`). Never put an `sk_` admin key in browser code. If the user has
   none, create one: `POST /v1/projects/<project_id>/api-keys` with
   `{"label": "web", "role": "ingest_and_flags"}` and an admin bearer token; the key is in
   `plaintext_key` and is shown only once.
2. **Create one client** at app start and export it:

   ```ts
   import { PulseWebClient } from "@pulse-analytics/ts-web";
   export const pulse = new PulseWebClient({
     endpointUrl: import.meta.env.VITE_PULSE_URL, // e.g. http://127.0.0.1:8080
     apiKey: import.meta.env.VITE_PULSE_KEY,
     autocapture: true,
   });
   ```

   Read the URL and key from the app's existing config mechanism rather than hard-coding them.
3. **Track the events that matter.** Before naming events, check what exists: if the `pulse` MCP
   server is connected, call `get_lexicon_schema` and reuse existing names and property keys. Use one
   naming style consistently (the demo data uses `Title_Snake_Case`, for example `KYC_PAN_Submitted`).
   `pulse.track(name, props)` takes flat props of string, number, boolean or null.
4. **Identify on login, reset on logout:** `await pulse.identify(userId, { plan })` and `pulse.reset()`.
5. **Single-page apps:** autocapture only sees `popstate`, so track a page view on each router navigation.
6. **Replay (optional):** set `enableWireframeReplay: true` and call
   `pulse.captureDomWireframeSnapshot()` after load and after each screen change, since the SDK doesn't
   snapshot on its own. Text is masked by default; add `data-pulse-unmask` only to static labels and
   headings, never to inputs or personal data. Errors are not captured automatically: call
   `pulse.recordConsoleError(message)` from the app's error handler.
7. **Privacy:** strip tokens or emails from URLs with `beforeSend`, since `$current_url` keeps the
   query string. Use `propertyBlacklist` for props that must never leave the browser.

Verify: send one event, then confirm it arrived, either with the `pulse` MCP tool `query_insights`
(`event_name` set to the new event, `days: 1`) or in the Pulse Studio.
