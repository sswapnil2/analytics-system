# Concepts

These are the terms used across the Pulse docs, the API and the Studio. Each term below means only what is defined here.

## Organisation, project, member

- An **organisation** is a team. Every Studio account belongs to exactly one organisation and sees only that organisation's projects.
- A **project** is one product or environment, for example "web app (prod)". *All* analytics data is scoped to a project: events, users, flags, dashboards, keys. A project has a time zone, which decides where "a day" starts.
- A **member** is a Studio account with a role in its organisation: `viewer`, `member`, `admin` or `owner`. See [roles](rest-api.md#users-organisations-and-roles).

## Keys and tokens

| Name | Prefix | Where it goes |
|---|---|---|
| **Ingest key** | `pk_live_` | In your app. Write-only plus flag reads; safe to make public |
| **Project admin key** | `sk_live_` | Scripts and AI agents. Full access to one project |
| **Root admin token** | `sk_root_` | The node operator only. Full access everywhere |
| **Studio session** | `ps_` | Set as a cookie when a member signs in |

## Event

An **event** is something a user did, with a name and properties, for example `Checkout_Started {cart_value: 99}`. Events are immutable once stored. Every event has:

- a **distinct id**: who did it
- a **session id**: which visit it was part of
- a **timestamp**
- an **event id**, used to deduplicate retries

Names that start with `$` are reserved for events and properties that Pulse or its SDKs create: `$pageview`, `$screen`, `$error`, `$rage_tap`, `$revenue`, `$current_url`, `$duration` and others.

## Distinct id, user, identity merge

- The **distinct id** is the id an event was sent with. Before login it is an anonymous id the SDK generates. After `identify(userId)` it is your user id.
- A **user** (canonical user) is one real person. **Identity merge** links an anonymous id to a user id. Queries then count both as the same user, including events sent *before* the merge. Stored events are never rewritten; the link is applied at query time.
- Two ids that are each already identified cannot be merged (`409`), because that would combine two real people.

## Session

A **session** is one visit. The SDKs start a new session after 30 minutes of inactivity. Server-side events that carry no `session_id` all fall into one session per user (`sess_<distinct_id>`).

## Profile and group

A **user profile** holds properties of the person: plan, signup date, total revenue and so on. You set them with `setUserProperties` or `$set`. A **group profile** does the same for an account or company (`setGroup(type, id)`). Query a profile property as `user.<name>`.

## Lexicon

The **lexicon** is the project's dictionary of every event and property seen so far. It records types, descriptions and owners, and a status for each entry. It is also a **governance** tool:

- A `dropped` event or property is not stored at ingest.
- A `pii_masked` property is masked at ingest, before it reaches storage.

## Query kinds

| Kind | Answers |
|---|---|
| **Insights** (Trends) | How many, over time: totals, uniques, sessions, DAU/WAU/MAU, sums, averages, percentiles, with breakdowns and formulas |
| **Funnel** | What share of users who did A then did B, then C, within a conversion window, and who dropped |
| **Retention** | Of users who did A in a period, how many came back to do B in later periods |
| **Flows** (Journeys) | What users most often did after (or before) an event |
| **Cohort** | Which users match a behavioural or property definition |

Queries are **exact by default**. Only very large ranges are **sampled**, and a sampled result always says so. See [How queries work](how-it-works.md#4-how-queries-work).

## Session replay (wireframe)

**Replay** reconstructs what a user saw as a *wireframe*: element boxes and labels. The SDK masks all text to `████` on the device unless you unmask it. Replay is not a video and not a DOM recording. **Rage taps** (3 or more fast taps in one spot) and **dead taps** (taps on elements that do nothing) are marked on the replay timeline.

## Feature flag, variant, experiment

- A **feature flag** turns a feature on for a percentage of users, or for a cohort.
- A flag can have **variants** (for example `control` and `treatment`) with weights and optional JSON payloads.
- SDKs download the project's flags as a **flag bundle** and evaluate them locally and deterministically.
- An **experiment** measures a flag's variants against goal metrics. It uses **exposure** events, recorded when a user actually saw a variant.

## Cohort

A **cohort** is a saved set of users. It is either *behavioural* (did or didn't do an event, matches a property, combinations of these) or a *fixed list*, for example "users who dropped at funnel step 3".

## Node, role, segment, bucket

These terms are for operators and contributors.

- A **node** is one running `pulse-node` process. Its **role** (`all`, `ingest`, `query`, `compactor`) decides what it serves.
- A **bucket** is one of N (default 16) shards that users are spread across by a hash of their device id.
- A **segment** is an immutable columnar file holding one day of events for one bucket.
- **L0** is the in-memory buffer of recent events that are not yet in a segment.
- The **WAL** is the write-ahead log that makes those L0 rows durable.

See [How Pulse works](how-it-works.md#2-how-events-are-stored).

## MCP

The **Model Context Protocol** is the standard AI agents use to call tools. Pulse serves an MCP endpoint (`POST /v1/mcp`), so agents can read the lexicon, run analytics queries, and inspect agent traces/evaluations. See [MCP for agents](mcp.md) and [AI agent observability](agent-observability-architecture.md).
