# Competitive analysis: Pulse vs PostHog, Mixpanel, Amplitude

**Sources**

- **Pulse:** verified in this review. See [test-results.md](test-results.md).
- **Competitors:** the reviewer's working knowledge of their public products and docs as of 2026. Not re-verified live during this review. Pricing tiers change often; treat pricing statements as directional.

## Summary matrix

Ratings compare Pulse against the best of the three competitors on each dimension: **Better**, **Comparable**, **Behind** or **Missing**.

| Dimension | Pulse | PostHog | Mixpanel | Amplitude | Pulse vs best |
|---|---|---|---|---|---|
| Integration | REST ingest; web SDK in-repo only; no mobile or server SDKs | npm/CDN, ~10 SDKs, autocapture | npm/CDN, mobile and server SDKs, warehouse connectors | Same as Mixpanel, plus CDPs | **Behind** |
| Onboarding / time to first insight | Build from source; seeded demo is good | Cloud signup in minutes; autocapture gives data on day one | Cloud signup, demo project | Cloud signup, demo project | **Behind** |
| Event analytics (Trends) | Multi-series, formulas, percentiles, compare; daily only, no filters in UI | Full trends, SQL (HogQL) | Best-in-class Insights UX | Very deep (formulas, derived metrics) | **Behind** |
| Funnels | Correct; three order modes, hold-constant, time to convert, drop-off replays; no filters in UI | Comparable, plus correlation analysis | Strong, with segments and filters everywhere | Strongest (conversion drivers, impact) | **Behind** in UI; **Comparable** in engine |
| Retention | Correct; three modes; UI is day-only, ≤13 periods | Comparable, plus stickiness and lifecycle | Strong, with cohort comparisons | Strongest (bracket, compass) | **Behind** |
| Segmentation | One event-property split; cohorts unusable; user props broken above 1,000 users | Rich | Rich | Richest | **Behind** (weakest area) |
| Session replay | Wireframe, on-device masking, funnel-linked; manual error/network capture | Full DOM replay, console and network capture, linked to events | Replay add-on | Replay add-on | **Comparable**. Better on privacy and funnel linkage; behind on fidelity and automatic capture. |
| Dashboards and collaboration | Boards, date range, TV mode, copy link | Boards, subscriptions, alerts, comments | Boards, alerts, digests, annotations | Boards, alerts, notebooks, annotations | **Behind** |
| Developer experience | Clear REST docs and errors; no packages; no key revocation; no CI | Excellent | Good | Good | **Behind** |
| AI integration (MCP) | Built-in, project-scoped, 5 read-only tools; thin | AI assistant plus MCP server | AI query features plus MCP | AI query features plus MCP | **Comparable** on the plumbing; **behind** on depth |
| Deployment / self-hosting | **One Rust binary, local disk or S3**; no image yet | Self-host needs ClickHouse, Kafka, Postgres and Redis; positioned as hobby-scale | SaaS only | SaaS only (warehouse-native option) | **Better** in design; **Comparable** in practice until packaged |
| Performance | ms-level queries at 1M events on a laptop; correct exact counts | Fast at scale (ClickHouse) | Fast | Fast | **Comparable** (unproven above about 10M events) |
| Pricing / ownership | Infrastructure cost only; Apache-2.0 core; some add-ons licensed | OSS core plus usage-based cloud | Event-based tiers with a free tier | MTU or event-based tiers | **Better** for predictability and sovereignty |

## Dimension notes

### Where Pulse is genuinely better

- **Operational footprint.** Self-hosting PostHog at 500k MAU means running ClickHouse and Kafka. Pulse is one process with an object store. If the packaging gaps close (Docker, releases, upgrades, backups), this is a decisive advantage for teams that must self-host.
- **Replay privacy model.** It masks on the device and records layout, not pixels or HTML, so PAN, card numbers and passwords never leave the device. For a regulated buyer this lowers the compliance review a lot. Competitors offer masking, but they capture DOM by default and mask by configuration.
- **Funnel-to-replay linkage.** In Mixpanel and Amplitude, replay is a separate surface. Pulse puts the dropped user's replay inside the funnel. PostHog can do this, but with more clicks.
- **Cost predictability.** There is no per-event or per-MTU meter.

### Where Pulse is comparable

- **Engine semantics.** Pulse has the same core semantics as the incumbents: funnel order modes, exclusions, hold-constant, time to convert, three retention modes, behavioural cohort algebra and DAU/WAU/MAU. Correctness was verified. The gap is exposure in the UI, not capability.
- **MCP.** Shipping an MCP server is now table stakes for all four. Pulse's per-project key scoping is clean.

### Where Pulse is behind

- **Segmentation and filters.** This is the single largest gap, and it's what a PM evaluates in the first ten minutes.
- **SDK coverage.** Competitors have mature SDKs on every platform with autocapture. Pulse has one unpublished web SDK.
- **Collaboration and monitoring.** Competitors have alerts, digests, annotations and comments. Pulse's alerts are API-only and licence-gated.
- **Ecosystem.** Pulse has no CDP destinations, warehouse sync or reverse-ETL. Raw export caps at 50,000 rows.

### Missing entirely

- Lifecycle and stickiness reports in the UI.
- Correlation and "conversion drivers".
- SQL access.
- Annotations.
- Scheduled reports.
- Mobile SDK packages.
- SSO/SAML in the community edition.

## Positioning recommendation

Don't fight Amplitude or Mixpanel on analytical breadth, and don't fight PostHog on product count. Win where they can't follow:

1. **"Self-hosted analytics that a two-person platform team can run."** One binary, S3, no ClickHouse. Prove it with a Docker/Helm install and a published upgrade story.
2. **"Replay your compliance team will approve."** On-device masking, no pixels, data in-region, DPDP/GDPR erasure with an audit trail.
3. **"From the drop to the failing request in two clicks."** Make automatic error and network capture real in the SDK so the demo experience matches production.

The ideal first customer is a regulated mid-size app (fintech, health, insurance, gov-tech) that is currently blocked from using SaaS replay and is unwilling to run ClickHouse.
