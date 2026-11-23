# Architecture Decision Records

These ADRs record the main design decisions in Pulse: what was decided, the evidence for it in the code, and the trade-offs. They are written *after the fact*, from the implementation on `main`. The repo contains no design discussions or meeting notes. So each ADR keeps three things apart:

- **Verified decision:** what the code does, with file references.
- **Inferred rationale:** why it is probably built this way, reasoned from the code, comments and tests. It is labelled as inferred.
- **Alternatives:** listed only where the repo itself names them. None are invented.

| ADR | Decision |
|---|---|
| [ADR-0001](0001-single-binary.md) | One binary, no external data middleware |
| [ADR-0002](0002-object-store.md) | The object store is the only durable layer for event data |
| [ADR-0003](0003-wal-before-ack.md) | Acknowledge ingest only after a durable WAL write |
| [ADR-0004](0004-user-bucketing.md) | Route events to buckets by a stable device id |
| [ADR-0005](0005-segments-and-manifests.md) | Immutable columnar segments, with footers kept in a CAS-committed manifest |
| [ADR-0006](0006-query-time-identity.md) | Resolve identity at query time and never rewrite history |
| [ADR-0007](0007-exact-by-default.md) | Exact results by default, with disclosed sampling only above a threshold |
| [ADR-0008](0008-node-roles.md) | Scale out by node role over shared storage |
| [ADR-0009](0009-metadata-in-sql.md) | Configuration and accounts in SQL (SQLite or Postgres) |
| [ADR-0010](0010-envelope-encryption.md) | Optional envelope encryption for all stored objects |
| [ADR-0011](0011-wireframe-replay.md) | Wireframe replay, masked on the device |
| [ADR-0012](0012-local-flag-evaluation.md) | Deterministic flag evaluation, shared by SDKs and server |

## Suggested future ADRs

These are decisions that exist in the code but have no recorded reasoning, or questions that are still open:

- **Multi-node write topology.** How several ingest nodes share projects safely, and which guarantees hold.
- **Data retention.** Whether and how to add time-based TTLs alongside erasure.
- **Vortex vs. Parquet for segments.** Why Vortex was chosen, and the plan for format upgrades.
- **Bucket count migration.** How to change `PULSE_NUM_BUCKETS` on existing data.
- **Experiment statistics.** Fixed-horizon versus sequential testing (in review in separate pull requests).
- **OpenTelemetry and agent tracing.** Design rationale for `pulse-agent` (`PTWL`/`PLSS` and OTLP GenAI ingestion).
- **Server SDKs.** Whether to ship language SDKs or keep HTTP-only.

## Writing a new ADR

Copy any ADR here. Name it `NNNN-short-name.md`, set its status (`Proposed`, `Accepted`, `Superseded by ADR-XXXX`), and link it from the table above.
