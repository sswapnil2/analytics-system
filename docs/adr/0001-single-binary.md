# ADR-0001: One binary, no external data middleware

> **Status:** Accepted, as implemented on `main`. **Evidence level:** the *decision* is verified in code. The repo holds no written design discussion, so the *rationale* below is **inferred** from the code, comments and tests, and is marked as such. Alternatives are listed only where the repo itself shows them.

## Context
A product analytics stack usually means operating several systems: a queue (Kafka), an OLAP store (ClickHouse, Druid), a cache (Redis), coordination (ZooKeeper), plus the app. Pulse targets teams that want to self-host.

## Problem
How many moving parts must an operator run to get funnels, retention, replay and flags?

## Decision
Ship everything (ingest, storage engine, query engine, control plane, Studio, MCP) in one Rust binary, `pulse-node`. The only state is a data directory, or an object store plus a SQL database. Evidence: `crates/pulse-node/src/lib.rs` (`start_pulse_node` wires every crate in-process). The README says: "Zero legacy big-data middleware (no ClickHouse, Kafka, ZooKeeper, or external Redis)".

## Alternatives (shown in the repo)
The README names the stacks it avoids: ClickHouse, Kafka, ZooKeeper and Redis. Nothing in the repo shows a comparison being run.

## Trade-offs
- **+** One process to deploy, back up (`deploy/backup.sh` tars one directory) and upgrade; no network hops between ingest, storage and query.
- **−** Pulse has to implement its own WAL, columnar storage, compaction, caching and admission control (`pulse-storage`, `pulse-exec`). Those are large, correctness-critical subsystems.
- **−** Vertical scaling first. Horizontal scaling goes through [node roles](0008-node-roles.md), which share storage.

## Consequences
Crates are libraries composed by `pulse-node`, so each subsystem is testable in-process (see `crates/*/tests`).

## Future considerations
Document a tested multi-node topology (see [Suggested ADRs](README.md#suggested-future-adrs)).
