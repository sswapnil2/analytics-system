# Step A plan: appender and processor as separate tasks in one process

Status: plan for review. No code yet. Scope is Step A of section 15.11 in `SCALABILITY_REVIEW.md` only. Line numbers are on this branch (stacked on the writer lease, #96, and group commit, #97).

## What Step A changes

Today one request does everything inline (`pulse-ingest/src/service.rs`, `ingest_for_project`): dedup pre-check, schema caps, identity interning and identity flush, then under the bucket lock a dedup re-check, the WAL write, the L0 insert and the dedup commit.

After Step A the request path is the appender and stops at the WAL write. A per-project processor task does the rest, in WAL order.

| Stage | Today | Step A |
|---|---|---|
| Auth, decode, lexicon rules, timestamp normalisation, bucket routing | request | appender (request), unchanged |
| Dedup | request, exact, before and under the lock | processor. The appender keeps the non-mutating pre-check only to fill `deduped` in the response (best effort) |
| Schema caps (`422`) | request | appender checks against the current dictionary (best effort); the processor enforces and dead-letters the excess |
| Identity interning and flush | request, before the WAL write | processor. Flush before a segment is published is already enforced in `flush_bucket` |
| WAL write | request, group-committed (#97) | appender, same code minus dedup |
| Row conversion, L0 insert, session flags, schema discovery | request | processor |
| `identify` / `alias` / profile calls inside a batch | request, after the append (`service.rs:580`) | unchanged, still in the request. See open question 2 |

The WAL layout, manifest fields and recovery order do not change. Those are Step B.

## Pieces

1. **Apply function shared by recovery and the live path.** Recovery already does exactly what the processor must do for a record: dedup by fingerprint, intern, convert, insert (`pulse-storage/src/engine.rs:319-334`). Extract it as `ProjectStorage::apply_wal_record(bucket, wal_seq, batch) -> applied / deduped / dead-lettered`, and call it from both. One code path means a record is treated the same whether it is applied live or after a crash.

2. **Processor task.** One task per open project, started by `IngestService` on first use, fed by a bounded in-process queue of `(bucket, wal_seq, batch, bytes)` in append order per bucket. It runs the dedup ring check and commit (the rings stay in `pulse-ingest`), calls `apply_wal_record`, records session flags, reports discovered schema, and schedules flushes. It only reads the object store during recovery; live records are handed over in memory. On a lost writer lease it stops with the project.

3. **Appender.** The group writer from #97 keeps the queue, the byte cap and the single PUT in flight, and loses the dedup and L0 steps. After the PUT it pushes the record to the processor queue and answers. `accepted` then means "durably logged".

4. **Dead-letter path.** `wire_event_to_row(...)?` appears in recovery (`engine.rs:331`) and in the live append after the WAL write (`engine.rs:502`). The second is worse than section 15 says: the request fails but the record is already durable, so the next restart cannot open the project. In `apply_wal_record`, an event that cannot be converted is written to `wal_dead/{project}/bucket_{NNN}/epoch_{E}_seq_{S}_{index}.json` (create-only, so replaying the same record after a crash does not write it twice), counted in `pulse_ingest_dead_lettered_events_total`, and skipped. Only `DictionaryFull` can fail conversion today (`engine.rs:391`, `401`); object-store errors while writing the dead-letter object still stop the processor and retry, because skipping then would lose the event.

5. **Backpressure.** Three bounds, each a `429` with `Retry-After`:
   - Unconsumed log bytes per project: bytes appended and not yet applied, an atomic updated by the appender and the processor. Limit from open question 3.
   - L0 bytes per project and per node. Event L0 is bounded by rows per bucket only (`engine.rs:535`); `MemBucket` has no byte accounting, so this adds an estimate per row at insert and subtracts it at flush, the same approach as #92 for the agent store.
   - Bytes per log object: the 8 MiB group cap from #97.

6. **Read-your-writes on one node.** A query right after a `200` may run before the processor has applied the event. Proposed: before taking its snapshot, a query on a node that is also the processor waits until the applied position reaches the appended position it observed, with a short timeout (default 250 ms). This keeps today's behaviour for a single node, the SDK tests and `scripts/verify_e2e.sh`, at the cost of one atomic comparison when there is no lag. Needs the owner's yes; the alternative is to accept the lag and change the tests.

7. **Metrics.** Log lag in bytes and seconds per project, dead-lettered events, apply batch size and latency, L0 bytes.

8. **Docs in the same PR.** `docs/rest-api.md` and `docs/limitations.md` get the contract table from 15.4 as it applies to one process: `accepted` means durably logged; `deduped` is best effort; schema-cap excess can be dead-lettered instead of `422`; `409` on identity conflicts is unchanged; new `429` codes. `docs/configuration.md` gets the new limits.

## Order of work, each with a failing test first

1. `apply_wal_record` extraction and the dead-letter path. Test: a WAL record whose event cannot be converted no longer blocks project open; the event is in `wal_dead/`; the rest of the record is applied.
2. L0 byte accounting and its `429`.
3. Processor task and the thin appender. Tests: all of `p1_durability_and_concurrency.rs` still pass; an acknowledged event that was never applied is applied after a crash; duplicates across two unapplied records are stored once.
4. Log-lag bound and the query wait.
5. Loadgen gets per-interval output, which it lacks today, to check the exit criterion "no five-second sample below half the mean".

Exit criteria as in 15.11: sustained load without stalls, a poisoned record does not block the project, RSS under the configured bounds.

## Open questions from 15.11 that affect Step A

- **Question 3 blocks.** How much unconsumed log is allowed before the appender returns `429`. Everything in memory beyond the applied position is also replay time after a crash. I need a number or a rule; a placeholder would be 256 MiB per project and 512 MiB per node, matching the agent memtable limits in #92, but that is the owner's call.
- **Question 2 blocks only if Step A must leave the appender with no writers.** If `identify`, `alias`, `engage`, `groups` and replay chunks may stay synchronous in the request path while the appender and processor share a process, Step A does not need an answer. This plan assumes they stay. One consequence to accept: an `identify` in a batch can then be applied before the events of the same batch.
- Questions 1, 4 and 5 do not affect Step A.

Two more decisions this plan needs that 15.11 does not list:

- Piece 6: wait-for-applied on same-node queries, or accept visible lag.
- Whether `deduped` in the response may become best effort on a single node. The alternative keeps it exact by answering only after the processor has applied the request, which removes most of the benefit.

## Where section 15 does not match the code

- **15.9, "Metadata DB down: the current processor continues".** With the time-based lease in #96 the holder must stop acknowledging once it cannot renew (`engine.rs`, `check_writer`), because it cannot tell an outage from a takeover. Flushes continue; ingest does not.
- **15.3 and 15.12 cite the poisoned-record problem as blocking recovery.** It also has a live cause: `engine.rs:502` fails after the WAL write at `engine.rs:487-493`, leaving a durable record that recovery then cannot apply.
- **15.4, "`deduped` is exact when the appender shares a process with the processor".** Not without extra work: a retry that arrives while the first copy is logged but not yet applied is not in the rings (`pulse-ingest/src/dedup.rs`), so the appender reports it as accepted. The event is still stored once.
- **15.7 calls the L0 byte bound "still open (C5)".** For events it does not exist at all; there is no byte accounting to enforce it with (`engine.rs:535-548` counts rows).
- **15.11 Step A exit criterion** needs five-second samples; `crates/pulse-node/examples/loadgen.rs` reports only whole-run percentiles.
