# Design note: local hot tier, S3 for compacted data only (proposed)

Status: proposal, revision 4 after three independent reviews (section 9). No code. It follows section 15 of [SCALABILITY_REVIEW.md](SCALABILITY_REVIEW.md) and the P0-7 decision (hot state on local disk). File and line references are to `fix/scalability-review-quick-wins` at `4895bbc`.

Sections 1 to 5 cover the storage half: one node, its volume, and what it sends to S3. Section 6 covers the other half of the owner's target: stateless writers that forward to storage nodes which own projects. Both halves were reviewed against the code (section 9).

Claims are marked **measured** (a number from a run), **code** (read in the source) or **estimate** (arithmetic from stated assumptions). Nothing here has been run on S3.

## 1. Goal

Make Pulse cheap to run on one machine with an EBS volume and an S3 bucket, without losing anything on a process crash. The owner's four points:

1. Shared nothing: a node owns its projects and its own disk.
2. Profiles, events and other hot data live on the EBS volume.
3. Segments are compacted on an interval and only then pushed to S3.
4. Profiles are updated in memory and on disk, and pushed to S3 on an interval.

P0-7 delivers point 2. This note covers points 3 and 4.

## 2. What it costs today

Counting object-store wrapper under one node, in-memory store, sequential requests, one new user per event (**measured**, staff engineer, integration head `4895bbc`):

| Source | PUTs per 1,000 requests | Path |
|---|---|---|
| Event WAL | 1,000 | acknowledge path |
| Identity store (SlateDB) | 1,000 | acknowledge path, when the request has a new user |
| Agent trace WAL | 1,000 | acknowledge path |
| Agent segments | 200 | flush, about one per 5 traces |
| Event segments + manifests | 30 | flush |

Idle projects make about 1,100 PUTs per hour from agent maintenance (**measured**).

After P0-7 the first three rows are zero. What remains is every flush:

- Each bucket flush writes one segment object per date to the segment store, then a full manifest object (**code**: `segment.rs:417`, `manifest.rs:208`).
- Compaction runs once a partition has 4 segments and rewrites them into one, reading the inputs back from the segment store (**code**: `engine.rs:69`, `engine.rs:1006`). An already-compacted segment is read and rewritten each time four more small ones arrive.
- Session-replay chunks cost one PUT and one LIST per chunk (**code**: `replay_store.rs:141`, `:158`).

## 3. Design

One rule: **an object is written to S3 once, when it is in its long-lived form.** Everything younger lives on the node's volume.

### 3.1 Two tiers by path prefix

A segment's location is its `object_path`; the path is built in one place and every read and delete goes through it, and query caches key on segment id, not path (**code**, confirmed in review: `segment.rs:451`, `:1029-1050`; `pulse-exec/src/cache.rs:214-341`). So tiering is a prefix router under the existing encryption wrapper, with no change to `SegmentMeta`.

| Prefix | Tier | Contents |
|---|---|---|
| `wal/`, `agent_wal/` | local | write-ahead logs (P0-7) |
| `wal_dead/` | local | dead-lettered events (`engine.rs:592`); local avoids 1 S3 PUT per bad event during a schema-cap storm |
| `identity/`, `profiles/` | local | SlateDB stores (P0-7) |
| `hot/segments/` | local | segments written by an L0 flush, not yet uploaded |
| `manifests/` | local | the authoritative manifest, every epoch |
| `segments/` | S3 | uploaded segments |
| `published/` | S3 | published manifests, identity and profile snapshots |
| `replay/`, `agent_segments/` | S3 (local hot in T4) | session-replay chunks and agent segments; move to local hot tier + cycle upload in T4 |

### 3.2 The upload cycle

A per-project task runs every `PULSE_UPLOAD_INTERVAL_SECS` (proposed default 300), and earlier if local segment bytes pass a threshold.

The order matters. A user id is the count of users at the time it was created (**code**: `pulse-identity/src/lib.rs:959`), so an identity store restored from a snapshot will hand out again any id the snapshot does not contain. Profile rows are keyed by that id too (`profile_store.rs:37`). Therefore identity is snapshotted last, after everything that can reference an id has been frozen (because identity is taken at `T_id >= T_prof >= T_freeze`, it is guaranteed to cover every id referenced by both the frozen segments and the profile snapshot; extra ids created in between without a profile yet are harmless):

1. **Freeze.** Record the exact list of local segment ids to upload, and write an intent record locally (the new S3 segment ids and their sources).
2. **Snapshot profiles** to S3.
3. **Flush and snapshot identity** to S3. Record the user count as a high-water mark.
4. **Upload.** Merge only the frozen segments, per partition, and write each result create-only to `segments/`. Existing S3 segments are not read or rewritten. Input bytes per output segment are capped, because the merge decodes its inputs into memory (`engine.rs:1072-1076`).
5. **Commit** the local manifest: frozen local segments out, new S3 segments in. This reuses the existing check that the sources are still active (`engine.rs:1134-1147`).
6. **Publish** a manifest to `published/`, create-only and named by its own dense, contiguous `publish_seq` (`0, 1, 2, …`) rather than the sparse local `epoch` (3.6), listing S3 segments only and carrying `writer_generation`, `source_local_epoch`, and the identity high-water mark. Like `commit_next` (`manifest.rs:220-232`), verify `head(publish_seq - 1)` after create so a writer more than `retain_epochs` behind cannot slip past GC. Then record `last_published_seq = publish_seq` in the local manifest.
7. **Clean up.** Delete the uploaded local segments once no snapshot pins them; clear the intent record.

Publish and restore both assert that the identity snapshot's user count is at least the manifest's high-water mark.

Crash handling. There is no sweep of stored segments today: superseded paths sit in an in-memory list (`engine.rs:154`), so anything not cleaned before a crash would leak. This design adds two things at project open: replay or discard the intent record (delete S3 objects it names that the manifest does not reference), and sweep `hot/segments/` against the local manifest.

| Crash after | State on restart | Handled by |
|---|---|---|
| 1-3 | Snapshots newer than needed; intent with nothing uploaded | intent discarded |
| 4 | S3 segments nothing references | intent replay deletes them |
| 5 | Local manifest ahead of the published one | step 6 on the next cycle |
| 6 (before local `last_published_seq` update) | `published/{P+1}` exists with our `writer_generation` and `source_local_epoch <= local.epoch` | open recognises its own publish, advances `last_published_seq` (3.6), sweep cleans local segments |
| 6 (after local update) | Uploaded local segments still on disk | sweep at open |

Review found no loss or duplication of an acknowledged event on a process crash in this sequence (**code**: flush order `engine.rs:743-809`, recovery from `committed_wal_seq`, `wal.rs:176`).

### 3.3 S3-side merge and when an S3 segment may be deleted

Uploading every interval leaves one S3 segment per interval per active partition. A slow job on the owning node merges a partition's S3 segments once it is cold (proposed: the date is over and nothing was uploaded to it for an hour).

Deletion rule, for this job and any other that supersedes an S3 segment: delete only after a published manifest that no longer references it is in S3, plus a grace of two publish intervals. The pending-delete list is persisted. Today superseded segments are deleted as soon as no local reader pins them (`engine.rs:1158-1224`), which would let a volume loss take whole partitions with it and would pull objects out from under readers on other machines.

Count-based compaction (every fourth flush) stops for the local tier, where segments are merged once at upload. It stays in force when the interval is zero (section 7).

### 3.4 Identity and profile snapshots

Review answered open question 1 from revision 1: a plain file-level mirror of a SlateDB 0.6 directory is **not** a consistent restore point. SlateDB's WAL tables are not referenced by its manifest, `Db::flush()` flushes only the WAL, and its own garbage collection can remove files mid-copy (**code**: slatedb `db_state.rs:241`, `db.rs:1136`, and the 300 s GC age in `pulse-identity/src/lib.rs:695`).

Two candidates; T1 picks one by test:

- **Checkpoint copy.** Pin with `Db::create_checkpoint`, which garbage collection respects. Upload that checkpoint's tables, then the WAL tables above its `last_compacted_wal_sst_id` in ascending order, then the manifest. Delete in S3 by the same rule. Track what has been uploaded locally, not by listing S3.
- **Logical export.** Write the identity graph and profile rows as one or a few objects. Correct by construction; costs a full rewrite per cycle, so it needs a size limit or a delta form.

Encryption binds the object path into the key wrap (`encrypted_store.rs:102-112`), so a snapshot must be restored to the same key it was sealed under, or re-sealed. Other machines read identity by downloading the snapshot to that same key locally; they do not open the S3 copy in place, because a SlateDB reader writes into the store it opens (slatedb `db_reader.rs:150`).

Cost caveat: SlateDB writes one WAL table per flush with pending changes, so a checkpoint copy defers those PUTs but may not reduce them much. The measured number decides between the two candidates.

Exit test either way: restore from S3 into an empty directory and resolve every user id in every published segment and profile row.

### 3.5 Restore after losing the volume

P0-7 makes a node refuse to start when the marker says `local` and the local stores are missing. That guard is what makes losing the volume an outage, not silent reuse of user ids. This design adds an explicit command:

`pulse-node restore --from-published`

1. Copies the identity and profile snapshots down and checks the high-water mark.
2. Reconciles the restored state with `erased_uids` in the external metadata DB (which is ahead of the snapshot):
   - Deletes from the restored profile store any profile whose `canonical_uid` (or any raw `uid` for its `distinct_id` in the restored identity graph) is in `erased_uids` with `updated_at_ms <= erased_at_ms`. Without this, users erased between the last publish and volume loss would have their profiles reappear, because profile reads do not currently filter by `erased_uids` (`api.rs:790`, `:3940`, `:3989`).
   - Deletes rows from `erased_uids` where `uid >= high_water_mark` (leaving `deletion_audit_log` intact). No restored segment or profile can reference those ids, and leaving them would poison the next new users who are handed those dense `u32` ids after restore if they send backdated or imported events with timestamps `<= erased_at_ms`.
3. Installs the newest published manifest as the local manifest (with `last_published_seq` set to its `publish_seq`).
4. Prints the publish time and states the loss: events, profile updates, identity merges and erasures after that time are gone.

A node never restores on its own.

The metadata database is part of this. By default it is SQLite in the data directory (`pulse-control/src/db.rs:227`), on the same volume, and it holds accounts, API keys, projects, cohorts and the erasure tombstones that hide erased users at query time (`api.rs:1037`). Losing it with the volume would bring erased users back.

**Decided by the owner: run an external metadata database.** A production deployment sets `DATABASE_URL` to a database that does not live on the node's volume. The tombstones, accounts and keys then survive the loss of a volume, so nothing about erasure needs to be published to S3. SQLite stays for evaluation on one machine. Proposed consequences (the owner has not ruled on these two): the node logs a warning at start when it runs local-tier mode on SQLite, and `restore --from-published` refuses on SQLite, because the accounts and projects it would restore into are gone with the same volume.

### 3.6 Fencing

Today the writer generation is fenced by `commit_next` in the shared store (`engine.rs:219-235`, `manifest.rs:208-235`), which only works because `epoch` is dense (`+1` per S3 object). With local flushes advancing `local.epoch` between publishes, `published/` would have sparse epochs if named by `local.epoch`: a stale writer at local epoch `105` (gen 1) could create-only `published/105` right past a restored writer that published `published/101` (gen 2), shadowing the newer generation. So:

- `published/` is named by its own dense, contiguous `publish_seq` (`0, 1, 2, …`), written create-only with `writer_generation` and `source_local_epoch`, plus the `head(publish_seq - 1)` GC guard (3.2 step 6). Two writers always contend on the same `publish_seq + 1` key, so the loser gets `AlreadyExists`.
- On project open for writing, a node compares the latest published manifest `(pub_seq, pub_gen, source_local_epoch)` against its local manifest `(local_last_pub_seq, local_gen, local_epoch)`. It refuses to open (except through `restore`) whenever `pub_gen > local_gen`, or whenever `pub_seq > local_last_pub_seq` except in the exact self-crash case from 3.2 step 6 (`pub_seq == local_last_pub_seq + 1`, `pub_gen == local_gen`, and `source_local_epoch <= local_epoch`, in which case it advances `local_last_pub_seq` and proceeds).

### 3.7 Backpressure and disk

- Local segment bytes are bounded per project and per node. Past the bound the upload task runs immediately; past twice the bound ingest answers `429`.
- If S3 is unreachable, ingest continues until that bound, then `429`.
- Disk full or unwritable is `503` on ingest, never a panic and never an acknowledged write.
- A restart while S3 is unreachable must not seed dedup from an empty read: today read errors are swallowed there (`engine.rs:336`), which would accept client retries as new events.

### 3.8 Durability on power loss

A process crash and a machine crash are different. Review found that the local store's single `put` fsyncs file and directory, but its multipart path does not (`store_factory.rs:80-97`), and SlateDB writes its tables through that path. On an operating-system crash an identity table might not be on disk while fsync'd segments already reference its ids. The multipart path must fsync before it reports success. This applies to P0-7 as soon as identity is local, and has been sent to that work.

### 3.9 Encryption and erasure

- The prefix router sits below the one encryption wrapper, so the local tier is encrypted when `PULSE_BYOK_MASTER_KEY` is set (`pulse-node/src/lib.rs:360`).
- **Decided by the owner: erasure is a tombstone, applied when querying.** The tombstone lives in the metadata database and hides the user in every query from the moment of erasure. Event queries already filter by `erased_uids`, but profile reads and the profile catalog do not (`api.rs:790`, `:3940`, `:3989`); T1 adds `erased_uids` filtering there too (`updated_at_ms <= erased_at_ms`), so erased profiles stay hidden both on remote readers reading a published snapshot and after `restore --from-published` (which also purges them from the restored `pstore`, 3.5). Rows are physically dropped only when a segment is rewritten (`engine.rs:1077`); an erasure does not trigger a rewrite. With S3 segments rewritten at most once, erased rows can stay in S3 indefinitely behind the tombstone, and the external id stays in the identity graph. `docs/limitations.md` must say this in plain words, because a deployment with a legal duty to delete bytes needs to know. One exception to check before relying on it: tombstones for agent data expire (`pulse-control/migrations/0011`, `db_agent.rs:1601`), so agent rows must be physically removed before their tombstone goes.

## 4. What changes for operators and clients

| Area | Today | After |
|---|---|---|
| Process crash | Nothing acknowledged is lost | Same |
| Volume lost | Not applicable (hot state in S3) | Outage until `restore`; everything since the last publish is lost |
| Query on the owning node | Sees acknowledged data | Same |
| Roles on another machine | Read S3 | See data up to one interval old; the compactor role has nothing to do in local mode |
| Data directory | Any disk | A persistent network volume (EBS) with scheduled snapshots |
| Metadata DB | SQLite in the data directory | External database in production; SQLite for evaluation only |

The storage tiering changes no API response. The asynchronous writer in section 6 does.

## 5. Cost after the change

**Estimate**, per busy project, 16 buckets active, one current date, 5-minute interval: 16 segment PUTs and 1 published manifest per cycle, about 200 per hour, plus the identity and profile snapshots. The snapshot cost is unknown until T1 is measured: with a checkpoint copy it could approach one PUT per identity flush, which is what the system pays today.

An idle project must make no calls. That requires tracking uploads locally, not by listing S3.

## 6. Writers, forwarding and ownership

Sections 1 to 5 are the storage-node half. This section is the other half of the owner's target: stateless writers in front, storage nodes that own projects behind, one process doing all of it on a single machine.

Decided by the owner:

- The event writer is asynchronous and returns once the event is in memory. Fast ingestion is its purpose.
- Writers know nothing about profiles. Profile creation has its own synchronous endpoint that returns an id. Events are linked to their profile later, by the storage node.
- A storage node owns whole projects.
- Storage nodes also run queries. Compute nodes that read S3 only are a later option.
- Handling a failed storage node is for the v2 release.

Everything else in this section is proposed.

### 6.1 The path of an event

```
client ─► writer ─► memory buffer ─► writer log (local disk) ─► forwarder ─► owning storage node
          answers here                                                        dedup, identity, WAL,
                                                                              segments, upload (3.2)
```

**Writer.** Authenticates the key, decodes the body, applies the checks that need no state (size limits, malformed events), applies the project's lexicon rules (drops and PII masking, so unmasked data never reaches the writer's disk), stamps the server time, appends to a bounded memory buffer and answers. It holds no identity, dedup or profile state, so any writer can take any request and writers can be added or removed freely. Two limits of that: the rate limit is counted in memory per node (`pulse-ingest/src/rate_limit.rs`), so N writers allow N times the quota until it is shared; and a batch that carries `identify` or `alias` items is split, with those items taking the synchronous path in 6.3. For a mixed batch, the writer checks buffer/log capacity first, forwards the synchronous `identify`/`alias` items to the owner, and appends the events to the memory buffer **only** if the synchronous call succeeds—otherwise it returns the error (`409`, `503`, etc.) without queuing events that the client would retry.

**Writer log.** A background task flushes the buffer to an append-only log on the writer's local disk (proposed bound: every 20 ms or 1 MiB, whichever comes first). There is one log and one cursor **per project** (`writer_log/{project_id}/`), not per storage node: a single project hitting its per-project `429` bound (3.7) or moving to a new owner (6.4) therefore cannot head-of-line-block other projects on the same storage node. Open file handles for idle projects are closed after a short idle timeout while keeping the directory and cursor on disk. Each project log is bounded in bytes (plus a node-wide writer disk cap); when the buffer or log is full the writer answers `429` for the affected project.

**Forwarder.** Runs inside the writer process. For each active project log, it reads batches in order and sends them to that project's current owning storage node, one batch in flight per project, in strict order. If the target answers "not the owner", the forwarder refreshes `project_owner` and retries against the new owner. It advances the project's durable cursor only after the storage node confirms the batch is in its WAL and fsync'd. Log files behind the cursor are deleted.

**Exactly once across the hop.** The forwarder retries, so the storage node must drop repeats. The existing dedup window is not enough for this: it covers 15 minutes and 524,288 entries (`pulse-ingest/src/dedup.rs:6-8`), and a storage node can be down for longer than that after applying a batch whose confirmation was lost. So each batch carries the writer's id and its per-project log offset, and the storage node remembers the highest offset it has applied. Three details make that hold (all found in review):

- *The key is (writer, bucket), not writer.* A batch becomes one WAL record per bucket, written in parallel and not atomically (`pulse-ingest/src/service.rs:392`, `engine.rs:522-533`). After a crash some buckets have the batch and some do not; the resend must be applied only to those that do not.
- *The offsets outlive the WAL.* WAL files are purged once a flush commits past them (`engine.rs:353-360`). So the offsets are written in the WAL record and also carried into the manifest by the same commit that advances `committed_wal_seq`. Otherwise: apply, lose the confirmation, flush, restart, resend, duplicate.
- *A writer's id belongs to its log, and log age is bounded below expiry.* The id is created with the writer's log root directory and stored there. A writer that lost its disk starts with a new id and offset zero, and nothing it sends is mistaken for a repeat. A writer that restarts with its log keeps its id and resends from each project's cursor. Entries for writers not seen for `T_expire` (proposed: 7 days) are removed from the map; to guarantee an expired writer can never resend and duplicate, the writer log enforces a hard maximum batch age strictly less than `T_expire` (proposed: 6 days), dropping and alerting on anything older.

The existing window keeps its job: repeats sent by clients.

**Order.** Events from one writer arrive in the order it accepted them. Events from different writers have no order between them. Event time is the time stamped by the writer, so forwarding delay does not move an event in time. Order between an event and an `identify` for the same user does not matter: events store the raw user id and merges are resolved when querying (`pulse-identity/src/lib.rs:104`).

### 6.2 What the writer's answer means

The answer says the events were received and passed the stateless checks. It does not say they are on disk, and it does not report the result of processing.

| | Today | Asynchronous writer |
|---|---|---|
| Returned when | The event is in the WAL in the store | The event is in memory |
| `accepted` | Events applied | Events received |
| `deduped` | Count of repeats dropped | Always 0; repeats are counted in a metric |
| `rejected`, `dropped` | All refusals, by index | Only refusals the writer can decide without state |
| Identity conflict (`409`) | Returned; it comes only from `identify` and `alias` items in a batch (`pulse-identity/src/lib.rs:497`), never from an event | Still returned: those items stay synchronous (6.3) |
| Schema cap (`422`) | Per request, refused before the WAL (`service.rs:330-347`) | The per-request cap is dropped on this path. The per-project cap stays; it already dead-letters after the WAL (`engine.rs:558-575`) |
| `profiles_updated` | Count of `identify` and `alias` items applied | Same, from the synchronous part of the batch |
| Not authenticated, malformed, too large, rate limited | `401`, `400`, `413`, `429` | Same |

The response keeps its fields (`IngestResponse`, `pulse-ingest/src/service.rs:55`), so existing SDKs still parse it. What changes is the meaning, and three things follow:

- The end-to-end gate that checks the dedup count must read the metric or query the data instead.
- Events refused during processing go to the dead-letter path that already exists (`wal_dead/`, routed to the local tier in 3.1). Nothing reads that path today except a counter (`pulse-node/src/metrics.rs:178`), and the reason for the refusal is only logged (`engine.rs:606-612`). This work stores the reason with the record and adds a command that lists a project's dead-lettered events, leaving out erased users (resolving each `WireEvent`'s `distinct_id` through the project's identity graph against `erased_uids`, since `wal_dead/` stores raw `WireEvent` rather than internal `u32` ids). A command is enough; an endpoint can wait.
- Existing SDKs are not broken by this. They read `deduped` and `rejected` only for debug logging, drop a batch on `400`, `413` and `422`, and retry everything else (`sdks/ts-web/src/index.ts:1854-1865`). Not checked: SDK tests that assert on `deduped`.
- Other callers of the same processing keep their synchronous results: bulk import, which reports `deduped` (`api.rs:4239-4260`), and the demo seeder. The QUIC ingest path (`pulse-ingest/src/quic.rs:219`) takes the same meaning as HTTP.
- The SDK documentation must stop promising per-event results on the event path.

**What can be lost.** State it once, plainly:

| Failure | Lost |
|---|---|
| Writer process crash | Events accepted since the last flush: at most the flush bound |
| Writer disk lost | Everything in the log not yet forwarded |
| Storage node crash | Nothing: the forwarder resends from its cursor |
| Storage node volume lost | As section 3.5: everything since the last publish |

The client is not told about any of these. So the flush bound, buffer fill, log bytes and forwarding lag (oldest unforwarded event) are exported as metrics, and forwarding lag has an alert. Lag longer than the dedup window also weakens the dropping of client repeats, which is a second reason to alert on it.

### 6.3 Profiles

Profile creation is synchronous: the call reaches the owning storage node, which writes the profile durably and returns its id. A writer that receives the call passes it through to the owner and waits; it adds no state of its own.

Decided by the owner: these also stay synchronous and pass through to the owner:

- `/v1/identify`, `/v1/engage` and `/v1/groups`. They are rare next to events, a property set twice must end with the later value, and their errors are worth returning.
- `identify` and `alias` items sent inside a `/v1/ingest` batch. They are profile calls in an event envelope, and they cannot be dead-lettered: `wal_dead/` holds events only (`engine.rs:583-588`).
- `/v1/replay/chunk`. Its answer carries `replay_offset_kb`, which the SDKs stamp on later events (`sdks/ts-web/src/index.ts:1795`).

One gap in today's code must close first: `apply_engage` and `on_identify` return nothing and only log a failure (`api.rs:1154-1232`), so a profile call can answer success without the profile being written. W2 makes them return the error.

Events carry the external id only. When the storage node processes an event it looks the id up, creates the mapping if the id is new, and writes the internal id into the event row, as ingest does today. An event for a user with no profile yet is therefore accepted and linked; the profile row appears when a profile call creates it.

### 6.4 Ownership

**Unit.** The whole project. One storage node holds all of a project's buckets, its identity store and its profiles. Nothing about a project is split across machines, so the identity store, which is one per project, does not need to be divided.

**Record.** Ownership is a row in the metadata database, not the result of a hash: a new table `project_owner(project_id, node_name, address)`, where `node_name` is a stable name set in the node's configuration. The owner of a project changes only by an explicit move.

The writer lease table cannot be that record as it works today, though it looks like it. Its `node_id` is a random value per process (`pulse-control/src/db.rs:2614`); any node may take a lease that has expired (`db.rs:2570`); a clean shutdown expires it on purpose; and every writable node tries to take the lease whenever it opens a project (`engine.rs:1450`). With several storage nodes, a node whose alert loop touched a paused project would take it within seconds. So the lease stays what it is, the fence between two processes, and gains one rule: a node may take the lease for a project only if `project_owner` names it.

**Why a table and not a ring alone.** On a ring, adding or removing a node moves partitions by itself. Here the state for a project sits on one node's disk, and moving it means publish, stop, restore on the new node, then resume. That cannot happen as a side effect of membership changing. So a new project is placed once, on the storage node that owns the fewest projects, and the choice is written down. Writers and the query router read the table, keep it in memory, and refresh it on a timer and whenever a storage node answers "not the owner".

**Background work.** Several loops run on every node over every project today: alert evaluation (`pulse-control/src/api.rs:4561-4585`), two agent jobs (`api_agent.rs:2965`, `3253`), the catalog reload (`api.rs:540-557`) and the compactor (`pulse-node/src/lib.rs:614`). With more than one storage node each of these must run only for the projects the node owns; otherwise a node that does not own a project evaluates an alert on data it does not have and claims the result. The agent store is one per node and collects garbage using every project's tombstones (`pulse-node/src/lib.rs:560-562`); agent traces (`/v1/agent/traces`) must be routed to the project's owner like everything else. Whether traces take the asynchronous path is open (section 8).

**Queries.** Every query route is under `/v1/projects/:id/`, so a query goes to the one node that owns the project. That node answers from its memory, its local segments and S3. No merging across nodes is needed while a project has one owner.

**Ceiling.** A project cannot grow beyond one storage node. Many projects spread across nodes; one very large project does not. The way past that is ownership by bucket, which needs the identity store divided or shared, and is not designed here.

**When a storage node is down (until v2).** Its projects pause. Writers keep accepting events for them into each project's log until the log bound, then answer `429` for those projects. Profile calls and queries for those projects answer `503`. When the node returns with its volume, the forwarders drain the backlog and the offset rule in 6.1 drops anything already applied. While a node is down nobody else may take its projects; the owner rule above is what guarantees that. If the volume is gone, an operator runs the restore in 3.5 on a new node and changes the owner row by hand; each project's forwarder sees the updated `project_owner` row and drains its buffered log to the new owner (dropping anything `<=` the restored manifest's offsets). Doing that automatically, and moving a healthy project between nodes, is v2.

### 6.5 One machine

One process plays every role, and the pieces that exist only to cross machines disappear:

- The writer's memory buffer flushes straight into the storage WAL through the group commit that already exists. There is no separate writer log and no forwarder, so an event is written to disk once, not twice.
- The owner of every project is this node; nothing is looked up. With no hop there is no resend, so the offset rule is not needed.
- The answer has the same meaning as in 6.2, and the loss on a crash is the same flush bound.

So on one machine this design is a change of the point at which ingest answers: today after the WAL write, after this change when the event is queued for it. The held appender and processor split (#98) is the base for that step.

### 6.6 Freshness

A query on the owning node sees an event after the writer flush, the forward and the storage node's apply: milliseconds to about a second when nothing is backed up, and as long as the forwarding lag when something is. A client that sends an event and queries at once may not see it. A profile call is different: when it returns, the profile is readable.

## 7. Order of work and exit criteria

Each step is one PR, merged before the next starts. Revision 1 had identity going local before any snapshot existed. That is acceptable only because of the start-up guard in 3.5 and the owner's decision to rely on EBS; the exposure ends at T1.

| Step | Change | Exit criteria |
|---|---|---|
| T0 | P0-7: WAL, identity and profiles local; multipart fsync (3.8) | Counting segment store: zero calls between request and acknowledgement; idle project zero calls; a node with the marker and no local identity refuses to start |
| T1 | Published manifest (create-only, dense `publish_seq`, generation, high-water mark) written at every flush, plus identity and profile snapshots taken before it; `restore --from-published` (with erased-profile purge and `uid >= HWM` tombstone cleanup); `erased_uids` filter on profile reads/catalog; metadata DB rule | Restore into an empty directory resolves every user id in every segment and profile row; erased users' events **and** profiles stay hidden after restore (external metadata DB); a user created and erased after the last publish does not poison the reused `uid` after restore; kill at each point and restore still passes; measured PUTs per hour for both snapshot candidates |
| T2 | Manifest authoritative on local disk; publish moves to the interval with dense `publish_seq`; fencing rule (3.6); S3 segment deletion rule (3.3) | Manifest PUTs per hour equal cycles per hour; a stale second writer cannot overwrite `published/` even when its local `epoch` is ahead of the active writer's; crash right after step 6 before local `last_published_seq` update recovers cleanly; a `query` node on another machine reads published state and hides erased profiles |
| T3 | Local segment tier and the full upload cycle; intent record and sweep | Query results identical before and after an upload; kill at each step, restart, no acknowledged event missing or duplicated and no leaked object; PUTs per hour at load-generator rate; local bytes bounded with S3 unreachable, then `429` |
| W1 | Asynchronous event acknowledgement on one machine (6.2, 6.5). Builds on the held appender and processor split (#98) | A request returns before any disk write; kill the process under load and the events missing are no more than the flush bound allows, counted; no event duplicated; dead-lettered events are listable; throughput and p99 from three runs each, before and after |
| W2 | Synchronous profile endpoint that returns the id (6.3) | Create, then read back on the next request; kill after the response and the profile is there after restart |
| T4 | S3-side cold merge; the same local tier for agent segments and replay chunks | Object count per closed day equals buckets; idle project zero calls including agent maintenance |
| W3 | Writer nodes with per-project local logs and forwarders; ownership table; routing of profile calls and queries to the owner (6.1, 6.4) | Two writers and two storage nodes: every event lands once on the owner; kill a writer mid-forward and nothing is duplicated; one project hitting `429` on a storage node does not stall another project on the same node; stop a storage node and writers hold events up to each project's log bound then answer `429`; the node returns (or a project is restored on the other node and `project_owner` updated) and the backlog drains with no duplicates; apply, flush, clean restart, resend gives no duplicate; a writer whose log was wiped loses only that log and nothing is dropped as a repeat; a node that is not the owner cannot take the lease; alerts fire once, on the owner |

Left for the v2 release, by the owner's decision: automatic handling of a failed storage node, moving a project between nodes, and compute nodes that read S3 only.

An interval of zero means "upload eagerly", not a return to today's layout: the manifest stays local-authoritative and count-based compaction stays on. Going back to the old layout after T2 is a documented manual procedure, not a setting.

## 8. Open questions

1. Checkpoint copy or logical export for identity and profiles (3.4)? Decided by the T1 measurement.
2. Default upload interval. 300 s is a proposal; it is the loss window on volume loss and the staleness on other machines.
3. Should the upload merge all buckets of a date into one object? It cuts PUTs by the bucket count but changes pruning. Not proposed.
4. Late events for old dates create small partitions. Proposed: they upload on a longer maximum age.
5. Session-replay chunks: buffer locally and upload per session on the cycle?
6. Writer flush bound. 20 ms or 1 MiB is a proposal; it is the most a writer crash can lose.
7. Do agent traces (`/v1/agent/traces`) take the asynchronous path or stay synchronous?
8. Should a durable acknowledgement remain available as a setting for deployments that cannot lose events on a crash? Not proposed: it keeps two ingest paths alive.
9. SQLite guardrails in local-tier mode (3.5): log a startup warning when running local-tier mode on SQLite, and have `restore --from-published` refuse on SQLite? Proposed yes; awaiting owner confirmation.

Answered by the owner and now in the text: metadata database (3.5), erasure (3.9), ownership unit, what a writer's acknowledgement means, which calls stay synchronous, storage-node failure (section 6).

## 9. Review record

Revision 1 was reviewed against the code by an independent agent (read-only). It confirmed the tiering-by-path premise, the flush and recovery order, and the code citations in section 2. It found six blocking problems, all addressed in this revision:

| Finding | Change |
|---|---|
| Snapshotting identity before uploading let a later flush carry ids the snapshot lacked; ids are dense counts, so a restore would reuse them | Freeze the segment list first, snapshot identity last, assert a high-water mark (3.2) |
| S3-side merge would delete segments a published manifest still referenced | Deletion only after a newer publish plus grace; persisted list (3.3) |
| Steps were not safe alone: identity local before any snapshot; a snapshot every 300 s under a manifest committed every flush | T1 ties snapshots to publication at every flush; the T0 exposure is named (7) |
| File-level mirror of SlateDB is not a consistent restore point | Checkpoint copy or logical export (3.4) |
| A local manifest removes the cross-machine writer fence | Create-only publish with generation; refuse to open behind the published epoch (3.6) |
| Restore ignored the metadata DB, so erased users would reappear | Erased set published; metadata DB rule (3.5). Later replaced by the owner's decision to run an external metadata database |

Also corrected from the review: there is no existing orphan sweep (intent record and sweep added), "interval zero" is not a rollback, the snapshot cost claim was unfounded, erased rows are not removed within one interval, remote readers cannot open the S3 snapshot in place, the upload merge needs a memory cap, dedup seeding must not swallow read errors, and the local multipart path does not fsync.

Not checked by the reviewer: the P0-7 marker rules (not yet in the code it read), the measured PUT counts, the agent store under T4, whether cohort or experiment tables store raw user ids, and real S3 conditional-put behaviour.

### Second review: section 6

The writer, forwarding and ownership text was reviewed the same way. The reviewer confirmed the citations, that existing SDKs tolerate the changed response, that every query route is scoped to a project, and that event and identify order does not matter. It found seven blocking problems, all addressed:

| Finding | Change |
|---|---|
| Offsets kept only in the WAL are lost when a flush purges it | Offsets carried into the manifest by the commit that advances the WAL position (6.1) |
| A batch is written per bucket, not atomically, so one offset per writer is wrong | Key is (writer, bucket); one batch in flight per project (6.1) |
| Writer identity was undefined: a lost disk or a new id either drops or duplicates data | The id is created with the log and lives in its directory; stale entries expire (6.1) |
| One log with one cursor cannot hold back a single project | One log and cursor per owning storage node (6.1) |
| The lease table cannot record ownership: random node id, takeover on expiry, acquire on every open | Separate `project_owner` table; lease acquire refused unless the caller is the owner (6.4) |
| `/v1/ingest` batches carry `identify` and `alias`, the only source of `409` | Those items split off and stay synchronous (6.2, 6.3) |
| Alerts, agent jobs, catalog reload and compactor loop over all projects on every node | Each runs only for owned projects (6.4) |

Also taken from the review: replay chunks stay synchronous, the per-request schema cap is dropped on the asynchronous path, profile calls must return their errors, the rate limit is per node, lexicon rules run before the writer log, the dead-letter reason must be stored, placement by fewest projects instead of a hash, and a command instead of an endpoint for dead letters.

Not checked: #98 (not on the reviewed branch), SDK tests that assert on `deduped`, cohort and experiment internals beyond their routes, the offset semantics of replay chunks, and which tier `wal_dead/` lands in.

### Third review: cross-section invariants and restore/fencing edge cases

A third read-only review against `4895bbc` checked the unchecked items from earlier reviews and cross-section interactions between storage, erasure, and writers. It found three blocking problems and four gaps, all addressed in revision 4:

| Finding | Change |
|---|---|
| Naming `published/` by sparse local `epoch` lets a stale writer whose local epoch is ahead shadow a newer generation's publish, and bypass the open check | `published/` uses its own dense, contiguous `publish_seq` with `head(publish_seq - 1)` GC guard; open checks `writer_generation` and `publish_seq` with a self-crash exception for step 6 (3.2, 3.6) |
| Profile reads and the profile catalog do not filter `erased_uids` (`api.rs:790`, `:3940`, `:3989`), so erased profiles reappear after `restore --from-published`; and tombstones for users created+erased after `T_publish` poison reused `u32` ids | `restore` purges erased profiles from `pstore` and deletes `erased_uids` where `uid >= high_water_mark`; profile reads/catalog also filter by `erased_uids` (3.5, 3.9, 7) |
| One writer log and cursor per storage node causes cross-project HOL blocking on per-project `429` (3.7) and orphans buffered batches when a single project moves to a new owner (6.4) | One log and cursor per project, with idle handle closure and clean repointing on `project_owner` change (6.1, 6.4, 7) |

Also addressed: `wal_dead/` (local) and `replay/`/`agent_segments/` (S3 until T4) added to the 3.1 prefix table; dead-letter listing resolves `WireEvent.distinct_id` through the identity graph (6.2); mixed `/v1/ingest` batch ordering specified so failed synchronous `identify`/`alias` calls never leave queued events (6.1); writer log max batch age bounded strictly below storage-node offset expiry (6.1); SQLite guardrails surfaced in section 8.
