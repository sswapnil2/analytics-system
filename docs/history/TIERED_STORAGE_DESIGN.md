# Design note: local hot tier, S3 for compacted data only (proposed)

Status: proposal, revision 2 after an independent review (section 9). No code. It follows section 15 of [SCALABILITY_REVIEW.md](SCALABILITY_REVIEW.md) and the P0-7 decision (hot state on local disk). File and line references are to `fix/scalability-review-quick-wins` at `4895bbc`.

This note covers the storage half only: one node, its volume, and what it sends to S3. The owner's target also has stateless writers that forward to storage nodes on a hash ring, and compute nodes that read S3. Section 6 lists what that adds; it will be written up once the open questions in section 8 are answered.

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
| `identity/`, `profiles/` | local | SlateDB stores (P0-7) |
| `hot/segments/` | local | segments written by an L0 flush, not yet uploaded |
| `manifests/` | local | the authoritative manifest, every epoch |
| `segments/` | S3 | uploaded segments |
| `published/` | S3 | published manifests, identity and profile snapshots, erased-user set |

### 3.2 The upload cycle

A per-project task runs every `PULSE_UPLOAD_INTERVAL_SECS` (proposed default 300), and earlier if local segment bytes pass a threshold.

The order matters. A user id is the count of users at the time it was created (**code**: `pulse-identity/src/lib.rs:959`), so an identity store restored from a snapshot will hand out again any id the snapshot does not contain. Profile rows are keyed by that id too (`profile_store.rs:37`). Therefore identity is snapshotted last, after everything that can reference an id has been frozen:

1. **Freeze.** Record the exact list of local segment ids to upload, and write an intent record locally (the new S3 segment ids and their sources).
2. **Snapshot profiles** to S3.
3. **Flush and snapshot identity** to S3. Record the user count as a high-water mark.
4. **Upload.** Merge only the frozen segments, per partition, and write each result create-only to `segments/`. Existing S3 segments are not read or rewritten. Input bytes per output segment are capped, because the merge decodes its inputs into memory (`engine.rs:1072-1076`).
5. **Commit** the local manifest: frozen local segments out, new S3 segments in. This reuses the existing check that the sources are still active (`engine.rs:1134-1147`).
6. **Publish** a manifest to `published/`, create-only and epoch-named like `commit_next`, listing S3 segments only and carrying `writer_generation`, the identity high-water mark and the erased-user set.
7. **Clean up.** Delete the uploaded local segments once no snapshot pins them; clear the intent record.

Publish and restore both assert that the identity snapshot's user count is at least the manifest's high-water mark.

Crash handling. There is no sweep of stored segments today: superseded paths sit in an in-memory list (`engine.rs:154`), so anything not cleaned before a crash would leak. This design adds two things at project open: replay or discard the intent record (delete S3 objects it names that the manifest does not reference), and sweep `hot/segments/` against the local manifest.

| Crash after | State on restart | Handled by |
|---|---|---|
| 1-3 | Snapshots newer than needed; intent with nothing uploaded | intent discarded |
| 4 | S3 segments nothing references | intent replay deletes them |
| 5 | Local manifest ahead of the published one | step 6 on the next cycle |
| 6 | Uploaded local segments still on disk | sweep at open |

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
2. Installs the newest published manifest as the local manifest.
3. Restores the erased-user set.
4. Prints the publish time and states the loss: events, profile updates, identity merges and erasures after that time are gone.

A node never restores on its own.

The metadata database is part of this. By default it is SQLite in the data directory (`pulse-control/src/db.rs:227`), on the same volume, and it holds accounts, API keys, projects, cohorts and the erasure tombstones that hide erased users at query time (`api.rs:1037`). Losing it with the volume would bring erased users back. Local-tier mode therefore needs one of: an external `DATABASE_URL`, or the metadata DB included in the publish and restore. The erased-user set is published with the manifest in both cases.

### 3.6 Fencing

Today the writer generation is fenced by a manifest commit in the shared store (`engine.rs:219-235`). With the manifest local, a second machine that took the lease would see an empty local manifest and could overwrite `published/`. So:

- `published/` is create-only and carries `writer_generation` (3.2 step 6).
- A node refuses to open a project for writing when the published epoch is ahead of its local epoch, except through `restore`.

### 3.7 Backpressure and disk

- Local segment bytes are bounded per project and per node. Past the bound the upload task runs immediately; past twice the bound ingest answers `429`.
- If S3 is unreachable, ingest continues until that bound, then `429`.
- Disk full or unwritable is `503` on ingest, never a panic and never an acknowledged write.
- A restart while S3 is unreachable must not seed dedup from an empty read: today read errors are swallowed there (`engine.rs:336`), which would accept client retries as new events.

### 3.8 Durability on power loss

A process crash and a machine crash are different. Review found that the local store's single `put` fsyncs file and directory, but its multipart path does not (`store_factory.rs:80-97`), and SlateDB writes its tables through that path. On an operating-system crash an identity table might not be on disk while fsync'd segments already reference its ids. The multipart path must fsync before it reports success. This applies to P0-7 as soon as identity is local, and has been sent to that work.

### 3.9 Encryption and erasure

- The prefix router sits below the one encryption wrapper, so the local tier is encrypted when `PULSE_BYOK_MASTER_KEY` is set (`pulse-node/src/lib.rs:360`).
- Erased rows are physically dropped only when a segment is rewritten (`engine.rs:1077`). With S3 segments rewritten at most once, erased rows can stay in S3 indefinitely, hidden by the tombstone. Erasure also leaves the external id in the identity graph. Either `docs/limitations.md` says so, or an erasure triggers a rewrite of the affected S3 segments. That is a product decision (section 8).

## 4. What changes for operators and clients

| Area | Today | After |
|---|---|---|
| Process crash | Nothing acknowledged is lost | Same |
| Volume lost | Not applicable (hot state in S3) | Outage until `restore`; everything since the last publish is lost |
| Query on the owning node | Sees acknowledged data | Same |
| Roles on another machine | Read S3 | See data up to one interval old; the compactor role has nothing to do in local mode |
| Data directory | Any disk | A persistent network volume (EBS) with scheduled snapshots |
| Metadata DB | SQLite in the data directory | External database, or included in publish and restore |

No API response changes.

## 5. Cost after the change

**Estimate**, per busy project, 16 buckets active, one current date, 5-minute interval: 16 segment PUTs and 1 published manifest per cycle, about 200 per hour, plus the identity and profile snapshots. The snapshot cost is unknown until T1 is measured: with a checkpoint copy it could approach one PUT per identity flush, which is what the system pays today.

An idle project must make no calls. That requires tracking uploads locally, not by listing S3.

## 6. The wider target: writers, storage nodes, compute nodes

The owner's target (stated after revision 1):

- Stateless writers append to their own local log and acknowledge; a processor forwards records to the storage node that owns the partition.
- Storage nodes sit on a consistent-hash ring, own partitions, do the stateful work on their own disk, and push to S3 as described above.
- Compute nodes read S3 only and see data one interval late.
- One machine runs the same pipeline in one process.

Decided by the owner so far:

- **Queries run on storage nodes.** A storage node is mostly I/O and has CPU and memory to spare, so it answers queries over its own fresh data and S3. A dashboard query across machines fans out to the owning storage nodes and merges. Compute nodes that read S3 only are a later option.
- **The event writer is asynchronous.** Its goal is fast ingestion: it accepts events and returns without any result of processing. Dedup counts, identity conflicts and schema-cap errors are no longer returned on the event path; they become metrics and dead-lettered records. This changes the ingest response and the SDKs, and the end-to-end gate that checks the dedup count.
- **Profiles have their own synchronous endpoint** that creates the profile on the owning storage node and returns its id. Events carry the external id only; the storage node links each event to its profile when it processes the event, so writers hold no identity state.

Open on the writer: whether "accepted" is returned after the event is on the writer's disk (a process crash then loses nothing) or before (faster, a crash loses what was buffered).

This note is the storage-node half and does not change under that target. Still to design: the writer log and forwarding, the ring and ownership unit, moving ownership (publish, stop, restore under the lease), and fresh reads. Section 15 of the review assumed a shared log in the object store; the owner's target uses writer-local logs and forwarding, so that section will be revised, not reused.

## 7. Order of work and exit criteria

Each step is one PR, merged before the next starts. Revision 1 had identity going local before any snapshot existed. That is acceptable only because of the start-up guard in 3.5 and the owner's decision to rely on EBS; the exposure ends at T1.

| Step | Change | Exit criteria |
|---|---|---|
| T0 | P0-7: WAL, identity and profiles local; multipart fsync (3.8) | Counting segment store: zero calls between request and acknowledgement; idle project zero calls; a node with the marker and no local identity refuses to start |
| T1 | Published manifest (create-only, generation, high-water mark, erased set) written at every flush, plus identity and profile snapshots taken before it; `restore --from-published`; metadata DB rule | Restore into an empty directory resolves every user id in every segment and profile row; erased users stay hidden after restore; kill at each point and restore still passes; measured PUTs per hour for both snapshot candidates |
| T2 | Manifest authoritative on local disk; publish moves to the interval; fencing rule (3.6); S3 segment deletion rule (3.3) | Manifest PUTs per hour equal cycles per hour; a stale second writer cannot overwrite `published/`; a `query` node on another machine reads published state |
| T3 | Local segment tier and the full upload cycle; intent record and sweep | Query results identical before and after an upload; kill at each step, restart, no acknowledged event missing or duplicated and no leaked object; PUTs per hour at load-generator rate; local bytes bounded with S3 unreachable, then `429` |
| T4 | S3-side cold merge; the same local tier for agent segments and replay chunks | Object count per closed day equals buckets; idle project zero calls including agent maintenance |

An interval of zero means "upload eagerly", not a return to today's layout: the manifest stays local-authoritative and count-based compaction stays on. Going back to the old layout after T2 is a documented manual procedure, not a setting.

## 8. Open questions

1. Checkpoint copy or logical export for identity and profiles (3.4)? Decided by the T1 measurement.
2. Default upload interval. 300 s is a proposal; it is the loss window on volume loss and the staleness on other machines.
3. Metadata DB in local-tier mode: require an external database, or publish and restore it?
4. Erasure: document that erased rows can remain in S3 behind the tombstone, or rewrite affected S3 segments on erasure?
5. Should the upload merge all buckets of a date into one object? It cuts PUTs by the bucket count but changes pruning. Not proposed.
6. Late events for old dates create small partitions. Proposed: they upload on a longer maximum age.
7. Session-replay chunks: buffer locally and upload per session on the cycle?
8. For the wider target (section 6): ownership unit on the ring (project or bucket), what a writer's acknowledgement means, whether any read needs to be fresher than the interval, and whether a failed storage node's partitions may pause until its volume returns.

## 9. Review record

Revision 1 was reviewed against the code by an independent agent (read-only). It confirmed the tiering-by-path premise, the flush and recovery order, and the code citations in section 2. It found six blocking problems, all addressed in this revision:

| Finding | Change |
|---|---|
| Snapshotting identity before uploading let a later flush carry ids the snapshot lacked; ids are dense counts, so a restore would reuse them | Freeze the segment list first, snapshot identity last, assert a high-water mark (3.2) |
| S3-side merge would delete segments a published manifest still referenced | Deletion only after a newer publish plus grace; persisted list (3.3) |
| Steps were not safe alone: identity local before any snapshot; a snapshot every 300 s under a manifest committed every flush | T1 ties snapshots to publication at every flush; the T0 exposure is named (7) |
| File-level mirror of SlateDB is not a consistent restore point | Checkpoint copy or logical export (3.4) |
| A local manifest removes the cross-machine writer fence | Create-only publish with generation; refuse to open behind the published epoch (3.6) |
| Restore ignored the metadata DB, so erased users would reappear | Erased set published; metadata DB rule (3.5) |

Also corrected from the review: there is no existing orphan sweep (intent record and sweep added), "interval zero" is not a rollback, the snapshot cost claim was unfounded, erased rows are not removed within one interval, remote readers cannot open the S3 snapshot in place, the upload merge needs a memory cap, dedup seeding must not swallow read errors, and the local multipart path does not fsync.

Not checked by the reviewer: the P0-7 marker rules (not yet in the code it read), the measured PUT counts, the agent store under T4, whether cohort or experiment tables store raw user ids, and real S3 conditional-put behaviour.
