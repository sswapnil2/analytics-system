# Design note: local hot tier, S3 for compacted data only (proposed)

Status: proposal for review. No code. It follows section 15 of [SCALABILITY_REVIEW.md](SCALABILITY_REVIEW.md) and the P0-7 decision (hot state on local disk). File and line references are to `fix/scalability-review-quick-wins` at `4895bbc`.

Claims are marked **measured** (a number from a run), **code** (read in the source) or **estimate** (arithmetic from stated assumptions). Nothing here has been run on S3.

## 1. Goal

Make Pulse cheap to run on one machine with an EBS volume and an S3 bucket, without losing anything on a process crash. The owner's four points:

1. Shared nothing: a node owns its projects and its own disk.
2. Profiles, events and other hot data live on the EBS volume.
3. Segments are compacted on an interval and only then pushed to S3.
4. Profiles are updated in memory and on disk, and pushed to S3 on an interval.

P0-7 delivers point 2. This note covers points 3 and 4 and says what point 1 still lacks.

## 2. What it costs today

Counting object-store wrapper under one node, in-memory store, sequential requests, one new user per event (**measured**, staff engineer, integration head `4895bbc`):

| Source | PUTs per 1,000 requests | Path |
|---|---|---|
| Event WAL | 1,000 | acknowledge path |
| Identity store (SlateDB) | 1,000 | acknowledge path, when the request has a new user |
| Agent trace WAL | 1,000 | acknowledge path |
| Agent segments | 200 | flush, about one per 5 traces |
| Event segments + manifests | 30 | flush |

Idle projects are not free either: about 1,100 PUTs per hour from agent maintenance (**measured**).

After P0-7 the first three rows are zero. What remains is every flush:

- Each bucket flush writes one segment object per date straight to the segment store, then a full manifest object (**code**: `write_vortex_segment` in `pulse-storage/src/segment.rs:417`, called from `flush_bucket` in `engine.rs`; `ManifestStore::commit_next` in `manifest.rs:208`).
- Compaction runs once a partition has 4 segments (`compaction_min_segments`, `engine.rs:69`) and rewrites them into one, reading the inputs back from the segment store and writing the result there (**code**: `compact_min_segments`, `engine.rs:1006`). A segment that was already compacted is read and rewritten again each time four more small ones arrive.
- Session-replay chunks are written one object per chunk (**code**: `replay_store.rs:158`).

## 3. Design

One rule: **an object is written to S3 once, when it is in its long-lived form.** Everything younger lives on the node's volume.

### 3.1 Two tiers by path prefix

P0-7 already routes some prefixes (`wal/`, `identity/`, `profiles/`) to a local fsync'd store. This design adds one more local prefix and keeps `SegmentMeta` unchanged, because a segment's location is already its `object_path` (**code**: `segment.rs:202`, and every read goes through that path: `segment.rs:1029-1050`).

| Prefix | Tier | Contents |
|---|---|---|
| `wal/`, `agent_wal/` | local | write-ahead logs (P0-7) |
| `identity/`, `profiles/` | local | SlateDB stores (P0-7) |
| `hot/segments/` | local | segments written by an L0 flush, not yet uploaded |
| `manifests/` | local | the authoritative manifest, every epoch |
| `segments/` | S3 | uploaded segments |
| `published/` | S3 | the published manifest, and the identity and profile snapshots |

Queries are unchanged in shape: a snapshot is L0 rows plus the manifest's segments, and the reader fetches each by path. Local segments are read from disk; S3 segments go through the existing block cache.

### 3.2 The upload cycle

A per-project task runs every `PULSE_UPLOAD_INTERVAL_SECS` (proposed default 300), and earlier if local segment bytes pass a threshold. For each partition (date, bucket) that has local segments:

1. Flush the identity store locally, then push the identity snapshot to S3 (3.4).
2. Merge the partition's local segments into one segment and write it to `segments/` in S3, create-only, under a new id. Existing S3 segments are not read or rewritten.
3. Commit the local manifest: replace the merged local segments with the new S3 segment. This is the existing compare-and-swap commit, now on local disk.
4. Publish: write a copy of the manifest to `published/` in S3, listing S3 segments only.
5. Delete the merged local segments once no query snapshot pins them (the existing pin and grace rules, `manifest.rs:297`).

Crash at any step loses nothing and corrupts nothing:

| Crash after | State on restart | Cleanup |
|---|---|---|
| 1 | Snapshot is newer than needed | none |
| 2 | An S3 segment nothing references | existing orphan sweep (`engine.rs:1165-1201`) |
| 3 | Local manifest is ahead of the published one | step 4 runs on the next cycle |
| 4 | Local segments still on disk, unreferenced | deleted on the next cycle |

**The ordering invariant.** A published manifest must never reference a segment whose user ids are not in the published identity snapshot, and it carries the dictionary (`dict_snapshot`) those segments were written with. Step 1 before step 2 gives the first; the manifest already carries the second. This is what makes a restore from S3 alone readable.

### 3.3 S3-side merge

Uploading every interval leaves one S3 segment per interval per active partition (96 a day per bucket at 5 minutes). A second, slow job merges a partition's S3 segments once the partition is cold (proposed: the date is over and nothing was uploaded to it for an hour). It reads them once and writes one object. It runs on the owning node, because only that node can commit the manifest.

Whole-partition compaction on every fourth flush (today's policy) is removed for partitions in the local tier: local segments are merged exactly once, at upload.

### 3.4 Identity and profile snapshots

Both are SlateDB stores on the local volume after P0-7. They are pushed to S3 on the same cycle, identity first (3.2 step 1).

Proposed mechanism, to be validated before it is relied on (**inference** about SlateDB 0.6 file layout): mirror the store's directory. SlateDB writes immutable table files and numbered manifest files. Upload every file not yet in S3, table files first and manifest files last in ascending order, then delete S3 files the newest uploaded manifest no longer references. Only new files are sent, so the cost follows the write rate, not the store size.

Fallback if the file-level mirror does not give a consistent restore point: a logical export of the identity graph and profile rows. It is correct by construction and costs a full rewrite per cycle, which is acceptable for small projects and not for 10M users.

Either way the exit test is the same: restore from S3 into an empty directory and resolve every user id present in every published segment.

### 3.5 Restore after losing the volume

P0-7 makes a node refuse to start when the marker says `local` and the local stores are missing. This design adds an explicit command, not an automatic path:

`pulse-node restore --from-published`

1. Copies the identity and profile snapshots down.
2. Installs the published manifest as the local manifest.
3. Prints the publish time of what it restored and states the loss: events, profile updates and identity merges after that time are gone.

A node never restores on its own. Starting with data missing must be something a person chose.

### 3.6 Backpressure and disk

- Local segment bytes are bounded per project and per node. Past the bound the upload task runs immediately; past twice the bound ingest answers `429`.
- If S3 is unreachable, ingest continues until that bound, then `429`. Nothing is lost.
- Disk full or unwritable is `503` on ingest, never a panic and never an acknowledged write (P0-7 rule).
- Sizing (**estimate**): the volume holds the WAL since the last flush, L0 flush segments for one upload interval, the identity and profile stores in full, and the manifest history.

### 3.7 Encryption and erasure

- The local tier must go through the same `EncryptedObjectStore` wrapper as S3 when `PULSE_BYOK_MASTER_KEY` is set (**code**: `pulse-node/src/lib.rs:360`). A local tier in plaintext beside an encrypted bucket would be a regression.
- A user erasure must reach all of: L0, local segments, S3 segments, the local identity and profile stores, and the published snapshots. The published snapshot is replaced on the next cycle; the note in `docs/limitations.md` must say erased data can remain in S3 for up to one interval.

## 4. What changes for operators and clients

| Area | Today | After |
|---|---|---|
| Process crash | Nothing acknowledged is lost | Same |
| Volume lost | Not applicable (hot state in S3) | Everything since the last publish is lost: up to one upload interval of events, profile updates and identity merges |
| Query on the owning node | Sees acknowledged data | Same |
| Query, ingest or compactor role on another machine | Reads S3 | Sees data up to one interval old. The compactor role has nothing to do in local mode; S3-side merge runs on the owner |
| Data directory | Any disk | A persistent network volume (EBS), with scheduled snapshots as a second backstop |

No API response changes.

## 5. Cost after the change

**Estimate**, per busy project, 16 buckets all active, one current date, 5-minute interval: 16 segment PUTs + 1 published manifest + the identity and profile mirror (a handful of files) per cycle is about 25 PUTs per cycle, 300 per hour. At $0.005 per 1,000 PUTs that is about $1.10 per month per busy project. An idle project makes no calls.

For comparison, today's acknowledge path alone makes two PUTs per request for a new user (**measured**, section 2).

The real number must come from the counting store at a load-generator rate; that run is an exit criterion (section 7).

## 6. Shared nothing: what is still missing

With this design a node shares nothing at request time: its disk is its own, and S3 is written by one owner per project. Two things are not built and are not needed for one machine:

- **Routing.** A request must reach the node that owns the project. Today a non-owner answers `503 not_writer`. A second node needs either a router in front or a redirect carrying the owner's address from the lease row.
- **Moving a project.** Ownership changes are safe only from published state: the old owner publishes and stops, the new owner restores. That is the restore command driven by the lease, and it is a later step.

Section 15 of the review (appenders on other machines writing a shared log) is the alternative for scaling ingest of a single project beyond one machine. It needs the log in a shared store, which this design removes by default. The two are compatible only with `PULSE_HOT_STORE=object`. The single-machine path is the priority; section 15 stays deferred.

## 7. Order of work and exit criteria

Each step is one PR, merged before the next starts.

| Step | Change | Exit criteria |
|---|---|---|
| T0 | P0-7: WAL, identity and profiles local (in progress) | Counting segment store: zero calls between request and acknowledgement; idle project zero calls |
| T1 | Identity and profile snapshots to S3 on an interval, and `restore --from-published` for those stores | Restore into an empty directory resolves every user id in every segment; kill at each point of the mirror and restore still passes |
| T2 | Manifest authoritative on local disk, published copy in S3 on an interval. Segments still upload on every flush | Counting store: manifest PUTs per hour equal cycles per hour; a second machine in `query` role reads the published manifest; restore test covers events |
| T3 | Local segment tier and the upload cycle | Query results identical before and after an upload for the same data; kill at each of the five steps, restart, no acknowledged event missing or duplicated; PUTs per hour at load-generator rate recorded; local bytes stay under the bound with S3 unreachable, then `429` |
| T4 | S3-side cold merge; the same local tier for agent segments and replay chunks | Object count per closed day equals buckets; idle project zero calls including agent maintenance |

Rollback for T2 and T3 is a setting: an upload interval of zero publishes and uploads on every flush, which is today's behaviour.

## 8. Open questions

1. Does a file-level mirror of a SlateDB 0.6 directory give a consistent restore point while the store is being written (3.4)? If not, T1 uses the logical export and needs a size limit.
2. Default upload interval. 300 s is a proposal; it is the loss window on volume loss and the staleness seen by other machines.
3. Should the upload merge all buckets of a date into one object? It would cut PUTs by the bucket count but changes how queries prune by bucket. Not proposed here.
4. Late events for old dates create small partitions. Proposed: they upload on a longer maximum age, not every cycle.
5. Session-replay chunks: buffer locally and upload per session on the same cycle, or leave as they are? They are one PUT per chunk today.
6. The manifest is rewritten in full on every commit (`manifest.rs:208`). On local disk that is cheap, but its size grows with segment count; T4 bounds it. Is that enough, or does the manifest need a delta form?

## 9. Review record

Not yet reviewed.
