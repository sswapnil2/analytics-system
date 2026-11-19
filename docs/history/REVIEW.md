# Pulse Analytics: First-Principles Code Review

**Date:** 2026-11-16
**Scope:** every crate under `crates/`, `sdks/`, `web/src` and `scripts/verify_e2e.sh` (~23k lines)
**Method:** the code was read end to end and each claimed guarantee in `PROGRESS.md` was checked against it. Findings marked **[repro]** were reproduced with a scratch harness. Findings marked **[verified]** were re-read in the code by a second reviewer. All other findings come from one careful read.

## Instructions for the implementing agent

- Work in priority order: P0, then P1, then P2, then P3. Within a tier, the order doesn't matter.
- Every finding has an ID. Reference that ID in commits and in your summary, e.g. `fix(P0-07): ...`.
- **Fix at the root, in the shared function.** Don't patch one caller.
- **Add one regression test per P0/P1 finding.** The test must fail before the fix and pass after it. Most of these bugs only show up under concurrency, failure injection or hostile input, and the existing suite covers none of those.
- **Don't re-tick `PROGRESS.md` gates while their findings are open.** Several gates are currently checked but not actually met (see Section 5).
- If you disagree with a finding, say so with evidence (a code reference or a test) instead of silently skipping it.
- Gate before claiming done: `cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace --release && npm --prefix web run typecheck`

---

## P0: Remote takeover and crash (fix before any network exposure)

### P0-01: The control, query, replay and MCP APIs have no authentication **[verified]**
- **Where:**
  - `crates/pulse-control/src/api.rs:268-373`: the router. The only key check in it is `handle_edge_flag_bundle`, around line 1575.
  - `crates/pulse-node/src/lib.rs:50-51`: binds `0.0.0.0`.
  - `crates/pulse-node/src/lib.rs:168,180`: `CorsLayer::permissive()`.
- **Failure:** anyone who can reach the host can call any of these:
  - `GET /v1/projects/X/api-keys`, which returns plaintext secrets.
  - `POST /v1/projects/X/api-keys`, which mints a valid ingest key for any project.
  - `PATCH` on flags, the Lexicon PII rules, `POST /v1/admin/flush`, and `POST /v1/mcp`.

  Because CORS allows every origin, any web page an operator has open can make the same calls from their browser.
- **Fix:**
  - Put an admin-auth middleware (a bearer token from config or env) on every non-ingest, non-flag-bundle route.
  - Bind to `127.0.0.1` by default. Make `0.0.0.0` opt-in.
  - Restrict CORS on the admin router to configured origins. The public ingest and flags routes may stay permissive.
- **Done when:** an integration test shows that admin routes return 401 without the token and 200 with it, and that ingest still works with a project key.

### P0-02: A known API key is hardcoded, published, and stored in plaintext **[verified]**
- **Where:**
  - `crates/pulse-node/src/lib.rs:55,225,294`: `pk_pulse_live_india_01`.
  - `/v1/status` returns `default_api_key`.
  - `crates/pulse-control/src/db.rs:34-41`: `key_secret TEXT PRIMARY KEY`.
  - `web/src/App.tsx:334`: the UI falls back to the hardcoded key.
- **Failure:** anyone who has seen the source or the status page can write into `proj_fintech_in` on every deployment.
- **Fix:**
  - Generate a random key on first boot and print it once.
  - Remove the key from `/v1/status`.
  - Store only a hash: SHA-256 is enough, since keys are high-entropy.
  - Split keys into a public `pk_` key (ingest and flags only) and a secret `sk_` key (admin).

### P0-03: Upserts let one tenant overwrite another's rows
- **Where:**
  - `crates/pulse-control/src/db.rs`: every `ON CONFLICT(id) DO UPDATE`, at lines ~225, 695, 776, 869, 938, 1010, 1115, 1294 and 1368.
  - The handlers accept a client-supplied `id`: `api.rs` ~695, 1210, 1403, 1444, 1517, 1641.
- **Failure:**
  - `POST /v1/projects/B/cohorts {"id":"<A's cohort id>", ...}` rewrites project A's cohort.
  - `POST /v1/projects {"id":"<victim>"}` renames the victim's project and mints a key bound to it (api.rs ~709-720).

  This remains exploitable even after P0-01 is fixed.
- **Fix:**
  - Add `WHERE <table>.project_id = excluded.project_id` to each upsert's `DO UPDATE`. Treat "0 rows affected" as a 404 or 409.
  - Better: generate ids on the server and reject ids supplied by the client.
  - Project creation must never upsert an existing project.

### P0-04: Cross-project reads (IDOR) **[verified]**
- **Where:**
  - `crates/pulse-control/src/api.rs:1665-1670`: `AxumPath((_id, exp_id))` discards the project id.
  - `db.rs:~1248`: `get_experiment_by_id` queries `WHERE id = ?` only.
  - `crates/pulse-control/src/mcp.rs:172-176`: MCP takes `project_id` from the tool arguments.
- **Failure:** any project's experiment results, lexicon, funnels or frustrated sessions can be read through any other project's URL, or through MCP.
- **Fix:**
  - Thread `project_id` into every `get_*_by_id` and require `AND project_id = ?`.
  - Bind MCP to the project of the authenticated principal, not to an argument.
  - Audit every `db.rs` read for a missing `project_id` predicate.

### P0-05: zstd decompression bomb **[repro]** **[verified]**
- **Where:**
  - `crates/pulse-wire/src/batch.rs:177`: `zstd::stream::decode_all(&bytes[6..])`.
  - `batch.rs:186-189`: `read_to_end` with no limit.
  - `crates/pulse-wire/src/replay.rs:~362`: same pattern.
- **Repro:** a 32,791-byte PLSB frame decompresses to 1 GiB. The 64 MiB body limit (`pulse-ingest/src/http.rs:19`) only bounds the compressed size.
- **Fix:**
  - Decode through a `Read::take(MAX_DECOMPRESSED + 1)` wrapper. Return an error if more than `MAX_DECOMPRESSED` bytes come out. 64 MiB is a reasonable limit.
  - Apply this to all three call sites with one helper.

### P0-06: Memory amplification without compression **[repro]**
- **Where:**
  - `crates/pulse-wire/src/batch.rs:378-385,454-457`: `strings.get(idx).cloned()` deep-copies the string for every reference.
  - `crates/pulse-wire/src/replay.rs:247,368`: `Vec::with_capacity(count)` takes `count` from an untrusted u32.
- **Repro:** a 69-byte frame (one 64 KiB string, 20k events, every id field pointing at it) allocated 5.24 GB.
- **Fix:**
  - Cap the total decoded string bytes per batch, or make string-table entries `Arc<str>`.
  - Clamp every `with_capacity(n)` to `min(n, remaining_bytes / min_elem_size)`.
  - Add per-field length caps in `ByteCursor`.

### P0-07: The QUIC listener has no resource limits
- **Where:** `crates/pulse-ingest/src/quic.rs:35,46-53`. It allows 256 streams per connection, and `read_to_end(32 MiB)` runs before any auth check. It binds `0.0.0.0:8443` by default.
- **Fix:** disable QUIC by default (see Section 5). If it stays:
  - Lower the stream and connection limits.
  - Authenticate on the first frame.
  - Cap the buffered bytes per connection.

### P0-08: Formula parser stack overflow aborts the process **[repro]**
- **Where:** `crates/pulse-exec/src/insights.rs:596-615`. `parse_factor` recurses with no depth limit.
- **Repro:** the formula `"-".repeat(2_000_000) + "A"` gives `fatal runtime error: stack overflow, aborting`.
- **Fix:**
  - Add a depth counter that errors above 64 and a maximum formula length.
  - The parser also fails silently in three ways; make each an error:
    - unknown variables evaluate to 0;
    - trailing junk is ignored (`A B`, `(A)))`);
    - `x/0` evaluates to 0 instead of None.

### P0-09: Cyclic cohort references recurse forever **[verified]**
- **Where:**
  - `crates/pulse-exec/src/scanner.rs:550-559`: `CohortRef` recurses into the saved definition.
  - `pulse-control/api.rs:240-242`: definitions are stored without any cycle check.
- **Fix:**
  - Pass a visited set, or a depth limit, through `evaluate_cohort` and error on a repeat.
  - Also reject cycles at save time.

---

## P1: Acknowledged data lost or corrupted (RPO=0 and exactly-once are not met)

### P1-01: Dedup marks the event before the durable write **[verified]**
- **Where:** `crates/pulse-ingest/src/service.rs:227-233` inserts the fingerprint, while `:271-272` runs `append_bucket_events` later.
- **Failure:**
  1. The WAL `put` fails and the client gets a 500.
  2. The client retries the same batch.
  3. Every event in the retry is counted as `deduped` and the server returns 200. The data is gone.
- **Fix:** commit fingerprints only after the append succeeds. Either:
  - check without inserting first, then insert after the append succeeds; or
  - on append error, remove the fingerprints that were inserted for that bucket.

  Watch the race between two in-flight requests with the same uuid. The per-bucket lock from P1-02 resolves it if you do check, append and insert under that lock.
- **Test:** inject an object-store `put` failure. The retry must be accepted and the event must be stored once.

### P1-02: WAL and L0 sequence numbers are assigned out of order with inserts **[verified]**
- **Where:**
  - `crates/pulse-storage/src/wal.rs:57`: `fetch_add` runs before the awaited `put`.
  - `engine.rs:248`: `l0_seq` is assigned in `wire_event_to_row`.
  - `engine.rs:314-323`: the append path.
  - `mem_bucket.rs:120-149`: staging.
  - `engine.rs:396-409`: watermark commit and `purge_up_to(max_wal)`.
- **Failure (WAL):**
  1. Request A gets wal_seq 5 and request B gets wal_seq 6.
  2. B's put finishes first. B lands in L0 and triggers a flush.
  3. The flush sets `committed_wal_seq = 6` and `purge_up_to(6)` can delete A's WAL file.
  4. After a crash, `recover(6)` skips seq 5, so A's batch is lost even though it got a 200.
- **Failure (L0):** if A has l0=10 and B has l0=11, the floor becomes 11. `complete_flush` then uses `retain(l0_seq > 11)`, so A's row is dropped, or filtered out of every later snapshot and flush.
- **Fix:** take a per-bucket `tokio::Mutex` across seq allocation, WAL put and the MemBucket append. Per-bucket granularity keeps the throughput cost small.
- **Test:** run concurrent appends to one bucket with a randomized delay in `put`, force a flush, simulate a crash, then recover. Every acknowledged event must be present.

### P1-03: The default WAL backend never fsyncs
- **Where:** `crates/pulse-storage/src/store_factory.rs:25-28` defaults to `object_store::LocalFileSystem`, whose `put` is a staged write plus rename with no fsync.
- **Fix:** do one of the following:
  - write the local WAL through `std::fs` with `sync_data` on the file and the directory;
  - wrap `LocalFileSystem` and fsync after put;
  - document that the local backend is not durable and remove the RPO=0 claim for it.

### P1-04: One bad WAL frame stops the whole project from opening
- **Where:** `crates/pulse-storage/src/wal.rs:97-98` (`decode_wal_frame(&bytes)?`) and `engine.rs:184`.
- **Fix:**
  - Quarantine a bad frame by moving it to `wal_corrupt/` and logging an error, then continue.
  - Retry transient GET errors.
  - Extend the CRC to cover the seq and len header fields.

### P1-05: GC can delete a segment an active reader is using
- **Where:**
  - `engine.rs:223-225`: `snapshot()` clones the manifest, releases the lock, and only then calls `pin_all`.
  - `crates/pulse-storage/src/manifest.rs:201-207`: `unpin_all` decrements, drops the entry, then calls `remove`, which can erase a pin another thread has just taken. **[verified]**
- **Fix:**
  - Pin while holding the manifest read lock, or have GC re-check under the same lock.
  - Replace the decrement-then-remove sequence with `self.segment_pins.remove_if(p, |_, v| *v == 0)` after the decrement.

### P1-06: Concurrent compactions duplicate every row
- **Where:** `engine.rs:527-594`. `compact_overdue` takes no lock, and both the background loop (~784) and `compact_all_projects` can process the same partition.
- **Fix:**
  - Hold a per-partition compaction lock.
  - At commit, verify that all source segments are still active; otherwise abort and delete the output.

### P1-07: Readers can miss rows that are mid-flush
- **Where:** `engine.rs:222-235`. The manifest and watermark are captured eagerly, but L0 is read lazily later.
- **Fix:** capture the L0 rows, or an `Arc` of them, in the same critical section as the manifest snapshot.

### P1-08: Identity `alias` merges two already-identified users **[verified]**
- **Where:** `crates/pulse-identity/src/lib.rs:322-324` sets `self.parent[r_anon] = r_user` unconditionally.
- **Failure:**
  - On a shared device, `identify(anon, A)` followed by `identify(anon, B)` merges A into B permanently.
  - Any holder of the public key can send `/v1/identify {anon_id:"alice", user_id:"mallory"}`.
- **Fix:**
  - Track which roots are identified. Refuse, or no-op and log, an alias when both roots are already identified.
  - Add `reset()`/logout to both SDKs so they rotate the `anon_id`.

### P1-09: Identity `flush` loses mutations when the write fails **[verified]**
- **Where:** `crates/pulse-identity/src/lib.rs:626-631`. `mem::take` runs before `db.write_with_options(...)?` at :664.
- **Failure:** a transient error drops the Intern and Alias records. After a restart, the same u32 ids are handed to other `distinct_id`s.
- **Fix:** clone or drain only after the write succeeds, or put the records back on error.

### P1-10: Interned ids are not durable before rows that use them reach L1
- **Where:** `service.rs:265,272-274` and `engine.rs:325-330`. An inline flush can write segments before `id_graph.flush()` runs.
- **Fix:** flush the identity graph before WAL append and L1 flush, or store `distinct_id` in the WAL rows so they can be re-interned during recovery.

### P1-11: Intern and Alias records can reach the log in the wrong order
- **Where:** `crates/pulse-identity/src/lib.rs:517-523` makes the intern visible before it is pushed to `pending_mutations`. Replay then silently drops the Alias at ~458-462, and missing chunks are skipped with `continue` at ~411.
- **Fix:**
  - Push the mutation while still holding the lock that publishes the intern.
  - Make replay fail loudly on a dangling reference.

### P1-12: Dictionary ids saturate at u16::MAX and alias silently **[verified]**
- **Where:** `crates/pulse-storage/src/dictionary.rs:55,88` uses `guard.len().min(u16::MAX as usize) as u16`.
- **Failure:** a client sends 65,536 or more distinct property keys or event names. Every name after that gets id 65535, so their columns merge and results are wrong.
- **Fix:**
  - Return an error once the dictionary is full.
  - Also add a per-project cap on new keys per batch, far below 65k, enforced by governance.

### P1-13: The clock-skew normalizer rewrites legitimate timestamps
- **Where:** `crates/pulse-ingest/src/dedup.rs:91-95`, together with:
  - `pulse-wire/src/event.rs:~228`: JSON input defaults `sent = event` when it is missing;
  - `pulse-sdk-core/src/lib.rs:~671-673`: `sent` is stamped at enqueue time.
- **Failure:**
  - Backfill or server-side JSON events older than 5 s are re-stamped to "now".
  - A phone offline for 2 h has every event shifted 2 h later.
- **Fix:**
  - The SDK should stamp `client_sent_ts_ms` when the batch is actually transmitted.
  - The server should only correct when `sent` is present and `event <= sent`, and then shift by `(server_now - sent)`.
  - Use `checked_*` or saturating arithmetic. `i64::MIN.abs()` currently panics in debug builds.

### P1-14: Synthetic event uuids collide, so different events are deduped together
- **Where:** `crates/pulse-wire/src/event.rs:247-254` uses `seq_salt = props.len()`.
- **Failure:** two `add_to_cart` events in the same millisecond with different SKUs get the same uuid, and one is dropped.
- **Fix:** hash the full canonical event, props included, or require a client uuid on JSON ingest.

### P1-15: Bool `true` and Str `"true"` share one inverted-index key
- **Where:** `crates/pulse-storage/src/segment.rs:568-571` and `:589-590`. The Str bitmap is inserted second and replaces the Bool bitmap.
- **Fix:** tag index keys by type, e.g. `(pk, Tag::Bool, "true")`. See P2-01 for how the same bug shows up in queries.

### P1-16: ReplayStore append is read-modify-write of the whole object
- **Where:** `crates/pulse-storage/src/replay_store.rs:56-87` (guarded only by an in-process mutex) and `:60-68,114-125` (the file path depends on the date and an in-memory cache).
- **Failure:**
  - Two processes appending to one session lose a chunk.
  - After a restart past midnight, the session's next chunk starts a new file, so the stored `replay_offset_kb` values point into the wrong file.
  - Total cost is O(n²) bytes over a session.
  - The suffix match `"/{session_id}.plsr"` lets a session id containing `/` resolve to another session. This last point was not verified.
- **Fix:**
  - Store each chunk as its own object: `replays/{project}/{session}/{seq:010}.plsr`.
  - Validate `session_id` as `[A-Za-z0-9_-]{1,128}`.

---

## P2: Wrong analytics answers

### P2-01: The inverted-index row mask has false negatives
- **Where:**
  - `crates/pulse-exec/src/scanner.rs:376-405`, used by `insights.rs:349-356` and by `PerformedEvent` cohorts (`scanner.rs:450-458`);
  - `segment.rs:540-555`.
- **Failure cases:**
  - (a) Int and Str values of one key in the same segment: `Eq(Str("2"))` drops the `Int(2)` rows.
  - (b) The Bool/Str key collision from P1-15.
  - (c) When Str values exceed the cardinality limit, their bitmaps are cleared but `has_inv` stays true if Bool values exist, so matches return 0.

  Funnels and retention don't use the mask, so the same filter gives different answers in different engines.
- **Fix:** the index may only skip rows when it is *complete* for the predicate's value type. Otherwise fall back to the row check. Add a property test asserting that the masked result equals the unmasked result.

### P2-02: Flag cohort targeting is stored but never evaluated **[verified]**
- **Where:**
  - `pulse-control/src/experiments.rs:47-60` and `models.rs:185-190`;
  - `targeting_cohort_id` is never put into `EdgeFlagRule`.
- **Failure:** a flag "targeted to beta testers at 100%" turns on for 100% of all users.
- **Fix:** either:
  - implement targeting (ship cohort membership, or evaluate on the server), or
  - reject `targeting_cohort_id` with a 400 until it is implemented.

### P2-03: Bucketing differs between the web SDK and the server and mobile SDKs
- **Where:**
  - `sdks/ts-web/src/index.ts:89-96,210` uses FNV-1a 32-bit;
  - `experiments.rs:22-24` and `pulse-sdk-core/src/lib.rs:~420` use `xxh3_64 % 10_000`.
- **Failure:** the same user gets different variants on web and iOS.
- **Fix:**
  - Use one hash in every implementation.
  - Add a shared test-vector file (input, expected bucket) that the Rust and TS tests both assert against.

### P2-04: The web SDK identity and session don't persist
- **Where:** `sdks/ts-web/src/index.ts:128-130`.
- **Failure:** every page load creates a new anon user and a new session. Uniques inflate, and variants flip on reload.
- **Fix:**
  - Persist `anonId` and `routingSeed` in localStorage, wrapped in try/catch.
  - Persist the session id together with its last-activity timestamp.

### P2-05: The web SDK drops batches when sending fails
- **Where:** `sdks/ts-web/src/index.ts:404-418`. Events are `splice`d out of the queue before `fetch`, with no catch and no requeue.
- **Fix:**
  - Re-queue on failure, with a bounded queue and backoff.
  - Catch the rejection.
  - Register the `visibilitychange` beacon unconditionally (it currently depends on replay being enabled, lines 139-141) and include `replayOps` in it.
  - Remove listeners in `shutdown()` (lines 433-439).

### P2-06: The Rust SDK flush pops by count and races
- **Where:** `crates/pulse-sdk-core/src/lib.rs:478-556` and `pop_sent_from_disk_queue(sent_count)`.
- **Failure:**
  - The ticker flush and a manual or shutdown flush send the same packets twice, then each pops `sent_count`, which removes packets that were never sent.
  - An eviction during a flush shifts the indices, so the pop removes newer packets.
- **Fix:**
  - Hold a single flush mutex.
  - Remove sent packets by id, not by count.
- **Also fix:**
  - Lines 541-546: a permanent 4xx blocks the queue head forever. Drop the packet on 400, 413 and 422, and use backoff for 5xx.

### P2-07: The SDK disk WAL bound is wrong, the file write isn't atomic, and the CRC is discarded **[verified]**
- **Where:** `crates/pulse-sdk-core/src/lib.rs:20,70-79,716-721`.
  - The 10 MB bound is computed on raw bytes, but the file is JSON of a `Vec<u8>`, about 3-4× larger.
  - The whole file is rewritten synchronously on the caller's (UI) thread.
  - There is no temp file plus rename.
  - A corrupt file is silently discarded (line ~167).
  - The CRC is computed and then dropped: `let _ = crc;`.
- **Fix:**
  - Use an append-only length-prefixed file with a CRC per record, or at least write a temp file, fsync, and rename.
  - Measure the bound on the bytes actually on disk.
  - Move the I/O off the calling thread.

### P2-08: UniFFI async has no runtime (inferred from the code, not run on a device)
- **Where:** `crates/pulse-sdk-core/src/lib.rs:142,198-216`. `#[uniffi::export]` has no `async_runtime = "tokio"`, yet the functions call `reqwest`. The background flush only starts if `Handle::try_current()` succeeds.
- **Fix:**
  - Use `#[uniffi::export(async_runtime = "tokio")]`, or own a runtime inside `PulseClient`.
  - Add a test that calls the exported functions from a thread that has no runtime.

### P2-09: Smaller metric semantics bugs
| ID | Where | Bug | Fix |
|---|---|---|---|
| P2-09a | `insights.rs:263` | `sketch.add(v.max(0.0))` clamps negatives, so percentiles are wrong. If every value is negative, p50 is 0. | Use a DDSketch that supports negatives, or keep separate positive and negative stores. |
| P2-09b | `insights.rs:436-451` | WAU/MAU are anchored at the start of the bucket. On Week or Month intervals they lag about one period. | Anchor at the end of the bucket. Treat `Dau` at Week interval as an error or redefine it. |
| P2-09c | `ast.rs:21-27` | The previous period is 1 ms short (`end - start`, with `end` inclusive). | Use `dur = end - start + 1`. |
| P2-09d | `ast.rs:41-53` | Month is a fixed 30 days and Week is aligned to the epoch (Thursday), all in UTC. | Calendar-aware bucketing in the project's timezone. |
| P2-09e | `ast.rs:88-89` | `Neq` treats a missing value as Null, so `country != US` excludes rows with no country. | Decide the semantics, document them, and test them. |
| P2-09f | `experiments.rs:147,192` | `conversion_window_ms.max(86_400_000)` silently raises windows under 24 h to 24 h. | Respect the configured window. |
| P2-09g | `funnel.rs:673-674,345` | `1u16 << user_input` is unchecked. Debug builds panic; release builds apply the exclusion to the wrong step. | Validate the step count (≤16) and the `between_steps` range at parse time. |
| P2-09h | `funnel.rs:257,471-474` | `c_idx as u16` truncates past 65,535 chunks, so samples point at the wrong data, with a possible out-of-bounds panic. | Use `u32`. |
| P2-09i | `catalog.rs:212-215` | DataFusion lowercases identifiers, so `userPlan` fails as "no field userplan". | Quote identifiers, or turn off identifier normalization. |
| P2-09j | `scanner.rs:159-166,320`; `catalog.rs:434-451` | A lookup-table column silently shadows a real property of the same name. It also does a full table scan and clone per chunk. | Physical properties win; require lookups to be qualified (`table.col`). Pre-index lookup columns once. |
| P2-09k | `insights.rs:142` | `.take(2)` silently drops a third breakdown dimension. | Return an error. |
| P2-09l | `segment.rs:451`, `1043-1080` | Null property values disappear after flush. | Preserve Null explicitly. |

### P2-10: Experiment statistics
- **Where:** `crates/pulse-control/src/experiments.rs:211-345`.
- **Correct as written:** the pooled two-proportion z-test, the two-sided p-value and the seeded Monte Carlo.
- **Missing:**
  - a sample-ratio-mismatch check;
  - correction for multiple variants;
  - correction for peeking (the endpoint applies a fixed `p < 0.05` to live results);
  - the CI divides by `p_c` but ignores the uncertainty in `p_c`.
- **Also:**
  - Variant `weight_bps` values are not validated to sum to 10000 (`api.rs:1500-1515`).
  - There is no `UNIQUE(project_id, flag_key)` constraint (`db.rs:110-120`).
  - The flag upsert never updates `description` or `flag_key` (`db.rs:1010-1015`).
- **Fix:**
  - Add the SRM check.
  - Validate weights.
  - Add the unique constraint.
  - Label live p-values as "not peeking-corrected", or implement a sequential test.

---

## P3: Privacy, resource governance, hygiene

### P3-01: PII masking is far weaker than claimed
- **Where:**
  - `sdks/ts-web/src/index.ts:73-86`: only three regexes (email, Indian mobile, PAN).
  - `:264,292-294`: `document.title` is sent unmasked.
  - `:280-282` and `pulse-wire/src/replay.rs:133`: masked values keep their length.
  - `replay.rs:122-131`: a prefix of up to 16 letters is kept, so `"John Smith: ████"` leaks the name.
  - `pulse-ingest/src/service.rs:362`: replay chunks skip Lexicon PII rules entirely.
- **Leaks today:** card numbers, Aadhaar numbers, OTPs, balances and `contenteditable` text.
- **Fix:**
  - Mask all text by default, with an allowlist of `data-pulse-unmask` elements.
  - Use a fixed-length mask.
  - Remove the prefix-keeping rule.
  - Apply Lexicon rules to replay labels.

### P3-02: No resource governance in the query engine
- **Where:**
  - Time buckets are unbounded, and `cur += step` can overflow (`insights.rs:86-99`).
  - Breakdown cardinality is unbounded (`funnel.rs:369-372`, `insights.rs:333,407-409`).
  - Flows loads every event in the range (`flows.rs:51-95`).
  - `search_sessions` materializes every session before applying the limit (`engine.rs:282-305,382-383`).
  - There is no query timeout.
- **Fix:**
  - Validate ranges and cap the bucket count (e.g. 10k).
  - Cap breakdown groups to the top N, with an "other" bucket.
  - Push the `limit` down into the session search.
  - Add a per-query deadline.

### P3-03: Unbounded memory growth on long-running nodes
- **Where:**
  - `crates/pulse-exec/src/cache.rs:49-51,217,310`: `tier_a_cache`, `inverted_cache` and `prop_col_cache` are never evicted, and compaction keeps creating new segment ids.
  - The dedup ring has no size cap within a window.
  - `session_flags` grows forever.
  - `GET /v1/status?project_id=<anything>` opens and caches state for arbitrary strings (`api.rs:405-414`).
- **Fix:**
  - Bound those caches by bytes (moka or foyer, which are already dependencies), or evict them on GC.
  - Cap the dedup ring.
  - Only open projects that exist in the control DB.

### P3-04: Startup cost grows with total history
- **Where:**
  - Dedup seeding loads every fingerprint from every active segment (`engine.rs:153-173`), though it only needs the last 30 minutes.
  - The identity log replays from genesis and is never compacted (`pulse-identity/src/lib.rs:~409`).
  - `snapshot()` costs O(n) under a global write lock.
- **Fix:**
  - Seed dedup only from WAL plus L0, plus segments whose max timestamp falls within the dedup window.
  - Periodically checkpoint the identity state.

### P3-05: Smaller items
- `pulse-ingest/src/http.rs:39`: the API key is accepted in the `?api_key=` query string, so it ends up in logs. Accept headers only.
- `pulse-node/src/main.rs:50`: only Ctrl-C is handled. Handle SIGTERM too, and await in-flight QUIC tasks before `close_all`.
- `pulse-control/src/api.rs:1004-1017`: Range parsing is wrong for suffix ranges (`bytes=-500`) and inverted ranges (should return 416).
- `scripts/verify_e2e.sh:232`: `grep -q "206"` matches any header containing "206". Match the status line instead.
- Rage-tap logic emits one event per click after the third, because history isn't cleared after detection (`pulse-sdk-core/src/lib.rs:355-367`, `ts-web/src/index.ts:327-351`).
- SDK session rotation doesn't seal pending replay ops first, so they are attributed to the new session (`pulse-sdk-core/src/lib.rs:579-585,656`; `ts-web/src/index.ts:391`). A backdated `track_at` also triggers spurious rotations.
- `funnel.rs:753,776`: a global Mutex is taken per row inside the rayon scan, which serializes the parallel work. Use per-shard maps and merge them.

---

## Section 5: Claims in PROGRESS.md that are not met

Un-tick these gates, or reword them, until the findings listed for each are fixed.

| Claim | Status | Findings |
|---|---|---|
| WAL "RPO = 0" | Not met | P1-01, P1-02, P1-03, P1-04 |
| "Strict read-after-write MVCC" | Not met | P1-05, P1-07 |
| "Zero duplicates" after recovery | Not met under concurrency | P1-06, P1-14 |
| "Differential correctness tests matching brute-force oracle 100%" | Overstated: see note below | P2-01, P2-09 |
| "Bounded 10 MB disk WAL", "CRC-checked" (SDK) | Not met | P2-07 |
| "PII masking", "only ████ placeholders leave the client" | Not met | P3-01 |
| Flag targeting / cohorts | Not implemented | P2-02 |
| "SQLite/Postgres schema & migrations" | Only `CREATE TABLE IF NOT EXISTS`, no versioning | Use `sqlx::migrate!` |
| QUIC ingest | No client exists, and the cert is generated in memory and never distributed | P0-07 |
| WAL epoch fencing | Epoch is only part of the path; `recover` and `purge` ignore it | Implement it or remove the claim |

**About the oracle:** the reference in `crates/pulse-exec/src/lib.rs:39-426` calls the engine's own `PropPredicate::matches` and `TimeInterval::align_floor`, and re-implements the same algorithms (greedy loose matching, the exclusion rule, the flows collapse). It never exercises filters, cohorts, `first_time_ever`, percentiles, `Neq` or window boundaries.

Rewrite it as an independent and deliberately naive oracle that:
- shares no code with the engine;
- runs on randomized inputs (proptest);
- covers each of the features listed above.

## Section 6: Delete or simplify

- **Remove demo code from the production router and default boot.** That means `/v1/admin/simulate-burst` (`api.rs:489-668`), the hardcoded org `org_pulse_india` (`api.rs:700`), the default funnels in MCP (`mcp.rs:309-314`), and `auto_seed` defaulting to on (`pulse-node/src/lib.rs:56`). Make seeding opt-in (`--seed`).
- **QUIC:** disable by default, or delete it until a client exists.
- **`db.rs`:** about 1,400 lines of near-duplicate SQLite and Postgres branches. Collapse them with `sqlx::Any` or a placeholder helper.
- **Bucketing:** one shared implementation plus shared test vectors (P2-03), instead of three copies.
- **Dead code:**
  - the `LATEST` manifest hint (written, never read);
  - the segment trailer and `SegmentFooterMeta` (never read);
  - `d/<distinct_id>` identity keys (written, never read);
  - `top_n_per_step` (accepted, ignored);
  - `key_order_map` (`experiments.rs:257,347`);
  - the empty `if` at `funnel.rs:645-647`;
  - `session_seq` and `may_contain_all_events`.
- **`handle_create_project`:** it calls `reload_caches_from_db()` and rebuilds every project's caches. Update only the new project.

## Reproduction harnesses

These scratch harnesses are on the reviewer's machine and are not part of the repo:
- `wirefuzz/`, with modes `bomb`, `amp` and `ops`: covers P0-05 and P0-06.
- `exp/`: covers P0-08, P2-09c and P2-09i.

Recreate them as permanent regression tests in the repo:
- `crates/pulse-wire/tests/hostile_input.rs`
- `crates/pulse-exec/tests/limits.rs`
