# Pulse Tiered Storage & Log-First Writers — Opaque-Box E2E Suite

Pure **Python 3.14+ stdlib** opaque-box E2E test suite (`scripts/e2e_tiered/`) targeting `pulse-node` exclusively over external HTTP, QUIC, CLI flags/subcommands, `/metrics`, `/healthz`, `/readyz`, SQLite/Postgres `MetaStore`, on-disk `PULSE_DATA_DIR`, and path-style S3 REST via `MockS3Server` (`PULSE_OBJECT_STORE_URL=s3://pulse-e2e` + `AWS_ENDPOINT`).

Zero third-party `pip` dependencies, MinIO, or Docker containers are required.

## Architecture

```
scripts/e2e_tiered/
├── README.md
├── runner.py                     # CLI runner (--tier, --feature, --milestone, --mode, --list, --json-out)
├── lib/
│   ├── __init__.py
│   ├── mock_s3.py                # Pure-stdlib ThreadingHTTPServer S3 REST mock + CAS + counters + faults
│   ├── node_mgr.py               # NodeManager, NodeInstance, ClusterHarness, CLI & disk/SQLite helpers
│   ├── client.py                 # PulseHttpClient, PlsbEncoder, PlsrEncoder, PlstEncoder, MetricsParser
│   └── oracle.py                 # @tier/@feature/@require_milestone gating, wait_until, fixture helpers
└── tests/
    ├── __init__.py
    ├── test_harness_self.py      # Offline harness tests + live pulse-node & MockS3Server integration (11 tests)
    ├── test_tier1_storage.py     # Tier 1 happy-path tests for F01–F21, F26–F27 (115 tests)
    ├── test_tier1_writers.py     # Tier 1 happy-path tests for F22–F25, F28–F31 (40 tests)
    ├── test_tier2_storage.py     # Tier 2 boundary/negative tests for F01–F21, F26–F27 (115 tests)
    ├── test_tier2_writers.py     # Tier 2 boundary/negative tests for F22–F25, F28–F31 (40 tests)
    ├── test_tier3_pairwise.py    # Tier 3 pairwise cross-feature interaction tests for F01–F31 (35 tests)
    └── test_tier4_workflows.py   # Tier 4 real-world multi-feature E2E workload scenarios S01–S18 (18 tests)
```

## Suite Coverage & Exact Test Counts

Total discovered tests: **374** across **31/31** features (`F01`–`F31`) and **9** milestones (`T0a`–`W3`):

### By Tier

| Tier | Scope | Count |
|---|---|---:|
| `self` | Harness unit & live `MockS3Server` + `pulse-node` integration (`test_harness_self.py`) | 11 |
| `1` | Isolated happy-path tests (`5` per feature × `31` features) | 155 |
| `2` | Boundary, corner-case, and negative/fault tests (`5` per feature × `31` features) | 155 |
| `3` | Pairwise cross-feature interaction tests covering all `F01`–`F31` | 35 |
| `4` | Real-world end-to-end multi-feature workload scenarios (`S01`–`S18`) | 18 |
| **Total** | | **374** |

### By Milestone

| Milestone | Scope | Tests |
|---|---|---:|
| `none` | Offline harness unit tests (`test_harness_self.py`) | 10 |
| `T0a` | Local WAL routing, mixed-mode refusal, `503` on WAL failure | 34 |
| `T0b` | Local `identity/` & `profiles/`, mode marker, `DurableLocalObjectStore` multipart fsync | 36 |
| `T1` | Published manifest (`publish_seq`), snapshots, T1 benchmark, `restore`, `erased_uids`, SQLite warn | 79 |
| `T2` | Local `manifests/`, dense `publish_seq` fencing, 2-interval deletion grace, remote `query` node | 45 |
| `T3` | Local `hot/segments/`, 7-step upload cycle (64 MiB cap), intent & sweep, byte bounds / `429` | 50 |
| `W1` | Single-node async event ack, `wal_dead/` reason & CLI, buffer/lag observability & alerts | 33 |
| `W2` | Sync profile endpoints returning `canonical_uid` & `apply_engage`/`on_identify` error propagation | 13 |
| `T4` | Cold S3 merge & local hot tier + upload/publish for agent & replay | 25 |
| `W3` | Stateless writer `writer_log/`, forwarder + `(writer_id, bucket)` offsets, `project_owner`, routing | 49 |
| **Total** | | **374** |

### By Feature (`F01`–`F31`)

Every feature has `5` Tier 1 + `5` Tier 2 + `>= 1` Tier 3 tests (plus Tier 4 workload coverage):
- `F01` (16), `F02` (15), `F03` (13), `F04` (16), `F05` (14), `F06` (13), `F07` (15), `F08` (16), `F09` (12), `F10` (17), `F11` (14), `F12` (14), `F13` (11), `F14` (14), `F15` (15), `F16` (13), `F17` (14), `F18` (14), `F19` (16), `F20` (14), `F21` (14), `F22` (15), `F23` (12), `F24` (13), `F25` (13), `F26` (14), `F27` (13), `F28` (14), `F29` (16), `F30` (15), `F31` (14).

## Quickstart

Discover and print exact test counts without executing:
```bash
python3 scripts/e2e_tiered/runner.py --list
```

Run harness self-tests (including live `MockS3Server` + `pulse-node` integration):
```bash
python3 scripts/e2e_tiered/runner.py --tier self -v
```

Run baseline-implemented milestones (e.g., `T0a` on `7757e8b`: executes `10` offline + `34` `T0a` = `44` live tests and cleanly skips `330` `T0b`–`W3` tests):
```bash
python3 scripts/e2e_tiered/runner.py --tier all --mode baseline --max-milestone T0a --no-build -v
```

Run targeted tiers, features, or milestones:
```bash
python3 scripts/e2e_tiered/runner.py --tier 1 --feature F01,F02,F03 --mode baseline --max-milestone T0a --no-build -v
python3 scripts/e2e_tiered/runner.py --milestone T1 --no-build -v
```

Run full strict acceptance suite (zero milestone skips allowed; `--max-milestone` is forbidden with `--mode full` and exits with code `2`, and `E2E_TIERED_MAX_MILESTONE` is ignored):
```bash
python3 scripts/e2e_tiered/runner.py --tier all --mode full --json-out /tmp/e2e_tiered_report.json
```

## Runner Flags

| Flag | Default | Description |
|---|---|---|
| `--tier {all,self,1,2,3,4}` | `all` | Tier to list or execute |
| `--feature F01..F31` | `None` | Filter by feature code (repeatable or comma-separated) |
| `--milestone T0a..W3` | `None` | Filter to a single milestone |
| `--mode {full,baseline}` | `full` | Strict `full` forbids milestone skips, rejects `--max-milestone` (exit `2`), and ignores `E2E_TIERED_MAX_MILESTONE`; `baseline` enables `--max-milestone` skipping |
| `--max-milestone T0a..W3` | `T0a` in `baseline` | Tests above this milestone raise `unittest.SkipTest` (only valid with `--mode baseline`) |
| `--list` | `False` | Print discovered tests and counts by tier, feature, and milestone |
| `--no-build` | `False` | Skip running `cargo build -p pulse-node --locked` |
| `--binary PATH` | auto | Explicit path to `pulse-node` binary |
| `--json-out PATH` | `None` | Write machine-readable JSON test report |
| `-v, --verbose` | `False` | Verbose test output |
| `--failfast` | `False` | Stop on first failure or error |

## Environment Variables

### Runner & Harness
- `E2E_TIERED_MODE`: Active runner mode (`full` or `baseline`). In `full` mode, milestone gating is strictly disabled.
- `E2E_TIERED_MAX_MILESTONE`: Max milestone threshold (`T0a`..`W3`) when running in `baseline` mode (ignored in `full` mode).
- `E2E_TIERED_BINARY`: Path to `pulse-node` binary used by `NodeManager`.
- `E2E_POSTGRES_URL` / `DATABASE_URL`: Optional external Postgres URL for tests requiring Postgres (`skip_if_no_postgres()`).

### Unified `PULSE_*` Node Configuration (Exercised by the Suite)
- **Hot store & object store**: `PULSE_HOT_STORE` (`local` | `object`), `PULSE_DATA_DIR`, `PULSE_OBJECT_STORE_URL`, `PULSE_BYOK_MASTER_KEY`, `PULSE_ALLOW_PRIVATE_ENDPOINTS`, `PULSE_ALLOW_EPHEMERAL_KEK`.
- **Upload, merge & local segment bounds**: `PULSE_UPLOAD_INTERVAL_SECS`, `PULSE_CLOSED_DATE_UPLOAD_SECS`, `PULSE_COLD_MERGE_IDLE_SECS`, `PULSE_MERGE_PART_MAX_BYTES` (64 MiB/partition merge cap), `PULSE_HOT_SEGMENTS_PROJECT_MAX_BYTES` (per-project local `hot/segments/` bound), `PULSE_HOT_SEGMENTS_MAX_BYTES` (node-level local `hot/segments/` bound), `PULSE_NUM_BUCKETS`.
- **Log-first writers & alerts**: `PULSE_WRITER_FLUSH_MS`, `PULSE_WRITER_FLUSH_BYTES`, `PULSE_WRITER_LOG_IDLE_CLOSE_MS`, `PULSE_WRITER_LOG_MAX_AGE_SECS`, `PULSE_WRITER_LOG_PROJECT_MAX_BYTES`, `PULSE_WRITER_LEASE_TTL_SECS`, `PULSE_L0_MAX_ROWS_PER_BUCKET`, `PULSE_ALERT_EVAL_INTERVAL_SECS`, `PULSE_NODE_NAME`, `PULSE_ADMIN_TOKEN`, `PULSE_METRICS_TOKEN`, `PULSE_SHUTDOWN_DRAIN_MS`.
