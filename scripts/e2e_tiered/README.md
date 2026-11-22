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
    ├── test_harness_self.py      # Offline harness tests + live pulse-node & MockS3Server integration (8 tests)
    ├── test_tier1_storage.py     # Tier 1 happy-path tests for F01–F21, F26–F27 (115 tests)
    ├── test_tier1_writers.py     # Tier 1 happy-path tests for F22–F25, F28–F31 (40 tests)
    ├── test_tier2_storage.py     # Tier 2 boundary/negative tests for F01–F21, F26–F27 (115 tests)
    ├── test_tier2_writers.py     # Tier 2 boundary/negative tests for F22–F25, F28–F31 (40 tests)
    ├── test_tier3_pairwise.py    # Tier 3 pairwise cross-feature interaction tests for F01–F31 (35 tests)
    └── test_tier4_workflows.py   # Tier 4 real-world multi-feature E2E workload scenarios S01–S18 (18 tests)
```

## Suite Coverage & Exact Test Counts

Total discovered tests: **371** across **31/31** features (`F01`–`F31`) and **9** milestones (`T0a`–`W3`):

### By Tier

| Tier | Scope | Count |
|---|---|---:|
| `self` | Harness unit & live `MockS3Server` + `pulse-node` integration (`test_harness_self.py`) | 8 |
| `1` | Isolated happy-path tests (`5` per feature × `31` features) | 155 |
| `2` | Boundary, corner-case, and negative/fault tests (`5` per feature × `31` features) | 155 |
| `3` | Pairwise cross-feature interaction tests covering all `F01`–`F31` | 35 |
| `4` | Real-world end-to-end multi-feature workload scenarios (`S01`–`S18`) | 18 |
| **Total** | | **371** |

### By Milestone

| Milestone | Scope | Tests |
|---|---|---:|
| `none` | Offline harness unit tests (`test_harness_self.py`) | 7 |
| `T0a` | Local WAL routing, mixed-mode refusal, `503` on WAL failure | 34 |
| `T0b` | Local `identity/` & `profiles/`, mode marker, `DurableLocalObjectStore` multipart fsync | 36 |
| `T1` | Published manifest (`publish_seq`), snapshots, T1 benchmark, `restore`, `erased_uids`, SQLite warn | 79 |
| `T2` | Local `manifests/`, dense `publish_seq` fencing, 2-interval deletion grace, remote `query` node | 45 |
| `T3` | Local `hot/segments/`, 7-step upload cycle (64 MiB cap), intent & sweep, byte bounds / `429` | 50 |
| `W1` | Single-node async event ack, `wal_dead/` reason & CLI, buffer/lag observability & alerts | 33 |
| `W2` | Sync profile endpoints returning `canonical_uid` & `apply_engage`/`on_identify` error propagation | 13 |
| `T4` | Cold S3 merge & local hot tier + upload/publish for agent & replay | 25 |
| `W3` | Stateless writer `writer_log/`, forwarder + `(writer_id, bucket)` offsets, `project_owner`, routing | 49 |
| **Total** | | **371** |

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

Run baseline-implemented milestones (e.g., `T0a` on `7757e8b`: executes `8` self + `33` `T0a` = `41` live tests and cleanly skips `330` `T0b`–`W3` tests):
```bash
python3 scripts/e2e_tiered/runner.py --tier all --mode baseline --max-milestone T0a --no-build -v
```

Run targeted tiers, features, or milestones:
```bash
python3 scripts/e2e_tiered/runner.py --tier 1 --feature F01,F02,F03 --mode baseline --max-milestone T0a --no-build -v
python3 scripts/e2e_tiered/runner.py --milestone T1 --no-build -v
```

Run full strict acceptance suite (zero milestone skips allowed):
```bash
python3 scripts/e2e_tiered/runner.py --tier all --mode full --json-out /tmp/e2e_tiered_report.json
```

## Runner Flags

| Flag | Default | Description |
|---|---|---|
| `--tier {all,self,1,2,3,4}` | `all` | Tier to list or execute |
| `--feature F01..F31` | `None` | Filter by feature code (repeatable or comma-separated) |
| `--milestone T0a..W3` | `None` | Filter to a single milestone |
| `--mode {full,baseline}` | `full` | `full` forbids milestone skips; `baseline` enables `--max-milestone` skipping |
| `--max-milestone T0a..W3` | `T0a` in `baseline` | Tests above this milestone raise `unittest.SkipTest` |
| `--list` | `False` | Print discovered tests and counts by tier, feature, and milestone |
| `--no-build` | `False` | Skip running `cargo build -p pulse-node --locked` |
| `--binary PATH` | auto | Explicit path to `pulse-node` binary |
| `--json-out PATH` | `None` | Write machine-readable JSON test report |
| `-v, --verbose` | `False` | Verbose test output |
| `--failfast` | `False` | Stop on first failure or error |

## Environment Variables

- `E2E_TIERED_MODE`: Active runner mode (`full` or `baseline`).
- `E2E_TIERED_MAX_MILESTONE`: Max milestone threshold (`T0a`..`W3`) when gating skips.
- `E2E_TIERED_BINARY`: Path to `pulse-node` binary used by `NodeManager`.
- `E2E_POSTGRES_URL` / `DATABASE_URL`: Optional external Postgres URL for tests requiring Postgres (`skip_if_no_postgres()`).
