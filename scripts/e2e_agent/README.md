# Pulse AI Agent Observability — Opaque-Box E2E Test Suite

Pure **Python 3.14+ stdlib** opaque-box E2E harness (`scripts/e2e_agent/`) targeting `pulse-node` exclusively over external HTTP, MCP JSON-RPC 2.0, CLI flags, `/metrics`, `/healthz`, `/readyz`, Pulse Studio static assets, and black-box `PULSE_DATA_DIR` artifact inspection.

Zero external `pip` dependencies are required (`compression.zstd`, `zlib`, `http.client`, `http.server`, `hmac`, `hashlib`, `sqlite3`, `unittest`, `struct`, `json`).

## Prerequisites

- **Python**: `3.14+` (`compression.zstd` stdlib module)
- **Target binary** (for live runs): pre-built `target/release/pulse-node` or `target/debug/pulse-node`, or pass `--binary` / `--base-url`

## Quickstart

Run offline harness self-tests:
```bash
python3 -m unittest discover -s scripts/e2e_agent/tests -t scripts/e2e_agent -p 'test_*.py' -v
```

Validate test discovery and wire/fixture contracts offline:
```bash
python3 scripts/e2e_agent/runner.py --dry-run --tier all
python3 scripts/e2e_agent/runner.py --list-tests
```

Enforce full 276-test minimum coverage thresholds (`>= 120` Tier 1, `>= 120` Tier 2, `>= 24` Tier 3, `>= 12` Tier 4):
```bash
python3 scripts/e2e_agent/runner.py --dry-run --enforce-thresholds
```

Run live E2E tests (auto-spawns isolated `pulse-node --no-quic` on a temp dir and dynamic loopback port):
```bash
python3 scripts/e2e_agent/runner.py --tier 1
python3 scripts/e2e_agent/runner.py --tier all --enforce-thresholds --json-report /tmp/e2e_agent_report.json
```

Run against an already-running `pulse-node`:
```bash
python3 scripts/e2e_agent/runner.py \
  --base-url http://127.0.0.1:18080 \
  --data-dir /path/to/pulse_data \
  --admin-token sk_root_... \
  --tier all
```

## CLI Flags

| Flag | Default | Description |
|---|---|---|
| `--tier {1,2,3,4,all}` | `all` | Tier(s) to list, dry-run, or execute |
| `--base-url URL` | `None` | Target an already-running `pulse-node` instead of spawning one |
| `--binary PATH` | auto | Explicit path to `pulse-node` (`target/release` -> `target/debug`) |
| `--data-dir PATH` | temp dir | Explicit `PULSE_DATA_DIR` path |
| `--admin-token TOKEN` | auto | Explicit root admin token (otherwise read from `<data_dir>/root_admin_token`) |
| `--filter PATTERN` | `None` | Case-insensitive regex/substring filter over test ID, feature, title, or tags |
| `--dry-run` | `False` | Validate discovery and offline `PLST`/fixture contracts without booting `pulse-node` |
| `--list-tests` | `False` | Print deterministic sorted inventory of discovered tests and per-tier counts |
| `--json-report PATH` | `None` | Write machine-readable JSON summary & per-test results |
| `--verbose` | `False` | Print verbose diagnostics and failure tracebacks |
| `--enforce-thresholds` | `False` | Require full coverage thresholds (`T1>=120`, `T2>=120`, `T3>=24`, `T4>=12`, `Total>=276`) |

## Coverage Thresholds (`F01`–`F24`)

- **Tier 1 (Feature Coverage)**: `>= 120` (`>= 5` per feature across `F01..F24`)
- **Tier 2 (Boundary & Corner Cases)**: `>= 120` (`>= 5` per feature across `F01..F24`)
- **Tier 3 (Cross-Feature Pairwise)**: `>= 24`
- **Tier 4 (Real-World Application Workflows)**: `>= 12`
- **Total Minimum**: `>= 276`

## Environment Variables

- `PULSE_ALLOW_PRIVATE_ENDPOINTS=1`: Automatically set on spawned `pulse-node` processes so loopback BYOK LLM & HMAC webhook mocks are reachable.
- `PULSE_AGENT_DICT_PATH`: Optional path to an exported `agent_zstd_dict` binary file (raw or formatted Zstd dictionary with `b"\x37\xa4\x30\xec"` magic) used by `lib/plst.py` for `DICT_ZSTD` (`AGENT_DICT_ID = 2`).
- `PULSE_DATA_DIR` / `PULSE_ADMIN_TOKEN`: Optional node data directory & root admin token when running externally.
