"""Tier 2 (boundary/edge/negative) opaque-box E2E tests for Log-First Writers (F22-F25, F28-F31).

Covers:
  - W1: F22 (async event ack boundaries & faults), F23 (dead-letter boundaries & CLI),
        F24 (observability auth & fault boundaries)
  - W2: F25 (sync profile error propagation, 409 conflict, $delete_user ignored)
  - W3: F28 (writer_log HOL, mixed-batch abort, wiped writer_id, 6d expiry, 503),
        F29 (mid-forward kill, WAL-purge resend, partial-bucket crash, storage down),
        F30 (non-owner lease/open/loop guards, no auto-takeover, post-failover fence),
        F31 (503 when owner down vs async buffering, 409 proxy, NotOwner cache refresh)

Exactly 5 tests per feature x 8 features = 40 Tier 2 tests.
"""

from __future__ import annotations

import os
import shutil
import sys
import time
import unittest
import uuid
from pathlib import Path

_HARNESS_ROOT = Path(__file__).resolve().parent.parent
if str(_HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(_HARNESS_ROOT))

from lib.client import PulseHttpClient  # noqa: E402
from lib.node_mgr import ClusterHarness, NodeInstance  # noqa: E402
from lib.oracle import (  # noqa: E402
    external_postgres_url,
    feature,
    make_agent_trace,
    make_event,
    make_events,
    make_replay_chunk,
    require_milestone,
    skip_if_no_postgres,
    tier,
    wait_until,
)


def _create_project(client: PulseHttpClient, name: str) -> tuple[str, str]:
    resp = client.create_project(name).raise_for_status().json()
    return str(resp["project"]["id"]), str(resp["api_key"]["plaintext_key"])


def _insights_total(
    client: PulseHttpClient,
    project_id: str,
    event_name: str,
    start_ts_ms: int,
    end_ts_ms: int,
) -> float:
    res = client.query_insights(
        project_id,
        {
            "time_range": {"start_ts_ms": start_ts_ms, "end_ts_ms": end_ts_ms},
            "interval": "day",
            "series": [{"label": "cnt", "event_name": event_name, "metric": "totals"}],
        },
    ).raise_for_status().json()
    series = res.get("series") or []
    return float(series[0]["overall_value"]) if series else 0.0


def _w3_meta_url(cluster: ClusterHarness) -> tuple[str, Path | None]:
    pg = external_postgres_url()
    if pg:
        return pg, None
    return _w3_sqlite_meta_url(cluster)


def _w3_sqlite_meta_url(cluster: ClusterHarness) -> tuple[str, Path]:
    shared_dir = cluster.node_mgr.make_temp_data_dir("pulse_w3_t2_meta_")
    db_path = (shared_dir / "shared_meta.db").resolve()
    return f"sqlite://{db_path}", db_path


def _transfer_project_for_failover(
    cluster: ClusterHarness,
    src_node: NodeInstance,
    dst_node: NodeInstance,
    project_id: str,
    db_url: str,
) -> None:
    """Restore via CLI when Postgres is active, or copy flushed local project dirs under SQLite."""
    if db_url.startswith(("postgres://", "postgresql://")):
        assert cluster.mock_s3 is not None
        proc = cluster.node_mgr.run_cli(
            ["restore", "--from-published", "--data-dir", str(dst_node.data_dir)],
            env={**cluster.mock_s3.env_vars(), "DATABASE_URL": db_url},
        )
        if proc.returncode != 0:
            raise AssertionError(f"restore --from-published failed: {proc.stderr}")
        return
    for prefix in ("identity", "profiles", "manifests", "hot/segments", "wal/identity", "wal/profiles", "wal/manifests", "wal/hot/segments"):
        src_p = src_node.data_dir / prefix / project_id
        dst_p = dst_node.data_dir / prefix / project_id
        if src_p.exists():
            dst_p.parent.mkdir(parents=True, exist_ok=True)
            if dst_p.exists():
                shutil.rmtree(dst_p)
            shutil.copytree(src_p, dst_p)


def _find_dead_letter_subcmd(cluster: ClusterHarness) -> str:
    caps = cluster.node_mgr.probe_capabilities()
    for candidate in ("dead-letters", "dead-letter", "wal-dead", "inspect-dead-letters"):
        if caps.has_subcommand(candidate):
            return candidate
    for sub in sorted(caps.subcommands):
        if "dead" in sub:
            return sub
    raise AssertionError(
        f"No dead-letter CLI subcommand found in `pulse-node --help`; subcommands={sorted(caps.subcommands)}"
    )


def _fill_event_dictionary(
    client: PulseHttpClient,
    api_key: str,
    count: int,
    base_ts_ms: int,
    *,
    prefix: str = "t2_dict",
) -> None:
    batch_size = 100
    for start in range(0, count, batch_size):
        end = min(count, start + batch_size)
        evs = [
            make_event(
                event_name=f"{prefix}_{i:05d}",
                distinct_id=f"u_d_{i % 16}",
                ts_ms=base_ts_ms + i,
            )
            for i in range(start, end)
        ]
        client.ingest({"events": evs}, api_key=api_key).raise_for_status()

    wait_until(
        lambda: client.parsed_metrics().sum_by_name("pulse_ingest_log_lag_bytes") == 0.0,
        timeout=30.0,
        interval=0.1,
        desc="dictionary drain",
    )


def _find_writer_id(writer_dir: Path) -> str | None:
    log_root = writer_dir / "writer_log"
    if not log_root.exists():
        return None
    for cand in (log_root / "writer_id", log_root / "writer_id.txt", log_root / "id"):
        if cand.is_file():
            txt = cand.read_text(encoding="utf-8").strip()
            if txt:
                return txt
    for p in sorted(log_root.iterdir()):
        if p.is_file() and "writer" in p.name and p.stat().st_size < 256:
            txt = p.read_text(encoding="utf-8").strip()
            if txt:
                return txt
    return None


def _chmod_tree(root: Path, dir_mode: int, file_mode: int) -> None:
    if not root.exists():
        return
    for dp, dns, fns in os.walk(root, topdown=False):
        for fn in fns:
            try:
                os.chmod(Path(dp) / fn, file_mode)
            except OSError:
                pass
        for dn in dns:
            try:
                os.chmod(Path(dp) / dn, dir_mode)
            except OSError:
                pass
    try:
        os.chmod(root, dir_mode)
    except OSError:
        pass


def _find_profiles_dir(node: NodeInstance, project_id: str) -> Path:
    for rel in (f"wal/profiles/{project_id}", f"profiles/{project_id}", "wal/profiles", "profiles"):
        cand = node.data_dir / rel
        if cand.exists():
            return cand
    return node.data_dir / "wal" / "profiles"


class TestTier2F22AsyncEventAckBoundaries(unittest.TestCase):
    """Tier 2 boundary/negative tests for F22: Single-node async event ack (W1)."""

    @tier(2)
    @feature("F22")
    @require_milestone("W1")
    def test_per_request_schema_cap_422_dropped_on_async_ingest(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F22 No 422")
            now_ms = int(time.time() * 1000)

            # Previously >128 event names or >256 prop keys in 1 request returned 422.
            # W1 drops the per-request 422 schema cap on async /v1/ingest.
            evs = [
                make_event(
                    event_name=f"f22_reqcap_ev_{i:03d}",
                    distinct_id="u_reqcap",
                    ts_ms=now_ms - 5000 + i,
                    props={f"k_{i:03d}_a": i, f"k_{i:03d}_b": i + 1},
                )
                for i in range(150)
            ]
            resp = client.ingest({"events": evs}, api_key=api_key)
            self.assertEqual(resp.status, 200, f"Expected 200 (per-request 422 dropped), got {resp.status}: {resp.text()}")
            body = resp.json()
            self.assertEqual(body["accepted"], 150)
            self.assertEqual(body["deduped"], 0)
            wait_until(
                lambda: _insights_total(client, pid, "f22_reqcap_ev_149", now_ms - 60_000, now_ms + 60_000) == 1.0,
                timeout=15.0,
            )

    @tier(2)
    @feature("F22")
    @require_milestone("W1")
    def test_sigkill_under_load_loses_at_most_one_flush_bound_and_zero_duplicates(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(
                role="all",
                hot_store="local",
                extra_env={"PULSE_WRITER_FLUSH_MS": "20", "PULSE_WRITER_FLUSH_BYTES": "1048576"},
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F22 Kill Bound")
            now_ms = int(time.time() * 1000)

            flushed_evs = [
                make_event(
                    event_name="f22_kill_ev",
                    distinct_id=f"u_kb_{i % 8}",
                    ts_ms=now_ms - 10_000 + i,
                    event_uuid=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"f22kb:{i}")),
                )
                for i in range(60)
            ]
            client.ingest({"events": flushed_evs}, api_key=api_key).raise_for_status()
            time.sleep(0.12)  # > 20ms flush bound: guaranteed WAL-durable

            tail_evs = [
                make_event(
                    event_name="f22_kill_ev",
                    distinct_id=f"u_kb_{i % 8}",
                    ts_ms=now_ms - 5_000 + i,
                    event_uuid=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"f22kb:{i}")),
                )
                for i in range(60, 80)
            ]
            client.ingest({"events": tail_evs}, api_key=api_key).raise_for_status()
            node.kill()

            restarted = node.restart(graceful_stop=False)
            assert isinstance(restarted, NodeInstance)
            r_client = restarted.client()
            wait_until(
                lambda: _insights_total(r_client, pid, "f22_kill_ev", now_ms - 60_000, now_ms + 60_000) >= 60.0,
                timeout=15.0,
                desc="flushed events recovered after SIGKILL",
            )
            recovered = _insights_total(r_client, pid, "f22_kill_ev", now_ms - 60_000, now_ms + 60_000)
            self.assertGreaterEqual(recovered, 60.0)
            self.assertLessEqual(recovered, 80.0)

            # Client retries all 80 events with identical UUIDs -> must land at exact 80 with 0 duplicates
            r_client.ingest({"events": flushed_evs + tail_evs}, api_key=api_key).raise_for_status()
            wait_until(
                lambda: _insights_total(r_client, pid, "f22_kill_ev", now_ms - 60_000, now_ms + 60_000) == 80.0,
                timeout=15.0,
            )

    @tier(2)
    @feature("F22")
    @require_milestone("W1")
    def test_mixed_batch_with_identify_conflict_409_aborts_event_queueing(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F22 Mixed 409")
            now_ms = int(time.time() * 1000)

            client.identify({"anon_id": "a_c1", "user_id": "u_c1"}, api_key=api_key).raise_for_status()
            client.identify({"anon_id": "a_c2", "user_id": "u_c2"}, api_key=api_key).raise_for_status()

            resp = client.ingest(
                {
                    "events": [
                        {"type": "identify", "userId": "u_c1", "anonymousId": "a_c2"},
                        make_event(event_name="f22_aborted_ev", distinct_id="u_c1", ts_ms=now_ms - 1000),
                    ]
                },
                api_key=api_key,
            )
            self.assertEqual(resp.status, 409)
            time.sleep(0.2)
            self.assertEqual(
                _insights_total(client, pid, "f22_aborted_ev", now_ms - 60_000, now_ms + 60_000),
                0.0,
            )

    @tier(2)
    @feature("F22")
    @require_milestone("W1")
    def test_ingest_validation_400_401_413_partial_rejected_and_429_backpressure(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(
                role="all",
                hot_store="local",
                extra_env={"PULSE_L0_MAX_ROWS_PER_BUCKET": "5"},
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F22 Errors")
            now_ms = int(time.time() * 1000)

            # 401 on invalid key
            self.assertEqual(client.ingest({"events": [make_event()]}, api_key="pk_live_bogus").status, 401)
            # 400 on malformed JSON
            self.assertEqual(client.ingest("{not-json", api_key=api_key).status, 400)
            # 413 on >8 MiB body
            huge = b'{"events":[' + (b" " * (8 * 1024 * 1024 + 64)) + b"]}"
            self.assertEqual(client.ingest(huge, api_key=api_key, content_type="application/json").status, 413)

            # Partial malformed item returns 200 with rejected index while queuing valid event
            part = client.ingest(
                {
                    "events": [
                        make_event(event_name="f22_part_ok", distinct_id="u_p1", ts_ms=now_ms - 1000),
                        {"distinct_id": "u_bad"},  # missing event_name
                    ]
                },
                api_key=api_key,
            )
            self.assertEqual(part.status, 200)
            pjson = part.json()
            self.assertEqual(pjson["accepted"], 1)
            self.assertEqual(len(pjson.get("rejected", [])), 1)
            self.assertEqual(pjson["rejected"][0]["index"], 1)

            # Trigger L0 backpressure 429 with tight row cap
            saw_429 = False
            for i in range(40):
                r = client.ingest(
                    {"events": make_events(50, event_name="f22_bp", distinct_id_prefix="u_same", base_ts_ms=now_ms + i)},
                    api_key=api_key,
                )
                if r.status == 429:
                    saw_429 = True
                    self.assertIn("retry-after", r.headers)
                    break
            self.assertTrue(saw_429, "Expected 429 backpressure under tiny PULSE_L0_MAX_ROWS_PER_BUCKET=5")

    @tier(2)
    @feature("F22")
    @require_milestone("W1")
    def test_unwritable_wal_dir_returns_503_never_200_or_500(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F22 WAL 503")
            now_ms = int(time.time() * 1000)

            # Seed one event so WAL directory exists
            client.ingest({"events": [make_event(event_name="f22_seed", ts_ms=now_ms - 2000)]}, api_key=api_key).raise_for_status()
            wal_root = node.data_dir / "wal" / "wal"
            if not wal_root.exists():
                wal_root = node.data_dir / "wal"
            _chmod_tree(wal_root, 0o555, 0o444)
            self.addCleanup(_chmod_tree, wal_root, 0o755, 0o644)

            saw_503 = False
            for i in range(5):
                # Use payload > 1 MiB or wait past flush bound so WAL write failure surfaces
                r = client.ingest(
                    {"events": make_events(10, event_name="f22_ro", distinct_id_prefix=f"u_ro_{i}", base_ts_ms=now_ms + i)},
                    api_key=api_key,
                )
                if r.status == 503:
                    saw_503 = True
                    break
                time.sleep(0.05)
            self.assertTrue(saw_503, "Expected HTTP 503 on unwritable WAL directory")

            _chmod_tree(wal_root, 0o755, 0o644)
            wait_until(
                lambda: client.ingest({"events": [make_event(event_name="f22_rec", ts_ms=now_ms)]}, api_key=api_key).status == 200,
                timeout=10.0,
            )


class TestTier2F23DeadLetterBoundaries(unittest.TestCase):
    """Tier 2 boundary/negative tests for F23: Dead-letter reason storage & CLI (W1)."""

    @tier(2)
    @feature("F23")
    @require_milestone("W1")
    def test_dead_letter_wal_replay_after_sigkill_is_idempotent(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F23 Idempotent Replay")
            now_ms = int(time.time() * 1000)

            _fill_event_dictionary(client, api_key, 8192, now_ms - 20_000, prefix="dl_idemp")
            client.ingest(
                {"events": [make_event(event_name="dl_idemp_overflow", distinct_id="u_idemp", ts_ms=now_ms - 500)]},
                api_key=api_key,
            ).raise_for_status()
            wait_until(
                lambda: any("wal_dead/" in p for p in node.local_files()),
                timeout=15.0,
            )
            dead_before = [p for p in node.local_files() if "wal_dead/" in p]
            node.kill()

            restarted = node.restart(graceful_stop=False)
            assert isinstance(restarted, NodeInstance)
            time.sleep(0.5)
            dead_after = [p for p in restarted.local_files() if "wal_dead/" in p]
            self.assertEqual(len(dead_after), len(dead_before), f"Duplicate dead-letter created on replay: {dead_before} -> {dead_after}")

    @tier(2)
    @feature("F23")
    @require_milestone("W1")
    def test_property_key_dictionary_cap_16384_dead_letters_with_reason(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            assert cluster.mock_s3 is not None
            subcmd = _find_dead_letter_subcmd(cluster)
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F23 Prop Cap 16384")
            now_ms = int(time.time() * 1000)

            # Fill 16,384 property keys across 32 events (~512 keys per event)
            props_total = 16384
            per_ev = 512
            evs = []
            for b in range(props_total // per_ev):
                p = {f"pk_{b * per_ev + j:05d}": j for j in range(per_ev)}
                evs.append(make_event(event_name="f23_pk_seed", distinct_id="u_pk", ts_ms=now_ms - 10_000 + b, props=p))
            client.ingest({"events": evs}, api_key=api_key).raise_for_status()
            wait_until(lambda: client.parsed_metrics().sum_by_name("pulse_ingest_log_lag_bytes") == 0.0, timeout=30.0)

            # Event introducing property key #16385 must be dead-lettered
            client.ingest(
                {"events": [make_event(event_name="f23_pk_seed", distinct_id="u_pk_over", ts_ms=now_ms - 500, props={"pk_overflow_16385": 1})]},
                api_key=api_key,
            ).raise_for_status()
            wait_until(
                lambda: client.parsed_metrics().sum_by_name("pulse_ingest_dead_lettered_events_total") >= 1.0,
                timeout=15.0,
            )
            node.stop(graceful=True)

            proc = cluster.node_mgr.run_cli(
                [subcmd, "--project-id", pid, "--data-dir", str(node.data_dir)],
                env=cluster.mock_s3.env_vars(),
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("u_pk_over", proc.stdout)

    @tier(2)
    @feature("F23")
    @require_milestone("W1")
    def test_dead_letter_cli_handles_empty_and_unknown_project_without_panic(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            assert cluster.mock_s3 is not None
            subcmd = _find_dead_letter_subcmd(cluster)
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            pid, _ = _create_project(node.client(), "F23 Empty CLI")
            node.stop(graceful=True)

            empty_proc = cluster.node_mgr.run_cli(
                [subcmd, "--project-id", pid, "--data-dir", str(node.data_dir)],
                env=cluster.mock_s3.env_vars(),
            )
            self.assertEqual(empty_proc.returncode, 0, empty_proc.stderr)
            self.assertNotIn("panicked", empty_proc.stderr.lower())

            missing_proc = cluster.node_mgr.run_cli(
                [subcmd, "--project-id", "proj_nonexistent_999", "--data-dir", str(node.data_dir)],
                env=cluster.mock_s3.env_vars(),
            )
            self.assertNotIn("panicked", missing_proc.stderr.lower())

    @tier(2)
    @feature("F23")
    @require_milestone("W1")
    def test_unwritable_wal_dead_retries_without_dropping_event(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F23 wal_dead Retry")
            now_ms = int(time.time() * 1000)

            _fill_event_dictionary(client, api_key, 8192, now_ms - 20_000, prefix="dl_ro")
            dead_root = node.data_dir / "wal" / "wal_dead"
            dead_root.mkdir(parents=True, exist_ok=True)
            _chmod_tree(dead_root, 0o555, 0o444)
            self.addCleanup(_chmod_tree, dead_root, 0o755, 0o644)

            client.ingest(
                {"events": [make_event(event_name="dl_ro_overflow", distinct_id="u_dl_ro", ts_ms=now_ms - 500)]},
                api_key=api_key,
            ).raise_for_status()
            time.sleep(0.4)
            # Must NOT have dropped/skipped the dead-letter write while wal_dead was read-only
            self.assertEqual(len([p for p in node.local_files() if "wal_dead/" in p]), 0)

            _chmod_tree(dead_root, 0o755, 0o644)
            wait_until(
                lambda: any("wal_dead/" in p for p in node.local_files()),
                timeout=15.0,
                desc="dead-letter written after wal_dead permissions restored",
            )

    @tier(2)
    @feature("F23")
    @require_milestone("W1")
    def test_dead_letter_cli_erasure_timestamp_boundary(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            assert cluster.mock_s3 is not None
            subcmd = _find_dead_letter_subcmd(cluster)
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F23 TS Boundary")
            now_ms = int(time.time() * 1000)

            client.identify({"anon_id": "a_tsb", "user_id": "u_tsb"}, api_key=api_key).raise_for_status()
            _fill_event_dictionary(client, api_key, 8192, now_ms - 30_000, prefix="dl_tsb")

            # Dead-letter event before erasure
            client.ingest(
                {"events": [make_event(event_name="dl_tsb_before", distinct_id="u_tsb", ts_ms=now_ms - 5000)]},
                api_key=api_key,
            ).raise_for_status()
            wait_until(lambda: client.parsed_metrics().sum_by_name("pulse_ingest_dead_lettered_events_total") >= 1.0, timeout=15.0)

            client.delete_user(pid, "u_tsb").raise_for_status()
            node.stop(graceful=True)

            proc = cluster.node_mgr.run_cli(
                [subcmd, "--project-id", pid, "--data-dir", str(node.data_dir)],
                env=cluster.mock_s3.env_vars(),
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertNotIn("dl_tsb_before", proc.stdout)
            self.assertNotIn("u_tsb", proc.stdout)


class TestTier2F24ObservabilityBoundaries(unittest.TestCase):
    """Tier 2 boundary/negative tests for F24: Ingest buffer/lag observability & alerts (W1)."""

    @tier(2)
    @feature("F24")
    @require_milestone("W1")
    def test_metrics_token_rejects_missing_wrong_and_non_bearer_headers_with_401(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local", metrics_token="secret_metrics_99")
            assert isinstance(node, NodeInstance)
            client = node.client()

            r_none = client.metrics(token=None)
            self.assertEqual(r_none.status, 401)
            self.assertEqual(r_none.body, b"")

            r_wrong = client.metrics(token="wrong_token")
            self.assertEqual(r_wrong.status, 401)

            r_hdr = client.request("GET", "/metrics", headers={"x-pulse-admin-key": "secret_metrics_99"})
            self.assertEqual(r_hdr.status, 401)

    @tier(2)
    @feature("F24")
    @require_milestone("W1")
    def test_metrics_finite_non_negative_under_idle_and_invalid_requests(self) -> None:
        import math

        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()

            client.ingest({"events": [make_event()]}, api_key="pk_live_invalid")
            client.ingest("{bad", api_key=node.extract_boot_ingest_key() or "pk_live_invalid")

            parsed = client.parsed_metrics()
            for (name, _), val in parsed.items():
                if name.startswith("pulse_"):
                    self.assertFalse(math.isnan(val), f"NaN in metric {name}")
                    self.assertFalse(math.isinf(val), f"Inf in metric {name}")
                    self.assertGreaterEqual(val, 0.0, f"Negative metric {name}={val}")
            self.assertEqual(parsed.sum_by_name("pulse_ingest_events_accepted_total"), 0.0)

    @tier(2)
    @feature("F24")
    @require_milestone("W1")
    def test_dead_letter_metric_stays_zero_until_dictionary_overflow(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F24 DL Metric")
            now_ms = int(time.time() * 1000)

            _fill_event_dictionary(client, api_key, 8192, now_ms - 20_000, prefix="f24_dlm")
            self.assertEqual(client.parsed_metrics().sum_by_name("pulse_ingest_dead_lettered_events_total"), 0.0)

            client.ingest(
                {
                    "events": [
                        make_event(event_name="f24_dlm_ov_1", distinct_id="u1", ts_ms=now_ms - 300),
                        make_event(event_name="f24_dlm_ov_2", distinct_id="u2", ts_ms=now_ms - 200),
                        make_event(event_name="f24_dlm_ov_3", distinct_id="u3", ts_ms=now_ms - 100),
                    ]
                },
                api_key=api_key,
            ).raise_for_status()
            wait_until(
                lambda: client.parsed_metrics().sum_by_name("pulse_ingest_dead_lettered_events_total") == 3.0,
                timeout=15.0,
            )

    @tier(2)
    @feature("F24")
    @require_milestone("W1")
    def test_metrics_endpoint_serves_200_across_all_node_roles(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            nodes = cluster.start_split_cluster(hot_store="local")
            for role, inst in nodes.items():
                m = inst.client().parsed_metrics()
                self.assertEqual(m.get_metric("pulse_ready"), 1.0, f"Role {role} not ready on /metrics")

    @tier(2)
    @feature("F24")
    @require_milestone("W1")
    def test_forwarding_lag_age_and_bytes_increase_monotonically_during_outage(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage, writer = topo["storages"][0], topo["writers"][0]
            _, api_key = _create_project(storage.client(), "F24 Lag Rise")

            storage.stop(graceful=True)
            writer.client().ingest({"events": make_events(15, event_name="f24_stall")}, api_key=api_key).raise_for_status()
            time.sleep(0.2)
            m1 = writer.client().parsed_metrics()
            lag_bytes_1 = sum(v for (n, _), v in m1.items() if ("lag" in n or "writer_log" in n) and "bytes" in n)
            lag_secs_1 = [v for (n, _), v in m1.items() if "lag" in n and ("sec" in n or "age" in n)]
            self.assertGreater(lag_bytes_1, 0.0)

            time.sleep(0.5)
            m2 = writer.client().parsed_metrics()
            lag_secs_2 = [v for (n, _), v in m2.items() if "lag" in n and ("sec" in n or "age" in n)]
            if lag_secs_1 and lag_secs_2:
                self.assertGreater(max(lag_secs_2), max(lag_secs_1))


class TestTier2F25SyncProfileBoundaries(unittest.TestCase):
    """Tier 2 boundary/negative tests for F25: Sync profile endpoint & error propagation (W2)."""

    @tier(2)
    @feature("F25")
    @require_milestone("W2")
    def test_unwritable_profiles_dir_propagates_error_on_engage_never_200(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F25 Engage Err")

            client.engage({"distinct_id": "u_eng_err", "$set": {"a": 1}}, api_key=api_key).raise_for_status()
            pdir = _find_profiles_dir(node, pid)
            _chmod_tree(pdir, 0o555, 0o444)
            self.addCleanup(_chmod_tree, pdir, 0o755, 0o644)

            resp = client.engage({"distinct_id": "u_eng_err_2", "$set": {"a": 2}}, api_key=api_key)
            self.assertIn(resp.status, (500, 503), f"Expected 500/503 error propagation on unwritable profiles/, got {resp.status}: {resp.text()}")

            _chmod_tree(pdir, 0o755, 0o644)
            self.assertEqual(client.engage({"distinct_id": "u_eng_err_3", "$set": {"a": 3}}, api_key=api_key).status, 200)

    @tier(2)
    @feature("F25")
    @require_milestone("W2")
    def test_unwritable_profiles_dir_propagates_error_on_identify_and_mixed_batch(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F25 Identify Err")

            client.identify({"anon_id": "a_seed", "user_id": "u_seed", "user_properties": {"k": "v"}}, api_key=api_key).raise_for_status()
            pdir = _find_profiles_dir(node, pid)
            _chmod_tree(pdir, 0o555, 0o444)
            self.addCleanup(_chmod_tree, pdir, 0o755, 0o644)

            r_id = client.identify({"anon_id": "a_fail", "user_id": "u_fail", "user_properties": {"k": "v"}}, api_key=api_key)
            self.assertIn(r_id.status, (500, 503), f"Expected 500/503 on identify with unwritable profiles/, got {r_id.status}")

            r_mix = client.ingest(
                {
                    "events": [
                        {"type": "identify", "userId": "u_mix_fail", "anonymousId": "a_mix_fail", "traits": {"k": "v"}},
                        make_event(event_name="f25_should_not_ack", distinct_id="u_mix_fail"),
                    ]
                },
                api_key=api_key,
            )
            self.assertIn(r_mix.status, (500, 503), f"Expected 500/503 on mixed batch identify failure, got {r_mix.status}")

    @tier(2)
    @feature("F25")
    @require_milestone("W2")
    def test_identify_conflict_between_distinct_identified_users_returns_409(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F25 409 Conflict")

            r1 = client.identify({"anon_id": "a_1", "user_id": "u_1", "user_properties": {"who": 1}}, api_key=api_key).raise_for_status().json()
            r2 = client.identify({"anon_id": "a_2", "user_id": "u_2", "user_properties": {"who": 2}}, api_key=api_key).raise_for_status().json()
            self.assertNotEqual(r1["canonical_uid"], r2["canonical_uid"])

            conflict = client.identify({"anon_id": "a_1", "user_id": "u_2"}, api_key=api_key)
            self.assertEqual(conflict.status, 409)
            self.assertEqual(client.get_user(pid, "u_1").raise_for_status().json()["canonical_uid"], r1["canonical_uid"])

    @tier(2)
    @feature("F25")
    @require_milestone("W2")
    def test_engage_delete_user_operator_ignored_with_ingest_key(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F25 Ignore Delete")

            client.engage({"distinct_id": "u_safe", "$set": {"keep": "yes"}}, api_key=api_key).raise_for_status()
            client.engage({"distinct_id": "u_safe", "$delete_user": True}, api_key=api_key)

            detail = client.get_user(pid, "u_safe").raise_for_status().json()
            self.assertEqual(detail["profile"]["props"]["keep"], "yes")
            self.assertEqual(client.list_deletions(pid).raise_for_status().json(), [])

    @tier(2)
    @feature("F25")
    @require_milestone("W2")
    def test_profile_endpoints_reject_invalid_payloads_and_non_ingest_auth(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            _, api_key = _create_project(client, "F25 Bad Auth")

            # sk_root cannot be used on public ingest profile routes
            self.assertEqual(client.identify({"anon_id": "a", "user_id": "u"}, api_key=cluster.admin_token).status, 401)
            self.assertEqual(client.engage({"distinct_id": "u", "$set": {"x": 1}}, api_key=cluster.admin_token).status, 401)
            # Missing required fields -> 400
            self.assertEqual(client.identify({"anon_id": ""}, api_key=api_key).status, 400)
            self.assertEqual(client.groups({"$set": {"x": 1}}, api_key=api_key).status, 400)


class TestTier2F28WriterLogBoundaries(unittest.TestCase):
    """Tier 2 boundary/negative tests for F28: Stateless writer & writer_log/ (W3)."""

    @tier(2)
    @feature("F28")
    @require_milestone("W3")
    def test_per_project_log_cap_429_does_not_head_of_line_block_other_project(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(
                num_writers=1,
                num_storage=2,
                database_url=db_url,
                extra_env={"PULSE_WRITER_LOG_PROJECT_MAX_BYTES": "65536"},
            )
            s1, s2 = topo["storages"]
            writer = topo["writers"][0]
            p1, k1 = _create_project(s1.client(), "F28 P1")
            p2, k2 = _create_project(s1.client(), "F28 P2")
            now_ms = int(time.time() * 1000)

            # Determine owner of P1 and stop it so P1 writer_log fills to 429
            o1 = s1.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (p1,), db_path=db_path)[0]["node_name"]
            o2 = s1.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (p2,), db_path=db_path)[0]["node_name"]
            if o1 == o2:
                other_s = s2 if o1 == "storage-0" else s1
                other_name = "storage-1" if o1 == "storage-0" else "storage-0"
                s1.sqlite_query("UPDATE project_owner SET node_name = ?, address = ? WHERE project_id = ?", (other_name, other_s.base_url, p2), db_path=db_path, readonly=False)
            p1_owner = s1 if o1 == "storage-0" else s2
            p2_owner = s2 if o1 == "storage-0" else s1

            p1_owner.stop(graceful=True)
            saw_429 = False
            big_props = {"blob": "x" * 2048}
            for i in range(80):
                r = writer.client().ingest(
                    {"events": make_events(5, event_name="f28_fill", distinct_id_prefix=f"uf_{i}", props=big_props, base_ts_ms=now_ms + i)},
                    api_key=k1,
                )
                if r.status == 429:
                    saw_429 = True
                    self.assertIn("retry-after", r.headers)
                    break
                time.sleep(0.02)
            self.assertTrue(saw_429, "Expected P1 to hit 429 writer_log cap")

            # P2 on the SAME writer must still return 200 and drain cleanly without HOL blocking
            r_p2 = writer.client().ingest({"events": make_events(3, event_name="f28_p2_ok", base_ts_ms=now_ms - 1000)}, api_key=k2)
            self.assertEqual(r_p2.status, 200)
            wait_until(
                lambda: _insights_total(p2_owner.client(), p2, "f28_p2_ok", now_ms - 60_000, now_ms + 60_000) == 3.0,
                timeout=15.0,
            )

    @tier(2)
    @feature("F28")
    @require_milestone("W3")
    def test_mixed_batch_sync_409_or_503_aborts_writer_log_queueing(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F28 Mixed Abort")
            now_ms = int(time.time() * 1000)

            writer.client().identify({"anon_id": "a_ma1", "user_id": "u_ma1"}, api_key=api_key).raise_for_status()
            writer.client().identify({"anon_id": "a_ma2", "user_id": "u_ma2"}, api_key=api_key).raise_for_status()

            r_409 = writer.client().ingest(
                {
                    "events": [
                        {"type": "identify", "userId": "u_ma1", "anonymousId": "a_ma2"},
                        make_event(event_name="f28_no_q_409", distinct_id="u_ma1", ts_ms=now_ms - 1000),
                    ]
                },
                api_key=api_key,
            )
            self.assertEqual(r_409.status, 409)

            storage.stop(graceful=True)
            before_files = list(writer.local_files(f"writer_log/{pid}/"))
            r_503 = writer.client().ingest(
                {
                    "events": [
                        {"type": "identify", "userId": "u_ma3", "anonymousId": "a_ma3"},
                        make_event(event_name="f28_no_q_503", distinct_id="u_ma3", ts_ms=now_ms - 500),
                    ]
                },
                api_key=api_key,
            )
            self.assertEqual(r_503.status, 503)
            time.sleep(0.1)
            self.assertEqual(writer.local_files(f"writer_log/{pid}/"), before_files)

            storage = storage.restart()
            assert isinstance(storage, NodeInstance)
            time.sleep(0.5)
            self.assertEqual(_insights_total(storage.client(), pid, "f28_no_q_409", now_ms - 60_000, now_ms + 60_000), 0.0)
            self.assertEqual(_insights_total(storage.client(), pid, "f28_no_q_503", now_ms - 60_000, now_ms + 60_000), 0.0)

    @tier(2)
    @feature("F28")
    @require_milestone("W3")
    def test_mixed_batch_checks_capacity_before_forwarding_sync_identify(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(
                num_writers=1,
                num_storage=1,
                database_url=db_url,
                extra_env={"PULSE_WRITER_LOG_PROJECT_MAX_BYTES": "32768"},
            )
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F28 Cap Before Sync")
            now_ms = int(time.time() * 1000)

            storage.stop(graceful=True)
            for i in range(60):
                r = writer.client().ingest(
                    {"events": make_events(5, event_name="f28_fill2", props={"pad": "y" * 2048}, base_ts_ms=now_ms + i)},
                    api_key=api_key,
                )
                if r.status == 429:
                    break
                time.sleep(0.02)

            # Restart storage so sync identify WOULD succeed if attempted, while writer_log is still full at 429
            # Wait: check capacity before sync identify; send immediately while still at cap (or while storage paused)
            r_mix = writer.client().ingest(
                {
                    "events": [
                        {"type": "identify", "userId": "u_cap_never", "anonymousId": "a_cap_never"},
                        make_event(event_name="f28_cap_never", distinct_id="u_cap_never", ts_ms=now_ms),
                    ]
                },
                api_key=api_key,
            )
            self.assertEqual(r_mix.status, 429)

            storage = storage.restart()
            assert isinstance(storage, NodeInstance)
            u_res = storage.client().get_user(pid, "u_cap_never")
            self.assertEqual(u_res.status, 200)
            u_body = u_res.json()
            self.assertIsNone(u_body.get("profile"))
            self.assertEqual(u_body.get("canonical_uid", 0), 0)

    @tier(2)
    @feature("F28")
    @require_milestone("W3")
    def test_wiped_writer_disk_creates_new_writer_id_without_dedup_drop(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F28 Wipe Disk")
            now_ms = int(time.time() * 1000)

            writer.client().ingest({"events": make_events(5, event_name="f28_wipe", base_ts_ms=now_ms - 5000)}, api_key=api_key).raise_for_status()
            wait_until(lambda: _insights_total(storage.client(), pid, "f28_wipe", now_ms - 60_000, now_ms + 60_000) == 5.0, timeout=15.0)
            old_wid = _find_writer_id(writer.data_dir)
            self.assertIsNotNone(old_wid)

            writer.stop(graceful=True)
            writer.rm_local("writer_log")
            writer = writer.restart(graceful_stop=True)
            assert isinstance(writer, NodeInstance)

            writer.client().ingest(
                {"events": make_events(3, event_name="f28_wipe", distinct_id_prefix="u_postwipe", base_ts_ms=now_ms - 1000)},
                api_key=api_key,
            ).raise_for_status()
            wait_until(lambda: _find_writer_id(writer.data_dir) is not None, timeout=10.0)
            new_wid = _find_writer_id(writer.data_dir)
            self.assertNotEqual(new_wid, old_wid)

            wait_until(
                lambda: _insights_total(storage.client(), pid, "f28_wipe", now_ms - 60_000, now_ms + 60_000) == 8.0,
                timeout=15.0,
                desc="events from wiped writer at offset 0 accepted by storage",
            )

    @tier(2)
    @feature("F28")
    @require_milestone("W3")
    def test_stale_writer_log_6d_max_age_and_unwritable_writer_log_503(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            # Test 1: Max batch age (< 7d expiry) via env override if available, plus unwritable writer_log -> 503
            topo = cluster.start_w3_cluster(
                num_writers=1,
                num_storage=1,
                database_url=db_url,
                extra_env={"PULSE_WRITER_LOG_MAX_AGE_SECS": "1"},
            )
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F28 Age & 503")
            now_ms = int(time.time() * 1000)

            storage.stop(graceful=True)
            writer.client().ingest(
                {"events": make_events(3, event_name="f28_expire", base_ts_ms=now_ms - 2000)},
                api_key=api_key,
            ).raise_for_status()
            time.sleep(1.5)
            storage = storage.restart()
            assert isinstance(storage, NodeInstance)
            time.sleep(0.5)
            # Expired batch (> max_age) must be dropped rather than forwarded
            self.assertEqual(_insights_total(storage.client(), pid, "f28_expire", now_ms - 60_000, now_ms + 60_000), 0.0)

            # Test 2: Unwritable writer_log/ -> 503 on fresh project (no pre-opened FD)
            _, api_key2 = _create_project(storage.client(), "F28 ReadOnly WriterLog")
            wlog = writer.data_dir / "writer_log"
            wlog.mkdir(parents=True, exist_ok=True)
            _chmod_tree(wlog, 0o555, 0o444)
            self.addCleanup(_chmod_tree, wlog, 0o755, 0o644)
            saw_503 = False
            for i in range(5):
                r = writer.client().ingest({"events": [make_event(event_name="f28_ro", ts_ms=now_ms + i)]}, api_key=api_key2)
                if r.status == 503:
                    saw_503 = True
                    break
                time.sleep(0.05)
            self.assertTrue(saw_503, "Expected 503 when writer_log/ is unwritable")


class TestTier2F29ForwarderAndOffsetBoundaries(unittest.TestCase):
    """Tier 2 boundary/negative tests for F29: Forwarder & (writer_id, bucket) offsets (W3)."""

    @tier(2)
    @feature("F29")
    @require_milestone("W3")
    def test_kill_writer_mid_forward_resends_with_zero_duplicates(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(
                num_writers=1,
                num_storage=1,
                database_url=db_url,
                extra_env={"PULSE_WRITER_FLUSH_MS": "20", "PULSE_WRITER_FLUSH_BYTES": "1048576"},
            )
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F29 Mid Kill")
            now_ms = int(time.time() * 1000)

            total = 200
            storage.stop(graceful=True)
            for b in range(0, 120, 20):
                evs = [
                    make_event(
                        event_name="f29_mk",
                        distinct_id=f"u_mk_{i % 10}",
                        ts_ms=now_ms - 10_000 + i,
                        props={"seq": i},
                        event_uuid=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"f29mk:{i}")),
                    )
                    for i in range(b, b + 20)
                ]
                writer.client().ingest({"events": evs}, api_key=api_key).raise_for_status()
            time.sleep(0.1)  # Ensure first 120 events are fsynced to writer_log before restart/kill
            storage = storage.restart()
            assert isinstance(storage, NodeInstance)
            time.sleep(0.02)
            writer.kill()
            writer = writer.restart(graceful_stop=False)
            assert isinstance(writer, NodeInstance)

            for b in range(120, total, 20):
                evs = [
                    make_event(
                        event_name="f29_mk",
                        distinct_id=f"u_mk_{i % 10}",
                        ts_ms=now_ms - 10_000 + i,
                        props={"seq": i},
                        event_uuid=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"f29mk:{i}")),
                    )
                    for i in range(b, b + 20)
                ]
                writer.client().ingest({"events": evs}, api_key=api_key).raise_for_status()

            wait_until(
                lambda: _insights_total(storage.client(), pid, "f29_mk", now_ms - 60_000, now_ms + 60_000) == 200.0,
                timeout=25.0,
            )
            # Every recovered event must exist exactly once (no duplicates, exact 200 rows)
            exp = storage.client().export_events(pid, format="json").raise_for_status().json()
            self.assertEqual(len(exp["events"]), 200)
            seqs = [e["props"]["seq"] for e in exp["events"]]
            self.assertEqual(sorted(seqs), list(range(200)), "Missing or duplicate seqs after writer mid-forward SIGKILL")
            keys = [(e["event_ts_ms"], e["distinct_id"]) for e in exp["events"]]
            self.assertEqual(len(keys), len(set(keys)), "Duplicate (event_ts_ms, distinct_id) found after writer mid-forward SIGKILL")

    @tier(2)
    @feature("F29")
    @require_milestone("W3")
    def test_apply_flush_wal_purge_restart_and_resend_drops_repeats_via_manifest(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F29 Purge Resend")
            now_ms = int(time.time() * 1000)

            # Buffer batch in writer_log while storage is stopped, snapshot writer_log dir
            storage.stop(graceful=True)
            writer.client().ingest(
                {"events": make_events(12, event_name="f29_pr", base_ts_ms=now_ms - 4000, seed_prefix="f29pr")},
                api_key=api_key,
            ).raise_for_status()
            wait_until(lambda: any(f"writer_log/{pid}/" in p for p in writer.local_files()), timeout=10.0)
            writer.stop(graceful=True)
            snap_dir = cluster.node_mgr.make_temp_data_dir("wlog_snap_")
            shutil.copytree(writer.data_dir / "writer_log", snap_dir / "writer_log")

            # Start storage + writer, let batch apply, then admin_flush (purging WAL!)
            storage = storage.restart()
            writer = writer.restart(graceful_stop=True)
            assert isinstance(storage, NodeInstance) and isinstance(writer, NodeInstance)
            wait_until(lambda: _insights_total(storage.client(), pid, "f29_pr", now_ms - 60_000, now_ms + 60_000) == 12.0, timeout=20.0)
            storage.client().admin_flush().raise_for_status()

            # Restore pre-ack writer_log snapshot and restart both -> resend must be deduped via Manifest.writer_offsets
            writer.stop(graceful=True)
            shutil.rmtree(writer.data_dir / "writer_log")
            shutil.copytree(snap_dir / "writer_log", writer.data_dir / "writer_log")
            storage = storage.restart(graceful_stop=True)
            writer = writer.restart(graceful_stop=True)
            assert isinstance(storage, NodeInstance) and isinstance(writer, NodeInstance)

            time.sleep(1.5)
            self.assertEqual(
                _insights_total(storage.client(), pid, "f29_pr", now_ms - 60_000, now_ms + 60_000),
                12.0,
            )

    @tier(2)
    @feature("F29")
    @require_milestone("W3")
    def test_partial_bucket_crash_replays_only_missing_buckets(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F29 Partial Bucket")
            now_ms = int(time.time() * 1000)

            # Generate events across 16 distinct users so multiple buckets are hit
            storage.stop(graceful=True)
            evs = make_events(16, event_name="f29_pb", distinct_id_prefix="u_pb", base_ts_ms=now_ms - 3000, seed_prefix="f29pb")
            writer.client().ingest({"events": evs}, api_key=api_key).raise_for_status()
            wait_until(lambda: any(f"writer_log/{pid}/" in p for p in writer.local_files()), timeout=10.0)
            writer.stop(graceful=True)
            snap_dir = cluster.node_mgr.make_temp_data_dir("wlog_pb_")
            shutil.copytree(writer.data_dir / "writer_log", snap_dir / "writer_log")

            storage = storage.restart()
            writer = writer.restart(graceful_stop=True)
            assert isinstance(storage, NodeInstance) and isinstance(writer, NodeInstance)
            wait_until(lambda: _insights_total(storage.client(), pid, "f29_pb", now_ms - 60_000, now_ms + 60_000) == 16.0, timeout=20.0)

            # Simulate 1 bucket missing before flush by replaying snapshot over existing WAL/manifest
            writer.stop(graceful=True)
            shutil.rmtree(writer.data_dir / "writer_log")
            shutil.copytree(snap_dir / "writer_log", writer.data_dir / "writer_log")
            writer = writer.restart(graceful_stop=True)
            assert isinstance(writer, NodeInstance)
            time.sleep(1.5)
            self.assertEqual(_insights_total(storage.client(), pid, "f29_pb", now_ms - 60_000, now_ms + 60_000), 16.0)

    @tier(2)
    @feature("F29")
    @require_milestone("W3")
    def test_storage_down_holds_cursor_and_drains_on_recovery(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F29 Hold Cursor")
            now_ms = int(time.time() * 1000)

            storage.stop(graceful=True)
            writer.client().ingest(
                {"events": make_events(25, event_name="f29_hold", base_ts_ms=now_ms - 3000)},
                api_key=api_key,
            ).raise_for_status()
            time.sleep(0.3)
            self.assertTrue(any(f"writer_log/{pid}/" in p for p in writer.local_files()))

            storage = storage.restart()
            assert isinstance(storage, NodeInstance)
            wait_until(
                lambda: _insights_total(storage.client(), pid, "f29_hold", now_ms - 60_000, now_ms + 60_000) == 25.0,
                timeout=20.0,
            )

    @tier(2)
    @feature("F29")
    @require_milestone("W3")
    def test_restore_from_published_carries_writer_offsets_and_skips_pre_publish_resend(self) -> None:
        pg_url = skip_if_no_postgres()
        with ClusterHarness(use_mock_s3=True) as cluster:
            assert cluster.mock_s3 is not None
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=pg_url, upload_interval_secs=1)
            s0 = topo["storages"][0]
            writer = topo["writers"][0]
            s0_port = s0.port
            pid, api_key = _create_project(s0.client(), "F29 Restore Offsets")
            now_ms = int(time.time() * 1000)

            # Buffer pre-publish batch and snapshot writer_log so we can force a post-restore resend
            s0.stop(graceful=True)
            writer.client().ingest(
                {"events": make_events(10, event_name="f29_rst", base_ts_ms=now_ms - 5000, seed_prefix="f29rst1")},
                api_key=api_key,
            ).raise_for_status()
            wait_until(lambda: any(f"writer_log/{pid}/" in p for p in writer.local_files()), timeout=10.0)
            writer.stop(graceful=True)
            snap_dir = cluster.node_mgr.make_temp_data_dir("wlog_rst_snap_")
            shutil.copytree(writer.data_dir / "writer_log", snap_dir / "writer_log")

            s0 = s0.restart()
            writer = writer.restart(graceful_stop=True)
            assert isinstance(s0, NodeInstance) and isinstance(writer, NodeInstance)
            wait_until(lambda: _insights_total(s0.client(), pid, "f29_rst", now_ms - 60_000, now_ms + 60_000) == 10.0, timeout=20.0)
            s0.client().admin_flush().raise_for_status()
            wait_until(lambda: any(k.startswith(f"published/{pid}/") for k in cluster.mock_s3.list_objects()), timeout=15.0)
            s0.stop(graceful=True)
            writer.stop(graceful=True)

            # Restore s0 volume from published into fresh dir (same storage-0 owner)
            restored_dir = cluster.node_mgr.make_temp_data_dir("f29_restored_")
            res = cluster.node_mgr.run_cli(
                ["restore", "--from-published"],
                data_dir=restored_dir,
                env={**cluster.mock_s3.env_vars(), "DATABASE_URL": pg_url},
            )
            self.assertEqual(res.returncode, 0, res.stderr)

            # Replay pre-publish writer_log snapshot onto writer; restored writer_offsets must skip it
            shutil.rmtree(writer.data_dir / "writer_log")
            shutil.copytree(snap_dir / "writer_log", writer.data_dir / "writer_log")

            s0_restored = cluster.start_node(
                role="all",
                port=s0_port,
                data_dir=restored_dir,
                database_url=pg_url,
                hot_store="local",
                node_name="storage-0",
                upload_interval_secs=1,
            )
            writer = writer.restart(graceful_stop=True)
            assert isinstance(s0_restored, NodeInstance) and isinstance(writer, NodeInstance)

            writer.client().ingest(
                {"events": make_events(5, event_name="f29_rst", distinct_id_prefix="u_r2", base_ts_ms=now_ms - 1000, seed_prefix="f29rst2")},
                api_key=api_key,
            ).raise_for_status()
            wait_until(
                lambda: _insights_total(s0_restored.client(), pid, "f29_rst", now_ms - 60_000, now_ms + 60_000) >= 15.0,
                timeout=20.0,
            )
            time.sleep(1.0)
            self.assertEqual(
                _insights_total(s0_restored.client(), pid, "f29_rst", now_ms - 60_000, now_ms + 60_000),
                15.0,
            )


class TestTier2F30ProjectOwnerBoundaries(unittest.TestCase):
    """Tier 2 boundary/negative tests for F30: project_owner & non-owner guards (W3)."""

    @tier(2)
    @feature("F30")
    @require_milestone("W3")
    def test_non_owner_storage_node_never_acquires_lease_or_opens_owner_stores(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=2, database_url=db_url, writer_lease_ttl_secs=3)
            s1, s2 = topo["storages"]
            writer = topo["writers"][0]
            pid, api_key = _create_project(s1.client(), "F30 Lease Guard")

            writer.client().identify({"anon_id": "a_lg", "user_id": "u_lg", "user_properties": {"ok": True}}, api_key=api_key).raise_for_status()
            # Exercise non-owner reads/calls on peer node
            s2.client().list_users(pid).raise_for_status()
            s1.client().list_users(pid).raise_for_status()
            time.sleep(1.0)

            owner_name = s1.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (pid,), db_path=db_path)[0]["node_name"]
            non_owner = s2 if owner_name == "storage-0" else s1
            # Non-owner must never open identity/ or profiles/ SlateDB locally
            self.assertFalse(
                any(f"identity/{pid}" in p or f"profiles/{pid}" in p for p in non_owner.local_files()),
                f"Non-owner opened project SlateDB locally: {non_owner.local_files()}",
            )

    @tier(2)
    @feature("F30")
    @require_milestone("W3")
    def test_owner_down_does_not_trigger_automatic_takeover_after_lease_expiry(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=2, database_url=db_url, writer_lease_ttl_secs=3)
            s1, s2 = topo["storages"]
            pid, api_key = _create_project(s1.client(), "F30 No Auto Takeover")

            s1.client().ingest({"events": [make_event(event_name="f30_nt")]}, api_key=api_key).raise_for_status()
            owner_row = s1.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (pid,), db_path=db_path)[0]
            owner_name = owner_row["node_name"]
            owner_node = s1 if owner_name == "storage-0" else s2
            peer_node = s2 if owner_name == "storage-0" else s1

            owner_node.stop(graceful=True)
            time.sleep(4.5)  # Exceed 3s writer_lease_ttl_secs

            after_owner = peer_node.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (pid,), db_path=db_path)[0]
            self.assertEqual(after_owner["node_name"], owner_name)
            # Peer node must NOT have acquired writer_leases for pid
            self.assertFalse(any(f"manifests/{pid}" in p or f"identity/{pid}" in p for p in peer_node.local_files()))

    @tier(2)
    @feature("F30")
    @require_milestone("W3")
    def test_non_owner_background_loops_skip_unowned_projects(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(
                num_writers=0,
                num_storage=2,
                database_url=db_url,
                extra_env={"PULSE_ALERT_EVAL_INTERVAL_SECS": "1"},
            )
            s1, s2 = topo["storages"]
            pid, api_key = _create_project(s1.client(), "F30 Loop Scope")

            s1.client().ingest({"events": [make_event(event_name="f30_loop")]}, api_key=api_key).raise_for_status()
            time.sleep(2.5)  # Allow several background ticks

            owner_name = s1.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (pid,), db_path=db_path)[0]["node_name"]
            non_owner = s2 if owner_name == "storage-0" else s1
            self.assertEqual([p for p in non_owner.local_files() if pid in p], [])

    @tier(2)
    @feature("F30")
    @require_milestone("W3")
    def test_restarted_deposed_owner_after_manual_failover_is_refused(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            assert cluster.mock_s3 is not None
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=2, database_url=db_url, upload_interval_secs=1)
            s1, s2 = topo["storages"]
            pid, api_key = _create_project(s1.client(), "F30 Deposed Guard")
            now_ms = int(time.time() * 1000)

            orig_name = s1.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (pid,), db_path=db_path)[0]["node_name"]
            orig_node = s1 if orig_name == "storage-0" else s2
            new_node = s2 if orig_name == "storage-0" else s1
            new_name = "storage-1" if orig_name == "storage-0" else "storage-0"

            orig_node.client().ingest({"events": [make_event(event_name="f30_dep", ts_ms=now_ms - 2000)]}, api_key=api_key).raise_for_status()
            wait_until(lambda: _insights_total(orig_node.client(), pid, "f30_dep", now_ms - 60_000, now_ms + 60_000) == 1.0, timeout=15.0)
            orig_node.client().admin_flush().raise_for_status()
            wait_until(lambda: any(k.startswith(f"published/{pid}/") for k in cluster.mock_s3.list_objects()), timeout=15.0)
            orig_node.stop(graceful=True)

            _transfer_project_for_failover(cluster, orig_node, new_node, pid, db_url)
            new_node.sqlite_query(
                "UPDATE project_owner SET node_name = ?, address = ? WHERE project_id = ?",
                (new_name, new_node.base_url, pid),
                db_path=db_path,
                readonly=False,
            )

            # Restart deposed original owner; it must NOT reclaim lease or fence new_node
            orig_restarted = orig_node.restart()
            assert isinstance(orig_restarted, NodeInstance)
            r_new = new_node.client().identify({"anon_id": "a_new_own", "user_id": "u_new_own"}, api_key=api_key)
            self.assertEqual(r_new.status, 200)

    @tier(2)
    @feature("F30")
    @require_milestone("W3")
    def test_missing_project_owner_row_returns_explicit_error_not_split_brain(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F30 Missing Owner")

            storage.sqlite_query("DELETE FROM project_owner WHERE project_id = ?", (pid,), db_path=db_path, readonly=False)
            r = writer.client().identify({"anon_id": "a_no_own", "user_id": "u_no_own"}, api_key=api_key)
            self.assertIn(r.status, (404, 500, 503), f"Expected error when project_owner row missing, got {r.status}")


class TestTier2F31ForwardingBoundaries(unittest.TestCase):
    """Tier 2 boundary/negative tests for F31: Forwarding/routing of profile/query/agent (W3)."""

    @tier(2)
    @feature("F31")
    @require_milestone("W3")
    def test_sync_profile_calls_return_503_when_owner_is_down(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=2, database_url=db_url)
            s1, s2 = topo["storages"]
            writer = topo["writers"][0]
            p1, k1 = _create_project(s1.client(), "F31 Owner Down Sync")
            p2, k2 = _create_project(s1.client(), "F31 Peer Up Sync")

            o1 = s1.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (p1,), db_path=db_path)[0]["node_name"]
            o2 = s1.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (p2,), db_path=db_path)[0]["node_name"]
            if o1 == o2:
                other_s = s2 if o1 == "storage-0" else s1
                other_name = "storage-1" if o1 == "storage-0" else "storage-0"
                s1.sqlite_query("UPDATE project_owner SET node_name = ?, address = ? WHERE project_id = ?", (other_name, other_s.base_url, p2), db_path=db_path, readonly=False)
            p1_owner = s1 if o1 == "storage-0" else s2

            p1_owner.stop(graceful=True)
            wc = writer.client()
            self.assertEqual(wc.identify({"anon_id": "a_503", "user_id": "u_503"}, api_key=k1).status, 503)
            self.assertEqual(wc.engage({"distinct_id": "u_503", "$set": {"k": 1}}, api_key=k1).status, 503)
            self.assertEqual(wc.groups({"group_type": "org", "group_id": "g_503", "$set": {"k": 1}}, api_key=k1).status, 503)
            self.assertEqual(wc.replay_chunk(make_replay_chunk(session_id="rs_503", project_id=p1), api_key=k1).status, 503)

            # P2 owned by healthy storage node must still return 200 through the same writer
            self.assertEqual(wc.identify({"anon_id": "a_p2_ok", "user_id": "u_p2_ok"}, api_key=k2).status, 200)

    @tier(2)
    @feature("F31")
    @require_milestone("W3")
    def test_queries_return_503_when_owner_is_down(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=2, database_url=db_url)
            s1, s2 = topo["storages"]
            writer = topo["writers"][0]
            p1, _ = _create_project(s1.client(), "F31 Query 503")
            p2, _ = _create_project(s1.client(), "F31 Query P2")
            now_ms = int(time.time() * 1000)

            o1 = s1.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (p1,), db_path=db_path)[0]["node_name"]
            p1_owner = s1 if o1 == "storage-0" else s2
            healthy_peer = s2 if o1 == "storage-0" else s1

            p1_owner.stop(graceful=True)
            q_payload = {
                "time_range": {"start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000},
                "interval": "day",
                "series": [{"label": "c", "event_name": "x", "metric": "totals"}],
            }
            self.assertEqual(healthy_peer.client().query_insights(p1, q_payload).status, 503)
            self.assertEqual(writer.client().query_insights(p1, q_payload).status, 503)
            self.assertEqual(healthy_peer.client().list_users(p1).status, 503)
            self.assertEqual(healthy_peer.client().list_agent_traces(p1).status, 503)
            self.assertEqual(healthy_peer.client().query_insights(p2, q_payload).status, 200)

    @tier(2)
    @feature("F31")
    @require_milestone("W3")
    def test_async_ingest_and_agent_traces_buffer_during_owner_outage_then_drain(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F31 Outage Buffer")
            now_ms = int(time.time() * 1000)

            storage.stop(graceful=True)
            r_ev = writer.client().ingest({"events": make_events(5, event_name="f31_buf", base_ts_ms=now_ms - 2000)}, api_key=api_key)
            r_ag = writer.client().ingest_agent_traces(make_agent_trace(trace_id="tr_f31_buf", start_ts_ms=now_ms - 1000), api_key=api_key)
            self.assertEqual(r_ev.status, 200)
            self.assertEqual(r_ag.status, 200)

            storage = storage.restart()
            assert isinstance(storage, NodeInstance)
            wait_until(
                lambda: _insights_total(storage.client(), pid, "f31_buf", now_ms - 60_000, now_ms + 60_000) == 5.0,
                timeout=20.0,
            )
            wait_until(
                lambda: any(t["trace_id"] == "tr_f31_buf" for t in storage.client().list_agent_traces(pid).raise_for_status().json().get("traces", [])),
                timeout=20.0,
            )

    @tier(2)
    @feature("F31")
    @require_milestone("W3")
    def test_profile_conflict_409_propagates_cleanly_through_writer_proxy(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=2, num_storage=2, database_url=db_url)
            s1 = topo["storages"][0]
            w1, w2 = topo["writers"]
            _, api_key = _create_project(s1.client(), "F31 409 Proxy")

            w1.client().identify({"anon_id": "a_px1", "user_id": "u_px1"}, api_key=api_key).raise_for_status()
            w1.client().identify({"anon_id": "a_px2", "user_id": "u_px2"}, api_key=api_key).raise_for_status()

            r = w2.client().identify({"anon_id": "a_px1", "user_id": "u_px2"}, api_key=api_key)
            self.assertEqual(r.status, 409)

    @tier(2)
    @feature("F31")
    @require_milestone("W3")
    def test_stale_cached_project_owner_refreshes_on_not_owner_response(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            assert cluster.mock_s3 is not None
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=2, database_url=db_url, upload_interval_secs=1)
            s1, s2 = topo["storages"]
            writer = topo["writers"][0]
            pid, api_key = _create_project(s1.client(), "F31 Stale Cache")
            now_ms = int(time.time() * 1000)

            orig = s1.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (pid,), db_path=db_path)[0]["node_name"]
            orig_node = s1 if orig == "storage-0" else s2
            new_node = s2 if orig == "storage-0" else s1
            new_name = "storage-1" if orig == "storage-0" else "storage-0"

            # Warm writer's project_owner cache on orig_node
            writer.client().identify({"anon_id": "a_warm", "user_id": "u_warm"}, api_key=api_key).raise_for_status()
            orig_node.client().admin_flush().raise_for_status()
            wait_until(lambda: any(k.startswith(f"published/{pid}/") for k in cluster.mock_s3.list_objects()), timeout=15.0)
            orig_node.stop(graceful=True)

            _transfer_project_for_failover(cluster, orig_node, new_node, pid, db_url)
            new_node.sqlite_query(
                "UPDATE project_owner SET node_name = ?, address = ? WHERE project_id = ?",
                (new_name, new_node.base_url, pid),
                db_path=db_path,
                readonly=False,
            )
            # Bring orig_node back up so it responds NotOwner (not connection refused)
            orig_node = orig_node.restart()
            assert isinstance(orig_node, NodeInstance)

            # Writer's cached owner still points to orig_node; NotOwner must trigger cache refresh + retry on new_node
            r = writer.client().identify({"anon_id": "a_post_mv", "user_id": "u_post_mv"}, api_key=api_key)
            self.assertEqual(r.status, 200, f"Expected transparent retry on NotOwner, got {r.status}: {r.text()}")
            self.assertEqual(new_node.client().get_user(pid, "u_post_mv").status, 200)


if __name__ == "__main__":
    unittest.main()
