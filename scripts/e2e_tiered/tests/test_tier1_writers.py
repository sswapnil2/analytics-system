"""Tier 1 (happy-path) opaque-box E2E tests for Log-First Writers (F22-F25, F28-F31).

Covers:
  - W1: F22 (async event ack), F23 (dead-letter reason & CLI), F24 (observability & alerts)
  - W2: F25 (sync profile endpoint returning ID & error propagation)
  - W3: F28 (stateless writer & writer_log/), F29 (forwarder & (writer_id, bucket) offsets),
        F30 (project_owner placement & guards), F31 (forwarding/routing of profile/query/agent)

Exactly 5 tests per feature x 8 features = 40 Tier 1 tests.
"""

from __future__ import annotations

import http.server
import shutil
import sys
import threading
import time
import unittest
import uuid
from pathlib import Path
from typing import Any

_HARNESS_ROOT = Path(__file__).resolve().parent.parent
if str(_HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(_HARNESS_ROOT))

from lib.client import HttpStatusError, MetricsParser, PulseHttpClient  # noqa: E402
from lib.node_mgr import ClusterHarness, NodeInstance, free_port  # noqa: E402
from lib.oracle import (  # noqa: E402
    decode_manifest,
    external_postgres_url,
    feature,
    make_agent_trace,
    make_event,
    make_events,
    make_replay_chunk,
    random_byok_hex,
    require_milestone,
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
    shared_dir = cluster.node_mgr.make_temp_data_dir("pulse_w3_t1_meta_")
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
    for prefix in ("hot/identity", "hot/profiles", "hot/manifests", "hot/hot/segments"):
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
    project_id: str,
    count: int,
    base_ts_ms: int,
    *,
    prefix: str = "dict_ev",
) -> None:
    batch_size = 100
    for start in range(0, count, batch_size):
        end = min(count, start + batch_size)
        evs = [
            make_event(
                event_name=f"{prefix}_{i:05d}",
                distinct_id=f"u_dict_{i % 16}",
                ts_ms=base_ts_ms + i,
            )
            for i in range(start, end)
        ]
        client.ingest({"events": evs}, api_key=api_key).raise_for_status()

    def _lag_drained() -> bool:
        m = client.parsed_metrics()
        return (
            m.sum_by_name("pulse_writer_buffer_events") == 0.0
            and m.sum_by_name("pulse_ingest_log_lag_bytes") == 0.0
        )

    wait_until(_lag_drained, timeout=30.0, interval=0.1, desc="dictionary fill drain")


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


def _start_webhook_server() -> tuple[http.server.ThreadingHTTPServer, list[dict[str, Any]], int]:
    received: list[dict[str, Any]] = []
    lock = threading.Lock()

    class _Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_POST(self) -> None:
            n = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(n) if n > 0 else b""
            with lock:
                received.append(
                    {
                        "path": self.path,
                        "body": raw.decode("utf-8", errors="replace"),
                    }
                )
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

    port = free_port()
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", port), _Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv, received, port


class TestTier1F22AsyncEventAck(unittest.TestCase):
    """Tier 1 tests for F22: Single-node async event ack (W1)."""

    @tier(1)
    @feature("F22")
    @require_milestone("W1")
    def test_async_ingest_json_returns_200_deduped_zero_and_dedup_on_apply(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F22 Async Ack")

            now_ms = int(time.time() * 1000)
            evs = make_events(6, event_name="f22_click", base_ts_ms=now_ms - 5000, seed_prefix="f22_r1")
            r1 = client.ingest({"events": evs}, api_key=api_key).raise_for_status().json()
            self.assertEqual(r1["status"], "ok")
            self.assertEqual(r1["accepted"], 6)
            self.assertEqual(r1["deduped"], 0)
            self.assertGreater(r1["server_ts_ms"], 0)

            # Retry the exact same batch; async ack still reports deduped == 0
            r2 = client.ingest({"events": evs}, api_key=api_key).raise_for_status().json()
            self.assertEqual(r2["accepted"], 6)
            self.assertEqual(r2["deduped"], 0)

            wait_until(
                lambda: _insights_total(client, pid, "f22_click", now_ms - 60_000, now_ms + 60_000) == 6.0,
                timeout=15.0,
                desc="f22 async events applied once",
            )
            metrics = client.parsed_metrics()
            self.assertGreaterEqual(metrics.sum_by_name("pulse_ingest_events_deduped_total"), 6.0)

    @tier(1)
    @feature("F22")
    @require_milestone("W1")
    def test_async_ingest_accepts_plsb_and_all_json_envelope_shapes(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F22 Envelopes")
            now_ms = int(time.time() * 1000)

            r_plsb = client.ingest_plsb(
                make_events(3, event_name="f22_env", distinct_id_prefix="u_plsb", base_ts_ms=now_ms - 4000),
                api_key=api_key,
            ).raise_for_status().json()
            self.assertEqual(r_plsb["accepted"], 3)
            self.assertEqual(r_plsb["deduped"], 0)

            r_batch = client.ingest(
                {"batch": make_events(2, event_name="f22_env", distinct_id_prefix="u_seg", base_ts_ms=now_ms - 3000)},
                api_key=api_key,
            ).raise_for_status().json()
            self.assertEqual(r_batch["accepted"], 2)
            self.assertEqual(r_batch["deduped"], 0)

            r_arr = client.ingest(
                make_events(2, event_name="f22_env", distinct_id_prefix="u_arr", base_ts_ms=now_ms - 2000),
                api_key=api_key,
            ).raise_for_status().json()
            self.assertEqual(r_arr["accepted"], 2)
            self.assertEqual(r_arr["deduped"], 0)

            r_single = client.ingest(
                make_event(event_name="f22_env", distinct_id="u_single", ts_ms=now_ms - 1000),
                api_key=api_key,
            ).raise_for_status().json()
            self.assertEqual(r_single["accepted"], 1)
            self.assertEqual(r_single["deduped"], 0)

            wait_until(
                lambda: _insights_total(client, pid, "f22_env", now_ms - 60_000, now_ms + 60_000) == 8.0,
                timeout=15.0,
                desc="all 8 envelope events queryable",
            )

    @tier(1)
    @feature("F22")
    @require_milestone("W1")
    def test_flush_bound_20ms_makes_acked_events_durable_across_sigkill(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(
                role="all",
                hot_store="local",
                writer_lease_ttl_secs=3,
                extra_env={"PULSE_WRITER_FLUSH_MS": "20", "PULSE_WRITER_FLUSH_BYTES": "1048576"},
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F22 Flush Bound")
            now_ms = int(time.time() * 1000)

            evs = make_events(5, event_name="f22_durable", base_ts_ms=now_ms - 2000)
            r = client.ingest({"events": evs}, api_key=api_key).raise_for_status().json()
            self.assertEqual(r["accepted"], 5)

            # Wait comfortably past the 20ms flush bound so group commit fsyncs WAL
            time.sleep(0.15)
            node.kill()

            restarted = node.restart(graceful_stop=False)
            assert isinstance(restarted, NodeInstance)
            r_client = restarted.client()

            def _durable_total_ready() -> bool:
                try:
                    return _insights_total(r_client, pid, "f22_durable", now_ms - 60_000, now_ms + 60_000) == 5.0
                except HttpStatusError:
                    return False

            wait_until(
                _durable_total_ready,
                timeout=15.0,
                desc="WAL-flushed events survive SIGKILL",
            )

    @tier(1)
    @feature("F22")
    @require_milestone("W1")
    def test_sync_bulk_import_keeps_exact_dedup_count(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, _ = _create_project(client, "F22 Sync Import")
            now_ms = int(time.time() * 1000)

            evs = make_events(4, event_name="f22_imp", base_ts_ms=now_ms - 4000, seed_prefix="f22_imp")
            imp1 = client.import_events(pid, {"events": evs}).raise_for_status().json()
            self.assertEqual(imp1["accepted"], 4)
            self.assertEqual(imp1["deduped"], 0)

            imp2 = client.import_events(pid, {"events": evs}).raise_for_status().json()
            self.assertEqual(imp2["accepted"], 0)
            self.assertEqual(imp2["deduped"], 4)
            self.assertEqual(_insights_total(client, pid, "f22_imp", now_ms - 60_000, now_ms + 60_000), 4.0)

    @tier(1)
    @feature("F22")
    @require_milestone("W1")
    def test_mixed_batch_runs_sync_identify_and_alias_before_buffering_events(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F22 Mixed Batch")
            now_ms = int(time.time() * 1000)

            batch = [
                {
                    "type": "identify",
                    "userId": "u_mixed_primary",
                    "anonymousId": "anon_mixed_1",
                    "traits": {"plan": "pro", "team": "core"},
                },
                {
                    "type": "alias",
                    "userId": "u_mixed_primary",
                    "previousId": "anon_mixed_2",
                },
                make_event(event_name="f22_mixed_ev", distinct_id="anon_mixed_1", ts_ms=now_ms - 1000),
                make_event(event_name="f22_mixed_ev", distinct_id="anon_mixed_2", ts_ms=now_ms - 500),
            ]
            resp = client.ingest({"events": batch}, api_key=api_key).raise_for_status().json()
            self.assertEqual(resp["status"], "ok")
            self.assertEqual(resp["accepted"], 2)
            self.assertEqual(resp["deduped"], 0)
            self.assertGreaterEqual(resp.get("profiles_updated", 0), 1)

            detail = client.get_user(pid, "u_mixed_primary").raise_for_status().json()
            self.assertEqual(detail["profile"]["props"]["plan"], "pro")
            wait_until(
                lambda: _insights_total(client, pid, "f22_mixed_ev", now_ms - 60_000, now_ms + 60_000) == 2.0,
                timeout=15.0,
                desc="mixed batch events applied",
            )


class TestTier1F23DeadLetterStorageAndCli(unittest.TestCase):
    """Tier 1 tests for F23: Dead-letter reason storage & CLI inspection (W1)."""

    @tier(1)
    @feature("F23")
    @require_milestone("W1")
    def test_dictionary_overflow_dead_letters_event_and_keeps_project_healthy(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F23 Dict Overflow")
            now_ms = int(time.time() * 1000)

            _fill_event_dictionary(client, api_key, pid, 8192, now_ms - 20_000)
            overflow = make_event(event_name="f23_overflow_8193", distinct_id="u_dl_1", ts_ms=now_ms - 1000)
            valid = make_event(event_name="dict_ev_00000", distinct_id="u_dl_ok", ts_ms=now_ms - 500)
            client.ingest({"events": [overflow, valid]}, api_key=api_key).raise_for_status()

            wait_until(
                lambda: client.parsed_metrics().sum_by_name("pulse_ingest_dead_lettered_events_total") >= 1.0,
                timeout=15.0,
                desc="dead letter metric incremented",
            )
            wait_until(
                lambda: _insights_total(client, pid, "dict_ev_00000", now_ms - 60_000, now_ms + 60_000) >= 2.0,
                timeout=15.0,
                desc="valid event still applied after dead-letter",
            )

    @tier(1)
    @feature("F23")
    @require_milestone("W1")
    def test_dead_letter_record_written_to_local_wal_dead_not_s3(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            assert cluster.mock_s3 is not None
            s3 = cluster.mock_s3
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F23 Local wal_dead")
            now_ms = int(time.time() * 1000)

            _fill_event_dictionary(client, api_key, pid, 8192, now_ms - 20_000, prefix="dl_loc")
            s3.reset_counts()

            client.ingest(
                {"events": [make_event(event_name="dl_loc_overflow", distinct_id="u_dl_loc", ts_ms=now_ms - 500)]},
                api_key=api_key,
            ).raise_for_status()

            wait_until(
                lambda: any("wal_dead/" in p for p in node.local_files()),
                timeout=15.0,
                desc="wal_dead file written locally",
            )
            self.assertEqual(s3.put_count("wal_dead/"), 0)
            self.assertTrue(any(pid in p for p in node.local_files() if "wal_dead/" in p))

    @tier(1)
    @feature("F23")
    @require_milestone("W1")
    def test_dead_letter_cli_lists_events_with_reason_and_identity(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            assert cluster.mock_s3 is not None
            subcmd = _find_dead_letter_subcmd(cluster)
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F23 CLI List")
            now_ms = int(time.time() * 1000)

            _fill_event_dictionary(client, api_key, pid, 8192, now_ms - 20_000, prefix="dl_cli")
            client.ingest(
                {"events": [make_event(event_name="dl_cli_overflow", distinct_id="u_cli_vis", ts_ms=now_ms - 500)]},
                api_key=api_key,
            ).raise_for_status()
            wait_until(
                lambda: client.parsed_metrics().sum_by_name("pulse_ingest_dead_lettered_events_total") >= 1.0,
                timeout=15.0,
                desc="dead letter stored before CLI",
            )
            node.stop(graceful=True)

            proc = cluster.node_mgr.run_cli(
                [subcmd, "--project-id", pid, "--data-dir", str(node.data_dir)],
                env=cluster.mock_s3.env_vars(),
            )
            self.assertEqual(proc.returncode, 0, f"CLI failed: stdout={proc.stdout}\nstderr={proc.stderr}")
            out = proc.stdout + "\n" + proc.stderr
            self.assertIn("dl_cli_overflow", out)
            self.assertIn("u_cli_vis", out)
            self.assertTrue(
                any(k in out.lower() for k in ("dictionary", "limit", "cap", "reason", "overflow")),
                f"Expected refusal reason in CLI output, got: {out}",
            )

    @tier(1)
    @feature("F23")
    @require_milestone("W1")
    def test_dead_letter_cli_omits_erased_users(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            assert cluster.mock_s3 is not None
            subcmd = _find_dead_letter_subcmd(cluster)
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F23 CLI Erase")
            now_ms = int(time.time() * 1000)

            # Ensure both users are interned in identity graph before erasure
            client.identify({"anon_id": "a_keep", "user_id": "u_dl_keep"}, api_key=api_key).raise_for_status()
            client.identify({"anon_id": "a_erase", "user_id": "u_dl_erase"}, api_key=api_key).raise_for_status()

            _fill_event_dictionary(client, api_key, pid, 8192, now_ms - 20_000, prefix="dl_er")
            client.ingest(
                {
                    "events": [
                        make_event(event_name="dl_er_ev_keep", distinct_id="u_dl_keep", ts_ms=now_ms - 1000),
                        make_event(event_name="dl_er_ev_gone", distinct_id="u_dl_erase", ts_ms=now_ms - 900),
                    ]
                },
                api_key=api_key,
            ).raise_for_status()
            wait_until(
                lambda: client.parsed_metrics().sum_by_name("pulse_ingest_dead_lettered_events_total") >= 2.0,
                timeout=15.0,
                desc="2 dead-lettered events recorded",
            )

            client.delete_user(pid, "u_dl_erase").raise_for_status()
            node.stop(graceful=True)

            proc = cluster.node_mgr.run_cli(
                [subcmd, "--project-id", pid, "--data-dir", str(node.data_dir)],
                env=cluster.mock_s3.env_vars(),
            )
            self.assertEqual(proc.returncode, 0, f"CLI failed: {proc.stderr}")
            self.assertIn("u_dl_keep", proc.stdout)
            self.assertNotIn("u_dl_erase", proc.stdout)
            self.assertNotIn("dl_er_ev_gone", proc.stdout)

    @tier(1)
    @feature("F23")
    @require_milestone("W1")
    def test_poisoned_record_does_not_block_subsequent_events_or_restart(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F23 No Poison Block")
            now_ms = int(time.time() * 1000)

            _fill_event_dictionary(client, api_key, pid, 8192, now_ms - 20_000, prefix="dl_nb")
            batch = [
                make_event(event_name="dl_nb_00001", distinct_id="u_ok_1", ts_ms=now_ms - 800),
                make_event(event_name="dl_nb_poison_8193", distinct_id="u_poison", ts_ms=now_ms - 700),
                make_event(event_name="dl_nb_00002", distinct_id="u_ok_2", ts_ms=now_ms - 600),
            ]
            client.ingest({"events": batch}, api_key=api_key).raise_for_status()

            wait_until(
                lambda: _insights_total(client, pid, "dl_nb_00002", now_ms - 60_000, now_ms + 60_000) >= 2.0,
                timeout=15.0,
                desc="post-poison event applied",
            )
            restarted = node.restart(graceful_stop=True)
            assert isinstance(restarted, NodeInstance)
            self.assertEqual(restarted.client().readyz().status, 200)
            self.assertGreaterEqual(
                _insights_total(restarted.client(), pid, "dl_nb_00002", now_ms - 60_000, now_ms + 60_000),
                2.0,
            )


class TestTier1F24IngestObservabilityAndAlerts(unittest.TestCase):
    """Tier 1 tests for F24: Ingest buffer/lag observability & alerts (W1)."""

    @tier(1)
    @feature("F24")
    @require_milestone("W1")
    def test_metrics_exports_ingest_buffer_flush_bound_and_lag_metrics(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            _, api_key = _create_project(client, "F24 Metrics")

            client.ingest({"events": make_events(3, event_name="f24_m")}, api_key=api_key).raise_for_status()
            resp = client.metrics().raise_for_status()
            self.assertIn("text/plain", resp.header("content-type", ""))
            parsed = MetricsParser.parse(resp.text())
            names = {n for (n, _) in parsed.keys()}

            for required in (
                "pulse_ingest_events_accepted_total",
                "pulse_ingest_events_deduped_total",
                "pulse_ingest_events_dropped_total",
                "pulse_ingest_dead_lettered_events_total",
                "pulse_ingest_batches_total",
                "pulse_ingest_log_lag_bytes",
                "pulse_l0_bytes",
                "pulse_l0_rows",
            ):
                self.assertIn(required, names, f"Missing metric {required}; present={sorted(names)}")

            # W1 also exports flush bound / buffer fill / lag seconds metrics
            self.assertTrue(
                any("flush" in n or "buffer" in n or "lag" in n for n in names if n.startswith("pulse_")),
                f"Expected flush/buffer/lag metrics in {sorted(names)}",
            )

    @tier(1)
    @feature("F24")
    @require_milestone("W1")
    def test_lag_and_l0_gauges_drain_back_to_zero_after_flush(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            _, api_key = _create_project(client, "F24 Drain Zero")

            client.ingest({"events": make_events(25, event_name="f24_drain")}, api_key=api_key).raise_for_status()
            wait_until(
                lambda: client.parsed_metrics().sum_by_name("pulse_ingest_log_lag_bytes") == 0.0,
                timeout=15.0,
                desc="log lag drained to 0",
            )
            client.admin_flush().raise_for_status()
            m = client.parsed_metrics()
            self.assertEqual(m.sum_by_name("pulse_ingest_log_lag_bytes"), 0.0)
            self.assertEqual(m.sum_by_name("pulse_l0_bytes"), 0.0)
            self.assertEqual(m.sum_by_name("pulse_l0_rows"), 0.0)

    @tier(1)
    @feature("F24")
    @require_milestone("W1")
    def test_metrics_accepted_deduped_and_batches_counters_reflect_load(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            _, api_key = _create_project(client, "F24 Counters")

            before = client.parsed_metrics()
            b_acc = before.sum_by_name("pulse_ingest_events_accepted_total")
            b_dup = before.sum_by_name("pulse_ingest_events_deduped_total")
            b_bat = before.sum_by_name("pulse_ingest_batches_total")

            evs = make_events(10, event_name="f24_cnt", seed_prefix="f24_cnt")
            client.ingest({"events": evs}, api_key=api_key).raise_for_status()
            client.ingest({"events": evs[:4]}, api_key=api_key).raise_for_status()

            def _updated() -> bool:
                now = client.parsed_metrics()
                return (
                    now.sum_by_name("pulse_ingest_events_accepted_total") >= b_acc + 10.0
                    and now.sum_by_name("pulse_ingest_events_deduped_total") >= b_dup + 4.0
                    and now.sum_by_name("pulse_ingest_batches_total") >= b_bat + 2.0
                )

            wait_until(_updated, timeout=15.0, desc="counters reflect accepted + deduped + batches")

    @tier(1)
    @feature("F24")
    @require_milestone("W1")
    def test_metrics_token_allows_authorized_bearer_request(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            token = "metrics_tok_t1_ok"
            node = cluster.start_node(role="all", hot_store="local", metrics_token=token)
            assert isinstance(node, NodeInstance)
            client = node.client()
            resp = client.metrics(token=token).raise_for_status()
            parsed = MetricsParser.parse(resp.text())
            self.assertEqual(parsed.get_metric("pulse_ready"), 1.0)
            self.assertTrue(any(n == "pulse_ingest_log_lag_bytes" for (n, _) in parsed.keys()))

    @tier(1)
    @feature("F24")
    @require_milestone("W1")
    def test_writer_log_and_forwarding_lag_metrics_track_stall_and_recovery(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage = topo["storages"][0]
            writer = topo["writers"][0]
            s_client = storage.client()
            w_client = writer.client()
            _, api_key = _create_project(s_client, "F24 Lag Stall")

            storage.stop(graceful=True)
            w_client.ingest({"events": make_events(20, event_name="f24_lag")}, api_key=api_key).raise_for_status()
            time.sleep(0.15)

            stalled = w_client.parsed_metrics()
            lag_vals = [
                v
                for (n, _), v in stalled.items()
                if "lag" in n or "writer_log" in n
            ]
            self.assertTrue(any(v > 0.0 for v in lag_vals), f"Expected positive lag/log metric while stalled: {stalled}")

            storage = storage.restart()
            assert isinstance(storage, NodeInstance)

            def _drained() -> bool:
                m = w_client.parsed_metrics()
                active = [v for (n, _), v in m.items() if ("forward" in n and "lag" in n)]
                return bool(active) and all(v == 0.0 for v in active)

            wait_until(_drained, timeout=20.0, desc="forwarding lag drains to 0")


class TestTier1F25SyncProfileEndpoint(unittest.TestCase):
    """Tier 1 tests for F25: Synchronous profile endpoint returning ID & error propagation (W2)."""

    @tier(1)
    @feature("F25")
    @require_milestone("W2")
    def test_identify_returns_canonical_uid_and_is_immediately_readable(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F25 Identify")

            res = client.identify(
                {
                    "anon_id": "anon_f25_1",
                    "user_id": "user_f25_1",
                    "user_properties": {"tier": "gold", "country": "IN"},
                },
                api_key=api_key,
            ).raise_for_status().json()
            self.assertEqual(res["status"], "ok")
            uid = res["canonical_uid"]
            self.assertIsInstance(uid, int)
            self.assertGreaterEqual(uid, 1)

            detail = client.get_user(pid, "user_f25_1").raise_for_status().json()
            self.assertEqual(detail["canonical_uid"], uid)
            self.assertEqual(detail["profile"]["props"]["tier"], "gold")

    @tier(1)
    @feature("F25")
    @require_milestone("W2")
    def test_engage_is_synchronous_returns_uid_and_preserves_update_order(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F25 Engage")

            e1 = client.engage(
                {"distinct_id": "user_f25_eng", "$set": {"plan": "free"}, "$setOnce": {"cohort": "2026-11"}},
                api_key=api_key,
            ).raise_for_status().json()
            self.assertEqual(e1["status"], "ok")
            self.assertIsInstance(e1.get("canonical_uid"), int)

            e2 = client.engage(
                {"distinct_id": "user_f25_eng", "$set": {"plan": "enterprise"}, "$add": {"credits": 25}},
                api_key=api_key,
            ).raise_for_status().json()
            self.assertEqual(e2["canonical_uid"], e1["canonical_uid"])

            detail = client.get_user(pid, "user_f25_eng").raise_for_status().json()
            self.assertEqual(detail["profile"]["props"]["plan"], "enterprise")
            self.assertEqual(detail["profile"]["props"]["cohort"], "2026-11")
            self.assertEqual(detail["profile"]["props"]["credits"], 25)

    @tier(1)
    @feature("F25")
    @require_milestone("W2")
    def test_groups_applies_group_traits_synchronously(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            _, api_key = _create_project(client, "F25 Groups")

            g1 = client.groups(
                {"group_type": "company", "group_id": "acme_1", "$set": {"seats": 10, "tier": "pro"}},
                api_key=api_key,
            ).raise_for_status().json()
            self.assertEqual(g1["status"], "ok")
            self.assertEqual(g1["updated"], 1)

            g2 = client.groups(
                {"group_type": "company", "group_id": "acme_1", "$set": {"seats": 50}},
                api_key=api_key,
            ).raise_for_status().json()
            self.assertEqual(g2["status"], "ok")
            self.assertEqual(g2["updated"], 1)

    @tier(1)
    @feature("F25")
    @require_milestone("W2")
    def test_replay_chunk_returns_offset_synchronously_and_reads_immediately(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local")
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F25 Replay")
            now_ms = int(time.time() * 1000)

            chunk = make_replay_chunk(session_id="rsess_f25_1", project_id=pid, chunk_start_ts_ms=now_ms)
            r = client.replay_chunk(chunk, api_key=api_key, binary=True).raise_for_status().json()
            self.assertEqual(r["status"], "ok")
            self.assertGreaterEqual(r["replay_offset_kb"], 0)
            self.assertGreater(r["chunk_bytes"], 0)

            got = client.get_replay(pid, "rsess_f25_1").raise_for_status().json()
            self.assertEqual(got["session_id"], "rsess_f25_1")
            self.assertGreaterEqual(len(got["chunks"]), 1)

    @tier(1)
    @feature("F25")
    @require_milestone("W2")
    def test_sigkill_immediately_after_identify_and_engage_preserves_profile(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "F25 Crash Durable")

            id_res = client.identify(
                {"anon_id": "a_kill_1", "user_id": "u_kill_1", "user_properties": {"durable": True}},
                api_key=api_key,
            ).raise_for_status().json()
            client.engage(
                {"distinct_id": "u_kill_1", "$set": {"level": 7}},
                api_key=api_key,
            ).raise_for_status()

            # SIGKILL immediately after 200 OK without drain
            node.kill()
            restarted = node.restart(graceful_stop=False)
            assert isinstance(restarted, NodeInstance)
            r_client = restarted.client()

            def _get_user_after_lease() -> dict[str, Any] | None:
                resp = r_client.get_user(pid, "u_kill_1")
                if resp.status == 200:
                    return resp.json()
                return None

            detail = wait_until(_get_user_after_lease, timeout=15.0, desc="user profile readable after lease expiry")
            assert detail is not None
            self.assertEqual(detail["canonical_uid"], id_res["canonical_uid"])
            self.assertTrue(detail["profile"]["props"]["durable"])
            self.assertEqual(detail["profile"]["props"]["level"], 7)


class TestTier1F28StatelessWriterAndWriterLog(unittest.TestCase):
    """Tier 1 tests for F28: Stateless writer + per-project local writer_log/ (W3)."""

    @tier(1)
    @feature("F28")
    @require_milestone("W3")
    def test_stateless_writer_creates_writer_id_and_per_project_writer_log(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F28 Writer Log")

            storage.stop(graceful=True)
            res = writer.client().ingest(
                {"events": make_events(4, event_name="f28_log")},
                api_key=api_key,
            ).raise_for_status().json()
            self.assertEqual(res["accepted"], 4)
            self.assertEqual(res["deduped"], 0)

            wait_until(
                lambda: any(f"writer_log/{pid}/" in p for p in writer.local_files()),
                timeout=10.0,
                desc="per-project writer_log created",
            )
            wid = _find_writer_id(writer.data_dir)
            self.assertIsNotNone(wid)
            # Stateless writer never opens SlateDB identity/ or profiles/
            self.assertFalse(any(p.startswith(("identity/", "profiles/", "wal/identity/", "wal/profiles/")) for p in writer.local_files()))

    @tier(1)
    @feature("F28")
    @require_milestone("W3")
    def test_writer_restart_retains_writer_id_and_forwards_buffered_log(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F28 Restart ID")
            now_ms = int(time.time() * 1000)

            storage.stop(graceful=True)
            writer.client().ingest(
                {"events": make_events(5, event_name="f28_keep_id", base_ts_ms=now_ms - 3000)},
                api_key=api_key,
            ).raise_for_status()
            wait_until(lambda: any(f"writer_log/{pid}/" in p for p in writer.local_files()), timeout=10.0)
            wid_before = _find_writer_id(writer.data_dir)
            self.assertIsNotNone(wid_before)

            writer = writer.restart(graceful_stop=True)
            assert isinstance(writer, NodeInstance)
            self.assertEqual(_find_writer_id(writer.data_dir), wid_before)

            storage = storage.restart()
            assert isinstance(storage, NodeInstance)
            wait_until(
                lambda: _insights_total(storage.client(), pid, "f28_keep_id", now_ms - 60_000, now_ms + 60_000) == 5.0,
                timeout=20.0,
                desc="restarted writer drains retained log",
            )

    @tier(1)
    @feature("F28")
    @require_milestone("W3")
    def test_lexicon_pii_masking_and_drops_execute_before_writer_log_disk_write(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage, writer = topo["storages"][0], topo["writers"][0]
            s_client = storage.client()
            pid, api_key = _create_project(s_client, "F28 Lexicon PII")
            now_ms = int(time.time() * 1000)

            s_client.update_lexicon_property(pid, "pan_secret", {"pii_masked": True}).raise_for_status()
            s_client.update_lexicon_event(pid, "f28_dropped_ev", {"status": "dropped"}).raise_for_status()
            # Ensure writer refreshes lexicon rules (or restart writer to force immediate reload)
            writer = writer.restart(graceful_stop=True)
            assert isinstance(writer, NodeInstance)

            storage.stop(graceful=True)
            raw_secret = "ABCDE9876Z"
            res = writer.client().ingest(
                {
                    "events": [
                        make_event(
                            event_name="f28_pii_ok",
                            distinct_id="u_pii",
                            ts_ms=now_ms - 1000,
                            props={"pan_secret": raw_secret},
                        ),
                        make_event(event_name="f28_dropped_ev", distinct_id="u_pii", ts_ms=now_ms - 500),
                    ]
                },
                api_key=api_key,
            ).raise_for_status().json()
            self.assertEqual(res["accepted"], 1)
            self.assertEqual(res["dropped"], 1)

            wait_until(lambda: any(f"writer_log/{pid}/" in p for p in writer.local_files()), timeout=10.0)
            for rel in writer.local_files("writer_log/"):
                self.assertNotIn(
                    raw_secret.encode("utf-8"),
                    writer.read_local_bytes(rel),
                    f"Unmasked PII landed on writer_log disk file {rel}",
                )

            storage = storage.restart()
            assert isinstance(storage, NodeInstance)
            wait_until(
                lambda: _insights_total(storage.client(), pid, "f28_pii_ok", now_ms - 60_000, now_ms + 60_000) == 1.0,
                timeout=20.0,
                desc="masked event drained to storage",
            )
            self.assertEqual(_insights_total(storage.client(), pid, "f28_dropped_ev", now_ms - 60_000, now_ms + 60_000), 0.0)

    @tier(1)
    @feature("F28")
    @require_milestone("W3")
    def test_writer_log_flush_bound_20ms_survives_writer_sigkill(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(
                num_writers=1,
                num_storage=1,
                database_url=db_url,
                extra_env={"PULSE_WRITER_FLUSH_MS": "20", "PULSE_WRITER_FLUSH_BYTES": "1048576"},
            )
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F28 Flush Bound")
            now_ms = int(time.time() * 1000)

            storage.stop(graceful=True)
            writer.client().ingest(
                {"events": make_events(6, event_name="f28_bound", base_ts_ms=now_ms - 2000)},
                api_key=api_key,
            ).raise_for_status()
            time.sleep(0.15)
            writer.kill()

            writer = writer.restart(graceful_stop=False)
            storage = storage.restart()
            assert isinstance(writer, NodeInstance) and isinstance(storage, NodeInstance)
            wait_until(
                lambda: _insights_total(storage.client(), pid, "f28_bound", now_ms - 60_000, now_ms + 60_000) == 6.0,
                timeout=20.0,
                desc="flushed writer_log survived writer SIGKILL",
            )

    @tier(1)
    @feature("F28")
    @require_milestone("W3")
    def test_idle_project_handles_close_while_retaining_dir_and_accepts_later_writes(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(
                num_writers=1,
                num_storage=1,
                database_url=db_url,
                extra_env={"PULSE_WRITER_LOG_IDLE_CLOSE_MS": "200"},
            )
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F28 Idle Close")
            now_ms = int(time.time() * 1000)

            writer.client().ingest(
                {"events": make_events(2, event_name="f28_idle", base_ts_ms=now_ms - 4000)},
                api_key=api_key,
            ).raise_for_status()
            wait_until(
                lambda: _insights_total(storage.client(), pid, "f28_idle", now_ms - 60_000, now_ms + 60_000) == 2.0,
                timeout=15.0,
            )
            time.sleep(0.4)
            self.assertTrue(any(f"writer_log/{pid}/" in p for p in writer.local_files()) or (writer.data_dir / "writer_log" / pid).exists())

            writer.client().ingest(
                {"events": make_events(2, event_name="f28_idle", distinct_id_prefix="u2", base_ts_ms=now_ms - 1000)},
                api_key=api_key,
            ).raise_for_status()
            wait_until(
                lambda: _insights_total(storage.client(), pid, "f28_idle", now_ms - 60_000, now_ms + 60_000) == 4.0,
                timeout=15.0,
                desc="reopened writer_log accepts post-idle writes",
            )


class TestTier1F29ForwarderAndOffsets(unittest.TestCase):
    """Tier 1 tests for F29: Per-project forwarder & (writer_id, bucket) offsets (W3)."""

    @tier(1)
    @feature("F29")
    @require_milestone("W3")
    def test_two_writers_two_storage_nodes_deliver_exactly_once(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=2, num_storage=2, database_url=db_url)
            s1, s2 = topo["storages"]
            w1, w2 = topo["writers"]
            p1, k1 = _create_project(s1.client(), "F29 P1")
            p2, k2 = _create_project(s1.client(), "F29 P2")
            now_ms = int(time.time() * 1000)

            w1.client().ingest({"events": make_events(50, event_name="f29_e", base_ts_ms=now_ms - 5000, seed_prefix="p1w1")}, api_key=k1).raise_for_status()
            w2.client().ingest({"events": make_events(50, event_name="f29_e", distinct_id_prefix="u_w2", base_ts_ms=now_ms - 4000, seed_prefix="p1w2")}, api_key=k1).raise_for_status()
            w1.client().ingest({"events": make_events(40, event_name="f29_e", base_ts_ms=now_ms - 5000, seed_prefix="p2w1")}, api_key=k2).raise_for_status()
            w2.client().ingest({"events": make_events(40, event_name="f29_e", distinct_id_prefix="u_w2", base_ts_ms=now_ms - 4000, seed_prefix="p2w2")}, api_key=k2).raise_for_status()

            wait_until(
                lambda: _insights_total(s1.client(), p1, "f29_e", now_ms - 60_000, now_ms + 60_000) == 100.0
                and _insights_total(s1.client(), p2, "f29_e", now_ms - 60_000, now_ms + 60_000) == 80.0,
                timeout=25.0,
                desc="2x2 W3 delivery exact counts",
            )

    @tier(1)
    @feature("F29")
    @require_milestone("W3")
    def test_forwarder_deletes_behind_cursor_log_files_after_wal_fsync_ack(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F29 Cursor Cleanup")
            now_ms = int(time.time() * 1000)

            storage.stop(graceful=True)
            for b in range(5):
                writer.client().ingest(
                    {"events": make_events(10, event_name="f29_gc", base_ts_ms=now_ms - 3000 + b)},
                    api_key=api_key,
                ).raise_for_status()
                time.sleep(0.04)

            wait_until(
                lambda: len([p for p in writer.local_files(f"writer_log/{pid}/") if p.endswith(".log") or "batch" in p or "seg" in p or "wal" in p]) >= 1
                or len(writer.local_files(f"writer_log/{pid}/")) >= 2,
                timeout=10.0,
            )
            before_files = writer.local_files(f"writer_log/{pid}/")

            storage = storage.restart()
            assert isinstance(storage, NodeInstance)
            wait_until(
                lambda: _insights_total(storage.client(), pid, "f29_gc", now_ms - 60_000, now_ms + 60_000) == 50.0,
                timeout=20.0,
            )
            wait_until(
                lambda: len(writer.local_files(f"writer_log/{pid}/")) < len(before_files) or all("cursor" in p for p in writer.local_files(f"writer_log/{pid}/")),
                timeout=15.0,
                desc="behind-cursor writer_log files cleaned up",
            )

    @tier(1)
    @feature("F29")
    @require_milestone("W3")
    def test_storage_persists_writer_offsets_in_manifest_surviving_wal_purge(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F29 Offsets Manifest")
            now_ms = int(time.time() * 1000)

            writer.client().ingest(
                {"events": make_events(8, event_name="f29_off", base_ts_ms=now_ms - 2000)},
                api_key=api_key,
            ).raise_for_status()
            wait_until(
                lambda: _insights_total(storage.client(), pid, "f29_off", now_ms - 60_000, now_ms + 60_000) == 8.0,
                timeout=15.0,
            )
            storage.client().admin_flush().raise_for_status()

            manifest_files = [p for p in storage.local_files() if f"manifests/{pid}/" in p and p.endswith(".json")]
            self.assertTrue(manifest_files, f"Expected local manifest for {pid}: {storage.local_files()}")
            newest = decode_manifest(storage.read_local_bytes(sorted(manifest_files)[-1]))
            self.assertIn("writer_offsets", newest)
            self.assertTrue(newest["writer_offsets"], f"Expected non-empty writer_offsets in manifest: {newest}")

    @tier(1)
    @feature("F29")
    @require_milestone("W3")
    def test_forwarder_refreshes_owner_on_not_owner_after_manual_transfer(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            assert cluster.mock_s3 is not None
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=2, database_url=db_url, upload_interval_secs=1)
            s1, s2 = topo["storages"]
            writer = topo["writers"][0]
            pid, api_key = _create_project(s1.client(), "F29 Owner Move")
            now_ms = int(time.time() * 1000)

            # Determine initial owner and peer
            rows = s1.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (pid,), db_path=db_path)
            self.assertEqual(len(rows), 1)
            orig_name = rows[0]["node_name"]
            new_owner = s2 if orig_name == "storage-0" else s1
            new_name = "storage-1" if orig_name == "storage-0" else "storage-0"
            orig_node = s1 if orig_name == "storage-0" else s2

            # Seed batch 1 and publish
            writer.client().ingest({"events": make_events(3, event_name="f29_mv", base_ts_ms=now_ms - 4000)}, api_key=api_key).raise_for_status()
            wait_until(lambda: _insights_total(orig_node.client(), pid, "f29_mv", now_ms - 60_000, now_ms + 60_000) == 3.0, timeout=15.0)
            orig_node.client().admin_flush().raise_for_status()
            wait_until(lambda: any(k.startswith(f"published/{pid}/") for k in cluster.mock_s3.list_objects()), timeout=15.0)
            orig_node.stop(graceful=True)

            # Restore/transfer project onto new_owner and update project_owner
            _transfer_project_for_failover(cluster, orig_node, new_owner, pid, db_url)
            new_owner.sqlite_query(
                "UPDATE project_owner SET node_name = ?, address = ? WHERE project_id = ?",
                (new_name, new_owner.base_url, pid),
                db_path=db_path,
                readonly=False,
            )
            writer.client().ingest({"events": make_events(2, event_name="f29_mv", distinct_id_prefix="u_mv2", base_ts_ms=now_ms - 1000)}, api_key=api_key).raise_for_status()
            wait_until(
                lambda: _insights_total(new_owner.client(), pid, "f29_mv", now_ms - 60_000, now_ms + 60_000) == 5.0,
                timeout=20.0,
                desc="forwarder routes to new project_owner",
            )

    @tier(1)
    @feature("F29")
    @require_milestone("W3")
    def test_forwarder_delivers_ordered_batches_with_one_in_flight(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=db_url)
            storage, writer = topo["storages"][0], topo["writers"][0]
            pid, api_key = _create_project(storage.client(), "F29 Strict Order")
            now_ms = int(time.time() * 1000)

            for seq_batch in range(5):
                evs = [
                    make_event(
                        event_name="f29_ord",
                        distinct_id="u_order_1",
                        ts_ms=now_ms - 10_000 + seq_batch * 100 + i,
                        props={"seq": seq_batch * 10 + i},
                        event_uuid=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"f29ord:{seq_batch}:{i}")),
                    )
                    for i in range(10)
                ]
                writer.client().ingest({"events": evs}, api_key=api_key).raise_for_status()

            wait_until(
                lambda: _insights_total(storage.client(), pid, "f29_ord", now_ms - 60_000, now_ms + 60_000) == 50.0,
                timeout=20.0,
            )
            exp = storage.client().export_events(pid, format="json").raise_for_status().json()
            seqs = sorted(e["props"]["seq"] for e in exp["events"])
            self.assertEqual(seqs, list(range(50)))


class TestTier1F30ProjectOwnerAndPlacement(unittest.TestCase):
    """Tier 1 tests for F30: project_owner table, placement & non-owner guards (W3)."""

    @tier(1)
    @feature("F30")
    @require_milestone("W3")
    def test_new_projects_balance_by_least_owned_count(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=2, database_url=db_url)
            s1 = topo["storages"][0]
            s_client = s1.client()

            pids = [_create_project(s_client, f"F30 Bal {i}")[0] for i in range(4)]
            rows = s1.sqlite_query(
                f"SELECT project_id, node_name FROM project_owner WHERE project_id IN ({','.join('?' for _ in pids)})",
                pids,
                db_path=db_path,
            )
            counts: dict[str, int] = {}
            for r in rows:
                counts[r["node_name"]] = counts.get(r["node_name"], 0) + 1
            self.assertEqual(counts.get("storage-0"), 2)
            self.assertEqual(counts.get("storage-1"), 2)

    @tier(1)
    @feature("F30")
    @require_milestone("W3")
    def test_project_owner_table_stores_node_name_address_and_timestamp(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=0, num_storage=2, database_url=db_url)
            s1, s2 = topo["storages"]
            pid, _ = _create_project(s1.client(), "F30 Schema")

            rows = s1.sqlite_query(
                "SELECT project_id, node_name, address, updated_at_ms FROM project_owner WHERE project_id = ?",
                (pid,),
                db_path=db_path,
            )
            self.assertEqual(len(rows), 1)
            self.assertIn(rows[0]["node_name"], ("storage-0", "storage-1"))
            self.assertIn(rows[0]["address"], (s1.base_url, s2.base_url))
            self.assertGreater(rows[0]["updated_at_ms"], 0)

    @tier(1)
    @feature("F30")
    @require_milestone("W3")
    def test_background_alert_loop_fires_once_only_on_owning_node(self) -> None:
        srv, received, port = _start_webhook_server()
        self.addCleanup(srv.shutdown)
        self.addCleanup(srv.server_close)

        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(
                num_writers=1,
                num_storage=2,
                database_url=db_url,
                extra_env={
                    "PULSE_ALERT_EVAL_INTERVAL_SECS": "1",
                    "PULSE_VAULT_KEK": random_byok_hex(),
                },
            )
            s1 = topo["storages"][0]
            writer = topo["writers"][0]
            pid, api_key = _create_project(s1.client(), "F30 Alert Once")
            now_ms = int(time.time() * 1000)

            s1.client().request(
                "PUT",
                f"/v1/projects/{pid}/byok",
                json_body={
                    "category": "webhook",
                    "provider": "custom_webhook",
                    "secret": f"http://127.0.0.1:{port}/hook",
                    "config_json": {"signing_secret": "e2e_webhook_signing_secret_12345"},
                },
                token=s1.admin_token,
            ).raise_for_status()

            s1.client().create_alert(
                pid,
                {
                    "name": "High Error Spike",
                    "event_name": "f30_alert_ev",
                    "operator": "gte",
                    "threshold": 5.0,
                    "webhook_url": f"http://127.0.0.1:{port}/hook",
                },
            ).raise_for_status()

            writer.client().ingest(
                {"events": make_events(5, event_name="f30_alert_ev", base_ts_ms=now_ms - 1000)},
                api_key=api_key,
            ).raise_for_status()

            wait_until(lambda: len(received) >= 1, timeout=15.0, desc="alert webhook fired")
            time.sleep(2.5)
            self.assertEqual(len(received), 1, f"Expected single alert from owner only, got {len(received)}")

    @tier(1)
    @feature("F30")
    @require_milestone("W3")
    def test_adding_third_storage_node_keeps_existing_owners_and_places_new_on_third(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=0, num_storage=2, database_url=db_url)
            s1 = topo["storages"][0]
            p1, _ = _create_project(s1.client(), "F30 Keep 1")
            p2, _ = _create_project(s1.client(), "F30 Keep 2")

            before = {
                r["project_id"]: r["node_name"]
                for r in s1.sqlite_query("SELECT project_id, node_name FROM project_owner", db_path=db_path)
            }

            s3 = cluster.start_node(
                role="all",
                database_url=db_url,
                hot_store="local",
                node_name="storage-2",
            )
            assert isinstance(s3, NodeInstance)
            p3, _ = _create_project(s1.client(), "F30 New 3")

            after = {
                r["project_id"]: r["node_name"]
                for r in s1.sqlite_query("SELECT project_id, node_name FROM project_owner", db_path=db_path)
            }
            self.assertEqual(after[p1], before[p1])
            self.assertEqual(after[p2], before[p2])
            self.assertEqual(after[p3], "storage-2")

    @tier(1)
    @feature("F30")
    @require_milestone("W3")
    def test_manual_project_owner_update_transfers_ownership_cleanly(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            assert cluster.mock_s3 is not None
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=2, database_url=db_url, upload_interval_secs=1)
            s1, s2 = topo["storages"]
            pid, api_key = _create_project(s1.client(), "F30 Failover")
            now_ms = int(time.time() * 1000)

            orig = s1.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (pid,), db_path=db_path)[0]["node_name"]
            owner_node = s1 if orig == "storage-0" else s2
            target_node = s2 if orig == "storage-0" else s1
            target_name = "storage-1" if orig == "storage-0" else "storage-0"

            owner_node.client().ingest({"events": make_events(4, event_name="f30_fo", base_ts_ms=now_ms - 3000)}, api_key=api_key).raise_for_status()
            wait_until(lambda: _insights_total(owner_node.client(), pid, "f30_fo", now_ms - 60_000, now_ms + 60_000) == 4.0, timeout=15.0)
            owner_node.client().admin_flush().raise_for_status()
            wait_until(lambda: any(k.startswith(f"published/{pid}/") for k in cluster.mock_s3.list_objects()), timeout=15.0)
            owner_node.stop(graceful=True)

            _transfer_project_for_failover(cluster, owner_node, target_node, pid, db_url)
            target_node.sqlite_query(
                "UPDATE project_owner SET node_name = ?, address = ? WHERE project_id = ?",
                (target_name, target_node.base_url, pid),
                db_path=db_path,
                readonly=False,
            )
            target_node.client().ingest({"events": make_events(2, event_name="f30_fo", distinct_id_prefix="u_fo2", base_ts_ms=now_ms - 1000)}, api_key=api_key).raise_for_status()
            wait_until(lambda: _insights_total(target_node.client(), pid, "f30_fo", now_ms - 60_000, now_ms + 60_000) == 6.0, timeout=15.0)


class TestTier1F31ForwardingAndRouting(unittest.TestCase):
    """Tier 1 tests for F31: Routing of profile calls, queries & agent traces (W3)."""

    @tier(1)
    @feature("F31")
    @require_milestone("W3")
    def test_writer_forwards_sync_identify_engage_and_groups_to_owner(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=2, num_storage=2, database_url=db_url)
            s1 = topo["storages"][0]
            w1, w2 = topo["writers"]
            pid, api_key = _create_project(s1.client(), "F31 Sync Proxy")

            id_res = w2.client().identify(
                {"anon_id": "a_f31_1", "user_id": "u_f31_1", "user_properties": {"role": "founder"}},
                api_key=api_key,
            ).raise_for_status().json()
            self.assertIsInstance(id_res["canonical_uid"], int)

            eng_res = w1.client().engage(
                {"distinct_id": "u_f31_1", "$set": {"plan": "growth"}},
                api_key=api_key,
            ).raise_for_status().json()
            self.assertEqual(eng_res["canonical_uid"], id_res["canonical_uid"])

            grp_res = w2.client().groups(
                {"group_type": "org", "group_id": "org_f31", "$set": {"mrr": 1000}},
                api_key=api_key,
            ).raise_for_status().json()
            self.assertEqual(grp_res["updated"], 1)

            detail = s1.client().get_user(pid, "u_f31_1").raise_for_status().json()
            self.assertEqual(detail["profile"]["props"]["plan"], "growth")
            self.assertEqual(detail["profile"]["props"]["role"], "founder")

    @tier(1)
    @feature("F31")
    @require_milestone("W3")
    def test_writer_forwards_replay_chunk_to_owner_and_returns_offset(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=2, database_url=db_url)
            s1, s2 = topo["storages"]
            writer = topo["writers"][0]
            pid, api_key = _create_project(s1.client(), "F31 Replay Proxy")
            now_ms = int(time.time() * 1000)

            chunk = make_replay_chunk(session_id="rsess_f31_1", project_id=pid, chunk_start_ts_ms=now_ms)
            r = writer.client().replay_chunk(chunk, api_key=api_key, binary=True).raise_for_status().json()
            self.assertEqual(r["status"], "ok")
            self.assertGreaterEqual(r["replay_offset_kb"], 0)

            got = s2.client().get_replay(pid, "rsess_f31_1").raise_for_status().json()
            self.assertEqual(got["session_id"], "rsess_f31_1")

    @tier(1)
    @feature("F31")
    @require_milestone("W3")
    def test_non_owner_storage_node_forwards_analytics_and_user_queries(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, db_path = _w3_sqlite_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=2, database_url=db_url)
            s1, s2 = topo["storages"]
            writer = topo["writers"][0]
            pid, api_key = _create_project(s1.client(), "F31 Query Route")
            now_ms = int(time.time() * 1000)

            writer.client().ingest({"events": make_events(5, event_name="f31_q", base_ts_ms=now_ms - 3000)}, api_key=api_key).raise_for_status()
            writer.client().identify({"anon_id": "a_q", "user_id": "u_q", "user_properties": {"k": "v"}}, api_key=api_key).raise_for_status()

            owner_name = s1.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (pid,), db_path=db_path)[0]["node_name"]
            non_owner = s2 if owner_name == "storage-0" else s1

            wait_until(
                lambda: _insights_total(non_owner.client(), pid, "f31_q", now_ms - 60_000, now_ms + 60_000) == 5.0,
                timeout=20.0,
                desc="non-owner proxies insights query to owner",
            )
            users = non_owner.client().list_users(pid).raise_for_status().json()
            self.assertTrue(any(u.get("distinct_id") == "u_q" for u in users))

    @tier(1)
    @feature("F31")
    @require_milestone("W3")
    def test_agent_traces_route_via_async_writer_log_to_owner(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=2, database_url=db_url)
            s1, s2 = topo["storages"]
            writer = topo["writers"][0]
            pid, api_key = _create_project(s1.client(), "F31 Agent Route")
            now_ms = int(time.time() * 1000)

            tr1 = make_agent_trace(trace_id="tr_f31_json", start_ts_ms=now_ms - 2000)
            tr2 = make_agent_trace(trace_id="tr_f31_plst", start_ts_ms=now_ms - 1000)
            r1 = writer.client().ingest_agent_traces(tr1, api_key=api_key).raise_for_status().json()
            r2 = writer.client().ingest_agent_traces(tr2, api_key=api_key, binary=True).raise_for_status().json()
            self.assertEqual(r1["accepted_traces"], 1)
            self.assertEqual(r2["accepted_traces"], 1)

            def _traces_visible() -> bool:
                res = s2.client().list_agent_traces(pid).raise_for_status().json()
                ids = {t["trace_id"] for t in res.get("traces", [])}
                return {"tr_f31_json", "tr_f31_plst"}.issubset(ids)

            wait_until(_traces_visible, timeout=20.0, desc="agent traces forwarded to owner")

    @tier(1)
    @feature("F31")
    @require_milestone("W3")
    def test_mixed_batch_on_writer_proxies_identify_sync_and_buffers_events(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            db_url, _ = _w3_meta_url(cluster)
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=2, database_url=db_url)
            s1, s2 = topo["storages"]
            writer = topo["writers"][0]
            pid, api_key = _create_project(s1.client(), "F31 Mixed Proxy")
            now_ms = int(time.time() * 1000)

            res = writer.client().ingest(
                {
                    "events": [
                        {
                            "type": "identify",
                            "userId": "u_w3_mix",
                            "anonymousId": "a_w3_mix",
                            "traits": {"tier": "vip"},
                        },
                        make_event(event_name="f31_mix_ev", distinct_id="a_w3_mix", ts_ms=now_ms - 1000),
                    ]
                },
                api_key=api_key,
            ).raise_for_status().json()
            self.assertEqual(res["accepted"], 1)
            self.assertEqual(res["deduped"], 0)
            self.assertEqual(res["profiles_updated"], 1)

            detail = s2.client().get_user(pid, "u_w3_mix").raise_for_status().json()
            self.assertEqual(detail["profile"]["props"]["tier"], "vip")
            wait_until(
                lambda: _insights_total(s1.client(), pid, "f31_mix_ev", now_ms - 60_000, now_ms + 60_000) == 1.0,
                timeout=20.0,
            )


if __name__ == "__main__":
    unittest.main()
