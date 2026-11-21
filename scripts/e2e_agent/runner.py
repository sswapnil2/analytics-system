#!/usr/bin/env python3
"""Opaque-box Python 3.14 stdlib E2E runner for Pulse AI Agent Observability."""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_HARNESS_ROOT = Path(__file__).resolve().parent
if str(_HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(_HARNESS_ROOT))

from lib import fixtures, plst  # noqa: E402
from lib.auth import AuthBootstrap  # noqa: E402
from lib.client import PulseClient  # noqa: E402
from lib.mocks import MockLlmServer, MockWebhookServer  # noqa: E402
from lib.node_mgr import NodeManager  # noqa: E402
from lib.registry import (  # noqa: E402
    PER_FEATURE_THRESHOLDS,
    TIER_THRESHOLDS,
    TOTAL_THRESHOLD,
    TestCase,
    ThresholdError,
    VALID_FEATURES,
    VALID_TIERS,
    discover_tests,
    summarize_counts,
    verify_thresholds,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for the E2E runner."""
    parser = argparse.ArgumentParser(
        description="Pulse AI Agent Observability Opaque-Box E2E Test Runner",
    )
    parser.add_argument(
        "--tier",
        choices=["1", "2", "3", "4", "all"],
        default="all",
        help="Tier to execute or inspect (1, 2, 3, 4, or all).",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="Optional base URL of an already-running pulse-node instance.",
    )
    parser.add_argument(
        "--binary",
        default=None,
        help="Optional explicit path to the pulse-node binary.",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Optional PULSE_DATA_DIR path (created as temp dir if omitted in auto-spawn mode).",
    )
    parser.add_argument(
        "--admin-token",
        default=None,
        help="Optional root admin token (required when attaching via --base-url without data-dir).",
    )
    parser.add_argument(
        "--filter",
        default=None,
        help="Case-insensitive regex/substring filter matched against test id, feature, title, or tags.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate test discovery, counts, and offline harness contracts without booting pulse-node.",
    )
    parser.add_argument(
        "--list-tests",
        action="store_true",
        help="Print deterministic sorted inventory of discovered tests and per-tier counts.",
    )
    parser.add_argument(
        "--json-report",
        default=None,
        help="Optional file path to write machine-readable JSON test report.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print verbose diagnostics and failure tracebacks.",
    )
    parser.add_argument(
        "--enforce-thresholds",
        action="store_true",
        help="Enforce full suite minimum coverage thresholds (T1>=120, T2>=120, T3>=24, T4>=12, Total>=276).",
    )
    return parser


def verify_harness_offline() -> dict[str, Any]:
    """Verify PLST wire codec, hostile builders, and deterministic fixtures offline."""
    # 1. PLST raw & dictionary round-trip + blob dedup
    dedup_batch = fixtures.make_repeated_prompt_batch(seed="dryrun", repeats=3, prompt_bytes=512)
    stats = plst.BlobDedupStats()
    raw_frame = plst.encode_plst_frame(dedup_batch, use_dict=False, stats_out=stats)
    dict_frame = plst.encode_plst_frame(dedup_batch, use_dict=True)
    decoded_raw = plst.decode_plst_frame(raw_frame)
    decoded_dict = plst.decode_plst_frame(dict_frame)
    if len(decoded_raw["traces"]) != 3 or len(decoded_dict["spans"]) != 3:
        raise AssertionError("Dry-run PLST round-trip count mismatch")
    if stats.deduped_blobs <= 0:
        raise AssertionError("Dry-run PLST blob dedup expected > 0 deduped blobs")

    # 2. Hostile frame rejection
    for kind in ("bad_magic", "bad_version", "crc_mismatch", "oversized_uncomp_len", "zstd_bomb"):
        hostile = plst.build_hostile_frame(kind)
        try:
            plst.decode_plst_frame(hostile)
        except plst.PlstError:
            pass
        else:
            raise AssertionError(f"Hostile PLST frame {kind!r} was unexpectedly accepted")

    # 3. Deterministic hierarchy & OTLP fixture smoke check
    hier = fixtures.make_hierarchy_batch(seed="dryrun_hier")
    if len(hier["traces"]) != 2 or len(hier["spans"]) < 10:
        raise AssertionError("Dry-run hierarchy fixture incomplete")
    otlp = fixtures.make_otlp_export_request(seed="dryrun_otlp")
    if not otlp.get("resourceSpans"):
        raise AssertionError("Dry-run OTLP fixture missing resourceSpans")

    return {
        "plst_raw_bytes": len(raw_frame),
        "plst_dict_bytes": len(dict_frame),
        "deduped_blobs": stats.deduped_blobs,
    }


def _parse_tiers(tier_arg: str) -> tuple[int, ...]:
    if tier_arg == "all":
        return VALID_TIERS
    return (int(tier_arg),)


def _format_counts_line(summary: dict[str, Any]) -> str:
    by_tier = summary["by_tier"]
    return (
        f"Tier 1: {by_tier[1]}/{TIER_THRESHOLDS[1]} | "
        f"Tier 2: {by_tier[2]}/{TIER_THRESHOLDS[2]} | "
        f"Tier 3: {by_tier[3]}/{TIER_THRESHOLDS[3]} | "
        f"Tier 4: {by_tier[4]}/{TIER_THRESHOLDS[4]} | "
        f"Total: {summary['total']}/{TOTAL_THRESHOLD}"
    )


def _write_json_report(path_str: str | None, payload: dict[str, Any]) -> None:
    if not path_str:
        return
    report_path = Path(path_str)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _execute_live(
    args: argparse.Namespace,
    selected_cases: list[TestCase],
    tiers: tuple[int, ...],
    full_summary: dict[str, Any],
    thresholds_ok: bool,
) -> int:
    results: list[dict[str, Any]] = []
    passed = 0
    failed = 0
    resolved_base_url = args.base_url.rstrip("/") if args.base_url else None

    with contextlib.ExitStack() as stack:
        mock_llm = stack.enter_context(MockLlmServer())
        mock_webhook = stack.enter_context(MockWebhookServer())
        node_mgr = stack.enter_context(NodeManager(binary=args.binary))

        if resolved_base_url:
            inst = node_mgr.attach_external(
                resolved_base_url,
                data_dir=args.data_dir,
                admin_token=args.admin_token,
            )
            admin_token = args.admin_token or inst.root_admin_token()
            data_dir: Path | None = Path(args.data_dir) if args.data_dir else None
        else:
            spawn_kwargs: dict[str, Any] = {}
            if args.data_dir is not None:
                spawn_kwargs["data_dir"] = Path(args.data_dir)
            if args.admin_token is not None:
                spawn_kwargs["admin_token"] = args.admin_token
            inst = node_mgr.start_all(**spawn_kwargs)
            resolved_base_url = inst.base_url
            admin_token = inst.root_admin_token()
            data_dir = inst.data_dir

        client = PulseClient(resolved_base_url)
        ctx = AuthBootstrap.bootstrap_context(
            client,
            data_dir,
            admin_token,
            mock_llm=mock_llm,
            mock_webhook=mock_webhook,
            node_manager=node_mgr,
        )

        for case in selected_cases:
            ctx.reset_mocks()
            t0 = time.monotonic()
            err_msg: str | None = None
            tb_text: str | None = None
            status = "pass"
            try:
                case.fn(ctx)
                passed += 1
            except Exception as exc:
                status = "fail"
                failed += 1
                err_msg = f"{type(exc).__name__}: {exc}"
                tb_text = traceback.format_exc()

            duration_ms = round((time.monotonic() - t0) * 1000.0, 2)
            label = "PASS" if status == "pass" else "FAIL"
            print(f"[{label}] {case.id} (T{case.tier}|{case.feature}) {case.title} ({duration_ms:.1f}ms)")
            if err_msg and args.verbose and tb_text:
                print(tb_text, file=sys.stderr)
            elif err_msg:
                print(f"       {err_msg}", file=sys.stderr)

            results.append(
                {
                    "id": case.id,
                    "tier": case.tier,
                    "feature": case.feature,
                    "title": case.title,
                    "status": status,
                    "duration_ms": duration_ms,
                    "error": err_msg,
                }
            )

    report = {
        "schema_version": 1,
        "mode": "live",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "base_url": resolved_base_url,
        "tiers_selected": list(tiers),
        "filter": args.filter,
        "thresholds_enforced": bool(args.enforce_thresholds),
        "thresholds_ok": thresholds_ok,
        "counts_by_tier": {str(k): v for k, v in full_summary["by_tier"].items()},
        "summary": {
            "total_selected": len(selected_cases),
            "passed": passed,
            "failed": failed,
        },
        "results": results,
    }
    _write_json_report(args.json_report, report)
    print(f"Summary: passed={passed} failed={failed} selected={len(selected_cases)}")
    return 0 if failed == 0 else 1


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint returning exit code 0 on success or non-zero on failure."""
    parser = build_parser()
    args = parser.parse_args(argv)
    tiers = _parse_tiers(args.tier)

    full_cases = discover_tests()
    full_summary = summarize_counts(full_cases)
    selected_cases = discover_tests(tiers=tiers, filter_expr=args.filter)

    thresholds_ok = True
    threshold_violations: list[str] = []
    if args.enforce_thresholds:
        try:
            verify_thresholds(full_cases)
        except ThresholdError as exc:
            thresholds_ok = False
            threshold_violations = exc.violations

    if args.list_tests:
        for case in selected_cases:
            tags_str = f" [{', '.join(case.tags)}]" if case.tags else ""
            print(f"{case.id}  [T{case.tier}|{case.feature}]  {case.title}{tags_str}")
        print(_format_counts_line(full_summary))
        print(f"Selected: {len(selected_cases)}")
        if args.enforce_thresholds and not thresholds_ok:
            for v in threshold_violations:
                print(f"THRESHOLD VIOLATION: {v}", file=sys.stderr)
        _write_json_report(
            args.json_report,
            {
                "schema_version": 1,
                "mode": "list-tests",
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "base_url": args.base_url,
                "tiers_selected": list(tiers),
                "filter": args.filter,
                "thresholds_enforced": bool(args.enforce_thresholds),
                "thresholds_ok": thresholds_ok,
                "threshold_violations": threshold_violations,
                "counts_by_tier": {str(k): v for k, v in full_summary["by_tier"].items()},
                "summary": {"total_selected": len(selected_cases), "passed": 0, "failed": 0},
                "results": [
                    {
                        "id": c.id,
                        "tier": c.tier,
                        "feature": c.feature,
                        "title": c.title,
                        "status": "listed",
                        "duration_ms": 0.0,
                        "error": None,
                    }
                    for c in selected_cases
                ],
            },
        )
        return 0 if thresholds_ok else 2

    if args.dry_run:
        offline_info = verify_harness_offline()
        print("Offline harness contract checks: OK")
        print(_format_counts_line(full_summary))
        print(f"Selected: {len(selected_cases)}")
        if args.verbose:
            print(
                f"Per-feature targets: Tier 1 >= {PER_FEATURE_THRESHOLDS[1]}, "
                f"Tier 2 >= {PER_FEATURE_THRESHOLDS[2]} across {len(VALID_FEATURES)} features"
            )
            print(f"Offline PLST check: {offline_info}")
        if args.enforce_thresholds and not thresholds_ok:
            for v in threshold_violations:
                print(f"THRESHOLD VIOLATION: {v}", file=sys.stderr)
        _write_json_report(
            args.json_report,
            {
                "schema_version": 1,
                "mode": "dry-run",
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "base_url": args.base_url,
                "tiers_selected": list(tiers),
                "filter": args.filter,
                "thresholds_enforced": bool(args.enforce_thresholds),
                "thresholds_ok": thresholds_ok,
                "threshold_violations": threshold_violations,
                "counts_by_tier": {str(k): v for k, v in full_summary["by_tier"].items()},
                "offline_checks": offline_info,
                "summary": {"total_selected": len(selected_cases), "passed": 0, "failed": 0},
                "results": [],
            },
        )
        return 0 if thresholds_ok else 2

    # Live execution mode
    if args.enforce_thresholds and not thresholds_ok:
        for v in threshold_violations:
            print(f"THRESHOLD VIOLATION: {v}", file=sys.stderr)
        _write_json_report(
            args.json_report,
            {
                "schema_version": 1,
                "mode": "live",
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "base_url": args.base_url,
                "tiers_selected": list(tiers),
                "filter": args.filter,
                "thresholds_enforced": bool(args.enforce_thresholds),
                "thresholds_ok": thresholds_ok,
                "threshold_violations": threshold_violations,
                "counts_by_tier": {str(k): v for k, v in full_summary["by_tier"].items()},
                "summary": {"total_selected": len(selected_cases), "passed": 0, "failed": 0},
                "results": [],
            },
        )
        return 2

    if not selected_cases:
        print(
            "Error: 0 test cases matched selection in live mode. "
            "Use --dry-run or populate tests/tier*.py before running live tests.",
            file=sys.stderr,
        )
        _write_json_report(
            args.json_report,
            {
                "schema_version": 1,
                "mode": "live",
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "base_url": args.base_url,
                "tiers_selected": list(tiers),
                "filter": args.filter,
                "thresholds_enforced": bool(args.enforce_thresholds),
                "thresholds_ok": thresholds_ok,
                "threshold_violations": threshold_violations,
                "counts_by_tier": {str(k): v for k, v in full_summary["by_tier"].items()},
                "summary": {"total_selected": 0, "passed": 0, "failed": 0},
                "results": [],
            },
        )
        return 2

    return _execute_live(args, selected_cases, tiers, full_summary, thresholds_ok)


if __name__ == "__main__":
    raise SystemExit(main())
