#!/usr/bin/env python3
"""Pure-stdlib opaque-box E2E test runner for Pulse Tiered Storage & Log-First Writers."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import unittest
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_HARNESS_ROOT = Path(__file__).resolve().parent
if str(_HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(_HARNESS_ROOT))

from lib.oracle import (  # noqa: E402
    MILESTONES,
    VALID_FEATURES,
    get_test_metadata,
    set_active_gating,
)

_TIER_ORDER: dict[Any, int] = {"self": 0, 1: 1, 2: 2, 3: 3, 4: 4, None: 99}


def _iter_test_cases(suite: unittest.TestSuite) -> list[unittest.TestCase]:
    cases: list[unittest.TestCase] = []
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            cases.extend(_iter_test_cases(item))
        elif isinstance(item, unittest.TestCase):
            cases.append(item)
    return cases


def _parse_features_arg(values: list[str] | None) -> tuple[str, ...] | None:
    if not values:
        return None
    out: list[str] = []
    for raw in values:
        for part in raw.split(","):
            f = part.strip().upper()
            if not f:
                continue
            if f not in VALID_FEATURES:
                raise SystemExit(f"Invalid --feature value {f!r}; expected F01..F31")
            if f not in out:
                out.append(f)
    return tuple(out) if out else None


def _discover_cases(
    *,
    tier_filter: str,
    features_filter: tuple[str, ...] | None,
    milestone_filter: str | None,
) -> tuple[list[unittest.TestCase], list[unittest.TestCase]]:
    tests_dir = _HARNESS_ROOT / "tests"
    loader = unittest.defaultTestLoader
    suite = loader.discover(
        start_dir=str(tests_dir),
        top_level_dir=str(_HARNESS_ROOT),
        pattern="test_*.py",
    )
    all_cases = _iter_test_cases(suite)

    def sort_key(tc: unittest.TestCase) -> tuple[int, str]:
        meta = get_test_metadata(tc)
        return (_TIER_ORDER.get(meta["tier"], 99), tc.id())

    all_cases.sort(key=sort_key)

    selected: list[unittest.TestCase] = []
    for tc in all_cases:
        meta = get_test_metadata(tc)
        tc_tier = meta["tier"]
        tc_feats = meta["features"]
        tc_ms = meta["milestone"]

        if tier_filter != "all":
            want_tier: Any = "self" if tier_filter == "self" else int(tier_filter)
            if tc_tier != want_tier:
                continue
        if features_filter is not None:
            if not any(f in tc_feats for f in features_filter):
                continue
        if milestone_filter is not None:
            if tc_ms != milestone_filter:
                continue
        selected.append(tc)

    return all_cases, selected


def _summarize(cases: list[unittest.TestCase]) -> dict[str, Any]:
    by_tier: Counter[str] = Counter()
    by_feature: Counter[str] = Counter()
    by_milestone: Counter[str] = Counter()

    for tc in cases:
        meta = get_test_metadata(tc)
        t_label = str(meta["tier"]) if meta["tier"] is not None else "untagged"
        by_tier[t_label] += 1
        for f in meta["features"]:
            by_feature[f] += 1
        m_label = str(meta["milestone"]) if meta["milestone"] is not None else "none"
        by_milestone[m_label] += 1

    ordered_tiers = {k: by_tier[k] for k in ("self", "1", "2", "3", "4") if by_tier[k] or k in ("self", "1", "2", "3", "4")}
    if by_tier.get("untagged"):
        ordered_tiers["untagged"] = by_tier["untagged"]
    ordered_features = {f: by_feature[f] for f in VALID_FEATURES if by_feature[f]}
    ordered_milestones = {m: by_milestone[m] for m in MILESTONES if by_milestone[m]}
    if by_milestone.get("none"):
        ordered_milestones["none"] = by_milestone["none"]

    return {
        "total": len(cases),
        "by_tier": ordered_tiers,
        "by_feature": ordered_features,
        "by_milestone": ordered_milestones,
    }


class _TrackingTestResult(unittest.TextTestResult):
    def __init__(self, stream: Any, descriptions: bool, verbosity: int) -> None:
        super().__init__(stream, descriptions, verbosity)
        self.records: list[dict[str, Any]] = []
        self._start_times: dict[str, float] = {}

    def startTest(self, test: unittest.TestCase) -> None:
        self._start_times[test.id()] = time.monotonic()
        super().startTest(test)

    def _record(self, test: unittest.TestCase, status: str, detail: str | None = None) -> None:
        t0 = self._start_times.pop(test.id(), time.monotonic())
        dur_ms = round((time.monotonic() - t0) * 1000.0, 2)
        meta = get_test_metadata(test)
        self.records.append(
            {
                "id": test.id(),
                "status": status,
                "duration_ms": dur_ms,
                "tier": meta["tier"],
                "features": list(meta["features"]),
                "milestone": meta["milestone"],
                "detail": detail,
            }
        )

    def addSuccess(self, test: unittest.TestCase) -> None:
        super().addSuccess(test)
        self._record(test, "pass")

    def addFailure(self, test: unittest.TestCase, err: Any) -> None:
        super().addFailure(test, err)
        self._record(test, "fail", self._exc_info_to_string(err, test))

    def addError(self, test: unittest.TestCase, err: Any) -> None:
        super().addError(test, err)
        self._record(test, "error", self._exc_info_to_string(err, test))

    def addSkip(self, test: unittest.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self._record(test, "skip", reason)

    def addExpectedFailure(self, test: unittest.TestCase, err: Any) -> None:
        super().addExpectedFailure(test, err)
        self._record(test, "xfail", self._exc_info_to_string(err, test))

    def addUnexpectedSuccess(self, test: unittest.TestCase) -> None:
        super().addUnexpectedSuccess(test)
        self._record(test, "xpass")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pulse Tiered Storage & Log-First Writers Opaque-Box E2E Runner",
    )
    parser.add_argument(
        "--tier",
        choices=["all", "self", "1", "2", "3", "4"],
        default="all",
        help="Tier to list or execute (default: all).",
    )
    parser.add_argument(
        "--feature",
        action="append",
        default=None,
        help="Filter to feature(s) F01..F31 (repeatable or comma-separated).",
    )
    parser.add_argument(
        "--milestone",
        choices=list(MILESTONES),
        default=None,
        help="Filter to a specific milestone (T0a..W3).",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "baseline"],
        default="full",
        help="Gating mode: 'full' (strict, no milestone skips) or 'baseline' (skips above --max-milestone).",
    )
    parser.add_argument(
        "--max-milestone",
        choices=list(MILESTONES),
        default=None,
        help="Max implemented milestone (defaults to T0a when --mode baseline).",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Discover and print exact counts by tier, feature, and milestone without running tests.",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Skip `cargo build -p pulse-node --locked` before running tests.",
    )
    parser.add_argument(
        "--binary",
        default=None,
        help="Explicit path to `pulse-node` binary.",
    )
    parser.add_argument(
        "--json-out",
        default=None,
        help="Write machine-readable JSON test report to this path.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose test output.",
    )
    parser.add_argument(
        "--failfast",
        action="store_true",
        help="Stop on first failure or error.",
    )
    return parser


def _write_json(path_str: str | None, payload: dict[str, Any]) -> None:
    if not path_str:
        return
    p = Path(path_str)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _prepare_binary(*, no_build: bool, explicit_binary: str | None) -> Path | None:
    repo_root = _HARNESS_ROOT.parents[1]
    if explicit_binary:
        p = Path(explicit_binary).resolve()
        if not p.is_file():
            raise SystemExit(f"Specified --binary does not exist: {p}")
        os.environ["E2E_TIERED_BINARY"] = str(p)
        return p

    if not no_build and (repo_root / "Cargo.toml").is_file():
        print("Building pulse-node (`cargo build -p pulse-node --locked`)...", flush=True)
        proc = subprocess.run(
            ["cargo", "build", "-p", "pulse-node", "--locked"],
            cwd=str(repo_root),
            check=False,
        )
        if proc.returncode != 0:
            raise SystemExit(f"cargo build -p pulse-node --locked failed with exit code {proc.returncode}")
        debug_bin = repo_root / "target" / "debug" / "pulse-node"
        if debug_bin.is_file():
            os.environ["E2E_TIERED_BINARY"] = str(debug_bin)
            return debug_bin

    for rel in ("target/release/pulse-node", "target/debug/pulse-node"):
        cand = repo_root / rel
        if cand.is_file():
            os.environ["E2E_TIERED_BINARY"] = str(cand)
            return cand
    return None


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    features_filter = _parse_features_arg(args.feature)

    eff_max = args.max_milestone
    if args.mode == "baseline" and eff_max is None:
        eff_max = "T0a"

    os.environ["E2E_TIERED_MODE"] = args.mode
    os.environ["E2E_TIERED_MAX_MILESTONE"] = eff_max or ""
    set_active_gating(mode=args.mode, max_milestone=eff_max or "")

    _, selected_cases = _discover_cases(
        tier_filter=args.tier,
        features_filter=features_filter,
        milestone_filter=args.milestone,
    )
    summary = _summarize(selected_cases)

    if args.list:
        for tc in selected_cases:
            meta = get_test_metadata(tc)
            feats = ",".join(meta["features"]) if meta["features"] else "-"
            ms = meta["milestone"] or "-"
            print(f"{tc.id()}  [tier={meta['tier']} | features={feats} | milestone={ms}]")
        print("-" * 72)
        print(f"Total selected: {summary['total']}")
        print(f"By tier:      {json.dumps(summary['by_tier'])}")
        print(f"By feature:   {json.dumps(summary['by_feature'])}")
        print(f"By milestone: {json.dumps(summary['by_milestone'])}")
        _write_json(
            args.json_out,
            {
                "schema_version": 1,
                "action": "list",
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "mode": args.mode,
                "max_milestone": eff_max,
                "filters": {
                    "tier": args.tier,
                    "features": list(features_filter) if features_filter else None,
                    "milestone": args.milestone,
                },
                "counts": summary,
                "tests": [
                    {
                        "id": tc.id(),
                        **get_test_metadata(tc),
                        "features": list(get_test_metadata(tc)["features"]),
                    }
                    for tc in selected_cases
                ],
            },
        )
        return 0

    if not selected_cases:
        print("Error: 0 test cases matched selection.", file=sys.stderr)
        _write_json(
            args.json_out,
            {
                "schema_version": 1,
                "action": "run",
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "mode": args.mode,
                "max_milestone": eff_max,
                "counts": summary,
                "summary": {"tests_run": 0, "executed": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0, "unexpected_skips": 0},
                "results": [],
            },
        )
        return 2

    _prepare_binary(no_build=args.no_build, explicit_binary=args.binary)

    suite = unittest.TestSuite(selected_cases)
    runner = unittest.TextTestRunner(
        verbosity=2 if args.verbose else 1,
        failfast=args.failfast,
        resultclass=_TrackingTestResult,
    )
    result = runner.run(suite)
    assert isinstance(result, _TrackingTestResult)

    skipped_count = len(result.skipped)
    executed_count = result.testsRun - skipped_count
    failed_count = len(result.failures)
    error_count = len(result.errors)
    passed_count = max(0, executed_count - failed_count - error_count - len(result.unexpectedSuccesses))

    unexpected_skips: list[dict[str, str]] = []
    for tc, reason in result.skipped:
        if reason.startswith("[external-tool]"):
            continue
        if eff_max is not None and reason.startswith("Milestone "):
            continue
        unexpected_skips.append({"id": tc.id(), "reason": reason})

    if args.mode == "full" and eff_max is None:
        # In strict full mode without --max-milestone, NO milestone skips are allowed
        for tc, reason in result.skipped:
            if not reason.startswith("[external-tool]") and not any(
                u["id"] == tc.id() for u in unexpected_skips
            ):
                unexpected_skips.append({"id": tc.id(), "reason": reason})

    exit_code = 0
    if failed_count > 0 or error_count > 0 or len(result.unexpectedSuccesses) > 0:
        exit_code = 1
    if args.mode == "full" and executed_count == 0:
        print("Error: 0 non-skipped tests executed in --mode full.", file=sys.stderr)
        exit_code = 1
    if args.mode == "full" and unexpected_skips:
        print(
            f"Error: {len(unexpected_skips)} unexpected skip(s) in --mode full:",
            file=sys.stderr,
        )
        for item in unexpected_skips:
            print(f"  - {item['id']}: {item['reason']}", file=sys.stderr)
        exit_code = 1

    _write_json(
        args.json_out,
        {
            "schema_version": 1,
            "action": "run",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "mode": args.mode,
            "max_milestone": eff_max,
            "filters": {
                "tier": args.tier,
                "features": list(features_filter) if features_filter else None,
                "milestone": args.milestone,
            },
            "counts": summary,
            "summary": {
                "tests_run": result.testsRun,
                "executed": executed_count,
                "passed": passed_count,
                "failed": failed_count,
                "errors": error_count,
                "skipped": skipped_count,
                "unexpected_skips": len(unexpected_skips),
            },
            "unexpected_skips": unexpected_skips,
            "results": result.records,
        },
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
