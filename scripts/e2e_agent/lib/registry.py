"""Deterministic test case registry, discovery, and coverage threshold verification."""

from __future__ import annotations

import importlib
import importlib.util
import re
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

VALID_FEATURES: tuple[str, ...] = tuple(f"F{i:02d}" for i in range(1, 25))
VALID_TIERS: tuple[int, ...] = (1, 2, 3, 4)

TIER_THRESHOLDS: dict[int, int] = {
    1: 120,
    2: 120,
    3: 24,
    4: 12,
}
PER_FEATURE_THRESHOLDS: dict[int, int] = {
    1: 5,
    2: 5,
}
TOTAL_THRESHOLD: int = 276


class ThresholdError(AssertionError):
    """Raised when discovered test cases fail minimum coverage thresholds."""

    def __init__(self, violations: Sequence[str]) -> None:
        self.violations: list[str] = list(violations)
        summary = "\n".join(f"  - {item}" for item in self.violations)
        super().__init__(f"Coverage threshold verification failed:\n{summary}")


@dataclass(frozen=True, slots=True)
class TestCase:
    """Metadata and callable for a single registered E2E test case."""

    id: str
    tier: int
    feature: str
    title: str
    fn: Callable[[Any], None]
    tags: tuple[str, ...] = ()
    module: str = ""


class TestRegistry:
    """Thread-unsafe deterministic registry storing E2E test cases by unique ID."""

    def __init__(self) -> None:
        self._cases: dict[str, TestCase] = {}
        self._imported_paths: set[Path] = set()

    def register(self, case: TestCase) -> TestCase:
        """Validate and register a TestCase, rejecting invalid metadata or duplicate IDs."""
        if not isinstance(case.id, str) or not case.id.strip():
            raise ValueError(f"Invalid test case id: {case.id!r}")
        if isinstance(case.tier, bool) or not isinstance(case.tier, int) or case.tier not in VALID_TIERS:
            raise ValueError(f"Invalid tier {case.tier!r} for test {case.id!r}; expected one of {VALID_TIERS}")
        if case.feature not in VALID_FEATURES:
            raise ValueError(f"Invalid feature {case.feature!r} for test {case.id!r}; expected F01..F24")
        if not isinstance(case.title, str) or not case.title.strip():
            raise ValueError(f"Invalid title {case.title!r} for test {case.id!r}")
        if not callable(case.fn):
            raise ValueError(f"Test case {case.id!r} fn is not callable")
        if case.id in self._cases:
            raise ValueError(f"Duplicate test case ID: {case.id}")
        self._cases[case.id] = case
        return case

    def all_cases(self) -> list[TestCase]:
        """Return all registered test cases sorted deterministically by (tier, id)."""
        return sorted(self._cases.values(), key=lambda c: (c.tier, c.id))

    def has_imported(self, resolved_path: Path) -> bool:
        """Return True if a file path has already been imported into this registry."""
        return resolved_path in self._imported_paths

    def mark_imported(self, resolved_path: Path) -> None:
        """Record a resolved file path as imported into this registry."""
        self._imported_paths.add(resolved_path)

    def clear(self) -> None:
        """Clear all registered cases and imported path records."""
        self._cases.clear()
        self._imported_paths.clear()


_DEFAULT_REGISTRY = TestRegistry()
_ACTIVE_DISCOVERY_REGISTRY: TestRegistry | None = None


def _target_registry(explicit: TestRegistry | None) -> TestRegistry:
    if explicit is not None:
        return explicit
    if _ACTIVE_DISCOVERY_REGISTRY is not None:
        return _ACTIVE_DISCOVERY_REGISTRY
    return _DEFAULT_REGISTRY


def register_case(case: TestCase, *, registry: TestRegistry | None = None) -> TestCase:
    """Register an already-constructed TestCase into the target registry."""
    return _target_registry(registry).register(case)


def test_case(
    id: str,
    tier: int,
    feature: str,
    title: str,
    tags: Iterable[str] = (),
    *,
    registry: TestRegistry | None = None,
) -> Callable[[Callable[[Any], None]], Callable[[Any], None]]:
    """Decorator registering an E2E test function in the deterministic test registry."""

    def decorator(fn: Callable[[Any], None]) -> Callable[[Any], None]:
        normalized_tags = tuple(str(t) for t in tags)
        case = TestCase(
            id=id,
            tier=tier,
            feature=feature,
            title=title,
            fn=fn,
            tags=normalized_tags,
            module=getattr(fn, "__module__", ""),
        )
        register_case(case, registry=registry)
        setattr(fn, "_test_case", case)
        return fn

    return decorator


def get_registered_cases(*, registry: TestRegistry | None = None) -> list[TestCase]:
    """Return all registered test cases sorted by (tier, id)."""
    return _target_registry(registry).all_cases()


def _ensure_harness_root_on_path() -> Path:
    harness_root = Path(__file__).resolve().parent.parent
    harness_str = str(harness_root)
    if harness_str not in sys.path:
        sys.path.insert(0, harness_str)
    return harness_root


def _import_tier_file(path: Path, *, default_tests_dir: Path, target_reg: TestRegistry) -> None:
    global _ACTIVE_DISCOVERY_REGISTRY
    resolved = path.resolve()
    if target_reg.has_imported(resolved):
        return

    use_package_import = (
        target_reg is _DEFAULT_REGISTRY
        and resolved.parent == default_tests_dir.resolve()
    )
    prev_active = _ACTIVE_DISCOVERY_REGISTRY
    _ACTIVE_DISCOVERY_REGISTRY = target_reg
    try:
        if use_package_import:
            mod_name = f"tests.{resolved.stem}"
            if mod_name in sys.modules:
                existing_file = getattr(sys.modules[mod_name], "__file__", None)
                if existing_file and Path(existing_file).resolve() == resolved:
                    target_reg.mark_imported(resolved)
                    return
            importlib.import_module(mod_name)
        else:
            mod_name = f"_e2e_tier_{resolved.stem}_{id(target_reg)}_{abs(hash(str(resolved)))}"
            spec = importlib.util.spec_from_file_location(mod_name, resolved)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not create module spec for {resolved}")
            module = importlib.util.module_from_spec(spec)
            sys.modules[mod_name] = module
            try:
                spec.loader.exec_module(module)
            except Exception:
                sys.modules.pop(mod_name, None)
                raise
        target_reg.mark_imported(resolved)
    finally:
        _ACTIVE_DISCOVERY_REGISTRY = prev_active


def _matches_filter(case: TestCase, pattern: re.Pattern[str]) -> bool:
    if pattern.search(case.id):
        return True
    if pattern.search(case.feature):
        return True
    if pattern.search(case.title):
        return True
    return any(pattern.search(tag) for tag in case.tags)


def discover_tests(
    tiers: Iterable[int] | None = None,
    filter_expr: str | None = None,
    *,
    tests_dir: Path | None = None,
    registry: TestRegistry | None = None,
) -> list[TestCase]:
    """Discover and import `tests/tier*.py` deterministically, then filter and sort cases."""
    harness_root = _ensure_harness_root_on_path()
    default_tests_dir = harness_root / "tests"
    target_dir = Path(tests_dir) if tests_dir is not None else default_tests_dir
    target_reg = registry if registry is not None else _DEFAULT_REGISTRY

    tier_set: set[int] | None = None
    if tiers is not None:
        tier_set = set()
        for t in tiers:
            if isinstance(t, bool) or not isinstance(t, int) or t not in VALID_TIERS:
                raise ValueError(f"Invalid tier filter {t!r}; expected one of {VALID_TIERS}")
            tier_set.add(t)

    if target_dir.is_dir():
        for path in sorted(target_dir.glob("tier*.py"), key=lambda p: p.name):
            _import_tier_file(path, default_tests_dir=default_tests_dir, target_reg=target_reg)

    cases = target_reg.all_cases()
    if tier_set is not None:
        cases = [c for c in cases if c.tier in tier_set]

    if filter_expr is not None and filter_expr != "":
        try:
            pattern = re.compile(filter_expr, re.IGNORECASE)
        except re.error:
            pattern = re.compile(re.escape(filter_expr), re.IGNORECASE)
        cases = [c for c in cases if _matches_filter(c, pattern)]

    return sorted(cases, key=lambda c: (c.tier, c.id))


def summarize_counts(cases: Sequence[TestCase]) -> dict[str, Any]:
    """Summarize case counts by tier and by feature within each tier."""
    by_tier: dict[int, int] = {tier: 0 for tier in VALID_TIERS}
    by_feature_per_tier: dict[int, dict[str, int]] = {
        tier: {feat: 0 for feat in VALID_FEATURES} for tier in VALID_TIERS
    }
    seen_ids: set[str] = set()
    duplicates: list[str] = []

    for case in cases:
        if case.id in seen_ids:
            duplicates.append(case.id)
        seen_ids.add(case.id)
        if case.tier in by_tier:
            by_tier[case.tier] += 1
        if case.tier in by_feature_per_tier and case.feature in by_feature_per_tier[case.tier]:
            by_feature_per_tier[case.tier][case.feature] += 1

    return {
        "total": len(cases),
        "by_tier": by_tier,
        "by_feature_per_tier": by_feature_per_tier,
        "duplicate_ids": duplicates,
    }


def verify_thresholds(cases: Sequence[TestCase]) -> dict[str, Any]:
    """Enforce minimum tier, per-feature, uniqueness, and total E2E test thresholds."""
    summary = summarize_counts(cases)
    violations: list[str] = []

    if summary["duplicate_ids"]:
        dups = ", ".join(sorted(set(summary["duplicate_ids"])))
        violations.append(f"Duplicate test case IDs detected: {dups}")

    for tier, minimum in TIER_THRESHOLDS.items():
        actual = summary["by_tier"][tier]
        if actual < minimum:
            violations.append(f"Tier {tier} count {actual} < required {minimum}")

    for tier, min_per_feat in PER_FEATURE_THRESHOLDS.items():
        feat_counts = summary["by_feature_per_tier"][tier]
        under = [f"{feat}={feat_counts[feat]}" for feat in VALID_FEATURES if feat_counts[feat] < min_per_feat]
        if under:
            violations.append(
                f"Tier {tier} features below minimum {min_per_feat}: {', '.join(under)}"
            )

    if summary["total"] < TOTAL_THRESHOLD:
        violations.append(f"Total test count {summary['total']} < required {TOTAL_THRESHOLD}")

    if violations:
        raise ThresholdError(violations)

    return summary
