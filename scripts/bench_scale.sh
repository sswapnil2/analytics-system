#!/usr/bin/env bash
set -euo pipefail

SCALE="${1:-1m}"

run_profile() {
  local profile="$1"
  echo ">>> Running Pulse Analytics Scale & SLO Benchmark: scale=${profile}"
  PULSE_BENCH_SCALE="${profile}" cargo test --release -p pulse-exec --test scale_slo -- --nocapture
}

if [ "${SCALE}" = "all" ]; then
  run_profile "100k"
  run_profile "1m"
else
  run_profile "${SCALE}"
fi
