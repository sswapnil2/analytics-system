# ADR-0007: Exact results by default, with disclosed sampling only above a threshold

> **Status:** Accepted, as implemented on `main`. **Evidence level:** the *decision* is verified in code. The repo holds no written design discussion, so the *rationale* below is **inferred** from the code, comments and tests, and is marked as such. Alternatives are listed only where the repo itself shows them.

## Context
Wrong analytics numbers destroy trust. CONTRIBUTING.md says: "A counting bug that ships is far more expensive than one caught in review."

## Decision
`SamplingMode::Auto` is the default. A query stays exact below `PULSE_AUTO_SAMPLE_MIN_ROWS` (10 M estimated rows). Above it, the engine reads whole buckets at a rate of 25 %, 12.5 % or 6.25 % and scales the result. Every result carries a `sampling` object with the rate and the estimated error, and `Exact` can be forced (`crates/pulse-exec/src/ast.rs`, `scanner.rs`). Correctness is checked against an independent oracle (`crates/pulse-exec/tests/proptest_oracle.rs`).

## Trade-offs
- **+** Small and medium projects never see estimates; large ranges stay interactive.
- **−** Large-range results are estimates unless forced exact, which costs time and memory.
