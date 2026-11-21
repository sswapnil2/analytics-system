# ADR-0005: Immutable columnar segments, with footers kept in a CAS-committed manifest

> **Status:** Accepted, as implemented on `main`. **Evidence level:** the *decision* is verified in code. The repo holds no written design discussion, so the *rationale* below is **inferred** from the code, comments and tests, and is marked as such. Alternatives are listed only where the repo itself shows them.

## Context
Most analytics queries touch a few columns over many rows, and many can be answered, or pruned, from summary statistics.

## Decision
- **Segments.** L0 flushes into immutable [Vortex](https://github.com/spiraldb/vortex) columnar segments, one per `(day, bucket)`. Each segment also carries a Roaring-bitmap inverted index and a session index (`crates/pulse-storage/src/segment.rs`).
- **Footers live in the manifest.** Each segment's *footer* sits in the manifest, not in the file. It holds row and session counts, the time range, a 64-bit event bloom filter, exact per-event counts, and HLL sketches for the top 32 events.
- **Manifest commits.** A new manifest epoch is committed with create-if-absent semantics, and a losing writer retries (`crates/pulse-storage/src/manifest.rs`).
- **Compaction.** It merges segments when a `(day, bucket)` has 4 or more of them.

## Inferred rationale
Footers in the manifest let the planner prune segments, and answer unfiltered totals and uniques, without reading any segment (tests in `crates/pulse-exec/tests/limits.rs` assert zero segment reads for those paths). Immutability means the segment cache needs no invalidation.

## Alternatives (shown in the repo)
Parquet and Arrow are dependencies too, used for lookup tables and warehouse export, but segments use Vortex. The reasons for that choice are not recorded.

## Trade-offs
- **+** Snapshot isolation for readers; cheap pruning; zero-I/O fast paths.
- **−** Manifest size grows with segment count; it is mitigated by compaction and zstd.
- **−** A custom storage format that must be maintained in-house.
