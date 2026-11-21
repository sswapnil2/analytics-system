# ADR-0006: Resolve identity at query time and never rewrite history

> **Status:** Accepted, as implemented on `main`. **Evidence level:** the *decision* is verified in code. The repo holds no written design discussion, so the *rationale* below is **inferred** from the code, comments and tests, and is marked as such. Alternatives are listed only where the repo itself shows them.

## Context
Users act anonymously, then log in. Their earlier events should count as theirs.

## Decision
Stored events keep the dense id of the distinct id they were sent with. An identity graph (union-find in SlateDB, `crates/pulse-identity`) maps ids to a canonical user. Every query resolves ids through an identity snapshot. Merging two *identified* users is refused (`409`).

## Inferred rationale
Rewriting history would mean rewriting immutable segments on every login. Resolving at read time keeps merges O(1) and retroactive. Refusing identified-to-identified merges prevents one bad `identify` call from fusing two real people.

## Trade-offs
- **+** Merges are instant and retroactive; segments stay immutable.
- **−** Every query pays the cost of resolving ids; the graph must be loaded or snapshotted.
- **−** No un-merge operation was found.
