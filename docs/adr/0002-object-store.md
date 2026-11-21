# ADR-0002: The object store is the only durable layer for event data

> **Status:** Accepted, as implemented on `main`. **Evidence level:** the *decision* is verified in code. The repo holds no written design discussion, so the *rationale* below is **inferred** from the code, comments and tests, and is marked as such. Alternatives are listed only where the repo itself shows them.

## Context
Event data must survive crashes. It should run on a laptop disk and also on S3 without a second code path.

## Decision
All event-side persistence goes through the `object_store` crate: WAL, segments, manifests, replays, and the SlateDB identity and profile stores. The default is `DurableLocalObjectStore`, which `fsync`s each file and its parent directory. `PULSE_OBJECT_STORE_URL` selects `s3://`, `file://` or `memory://` (`crates/pulse-storage/src/store_factory.rs`).

## Inferred rationale
One interface means one code path for laptop and cloud. Object stores give simple atomic primitives (whole-object PUT, create-if-absent), and the WAL and manifest protocols are built on exactly those (see [ADR-0003](0003-wal-before-ack.md) and [ADR-0005](0005-segments-and-manifests.md)).

## Trade-offs
- **+** S3-compatible storage (S3, R2, MinIO) works with no code changes; encryption wraps one interface ([ADR-0010](0010-envelope-encryption.md)).
- **−** One WAL object per append means many small objects, and on S3 each one is a request.
- **−** No streaming log; durability is per request.

## Consequences
Configuration and accounts are deliberately *not* in the object store; they live in SQL ([ADR-0009](0009-metadata-in-sql.md)).
