# ADR-0010: Optional envelope encryption for all stored objects

> **Status:** Accepted, as implemented on `main`. **Evidence level:** the *decision* is verified in code. The repo holds no written design discussion, so the *rationale* below is **inferred** from the code, comments and tests, and is marked as such. Alternatives are listed only where the repo itself shows them.

## Decision
If `PULSE_BYOK_MASTER_KEY` is set, `EncryptedObjectStore` wraps the object store. Every object gets its own AES-256-GCM data key, which is wrapped by the master key. The object path is bound into the AAD, and data is encrypted in 64 KiB chunks so byte ranges can still be read (`crates/pulse-storage/src/encrypted_store.rs`). Stored secrets in SQL are separately sealed by a cluster KEK (`PULSE_VAULT_KEK`).

## Inferred rationale
Wrapping the single storage interface ([ADR-0002](0002-object-store.md)) encrypts everything at once: WAL, segments, manifests, replays and profiles. Chunking preserves the range reads that the query engine relies on.

## Trade-offs
- **+** One switch covers all event data; keys can be operator-held.
- **−** Off by default. Plaintext objects stay readable unless `PULSE_ENCRYPTION_STRICT=1`.
- **−** A passphrase is stretched with a fixed salt.
- **−** Persistence of rotated keys is not verified.
