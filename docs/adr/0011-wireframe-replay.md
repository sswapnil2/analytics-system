# ADR-0011: Wireframe replay, masked on the device

> **Status:** Accepted, as implemented on `main`. **Evidence level:** the *decision* is verified in code. The repo holds no written design discussion, so the *rationale* below is **inferred** from the code, comments and tests, and is marked as such. Alternatives are listed only where the repo itself shows them.

## Decision
The SDKs record element boxes, kinds, colours and labels, with text masked to `████` *on the device*. They do not record pixels or DOM HTML. Inputs are always masked. Ops are sent as compact zstd `PLSR` frames (`sdks/ts-web/src/index.ts`, `crates/pulse-wire/src/replay.rs`). The server applies lexicon masking again before storing.

## Inferred rationale
Sensitive text never leaves the user's device, and payloads are small. The same model works for web and native.

## Alternatives (shown in the repo)
None recorded. There is no rrweb dependency.

## Trade-offs
- **+** Privacy by default; small payloads; one cross-platform format.
- **−** It is not a faithful visual reproduction (snapshots are capped at 120 elements, at most once a second).
