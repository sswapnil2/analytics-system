# Contributing to Pulse

Thanks for helping. This page covers how to build Pulse, test a change and get it merged.

## Before you start

- **Bugs:** open an issue with the bug template. Include the Pulse commit, how you ran the node and the smallest request or dataset that reproduces it. Wrong numbers are the most valuable bug reports we get: please include the events you sent and the result you expected.
- **Features:** open an issue first so we can agree on the shape before you write code.
- **Security problems:** do not open a public issue. Follow [SECURITY.md](SECURITY.md).

## Build

Prerequisites:

- the Rust toolchain pinned in `rust-toolchain.toml` (rustup installs it on first use);
- Node.js 22 for the Studio and the web SDK.

```bash
npm --prefix web ci && npm --prefix web run build
cargo build -p pulse-node              # debug build; much faster to compile than --release
./target/debug/pulse-node --seed       # Studio, API and MCP on http://127.0.0.1:8080
```

See [docs/quickstart.md](docs/quickstart.md) for tokens, ingest keys and flags.

## Test

CI runs these on every pull request, and they must pass:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

If you touched the Studio or the SDK, also run:

```bash
npm --prefix web run typecheck && npm --prefix web test && npm --prefix web run build
npm --prefix sdks/ts-web run typecheck && npm --prefix sdks/ts-web test
```

Changes to ingest, storage or query code should add a test that pins the expected numbers. A counting bug that ships is far more expensive than one caught in review.

## Pull requests

- Keep one change per pull request, and keep it as small as it can be.
- Describe what changed and why. For UI changes, add a screenshot.
- Update `docs/` when behaviour, an API or a flag changes.
- Sign off every commit (see below).

## Sign your commits (DCO)

Pulse uses the [Developer Certificate of Origin](https://developercertificate.org/). It certifies that you wrote the change, or otherwise have the right to submit it under the project's licence. Add a `Signed-off-by` line to each commit:

```bash
git commit -s -m "fix(funnel): count ties on the lowest step first"
```

Contributions are accepted under the [Apache License 2.0](LICENSE), the same licence as the project.

## Conduct

Everyone taking part is expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
