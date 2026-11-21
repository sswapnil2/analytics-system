# Contributing

Pulse is open source under Apache-2.0. The rules for getting a change merged (DCO sign-off, CI checks, one change per pull request) are in [CONTRIBUTING.md](../CONTRIBUTING.md). This page is the developer's map: where things live, how to run them, and where to start.

## Set up

Prerequisites:

- **Rust.** The toolchain pinned in `rust-toolchain.toml` (1.88); rustup installs it.
- **Node.js 22.** Needed for the Studio and the Web SDK.

```bash
git clone https://github.com/sswapnil2/analytics-system && cd analytics-system
npm --prefix web ci && npm --prefix web run build   # Studio → web/dist
cargo build -p pulse-node                           # debug build, fast to compile
./target/debug/pulse-node --seed                    # http://127.0.0.1:8080 with demo data
```

For Studio work, run the Vite dev server with `npm --prefix web run dev`. It serves on `:5173` and proxies `/v1` to the node on `:8080`.

## Test

These must pass in CI (`.github/workflows/rust.yml`):

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
npm --prefix web run typecheck && npm --prefix web test && npm --prefix web run build
npm --prefix sdks/ts-web run typecheck && npm --prefix sdks/ts-web test
```

Two more checks run against a live node:

- `./scripts/verify_e2e.sh`: an end-to-end check.
- `./scripts/bench_scale.sh 100k`: the latency SLO test, which asserts its budgets in release builds only.

## Where to make a change

| You want to change… | Start in | Tests next to it |
|---|---|---|
| What the SDK sends, JSON field aliases, batch format | `crates/pulse-wire/src/event.rs`, `batch.rs` | `crates/pulse-wire/tests` |
| Ingest validation, dedup, rate limits, status codes | `crates/pulse-ingest/src/service.rs`, `http.rs` | `crates/pulse-ingest/tests` |
| WAL, segments, flush, compaction, encryption | `crates/pulse-storage/src` | Unit tests in `crates/pulse-storage/src`; end to end in `crates/pulse-ingest/tests`, `crates/pulse-node/tests` |
| Identity merging | `crates/pulse-identity/src/lib.rs` | `crates/pulse-identity/tests` |
| Query semantics (funnels, retention, insights, flows) | `crates/pulse-exec/src/{funnel,retention,insights,flows}.rs`, `ast.rs` | `crates/pulse-exec/tests` (including `proptest_oracle.rs`) |
| A REST route, roles, auth | `crates/pulse-control/src/api.rs`, `api/accounts.rs`, `api/auth.rs` | `crates/pulse-control/tests` |
| Database schema | a new file in `crates/pulse-control/migrations/` (never edit an applied one) | `crates/pulse-control/tests` |
| MCP tools | `crates/pulse-control/src/mcp.rs` | `crates/pulse-control/tests` |
| Experiments stats | `crates/pulse-control/src/experiments.rs` | — |
| Node flags, wiring, metrics | `crates/pulse-node/src/{main,lib,metrics}.rs` | `crates/pulse-node/tests` |
| Studio | `web/src/workspaces/*` | `web/src/lib/*.test.ts` |
| Web SDK | `sdks/ts-web/src/index.ts` | `sdks/ts-web/test/sdk.test.mjs` |
| These docs | `docs/*.md`; the site viewer is `site/docs/index.html` | Preview steps below |

See the [architecture](architecture.md) and [How Pulse works](how-it-works.md) for how the pieces fit together.

## Expectations for a pull request

- **Counting changes need a test that pins the numbers.** A test with expected counts, or an oracle case, for anything in ingest, storage or query.
- **Update the docs when behaviour changes.** Any change to an API, flag, env var or limit updates `docs/`, including [Configuration](configuration.md) and [Limitations](limitations.md).
- **Show UI changes.** Add a screenshot.
- **Sign off every commit.** Use `git commit -s` (DCO).

## Working on the docs

The docs are plain Markdown in `docs/`, readable on GitHub and on the website. The website viewer (`site/docs/index.html`) renders the same files in the browser, Mermaid diagrams included. On deploy, the Pages workflow runs `scripts/build_docs_site.py`. It copies `docs/*.md` and `docs/adr/` into `site/docs/md/`, and writes `site/llms.txt` (an index for AI agents) and `site/llms-full.txt` (every page in one file). To preview locally:

```bash
python3 scripts/build_docs_site.py
python3 -m http.server 8000 --directory site    # open http://localhost:8000/docs/
```

The generated files are git-ignored. Run `python3 scripts/check_docs_links.py` before you push: CI runs it too, and fails on any broken relative link, heading anchor or nav entry. To add a page, write `docs/<name>.md` and add it to the `NAV` list at the top of the script in `site/docs/index.html`. Link between pages with relative `.md` links, as on GitHub. The viewer rewrites them, and sends links outside `docs/` to GitHub.

## Cutting a release

1. **Bump the version.** Set `[workspace.package] version` in `Cargo.toml` to the new version. All crates inherit it, and so do `pulse-node --version` and the MCP `serverInfo`. Run `cargo update --workspace --offline` to update `Cargo.lock`.
2. **Update the changelog.** Move the *Unreleased* entries in `CHANGELOG.md` under a new `[x.y.z] - YYYY-MM-DD` heading, and add the compare link at the bottom.
3. **Merge both** to `main` and wait for CI to pass.
4. **Tag `main`:** `git tag -a vX.Y.Z -m "Pulse vX.Y.Z" && git push origin vX.Y.Z`.

`.github/workflows/release.yml` then:
- builds and smoke-tests the Linux tarballs;
- pushes `ghcr.io/sswapnil2/pulse:X.Y.Z` and `:latest`;
- packs the Web SDK at version `X.Y.Z`;
- publishes the GitHub release with `SHA256SUMS`.

A tag containing `-` (for example `v0.2.0-rc1`) is published as a prerelease.

## Where to start

- **Pick from the list of gaps.** [Docs status & roadmap](docs-status.md) and [Limitations](limitations.md) list known gaps. Many are good first issues, such as a server SDK, retention TTLs, or React Native docs and CI.
- **Wrong numbers are the most valuable bug reports.** Include the events you sent and the result you expected.
- **Security issues go to private reporting.** See [SECURITY.md](../SECURITY.md).

Everyone taking part follows the [Code of Conduct](../CODE_OF_CONDUCT.md).
