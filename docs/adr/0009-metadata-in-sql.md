# ADR-0009: Configuration and accounts in SQL (SQLite or Postgres)

> **Status:** Accepted, as implemented on `main`. **Evidence level:** the *decision* is verified in code. The repo holds no written design discussion, so the *rationale* below is **inferred** from the code, comments and tests, and is marked as such. Alternatives are listed only where the repo itself shows them.

## Decision
Organisations, users, sessions, projects, keys, lexicon, cohorts, dashboards, flags, experiments, alerts, BYOK secrets and audit logs live in a relational DB through `sqlx`, with migrations in `crates/pulse-control/migrations/`. SQLite in the data directory is the default; set `DATABASE_URL=postgres://…` for multi-node.

## Inferred rationale
This data is small, relational and transactional: uniqueness of keys, emails and flag keys, plus role checks. SQLite keeps the single-binary promise, and Postgres allows nodes to share state.

## Trade-offs
- **+** Transactions and unique constraints for configuration; easy backup.
- **−** Two stores to keep consistent (SQL and the object store).
- **−** Only one foreign key is enforced; the other relationships are kept by application code.
- **−** There is legacy duplication (`orgs` and `organizations`).
