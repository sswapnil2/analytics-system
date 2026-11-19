# Security policy

Pulse stores behavioural data about your users, so we treat security reports as a priority.

## Reporting a vulnerability

**Please do not open a public issue, discussion or pull request for a security problem.**

Report it privately through GitHub: go to the repository's **Security** tab and choose **Report a vulnerability**. This opens a private advisory that only the maintainers can see.

Please include:

- the affected component (ingest, query engine, Studio, SDK, MCP, auth);
- the Pulse commit or version;
- steps or a proof of concept that reproduces the problem;
- the impact you believe it has.

## What to expect

- We acknowledge new reports within **3 working days**.
- We send an initial assessment within **10 working days**.
- We agree a disclosure date with you, and credit you in the advisory unless you ask us not to.

## Supported versions

Pulse has not reached 1.0. Security fixes land on `main` and in the next release; older commits are not patched.

## Scope

In scope:

- authentication and authorisation (root, project and ingest keys, Studio sessions);
- cross-project data access;
- privacy failures, such as replay masking leaking typed values or erased users remaining queryable;
- injection, remote code execution and denial of service in the node.

Out of scope:

- problems that need an already-leaked root token;
- findings that only affect a demo node started with `--seed`.
