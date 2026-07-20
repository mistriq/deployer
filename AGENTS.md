# Hosting execution engine instructions

## Mission

Turn this repository into the private, production-grade execution engine for Light Apps Hosting. The customer browser must never call Deployer directly. The only caller of hosting-specific endpoints is the hosting control plane through authenticated internal APIs.

Read `HOSTING_IMPLEMENTATION_PLAN.md` and `HOSTING_TODO.md` before changing code. Work through the dependency order in those files and keep `HOSTING_TODO.md` current.

## Hard scope

You may change files only inside `/home/karel/projects/deployer`.

This repository owns:

- scoped service authentication for the hosting control plane,
- idempotent project provisioning and deployment orchestration,
- exact commit/artifact identity,
- build and runtime job state,
- immutable releases, health gating, cancel and rollback,
- signed callbacks plus polling reconciliation,
- runner capabilities, heartbeats, capacity and stable failure codes,
- the private client for the colleague-owned versioned reverse-proxy adapter,
- log/secret redaction and execution-engine audit data.

This repository does not own:

- customer accounts, sessions or customer-facing UI,
- GitHub OAuth/App installation UI,
- Stripe products, checkout, invoices or subscription policy,
- public domain verification UX,
- reverse-proxy implementation or direct configuration-file writes,
- marketing pages, CRM or the existing Socials Century dashboard.

Implement only the private contracts required by the hosting control plane. Do not turn the existing Deployer admin UI into a customer dashboard.

## Quality rules

- Build production behavior, not demos, mock application paths, hard-coded success responses or temporary fake implementations.
- Test doubles and fixture repositories are allowed only in automated tests.
- Never claim a feature is complete when only the happy path or UI exists.
- Preserve all pre-existing and uncommitted user changes. Inspect `git status` and diffs before editing overlapping files.
- The repository currently contains concurrent uncommitted hosting WIP. Start with an inventory and green baseline; never reset, checkout, stash, delete or overwrite it. Do not add or remove `deployer.db.bak-*` without explicit user approval.
- Never read, print, commit or rotate real production secrets. Never deploy to production, change DNS, modify a live reverse proxy, push branches or publish releases without explicit user authorization.
- Do not weaken authentication, isolation, validation or tests to make a check pass.
- Public API compatibility matters. Version internal hosting endpoints under `/api/internal/v1` and document changes in OpenAPI.
- Every mutating machine endpoint must be authenticated, scoped, idempotent and auditable.
- Structured state and stable error codes are authoritative; clients must never parse human log text.
- Keep the legacy trusted-admin deployment path working. Hosting execution uses separate models/jobs and generated static/Node recipes; it must not expose legacy Compose or `post_deploy` capabilities.
- Run proportional tests after each task and the full verification suite before declaring the goal complete.

## Delegation

Use subagents for bounded independent work. At the start of a substantial phase, delegate at least:

1. a read-only architecture/security review,
2. a test-gap or concurrency review,
3. an OpenAPI/documentation consistency review when contracts change.

Only one agent may edit a given file set at a time. Prefer read-only subagents; the primary agent integrates changes, waits for all requested results, and reruns the complete suite. Do not let subagents edit `/home/karel/projects/hosting` or any reverse-proxy repository.

## Verification and completion

Minimum verification:

```bash
gofmt -w <changed-go-files>
go test ./...
go test -race ./...
go vet ./...
git diff --check
```

Run `govulncheck ./...` when available. Update `docs/openapi.yaml`, README and tests together with API changes.

The goal is complete only when every in-scope item in `HOSTING_TODO.md` is checked, all required tests pass, no P0/P1 correctness or security issue remains, and no placeholder production path exists. If an external credential or live service blocks a test, record the exact blocker and reproducible verification command in `BLOCKERS.md`, continue all other in-scope work, and do not falsely mark the blocked item complete.
