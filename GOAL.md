# Goal: production hosting execution engine

Complete the entire production hosting execution-engine scope defined by `AGENTS.md`, `HOSTING_IMPLEMENTATION_PLAN.md` and `HOSTING_TODO.md`.

## Required outcome

Deliver a private, production-grade Deployer path that the hosting control plane can use to provision supported projects, deploy an exact commit through an immutable artifact, observe durable phases, activate only healthy releases through the colleague-owned proxy adapter, cancel, suspend/resume and roll back safely, receive/reconcile signed callbacks, and recover after duplicate requests or process/runner failure.

The automatic path supports only platform-generated static and allowlisted Node HTTP workloads. It must not expose customer Dockerfiles, Compose, arbitrary shell, host mounts, privileged execution, local production databases, cron or workers.

## Autonomy and delegation

- Work autonomously through the full dependency order until every in-scope TODO and verification gate is complete.
- Spawn bounded subagents for read-only architecture/security review, concurrency/test review and OpenAPI consistency. Wait for them, integrate findings and rerun verification.
- Keep one writer per file set. Do not let subagents edit the hosting control-plane or reverse-proxy repositories.
- Make reversible implementation decisions independently when the plans already define the intended behavior.
- Stop only for a genuinely irreversible/external decision or unavailable credential/infrastructure. Record such items precisely in `BLOCKERS.md` and continue every other task.

## Hard constraints

- Change only `/home/karel/projects/deployer`.
- Preserve all current modified/untracked work. Never reset, checkout, stash, delete or overwrite it. Do not touch `deployer.db.bak-*` without explicit approval.
- Begin by inventorying and stabilizing the current concurrent WIP. Do not trust existing checked/partial work until fresh/upgrade migrations and the full baseline pass.
- Preserve the legacy trusted-admin deployment path.
- No demo, placeholder, fake production success, silent fallback, `not implemented` production endpoint or UI-only completion.
- Test fixtures/fake proxy are allowed only in tests; real staging acceptance remains required.
- Do not deploy production, change DNS/proxy, rotate real secrets, push or publish without explicit user authorization.
- Never mark a TODO complete until implementation, tests and documentation pass.

## Completion proof

Completion requires all checkboxes in `HOSTING_TODO.md`, valid OpenAPI/docs, no open P0/P1 security/correctness/data-loss issue, no placeholder production path, and successful formatting, `go test ./...`, `go test -race ./...`, `go vet ./...`, `git diff --check`, `govulncheck ./...` when available, plus the real staging static/Node activation and rollback gates defined in the plan. Report exact changed files, verification results and any remaining credential-only blocker. Do not claim the goal complete while any required gate is incomplete.
