# Hosting WIP inventory

Snapshot started 2026-07-20 before implementation changes by this goal. This
inventory records the inherited uncommitted work so it can be integrated
without reset, stash, checkout, or broad replacement. Classifications describe
the state against `HOSTING_IMPLEMENTATION_PLAN.md`, not whether existing narrow
tests happened to pass.

The initial baseline (`go test ./...`, `go test -race ./...`, `go vet ./...`,
and `git diff --check`) passed. During the read-only review, another concurrent
writer began moving hosting persistence out of legacy tables; the worktree was
temporarily non-compiling. Schema ownership must therefore remain serialized
until that edit settles, followed by a fresh baseline.

| Path | Inherited state | Classification and integration note |
| --- | --- | --- |
| `.env.example` | Modified | Partial: documents three path/runner settings for the legacy-backed draft; the production subsystem needs callback, proxy, capacity, retention, kill-switch, and recovery settings. |
| `README.md` | Modified | Partial: documents service tokens and three draft endpoints, but paths/contracts differ from the required API and lifecycle/operations/runbooks are absent. |
| `docs/openapi.yaml` | Modified | Partial: documents service-token CRUD and draft provision/deploy/status only; required operations and many actual error/content constraints are missing or inconsistent. |
| `internal/app/api_errors.go` | Modified | Partial: adds draft hosting/auth codes; lifecycle, idempotency, callback, capacity, proxy, and transition codes remain undefined. |
| `internal/app/builder.go` | Modified | Failing scope: draft hosting deployments enter the trusted legacy builder, which can execute Dockerfiles, Compose, and current HEAD. Hosting must use separate jobs and generated structured recipes. |
| `internal/app/capabilities.go` | Modified | Partial/overclaiming: advertises provision/signature features before the complete production path exists. |
| `internal/app/capabilities_test.go` | Modified | Partial: checks only advertised draft flags/codes and cannot prove hosting behavior. |
| `internal/app/config.go` | Modified | Partial: adds derived hosting roots and a fixed legacy runner ID; production configuration is incomplete. |
| `internal/app/config_test.go` | Modified | Partial: covers only the three draft settings. |
| `internal/app/main.go` | Modified | Partial: mounts service-token and internal draft routes, but does not start hosting reconciliation, callback, cleanup, or metrics workers. |
| `internal/app/models.go` | Modified, concurrent | Partial/failing during inventory: initial draft added hosting identity to legacy tables; a concurrent writer began separate hosting tables. Must integrate only after the writer settles and fresh/legacy migrations compile and pass. |
| `internal/app/models_test.go` | Modified | Partial/failing during concurrent schema rewrite: migration count/column assertions described the earlier legacy-backed draft. Needs fresh and representative legacy upgrades for all final hosting tables/indexes. |
| `internal/app/security.go` | Modified | Complete only for the narrow CSRF boundary: internal machine routes are excluded from browser CSRF and still require their own bearer middleware. Full request/auth security remains incomplete elsewhere. |
| `AGENTS.md` | Untracked | Governing repository instructions; preserve and follow. |
| `GOAL.md` | Untracked | Governing goal; preserve and follow. |
| `HOSTING_IMPLEMENTATION_PLAN.md` | Untracked | Governing implementation plan; preserve and follow. |
| `HOSTING_TODO.md` | Untracked | Governing completion checklist; keep current only after implementation and verification. |
| `deployer.db.bak-20260717-afterround-move` | Untracked | Protected user backup; unrelated to code implementation and must remain untracked and untouched without explicit approval. |
| `internal/app/hosting_projects.go` | Untracked | Partial/failing scope: strict manifest parsing/signing/path derivation are useful, but persistence/execution is wired to legacy `Project` and a process-local mutex. |
| `internal/app/hosting_projects_test.go` | Untracked | Partial: useful validation/signature/concurrency cases, but tests the legacy-backed design and omits exact input, lifecycle, recovery, redaction, and failure gates. |
| `internal/app/internal_api.go` | Untracked | Partial/security-critical: only draft provision/deploy/status exist; numeric fallback can reach/read trusted-admin objects and required endpoints are absent. |
| `internal/app/service_tokens.go` | Untracked | Partial/security-critical: tokens are hashed and scoped, but rotation immediately invalidates the old credential and audit events are absent. Admin JSON parsing also lacks final strict limits. |
| `internal/app/service_tokens_test.go` | Untracked | Partial: covers hashing/scope/revocation and immediate rotation, but the rotation expectation conflicts with required overlap and there are no audit/concurrency tests. |

Files initially unmodified are not inherited WIP. They still require review when
they form part of a hosting boundary (notably runner/agent execution, logging,
redaction, `SECURITY.md`, and `CHANGELOG.md`).

## Current-goal re-audit — 2026-07-20

The new goal began at commit `f623d34`. All tracked work from the earlier
inventory had been integrated into four local commits and the tracked worktree
was clean. The only untracked path was
`deployer.db.bak-20260717-afterround-move`; it is unrelated, protected user data
and was not opened, modified, staged or committed. Its inventory metadata was
size 1,204,224 bytes and modification time 2026-07-17 05:57:13 +0200.

The fresh baseline passed `go test ./...`, `go test -race ./...`, `go vet ./...`
and `git diff --check`. The race suite completed in 113.021 seconds. The legacy
trusted-admin tests remained green alongside the separate hosting tests.
`govulncheck` was not installed; that verification gap is recorded in
`BLOCKERS.md`.
