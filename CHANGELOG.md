# Changelog

## Unreleased

- Add a private `/api/internal/v1` Light Apps Hosting execution API with scoped,
  hashed, overlapping service credentials, strict signed manifests, canonical
  idempotency, stable errors, and audit records.
- Separate hosting projects, deployments, jobs, runners, releases, events,
  logs, callbacks, and capacity from the legacy trusted-admin deployment path.
- Verify full Git commit identity and immutable source-artifact digests before
  atomically reserving runner capacity and creating hosting work.
- Add the dedicated `hosting-agent` protocol and executable with generated
  static/Node recipes, safe archive extraction, private ports, workload limits,
  lease fencing, cancellation, health evidence, and retained-release cleanup.
- Fence normal hosting-job completion with a durable payload fingerprint so a
  conflicting terminal report cannot race candidate activation or replay as
  success.
- Make release artifact uploads use unique temporary files and lease-fenced
  compare-and-swap attachment with exact replay validation.
- Add durable per-project route generations to activation, rollback,
  suspend/resume, recovery and compensation so late adapter calls cannot
  override newer desired state and corrections survive restart.
- Add immutable health-gated activation through the private versioned proxy
  adapter client, selected-release rollback, suspend/resume, and compensation
  when cancellation races activation.
- Persist an immutable runtime-manifest snapshot per release and use it for
  rollback health gates and retained-image recovery after later manifest edits.
- Make rollback select a release by exact external deployment identity plus
  content digest, avoiding ambiguity between content-identical deployments.
- Add a transactional signed callback outbox with retries, dead-letter state,
  polling reconciliation, runner-loss recovery/reassignment, and deterministic
  retry exhaustion.
- Add per-project and global execution kill switches, aggregate hosting metrics,
  hosting retention policies, expanded secret redaction, OpenAPI contracts, and
  hosting operations/security runbooks.
- Make legacy and hosting schema migrations atomic and test fresh,
  representative legacy, concurrent migration, idempotency, lease, capacity,
  activation, rollback, callback, and recovery behavior.
- Prepare repository for public release.
- Remove private runtime artifacts and production-specific documentation.
- Delegate admin UI/API authentication to an upstream authorization gateway, and
  keep CSRF checks, bearer-token agent authentication, and safer security
  headers.
- Store new runner tokens hashed at rest.
- Migrate legacy plaintext runner tokens to hashes at startup and add runner
  token rotation.
- Add safe files-mode archive extraction.
- Add validation for deployment paths, health checks, permissions, preserve
  paths, and shell-sensitive fields.
- Redact common token formats from persisted and streamed build logs.
- Add managed artifact directories, stale artifact cleanup, build-log retention,
  and a persisted build-log size cap.
- Disable broad Docker prune and agent auto-update by default.
- Add checksum verification for agent self-update downloads.
- Add graceful shutdown with build cancellation and runner/job cleanup
  improvements.
- Add configurable server, build, transfer, SSH, health-check, and agent HTTP
  timeouts.
- Add SSE keepalive heartbeats and write-deadline handling.
- Add retry/backoff for agent artifact transfer, log delivery, and completion
  reporting.
- Make persistent agent log-delivery failures fail/report jobs instead of only
  writing local agent logs.
- Replace fixed-size command log scanners with buffered line readers.
- Store new timestamps as UTC RFC3339 values while preserving legacy timestamp
  parsing.
- Use request contexts for SSE and agent long-poll database work.
- Record named schema migrations in SQLite and test upgrades from an older
  schema shape.
- Update `modernc.org/sqlite` and transitive `golang.org/x/sys` to current
  release-safe versions and raise the module minimum to Go 1.25.
- Pin `govulncheck` in CI and development docs.
- Document operational prerequisites, backups, snapshots, auto-update behavior,
  reverse proxy guidance, recovery, migrations, and release steps.
- Add public release hygiene files and CI.
