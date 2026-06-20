# Changelog

## Unreleased

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
