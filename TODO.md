# TODO

This repository is not ready to publish as-is. The items below capture the
known blockers, security issues, correctness risks, missing project hygiene, and
quality gaps found during the audit.

## P0 - Do Not Publish Until Fixed

- [x] Remove `deployer.db`, `deployer.db-shm`, and `deployer.db-wal` from the
  repository tree before publishing.
- [x] Keep local runner-token databases out of the public repository. Token
  rotation was not required because the owner confirmed the runtime database was
  never published.
- [x] Remove the compiled `deployer` binary from source control/public release
  history.
- [x] Remove or recreate all screenshot PNGs before publishing:
  - `image.png` shows a GitHub self-hosted runner registration token and
    personal browser/profile context.
  - `image (2).png` exposes private project configuration, local paths, domains,
    and browser context.
  - `image (1).png` and `image (3).png` should be treated as non-public build
    artifacts unless carefully reviewed/redacted.
- [x] Keep local screenshots out of the public repository. Token rotation was
  not required because the owner confirmed the screenshots were never published.
- [x] Remove production-specific/private documentation from `README.md`,
  especially private project sections, private URLs, local paths, VPS paths,
  service names, sudo helper names, and post-deploy command examples tied to a
  real deployment.
- [x] Rewrite `CLAUDE.md` or decide whether it should be published. It contains
  internal implementation notes and production-flavored examples.
- [x] Add `.gitignore` before creating a public Git repo. It should exclude at
  least binaries, SQLite databases/WAL files, logs, archives, screenshots, env
  files, keys, and local tool/cache output.
- [x] Create a fresh public repository from cleaned source only, or run a full
  git-history secret scan if an existing hidden/original Git history is found.
  This repository now uses a fresh Git history from the cleaned source tree.

## P0 - Security

- [x] Require authentication/authorization for the main UI and REST API through
  an upstream gateway. Deployer no longer implements local admin password login,
  so the UI/API must not be exposed directly to untrusted networks.
- [x] Add CSRF protection for state-changing browser endpoints.
- [x] Do not pass agent tokens in URL query strings. Move agent authentication
  to an `Authorization: Bearer ...` header or another non-URL credential
  channel.
- [x] Stop logging or exposing tokens through URLs, browser history, reverse
  proxy logs, access logs, and command examples.
- [x] Do not return runner tokens from `GET /api/runners/:id`. Tokens should be
  shown only at creation time, and even that flow should be explicit.
- [x] Store runner tokens hashed at rest instead of plaintext in SQLite.
- [x] Add token rotation/revocation support for runners.
- [x] Use constant-time token comparison for runner authentication after moving
  away from direct plaintext database lookups.
- [x] Require HTTPS for agent/server communication in production, or document
  that plaintext HTTP is only safe on trusted networks.
- [x] Add request timeouts and hardened HTTP clients for all agent calls. Several
  `http.Get`/`http.Post` calls use the default client with no timeout.
- [x] Add server-level read/write/header timeouts instead of `http.ListenAndServe`
  with the default server.
- [x] Disable or protect `/download/deployer`; unauthenticated binary download
  makes agent auto-update convenient but broadens the attack surface.
- [x] Sign or checksum the auto-update binary before agents replace themselves.
- [x] Consider disabling auto-update by default, or require a trusted release
  channel.
- [x] Add size limits for artifact downloads served to agents, not only snapshot
  uploads.
- [x] Tighten snapshot download access. `GET /api/builds/:id/artifact` currently
  depends on the unauthenticated main API surface.
- [x] Sanitize build logs before rendering them in the frontend. Log group names
  are inserted via `innerHTML`, creating DOM XSS risk if logs contain malicious
  `##[group]...` content.
- [x] Remove inline event handlers in templates (`onclick`, `onsubmit`,
  `onchange`) so a reasonable Content Security Policy can be used.
- [x] Add a Content Security Policy after removing inline JavaScript.
- [x] Quote/sanitize all values interpolated into shell scripts. The SSH deploy
  script interpolates deploy directories, image names, service names, and tar
  paths into shell text.
- [x] Remove `StrictHostKeyChecking=no` from SSH/SCP paths or gate it behind an
  explicit insecure option. Default should verify host keys.
- [x] Avoid `bash -c`/`sh -c` where structured command arguments can be used.
- [x] Validate `health_url`, `health_container`, `compose_services`,
  `compose_file`, `deploy_dir`, `image_name`, and `post_deploy` more strictly.
- [x] Treat `post_deploy` as privileged code execution and document the trust
  model clearly.
- [x] Validate `permissions` owner/mode/pattern inputs before running `chown` and
  `chmod`.
- [x] Protect against path traversal and unsafe absolute paths in `preserve`,
  archive extraction, permissions patterns, and artifact paths.
- [x] Replace shelling out to `tar xzf` with safe archive extraction that rejects
  `../`, absolute paths, symlink escapes, device files, and ownership surprises.
- [x] Review `packageFiles` symlink handling. Archives can preserve symlinks that
  may be dangerous when extracted on the target.
- [x] Redact secrets from build logs before persisting or streaming them.
- [x] Add a log retention and deletion policy; build logs may contain secrets.
- [x] Add an explicit threat model for local-only use versus internet-facing use.

## P1 - Correctness And Reliability

- [x] Make the database path configurable instead of hardcoding `deployer.db`.
- [x] Avoid running migrations by ignoring `ALTER TABLE` errors. Check whether
  the error is "duplicate column" and fail on other errors.
- [x] Move schema/migrations to versioned migration files or a structured
  migration system.
- [x] Add foreign-key enforcement with `PRAGMA foreign_keys=ON`.
- [x] Add database busy timeout and connection settings suitable for concurrent
  writes.
- [x] Review concurrent build/job state transitions. A pending job can be picked
  by polling logic without an atomic compare-and-update guard.
- [x] Make `getPendingJob` and `updateJobStatus(..., "running")` atomic so two
  overlapping polls cannot claim the same job.
- [x] Handle and log errors from `updateBuild`, `updateJobStatus`,
  `updateRunnerHeartbeat`, `db.Exec`, and `json.Encoder.Encode` where currently
  ignored.
- [x] Use context-aware database calls for long-running request handlers.
- [x] Add graceful shutdown so in-flight builds/jobs and SSE streams are handled
  cleanly.
- [x] Persist enough state to reconnect/cancel agent-backed jobs after server
  restart.
- [x] Reconcile orphaned `jobs` as well as orphaned `builds` on startup.
- [x] Add cleanup for stale temporary artifacts in `/tmp` and
  `/tmp/deployer-snapshots`.
- [x] Ensure artifacts are removed on every failed/cancelled path, including
  files-mode archives.
- [x] Use `filepath.Join` instead of string concatenation for local paths.
- [x] Fix preserve-path parent directory creation. Current string splitting is
  fragile for single-segment paths and edge cases.
- [x] Validate that `deploy_dir` is not empty, `/`, or another dangerous target
  before extract, `chown -R`, or `rm -rf` style operations.
- [x] Avoid `docker image prune -af` and `docker builder prune -af` by default.
  They are broad host-level cleanup operations and can remove unrelated images.
- [x] Make Docker build, SCP, SSH, artifact upload/download, and health-check
  timeouts configurable.
- [x] Capture stderr for `runCapture`, especially for `git rev-parse`, so errors
  are actionable.
- [x] Handle scanner errors after reading command output.
- [x] Revisit the 1 MB scanner line limit for build logs; very long output lines
  can fail scanning.
- [x] Limit persisted build log size or stream logs to files/object storage to
  avoid unbounded SQLite growth.
- [x] Store timestamps consistently with timezone information. Current SQLite
  timestamp parsing handles multiple formats but stores local formatted strings.
- [x] Use URL encoding for tokens and path/query parameters until query-token
  auth is removed.
- [x] Validate `serverURL` scheme and host in the agent before making requests.
- [x] Avoid immediate tight-loop polling when no job is available if the server
  quickly returns 204 due to errors or proxy behavior.
- [x] Add retry/backoff for artifact download/upload and log sends.
- [x] Make agent log sending report failures to the job, not just local logs.
- [x] Avoid replacing a running executable directly for auto-update without an
  atomic, verified update strategy per platform.
- [x] Make agent update behavior work safely outside systemd or document that
  systemd restart is required.

## P1 - Frontend And UX

- [x] Replace string-built `innerHTML` in log rendering with DOM nodes and
  `textContent`.
- [x] Escape project names passed into inline JavaScript handlers. Names with
  quotes can break handlers or create injection bugs.
- [x] Add loading/error states for deploy, snapshot, save, delete, runner create,
  and cancel actions.
- [x] Disable destructive/action buttons while requests are in flight.
- [x] Replace `alert`/`confirm` flows with safer, clearer UI dialogs.
- [x] Add explicit indication that interactive admin authentication is delegated
  to an upstream gateway.
- [x] Add pagination for projects, builds, runners, and logs.
- [x] Add search/filtering for builds and projects.
- [x] Add mobile review after fixing layout issues from long paths and long
  project names.
- [x] Avoid exposing full local repo paths in the dashboard/project page unless
  authenticated users need them.
- [x] Make runner setup commands generated from a configurable public server URL
  instead of `window.location.origin` only.
- [x] Add a way to copy runner tokens only once and acknowledge storage
  requirements.
- [x] Add runner token rotation UI.
- [x] Add clearer validation messages for JSON fields.

## P1 - Tests And Verification

- [x] Raise test coverage. Coverage is now about 54.0% of statements.
- [x] Add tests for `initDB` migrations on fresh and older schemas.
- [x] Add tests for project CRUD, runner CRUD, build CRUD, and job lifecycle.
- [x] Add tests for atomic job claiming once fixed.
- [x] Add tests for agent authentication, runner-token redaction, and token
  rotation.
- [x] Add tests for files-mode packaging and ignore matching.
- [x] Add tests for unsafe archive paths and symlink extraction after replacing
  shell extraction.
- [x] Add tests for preserve backup/restore edge cases.
- [x] Add tests for permission config validation.
- [x] Add tests for shell argument/script quoting.
- [x] Add tests for SSE escaping and log rendering assumptions.
- [x] Add tests for API authorization once authentication exists.
- [x] Add integration tests that run server/agent locally against a temp SQLite
  database.
- [x] Add CI that runs `go test ./...`, `go test -race ./...`, `go vet ./...`,
  `govulncheck ./...`, formatting checks, and frontend linting if added.
- [x] Add a reproducible release check that builds with a patched Go version.

## P1 - Dependencies And Toolchain

- [x] Rebuild with a patched Go toolchain. `govulncheck` reported standard
  library vulnerabilities in Go 1.26.3 fixed in Go 1.26.4.
- [x] Decide whether the module should target `go 1.25` or a newer minimum Go
  version, then document it.
- [x] Review and update old dependencies where appropriate, especially
  `modernc.org/sqlite` and transitive `golang.org/x/*` modules.
- [x] Add `govulncheck` to release/CI docs.
- [x] Consider pinning tool versions for reproducible checks.

## P1 - Repository Hygiene For OSS

- [x] Initialize a Git repository only after cleanup, or import clean source into
  a fresh public repo.
- [x] Add `LICENSE`.
- [x] Add `SECURITY.md` with vulnerability reporting guidance.
- [x] Add `CONTRIBUTING.md`.
- [x] Add `CODE_OF_CONDUCT.md` if accepting outside contributors.
- [x] Add a public-safe `README.md` with generic examples only.
- [x] Add screenshots that show demo data only.
- [x] Add sample config and example systemd units that use placeholders instead
  of real names, paths, domains, or users.
- [x] Add `.editorconfig`.
- [x] Add a release workflow or documented manual release process.
- [x] Add a changelog or release notes policy.
- [x] Decide on module path before publishing, for example a GitHub import path
  instead of `module deployer`.
- [x] Decide whether `CLAUDE.md` should become `AGENTS.md`, stay private, or be
  removed.

## P2 - Product And Documentation

- [x] Document the intended deployment model: local-only, LAN-only, or
  internet-facing behind an auth proxy.
- [x] Document prerequisites: Docker, Git, systemd, tar, curl, shell, and OS
  assumptions.
- [x] Document how to back up and restore SQLite safely with WAL mode.
- [x] Document database location and retention.
- [x] Document runner trust assumptions and what a compromised runner token can
  do.
- [x] Document project field validation and examples for Docker mode.
- [x] Document files mode, preserve behavior, and archive safety assumptions.
- [x] Document snapshot behavior and its security implications.
- [x] Document auto-update behavior, risks, and how to disable it.
- [x] Document reverse proxy configuration with HTTPS, auth, request-size limits,
  and timeouts.
- [x] Document operational recovery from cancelled builds, failed jobs, stale
  runners, and server restarts.
- [x] Document migration policy for database schema changes.
- [x] Add a public roadmap that separates personal-use features from
  production-hardening work.

## P2 - Design And Maintainability

- [x] Move the binary entrypoint into `cmd/deployer` and the implementation into
  `internal/app` so the repository no longer has a large root package.
- [ ] Split the large `internal/app` package into clearer packages after
  security work: server, models/storage, agent, builder, web, and config.
- [ ] Introduce a config layer for address, database path, public URL, auth,
  artifact directory, timeouts, log retention, and update behavior.
- [ ] Replace global `db`, `broker`, `builder`, and `tmpl` with explicit
  application/server structs.
- [x] Replace default `http.DefaultServeMux` with an explicit mux.
- [x] Add middleware for logging, authentication, panic recovery, request IDs,
  and security headers.
- [x] Add structured logging with redaction.
- [x] Centralize JSON error handling and method validation.
- [x] Replace ad hoc route parsing with a small router or stricter path parsing.
- [ ] Normalize and validate project config in one layer instead of splitting
  assumptions between frontend and backend.
- [x] Make artifact storage an interface so local disk, temp dir, and future
  object storage can be tested.
- [x] Make command execution an interface for tests and safer policy enforcement.
- [ ] Separate trusted admin configuration from untrusted runtime job data.

## P2 - Product Features

- [ ] Add remote build mode where the agent pulls/builds on the target server
  instead of the main server building locally and transferring Docker tar files.
- [ ] Allow deploying a selected branch, tag, or commit instead of always
  deploying the current local `HEAD`.
- [ ] Add project environments, e.g. staging and production targets under one
  project, each with its own runner, deploy directory, health check, and deploy
  settings.
- [ ] Add rollback support for files and Docker deploys, backed by previous
  release artifacts or automatic pre-deploy snapshots.
- [ ] Add build queue visibility with pending/running jobs, runner assignment,
  queue position, and cancel-pending actions.
- [ ] Use runner labels for job routing instead of requiring each project to
  point at exactly one runner.
- [ ] Add webhook-triggered deploys with branch filters for GitHub/GitLab push
  events.
- [ ] Add scheduled deploys or scheduled remote maintenance jobs.

## P2 - Remote Server Operations

- [ ] Add runner detail pages showing hostname, OS, agent version, uptime, disk
  space, Docker availability, current job, and last heartbeat.
- [ ] Add remote project actions through the agent: `docker compose ps`, recent
  compose logs, restart service, stop service, and run health check now.
- [ ] Add configurable deploy hooks for both Docker and files mode: pre-build,
  post-build, pre-deploy, post-deploy, and failure hook.
- [ ] Add files deploy preview/dry-run showing included/excluded files, archive
  size, preserve paths, and remote changes before extraction.
- [ ] Add snapshot browser with download, compare, and restore actions.
- [ ] Add deploy notifications for success/failure via Discord, Slack, email, or
  generic webhook.

## P2 - AI And Automation Features

- [x] Add an OpenAPI spec for the REST API with examples for creating projects,
  triggering deploys, watching builds, cancelling builds, and managing runners.
- [x] Add a machine-readable capabilities endpoint, e.g. `/api/capabilities`,
  that exposes supported deploy modes, API version, server version, and enabled
  features.
- [x] Add structured JSON build events alongside raw logs so AI tools can read
  exact step names, statuses, durations, errors, artifact IDs, and runner IDs.
- [x] Add a deploy preview endpoint that returns the planned actions without
  starting a build: repo, commit, mode, runner, deploy dir, hooks, health check,
  and expected artifact type.
- [x] Add an AI-focused project summary endpoint that returns one compact JSON
  object with project config, last build, runner status, recent failures, and
  recommended next actions.
- [x] Add stable `code` fields to JSON API error responses and document the
  supported error-code list through `/api/capabilities` and OpenAPI.
- [x] Extend runtime failure taxonomy so API clients can distinguish offline
  runners, failed health checks, cancelled terminal builds, and artifact
  failures through persisted build/job error codes instead of parsing messages.
- [ ] Add a CLI with JSON output, e.g. `deployer projects list --json`,
  `deployer deploy PROJECT --json`, and `deployer builds watch BUILD --json`.
- [ ] Add an MCP server for Deployer so AI agents can list projects, inspect
  runners, trigger deploys, watch logs, fetch artifacts, and summarize failures
  through typed tools.
- [x] Add generated AI runbooks per project: how to deploy it, what files matter,
  what runner it uses, how rollback should work, and common failure recovery.
- [x] Add build failure summarization that extracts the failing step, likely
  cause, relevant log lines, and suggested fix.
- [x] Add commit/deploy annotations so an AI agent can attach notes like
  "deployed after dependency update" or "rollback candidate".

## P3 - Workflow Features

- [x] Add project cloning/templates so common Docker/files deploy settings can be
  reused.
- [x] Add repo autodetection for Dockerfile, compose files, package manager,
  `.deployignore`, and likely deploy mode.
- [x] Add build duration/history charts per project and runner.
- [x] Add release notes per build using commit metadata between the previous and
  current deploy.
- [ ] Add project grouping/tags/favorites for dashboards with many projects.
- [x] Add project import/export with secrets omitted.
- [ ] Add health-check status detail and configurable success criteria.
- [x] Add demo mode or seeded demo database for screenshots.
