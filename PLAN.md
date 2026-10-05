# Deployer roadmap

Build a reliable foundation, then deliver features that improve everyday deployment work. Keep each step independently reviewable and avoid a whole-application rewrite.

## 1. Migrate to PostgreSQL

**Status: complete (2026-09-08).** PostgreSQL-backed tests verify import contents, concurrent claims/logs/heartbeats, a disposable files deployment preserving uploads, and restart/authentication recovery. Preservation safety was committed in `5f4f246`. Production cutover was performed on 2026-10-05 after a real-data rehearsal. All 16 projects, 3 runners, 270 builds, and 253 jobs were verified, and disposable success/failure deployments preserved their data fixture. Daily protected dumps and an isolated restore were verified. The original SQLite file and executable/configuration backups were retained; see `docs/postgresql-migration.md`.

- Add PostgreSQL storage and versioned schema migrations.
- Provide a repeatable SQLite import preserving project and runner IDs, token hashes, settings, build/job history, annotations, and other persisted records.
- Verify imported record counts, relationships, timestamps, and authentication compatibility without exposing secrets.
- Test concurrent log writes, heartbeats, and job claims; ensure a job cannot be claimed twice.
- Rehearse a cutover with backups, a write pause, validation, and a documented recovery procedure. Preserve the original SQLite database.
- Decide explicitly whether SQLite remains supported; avoid accidentally committing to maintaining two storage implementations indefinitely.
- Alongside this work, fix files-mode preservation failures: abort on backup failure, report restore failure, and retain recovery backups after unsuccessful extraction or restoration.

**Done when:** a representative database imports successfully, concurrency tests pass, and a rehearsed deployment works end to end against PostgreSQL with a verified recovery path.

## 2. Add a focused MCP integration

**Status: complete (2026-09-08).** Verified with an official MCP SDK client and PostgreSQL-backed API tests for scoped authorization, previews, deployment/failure follow-up, snapshot upload, concurrent retries, and crash recovery. The isolated Stage 2 full test suite passes; setup is documented in `docs/mcp.md`.

- Expose typed tools for listing projects and runners, reading project summaries/configuration, and inspecting recent builds.
- Expose bounded logs, structured events, failure summaries, and generated runbooks.
- Provide deployment previews showing source version, target, preserve paths, hooks, and health checks.
- Support triggering deployments, cancelling builds, and requesting snapshots.
- Reuse the existing application/API operations instead of implementing separate deployment logic.
- Define authentication and separate read access from privileged actions; redact secrets from responses and logs.
- Return build IDs promptly for asynchronous operations and provide separate progress/result tools.
- Make trigger retries idempotent so a repeated call does not create duplicate deployments.

**Done when:** an MCP client can inspect a project, preview and trigger an authorized deployment, follow its outcome, and diagnose a failure; unauthorized writes and duplicate triggers are covered by tests.

This step can be developed independently of the database migration if earlier feature delivery is useful.

## 3. Add environments and source-version selection

- Group staging and production environments under one project, with environment-specific runners, deploy directories, health checks, and deployment settings.
- Support selecting a branch, tag, or commit and resolve it to an immutable commit identifier before building.
- Build from an isolated checkout so deployment does not modify the developer's working tree or include unrelated changes.
- Record the source revision and artifact identity for every release.
- Support promoting the same retained artifact from staging to production, with environment-specific configuration handled explicitly.
- Carry environment and version selection through the UI, API, and MCP tools.
- Migrate existing projects into a compatible default environment without changing their deployment targets.

**Done when:** a selected revision deploys to staging and the same artifact can be promoted to production, with both outcomes and exact revisions visible in history.

## 4. Add runner visibility and remote operations

**Status: complete (2026-09-08).** The isolated PostgreSQL-backed full suite passes, including concurrent runner claims, cancellation, legacy-agent rejection, a recovering HTTP health endpoint, and an actual disposable Docker status/log/restart/stop lifecycle. Populated desktop/mobile runner views were checked, and the operations are available through the API and scoped MCP tools.

- Add runner detail pages with hostname, OS, agent version, uptime, disk space, Docker availability, heartbeat freshness, and current job.
- Show pending/running jobs, runner assignment, and cancellation of pending work.
- Add bounded service status and recent Compose log inspection.
- Add authorized service restart/stop actions and on-demand health checks.
- Report unsupported capabilities and offline runners clearly.
- Record privileged operations and their outcomes; expose appropriate operations through the API and MCP.

**Done when:** an operator can diagnose an offline or unhealthy deployment from Deployer and perform supported recovery actions with visible results.

## 5. Add retained releases and rollback

- Retain known-good releases with explicit retention rules that protect active and rollback artifacts.
- For files mode, stage and validate releases before activation and keep mutable customer data separate from release files.
- For Docker mode, retain the previous image and the deployment configuration needed to restore it.
- Add an explicit rollback action in build history, the API, and MCP, with target preview and progress reporting.
- Preserve current uploads, registrations, databases, and other mutable data during code rollback.
- Define how deployments with database migrations or irreversible hooks affect rollback eligibility.
- Improve health verification with expected HTTP status/body checks, bounded request timeouts, and clear failure details.

**Done when:** deliberately broken files and Docker deployments can be rolled back to a retained release, health is verified, and mutable customer data remains intact.

## 6. Add webhooks and notifications

**Status: partially implemented (2026-10-05).** Unlimited personal browser,
Discord, Slack, and generic webhook channels are implemented with project and
outcome filters, encrypted endpoints, durable routing/retries, and delivery
history. GitHub/GitLab deployment triggers remain pending.

- Add authenticated GitHub/GitLab webhook deployment triggers with branch filters and explicit environment mapping.
- Validate webhook signatures, deduplicate deliveries, and define queue behavior for repeated pushes.
- Reuse the same version resolution, deployment permissions, and lifecycle as manual/API deployments.
- Add configurable success/failure notifications, beginning with a generic webhook and extending to selected channels as needed.
- Support any number of personal channels, including browser alerts while the app is open, Discord, Slack, and generic HTTPS webhooks.
- Include project, environment, revision, outcome, duration, and a build link while excluding secrets.
- Ensure notification delivery failures do not change deployment outcomes; use bounded retries and expose delivery status.

**Done when:** a matching push produces exactly one intended deployment, unrelated branches do not deploy, and notifications report the final outcome reliably.

## Delivery principles

- Deliver small, verified changes with focused tests for consequential behavior.
- Keep UI, API, and MCP behavior consistent through shared application logic.
- Refactor storage and deployment boundaries only where the roadmap requires it.
- Preserve existing customer data, local work, and operational history.
- Distinguish local validation, migration rehearsal, and production verification in completion reports.
