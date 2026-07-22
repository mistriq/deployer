# Deployer hosting TODO

The primary agent owns this checklist and updates it only after implementation and verification. Existing completed work must still be reviewed against the current diff before it is trusted.

## P0 — current WIP stabilization

- [x] Inventory every current modified/untracked file and classify complete/partial/failing/unrelated without discarding anything.
- [x] Repair fresh DB and legacy migration expectations.
- [x] Establish green `test`, `race` and `vet` baseline before trusting inherited hosting changes.
- [x] Preserve legacy trusted-admin deployment behavior and tests.
- [x] Keep `deployer.db.bak-*` untracked and untouched unless the user explicitly decides otherwise.

## P0 — contract and security

- [x] Verify hashed service tokens, scopes, rotation and revocation against the current WIP and full test gate.
- [x] Verify private versioned API and CSRF separation against the current WIP and full test gate.
- [x] Replace simple deployment-key persistence with issuer + operation + key + request hash + original response.
- [x] Add service-token audit events and overlapping no-downtime rotation.
- [x] Add `external_project_id` and `external_deployment_id` with unique constraints.
- [x] Define and document all internal schemas, scopes and stable error codes in OpenAPI.
- [x] Add strict manifest schema/version/hash validation.
- [x] Ensure hosting APIs accept no arbitrary shell, host path, Compose or privileged runtime fields.
- [x] Automatic path uses platform-generated static/Node recipes and never executes customer Dockerfiles.
- [x] Add request size limits, content-type validation and consistent JSON errors.

## P0 — project and deployment lifecycle

- [x] Idempotent hosting project provision/upsert endpoint.
- [x] Separate hosting projects/jobs/runners from trusted admin projects.
- [x] Exact commit SHA and immutable artifact digest on every hosting deployment.
- [x] Atomic idempotency and one-active-deployment guard under concurrency.
- [x] Stable deployment phases and transition validation.
- [x] Fence conflicting runner completion payloads before proxy activation.
- [x] Compare-and-swap immutable release artifact attachment under concurrent uploads.
- [x] Idempotent cancel for queued and running deployments.
- [x] Cancellation intent and recovery remain durable after process restart.
- [x] Generation-fence all proxy intents and persist compensations before external calls.
- [x] Immutable release persistence and selected-release rollback.
- [x] Bind rollback and runtime recovery to each release's immutable runtime manifest.
- [x] Make rollback identity unambiguous for content-identical release instances.
- [x] Candidate health gate before activation.
- [x] Failed candidate leaves previous healthy release untouched.
- [x] Suspend/resume without deleting releases.
- [x] Restart reconciliation for Deployer and runner interruptions.

## P0 — callbacks and reconciliation

- [x] Transactional callback outbox with unique event IDs.
- [x] HMAC signature, timestamp validation contract and replay protection.
- [x] Retry/backoff/dead-letter behavior.
- [x] Terminal callback safe to deliver more than once.
- [x] Polling endpoint contains enough state to repair a missed callback.
- [x] Redact secrets from logs, callbacks, errors and structured events.

## P1 — runner and capacity

- [x] Hosting runner class, capability/version negotiation and labels.
- [x] Heartbeat includes free CPU/RAM/disk/PID capacity and drain state.
- [x] Scheduler refuses placement below configured reserve.
- [x] Structured workload limits propagated and enforced.
- [x] Deny public host ports, privileged mode, capabilities, host mounts and Docker socket.
- [x] Runtime receives secrets by short-lived reference/identity; Deployer/build DB and logs never persist plaintext.
- [x] Runner loss and recovery state are deterministic.
- [x] Per-project and global execution kill switches with audit reason.

## P1 — observability and operations

- [x] Deployment event timeline with phase durations and stable failure codes.
- [x] Metrics for queue age, success rate, duration, capacity and callback lag.
- [x] Service token, DB backup/restore, stale job and runner-loss runbooks.
- [x] Retention and cleanup for artifacts, releases, logs and callback records.
- [x] Update README, SECURITY, CHANGELOG and OpenAPI.

## Required verification

- [x] Fresh DB and legacy migration tests.
- [x] Auth, scope, rotation, revocation and audit tests.
- [x] Idempotency replay and concurrent duplicate tests.
- [x] Per-project deployment serialization tests.
- [x] Exact commit/artifact mismatch rejection tests.
- [x] Callback signing, retry, replay and polling reconciliation tests.
- [x] Health failure preserves active release test.
- [x] Cancel, rollback, suspend/resume and restart recovery tests.
- [x] Forbidden workload and secret-redaction security tests.
- [x] Full `go test ./...`, `go test -race ./...`, `go vet ./...` and `git diff --check` pass.
- [x] `govulncheck ./...` passes when available or blocker is documented.
- [ ] No open P0/P1 issue and no demo/placeholder production path remains.
- [ ] Real staging static + Node deployment, proxy activation and rollback pass; test fake alone is insufficient.
