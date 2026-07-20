# Deployer — production hosting execution plan

## Outcome

Deployer becomes a private execution engine behind the single customer-facing hosting control plane. It accepts only authenticated, policy-approved, structured requests; deploys exact immutable inputs; reports deterministic state; and preserves the last healthy release across failure, restart and retry.

There is no customer-facing Deployer workflow. The browser calls the hosting control plane, and the control plane calls `/api/internal/v1` over a private network.

## Definition of done

The Deployer scope is complete when the hosting control plane can, without SSH or operator intervention:

1. idempotently provision/update a hosting project from a versioned approved manifest,
2. deploy an exact commit and immutable artifact digest,
3. observe stable deployment phases and redacted logs,
4. receive authenticated terminal callbacks and recover missed callbacks by polling,
5. cancel queued/running work safely,
6. activate a candidate only after health succeeds,
7. keep the previous healthy release active on failure,
8. roll back to a selected healthy immutable release,
9. suspend/resume a runtime without deleting release metadata,
10. recover declared state after Deployer or runner restart,
11. enforce runner capability/capacity and workload limits,
12. pass authentication, idempotency, concurrency, restart, failure, rollback and redaction tests.

## Mandatory zero step: stabilize current WIP

This repository already contains a broad uncommitted hosting implementation in progress. Treat every current checkbox as unverified until the active worktree is inventoried and the full baseline is green.

- Preserve every existing user/agent change; do not reset, checkout, stash, delete or broadly rewrite it.
- Do not commit or delete `deployer.db.bak-*` without explicit user authorization.
- Classify each current change as complete, partial, failing or unrelated before extending it.
- Fix fresh/legacy migrations and get `go test ./...`, `go test -race ./...` and `go vet ./...` green before relying on current service-token/internal-API work.
- Preserve the existing trusted-admin deployment behavior and its tests throughout the hosting implementation.

## Boundary contracts

### Hosting control plane → Deployer

All endpoints are private, versioned, bearer-authenticated with hashed/rotatable service tokens, scoped and JSON-only.

Required operations:

- `PUT /api/internal/v1/projects/{external_project_id}` — idempotent provision/upsert from a signed/versioned manifest.
- `POST /api/internal/v1/projects/{external_project_id}/deployments` — start exact deployment; requires `Idempotency-Key` and `external_deployment_id`.
- `GET /api/internal/v1/deployments/{external_deployment_id}` — state, phase, failure code, release identity and redacted log reference.
- `POST /api/internal/v1/deployments/{external_deployment_id}/cancel` — idempotent cancel.
- `POST /api/internal/v1/projects/{external_project_id}/rollback` — activate a specific healthy release.
- `GET /api/internal/v1/projects/{external_project_id}/releases` — immutable release history.
- `POST /api/internal/v1/projects/{external_project_id}/suspend` and `/resume` — execution state only; billing decisions remain outside Deployer.
- `GET /api/internal/v1/runners` and `/capabilities` — health, capacity, supported manifest/runtime versions.

Every response uses stable codes and identifiers. No endpoint accepts arbitrary shell, Compose, host paths, privileged flags or unvalidated runtime configuration from the control plane.

### Deployer → hosting control plane

Callbacks contain event ID, timestamp, external project/deployment ID, phase, terminal status, stable failure code, artifact/release digest and redacted metadata. They are HMAC-signed, replay-protected and safe to deliver multiple times. Callback failure never loses state; the control plane can reconcile by polling.

### Deployer → runner

Jobs contain only structured, validated execution data and immutable artifact identity. Runner tokens remain separate from control-plane service tokens. A hosting runner cannot claim trusted/admin projects and a trusted/admin runner cannot accidentally receive hosting jobs.

## Data model changes

- `service_tokens`: name, hash, scopes, created/last-used/revoked timestamps.
- `hosting_projects`: external ID, manifest version/hash, runtime class, desired state, assigned runner constraints.
- `hosting_deployments`: external deployment ID, exact commit SHA, manifest/artifact digest, phase, failure code and callback state.
- `hosting_jobs`: durable lease, attempts, runner assignment, cancellation and restart recovery state.
- `hosting_idempotency`: issuer, operation, key, request hash and original response; reuse with a different payload is a stable conflict.
- `hosting_releases`: immutable digest, project/deployment link, health evidence, route revision, previous release, activated/deactivated timestamps and status.
- `callback_outbox`: unique event ID, payload hash, attempts, next attempt and delivered timestamp.
- runner fields: execution class, labels/capabilities, version, capacity, heartbeat and drain state.

Use separate hosting tables instead of adding customer-hosting behavior to legacy trusted `Build/Job` fields. Migrations must work on fresh and existing SQLite databases. Add uniqueness, foreign-key and compare-and-swap transition constraints for all external IDs and idempotency records.

## Implementation order

### 0. WIP inventory and green baseline

- Inventory the current diff without modifying or discarding user work.
- Reconcile migration numbers/schema expectations and run fresh plus legacy upgrade tests.
- Run full test/race/vet gates and repair regressions before marking inherited work complete.
- Record any overlapping concurrent edits and serialize ownership before continuing.

### 1. Contract and persistence

- Finalize OpenAPI schemas, scopes, error codes and state transitions.
- Add separate hosting identifiers, manifests, deployments, jobs, releases, idempotency records and callback outbox migrations.
- Implement typed validation and normalization in one layer.
- Preserve current admin API compatibility unless a documented migration is necessary.

### 2. Project provisioning

- Implement idempotent hosting-project upsert.
- Validate manifest version/hash and enforce the supported execution allowlist.
- Reject arbitrary operator fields and shell escape hatches.
- Separate hosting projects/runners/jobs from trusted admin deployments.
- Generate platform-owned static/Node build recipes; never run a customer Dockerfile in the automatic path.

### 3. Exact deployments

- Replace local-current-HEAD assumptions for hosting jobs with exact commit/artifact identity.
- Make deployment creation atomic with idempotency and per-project serialization.
- Store request hash and original response; the same key with different payload returns a stable conflict.
- Persist stable phases and expose terminal/non-terminal status consistently.
- Define deterministic handling of a newer request while another deployment is active.

### 4. Immutable releases and health activation

- Persist immutable release candidates and previous healthy releases.
- Start candidate beside active release.
- Record health attempts/evidence and activate only on success.
- Ensure failed candidate cleanup never changes the active release.
- Implement cancel and rollback as idempotent state transitions.
- Persist cancellation intent so cancel/recovery works after server restart, not only through an in-memory context map.

### 5. Callbacks and reconciliation

- Implement transactional callback outbox.
- Sign callbacks with HMAC, timestamp and event ID.
- Retry with backoff and terminal dead-letter state.
- Expose polling state sufficient for a control-plane reconciler.
- Reconcile interrupted jobs after server/runner restart.

### 6. Runner policy and capacity

- Add hosting runner class/capabilities/version negotiation.
- Report CPU/RAM/disk/PID capacity and drain state.
- Refuse placement without reserve; never partially create a deployment.
- Enforce structured runtime limits and deny public host ports/privileged modes/host mounts/Docker socket.
- Deliver production secrets by short-lived runtime reference/identity; Deployer and build storage must not persist their plaintext values.

### 7. Observability and operations

- Structured redacted logs and event timelines.
- Heartbeats, queue age, deploy duration and failure-code metrics.
- Per-project and global kill switches with audit reason.
- Safe token rotation/revocation and service identity runbook.
- Support overlapping service-token rotation; do not invalidate the old token before the new credential is active.
- Recovery documentation for DB restore, missed callbacks, stale jobs and runner loss.

### 8. Verification

- Unit and migration tests for all state and validation rules.
- Contract tests for every internal endpoint and callback signature.
- Race/concurrency tests for duplicate requests and simultaneous deployments.
- Failure tests for build, health, callback, runner loss and restart.
- Security tests for auth scopes, redaction, path/input validation and forbidden execution settings.
- End-to-end runner tests proving activation, failure preservation, rollback, suspend/resume and recovery.
- Real staging tests against the colleague-owned proxy adapter; a fake adapter is allowed only in automated tests and never proves production readiness.

## Explicitly out of scope

- Customer UI, GitHub user authorization, Stripe and billing policy.
- Reverse-proxy configuration or public DNS/TLS actions.
- Arbitrary Compose, local production DB, cron, workers, Kubernetes and multi-region.
- Demo modes or fake production success paths.

Test fixtures may simulate external systems, but production code must fail closed when a real dependency is unavailable.
