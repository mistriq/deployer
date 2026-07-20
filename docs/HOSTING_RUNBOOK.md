# Hosting operations runbook

This runbook covers the private Light Apps Hosting execution path. Customer
browsers and hosted applications never call Deployer. Only the hosting control
plane calls `/api/internal/v1`, using a scoped service credential over the
private network.

## Production prerequisites

- Put the Deployer admin surface behind the trusted authorization gateway and
  keep the server listener private.
- Configure a writable SQLite volume, artifact directory, and dedicated
  hosting-agent work root. Back up the database independently from artifacts.
- Make exact Git objects available below `DEPLOYER_HOSTING_REPO_ROOT`. The
  control plane provisions repository identity; Deployer derives the local path
  from `external_project_id` and never accepts a caller-supplied host path. This
  is currently an operator-provided handoff: Deployer does not yet perform an
  installation-authenticated clone/fetch or prove that the local object belongs
  to the provisioned repository identity. Do not mark the production source
  handoff complete until that ownership and retry contract is implemented.
- Configure `DEPLOYER_PROXY_ADAPTER_URL` and its bearer credential. HTTPS is
  mandatory except for loopback development. Deployer only calls the adapter's
  versioned private activation/suspension API and never writes proxy config.
- Configure the HTTPS terminal callback URL and an independent HMAC key of at
  least 32 random bytes.
- Register at least one dedicated hosting runner with capacity and reserve
  values, then run `deployer hosting-agent` under a dedicated OS account. Keep
  the Docker engine and host kernel patched. Set
  `DEPLOYER_HOSTING_AGENT_RUNTIME_BIND_ADDRESS` to a loopback or private IPv4
  address reachable by the proxy adapter; public bind addresses are rejected.
- Create a Docker network with `--internal` and label it
  `light-apps.hosting.restricted-egress=true`; connect only approved internal
  package mirrors, then set `DEPLOYER_HOSTING_AGENT_BUILD_NETWORK` to its name.
  Set `DEPLOYER_HOSTING_AGENT_BUILD_TIMEOUT` between 1 and 60 minutes.

Do not paste credentials into command history. Prefer a root-readable
environment file, secret manager injection, or a short-lived protected file.

## Service identities and rotation

Create control-plane credentials through the trusted admin API with only the
needed scopes:

- `projects:write` — provision manifests, suspend/resume, and project kill switch.
- `deployments:read` — poll state, events, logs, releases, runners, capabilities, and metrics.
- `deployments:write` — create, cancel, and roll back deployments.
- `hosting:admin` — global execution kill switch; use a separate operator identity.

The plaintext `dpl_` credential is returned only at creation or rotation and is
stored only as a hash. Rotation creates a new credential while the previous one
remains valid for `DEPLOYER_SERVICE_TOKEN_ROTATION_OVERLAP` (24 hours by
default). Safe rotation sequence:

1. Create the new credential with `POST /api/service-tokens/{id}/rotate`.
2. Install it in the control plane and verify an authenticated capabilities call.
3. Observe `authenticated` audit events for the new credential.
4. Allow the overlap to expire, or revoke the service token immediately if the
   old credential may be compromised. Revocation invalidates all overlapping
   credentials.

Hosting runner credentials use the `htr_` prefix and are never accepted as
service or legacy runner credentials. Runner rotation is immediate: install the
new value before restarting the agent, and expect the old agent connection to
be rejected.

## Normal deployment and reconciliation

1. Provision the strict v1 manifest with a signed `PUT` request. Sign
   `<unix timestamp>.<exact raw body>` using the service token and send the
   hexadecimal HMAC-SHA256 in `X-Deployer-Signature`.
2. Create a deployment with a unique `Idempotency-Key`, full 40-character Git
   SHA, current manifest digest, and expected SHA-256 of the exact Git archive.
3. Poll the returned `Location`. Structured `status`, `phase`, and
   `failure_code` are authoritative. Do not parse log or error text.
4. Treat the terminal callback as at-least-once. Verify the HMAC over
   `<timestamp>.<event_id>.<raw body>`, reject timestamps outside five minutes,
   and deduplicate event IDs before applying state.
5. Poll the deployment whenever a callback is late, duplicated, or reports
   `dead_letter`. Polling contains the release identity and log/event references
   needed to repair control-plane state.

An active release has passed the candidate health gate and has an adapter route
revision. A failed build, health check, or activation leaves the prior active
release unchanged. Rollback accepts any retained `healthy`, `inactive`, or
already `active` release with a verified private runtime endpoint. Select it by
the exact `external_deployment_id` plus `release_digest`; content digests are not
unique release-instance identifiers. Each release
stores the runtime/health contract from its original deployment recipe;
subsequent project-manifest updates do not change rollback or restore behavior.

## Queue, capacity, and runner loss

Use `/api/internal/v1/metrics` and `/api/internal/v1/runners` to inspect queue
age, outcome rate, duration, callback lag, negotiated versions, free capacity,
reserve, drain, and liveness.

If work remains queued:

1. Confirm a hosting runner is `online`, not draining, supports manifest v1 and
   the requested Node version, and has free CPU/RAM/disk/PIDs above reserve.
2. Confirm neither the global nor project execution kill switch is enabled and
   the project desired state is `active`.
3. Confirm the exact source artifact still exists. Active hosting artifacts are
   refreshed before generic cleanup; `artifact_unavailable` indicates storage
   damage or an operator deletion.

Agents renew a generation-fenced two-minute lease. A stale runner cannot write
phase, logs, heartbeat, or completion after expiry/reassignment. If its
heartbeat becomes stale, Deployer marks it offline. After a lost lease,
capacity is restored exactly once and uncancelled work is transactionally
placed on another compatible runner. After three lost attempts, the deployment
terminates with `runner_lost` and emits the usual callback. Cancellation intent
is durable, but the late adapter-response ordering gap below remains open.
Release artifact attachment is also generation-fenced and compare-and-swap;
an identical upload is a safe replay and a different artifact conflicts.

For an already active release, runner loss creates a separate durable recovery
record. A compatible runner downloads the retained Docker image archive through
the recovery lease, verifies the archive digest and loaded image ID, starts the
runtime with the release's immutable runtime snapshot, original limits and
private bind policy, and reports health
evidence. Adapter activation is persisted before the external call and is
reconciled after a Deployer restart. A stale recovery generation is fenced; a
project suspension or kill switch records durable cancellation intent for
current recovery work. The remaining late-response correction is not yet a
durable, revision-ordered adapter saga: an activation returning across a process
crash or concurrent suspend/resume change can require operator reconciliation.
Keep cancellation/recovery durability unchecked until adapter-side ordering is
implemented and tested. If no retained artifact exists or all attempts fail,
Deployer records a stable failure and stages adapter suspension instead of
rebuilding untrusted or mutable source.

Restore support is negotiated independently from protocol v1. Older agents
that omit `operations` remain build-only. Upgrade agents first, verify that
`restore` is reported by `/api/internal/v1/runners`, and only then rely on
runtime recovery. Poll `runtime_status` on the original deployment:
`recovering` identifies an active recovery lease and `unavailable` means the
retained runtime could not be restored and adapter suspension was staged.

Secret-bearing runtime recovery is not a completed production path. The
original deployment contract persists opaque references, but there is no
workload-identity redemption or renewal exchange; recovery therefore fails such
a release closed before creating a restore lease. Keep the runtime-secret and
overall runner-recovery TODO items open until that path is implemented and
tested without plaintext persistence.

Recovery currently detects whole-runner liveness loss. The heartbeat response
describes what the agent must retain, but the agent does not yet report an
observed runtime inventory back to Deployer. A missing or crashed container on
an otherwise online runner therefore remains an open production-path gap.

For an intentional drain, mark the runner draining through its heartbeat
configuration, wait for current leases to finish, then stop it. For an
unexpected loss, preserve the database and artifact directory, restore the
agent, and let reconciliation run; do not edit job rows manually.

## Kill switches

Both kill-switch endpoints require a one-line audit reason and an
`Idempotency-Key`. Enabling a switch:

- refuses new placement,
- cancels queued work and restores its reserved capacity,
- persists cancellation intent for leased/running work, and
- records redacted hosting audit/events.

Use the project switch for an isolated application incident. Use the
`hosting:admin` global switch for a platform-wide safety event. Clearing a
switch does not automatically retry terminal work; submit a new deployment
identity after the incident is resolved.

## Database backup and restore

SQLite uses WAL mode. Never copy only a live `.db` file while ignoring its WAL.
Use SQLite online backup tooling, or stop Deployer and copy the database:

```bash
systemctl stop deployer
install -m 0600 /var/lib/deployer/deployer.db /var/backups/deployer-YYYYMMDD.db
systemctl start deployer
```

For restore:

1. Stop Deployer and the hosting agents.
2. Preserve the current database and managed artifacts for investigation.
3. Restore a matched, integrity-checked database backup without modifying any
   repository-owned `deployer.db.bak-*` file.
4. Run `sqlite3 /var/lib/deployer/deployer.db 'PRAGMA integrity_check;'`.
5. Start Deployer first. Atomic migrations run once; the reconciler marks stale
   runners and callbacks and fences expired leases.
6. Start agents and verify capabilities, runner heartbeat, queue age, and
   callback lag before allowing new deployments.
7. Reconcile the hosting control plane by polling all nonterminal deployments.

Do not manually mark a release active. Adapter activation and the database
state transition are intentionally coupled through idempotent operation IDs and
health evidence.

## Retention

Cleanup runs at startup and every six hours:

- hosting logs: 30 days,
- events: 90 days,
- inactive/failed releases: 90 days,
- delivered/dead-letter callbacks: 30 days,
- hosting and service-token audits: 365 days,
- expired idempotency responses: immediately eligible.

Each period is configurable; zero disables that deletion. Active releases are
never aged out. Runner heartbeat returns authoritative project/deployment
release-instance identities. The agent removes only containers carrying the
platform-managed label whose exact instance is no longer retained, and then
best-effort removes the unused image. Immutable image archives are stored
content-addressably in Deployer artifact storage; back up that storage with the
database. Never add the platform-managed label to operator containers.

## Required staging acceptance

Before release, run both fixture applications through the real control plane,
dedicated runner, and colleague-owned staging adapter:

1. Static app: provision, deploy exact commit/artifact, observe build and health
   phases, verify private candidate then public staging route, and verify the
   signed terminal callback plus polling state.
2. Node app: repeat with a private Node health endpoint and short-lived secret
   references; verify no plaintext appears in the database, logs, events,
   callbacks, or errors.
3. Deploy a second healthy Node release, roll back to the first selected digest,
   and verify adapter route revision and active/inactive release states.
4. Verify a health failure leaves the previous route active, cancellation wins
   over a late completion, suspend removes routing without deleting releases,
   and resume restores routing.
5. Record exact external project/deployment IDs, commit and artifact digests,
   route revisions, callback event IDs, and verification timestamps in the
   private staging change record. Do not commit credentials or customer data.

If staging credentials or the adapter are unavailable, record the owner,
missing input, and reproducible commands in `BLOCKERS.md`; local fakes do not
satisfy this acceptance gate.
