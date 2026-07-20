# Security Policy

Deployer is a deployment tool. Treat access to the server UI/API and runner
tokens as production-level privileges.

## Supported Versions

Security fixes are provided for the latest released version only until the
project has a formal release policy.

## Reporting Vulnerabilities

Please report security issues privately by opening a GitHub Security Advisory
for this repository, or by contacting the maintainer through the repository's
published contact channel.

Do not open public issues for vulnerabilities that expose secrets, enable
unauthorized deploys, compromise runners, or allow arbitrary file writes or
command execution.

## Operational Guidance

- Run the server behind HTTPS and an authorization gateway. This deployment uses
  [Pocket ID](https://github.com/pocket-id/pocket-id) as the upstream OIDC
  gateway.
- Do not expose the Deployer UI/API directly to untrusted networks; local admin
  password login is not implemented.
- Keep runner tokens private and rotate them if they appear in logs, URLs, shell
  history, screenshots, or support bundles.
- Keep legacy runner, dedicated hosting-runner (`htr_`), hosting control-plane
  service (`dpl_`), source-broker, workload-identity HMAC, callback HMAC, and proxy-adapter credentials
  separate; never reuse one value for another integration. The source broker
  must issue a credential scoped and audience-bound only to artifact redemption.
- Expose `/api/internal/v1` only to the authenticated hosting control plane.
  Customer browsers and hosted workloads must never call Deployer directly.
- Configure the source broker, proxy adapter, and callback receiver with HTTPS.
  Plain HTTP is accepted only for loopback development endpoints. Source
  references are opaque identifiers, not caller-selected URLs. Treat the
  redemption JSON body as sensitive metadata and redact it, the Authorization
  header, and identity headers from broker traces and request logs.
- Grant the smallest service-token scopes possible. `hosting:admin` controls the
  global execution kill switch and should use a separate operational identity.
- Treat short-lived secret references as sensitive metadata even though
  Deployer never stores their plaintext values. Do not place plaintext secrets
  in manifests, idempotency keys, external IDs, audit reasons, or log messages.
- Configure `DEPLOYER_HOSTING_WORKLOAD_IDENTITY_SECRET` as an independent key of
  at least 32 random bytes. The secret broker validates issuer, audience,
  lifetime, reference digest, lease identity, and one-time token ID. Rotate with
  current/previous verification-key overlap of at least 90 seconds. Hosting
  agents redeem directly over HTTPS and place plaintext only in a `0700`,
  work-root-specific host tmpfs namespace mounted read-only into the Node container; applications read the
  named files from `DEPLOYER_SECRETS_DIR`. Do not enable swap-backed or
  disk-backed substitutes for that tmpfs.
- Run the dedicated hosting agent under its own OS account with access to the
  Docker engine and an isolated absolute work root. The agent executes
  untrusted customer build code inside platform-generated containers; keep the
  host kernel and Docker engine patched and do not mount the Docker socket into
  customer workloads.
- Every generated managed runtime carries a canonical-work-root namespace
  label. Agent inventory and pruning filter that exact namespace; unnamespaced
  legacy containers become eligible only after an exact retention match is
  durably recorded in that work root. Random boot sessions and
  monotonic heartbeat sequences fence duplicated runner credentials from
  publishing competing complete snapshots, and inventory collection failure
  never refreshes runner liveness.
- A replacement agent does not own or prune inherited runtimes until its first
  accepted session heartbeat. Legacy containers are adopted only after an
  exact authoritative retention match is durably recorded; unknown legacy or
  cross-namespace containers are never removed. Accepted superseded sessions
  are durably tombstoned so they cannot reclaim ownership. They use only the
  authenticated bounded retention response or non-owning session-retention
  endpoint, backed by a `0600` work-root acceptance marker, and exit after
  deleting handed-off containers and secret material without deleting
  potentially routed runtimes.
- Inventory-session activation requires recent exact instance evidence from
  the current session and a fresh control-plane health check. Rollback commits
  compare the same runner/instance identity and compensate a racing ownership
  loss; resume cannot route to a known-missing runtime.
- Do not publish `deployer.db`, build logs, screenshots, or local systemd/env
  files.

Hosting source archives, logs, callbacks, events, releases, and audit records
have separate retention settings. Active source artifacts are protected from
generic artifact cleanup, and active releases are never aged out. Review
`docs/HOSTING_RUNBOOK.md` before changing retention or restoring a database.
