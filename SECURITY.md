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
  service (`dpl_`), callback HMAC, and proxy-adapter credentials separate. None
  is valid in another credential class.
- Expose `/api/internal/v1` only to the authenticated hosting control plane.
  Customer browsers and hosted workloads must never call Deployer directly.
- Configure the proxy adapter and callback receiver with HTTPS. Plain HTTP is
  accepted only for loopback development endpoints.
- Grant the smallest service-token scopes possible. `hosting:admin` controls the
  global execution kill switch and should use a separate operational identity.
- Treat short-lived secret references as sensitive metadata even though
  Deployer never stores their plaintext values. Do not place plaintext secrets
  in manifests, idempotency keys, external IDs, audit reasons, or log messages.
- Run the dedicated hosting agent under its own OS account with access to the
  Docker engine and an isolated absolute work root. The agent executes
  untrusted customer build code inside platform-generated containers; keep the
  host kernel and Docker engine patched and do not mount the Docker socket into
  customer workloads.
- Do not publish `deployer.db`, build logs, screenshots, or local systemd/env
  files.

Hosting source archives, logs, callbacks, events, releases, and audit records
have separate retention settings. Active source artifacts are protected from
generic artifact cleanup, and active releases are never aged out. Review
`docs/HOSTING_RUNBOOK.md` before changing retention or restoring a database.
