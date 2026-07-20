# Deployer

Deployer is a small self-hosted CI/CD tool written in Go. It runs as a single
binary with an embedded web UI, a SQLite database, live build logs over SSE, and
polling agents for remote machines.

The project is intended for maintainers who want a lightweight deployment
control plane without running a full CI system.

## Status

This repository is being prepared for a first public release. Review
`TODO.md` before using it for production workloads.

## Features

- Single Go binary with embedded HTML/CSS/JavaScript.
- SQLite storage.
- Docker deploy mode: build an image, transfer it to a runner, load it, and
  restart Docker Compose services.
- Files deploy mode: package files, transfer them to a runner, preserve selected
  paths, apply optional permissions, and run an optional post-deploy command.
- Remote snapshots: ask a runner to package the current deploy directory and
  upload it back to the server.
- Live build logs with collapsible steps.
- Runner auto-update from the server binary.

## Screenshots

The screenshots below use demo data only.

![Dashboard showing demo workflow runs](docs/screenshots/dashboard.png)

![Runner list showing demo self-hosted runners](docs/screenshots/runners.png)

![Project settings and build history for a demo API](docs/screenshots/project.png)

## Security Model

Deployer can execute commands and write files on every configured runner. Treat
server admin access and runner tokens as privileged production credentials.

By default, the server binds to `127.0.0.1:9090`. If you bind to a non-loopback
address, protect the UI/API with an upstream authorization gateway. Deployer no
longer implements local admin password login; direct network access to the
UI/API is admin access.

Recommended production setup:

- Put the server behind HTTPS and an authorization gateway.
- This deployment uses [Pocket ID](https://github.com/pocket-id/pocket-id) as
  the upstream OIDC authorization gateway.
- Bind Deployer to loopback or a private interface unless the gateway controls
  all public entry points.
- Use a reverse proxy with request-size limits appropriate for your artifacts.
- Keep runner tokens out of URLs, logs, shell history, and screenshots.
- Do not publish `deployer.db`, logs, generated binaries, or local config files.

## Installation

Prerequisites:

- Linux host for server and runners.
- Go 1.25 or newer for development. Release checks currently use Go 1.26.4.
- Git for source checkout and optional `git pull` before deploys.
- Docker with the Compose plugin for Docker mode.
- `tar`, `gzip`, `curl`, `ssh`, and `scp` for agent and remote operations.
- systemd if you use the example service units.

```bash
go build -o deployer ./cmd/deployer
./deployer
```

Open `http://127.0.0.1:9090` for local development, or access the production
URL through your authorization gateway.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `DEPLOYER_ADDR` | `127.0.0.1:9090` | HTTP listen address. |
| `DEPLOYER_DB_PATH` | `deployer.db` | SQLite database path. |
| `DEPLOYER_PUBLIC_URL` | empty | Public URL used in generated runner setup commands. |
| `DEPLOYER_ARTIFACT_DIR` | `/tmp/deployer-artifacts` | Managed server-side build artifact directory. |
| `DEPLOYER_SNAPSHOT_DIR` | `/tmp/deployer-snapshots` | Managed server-side snapshot artifact directory. |
| `DEPLOYER_ARTIFACT_RETENTION_HOURS` | `24` | Age after which stale artifact files are removed. |
| `DEPLOYER_LOG_RETENTION_DAYS` | `30` | Age after which stored build logs are cleared. |
| `DEPLOYER_DOCKER_PRUNE` | `false` | Enables broad Docker image/builder prune after deploys. |
| `DEPLOYER_AGENT_AUTO_UPDATE` | `false` | Enables checksum-verified agent self-update. Agent-side setting. |
| `DEPLOYER_SERVER_READ_TIMEOUT` | `30s` | Server request read timeout. |
| `DEPLOYER_SERVER_WRITE_TIMEOUT` | `5m` | Server response write timeout. SSE streams send heartbeats to stay active. |
| `DEPLOYER_DOCKER_BUILD_TIMEOUT` | `15m` | Docker build timeout. |
| `DEPLOYER_SCP_TIMEOUT` | `10m` | Remote SCP transfer timeout. |
| `DEPLOYER_SSH_TIMEOUT` | `5m` | Remote SSH deploy timeout. |
| `DEPLOYER_HEALTH_CHECK_TIMEOUT` | `60s` | Health-check timeout. |
| `DEPLOYER_DEMO_MODE` | `false` | Seeds public-safe demo projects, runners, and builds into an empty database for screenshots. |
| `DEPLOYER_HOSTING_DEPLOY_ROOT` | `/srv/deployer/hosting/apps` | Base directory used to derive hosted-project deploy paths. The API never accepts an arbitrary deploy path. |
| `DEPLOYER_HOSTING_SOURCE_BROKER_URL` | empty | Fixed private source-broker origin. HTTPS is required except for loopback tests; redirects are rejected. |
| `DEPLOYER_HOSTING_SOURCE_BROKER_TOKEN` | empty | Dedicated source-broker bearer credential. Never written to the database or logs. |
| `DEPLOYER_HOSTING_SOURCE_BROKER_TIMEOUT` | `4m` | Exact-source download timeout; must remain shorter than the server write timeout. |
| `DEPLOYER_HOSTING_SOURCE_MAX_BYTES` | `536870912` | Maximum accepted source tar size in bytes. |
| `DEPLOYER_HOSTING_WORKLOAD_IDENTITY_SECRET` | empty | Server-side HMAC key used only for 90-second secret-broker workload identities; use at least 32 random bytes. |
| `DEPLOYER_PROXY_ADAPTER_URL` | empty | Colleague-owned versioned reverse-proxy adapter URL. HTTPS is required except for loopback development. |
| `DEPLOYER_PROXY_ADAPTER_TOKEN` | empty | Private adapter bearer credential. Never written to the database or logs. |
| `DEPLOYER_PROXY_ADAPTER_TIMEOUT` | `15s` | Activation/suspension adapter request timeout. |
| `DEPLOYER_HOSTING_CALLBACK_URL` | empty | Hosting control-plane terminal callback URL. HTTPS is required except for loopback tests. |
| `DEPLOYER_HOSTING_CALLBACK_SECRET` | empty | HMAC callback key; use at least 32 random bytes. |
| `DEPLOYER_HOSTING_CALLBACK_TIMEOUT` | `15s` | Per-attempt callback timeout. |
| `DEPLOYER_HOSTING_CALLBACK_MAX_ATTEMPTS` | `12` | Attempts before a callback enters `dead_letter`. |
| `DEPLOYER_SERVICE_TOKEN_ROTATION_OVERLAP` | `24h` | Validity overlap retained for the prior service-token credential during rotation. |
| `DEPLOYER_HOSTING_LOG_RETENTION_DAYS` | `30` | Hosting log retention; `0` disables deletion. |
| `DEPLOYER_HOSTING_EVENT_RETENTION_DAYS` | `90` | Hosting event retention; `0` disables deletion. |
| `DEPLOYER_HOSTING_RELEASE_RETENTION_DAYS` | `90` | Inactive/failed release retention; active releases are never aged out. |
| `DEPLOYER_HOSTING_CALLBACK_RETENTION_DAYS` | `30` | Delivered/dead-letter callback retention. |
| `DEPLOYER_HOSTING_AUDIT_RETENTION_DAYS` | `365` | Hosting and service-token audit retention. |
| `DEPLOYER_AGENT_CONTROL_TIMEOUT` | `45s` | Agent heartbeat, poll, log, completion, and version-check HTTP timeout. Agent-side setting. |
| `DEPLOYER_AGENT_ARTIFACT_TIMEOUT` | `30m` | Agent artifact download/upload/update HTTP timeout. Agent-side setting. |
| `DEPLOYER_HOSTING_AGENT_WORK_ROOT` | `/tmp/deployer-hosting-agent` | Absolute dedicated hosting-agent work root. One agent process exclusively locks each root. Agent-side setting. |
| `DEPLOYER_HOSTING_AGENT_RUNTIME_BIND_ADDRESS` | `127.0.0.1` | Loopback or private IPv4 address reachable by the staging/production proxy adapter; public binds are rejected. |
| `DEPLOYER_HOSTING_AGENT_BUILD_NETWORK` | required | Docker internal network labeled `light-apps.hosting.restricted-egress=true` and connected only to approved package mirrors. |
| `DEPLOYER_HOSTING_AGENT_BUILD_TIMEOUT` | `15m` | Hard customer-build deadline; accepted range is 1–60 minutes. |
| `DEPLOYER_HOSTING_SECRET_BROKER_URL` | empty | Agent-side fixed private workload-secret broker origin. HTTPS is required except for loopback tests; redirects are rejected. |
| `DEPLOYER_HOSTING_SECRET_BROKER_TIMEOUT` | `15s` | Agent-side workload-secret redemption timeout; accepted range is greater than zero through one minute. |
| `DEPLOYER_HOSTING_AGENT_DRAINING` | `false` | Advertise drain and stop claiming new hosting work. Agent-side setting. |

One Deployer process exclusively owns each configured SQLite database. Startup
takes an OS lock on `DEPLOYER_DB_PATH` itself and fails closed on contention,
including symlink or hard-link aliases. In-memory server databases are rejected.
Give that database dedicated artifact and snapshot directories; this invariant
coordinates idempotent source acquisition with artifact cleanup.

See `.env.example` for a starter environment file.

## Running As A Service

Example user service:

```ini
[Unit]
Description=Deployer
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/deployer
ExecStart=/opt/deployer/deployer
Restart=always
RestartSec=3
Environment=DEPLOYER_ADDR=127.0.0.1:9090
Environment=DEPLOYER_DB_PATH=/var/lib/deployer/deployer.db
Environment=DEPLOYER_ARTIFACT_DIR=/var/lib/deployer/artifacts
Environment=DEPLOYER_SNAPSHOT_DIR=/var/lib/deployer/snapshots
Environment=DEPLOYER_ARTIFACT_RETENTION_HOURS=24
Environment=DEPLOYER_LOG_RETENTION_DAYS=30
Environment=DEPLOYER_SERVER_READ_TIMEOUT=30s
Environment=DEPLOYER_SERVER_WRITE_TIMEOUT=5m
Environment=DEPLOYER_DOCKER_BUILD_TIMEOUT=15m
Environment=DEPLOYER_SCP_TIMEOUT=10m
Environment=DEPLOYER_SSH_TIMEOUT=5m
Environment=DEPLOYER_HEALTH_CHECK_TIMEOUT=60s
Environment=DEPLOYER_PUBLIC_URL=https://deployer.example.com

[Install]
WantedBy=default.target
```

## Runner Setup

Create a runner from the **Runners** page. The setup command is shown once and
includes the runner token.

Agents authenticate with:

```text
Authorization: Bearer <runner-token>
```

The token is stored in `/etc/deployer/deployer-agent.env` in the generated
systemd example. Keep that file mode `0600`.

Generated setup commands prompt for the token rather than embedding it directly,
which keeps it out of pasted command examples and shell history.

## Deploy Modes

### Docker Mode

Server-side:

1. Optionally run `git pull`.
2. Read the current commit SHA.
3. Build a Docker image.
4. Save the image to a tar artifact.
5. Create a runner job.

Runner-side:

1. Download the image artifact.
2. Run `docker load`.
3. Restart configured Docker Compose services.
4. Run optional health checks.

### Files Mode

Server-side:

1. Optionally run `git pull`.
2. Package repository files into `tar.gz`.
3. Create a runner job.

Runner-side:

1. Download the archive.
2. Back up configured preserve paths.
3. Extract files into `deploy_dir`.
4. Restore preserve paths.
5. Apply optional ownership and permissions.
6. Run optional post-deploy command.
7. Run optional health checks.

## Ignore Files

Files mode reads `.deployignore` from the source repository. If missing, it
falls back to `.dockerignore`.

The matcher currently uses Go `filepath.Match`-style patterns rather than full
`.gitignore` semantics.

Example:

```text
node_modules
*.log
.env
.git
```

## API Overview

Admin UI/API routes rely on the upstream authorization gateway. Browser
state-changing requests also include `X-Deployer-CSRF: 1`.

Agent routes require `Authorization: Bearer <runner-token>`.

Private control-plane routes require a scoped Deployer service token. Create
the token through the admin API; its plaintext value is returned only once and
is stored hashed. Customer-facing applications must not call these routes
directly.

See `docs/openapi.yaml` for a machine-readable OpenAPI reference with request
and response examples.

Failed or cancelled builds and jobs include stable `error_code` fields for
runtime failures such as `artifact_failed`, `health_check_failed`,
`runner_offline`, and `cancelled`. API clients should use those fields instead
of parsing `error_message`.

HTML:

- `GET /`
- `GET /projects/new`
- `GET /projects/:id`
- `GET /builds/:id`
- `GET /runners`

Admin API:

- `GET /api/capabilities`
- `GET /api/projects`
- `POST /api/projects`
- `POST /api/projects/import`
- `GET /api/projects/:id`
- `GET /api/projects/:id/summary`
- `GET /api/projects/:id/preview`
- `GET /api/projects/:id/runbook`
- `GET /api/projects/:id/autodetect`
- `GET /api/projects/:id/history`
- `GET /api/projects/:id/export`
- `POST /api/projects/:id/clone`
- `PUT /api/projects/:id`
- `DELETE /api/projects/:id`
- `POST /api/projects/:id/deploy`
- `POST /api/projects/:id/snapshot`
- `GET /api/builds/:id`
- `GET /api/builds/:id/stream`
- `GET /api/builds/:id/events`
- `GET /api/builds/:id/failure-summary`
- `GET /api/builds/:id/release-notes`
- `GET /api/builds/:id/annotations`
- `POST /api/builds/:id/annotations`
- `DELETE /api/builds/:id/annotations/:annotationId`
- `POST /api/builds/:id/cancel`
- `GET /api/builds/:id/artifact`
- `GET /api/runners`
- `POST /api/runners`
- `GET /api/runners/:id`
- `GET /api/runners/:id/history`
- `POST /api/runners/:id/rotate`
- `DELETE /api/runners/:id`
- `GET /api/service-tokens`
- `POST /api/service-tokens`
- `POST /api/service-tokens/:id/rotate`
- `DELETE /api/service-tokens/:id`

Private control-plane API:

- `PUT /api/internal/v1/projects/:externalProjectId`
- `POST /api/internal/v1/projects/:externalProjectId/deployments`
- `GET /api/internal/v1/deployments/:externalDeploymentId`
- `POST /api/internal/v1/deployments/:externalDeploymentId/cancel`
- `GET /api/internal/v1/deployments/:externalDeploymentId/events`
- `GET /api/internal/v1/deployments/:externalDeploymentId/logs`
- `GET /api/internal/v1/projects/:externalProjectId/releases`
- `POST /api/internal/v1/projects/:externalProjectId/rollback`
- `POST /api/internal/v1/projects/:externalProjectId/suspend`
- `POST /api/internal/v1/projects/:externalProjectId/resume`
- `PUT /api/internal/v1/projects/:externalProjectId/kill-switch`
- `PUT /api/internal/v1/settings/kill-switch`
- `GET /api/internal/v1/capabilities`
- `GET /api/internal/v1/runners`
- `GET /api/internal/v1/metrics`

Rollback selects an immutable release instance with both its
`external_deployment_id` and `release_digest`; a digest alone is intentionally
insufficient because content-identical deployments can have distinct runtime
instances.

Scopes are `projects:write`, `deployments:read`, `deployments:write`, and
`hosting:admin`. Service credentials are hashed, rotation keeps the configured
overlap window, revocation invalidates every credential, and authentication,
scope denial, rotation, revocation, runner credential changes, and kill-switch
changes are audited. Mutating lifecycle calls use `Idempotency-Key`; a key is
scoped to issuer and operation and conflicts if reused for a different payload.
Suspend/resume responses acknowledge durable desired state; adapter
convergence is asynchronous and generation-fenced during an adapter outage.

Provision accepts only the versioned hosting manifest documented in OpenAPI:
GitHub App installation/repository IDs, `static` or `node`, an allowlisted Node
version and package manager, package.json script names, paths, port, health
path, and a resource profile. It does not accept repository/deploy filesystem
paths, SSH targets, Compose, build args, environment secrets, or shell hooks.
Deployer derives runtime namespaces from `external_project_id` and stores hosting
state separately from trusted admin projects/builds. Hosting jobs cannot enter
the legacy builder, Compose, Dockerfile, SSH, or `post_deploy` paths.

The exact provision request body must be authenticated with the service token
and sent as `Content-Type: application/json`, signed using these headers:

- `X-Deployer-Timestamp`: current Unix timestamp in seconds (maximum clock
  skew five minutes).
- `X-Deployer-Signature`: `sha256=<hex HMAC-SHA256>`, where the HMAC key is the
  service token and the signed bytes are `<timestamp>.<raw request body>`.

Deployment creation requires an opaque, short-lived `control-plane`
`source_reference`. Deployer redeems it only against the fixed authenticated
source broker, with the provisioned installation ID, repository ID/full name,
full 40-character commit SHA, and expected SHA-256 digest in the request. It
requires the broker response to echo that identity, rejects redirects,
oversize bodies, malformed/unsafe tar entries, and digest mismatches, hashes
the stream, and durably commits it to managed content-addressed storage before
deployment state is created. Deployer never persists the broker credential or
source reference. Completed idempotent replays remain valid after the reference
expires because they perform no new redemption. The exact broker request,
response, authentication, and retry contract is in
[`docs/source-broker-openapi.yaml`](docs/source-broker-openapi.yaml).

Only opaque `control-plane` secret references and their safe uppercase file
names are persisted. Immediately before a Node runtime starts, the assigned
agent obtains a 90-second identity bound to its current job or recovery lease
and exact reference-set digest, then redeems those references directly against
the fixed HTTPS secret broker. Plaintext bypasses the Deployer server,
database, build storage, logs, callbacks, Docker arguments, and images; it is
handled only by the control-plane secret-broker subsystem and the assigned
dedicated agent/runtime. The agent writes the exact response set into an
isolated host tmpfs namespace and mounts it
read-only at `/run/secrets/deployer`. The application reads files by name from
`DEPLOYER_SECRETS_DIR`. Static workloads cannot request secrets. A restore uses
a newly minted identity and re-redemption, not cached plaintext. Agent startup
reconciles leftover tmpfs directories with managed containers. If a reboot
erases material for a retained secret container, the agent refuses to heartbeat
so runner-loss recovery performs fresh redemption. The boundary
contract and HMAC key-overlap procedure are in
[`docs/secret-broker-openapi.yaml`](docs/secret-broker-openapi.yaml). Callers
must branch on stable JSON `code`, `status`, `phase`, and `failure_code` values
rather than human text.

Terminal callbacks are delivered at least once. Their signature covers
`<timestamp>.<event_id>.<raw body>`; receivers reject timestamps outside five
minutes and deduplicate event IDs. Polling remains authoritative when delivery
is missed or reaches `dead_letter`. See [the hosting operations runbook](docs/HOSTING_RUNBOOK.md)
and `docs/openapi.yaml` for the complete schemas and recovery contract.

Dedicated hosting runner:

```bash
./deployer hosting-agent \
  --server https://deployer.internal.example \
  --token 'htr_…' \
  --work-root /var/lib/deployer-hosting-agent \
  --runtime-bind-address 10.20.0.15 \
  --build-network hosting-build-egress \
  --secret-broker https://hosting-control.internal.example \
  --build-timeout 15m
```

Register and rotate hosting runners through `/api/hosting-runners`; credentials
are separate from legacy runner and service-token credentials. The agent
requires Docker and a pre-created internal, restricted-egress build network,
accepts only generated static/Node recipes, rejects unsafe tar entries, binds
candidate ports to the configured private address, drops all capabilities,
enables `no-new-privileges`, uses a read-only root filesystem, and enforces CPU,
RAM, disk, PID, build-time, and temporary-filesystem limits. Before activation
it uploads a digest-verified Docker image archive to Deployer-managed artifact
storage. Heartbeats reconcile exact project/deployment release instances. If
an active runner is lost, a generation-fenced recovery lease downloads that
retained image, verifies both archive and image identity, starts and health
checks it without rebuilding customer source, and switches the private adapter
through a durable idempotent operation. Restore and rollback use the immutable
runtime/health snapshot stored with the release, not a later project manifest.

Every activation, rollback, suspension, resume, recovery and compensation sent
to the proxy adapter carries a positive, per-project monotonic
`route_generation`. The adapter must atomically ignore requests below the
highest generation it has applied for that project. Deployer persists each
generation before the network call and reconciles pending operations after
restart; compensations receive a newer generation.

Restore is explicitly negotiated through the runner `operations` capability.
Legacy v1 agents that omit it are treated as build-only and never receive a
restore recipe. During a rolling upgrade, deploy the new agents and confirm
`restore` appears in `/api/internal/v1/runners` before enabling reliance on
active-runtime recovery. Deployment polling exposes `runtime_status` as
`available`, `recovering`, or `unavailable`; it remains authoritative after the
original deployment callback. Secret-bearing releases obtain a fresh
restore-lease identity and redeem their references again after runner loss;
plaintext is never copied from the prior runner. Recovery currently starts
from whole-runner liveness loss. The heartbeat
response is an authoritative retention set, not a runner-reported runtime
inventory, so a missing container on an otherwise online runner is not yet
detected and the overall runner-recovery TODO item remains open.

Runtime-secret support is negotiated independently as the
`runtime-secrets-v1` runner operation. The agent advertises it only when a fixed
secret broker is configured and its work-root-specific tmpfs namespace passes
startup reconciliation. Secret-bearing jobs and recoveries are never assigned
or claimed without that capability. During rollout, update and heartbeat
secret-ready agents before submitting secret-bearing deployments; older v1
agents remain eligible only for work that has no runtime secrets.

The local client, persistence and reconciliation paths are generation-fenced.
Real staging acceptance must still prove that the colleague-owned adapter
enforces the matching monotonic-generation contract.

Agent API:

- `GET /api/agent/poll`
- `GET /api/agent/artifact/:buildId`
- `POST /api/agent/snapshot/:buildId`
- `POST /api/agent/log/:buildId`
- `POST /api/agent/complete/:buildId`
- `POST /api/agent/heartbeat`

Dedicated hosting-agent API:

- `POST /api/hosting-agent/v1/heartbeat`
- `POST /api/hosting-agent/v1/poll`
- `/api/hosting-agent/v1/jobs/:jobId/{source,release-artifact,heartbeat,workload-identity,phase,logs,complete}`
- `/api/hosting-agent/v1/recoveries/:recoveryId/{artifact,heartbeat,workload-identity,logs,complete}`

Agent-accessible release endpoints:

- `GET /api/version`
- `GET /download/deployer`

Project exports use a `deployer.project` envelope. Runtime bindings and likely
secret-bearing values are intentionally left out: runner assignment,
post-deploy shell commands, and build args whose names or values look sensitive.
The response includes `omitted_fields` so operators can review and re-enter
environment-specific values after import.

## Project Field Validation

Project configuration is trusted admin input, but the server rejects several
dangerous values before jobs are created:

- `deploy_dir` must not be empty or `/`.
- `dockerfile_path` and `compose_file` must be relative paths and must not
  traverse outside the project.
- `health_url` must be `http` or `https` and include a host.
- `image_name`, `health_container`, `compose_services`, and `ssh_host` reject
  shell-sensitive characters.
- `preserve` paths and permission patterns must be relative paths.
- `permissions` must be valid JSON with octal modes and safe owner names.

Example permissions JSON:

```json
{
  "owner": "www-data:www-data",
  "files": {
    "*.php": "0644"
  },
  "dirs": {
    "storage": "0775"
  }
}
```

`post_deploy` is privileged code execution on the runner. Only configure it for
projects and operators you trust. Fixed deploy steps use structured command
arguments; `post_deploy` is the intentional shell-script escape hatch for
operator-defined commands.

## Files Mode Safety

Files mode creates a `tar.gz` archive from the source repository and extracts it
on the runner. Extraction rejects absolute paths, `../` traversal, symlinks,
device files, and unsupported archive entry types.

Preserve paths are backed up before extraction and restored afterward. Preserve
entries are relative to `deploy_dir`; absolute paths and traversal are rejected.

Packaging can still record symlinks from the source tree. Review `TODO.md`
before relying on files mode for untrusted repositories.

## Snapshots

Snapshots ask a runner to package its current `deploy_dir` and upload the
archive to the server. Snapshot artifacts may contain secrets from deployed
applications, so restrict artifact access, keep request-size limits high enough
for expected snapshots, and delete old snapshots according to your retention
policy.

## Auto-Update

Agent self-update is disabled by default. Set `DEPLOYER_AGENT_AUTO_UPDATE=true`
or pass `--auto-update` to opt in.

When enabled, agents check `/api/version` and download `/download/deployer` when
the server version changes. Protect these endpoints with the same authorization
gateway used for the UI/API, or expose them only on a trusted network for
runner access. The agent verifies the downloaded binary against the
`checksum_sha256` value returned by `/api/version`, stages it next to the
current executable, atomically swaps it into place, and keeps the previous
binary as `<name>.old`.

After installing an update, the agent exits and expects an external process
manager such as systemd to restart it. Binary signing and configurable update
channels are still tracked in `TODO.md`.

## Database Operations

SQLite uses WAL mode. The default database path is `deployer.db`, and the
recommended service path is `/var/lib/deployer/deployer.db`. New timestamps are
stored as UTC RFC3339 values. Build artifacts and snapshots are stored in
`DEPLOYER_ARTIFACT_DIR` and `DEPLOYER_SNAPSHOT_DIR`.

Back up the database while the server is stopped, or use SQLite's online backup
tooling so the `deployer.db`, `deployer.db-wal`, and `deployer.db-shm` state is
captured consistently.

Example stopped backup:

```bash
systemctl --user stop deployer
cp /var/lib/deployer/deployer.db /var/backups/deployer.db
systemctl --user start deployer
```

Restore by stopping the service, replacing the database file with a known-good
backup, and starting the service again. Do not publish database backups.

Persisted build logs are capped at 4 MiB per build. By default, stale artifact
files are removed after 24 hours and build logs are cleared after 30 days. Set
`DEPLOYER_ARTIFACT_RETENTION_HOURS=0` or `DEPLOYER_LOG_RETENTION_DAYS=0` to
disable those cleanup policies.

## Reverse Proxy

For internet-facing use, terminate HTTPS at a reverse proxy and forward to the
loopback listener. Configure:

- HTTPS with modern TLS settings.
- An authorization gateway that authenticates users before forwarding UI/API
  requests to Deployer. This deployment uses
  [Pocket ID](https://github.com/pocket-id/pocket-id) for that gateway.
- Request-size limits large enough for Docker image artifacts and snapshots.
- Proxy read timeouts longer than agent long-polling and large artifact
  transfers.
- Access logs that do not record Authorization headers.

## Operational Recovery

On startup, running builds are marked cancelled and unfinished jobs are marked
failed. If a deploy is interrupted, inspect the build log, runner service logs,
and target Docker Compose state before retrying.

Temporary artifacts are written under the managed artifact directories and are
cleaned periodically according to `DEPLOYER_ARTIFACT_RETENTION_HOURS`. Clean
stale files manually after crashes only once you have confirmed no active build
needs them.

## Migration Policy

Current schema changes are applied from code at startup by named, idempotent
migrations recorded in the `schema_migrations` table. Future public releases
should document migrations in the changelog and avoid destructive schema changes
without an explicit backup step.

## Development

```bash
go test ./...
go vet ./...
go test -race ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.3.0 ./...
```

Use a temporary database for local development:

```bash
DEPLOYER_DB_PATH=/tmp/deployer-dev.db go run ./cmd/deployer
```

Seed public-safe screenshot data into an empty temporary database:

```bash
DEPLOYER_DEMO_MODE=true DEPLOYER_DB_PATH=/tmp/deployer-demo.db go run ./cmd/deployer
```

## Repository Layout

- `cmd/deployer` contains the binary entrypoint.
- `internal/app` contains the server, agent, storage, builder, handlers, and
  package tests.
- `internal/app/web` contains embedded templates and static assets.
- `docs` contains OpenAPI documentation and public-safe screenshots.

## Release Checklist

Before publishing a release:

- Build with a patched Go toolchain.
- Run tests, race tests, vet, and `govulncheck`.
- Confirm no databases, binaries, unreviewed screenshots, logs, keys, `.env`
  files, or machine-specific configs are present.
- Rotate any token that was ever present in screenshots, logs, local databases,
  or shell history.
- Review `TODO.md` for remaining security and reliability work.

Manual release process:

1. Create a clean Git worktree with only source and public-safe documentation.
2. Run the release checks listed above.
3. Tag the release and build binaries from the tag.
4. Publish checksums with the binaries.
5. Update `CHANGELOG.md`.

## Roadmap

The immediate roadmap is public-release hardening: token rotation, signed
updates, artifact retention, broader test coverage, and clearer operations docs.
Longer-term product ideas such as webhooks, rollback, project templates, and AI
tooling are tracked separately in `TODO.md`.
