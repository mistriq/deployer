# One server first, multiple Runtime servers later

These are deployment templates, not evidence of an installed or verified server.
The portal and Deployer run as separate host services, with a private loopback
API between them. A registry stores immutable image digests. The existing Runtime
continues to own application containers, health checks and public routing.
No Runtime image, environment contract or replacement stack is assumed here.

## Layout and prerequisites

Install portal source and its built frontend at `/opt/molo/portal`, the Linux
Deployer binary at `/usr/local/bin/runtime-deployer`, private configuration under
`/etc/molo` and `/etc/runtime-deployer`, and persistent state under `/var/lib`.
Use Node >=22.13, Git, Docker CLI with Buildx, and the Go version in `go.mod` when
building the binary. Create separate `molo` and `runtime-deployer` service users.
Use private regular credential/config files (0600, owned by the consuming user).
The organization-token JSON maps actual portal organization IDs to random service
tokens. Install equivalent production token maps privately for both services;
never copy Sandbox tokens, Runtime credentials or registry passwords into Production.

Preserve the existing portal database and encryption key together, and Deployer
state plus its state key together. Stop the relevant writer before migration and
back up both data and keys; do not initialize replacement keys for existing state.
Do not migrate an in-flight deployment to another worker or Runtime by editing JSON.

## Registry

`deploy/one-server/compose.yaml` runs only a registry, bound to host loopback.
Set `REGISTRY_IMAGE` to an operator-reviewed Distribution registry image pinned
by digest and `REGISTRY_AUTH_DIR` to a private directory containing `htpasswd`.
Generate bcrypt htpasswd entries interactively; do not place passwords in command
arguments. Keep storage on the persistent named volume and include it in backups.

Adapt `nginx-registry.conf.example` into the existing TLS proxy and issue a trusted
certificate for the selected registry hostname. Make that hostname resolve and
connect correctly from both the builder and Runtime containers. Container
`localhost` is not host loopback. If the existing proxy is itself containerized,
its upstream must reach the host explicitly; the provided host-proxy example is
not directly usable from a container. Keep `/v2/` authentication at the registry.

A registry with htpasswd authenticates users but does **not** provide repository
ACLs or pull-only identities: all authenticated users have equivalent repository
access. This is an initial private single-operator deployment, not tenant storage
isolation. For enforced namespace/push/pull separation, use an authorization-capable
registry before granting credentials to additional operators/nodes. Different
passwords alone do not implement ACLs.

Use the TLS hostname in `REGISTRY_IMAGE_PREFIX`, including on this first server.
Restrict access to the server/private network as appropriate. Later nodes can
reach the same endpoint over a private network without rebuilding artifacts.
Runtime's operator must configure its supported registry credential mechanism;
a host `docker login` is not proof that Runtime's daemon/API client can pull.

## Builder boundary and limits

The worker invokes `docker buildx build` in a temporary Docker configuration.
Install the supplied `docker` wrapper at `/opt/molo/builder-bin/docker`; its
service PATH routes Docker calls to `/run/deployer-docker/docker.sock`, an
operator-provisioned **dedicated builder daemon**, not the Runtime socket. The
real CLI path `/usr/bin/docker` must match the installation. Grant the worker
access to that socket only. The wrapper is necessary because the builder sanitizes
its subprocess environment; exporting `DOCKER_HOST` alone is not sufficient.

The daemon must support the default Docker Buildx driver. Configure and test the
builder's aggregate CPU/memory/disk limits before enabling source builds. The
systemd limits supplied here constrain worker processes only; they do not constrain
containers spawned by another daemon. A separate daemon by itself is neither a
resource limit nor a security boundary. Use a properly bounded builder VM or
verified container/cgroup placement; a VM may live on this same physical server.
Do not expose arbitrary customer Dockerfiles to the Runtime daemon. Moving the
builder to a dedicated build host later is strongly preferable for untrusted tenants.
This bundle deliberately does not invent a safe daemon configuration for the
operator's unknown Docker/cgroup installation. Builder isolation and limits remain
an installation acceptance requirement, not a claimed completed feature.

## Services and startup

Adapt the provided production environment files and systemd units. The portal
profile uses an external Deployer at `http://127.0.0.1:8091` so it does not launch
managed workers or require Docker socket access. Build the portal with `npm ci`
and `npm run build` before starting its service. Add portal OAuth/SMTP settings
from its existing deployment documentation. Expose portal port 4173 only through
the existing HTTPS proxy; both service listeners remain loopback.

For Runtime, retain the maintainer's existing service/image, persistent state,
secret key, proxy and application volumes. Supply its actual API address and a
separate production service token. This bundle does not start or restart Runtime.

Run `docker compose --env-file <private-registry-env> -f compose.yaml config --quiet`
in the template directory, then start the registry after configuring auth/TLS.
Validate the worker configuration with its actual service environment and
`runtime-deployer --check-config`; this opens its state store and is not a network
or registry readiness test. Do not run it against a live worker's state concurrently.
Install/start the units only after prerequisites above are met.

## Multiple Runtime targets

Keep stable target IDs distinct from the Production/Sandbox environment. A project
must stay assigned to a target; deployment snapshots must preserve that target so
status, retries, log requests and rollback return to the same server. Register a
second target with its own URL and credential when it is available. Start with one
default target and explicit placement; automatic capacity scheduling, moving live
projects and cross-server rollback are separate features, not implied by this setup.

The supplied `production-targets.json.example` starts with target `default`.
Replace its endpoint with the existing Runtime's actual local API binding and
install it as `/etc/runtime-deployer/production-targets.json` (0600). The example
loopback port 9510 must be verified against the installed Runtime mapping.
`runtime.env.example` selects this file through `RUNTIME_TARGETS_FILE`.

Add another entry with a new stable ID and its own HTTPS endpoint/token file to
register another server. Change `default_target_id` only to change placement for
new projects that omit an explicit target. Existing projects and all deployment
snapshots preserve their original target. Keep the `default` entry bound to the
original server for legacy records. An ID must never be reused for another node.
See [Runtime target configuration](runtime-targets.md) for the complete contract.

## Acceptance before dashboard Deploy

1. Confirm the registry requires authentication and has valid TLS from the builder
   and Runtime network contexts. Verify a pushed digest can be pulled by Runtime.
2. Prove builder CPU/memory limits and available disk headroom under a representative
   build, while existing application health remains good.
3. Validate the production profile and organization scoping; keep all existing
   Sandbox projects and credentials isolated.
4. Deploy one dedicated source project through the portal, recording build ID,
   commit, image digest, Runtime deployment/release and converged route revisions.
5. Verify public HTTPS content, build logs and runtime logs. Then verify a failed
   update preserves the old app, retries do not duplicate, and rollback restores
   the previous digest on its original target.

Sandbox lifecycle checks are separate from these actual container-hosting checks.
No server deployment, TLS issuance, Runtime credential provisioning or live
end-to-end verification was performed by creating these templates.
