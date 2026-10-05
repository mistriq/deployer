# Portal agent task: real deployments

Finish the portal's part in `../../portal`. The Deployer agent owns
`../deployer`, Docker/building, registry publishing, and the Runtime adapter.
You own portal configuration, account/project integration, and UI.

## Current state

- Runtime: https://scr.socen.eu. Real nginx and CyberChef hosting was verified
  directly through Runtime, not through the portal.
- Portal currently uses Sandbox. Sandbox lifecycle success is not proof of
  actual container hosting.
- User organization: `org_ba99ad7d7fb1461587430dbfcce5ad73`
  (Jakub Illich – pracovní prostor).
- Prepared source projects, with no deployments at creation:
  - CyberChef: `prj_9ea421960c00b2d90f71691dfce391e4`;
    `https://github.com/gchq/CyberChef`, ref `master`, context `.`,
    Dockerfile `Dockerfile`, static, port 8080, output `build/prod`.
  - NGINX: `prj_d597a2d3dcd8a3e0218e04b2eb195927`;
    `https://github.com/nginx/docker-nginx-unprivileged`, ref `main`,
    context `stable/alpine`, Dockerfile `Dockerfile`, static, port 8080.
- These are new source projects, not imports of the existing real containers.

## Your work

1. Make the dashboard ready for deliberate real deployments with an
   unmistakable environment indicator.
2. Preserve isolation between sandbox and production worker stores,
   project/deployment records, and credentials. Do not switch the global
   environment and reinterpret old sandbox records as production.
3. Preserve custom Dockerfile/context settings through editing and deployment;
   add UI support where missing.
4. Preserve organization authorization, idempotency, deployment status,
   both log sources, failure preservation, and rollback against the existing
   Deployer API (`runtime-deployer.md` and `openapi.yaml`).
5. Report configuration/build/registry failures truthfully. Only show online
   after real release and routing verification. Do not represent sandbox
   simulation as public hosting.
6. Run appropriate portal tests and report changes, checks, exact startup
   requirements, and remaining Deployer dependencies.

Do not modify Deployer, reset shared Runtime state, delete other projects,
expose credentials, or fabricate imported deployment history. Preserve the
existing real apps. Real deployments of our dedicated test apps are authorized;
coordinate the final end-to-end test after builder/registry readiness.

## Deployer update — 2026-09-22

- Docker server query still timed out after 12 seconds; context is
  `desktop-linux`. Docker processes exist, so this is not merely a missing CLI.
- Configured registry is `127.0.0.1:5500/molo`. A remote Runtime cannot use this
  as the address of the builder's registry. No remote push registry was configured.
- Added startup / `--check-config` rejection of loopback or unspecified registry
  addresses when using a non-local Runtime with `RUNTIME_SANDBOX=false`.
  Sandbox and local integration configurations remain accepted.
- This check is configuration validation only. Passing it does not prove DNS,
  registry authentication, image push, or Runtime pull access.
- Tests passed for command configuration, control plane, OCI builder, and
  Runtime adapter. Actual Docker integration was not rerun while Docker hangs.
- The worker binary was rebuilt. Existing workers must be restarted deliberately
  to load the new binary; no portal worker was restarted by this change.

Still needed: a responsive Docker builder; a registry address reachable from
both builder and Runtime; push credentials and, for private images, Runtime
pull access. Keep secrets in private files, never in this handoff or browser.

### Docker recovery verified later on 2026-09-22

Docker Desktop and engine were hung. Normal restart stalled. After terminating
Docker processes, waiting for VM shutdown, and force-stopping the one lingering
backend, Docker Desktop started successfully. Engine 28.3.3 and Buildx
v0.27.0 respond. The actual Docker integration test passed in 7.03 seconds:
static and Node builds, authenticated temporary registry push/pull by digest,
and restricted-container health checks. This supersedes the Docker blocker above.

The remote registry remains unresolved. Existing `molo-runtime-registry` starts
but has no host port bindings, and localhost:5500 refuses connections. It was
not recreated or reconfigured. Neither prepared portal project was deployed.
