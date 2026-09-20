# Runtime Engine adapter contract

The adapter uses the 20 September 2026 published Runtime Engine catalogue at
`https://scr.socen.eu/api/internal/v1`. No production mutation was performed while
implementing it. Read-only authenticated entity responses could not be inspected:
the available public snapshot documents request shapes but omits response schemas.
**The entity response assumptions below require verification against the target
node before production use.** Successful HTTP admission is never sufficient to
mark a deployment active.

Configure an HTTPS origin or its `/api/internal/v1` URL and a server-side scoped
service token. HTTP is permitted only for loopback test servers. Redirects are
never followed, including same-host redirects. Sandbox mode puts
`X-Socen-Sandbox: true` on every request. The adapter never retries a mutation
internally. Errors retain known failure codes, HTTP status and a retryability
hint; remote error messages and bodies are discarded.

Required scopes are `projects:write`, `projects:read`, `secrets:write`,
`deployments:write`, `deployments:read`, and `routes:read`. No admin scope is
necessary. Environment writes replace the whole environment; an empty non-nil
map clears it. Values must never be logged or returned to a browser.

Expected JSON responses are direct entities, `{ "project": ... }`,
`{ "deployment": ... }`, `{ "release": ... }`, or `{ "data": ... }`:

- Project: `id` (or `project_id`), `active_release_id`, optional `route_id`.
- Deployment: `id`, `project_id`, `release_id`, `status`, `phase`, optional
  `failure_code`. An active deployment has status `active`. Failed, cancelled,
  canceled and superseded deployments return terminal adapter errors.
- Release: `id`, `project_id`, and either `healthy: true` or
  `health_status: "healthy"`.
- Routes: an array, `{ "items": [...] }` or `{ "routes": [...] }`.
  Entries have `id`, `project_id`, `release_id` (or `active_release_id`),
  `status`, `desired_revision`, `applied_revision`, optional `hostname`.
- Logs: `{ "logs": "..." }`; only server-redacted logs should be exposed.
- Capabilities: `manifest_version`, `supported_manifest_kinds`, `limits` using
  the capitalized limit keys in the published catalogue.

Activation is verified by independently reading deployment, release, project and
routes. The release must belong to the project and report healthy, the project's
active release must match it, and its active route must reference that exact
release with equal, positive desired/applied revisions. Missing or unrecognized
state cannot activate a deployment. Polling needs a controller-side deadline.

Deploy requests require an immutable SHA-256 digest and an idempotency key.
Rollback requires an explicit release that belongs to the project. The published
rollback endpoint does **not** promise idempotency, so an ambiguous rollback
transport failure must not be automatically retried. Reconcile state first, or
use a new deployment of an approved historical artifact with a stable deployment
idempotency key. The rollback response is assumed to be a deployment entity;
confirm this with the target node before enabling direct rollback.

Validation enforces manifest v1, `static`/`node-http`, a non-root numeric user,
read-only root, bounded resources and health checks, unique environment names,
and restricted temporary mounts. Capability validation also respects lower
advertised node limits. No privileged mounts, host commands or arbitrary fields
are represented by the typed manifest.

Run `go test -race ./internal/runtimeengine` for the isolated contract tests.
Tests use local HTTP fixtures and make no live Runtime Engine calls.
