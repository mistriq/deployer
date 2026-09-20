# Runtime Engine adapter contract

The adapter was verified against the authenticated sandbox at
`https://scr.socen.eu/api/internal/v1` on 20 September 2026. Read-only inspection
identified the actual response schema; opt-in live tests then passed for both
`static` and `node-http`, including deployment, health failure, preserved previous
release and historical-artifact restoration. The sandbox reported `nod_sandbox`,
`driver: mock`, `proxy: mock`, `store: memory`. This is not a production VPS pull or
container deployment. Results and operation IDs are in [runtime-verification.md](runtime-verification.md).

Configure an HTTPS origin or its `/api/internal/v1` URL and a server-side scoped
service token. HTTP is permitted only for loopback test servers. Redirects are
never followed, including same-host redirects. Sandbox mode puts
`X-Socen-Sandbox: true` on every request. The live test additionally validates the
exact host, sandbox header and fixture project identity independently before each
mutation. No production mutation, sandbox reset or token-management operation was
performed. Errors retain known failure codes, HTTP status and retryability;
remote error messages and bodies are discarded.

Required scopes are `projects:write`, `projects:read`, `secrets:write`,
`deployments:write`, `deployments:read`, and `routes:read`. No admin scope is
necessary. Environment writes replace the whole environment; an empty non-nil
map clears it. Values must never be logged or returned to a browser.

## Observed wire schema

- Project is a direct entity with `id`, `active_release_id`, `route_id`,
  `hostname`, manifest and metadata. Upsert returns the entity, not a job.
- Deployment is a direct entity with `id`, `project_id`, `release_id`, **`phase`**,
  optional `failure_code`, and `events: [{phase, at, ...}]`. There is no `status`
  field in the observed response. The adapter normalizes `Status` from `phase`.
- Release has `id`, `project_id`, `deployment_id`, `status`, `route_revision`,
  and `activated_at` after activation. The observed API has **no `healthy` or
  `health_status` field**. Its lifecycle evidence is checked as described below.
- Routes are `{ "items": [...] , "count": n }`. Entries use **`route_id`**,
  `project_id`, `release_id`, `status`, `desired_revision`, `applied_revision`,
  and `hostname`; `route_id` normalizes to `Route.ID`.
- Logs are `{ "items": [{"at": "ISO timestamp", "stream": "stdout|stderr",
  "text": "..."}], "count": n }`. `LogEntries` and `ReleaseLogEntries`
  preserve timestamps and streams; legacy `Logs` methods join the text.
  Missing/malformed records fail closed rather than silently losing logs.
- Environment metadata is `{project_id, env_version, variables: [{name, mask,
  digest, updated_at}]}`. Actual replacement, deletion and value nondisclosure
  were verified in the sandbox.
- Capabilities have `manifest_version`, `supported_manifest_kinds`, and `limits`
  with the capitalized limit names preserved in the original snapshot.
- Errors have `{error: {code, message, request_id, retryable}}`. The adapter
  retains only known codes and retryability. Malformed digest was observed as
  HTTP 422 / `ARTIFACT_DIGEST_MISMATCH`.

The adapter also accepts previously tested entity envelopes (`project`,
`deployment`, `release`, `data`), `status`, route `id`, and legacy `{logs: "..."}`
responses. Those compatibility shapes are unit-tested, not claimed as observed
on the current sandbox.

## Activation proof

Without an explicit healthy field, all of these must hold:

1. Deployment phase is `active`, with ordered, timestamped `health_check` →
   `activating` → `active` events and no failure/cancellation event.
2. Release is `active`, belongs to this project and this exact deployment,
   has `activated_at`, and a positive `route_revision`.
3. Project's `active_release_id` is this release.
4. An active route refers to this exact release; desired and applied revisions
   are equal and match the release's route revision.

Explicit unhealthy evidence, when present, denies activation. The compatibility
schema can report explicit healthy evidence instead of lifecycle events. Missing
or unrecognized evidence never marks a deployment online. This verifies Runtime's
health-gated lifecycle; it does not independently probe a production hostname from
Deployer. Controller verification timeout retains the same operation for
reconciliation instead of freeing its project for an ambiguous duplicate.

## Rollback and validation

Deploy requests require an immutable SHA-256 digest and an idempotency key.
The published native `/rollback` endpoint does not promise idempotency. Its
adapter method exists but is not used or live-tested: the portal rollback submits
a new idempotent deployment of a verified historical artifact and manifest/env
snapshot. This restoration path and duplicate replay were verified in sandbox.

Validation enforces manifest v1, `static`/`node-http`, a non-root numeric user,
read-only root, bounded resources/health checks, unique environment names, and
restricted temporary mounts. Capability validation respects lower advertised
node limits. Privileged mounts and host commands are not represented by the typed
manifest.

`go test -race ./internal/runtimeengine` runs only local fixtures by default.
The explicitly enabled live test is documented in [runtime-deployer.md](runtime-deployer.md).
