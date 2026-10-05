# Runtime target routing

A deployment environment (Sandbox/Production) and a Runtime target (server) are
separate concepts. Each worker belongs to one environment and can select among
configured servers. Every request to every server uses the worker's
`RUNTIME_SANDBOX` value; a catalogue cannot override it per server.

## Configuration

Legacy `RUNTIME_BASE_URL` and `RUNTIME_TOKEN_FILE` create the target `default`.
Alternatively set `RUNTIME_TARGETS_FILE` to an absolute private regular JSON file
(0600/0400). It fully replaces legacy Runtime settings, with no fallback:

```json
{
  "default_target_id": "default",
  "targets": {
    "default": {
      "base_url": "http://127.0.0.1:9510",
      "token_file": "/etc/runtime-deployer/production-runtime-token"
    },
    "node-2": {
      "base_url": "https://runtime-2.example.com",
      "token_file": "/etc/runtime-deployer/node-2-token"
    }
  }
}
```

The addresses above are examples: configure only servers actually provisioned.
HTTP is allowed only on loopback; remote endpoints require HTTPS. Credentials
are loaded from absolute private regular file paths, included in log redaction,
and never returned through the public API. Unknown JSON fields and trailing
JSON are rejected. Target IDs match `^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$`; 1–128
targets are supported and the configured default must exist. Configuration is
loaded at startup; restart the worker deliberately to reload it.

Use separate worker stores and credential catalogues per environment. The
registry must be reachable from every configured target and the builder. The
configuration check rejects loopback registry addresses with remote production
Runtime endpoints; it does not probe networking or prove registry authorization.

## API and durable placement

`PUT /internal/v1/projects/{id}` accepts optional top-level `runtime_target_id`
alongside `repository`, `ref`, `build`, and `manifest`. The response includes the
resolved `runtime_target_id`. Creating a project without it uses
`default_target_id`; updating a project without it preserves the existing target.
An unknown target returns HTTP 400 `RUNTIME_TARGET_UNKNOWN`.

The target can change only before any deployment job exists. Once deployment
history exists (including failed jobs), a different target returns HTTP 409
`RUNTIME_TARGET_LOCKED`. Project migration is not implemented.

Deployment creation/rollback responses and GET deployment status expose
`runtime_target_id`. The value comes from the persisted deployment snapshot.
Worker submission, verification, runtime log reads, and rollback all use that
snapshot. Rollback release lookup also filters by target, since release IDs need
not be globally unique across servers. Build log reads remain local.

## Legacy records and failure handling

A historical project/job without a target always means literal `default`, even
if `default_target_id` later changes. When migrating an existing store to a
catalogue, keep a `default` entry pointing at its original Runtime. Never change
an existing target ID to represent another physical/logical node. Endpoint
changes for the same node require operator verification; the implementation
does not authenticate immutable node identity independently of endpoint/token.

Removing a target does not reassign its jobs. Pending jobs retain their identity
and expose `RUNTIME_TARGET_UNAVAILABLE`; they make no build/Runtime calls until
the target is restored. Runtime logs fail closed when the server is absent.
New requests cannot use an unconfigured target. No automatic failover, load-based
scheduling, live project migration, or cross-server rollback is implied.

## Verification scope

Tests cover two Runtime fixtures, default changes, encrypted-store restart,
legacy records, target removal, locked placement, rollback and log routing.
CLI tests check independent credentials and Sandbox headers for both targets.
These tests prove control-plane routing, not live hosting on two physical nodes.
