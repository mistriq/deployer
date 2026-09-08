# MCP integration

Deployer includes a focused MCP server that uses the existing authenticated HTTP API. It does not open the database or run deployment logic directly.

Configure a read credential and start the stdio server:

```sh
export DEPLOYER_MCP_URL=https://deployer.example.com
export DEPLOYER_MCP_READ_TOKEN='read-only bearer credential'
deployer mcp
```

Set the same `DEPLOYER_MCP_READ_TOKEN` on the Deployer server. Set `DEPLOYER_MCP_WRITE_TOKEN` on both sides to expose `deployment_trigger`, `build_cancel`, and `snapshot_request`. Deployer validates these bearer credentials itself with constant-time comparison: the read credential can call GET endpoints, while state-changing API calls require the write credential. A valid machine credential replaces the browser CSRF header; an invalid bearer fails closed and never falls through to interactive gateway authentication. Requests without `Authorization` continue to use the existing browser/gateway flow. Keep these environment variables out of configuration checked into source control, and use distinct random values.

The server supports MCP protocol `2025-06-18` over newline-delimited JSON-RPC stdio. Its typed tools cover project and runner lists, project summary and configuration, recent builds, bounded build logs, events, failure summaries, runbooks, deployment previews, deployment trigger, cancellation, and snapshot requests. Deployment and snapshot tools immediately return a build ID. Use `build_get`, `build_events`, and `build_failure_summary` to follow and diagnose the asynchronous operation.

Every deployment or snapshot call requires an `idempotency_key` of 8 to 200 characters. Reuse it when retrying the same intended action. Deployer persists the action, project, a hash of the key, and the build ID, so a retry returns the original build instead of starting another one. PostgreSQL serializes each key with a session advisory lock. If the server exits mid-request, a retry either finds the uniquely marked build or safely reclaims an empty reservation.

MCP responses redact token, secret, password, API-key, and build-argument values. `build_get.log_bytes` limits returned log data to at most 64 KiB and defaults to 16 KiB.

Example client configuration:

```json
{
  "mcpServers": {
    "deployer": {
      "command": "/usr/local/bin/deployer",
      "args": ["mcp"],
      "env": {
        "DEPLOYER_MCP_URL": "https://deployer.example.com",
        "DEPLOYER_MCP_READ_TOKEN": "${DEPLOYER_MCP_READ_TOKEN}",
        "DEPLOYER_MCP_WRITE_TOKEN": "${DEPLOYER_MCP_WRITE_TOKEN}"
      }
    }
  }
}
```

New tools should remain thin mappings to authenticated HTTP endpoints. Add their JSON Schema in `mcpServer.tools` and route them in `mcpServer.call`; do not duplicate application operations in the MCP process.
