# Runtime sandbox capacity — handoff, 20 September 2026

## Message to forward to the Runtime maintainer

Please unblock the existing sandbox deployment without resetting the sandbox or touching production. Every request must carry `X-Socen-Sandbox: true` to `https://scr.socen.eu`. Preserve shared fixtures, other projects, release history, secrets, routes and idempotency records. Do not resubmit the portal operation under a new ID.

The Deployer operation `dep_ecf8156baba32e8bdb8c89cb2b0afbd6` remains `deploying/submitting`, retrying `NODE_CAPACITY_EXHAUSTED`. Its Runtime project is `prj_ec24c947789e0d0f3e341f6ddbce2443`; its portal project is `prj_b12a91f9bb48ce3443ec023de1f881f6`. Runtime project deployment history is empty: admission has not yet created a deployment. Deployer has exactly one local job and continues retrying the same immutable external ID and idempotency key, both equal to the Deployer operation ID.

Observed at approximately 02:04–02:06 UTC:

| `/api/internal/v1/node` | CPU millicores | Memory MB | Disk MB |
| --- | ---: | ---: | ---: |
| total | 8000 | 16384 | 204800 |
| allocated | 7500 | 7936 | 15360 |
| reserve | 500 | 1024 | 10240 |
| available after reserve | **0** | 7424 | 179200 |
| pending candidate needs | **500** | 512 | 1024 |

Node reports `driver=mock`, `proxy=mock`, 15 running releases, 8 active releases, no pending jobs, no diverged routes, no drain/global stop. CPU admission is the immediate blocker, not image building, RAM or disk.

### Investigation required in Runtime

The release inventory has 8 `active`, 7 `superseded`, 3 `retired`, and 1 `failed` records. Each requests 500 CPU millicores. **8 active + 7 superseded exactly explains both 15 running and 7500 allocated.** This strongly suggests status-based accounting or retained historical containers; this is an inference from the API, not a source-code-confirmed formula.

Four superseded non-seed releases still return successful mock container stats. More significantly, shared fixture `prj_sandbox_tinyshop` is suspended, its sole release `rel_sandbox_ts_001` is superseded with **no container ID**, its route is suspended with no upstream, and its stats endpoint returns 404 saying the release has no container. Nevertheless, including this record in the superseded tally is necessary to reproduce the reported running count. Check accounting for stopped/suspended/no-container releases and the lifecycle transition that should retire superseded containers after a bounded rollback grace period.

Superseded inventory, all 500 CPU each:

| Project | Release | Ownership / observation |
| --- | --- | --- |
| `prj_codex_static_1b7861d48b85c9e4` | `rel_06gbrj4h9j3pmarm37gpx3be90` | This Deployer task's old test; stats 200 |
| `prj_codex_node_http_1b7861d48b85c9e4` | `rel_06gbrj56qkmges3d5y0r2t3zmc` | This Deployer task's old test; stats 200 |
| `prj_6b7c8184c11604300b549593ea27c4f9` | `rel_06gbrpx9y90t4dxshycyew2ee0` | Another agent's project; preserve; stats 200 |
| `prj_6b7c8184c11604300b549593ea27c4f9` | `rel_06gbrq14mdh2c0f4cgtt4rscqc` | Another agent's project; preserve; stats 200 |
| `prj_sandbox_acme` | `rel_sandbox_acme_001` | Shared seed; preserve; stats 503 |
| `prj_sandbox_acme` | `rel_sandbox_acme_002` | Shared seed; preserve; stats 503 |
| `prj_sandbox_tinyshop` | `rel_sandbox_ts_001` | Shared suspended seed; preserve; no container; stats 404 |

Prefer correcting lifecycle/accounting while retaining immutable history and rollback artifacts. If retaining all historical running containers is intentional, the sandbox needs a higher **sandbox-only** capacity limit and a documented retention policy. An increase from 8000 to at least 8500 CPU millicores admits this single 500-millicore candidate with the existing reserve, but gives no subsequent deployment headroom. Budget additional candidate capacity for rollback and QA, with concurrency accounted for. Increasing the limit alone postpones exhaustion if historical running allocations grow unbounded.

The published endpoint catalogue has no capacity-update or per-release retire operation. Project suspension promises to stop containers and retain data, but Tinyshop's observed accounting means it cannot be assumed to free admission capacity. Do not use shared reset, global stop, drain, broad reconcile or project deletion as a workaround. Do not restart this in-memory sandbox unless its state can be preserved.

### Exact cleanup candidates, if a targeted cleanup is chosen

No cleanup was executed. The only disposable resources positively attributed to this Deployer task are the following. This is a proposal for targeted container retirement after validating ownership and current routes, **not** permission to delete shared fixtures or another agent's projects.

Minimal proposal: stop/retire only the two superseded releases in the first two rows above, preserving their records/artifacts. Their declared combined allocation is 1000 CPU millicores. Neither is the current routed release. Confirm that the actual node allocation decreases and leave current active releases serving.

Whole-test-project retirement is a larger alternative, not needed for the minimal proposal:

| Project | All release IDs | Current route |
| --- | --- | --- |
| `prj_codex_static_1b7861d48b85c9e4` | old `rel_06gbrj4h9j3pmarm37gpx3be90`; failed/retired `rel_06gbrj4qfkbhz9zdw7a8gp0d5c`; active `rel_06gbrj4wnqqkfkqv5z6p2ntcc0` | `rte_06gbrj4n6xmgwqdtkqrqynf6zw`, revision 19 |
| `prj_codex_node_http_1b7861d48b85c9e4` | old `rel_06gbrj56qkmges3d5y0r2t3zmc`; failed/retired `rel_06gbrj5d3a22eb8ptxcx3v8e6r`; active `rel_06gbrj5jrxps785a4sfg491w24` | `rte_06gbrj5anbynhzh8s836ny5s1c`, revision 21 |

`prj_codex_probe_38ed10711f47` is also ours but has zero releases, so removing it cannot solve capacity. The failed candidates above are already retired and should not be counted as reclaimable running capacity.

### Resume and acceptance checks

1. Restore sandbox admission headroom without losing state. Confirm `/node` accounting and that all protected project pointers/routes are unchanged.
2. Leave the existing Deployer worker running. It retries automatically. Do not click a fresh deploy, rebuild the image, change its CPU request, or create another job.
3. Observe the same Deployer ID progress to `online/active`; query Runtime project history and require exactly one deployment with external ID `dep_ecf8156baba32e8bdb8c89cb2b0afbd6`. Its idempotency key must be the same ID.
4. Require its original artifact digest `sha256:a8d1f9bb91443da3b3986c7af2c491d3bbc1cae7f9317f6748e6a24d006194da`, commit `6c7a360ddb4a0d75be06044bf8a914f260ff10c7`, and unchanged build ID. The image is already in `127.0.0.1:5500/molo/prj_ec24c947789e0d0f3e341f6ddbce2443` on the local Docker host.
5. Verify health-check → activating → active events, matching active project/release, and matching desired/applied route revision. Read both Deployer `build` and `runtime` log sources and their cursor replay. The pending operation currently has 69 build lines and zero runtime lines, as expected before admission.
6. With explicit capacity budget, finish fresh static/Node portal-path verification, failed-candidate preservation and rollback. Preserve other agents' projects throughout.

### Verification scope — do not conflate these results

**Existing remote sandbox lifecycle:** read-only checks during this investigation reconfirmed the two dedicated static/Node test projects' active release pointers, health/activation event histories, route convergence at revisions 19/21, and deployment + release log endpoints (one line each). Both retained failed candidates have `HEALTH_CHECK_FAILED` and reference their original healthy predecessor; restoration deployments use the same historical digest. The earlier live test observed preservation at failure time and idempotent restoration. This investigation did not replay these mutations while capacity was full.

**Current portal operation:** still blocked at admission. Image/build identity remains unchanged and there is no accepted Runtime duplicate. Actual resume and the requested fresh full workflow remain unverified until Runtime headroom is restored. A local regression test additionally proves repeated capacity failures, restart recovery, no rebuild, stable IDs, exactly one accepted simulated operation, and eventual verified activation.

**Actual container hosting:** previous independent local Docker tests passed static/Node build, authenticated registry push/pull, container isolation and HTTP health checks. This remote sandbox uses mock driver/proxy, mock artifacts/logs and `.apps.sandbox.example` hostnames. It does not prove remote image pull or public hosting. In particular, the pending image's loopback registry is not reachable as the same registry from a remote production host. No production request or production deployment was made.
