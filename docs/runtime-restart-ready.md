# Coordinated sandbox restart preparation — 2026-09-20

Prepared at 10:30 UTC. No remote restart/reset, project mutation or portal file edit was performed.

## Completed

- No local `runtime-deployer` process, portal listener on 4173, worker listener on 52672, or deployment test process was running. The portal task's recorded test run is completed. No process needed killing; do not start the portal until project recovery finishes, because startup automatically resumes pending workers.
- Private backup directory: `deployer/runtime-state/recovery-2026-09-20T10-30-45-327Z` (0700, ignored by Git). Its 14 captured files have SHA-256 checksums. Files are 0600; never send this directory to the Runtime maintainer, as it includes state decryption keys and local service tokens.
- Saved sandbox inventory: 11 projects, 19 releases, deployment histories, routes and env metadata. This preserves evidence, not an importable copy of Runtime's in-memory store or original idempotency records.
- Saved both local organizations' encrypted state, decryption keys, local service token maps and connection metadata. Four Runtime projects map to recoverable local configuration; one has two env variables. Secret values were not printed or written in plaintext.
- Original operation `dep_ecf8156baba32e8bdb8c89cb2b0afbd6` remains `deploying/submitting`, without a Runtime deployment ID. Its artifact digest is still `sha256:a8d1f9bb91443da3b3986c7af2c491d3bbc1cae7f9317f6748e6a24d006194da`. Its immutable snapshot has zero env variables, so recovery must explicitly PUT an empty env map.
- Runtime manifest matches the local job manifest after normalizing omitted `env_names` to an empty array.
- Current remote sandbox is still the old generation, total CPU 8000, allocated 7500, reserve 500, pending jobs zero. No local tests are using it. This does not establish whether another external client is running tests; the Runtime maintainer coordinates that side of the window.
- Local registry port 5500 is not listening, and Docker image inspection did not respond. The encrypted job retains the built artifact reference; current registry/image availability must be rechecked when local Docker starts. The mock sandbox does not prove a registry pull.

## Ready-to-forward message

> Z naší strany je příprava hotová. Portál i worker jsou vypnuté, lokální testy neběží. Máme uložený inventář sandboxu a šifrovaný stav obou organizací včetně podkladů pro env. Původní deployment i digest jsou zachované. Jakmile ověříš, že sandbox nepoužívá další externí test, můžeš provést domluvený restart s opraveným image. Potom nám potvrď dokončení; nejprve obnovíme projekt a env a teprve pak spustíme worker. Historické Runtime/idempotency záznamy restart nepřežijí, jejich inventář máme uložený pro porovnání.

## Recovery commands after the maintainer confirms restart

Run from the `deployer` directory:

```sh
node scripts/sandbox-recover.mjs check './runtime-state/recovery-2026-09-20T10-30-45-327Z'
node scripts/sandbox-recover.mjs restore './runtime-state/recovery-2026-09-20T10-30-45-327Z'
```

The script verifies the unchanged original job, artifact, tenant/project mapping, saved manifest and stopped local processes. All Runtime requests use the exact HTTPS host and `X-Socen-Sandbox: true`; redirects are refused. Writes are allowlisted to POST the original project and PUT its env. Restore refuses to run before the sandbox generation changes, before CPU capacity reaches 16000, or without candidate headroom. It refuses conflicts or any existing deployment history, and it never submits a deployment or starts a worker. A successful restoration saves a private receipt.

Validation performed: `node --check`, live read-only `check` succeeded, and `restore` against the old generation refused before any mutation as expected. Successful post-restart restoration is still pending.

After successful restoration, start the existing managed portal/worker with sandbox configuration. Verify one accepted Runtime deployment for the original external ID and digest, matching project/release/route activation, and both build/runtime log sources. Then finish the static/Node failure/rollback QA with the new capacity budget. Preserve the backup and explicitly distinguish fresh Runtime IDs from pre-reset historical references in the portal's older QA results. Do not silently rewrite portal history or redeploy other agents' projects.

## Maintainer clarification

Runtime maintainer confirmed status-only capacity accounting was incorrect for suspended/no-container releases. Two superseded releases per project intentionally remain running for immediate rollback; their resource usage is by design. The prepared Runtime fix shares allocation rules between admission and heartbeat and raises sandbox CPU capacity to 16000. These changes are reported by the maintainer, not yet observed live. The production file store and token survival were also confirmed by the maintainer; this task did not read or modify production.
