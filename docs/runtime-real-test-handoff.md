# Real Runtime test — 20 September 2026, 12:33 UTC

User explicitly authorized testing the real node without the sandbox header. Confirmed `node_id=runtime-01`, `driver=docker`, `proxy=file`, `store=file`. This test is separate from all previous mock sandbox results.

## Result: container works, public activation fails

- Project: `prj_codex_real_1a0bece7651`
- Deployment: `dep_06gbxkmx8bb6c1cc3twxgrd9p0`
- External ID / idempotency key: `dep_prj_codex_real_1a0bece7651`
- Public hostname: `prj-codex-real-1a0bece7651.apps.socialscentury.com`
- Image: `docker.io/nginxinc/nginx-unprivileged`
- Pinned linux/amd64 digest: `sha256:cc92c08186c41fba80d9ca5fdffe09458bbc4bc94e8025586add6806ba768719`
- Manifest: static, port 8080, UID/GID 10001, read-only root, /tmp tmpfs 64 MB, CPU 500m, RAM 256 MB, PID limit 128; health GET `/` expecting 200–299.

The real server successfully pulled the image, started actual containers, produced nginx runtime logs and passed candidate health after one probe on each attempt. Activation failed three times. Runtime performed three candidate attempts under one accepted deployment, not three submitted deployments.

Terminal error:

```text
ROUTE_APPLY_FAILED: proxy configuration for revision 5 failed validation; revision 4 restored
```

The final release is `rel_06gbxkqcxawt3v0kwpsf49zwmm`; earlier failed candidates are `rel_06gbxknb67dan5wk3gqjahh24r` and `rel_06gbxkp6dvsxhzqkqdtmgqrk48`. All have failed/retired metadata. After the test the node reports zero allocated CPU, zero running/active releases, zero pending jobs and zero routes. The test project/history remain available for diagnosis.

Independent public check: DNS resolves the hostname to `217.154.71.237`, but standard HTTPS verification fails with `ERR_TLS_CERT_ALTNAME_INVALID`. TLS verification was not disabled. This is a separate observed problem; the API does not establish that certificate mismatch caused proxy configuration validation to fail.

## Runtime maintainer next steps

Inspect the actual proxy validation command and its stderr for the above operation around 12:33:32–12:33:50 UTC. Check generated file content/syntax, configuration include paths, access permissions, validator/reload execution environment and availability of the configured proxy executable. The HTTP API exposes only the generic validation failure, so the exact cause requires node-side diagnostics. Confirm why revision 5 validation failed and revision 4 was restored.

Also configure/verify the edge certificate covering `*.apps.socialscentury.com` and the matching SNI virtual host. DNS resolves, but that alone does not validate HTTPS routing. After fixing both issues, retry this dedicated test project with a new explicit deployment ID; the existing deployment is terminal and its idempotency record should not be overwritten. Prove active release + desired/applied route convergence and a normal externally verified HTTPS 200.

## Node state and boundaries

The real node initially had no projects but had `draining=true`, set by an operator on September 18. For this user-authorized isolated test, drain was disabled only around submission, then restored in a finally block immediately after HTTP 202. Final drain remains true. No existing projects were changed, no sandbox reset occurred, and no global reconcile was invoked.

This was a direct Runtime infrastructure smoke test using a public pinned artifact. It was not a portal → build → private registry → public hosting end-to-end pass. Local Docker still timed out on version/status commands, and no remotely reachable private push registry is configured in Deployer. Portal configuration and source files were left unchanged. These remain separate prerequisites after the Runtime edge failure is fixed.

[Structured evidence](runtime-real-test-evidence.json) contains the exact deployment timeline, release records, project, node state and DNS/TLS observation, with no credentials.
