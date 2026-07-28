# GOAL — Support no-build static sites

The pipeline can't deploy a plain static site (no package.json, no build step).
The analyzer marks such repos SUPPORTED, but the Deployer rejects them. Fix the
Deployer side. (Master goal is in `AGENTS.md`/`HOSTING_IMPLEMENTATION_PLAN.md` +
git history — this file is the current focused task.)

## Shared contract (hosting builds this, you must accept it)
A **no-build static** project sends a `runtime` object:
```json
{ "kind": "static", "output_directory": "<safe relative path, e.g. \".\">" }
```
i.e. `package_manager`, `build_script`, and `node_version` are ABSENT. This means:
serve `output_directory` from the repository checkout AS-IS — no npm install, no
build stage, no Node image. Existing static-with-build (`package_manager` +
`build_script` present) and node runtimes are UNCHANGED.

## Changes
1. `internal/app/hosting_projects.go` validation (~line 225-282): for
   `kind == "static"`, allow `package_manager` and `build_script` both empty →
   valid no-build static. When `package_manager` is present, keep requiring it in
   {npm,pnpm,yarn} + a `build_script`. `node_version` optional when no build.
   Still fully validate `output_directory` is a safe canonical relative path.
2. `internal/app/hosting_agent.go` recipe/Dockerfile (`writeGeneratedHostingRecipe`,
   ~line 1455-1471): for no-build static emit a Dockerfile with ONLY the nginx
   stage copying `output_directory` from source into
   `/usr/share/nginx/html` (no `FROM node ... AS build`, no install/build RUN).
   Keep the existing build-stage path when a build is defined.
3. Update `docs/openapi.yaml` for the optional fields, and any manifest digest
   logic that assumes package_manager/build_script exist.

## Rules
- No mock passes. Add tests: manifest validation accepts no-build static and
  rejects malformed variants; recipe generation produces a node-free nginx
  Dockerfile that publishes `output_directory`; output_directory injection still
  rejected.
- Full gate: build, vet, unit/integration tests. Don't touch hosting or the proxy
  adapter.

STOP and report: exact wire shape you accept for no-build static, files changed,
and the test names proving validation + recipe both work.
