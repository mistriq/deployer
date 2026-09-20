# Runtime smoke-test projects

These projects exercise the generated build recipes without package downloads.
Commit these files to the public HTTPS Git repository used by the controller;
the builder reads committed content only. Do not pass a working-tree path in a
production API request.

Use the matching `build-spec.json` as the service build configuration. Its
`context_dir` assumes this repository's root. If copying an example into a new
repository's root, change `context_dir` to `.`. Choose a registry repository you
can push to and configure separate runtime pull credentials.

| Example | Container port | Health path | Behavior |
| --- | --- | --- | --- |
| `runtime-static` | 8080 | `/healthz` | Static HTML served by unprivileged nginx |
| `runtime-node` | 3000 | `/healthz` | Dependency-free Node HTTP server |

Both support UID/GID 10001, a read-only root filesystem, and writable `/tmp`.
Set the runtime service's port to match this table; `build_spec.port` alone does
not configure runtime routing. The examples explicitly target `linux/amd64`;
change to `linux/arm64` when the target runtime machine uses ARM. Cross-building
requires a Buildx builder with the appropriate emulation or native worker.

The static example overrides npm install/build defaults with a file copy.
The Node example overrides those defaults with a syntax check and starts Node
directly with `exec`, allowing runtime stop signals to reach the process.

Current build limitations: only public HTTPS Git repositories are supported;
Git credential helpers, SSH credentials, submodule initialization, and Git LFS
object download are not configured. Source build commands run inside BuildKit.
Explicit build arguments are public build inputs, never a channel for secrets.
Base-image tags are currently mutable, so rebuilding the same source commit can
produce a new digest. Deployments always consume the returned digest. Buildx
must be available through system plugin paths or the trusted plugin directory
configuration described below. The Docker daemon must be available on its default socket.

## Real Docker integration test

With Docker running and `/usr/sbin/htpasswd` installed:

```sh
OCI_DOCKER_INTEGRATION=1 go test -v ./internal/ocibuild -run TestDockerRegistryIntegration -count=1 -timeout=10m
```

The test creates a temporary authenticated registry bound to a random loopback
port inside the Docker host (the Linux VM on Docker Desktop). It commits fresh
copies of both examples, builds and pushes them for `linux/amd64`, pulls each
returned immutable digest, and starts each application with the runtime's
read-only/nonroot/resource restrictions. Docker inspect confirms those settings;
`/healthz` must return 200 and an unknown path must return 404. Registry access
without authentication must return 401.

Push and pull use different disposable identities and isolated Docker configs.
The fixture's standard registry htpasswd authentication gives both identities
read/write access; this verifies credential separation, not pull-only ACLs.
Production registries must enforce pull-only permissions for the runtime identity.
Temporary containers, their anonymous registry volume, credentials, and fixture
image references are cleaned. Shared downloaded base images and BuildKit cache
are retained; no existing containers or Docker daemon settings are changed.

On macOS the builder discovers Docker Desktop's standard trusted CLI plugin
folder for Buildx without importing the user's Docker auth/config. Other custom
installations can supply `Builder.CLIPluginDirs`; Linux installations should make
Buildx available system-wide. All fixture Docker commands also use isolated
configs, avoiding user credential helpers or keychain prompts.
