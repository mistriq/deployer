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
must be installed system-wide because the builder intentionally isolates HOME
and DOCKER_CONFIG, and the Docker daemon must be available on its default socket.
