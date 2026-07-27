package app

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHostingBuildNetworkModeIsolatesNoBuildStaticRecipes(t *testing.T) {
	configured := "hosting-build-egress"
	noBuild := hostingJobRecipe{Runtime: HostingRuntimeManifest{
		Kind: "static", OutputDirectory: ".",
	}}
	if got := hostingBuildNetworkMode(noBuild, configured); got != "none" {
		t.Fatalf("no-build static network=%q", got)
	}
	withBuild := noBuild
	withBuild.Runtime.PackageManager = "npm"
	withBuild.Runtime.NodeVersion = "22"
	withBuild.Runtime.BuildScript = "build"
	if got := hostingBuildNetworkMode(withBuild, configured); got != configured {
		t.Fatalf("build-required network=%q", got)
	}
}

func writeHostingTestTar(t *testing.T, headers ...*tar.Header) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.tar")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	writer := tar.NewWriter(file)
	for _, header := range headers {
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if header.Typeflag == tar.TypeReg && header.Size > 0 {
			if _, err := io.CopyN(writer, bytes.NewReader(bytes.Repeat([]byte("x"), int(header.Size))), header.Size); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestExtractHostingSourceRejectsUnsafeEntries(t *testing.T) {
	tests := []struct {
		name   string
		header *tar.Header
	}{
		{name: "parent traversal", header: &tar.Header{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0600}},
		{name: "absolute", header: &tar.Header{Name: "/escape", Typeflag: tar.TypeReg, Mode: 0600}},
		{name: "symlink", header: &tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd", Mode: 0777}},
		{name: "hardlink", header: &tar.Header{Name: "link", Typeflag: tar.TypeLink, Linkname: "target", Mode: 0777}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			archive := writeHostingTestTar(t, test.header)
			destination := t.TempDir()
			if err := extractHostingSource(archive, destination); err == nil {
				t.Fatal("unsafe archive entry was accepted")
			}
		})
	}
}

func TestExtractHostingSourceWritesOnlyRegularFiles(t *testing.T) {
	archive := writeHostingTestTar(t,
		&tar.Header{Name: "src", Typeflag: tar.TypeDir, Mode: 0755},
		&tar.Header{Name: "src/index.js", Typeflag: tar.TypeReg, Mode: 0755, Size: 4},
	)
	destination := t.TempDir()
	if err := extractHostingSource(archive, destination); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(destination, "src", "index.js"))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "xxxx" {
		t.Fatalf("content = %q", content)
	}
	info, err := os.Stat(filepath.Join(destination, "src", "index.js"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0644 {
		t.Fatalf("file mode = %o", info.Mode().Perm())
	}
}

func TestDownloadHostingSourceRejectsDeclaredOversize(t *testing.T) {
	oldClient := agentArtifactClient
	t.Cleanup(func() { agentArtifactClient = oldClient })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "8589934593")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	agentArtifactClient = server.Client()

	destination := filepath.Join(t.TempDir(), "source.tar")
	err := downloadHostingSource(context.Background(), hostingAgentConfig{ServerURL: server.URL, Token: "htr_test"}, &hostingClaimedJob{
		JobID: 1, LeaseGeneration: 1, LeaseToken: "lease", Recipe: hostingJobRecipe{SourceArtifactURL: "/source"},
	}, destination)
	if err == nil || !strings.Contains(err.Error(), "size limit") {
		t.Fatalf("error = %v", err)
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("destination should not exist, stat error = %v", err)
	}
}

func TestGeneratedHostingRecipesIgnoreCustomerDockerfile(t *testing.T) {
	tests := []HostingRuntimeManifest{
		{Kind: "static", NodeVersion: "22", PackageManager: "npm", BuildScript: "build", OutputDirectory: "dist"},
		{Kind: "node", NodeVersion: "20", PackageManager: "pnpm", BuildScript: "compile", StartScript: "start", Port: 3000, HealthPath: "/healthz"},
	}
	for _, runtime := range tests {
		t.Run(runtime.Kind, func(t *testing.T) {
			directory := t.TempDir()
			customerDockerfile := []byte("FROM malicious\n")
			if err := os.WriteFile(filepath.Join(directory, "Dockerfile"), customerDockerfile, 0600); err != nil {
				t.Fatal(err)
			}
			if err := writeGeneratedHostingRecipe(directory, hostingJobRecipe{Runtime: runtime}); err != nil {
				t.Fatal(err)
			}
			generated, err := os.ReadFile(filepath.Join(directory, ".deployer", "Dockerfile"))
			if err != nil {
				t.Fatal(err)
			}
			text := string(generated)
			for _, forbidden := range []string{"--privileged", "docker.sock", "--cap-add", "FROM malicious"} {
				if strings.Contains(text, forbidden) {
					t.Fatalf("generated recipe contains %q: %s", forbidden, text)
				}
			}
			if runtime.Kind == "node" && !strings.Contains(text, "COPY --chown=node:node . .\n") {
				t.Fatalf("node recipe does not preserve unprivileged runtime access: %s", text)
			}
			unchanged, err := os.ReadFile(filepath.Join(directory, "Dockerfile"))
			if err != nil || !bytes.Equal(unchanged, customerDockerfile) {
				t.Fatalf("customer Dockerfile was changed: %q, %v", unchanged, err)
			}
		})
	}
}

func TestGeneratedNoBuildStaticRecipeUsesOnlyNginx(t *testing.T) {
	for _, test := range []struct {
		name       string
		output     string
		copySource string
	}{
		{name: "repository root", output: ".", copySource: "."},
		{name: "nested output", output: "public/assets", copySource: "./public/assets"},
		{name: "option-like output", output: "--from/customer-site", copySource: "./--from/customer-site"},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			runtime := HostingRuntimeManifest{Kind: "static", OutputDirectory: test.output}
			if err := writeGeneratedHostingRecipe(directory, hostingJobRecipe{Runtime: runtime}); err != nil {
				t.Fatal(err)
			}
			generated, err := os.ReadFile(filepath.Join(directory, ".deployer", "Dockerfile"))
			if err != nil {
				t.Fatal(err)
			}
			text := string(generated)
			if strings.Count(text, "FROM ") != 1 || !strings.Contains(text, "FROM nginxinc/nginx-unprivileged:1.27-alpine") {
				t.Fatalf("no-build recipe is not a single nginx stage: %s", text)
			}
			for _, forbidden := range []string{"node:", " AS build", "RUN ", "npm", "pnpm", "yarn"} {
				if strings.Contains(text, forbidden) {
					t.Fatalf("no-build recipe contains %q: %s", forbidden, text)
				}
			}
			copyJSON, _ := json.Marshal([]string{test.copySource, "/usr/share/nginx/html/"})
			if !strings.Contains(text, "COPY --chown=101:101 "+string(copyJSON)+"\n") {
				t.Fatalf("no-build recipe does not publish %q: %s", test.output, text)
			}
			ignore, err := os.ReadFile(filepath.Join(directory, ".deployer", "Dockerfile.dockerignore"))
			if err != nil || string(ignore) != ".deployer\n" {
				t.Fatalf("platform recipe files are not excluded from static output: %q err=%v", ignore, err)
			}
			if _, err := os.Stat(filepath.Join(directory, ".deployer", "nginx.conf")); !os.IsNotExist(err) {
				t.Fatalf("no-build recipe unexpectedly generated nginx config: %v", err)
			}
		})
	}
}

func TestGeneratedBuiltStaticRecipeKeepsNodeBuildStage(t *testing.T) {
	directory := t.TempDir()
	runtime := HostingRuntimeManifest{Kind: "static", NodeVersion: "22", PackageManager: "npm", BuildScript: "build", OutputDirectory: "dist"}
	if err := writeGeneratedHostingRecipe(directory, hostingJobRecipe{Runtime: runtime}); err != nil {
		t.Fatal(err)
	}
	generated, err := os.ReadFile(filepath.Join(directory, ".deployer", "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(generated)
	for _, required := range []string{"FROM node:22-bookworm-slim AS build", "RUN npm ci --ignore-scripts", "RUN npm run build", "COPY --chown=101:101 .deployer/nginx.conf /etc/nginx/conf.d/default.conf", "COPY --chown=101:101 --from=build /app/dist /usr/share/nginx/html"} {
		if !strings.Contains(text, required) {
			t.Fatalf("built static recipe is missing %q: %s", required, text)
		}
	}
}

func TestHostingWorkloadLimitsAreFailClosedAtExecutionBoundary(t *testing.T) {
	valid, err := hostingLimitsForProfile("starter")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateHostingWorkloadLimits(valid); err != nil {
		t.Fatalf("profile limits rejected: %v", err)
	}
	for name, limits := range map[string]hostingWorkloadLimits{
		"zero cpu":      valid,
		"negative ram":  valid,
		"oversized ram": valid,
	} {
		switch name {
		case "zero cpu":
			limits.CPUMillis = 0
		case "negative ram":
			limits.RAMBytes = -1
		case "oversized ram":
			limits.RAMBytes = 1 << 41
		}
		if err := validateHostingWorkloadLimits(limits); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}

func TestHostingRuntimeContainerPolicyDeniesHostExposureAndPrivileges(t *testing.T) {
	secretDir := "/run/user/1000/secrets"
	valid := []string{"run", "-d", "--cap-drop=ALL", "-p", "127.0.0.1::3000", "image"}
	if err := validateHostingRuntimeContainerArgs(valid, "127.0.0.1", ""); err != nil {
		t.Fatalf("valid private runtime rejected: %v", err)
	}
	withSecret := append([]string{}, valid[:len(valid)-1]...)
	withSecret = append(withSecret, "--mount", "type=bind,source="+secretDir+",target=/run/secrets/deployer,readonly,bind-propagation=rprivate", "image")
	if err := validateHostingRuntimeContainerArgs(withSecret, "127.0.0.1", secretDir); err != nil {
		t.Fatalf("controlled secret mount rejected: %v", err)
	}
	for name, args := range map[string][]string{
		"public port":   {"run", "-p", "0.0.0.0::3000"},
		"privileged":    {"run", "--privileged"},
		"capability":    {"run", "--cap-add=SYS_ADMIN"},
		"host volume":   {"run", "--volume", "/:/host"},
		"docker socket": {"run", "--mount", "type=bind,source=/var/run/docker.sock,target=/run/docker.sock"},
	} {
		if err := validateHostingRuntimeContainerArgs(args, "127.0.0.1", secretDir); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}

func TestGeneratedHostingRecipeRejectsOutputDirectoryInjection(t *testing.T) {
	for _, output := range []string{
		"dist\nRUN touch /owned",
		"dist # comment",
		"dist /tmp/other",
		`dist\escape`,
		"dist/../public",
		"dist//public",
	} {
		for _, mode := range []string{"no-build", "built"} {
			t.Run(mode+"_"+strings.ReplaceAll(output, "/", "_"), func(t *testing.T) {
				directory := t.TempDir()
				runtime := HostingRuntimeManifest{Kind: "static", OutputDirectory: output}
				if mode == "built" {
					runtime.NodeVersion = "22"
					runtime.PackageManager = "npm"
					runtime.BuildScript = "build"
				}
				if err := writeGeneratedHostingRecipe(directory, hostingJobRecipe{Runtime: runtime}); err == nil {
					t.Fatalf("unsafe output directory %q was accepted", output)
				}
				if _, err := os.Stat(filepath.Join(directory, ".deployer")); !os.IsNotExist(err) {
					t.Fatalf("generated recipe directory exists after rejected output %q: %v", output, err)
				}
			})
		}
	}
}

func TestParseLoopbackDockerPort(t *testing.T) {
	port, err := parseLoopbackDockerPort("127.0.0.1:49152\n")
	if err != nil || port != 49152 {
		t.Fatalf("port=%d err=%v", port, err)
	}
	for _, value := range []string{"0.0.0.0:49152", "[::]:49152", "127.0.0.1:80", "garbage"} {
		if _, err := parseLoopbackDockerPort(value); err == nil {
			t.Fatalf("public or invalid port %q was accepted", value)
		}
	}
	if port, err := parseDockerBoundPort("10.20.0.15:49153", "10.20.0.15"); err != nil || port != 49153 {
		t.Fatalf("private port=%d err=%v", port, err)
	}
	if endpoint, err := parseHostingPublishedRuntimeEndpoint("127.0.0.1:49154->3000/tcp, 3001/tcp"); err != nil || endpoint != "http://127.0.0.1:49154" {
		t.Fatalf("published endpoint=%q err=%v", endpoint, err)
	}
	for _, value := range []string{"3000/tcp", "0.0.0.0:49154->3000/tcp", "203.0.113.10:49154->3000/tcp",
		"127.0.0.1:49154->3000/tcp, 127.0.0.1:49155->3001/tcp"} {
		if _, err := parseHostingPublishedRuntimeEndpoint(value); err == nil {
			t.Fatalf("unsafe published ports %q accepted", value)
		}
	}
	for _, value := range []string{"0.0.0.0", "203.0.113.10", "::1", "hostname.internal"} {
		if err := validateHostingRuntimeBindAddress(value); err == nil {
			t.Fatalf("unsafe runtime bind address %q was accepted", value)
		}
	}
	for _, value := range []string{"127.0.0.1", "10.20.0.15", "192.168.1.20"} {
		if err := validateHostingRuntimeBindAddress(value); err != nil {
			t.Fatalf("private runtime bind address %q rejected: %v", value, err)
		}
	}
}

func TestReconcileHostingAgentReleasesRemovesOnlyUnretainedManagedContainers(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "docker.log")
	keep := "sha256:" + strings.Repeat("a", 64)
	drop := "sha256:" + strings.Repeat("b", 64)
	imageDigest := "sha256:" + strings.Repeat("c", 64)
	namespace := hostingAgentRuntimeNamespace(directory)
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> %q
if [ "$1" = "ps" ]; then
  printf 'keep-container\t%s\t%s\tproject_01JKEEPX\tdeployment_01JKEEPX\tbuild-1-1\trunning\t127.0.0.1:49151->3000/tcp\ndrop-container\t%s\t%s\tproject_01JDROPX\tdeployment_01JDROPX\tbuild-2-1\trunning\t127.0.0.1:49152->3000/tcp\nstale-container\t%s\t%s\tproject_01JKEEPX\tdeployment_01JKEEPX\tbuild-1-1\texited\t127.0.0.1:49153->3000/tcp\n'
elif [ "$1" = "inspect" ]; then
  printf '%s\n'
fi
`, logPath, namespace, keep, namespace, drop, namespace, drop, imageDigest)
	dockerPath := filepath.Join(directory, "docker")
	if err := os.WriteFile(dockerPath, []byte(script), 0750); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	config := hostingAgentConfig{WorkRoot: directory}
	if err := reconcileHostingAgentReleases(context.Background(), config, []hostingRetainedRelease{{ReleaseDigest: keep, ExternalProjectID: "project_01JKEEPX", ExternalDeploymentID: "deployment_01JKEEPX", RuntimeInstanceID: "build-1-1", Status: "active"}}); err != nil {
		t.Fatal(err)
	}
	commands, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(commands)
	if strings.Contains(text, "rm -f keep-container") || !strings.Contains(text, "rm -f drop-container") ||
		!strings.Contains(text, "rm -f stale-container") || !strings.Contains(text, "image rm "+imageDigest) {
		t.Fatalf("docker commands = %s", text)
	}
	if err := reconcileHostingAgentReleases(context.Background(), config, []hostingRetainedRelease{{ReleaseDigest: "invalid", ExternalProjectID: "project_01JKEEPX", ExternalDeploymentID: "deployment_01JKEEPX"}}); err == nil {
		t.Fatal("invalid retained digest was accepted")
	}
}

func TestReconcileHostingAgentReleasesScopesDockerInventoryToAgentNamespace(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "docker.log")
	digest := "sha256:" + strings.Repeat("a", 64)
	workRoot := t.TempDir()
	namespace := hostingAgentRuntimeNamespace(workRoot)
	otherNamespace := strings.Repeat("f", 32)
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> %q
if [ "$1" = "ps" ]; then
  printf 'owned-container\t%s\t%s\tproject_01JOWNEDX\tdeployment_01JOWNEDX\tbuild-9-1\trunning\t127.0.0.1:49154->3000/tcp\nother-agent-container\t%s\t%s\tproject_01JOTHERX\tdeployment_01JOTHERX\tbuild-10-1\trunning\t127.0.0.1:49155->3000/tcp\n'
elif [ "$1" = "inspect" ]; then
  printf '%s\n'
fi
`, logPath, namespace, digest, otherNamespace, digest, digest)
	dockerPath := filepath.Join(directory, "docker")
	if err := os.WriteFile(dockerPath, []byte(script), 0750); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	retained := []hostingRetainedRelease{{ReleaseDigest: digest, ExternalProjectID: "project_01JOWNEDX",
		ExternalDeploymentID: "deployment_01JOWNEDX", RuntimeInstanceID: "build-9-1", Status: "active"}}
	if err := reconcileHostingAgentReleases(t.Context(), hostingAgentConfig{WorkRoot: workRoot}, retained); err != nil {
		t.Fatal(err)
	}
	commands, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(commands)
	if strings.Contains(text, "rm -f other-agent-container") || strings.Contains(text, "rm -f owned-container") {
		t.Fatalf("namespace-scoped cleanup commands = %s", text)
	}
}

func TestRemoveStaleHostingContainerRefusesCrossNamespaceNameCollision(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "docker.log")
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> %q
if [ "$1" = "inspect" ]; then
  printf 'true\tother-namespace\tsha256:%s\tproject_01JCOLLIDE\tdeployment_01JCOLLIDE\tbuild-3-1\n'
fi
`, logPath, strings.Repeat("a", 64))
	if err := os.WriteFile(filepath.Join(directory, "docker"), []byte(script), 0750); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	err := removeStaleHostingContainer(t.Context(), "colliding-name", "owned-namespace",
		"sha256:"+strings.Repeat("a", 64), "project_01JCOLLIDE", "deployment_01JCOLLIDE", "build-3-1")
	if err == nil || !strings.Contains(err.Error(), "mismatched ownership labels") {
		t.Fatalf("cross-namespace collision err=%v", err)
	}
	commands, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if strings.Contains(string(commands), "rm -f") {
		t.Fatalf("cross-namespace container was removed: %s", commands)
	}
}

func TestSupersededHostingAgentSessionRetainsThenCleansRuntimesAfterHandoff(t *testing.T) {
	workRoot := t.TempDir()
	binDirectory := t.TempDir()
	logPath := filepath.Join(binDirectory, "docker.log")
	namespace := hostingAgentRuntimeNamespace(workRoot)
	digest := "sha256:" + strings.Repeat("e", 64)
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> %q
if [ "$1" = "ps" ]; then
  printf 'owned-container\t%s\t%s\tproject_01JSUPERX\tdeployment_01JSUPERX\tbuild-4-1\trunning\t127.0.0.1:49156->3000/tcp\n'
elif [ "$1" = "inspect" ]; then
  printf '%s\n'
fi
`, logPath, namespace, digest, digest)
	if err := os.WriteFile(filepath.Join(binDirectory, "docker"), []byte(script), 0750); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDirectory+string(os.PathListSeparator)+os.Getenv("PATH"))
	var retainServing atomic.Bool
	retainServing.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/hosting-agent/v1/heartbeat" {
			http.NotFound(w, r)
			return
		}
		response := apiErrorResponse{Error: "another session is active", Code: errCodeRunnerSessionSuperseded,
			CleanupAuthorized: true, RetainedReleases: make([]hostingRetainedRelease, 0)}
		if retainServing.Load() {
			response.RetainedReleases = append(response.RetainedReleases, hostingRetainedRelease{
				ReleaseDigest: digest, ExternalProjectID: "project_01JSUPERX",
				ExternalDeploymentID: "deployment_01JSUPERX", RuntimeInstanceID: "build-4-1", Status: "active",
			})
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	config := hostingAgentConfig{ServerURL: server.URL, Token: "test-token", WorkRoot: workRoot,
		SessionID: strings.Repeat("f", 48), HeartbeatSequence: new(atomic.Int64), SessionAccepted: new(atomic.Bool)}
	if err := publishHostingAgentHeartbeat(t.Context(), config, true); errors.Is(err, errHostingAgentSessionSuperseded) {
		t.Fatalf("unaccepted replacement session claimed namespace ownership: %v", err)
	}
	beforeAcceptance, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if strings.Contains(string(beforeAcceptance), "rm -f owned-container") {
		t.Fatalf("unaccepted replacement session cleaned inherited runtime: %s", beforeAcceptance)
	}
	config.SessionAccepted.Store(true)
	err := publishHostingAgentHeartbeat(t.Context(), config, true)
	if !errors.Is(err, errHostingAgentSessionDraining) {
		t.Fatalf("superseded heartbeat err=%v", err)
	}
	commands, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if strings.Contains(string(commands), "rm -f owned-container") {
		t.Fatalf("superseded session deleted a potentially routed runtime: %s", commands)
	}
	retainServing.Store(false)
	err = publishHostingAgentHeartbeat(t.Context(), config, true)
	if !errors.Is(err, errHostingAgentSessionSuperseded) {
		t.Fatalf("completed superseded cleanup err=%v", err)
	}
	commands, readErr = os.ReadFile(logPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(commands), "rm -f owned-container") {
		t.Fatalf("superseded session did not clean handed-off runtime: %s", commands)
	}
}

func TestRestartedSupersededAgentUsesDurableSessionForCleanup(t *testing.T) {
	workRoot := t.TempDir()
	binDirectory := t.TempDir()
	logPath := filepath.Join(binDirectory, "docker.log")
	namespace := hostingAgentRuntimeNamespace(workRoot)
	digest := "sha256:" + strings.Repeat("6", 64)
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> %q
if [ "$1" = "ps" ]; then
  printf 'old-container\t%s\t%s\tproject_01JOLDSESS\tdeployment_01JOLDSESS\tbuild-7-1\trunning\t127.0.0.1:49157->3000/tcp\n'
elif [ "$1" = "inspect" ]; then
  printf '%s\n'
fi
`, logPath, namespace, digest, digest)
	if err := os.WriteFile(filepath.Join(binDirectory, "docker"), []byte(script), 0750); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDirectory+string(os.PathListSeparator)+os.Getenv("PATH"))
	oldSession := strings.Repeat("a", 48)
	if err := persistHostingAcceptedSession(workRoot, oldSession); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/hosting-agent/v1/heartbeat":
			jsonErrorCode(w, errCodeRunnerSessionSuperseded, "another session is active", http.StatusConflict)
		case "/api/hosting-agent/v1/session-retention":
			var request hostingSessionRetentionRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.SessionID != oldSession {
				t.Errorf("cleanup session request=%+v err=%v", request, err)
			}
			jsonResponse(w, hostingHeartbeatResponse{RetainedReleases: []hostingRetainedRelease{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	config := hostingAgentConfig{ServerURL: server.URL, Token: "test-token", WorkRoot: workRoot,
		SessionID: strings.Repeat("b", 48), PriorAcceptedSessionIDs: []string{oldSession},
		HeartbeatSequence: new(atomic.Int64), SessionAccepted: new(atomic.Bool)}
	err := publishHostingAgentHeartbeat(t.Context(), config, true)
	if !errors.Is(err, errHostingAgentSessionSuperseded) {
		t.Fatalf("restarted cleanup err=%v", err)
	}
	commands, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(commands), "rm -f old-container") {
		t.Fatalf("restarted prior owner did not clean old runtime: %s", commands)
	}
	if _, err := os.Stat(hostingAcceptedSessionPath(workRoot)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("accepted session marker survived completed cleanup: %v", err)
	}
}

func TestRestartedSupersededWorkRootCanTakeOverFailedReplacement(t *testing.T) {
	withTempDB(t)
	withFakeProxy(t)
	project, _, runnerID, job := createAndClaimHostingJob(t, "project_01JTAKEOLD", "deployment_01JTAKEOLD")
	digest := "sha256:" + strings.Repeat("5", 64)
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken,
		hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, digest), RuntimeEndpoint: healthyHostingEndpointForTest(t),
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	oldSession := strings.Repeat("a", 48)
	replacementSession := strings.Repeat("b", 48)
	freshSession := strings.Repeat("c", 48)
	now := time.Now().UTC()
	if _, err := db.Exec(`UPDATE hosting_runners SET active_session_id=?, last_heartbeat_sequence=4,
		status='online', last_seen=? WHERE id=?`, replacementSession,
		formatSQLiteTime(now.Add(-hostingRunnerSessionTakeoverAfter-time.Second)), runnerID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO hosting_runner_superseded_sessions
		(hosting_runner_id, session_id, superseded_at) VALUES (?, ?, ?)`, runnerID, oldSession,
		formatSQLiteTime(now.Add(-time.Minute))); err != nil {
		t.Fatal(err)
	}
	workRoot := t.TempDir()
	binDirectory := t.TempDir()
	logPath := filepath.Join(binDirectory, "docker.log")
	namespace := hostingAgentRuntimeNamespace(workRoot)
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> %q
if [ "$1" = "ps" ]; then
  printf 'retained-container\t%s\t%s\t%s\tdeployment_01JTAKEOLD\tbuild-%d-%d\trunning\t127.0.0.1:49158->3000/tcp\n'
elif [ "$1" = "inspect" ]; then
  printf '%s\n'
fi
`, logPath, namespace, digest, project.ExternalProjectID, job.JobID, job.LeaseGeneration, digest)
	if err := os.WriteFile(filepath.Join(binDirectory, "docker"), []byte(script), 0750); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDirectory+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := persistHostingAcceptedSessions(workRoot, []string{oldSession, replacementSession}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), hostingRunnerContextKey{}, &HostingRunner{ID: runnerID}))
		switch r.URL.Path {
		case "/api/hosting-agent/v1/session-retention":
			handleHostingAgentSessionRetention(w, r)
		case "/api/hosting-agent/v1/heartbeat":
			handleHostingAgentHeartbeat(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	config := hostingAgentConfig{ServerURL: server.URL, Token: "test-token", WorkRoot: workRoot,
		SessionID: freshSession, PriorAcceptedSessionIDs: []string{oldSession, replacementSession},
		HeartbeatSequence: new(atomic.Int64), SessionAccepted: new(atomic.Bool)}
	if err := publishHostingAgentHeartbeat(t.Context(), config, true); err != nil {
		t.Fatalf("fresh session did not take over failed replacement: %v", err)
	}
	var activeSession string
	if err := db.QueryRow(`SELECT active_session_id FROM hosting_runners WHERE id=?`, runnerID).Scan(&activeSession); err != nil {
		t.Fatal(err)
	}
	if activeSession != freshSession || !config.SessionAccepted.Load() {
		t.Fatalf("active session=%q accepted=%v", activeSession, config.SessionAccepted.Load())
	}
	sessions, err := loadHostingAcceptedSessions(workRoot)
	if err != nil || len(sessions) != 1 || sessions[0] != freshSession {
		t.Fatalf("accepted session journal=%v err=%v", sessions, err)
	}
	var replacementTombstone int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_runner_superseded_sessions
		WHERE hosting_runner_id=? AND session_id=?`, runnerID, replacementSession).Scan(&replacementTombstone); err != nil {
		t.Fatal(err)
	}
	if replacementTombstone != 1 {
		t.Fatal("failed replacement session was not durably tombstoned")
	}
}

func TestHostingAgentDecodesMaximumSupersededRetentionResponse(t *testing.T) {
	retained := make([]hostingRetainedRelease, hostingMaxRuntimeInventoryEntries)
	for index := range retained {
		retained[index] = hostingRetainedRelease{ReleaseDigest: "sha256:" + strings.Repeat("a", 64),
			ExternalProjectID:    fmt.Sprintf("project_%0120d", index),
			ExternalDeploymentID: fmt.Sprintf("deployment_%0117d", index),
			RuntimeInstanceID:    fmt.Sprintf("build-%d-1", index+1), Status: "inactive"}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(hostingSupersededSessionError{Error: "superseded",
			Code: errCodeRunnerSessionSuperseded, CleanupAuthorized: true, RetainedReleases: retained})
	}))
	defer server.Close()
	_, err := hostingAgentJSONStatus(t.Context(), hostingAgentConfig{ServerURL: server.URL, Token: "test"},
		http.MethodPost, "", map[string]bool{"heartbeat": true}, nil, nil)
	var apiErr *hostingAgentAPIError
	if !errors.As(err, &apiErr) || !apiErr.CleanupAuthorized || len(apiErr.RetainedReleases) != len(retained) {
		t.Fatalf("maximum retention error=%T %v decoded=%d", err, err, func() int {
			if apiErr == nil {
				return 0
			}
			return len(apiErr.RetainedReleases)
		}())
	}
}

func TestHostingAgentSafelyAdoptsLegacyRuntimeBeforeCleanup(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "docker.log")
	digest := "sha256:" + strings.Repeat("d", 64)
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> %q
if [ "$1" = "ps" ]; then
  printf 'legacy-container\t\t%s\tproject_01JLEGACY\tdeployment_01JLEGACY\tbuild-11-2\trunning\t127.0.0.1:49152->3000/tcp\n'
elif [ "$1" = "inspect" ]; then
  printf '%s\n'
fi
`, logPath, digest, digest)
	if err := os.WriteFile(filepath.Join(directory, "docker"), []byte(script), 0750); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	otherConfig := hostingAgentConfig{WorkRoot: t.TempDir()}
	if err := reconcileHostingAgentReleases(t.Context(), otherConfig, nil); err != nil {
		t.Fatal(err)
	}
	commands, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(commands), "rm -f legacy-container") {
		t.Fatal("unowned legacy runtime was removed")
	}
	ownerConfig := hostingAgentConfig{WorkRoot: t.TempDir()}
	retained := []hostingRetainedRelease{{ReleaseDigest: digest, ExternalProjectID: "project_01JLEGACY",
		ExternalDeploymentID: "deployment_01JLEGACY", RuntimeInstanceID: "build-11-2", Status: "active"}}
	if err := reconcileHostingAgentReleases(t.Context(), ownerConfig, retained); err != nil {
		t.Fatal(err)
	}
	registry, err := loadHostingRuntimeAdoptions(ownerConfig.WorkRoot)
	if err != nil || len(registry) != 1 {
		t.Fatalf("legacy runtime adoption registry=%v err=%v", registry, err)
	}
	if err := reconcileHostingAgentReleases(t.Context(), ownerConfig, nil); err != nil {
		t.Fatal(err)
	}
	commands, err = os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(commands), "rm -f legacy-container") {
		t.Fatalf("adopted legacy runtime was not removed: %s", commands)
	}
}

func TestHostingAgentPublishesLegacyRuntimeOnlyAfterDurableAdoption(t *testing.T) {
	directory := t.TempDir()
	workRoot := t.TempDir()
	digest := "sha256:" + strings.Repeat("b", 64)
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "ps" ]; then
  printf 'legacy-container\t\t%s\tproject_01JLEGACY2\tdeployment_01JLEGACY2\tbuild-12-3\trunning\t127.0.0.1:49153->3000/tcp\n'
elif [ "$1" = "inspect" ]; then
  printf '%s\n'
fi
`, digest, digest)
	if err := os.WriteFile(filepath.Join(directory, "docker"), []byte(script), 0750); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	var inventories [][]hostingObservedRuntime
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request hostingHeartbeatRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode heartbeat: %v", err)
		}
		inventories = append(inventories, append([]hostingObservedRuntime(nil), (*request.RuntimeInventory)...))
		jsonResponse(w, hostingHeartbeatResponse{RetainedReleases: []hostingRetainedRelease{{
			ReleaseDigest: digest, ExternalProjectID: "project_01JLEGACY2",
			ExternalDeploymentID: "deployment_01JLEGACY2", RuntimeInstanceID: "build-12-3", Status: "active",
		}}})
	}))
	defer server.Close()
	config := hostingAgentConfig{ServerURL: server.URL, Token: "htr_test", WorkRoot: workRoot,
		SessionID: strings.Repeat("c", 48), HeartbeatSequence: new(atomic.Int64), SessionAccepted: new(atomic.Bool)}
	if err := publishHostingAgentHeartbeat(t.Context(), config, true); err != nil {
		t.Fatal(err)
	}
	if err := publishHostingAgentHeartbeat(t.Context(), config, true); err != nil {
		t.Fatal(err)
	}
	if len(inventories) != 2 || len(inventories[0]) != 0 || len(inventories[1]) != 1 ||
		inventories[1][0].RuntimeInstanceID != "build-12-3" {
		t.Fatalf("legacy adoption inventories=%+v", inventories)
	}
}

func TestHostingAgentInventoryFailureSuppressesHeartbeat(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "docker"), []byte("#!/bin/sh\nexit 1\n"), 0750); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		jsonResponse(w, hostingHeartbeatResponse{})
	}))
	defer server.Close()
	config := hostingAgentConfig{ServerURL: server.URL, Token: "htr_test", WorkRoot: t.TempDir(),
		SessionID: strings.Repeat("a", 48), HeartbeatSequence: new(atomic.Int64)}
	if err := publishHostingAgentHeartbeat(t.Context(), config, false); err == nil {
		t.Fatal("Docker inventory failure was accepted")
	}
	if requests != 0 || config.HeartbeatSequence.Load() != 0 {
		t.Fatalf("inventory failure sent heartbeat requests=%d sequence=%d", requests, config.HeartbeatSequence.Load())
	}
}

func TestHostingAgentCompletionPersistsAcrossControlPlaneFailure(t *testing.T) {
	workRoot := t.TempDir()
	failedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary", http.StatusServiceUnavailable)
	}))
	config := hostingAgentConfig{ServerURL: failedServer.URL, Token: "htr_test", WorkRoot: workRoot}
	job := &hostingClaimedJob{JobID: 42, LeaseGeneration: 3, LeaseToken: "lease-secret"}
	completion := hostingCompletionRequest{Status: "failed", FailureCode: "build_failed", FailureMessage: "safe"}
	if err := queueHostingAgentCompletion(config, job, completion); err != nil {
		t.Fatal(err)
	}
	if err := flushHostingAgentCompletions(t.Context(), config); err == nil {
		t.Fatal("temporary completion failure was not reported")
	}
	failedServer.Close()
	entries, err := os.ReadDir(hostingAgentCompletionDir(config))
	if err != nil || len(entries) != 1 {
		t.Fatalf("completion was not retained: entries=%d err=%v", len(entries), err)
	}
	info, err := entries[0].Info()
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("completion permissions=%v err=%v", info.Mode().Perm(), err)
	}
	var delivered hostingCompletionRequest
	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Deployer-Lease-Generation") != "3" || r.Header.Get("X-Deployer-Lease-Token") != "lease-secret" {
			t.Errorf("missing lease fencing headers")
		}
		if err := json.NewDecoder(r.Body).Decode(&delivered); err != nil {
			t.Errorf("decode completion: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer successServer.Close()
	config.ServerURL = successServer.URL
	if err := flushHostingAgentCompletions(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	entries, err = os.ReadDir(hostingAgentCompletionDir(config))
	if err != nil || len(entries) != 0 || delivered.FailureCode != "build_failed" {
		t.Fatalf("completion retry entries=%d delivered=%+v err=%v", len(entries), delivered, err)
	}
}

func TestHostingAgentRestoreUsesRecoveryContractAndRetainedArtifact(t *testing.T) {
	oldArtifactClient := agentArtifactClient
	oldControlClient := agentControlClient
	t.Cleanup(func() {
		agentArtifactClient = oldArtifactClient
		agentControlClient = oldControlClient
	})
	artifact := []byte("retained docker image archive")
	hash := sha256.Sum256(artifact)
	artifactDigest := "sha256:" + hex.EncodeToString(hash[:])
	releaseDigest := "sha256:" + strings.Repeat("d", 64)
	var artifactRequests, logRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/hosting-agent/v1/recoveries/77/artifact":
			artifactRequests++
			if r.Header.Get("X-Deployer-Lease-Generation") != "4" || r.Header.Get("X-Deployer-Lease-Token") != "recovery-lease" {
				t.Errorf("artifact request missing recovery lease fencing")
			}
			w.Write(artifact)
		case r.URL.Path == "/api/hosting-agent/v1/recoveries/77/logs":
			logRequests++
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet:
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	agentArtifactClient = server.Client()
	agentControlClient = server.Client()
	var healthPort int
	if _, err := fmt.Sscanf(server.URL, "http://127.0.0.1:%d", &healthPort); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	commandLog := filepath.Join(directory, "docker.log")
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> %q
if [ "$1" = "image" ] && [ "$2" = "load" ]; then
  printf 'Loaded image ID: %s\n'
elif [ "$1" = "image" ] && [ "$2" = "inspect" ]; then
  printf '%s\n'
elif [ "$1" = "inspect" ]; then
  printf 'Error: No such object\n' >&2
  exit 1
elif [ "$1" = "run" ]; then
  printf 'container-id\n'
elif [ "$1" = "port" ]; then
  printf '127.0.0.1:%d\n'
fi
`, commandLog, releaseDigest, releaseDigest, healthPort)
	if err := os.WriteFile(filepath.Join(directory, "docker"), []byte(script), 0750); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	job := &hostingClaimedJob{JobID: 77, LeaseGeneration: 4, LeaseToken: "recovery-lease", Recipe: hostingJobRecipe{
		Operation: "restore", ExternalProjectID: "project_01JRESTORE", ExternalDeploymentID: "deployment_01JRESTORE",
		Runtime:       HostingRuntimeManifest{Kind: "node", Port: 3000, HealthPath: "/"},
		Limits:        hostingWorkloadLimits{CPUMillis: 250, RAMBytes: 128 << 20, DiskBytes: 512 << 20, PIDs: 64},
		ReleaseDigest: releaseDigest, ReleaseArtifactDigest: artifactDigest,
		ReleaseArtifactURL: "/api/hosting-agent/v1/recoveries/77/artifact",
	}}
	workRoot := t.TempDir()
	completion := runHostingWorkload(t.Context(), hostingAgentConfig{ServerURL: server.URL, Token: "runner-token",
		WorkRoot: workRoot, RuntimeBindAddress: "127.0.0.1"}, job)
	if completion.Status != "success" || completion.ReleaseDigest != releaseDigest || completion.ReleaseArtifactDigest != artifactDigest {
		t.Fatalf("restore completion = %+v", completion)
	}
	commands, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatal(err)
	}
	text := string(commands)
	if !strings.Contains(text, "image load --input") || !strings.Contains(text, "run -d") ||
		!strings.Contains(text, "--cap-drop=ALL") || !strings.Contains(text, "127.0.0.1::3000") ||
		!strings.Contains(text, "--name deployer-hosting-deployment_01JRESTORE-restore-77-4") ||
		!strings.Contains(text, "light-apps.hosting.instance=restore-77-4") ||
		!strings.Contains(text, "light-apps.hosting.agent-namespace="+hostingAgentRuntimeNamespace(workRoot)) ||
		strings.Contains(text, " build ") {
		t.Fatalf("unexpected restore docker commands:\n%s", text)
	}
	if artifactRequests != 1 || logRequests == 0 {
		t.Fatalf("recovery requests artifact=%d logs=%d", artifactRequests, logRequests)
	}
}

func TestHostingAgentRestoreCompletionUsesRecoveryEndpoint(t *testing.T) {
	workRoot := t.TempDir()
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	config := hostingAgentConfig{ServerURL: server.URL, Token: "htr_test", WorkRoot: workRoot}
	job := &hostingClaimedJob{JobID: 9, LeaseGeneration: 2, LeaseToken: "lease", Recipe: hostingJobRecipe{Operation: "restore"}}
	if err := queueHostingAgentCompletion(config, job, hostingCompletionRequest{Status: "failed", FailureCode: "runner_lost"}); err != nil {
		t.Fatal(err)
	}
	if err := flushHostingAgentCompletions(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	if path != "/api/hosting-agent/v1/recoveries/9/complete" {
		t.Fatalf("completion path = %q", path)
	}
}
