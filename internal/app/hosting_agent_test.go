package app

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
		{Kind: "node", NodeVersion: "20", PackageManager: "pnpm", BuildScript: "compile", StartScript: "start", Port: 3000},
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
			unchanged, err := os.ReadFile(filepath.Join(directory, "Dockerfile"))
			if err != nil || !bytes.Equal(unchanged, customerDockerfile) {
				t.Fatalf("customer Dockerfile was changed: %q, %v", unchanged, err)
			}
		})
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
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> %q
if [ "$1" = "ps" ]; then
  printf 'keep-container\t%s\tproject_01JKEEPX\tdeployment_01JKEEPX\tbuild-1-1\ndrop-container\t%s\tproject_01JDROPX\tdeployment_01JDROPX\tbuild-2-1\nstale-container\t%s\tproject_01JKEEPX\tdeployment_01JKEEPX\tbuild-1-1\n'
elif [ "$1" = "inspect" ]; then
  printf '%s\n'
fi
`, logPath, keep, drop, drop, imageDigest)
	dockerPath := filepath.Join(directory, "docker")
	if err := os.WriteFile(dockerPath, []byte(script), 0750); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := reconcileHostingAgentReleases(context.Background(), []hostingRetainedRelease{{ReleaseDigest: keep, ExternalProjectID: "project_01JKEEPX", ExternalDeploymentID: "deployment_01JKEEPX", RuntimeInstanceID: "build-1-1", Status: "active"}}); err != nil {
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
	if err := reconcileHostingAgentReleases(context.Background(), []hostingRetainedRelease{{ReleaseDigest: "invalid", ExternalProjectID: "project_01JKEEPX", ExternalDeploymentID: "deployment_01JKEEPX"}}); err == nil {
		t.Fatal("invalid retained digest was accepted")
	}
}

func TestReconcileHostingAgentReleasesAcceptsOnlyUnlabelledLegacyInstance(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "docker.log")
	digest := "sha256:" + strings.Repeat("a", 64)
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> %q
if [ "$1" = "ps" ]; then
  printf 'legacy-container\t%s\tproject_01JLEGACY\tdeployment_01JLEGACY\t\nlabelled-container\t%s\tproject_01JLEGACY\tdeployment_01JLEGACY\tbuild-9-1\n'
elif [ "$1" = "inspect" ]; then
  printf '%s\n'
fi
`, logPath, digest, digest, digest)
	dockerPath := filepath.Join(directory, "docker")
	if err := os.WriteFile(dockerPath, []byte(script), 0750); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	retained := []hostingRetainedRelease{{ReleaseDigest: digest, ExternalProjectID: "project_01JLEGACY",
		ExternalDeploymentID: "deployment_01JLEGACY", Status: "active"}}
	if err := reconcileHostingAgentReleases(t.Context(), retained); err != nil {
		t.Fatal(err)
	}
	commands, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(commands)
	if strings.Contains(text, "rm -f legacy-container") || !strings.Contains(text, "rm -f labelled-container") {
		t.Fatalf("legacy identity cleanup commands = %s", text)
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
	completion := runHostingWorkload(t.Context(), hostingAgentConfig{ServerURL: server.URL, Token: "runner-token",
		WorkRoot: t.TempDir(), RuntimeBindAddress: "127.0.0.1"}, job)
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
