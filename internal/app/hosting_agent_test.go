package app

import (
	"archive/tar"
	"bytes"
	"context"
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
  printf 'keep-container\t%s\tproject_01JKEEPX\tdeployment_01JKEEPX\ndrop-container\t%s\tproject_01JDROPX\tdeployment_01JDROPX\n'
elif [ "$1" = "inspect" ]; then
  printf '%s\n'
fi
`, logPath, keep, drop, imageDigest)
	dockerPath := filepath.Join(directory, "docker")
	if err := os.WriteFile(dockerPath, []byte(script), 0750); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := reconcileHostingAgentReleases(context.Background(), []hostingRetainedRelease{{ReleaseDigest: keep, ExternalProjectID: "project_01JKEEPX", ExternalDeploymentID: "deployment_01JKEEPX", Status: "active"}}); err != nil {
		t.Fatal(err)
	}
	commands, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(commands)
	if strings.Contains(text, "rm -f keep-container") || !strings.Contains(text, "rm -f drop-container") || !strings.Contains(text, "image rm "+imageDigest) {
		t.Fatalf("docker commands = %s", text)
	}
	if err := reconcileHostingAgentReleases(context.Background(), []hostingRetainedRelease{{ReleaseDigest: "invalid", ExternalProjectID: "project_01JKEEPX", ExternalDeploymentID: "deployment_01JKEEPX"}}); err == nil {
		t.Fatal("invalid retained digest was accepted")
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
