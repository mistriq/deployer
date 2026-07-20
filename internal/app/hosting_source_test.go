package app

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func runGitTestCommand(t *testing.T, directory string, args ...string) string {
	t.Helper()
	commandArgs := append([]string{"-C", directory}, args...)
	output, err := exec.Command("git", commandArgs...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

func TestPrepareHostingGitSourceUsesExactCommitNotWorkingTreeOrHead(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is required for exact-source integration test")
	}
	oldConfig := appConfig
	oldStorage := artifactStorage
	t.Cleanup(func() {
		appConfig = oldConfig
		artifactStorage = oldStorage
	})
	root := t.TempDir()
	repository := filepath.Join(root, "repository")
	if err := os.Mkdir(repository, 0750); err != nil {
		t.Fatal(err)
	}
	runGitTestCommand(t, repository, "init")
	runGitTestCommand(t, repository, "config", "user.email", "test@example.invalid")
	runGitTestCommand(t, repository, "config", "user.name", "Hosting Test")
	file := filepath.Join(repository, "index.js")
	if err := os.WriteFile(file, []byte("first\n"), 0640); err != nil {
		t.Fatal(err)
	}
	runGitTestCommand(t, repository, "add", "index.js")
	runGitTestCommand(t, repository, "commit", "-m", "first")
	firstCommit := runGitTestCommand(t, repository, "rev-parse", "HEAD")
	if err := os.WriteFile(file, []byte("second\n"), 0640); err != nil {
		t.Fatal(err)
	}
	runGitTestCommand(t, repository, "add", "index.js")
	runGitTestCommand(t, repository, "commit", "-m", "second")
	if err := os.WriteFile(file, []byte("uncommitted\n"), 0640); err != nil {
		t.Fatal(err)
	}
	appConfig.ArtifactDir = filepath.Join(root, "artifacts")
	appConfig.SnapshotDir = filepath.Join(root, "snapshots")
	configureArtifactStorage(appConfig)
	if err := currentArtifactStorage().Ensure(); err != nil {
		t.Fatal(err)
	}
	path, digest, err := prepareHostingGitSource(context.Background(), &HostingProject{RepoPath: repository}, firstCommit)
	if err != nil {
		t.Fatal(err)
	}
	if !validSHA256Digest(digest) || !isManagedArtifactPath(path) {
		t.Fatalf("artifact path=%q digest=%q", path, digest)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader := tar.NewReader(bytes.NewReader(data))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			t.Fatal("index.js missing from source archive")
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == "index.js" {
			content, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			if string(content) != "first\n" {
				t.Fatalf("archive used mutable source: %q", content)
			}
			break
		}
	}
	if _, _, err := prepareHostingGitSource(context.Background(), &HostingProject{RepoPath: repository}, strings.Repeat("f", 40)); err == nil {
		t.Fatal("missing exact commit was accepted")
	}
}
