package app

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestHandleAPIProjectAutodetectFindsRepoSignals(t *testing.T) {
	withTempDB(t)

	repoPath := t.TempDir()
	writeRepoFile(t, repoPath, "Dockerfile")
	writeRepoFile(t, repoPath, "compose.yaml")
	writeRepoFile(t, repoPath, "package-lock.json")
	writeRepoFile(t, repoPath, "go.mod")
	writeRepoFile(t, repoPath, ".deployignore")

	project := &Project{
		Name:       "Dashboard",
		RepoPath:   repoPath,
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "files",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/projects/%d/autodetect", project.ID), nil)
	rr := httptest.NewRecorder()
	handleAPIProject(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	var got repoAutodetectResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode autodetect response: %v", err)
	}
	if got.ProjectID != project.ID || got.RepoPath != repoPath {
		t.Fatalf("unexpected repo metadata: %+v", got)
	}
	if got.LikelyDeployMode != "docker" || got.SuggestedFields["deploy_mode"] != "docker" {
		t.Fatalf("expected docker deploy mode, got %+v", got)
	}
	if !containsString(got.Dockerfiles, "Dockerfile") || got.SuggestedFields["dockerfile_path"] != "Dockerfile" {
		t.Fatalf("expected Dockerfile detection, got %+v", got)
	}
	if !containsString(got.ComposeFiles, "compose.yaml") || got.SuggestedFields["compose_file"] != "compose.yaml" {
		t.Fatalf("expected compose file detection, got %+v", got)
	}
	if got.IgnoreFile != ".deployignore" {
		t.Fatalf("expected .deployignore, got %q", got.IgnoreFile)
	}
	if !containsString(got.PackageManagers, "npm") || !containsString(got.PackageManagers, "go") {
		t.Fatalf("expected npm and go package managers, got %#v", got.PackageManagers)
	}
}

func TestHandleAPIProjectAutodetectWarnsForMissingRepo(t *testing.T) {
	withTempDB(t)

	project := &Project{
		Name:       "Missing Repo",
		RepoPath:   filepath.Join(t.TempDir(), "missing"),
		DeployDir:  "/srv/apps/missing",
		DeployMode: "files",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/projects/%d/autodetect", project.ID), nil)
	rr := httptest.NewRecorder()
	handleAPIProject(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	var got repoAutodetectResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode autodetect response: %v", err)
	}
	if got.LikelyDeployMode != "files" || got.SuggestedFields["deploy_mode"] != "files" {
		t.Fatalf("expected files fallback, got %+v", got)
	}
	if !hasStringContaining(got.Warnings, "not readable") {
		t.Fatalf("expected unreadable repo warning, got %#v", got.Warnings)
	}
}

func writeRepoFile(t *testing.T, repoPath, name string) {
	t.Helper()
	path := filepath.Join(repoPath, name)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("create repo dir: %v", err)
	}
	if err := os.WriteFile(path, []byte("test\n"), 0644); err != nil {
		t.Fatalf("write repo file %s: %v", name, err)
	}
}
