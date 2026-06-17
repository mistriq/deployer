package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestHandleAPIProjectPreviewReturnsReadOnlyDeployPlan(t *testing.T) {
	withTempDB(t)

	repoPath, wantSHA := initPreviewGitRepo(t)
	runner := &Runner{Name: "production-runner", Labels: "linux,docker"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	project := &Project{
		Name:               "Dashboard",
		RepoPath:           repoPath,
		DeployDir:          "/srv/apps/dashboard",
		DeployMode:         "docker",
		ImageName:          "example/dashboard",
		RunnerID:           runner.ID,
		GitPullBeforeBuild: true,
		HealthURL:          "https://dashboard.example.com/health",
		PostDeploy:         "echo migrated",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/projects/%d/preview", project.ID), nil)
	rr := httptest.NewRecorder()
	handleAPIProject(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	var got deployPreviewResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode deploy preview: %v", err)
	}
	if got.ProjectID != project.ID || got.ProjectName != project.Name || got.RepoPath != repoPath {
		t.Fatalf("unexpected project metadata: %+v", got)
	}
	if got.CommitSHA != wantSHA || got.CommitError != "" {
		t.Fatalf("unexpected commit metadata: sha=%q err=%q want=%q", got.CommitSHA, got.CommitError, wantSHA)
	}
	if got.Transport != "agent" || got.Runner == nil || got.Runner.ID != runner.ID {
		t.Fatalf("unexpected transport/runner: transport=%q runner=%+v", got.Transport, got.Runner)
	}
	if got.ExpectedArtifact.Type != "docker_image_tar" || got.ExpectedArtifact.Extension != ".tar" || !got.ExpectedArtifact.ServerManaged {
		t.Fatalf("unexpected artifact preview: %+v", got.ExpectedArtifact)
	}
	for _, step := range []string{"git_pull", "get_commit_sha", "docker_build", "docker_save", "create_agent_job", "wait_for_agent"} {
		if !containsString(got.PlannedSteps, step) {
			t.Fatalf("expected step %q in %#v", step, got.PlannedSteps)
		}
	}
	if !got.Hooks.PostDeployEnabled || got.Hooks.PostDeploy != "echo migrated" {
		t.Fatalf("unexpected hooks: %+v", got.Hooks)
	}
	if !got.HealthCheck.Enabled || got.HealthCheck.URL != "https://dashboard.example.com/health" {
		t.Fatalf("unexpected health check: %+v", got.HealthCheck)
	}
}

func TestHandleAPIProjectPreviewIncludesCommitErrorWithoutFailing(t *testing.T) {
	withTempDB(t)

	project := &Project{
		Name:       "Static Site",
		RepoPath:   t.TempDir(),
		DeployDir:  "/srv/apps/site",
		DeployMode: "files",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/projects/%d/preview", project.ID), nil)
	rr := httptest.NewRecorder()
	handleAPIProject(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	var got deployPreviewResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode deploy preview: %v", err)
	}
	if got.CommitSHA != "" || got.CommitError == "" {
		t.Fatalf("expected commit error without sha, got sha=%q err=%q", got.CommitSHA, got.CommitError)
	}
	if got.ExpectedArtifact.Type != "files_archive" || got.Transport != "local" {
		t.Fatalf("unexpected files preview: artifact=%+v transport=%q", got.ExpectedArtifact, got.Transport)
	}
	if !hasWarning(got.Warnings, "Files mode should use an agent runner") {
		t.Fatalf("expected files-mode warning, got %#v", got.Warnings)
	}
}

func initPreviewGitRepo(t *testing.T) (string, string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	repoPath := t.TempDir()
	runGit(t, repoPath, "init")
	runGit(t, repoPath, "config", "user.email", "test@example.com")
	runGit(t, repoPath, "config", "user.name", "Test User")
	if err := os.WriteFile(filepath.Join(repoPath, "README.md"), []byte("preview\n"), 0644); err != nil {
		t.Fatalf("write git file: %v", err)
	}
	runGit(t, repoPath, "add", "README.md")
	runGit(t, repoPath, "commit", "-m", "initial commit")
	out := runGit(t, repoPath, "rev-parse", "--short", "HEAD")
	return repoPath, strings.TrimSpace(out)
}

func runGit(t *testing.T, repoPath string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", repoPath}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func hasWarning(warnings []string, prefix string) bool {
	for _, warning := range warnings {
		if strings.HasPrefix(warning, prefix) {
			return true
		}
	}
	return false
}
