package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandleAPIProjectRunbookReturnsOperationalGuidance(t *testing.T) {
	withTempDB(t)

	runner := &Runner{Name: "production-runner", Labels: "linux"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	project := &Project{
		Name:        "Dashboard",
		RepoPath:    t.TempDir(),
		DeployDir:   "/srv/apps/dashboard",
		DeployMode:  "files",
		RunnerID:    runner.ID,
		HealthURL:   "https://dashboard.example.com/health",
		PostDeploy:  "php artisan migrate --force",
		Preserve:    "uploads\n.env",
		Permissions: `{"owner":"www-data:www-data"}`,
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	build, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create build: %v", err)
	}
	finishedAt := time.Now().UTC()
	build.Status = "failed"
	build.FinishedAt = &finishedAt
	build.ErrorMessage = "post_deploy: migration failed"
	if err := updateBuild(build); err != nil {
		t.Fatalf("update build: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/projects/%d/runbook", project.ID), nil)
	rr := httptest.NewRecorder()
	handleAPIProject(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	var got projectRunbookResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode runbook: %v", err)
	}
	if got.ProjectID != project.ID || got.ProjectName != project.Name || got.DeployMode != "files" {
		t.Fatalf("unexpected runbook metadata: %+v", got)
	}
	if got.Runner == nil || got.Runner.ID != runner.ID || got.Runner.Status != "offline" {
		t.Fatalf("unexpected runner: %+v", got.Runner)
	}
	if got.Rollback.Supported || !hasStringContaining(got.Rollback.Notes, "not implemented") {
		t.Fatalf("unexpected rollback guidance: %+v", got.Rollback)
	}
	for _, want := range []string{"Review the deploy preview", "packages repository files", "assigned runner downloads"} {
		if !hasStringContaining(got.HowToDeploy, want) {
			t.Fatalf("missing deploy guidance %q in %#v", want, got.HowToDeploy)
		}
	}
	for _, want := range []string{project.RepoPath, ".deployignore or .dockerignore", "preserve:uploads", "preserve:.env", "permissions configuration"} {
		if !containsString(got.ImportantFiles, want) {
			t.Fatalf("missing important file %q in %#v", want, got.ImportantFiles)
		}
	}
	for _, want := range []string{"runner is offline", "health_url failed", "files deploy failures"} {
		if !hasStringContainingFold(got.FailureRecovery, want) {
			t.Fatalf("missing failure recovery %q in %#v", want, got.FailureRecovery)
		}
	}
	if !hasStringContaining(got.OperationalNotes, "post_deploy is privileged") {
		t.Fatalf("expected privileged post_deploy note, got %#v", got.OperationalNotes)
	}
}

func hasStringContaining(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}

func hasStringContainingFold(values []string, want string) bool {
	want = strings.ToLower(want)
	for _, value := range values {
		if strings.Contains(strings.ToLower(value), want) {
			return true
		}
	}
	return false
}
