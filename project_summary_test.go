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

func TestHandleAPIProjectSummaryReturnsCompactProjectState(t *testing.T) {
	withTempDB(t)

	runner := &Runner{Name: "production-runner", Labels: "linux,docker"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	project := &Project{
		Name:       "Dashboard",
		RepoPath:   "/srv/repos/dashboard",
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "files",
		RunnerID:   runner.ID,
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}

	failedBuild, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create failed build: %v", err)
	}
	finishedAt := time.Now().UTC()
	duration := 12
	failedBuild.Status = "failed"
	failedBuild.ErrorMessage = "health check failed"
	failedBuild.FinishedAt = &finishedAt
	failedBuild.DurationSeconds = &duration
	failedBuild.Log = strings.Repeat("log line\n", 100)
	if err := updateBuild(failedBuild); err != nil {
		t.Fatalf("update failed build: %v", err)
	}

	runningBuild, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create running build: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/projects/%d/summary", project.ID), nil)
	rr := httptest.NewRecorder()
	handleAPIProject(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}

	var got projectSummaryResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode project summary: %v", err)
	}
	if got.Project.ID != project.ID || got.Project.Name != project.Name {
		t.Fatalf("unexpected project summary: %+v", got.Project)
	}
	if got.Runner == nil || got.Runner.ID != runner.ID || got.Runner.Status != "offline" {
		t.Fatalf("unexpected runner summary: %+v", got.Runner)
	}
	if got.LastBuild == nil || got.LastBuild.ID != runningBuild.ID || got.LastBuild.Status != "running" {
		t.Fatalf("unexpected last build summary: %+v", got.LastBuild)
	}
	if len(got.RecentFailures) != 1 ||
		got.RecentFailures[0].ID != failedBuild.ID ||
		got.RecentFailures[0].ErrorMessage != "health check failed" ||
		got.RecentFailures[0].ErrorCode != runtimeErrHealthCheckFailed {
		t.Fatalf("unexpected recent failures: %+v", got.RecentFailures)
	}
	if !hasRecommendedAction(got.RecommendedActions, "Runner is offline") ||
		!hasRecommendedAction(got.RecommendedActions, "A build is running") ||
		!hasRecommendedAction(got.RecommendedActions, "No health check") {
		t.Fatalf("unexpected recommended actions: %#v", got.RecommendedActions)
	}
}

func hasRecommendedAction(actions []string, prefix string) bool {
	for _, action := range actions {
		if strings.HasPrefix(action, prefix) {
			return true
		}
	}
	return false
}
