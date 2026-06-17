package app

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandleAPIBuildFailureSummaryExtractsStepCauseAndFix(t *testing.T) {
	withTempDB(t)

	project := &Project{
		Name:       "Dashboard",
		RepoPath:   "/srv/repos/dashboard",
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "files",
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
	build.ErrorMessage = "health check: health URL not responding"
	build.Log = strings.Join([]string{
		"##[group]Health Check",
		"Checking https://dashboard.example.com/health",
		"curl: failed to connect",
		"FAILED at step 'health check': health URL not responding",
	}, "\n")
	if err := updateBuild(build); err != nil {
		t.Fatalf("update build: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/builds/%d/failure-summary", build.ID), nil)
	rr := httptest.NewRecorder()
	handleAPIBuild(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	var got buildFailureSummaryResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode failure summary: %v", err)
	}
	if !got.HasFailure || got.FailedStep != "health check" || got.LikelyCause != "health URL not responding" {
		t.Fatalf("unexpected failure summary: %+v", got)
	}
	if got.ErrorCode != runtimeErrHealthCheckFailed {
		t.Fatalf("expected health error code, got %+v", got)
	}
	if !strings.Contains(got.SuggestedFix, "health") {
		t.Fatalf("expected health suggested fix, got %q", got.SuggestedFix)
	}
	if !containsString(got.RelevantLogLines, "curl: failed to connect") {
		t.Fatalf("expected relevant curl line, got %#v", got.RelevantLogLines)
	}
}

func TestHandleAPIBuildFailureSummaryReportsNoFailureForSuccessfulBuild(t *testing.T) {
	withTempDB(t)

	project := &Project{
		Name:       "Dashboard",
		RepoPath:   "/srv/repos/dashboard",
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "files",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	build, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create build: %v", err)
	}
	finishedAt := time.Now().UTC()
	build.Status = "success"
	build.FinishedAt = &finishedAt
	if err := updateBuild(build); err != nil {
		t.Fatalf("update build: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/builds/%d/failure-summary", build.ID), nil)
	rr := httptest.NewRecorder()
	handleAPIBuild(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	var got buildFailureSummaryResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode failure summary: %v", err)
	}
	if got.HasFailure || got.FailedStep != "" || got.SuggestedFix != "" {
		t.Fatalf("expected no failure summary, got %+v", got)
	}
}
