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

func TestHandleAPIBuildEventsReturnsStructuredLogAndJobMetadata(t *testing.T) {
	withTempDB(t)

	runner := &Runner{Name: "production-runner"}
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
	build, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create build: %v", err)
	}
	job, err := createJob(build.ID, runner.ID, project, "/tmp/deployer-artifacts/dashboard.tar.gz")
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	finishedAt := time.Now().UTC()
	build.Status = "failed"
	build.FinishedAt = &finishedAt
	build.ErrorMessage = "package files: permission denied"
	build.CommitSHA = "abc1234"
	build.Log = strings.Join([]string{
		"##[group]Package Files",
		"Packaging files from /srv/repos/dashboard",
		"##[endgroup:3]",
		"FAILED at step 'package files': permission denied",
	}, "\n")
	if err := updateBuild(build); err != nil {
		t.Fatalf("update build: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/builds/%d/events", build.ID), nil)
	rr := httptest.NewRecorder()
	handleAPIBuild(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	var got buildEventsResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode build events: %v", err)
	}
	if got.BuildID != build.ID || got.ProjectID != project.ID || got.Status != "failed" || got.CommitSHA != "abc1234" || got.ErrorCode != runtimeErrArtifactFailed {
		t.Fatalf("unexpected build metadata: %+v", got)
	}
	if got.JobID == nil || *got.JobID != job.ID || got.RunnerID == nil || *got.RunnerID != runner.ID {
		t.Fatalf("unexpected job metadata: job=%v runner=%v", got.JobID, got.RunnerID)
	}
	if got.ArtifactID != fmt.Sprintf("job-%d-artifact", job.ID) || !got.ArtifactAvailable {
		t.Fatalf("unexpected artifact metadata: id=%q available=%v", got.ArtifactID, got.ArtifactAvailable)
	}
	if !hasEvent(got.Events, "step_started", "Package Files", "running", "") {
		t.Fatalf("missing step_started event: %+v", got.Events)
	}
	if !hasDurationEvent(got.Events, "Package Files", 3) {
		t.Fatalf("missing duration event: %+v", got.Events)
	}
	if !hasEvent(got.Events, "step_failed", "package files", "failed", "permission denied") {
		t.Fatalf("missing failed step event: %+v", got.Events)
	}
	if !hasEvent(got.Events, "build_finished", "", "failed", "package files: permission denied") {
		t.Fatalf("missing terminal build event: %+v", got.Events)
	}
	if !hasErrorCodeEvent(got.Events, "build_finished", runtimeErrArtifactFailed) {
		t.Fatalf("missing terminal error code event: %+v", got.Events)
	}
}

func hasEvent(events []structuredEvent, eventType, step, status, errorText string) bool {
	for _, event := range events {
		if event.Type != eventType || event.Step != step || event.Status != status {
			continue
		}
		if errorText != "" && event.Error != errorText {
			continue
		}
		return true
	}
	return false
}

func hasErrorCodeEvent(events []structuredEvent, eventType, errorCode string) bool {
	for _, event := range events {
		if event.Type == eventType && event.ErrorCode == errorCode {
			return true
		}
	}
	return false
}

func hasDurationEvent(events []structuredEvent, step string, duration int) bool {
	for _, event := range events {
		if event.Type == "step_finished" && event.Step == step && event.DurationSeconds != nil && *event.DurationSeconds == duration {
			return true
		}
	}
	return false
}
