package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestProjectBuildHistorySummarizesDurationsAndStatuses(t *testing.T) {
	withTempDB(t)

	project := createHistoryProject(t, "Dashboard")
	success := createHistoryBuild(t, project.ID, "success", 30)
	failed := createHistoryBuild(t, project.ID, "failed", 90)
	_ = failed
	running, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create running build: %v", err)
	}
	running.CommitSHA = "running"
	if err := updateBuild(running); err != nil {
		t.Fatalf("update running build: %v", err)
	}

	history, err := newProjectBuildHistory(project.ID, 10)
	if err != nil {
		t.Fatalf("project history: %v", err)
	}
	if history.Scope != "project" || history.ScopeID != project.ID || history.ScopeName != "Dashboard" {
		t.Fatalf("unexpected scope: %+v", history)
	}
	if history.Counts.Total != 3 || history.Counts.Success != 1 || history.Counts.Failed != 1 || history.Counts.Running != 1 {
		t.Fatalf("unexpected counts: %+v", history.Counts)
	}
	if history.AverageDurationSeconds == nil || *history.AverageDurationSeconds != 60 || history.MaxDurationSeconds != 90 {
		t.Fatalf("unexpected duration aggregate: avg=%v max=%d", history.AverageDurationSeconds, history.MaxDurationSeconds)
	}
	if len(history.Points) != 3 {
		t.Fatalf("expected 3 points, got %d", len(history.Points))
	}
	for _, point := range history.Points {
		if point.BuildID == success.ID && point.HeightPercent != 33 {
			t.Fatalf("expected success build height 33, got %+v", point)
		}
		if point.BuildID == running.ID && point.HeightPercent != 12 {
			t.Fatalf("expected running build fallback height 12, got %+v", point)
		}
	}
}

func TestRunnerBuildHistoryUsesAssignedJobs(t *testing.T) {
	withTempDB(t)

	runner := &Runner{Name: "runner-1"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	otherRunner := &Runner{Name: "runner-2"}
	if err := createRunner(otherRunner); err != nil {
		t.Fatalf("create other runner: %v", err)
	}
	project := createHistoryProject(t, "Dashboard")
	first := createHistoryBuild(t, project.ID, "success", 20)
	second := createHistoryBuild(t, project.ID, "cancelled", 40)
	other := createHistoryBuild(t, project.ID, "failed", 80)
	if _, err := createJob(first.ID, runner.ID, project, ""); err != nil {
		t.Fatalf("create first job: %v", err)
	}
	if _, err := createJob(second.ID, runner.ID, project, ""); err != nil {
		t.Fatalf("create second job: %v", err)
	}
	if _, err := createJob(other.ID, otherRunner.ID, project, ""); err != nil {
		t.Fatalf("create other job: %v", err)
	}

	history, err := newRunnerBuildHistory(runner.ID, 10)
	if err != nil {
		t.Fatalf("runner history: %v", err)
	}
	if history.Scope != "runner" || history.ScopeName != "runner-1" {
		t.Fatalf("unexpected scope: %+v", history)
	}
	if history.Counts.Total != 2 || history.Counts.Success != 1 || history.Counts.Cancelled != 1 || history.Counts.Failed != 0 {
		t.Fatalf("unexpected runner counts: %+v", history.Counts)
	}
	for _, point := range history.Points {
		if point.ProjectName != "Dashboard" {
			t.Fatalf("expected joined project name, got %+v", point)
		}
		if point.BuildID == other.ID {
			t.Fatalf("runner history included other runner build: %+v", point)
		}
	}
}

func TestBuildHistoryAPIHandlers(t *testing.T) {
	withTempDB(t)

	runner := &Runner{Name: "runner-1"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	project := createHistoryProject(t, "Dashboard")
	build := createHistoryBuild(t, project.ID, "success", 15)
	if _, err := createJob(build.ID, runner.ID, project, ""); err != nil {
		t.Fatalf("create job: %v", err)
	}

	projectReq := httptest.NewRequest(http.MethodGet, "/api/projects/1/history", nil)
	projectRR := httptest.NewRecorder()
	handleAPIProjectHistory(projectRR, projectReq, project.ID)
	if projectRR.Code != http.StatusOK {
		t.Fatalf("project history status = %d body=%s", projectRR.Code, projectRR.Body.String())
	}
	var projectHistory buildHistorySummaryResponse
	if err := json.NewDecoder(projectRR.Body).Decode(&projectHistory); err != nil {
		t.Fatalf("decode project history: %v", err)
	}
	if projectHistory.Counts.Total != 1 || projectHistory.Points[0].BuildID != build.ID {
		t.Fatalf("unexpected project history: %+v", projectHistory)
	}

	runnerReq := httptest.NewRequest(http.MethodGet, "/api/runners/1/history", nil)
	runnerRR := httptest.NewRecorder()
	handleAPIRunnerHistory(runnerRR, runnerReq, runner.ID)
	if runnerRR.Code != http.StatusOK {
		t.Fatalf("runner history status = %d body=%s", runnerRR.Code, runnerRR.Body.String())
	}
	var runnerHistory buildHistorySummaryResponse
	if err := json.NewDecoder(runnerRR.Body).Decode(&runnerHistory); err != nil {
		t.Fatalf("decode runner history: %v", err)
	}
	if runnerHistory.Counts.Total != 1 || runnerHistory.Points[0].ProjectName != "Dashboard" {
		t.Fatalf("unexpected runner history: %+v", runnerHistory)
	}
}

func createHistoryProject(t *testing.T, name string) *Project {
	t.Helper()
	project := &Project{
		Name:       name,
		RepoPath:   "/srv/repos/" + name,
		DeployDir:  "/srv/apps/" + name,
		ImageName:  "example/" + name,
		DeployMode: "docker",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	return project
}

func createHistoryBuild(t *testing.T, projectID int64, status string, durationSeconds int) *Build {
	t.Helper()
	build, err := createBuild(projectID, "manual")
	if err != nil {
		t.Fatalf("create build: %v", err)
	}
	finished := build.StartedAt.Add(time.Duration(durationSeconds) * time.Second)
	build.Status = status
	build.CommitSHA = status + "-sha"
	build.FinishedAt = &finished
	build.DurationSeconds = &durationSeconds
	if status == "failed" {
		build.ErrorMessage = "command failed"
	}
	if err := updateBuild(build); err != nil {
		t.Fatalf("update build: %v", err)
	}
	return build
}
