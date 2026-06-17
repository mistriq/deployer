package main

import (
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestAgentHTTPIntegrationPollLogComplete(t *testing.T) {
	withTempDB(t)

	oldBroker := broker
	broker = NewSSEBroker()
	t.Cleanup(func() { broker = oldBroker })

	runner := &Runner{Name: "integration-runner"}
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
	job, err := createJob(build.ID, runner.ID, project, "")
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/agent/poll", handleAgentPoll)
	mux.HandleFunc("/api/agent/log/", handleAgentLog)
	mux.HandleFunc("/api/agent/complete/", handleAgentComplete)
	server := httptest.NewServer(wrapHTTPHandler(AppConfig{
		AdminUser:     "admin",
		AdminPassword: "secret",
	}, mux))
	defer server.Close()

	polled := agentRequest(t, server.URL, runner.Token, http.MethodGet, "/api/agent/poll", nil)
	if polled.StatusCode != http.StatusOK {
		t.Fatalf("poll status = %d", polled.StatusCode)
	}
	var claimed Job
	if err := json.NewDecoder(polled.Body).Decode(&claimed); err != nil {
		t.Fatalf("decode poll response: %v", err)
	}
	polled.Body.Close()
	if claimed.ID != job.ID || claimed.Status != "running" || claimed.RunnerID != runner.ID {
		t.Fatalf("unexpected claimed job: %+v", claimed)
	}

	logResp := agentRequest(t, server.URL, runner.Token, http.MethodPost, "/api/agent/log/"+itoa(build.ID), strings.NewReader("agent log line\n"))
	if logResp.StatusCode != http.StatusOK {
		t.Fatalf("log status = %d", logResp.StatusCode)
	}
	logResp.Body.Close()
	loggedBuild, err := getBuild(build.ID)
	if err != nil {
		t.Fatalf("get logged build: %v", err)
	}
	if !strings.Contains(loggedBuild.Log, "agent log line") {
		t.Fatalf("expected build log to contain agent output, got %q", loggedBuild.Log)
	}

	completeBody := strings.NewReader(`{"status":"success","error":""}`)
	completeResp := agentRequest(t, server.URL, runner.Token, http.MethodPost, "/api/agent/complete/"+itoa(build.ID), completeBody)
	if completeResp.StatusCode != http.StatusOK {
		t.Fatalf("complete status = %d", completeResp.StatusCode)
	}
	completeResp.Body.Close()

	completedBuild, err := getBuild(build.ID)
	if err != nil {
		t.Fatalf("get completed build: %v", err)
	}
	if completedBuild.Status != "success" || completedBuild.FinishedAt == nil || completedBuild.DurationSeconds == nil {
		t.Fatalf("unexpected completed build: %+v", completedBuild)
	}
	completedJob, err := getJobByBuildID(build.ID)
	if err != nil {
		t.Fatalf("get completed job: %v", err)
	}
	if completedJob.Status != "completed" {
		t.Fatalf("expected completed job status, got %q", completedJob.Status)
	}
}

func TestAgentCompletionDoesNotOverwriteCancelledBuild(t *testing.T) {
	withTempDB(t)

	oldBroker := broker
	broker = NewSSEBroker()
	t.Cleanup(func() { broker = oldBroker })

	runner := &Runner{Name: "integration-runner"}
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
	job, err := createJob(build.ID, runner.ID, project, "")
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := updateJobStatus(job.ID, "running"); err != nil {
		t.Fatalf("mark job running: %v", err)
	}
	if ok, err := cancelRunningBuild(build.ID, "cancelled by user"); err != nil || !ok {
		t.Fatalf("cancel running build ok=%v err=%v", ok, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/agent/complete/", handleAgentComplete)
	server := httptest.NewServer(wrapHTTPHandler(AppConfig{
		AdminUser:     "admin",
		AdminPassword: "secret",
	}, mux))
	defer server.Close()

	completeResp := agentRequest(t, server.URL, runner.Token, http.MethodPost, "/api/agent/complete/"+itoa(build.ID), strings.NewReader(`{"status":"success","error":""}`))
	if completeResp.StatusCode != http.StatusOK {
		t.Fatalf("complete status = %d", completeResp.StatusCode)
	}
	completeResp.Body.Close()

	cancelledBuild, err := getBuild(build.ID)
	if err != nil {
		t.Fatalf("get cancelled build: %v", err)
	}
	if cancelledBuild.Status != "cancelled" || cancelledBuild.ErrorMessage != "cancelled by user" {
		t.Fatalf("expected cancelled build to remain cancelled, got %+v", cancelledBuild)
	}
	cancelledJob, err := getJobByBuildID(build.ID)
	if err != nil {
		t.Fatalf("get cancelled job: %v", err)
	}
	if cancelledJob.Status != "cancelled" {
		t.Fatalf("expected cancelled job status, got %q", cancelledJob.Status)
	}
	if cancelledJob.ErrorCode != runtimeErrCancelled {
		t.Fatalf("expected cancelled job error code, got %+v", cancelledJob)
	}
}

func TestAgentCompletionRejectsInvalidStatus(t *testing.T) {
	withTempDB(t)

	oldBroker := broker
	broker = NewSSEBroker()
	t.Cleanup(func() { broker = oldBroker })

	runner := &Runner{Name: "integration-runner"}
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
	job, err := createJob(build.ID, runner.ID, project, "")
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := updateJobStatus(job.ID, "running"); err != nil {
		t.Fatalf("mark job running: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/agent/complete/", handleAgentComplete)
	server := httptest.NewServer(wrapHTTPHandler(AppConfig{
		AdminUser:     "admin",
		AdminPassword: "secret",
	}, mux))
	defer server.Close()

	completeResp := agentRequest(t, server.URL, runner.Token, http.MethodPost, "/api/agent/complete/"+itoa(build.ID), strings.NewReader(`{"status":"unknown","error":""}`))
	if completeResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("complete status = %d", completeResp.StatusCode)
	}
	var errorBody apiErrorResponse
	if err := json.NewDecoder(completeResp.Body).Decode(&errorBody); err != nil {
		t.Fatalf("decode completion error: %v", err)
	}
	completeResp.Body.Close()
	if errorBody.Code != errCodeInvalidAgentCompletion {
		t.Fatalf("expected invalid completion code, got %+v", errorBody)
	}

	runningBuild, err := getBuild(build.ID)
	if err != nil {
		t.Fatalf("get build: %v", err)
	}
	if runningBuild.Status != "running" {
		t.Fatalf("expected build to remain running, got %+v", runningBuild)
	}
}

func TestAgentCompletionPersistsRuntimeErrorCodes(t *testing.T) {
	withTempDB(t)

	oldBroker := broker
	broker = NewSSEBroker()
	t.Cleanup(func() { broker = oldBroker })

	runner := &Runner{Name: "integration-runner"}
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
	job, err := createJob(build.ID, runner.ID, project, "")
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := updateJobStatus(job.ID, "running"); err != nil {
		t.Fatalf("mark job running: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/agent/complete/", handleAgentComplete)
	server := httptest.NewServer(wrapHTTPHandler(AppConfig{
		AdminUser:     "admin",
		AdminPassword: "secret",
	}, mux))
	defer server.Close()

	body := strings.NewReader(`{"status":"failed","error":"download artifact: status 404"}`)
	resp := agentRequest(t, server.URL, runner.Token, http.MethodPost, "/api/agent/complete/"+itoa(build.ID), body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("complete status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	failedBuild, err := getBuild(build.ID)
	if err != nil {
		t.Fatalf("get failed build: %v", err)
	}
	if failedBuild.Status != "failed" || failedBuild.ErrorCode != runtimeErrArtifactFailed {
		t.Fatalf("expected artifact error code on build, got %+v", failedBuild)
	}
	failedJob, err := getJobByBuildID(build.ID)
	if err != nil {
		t.Fatalf("get failed job: %v", err)
	}
	if failedJob.Status != "failed" || failedJob.ErrorCode != runtimeErrArtifactFailed {
		t.Fatalf("expected artifact error code on job, got %+v", failedJob)
	}
}

func TestAgentArtifactAndSnapshotHandlers(t *testing.T) {
	withTempDB(t)

	oldConfig := appConfig
	base := t.TempDir()
	appConfig = AppConfig{
		ArtifactDir: filepath.Join(base, "artifacts"),
		SnapshotDir: filepath.Join(base, "snapshots"),
	}
	t.Cleanup(func() { appConfig = oldConfig })
	if err := ensureRuntimeDirs(appConfig); err != nil {
		t.Fatalf("ensure runtime dirs: %v", err)
	}

	runner := &Runner{Name: "artifact-runner"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	otherRunner := &Runner{Name: "other-runner"}
	if err := createRunner(otherRunner); err != nil {
		t.Fatalf("create other runner: %v", err)
	}
	project := &Project{
		Name:       "Dashboard API",
		RepoPath:   "/srv/repos/dashboard",
		ImageName:  "registry.example.com/dashboard/api",
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "docker",
		RunnerID:   runner.ID,
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	build, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create build: %v", err)
	}
	artifactPath := managedArtifactPath("dashboard-api.tar")
	if err := os.WriteFile(artifactPath, []byte("docker image tar"), 0640); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	if _, err := createJob(build.ID, runner.ID, project, artifactPath); err != nil {
		t.Fatalf("create artifact job: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/agent/artifact/", handleAgentArtifact)
	mux.HandleFunc("/api/agent/snapshot/", handleAgentSnapshotUpload)
	server := httptest.NewServer(wrapHTTPHandler(AppConfig{
		AdminUser:     "admin",
		AdminPassword: "secret",
	}, mux))
	defer server.Close()

	forbidden := agentRequest(t, server.URL, otherRunner.Token, http.MethodGet, "/api/agent/artifact/"+itoa(build.ID), nil)
	if forbidden.StatusCode != http.StatusForbidden {
		t.Fatalf("artifact for wrong runner status = %d", forbidden.StatusCode)
	}
	forbidden.Body.Close()

	artifactResp := agentRequest(t, server.URL, runner.Token, http.MethodGet, "/api/agent/artifact/"+itoa(build.ID), nil)
	if artifactResp.StatusCode != http.StatusOK {
		t.Fatalf("artifact status = %d", artifactResp.StatusCode)
	}
	body, err := io.ReadAll(artifactResp.Body)
	if err != nil {
		t.Fatalf("read artifact response: %v", err)
	}
	artifactResp.Body.Close()
	if string(body) != "docker image tar" {
		t.Fatalf("unexpected artifact body %q", body)
	}
	_, params, err := mime.ParseMediaType(artifactResp.Header.Get("Content-Disposition"))
	if err != nil {
		t.Fatalf("parse content disposition: %v", err)
	}
	if params["filename"] != "registry.example.com-dashboard-api.tar" {
		t.Fatalf("unexpected artifact filename %q", params["filename"])
	}

	build.Status = "success"
	if err := updateBuild(build); err != nil {
		t.Fatalf("finish artifact build: %v", err)
	}

	snapshotBuild, err := createBuild(project.ID, "snapshot")
	if err != nil {
		t.Fatalf("create snapshot build: %v", err)
	}
	snapshotPath := managedSnapshotPath("dashboard-api-snapshot.tar.gz")
	if _, err := createSnapshotJob(snapshotBuild.ID, runner.ID, project, snapshotPath); err != nil {
		t.Fatalf("create snapshot job: %v", err)
	}
	snapshotResp := agentRequest(t, server.URL, runner.Token, http.MethodPost, "/api/agent/snapshot/"+itoa(snapshotBuild.ID), strings.NewReader("snapshot bytes"))
	if snapshotResp.StatusCode != http.StatusOK {
		t.Fatalf("snapshot upload status = %d", snapshotResp.StatusCode)
	}
	snapshotResp.Body.Close()
	gotSnapshot, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read uploaded snapshot: %v", err)
	}
	if string(gotSnapshot) != "snapshot bytes" {
		t.Fatalf("unexpected snapshot body %q", gotSnapshot)
	}
}

func TestRunnerAPIHandlersHideAndRotateTokens(t *testing.T) {
	withTempDB(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/runners", handleAPIRunners)
	mux.HandleFunc("/api/runners/", handleAPIRunner)
	server := httptest.NewServer(wrapHTTPHandler(AppConfig{
		AdminUser:     "admin",
		AdminPassword: "secret",
	}, mux))
	defer server.Close()

	client := server.Client()
	createReq, err := http.NewRequest(http.MethodPost, server.URL+"/api/runners", strings.NewReader(`{"name":"runner-1","labels":"prod linux"}`))
	if err != nil {
		t.Fatalf("create runner request: %v", err)
	}
	createReq.Header.Set(csrfHeader, "1")
	createReq.SetBasicAuth("admin", "secret")
	createResp, err := client.Do(createReq)
	if err != nil {
		t.Fatalf("create runner: %v", err)
	}
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("create runner status = %d", createResp.StatusCode)
	}
	var created Runner
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("decode created runner: %v", err)
	}
	createResp.Body.Close()
	if created.ID == 0 || created.Token == "" {
		t.Fatalf("expected creation response to include one-time token, got %+v", created)
	}

	getReq, err := http.NewRequest(http.MethodGet, server.URL+"/api/runners/"+itoa(created.ID), nil)
	if err != nil {
		t.Fatalf("get runner request: %v", err)
	}
	getReq.SetBasicAuth("admin", "secret")
	getResp, err := client.Do(getReq)
	if err != nil {
		t.Fatalf("get runner: %v", err)
	}
	var fetched Runner
	if err := json.NewDecoder(getResp.Body).Decode(&fetched); err != nil {
		t.Fatalf("decode fetched runner: %v", err)
	}
	getResp.Body.Close()
	if fetched.Token != "" {
		t.Fatalf("expected fetched runner token to be hidden, got %q", fetched.Token)
	}

	listReq, err := http.NewRequest(http.MethodGet, server.URL+"/api/runners", nil)
	if err != nil {
		t.Fatalf("list runners request: %v", err)
	}
	listReq.SetBasicAuth("admin", "secret")
	listResp, err := client.Do(listReq)
	if err != nil {
		t.Fatalf("list runners: %v", err)
	}
	var runners []Runner
	if err := json.NewDecoder(listResp.Body).Decode(&runners); err != nil {
		t.Fatalf("decode runners: %v", err)
	}
	listResp.Body.Close()
	if len(runners) != 1 || runners[0].Token != "" {
		t.Fatalf("expected listed token to be hidden, got %+v", runners)
	}

	rotateReq, err := http.NewRequest(http.MethodPost, server.URL+"/api/runners/"+itoa(created.ID)+"/rotate", nil)
	if err != nil {
		t.Fatalf("rotate runner request: %v", err)
	}
	rotateReq.Header.Set(csrfHeader, "1")
	rotateReq.SetBasicAuth("admin", "secret")
	rotateResp, err := client.Do(rotateReq)
	if err != nil {
		t.Fatalf("rotate runner: %v", err)
	}
	var rotated Runner
	if err := json.NewDecoder(rotateResp.Body).Decode(&rotated); err != nil {
		t.Fatalf("decode rotated runner: %v", err)
	}
	rotateResp.Body.Close()
	if rotated.Token == "" || rotated.Token == created.Token {
		t.Fatalf("expected rotated response to include a new token, got %+v", rotated)
	}
}

func TestAdminProjectAndBuildAPIHandlers(t *testing.T) {
	withTempDB(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/projects", handleAPIProjects)
	mux.HandleFunc("/api/projects/", handleAPIProject)
	mux.HandleFunc("/api/builds/", handleAPIBuild)
	server := httptest.NewServer(wrapHTTPHandler(AppConfig{
		AdminUser:     "admin",
		AdminPassword: "secret",
	}, mux))
	defer server.Close()
	client := server.Client()

	createReq := adminRequest(t, server.URL, http.MethodPost, "/api/projects", strings.NewReader(`{
		"name": "Dashboard",
		"repo_path": "/srv/repos/dashboard",
		"deploy_dir": "/srv/apps/dashboard",
		"deploy_mode": "files",
		"build_args": {"APP_ENV": "production"}
	}`))
	createResp, err := client.Do(createReq)
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("create project status = %d", createResp.StatusCode)
	}
	var created Project
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("decode created project: %v", err)
	}
	createResp.Body.Close()
	if created.ID == 0 || created.BuildArgs["APP_ENV"] != "production" {
		t.Fatalf("unexpected created project: %+v", created)
	}

	listResp, err := client.Do(adminRequest(t, server.URL, http.MethodGet, "/api/projects", nil))
	if err != nil {
		t.Fatalf("list projects: %v", err)
	}
	var projects []Project
	if err := json.NewDecoder(listResp.Body).Decode(&projects); err != nil {
		t.Fatalf("decode projects: %v", err)
	}
	listResp.Body.Close()
	if len(projects) != 1 || projects[0].ID != created.ID {
		t.Fatalf("unexpected projects list: %+v", projects)
	}

	updateReq := adminRequest(t, server.URL, http.MethodPut, "/api/projects/"+itoa(created.ID), strings.NewReader(`{
		"name": "Dashboard API",
		"repo_path": "/srv/repos/dashboard",
		"deploy_dir": "/srv/apps/dashboard",
		"deploy_mode": "files",
		"post_deploy": "systemctl reload app"
	}`))
	updateResp, err := client.Do(updateReq)
	if err != nil {
		t.Fatalf("update project: %v", err)
	}
	if updateResp.StatusCode != http.StatusOK {
		t.Fatalf("update project status = %d", updateResp.StatusCode)
	}
	updateResp.Body.Close()

	getResp, err := client.Do(adminRequest(t, server.URL, http.MethodGet, "/api/projects/"+itoa(created.ID), nil))
	if err != nil {
		t.Fatalf("get project: %v", err)
	}
	var fetched Project
	if err := json.NewDecoder(getResp.Body).Decode(&fetched); err != nil {
		t.Fatalf("decode fetched project: %v", err)
	}
	getResp.Body.Close()
	if fetched.Name != "Dashboard API" || fetched.PostDeploy != "systemctl reload app" {
		t.Fatalf("unexpected fetched project: %+v", fetched)
	}

	build, err := createBuild(created.ID, "manual")
	if err != nil {
		t.Fatalf("create build: %v", err)
	}
	build.Status = "failed"
	build.ErrorMessage = "package files: no such file"
	if err := updateBuild(build); err != nil {
		t.Fatalf("update build: %v", err)
	}
	buildResp, err := client.Do(adminRequest(t, server.URL, http.MethodGet, "/api/builds/"+itoa(build.ID), nil))
	if err != nil {
		t.Fatalf("get build: %v", err)
	}
	var fetchedBuild Build
	if err := json.NewDecoder(buildResp.Body).Decode(&fetchedBuild); err != nil {
		t.Fatalf("decode build: %v", err)
	}
	buildResp.Body.Close()
	if fetchedBuild.ID != build.ID || fetchedBuild.Status != "failed" {
		t.Fatalf("unexpected fetched build: %+v", fetchedBuild)
	}
}

func TestBuildArtifactHandlerServesSuccessfulSnapshots(t *testing.T) {
	withTempDB(t)

	oldConfig := appConfig
	base := t.TempDir()
	appConfig = AppConfig{
		ArtifactDir: filepath.Join(base, "artifacts"),
		SnapshotDir: filepath.Join(base, "snapshots"),
	}
	t.Cleanup(func() { appConfig = oldConfig })
	if err := ensureRuntimeDirs(appConfig); err != nil {
		t.Fatalf("ensure runtime dirs: %v", err)
	}

	runner := &Runner{Name: "snapshot-runner"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	project := &Project{
		Name:       "Dashboard API",
		RepoPath:   "/srv/repos/dashboard",
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "files",
		RunnerID:   runner.ID,
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	build, err := createBuild(project.ID, "snapshot")
	if err != nil {
		t.Fatalf("create snapshot build: %v", err)
	}
	snapshotPath := managedSnapshotPath("dashboard-api.tar.gz")
	if err := os.WriteFile(snapshotPath, []byte("snapshot archive"), 0640); err != nil {
		t.Fatalf("write snapshot artifact: %v", err)
	}
	if _, err := createSnapshotJob(build.ID, runner.ID, project, snapshotPath); err != nil {
		t.Fatalf("create snapshot job: %v", err)
	}
	build.Status = "success"
	finishedAt := parseSQLiteTime("2026-06-16T10:00:00Z")
	build.FinishedAt = &finishedAt
	if err := updateBuild(build); err != nil {
		t.Fatalf("finish snapshot build: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/builds/"+itoa(build.ID)+"/artifact", nil)
	handleBuildArtifact(rec, req, build.ID)
	if rec.Code != http.StatusOK {
		t.Fatalf("artifact status = %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "snapshot archive" {
		t.Fatalf("unexpected artifact body %q", got)
	}
	if !strings.Contains(rec.Header().Get("Content-Disposition"), "Dashboard-API-snapshot-") {
		t.Fatalf("unexpected content disposition %q", rec.Header().Get("Content-Disposition"))
	}
}

func TestSSEStreamSendsExistingCompletedBuild(t *testing.T) {
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
	finishedAt := parseSQLiteTime("2026-06-16T10:00:00Z")
	duration := 3
	build.Status = "success"
	build.FinishedAt = &finishedAt
	build.DurationSeconds = &duration
	build.Log = "first line\nsecond line"
	if err := updateBuild(build); err != nil {
		t.Fatalf("update build: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/builds/"+itoa(build.ID)+"/stream", nil)
	handleSSEStream(NewSSEBroker(), rec, req, build.ID)

	if rec.Code != http.StatusOK {
		t.Fatalf("sse status = %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "data: first line") || !strings.Contains(body, "event: status") || !strings.Contains(body, "data: success") {
		t.Fatalf("unexpected SSE body:\n%s", body)
	}
}

func agentRequest(t *testing.T, baseURL, token, method, path string, body io.Reader) *http.Response {
	t.Helper()

	req, err := http.NewRequest(method, baseURL+path, body)
	if err != nil {
		t.Fatalf("create %s %s request: %v", method, path, err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func adminRequest(t *testing.T, baseURL, method, path string, body io.Reader) *http.Request {
	t.Helper()

	req, err := http.NewRequest(method, baseURL+path, body)
	if err != nil {
		t.Fatalf("create %s %s request: %v", method, path, err)
	}
	req.SetBasicAuth("admin", "secret")
	if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch || method == http.MethodDelete {
		req.Header.Set(csrfHeader, "1")
	}
	return req
}

func itoa(value int64) string {
	return strconv.FormatInt(value, 10)
}
