package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCloneProjectCopiesConfigWithoutBuildHistory(t *testing.T) {
	withTempDB(t)

	runner := &Runner{Name: "template-runner", Labels: "linux"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	source := &Project{
		Name:               "Template API",
		RepoPath:           "/srv/repos/template-api",
		DockerfilePath:     "Dockerfile",
		ComposeFile:        "compose.yaml",
		ImageName:          "registry.example.com/template-api",
		DeployDir:          "/srv/apps/template-api",
		HealthURL:          "https://template.example.com/health",
		BuildArgs:          map[string]string{"APP_ENV": "production"},
		ComposeServices:    "web worker",
		GitPullBeforeBuild: true,
		RunnerID:           runner.ID,
		DeployMode:         "docker",
		PostDeploy:         "systemctl reload template-api",
		Preserve:           ".env\nuploads",
	}
	if err := createProject(source); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createHistoryBuild(t, source.ID, "success", 30)

	clone, err := cloneProject(source.ID, "Cloned API")
	if err != nil {
		t.Fatalf("clone project: %v", err)
	}
	if clone.ID == 0 || clone.ID == source.ID || clone.Name != "Cloned API" {
		t.Fatalf("unexpected clone identity: %+v", clone)
	}
	if clone.RepoPath != source.RepoPath || clone.DeployDir != source.DeployDir || clone.ImageName != source.ImageName ||
		clone.RunnerID != source.RunnerID || clone.PostDeploy != source.PostDeploy || clone.BuildArgs["APP_ENV"] != "production" {
		t.Fatalf("clone did not copy source config: source=%+v clone=%+v", source, clone)
	}
	clone.BuildArgs["APP_ENV"] = "staging"
	if source.BuildArgs["APP_ENV"] != "production" {
		t.Fatalf("expected build args to be deep copied, source args=%#v clone args=%#v", source.BuildArgs, clone.BuildArgs)
	}
	builds, err := listBuilds(clone.ID, 10)
	if err != nil {
		t.Fatalf("list cloned builds: %v", err)
	}
	if len(builds) != 0 {
		t.Fatalf("expected clone to start without build history, got %+v", builds)
	}
}

func TestCloneProjectUsesDefaultCopyName(t *testing.T) {
	withTempDB(t)

	source := &Project{
		Name:       "Template Site",
		RepoPath:   "/srv/repos/template-site",
		DeployDir:  "/srv/apps/template-site",
		DeployMode: "files",
	}
	if err := createProject(source); err != nil {
		t.Fatalf("create project: %v", err)
	}
	clone, err := cloneProject(source.ID, "")
	if err != nil {
		t.Fatalf("clone project: %v", err)
	}
	if clone.Name != "Template Site Copy" {
		t.Fatalf("expected default copy name, got %q", clone.Name)
	}
}

func TestProjectCloneAPIHandler(t *testing.T) {
	withTempDB(t)

	source := &Project{
		Name:       "Template Site",
		RepoPath:   "/srv/repos/template-site",
		DeployDir:  "/srv/apps/template-site",
		DeployMode: "files",
	}
	if err := createProject(source); err != nil {
		t.Fatalf("create project: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/projects/1/clone", strings.NewReader(`{"name":"Imported Template"}`))
	rr := httptest.NewRecorder()
	handleAPIProject(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("clone status = %d body=%s", rr.Code, rr.Body.String())
	}
	var clone Project
	if err := json.NewDecoder(rr.Body).Decode(&clone); err != nil {
		t.Fatalf("decode clone: %v", err)
	}
	if clone.ID == source.ID || clone.Name != "Imported Template" || clone.DeployMode != "files" {
		t.Fatalf("unexpected clone response: %+v", clone)
	}
}

func TestProjectCloneRejectsWrongMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/projects/1/clone", nil)
	rr := httptest.NewRecorder()
	handleAPIProject(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected method not allowed, got %d body=%s", rr.Code, rr.Body.String())
	}
}
