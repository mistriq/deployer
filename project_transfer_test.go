package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProjectExportOmitsSecretsAndRuntimeFields(t *testing.T) {
	withTempDB(t)

	runner := &Runner{Name: "demo-runner", Token: "runner-token", Labels: "linux"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	project := &Project{
		Name:               "Dashboard",
		RepoPath:           "/srv/repos/dashboard",
		DockerfilePath:     "Dockerfile",
		ComposeFile:        "compose.yaml",
		ImageName:          "registry.example.com/dashboard",
		DeployDir:          "/srv/apps/dashboard",
		HealthURL:          "https://dashboard.example.com/health",
		BuildArgs:          map[string]string{"APP_ENV": "production", "API_TOKEN": "secret-token", "HEADER": "Authorization: Bearer live-token"},
		ComposeServices:    "web worker",
		GitPullBeforeBuild: true,
		RunnerID:           runner.ID,
		DeployMode:         "docker",
		PostDeploy:         "DEPLOYER_TOKEN=secret reload-dashboard",
		Preserve:           ".env\nuploads",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}

	exported, err := newProjectExport(project.ID)
	if err != nil {
		t.Fatalf("export project: %v", err)
	}
	if exported.Kind != "deployer.project" || exported.Version != projectExportVersion {
		t.Fatalf("unexpected export envelope: %+v", exported)
	}
	if exported.Project.BuildArgs["APP_ENV"] != "production" {
		t.Fatalf("expected safe build arg to remain, got %#v", exported.Project.BuildArgs)
	}
	for _, key := range []string{"API_TOKEN", "HEADER"} {
		if _, ok := exported.Project.BuildArgs[key]; ok {
			t.Fatalf("expected sensitive build arg %s to be omitted, got %#v", key, exported.Project.BuildArgs)
		}
	}
	for _, want := range []string{"build_args.API_TOKEN", "build_args.HEADER", "post_deploy", "runner_id"} {
		if !containsString(exported.OmittedFields, want) {
			t.Fatalf("expected omitted field %q in %#v", want, exported.OmittedFields)
		}
	}

	payload, err := json.Marshal(exported)
	if err != nil {
		t.Fatalf("marshal export: %v", err)
	}
	for _, secret := range []string{"secret-token", "live-token", "runner-token", "DEPLOYER_TOKEN"} {
		if strings.Contains(string(payload), secret) {
			t.Fatalf("expected export payload to omit %q, got %s", secret, payload)
		}
	}
	for _, property := range []string{`"post_deploy":`, `"runner_id":`} {
		if strings.Contains(string(payload), property) {
			t.Fatalf("expected export payload to omit property %q, got %s", property, payload)
		}
	}
}

func TestProjectImportCreatesProjectWithoutOmittedRuntimeFields(t *testing.T) {
	withTempDB(t)

	body := `{
		"kind": "deployer.project",
		"version": 1,
		"exported_at": "2026-06-17T00:00:00Z",
		"omitted_fields": ["runner_id", "post_deploy", "build_args.API_TOKEN"],
		"project": {
			"name": "Imported Dashboard",
			"repo_path": "/srv/repos/imported-dashboard",
			"dockerfile_path": "Dockerfile",
			"compose_file": "compose.yaml",
			"image_name": "registry.example.com/imported-dashboard",
			"deploy_dir": "/srv/apps/imported-dashboard",
			"health_url": "https://imported.example.com/health",
			"build_args": {"APP_ENV": "staging"},
			"compose_services": "web",
			"git_pull_before_build": true,
			"deploy_mode": "docker",
			"preserve": ".env\\nuploads"
		}
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/projects/import", strings.NewReader(body))
	rr := httptest.NewRecorder()

	handleAPIProjectImport(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", rr.Code, rr.Body.String())
	}
	var created Project
	if err := json.NewDecoder(rr.Body).Decode(&created); err != nil {
		t.Fatalf("decode created project: %v", err)
	}
	if created.ID == 0 || created.RunnerID != 0 || created.PostDeploy != "" {
		t.Fatalf("expected imported project without runtime fields, got %+v", created)
	}
	if created.BuildArgs["APP_ENV"] != "staging" {
		t.Fatalf("expected safe build args to import, got %#v", created.BuildArgs)
	}
	stored, err := getProject(created.ID)
	if err != nil {
		t.Fatalf("get imported project: %v", err)
	}
	if stored.Name != "Imported Dashboard" || stored.RunnerID != 0 || stored.PostDeploy != "" {
		t.Fatalf("unexpected stored imported project: %+v", stored)
	}
}

func TestProjectImportRejectsUnsupportedEnvelope(t *testing.T) {
	withTempDB(t)

	req := httptest.NewRequest(http.MethodPost, "/api/projects/import", strings.NewReader(`{
		"kind": "deployer.project",
		"version": 99,
		"project": {"name": "Bad"}
	}`))
	rr := httptest.NewRecorder()

	handleAPIProjectImport(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestProjectImportRouteRejectsWrongMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/projects/import", nil)
	rr := httptest.NewRecorder()

	handleAPIProject(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected method not allowed, got %d body=%s", rr.Code, rr.Body.String())
	}
}
