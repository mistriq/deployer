package main

import (
	"strings"
	"testing"
)

func TestSeedDemoDataIfEnabledSeedsEmptyDatabase(t *testing.T) {
	withTempDB(t)

	if err := seedDemoDataIfEnabled(AppConfig{DemoMode: true}); err != nil {
		t.Fatalf("seed demo data: %v", err)
	}

	projects, err := listProjects()
	if err != nil {
		t.Fatalf("list projects: %v", err)
	}
	if len(projects) != 2 {
		t.Fatalf("expected 2 demo projects, got %d", len(projects))
	}
	for _, project := range projects {
		if !strings.Contains(project.RepoPath, "/srv/demo/") && !strings.Contains(project.DeployDir, "/var/www/demo") {
			t.Fatalf("expected placeholder demo paths, got %+v", project)
		}
		if strings.Contains(project.RepoPath, "/home/") || strings.Contains(project.DeployDir, "/home/") {
			t.Fatalf("demo project contains personal-looking path: %+v", project)
		}
	}

	runners, err := listRunners()
	if err != nil {
		t.Fatalf("list runners: %v", err)
	}
	if len(runners) != 2 {
		t.Fatalf("expected 2 demo runners, got %d", len(runners))
	}
	for _, runner := range runners {
		if runner.Token == "demo-runner-token" || runner.Token == "demo-runner-token-2" || !isHashedToken(runner.Token) {
			t.Fatalf("expected stored demo runner token to be hashed, got %+v", runner)
		}
	}

	demoAPI := projectByName(t, projects, "Demo API")
	builds, err := listBuilds(demoAPI.ID, 10)
	if err != nil {
		t.Fatalf("list builds: %v", err)
	}
	if len(builds) != 2 {
		t.Fatalf("expected 2 demo API builds, got %d", len(builds))
	}
	if builds[0].Status == "" || builds[0].CommitSHA == "" {
		t.Fatalf("expected seeded builds to include status and commit SHA, got %+v", builds[0])
	}
	annotations, err := listBuildAnnotations(builds[0].ID)
	if err != nil {
		t.Fatalf("list annotations: %v", err)
	}
	if len(annotations) == 0 || annotations[0].Note != "Demo data for public screenshots" {
		t.Fatalf("expected demo annotation, got %+v", annotations)
	}
}

func TestSeedDemoDataIfEnabledSkipsNonEmptyDatabase(t *testing.T) {
	withTempDB(t)

	project := &Project{
		Name:       "Existing Project",
		RepoPath:   "/srv/existing/repo",
		DeployDir:  "/srv/existing/app",
		ImageName:  "example/existing",
		DeployMode: "docker",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create existing project: %v", err)
	}

	if err := seedDemoDataIfEnabled(AppConfig{DemoMode: true}); err != nil {
		t.Fatalf("seed demo data: %v", err)
	}
	projects, err := listProjects()
	if err != nil {
		t.Fatalf("list projects: %v", err)
	}
	if len(projects) != 1 || projects[0].Name != "Existing Project" {
		t.Fatalf("expected existing data to be left alone, got %+v", projects)
	}
}

func TestSeedDemoDataIfDisabledDoesNothing(t *testing.T) {
	withTempDB(t)

	if err := seedDemoDataIfEnabled(AppConfig{}); err != nil {
		t.Fatalf("seed demo data disabled: %v", err)
	}
	projects, err := listProjects()
	if err != nil {
		t.Fatalf("list projects: %v", err)
	}
	if len(projects) != 0 {
		t.Fatalf("expected no seeded projects, got %+v", projects)
	}
}

func projectByName(t *testing.T, projects []Project, name string) Project {
	t.Helper()
	for _, project := range projects {
		if project.Name == name {
			return project
		}
	}
	t.Fatalf("project %q not found in %+v", name, projects)
	return Project{}
}
