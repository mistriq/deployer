package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildReleaseNotesUsesGitCommitRange(t *testing.T) {
	withTempDB(t)
	repoPath, commits := createReleaseNotesRepo(t)
	project := createReleaseNotesProject(t, repoPath)
	previous := createReleaseNotesBuild(t, project.ID, commits[0])
	current := createReleaseNotesBuild(t, project.ID, commits[2])

	notes, err := newBuildReleaseNotes(current.ID)
	if err != nil {
		t.Fatalf("build release notes: %v", err)
	}
	if notes.BuildID != current.ID || notes.ProjectID != project.ID || notes.ProjectName != project.Name {
		t.Fatalf("unexpected release note scope: %+v", notes)
	}
	if notes.PreviousBuildID == nil || *notes.PreviousBuildID != previous.ID {
		t.Fatalf("expected previous build %d, got %+v", previous.ID, notes.PreviousBuildID)
	}
	if notes.PreviousCommit != commits[0] || notes.CurrentCommit != commits[2] {
		t.Fatalf("unexpected commit range: %+v", notes)
	}
	if len(notes.Commits) != 2 {
		t.Fatalf("expected 2 commits in range, got %+v", notes.Commits)
	}
	if notes.Commits[0].Subject != "add worker" || notes.Commits[1].Subject != "update health check" {
		t.Fatalf("unexpected commit subjects: %+v", notes.Commits)
	}
	if notes.Commits[0].Hash == "" || notes.Commits[0].ShortHash == "" || notes.Commits[0].AuthorName == "" || notes.Commits[0].AuthorEmail == "" || notes.Commits[0].AuthoredAt.IsZero() {
		t.Fatalf("expected commit metadata to be populated: %+v", notes.Commits[0])
	}
}

func TestBuildReleaseNotesFallsBackToSingleCommitWithoutPreviousBuild(t *testing.T) {
	withTempDB(t)
	repoPath, commits := createReleaseNotesRepo(t)
	project := createReleaseNotesProject(t, repoPath)
	current := createReleaseNotesBuild(t, project.ID, commits[1])

	notes, err := newBuildReleaseNotes(current.ID)
	if err != nil {
		t.Fatalf("build release notes: %v", err)
	}
	if notes.PreviousBuildID != nil || len(notes.Warnings) == 0 {
		t.Fatalf("expected no previous build warning, got %+v", notes)
	}
	if len(notes.Commits) != 1 || notes.Commits[0].Subject != "add worker" {
		t.Fatalf("expected current commit metadata, got %+v", notes.Commits)
	}
}

func TestBuildReleaseNotesWarnsWhenCurrentCommitMissing(t *testing.T) {
	withTempDB(t)
	project := createReleaseNotesProject(t, t.TempDir())
	current := createReleaseNotesBuild(t, project.ID, "")

	notes, err := newBuildReleaseNotes(current.ID)
	if err != nil {
		t.Fatalf("build release notes: %v", err)
	}
	if len(notes.Commits) != 0 || len(notes.Warnings) != 1 || !strings.Contains(notes.Warnings[0], "no commit SHA") {
		t.Fatalf("expected missing SHA warning, got %+v", notes)
	}
}

func TestBuildReleaseNotesAPIHandler(t *testing.T) {
	withTempDB(t)
	repoPath, commits := createReleaseNotesRepo(t)
	project := createReleaseNotesProject(t, repoPath)
	build := createReleaseNotesBuild(t, project.ID, commits[0])

	req := httptest.NewRequest(http.MethodGet, "/api/builds/1/release-notes", nil)
	rr := httptest.NewRecorder()
	handleAPIBuild(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("release notes status = %d body=%s", rr.Code, rr.Body.String())
	}
	var got buildReleaseNotesResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode release notes: %v", err)
	}
	if got.BuildID != build.ID || len(got.Commits) != 1 || got.Commits[0].Subject != "initial commit" {
		t.Fatalf("unexpected release notes response: %+v", got)
	}
}

func TestParseReleaseNoteCommitsRejectsMalformedLines(t *testing.T) {
	if _, err := parseReleaseNoteCommits("only\x1ftwo"); err == nil {
		t.Fatal("expected malformed git log line to fail")
	}
}

func createReleaseNotesRepo(t *testing.T) (string, []string) {
	t.Helper()
	repoPath := t.TempDir()
	runGit(t, repoPath, "init")
	runGit(t, repoPath, "config", "user.name", "Release Bot")
	runGit(t, repoPath, "config", "user.email", "release@example.com")

	commits := []string{}
	commitFile := func(name, body, message string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(repoPath, name), []byte(body), 0644); err != nil {
			t.Fatalf("write git file: %v", err)
		}
		runGit(t, repoPath, "add", name)
		runGit(t, repoPath, "commit", "-m", message)
		commits = append(commits, strings.TrimSpace(runGit(t, repoPath, "rev-parse", "--short", "HEAD")))
	}
	commitFile("README.md", "initial\n", "initial commit")
	commitFile("worker.txt", "worker\n", "add worker")
	commitFile("health.txt", "health\n", "update health check")
	return repoPath, commits
}

func createReleaseNotesProject(t *testing.T, repoPath string) *Project {
	t.Helper()
	project := &Project{
		Name:       "Release Notes App",
		RepoPath:   repoPath,
		DeployDir:  "/srv/apps/release-notes",
		ImageName:  "example/release-notes",
		DeployMode: "docker",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	return project
}

func createReleaseNotesBuild(t *testing.T, projectID int64, commitSHA string) *Build {
	t.Helper()
	build, err := createBuild(projectID, "manual")
	if err != nil {
		t.Fatalf("create build: %v", err)
	}
	duration := 12
	finished := time.Now().UTC()
	build.Status = "success"
	build.CommitSHA = commitSHA
	build.FinishedAt = &finished
	build.DurationSeconds = &duration
	if err := updateBuild(build); err != nil {
		t.Fatalf("update build: %v", err)
	}
	return build
}
