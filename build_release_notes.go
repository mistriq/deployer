package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const releaseNotesGitTimeout = 5 * time.Second

type buildReleaseNotesResponse struct {
	BuildID         int64               `json:"build_id"`
	ProjectID       int64               `json:"project_id"`
	ProjectName     string              `json:"project_name"`
	PreviousBuildID *int64              `json:"previous_build_id,omitempty"`
	PreviousCommit  string              `json:"previous_commit,omitempty"`
	CurrentCommit   string              `json:"current_commit,omitempty"`
	CompareRange    string              `json:"compare_range,omitempty"`
	Commits         []releaseNoteCommit `json:"commits"`
	Warnings        []string            `json:"warnings,omitempty"`
}

type releaseNoteCommit struct {
	Hash        string    `json:"hash"`
	ShortHash   string    `json:"short_hash"`
	AuthorName  string    `json:"author_name"`
	AuthorEmail string    `json:"author_email"`
	AuthoredAt  time.Time `json:"authored_at"`
	Subject     string    `json:"subject"`
}

func handleAPIBuildReleaseNotes(w http.ResponseWriter, r *http.Request, buildID int64) {
	notes, err := newBuildReleaseNotes(buildID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			jsonErrorCode(w, errCodeBuildNotFound, "build not found", http.StatusNotFound)
			return
		}
		jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
		return
	}
	jsonResponse(w, notes)
}

func newBuildReleaseNotes(buildID int64) (*buildReleaseNotesResponse, error) {
	build, err := getBuild(buildID)
	if err != nil {
		return nil, err
	}
	project, err := getProject(build.ProjectID)
	if err != nil {
		return nil, err
	}

	notes := &buildReleaseNotesResponse{
		BuildID:       build.ID,
		ProjectID:     build.ProjectID,
		ProjectName:   build.ProjectName,
		CurrentCommit: build.CommitSHA,
		Commits:       []releaseNoteCommit{},
	}

	if strings.TrimSpace(build.CommitSHA) == "" {
		notes.Warnings = append(notes.Warnings, "Current build has no commit SHA.")
		return notes, nil
	}

	previous, err := previousBuildWithCommit(build)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		notes.Warnings = append(notes.Warnings, "No previous build with a commit SHA was found for this project.")
		commits, gitErr := gitSingleCommit(project.RepoPath, build.CommitSHA)
		if gitErr != nil {
			return nil, gitErr
		}
		notes.Commits = commits
		return notes, nil
	}

	notes.PreviousBuildID = &previous.ID
	notes.PreviousCommit = previous.CommitSHA
	notes.CompareRange = previous.CommitSHA + ".." + build.CommitSHA
	if previous.CommitSHA == build.CommitSHA {
		notes.Warnings = append(notes.Warnings, "Previous and current builds point at the same commit.")
		return notes, nil
	}

	commits, err := gitCommitsBetween(project.RepoPath, previous.CommitSHA, build.CommitSHA)
	if err != nil {
		return nil, err
	}
	notes.Commits = commits
	if len(commits) == 0 {
		notes.Warnings = append(notes.Warnings, "No commits were found in the selected range.")
	}
	return notes, nil
}

func previousBuildWithCommit(build *Build) (*Build, error) {
	previous := &Build{}
	var startedAt string
	var finishedAt sql.NullString
	var durSeconds sql.NullInt64
	err := db.QueryRow(`SELECT id, project_id, status, commit_sha, started_at, finished_at, duration_seconds, log, error_message, error_code, triggered_by FROM builds WHERE project_id=? AND id < ? AND commit_sha <> '' ORDER BY id DESC LIMIT 1`, build.ProjectID, build.ID).Scan(
		&previous.ID, &previous.ProjectID, &previous.Status, &previous.CommitSHA, &startedAt, &finishedAt, &durSeconds, &previous.Log, &previous.ErrorMessage, &previous.ErrorCode, &previous.TriggeredBy,
	)
	if err != nil {
		return nil, err
	}
	previous.StartedAt = parseSQLiteTime(startedAt)
	if finishedAt.Valid {
		t := parseSQLiteTime(finishedAt.String)
		previous.FinishedAt = &t
	}
	if durSeconds.Valid {
		d := int(durSeconds.Int64)
		previous.DurationSeconds = &d
	}
	return previous, nil
}

func gitCommitsBetween(repoPath, previousCommit, currentCommit string) ([]releaseNoteCommit, error) {
	return gitCommitLog(repoPath, true, previousCommit+".."+currentCommit)
}

func gitSingleCommit(repoPath, commit string) ([]releaseNoteCommit, error) {
	return gitCommitLog(repoPath, false, "-1", commit)
}

func gitCommitLog(repoPath string, reverse bool, revArgs ...string) ([]releaseNoteCommit, error) {
	ctx, cancel := context.WithTimeout(context.Background(), releaseNotesGitTimeout)
	defer cancel()

	args := []string{"-C", repoPath, "log"}
	if reverse {
		args = append(args, "--reverse")
	}
	args = append(args, "--date=iso-strict", "--format=%H%x1f%h%x1f%an%x1f%ae%x1f%aI%x1f%s")
	args = append(args, revArgs...)
	out, err := systemCommands.CombinedOutput(ctx, "", "git", args...)
	if err != nil {
		output := strings.TrimSpace(string(out))
		if output != "" {
			return nil, fmt.Errorf("git log: %w: %s", err, output)
		}
		return nil, fmt.Errorf("git log: %w", err)
	}
	return parseReleaseNoteCommits(string(out))
}

func parseReleaseNoteCommits(output string) ([]releaseNoteCommit, error) {
	output = strings.TrimSpace(output)
	if output == "" {
		return []releaseNoteCommit{}, nil
	}
	commits := []releaseNoteCommit{}
	for _, line := range strings.Split(output, "\n") {
		parts := strings.SplitN(line, "\x1f", 6)
		if len(parts) != 6 {
			return nil, fmt.Errorf("parse git log: expected 6 fields, got %d", len(parts))
		}
		authoredAt, err := time.Parse(time.RFC3339, parts[4])
		if err != nil {
			return nil, fmt.Errorf("parse git authored date: %w", err)
		}
		commits = append(commits, releaseNoteCommit{
			Hash:        parts[0],
			ShortHash:   parts[1],
			AuthorName:  parts[2],
			AuthorEmail: parts[3],
			AuthoredAt:  authoredAt,
			Subject:     parts[5],
		})
	}
	return commits, nil
}
