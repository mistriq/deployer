package app

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"
)

type buildFailureSummaryResponse struct {
	BuildID          int64    `json:"build_id"`
	ProjectID        int64    `json:"project_id"`
	Status           string   `json:"status"`
	HasFailure       bool     `json:"has_failure"`
	FailedStep       string   `json:"failed_step,omitempty"`
	LikelyCause      string   `json:"likely_cause,omitempty"`
	RelevantLogLines []string `json:"relevant_log_lines"`
	SuggestedFix     string   `json:"suggested_fix,omitempty"`
	ErrorMessage     string   `json:"error_message,omitempty"`
	ErrorCode        string   `json:"error_code,omitempty"`
}

func handleAPIBuildFailureSummary(w http.ResponseWriter, r *http.Request, buildID int64) {
	summary, err := newBuildFailureSummary(buildID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			jsonErrorCode(w, errCodeBuildNotFound, "build not found", http.StatusNotFound)
			return
		}
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, summary)
}

func newBuildFailureSummary(buildID int64) (*buildFailureSummaryResponse, error) {
	build, err := getBuild(buildID)
	if err != nil {
		return nil, err
	}
	summary := &buildFailureSummaryResponse{
		BuildID:          build.ID,
		ProjectID:        build.ProjectID,
		Status:           build.Status,
		HasFailure:       build.Status == "failed",
		ErrorMessage:     build.ErrorMessage,
		ErrorCode:        build.ErrorCode,
		RelevantLogLines: []string{},
	}
	if !summary.HasFailure {
		return summary, nil
	}

	events := parseStructuredBuildEvents(build)
	for _, event := range events {
		if event.Type == "step_failed" {
			summary.FailedStep = event.Step
			summary.LikelyCause = event.Error
			break
		}
	}
	if summary.FailedStep == "" || summary.LikelyCause == "" {
		step, cause := splitBuildError(build.ErrorMessage)
		if summary.FailedStep == "" {
			summary.FailedStep = step
		}
		if summary.LikelyCause == "" {
			summary.LikelyCause = cause
		}
	}
	summary.RelevantLogLines = relevantFailureLines(build.Log)
	summary.SuggestedFix = suggestedFailureFix(summary.FailedStep, summary.LikelyCause, summary.ErrorMessage)
	return summary, nil
}

func splitBuildError(message string) (string, string) {
	parts := strings.SplitN(message, ":", 2)
	if len(parts) != 2 {
		return "", strings.TrimSpace(message)
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
}

func relevantFailureLines(logText string) []string {
	lines := nonEmptyLogLines(logText)
	if len(lines) == 0 {
		return []string{}
	}
	failureIndex := -1
	for i, line := range lines {
		if strings.Contains(strings.ToLower(line), "failed") || strings.Contains(strings.ToLower(line), "error") {
			failureIndex = i
		}
	}
	if failureIndex == -1 {
		start := len(lines) - 5
		if start < 0 {
			start = 0
		}
		return lines[start:]
	}
	start := failureIndex - 3
	if start < 0 {
		start = 0
	}
	end := failureIndex + 2
	if end > len(lines) {
		end = len(lines)
	}
	return lines[start:end]
}

func nonEmptyLogLines(logText string) []string {
	var lines []string
	for _, line := range strings.Split(logText, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "##[group]") && !strings.HasPrefix(line, "##[endgroup:") {
			lines = append(lines, line)
		}
	}
	return lines
}

func suggestedFailureFix(step, cause, message string) string {
	text := strings.ToLower(strings.Join([]string{step, cause, message}, " "))
	switch {
	case strings.Contains(text, "health"):
		return "Check the configured health endpoint or container health status, then review the service logs on the target host."
	case strings.Contains(text, "permission denied"):
		return "Check deploy directory ownership, file permissions, and the configured permissions rules."
	case strings.Contains(text, "no such file") || strings.Contains(text, "not found"):
		return "Check configured paths, expected files, and whether the repository or artifact contains the required target."
	case strings.Contains(text, "docker"):
		return "Inspect Docker build or Compose output, then verify the Dockerfile, image name, and compose services."
	case strings.Contains(text, "git"):
		return "Check repository path, branch state, credentials, and whether the working tree can run the planned Git command."
	default:
		return "Inspect the relevant log lines and rerun the deploy after correcting the failing step."
	}
}
