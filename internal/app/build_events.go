package app

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

var failedStepPattern = regexp.MustCompile(`^FAILED at step '([^']+)':\s*(.*)$`)

type buildEventsResponse struct {
	BuildID           int64             `json:"build_id"`
	ProjectID         int64             `json:"project_id"`
	Status            string            `json:"status"`
	ErrorCode         string            `json:"error_code,omitempty"`
	TriggeredBy       string            `json:"triggered_by"`
	CommitSHA         string            `json:"commit_sha,omitempty"`
	JobID             *int64            `json:"job_id,omitempty"`
	RunnerID          *int64            `json:"runner_id,omitempty"`
	JobErrorCode      string            `json:"job_error_code,omitempty"`
	ArtifactID        string            `json:"artifact_id,omitempty"`
	ArtifactAvailable bool              `json:"artifact_available"`
	Events            []structuredEvent `json:"events"`
}

type structuredEvent struct {
	Sequence        int    `json:"sequence"`
	Type            string `json:"type"`
	Step            string `json:"step,omitempty"`
	Status          string `json:"status,omitempty"`
	DurationSeconds *int   `json:"duration_seconds,omitempty"`
	Message         string `json:"message,omitempty"`
	Error           string `json:"error,omitempty"`
	ErrorCode       string `json:"error_code,omitempty"`
}

func handleAPIBuildEvents(w http.ResponseWriter, r *http.Request, buildID int64) {
	events, err := newBuildEventsResponse(buildID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			jsonErrorCode(w, errCodeBuildNotFound, "build not found", http.StatusNotFound)
			return
		}
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, events)
}

func newBuildEventsResponse(buildID int64) (*buildEventsResponse, error) {
	build, err := getBuild(buildID)
	if err != nil {
		return nil, err
	}
	response := &buildEventsResponse{
		BuildID:     build.ID,
		ProjectID:   build.ProjectID,
		Status:      build.Status,
		ErrorCode:   build.ErrorCode,
		TriggeredBy: build.TriggeredBy,
		CommitSHA:   build.CommitSHA,
		Events:      parseStructuredBuildEvents(build),
	}
	if job, err := getJobByBuildID(buildID); err == nil {
		response.JobID = int64Ptr(job.ID)
		response.RunnerID = int64Ptr(job.RunnerID)
		response.JobErrorCode = job.ErrorCode
		if job.ArtifactPath != "" {
			response.ArtifactID = fmt.Sprintf("job-%d-artifact", job.ID)
			response.ArtifactAvailable = true
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	return response, nil
}

func parseStructuredBuildEvents(build *Build) []structuredEvent {
	var events []structuredEvent
	currentStep := ""
	for _, line := range strings.Split(build.Log, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}
		switch {
		case strings.HasPrefix(line, "##[group]"):
			currentStep = strings.TrimSpace(strings.TrimPrefix(line, "##[group]"))
			events = append(events, structuredEvent{
				Sequence: len(events) + 1,
				Type:     "step_started",
				Step:     currentStep,
				Status:   "running",
			})
		case strings.HasPrefix(line, "##[endgroup:") && strings.HasSuffix(line, "]"):
			duration, ok := parseEndGroupDuration(line)
			event := structuredEvent{
				Sequence: len(events) + 1,
				Type:     "step_finished",
				Step:     currentStep,
				Status:   "completed",
			}
			if ok {
				event.DurationSeconds = &duration
			}
			events = append(events, event)
			currentStep = ""
		case failedStepPattern.MatchString(line):
			matches := failedStepPattern.FindStringSubmatch(line)
			events = append(events, structuredEvent{
				Sequence:  len(events) + 1,
				Type:      "step_failed",
				Step:      matches[1],
				Status:    "failed",
				Error:     matches[2],
				ErrorCode: classifyRuntimeError(matches[1] + ": " + matches[2]),
			})
		default:
			events = append(events, structuredEvent{
				Sequence: len(events) + 1,
				Type:     "log",
				Step:     currentStep,
				Message:  line,
			})
		}
	}
	if build.Status != "running" {
		events = append(events, structuredEvent{
			Sequence:  len(events) + 1,
			Type:      "build_finished",
			Status:    build.Status,
			Error:     build.ErrorMessage,
			ErrorCode: build.ErrorCode,
		})
	}
	return events
}

func parseEndGroupDuration(line string) (int, bool) {
	value := strings.TrimSuffix(strings.TrimPrefix(line, "##[endgroup:"), "]")
	duration, err := strconv.Atoi(value)
	return duration, err == nil
}

func int64Ptr(value int64) *int64 {
	return &value
}
