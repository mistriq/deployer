package app

import (
	"database/sql"
	"errors"
	"net/http"
	"time"
)

type projectSummaryResponse struct {
	Project            Project        `json:"project"`
	Runner             *runnerSummary `json:"runner,omitempty"`
	LastBuild          *buildSummary  `json:"last_build,omitempty"`
	RecentFailures     []buildSummary `json:"recent_failures"`
	RecommendedActions []string       `json:"recommended_actions"`
}

type runnerSummary struct {
	ID        int64      `json:"id"`
	Name      string     `json:"name"`
	Labels    string     `json:"labels"`
	Status    string     `json:"status"`
	LastSeen  *time.Time `json:"last_seen,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

type buildSummary struct {
	ID              int64      `json:"id"`
	ProjectID       int64      `json:"project_id"`
	Status          string     `json:"status"`
	CommitSHA       string     `json:"commit_sha"`
	StartedAt       time.Time  `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
	DurationSeconds *int       `json:"duration_seconds,omitempty"`
	ErrorMessage    string     `json:"error_message,omitempty"`
	ErrorCode       string     `json:"error_code,omitempty"`
	TriggeredBy     string     `json:"triggered_by"`
}

func handleAPIProjectSummary(w http.ResponseWriter, r *http.Request, projectID int64) {
	summary, err := newProjectSummary(projectID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			jsonErrorCode(w, errCodeProjectNotFound, "project not found", http.StatusNotFound)
			return
		}
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, summary)
}

func newProjectSummary(projectID int64) (*projectSummaryResponse, error) {
	project, err := getProject(projectID)
	if err != nil {
		return nil, err
	}
	project.LastBuild = nil

	builds, err := listBuilds(projectID, 20)
	if err != nil {
		return nil, err
	}

	summary := &projectSummaryResponse{
		Project:        *project,
		RecentFailures: []buildSummary{},
	}

	if project.RunnerID > 0 {
		runner, err := getRunner(project.RunnerID)
		if err == nil {
			runner.Token = ""
			summary.Runner = summarizeRunner(*runner)
		} else if errors.Is(err, sql.ErrNoRows) {
			summary.RecommendedActions = append(summary.RecommendedActions, "Runner is missing; assign an existing runner before deploying.")
		} else {
			return nil, err
		}
	}

	if len(builds) > 0 {
		last := summarizeBuild(builds[0])
		summary.LastBuild = &last
		for _, build := range builds {
			if build.Status == "failed" || build.Status == "cancelled" {
				summary.RecentFailures = append(summary.RecentFailures, summarizeBuild(build))
			}
			if len(summary.RecentFailures) == 5 {
				break
			}
		}
	}

	summary.RecommendedActions = append(summary.RecommendedActions, projectSummaryRecommendations(project, summary.Runner, summary.LastBuild)...)
	return summary, nil
}

func summarizeRunner(runner Runner) *runnerSummary {
	return &runnerSummary{
		ID:        runner.ID,
		Name:      runner.Name,
		Labels:    runner.Labels,
		Status:    runner.Status,
		LastSeen:  runner.LastSeen,
		CreatedAt: runner.CreatedAt,
	}
}

func summarizeBuild(build Build) buildSummary {
	return buildSummary{
		ID:              build.ID,
		ProjectID:       build.ProjectID,
		Status:          build.Status,
		CommitSHA:       build.CommitSHA,
		StartedAt:       build.StartedAt,
		FinishedAt:      build.FinishedAt,
		DurationSeconds: build.DurationSeconds,
		ErrorMessage:    build.ErrorMessage,
		ErrorCode:       build.ErrorCode,
		TriggeredBy:     build.TriggeredBy,
	}
}

func projectSummaryRecommendations(project *Project, runner *runnerSummary, lastBuild *buildSummary) []string {
	var actions []string
	if runner != nil && runner.Status == "offline" {
		actions = append(actions, "Runner is offline; check the agent service and runner token before deploying.")
	}
	if lastBuild == nil {
		actions = append(actions, "No builds have run yet; trigger a deploy when the project configuration is ready.")
	} else {
		switch lastBuild.Status {
		case "running":
			actions = append(actions, "A build is running; watch its SSE log stream before starting another deployment.")
		case "failed":
			actions = append(actions, "Last build failed; inspect the build log and error message before redeploying.")
		case "cancelled":
			actions = append(actions, "Last build was cancelled; verify whether the cancellation was intentional before redeploying.")
		}
	}
	if project.HealthURL == "" && project.HealthContainer == "" {
		actions = append(actions, "No health check is configured; add one to make deploy failures easier to detect.")
	}
	return actions
}
