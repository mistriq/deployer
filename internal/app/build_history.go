package app

import (
	"database/sql"
	"errors"
	"net/http"
	"time"
)

const buildHistoryLimit = 30

type buildHistorySummaryResponse struct {
	Scope                  string              `json:"scope"`
	ScopeID                int64               `json:"scope_id"`
	ScopeName              string              `json:"scope_name"`
	Points                 []buildHistoryPoint `json:"points"`
	Counts                 buildHistoryCounts  `json:"counts"`
	AverageDurationSeconds *int                `json:"average_duration_seconds,omitempty"`
	MaxDurationSeconds     int                 `json:"max_duration_seconds"`
}

type buildHistoryPoint struct {
	BuildID         int64      `json:"build_id"`
	ProjectID       int64      `json:"project_id"`
	ProjectName     string     `json:"project_name,omitempty"`
	Status          string     `json:"status"`
	CommitSHA       string     `json:"commit_sha,omitempty"`
	StartedAt       time.Time  `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
	DurationSeconds *int       `json:"duration_seconds,omitempty"`
	TriggeredBy     string     `json:"triggered_by"`
	HeightPercent   int        `json:"height_percent"`
}

type buildHistoryCounts struct {
	Total     int `json:"total"`
	Success   int `json:"success"`
	Failed    int `json:"failed"`
	Cancelled int `json:"cancelled"`
	Running   int `json:"running"`
}

type runnerPageRow struct {
	Runner  Runner
	History *buildHistorySummaryResponse
}

func handleAPIProjectHistory(w http.ResponseWriter, r *http.Request, projectID int64) {
	history, err := newProjectBuildHistory(projectID, buildHistoryLimit)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			jsonErrorCode(w, errCodeProjectNotFound, "project not found", http.StatusNotFound)
			return
		}
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, history)
}

func handleAPIRunnerHistory(w http.ResponseWriter, r *http.Request, runnerID int64) {
	history, err := newRunnerBuildHistory(runnerID, buildHistoryLimit)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			jsonErrorCode(w, errCodeRunnerNotFound, "runner not found", http.StatusNotFound)
			return
		}
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, history)
}

func newProjectBuildHistory(projectID int64, limit int) (*buildHistorySummaryResponse, error) {
	project, err := getProject(projectID)
	if err != nil {
		return nil, err
	}
	builds, err := listBuilds(projectID, limit)
	if err != nil {
		return nil, err
	}
	points := make([]buildHistoryPoint, 0, len(builds))
	for _, build := range builds {
		points = append(points, buildHistoryPointFromBuild(build, project.Name))
	}
	return newBuildHistorySummary("project", project.ID, project.Name, points), nil
}

func newRunnerBuildHistory(runnerID int64, limit int) (*buildHistorySummaryResponse, error) {
	runner, err := getRunner(runnerID)
	if err != nil {
		return nil, err
	}
	points, err := listRunnerBuildHistoryPoints(runnerID, limit)
	if err != nil {
		return nil, err
	}
	return newBuildHistorySummary("runner", runner.ID, runner.Name, points), nil
}

func listRunnerBuildHistoryPoints(runnerID int64, limit int) ([]buildHistoryPoint, error) {
	if limit <= 0 {
		limit = buildHistoryLimit
	}
	rows, err := db.Query(`
		SELECT b.id, b.project_id, p.name, b.status, b.commit_sha, b.started_at,
		       b.finished_at, b.duration_seconds, b.triggered_by
		FROM jobs j
		JOIN builds b ON b.id = j.build_id
		JOIN projects p ON p.id = b.project_id
		WHERE j.runner_id=?
		ORDER BY datetime(b.started_at) DESC, b.id DESC
		LIMIT ?`, runnerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	points := []buildHistoryPoint{}
	for rows.Next() {
		var point buildHistoryPoint
		var startedAt string
		var finishedAt sql.NullString
		var duration sql.NullInt64
		if err := rows.Scan(&point.BuildID, &point.ProjectID, &point.ProjectName, &point.Status, &point.CommitSHA, &startedAt, &finishedAt, &duration, &point.TriggeredBy); err != nil {
			return nil, err
		}
		point.StartedAt = parseSQLiteTime(startedAt)
		if finishedAt.Valid {
			t := parseSQLiteTime(finishedAt.String)
			point.FinishedAt = &t
		}
		if duration.Valid {
			d := int(duration.Int64)
			point.DurationSeconds = &d
		}
		points = append(points, point)
	}
	return points, rows.Err()
}

func buildHistoryPointFromBuild(build Build, projectName string) buildHistoryPoint {
	return buildHistoryPoint{
		BuildID:         build.ID,
		ProjectID:       build.ProjectID,
		ProjectName:     projectName,
		Status:          build.Status,
		CommitSHA:       build.CommitSHA,
		StartedAt:       build.StartedAt,
		FinishedAt:      build.FinishedAt,
		DurationSeconds: build.DurationSeconds,
		TriggeredBy:     build.TriggeredBy,
	}
}

func newBuildHistorySummary(scope string, scopeID int64, scopeName string, points []buildHistoryPoint) *buildHistorySummaryResponse {
	counts := buildHistoryCounts{Total: len(points)}
	maxDuration := 0
	totalDuration := 0
	durationCount := 0

	for i := range points {
		switch points[i].Status {
		case "success":
			counts.Success++
		case "failed":
			counts.Failed++
		case "cancelled":
			counts.Cancelled++
		case "running":
			counts.Running++
		}
		if points[i].DurationSeconds != nil {
			duration := *points[i].DurationSeconds
			totalDuration += duration
			durationCount++
			if duration > maxDuration {
				maxDuration = duration
			}
		}
	}

	for i := range points {
		points[i].HeightPercent = buildHistoryHeight(points[i].DurationSeconds, maxDuration)
	}

	var average *int
	if durationCount > 0 {
		value := totalDuration / durationCount
		average = &value
	}

	return &buildHistorySummaryResponse{
		Scope:                  scope,
		ScopeID:                scopeID,
		ScopeName:              scopeName,
		Points:                 points,
		Counts:                 counts,
		AverageDurationSeconds: average,
		MaxDurationSeconds:     maxDuration,
	}
}

func buildHistoryHeight(duration *int, maxDuration int) int {
	if duration == nil || maxDuration <= 0 {
		return 12
	}
	height := (*duration * 100) / maxDuration
	if height < 12 {
		return 12
	}
	if height > 100 {
		return 100
	}
	return height
}
