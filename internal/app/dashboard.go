package app

import "database/sql"

// Fetch bounded build metadata for every project in one round trip. Logs belong
// on the build detail endpoint, never on a project list or a deploy chart.
func listRecentProjectBuilds(limit int) (map[int64][]Build, error) {
	if limit < 1 {
		limit = 1
	}
	query := `SELECT id, project_id, status, commit_sha, started_at, finished_at,
		duration_seconds, error_message, error_code, triggered_by FROM (
		SELECT id, project_id, status, commit_sha, started_at, finished_at,
		       duration_seconds, error_message, error_code, triggered_by,
		       ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY id DESC) AS position
		FROM builds
	) recent WHERE position <= ? ORDER BY project_id, id DESC`
	if db.dialect == "postgres" {
		// Each indexed lookup stops at LIMIT instead of ranking the full history.
		query = `SELECT recent.id, recent.project_id, recent.status, recent.commit_sha,
			recent.started_at, recent.finished_at, recent.duration_seconds,
			recent.error_message, recent.error_code, recent.triggered_by
			FROM projects p CROSS JOIN LATERAL (
				SELECT id, project_id, status, commit_sha, started_at, finished_at,
				       duration_seconds, error_message, error_code, triggered_by
				FROM builds WHERE project_id = p.id ORDER BY id DESC LIMIT ?
			) recent ORDER BY recent.project_id, recent.id DESC`
	}
	rows, err := db.Query(query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byProject := make(map[int64][]Build)
	for rows.Next() {
		var build Build
		var startedAt string
		var finishedAt sql.NullString
		var duration sql.NullInt64
		if err := rows.Scan(&build.ID, &build.ProjectID, &build.Status, &build.CommitSHA,
			&startedAt, &finishedAt, &duration, &build.ErrorMessage, &build.ErrorCode, &build.TriggeredBy); err != nil {
			return nil, err
		}
		build.StartedAt = parseSQLiteTime(startedAt)
		if finishedAt.Valid {
			finished := parseSQLiteTime(finishedAt.String)
			build.FinishedAt = &finished
		}
		if duration.Valid {
			seconds := int(duration.Int64)
			build.DurationSeconds = &seconds
		}
		byProject[build.ProjectID] = append(byProject[build.ProjectID], build)
	}
	return byProject, rows.Err()
}
