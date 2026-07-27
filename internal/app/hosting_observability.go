package app

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"time"
)

const (
	maxHostingLogBytesPerDeployment = 4 << 20
	maxHostingEventsPerDeployment   = 10_000
)

type hostingMetricsResponse struct {
	ObservedAt time.Time `json:"observed_at"`
	Window     string    `json:"window"`
	Queue      struct {
		QueuedJobs       int     `json:"queued_jobs"`
		OldestAgeSeconds float64 `json:"oldest_age_seconds"`
	} `json:"queue"`
	Deployments struct {
		Terminal               int            `json:"terminal"`
		Successful             int            `json:"successful"`
		Failed                 int            `json:"failed"`
		Cancelled              int            `json:"cancelled"`
		SuccessRate            float64        `json:"success_rate"`
		AverageDurationSeconds float64        `json:"average_duration_seconds"`
		FailureCodes           map[string]int `json:"failure_codes"`
	} `json:"deployments"`
	Runners struct {
		Online   int         `json:"online"`
		Offline  int         `json:"offline"`
		Capacity capacityDTO `json:"capacity"`
		Free     capacityDTO `json:"free"`
		Reserve  capacityDTO `json:"reserve"`
	} `json:"runners"`
	Callbacks struct {
		Pending          int     `json:"pending"`
		DeadLetter       int     `json:"dead_letter"`
		OldestLagSeconds float64 `json:"oldest_lag_seconds"`
	} `json:"callbacks"`
}

func handleInternalHostingMetrics(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) || !requireServiceTokenScope(w, r, serviceScopeDeploymentsRead) {
		return
	}
	metrics, err := collectHostingMetrics(r.Context(), time.Now().UTC())
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "collect hosting metrics failed", http.StatusInternalServerError)
		return
	}
	jsonResponse(w, metrics)
}

func collectHostingMetrics(ctx context.Context, now time.Time) (*hostingMetricsResponse, error) {
	result := &hostingMetricsResponse{ObservedAt: now, Window: "24h"}
	result.Deployments.FailureCodes = make(map[string]int)
	var oldestQueued sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), MIN(created_at) FROM hosting_jobs WHERE status='queued'`).Scan(&result.Queue.QueuedJobs, &oldestQueued); err != nil {
		return nil, err
	}
	if oldestQueued.Valid {
		result.Queue.OldestAgeSeconds = maxFloat(0, now.Sub(parseSQLiteTime(oldestQueued.String)).Seconds())
	}
	cutoff := formatSQLiteTime(now.Add(-24 * time.Hour))
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*),
		COALESCE(SUM(CASE WHEN status='active' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END), 0),
		COALESCE(AVG(CASE WHEN started_at IS NOT NULL AND finished_at IS NOT NULL
			THEN (julianday(finished_at)-julianday(started_at))*86400 END), 0)
		FROM hosting_deployments WHERE status IN ('active','failed','cancelled') AND finished_at>=?`, cutoff).Scan(
		&result.Deployments.Terminal, &result.Deployments.Successful, &result.Deployments.Failed,
		&result.Deployments.Cancelled, &result.Deployments.AverageDurationSeconds); err != nil {
		return nil, err
	}
	if result.Deployments.Terminal > 0 {
		result.Deployments.SuccessRate = float64(result.Deployments.Successful) / float64(result.Deployments.Terminal)
	}
	failureRows, err := db.QueryContext(ctx, `SELECT failure_code, COUNT(*) FROM hosting_deployments
		WHERE status IN ('failed','cancelled') AND finished_at>=? AND failure_code<>'' GROUP BY failure_code`, cutoff)
	if err != nil {
		return nil, err
	}
	for failureRows.Next() {
		var code string
		var count int
		if err := failureRows.Scan(&code, &count); err != nil {
			failureRows.Close()
			return nil, err
		}
		if !validHostingEventFailureCode(code) {
			failureRows.Close()
			return nil, fmt.Errorf("invalid persisted hosting failure code %q", code)
		}
		result.Deployments.FailureCodes[code] = count
	}
	if err := failureRows.Close(); err != nil {
		return nil, err
	}
	if err := db.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN status='online' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status='offline' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status='online' THEN capacity_cpu_millis ELSE 0 END), 0), COALESCE(SUM(CASE WHEN status='online' THEN capacity_ram_bytes ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status='online' THEN capacity_disk_bytes ELSE 0 END), 0), COALESCE(SUM(CASE WHEN status='online' THEN capacity_pids ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status='online' THEN free_cpu_millis ELSE 0 END), 0), COALESCE(SUM(CASE WHEN status='online' THEN free_ram_bytes ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status='online' THEN free_disk_bytes ELSE 0 END), 0), COALESCE(SUM(CASE WHEN status='online' THEN free_pids ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status='online' THEN reserve_cpu_millis ELSE 0 END), 0), COALESCE(SUM(CASE WHEN status='online' THEN reserve_ram_bytes ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status='online' THEN reserve_disk_bytes ELSE 0 END), 0), COALESCE(SUM(CASE WHEN status='online' THEN reserve_pids ELSE 0 END), 0)
		FROM hosting_runners`).Scan(&result.Runners.Online, &result.Runners.Offline,
		&result.Runners.Capacity.CPUMillis, &result.Runners.Capacity.RAMBytes, &result.Runners.Capacity.DiskBytes, &result.Runners.Capacity.PIDs,
		&result.Runners.Free.CPUMillis, &result.Runners.Free.RAMBytes, &result.Runners.Free.DiskBytes, &result.Runners.Free.PIDs,
		&result.Runners.Reserve.CPUMillis, &result.Runners.Reserve.RAMBytes, &result.Runners.Reserve.DiskBytes, &result.Runners.Reserve.PIDs); err != nil {
		return nil, err
	}
	var oldestCallback sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN status IN ('pending','delivering') THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status='dead_letter' THEN 1 ELSE 0 END), 0),
		MIN(CASE WHEN status IN ('pending','delivering') THEN created_at END)
		FROM callback_outbox`).Scan(&result.Callbacks.Pending, &result.Callbacks.DeadLetter, &oldestCallback); err != nil {
		return nil, err
	}
	if oldestCallback.Valid {
		result.Callbacks.OldestLagSeconds = maxFloat(0, now.Sub(parseSQLiteTime(oldestCallback.String)).Seconds())
	}
	return result, nil
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func protectActiveHostingArtifacts() error {
	if db == nil {
		return fmt.Errorf("hosting database is not initialized")
	}
	now := time.Now()
	for _, query := range []string{
		`SELECT DISTINCT source_artifact_path FROM hosting_jobs
			WHERE status IN ('queued','leased','running') AND source_artifact_path<>''`,
		`SELECT DISTINCT artifact.artifact_path FROM hosting_release_artifacts artifact
			JOIN hosting_releases release ON release.release_artifact_digest=artifact.artifact_digest`,
	} {
		rows, err := db.Query(query)
		if err != nil {
			return fmt.Errorf("list protected hosting artifacts: %w", err)
		}
		for rows.Next() {
			var path string
			if err := rows.Scan(&path); err != nil {
				rows.Close()
				return fmt.Errorf("read protected hosting artifact: %w", err)
			}
			if !isManagedArtifactPath(path) {
				rows.Close()
				return fmt.Errorf("referenced hosting artifact is outside managed storage")
			}
			if err := os.Chtimes(path, now, now); err != nil {
				rows.Close()
				return fmt.Errorf("refresh protected hosting artifact: %w", err)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("iterate protected hosting artifacts: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close protected hosting artifacts: %w", err)
		}
	}
	return nil
}

func cleanupHostingRecords(ctx context.Context, cfg AppConfig, now time.Time) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	deleteOlder := func(query string, days int) error {
		if days <= 0 {
			return nil
		}
		_, err := tx.ExecContext(ctx, query, formatSQLiteTime(now.Add(-time.Duration(days)*24*time.Hour)))
		return err
	}
	if cfg.HostingLogRetentionDays > 0 {
		if err := expireHostingStreamRows(ctx, tx, "logs", `SELECT hosting_deployment_id, id, created_at
			FROM hosting_logs WHERE created_at<?`, formatSQLiteTime(now.Add(-time.Duration(cfg.HostingLogRetentionDays)*24*time.Hour)), now); err != nil {
			return err
		}
	}
	if cfg.HostingEventRetentionDays > 0 {
		cutoff := formatSQLiteTime(now.Add(-time.Duration(cfg.HostingEventRetentionDays) * 24 * time.Hour))
		if err := expireHostingStreamRows(ctx, tx, "events", `SELECT hosting_deployment_id, id, created_at
			FROM hosting_events WHERE hosting_deployment_id IS NOT NULL AND created_at<?`, cutoff, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM hosting_events WHERE hosting_deployment_id IS NULL AND created_at<?`, cutoff); err != nil {
			return err
		}
	}
	if err := expireHostingStreamRows(ctx, tx, "logs", `SELECT hosting_deployment_id, id, created_at FROM (
		SELECT hosting_deployment_id, id, created_at,
		SUM(length(CAST(message AS BLOB))) OVER (
			PARTITION BY hosting_deployment_id ORDER BY id DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS retained_bytes FROM hosting_logs
	) WHERE retained_bytes>?`, maxHostingLogBytesPerDeployment, now); err != nil {
		return err
	}
	if err := expireHostingStreamRows(ctx, tx, "events", `SELECT hosting_deployment_id, id, created_at FROM (
		SELECT hosting_deployment_id, id, created_at,
		ROW_NUMBER() OVER (PARTITION BY hosting_deployment_id ORDER BY id DESC) AS retained_rows
		FROM hosting_events WHERE hosting_deployment_id IS NOT NULL
	) WHERE retained_rows>?`, maxHostingEventsPerDeployment, now); err != nil {
		return err
	}
	if err := deleteOlder(`DELETE FROM hosting_releases WHERE status IN ('inactive','failed') AND created_at<?`, cfg.HostingReleaseRetentionDays); err != nil {
		return err
	}
	orphanRows, err := tx.QueryContext(ctx, `SELECT artifact_path FROM hosting_release_artifacts artifact
		WHERE NOT EXISTS (SELECT 1 FROM hosting_releases release
			WHERE release.release_artifact_digest=artifact.artifact_digest)`)
	if err != nil {
		return err
	}
	var orphanArtifacts []string
	for orphanRows.Next() {
		var path string
		if err := orphanRows.Scan(&path); err != nil {
			orphanRows.Close()
			return err
		}
		orphanArtifacts = append(orphanArtifacts, path)
	}
	if err := orphanRows.Close(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM hosting_release_artifacts
		WHERE NOT EXISTS (SELECT 1 FROM hosting_releases release
			WHERE release.release_artifact_digest=hosting_release_artifacts.artifact_digest)`); err != nil {
		return err
	}
	if err := deleteOlder(`DELETE FROM callback_outbox WHERE status IN ('delivered','dead_letter')
		AND COALESCE(finalized_at, delivered_at, created_at)<?`, cfg.HostingCallbackRetentionDays); err != nil {
		return err
	}
	if err := deleteOlder(`DELETE FROM hosting_audit_events WHERE created_at<?`, cfg.HostingAuditRetentionDays); err != nil {
		return err
	}
	if err := deleteOlder(`DELETE FROM service_token_audit_events WHERE created_at<?`, cfg.HostingAuditRetentionDays); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM hosting_idempotency
		WHERE expires_at<? AND response_status IS NOT NULL AND response_body IS NOT NULL`, formatSQLiteTime(now)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for _, path := range orphanArtifacts {
		if err := currentArtifactStorage().Remove(path); err != nil {
			return fmt.Errorf("remove expired hosting release artifact: %w", err)
		}
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	runnerRows, err := conn.QueryContext(ctx, `SELECT id FROM hosting_runners`)
	if err != nil {
		return err
	}
	var runnerIDs []int64
	for runnerRows.Next() {
		var id int64
		if err := runnerRows.Scan(&id); err != nil {
			runnerRows.Close()
			return err
		}
		runnerIDs = append(runnerIDs, id)
	}
	if err := runnerRows.Close(); err != nil {
		return err
	}
	for _, id := range runnerIDs {
		if err := recomputeHostingRunnerCapacity(ctx, conn, id); err != nil {
			return err
		}
	}
	return nil
}

func expireHostingStreamRows(ctx context.Context, tx *sql.Tx, stream, query string, argument any, now time.Time) error {
	rows, err := tx.QueryContext(ctx, query, argument)
	if err != nil {
		return err
	}
	type expiry struct {
		maximumID int64
		maximumAt time.Time
		ids       []int64
	}
	byDeployment := make(map[int64]*expiry)
	for rows.Next() {
		var deploymentID, id int64
		var createdAt string
		if err := rows.Scan(&deploymentID, &id, &createdAt); err != nil {
			rows.Close()
			return err
		}
		entry := byDeployment[deploymentID]
		if entry == nil {
			entry = &expiry{}
			byDeployment[deploymentID] = entry
		}
		entry.ids = append(entry.ids, id)
		if id > entry.maximumID {
			entry.maximumID = id
		}
		created := parseSQLiteTime(createdAt)
		if created.After(entry.maximumAt) {
			entry.maximumAt = created
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	table := "hosting_" + stream
	for deploymentID, entry := range byDeployment {
		for _, id := range entry.ids {
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE hosting_deployment_id=? AND id=?`, deploymentID, id); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO hosting_stream_expiry_watermarks
			(hosting_deployment_id, stream, expired_through_id, expired_through_at, updated_at)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(hosting_deployment_id, stream) DO UPDATE SET
			expired_through_id=MAX(expired_through_id, excluded.expired_through_id),
			expired_through_at=MAX(expired_through_at, excluded.expired_through_at),
			updated_at=excluded.updated_at`, deploymentID, stream, entry.maximumID,
			formatSQLiteTime(entry.maximumAt), formatSQLiteTime(now)); err != nil {
			return err
		}
	}
	return nil
}
