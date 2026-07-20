package app

import (
	"context"
	"database/sql"
	"net/http"
	"os"
	"time"
)

type hostingMetricsResponse struct {
	ObservedAt time.Time `json:"observed_at"`
	Window     string    `json:"window"`
	Queue      struct {
		QueuedJobs       int     `json:"queued_jobs"`
		OldestAgeSeconds float64 `json:"oldest_age_seconds"`
	} `json:"queue"`
	Deployments struct {
		Terminal               int     `json:"terminal"`
		Successful             int     `json:"successful"`
		Failed                 int     `json:"failed"`
		Cancelled              int     `json:"cancelled"`
		SuccessRate            float64 `json:"success_rate"`
		AverageDurationSeconds float64 `json:"average_duration_seconds"`
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
	if err := db.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN status='online' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status='offline' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(capacity_cpu_millis), 0), COALESCE(SUM(capacity_ram_bytes), 0),
		COALESCE(SUM(capacity_disk_bytes), 0), COALESCE(SUM(capacity_pids), 0),
		COALESCE(SUM(free_cpu_millis), 0), COALESCE(SUM(free_ram_bytes), 0),
		COALESCE(SUM(free_disk_bytes), 0), COALESCE(SUM(free_pids), 0),
		COALESCE(SUM(reserve_cpu_millis), 0), COALESCE(SUM(reserve_ram_bytes), 0),
		COALESCE(SUM(reserve_disk_bytes), 0), COALESCE(SUM(reserve_pids), 0)
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

func protectActiveHostingArtifacts() {
	if db == nil {
		return
	}
	rows, err := db.Query(`SELECT DISTINCT source_artifact_path FROM hosting_jobs
		WHERE status IN ('queued','leased','running') AND source_artifact_path<>''`)
	if err != nil {
		return
	}
	defer rows.Close()
	now := time.Now()
	for rows.Next() {
		var path string
		if rows.Scan(&path) == nil && isManagedArtifactPath(path) {
			_ = os.Chtimes(path, now, now)
		}
	}
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
	if err := deleteOlder(`DELETE FROM hosting_logs WHERE created_at<?`, cfg.HostingLogRetentionDays); err != nil {
		return err
	}
	if err := deleteOlder(`DELETE FROM hosting_events WHERE created_at<?`, cfg.HostingEventRetentionDays); err != nil {
		return err
	}
	if err := deleteOlder(`DELETE FROM hosting_releases WHERE status IN ('inactive','failed') AND created_at<?`, cfg.HostingReleaseRetentionDays); err != nil {
		return err
	}
	if err := deleteOlder(`DELETE FROM callback_outbox WHERE status IN ('delivered','dead_letter') AND created_at<?`, cfg.HostingCallbackRetentionDays); err != nil {
		return err
	}
	if err := deleteOlder(`DELETE FROM hosting_audit_events WHERE created_at<?`, cfg.HostingAuditRetentionDays); err != nil {
		return err
	}
	if err := deleteOlder(`DELETE FROM service_token_audit_events WHERE created_at<?`, cfg.HostingAuditRetentionDays); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM hosting_idempotency WHERE expires_at<?`, formatSQLiteTime(now)); err != nil {
		return err
	}
	return tx.Commit()
}
