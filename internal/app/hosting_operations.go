package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type hostingKillSwitchRequest struct {
	Enabled bool   `json:"enabled"`
	Reason  string `json:"reason"`
}

type hostingKillSwitchResponse struct {
	Scope             string `json:"scope"`
	ExternalProjectID string `json:"external_project_id,omitempty"`
	Enabled           bool   `json:"enabled"`
	Reason            string `json:"reason"`
	UpdatedAt         string `json:"updated_at"`
}

func handleInternalGlobalKillSwitch(w http.ResponseWriter, r *http.Request) {
	handleInternalKillSwitch(w, r, "", serviceScopeHostingAdmin)
}

func handleInternalProjectKillSwitch(w http.ResponseWriter, r *http.Request, externalProjectID string) {
	handleInternalKillSwitch(w, r, externalProjectID, serviceScopeProjectsWrite)
}

func handleInternalKillSwitch(w http.ResponseWriter, r *http.Request, externalProjectID, requiredScope string) {
	if !requireMethod(w, r, http.MethodPut) || !requireServiceTokenScope(w, r, requiredScope) || !requireJSONContentType(w, r) {
		return
	}
	key := strings.TrimSpace(r.Header.Get(idempotencyKeyHeader))
	if !validIdempotencyKey(key) {
		jsonErrorCode(w, errCodeInvalidIdempotencyKey, "Idempotency-Key must contain 8-128 safe characters", http.StatusBadRequest)
		return
	}
	var input hostingKillSwitchRequest
	if !decodeInternalJSON(w, r, 8<<10, &input) {
		return
	}
	input.Reason = strings.TrimSpace(input.Reason)
	if len(input.Reason) < 3 || len(input.Reason) > 500 || strings.ContainsAny(input.Reason, "\x00\r\n") {
		jsonErrorCode(w, errCodeValidation, "reason must contain 3-500 characters on one line", http.StatusBadRequest)
		return
	}
	token, _ := r.Context().Value(serviceTokenContextKey{}).(*ServiceToken)
	response, replayed, err := setHostingExecutionKillSwitch(r.Context(), token, externalProjectID, input, key, requestIDFromContext(r.Context()))
	if err != nil {
		writeHostingAPIError(w, err)
		return
	}
	if replayed {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	jsonResponse(w, response)
}

func setHostingExecutionKillSwitch(ctx context.Context, token *ServiceToken, externalProjectID string, input hostingKillSwitchRequest, key, requestID string) (*hostingKillSwitchResponse, bool, error) {
	if token == nil {
		return nil, false, &hostingAPIError{Code: errCodeServiceTokenRequired, Message: "service token is required", StatusCode: http.StatusUnauthorized}
	}
	scope := "global"
	operation := "hosting.execution.kill_switch.global"
	if externalProjectID != "" {
		scope = "project"
		operation = "hosting.execution.kill_switch.project"
	}
	requestHash := hashHostingOperation(operation, externalProjectID, fmt.Sprint(input.Enabled), input.Reason)
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, false, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	if replay, err := findGenericIdempotencyReplay(ctx, conn, token.ID, operation, key, requestHash); err != nil || replay != nil {
		if err != nil {
			return nil, false, err
		}
		var response hostingKillSwitchResponse
		if err := json.Unmarshal(replay, &response); err != nil {
			return nil, false, err
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return nil, false, err
		}
		committed = true
		return &response, true, nil
	}

	now := time.Now().UTC()
	var projectID sql.NullInt64
	storedReason := ""
	if input.Enabled {
		storedReason = redactSecrets(input.Reason)
	}
	if externalProjectID == "" {
		enabled := 0
		if input.Enabled {
			enabled = 1
		}
		if _, err := conn.ExecContext(ctx, `UPDATE hosting_settings SET global_kill_switch=?, kill_switch_reason=?, updated_at=? WHERE id=1`, enabled, storedReason, formatSQLiteTime(now)); err != nil {
			return nil, false, err
		}
	} else {
		var id int64
		if err := conn.QueryRowContext(ctx, `SELECT id FROM hosting_projects WHERE external_project_id=?`, externalProjectID).Scan(&id); err != nil {
			if err == sql.ErrNoRows {
				return nil, false, &hostingAPIError{Code: errCodeProjectNotFound, Message: "project not found", StatusCode: http.StatusNotFound}
			}
			return nil, false, err
		}
		projectID = sql.NullInt64{Int64: id, Valid: true}
		if _, err := conn.ExecContext(ctx, `UPDATE hosting_projects SET kill_switch_reason=?, updated_at=? WHERE id=?`, storedReason, formatSQLiteTime(now), id); err != nil {
			return nil, false, err
		}
		eventType := "project_execution_enabled"
		if input.Enabled {
			eventType = "project_execution_disabled"
		}
		if err := recordHostingEvent(ctx, conn, id, 0, eventType, "", "", map[string]any{"reason": input.Reason}, now); err != nil {
			return nil, false, err
		}
	}
	if input.Enabled {
		if err := cancelHostingJobsForKillSwitch(ctx, conn, projectID, input.Reason, now); err != nil {
			return nil, false, err
		}
	}
	metadata, _ := json.Marshal(map[string]any{"enabled": input.Enabled, "scope": scope, "external_project_id": externalProjectID})
	var auditProject any
	if projectID.Valid {
		auditProject = projectID.Int64
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_audit_events
		(issuer_token_id, hosting_project_id, event_type, reason, request_id, metadata_json, created_at)
		VALUES (?, ?, 'execution_kill_switch_changed', ?, ?, ?, ?)`, token.ID, auditProject,
		redactSecrets(input.Reason), requestID, redactSecrets(string(metadata)), formatSQLiteTime(now)); err != nil {
		return nil, false, err
	}
	response := &hostingKillSwitchResponse{Scope: scope, ExternalProjectID: externalProjectID, Enabled: input.Enabled, Reason: redactSecrets(input.Reason), UpdatedAt: formatSQLiteTime(now)}
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, false, err
	}
	if err := storeGenericIdempotency(ctx, conn, token.ID, operation, key, requestHash, http.StatusOK, encoded, now); err != nil {
		return nil, false, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, false, err
	}
	committed = true
	return response, false, nil
}

func cancelHostingJobsForKillSwitch(ctx context.Context, conn *sql.Conn, projectID sql.NullInt64, reason string, now time.Time) error {
	query := `SELECT j.id, j.hosting_deployment_id, d.hosting_project_id, j.hosting_runner_id,
		j.status, j.required_cpu_millis, j.required_ram_bytes, j.required_disk_bytes, j.required_pids
		FROM hosting_jobs j JOIN hosting_deployments d ON d.id=j.hosting_deployment_id
		WHERE j.status IN ('queued','leased','running') AND d.status IN ('queued','running')`
	args := []any{}
	if projectID.Valid {
		query += ` AND d.hosting_project_id=?`
		args = append(args, projectID.Int64)
	}
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	type jobState struct {
		jobID, deploymentID, projectID int64
		runnerID                       sql.NullInt64
		status                         string
		limits                         hostingWorkloadLimits
	}
	var jobs []jobState
	for rows.Next() {
		var job jobState
		if err := rows.Scan(&job.jobID, &job.deploymentID, &job.projectID, &job.runnerID, &job.status,
			&job.limits.CPUMillis, &job.limits.RAMBytes, &job.limits.DiskBytes, &job.limits.PIDs); err != nil {
			rows.Close()
			return err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, job := range jobs {
		if job.status == "queued" {
			if job.runnerID.Valid {
				if err := restoreHostingRunnerCapacity(ctx, conn, job.runnerID.Int64, job.limits); err != nil {
					return err
				}
			}
			if _, err := conn.ExecContext(ctx, `UPDATE hosting_jobs SET status='cancelled', cancel_requested_at=?, completed_at=?, hosting_runner_id=NULL WHERE id=? AND status='queued'`, formatSQLiteTime(now), formatSQLiteTime(now), job.jobID); err != nil {
				return err
			}
			if _, err := conn.ExecContext(ctx, `UPDATE hosting_deployments SET status='cancelled', phase='cancelled', failure_code='cancelled', failure_message=?, cancel_requested_at=?, finished_at=?, updated_at=? WHERE id=? AND status='queued'`, redactSecrets(reason), formatSQLiteTime(now), formatSQLiteTime(now), formatSQLiteTime(now), job.deploymentID); err != nil {
				return err
			}
			if err := recordHostingEvent(ctx, conn, job.projectID, job.deploymentID, "deployment_cancelled_by_kill_switch", hostingPhaseCancelled, "cancelled", map[string]any{"reason": reason}, now); err != nil {
				return err
			}
			if err := enqueueTerminalCallback(ctx, conn, job.deploymentID, now); err != nil {
				return err
			}
			continue
		}
		if _, err := conn.ExecContext(ctx, `UPDATE hosting_jobs SET cancel_requested_at=? WHERE id=? AND status IN ('leased','running')`, formatSQLiteTime(now), job.jobID); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `UPDATE hosting_deployments SET phase='cancelling', failure_message=?, cancel_requested_at=?, updated_at=? WHERE id=? AND status='running'`, redactSecrets(reason), formatSQLiteTime(now), formatSQLiteTime(now), job.deploymentID); err != nil {
			return err
		}
		if err := recordHostingEvent(ctx, conn, job.projectID, job.deploymentID, "cancellation_requested_by_kill_switch", hostingPhaseCancelling, "", map[string]any{"reason": reason}, now); err != nil {
			return err
		}
	}
	return nil
}
