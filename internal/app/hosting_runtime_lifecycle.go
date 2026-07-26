package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type hostingRuntimeRollbackState struct {
	OperationID, Status, LastErrorCode                     string
	ProjectID, DeploymentID, ReleaseID, TargetRunnerID     int64
	ExternalProjectID, ExternalDeploymentID, ReleaseDigest string
	PreviousReleaseDigest, Endpoint, InstanceID, SessionID string
	DesiredState, KillReason, RuntimeJSON                  string
	GlobalKill                                             int
	ReceiptStatus                                          sql.NullInt64
	ReceiptBody                                            sql.NullString
}

func rollbackHostingRuntimeOnlyRelease(ctx context.Context, token *ServiceToken, externalProjectID,
	externalDeploymentID, digest, key, requestHash string) (*hostingRollbackResult, error) {
	operationID, replay, err := stageHostingRuntimeRollback(ctx, token, externalProjectID,
		externalDeploymentID, digest, key, requestHash)
	if err != nil || replay != nil {
		return replay, err
	}
	return runHostingRuntimeRollback(ctx, operationID)
}

func stageHostingRuntimeRollback(ctx context.Context, token *ServiceToken, externalProjectID,
	externalDeploymentID, digest, key, requestHash string) (string, *hostingRollbackResult, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return "", nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return "", nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	if existing, err := findGenericIdempotencyReplay(ctx, conn, token.ID, hostingRollbackOperation,
		key, requestHash); err != nil || existing != nil {
		if err != nil {
			return "", nil, err
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return "", nil, err
		}
		committed = true
		replay, err := decodeHostingRollbackReplay(existing, true)
		return "", replay, err
	}

	now := time.Now().UTC()
	var state hostingRuntimeRollbackState
	var publicationMode, runnerStatus, runnerLastSeen, observedAt sql.NullString
	var evidenceJSON, observedSession string
	err = conn.QueryRowContext(ctx, `SELECT project.id, project.publication_mode, project.desired_state,
		project.kill_switch_reason, settings.global_kill_switch, release.id, deployment.id,
		deployment.external_deployment_id, release.release_digest, release.runtime_endpoint,
		release.runtime_runner_id, release.runtime_instance_id, runner.active_session_id,
		runner.status, runner.last_seen, release.runtime_observed_at,
		release.runtime_observed_session_id, release.runtime_failure_code,
		release.status, release.health_evidence_json, release.runtime_manifest_json
		FROM hosting_projects project JOIN hosting_settings settings ON settings.id=1
		JOIN hosting_releases release ON release.hosting_project_id=project.id
		JOIN hosting_deployments deployment ON deployment.id=release.hosting_deployment_id
		JOIN hosting_runners runner ON runner.id=release.runtime_runner_id
		WHERE project.external_project_id=? AND deployment.external_deployment_id=?
		  AND release.release_digest=?`, externalProjectID, externalDeploymentID, digest).Scan(
		&state.ProjectID, &publicationMode, &state.DesiredState, &state.KillReason, &state.GlobalKill,
		&state.ReleaseID, &state.DeploymentID, &state.ExternalDeploymentID, &state.ReleaseDigest,
		&state.Endpoint, &state.TargetRunnerID, &state.InstanceID, &state.SessionID,
		&runnerStatus, &runnerLastSeen, &observedAt, &observedSession, &state.LastErrorCode,
		&state.Status, &evidenceJSON, &state.RuntimeJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, &hostingAPIError{Code: errCodeReleaseNotFound, Message: "release not found", StatusCode: http.StatusNotFound}
		}
		return "", nil, err
	}
	if publicationMode.String != hostingPublicationRuntimeOnlyV1 {
		return "", nil, &hostingAPIError{Code: errCodeConflict, Message: "project publication mode changed", StatusCode: http.StatusConflict}
	}
	if state.DesiredState != "active" || state.KillReason != "" || state.GlobalKill != 0 {
		return "", nil, hostingRollbackTerminalError(errCodeProjectSuspended)
	}
	if state.Status != "healthy" && state.Status != "inactive" && state.Status != "active" {
		return "", nil, hostingRollbackTerminalError(errCodeReleaseNotHealthy)
	}
	var evidence map[string]any
	if json.Unmarshal([]byte(evidenceJSON), &evidence) != nil || !validHealthEvidence(evidence) ||
		state.Endpoint == "" || state.InstanceID == "" || state.SessionID == "" ||
		!runnerStatus.Valid || runnerStatus.String != "online" || !runnerLastSeen.Valid ||
		parseSQLiteTime(runnerLastSeen.String).Before(now.Add(-hostingRunnerStaleAfter)) ||
		!observedAt.Valid || parseSQLiteTime(observedAt.String).Before(now.Add(-hostingRunnerStaleAfter)) ||
		state.LastErrorCode != "" {
		return "", nil, hostingRollbackTerminalError(errCodeReleaseNotHealthy)
	}
	if observedSession != state.SessionID {
		return "", nil, hostingRollbackTerminalError(errCodeReleaseNotHealthy)
	}
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE((SELECT release_digest FROM hosting_releases
		WHERE hosting_project_id=? AND status='active'), '')`, state.ProjectID).Scan(
		&state.PreviousReleaseDigest); err != nil {
		return "", nil, err
	}
	if state.Status == "active" && state.PreviousReleaseDigest == state.ReleaseDigest {
		release, err := getHostingRollbackReleaseOn(ctx, conn, state.ReleaseID)
		if err != nil {
			return "", nil, err
		}
		body, err := encodeHostingRollbackResponse(release)
		if err != nil {
			return "", nil, err
		}
		if err := storeGenericIdempotency(ctx, conn, token.ID, hostingRollbackOperation, key,
			requestHash, http.StatusOK, body, now); err != nil {
			return "", nil, err
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return "", nil, err
		}
		committed = true
		return "", &hostingRollbackResult{Release: release, StatusCode: http.StatusOK,
			ResponseBody: body}, nil
	}
	state.ExternalProjectID = externalProjectID
	state.OperationID = hashHostingOperation("runtime-only-rollback", fmt.Sprint(token.ID), key, requestHash)
	if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_idempotency
		(issuer_token_id, operation, idempotency_key, request_hash, operation_reference, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, token.ID, hostingRollbackOperation, key, requestHash,
		state.OperationID, formatSQLiteTime(now), formatSQLiteTime(now.Add(hostingIdempotencyTTL))); err != nil {
		return "", nil, err
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_runtime_rollbacks
		(operation_id, hosting_project_id, hosting_deployment_id, hosting_release_id,
		 expected_previous_release_digest, target_runtime_runner_id, target_runtime_instance_id,
		 target_runtime_session_id, target_runtime_endpoint, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)`, state.OperationID, state.ProjectID,
		state.DeploymentID, state.ReleaseID, state.PreviousReleaseDigest, state.TargetRunnerID,
		state.InstanceID, state.SessionID, state.Endpoint, formatSQLiteTime(now), formatSQLiteTime(now)); err != nil {
		return "", nil, err
	}
	metadata, _ := json.Marshal(map[string]any{"release_digest": digest,
		"previous_release_digest": state.PreviousReleaseDigest, "publication_mode": hostingPublicationRuntimeOnlyV1})
	if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_audit_events
		(issuer_token_id, hosting_project_id, event_type, reason, request_id, metadata_json, created_at)
		VALUES (?, ?, 'hosting_release_rollback_requested', 'control-plane rollback', ?, ?, ?)`, token.ID,
		state.ProjectID, requestIDFromContext(ctx), redactSecrets(string(metadata)), formatSQLiteTime(now)); err != nil {
		return "", nil, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return "", nil, err
	}
	committed = true
	return state.OperationID, nil, nil
}

func loadHostingRuntimeRollbackOn(ctx context.Context, querier hostingRouteGenerationQuerier,
	operationID string) (*hostingRuntimeRollbackState, error) {
	var state hostingRuntimeRollbackState
	err := querier.QueryRowContext(ctx, `SELECT rollback.operation_id, rollback.status,
		rollback.last_error_code, rollback.hosting_project_id, rollback.hosting_deployment_id,
		rollback.hosting_release_id, project.external_project_id, deployment.external_deployment_id,
		release.release_digest, rollback.expected_previous_release_digest,
		rollback.target_runtime_endpoint, rollback.target_runtime_runner_id,
		rollback.target_runtime_instance_id, rollback.target_runtime_session_id,
		project.desired_state, project.kill_switch_reason, settings.global_kill_switch,
		release.runtime_manifest_json, receipt.response_status, receipt.response_body
		FROM hosting_runtime_rollbacks rollback
		JOIN hosting_projects project ON project.id=rollback.hosting_project_id
		JOIN hosting_settings settings ON settings.id=1
		JOIN hosting_deployments deployment ON deployment.id=rollback.hosting_deployment_id
		JOIN hosting_releases release ON release.id=rollback.hosting_release_id
		LEFT JOIN hosting_idempotency receipt ON receipt.operation_reference=rollback.operation_id
		WHERE rollback.operation_id=?`, operationID).Scan(&state.OperationID, &state.Status,
		&state.LastErrorCode, &state.ProjectID, &state.DeploymentID, &state.ReleaseID,
		&state.ExternalProjectID, &state.ExternalDeploymentID, &state.ReleaseDigest,
		&state.PreviousReleaseDigest, &state.Endpoint, &state.TargetRunnerID, &state.InstanceID,
		&state.SessionID, &state.DesiredState, &state.KillReason, &state.GlobalKill,
		&state.RuntimeJSON, &state.ReceiptStatus, &state.ReceiptBody)
	return &state, err
}

func runHostingRuntimeRollback(ctx context.Context, operationID string) (*hostingRollbackResult, error) {
	state, err := loadHostingRuntimeRollbackOn(ctx, db, operationID)
	if err != nil {
		return nil, err
	}
	if state.ReceiptStatus.Valid && state.ReceiptBody.Valid {
		return decodeHostingRollbackReplay(&hostingIdempotencyResponse{StatusCode: int(state.ReceiptStatus.Int64),
			Body: []byte(state.ReceiptBody.String)}, false)
	}
	if state.Status == "failed" {
		return settleHostingRuntimeRollbackFailure(ctx, operationID, hostingRollbackTerminalError(state.LastErrorCode))
	}
	if state.Status == "committed" {
		return completeHostingRuntimeRollbackReceipt(ctx, operationID)
	}
	if state.DesiredState != "active" || state.KillReason != "" || state.GlobalKill != 0 {
		return settleHostingRuntimeRollbackFailure(ctx, operationID, hostingRollbackTerminalError(errCodeProjectSuspended))
	}
	fence, available, err := currentHostingReleaseRuntimeFence(ctx, db, state.ProjectID,
		state.ReleaseDigest, state.Endpoint)
	if err != nil {
		return nil, err
	}
	if !available || fence.RunnerID != state.TargetRunnerID || fence.InstanceID != state.InstanceID ||
		fence.SessionID != state.SessionID {
		return settleHostingRuntimeRollbackFailure(ctx, operationID, hostingRollbackTerminalError(errCodeReleaseNotHealthy))
	}
	var runtime HostingRuntimeManifest
	if json.Unmarshal([]byte(state.RuntimeJSON), &runtime) != nil || validateHostingRuntimeManifest(&runtime) != nil {
		return nil, fmt.Errorf("invalid persisted rollback runtime manifest")
	}
	healthPath := runtime.HealthPath
	if healthPath == "" {
		healthPath = "/"
	}
	healthCtx, cancel := context.WithTimeout(ctx, hostingActivationReconcileHealthTimeout)
	_, _, healthErr := checkHostingCandidateHealth(healthCtx, strings.TrimRight(state.Endpoint, "/")+healthPath)
	cancel()
	if healthErr != nil {
		return settleHostingRuntimeRollbackFailure(ctx, operationID, hostingRollbackTerminalError(errCodeReleaseNotHealthy))
	}
	result, err := commitHostingRuntimeRollback(ctx, operationID)
	if errors.Is(err, errHostingStateConflict) {
		return settleHostingRuntimeRollbackFailure(ctx, operationID,
			hostingRollbackTerminalError(errCodeConflict))
	}
	return result, err
}

func commitHostingRuntimeRollback(ctx context.Context, operationID string) (*hostingRollbackResult, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	state, err := loadHostingRuntimeRollbackOn(ctx, conn, operationID)
	if err != nil {
		return nil, err
	}
	if state.ReceiptStatus.Valid && state.ReceiptBody.Valid {
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return nil, err
		}
		committed = true
		return decodeHostingRollbackReplay(&hostingIdempotencyResponse{StatusCode: int(state.ReceiptStatus.Int64), Body: []byte(state.ReceiptBody.String)}, false)
	}
	if state.Status == "committed" {
		result, err := completeHostingRuntimeRollbackReceiptOn(ctx, conn, state)
		if err != nil {
			return nil, err
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return nil, err
		}
		committed = true
		return result, nil
	}
	if state.Status != "pending" || state.DesiredState != "active" || state.KillReason != "" || state.GlobalKill != 0 {
		return nil, errHostingStateConflict
	}
	fence, available, err := currentHostingReleaseRuntimeFence(ctx, conn, state.ProjectID, state.ReleaseDigest, state.Endpoint)
	if err != nil {
		return nil, err
	}
	if !available || fence.RunnerID != state.TargetRunnerID || fence.InstanceID != state.InstanceID || fence.SessionID != state.SessionID {
		return nil, errHostingStateConflict
	}
	var currentActiveDigest string
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE((SELECT release_digest FROM hosting_releases
		WHERE hosting_project_id=? AND status='active'), '')`, state.ProjectID).Scan(&currentActiveDigest); err != nil {
		return nil, err
	}
	if currentActiveDigest != state.PreviousReleaseDigest {
		return nil, errHostingStateConflict
	}
	now := time.Now().UTC()
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET status='inactive', deactivated_at=?
		WHERE hosting_project_id=? AND status='active' AND id<>?`, formatSQLiteTime(now), state.ProjectID, state.ReleaseID); err != nil {
		return nil, err
	}
	result, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET status='active', route_revision='',
		previous_release_digest=?, activated_at=?, deactivated_at=NULL
		WHERE id=? AND hosting_project_id=? AND status IN ('healthy','inactive','active')
		  AND runtime_runner_id=? AND runtime_instance_id=? AND runtime_failure_code=''
		  AND runtime_missing_since IS NULL
		  AND (SELECT active_session_id FROM hosting_runners WHERE id=?)=?
		  AND runtime_observed_at>=? AND runtime_observed_session_id=?`, state.PreviousReleaseDigest,
		formatSQLiteTime(now), state.ReleaseID, state.ProjectID, state.TargetRunnerID, state.InstanceID,
		state.TargetRunnerID, state.SessionID, formatSQLiteTime(now.Add(-hostingRunnerStaleAfter)), state.SessionID)
	if err != nil {
		return nil, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return nil, errHostingStateConflict
	}
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_runtime_rollbacks SET status='committed',
		last_error_code='', updated_at=? WHERE operation_id=? AND status='pending'`, formatSQLiteTime(now), operationID); err != nil {
		return nil, err
	}
	state.Status = "committed"
	resultResponse, err := completeHostingRuntimeRollbackReceiptOn(ctx, conn, state)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, err
	}
	committed = true
	return resultResponse, nil
}

func completeHostingRuntimeRollbackReceipt(ctx context.Context, operationID string) (*hostingRollbackResult, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, err
	}
	state, err := loadHostingRuntimeRollbackOn(ctx, conn, operationID)
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		return nil, err
	}
	result, err := completeHostingRuntimeRollbackReceiptOn(ctx, conn, state)
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, err
	}
	return result, nil
}

func completeHostingRuntimeRollbackReceiptOn(ctx context.Context, conn *sql.Conn,
	state *hostingRuntimeRollbackState) (*hostingRollbackResult, error) {
	release, err := getHostingRollbackReleaseOn(ctx, conn, state.ReleaseID)
	if err != nil {
		return nil, err
	}
	if release.Status != "active" || release.RouteRevision != "" {
		return nil, errHostingStateConflict
	}
	body, err := encodeHostingRollbackResponse(release)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	var existing int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM hosting_events WHERE hosting_project_id=?
		AND event_type='release_rolled_back' AND json_extract(metadata_json, '$.operation_id')=?`,
		state.ProjectID, state.OperationID).Scan(&existing); err != nil {
		return nil, err
	}
	if existing == 0 {
		if err := recordHostingEvent(ctx, conn, state.ProjectID, state.DeploymentID, "release_rolled_back",
			hostingPhaseRollingBack, "", map[string]any{"operation_id": state.OperationID,
				"release_digest": state.ReleaseDigest, "publication_mode": hostingPublicationRuntimeOnlyV1}, now); err != nil {
			return nil, err
		}
	}
	if err := completeHostingRollbackReceipt(ctx, conn, state.OperationID, http.StatusOK, body, now); err != nil {
		return nil, err
	}
	return &hostingRollbackResult{Release: release, StatusCode: http.StatusOK, ResponseBody: body}, nil
}

func settleHostingRuntimeRollbackFailure(ctx context.Context, operationID string,
	apiErr *hostingAPIError) (*hostingRollbackResult, error) {
	body, err := encodeHostingRollbackResponse(apiErrorResponse{Error: apiErr.Message, Code: apiErr.Code})
	if err != nil {
		return nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_runtime_rollbacks SET status='failed',
		last_error_code=?, updated_at=? WHERE operation_id=? AND status IN ('pending','failed')`, apiErr.Code,
		formatSQLiteTime(time.Now().UTC()), operationID); err != nil {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		return nil, err
	}
	if err := completeHostingRollbackReceipt(ctx, conn, operationID, apiErr.StatusCode, body, time.Now().UTC()); err != nil {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, err
	}
	return &hostingRollbackResult{StatusCode: apiErr.StatusCode, ResponseBody: body}, nil
}

func reconcileHostingRuntimeRollbacks(ctx context.Context) error {
	rows, err := db.QueryContext(ctx, `SELECT operation_id FROM hosting_runtime_rollbacks WHERE status='pending' ORDER BY created_at`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := runHostingRuntimeRollback(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func suspendRuntimeOnlyWorkloads(ctx context.Context, conn *sql.Conn, projectID sql.NullInt64,
	reason string, now time.Time) (int, error) {
	query := `SELECT release.id, release.hosting_project_id, release.hosting_deployment_id,
		release.runtime_runner_id, release.runtime_instance_id, runner.active_session_id,
		runner.last_heartbeat_sequence
		FROM hosting_releases release
		JOIN hosting_projects project ON project.id=release.hosting_project_id
		JOIN hosting_runners runner ON runner.id=release.runtime_runner_id
		WHERE project.publication_mode=? AND release.status='active'
		  AND release.runtime_runner_id IS NOT NULL
		  AND NOT EXISTS (SELECT 1 FROM hosting_runtime_stops stop
		    WHERE stop.hosting_release_id=release.id AND stop.status='pending')`
	args := []any{hostingPublicationRuntimeOnlyV1}
	if projectID.Valid {
		query += ` AND project.id=?`
		args = append(args, projectID.Int64)
	}
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	type runtimeState struct {
		releaseID, projectID, deploymentID, runnerID int64
		instanceID, sessionID                        string
		sequence                                     int64
	}
	var runtimes []runtimeState
	for rows.Next() {
		var runtime runtimeState
		if err := rows.Scan(&runtime.releaseID, &runtime.projectID, &runtime.deploymentID,
			&runtime.runnerID, &runtime.instanceID, &runtime.sessionID, &runtime.sequence); err != nil {
			rows.Close()
			return 0, err
		}
		runtimes = append(runtimes, runtime)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	for _, runtime := range runtimes {
		if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_runtime_stops
			(hosting_release_id, hosting_runner_id, runtime_instance_id, runner_session_id,
			 requested_sequence, status, reason, created_at)
			VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)`, runtime.releaseID, runtime.runnerID,
			runtime.instanceID, runtime.sessionID, runtime.sequence, redactSecrets(reason),
			formatSQLiteTime(now)); err != nil {
			return 0, err
		}
		if err := recordHostingEvent(ctx, conn, runtime.projectID, runtime.deploymentID,
			"runtime_stop_requested", hostingPhaseCancelling, "",
			map[string]any{"reason": redactSecrets(reason), "publication_mode": hostingPublicationRuntimeOnlyV1}, now); err != nil {
			return 0, err
		}
	}
	return len(runtimes), nil
}

func reconcileHostingRuntimeStopsOnHeartbeat(ctx context.Context, conn *sql.Conn, runnerID int64,
	sessionID string, sequence int64, now time.Time) error {
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_runtime_stops SET runner_session_id=?,
		requested_sequence=? WHERE hosting_runner_id=? AND status='pending' AND runner_session_id<>?`,
		sessionID, sequence, runnerID, sessionID); err != nil {
		return err
	}
	rows, err := conn.QueryContext(ctx, `SELECT stop.id, stop.hosting_release_id,
		release.hosting_project_id, release.hosting_deployment_id, stop.runtime_instance_id,
		job.required_cpu_millis, job.required_ram_bytes, job.required_disk_bytes, job.required_pids
		FROM hosting_runtime_stops stop
		JOIN hosting_releases release ON release.id=stop.hosting_release_id
		JOIN hosting_jobs job ON job.hosting_deployment_id=release.hosting_deployment_id
		WHERE stop.hosting_runner_id=? AND stop.status='pending' AND stop.runner_session_id=?
		  AND ? > stop.requested_sequence
		  AND release.runtime_observed_at IS NULL`, runnerID, sessionID, sequence)
	if err != nil {
		return err
	}
	type stoppedRuntime struct {
		stopID, releaseID, projectID, deploymentID int64
		instanceID                                 string
		limits                                     hostingWorkloadLimits
	}
	var stopped []stoppedRuntime
	for rows.Next() {
		var runtime stoppedRuntime
		if err := rows.Scan(&runtime.stopID, &runtime.releaseID, &runtime.projectID,
			&runtime.deploymentID, &runtime.instanceID, &runtime.limits.CPUMillis,
			&runtime.limits.RAMBytes, &runtime.limits.DiskBytes, &runtime.limits.PIDs); err != nil {
			rows.Close()
			return err
		}
		stopped = append(stopped, runtime)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, runtime := range stopped {
		result, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET runtime_runner_id=NULL,
			runtime_observed_session_id='', runtime_missing_since=NULL, runtime_missing_observations=0,
			runtime_failure_code='execution_disabled'
			WHERE id=? AND status='active' AND runtime_runner_id=? AND runtime_instance_id=?
			  AND runtime_observed_at IS NULL`, runtime.releaseID, runnerID, runtime.instanceID)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			continue
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_runtime_recoveries
			(hosting_release_id, runtime_owner_runner_id, allow_owner_runner, status,
			 required_cpu_millis, required_ram_bytes, required_disk_bytes, required_pids, created_at)
			VALUES (?, ?, 1, 'queued', ?, ?, ?, ?, ?)`, runtime.releaseID, runnerID,
			runtime.limits.CPUMillis, runtime.limits.RAMBytes, runtime.limits.DiskBytes,
			runtime.limits.PIDs, formatSQLiteTime(now)); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `UPDATE hosting_runtime_stops SET status='committed',
			completed_at=? WHERE id=? AND status='pending'`, formatSQLiteTime(now), runtime.stopID); err != nil {
			return err
		}
		if err := recomputeHostingRunnerCapacity(ctx, conn, runnerID); err != nil {
			return err
		}
		if err := recordHostingEvent(ctx, conn, runtime.projectID, runtime.deploymentID,
			"runtime_execution_suspended", hostingPhaseCancelled, "", map[string]any{
				"runner_id": runnerID, "runner_session_id": sessionID,
				"publication_mode": hostingPublicationRuntimeOnlyV1}, now); err != nil {
			return err
		}
	}
	return nil
}

func ensureResumableRuntimeOnlyRecoveries(ctx context.Context, conn *sql.Conn,
	projectID sql.NullInt64, now time.Time) (int, error) {
	query := `SELECT release.id, release.hosting_project_id, release.hosting_deployment_id,
		prior.runtime_owner_runner_id, prior.allow_owner_runner, prior.required_cpu_millis,
		prior.required_ram_bytes, prior.required_disk_bytes, prior.required_pids
		FROM hosting_releases release
		JOIN hosting_projects project ON project.id=release.hosting_project_id
		JOIN hosting_settings settings ON settings.id=1
		JOIN hosting_runtime_recoveries prior ON prior.id=(
		  SELECT previous.id FROM hosting_runtime_recoveries previous
		  WHERE previous.hosting_release_id=release.id ORDER BY previous.id DESC LIMIT 1)
		WHERE project.publication_mode=? AND project.desired_state='active'
		  AND project.kill_switch_reason='' AND settings.global_kill_switch=0
		  AND release.status='active' AND release.runtime_runner_id IS NULL
		  AND release.runtime_failure_code='execution_disabled'
		  AND NOT EXISTS (SELECT 1 FROM hosting_runtime_recoveries active
		    WHERE active.hosting_release_id=release.id AND active.status IN ('queued','leased','running'))`
	args := []any{hostingPublicationRuntimeOnlyV1}
	if projectID.Valid {
		query += ` AND project.id=?`
		args = append(args, projectID.Int64)
	}
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	type resumableRuntime struct {
		releaseID, projectID, deploymentID, ownerRunnerID int64
		allowOwner                                        bool
		limits                                            hostingWorkloadLimits
	}
	var runtimes []resumableRuntime
	for rows.Next() {
		var runtime resumableRuntime
		if err := rows.Scan(&runtime.releaseID, &runtime.projectID, &runtime.deploymentID,
			&runtime.ownerRunnerID, &runtime.allowOwner, &runtime.limits.CPUMillis,
			&runtime.limits.RAMBytes, &runtime.limits.DiskBytes, &runtime.limits.PIDs); err != nil {
			rows.Close()
			return 0, err
		}
		runtimes = append(runtimes, runtime)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	created := 0
	for _, runtime := range runtimes {
		result, err := conn.ExecContext(ctx, `INSERT INTO hosting_runtime_recoveries
			(hosting_release_id, runtime_owner_runner_id, allow_owner_runner, status,
			 required_cpu_millis, required_ram_bytes, required_disk_bytes, required_pids, created_at)
			SELECT ?, ?, ?, 'queued', ?, ?, ?, ?, ? WHERE NOT EXISTS (
			 SELECT 1 FROM hosting_runtime_recoveries active WHERE active.hosting_release_id=?
			   AND active.status IN ('queued','leased','running'))`, runtime.releaseID,
			runtime.ownerRunnerID, runtime.allowOwner, runtime.limits.CPUMillis,
			runtime.limits.RAMBytes, runtime.limits.DiskBytes, runtime.limits.PIDs,
			formatSQLiteTime(now), runtime.releaseID)
		if err != nil {
			return created, err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			continue
		}
		created++
		if err := recordHostingEvent(ctx, conn, runtime.projectID, runtime.deploymentID,
			"runtime_recovery_requeued_after_resume", hostingPhaseQueued, "", map[string]any{
				"publication_mode": hostingPublicationRuntimeOnlyV1}, now); err != nil {
			return created, err
		}
	}
	return created, nil
}
