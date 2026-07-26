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

const hostingRollbackOperation = "hosting.project.rollback"

type hostingRollbackResult struct {
	Release      *HostingRelease
	StatusCode   int
	ResponseBody []byte
	Replayed     bool
}

type hostingRollbackOperationState struct {
	OperationID               string
	Status                    string
	LastErrorCode             string
	ProjectID                 int64
	DeploymentID              int64
	ReleaseID                 int64
	ExternalProjectID         string
	ExternalDeploymentID      string
	ReleaseDigest             string
	RuntimeEndpoint           string
	Runtime                   HostingRuntimeManifest
	PreviousReleaseDigest     string
	PreviousRuntimeEndpoint   string
	RouteRevision             string
	RouteGeneration           int64
	CurrentRouteGeneration    int64
	TargetRunnerID            sql.NullInt64
	TargetRuntimeInstanceID   string
	TargetRuntimeSessionID    string
	CurrentRuntimeRunnerID    sql.NullInt64
	CurrentRuntimeInstanceID  string
	CurrentRuntimeFailureCode string
	ProjectDesiredState       string
	ProjectKillSwitchReason   string
	GlobalKillSwitch          int
	ReceiptOperationReference sql.NullString
	ReceiptResponseStatus     sql.NullInt64
	ReceiptResponseBody       sql.NullString
	UpdatedAt                 time.Time
}

func rollbackHostingRelease(ctx context.Context, token *ServiceToken, externalProjectID,
	externalDeploymentID, digest, key string) (*HostingRelease, bool, error) {
	result, err := rollbackHostingReleaseResult(ctx, token, externalProjectID, externalDeploymentID, digest, key)
	if err != nil {
		return nil, false, err
	}
	if result.StatusCode != http.StatusOK {
		return nil, result.Replayed, decodeHostingRollbackAPIError(result.StatusCode, result.ResponseBody)
	}
	return result.Release, result.Replayed, nil
}

func rollbackHostingReleaseResult(ctx context.Context, token *ServiceToken, externalProjectID,
	externalDeploymentID, digest, key string) (*hostingRollbackResult, error) {
	if token == nil {
		return nil, &hostingAPIError{Code: errCodeServiceTokenRequired, Message: "service token is required", StatusCode: http.StatusUnauthorized}
	}
	requestHash := hashHostingOperation(hostingRollbackOperation, externalProjectID, externalDeploymentID, digest)
	if replay, err := loadIdempotentResponse(ctx, token.ID, hostingRollbackOperation, key, requestHash); err != nil || replay != nil {
		if err != nil {
			return nil, err
		}
		return decodeHostingRollbackReplay(replay, true)
	}
	var publicationMode string
	if err := db.QueryRowContext(ctx, `SELECT publication_mode FROM hosting_projects WHERE external_project_id=?`,
		externalProjectID).Scan(&publicationMode); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &hostingAPIError{Code: errCodeProjectNotFound, Message: "project not found", StatusCode: http.StatusNotFound}
		}
		return nil, err
	}
	if publicationMode == hostingPublicationRuntimeOnlyV1 {
		return rollbackHostingRuntimeOnlyRelease(ctx, token, externalProjectID,
			externalDeploymentID, digest, key, requestHash)
	}
	operation, replay, err := stageHostingRollbackOperation(ctx, token, externalProjectID,
		externalDeploymentID, digest, key, requestHash)
	if err != nil || replay != nil {
		return replay, err
	}
	return runHostingRollbackOperation(ctx, operation.OperationID)
}

func decodeHostingRollbackReplay(replay *hostingIdempotencyResponse, replayed bool) (*hostingRollbackResult, error) {
	if replay == nil || replay.StatusCode < 100 || replay.StatusCode > 599 || len(replay.Body) == 0 {
		return nil, fmt.Errorf("invalid rollback idempotency response")
	}
	result := &hostingRollbackResult{StatusCode: replay.StatusCode,
		ResponseBody: hostingJSONWireResponse(replay.Body), Replayed: replayed}
	if replay.StatusCode == http.StatusOK {
		var release HostingRelease
		if err := json.Unmarshal(replay.Body, &release); err != nil {
			return nil, fmt.Errorf("decode rollback idempotency response: %w", err)
		}
		result.Release = &release
		return result, nil
	}
	var response apiErrorResponse
	if err := json.Unmarshal(replay.Body, &response); err != nil || response.Code == "" || response.Error == "" {
		return nil, fmt.Errorf("decode rollback idempotency error response")
	}
	return result, nil
}

func decodeHostingRollbackAPIError(status int, body []byte) error {
	var response apiErrorResponse
	if err := json.Unmarshal(body, &response); err != nil || response.Code == "" || response.Error == "" {
		return fmt.Errorf("invalid rollback error response")
	}
	return &hostingAPIError{Code: response.Code, Message: response.Error, StatusCode: status}
}

func hostingJSONWireResponse(body []byte) []byte {
	if len(body) > 0 && body[len(body)-1] == '\n' {
		return body
	}
	result := make([]byte, len(body), len(body)+1)
	copy(result, body)
	return append(result, '\n')
}

func encodeHostingRollbackResponse(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return hostingJSONWireResponse(encoded), nil
}

func stageHostingRollbackOperation(ctx context.Context, token *ServiceToken, externalProjectID,
	externalDeploymentID, digest, key, requestHash string) (*hostingRollbackOperationState, *hostingRollbackResult, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	if existing, err := findGenericIdempotencyReplay(ctx, conn, token.ID, hostingRollbackOperation, key, requestHash); err != nil || existing != nil {
		if err != nil {
			return nil, nil, err
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return nil, nil, err
		}
		committed = true
		replay, err := decodeHostingRollbackReplay(existing, true)
		return nil, replay, err
	}

	now := time.Now().UTC()
	var projectID int64
	var desiredState, killReason string
	var globalKill, desiredStateOperationCount int
	if err := conn.QueryRowContext(ctx, `SELECT project.id, project.desired_state,
		project.kill_switch_reason, settings.global_kill_switch,
		(SELECT COUNT(*) FROM hosting_proxy_operations desired_operation
		 WHERE desired_operation.hosting_project_id=project.id
		   AND desired_operation.operation_type IN ('suspend','resume')
		   AND desired_operation.status IN ('pending','applied'))
		FROM hosting_projects project JOIN hosting_settings settings ON settings.id=1
		WHERE project.external_project_id=?`, externalProjectID).Scan(&projectID, &desiredState,
		&killReason, &globalKill, &desiredStateOperationCount); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, &hostingAPIError{Code: errCodeProjectNotFound, Message: "project not found", StatusCode: http.StatusNotFound}
		}
		return nil, nil, err
	}
	if desiredState != "active" || killReason != "" || globalKill != 0 || desiredStateOperationCount != 0 {
		return nil, nil, &hostingAPIError{Code: errCodeProjectSuspended,
			Message: "project execution is disabled or suspended", StatusCode: http.StatusConflict}
	}

	var release HostingRelease
	var releaseID, deploymentID, runtimeRunnerID int64
	var createdAt, evidenceJSON, runtimeJSON, runtimeInstanceID, runtimeFailureCode string
	var runnerStatus, activeSession, observedSession string
	var runnerLastSeen, runtimeObservedAt sql.NullString
	if err := conn.QueryRowContext(ctx, `SELECT release.id, deployment.id, release.release_digest,
		deployment.external_deployment_id, release.commit_sha, release.artifact_digest, release.status,
		release.health_evidence_json, release.route_revision, release.previous_release_digest,
		release.runtime_endpoint, release.created_at, runner.status, runner.last_seen,
		release.runtime_manifest_json, release.runtime_runner_id, release.runtime_instance_id,
		release.runtime_failure_code, runner.active_session_id, release.runtime_observed_at,
		release.runtime_observed_session_id
		FROM hosting_releases release
		JOIN hosting_deployments deployment ON deployment.id=release.hosting_deployment_id
		JOIN hosting_runners runner ON runner.id=release.runtime_runner_id
		WHERE release.hosting_project_id=? AND deployment.external_deployment_id=?
		  AND release.release_digest=?`, projectID, externalDeploymentID, digest).Scan(
		&releaseID, &deploymentID, &release.Digest, &release.ExternalDeploymentID,
		&release.CommitSHA, &release.ArtifactDigest, &release.Status, &evidenceJSON,
		&release.RouteRevision, &release.PreviousRelease, &release.RuntimeEndpoint, &createdAt,
		&runnerStatus, &runnerLastSeen, &runtimeJSON, &runtimeRunnerID, &runtimeInstanceID,
		&runtimeFailureCode, &activeSession, &runtimeObservedAt, &observedSession); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, &hostingAPIError{Code: errCodeReleaseNotFound, Message: "release not found", StatusCode: http.StatusNotFound}
		}
		return nil, nil, err
	}
	if release.Status != "healthy" && release.Status != "inactive" && release.Status != "active" {
		return nil, nil, &hostingAPIError{Code: errCodeReleaseNotHealthy,
			Message: "release is not healthy and cannot be activated", StatusCode: http.StatusConflict}
	}
	if strings.TrimSpace(release.RuntimeEndpoint) == "" {
		return nil, nil, &hostingAPIError{Code: errCodeReleaseNotHealthy,
			Message: "release has no verified runtime endpoint", StatusCode: http.StatusConflict}
	}
	if runnerStatus != "online" || !runnerLastSeen.Valid ||
		parseSQLiteTime(runnerLastSeen.String).Before(now.Add(-hostingRunnerStaleAfter)) || runtimeFailureCode != "" {
		return nil, nil, &hostingAPIError{Code: errCodeReleaseNotHealthy,
			Message: "release runner is not live", StatusCode: http.StatusConflict}
	}
	if activeSession != "" && (!runtimeObservedAt.Valid || observedSession != activeSession ||
		parseSQLiteTime(runtimeObservedAt.String).Before(now.Add(-hostingRunnerStaleAfter))) {
		return nil, nil, &hostingAPIError{Code: errCodeReleaseNotHealthy,
			Message: "current runner session has not recently observed the exact rollback runtime", StatusCode: http.StatusConflict}
	}
	var runtimeManifest HostingRuntimeManifest
	if err := json.Unmarshal([]byte(runtimeJSON), &runtimeManifest); err != nil {
		return nil, nil, fmt.Errorf("decode immutable release runtime: %w", err)
	}
	if err := validateHostingRuntimeManifest(&runtimeManifest); err != nil {
		return nil, nil, fmt.Errorf("validate immutable release runtime: %w", err)
	}

	previousDigest, previousEndpoint := "", ""
	previousErr := conn.QueryRowContext(ctx, `SELECT release_digest, runtime_endpoint
		FROM hosting_releases WHERE hosting_project_id=? AND status='active'`, projectID).Scan(
		&previousDigest, &previousEndpoint)
	if previousErr != nil && !errors.Is(previousErr, sql.ErrNoRows) {
		return nil, nil, previousErr
	}

	legacyOperationID := hashHostingOperation(fmt.Sprint(token.ID), hostingRollbackOperation, key)
	operationID := legacyOperationID
	createOperation := false
	legacyOperation, legacyErr := loadHostingRollbackOperationOn(ctx, conn, legacyOperationID)
	if legacyErr != nil && !errors.Is(legacyErr, sql.ErrNoRows) {
		return nil, nil, legacyErr
	}
	if legacyErr == nil {
		legacyReusable := legacyOperation.Status == "pending" || legacyOperation.Status == "applied" ||
			legacyOperation.UpdatedAt.After(now.Add(-hostingIdempotencyTTL))
		if legacyReusable {
			if legacyOperation.ProjectID != projectID || legacyOperation.DeploymentID != deploymentID ||
				legacyOperation.ReleaseDigest != digest || legacyOperation.ExternalDeploymentID != externalDeploymentID {
				return nil, nil, &hostingAPIError{Code: errCodeIdempotencyConflict,
					Message: "Idempotency-Key was already used with a different request", StatusCode: http.StatusConflict}
			}
		} else {
			createOperation = true
		}
	} else {
		createOperation = true
	}
	if createOperation {
		epoch, err := randomEventID()
		if err != nil {
			return nil, nil, err
		}
		operationID = hashHostingOperation(legacyOperationID, requestHash, epoch)
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_idempotency
		(issuer_token_id, operation, idempotency_key, request_hash, response_status, response_body,
		 operation_reference, created_at, expires_at)
		VALUES (?, ?, ?, ?, NULL, NULL, ?, ?, ?)`, token.ID, hostingRollbackOperation, key,
		requestHash, operationID, formatSQLiteTime(now), formatSQLiteTime(now.Add(hostingIdempotencyTTL))); err != nil {
		if isSQLiteUniqueConstraint(err) {
			return nil, nil, &hostingAPIError{Code: errCodeIdempotencyInProgress,
				Message: "the original request is still being processed", StatusCode: http.StatusConflict, Err: err}
		}
		return nil, nil, err
	}
	if createOperation {
		currentFence, available, err := currentHostingReleaseRuntimeFence(ctx, conn,
			projectID, digest, release.RuntimeEndpoint)
		if err != nil || !available || currentFence.RunnerID != runtimeRunnerID ||
			currentFence.InstanceID != runtimeInstanceID || currentFence.SessionID != activeSession {
			return nil, nil, &hostingAPIError{Code: errCodeReleaseNotHealthy,
				Message:    "rollback runtime identity changed before intent was persisted",
				StatusCode: http.StatusConflict, Err: err}
		}
		routeGeneration, err := nextHostingRouteGeneration(ctx, conn, projectID)
		if err != nil {
			return nil, nil, err
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_proxy_operations
			(operation_id, hosting_project_id, hosting_deployment_id, operation_type, release_digest,
			 runtime_endpoint, expected_previous_release_digest, expected_previous_runtime_endpoint,
			 target_runtime_runner_id, target_runtime_instance_id, target_runtime_session_id,
			 route_generation, status, created_at, updated_at)
			VALUES (?, ?, ?, 'rollback', ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)`, operationID,
			projectID, deploymentID, digest, release.RuntimeEndpoint, previousDigest, previousEndpoint,
			runtimeRunnerID, runtimeInstanceID, activeSession, routeGeneration,
			formatSQLiteTime(now), formatSQLiteTime(now)); err != nil {
			return nil, nil, err
		}
		metadata, _ := json.Marshal(map[string]any{"release_digest": digest,
			"previous_release_digest": previousDigest})
		if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_audit_events
			(issuer_token_id, hosting_project_id, event_type, reason, request_id, metadata_json, created_at)
			VALUES (?, ?, 'hosting_release_rollback_requested', 'control-plane rollback', ?, ?, ?)`,
			token.ID, projectID, requestIDFromContext(ctx), redactSecrets(string(metadata)),
			formatSQLiteTime(now)); err != nil {
			return nil, nil, err
		}
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, nil, err
	}
	committed = true
	operation, err := loadHostingRollbackOperation(ctx, operationID)
	return operation, nil, err
}

func loadHostingRollbackOperation(ctx context.Context, operationID string) (*hostingRollbackOperationState, error) {
	return loadHostingRollbackOperationOn(ctx, db, operationID)
}

func loadHostingRollbackOperationOn(ctx context.Context, querier hostingRouteGenerationQuerier,
	operationID string) (*hostingRollbackOperationState, error) {
	var operation hostingRollbackOperationState
	var runtimeJSON, updatedAt string
	err := querier.QueryRowContext(ctx, `SELECT operation.operation_id, operation.status,
		operation.last_error_code, operation.hosting_project_id, operation.hosting_deployment_id,
		target.id, project.external_project_id, deployment.external_deployment_id,
		operation.release_digest, operation.runtime_endpoint, target.runtime_manifest_json,
		operation.expected_previous_release_digest, operation.expected_previous_runtime_endpoint,
		operation.route_revision, operation.route_generation, project.route_generation,
		operation.target_runtime_runner_id, operation.target_runtime_instance_id,
		operation.target_runtime_session_id, target.runtime_runner_id, target.runtime_instance_id,
		target.runtime_failure_code, project.desired_state, project.kill_switch_reason,
		settings.global_kill_switch, receipt.operation_reference, receipt.response_status,
		receipt.response_body, operation.updated_at
		FROM hosting_proxy_operations operation
		JOIN hosting_projects project ON project.id=operation.hosting_project_id
		JOIN hosting_settings settings ON settings.id=1
		JOIN hosting_deployments deployment ON deployment.id=operation.hosting_deployment_id
		JOIN hosting_releases target ON target.hosting_project_id=operation.hosting_project_id
		 AND target.hosting_deployment_id=operation.hosting_deployment_id
		 AND target.release_digest=operation.release_digest
		LEFT JOIN hosting_idempotency receipt ON receipt.operation_reference=operation.operation_id
		WHERE operation.operation_id=? AND operation.operation_type='rollback'`, operationID).Scan(
		&operation.OperationID, &operation.Status, &operation.LastErrorCode, &operation.ProjectID,
		&operation.DeploymentID, &operation.ReleaseID, &operation.ExternalProjectID,
		&operation.ExternalDeploymentID, &operation.ReleaseDigest, &operation.RuntimeEndpoint,
		&runtimeJSON, &operation.PreviousReleaseDigest, &operation.PreviousRuntimeEndpoint,
		&operation.RouteRevision, &operation.RouteGeneration, &operation.CurrentRouteGeneration,
		&operation.TargetRunnerID, &operation.TargetRuntimeInstanceID,
		&operation.TargetRuntimeSessionID, &operation.CurrentRuntimeRunnerID,
		&operation.CurrentRuntimeInstanceID, &operation.CurrentRuntimeFailureCode,
		&operation.ProjectDesiredState, &operation.ProjectKillSwitchReason,
		&operation.GlobalKillSwitch, &operation.ReceiptOperationReference,
		&operation.ReceiptResponseStatus, &operation.ReceiptResponseBody, &updatedAt)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(runtimeJSON), &operation.Runtime); err != nil {
		return nil, fmt.Errorf("decode rollback runtime: %w", err)
	}
	if err := validateHostingRuntimeManifest(&operation.Runtime); err != nil {
		return nil, fmt.Errorf("validate rollback runtime: %w", err)
	}
	operation.UpdatedAt = parseSQLiteTime(updatedAt)
	return &operation, nil
}

func runHostingRollbackOperation(ctx context.Context, operationID string) (*hostingRollbackResult, error) {
	operation, err := loadHostingRollbackOperation(ctx, operationID)
	if err != nil {
		return nil, err
	}
	if operation.ReceiptResponseStatus.Valid && operation.ReceiptResponseBody.Valid {
		return decodeHostingRollbackReplay(&hostingIdempotencyResponse{StatusCode: int(operation.ReceiptResponseStatus.Int64),
			Body: []byte(operation.ReceiptResponseBody.String)}, false)
	}
	if operation.Status == "committed" {
		return completeHostingRollbackSuccess(ctx, operationID)
	}
	if operation.Status == "failed" {
		return settleHostingRollbackFailure(ctx, operationID, hostingRollbackTerminalError(operation.LastErrorCode))
	}
	if operation.RouteGeneration != operation.CurrentRouteGeneration {
		exists, converged, err := hostingRollbackCompensationConvergence(ctx, operation)
		if err != nil {
			return nil, err
		}
		if exists && !converged {
			return nil, hostingRollbackProxyUnavailable(errors.New("rollback compensation remains pending"))
		}
		if converged && operation.LastErrorCode != "" && operation.LastErrorCode != errCodeProxyUnavailable {
			return settleHostingRollbackFailure(ctx, operationID,
				hostingRollbackTerminalError(operation.LastErrorCode))
		}
		return settleHostingRollbackFailure(ctx, operationID, hostingRollbackTerminalError(errCodeConflict))
	}
	if operation.LastErrorCode != "" && operation.LastErrorCode != errCodeProxyUnavailable {
		return compensateAndSettleHostingRollback(ctx, operation,
			hostingRollbackTerminalError(operation.LastErrorCode))
	}
	if operation.ProjectDesiredState != "active" || operation.ProjectKillSwitchReason != "" ||
		operation.GlobalKillSwitch != 0 {
		return compensateAndSettleHostingRollback(ctx, operation, hostingRollbackTerminalError(errCodeProjectSuspended))
	}
	fence, available, err := currentHostingReleaseRuntimeFence(ctx, db, operation.ProjectID,
		operation.ReleaseDigest, operation.RuntimeEndpoint)
	if err != nil {
		return nil, err
	}
	if !available || !operation.TargetRunnerID.Valid || fence.RunnerID != operation.TargetRunnerID.Int64 ||
		fence.InstanceID != operation.TargetRuntimeInstanceID || fence.SessionID != operation.TargetRuntimeSessionID {
		return compensateAndSettleHostingRollback(ctx, operation, hostingRollbackTerminalError(errCodeReleaseNotHealthy))
	}
	healthPath := operation.Runtime.HealthPath
	if healthPath == "" {
		healthPath = "/"
	}
	healthCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	_, _, healthErr := checkHostingCandidateHealth(healthCtx,
		strings.TrimRight(operation.RuntimeEndpoint, "/")+healthPath)
	cancel()
	if healthErr != nil {
		return compensateAndSettleHostingRollback(ctx, operation, hostingRollbackTerminalError(errCodeReleaseNotHealthy))
	}
	if operation.Status == "pending" {
		proxy, err := hostingProxyClientFactory(appConfig)
		if err != nil {
			_ = markHostingRollbackProxyOperationTransient(ctx, operationID)
			return nil, hostingRollbackProxyUnavailable(err)
		}
		activation, err := proxy.Activate(ctx, proxyActivationRequest{
			OperationID: operation.OperationID, ExternalProjectID: operation.ExternalProjectID,
			ReleaseDigest: operation.ReleaseDigest, RuntimeEndpoint: operation.RuntimeEndpoint,
			ExpectedPreviousReleaseDigest: operation.PreviousReleaseDigest,
			RouteGeneration:               operation.RouteGeneration,
		})
		if err != nil {
			var apiErr *hostingAPIError
			if errors.As(err, &apiErr) && apiErr.Code == errCodeProxyRejected {
				return compensateAndSettleHostingRollback(ctx, operation,
					hostingRollbackTerminalError(errCodeProxyRejected))
			}
			_ = markHostingRollbackProxyOperationTransient(ctx, operationID)
			return nil, hostingRollbackProxyUnavailable(err)
		}
		applied, err := markHostingRollbackProxyOperationApplied(ctx, operationID, activation.RouteRevision)
		if err != nil {
			return nil, err
		}
		if !applied {
			operation, err := loadHostingRollbackOperation(ctx, operationID)
			if err != nil {
				return nil, err
			}
			return compensateAndSettleHostingRollback(ctx, operation,
				hostingRollbackTerminalError(operation.LastErrorCode))
		}
	}
	result, terminal, err := commitHostingRollbackOperation(ctx, operationID)
	if err != nil || result != nil {
		return result, err
	}
	operation, loadErr := loadHostingRollbackOperation(ctx, operationID)
	if loadErr != nil {
		return nil, loadErr
	}
	return compensateAndSettleHostingRollback(ctx, operation, terminal)
}

func hostingRollbackProxyUnavailable(err error) error {
	var apiErr *hostingAPIError
	if errors.As(err, &apiErr) && apiErr.Code == errCodeProxyUnavailable {
		return err
	}
	return &hostingAPIError{Code: errCodeProxyUnavailable,
		Message:    "reverse-proxy adapter is unavailable; rollback remains pending",
		StatusCode: http.StatusServiceUnavailable, Err: err}
}

func compensateAndSettleHostingRollback(ctx context.Context, operation *hostingRollbackOperationState,
	terminal *hostingAPIError) (*hostingRollbackResult, error) {
	if operation.Status == "pending" || operation.Status == "applied" {
		remembered, err := rememberHostingRollbackTerminalIntent(ctx, operation.OperationID, terminal.Code)
		if err != nil {
			return nil, err
		}
		terminal = hostingRollbackTerminalError(remembered)
		operation.LastErrorCode = remembered
		proxy, err := hostingProxyClientFactory(appConfig)
		if err != nil {
			return nil, hostingRollbackProxyUnavailable(err)
		}
		compensationID := hashHostingOperation("compensate", operation.OperationID,
			operation.ExternalDeploymentID, operation.ReleaseDigest)
		err = executeHostingCompensation(ctx, proxy, compensationID, operation.OperationID,
			operation.ProjectID, operation.DeploymentID, operation.ExternalProjectID,
			operation.ReleaseDigest, operation.PreviousReleaseDigest, operation.PreviousRuntimeEndpoint)
		if errors.Is(err, errHostingProxyOperationSuperseded) {
			return settleHostingRollbackFailure(ctx, operation.OperationID, terminal)
		}
		if err != nil {
			return nil, hostingRollbackProxyUnavailable(err)
		}
	}
	return settleHostingRollbackFailure(ctx, operation.OperationID, terminal)
}

func markHostingRollbackProxyOperationTransient(ctx context.Context, operationID string) error {
	_, err := db.ExecContext(ctx, `UPDATE hosting_proxy_operations SET last_error_code=?, updated_at=?
		WHERE operation_id=? AND status='pending' AND last_error_code IN ('', ?)`,
		errCodeProxyUnavailable, formatSQLiteTime(time.Now().UTC()), operationID,
		errCodeProxyUnavailable)
	return err
}

func markHostingRollbackProxyOperationApplied(ctx context.Context, operationID,
	routeRevision string) (bool, error) {
	result, err := db.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='applied',
		route_revision=?, last_error_code='', updated_at=? WHERE operation_id=?
		  AND status IN ('pending','applied') AND last_error_code IN ('', ?)`, routeRevision,
		formatSQLiteTime(time.Now().UTC()), operationID, errCodeProxyUnavailable)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected == 1 {
		return true, nil
	}
	var status, storedRevision, lastError string
	if err := db.QueryRowContext(ctx, `SELECT status, route_revision, last_error_code
		FROM hosting_proxy_operations WHERE operation_id=?`, operationID).Scan(
		&status, &storedRevision, &lastError); err != nil {
		return false, err
	}
	if status == "committed" && storedRevision == routeRevision {
		return true, nil
	}
	if (status == "pending" || status == "applied") && lastError != "" &&
		lastError != errCodeProxyUnavailable {
		return false, nil
	}
	return false, fmt.Errorf("rollback proxy operation is no longer pending")
}

func rememberHostingRollbackTerminalIntent(ctx context.Context, operationID, code string) (string, error) {
	if code == "" || code == errCodeProxyUnavailable {
		code = errCodeConflict
	}
	if _, err := db.ExecContext(ctx, `UPDATE hosting_proxy_operations SET last_error_code=?, updated_at=?
		WHERE operation_id=? AND status IN ('pending','applied')
		  AND last_error_code IN ('', ?)`, code, formatSQLiteTime(time.Now().UTC()),
		operationID, errCodeProxyUnavailable); err != nil {
		return "", err
	}
	var remembered string
	if err := db.QueryRowContext(ctx, `SELECT last_error_code FROM hosting_proxy_operations
		WHERE operation_id=?`, operationID).Scan(&remembered); err != nil {
		return "", err
	}
	if remembered == "" || remembered == errCodeProxyUnavailable {
		return code, nil
	}
	return remembered, nil
}

func hostingRollbackCompensationConvergence(ctx context.Context,
	operation *hostingRollbackOperationState) (bool, bool, error) {
	compensationID := hashHostingOperation("compensate", operation.OperationID,
		operation.ExternalDeploymentID, operation.ReleaseDigest)
	var status string
	err := db.QueryRowContext(ctx, `SELECT status FROM hosting_proxy_operations
		WHERE operation_id=? AND operation_type='compensate'`, compensationID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	switch status {
	case "committed":
		return true, true, nil
	case "pending", "applied":
		return true, false, nil
	}
	correctiveID := hashHostingOperation("suspend-stale-compensation", compensationID)
	err = db.QueryRowContext(ctx, `SELECT status FROM hosting_proxy_operations
		WHERE operation_id=? AND operation_type='compensate'`, correctiveID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	switch status {
	case "committed":
		return true, true, nil
	case "pending", "applied":
		return true, false, nil
	default:
		return false, false, nil
	}
}

func hostingRollbackTerminalError(code string) *hostingAPIError {
	switch code {
	case errCodeProjectSuspended:
		return &hostingAPIError{Code: code, Message: "project execution was disabled during rollback", StatusCode: http.StatusConflict}
	case errCodeReleaseNotHealthy:
		return &hostingAPIError{Code: code, Message: "release is no longer healthy enough to activate", StatusCode: http.StatusConflict}
	case errCodeProxyRejected:
		return &hostingAPIError{Code: code, Message: "reverse-proxy adapter rejected the rollback", StatusCode: http.StatusBadGateway}
	default:
		return &hostingAPIError{Code: errCodeConflict, Message: "a newer routing intent superseded rollback", StatusCode: http.StatusConflict}
	}
}

func settleHostingRollbackFailure(ctx context.Context, operationID string,
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
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	operation, err := loadHostingRollbackOperationOn(ctx, conn, operationID)
	if err != nil {
		return nil, err
	}
	if operation.Status == "committed" {
		result, err := completeHostingRollbackSuccessOn(ctx, conn, operation, false)
		if err != nil {
			return nil, err
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return nil, err
		}
		committed = true
		return result, nil
	}
	now := time.Now().UTC()
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='failed',
		last_error_code=?, updated_at=? WHERE operation_id=? AND status IN ('pending','applied','failed')`,
		apiErr.Code, formatSQLiteTime(now), operationID); err != nil {
		return nil, err
	}
	if err := completeHostingRollbackReceipt(ctx, conn, operationID, apiErr.StatusCode, body, now); err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, err
	}
	committed = true
	return &hostingRollbackResult{StatusCode: apiErr.StatusCode, ResponseBody: body}, nil
}

func commitHostingRollbackOperation(ctx context.Context, operationID string) (*hostingRollbackResult,
	*hostingAPIError, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	operation, err := loadHostingRollbackOperationOn(ctx, conn, operationID)
	if err != nil {
		return nil, nil, err
	}
	if operation.Status == "committed" {
		result, err := completeHostingRollbackSuccessOn(ctx, conn, operation, false)
		if err != nil {
			return nil, nil, err
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return nil, nil, err
		}
		committed = true
		return result, nil, nil
	}
	if operation.Status != "applied" || operation.RouteGeneration != operation.CurrentRouteGeneration {
		return nil, hostingRollbackTerminalError(errCodeConflict), nil
	}
	if operation.LastErrorCode != "" {
		return nil, hostingRollbackTerminalError(operation.LastErrorCode), nil
	}
	if operation.ProjectDesiredState != "active" || operation.ProjectKillSwitchReason != "" ||
		operation.GlobalKillSwitch != 0 {
		return nil, hostingRollbackTerminalError(errCodeProjectSuspended), nil
	}
	fence, available, err := currentHostingReleaseRuntimeFence(ctx, conn, operation.ProjectID,
		operation.ReleaseDigest, operation.RuntimeEndpoint)
	if err != nil {
		return nil, nil, err
	}
	if !available || !operation.TargetRunnerID.Valid || fence.RunnerID != operation.TargetRunnerID.Int64 ||
		fence.InstanceID != operation.TargetRuntimeInstanceID || fence.SessionID != operation.TargetRuntimeSessionID {
		return nil, hostingRollbackTerminalError(errCodeReleaseNotHealthy), nil
	}
	now := time.Now().UTC()
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET status='inactive', deactivated_at=?
		WHERE hosting_project_id=? AND status='active' AND id<>?`, formatSQLiteTime(now),
		operation.ProjectID, operation.ReleaseID); err != nil {
		return nil, nil, err
	}
	result, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET status='active', route_revision=?,
		previous_release_digest=?, activated_at=?, deactivated_at=NULL
		WHERE id=? AND hosting_project_id=? AND hosting_deployment_id=? AND release_digest=?
		  AND status IN ('healthy','inactive','active') AND runtime_runner_id=?
		  AND runtime_instance_id=? AND runtime_failure_code='' AND runtime_missing_since IS NULL
		  AND COALESCE((SELECT active_session_id FROM hosting_runners WHERE id=?), '')=?
		  AND (?='' OR (runtime_observed_at>=? AND runtime_observed_session_id=?))
		  AND ?=(SELECT route_generation FROM hosting_projects WHERE id=?)
		  AND EXISTS (SELECT 1 FROM hosting_proxy_operations proxy_operation
		    WHERE proxy_operation.operation_id=? AND proxy_operation.status='applied'
		      AND proxy_operation.route_generation=? AND proxy_operation.last_error_code='')`, operation.RouteRevision,
		operation.PreviousReleaseDigest, formatSQLiteTime(now), operation.ReleaseID,
		operation.ProjectID, operation.DeploymentID, operation.ReleaseDigest,
		operation.TargetRunnerID.Int64, operation.TargetRuntimeInstanceID,
		operation.TargetRunnerID.Int64, operation.TargetRuntimeSessionID,
		operation.TargetRuntimeSessionID, formatSQLiteTime(now.Add(-hostingRunnerStaleAfter)),
		operation.TargetRuntimeSessionID, operation.RouteGeneration, operation.ProjectID,
		operation.OperationID, operation.RouteGeneration)
	if err != nil {
		return nil, nil, err
	}
	if affected, err := result.RowsAffected(); err != nil {
		return nil, nil, err
	} else if affected != 1 {
		return nil, hostingRollbackTerminalError(errCodeReleaseNotHealthy), nil
	}
	result, err = conn.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='committed',
		last_error_code='', updated_at=? WHERE operation_id=? AND status='applied'
		  AND last_error_code=''
		  AND route_generation=(SELECT route_generation FROM hosting_projects WHERE id=?)`,
		formatSQLiteTime(now), operationID, operation.ProjectID)
	if err != nil {
		return nil, nil, err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		if err != nil {
			return nil, nil, err
		}
		return nil, hostingRollbackTerminalError(errCodeConflict), nil
	}
	resultResponse, err := completeHostingRollbackSuccessOn(ctx, conn, operation, true)
	if err != nil {
		return nil, nil, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, nil, err
	}
	committed = true
	return resultResponse, nil, nil
}

func completeHostingRollbackSuccess(ctx context.Context, operationID string) (*hostingRollbackResult, error) {
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
	operation, err := loadHostingRollbackOperationOn(ctx, conn, operationID)
	if err != nil {
		return nil, err
	}
	result, err := completeHostingRollbackSuccessOn(ctx, conn, operation, false)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, err
	}
	committed = true
	return result, nil
}

func completeHostingRollbackSuccessOn(ctx context.Context, conn *sql.Conn,
	operation *hostingRollbackOperationState, recordEvent bool) (*hostingRollbackResult, error) {
	release, err := getHostingRollbackReleaseOn(ctx, conn, operation.ReleaseID)
	if err != nil {
		return nil, err
	}
	legacyCommitted := operation.Status == "committed" && !operation.ReceiptResponseStatus.Valid
	if release.Status != "active" && !(legacyCommitted && release.Status == "inactive") {
		return nil, errHostingStateConflict
	}
	if legacyCommitted {
		// A pre-receipt operation may be adopted after a newer route has made its
		// release inactive. Reconstruct the original committed success response;
		// do not wedge the newly attached idempotency receipt indefinitely.
		release.Status = "active"
		release.RouteRevision = operation.RouteRevision
		release.PreviousRelease = operation.PreviousReleaseDigest
		activatedAt := operation.UpdatedAt
		release.ActivatedAt = &activatedAt
		release.DeactivatedAt = nil
	}
	body, err := encodeHostingRollbackResponse(release)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	shouldRecordEvent := recordEvent
	if !recordEvent && operation.Status == "committed" && !operation.ReceiptResponseStatus.Valid {
		var existing int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM hosting_events
			WHERE hosting_project_id=? AND event_type='release_rolled_back'
			  AND (json_extract(metadata_json, '$.operation_id')=? OR
			    (created_at=? AND json_extract(metadata_json, '$.release_digest')=?))`, operation.ProjectID,
			operation.OperationID, formatSQLiteTime(operation.UpdatedAt), operation.ReleaseDigest).Scan(&existing); err != nil {
			return nil, err
		}
		shouldRecordEvent = existing == 0
	}
	if shouldRecordEvent {
		metadata, _ := json.Marshal(map[string]string{"operation_id": operation.OperationID,
			"release_digest": operation.ReleaseDigest})
		if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_events
			(hosting_project_id, hosting_deployment_id, event_type, phase, metadata_json, created_at)
			VALUES (?, ?, 'release_rolled_back', 'rolling_back', ?, ?)`, operation.ProjectID,
			operation.DeploymentID, string(metadata), formatSQLiteTime(now)); err != nil {
			return nil, err
		}
	}
	if err := completeHostingRollbackReceipt(ctx, conn, operation.OperationID,
		http.StatusOK, body, now); err != nil {
		return nil, err
	}
	return &hostingRollbackResult{Release: release, StatusCode: http.StatusOK, ResponseBody: body}, nil
}

func getHostingRollbackReleaseOn(ctx context.Context, conn *sql.Conn, releaseID int64) (*HostingRelease, error) {
	var release HostingRelease
	var evidenceJSON, createdAt string
	var activatedAt, deactivatedAt sql.NullString
	if err := conn.QueryRowContext(ctx, `SELECT release.release_digest,
		deployment.external_deployment_id, release.commit_sha, release.artifact_digest,
		release.status, release.health_evidence_json, release.route_revision,
		release.runtime_endpoint, release.previous_release_digest, release.created_at,
		release.activated_at, release.deactivated_at, project.publication_mode
		FROM hosting_releases release JOIN hosting_deployments deployment
		  ON deployment.id=release.hosting_deployment_id
		JOIN hosting_projects project ON project.id=release.hosting_project_id WHERE release.id=?`, releaseID).Scan(
		&release.Digest, &release.ExternalDeploymentID, &release.CommitSHA,
		&release.ArtifactDigest, &release.Status, &evidenceJSON, &release.RouteRevision,
		&release.RuntimeEndpoint, &release.PreviousRelease, &createdAt, &activatedAt,
		&deactivatedAt, &release.PublicationMode); err != nil {
		return nil, err
	}
	if err := decodeNonEmptyHealthEvidence(evidenceJSON, &release.HealthEvidence); err != nil {
		return nil, fmt.Errorf("decode rollback health evidence: %w", err)
	}
	release.CreatedAt = parseSQLiteTime(createdAt)
	release.ActivatedAt = nullableSQLiteTime(activatedAt)
	release.DeactivatedAt = nullableSQLiteTime(deactivatedAt)
	return &release, nil
}

func completeHostingRollbackReceipt(ctx context.Context, conn *sql.Conn, operationID string,
	status int, body []byte, now time.Time) error {
	if status < 100 || status > 599 || len(body) == 0 {
		return fmt.Errorf("invalid rollback idempotency response")
	}
	result, err := conn.ExecContext(ctx, `UPDATE hosting_idempotency SET response_status=?,
		response_body=?, expires_at=? WHERE operation_reference=?
		  AND response_status IS NULL AND response_body IS NULL`, status, string(body),
		formatSQLiteTime(now.Add(hostingIdempotencyTTL)), operationID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 1 {
		return nil
	}
	var storedStatus sql.NullInt64
	var storedBody sql.NullString
	err = conn.QueryRowContext(ctx, `SELECT response_status, response_body FROM hosting_idempotency
		WHERE operation_reference=?`, operationID).Scan(&storedStatus, &storedBody)
	if errors.Is(err, sql.ErrNoRows) {
		// Pre-migration rollback operations have no recoverable issuer/key linkage.
		return nil
	}
	if err != nil {
		return err
	}
	if !storedStatus.Valid || !storedBody.Valid || int(storedStatus.Int64) != status || storedBody.String != string(body) {
		return errHostingStateConflict
	}
	return nil
}

func reconcileHostingRollbackOperations(ctx context.Context) error {
	rows, err := db.QueryContext(ctx, `SELECT operation_id FROM hosting_proxy_operations
		WHERE operation_type='rollback' AND status IN ('pending','applied') ORDER BY id`)
	if err != nil {
		return err
	}
	var operationIDs []string
	for rows.Next() {
		var operationID string
		if err := rows.Scan(&operationID); err != nil {
			rows.Close()
			return err
		}
		operationIDs = append(operationIDs, operationID)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, operationID := range operationIDs {
		if _, err := runHostingRollbackOperation(ctx, operationID); err != nil {
			var apiErr *hostingAPIError
			if errors.As(err, &apiErr) && apiErr.Code == errCodeProxyUnavailable {
				continue
			}
			return err
		}
	}
	return nil
}
