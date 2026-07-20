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

var allowedHostingFailureCodes = map[string]struct{}{
	"artifact_digest_mismatch":     {},
	"artifact_unavailable":         {},
	"build_failed":                 {},
	"build_timeout":                {},
	"cancelled":                    {},
	"health_check_failed":          {},
	"proxy_activation_failed":      {},
	"runner_lost":                  {},
	"secret_reference_unavailable": {},
	"runtime_start_failed":         {},
	"release_persistence_failed":   {},
	"source_fetch_failed":          {},
	"workload_policy_violation":    {},
}

type hostingCompletionState struct {
	JobID                   int64
	DeploymentID            int64
	ProjectID               int64
	RunnerID                int64
	LeaseGeneration         int64
	LeaseTokenHash          string
	JobStatus               string
	ExternalProjectID       string
	ExternalDeploymentID    string
	CommitSHA               string
	ArtifactDigest          string
	CancelRequestedAt       sql.NullString
	LeaseExpiresAt          sql.NullString
	Limits                  hostingWorkloadLimits
	PreviousReleaseDigest   string
	PreviousRuntimeEndpoint string
	ReleaseUploadDigest     string
	ReleaseUploadRelease    string
	ReleaseUploadPath       string
	ReleaseUploadSize       int64
	CompletionFingerprint   string
	RouteGeneration         int64
	Runtime                 HostingRuntimeManifest
}

type hostingRouteGenerationQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func nextHostingRouteGeneration(ctx context.Context, querier hostingRouteGenerationQuerier, projectID int64) (int64, error) {
	var generation int64
	err := querier.QueryRowContext(ctx, `UPDATE hosting_projects SET route_generation=route_generation+1
		WHERE id=? RETURNING route_generation`, projectID).Scan(&generation)
	if err != nil {
		return 0, err
	}
	if generation <= 0 {
		return 0, fmt.Errorf("invalid hosting route generation")
	}
	return generation, nil
}

func completeHostingJob(ctx context.Context, runnerID, jobID, generation int64, leaseToken string, input hostingCompletionRequest) error {
	state, err := getHostingCompletionState(ctx, runnerID, jobID, generation, leaseToken)
	if err == sql.ErrNoRows {
		return &hostingAPIError{Code: errCodeJobForbidden, Message: "valid current hosting job lease is required", StatusCode: http.StatusForbidden}
	}
	if err != nil {
		return err
	}
	input.Status = strings.TrimSpace(input.Status)
	input.FailureCode = strings.TrimSpace(input.FailureCode)
	input.FailureMessage = redactSecrets(strings.TrimSpace(input.FailureMessage))
	input.ReleaseDigest = strings.ToLower(strings.TrimSpace(input.ReleaseDigest))
	input.ReleaseArtifactDigest = strings.ToLower(strings.TrimSpace(input.ReleaseArtifactDigest))
	input.RuntimeEndpoint = strings.TrimSpace(input.RuntimeEndpoint)
	if state.CancelRequestedAt.Valid {
		input = hostingCompletionRequest{Status: "cancelled", FailureCode: "cancelled", FailureMessage: "cancelled by control plane"}
	}
	switch input.Status {
	case "cancelled":
	case "failed":
		if _, ok := allowedHostingFailureCodes[input.FailureCode]; !ok || input.FailureCode == "cancelled" {
			return &hostingAPIError{Code: errCodeInvalidDeployment, Message: "runner returned an unsupported failure_code", StatusCode: http.StatusBadRequest}
		}
	case "success":
		if !validSHA256Digest(input.ReleaseDigest) {
			return &hostingAPIError{Code: errCodeInvalidDeployment, Message: "release_digest must be a sha256 digest", StatusCode: http.StatusBadRequest}
		}
		if !validSHA256Digest(input.ReleaseArtifactDigest) || input.ReleaseArtifactDigest != state.ReleaseUploadDigest ||
			input.ReleaseDigest != state.ReleaseUploadRelease || state.ReleaseUploadSize <= 0 || !isManagedArtifactPath(state.ReleaseUploadPath) {
			return &hostingAPIError{Code: errCodeArtifactDigestMismatch, Message: "completion does not match the persisted release artifact", StatusCode: http.StatusConflict}
		}
		if err := validateRuntimeEndpoint(input.RuntimeEndpoint); err != nil {
			return &hostingAPIError{Code: errCodeInvalidDeployment, Message: err.Error(), StatusCode: http.StatusBadRequest}
		}
	default:
		return &hostingAPIError{Code: errCodeInvalidDeployment, Message: "completion status must be success, failed, or cancelled", StatusCode: http.StatusBadRequest}
	}
	encodedCompletion, err := json.Marshal(input)
	if err != nil {
		return err
	}
	completionFingerprint := hashHostingOperation("hosting.job.complete", string(encodedCompletion))
	if state.JobStatus == "succeeded" || state.JobStatus == "failed" || state.JobStatus == "cancelled" {
		return validateTerminalHostingJobReplay(state, input.Status, completionFingerprint)
	}
	if !state.LeaseExpiresAt.Valid || !parseSQLiteTime(state.LeaseExpiresAt.String).After(time.Now().UTC()) {
		return &hostingAPIError{Code: errCodeJobForbidden, Message: "hosting job lease has expired", StatusCode: http.StatusForbidden}
	}
	result, err := db.ExecContext(ctx, `UPDATE hosting_jobs SET completion_fingerprint=?
		WHERE id=? AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=?
		AND status IN ('leased','running') AND lease_expires_at>?
		AND (completion_fingerprint='' OR completion_fingerprint=?)`, completionFingerprint,
		state.JobID, state.RunnerID, state.LeaseGeneration, state.LeaseTokenHash,
		formatSQLiteTime(time.Now().UTC()), completionFingerprint)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		latest, latestErr := getHostingCompletionState(ctx, runnerID, jobID, generation, leaseToken)
		if latestErr != nil {
			return &hostingAPIError{Code: errCodeJobForbidden, Message: "valid current hosting job lease is required", StatusCode: http.StatusForbidden, Err: latestErr}
		}
		if latest.JobStatus == "succeeded" || latest.JobStatus == "failed" || latest.JobStatus == "cancelled" {
			return validateTerminalHostingJobReplay(latest, input.Status, completionFingerprint)
		}
		if latest.CompletionFingerprint != "" && latest.CompletionFingerprint != completionFingerprint {
			return &hostingAPIError{Code: errCodeIdempotencyConflict, Message: "job completion conflicts with the durable completion intent", StatusCode: http.StatusConflict}
		}
		return &hostingAPIError{Code: errCodeJobForbidden, Message: "hosting job lease is no longer current", StatusCode: http.StatusForbidden}
	}
	state.CompletionFingerprint = completionFingerprint
	if input.Status == "cancelled" {
		return finishHostingJobTerminal(ctx, state, hostingStatusCancelled, hostingPhaseCancelled, "cancelled", input.FailureMessage)
	}
	if input.Status == "failed" {
		return finishHostingJobTerminal(ctx, state, hostingStatusFailed, hostingPhaseFailed, input.FailureCode, input.FailureMessage)
	}
	if !validHealthEvidence(input.HealthEvidence) {
		return finishHostingJobTerminal(ctx, state, hostingStatusFailed, hostingPhaseFailed, "health_check_failed", "candidate did not provide successful health evidence")
	}
	if err := stageHealthyHostingRelease(ctx, state, input); err != nil {
		return err
	}
	if err := verifyStagedHostingCandidate(ctx, state, input.ReleaseDigest, input.RuntimeEndpoint); err != nil {
		return finishHostingJobTerminal(ctx, state, hostingStatusFailed, hostingPhaseFailed,
			"health_check_failed", err.Error())
	}

	state, err = getHostingCompletionState(ctx, runnerID, jobID, generation, leaseToken)
	if err != nil {
		return err
	}
	allowed, err := hostingActivationAllowed(ctx, state.ProjectID)
	if err != nil {
		return err
	}
	if !allowed {
		proxy, proxyErr := hostingProxyClientFactory(appConfig)
		if proxyErr != nil {
			return proxyErr
		}
		operationID := hashHostingOperation("activate", state.ExternalDeploymentID, input.ReleaseDigest)
		if _, err := compensateHostingActivationIfCurrent(ctx, proxy, operationID, state, input.ReleaseDigest); err != nil {
			return err
		}
		if err := markHostingProxyOperationCommitted(ctx, operationID); err != nil {
			return err
		}
		return finishHostingJobTerminal(ctx, state, hostingStatusCancelled, hostingPhaseCancelled, "cancelled", "project execution is disabled or suspended")
	}
	proxy, err := hostingProxyClientFactory(appConfig)
	if err != nil {
		return failHostingActivation(ctx, state, input.ReleaseDigest, err)
	}
	operationID := hashHostingOperation("activate", state.ExternalDeploymentID, input.ReleaseDigest)
	activation, err := proxy.Activate(ctx, proxyActivationRequest{
		OperationID:                   operationID,
		ExternalProjectID:             state.ExternalProjectID,
		ReleaseDigest:                 input.ReleaseDigest,
		RuntimeEndpoint:               input.RuntimeEndpoint,
		ExpectedPreviousReleaseDigest: state.PreviousReleaseDigest,
		RouteGeneration:               state.RouteGeneration,
	})
	if err != nil {
		errorCode := errCodeProxyUnavailable
		var apiErr *hostingAPIError
		if errorsAsHosting(err, &apiErr) {
			errorCode = apiErr.Code
		}
		_ = markHostingProxyOperationError(ctx, operationID, errorCode)
		if errorCode == errCodeProxyRejected {
			_ = markHostingProxyOperationFailed(ctx, operationID, errorCode)
			return failHostingActivation(ctx, state, input.ReleaseDigest, err)
		}
		return err
	}
	current, err := hostingProxyOperationIsCurrent(ctx, operationID)
	if err != nil {
		return err
	}
	if !current {
		if err := markHostingProxyOperationFailed(ctx, operationID, errCodeConflict); err != nil {
			return err
		}
		return finishHostingJobTerminal(ctx, state, hostingStatusCancelled, hostingPhaseCancelled,
			"cancelled", "a newer routing intent superseded candidate activation")
	}
	if err := markHostingProxyOperationApplied(ctx, operationID, activation.RouteRevision); err != nil {
		return err
	}

	cancelled, err := activateHealthyHostingRelease(ctx, state, input.ReleaseDigest, activation.RouteRevision)
	if err != nil {
		current, currentErr := hostingProxyOperationIsCurrent(ctx, operationID)
		if currentErr != nil {
			return currentErr
		}
		if !current {
			_ = markHostingProxyOperationFailed(ctx, operationID, errCodeConflict)
			return finishHostingJobTerminal(ctx, state, hostingStatusCancelled, hostingPhaseCancelled,
				"cancelled", "a newer routing intent superseded candidate activation")
		}
		if _, compensateErr := compensateHostingActivationIfCurrent(ctx, proxy, operationID, state, input.ReleaseDigest); compensateErr != nil {
			return &hostingAPIError{Code: errCodeProxyRejected,
				Message: "candidate commit failed and proxy compensation failed", StatusCode: http.StatusBadGateway, Err: compensateErr}
		}
		_ = markHostingProxyOperationFailed(ctx, operationID, errCodeReleaseNotHealthy)
		if finishErr := finishHostingJobTerminal(ctx, state, hostingStatusFailed, hostingPhaseFailed,
			"health_check_failed", "candidate identity changed before activation committed"); finishErr != nil {
			return finishErr
		}
		return nil
	}
	if !cancelled {
		return nil
	}
	if _, err := compensateHostingActivationIfCurrent(ctx, proxy, operationID, state, input.ReleaseDigest); err != nil {
		return &hostingAPIError{Code: errCodeProxyRejected, Message: "candidate activation was cancelled but proxy compensation failed", StatusCode: http.StatusBadGateway, Err: err}
	}
	if err := markHostingProxyOperationCommitted(ctx, operationID); err != nil {
		return err
	}
	return finishHostingJobTerminal(ctx, state, hostingStatusCancelled, hostingPhaseCancelled, "cancelled", "cancelled during activation")
}

func validateTerminalHostingJobReplay(state *hostingCompletionState, inputStatus, completionFingerprint string) error {
	if state.CompletionFingerprint != "" && state.CompletionFingerprint != completionFingerprint {
		return &hostingAPIError{Code: errCodeIdempotencyConflict, Message: "terminal job completion conflicts with the committed result", StatusCode: http.StatusConflict}
	}
	if state.CompletionFingerprint == "" {
		expected := map[string]string{"succeeded": "success", "failed": "failed", "cancelled": "cancelled"}[state.JobStatus]
		if inputStatus != expected {
			return &hostingAPIError{Code: errCodeIdempotencyConflict, Message: "terminal job completion conflicts with the committed result", StatusCode: http.StatusConflict}
		}
	}
	return nil
}

func getHostingCompletionState(ctx context.Context, runnerID, jobID, generation int64, leaseToken string) (*hostingCompletionState, error) {
	var state hostingCompletionState
	err := db.QueryRowContext(ctx, `SELECT j.id, j.hosting_deployment_id, d.hosting_project_id,
		j.hosting_runner_id, j.lease_generation, j.lease_token_hash, j.status,
		p.external_project_id, d.external_deployment_id, d.commit_sha, d.artifact_digest,
		j.cancel_requested_at, j.lease_expires_at,
		j.required_cpu_millis, j.required_ram_bytes, j.required_disk_bytes, j.required_pids,
		j.release_upload_digest, j.release_upload_release_digest, j.release_upload_path, j.release_upload_size,
		j.completion_fingerprint
		FROM hosting_jobs j
		JOIN hosting_deployments d ON d.id=j.hosting_deployment_id
		JOIN hosting_projects p ON p.id=d.hosting_project_id
		WHERE j.id=? AND j.hosting_runner_id=? AND j.lease_generation=? AND j.lease_token_hash=?`,
		jobID, runnerID, generation, hashToken(leaseToken)).Scan(
		&state.JobID, &state.DeploymentID, &state.ProjectID, &state.RunnerID,
		&state.LeaseGeneration, &state.LeaseTokenHash, &state.JobStatus,
		&state.ExternalProjectID, &state.ExternalDeploymentID, &state.CommitSHA, &state.ArtifactDigest,
		&state.CancelRequestedAt, &state.LeaseExpiresAt,
		&state.Limits.CPUMillis, &state.Limits.RAMBytes, &state.Limits.DiskBytes, &state.Limits.PIDs,
		&state.ReleaseUploadDigest, &state.ReleaseUploadRelease, &state.ReleaseUploadPath, &state.ReleaseUploadSize,
		&state.CompletionFingerprint)
	if err != nil {
		return nil, err
	}
	_ = db.QueryRowContext(ctx, `SELECT release_digest, runtime_endpoint FROM hosting_releases
		WHERE hosting_project_id=? AND status='active'`, state.ProjectID).Scan(&state.PreviousReleaseDigest, &state.PreviousRuntimeEndpoint)
	_ = db.QueryRowContext(ctx, `SELECT route_generation FROM hosting_proxy_operations
		WHERE hosting_deployment_id=? AND operation_type='activate' ORDER BY id DESC LIMIT 1`,
		state.DeploymentID).Scan(&state.RouteGeneration)
	return &state, nil
}

func validHealthEvidence(evidence map[string]any) bool {
	healthy, ok := evidence["healthy"].(bool)
	if !ok || !healthy {
		return false
	}
	attempts, ok := evidence["attempts"].(float64)
	return ok && attempts >= 1 && attempts <= 100
}

func stageHealthyHostingRelease(ctx context.Context, state *hostingCompletionState, input hostingCompletionRequest) error {
	healthJSON, err := json.Marshal(input.HealthEvidence)
	if err != nil {
		return err
	}
	healthJSON = []byte(redactSecrets(string(healthJSON)))
	now := time.Now().UTC()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	var status, recipeJSON, observedSession, observedDigest, observedInstance string
	var cancelRequested sql.NullString
	var observedAt sql.NullString
	err = conn.QueryRowContext(ctx, `SELECT status, cancel_requested_at, recipe_json,
		runtime_observed_at, runtime_observed_session_id, runtime_observed_release_digest,
		runtime_observed_instance_id FROM hosting_jobs
		WHERE id=? AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=? AND lease_expires_at>?`,
		state.JobID, state.RunnerID, state.LeaseGeneration, state.LeaseTokenHash, formatSQLiteTime(now)).Scan(
		&status, &cancelRequested, &recipeJSON, &observedAt, &observedSession, &observedDigest, &observedInstance)
	if err != nil {
		return err
	}
	if cancelRequested.Valid {
		return &hostingAPIError{Code: errCodeProjectBusy, Message: "deployment cancellation is in progress", StatusCode: http.StatusConflict}
	}
	if status != "leased" && status != "running" {
		return &hostingAPIError{Code: errCodeConflict, Message: "hosting job is no longer running", StatusCode: http.StatusConflict}
	}
	var recipe hostingJobRecipe
	if err := json.Unmarshal([]byte(recipeJSON), &recipe); err != nil {
		return fmt.Errorf("decode immutable hosting recipe: %w", err)
	}
	state.Runtime = recipe.Runtime
	var activeSession, runnerStatus string
	var runnerLastSeen sql.NullString
	if err := conn.QueryRowContext(ctx, `SELECT active_session_id, status, last_seen
		FROM hosting_runners WHERE id=?`, state.RunnerID).Scan(&activeSession, &runnerStatus, &runnerLastSeen); err != nil {
		return err
	}
	expectedInstance := fmt.Sprintf("build-%d-%d", state.JobID, state.LeaseGeneration)
	if activeSession != "" && (runnerStatus != "online" || !runnerLastSeen.Valid ||
		parseSQLiteTime(runnerLastSeen.String).Before(now.Add(-hostingRunnerStaleAfter)) ||
		!observedAt.Valid || parseSQLiteTime(observedAt.String).Before(now.Add(-hostingRunnerStaleAfter)) ||
		observedSession != activeSession || observedDigest != input.ReleaseDigest || observedInstance != expectedInstance) {
		return &hostingAPIError{Code: errCodeReleaseNotHealthy,
			Message: "current runner session has not observed the exact candidate runtime", StatusCode: http.StatusConflict}
	}
	runtimeJSON, err := json.Marshal(recipe.Runtime)
	if err != nil {
		return fmt.Errorf("encode immutable runtime manifest: %w", err)
	}
	artifactInfo, err := currentArtifactStorage().Stat(state.ReleaseUploadPath)
	if err != nil || artifactInfo.Size() != state.ReleaseUploadSize {
		return &hostingAPIError{Code: errCodeArtifactUnavailable, Message: "persisted release artifact is unavailable", StatusCode: http.StatusConflict, Err: err}
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_release_artifacts
		(artifact_digest, release_digest, artifact_path, size_bytes, created_at)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT(artifact_digest) DO NOTHING`, input.ReleaseArtifactDigest,
		input.ReleaseDigest, state.ReleaseUploadPath, state.ReleaseUploadSize, formatSQLiteTime(now)); err != nil {
		return err
	}
	var storedReleaseDigest, storedPath string
	var storedSize int64
	if err := conn.QueryRowContext(ctx, `SELECT release_digest, artifact_path, size_bytes
		FROM hosting_release_artifacts WHERE artifact_digest=?`, input.ReleaseArtifactDigest).Scan(
		&storedReleaseDigest, &storedPath, &storedSize); err != nil {
		return err
	}
	if storedReleaseDigest != input.ReleaseDigest || storedPath != state.ReleaseUploadPath || storedSize != state.ReleaseUploadSize {
		return &hostingAPIError{Code: errCodeArtifactDigestMismatch, Message: "release artifact identity conflicts with persisted content", StatusCode: http.StatusConflict}
	}
	var existingDeploymentID int64
	err = conn.QueryRowContext(ctx, `SELECT hosting_deployment_id FROM hosting_releases WHERE hosting_deployment_id=?`, state.DeploymentID).Scan(&existingDeploymentID)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == sql.ErrNoRows {
		if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_releases
			(hosting_project_id, hosting_deployment_id, release_digest, release_artifact_digest, commit_sha, artifact_digest,
			 status, health_evidence_json, previous_release_digest, runtime_endpoint, runtime_manifest_json,
			 runtime_runner_id, runtime_instance_id, runtime_observed_at, runtime_observed_session_id, created_at)
			VALUES (?, ?, ?, ?, ?, ?, 'healthy', ?, ?, ?, ?, ?, ?, ?, ?, ?)`, state.ProjectID, state.DeploymentID,
			input.ReleaseDigest, input.ReleaseArtifactDigest, state.CommitSHA, state.ArtifactDigest, string(healthJSON),
			state.PreviousReleaseDigest, input.RuntimeEndpoint, string(runtimeJSON), state.RunnerID,
			expectedInstance, nullableSQLiteTimeValue(observedAt), observedSession, formatSQLiteTime(now)); err != nil {
			return err
		}
	}
	result, err := conn.ExecContext(ctx, `UPDATE hosting_jobs SET status='running', lease_expires_at=?
		WHERE id=? AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=? AND lease_expires_at>?`,
		formatSQLiteTime(now.Add(hostingJobLeaseDuration)), state.JobID, state.RunnerID, state.LeaseGeneration, state.LeaseTokenHash, formatSQLiteTime(now))
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return &hostingAPIError{Code: errCodeJobForbidden, Message: "hosting job lease is no longer current", StatusCode: http.StatusForbidden}
	}
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_deployments SET phase='activating', release_digest=?,
		previous_release_digest=?, updated_at=? WHERE id=? AND status='running'`, input.ReleaseDigest,
		state.PreviousReleaseDigest, formatSQLiteTime(now), state.DeploymentID); err != nil {
		return err
	}
	if err := recordHostingEvent(ctx, conn, state.ProjectID, state.DeploymentID, "candidate_healthy", hostingPhaseActivating, "", map[string]any{"release_digest": input.ReleaseDigest}, now); err != nil {
		return err
	}
	operationID := hashHostingOperation("activate", state.ExternalDeploymentID, input.ReleaseDigest)
	var routeGeneration int64
	var storedDigest, storedEndpoint string
	err = conn.QueryRowContext(ctx, `SELECT route_generation, release_digest, runtime_endpoint
		FROM hosting_proxy_operations WHERE operation_id=?`, operationID).Scan(&routeGeneration, &storedDigest, &storedEndpoint)
	if err == sql.ErrNoRows {
		routeGeneration, err = nextHostingRouteGeneration(ctx, conn, state.ProjectID)
		if err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_proxy_operations
			(operation_id, hosting_project_id, hosting_deployment_id, operation_type, release_digest,
			 runtime_endpoint, expected_previous_release_digest, route_generation, status, created_at, updated_at)
			VALUES (?, ?, ?, 'activate', ?, ?, ?, ?, 'pending', ?, ?)`, operationID, state.ProjectID, state.DeploymentID,
			input.ReleaseDigest, input.RuntimeEndpoint, state.PreviousReleaseDigest, routeGeneration,
			formatSQLiteTime(now), formatSQLiteTime(now)); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if storedDigest != input.ReleaseDigest || storedEndpoint != input.RuntimeEndpoint {
		return &hostingAPIError{Code: errCodeIdempotencyConflict, Message: "activation intent conflicts with the durable operation", StatusCode: http.StatusConflict}
	}
	state.RouteGeneration = routeGeneration
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return err
	}
	committed = true
	return nil
}

func nullableSQLiteTimeValue(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return value.String
}

func verifyStagedHostingCandidate(ctx context.Context, state *hostingCompletionState, releaseDigest, endpoint string) error {
	var runnerStatus, activeSession, observedSession, runtimeFailureCode, runtimeInstanceID string
	var runnerLastSeen, observedAt sql.NullString
	err := db.QueryRowContext(ctx, `SELECT runner.status, runner.last_seen, runner.active_session_id,
		release.runtime_observed_at, release.runtime_observed_session_id, release.runtime_failure_code,
		release.runtime_instance_id
		FROM hosting_releases release
		JOIN hosting_runners runner ON runner.id=release.runtime_runner_id
		WHERE release.hosting_project_id=? AND release.hosting_deployment_id=?
		  AND release.release_digest=? AND release.runtime_endpoint=? AND release.status='healthy'
		  AND release.runtime_runner_id=?`, state.ProjectID, state.DeploymentID, releaseDigest, endpoint,
		state.RunnerID).Scan(&runnerStatus, &runnerLastSeen, &activeSession, &observedAt,
		&observedSession, &runtimeFailureCode, &runtimeInstanceID)
	if err != nil {
		return fmt.Errorf("candidate identity is no longer current: %w", err)
	}
	now := time.Now().UTC()
	expectedInstance := fmt.Sprintf("build-%d-%d", state.JobID, state.LeaseGeneration)
	if runnerStatus != "online" || !runnerLastSeen.Valid ||
		parseSQLiteTime(runnerLastSeen.String).Before(now.Add(-hostingRunnerStaleAfter)) ||
		runtimeFailureCode != "" || runtimeInstanceID != expectedInstance {
		return fmt.Errorf("candidate runner or exact runtime instance is no longer live")
	}
	if activeSession != "" && (!observedAt.Valid ||
		parseSQLiteTime(observedAt.String).Before(now.Add(-hostingRunnerStaleAfter)) || observedSession != activeSession) {
		return fmt.Errorf("current runner session has not recently observed the exact candidate runtime")
	}
	// Legacy agents predate authoritative inventory sessions. Preserve their
	// existing completion contract during the rolling upgrade; every
	// inventory-capable session is subject to the fresh control-plane gate.
	if activeSession == "" {
		return nil
	}
	healthPath := state.Runtime.HealthPath
	if healthPath == "" {
		healthPath = "/"
	}
	healthCtx, cancel := context.WithTimeout(ctx, hostingActivationReconcileHealthTimeout)
	defer cancel()
	if _, _, err := checkHostingCandidateHealth(healthCtx, strings.TrimRight(endpoint, "/")+healthPath); err != nil {
		return fmt.Errorf("candidate failed control-plane health gate: %w", err)
	}
	return nil
}

func activateHealthyHostingRelease(ctx context.Context, state *hostingCompletionState, releaseDigest, routeRevision string) (bool, error) {
	now := time.Now().UTC()
	conn, err := db.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return false, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	var cancelRequested sql.NullString
	var status string
	if err := conn.QueryRowContext(ctx, `SELECT status, cancel_requested_at FROM hosting_jobs
		WHERE id=? AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=?`,
		state.JobID, state.RunnerID, state.LeaseGeneration, state.LeaseTokenHash).Scan(&status, &cancelRequested); err != nil {
		return false, err
	}
	if cancelRequested.Valid {
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return false, err
		}
		committed = true
		return true, nil
	}
	if status != "running" {
		return false, &hostingAPIError{Code: errCodeConflict, Message: "hosting job is no longer running", StatusCode: http.StatusConflict}
	}
	var desiredState, projectKillReason string
	var globalKill int
	if err := conn.QueryRowContext(ctx, `SELECT p.desired_state, p.kill_switch_reason, s.global_kill_switch
		FROM hosting_projects p JOIN hosting_settings s ON s.id=1 WHERE p.id=?`, state.ProjectID).Scan(&desiredState, &projectKillReason, &globalKill); err != nil {
		return false, err
	}
	if desiredState != "active" || projectKillReason != "" || globalKill != 0 {
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return false, err
		}
		committed = true
		return true, nil
	}
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET status='inactive', deactivated_at=?
		WHERE hosting_project_id=? AND status='active' AND hosting_deployment_id<>?`, formatSQLiteTime(now), state.ProjectID, state.DeploymentID); err != nil {
		return false, err
	}
	expectedInstance := fmt.Sprintf("build-%d-%d", state.JobID, state.LeaseGeneration)
	result, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET status='active', route_revision=?, activated_at=?,
		deactivated_at=NULL, runtime_generation=runtime_generation+1
		WHERE hosting_project_id=? AND hosting_deployment_id=? AND release_digest=? AND status='healthy'
		  AND runtime_runner_id=? AND runtime_instance_id=? AND runtime_failure_code=''
		  AND (COALESCE((SELECT active_session_id FROM hosting_runners WHERE id=?), '')=''
		    OR (runtime_observed_at>=? AND runtime_observed_session_id=(SELECT active_session_id FROM hosting_runners WHERE id=?)))
		  AND ?=(SELECT route_generation FROM hosting_projects WHERE id=?)
		  AND EXISTS (SELECT 1 FROM hosting_proxy_operations operation
		    WHERE operation.operation_id=? AND operation.status='applied' AND operation.route_generation=?)`,
		routeRevision, formatSQLiteTime(now), state.ProjectID, state.DeploymentID, releaseDigest,
		state.RunnerID, expectedInstance, state.RunnerID,
		formatSQLiteTime(now.Add(-hostingRunnerStaleAfter)), state.RunnerID, state.RouteGeneration,
		state.ProjectID, hashHostingOperation("activate", state.ExternalDeploymentID, releaseDigest), state.RouteGeneration)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return false, &hostingAPIError{Code: errCodeReleaseNotHealthy, Message: "healthy candidate was not available for activation", StatusCode: http.StatusConflict}
	}
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_jobs SET status='succeeded', completed_at=?, lease_expires_at=NULL
		WHERE id=? AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=?`, formatSQLiteTime(now),
		state.JobID, state.RunnerID, state.LeaseGeneration, state.LeaseTokenHash); err != nil {
		return false, err
	}
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_deployments SET status='active', phase='active', callback_state='pending',
		release_digest=?, finished_at=?, updated_at=? WHERE id=? AND status='running'`, releaseDigest,
		formatSQLiteTime(now), formatSQLiteTime(now), state.DeploymentID); err != nil {
		return false, err
	}
	if err := recordHostingEvent(ctx, conn, state.ProjectID, state.DeploymentID, "release_activated", hostingPhaseActive, "", map[string]any{"release_digest": releaseDigest, "route_revision": routeRevision}, now); err != nil {
		return false, err
	}
	if err := enqueueTerminalCallback(ctx, conn, state.DeploymentID, now); err != nil {
		return false, err
	}
	operationID := hashHostingOperation("activate", state.ExternalDeploymentID, releaseDigest)
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='committed',
		updated_at=? WHERE operation_id=? AND status IN ('pending','applied','committed')`, formatSQLiteTime(now), operationID); err != nil {
		return false, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return false, err
	}
	committed = true
	return false, nil
}

func failHostingActivation(ctx context.Context, state *hostingCompletionState, releaseDigest string, activationErr error) error {
	message := redactSecrets(activationErr.Error())
	if err := finishHostingJobTerminal(ctx, state, hostingStatusFailed, hostingPhaseFailed, "proxy_activation_failed", message); err != nil {
		return err
	}
	_, _ = db.ExecContext(ctx, `UPDATE hosting_releases SET status='failed' WHERE hosting_deployment_id=? AND release_digest=? AND status='healthy'`, state.DeploymentID, releaseDigest)
	return nil
}

func finishHostingJobTerminal(ctx context.Context, state *hostingCompletionState, status, phase, failureCode, failureMessage string) error {
	now := time.Now().UTC()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	jobStatus := "failed"
	if status == hostingStatusCancelled {
		jobStatus = "cancelled"
	}
	result, err := conn.ExecContext(ctx, `UPDATE hosting_jobs SET status=?, completed_at=?, lease_expires_at=NULL
		WHERE id=? AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=?
		  AND status IN ('leased','running')`, jobStatus, formatSQLiteTime(now), state.JobID,
		state.RunnerID, state.LeaseGeneration, state.LeaseTokenHash)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		var existing string
		if err := conn.QueryRowContext(ctx, `SELECT status FROM hosting_jobs WHERE id=?`, state.JobID).Scan(&existing); err == nil && (existing == "failed" || existing == "cancelled" || existing == "succeeded") {
			if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
				return err
			}
			committed = true
			return nil
		}
		return &hostingAPIError{Code: errCodeJobForbidden, Message: "hosting job lease is no longer current", StatusCode: http.StatusForbidden}
	}
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_deployments SET status=?, phase=?, failure_code=?, failure_message=?,
		callback_state='pending', finished_at=?, updated_at=? WHERE id=? AND status IN ('queued','running')`, status, phase,
		failureCode, redactSecrets(failureMessage), formatSQLiteTime(now), formatSQLiteTime(now), state.DeploymentID); err != nil {
		return err
	}
	if err := restoreHostingRunnerCapacity(ctx, conn, state.RunnerID, state.Limits); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET status='failed' WHERE hosting_deployment_id=? AND status IN ('candidate','healthy')`, state.DeploymentID); err != nil {
		return err
	}
	if err := recordHostingEvent(ctx, conn, state.ProjectID, state.DeploymentID, "deployment_"+status, phase, failureCode, nil, now); err != nil {
		return err
	}
	if err := enqueueTerminalCallback(ctx, conn, state.DeploymentID, now); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return err
	}
	committed = true
	return nil
}

func compensateHostingActivationIfCurrent(ctx context.Context, proxy hostingProxyClient, operationID string,
	state *hostingCompletionState, candidateDigest string) (bool, error) {
	compensationID := hashHostingOperation("compensate", operationID, state.ExternalDeploymentID, candidateDigest)
	err := executeHostingCompensation(ctx, proxy, compensationID, operationID, state.ProjectID, state.DeploymentID,
		state.ExternalProjectID, candidateDigest, state.PreviousReleaseDigest, state.PreviousRuntimeEndpoint)
	if errors.Is(err, errHostingProxyOperationSuperseded) {
		if err := markHostingProxyOperationFailed(ctx, operationID, errCodeConflict); err != nil {
			return false, err
		}
		return false, nil
	}
	if err != nil {
		return true, err
	}
	return true, nil
}

func executeHostingCompensation(ctx context.Context, proxy hostingProxyClient, operationID, sourceOperationID string,
	projectID, deploymentID int64, externalProjectID, candidateDigest, previousDigest, previousEndpoint string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	var generation int64
	var storedDigest, storedEndpoint, storedStatus string
	var targetRunnerID sql.NullInt64
	var targetInstance, targetSession string
	err = conn.QueryRowContext(ctx, `SELECT route_generation, release_digest, runtime_endpoint, status,
		target_runtime_runner_id, target_runtime_instance_id, target_runtime_session_id
		FROM hosting_proxy_operations WHERE operation_id=? AND operation_type='compensate'`, operationID).Scan(
		&generation, &storedDigest, &storedEndpoint, &storedStatus, &targetRunnerID, &targetInstance, &targetSession)
	if err == sql.ErrNoRows {
		if sourceOperationID != "" {
			var sourceCurrent int
			sourceErr := conn.QueryRowContext(ctx, `SELECT CASE
				WHEN source.route_generation=project.route_generation AND source.status IN ('pending','applied')
				THEN 1 ELSE 0 END
				FROM hosting_proxy_operations source JOIN hosting_projects project
				  ON project.id=source.hosting_project_id
				WHERE source.operation_id=? AND source.hosting_project_id=?`, sourceOperationID, projectID).Scan(&sourceCurrent)
			if sourceErr == sql.ErrNoRows || (sourceErr == nil && sourceCurrent == 0) {
				if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
					return err
				}
				committed = true
				return errHostingProxyOperationSuperseded
			}
			if sourceErr != nil {
				return sourceErr
			}
		}
		var desired, kill string
		var global int
		if err := conn.QueryRowContext(ctx, `SELECT project.desired_state, project.kill_switch_reason,
			settings.global_kill_switch FROM hosting_projects project JOIN hosting_settings settings ON settings.id=1
			WHERE project.id=?`, projectID).Scan(&desired, &kill, &global); err != nil {
			return err
		}
		storedDigest, storedEndpoint = previousDigest, previousEndpoint
		if desired != "active" || kill != "" || global != 0 || storedDigest == "" || storedEndpoint == "" {
			storedDigest, storedEndpoint = "", ""
		} else {
			fence, available, fenceErr := currentHostingReleaseRuntimeFence(ctx, conn, projectID, storedDigest, storedEndpoint)
			if fenceErr != nil {
				return fenceErr
			}
			if !available {
				storedDigest, storedEndpoint = "", ""
			} else {
				targetRunnerID = sql.NullInt64{Int64: fence.RunnerID, Valid: true}
				targetInstance, targetSession = fence.InstanceID, fence.SessionID
			}
		}
		generation, err = nextHostingRouteGeneration(ctx, conn, projectID)
		if err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_proxy_operations
			(operation_id, hosting_project_id, hosting_deployment_id, operation_type, release_digest,
			 runtime_endpoint, expected_previous_release_digest, target_runtime_runner_id,
			 target_runtime_instance_id, target_runtime_session_id, route_generation, status, created_at, updated_at)
			VALUES (?, ?, NULLIF(?, 0), 'compensate', ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)`, operationID,
			projectID, deploymentID, storedDigest, storedEndpoint, candidateDigest, targetRunnerID,
			targetInstance, targetSession, generation,
			formatSQLiteTime(time.Now().UTC()), formatSQLiteTime(time.Now().UTC())); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if storedStatus == "committed" {
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return err
		}
		committed = true
		return nil
	} else if storedStatus == "failed" {
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return err
		}
		committed = true
		return errHostingProxyOperationSuperseded
	} else {
		var currentGeneration int64
		if err := conn.QueryRowContext(ctx, `SELECT route_generation FROM hosting_projects WHERE id=?`,
			projectID).Scan(&currentGeneration); err != nil {
			return err
		}
		if generation != currentGeneration {
			if _, err := conn.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='failed',
				last_error_code=?, updated_at=? WHERE operation_id=? AND status IN ('pending','applied')`,
				errCodeConflict, formatSQLiteTime(time.Now().UTC()), operationID); err != nil {
				return err
			}
			if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
				return err
			}
			committed = true
			return errHostingProxyOperationSuperseded
		}
	}
	if storedDigest != "" {
		fence, available, fenceErr := currentHostingReleaseRuntimeFence(ctx, conn, projectID, storedDigest, storedEndpoint)
		if fenceErr != nil {
			return fenceErr
		}
		if !available || !targetRunnerID.Valid || fence.RunnerID != targetRunnerID.Int64 ||
			fence.InstanceID != targetInstance || fence.SessionID != targetSession {
			correctiveID, err := stageCorrectiveHostingSuspension(ctx, conn, operationID, projectID,
				deploymentID, candidateDigest)
			if err != nil {
				return err
			}
			if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
				return err
			}
			committed = true
			return executeHostingCompensation(ctx, proxy, correctiveID, "", projectID, deploymentID,
				externalProjectID, candidateDigest, "", "")
		}
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return err
	}
	committed = true
	if storedDigest != "" && storedEndpoint != "" {
		_, err = proxy.Activate(ctx, proxyActivationRequest{OperationID: operationID,
			ExternalProjectID: externalProjectID, ReleaseDigest: storedDigest, RuntimeEndpoint: storedEndpoint,
			ExpectedPreviousReleaseDigest: candidateDigest, RouteGeneration: generation})
	} else {
		err = proxy.SetSuspended(ctx, proxySuspendRequest{OperationID: operationID,
			ExternalProjectID: externalProjectID, Suspended: true, RouteGeneration: generation})
	}
	if err != nil {
		errorCode := errCodeProxyUnavailable
		var apiErr *hostingAPIError
		if errorsAsHosting(err, &apiErr) && apiErr.Code == errCodeProxyRejected {
			errorCode = errCodeProxyRejected
		}
		_ = markHostingProxyOperationError(ctx, operationID, errorCode)
		if errorCode == errCodeProxyRejected && storedDigest != "" {
			correctiveID, stageErr := stageCorrectiveHostingSuspensionContext(ctx, operationID,
				projectID, deploymentID, candidateDigest)
			if stageErr != nil {
				return stageErr
			}
			return executeHostingCompensation(ctx, proxy, correctiveID, "", projectID,
				deploymentID, externalProjectID, candidateDigest, "", "")
		}
		return err
	}
	correctiveID, err := finalizeHostingCompensation(ctx, operationID, projectID, deploymentID,
		storedDigest, storedEndpoint, candidateDigest, targetRunnerID, targetInstance, targetSession)
	if err != nil {
		return err
	}
	if correctiveID != "" {
		return executeHostingCompensation(ctx, proxy, correctiveID, "", projectID, deploymentID,
			externalProjectID, candidateDigest, "", "")
	}
	return nil
}

func stageCorrectiveHostingSuspension(ctx context.Context, conn *sql.Conn, sourceOperationID string,
	projectID, deploymentID int64, candidateDigest string) (string, error) {
	correctiveID := hashHostingOperation("suspend-stale-compensation", sourceOperationID)
	now := formatSQLiteTime(time.Now().UTC())
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='failed',
		last_error_code=?, updated_at=? WHERE operation_id=? AND status IN ('pending','applied')`,
		errCodeReleaseNotHealthy, now, sourceOperationID); err != nil {
		return "", err
	}
	var existing int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM hosting_proxy_operations WHERE operation_id=?`,
		correctiveID).Scan(&existing); err != nil {
		return "", err
	}
	if existing == 0 {
		generation, err := nextHostingRouteGeneration(ctx, conn, projectID)
		if err != nil {
			return "", err
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_proxy_operations
			(operation_id, hosting_project_id, hosting_deployment_id, operation_type,
			 expected_previous_release_digest, route_generation, status, created_at, updated_at)
			VALUES (?, ?, NULLIF(?, 0), 'compensate', ?, ?, 'pending', ?, ?)`, correctiveID,
			projectID, deploymentID, candidateDigest, generation, now, now); err != nil {
			return "", err
		}
	}
	return correctiveID, nil
}

func stageCorrectiveHostingSuspensionContext(ctx context.Context, sourceOperationID string,
	projectID, deploymentID int64, candidateDigest string) (string, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return "", err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	correctiveID, err := stageCorrectiveHostingSuspension(ctx, conn, sourceOperationID,
		projectID, deploymentID, candidateDigest)
	if err != nil {
		return "", err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return "", err
	}
	committed = true
	return correctiveID, nil
}

func finalizeHostingCompensation(ctx context.Context, operationID string, projectID, deploymentID int64,
	storedDigest, storedEndpoint, candidateDigest string, targetRunnerID sql.NullInt64,
	targetInstance, targetSession string) (string, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return "", err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	var generation, currentGeneration int64
	var status string
	if err := conn.QueryRowContext(ctx, `SELECT operation.route_generation, operation.status,
		project.route_generation FROM hosting_proxy_operations operation JOIN hosting_projects project
		  ON project.id=operation.hosting_project_id
		WHERE operation.operation_id=? AND operation.hosting_project_id=?`, operationID, projectID).Scan(
		&generation, &status, &currentGeneration); err != nil {
		return "", err
	}
	if generation != currentGeneration || (status != "pending" && status != "applied") {
		if _, err := conn.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='failed',
			last_error_code=?, updated_at=? WHERE operation_id=? AND status IN ('pending','applied')`,
			errCodeConflict, formatSQLiteTime(time.Now().UTC()), operationID); err != nil {
			return "", err
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return "", err
		}
		committed = true
		return "", errHostingProxyOperationSuperseded
	}
	correctiveID := ""
	if storedDigest != "" {
		fence, available, fenceErr := currentHostingReleaseRuntimeFence(ctx, conn, projectID, storedDigest, storedEndpoint)
		if fenceErr != nil {
			return "", fenceErr
		}
		if !available || !targetRunnerID.Valid || fence.RunnerID != targetRunnerID.Int64 ||
			fence.InstanceID != targetInstance || fence.SessionID != targetSession {
			correctiveID, err = stageCorrectiveHostingSuspension(ctx, conn, operationID, projectID,
				deploymentID, candidateDigest)
		}
	}
	if err == nil && correctiveID == "" {
		result, updateErr := conn.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='committed',
			last_error_code='', updated_at=? WHERE operation_id=? AND status IN ('pending','applied')
			  AND route_generation=(SELECT route_generation FROM hosting_projects WHERE id=?)`,
			formatSQLiteTime(time.Now().UTC()), operationID, projectID)
		err = updateErr
		if err == nil {
			if affected, _ := result.RowsAffected(); affected != 1 {
				err = errHostingStateConflict
			}
		}
	}
	if err != nil {
		return "", err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return "", err
	}
	committed = true
	return correctiveID, nil
}

type hostingRuntimeFence struct {
	RunnerID   int64
	InstanceID string
	SessionID  string
}

func currentHostingReleaseRuntimeFence(ctx context.Context, querier hostingRouteGenerationQuerier,
	projectID int64, digest, endpoint string) (hostingRuntimeFence, bool, error) {
	var fence hostingRuntimeFence
	var runnerStatus, failureCode, observedSession string
	var runnerLastSeen, observedAt, missingSince sql.NullString
	err := querier.QueryRowContext(ctx, `SELECT release.runtime_runner_id, release.runtime_instance_id,
		runner.active_session_id, runner.status, runner.last_seen, release.runtime_failure_code,
		release.runtime_observed_at, release.runtime_observed_session_id, release.runtime_missing_since
		FROM hosting_releases release JOIN hosting_runners runner ON runner.id=release.runtime_runner_id
		WHERE release.hosting_project_id=? AND release.release_digest=? AND release.runtime_endpoint=?
		  AND release.status IN ('healthy','active','inactive')
		  AND NOT EXISTS (SELECT 1 FROM hosting_runtime_recoveries recovery
		    WHERE recovery.hosting_release_id=release.id AND recovery.status IN ('queued','leased','running'))`,
		projectID, digest, endpoint).Scan(&fence.RunnerID, &fence.InstanceID, &fence.SessionID,
		&runnerStatus, &runnerLastSeen, &failureCode, &observedAt, &observedSession, &missingSince)
	if err == sql.ErrNoRows {
		return hostingRuntimeFence{}, false, nil
	}
	if err != nil {
		return hostingRuntimeFence{}, false, err
	}
	now := time.Now().UTC()
	available := failureCode == "" && !missingSince.Valid && runnerStatus == "online" &&
		runnerLastSeen.Valid && !parseSQLiteTime(runnerLastSeen.String).Before(now.Add(-hostingRunnerStaleAfter))
	if available && fence.SessionID != "" {
		available = observedAt.Valid && observedSession == fence.SessionID &&
			!parseSQLiteTime(observedAt.String).Before(now.Add(-hostingRunnerStaleAfter))
	}
	return fence, available, nil
}

func hostingActivationAllowed(ctx context.Context, projectID int64) (bool, error) {
	var desiredState, projectKillReason string
	var globalKill int
	err := db.QueryRowContext(ctx, `SELECT p.desired_state, p.kill_switch_reason, s.global_kill_switch
		FROM hosting_projects p JOIN hosting_settings s ON s.id=1 WHERE p.id=?
		  AND NOT EXISTS (SELECT 1 FROM hosting_proxy_operations op WHERE op.hosting_project_id=p.id
			AND op.operation_type IN ('suspend','resume') AND op.status IN ('pending','applied'))`, projectID).Scan(&desiredState, &projectKillReason, &globalKill)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return desiredState == "active" && projectKillReason == "" && globalKill == 0, nil
}

func markHostingProxyOperationApplied(ctx context.Context, operationID, routeRevision string) error {
	result, err := db.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='applied', route_revision=?,
		last_error_code='', updated_at=? WHERE operation_id=? AND status IN ('pending','applied')`,
		routeRevision, formatSQLiteTime(time.Now().UTC()), operationID)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		var status, storedRevision string
		if err := db.QueryRowContext(ctx, `SELECT status, route_revision FROM hosting_proxy_operations
			WHERE operation_id=?`, operationID).Scan(&status, &storedRevision); err != nil {
			return fmt.Errorf("proxy operation is no longer pending: %w", err)
		}
		if status == "committed" && storedRevision == routeRevision {
			return nil
		}
		return fmt.Errorf("proxy operation is no longer pending")
	}
	return nil
}

func markHostingProxyOperationError(ctx context.Context, operationID, errorCode string) error {
	_, err := db.ExecContext(ctx, `UPDATE hosting_proxy_operations SET last_error_code=?, updated_at=?
		WHERE operation_id=? AND status='pending'`, errorCode, formatSQLiteTime(time.Now().UTC()), operationID)
	return err
}

func markHostingProxyOperationFailed(ctx context.Context, operationID, errorCode string) error {
	_, err := db.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='failed', last_error_code=?, updated_at=?
		WHERE operation_id=? AND status IN ('pending','applied')`, errorCode, formatSQLiteTime(time.Now().UTC()), operationID)
	return err
}

func markHostingProxyOperationCommitted(ctx context.Context, operationID string) error {
	_, err := db.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='committed', updated_at=?
		WHERE operation_id=? AND status IN ('pending','applied','committed')`, formatSQLiteTime(time.Now().UTC()), operationID)
	return err
}

func hostingProxyOperationIsCurrent(ctx context.Context, operationID string) (bool, error) {
	var current int
	err := db.QueryRowContext(ctx, `SELECT CASE WHEN operation.route_generation=project.route_generation THEN 1 ELSE 0 END
		FROM hosting_proxy_operations operation JOIN hosting_projects project ON project.id=operation.hosting_project_id
		WHERE operation.operation_id=?`, operationID).Scan(&current)
	return current == 1, err
}
