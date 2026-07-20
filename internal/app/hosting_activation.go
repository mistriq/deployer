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
		if err := compensateCancelledActivation(ctx, proxy, state, input.ReleaseDigest); err != nil {
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
	if err := markHostingProxyOperationApplied(ctx, operationID, activation.RouteRevision); err != nil {
		return err
	}

	cancelled, err := activateHealthyHostingRelease(ctx, state, input.ReleaseDigest, activation.RouteRevision)
	if err != nil {
		return err
	}
	if !cancelled {
		return nil
	}
	if err := compensateCancelledActivation(ctx, proxy, state, input.ReleaseDigest); err != nil {
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
	var status string
	var cancelRequested sql.NullString
	err = conn.QueryRowContext(ctx, `SELECT status, cancel_requested_at FROM hosting_jobs
		WHERE id=? AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=? AND lease_expires_at>?`,
		state.JobID, state.RunnerID, state.LeaseGeneration, state.LeaseTokenHash, formatSQLiteTime(now)).Scan(&status, &cancelRequested)
	if err != nil {
		return err
	}
	if cancelRequested.Valid {
		return &hostingAPIError{Code: errCodeProjectBusy, Message: "deployment cancellation is in progress", StatusCode: http.StatusConflict}
	}
	if status != "leased" && status != "running" {
		return &hostingAPIError{Code: errCodeConflict, Message: "hosting job is no longer running", StatusCode: http.StatusConflict}
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
			 status, health_evidence_json, previous_release_digest, runtime_endpoint, created_at)
			VALUES (?, ?, ?, ?, ?, ?, 'healthy', ?, ?, ?, ?)`, state.ProjectID, state.DeploymentID,
			input.ReleaseDigest, input.ReleaseArtifactDigest, state.CommitSHA, state.ArtifactDigest, string(healthJSON),
			state.PreviousReleaseDigest, input.RuntimeEndpoint, formatSQLiteTime(now)); err != nil {
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
	if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_proxy_operations
		(operation_id, hosting_project_id, hosting_deployment_id, operation_type, release_digest,
		 runtime_endpoint, expected_previous_release_digest, status, created_at, updated_at)
		VALUES (?, ?, ?, 'activate', ?, ?, ?, 'pending', ?, ?)
		ON CONFLICT(operation_id) DO NOTHING`, operationID, state.ProjectID, state.DeploymentID,
		input.ReleaseDigest, input.RuntimeEndpoint, state.PreviousReleaseDigest, formatSQLiteTime(now), formatSQLiteTime(now)); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return err
	}
	committed = true
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
	result, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET status='active', route_revision=?, activated_at=?,
		deactivated_at=NULL, runtime_runner_id=?, runtime_generation=runtime_generation+1, runtime_instance_id=?
		WHERE hosting_project_id=? AND hosting_deployment_id=? AND release_digest=? AND status='healthy'`,
		routeRevision, formatSQLiteTime(now), state.RunnerID,
		fmt.Sprintf("build-%d-%d", state.JobID, state.LeaseGeneration), state.ProjectID, state.DeploymentID, releaseDigest)
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

func compensateCancelledActivation(ctx context.Context, proxy hostingProxyClient, state *hostingCompletionState, candidateDigest string) error {
	operationID := hashHostingOperation("compensate", state.ExternalDeploymentID, candidateDigest)
	var desiredState string
	if err := db.QueryRowContext(ctx, `SELECT desired_state FROM hosting_projects WHERE id=?`, state.ProjectID).Scan(&desiredState); err != nil {
		return err
	}
	if desiredState == "suspended" {
		return proxy.SetSuspended(ctx, proxySuspendRequest{OperationID: operationID, ExternalProjectID: state.ExternalProjectID, Suspended: true})
	}
	if state.PreviousReleaseDigest == "" || state.PreviousRuntimeEndpoint == "" {
		return proxy.SetSuspended(ctx, proxySuspendRequest{OperationID: operationID, ExternalProjectID: state.ExternalProjectID, Suspended: true})
	}
	_, err := proxy.Activate(ctx, proxyActivationRequest{
		OperationID:       operationID,
		ExternalProjectID: state.ExternalProjectID,
		ReleaseDigest:     state.PreviousReleaseDigest,
		RuntimeEndpoint:   state.PreviousRuntimeEndpoint,
	})
	return err
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
