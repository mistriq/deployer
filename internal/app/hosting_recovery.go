package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

type hostingRuntimeRecoveryState struct {
	ID                      int64
	ReleaseID               int64
	DeploymentID            int64
	ProjectID               int64
	RunnerID                int64
	RuntimeOwnerRunnerID    int64
	LeaseGeneration         int64
	LeaseTokenHash          string
	CompletionFingerprint   string
	Status                  string
	Attempts                int
	CancelRequested         sql.NullString
	LeaseExpiresAt          sql.NullString
	RunnerStatus            string
	RunnerLastSeen          sql.NullString
	ExternalProjectID       string
	ExternalDeploymentID    string
	ReleaseDigest           string
	ReleaseArtifactDigest   string
	ReleaseArtifactPath     string
	ReleaseArtifactSize     int64
	PreviousRuntimeEndpoint string
	Limits                  hostingWorkloadLimits
	Runtime                 HostingRuntimeManifest
	SecretRefs              []HostingSecretReference
}

var allowedHostingRecoveryFailureCodes = map[string]struct{}{
	"artifact_digest_mismatch":   {},
	"artifact_unavailable":       {},
	"health_check_failed":        {},
	"proxy_activation_failed":    {},
	"release_persistence_failed": {},
	"runner_lost":                {},
	"runtime_start_failed":       {},
	"workload_policy_violation":  {},
}

func claimHostingRuntimeRecovery(ctx context.Context, runnerID int64) (*hostingClaimedJob, error) {
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
	var state hostingRuntimeRecoveryState
	var runtimeJSON, secretsJSON string
	err = conn.QueryRowContext(ctx, `SELECT recovery.id, recovery.hosting_release_id,
		release.hosting_deployment_id, release.hosting_project_id, release.runtime_runner_id,
		recovery.lease_generation,
		project.external_project_id, deployment.external_deployment_id, release.release_digest,
		release.release_artifact_digest, artifact.artifact_path, artifact.size_bytes,
		release.runtime_endpoint, recovery.required_cpu_millis, recovery.required_ram_bytes,
		recovery.required_disk_bytes, recovery.required_pids, release.runtime_manifest_json, job.secret_refs_json,
		recovery.attempts
		FROM hosting_runtime_recoveries recovery
		JOIN hosting_releases release ON release.id=recovery.hosting_release_id
		JOIN hosting_release_artifacts artifact ON artifact.artifact_digest=release.release_artifact_digest
		JOIN hosting_projects project ON project.id=release.hosting_project_id
		JOIN hosting_deployments deployment ON deployment.id=release.hosting_deployment_id
		JOIN hosting_jobs job ON job.hosting_deployment_id=deployment.id
		JOIN hosting_runners runner ON runner.id=recovery.hosting_runner_id
		JOIN hosting_settings settings ON settings.id=1
		WHERE recovery.hosting_runner_id=? AND recovery.status='queued'
		  AND recovery.cancel_requested_at IS NULL AND release.status='active'
		  AND project.desired_state='active' AND project.kill_switch_reason=''
		  AND settings.global_kill_switch=0 AND runner.status='online' AND runner.draining=0
		  AND EXISTS (SELECT 1 FROM json_each(runner.operation_capabilities_json) WHERE value='restore')
		ORDER BY recovery.id LIMIT 1`, runnerID).Scan(
		&state.ID, &state.ReleaseID, &state.DeploymentID, &state.ProjectID, &state.RuntimeOwnerRunnerID,
		&state.LeaseGeneration,
		&state.ExternalProjectID, &state.ExternalDeploymentID, &state.ReleaseDigest,
		&state.ReleaseArtifactDigest, &state.ReleaseArtifactPath, &state.ReleaseArtifactSize,
		&state.PreviousRuntimeEndpoint, &state.Limits.CPUMillis, &state.Limits.RAMBytes,
		&state.Limits.DiskBytes, &state.Limits.PIDs, &runtimeJSON, &secretsJSON, &state.Attempts)
	if err == sql.ErrNoRows {
		return nil, errNoHostingJob
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(runtimeJSON), &state.Runtime); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(secretsJSON), &state.SecretRefs); err != nil {
		return nil, err
	}
	if state.SecretRefs == nil {
		state.SecretRefs = make([]HostingSecretReference, 0)
	}
	leaseToken, err := generateLeaseToken()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	expires := now.Add(hostingJobLeaseDuration)
	state.LeaseGeneration++
	result, err := conn.ExecContext(ctx, `UPDATE hosting_runtime_recoveries SET status='leased',
		lease_generation=?, lease_token_hash=?, lease_expires_at=?, attempts=attempts+1,
		completion_fingerprint='', started_at=COALESCE(started_at, ?) WHERE id=? AND status='queued' AND hosting_runner_id=?`,
		state.LeaseGeneration, hashToken(leaseToken), formatSQLiteTime(expires), formatSQLiteTime(now), state.ID, runnerID)
	if err != nil {
		return nil, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return nil, errNoHostingJob
	}
	if err := recordHostingEvent(ctx, conn, state.ProjectID, state.DeploymentID,
		"runtime_recovery_leased", hostingPhaseStarting, "", map[string]any{"runner_id": runnerID, "attempt": state.Attempts + 1}, now); err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, err
	}
	committed = true
	recipe := hostingJobRecipe{
		Operation:             "restore",
		SchemaVersion:         hostingRunnerProtocolVersion,
		ExternalProjectID:     state.ExternalProjectID,
		ExternalDeploymentID:  state.ExternalDeploymentID,
		Runtime:               state.Runtime,
		Limits:                state.Limits,
		ReleaseDigest:         state.ReleaseDigest,
		ReleaseArtifactDigest: state.ReleaseArtifactDigest,
		ReleaseArtifactURL:    fmt.Sprintf("/api/hosting-agent/v1/recoveries/%d/artifact", state.ID),
	}
	return &hostingClaimedJob{JobID: state.ID, LeaseGeneration: state.LeaseGeneration, LeaseToken: leaseToken,
		LeaseExpiresAt: expires, Recipe: recipe, SecretRefs: state.SecretRefs}, nil
}

func handleHostingAgentRecovery(w http.ResponseWriter, r *http.Request) {
	recoveryID, suffix, ok := parseIDPath(r.URL.Path, "/api/hosting-agent/v1/recoveries/")
	if !ok {
		jsonErrorCode(w, errCodeJobNotFound, "hosting runtime recovery not found", http.StatusNotFound)
		return
	}
	switch suffix {
	case "artifact":
		handleHostingRecoveryArtifact(w, r, recoveryID)
	case "heartbeat":
		handleHostingRecoveryHeartbeat(w, r, recoveryID)
	case "logs":
		handleHostingRecoveryLogs(w, r, recoveryID)
	case "complete":
		handleHostingRecoveryComplete(w, r, recoveryID)
	default:
		jsonErrorCode(w, errCodeNotFound, "hosting runtime recovery operation not found", http.StatusNotFound)
	}
}

func authenticateHostingRecoveryLease(ctx context.Context, runnerID, recoveryID, generation int64, leaseToken string) (*hostingRuntimeRecoveryState, error) {
	var state hostingRuntimeRecoveryState
	var runtimeJSON, secretsJSON string
	err := db.QueryRowContext(ctx, `SELECT recovery.id, recovery.hosting_release_id,
		release.hosting_deployment_id, release.hosting_project_id, recovery.hosting_runner_id,
		release.runtime_runner_id,
		recovery.lease_generation, recovery.lease_token_hash, recovery.completion_fingerprint,
		recovery.status, recovery.attempts,
		recovery.cancel_requested_at,
		project.external_project_id, deployment.external_deployment_id, release.release_digest,
		release.release_artifact_digest, artifact.artifact_path, artifact.size_bytes,
		release.runtime_endpoint, recovery.required_cpu_millis, recovery.required_ram_bytes,
		recovery.required_disk_bytes, recovery.required_pids, release.runtime_manifest_json, job.secret_refs_json
		FROM hosting_runtime_recoveries recovery
		JOIN hosting_releases release ON release.id=recovery.hosting_release_id
		JOIN hosting_release_artifacts artifact ON artifact.artifact_digest=release.release_artifact_digest
		JOIN hosting_projects project ON project.id=release.hosting_project_id
		JOIN hosting_deployments deployment ON deployment.id=release.hosting_deployment_id
		JOIN hosting_jobs job ON job.hosting_deployment_id=deployment.id
		WHERE recovery.id=? AND recovery.hosting_runner_id=? AND recovery.lease_generation=?
		  AND recovery.lease_token_hash=? AND recovery.status IN ('leased','running')
		  AND recovery.lease_expires_at>?`, recoveryID, runnerID, generation, hashToken(leaseToken),
		formatSQLiteTime(time.Now().UTC())).Scan(&state.ID, &state.ReleaseID, &state.DeploymentID,
		&state.ProjectID, &state.RunnerID, &state.RuntimeOwnerRunnerID, &state.LeaseGeneration, &state.LeaseTokenHash,
		&state.CompletionFingerprint, &state.Status, &state.Attempts, &state.CancelRequested,
		&state.ExternalProjectID, &state.ExternalDeploymentID,
		&state.ReleaseDigest, &state.ReleaseArtifactDigest, &state.ReleaseArtifactPath,
		&state.ReleaseArtifactSize, &state.PreviousRuntimeEndpoint, &state.Limits.CPUMillis,
		&state.Limits.RAMBytes, &state.Limits.DiskBytes, &state.Limits.PIDs, &runtimeJSON, &secretsJSON)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(runtimeJSON), &state.Runtime); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(secretsJSON), &state.SecretRefs); err != nil {
		return nil, err
	}
	if state.SecretRefs == nil {
		state.SecretRefs = make([]HostingSecretReference, 0)
	}
	return &state, nil
}

func handleHostingRecoveryArtifact(w http.ResponseWriter, r *http.Request, recoveryID int64) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	runner := r.Context().Value(hostingRunnerContextKey{}).(*HostingRunner)
	generation, token, ok := leaseCredentials(r)
	if !ok {
		jsonErrorCode(w, errCodeJobForbidden, "valid recovery lease is required", http.StatusForbidden)
		return
	}
	state, err := authenticateHostingRecoveryLease(r.Context(), runner.ID, recoveryID, generation, token)
	if err != nil || !isManagedArtifactPath(state.ReleaseArtifactPath) {
		jsonErrorCode(w, errCodeJobForbidden, "valid recovery lease is required", http.StatusForbidden)
		return
	}
	artifact, err := currentArtifactStorage().Open(state.ReleaseArtifactPath)
	if err != nil {
		logOperationalError("serve hosting release artifact", err)
		jsonErrorCode(w, errCodeArtifactUnavailable, "retained recovery artifact is unavailable", http.StatusNotFound)
		return
	}
	defer artifact.Close()
	info, err := currentArtifactStorage().Stat(state.ReleaseArtifactPath)
	if err != nil {
		logOperationalError("stat hosting release artifact", err)
		jsonErrorCode(w, errCodeArtifactUnavailable, "retained recovery artifact is unavailable", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("X-Deployer-Artifact-Digest", state.ReleaseArtifactDigest)
	http.ServeContent(w, r, filepath.Base(state.ReleaseArtifactPath), info.ModTime(), artifact)
}

func handleHostingRecoveryHeartbeat(w http.ResponseWriter, r *http.Request, recoveryID int64) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	runner := r.Context().Value(hostingRunnerContextKey{}).(*HostingRunner)
	generation, token, ok := leaseCredentials(r)
	if !ok {
		jsonErrorCode(w, errCodeJobForbidden, "valid recovery lease is required", http.StatusForbidden)
		return
	}
	now := time.Now().UTC()
	result, err := db.ExecContext(r.Context(), `UPDATE hosting_runtime_recoveries SET status='running',
		lease_expires_at=? WHERE id=? AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=?
		AND status IN ('leased','running') AND lease_expires_at>?`, formatSQLiteTime(now.Add(hostingJobLeaseDuration)),
		recoveryID, runner.ID, generation, hashToken(token), formatSQLiteTime(now))
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "extend runtime recovery lease failed", http.StatusInternalServerError)
		return
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		jsonErrorCode(w, errCodeJobForbidden, "valid recovery lease is required", http.StatusForbidden)
		return
	}
	if _, err := db.ExecContext(r.Context(), `UPDATE hosting_runners SET status='online', last_seen=? WHERE id=?`,
		formatSQLiteTime(now), runner.ID); err != nil {
		jsonErrorCode(w, errCodeInternal, "update recovery runner liveness failed", http.StatusInternalServerError)
		return
	}
	var cancelled sql.NullString
	if err := db.QueryRowContext(r.Context(), `SELECT cancel_requested_at FROM hosting_runtime_recoveries WHERE id=?`, recoveryID).Scan(&cancelled); err != nil {
		jsonErrorCode(w, errCodeInternal, "read runtime recovery cancellation failed", http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]bool{"cancel_requested": cancelled.Valid})
}

func handleHostingRecoveryLogs(w http.ResponseWriter, r *http.Request, recoveryID int64) {
	if !requireMethod(w, r, http.MethodPost) || !requireJSONContentType(w, r) {
		return
	}
	runner := r.Context().Value(hostingRunnerContextKey{}).(*HostingRunner)
	generation, token, ok := leaseCredentials(r)
	if !ok {
		jsonErrorCode(w, errCodeJobForbidden, "valid recovery lease is required", http.StatusForbidden)
		return
	}
	state, err := authenticateHostingRecoveryLease(r.Context(), runner.ID, recoveryID, generation, token)
	if err != nil {
		jsonErrorCode(w, errCodeJobForbidden, "valid recovery lease is required", http.StatusForbidden)
		return
	}
	var input hostingLogRequest
	if !decodeInternalJSON(w, r, maxHostingLogChunkBytes, &input) {
		return
	}
	if input.Stream != "runtime" && input.Stream != "system" {
		jsonErrorCode(w, errCodeValidation, "recovery logs must use runtime or system stream", http.StatusBadRequest)
		return
	}
	input.Message = redactSecrets(input.Message)
	if len(input.Message) > maxHostingLogChunkBytes {
		input.Message = input.Message[len(input.Message)-maxHostingLogChunkBytes:]
	}
	if _, err := db.ExecContext(r.Context(), `INSERT INTO hosting_logs
		(hosting_deployment_id, stream, message, created_at) VALUES (?, ?, ?, ?)`, state.DeploymentID,
		input.Stream, input.Message, formatSQLiteTime(time.Now().UTC())); err != nil {
		jsonErrorCode(w, errCodeInternal, "persist recovery log failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleHostingRecoveryComplete(w http.ResponseWriter, r *http.Request, recoveryID int64) {
	if !requireMethod(w, r, http.MethodPost) || !requireJSONContentType(w, r) {
		return
	}
	runner := r.Context().Value(hostingRunnerContextKey{}).(*HostingRunner)
	generation, token, ok := leaseCredentials(r)
	if !ok {
		jsonErrorCode(w, errCodeJobForbidden, "valid recovery lease is required", http.StatusForbidden)
		return
	}
	var input hostingCompletionRequest
	if !decodeInternalJSON(w, r, 32<<10, &input) {
		return
	}
	if err := completeHostingRuntimeRecovery(r.Context(), runner.ID, recoveryID, generation, token, input); err != nil {
		writeHostingAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func queueLostHostingRuntimes(ctx context.Context, conn *sql.Conn, now time.Time) error {
	rows, err := conn.QueryContext(ctx, `SELECT release.id, release.hosting_project_id,
		release.hosting_deployment_id, release.runtime_runner_id, job.required_cpu_millis,
			job.required_ram_bytes, job.required_disk_bytes, job.required_pids, release.release_artifact_digest
			, job.secret_refs_json
		FROM hosting_releases release
		JOIN hosting_runners runner ON runner.id=release.runtime_runner_id
		JOIN hosting_jobs job ON job.hosting_deployment_id=release.hosting_deployment_id
		JOIN hosting_projects project ON project.id=release.hosting_project_id
		JOIN hosting_settings settings ON settings.id=1
		WHERE release.status='active' AND runner.status='offline' AND project.desired_state='active'
		  AND project.kill_switch_reason='' AND settings.global_kill_switch=0
		  AND NOT EXISTS (SELECT 1 FROM hosting_runtime_recoveries active
			WHERE active.hosting_release_id=release.id AND active.status IN ('queued','leased','running'))`)
	if err != nil {
		return err
	}
	type lostRuntime struct {
		releaseID, projectID, deploymentID, runnerID int64
		limits                                       hostingWorkloadLimits
		artifactDigest                               sql.NullString
		secretRefsJSON                               string
	}
	var lost []lostRuntime
	for rows.Next() {
		var runtime lostRuntime
		if err := rows.Scan(&runtime.releaseID, &runtime.projectID, &runtime.deploymentID, &runtime.runnerID,
			&runtime.limits.CPUMillis, &runtime.limits.RAMBytes, &runtime.limits.DiskBytes,
			&runtime.limits.PIDs, &runtime.artifactDigest, &runtime.secretRefsJSON); err != nil {
			rows.Close()
			return err
		}
		lost = append(lost, runtime)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, runtime := range lost {
		var secretRefs []HostingSecretReference
		if err := json.Unmarshal([]byte(runtime.secretRefsJSON), &secretRefs); err != nil {
			return err
		}
		if len(secretRefs) > 0 {
			if _, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET status='failed', runtime_runner_id=NULL WHERE id=?`, runtime.releaseID); err != nil {
				return err
			}
			if err := stageHostingRuntimeUnavailable(ctx, conn, runtime.projectID, runtime.deploymentID,
				runtime.releaseID, now); err != nil {
				return err
			}
			if err := recordHostingEvent(ctx, conn, runtime.projectID, runtime.deploymentID,
				"runtime_recovery_unavailable", hostingPhaseFailed, "secret_reference_unavailable", nil, now); err != nil {
				return err
			}
			if err := recomputeHostingRunnerCapacity(ctx, conn, runtime.runnerID); err != nil {
				return err
			}
			continue
		}
		if !runtime.artifactDigest.Valid || !validSHA256Digest(runtime.artifactDigest.String) {
			if _, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET status='failed', runtime_runner_id=NULL WHERE id=?`, runtime.releaseID); err != nil {
				return err
			}
			if err := stageHostingRuntimeUnavailable(ctx, conn, runtime.projectID, runtime.deploymentID,
				runtime.releaseID, now); err != nil {
				return err
			}
			if err := recordHostingEvent(ctx, conn, runtime.projectID, runtime.deploymentID,
				"runtime_recovery_unavailable", hostingPhaseFailed, "artifact_unavailable", nil, now); err != nil {
				return err
			}
			if err := recomputeHostingRunnerCapacity(ctx, conn, runtime.runnerID); err != nil {
				return err
			}
			continue
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_runtime_recoveries
			(hosting_release_id, status, required_cpu_millis, required_ram_bytes,
			 required_disk_bytes, required_pids, created_at)
			VALUES (?, 'queued', ?, ?, ?, ?, ?)`, runtime.releaseID, runtime.limits.CPUMillis,
			runtime.limits.RAMBytes, runtime.limits.DiskBytes, runtime.limits.PIDs, formatSQLiteTime(now)); err != nil {
			return err
		}
		if err := recordHostingEvent(ctx, conn, runtime.projectID, runtime.deploymentID,
			"runtime_recovery_queued", hostingPhaseStarting, "runner_lost", nil, now); err != nil {
			return err
		}
	}
	return nil
}

func placeHostingRuntimeRecoveries(ctx context.Context, conn *sql.Conn, now time.Time) error {
	staleBefore := formatSQLiteTime(now.Add(-hostingRunnerStaleAfter))
	invalidRows, err := conn.QueryContext(ctx, `SELECT recovery.id, recovery.hosting_runner_id,
		recovery.required_cpu_millis, recovery.required_ram_bytes,
		recovery.required_disk_bytes, recovery.required_pids
		FROM hosting_runtime_recoveries recovery
		JOIN hosting_runners runner ON runner.id=recovery.hosting_runner_id
		WHERE recovery.status='queued' AND recovery.hosting_runner_id IS NOT NULL
		  AND (runner.status<>'online' OR runner.draining<>0 OR runner.last_seen IS NULL OR runner.last_seen<?
		    OR NOT EXISTS (SELECT 1 FROM json_each(runner.operation_capabilities_json) WHERE value='restore'))`, staleBefore)
	if err != nil {
		return err
	}
	type invalidAssignment struct {
		recoveryID, runnerID int64
		limits               hostingWorkloadLimits
	}
	var invalidAssignments []invalidAssignment
	for invalidRows.Next() {
		var assignment invalidAssignment
		if err := invalidRows.Scan(&assignment.recoveryID, &assignment.runnerID,
			&assignment.limits.CPUMillis, &assignment.limits.RAMBytes,
			&assignment.limits.DiskBytes, &assignment.limits.PIDs); err != nil {
			invalidRows.Close()
			return err
		}
		invalidAssignments = append(invalidAssignments, assignment)
	}
	if err := invalidRows.Close(); err != nil {
		return err
	}
	for _, assignment := range invalidAssignments {
		result, err := conn.ExecContext(ctx, `UPDATE hosting_runtime_recoveries SET hosting_runner_id=NULL
			WHERE id=? AND status='queued' AND hosting_runner_id=?`, assignment.recoveryID, assignment.runnerID)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			continue
		}
		if err := restoreHostingRunnerCapacity(ctx, conn, assignment.runnerID, assignment.limits); err != nil {
			return err
		}
	}

	rows, err := conn.QueryContext(ctx, `SELECT recovery.id, release.hosting_project_id,
		project.external_project_id, project.manifest_json, project.manifest_digest,
		project.resource_profile, project.deploy_path, project.desired_state, project.kill_switch_reason,
		recovery.required_cpu_millis, recovery.required_ram_bytes, recovery.required_disk_bytes,
			recovery.required_pids, release.runtime_runner_id
		FROM hosting_runtime_recoveries recovery
		JOIN hosting_releases release ON release.id=recovery.hosting_release_id
		JOIN hosting_projects project ON project.id=release.hosting_project_id
		JOIN hosting_settings settings ON settings.id=1
		WHERE recovery.status='queued' AND recovery.hosting_runner_id IS NULL
		  AND recovery.cancel_requested_at IS NULL AND project.desired_state='active'
		  AND project.kill_switch_reason='' AND settings.global_kill_switch=0
		ORDER BY recovery.created_at, recovery.id`)
	if err != nil {
		return err
	}
	type queuedRecovery struct {
		id           int64
		project      hostingProjectState
		limits       hostingWorkloadLimits
		manifestJSON string
		ownerRunner  int64
	}
	var recoveries []queuedRecovery
	for rows.Next() {
		var recovery queuedRecovery
		if err := rows.Scan(&recovery.id, &recovery.project.ID, &recovery.project.ExternalProjectID,
			&recovery.manifestJSON, &recovery.project.ManifestDigest, &recovery.project.ResourceProfile,
			&recovery.project.DeployPath, &recovery.project.DesiredState, &recovery.project.KillSwitchReason,
			&recovery.limits.CPUMillis, &recovery.limits.RAMBytes, &recovery.limits.DiskBytes,
			&recovery.limits.PIDs, &recovery.ownerRunner); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal([]byte(recovery.manifestJSON), &recovery.project.Manifest); err != nil {
			rows.Close()
			return err
		}
		recoveries = append(recoveries, recovery)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, recovery := range recoveries {
		runnerID, err := reserveHostingRunner(ctx, conn, &recovery.project, recovery.limits, "restore", recovery.ownerRunner)
		if err != nil {
			var apiErr *hostingAPIError
			if errorsAsHosting(err, &apiErr) && apiErr.Code == errCodeRunnerCapacityUnavailable {
				continue
			}
			return err
		}
		result, err := conn.ExecContext(ctx, `UPDATE hosting_runtime_recoveries SET hosting_runner_id=?
			WHERE id=? AND status='queued' AND hosting_runner_id IS NULL`, runnerID, recovery.id)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			if err := restoreHostingRunnerCapacity(ctx, conn, runnerID, recovery.limits); err != nil {
				return err
			}
		}
	}
	return nil
}

func reconcileExpiredHostingRecoveries(ctx context.Context, conn *sql.Conn, now time.Time) error {
	rows, err := conn.QueryContext(ctx, `SELECT recovery.id, recovery.hosting_release_id,
		release.hosting_project_id, release.hosting_deployment_id, recovery.hosting_runner_id,
		recovery.attempts, recovery.required_cpu_millis, recovery.required_ram_bytes,
		recovery.required_disk_bytes, recovery.required_pids, recovery.cancel_requested_at
		FROM hosting_runtime_recoveries recovery
		JOIN hosting_releases release ON release.id=recovery.hosting_release_id
		WHERE recovery.status IN ('leased','running') AND recovery.lease_expires_at<?
		  AND NOT EXISTS (SELECT 1 FROM hosting_proxy_operations operation
			WHERE operation.hosting_runtime_recovery_id=recovery.id AND operation.status IN ('pending','applied'))`, formatSQLiteTime(now))
	if err != nil {
		return err
	}
	type expiredRecovery struct {
		id, releaseID, projectID, deploymentID, runnerID int64
		attempts                                         int
		limits                                           hostingWorkloadLimits
		cancelRequested                                  sql.NullString
	}
	var expired []expiredRecovery
	for rows.Next() {
		var recovery expiredRecovery
		if err := rows.Scan(&recovery.id, &recovery.releaseID, &recovery.projectID,
			&recovery.deploymentID, &recovery.runnerID, &recovery.attempts,
			&recovery.limits.CPUMillis, &recovery.limits.RAMBytes,
			&recovery.limits.DiskBytes, &recovery.limits.PIDs, &recovery.cancelRequested); err != nil {
			rows.Close()
			return err
		}
		expired = append(expired, recovery)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, recovery := range expired {
		if err := restoreHostingRunnerCapacity(ctx, conn, recovery.runnerID, recovery.limits); err != nil {
			return err
		}
		if recovery.cancelRequested.Valid {
			if _, err := conn.ExecContext(ctx, `UPDATE hosting_runtime_recoveries SET status='cancelled',
				lease_expires_at=NULL, completed_at=? WHERE id=? AND status IN ('leased','running')`,
				formatSQLiteTime(now), recovery.id); err != nil {
				return err
			}
			if err := recordHostingEvent(ctx, conn, recovery.projectID, recovery.deploymentID,
				"runtime_recovery_cancelled_after_lease_loss", hostingPhaseCancelled, "cancelled", nil, now); err != nil {
				return err
			}
			continue
		}
		if recovery.attempts >= hostingMaxJobAttempts {
			if _, err := conn.ExecContext(ctx, `UPDATE hosting_runtime_recoveries SET status='failed',
				hosting_runner_id=NULL, lease_token_hash='', lease_expires_at=NULL, completed_at=? WHERE id=?`, formatSQLiteTime(now), recovery.id); err != nil {
				return err
			}
			if _, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET status='failed', runtime_runner_id=NULL WHERE id=?`, recovery.releaseID); err != nil {
				return err
			}
			if err := stageHostingRuntimeUnavailable(ctx, conn, recovery.projectID, recovery.deploymentID,
				recovery.releaseID, now); err != nil {
				return err
			}
			if err := recordHostingEvent(ctx, conn, recovery.projectID, recovery.deploymentID,
				"runtime_recovery_failed", hostingPhaseFailed, "runner_lost", nil, now); err != nil {
				return err
			}
			continue
		}
		if _, err := conn.ExecContext(ctx, `UPDATE hosting_runtime_recoveries SET status='queued',
			hosting_runner_id=NULL, lease_token_hash='', lease_expires_at=NULL,
			completion_fingerprint='' WHERE id=?`, recovery.id); err != nil {
			return err
		}
	}
	return nil
}

func stageHostingRuntimeUnavailable(ctx context.Context, conn *sql.Conn, projectID, deploymentID, releaseID int64, now time.Time) error {
	operationID := hashHostingOperation("runtime-unavailable", fmt.Sprint(releaseID))
	_, err := conn.ExecContext(ctx, `INSERT INTO hosting_proxy_operations
		(operation_id, hosting_project_id, hosting_deployment_id, operation_type, status, created_at, updated_at)
		VALUES (?, ?, ?, 'suspend', 'pending', ?, ?) ON CONFLICT(operation_id) DO NOTHING`, operationID,
		projectID, deploymentID, formatSQLiteTime(now), formatSQLiteTime(now))
	return err
}

func compensateCancelledHostingRecovery(ctx context.Context, proxy hostingProxyClient, state *hostingRuntimeRecoveryState) error {
	operationID := hashHostingOperation("compensate-recovery", fmt.Sprint(state.ID), fmt.Sprint(state.LeaseGeneration))
	return proxy.SetSuspended(ctx, proxySuspendRequest{
		OperationID:       operationID,
		ExternalProjectID: state.ExternalProjectID,
		Suspended:         true,
	})
}

func reconcileLateHostingRecoveryActivation(ctx context.Context, proxy hostingProxyClient, state *hostingRuntimeRecoveryState) error {
	var desiredState, killSwitchReason string
	var globalKillSwitch int
	if err := db.QueryRowContext(ctx, `SELECT project.desired_state, project.kill_switch_reason,
		settings.global_kill_switch FROM hosting_projects project JOIN hosting_settings settings ON settings.id=1
		WHERE project.id=?`, state.ProjectID).Scan(&desiredState, &killSwitchReason, &globalKillSwitch); err != nil {
		return err
	}
	operationID := hashHostingOperation("compensate-recovery-late-activation", fmt.Sprint(state.ID), fmt.Sprint(state.LeaseGeneration))
	if desiredState == "active" && killSwitchReason == "" && globalKillSwitch == 0 && state.PreviousRuntimeEndpoint != "" {
		_, err := proxy.Activate(ctx, proxyActivationRequest{
			OperationID:                   operationID,
			ExternalProjectID:             state.ExternalProjectID,
			ReleaseDigest:                 state.ReleaseDigest,
			RuntimeEndpoint:               state.PreviousRuntimeEndpoint,
			ExpectedPreviousReleaseDigest: state.ReleaseDigest,
		})
		return err
	}
	return proxy.SetSuspended(ctx, proxySuspendRequest{
		OperationID:       operationID,
		ExternalProjectID: state.ExternalProjectID,
		Suspended:         true,
	})
}

func completeHostingRuntimeRecovery(ctx context.Context, runnerID, recoveryID, generation int64, leaseToken string, input hostingCompletionRequest) error {
	input.Status = strings.TrimSpace(input.Status)
	input.FailureCode = strings.TrimSpace(input.FailureCode)
	input.FailureMessage = redactSecrets(strings.TrimSpace(input.FailureMessage))
	input.ReleaseDigest = strings.ToLower(strings.TrimSpace(input.ReleaseDigest))
	input.ReleaseArtifactDigest = strings.ToLower(strings.TrimSpace(input.ReleaseArtifactDigest))
	input.RuntimeEndpoint = strings.TrimSpace(input.RuntimeEndpoint)
	encodedCompletion, err := json.Marshal(input)
	if err != nil {
		return err
	}
	completionFingerprint := hashHostingOperation("hosting.runtime_recovery.complete", string(encodedCompletion))
	state, err := authenticateHostingRecoveryLease(ctx, runnerID, recoveryID, generation, leaseToken)
	if err == sql.ErrNoRows {
		var acceptedFingerprint string
		receiptErr := db.QueryRowContext(ctx, `SELECT completion_fingerprint
			FROM hosting_runtime_recovery_completion_receipts
			WHERE hosting_runtime_recovery_id=? AND lease_generation=? AND hosting_runner_id=?
			AND lease_token_hash=?`, recoveryID, generation, runnerID, hashToken(leaseToken)).Scan(&acceptedFingerprint)
		if receiptErr == nil {
			if acceptedFingerprint != completionFingerprint {
				return &hostingAPIError{Code: errCodeIdempotencyConflict,
					Message: "recovery completion conflicts with the accepted result", StatusCode: http.StatusConflict}
			}
			return nil
		}
		if receiptErr != sql.ErrNoRows {
			return receiptErr
		}
		var terminalStatus, storedFingerprint string
		terminalErr := db.QueryRowContext(ctx, `SELECT status, completion_fingerprint FROM hosting_runtime_recoveries
			WHERE id=? AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=?
			AND status IN ('succeeded','failed','cancelled')`, recoveryID, runnerID, generation,
			hashToken(leaseToken)).Scan(&terminalStatus, &storedFingerprint)
		if terminalErr == nil {
			if storedFingerprint != "" && storedFingerprint != completionFingerprint {
				return &hostingAPIError{Code: errCodeIdempotencyConflict,
					Message: "terminal recovery completion conflicts with the committed result", StatusCode: http.StatusConflict}
			}
			if terminalStatus == "succeeded" {
				var releaseDigest, artifactDigest, endpoint string
				terminalErr = db.QueryRowContext(ctx, `SELECT release.release_digest,
					release.release_artifact_digest, recovery.runtime_endpoint
					FROM hosting_runtime_recoveries recovery
					JOIN hosting_releases release ON release.id=recovery.hosting_release_id
					WHERE recovery.id=?`, recoveryID).Scan(&releaseDigest, &artifactDigest, &endpoint)
				if terminalErr != nil {
					return terminalErr
				}
				if input.Status != "success" || input.ReleaseDigest != releaseDigest ||
					input.ReleaseArtifactDigest != artifactDigest || input.RuntimeEndpoint != endpoint {
					return &hostingAPIError{Code: errCodeIdempotencyConflict,
						Message: "terminal recovery completion conflicts with the committed result", StatusCode: http.StatusConflict}
				}
			} else if storedFingerprint == "" && ((terminalStatus == "failed" && input.Status != "failed") ||
				(terminalStatus == "cancelled" && input.Status != "cancelled")) {
				return &hostingAPIError{Code: errCodeIdempotencyConflict,
					Message: "terminal recovery completion conflicts with the committed result", StatusCode: http.StatusConflict}
			}
			return nil
		}
		if terminalErr != sql.ErrNoRows {
			return terminalErr
		}
		return &hostingAPIError{Code: errCodeJobForbidden, Message: "valid current recovery lease is required", StatusCode: http.StatusForbidden}
	}
	if err != nil {
		return err
	}
	if state.CancelRequested.Valid {
		if state.CompletionFingerprint == "" {
			state.CompletionFingerprint = completionFingerprint
		}
		return cancelHostingRuntimeRecovery(ctx, state, "durable cancellation intent won over recovery completion")
	}
	if state.CompletionFingerprint != "" && state.CompletionFingerprint != completionFingerprint {
		return &hostingAPIError{Code: errCodeIdempotencyConflict,
			Message: "recovery completion conflicts with the durable completion intent", StatusCode: http.StatusConflict}
	}
	state.CompletionFingerprint = completionFingerprint
	switch input.Status {
	case "cancelled":
		if code := strings.TrimSpace(input.FailureCode); code != "" && code != "cancelled" {
			return &hostingAPIError{Code: errCodeValidation, Message: "cancelled recovery must use the cancelled failure code", StatusCode: http.StatusBadRequest}
		}
		return cancelHostingRuntimeRecovery(ctx, state, redactSecrets(input.FailureMessage))
	case "failed":
		failureCode := strings.TrimSpace(input.FailureCode)
		if _, ok := allowedHostingRecoveryFailureCodes[failureCode]; !ok {
			return &hostingAPIError{Code: errCodeValidation, Message: "failed recovery requires a stable failure code", StatusCode: http.StatusBadRequest}
		}
		return failHostingRuntimeRecovery(ctx, state, failureCode, redactSecrets(input.FailureMessage))
	case "success":
	default:
		return &hostingAPIError{Code: errCodeValidation, Message: "recovery status must be success, failed, or cancelled", StatusCode: http.StatusBadRequest}
	}
	if input.ReleaseDigest != state.ReleaseDigest || input.ReleaseArtifactDigest != state.ReleaseArtifactDigest ||
		!validHealthEvidence(input.HealthEvidence) {
		return failHostingRuntimeRecovery(ctx, state, "artifact_digest_mismatch", "restored runtime identity or health evidence did not match")
	}
	if err := validateRuntimeEndpoint(input.RuntimeEndpoint); err != nil {
		return &hostingAPIError{Code: errCodeInvalidDeployment, Message: err.Error(), StatusCode: http.StatusBadRequest}
	}
	operationID := hashHostingOperation("recover", fmt.Sprint(state.ID), fmt.Sprint(state.LeaseGeneration), state.ReleaseDigest)
	healthJSON, _ := json.Marshal(input.HealthEvidence)
	now := time.Now().UTC()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		conn.Close()
		return err
	}
	result, err := conn.ExecContext(ctx, `UPDATE hosting_runtime_recoveries SET status='running',
		runtime_endpoint=?, health_evidence_json=?, completion_fingerprint=?, lease_expires_at=?
		WHERE id=? AND hosting_runner_id=?
		AND lease_generation=? AND lease_token_hash=? AND status IN ('leased','running')
		AND cancel_requested_at IS NULL AND (completion_fingerprint='' OR completion_fingerprint=?)`, input.RuntimeEndpoint,
		redactSecrets(string(healthJSON)), state.CompletionFingerprint,
		formatSQLiteTime(now.Add(hostingJobLeaseDuration)), state.ID,
		state.RunnerID, state.LeaseGeneration, state.LeaseTokenHash, state.CompletionFingerprint)
	if err == nil {
		if affected, _ := result.RowsAffected(); affected != 1 {
			err = errHostingStateConflict
		}
	}
	if err == nil {
		_, err = conn.ExecContext(ctx, `INSERT INTO hosting_proxy_operations
			(operation_id, hosting_project_id, hosting_deployment_id, hosting_runtime_recovery_id,
			 operation_type, release_digest, runtime_endpoint, expected_previous_release_digest,
			 status, created_at, updated_at)
			VALUES (?, ?, ?, ?, 'recover', ?, ?, ?, 'pending', ?, ?)
			ON CONFLICT(operation_id) DO NOTHING`, operationID, state.ProjectID, state.DeploymentID,
			state.ID, state.ReleaseDigest, input.RuntimeEndpoint, state.ReleaseDigest,
			formatSQLiteTime(now), formatSQLiteTime(now))
	}
	if err == nil {
		var storedRecoveryID int64
		var storedRelease, storedEndpoint, storedExpected string
		err = conn.QueryRowContext(ctx, `SELECT hosting_runtime_recovery_id, release_digest,
			runtime_endpoint, expected_previous_release_digest FROM hosting_proxy_operations
			WHERE operation_id=?`, operationID).Scan(&storedRecoveryID, &storedRelease, &storedEndpoint, &storedExpected)
		if err == nil && (storedRecoveryID != state.ID || storedRelease != state.ReleaseDigest ||
			storedEndpoint != input.RuntimeEndpoint || storedExpected != state.ReleaseDigest) {
			err = &hostingAPIError{Code: errCodeIdempotencyConflict,
				Message: "recovery completion conflicts with the durable proxy intent", StatusCode: http.StatusConflict}
		}
	}
	if err == nil {
		_, err = conn.ExecContext(ctx, `COMMIT`)
	} else {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
	}
	conn.Close()
	if err != nil {
		return err
	}
	proxy, err := hostingProxyClientFactory(appConfig)
	if err != nil {
		_ = markHostingProxyOperationError(ctx, operationID, errCodeProxyUnavailable)
		return err
	}
	activation, err := proxy.Activate(ctx, proxyActivationRequest{OperationID: operationID,
		ExternalProjectID: state.ExternalProjectID, ReleaseDigest: state.ReleaseDigest,
		RuntimeEndpoint: input.RuntimeEndpoint, ExpectedPreviousReleaseDigest: state.ReleaseDigest})
	if err != nil {
		code := errCodeProxyUnavailable
		var apiErr *hostingAPIError
		if errorsAsHosting(err, &apiErr) {
			code = apiErr.Code
		}
		if code == errCodeProxyRejected {
			if markErr := markHostingProxyOperationFailed(ctx, operationID, code); markErr != nil {
				return markErr
			}
			state.Attempts = hostingMaxJobAttempts
			return failHostingRuntimeRecovery(ctx, state, "proxy_activation_failed", err.Error())
		}
		_ = markHostingProxyOperationError(ctx, operationID, code)
		return err
	}
	if err := markHostingProxyOperationApplied(ctx, operationID, activation.RouteRevision); err != nil {
		var operationStatus string
		if statusErr := db.QueryRowContext(ctx, `SELECT status FROM hosting_proxy_operations
			WHERE operation_id=?`, operationID).Scan(&operationStatus); statusErr == nil && operationStatus == "failed" {
			if compensateErr := reconcileLateHostingRecoveryActivation(ctx, proxy, state); compensateErr != nil {
				return compensateErr
			}
			return &hostingAPIError{Code: errCodeProjectSuspended,
				Message: "runtime recovery activation was fenced and compensated", StatusCode: http.StatusConflict}
		}
		return err
	}
	return commitHostingRuntimeRecovery(ctx, state, input.RuntimeEndpoint, activation.RouteRevision)
}

func cancelHostingRuntimeRecovery(ctx context.Context, state *hostingRuntimeRecoveryState, message string) error {
	now := time.Now().UTC()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	result, err := conn.ExecContext(ctx, `UPDATE hosting_runtime_recoveries SET status='cancelled',
		cancel_requested_at=COALESCE(cancel_requested_at, ?), completed_at=?, lease_expires_at=NULL,
		completion_fingerprint=CASE WHEN completion_fingerprint='' THEN ? ELSE completion_fingerprint END
		WHERE id=? AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=?
		AND (completion_fingerprint='' OR ?='' OR completion_fingerprint=?)
		AND status IN ('leased','running') AND NOT EXISTS (
			SELECT 1 FROM hosting_proxy_operations operation
			WHERE operation.hosting_runtime_recovery_id=hosting_runtime_recoveries.id
			  AND operation.status IN ('pending','applied'))`, formatSQLiteTime(now), formatSQLiteTime(now),
		state.CompletionFingerprint, state.ID, state.RunnerID, state.LeaseGeneration, state.LeaseTokenHash,
		state.CompletionFingerprint, state.CompletionFingerprint)
	if err == nil {
		if affected, _ := result.RowsAffected(); affected != 1 {
			err = errHostingStateConflict
		}
	}
	if err == nil {
		err = restoreHostingRunnerCapacity(ctx, conn, state.RunnerID, state.Limits)
	}
	if err == nil {
		err = recordHostingEvent(ctx, conn, state.ProjectID, state.DeploymentID,
			"runtime_recovery_cancelled", hostingPhaseCancelled, "cancelled",
			map[string]any{"message": redactSecrets(message)}, now)
	}
	if err == nil {
		_, err = conn.ExecContext(ctx, `COMMIT`)
	} else {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
	}
	return err
}

func commitHostingRuntimeRecovery(ctx context.Context, state *hostingRuntimeRecoveryState, endpoint, routeRevision string) error {
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
	var desired, kill string
	var global int
	if err := conn.QueryRowContext(ctx, `SELECT project.desired_state, project.kill_switch_reason,
		settings.global_kill_switch FROM hosting_projects project JOIN hosting_settings settings ON settings.id=1
		WHERE project.id=?`, state.ProjectID).Scan(&desired, &kill, &global); err != nil {
		return err
	}
	if desired != "active" || kill != "" || global != 0 {
		return &hostingAPIError{Code: errCodeProjectSuspended, Message: "project execution is disabled during runtime recovery", StatusCode: http.StatusConflict}
	}
	result, err := conn.ExecContext(ctx, `UPDATE hosting_runtime_recoveries SET status='succeeded',
		runtime_endpoint=?, completed_at=?, lease_expires_at=NULL WHERE id=? AND hosting_runner_id=?
		AND lease_generation=? AND lease_token_hash=? AND status='running' AND cancel_requested_at IS NULL`, endpoint, formatSQLiteTime(now),
		state.ID, state.RunnerID, state.LeaseGeneration, state.LeaseTokenHash)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		var recoveryStatus, storedEndpoint, runtimeInstance, operationStatus, storedRevision string
		var runtimeRunnerID int64
		operationID := hashHostingOperation("recover", fmt.Sprint(state.ID), fmt.Sprint(state.LeaseGeneration), state.ReleaseDigest)
		replayErr := conn.QueryRowContext(ctx, `SELECT recovery.status, recovery.runtime_endpoint,
			release.runtime_runner_id, release.runtime_instance_id, operation.status, operation.route_revision
			FROM hosting_runtime_recoveries recovery
			JOIN hosting_releases release ON release.id=recovery.hosting_release_id
			JOIN hosting_proxy_operations operation ON operation.hosting_runtime_recovery_id=recovery.id
			WHERE recovery.id=? AND operation.operation_id=?`, state.ID, operationID).Scan(&recoveryStatus,
			&storedEndpoint, &runtimeRunnerID, &runtimeInstance, &operationStatus, &storedRevision)
		if replayErr == nil && recoveryStatus == "succeeded" && storedEndpoint == endpoint &&
			runtimeRunnerID == state.RunnerID && runtimeInstance == fmt.Sprintf("restore-%d-%d", state.ID, state.LeaseGeneration) &&
			operationStatus == "committed" && storedRevision == routeRevision {
			if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
				return err
			}
			committed = true
			return nil
		}
		return &hostingAPIError{Code: errCodeJobForbidden, Message: "runtime recovery lease is no longer current", StatusCode: http.StatusForbidden}
	}
	releaseResult, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET runtime_endpoint=?, route_revision=?,
		runtime_runner_id=?, runtime_generation=runtime_generation+1, runtime_instance_id=?
		WHERE id=? AND status='active'`, endpoint, routeRevision, state.RunnerID,
		fmt.Sprintf("restore-%d-%d", state.ID, state.LeaseGeneration), state.ReleaseID)
	if err != nil {
		return err
	}
	if affected, _ := releaseResult.RowsAffected(); affected != 1 {
		return errHostingStateConflict
	}
	operationID := hashHostingOperation("recover", fmt.Sprint(state.ID), fmt.Sprint(state.LeaseGeneration), state.ReleaseDigest)
	operationResult, err := conn.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='committed',
		route_revision=?, updated_at=? WHERE operation_id=? AND status='applied'`, routeRevision,
		formatSQLiteTime(now), operationID)
	if err != nil {
		return err
	}
	if affected, _ := operationResult.RowsAffected(); affected != 1 {
		return errHostingStateConflict
	}
	if state.RuntimeOwnerRunnerID != state.RunnerID {
		if err := recomputeHostingRunnerCapacity(ctx, conn, state.RuntimeOwnerRunnerID); err != nil {
			return err
		}
	}
	if err := recordHostingEvent(ctx, conn, state.ProjectID, state.DeploymentID,
		"runtime_recovered", hostingPhaseActive, "", map[string]any{"runner_id": state.RunnerID, "route_revision": routeRevision}, now); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return err
	}
	committed = true
	return nil
}

func failHostingRuntimeRecovery(ctx context.Context, state *hostingRuntimeRecoveryState, failureCode, message string) error {
	if _, ok := allowedHostingRecoveryFailureCodes[failureCode]; !ok {
		failureCode = "runtime_start_failed"
	}
	now := time.Now().UTC()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	status := "queued"
	if state.Attempts >= hostingMaxJobAttempts {
		status = "failed"
	}
	result, err := conn.ExecContext(ctx, `UPDATE hosting_runtime_recoveries SET status=?,
		hosting_runner_id=CASE WHEN ?='queued' THEN NULL ELSE hosting_runner_id END,
		lease_token_hash=CASE WHEN ?='queued' THEN '' ELSE lease_token_hash END,
		lease_expires_at=NULL, completed_at=CASE WHEN ?='failed' THEN ? ELSE completed_at END,
		completion_fingerprint=CASE WHEN ?='queued' THEN ''
			WHEN completion_fingerprint='' THEN ? ELSE completion_fingerprint END
		WHERE id=? AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=?
		AND (completion_fingerprint='' OR ?='' OR completion_fingerprint=?)
		AND status IN ('leased','running') AND NOT EXISTS (
			SELECT 1 FROM hosting_proxy_operations operation
			WHERE operation.hosting_runtime_recovery_id=hosting_runtime_recoveries.id
			  AND operation.status IN ('pending','applied'))`, status, status, status,
		status, formatSQLiteTime(now), status, state.CompletionFingerprint, state.ID, state.RunnerID,
		state.LeaseGeneration, state.LeaseTokenHash, state.CompletionFingerprint, state.CompletionFingerprint)
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		var acceptedFingerprint string
		receiptErr := db.QueryRowContext(ctx, `SELECT completion_fingerprint
			FROM hosting_runtime_recovery_completion_receipts
			WHERE hosting_runtime_recovery_id=? AND lease_generation=? AND hosting_runner_id=?
			AND lease_token_hash=?`, state.ID, state.LeaseGeneration, state.RunnerID,
			state.LeaseTokenHash).Scan(&acceptedFingerprint)
		if receiptErr == nil {
			if acceptedFingerprint == state.CompletionFingerprint {
				return nil
			}
			return &hostingAPIError{Code: errCodeIdempotencyConflict,
				Message: "recovery completion conflicts with the accepted result", StatusCode: http.StatusConflict}
		}
		if receiptErr != sql.ErrNoRows {
			return receiptErr
		}
		return errHostingStateConflict
	}
	if status == "queued" {
		_, err = conn.ExecContext(ctx, `INSERT INTO hosting_runtime_recovery_completion_receipts
			(hosting_runtime_recovery_id, lease_generation, hosting_runner_id, lease_token_hash,
			 completion_fingerprint, accepted_at) VALUES (?, ?, ?, ?, ?, ?)`, state.ID,
			state.LeaseGeneration, state.RunnerID, state.LeaseTokenHash, state.CompletionFingerprint,
			formatSQLiteTime(now))
		if err != nil {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
			return err
		}
	}
	if err := restoreHostingRunnerCapacity(ctx, conn, state.RunnerID, state.Limits); err != nil {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		return err
	}
	if status == "failed" {
		if _, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET status='failed', runtime_runner_id=NULL WHERE id=?`, state.ReleaseID); err != nil {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
			return err
		}
		if err := stageHostingRuntimeUnavailable(ctx, conn, state.ProjectID, state.DeploymentID,
			state.ReleaseID, now); err != nil {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
			return err
		}
		if state.RuntimeOwnerRunnerID != state.RunnerID {
			if err := recomputeHostingRunnerCapacity(ctx, conn, state.RuntimeOwnerRunnerID); err != nil {
				_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
				return err
			}
		}
	}
	if err := recordHostingEvent(ctx, conn, state.ProjectID, state.DeploymentID,
		"runtime_recovery_"+status, hostingPhaseFailed, failureCode, map[string]any{"message": redactSecrets(message)}, now); err != nil {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		return err
	}
	_, err = conn.ExecContext(ctx, `COMMIT`)
	return err
}

func reconcileHostingRecoveryProxyOperations(ctx context.Context) error {
	rows, err := db.QueryContext(ctx, `SELECT operation.operation_id, operation.status,
		operation.runtime_endpoint, operation.route_revision, recovery.id, recovery.hosting_release_id,
		release.hosting_deployment_id, release.hosting_project_id, recovery.hosting_runner_id,
		release.runtime_runner_id,
		recovery.lease_generation, recovery.lease_token_hash, recovery.completion_fingerprint,
		recovery.status, recovery.attempts,
		project.external_project_id, deployment.external_deployment_id, release.release_digest,
		release.runtime_endpoint, recovery.required_cpu_millis, recovery.required_ram_bytes,
		recovery.required_disk_bytes, recovery.required_pids, recovery.cancel_requested_at,
		recovery.lease_expires_at, runner.status, runner.last_seen, project.manifest_json
		FROM hosting_proxy_operations operation
		JOIN hosting_runtime_recoveries recovery ON recovery.id=operation.hosting_runtime_recovery_id
		JOIN hosting_releases release ON release.id=recovery.hosting_release_id
		JOIN hosting_projects project ON project.id=release.hosting_project_id
		JOIN hosting_deployments deployment ON deployment.id=release.hosting_deployment_id
		JOIN hosting_runners runner ON runner.id=recovery.hosting_runner_id
		WHERE operation.operation_type='recover' AND operation.status IN ('pending','applied')
		ORDER BY operation.id`)
	if err != nil {
		return err
	}
	type recoveryOperation struct {
		operationID, operationStatus, endpoint, routeRevision string
		state                                                 hostingRuntimeRecoveryState
	}
	var operations []recoveryOperation
	for rows.Next() {
		var operation recoveryOperation
		var manifestJSON string
		if err := rows.Scan(&operation.operationID, &operation.operationStatus, &operation.endpoint,
			&operation.routeRevision, &operation.state.ID, &operation.state.ReleaseID,
			&operation.state.DeploymentID, &operation.state.ProjectID, &operation.state.RunnerID,
			&operation.state.RuntimeOwnerRunnerID, &operation.state.LeaseGeneration,
			&operation.state.LeaseTokenHash, &operation.state.CompletionFingerprint, &operation.state.Status,
			&operation.state.Attempts, &operation.state.ExternalProjectID,
			&operation.state.ExternalDeploymentID, &operation.state.ReleaseDigest,
			&operation.state.PreviousRuntimeEndpoint, &operation.state.Limits.CPUMillis,
			&operation.state.Limits.RAMBytes, &operation.state.Limits.DiskBytes,
			&operation.state.Limits.PIDs, &operation.state.CancelRequested,
			&operation.state.LeaseExpiresAt, &operation.state.RunnerStatus,
			&operation.state.RunnerLastSeen, &manifestJSON); err != nil {
			rows.Close()
			return err
		}
		var manifest HostingProjectManifest
		if err := json.Unmarshal([]byte(manifestJSON), &manifest); err != nil {
			rows.Close()
			return err
		}
		operation.state.Runtime = manifest.Runtime
		operations = append(operations, operation)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, operation := range operations {
		cancelledOrTerminal := operation.state.CancelRequested.Valid ||
			operation.state.Status == "cancelled" || operation.state.Status == "failed"
		if cancelledOrTerminal {
			proxy, err := hostingProxyClientFactory(appConfig)
			if err != nil {
				_ = markHostingProxyOperationError(ctx, operation.operationID, errCodeProxyUnavailable)
				continue
			}
			if err := compensateCancelledHostingRecovery(ctx, proxy, &operation.state); err != nil {
				return err
			}
			if err := markHostingProxyOperationFailed(ctx, operation.operationID, errCodeProjectSuspended); err != nil {
				return err
			}
			if operation.state.Status == "leased" || operation.state.Status == "running" {
				if err := cancelHostingRuntimeRecovery(ctx, &operation.state, "durable cancellation intent won over proxy activation"); err != nil {
					return err
				}
			}
			continue
		}
		proxy, err := hostingProxyClientFactory(appConfig)
		if err != nil {
			_ = markHostingProxyOperationError(ctx, operation.operationID, errCodeProxyUnavailable)
			continue
		}
		allowed, err := hostingActivationAllowed(ctx, operation.state.ProjectID)
		if err != nil {
			return err
		}
		if !allowed {
			if err := compensateCancelledHostingRecovery(ctx, proxy, &operation.state); err != nil {
				return err
			}
			if err := markHostingProxyOperationFailed(ctx, operation.operationID, errCodeProjectSuspended); err != nil {
				return err
			}
			if err := cancelHostingRuntimeRecovery(ctx, &operation.state, "execution disabled during recovery"); err != nil {
				return err
			}
			continue
		}
		runnerLive := operation.state.RunnerStatus == "online" && operation.state.RunnerLastSeen.Valid &&
			!parseSQLiteTime(operation.state.RunnerLastSeen.String).Before(time.Now().UTC().Add(-hostingRunnerStaleAfter))
		healthPath := operation.state.Runtime.HealthPath
		if healthPath == "" {
			healthPath = "/"
		}
		healthCtx, cancelHealth := context.WithTimeout(ctx, 30*time.Second)
		_, _, healthErr := checkHostingCandidateHealth(healthCtx, strings.TrimRight(operation.endpoint, "/")+healthPath)
		cancelHealth()
		if !runnerLive || healthErr != nil {
			if err := markHostingProxyOperationFailed(ctx, operation.operationID, errCodeReleaseNotHealthy); err != nil {
				return err
			}
			operation.state.Attempts = hostingMaxJobAttempts
			message := "recovery runner is no longer live"
			if healthErr != nil {
				message = "recovered runtime failed reconciliation health gate"
			}
			if err := failHostingRuntimeRecovery(ctx, &operation.state, "health_check_failed", message); err != nil {
				return err
			}
			if err := reconcileHostingDesiredStateOperations(ctx); err != nil {
				return err
			}
			continue
		}
		if operation.operationStatus == "pending" {
			activation, activateErr := proxy.Activate(ctx, proxyActivationRequest{
				OperationID: operation.operationID, ExternalProjectID: operation.state.ExternalProjectID,
				ReleaseDigest: operation.state.ReleaseDigest, RuntimeEndpoint: operation.endpoint,
				ExpectedPreviousReleaseDigest: operation.state.ReleaseDigest,
			})
			if activateErr != nil {
				code := errCodeProxyUnavailable
				var apiErr *hostingAPIError
				if errorsAsHosting(activateErr, &apiErr) {
					code = apiErr.Code
				}
				if code == errCodeProxyRejected {
					if err := markHostingProxyOperationFailed(ctx, operation.operationID, code); err != nil {
						return err
					}
					operation.state.Attempts = hostingMaxJobAttempts
					if err := failHostingRuntimeRecovery(ctx, &operation.state, "proxy_activation_failed", activateErr.Error()); err != nil {
						return err
					}
					continue
				}
				_ = markHostingProxyOperationError(ctx, operation.operationID, code)
				continue
			}
			operation.routeRevision = activation.RouteRevision
			if err := markHostingProxyOperationApplied(ctx, operation.operationID, activation.RouteRevision); err != nil {
				return err
			}
		}
		if err := commitHostingRuntimeRecovery(ctx, &operation.state, operation.endpoint, operation.routeRevision); err != nil {
			return err
		}
	}
	return nil
}
