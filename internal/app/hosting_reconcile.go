package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	hostingReconcileInterval = 15 * time.Second
	hostingMaxJobAttempts    = 3
)

func runHostingReconciler(ctx context.Context) {
	if err := reconcileHostingState(ctx, time.Now().UTC()); err != nil {
		logOperationalError("reconcile hosting state", err)
	}
	ticker := time.NewTicker(hostingReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if err := reconcileHostingState(ctx, now.UTC()); err != nil {
				logOperationalError("reconcile hosting state", err)
			}
		}
	}
}

func reconcileHostingState(ctx context.Context, now time.Time) error {
	if err := reconcileHostingDesiredStateOperations(ctx); err != nil {
		return err
	}
	if err := reconcileHostingRollbackOperations(ctx); err != nil {
		return err
	}
	if err := reconcileHostingRecoveryProxyOperations(ctx); err != nil {
		return err
	}
	if err := reconcileHostingActivationOperations(ctx); err != nil {
		return err
	}
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
	staleBefore := formatSQLiteTime(now.Add(-hostingRunnerStaleAfter))
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_runners SET status='offline'
		WHERE status='online' AND (last_seen IS NULL OR last_seen<?)`, staleBefore); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `UPDATE callback_outbox SET status='pending', locked_at=NULL, claim_token_hash=''
		WHERE status='delivering' AND locked_at<?`, formatSQLiteTime(now.Add(-callbackLeaseTimeout))); err != nil {
		return err
	}
	if err := reconcileExpiredHostingRecoveries(ctx, conn, now); err != nil {
		return err
	}
	if err := queueLostHostingRuntimes(ctx, conn, now); err != nil {
		return err
	}
	if err := placeHostingRuntimeRecoveries(ctx, conn, now); err != nil {
		return err
	}

	type expiredJob struct {
		jobID, deploymentID, projectID, runnerID int64
		attempts                                 int
		limits                                   hostingWorkloadLimits
		cancelRequested                          sql.NullString
	}
	rows, err := conn.QueryContext(ctx, `SELECT j.id, j.hosting_deployment_id, d.hosting_project_id,
		j.hosting_runner_id, j.required_cpu_millis, j.required_ram_bytes, j.required_disk_bytes,
		j.required_pids, j.attempts, j.cancel_requested_at
		FROM hosting_jobs j
		JOIN hosting_deployments d ON d.id=j.hosting_deployment_id
		JOIN hosting_runners r ON r.id=j.hosting_runner_id
		WHERE j.status IN ('leased','running') AND j.lease_expires_at<?
		  AND NOT EXISTS (SELECT 1 FROM hosting_proxy_operations op
			WHERE op.hosting_deployment_id=j.hosting_deployment_id AND op.status IN ('pending','applied'))`, formatSQLiteTime(now))
	if err != nil {
		return err
	}
	var expired []expiredJob
	for rows.Next() {
		var job expiredJob
		if err := rows.Scan(&job.jobID, &job.deploymentID, &job.projectID, &job.runnerID,
			&job.limits.CPUMillis, &job.limits.RAMBytes, &job.limits.DiskBytes, &job.limits.PIDs,
			&job.attempts, &job.cancelRequested); err != nil {
			rows.Close()
			return err
		}
		expired = append(expired, job)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, job := range expired {
		if err := restoreHostingRunnerCapacity(ctx, conn, job.runnerID, job.limits); err != nil {
			return err
		}
		if job.cancelRequested.Valid {
			if _, err := conn.ExecContext(ctx, `UPDATE hosting_jobs SET status='cancelled', hosting_runner_id=NULL,
				lease_token_hash='', lease_expires_at=NULL, completed_at=? WHERE id=?`, formatSQLiteTime(now), job.jobID); err != nil {
				return err
			}
			if _, err := conn.ExecContext(ctx, `UPDATE hosting_deployments SET status='cancelled', phase='cancelled',
				failure_code='cancelled', finished_at=?, updated_at=? WHERE id=? AND status='running'`, formatSQLiteTime(now), formatSQLiteTime(now), job.deploymentID); err != nil {
				return err
			}
			if err := recordHostingEvent(ctx, conn, job.projectID, job.deploymentID, "deployment_cancelled_after_runner_loss", hostingPhaseCancelled, "cancelled", nil, now); err != nil {
				return err
			}
			if err := enqueueTerminalCallback(ctx, conn, job.deploymentID, now); err != nil {
				return err
			}
			continue
		}
		if job.attempts >= hostingMaxJobAttempts {
			if _, err := conn.ExecContext(ctx, `UPDATE hosting_jobs SET status='failed', hosting_runner_id=NULL,
				lease_token_hash='', lease_expires_at=NULL, completed_at=? WHERE id=?`, formatSQLiteTime(now), job.jobID); err != nil {
				return err
			}
			if _, err := conn.ExecContext(ctx, `UPDATE hosting_deployments SET status='failed', phase='failed',
				failure_code='runner_lost', failure_message='hosting runner lease was lost repeatedly',
				finished_at=?, updated_at=? WHERE id=? AND status='running'`, formatSQLiteTime(now), formatSQLiteTime(now), job.deploymentID); err != nil {
				return err
			}
			if err := recordHostingEvent(ctx, conn, job.projectID, job.deploymentID, "deployment_failed_after_runner_loss", hostingPhaseFailed, "runner_lost", nil, now); err != nil {
				return err
			}
			if err := enqueueTerminalCallback(ctx, conn, job.deploymentID, now); err != nil {
				return err
			}
			continue
		}
		if _, err := conn.ExecContext(ctx, `UPDATE hosting_releases SET status='failed'
			WHERE hosting_deployment_id=? AND status IN ('candidate','healthy')`, job.deploymentID); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `UPDATE hosting_jobs SET status='queued', hosting_runner_id=NULL,
			lease_token_hash='', lease_expires_at=NULL, completion_fingerprint='' WHERE id=?`, job.jobID); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `UPDATE hosting_deployments SET phase='queued', failure_code='',
			failure_message='', updated_at=? WHERE id=? AND status='running'`, formatSQLiteTime(now), job.deploymentID); err != nil {
			return err
		}
		if err := recordHostingEvent(ctx, conn, job.projectID, job.deploymentID, "runner_lease_expired", hostingPhaseQueued, "runner_lost", nil, now); err != nil {
			return err
		}
	}
	if err := placeUnassignedHostingJobs(ctx, conn, now); err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return err
	}
	committed = true
	return nil
}

func reconcileHostingRollbackOperations(ctx context.Context) error {
	rows, err := db.QueryContext(ctx, `SELECT op.operation_id, op.status, op.release_digest,
		op.runtime_endpoint, op.expected_previous_release_digest, op.route_revision,
		op.hosting_project_id, p.external_project_id, d.external_deployment_id, target.runtime_manifest_json,
		COALESCE(previous.runtime_endpoint, '')
		FROM hosting_proxy_operations op
		JOIN hosting_projects p ON p.id=op.hosting_project_id
		JOIN hosting_releases target ON target.hosting_project_id=op.hosting_project_id
			AND target.release_digest=op.release_digest
		JOIN hosting_deployments d ON d.id=target.hosting_deployment_id
		LEFT JOIN hosting_releases previous ON previous.hosting_project_id=op.hosting_project_id
			AND previous.release_digest=op.expected_previous_release_digest
		WHERE op.operation_type='rollback' AND op.status IN ('pending','applied') ORDER BY op.id`)
	if err != nil {
		return err
	}
	type rollbackOperation struct {
		operationID, status, digest, endpoint, previous, routeRevision          string
		projectID                                                               int64
		externalProjectID, externalDeploymentID, manifestJSON, previousEndpoint string
	}
	var operations []rollbackOperation
	for rows.Next() {
		var operation rollbackOperation
		if err := rows.Scan(&operation.operationID, &operation.status, &operation.digest,
			&operation.endpoint, &operation.previous, &operation.routeRevision, &operation.projectID,
			&operation.externalProjectID, &operation.externalDeploymentID, &operation.manifestJSON,
			&operation.previousEndpoint); err != nil {
			rows.Close()
			return err
		}
		operations = append(operations, operation)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, operation := range operations {
		allowed, err := hostingActivationAllowed(ctx, operation.projectID)
		if err != nil {
			return err
		}
		proxy, err := hostingProxyClientFactory(appConfig)
		if err != nil {
			_ = markHostingProxyOperationError(ctx, operation.operationID, errCodeProxyUnavailable)
			continue
		}
		state := &hostingCompletionState{ProjectID: operation.projectID,
			ExternalProjectID: operation.externalProjectID, ExternalDeploymentID: operation.externalDeploymentID,
			PreviousReleaseDigest: operation.previous, PreviousRuntimeEndpoint: operation.previousEndpoint}
		if !allowed {
			if operation.status == "applied" {
				if err := compensateCancelledActivation(ctx, proxy, state, operation.digest); err != nil {
					return err
				}
			}
			if err := markHostingProxyOperationFailed(ctx, operation.operationID, errCodeProjectSuspended); err != nil {
				return err
			}
			continue
		}
		if operation.status == "pending" {
			var runtime HostingRuntimeManifest
			if err := json.Unmarshal([]byte(operation.manifestJSON), &runtime); err != nil {
				return err
			}
			healthPath := runtime.HealthPath
			if healthPath == "" {
				healthPath = "/"
			}
			healthCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			_, _, healthErr := checkHostingCandidateHealth(healthCtx, strings.TrimRight(operation.endpoint, "/")+healthPath)
			cancel()
			if healthErr != nil {
				_ = markHostingProxyOperationFailed(ctx, operation.operationID, errCodeReleaseNotHealthy)
				continue
			}
			activation, activateErr := proxy.Activate(ctx, proxyActivationRequest{
				OperationID: operation.operationID, ExternalProjectID: operation.externalProjectID,
				ReleaseDigest: operation.digest, RuntimeEndpoint: operation.endpoint,
				ExpectedPreviousReleaseDigest: operation.previous,
			})
			if activateErr != nil {
				_ = markHostingProxyOperationError(ctx, operation.operationID, errCodeProxyUnavailable)
				continue
			}
			operation.routeRevision = activation.RouteRevision
			if err := markHostingProxyOperationApplied(ctx, operation.operationID, activation.RouteRevision); err != nil {
				return err
			}
		}
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
			conn.Close()
			return err
		}
		var desired, kill string
		var global int
		err = conn.QueryRowContext(ctx, `SELECT p.desired_state, p.kill_switch_reason, s.global_kill_switch
			FROM hosting_projects p JOIN hosting_settings s ON s.id=1 WHERE p.id=?`, operation.projectID).Scan(&desired, &kill, &global)
		if err == nil && desired == "active" && kill == "" && global == 0 {
			_, err = conn.ExecContext(ctx, `UPDATE hosting_releases SET status='inactive', deactivated_at=?
				WHERE hosting_project_id=? AND status='active' AND release_digest<>?`, formatSQLiteTime(time.Now().UTC()), operation.projectID, operation.digest)
		}
		if err == nil && desired == "active" && kill == "" && global == 0 {
			_, err = conn.ExecContext(ctx, `UPDATE hosting_releases SET status='active', route_revision=?,
				previous_release_digest=?, activated_at=?, deactivated_at=NULL
				WHERE hosting_project_id=? AND release_digest=?`, operation.routeRevision, operation.previous,
				formatSQLiteTime(time.Now().UTC()), operation.projectID, operation.digest)
		}
		if err == nil && desired == "active" && kill == "" && global == 0 {
			_, err = conn.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='committed', updated_at=?
				WHERE operation_id=? AND status='applied'`, formatSQLiteTime(time.Now().UTC()), operation.operationID)
		}
		if err == nil && desired == "active" && kill == "" && global == 0 {
			_, err = conn.ExecContext(ctx, `COMMIT`)
		} else {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
		conn.Close()
		if err != nil {
			return err
		}
		if desired != "active" || kill != "" || global != 0 {
			if err := compensateCancelledActivation(ctx, proxy, state, operation.digest); err != nil {
				return err
			}
			if err := markHostingProxyOperationFailed(ctx, operation.operationID, errCodeProjectSuspended); err != nil {
				return err
			}
		}
	}
	return nil
}

func reconcileHostingDesiredStateOperations(ctx context.Context) error {
	rows, err := db.QueryContext(ctx, `SELECT op.operation_id, op.operation_type, p.external_project_id
		FROM hosting_proxy_operations op JOIN hosting_projects p ON p.id=op.hosting_project_id
		WHERE op.operation_type IN ('suspend','resume') AND op.status='pending' ORDER BY op.id`)
	if err != nil {
		return err
	}
	type desiredOperation struct {
		operationID, operationType, externalProjectID string
	}
	var operations []desiredOperation
	for rows.Next() {
		var operation desiredOperation
		if err := rows.Scan(&operation.operationID, &operation.operationType, &operation.externalProjectID); err != nil {
			rows.Close()
			return err
		}
		operations = append(operations, operation)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, operation := range operations {
		proxy, err := hostingProxyClientFactory(appConfig)
		if err != nil {
			_ = markHostingProxyOperationError(ctx, operation.operationID, errCodeProxyUnavailable)
			continue
		}
		err = proxy.SetSuspended(ctx, proxySuspendRequest{
			OperationID:       operation.operationID,
			ExternalProjectID: operation.externalProjectID,
			Suspended:         operation.operationType == "suspend",
		})
		if err != nil {
			code := errCodeProxyUnavailable
			var apiErr *hostingAPIError
			if errorsAsHosting(err, &apiErr) {
				code = apiErr.Code
			}
			_ = markHostingProxyOperationError(ctx, operation.operationID, code)
			continue
		}
		if err := markHostingProxyOperationCommitted(ctx, operation.operationID); err != nil {
			return err
		}
	}
	return nil
}

func reconcileHostingActivationOperations(ctx context.Context) error {
	rows, err := db.QueryContext(ctx, `SELECT op.operation_id, op.status, op.release_digest,
		op.runtime_endpoint, op.expected_previous_release_digest, op.route_revision,
		j.id, j.hosting_deployment_id, d.hosting_project_id, j.hosting_runner_id,
		j.lease_generation, j.lease_token_hash, j.status, p.external_project_id,
		d.external_deployment_id, d.commit_sha, d.artifact_digest, j.cancel_requested_at,
		j.lease_expires_at, j.required_cpu_millis, j.required_ram_bytes,
		j.required_disk_bytes, j.required_pids
		FROM hosting_proxy_operations op
		JOIN hosting_deployments d ON d.id=op.hosting_deployment_id
		JOIN hosting_projects p ON p.id=d.hosting_project_id
		JOIN hosting_jobs j ON j.hosting_deployment_id=d.id
		WHERE op.operation_type='activate' AND op.status IN ('pending','applied') ORDER BY op.id`)
	if err != nil {
		return err
	}
	type pendingActivation struct {
		operationID, status, releaseDigest, runtimeEndpoint, routeRevision string
		state                                                              hostingCompletionState
	}
	var operations []pendingActivation
	for rows.Next() {
		var operation pendingActivation
		if err := rows.Scan(&operation.operationID, &operation.status, &operation.releaseDigest,
			&operation.runtimeEndpoint, &operation.state.PreviousReleaseDigest, &operation.routeRevision,
			&operation.state.JobID, &operation.state.DeploymentID, &operation.state.ProjectID,
			&operation.state.RunnerID, &operation.state.LeaseGeneration, &operation.state.LeaseTokenHash,
			&operation.state.JobStatus, &operation.state.ExternalProjectID,
			&operation.state.ExternalDeploymentID, &operation.state.CommitSHA,
			&operation.state.ArtifactDigest, &operation.state.CancelRequestedAt,
			&operation.state.LeaseExpiresAt, &operation.state.Limits.CPUMillis,
			&operation.state.Limits.RAMBytes, &operation.state.Limits.DiskBytes,
			&operation.state.Limits.PIDs); err != nil {
			rows.Close()
			return err
		}
		_ = db.QueryRowContext(ctx, `SELECT runtime_endpoint FROM hosting_releases
			WHERE hosting_project_id=? AND release_digest=?`, operation.state.ProjectID,
			operation.state.PreviousReleaseDigest).Scan(&operation.state.PreviousRuntimeEndpoint)
		operations = append(operations, operation)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, operation := range operations {
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
			if err := compensateCancelledActivation(ctx, proxy, &operation.state, operation.releaseDigest); err != nil {
				return err
			}
			if err := markHostingProxyOperationCommitted(ctx, operation.operationID); err != nil {
				return err
			}
			if err := finishHostingJobTerminal(ctx, &operation.state, hostingStatusCancelled, hostingPhaseCancelled, "cancelled", "execution disabled while reconciling activation"); err != nil {
				return err
			}
			continue
		}
		if operation.status == "pending" {
			activation, activateErr := proxy.Activate(ctx, proxyActivationRequest{
				OperationID:                   operation.operationID,
				ExternalProjectID:             operation.state.ExternalProjectID,
				ReleaseDigest:                 operation.releaseDigest,
				RuntimeEndpoint:               operation.runtimeEndpoint,
				ExpectedPreviousReleaseDigest: operation.state.PreviousReleaseDigest,
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
					if err := failHostingActivation(ctx, &operation.state, operation.releaseDigest, activateErr); err != nil {
						return err
					}
				} else {
					_ = markHostingProxyOperationError(ctx, operation.operationID, code)
				}
				continue
			}
			operation.routeRevision = activation.RouteRevision
			if err := markHostingProxyOperationApplied(ctx, operation.operationID, activation.RouteRevision); err != nil {
				return err
			}
		}
		cancelled, err := activateHealthyHostingRelease(ctx, &operation.state, operation.releaseDigest, operation.routeRevision)
		if err != nil {
			return fmt.Errorf("commit reconciled proxy activation %s: %w", operation.operationID, err)
		}
		if !cancelled {
			continue
		}
		if err := compensateCancelledActivation(ctx, proxy, &operation.state, operation.releaseDigest); err != nil {
			return err
		}
		if err := markHostingProxyOperationCommitted(ctx, operation.operationID); err != nil {
			return err
		}
		if err := finishHostingJobTerminal(ctx, &operation.state, hostingStatusCancelled, hostingPhaseCancelled, "cancelled", "cancelled while reconciling activation"); err != nil {
			return err
		}
	}
	return nil
}

func placeUnassignedHostingJobs(ctx context.Context, conn *sql.Conn, now time.Time) error {
	var globallyDisabled int
	if err := conn.QueryRowContext(ctx, `SELECT global_kill_switch FROM hosting_settings WHERE id=1`).Scan(&globallyDisabled); err != nil {
		return err
	}
	if globallyDisabled != 0 {
		return nil
	}
	rows, err := conn.QueryContext(ctx, `SELECT j.id, j.hosting_deployment_id, d.hosting_project_id,
		p.external_project_id, p.manifest_json, p.manifest_digest, p.resource_profile, p.deploy_path,
		p.desired_state, p.kill_switch_reason, j.required_cpu_millis, j.required_ram_bytes,
		j.required_disk_bytes, j.required_pids
		FROM hosting_jobs j
		JOIN hosting_deployments d ON d.id=j.hosting_deployment_id
		JOIN hosting_projects p ON p.id=d.hosting_project_id
		WHERE j.status='queued' AND j.hosting_runner_id IS NULL AND j.cancel_requested_at IS NULL
		  AND d.status IN ('queued','running') AND p.desired_state='active' AND p.kill_switch_reason=''
		ORDER BY j.created_at, j.id`)
	if err != nil {
		return err
	}
	type queuedJob struct {
		jobID, deploymentID, projectID int64
		project                        hostingProjectState
		manifestJSON                   string
		limits                         hostingWorkloadLimits
	}
	var jobs []queuedJob
	for rows.Next() {
		var job queuedJob
		if err := rows.Scan(&job.jobID, &job.deploymentID, &job.projectID,
			&job.project.ExternalProjectID, &job.manifestJSON, &job.project.ManifestDigest,
			&job.project.ResourceProfile, &job.project.DeployPath, &job.project.DesiredState,
			&job.project.KillSwitchReason, &job.limits.CPUMillis, &job.limits.RAMBytes,
			&job.limits.DiskBytes, &job.limits.PIDs); err != nil {
			rows.Close()
			return err
		}
		job.project.ID = job.projectID
		if err := json.Unmarshal([]byte(job.manifestJSON), &job.project.Manifest); err != nil {
			rows.Close()
			return err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, job := range jobs {
		runnerID, err := reserveHostingRunner(ctx, conn, &job.project, job.limits, "build", 0)
		if err != nil {
			var apiErr *hostingAPIError
			if errors.As(err, &apiErr) && apiErr.Code == errCodeRunnerCapacityUnavailable {
				continue
			}
			return err
		}
		result, err := conn.ExecContext(ctx, `UPDATE hosting_jobs SET hosting_runner_id=?
			WHERE id=? AND status='queued' AND hosting_runner_id IS NULL`, runnerID, job.jobID)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 1 {
			if err := restoreHostingRunnerCapacity(ctx, conn, runnerID, job.limits); err != nil {
				return err
			}
			continue
		}
		if err := recordHostingEvent(ctx, conn, job.projectID, job.deploymentID, "job_reassigned", hostingPhaseQueued, "", map[string]any{"runner_id": runnerID}, now); err != nil {
			return err
		}
	}
	return nil
}
