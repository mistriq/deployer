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

var hostingActivationReconcileHealthTimeout = 30 * time.Second

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
	var externalErr error
	if err := reconcileHostingCompensationOperations(ctx); err != nil {
		externalErr = errors.Join(externalErr, fmt.Errorf("reconcile hosting compensation: %w", err))
	}
	if err := reconcileHostingDesiredStateOperations(ctx); err != nil {
		externalErr = errors.Join(externalErr, fmt.Errorf("reconcile hosting desired state: %w", err))
	}
	if err := reconcileHostingRuntimeRollbacks(ctx); err != nil {
		externalErr = errors.Join(externalErr, fmt.Errorf("reconcile hosting runtime-only rollback: %w", err))
	}
	if err := reconcileHostingRollbackOperations(ctx); err != nil {
		externalErr = errors.Join(externalErr, fmt.Errorf("reconcile hosting rollback: %w", err))
	}
	if err := reconcileHostingRecoveryProxyOperations(ctx); err != nil {
		externalErr = errors.Join(externalErr, fmt.Errorf("reconcile hosting recovery routing: %w", err))
	}
	if err := reconcileHostingRuntimeOnlyRecoveries(ctx); err != nil {
		externalErr = errors.Join(externalErr, fmt.Errorf("reconcile hosting runtime-only recovery: %w", err))
	}
	if err := reconcileHostingRuntimeOnlyActivations(ctx); err != nil {
		externalErr = errors.Join(externalErr, fmt.Errorf("reconcile hosting runtime-only activation: %w", err))
	}
	if err := reconcileHostingActivationOperations(ctx); err != nil {
		externalErr = errors.Join(externalErr, fmt.Errorf("reconcile hosting activation: %w", err))
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
		WHERE status='delivering' AND (locked_at IS NULL OR locked_at<?)`, formatSQLiteTime(now.Add(-callbackLeaseTimeout))); err != nil {
		return err
	}
	if err := reconcileExpiredHostingRecoveries(ctx, conn, now); err != nil {
		return err
	}
	if err := queueLostHostingRuntimes(ctx, conn, now); err != nil {
		return err
	}
	if _, err := ensureResumableRuntimeOnlyRecoveries(ctx, conn, sql.NullInt64{}, now); err != nil {
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
			WHERE op.hosting_deployment_id=j.hosting_deployment_id AND op.status IN ('pending','applied'))
		  AND NOT EXISTS (SELECT 1 FROM hosting_proxy_operations current_route
			JOIN hosting_projects route_project ON route_project.id=current_route.hosting_project_id
			WHERE current_route.hosting_project_id=d.hosting_project_id
			  AND current_route.status IN ('pending','applied')
			  AND current_route.route_generation=route_project.route_generation)`, formatSQLiteTime(now))
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
	return externalErr
}

func reconcileHostingRuntimeOnlyActivations(ctx context.Context) error {
	rows, err := db.QueryContext(ctx, `SELECT job.id, job.hosting_deployment_id, deployment.hosting_project_id,
		job.hosting_runner_id, job.lease_generation, job.lease_token_hash, job.status,
		project.external_project_id, deployment.external_deployment_id, deployment.commit_sha,
		deployment.artifact_digest, deployment.release_digest, job.cancel_requested_at,
		job.lease_expires_at, job.required_cpu_millis, job.required_ram_bytes,
		job.required_disk_bytes, job.required_pids, release.runtime_endpoint,
		release.runtime_manifest_json
		FROM hosting_deployments deployment
		JOIN hosting_projects project ON project.id=deployment.hosting_project_id
		JOIN hosting_jobs job ON job.hosting_deployment_id=deployment.id
		JOIN hosting_releases release ON release.hosting_deployment_id=deployment.id
		  AND release.release_digest=deployment.release_digest
		WHERE project.publication_mode=? AND deployment.status='running'
		  AND deployment.phase='activating' AND job.status='running' AND release.status='healthy'
		ORDER BY deployment.id`, hostingPublicationRuntimeOnlyV1)
	if err != nil {
		return err
	}
	type pendingRuntimeActivation struct {
		state           hostingCompletionState
		releaseDigest   string
		runtimeEndpoint string
		runtimeJSON     string
	}
	var pending []pendingRuntimeActivation
	for rows.Next() {
		var item pendingRuntimeActivation
		item.state.PublicationMode = hostingPublicationRuntimeOnlyV1
		if err := rows.Scan(&item.state.JobID, &item.state.DeploymentID, &item.state.ProjectID,
			&item.state.RunnerID, &item.state.LeaseGeneration, &item.state.LeaseTokenHash,
			&item.state.JobStatus, &item.state.ExternalProjectID, &item.state.ExternalDeploymentID,
			&item.state.CommitSHA, &item.state.ArtifactDigest, &item.releaseDigest,
			&item.state.CancelRequestedAt, &item.state.LeaseExpiresAt,
			&item.state.Limits.CPUMillis, &item.state.Limits.RAMBytes,
			&item.state.Limits.DiskBytes, &item.state.Limits.PIDs,
			&item.runtimeEndpoint, &item.runtimeJSON); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal([]byte(item.runtimeJSON), &item.state.Runtime); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, item := range pending {
		if item.state.CancelRequestedAt.Valid {
			if err := cancelStagedHostingActivation(ctx, &item.state, item.releaseDigest,
				"durable cancellation intent won during runtime-only activation reconciliation"); err != nil {
				return err
			}
			continue
		}
		if err := verifyStagedHostingCandidate(ctx, &item.state, item.releaseDigest, item.runtimeEndpoint); err != nil {
			// A fresh health request can fail transiently during restart. Keep the
			// exact healthy staged candidate durable for the next reconciliation;
			// ordinary lease/runner-loss reconciliation remains authoritative for
			// terminal stale-runtime handling.
			continue
		}
		cancelled, err := activateHealthyHostingReleaseRuntimeOnly(ctx, &item.state, item.releaseDigest)
		if err != nil {
			return err
		}
		if cancelled {
			if err := cancelStagedHostingActivation(ctx, &item.state, item.releaseDigest,
				"execution disabled while reconciling runtime-only activation"); err != nil {
				return err
			}
		}
	}
	return nil
}

func reconcileHostingCompensationOperations(ctx context.Context) error {
	rows, err := db.QueryContext(ctx, `SELECT operation.operation_id, operation.hosting_project_id,
		operation.hosting_deployment_id, project.external_project_id,
		operation.expected_previous_release_digest, operation.release_digest, operation.runtime_endpoint,
		operation.desired_state
		FROM hosting_proxy_operations operation JOIN hosting_projects project ON project.id=operation.hosting_project_id
		WHERE operation.operation_type='compensate'
		  AND (operation.status='pending' OR
		    (operation.status='committed' AND operation.desired_state='resume_after_runtime'))
		ORDER BY operation.id`)
	if err != nil {
		return err
	}
	type compensation struct {
		operationID, externalProjectID, candidateDigest, previousDigest, previousEndpoint, desiredState string
		projectID                                                                                       int64
		deploymentID                                                                                    sql.NullInt64
	}
	var operations []compensation
	for rows.Next() {
		var operation compensation
		if err := rows.Scan(&operation.operationID, &operation.projectID, &operation.deploymentID,
			&operation.externalProjectID, &operation.candidateDigest, &operation.previousDigest,
			&operation.previousEndpoint, &operation.desiredState); err != nil {
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
		if err := executeHostingCompensation(ctx, proxy, operation.operationID, "", operation.projectID,
			operation.deploymentID.Int64, operation.externalProjectID, operation.candidateDigest,
			operation.previousDigest, operation.previousEndpoint); err != nil {
			continue
		}
		if operation.desiredState == "resume_after_runtime" {
			_, _, available, err := hostingProjectActiveRoute(ctx, operation.projectID)
			if err != nil {
				return err
			}
			if !available {
				continue
			}
			if err := stageHostingResumeRetry(ctx, operation.projectID, operation.operationID); err != nil {
				return err
			}
		}
	}
	return nil
}

func reconcileHostingDesiredStateOperations(ctx context.Context) error {
	rows, err := db.QueryContext(ctx, `SELECT op.operation_id, op.hosting_project_id, op.operation_type,
		op.status, op.route_generation, op.desired_state,
		p.external_project_id, p.route_generation, p.desired_state
		FROM hosting_proxy_operations op JOIN hosting_projects p ON p.id=op.hosting_project_id
		WHERE op.operation_type IN ('suspend','resume') AND op.status IN ('pending','applied') ORDER BY op.id`)
	if err != nil {
		return err
	}
	type desiredOperation struct {
		operationID, operationType, operationStatus, operationDesiredState, externalProjectID string
		projectID, routeGeneration, currentGeneration                                         int64
		desiredState                                                                          string
	}
	var operations []desiredOperation
	for rows.Next() {
		var operation desiredOperation
		if err := rows.Scan(&operation.operationID, &operation.projectID, &operation.operationType,
			&operation.operationStatus, &operation.routeGeneration, &operation.operationDesiredState,
			&operation.externalProjectID, &operation.currentGeneration, &operation.desiredState); err != nil {
			rows.Close()
			return err
		}
		operations = append(operations, operation)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, operation := range operations {
		expectedType := "resume"
		if operation.desiredState == "suspended" {
			expectedType = "suspend"
		}
		if operation.routeGeneration != operation.currentGeneration || (operation.operationDesiredState != "" &&
			(operation.operationType != expectedType || operation.operationDesiredState != operation.desiredState)) {
			if err := markHostingProxyOperationFailed(ctx, operation.operationID, errCodeConflict); err != nil {
				return err
			}
			continue
		}
		if operation.operationType == "resume" && operation.operationStatus == "applied" {
			resumeState, commitErr := commitHostingResumeOperation(ctx, operation.projectID,
				operation.operationID)
			if commitErr != nil {
				return commitErr
			}
			if resumeState == hostingResumeCommitted {
				continue
			}
			if resumeState == hostingResumeSuperseded {
				if err := markHostingProxyOperationFailed(ctx, operation.operationID, errCodeConflict); err != nil {
					return err
				}
				continue
			}
			if err := fenceUnavailableHostingResume(ctx, nil, operation.projectID,
				operation.externalProjectID, operation.operationID); err != nil {
				return err
			}
			continue
		}
		var resumeDigest, resumeEndpoint string
		if operation.operationType == "resume" {
			var available bool
			resumeDigest, resumeEndpoint, available, err = hostingProjectActiveRoute(ctx, operation.projectID)
			if err != nil {
				return err
			}
			if !available {
				if err := fenceUnavailableHostingResume(ctx, nil, operation.projectID,
					operation.externalProjectID, operation.operationID); err != nil {
					return err
				}
				continue
			}
		}
		proxy, err := hostingProxyClientFactory(appConfig)
		if err != nil {
			_ = markHostingProxyOperationError(ctx, operation.operationID, errCodeProxyUnavailable)
			continue
		}
		if operation.operationType == "resume" {
			var activation *proxyActivationResponse
			activation, err = proxy.Activate(ctx, proxyActivationRequest{
				OperationID: operation.operationID, ExternalProjectID: operation.externalProjectID,
				ReleaseDigest: resumeDigest, RuntimeEndpoint: resumeEndpoint,
				RouteGeneration: operation.routeGeneration,
			})
			if err == nil {
				err = markHostingProxyOperationApplied(ctx, operation.operationID, activation.RouteRevision)
			}
		} else {
			err = proxy.SetSuspended(ctx, proxySuspendRequest{
				OperationID: operation.operationID, ExternalProjectID: operation.externalProjectID,
				Suspended: true, RouteGeneration: operation.routeGeneration,
			})
		}
		if err != nil {
			code := errCodeProxyUnavailable
			var apiErr *hostingAPIError
			if errorsAsHosting(err, &apiErr) {
				code = apiErr.Code
			}
			_ = markHostingProxyOperationError(ctx, operation.operationID, code)
			continue
		}
		if operation.operationType == "resume" {
			resumeState, commitErr := commitHostingResumeOperation(ctx, operation.projectID, operation.operationID)
			if commitErr != nil {
				return commitErr
			}
			if resumeState == hostingResumeUnavailable {
				if err := fenceUnavailableHostingResume(ctx, proxy, operation.projectID,
					operation.externalProjectID, operation.operationID); err != nil {
					return err
				}
			}
			if resumeState == hostingResumeSuperseded {
				if err := markHostingProxyOperationFailed(ctx, operation.operationID, errCodeConflict); err != nil {
					return err
				}
			}
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
		op.runtime_endpoint, op.expected_previous_release_digest, op.expected_previous_runtime_endpoint, op.route_revision,
		op.route_generation, p.route_generation,
		j.id, j.hosting_deployment_id, d.hosting_project_id, j.hosting_runner_id,
		j.lease_generation, j.lease_token_hash, j.status, p.external_project_id,
		d.external_deployment_id, d.commit_sha, d.artifact_digest, j.cancel_requested_at,
		j.lease_expires_at, j.required_cpu_millis, j.required_ram_bytes,
		j.required_disk_bytes, j.required_pids, runner.status, runner.last_seen,
		runner.active_session_id, release.runtime_manifest_json, release.runtime_failure_code,
		release.runtime_observed_at, release.runtime_observed_session_id, release.runtime_instance_id
		FROM hosting_proxy_operations op
		JOIN hosting_deployments d ON d.id=op.hosting_deployment_id
		JOIN hosting_projects p ON p.id=d.hosting_project_id
		JOIN hosting_jobs j ON j.hosting_deployment_id=d.id
		JOIN hosting_runners runner ON runner.id=j.hosting_runner_id
		JOIN hosting_releases release ON release.hosting_deployment_id=d.id AND release.release_digest=op.release_digest
		WHERE op.operation_type='activate' AND op.status IN ('pending','applied') ORDER BY op.id`)
	if err != nil {
		return err
	}
	type pendingActivation struct {
		operationID, status, releaseDigest, runtimeEndpoint, routeRevision   string
		state                                                                hostingCompletionState
		currentGeneration                                                    int64
		runnerStatus, activeSession, runtimeManifestJSON, runtimeFailureCode string
		observedSession, runtimeInstanceID                                   string
		runnerLastSeen, observedAt                                           sql.NullString
	}
	var operations []pendingActivation
	for rows.Next() {
		var operation pendingActivation
		if err := rows.Scan(&operation.operationID, &operation.status, &operation.releaseDigest,
			&operation.runtimeEndpoint, &operation.state.PreviousReleaseDigest,
			&operation.state.PreviousRuntimeEndpoint, &operation.routeRevision,
			&operation.state.RouteGeneration, &operation.currentGeneration,
			&operation.state.JobID, &operation.state.DeploymentID, &operation.state.ProjectID,
			&operation.state.RunnerID, &operation.state.LeaseGeneration, &operation.state.LeaseTokenHash,
			&operation.state.JobStatus, &operation.state.ExternalProjectID,
			&operation.state.ExternalDeploymentID, &operation.state.CommitSHA,
			&operation.state.ArtifactDigest, &operation.state.CancelRequestedAt,
			&operation.state.LeaseExpiresAt, &operation.state.Limits.CPUMillis,
			&operation.state.Limits.RAMBytes, &operation.state.Limits.DiskBytes,
			&operation.state.Limits.PIDs, &operation.runnerStatus, &operation.runnerLastSeen,
			&operation.activeSession, &operation.runtimeManifestJSON, &operation.runtimeFailureCode,
			&operation.observedAt, &operation.observedSession, &operation.runtimeInstanceID); err != nil {
			rows.Close()
			return err
		}
		operations = append(operations, operation)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, operation := range operations {
		if operation.state.CancelRequestedAt.Valid {
			if err := cancelStagedHostingActivation(ctx, &operation.state, operation.releaseDigest,
				"durable cancellation intent won during activation reconciliation"); err != nil {
				return err
			}
			continue
		}
		if operation.state.RouteGeneration != operation.currentGeneration {
			if err := markHostingProxyOperationFailed(ctx, operation.operationID, errCodeConflict); err != nil {
				return err
			}
			if err := finishHostingJobTerminal(ctx, &operation.state, hostingStatusCancelled,
				hostingPhaseCancelled, "cancelled", "a newer routing intent superseded candidate activation"); err != nil {
				return err
			}
			continue
		}
		var runtimeManifest HostingRuntimeManifest
		if err := json.Unmarshal([]byte(operation.runtimeManifestJSON), &runtimeManifest); err != nil {
			return err
		}
		runnerLive := operation.activeSession != "" && operation.runnerStatus == "online" && operation.runnerLastSeen.Valid &&
			!parseSQLiteTime(operation.runnerLastSeen.String).Before(time.Now().UTC().Add(-hostingRunnerStaleAfter))
		expectedInstance := fmt.Sprintf("build-%d-%d", operation.state.JobID, operation.state.LeaseGeneration)
		exactRuntimeLive := operation.runtimeFailureCode == "" && operation.runtimeInstanceID == expectedInstance &&
			operation.observedAt.Valid &&
			!parseSQLiteTime(operation.observedAt.String).Before(time.Now().UTC().Add(-hostingRunnerStaleAfter)) &&
			operation.observedSession == operation.activeSession
		healthPath := runtimeManifest.HealthPath
		if healthPath == "" {
			healthPath = "/"
		}
		var healthErr error
		if runnerLive && exactRuntimeLive {
			healthCtx, cancelHealth := context.WithTimeout(ctx, hostingActivationReconcileHealthTimeout)
			_, _, healthErr = checkHostingCandidateHealth(healthCtx,
				strings.TrimRight(operation.runtimeEndpoint, "/")+healthPath)
			cancelHealth()
		} else {
			healthErr = fmt.Errorf("candidate runner or exact runtime instance is no longer live")
		}
		if healthErr != nil {
			proxy, err := hostingProxyClientFactory(appConfig)
			if err != nil {
				_ = markHostingProxyOperationError(ctx, operation.operationID, errCodeProxyUnavailable)
				continue
			}
			if _, err := compensateHostingActivationIfCurrent(ctx, proxy, operation.operationID,
				&operation.state, operation.releaseDigest); err != nil {
				return err
			}
			if err := markHostingProxyOperationFailed(ctx, operation.operationID, errCodeReleaseNotHealthy); err != nil {
				return err
			}
			if err := finishHostingJobTerminal(ctx, &operation.state, hostingStatusFailed,
				hostingPhaseFailed, "health_check_failed", "candidate failed reconciliation health gate"); err != nil {
				return err
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
			if _, err := compensateHostingActivationIfCurrent(ctx, proxy, operation.operationID,
				&operation.state, operation.releaseDigest); err != nil {
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
				RouteGeneration:               operation.state.RouteGeneration,
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
			if _, compensateErr := compensateHostingActivationIfCurrent(ctx, proxy, operation.operationID,
				&operation.state, operation.releaseDigest); compensateErr != nil {
				return compensateErr
			}
			if markErr := markHostingProxyOperationFailed(ctx, operation.operationID, errCodeReleaseNotHealthy); markErr != nil {
				return markErr
			}
			if finishErr := finishHostingJobTerminal(ctx, &operation.state, hostingStatusFailed,
				hostingPhaseFailed, "health_check_failed", "candidate identity changed before activation committed"); finishErr != nil {
				return finishErr
			}
			continue
		}
		if !cancelled {
			continue
		}
		if _, err := compensateHostingActivationIfCurrent(ctx, proxy, operation.operationID,
			&operation.state, operation.releaseDigest); err != nil {
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
	staleBefore := formatSQLiteTime(now.Add(-hostingRunnerStaleAfter))
	incompatibleRows, err := conn.QueryContext(ctx, `SELECT j.id, j.hosting_runner_id,
		j.required_cpu_millis, j.required_ram_bytes, j.required_disk_bytes, j.required_pids
		FROM hosting_jobs j JOIN hosting_runners runner ON runner.id=j.hosting_runner_id
		WHERE j.status='queued' AND j.hosting_runner_id IS NOT NULL
		  AND (runner.status<>'online' OR runner.draining<>0 OR runner.last_seen IS NULL OR runner.last_seen<?
		    OR runner.active_session_id=''
		    OR NOT EXISTS (SELECT 1 FROM json_each(runner.operation_capabilities_json) WHERE value='build')
		    OR NOT EXISTS (SELECT 1 FROM json_each(runner.operation_capabilities_json) WHERE value=?)
		    OR (json_array_length(j.secret_refs_json)>0 AND NOT EXISTS
		      (SELECT 1 FROM json_each(runner.operation_capabilities_json) WHERE value=?)))`, staleBefore,
		hostingRunnerInventoryOperation, hostingRunnerSecretOperation)
	if err != nil {
		return err
	}
	type incompatibleJob struct {
		jobID, runnerID int64
		limits          hostingWorkloadLimits
	}
	var incompatible []incompatibleJob
	for incompatibleRows.Next() {
		var job incompatibleJob
		if err := incompatibleRows.Scan(&job.jobID, &job.runnerID, &job.limits.CPUMillis,
			&job.limits.RAMBytes, &job.limits.DiskBytes, &job.limits.PIDs); err != nil {
			incompatibleRows.Close()
			return err
		}
		incompatible = append(incompatible, job)
	}
	if err := incompatibleRows.Close(); err != nil {
		return err
	}
	for _, job := range incompatible {
		result, err := conn.ExecContext(ctx, `UPDATE hosting_jobs SET hosting_runner_id=NULL
			WHERE id=? AND status='queued' AND hosting_runner_id=?`, job.jobID, job.runnerID)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected == 1 {
			if err := restoreHostingRunnerCapacity(ctx, conn, job.runnerID, job.limits); err != nil {
				return err
			}
		}
	}
	rows, err := conn.QueryContext(ctx, `SELECT j.id, j.hosting_deployment_id, d.hosting_project_id,
		p.external_project_id, p.manifest_json, p.manifest_digest, p.resource_profile, p.deploy_path,
		p.desired_state, p.kill_switch_reason, j.required_cpu_millis, j.required_ram_bytes,
		j.required_disk_bytes, j.required_pids, j.secret_refs_json
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
		requireSecrets                 bool
	}
	var jobs []queuedJob
	for rows.Next() {
		var job queuedJob
		var secretsJSON string
		if err := rows.Scan(&job.jobID, &job.deploymentID, &job.projectID,
			&job.project.ExternalProjectID, &job.manifestJSON, &job.project.ManifestDigest,
			&job.project.ResourceProfile, &job.project.DeployPath, &job.project.DesiredState,
			&job.project.KillSwitchReason, &job.limits.CPUMillis, &job.limits.RAMBytes,
			&job.limits.DiskBytes, &job.limits.PIDs, &secretsJSON); err != nil {
			rows.Close()
			return err
		}
		job.project.ID = job.projectID
		if err := json.Unmarshal([]byte(job.manifestJSON), &job.project.Manifest); err != nil {
			rows.Close()
			return err
		}
		var refs []HostingSecretReference
		if err := json.Unmarshal([]byte(secretsJSON), &refs); err != nil {
			rows.Close()
			return err
		}
		job.requireSecrets = len(refs) > 0
		jobs = append(jobs, job)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, job := range jobs {
		runnerID, err := reserveHostingRunner(ctx, conn, &job.project, job.limits, "build", job.requireSecrets, 0)
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
