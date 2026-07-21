package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func hostingJobHeartbeatForTest(t *testing.T, runnerID int64, job *hostingClaimedJob) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/api/hosting-agent/v1/jobs/%d/heartbeat", job.JobID), nil)
	request.Header.Set("X-Deployer-Lease-Generation", fmt.Sprint(job.LeaseGeneration))
	request.Header.Set("X-Deployer-Lease-Token", job.LeaseToken)
	request = request.WithContext(context.WithValue(request.Context(), hostingRunnerContextKey{},
		&HostingRunner{ID: runnerID}))
	recorder := httptest.NewRecorder()
	handleHostingJobLeaseHeartbeat(recorder, request, job.JobID)
	return recorder
}

func hostingRecoveryHeartbeatForTest(t *testing.T, runnerID int64, recovery *hostingClaimedJob) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/api/hosting-agent/v1/recoveries/%d/heartbeat", recovery.JobID), nil)
	request.Header.Set("X-Deployer-Lease-Generation", fmt.Sprint(recovery.LeaseGeneration))
	request.Header.Set("X-Deployer-Lease-Token", recovery.LeaseToken)
	request = request.WithContext(context.WithValue(request.Context(), hostingRunnerContextKey{},
		&HostingRunner{ID: runnerID}))
	recorder := httptest.NewRecorder()
	handleHostingRecoveryHeartbeat(recorder, request, recovery.JobID)
	return recorder
}

func TestHostingCancellationOfRequeuedJobTerminalizesDeploymentAndRestoresCapacityOnce(t *testing.T) {
	withTempDB(t)
	withFakeProxy(t)
	project, token, firstRunnerID, job := createAndClaimHostingJob(t,
		"project_01JCANCELREQUEUE", "deployment_01JCANCELREQUEUE")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	replacementCapacity := hostingWorkloadLimits{CPUMillis: limits.CPUMillis * 3,
		RAMBytes: limits.RAMBytes * 3, DiskBytes: limits.DiskBytes * 3, PIDs: limits.PIDs * 3}
	replacementRunnerID := insertHostingRunnerForTest(t, "cancel-requeue-replacement", replacementCapacity)
	past := formatSQLiteTime(time.Now().UTC().Add(-time.Minute))
	if _, err := db.Exec(`UPDATE hosting_jobs SET lease_expires_at=? WHERE id=?`, past, job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=? WHERE id=?`,
		past, firstRunnerID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var assignedRunnerID int64
	var jobStatus, deploymentStatus, deploymentPhase string
	if err := db.QueryRow(`SELECT job.status, job.hosting_runner_id, deployment.status, deployment.phase
		FROM hosting_jobs job JOIN hosting_deployments deployment ON deployment.id=job.hosting_deployment_id
		WHERE job.id=?`, job.JobID).Scan(&jobStatus, &assignedRunnerID, &deploymentStatus, &deploymentPhase); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "queued" || assignedRunnerID != replacementRunnerID ||
		deploymentStatus != hostingStatusRunning || deploymentPhase != hostingPhaseQueued {
		t.Fatalf("requeued state job=%q runner=%d deployment=%q/%q", jobStatus,
			assignedRunnerID, deploymentStatus, deploymentPhase)
	}

	holdingProject, _, _, err := upsertHostingProject(t.Context(), "project_01JCANCELHOLD", validHostingManifest("node"))
	if err != nil {
		t.Fatal(err)
	}
	holdingRequest := validHostingDeploymentRequest("deployment_01JCANCELHOLD")
	holdingRequest.ManifestDigest = holdingProject.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, holdingProject.ExternalProjectID,
		"create_01JCANCELHOLD", holdingRequest); err != nil {
		t.Fatal(err)
	}

	cancelled, replayed, err := cancelHostingDeployment(t.Context(), token,
		"deployment_01JCANCELREQUEUE", "cancel_01JCANCELREQUEUE")
	if err != nil || replayed || cancelled.Status != hostingStatusCancelled {
		t.Fatalf("cancelled=%+v replayed=%v err=%v", cancelled, replayed, err)
	}
	var runnerIDAfter any
	if err := db.QueryRow(`SELECT status, hosting_runner_id FROM hosting_jobs WHERE id=?`, job.JobID).Scan(
		&jobStatus, &runnerIDAfter); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "cancelled" || runnerIDAfter != nil {
		t.Fatalf("cancelled job status=%q runner=%v", jobStatus, runnerIDAfter)
	}
	var free hostingWorkloadLimits
	if err := db.QueryRow(`SELECT free_cpu_millis, free_ram_bytes, free_disk_bytes, free_pids
		FROM hosting_runners WHERE id=?`, replacementRunnerID).Scan(&free.CPUMillis, &free.RAMBytes,
		&free.DiskBytes, &free.PIDs); err != nil {
		t.Fatal(err)
	}
	wantFree := hostingWorkloadLimits{CPUMillis: replacementCapacity.CPUMillis - limits.CPUMillis,
		RAMBytes:  replacementCapacity.RAMBytes - limits.RAMBytes,
		DiskBytes: replacementCapacity.DiskBytes - limits.DiskBytes,
		PIDs:      replacementCapacity.PIDs - limits.PIDs}
	if free != wantFree {
		t.Fatalf("capacity after one cancellation restore=%+v want=%+v", free, wantFree)
	}
	var events, callbacks int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_events event
		JOIN hosting_deployments deployment ON deployment.id=event.hosting_deployment_id
		WHERE deployment.external_deployment_id=? AND event.event_type='deployment_cancelled'`,
		"deployment_01JCANCELREQUEUE").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM callback_outbox callback
		JOIN hosting_deployments deployment ON deployment.id=callback.hosting_deployment_id
		WHERE deployment.external_deployment_id=?`, "deployment_01JCANCELREQUEUE").Scan(&callbacks); err != nil {
		t.Fatal(err)
	}
	if events != 1 || callbacks != 1 {
		t.Fatalf("terminal cancellation events=%d callbacks=%d", events, callbacks)
	}
}

func TestHostingRunningCancellationStopsLeaseRenewalAndSurvivesRestart(t *testing.T) {
	withTempDB(t)
	_, token, runnerID, job := createAndClaimHostingJobAtFetching(t,
		"project_01JCANCELRESTART", "deployment_01JCANCELRESTART")
	var originalExpiry string
	if err := db.QueryRow(`SELECT lease_expires_at FROM hosting_jobs WHERE id=?`, job.JobID).Scan(&originalExpiry); err != nil {
		t.Fatal(err)
	}
	first, replayed, err := cancelHostingDeployment(t.Context(), token,
		"deployment_01JCANCELRESTART", "cancel_01JCANCELRESTART")
	if err != nil || replayed || first.Status != hostingStatusRunning || first.Phase != hostingPhaseCancelling {
		t.Fatalf("initial cancellation=%+v replayed=%v err=%v", first, replayed, err)
	}
	for range 3 {
		recorder := hostingJobHeartbeatForTest(t, runnerID, job)
		if recorder.Code != http.StatusOK {
			t.Fatalf("cancel heartbeat status=%d body=%s", recorder.Code, recorder.Body.String())
		}
		var response map[string]bool
		if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil || !response["cancel_requested"] {
			t.Fatalf("cancel heartbeat response=%v err=%v", response, err)
		}
	}
	var expiryAfterHeartbeats string
	if err := db.QueryRow(`SELECT lease_expires_at FROM hosting_jobs WHERE id=?`, job.JobID).Scan(&expiryAfterHeartbeats); err != nil {
		t.Fatal(err)
	}
	if expiryAfterHeartbeats != originalExpiry {
		t.Fatalf("cancelled lease renewed from %q to %q", originalExpiry, expiryAfterHeartbeats)
	}
	var sequence int
	var databaseName, databasePath string
	if err := db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &databaseName, &databasePath); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := initDB(databasePath); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), parseSQLiteTime(originalExpiry).Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	current, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JCANCELRESTART")
	if err != nil || current.Status != hostingStatusCancelled || current.Phase != hostingPhaseCancelled {
		t.Fatalf("restarted cancellation state=%+v err=%v", current, err)
	}
	replayedResponse, replayed, err := cancelHostingDeployment(t.Context(), token,
		"deployment_01JCANCELRESTART", "cancel_01JCANCELRESTART")
	if err != nil || !replayed || replayedResponse.Status != hostingStatusRunning ||
		replayedResponse.Phase != hostingPhaseCancelling {
		t.Fatalf("original cancellation replay=%+v replayed=%v err=%v", replayedResponse, replayed, err)
	}
}

func TestHostingRestartCancellationFencesStagedActivationAndDrainsCompletionOutbox(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	var healthy atomic.Bool
	healthy.Store(true)
	var healthCalls atomic.Int64
	healthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		healthCalls.Add(1)
		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(healthServer.Close)
	_, token, runnerID, job := createAndClaimHostingJob(t,
		"project_01JCANCELSTAGED", "deployment_01JCANCELSTAGED")
	digest := "sha256:" + strings.Repeat("6", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	observeHostingJobEndpointForTest(t, job.JobID, healthServer.URL)
	completion := hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
		ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: healthServer.URL,
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}
	fake.fail = true
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, completion); err == nil {
		t.Fatal("proxy outage did not leave a staged activation")
	}
	if _, _, err := cancelHostingDeployment(t.Context(), token, "deployment_01JCANCELSTAGED",
		"cancel_01JCANCELSTAGED"); err != nil {
		t.Fatal(err)
	}
	healthCallsBeforeRestart := healthCalls.Load()
	healthy.Store(false)
	var sequence int
	var databaseName, databasePath string
	if err := db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &databaseName, &databasePath); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := initDB(databasePath); err != nil {
		t.Fatal(err)
	}
	fake.fail = false
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JCANCELSTAGED")
	if err != nil || deployment.Status != hostingStatusCancelled || deployment.Phase != hostingPhaseCancelled {
		t.Fatalf("staged cancellation deployment=%+v err=%v", deployment, err)
	}
	if healthCalls.Load() != healthCallsBeforeRestart || len(fake.activations) != 1 ||
		len(fake.suspensions) != 2 || !fake.suspensions[0].Suspended || !fake.suspensions[1].Suspended {
		t.Fatalf("cancel reconciliation health calls=%d/%d activations=%+v suspensions=%+v",
			healthCalls.Load(), healthCallsBeforeRestart, fake.activations, fake.suspensions)
	}

	oldAgentClient := agentControlClient
	t.Cleanup(func() { agentControlClient = oldAgentClient })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), hostingRunnerContextKey{}, &HostingRunner{ID: runnerID}))
		handleHostingJobComplete(w, r, job.JobID)
	}))
	t.Cleanup(server.Close)
	agentControlClient = server.Client()
	config := hostingAgentConfig{ServerURL: server.URL, Token: "test-token", WorkRoot: t.TempDir()}
	if err := queueHostingAgentCompletion(config, job, completion); err != nil {
		t.Fatal(err)
	}
	if err := flushHostingAgentCompletions(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(hostingAgentCompletionDir(config))
	if err != nil || len(entries) != 0 {
		t.Fatalf("terminal cancelled completion outbox entries=%d err=%v", len(entries), err)
	}
}

func TestHostingRestartCancellationCompensatesAppliedActivationBeforeHealth(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	var healthCalls atomic.Int64
	healthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		healthCalls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(healthServer.Close)
	_, token, runnerID, job := createAndClaimHostingJob(t,
		"project_01JCANCELAPPLIED", "deployment_01JCANCELAPPLIED")
	digest := "sha256:" + strings.Repeat("8", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	observeHostingJobEndpointForTest(t, job.JobID, healthServer.URL)
	state, err := getHostingCompletionState(t.Context(), runnerID, job.JobID,
		job.LeaseGeneration, job.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := stageHealthyHostingRelease(t.Context(), state, hostingCompletionRequest{Status: "success",
		ReleaseDigest: digest, ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: healthServer.URL,
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_proxy_operations SET status='applied', route_revision='route-applied'
		WHERE hosting_deployment_id=? AND operation_type='activate'`, state.DeploymentID); err != nil {
		t.Fatal(err)
	}
	fake.currentEndpoint = healthServer.URL
	if _, _, err := cancelHostingDeployment(t.Context(), token, "deployment_01JCANCELAPPLIED",
		"cancel_01JCANCELAPPLIED"); err != nil {
		t.Fatal(err)
	}
	var sequence int
	var databaseName, databasePath string
	if err := db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &databaseName, &databasePath); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := initDB(databasePath); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JCANCELAPPLIED")
	if err != nil || deployment.Status != hostingStatusCancelled || healthCalls.Load() != 0 ||
		len(fake.activations) != 0 || len(fake.suspensions) != 1 || !fake.currentSuspended {
		t.Fatalf("applied cancellation deployment=%+v err=%v health=%d activations=%+v suspensions=%+v",
			deployment, err, healthCalls.Load(), fake.activations, fake.suspensions)
	}
}

func TestHostingCancellationFencesInFlightProxyActivation(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	_, token, runnerID, job := createAndClaimHostingJob(t,
		"project_01JCANCELRACE", "deployment_01JCANCELRACE")
	digest := "sha256:" + strings.Repeat("a", 64)
	endpoint := healthyHostingEndpointForTest(t)
	completion := hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
		ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, digest),
		RuntimeEndpoint:       endpoint,
		HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}
	observeHostingJobEndpointForTest(t, job.JobID, endpoint)
	fake.activateStarted = make(chan struct{}, 1)
	fake.activateContinue = make(chan struct{})
	completed := make(chan error, 1)
	go func() {
		completed <- completeHostingJob(context.Background(), runnerID, job.JobID,
			job.LeaseGeneration, job.LeaseToken, completion)
	}()
	select {
	case <-fake.activateStarted:
	case <-time.After(3 * time.Second):
		close(fake.activateContinue)
		t.Fatal("candidate activation did not reach the proxy")
	}

	cancelled, replayed, err := cancelHostingDeployment(t.Context(), token,
		"deployment_01JCANCELRACE", "cancel_01JCANCELRACE")
	if err != nil || replayed || cancelled.Status != hostingStatusRunning ||
		cancelled.Phase != hostingPhaseCancelling {
		close(fake.activateContinue)
		t.Fatalf("cancel response=%+v replayed=%v err=%v", cancelled, replayed, err)
	}
	var activateGeneration, compensationGeneration, currentGeneration int64
	var compensationStatus string
	if err := db.QueryRow(`SELECT route_generation FROM hosting_proxy_operations
		WHERE hosting_deployment_id=(SELECT id FROM hosting_deployments WHERE external_deployment_id=?)
		  AND operation_type='activate'`, "deployment_01JCANCELRACE").Scan(&activateGeneration); err != nil {
		close(fake.activateContinue)
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT route_generation, status FROM hosting_proxy_operations
		WHERE hosting_deployment_id=(SELECT id FROM hosting_deployments WHERE external_deployment_id=?)
		  AND operation_type='compensate'`, "deployment_01JCANCELRACE").Scan(
		&compensationGeneration, &compensationStatus); err != nil {
		close(fake.activateContinue)
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT route_generation FROM hosting_projects WHERE external_project_id=?`,
		"project_01JCANCELRACE").Scan(&currentGeneration); err != nil {
		close(fake.activateContinue)
		t.Fatal(err)
	}
	fake.mu.Lock()
	suspendedBeforeLateActivation := fake.currentSuspended
	fake.mu.Unlock()
	if compensationGeneration <= activateGeneration || currentGeneration != compensationGeneration ||
		compensationStatus != "committed" || !suspendedBeforeLateActivation {
		close(fake.activateContinue)
		t.Fatalf("activate generation=%d compensation=%d/%s current=%d suspended=%v",
			activateGeneration, compensationGeneration, compensationStatus, currentGeneration,
			suspendedBeforeLateActivation)
	}

	close(fake.activateContinue)
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	currentEndpoint, currentSuspended := fake.currentEndpoint, fake.currentSuspended
	fake.mu.Unlock()
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JCANCELRACE")
	if err != nil || deployment.Status != hostingStatusCancelled ||
		deployment.Phase != hostingPhaseCancelled || currentEndpoint != "" || !currentSuspended {
		t.Fatalf("late activation deployment=%+v err=%v endpoint=%q suspended=%v",
			deployment, err, currentEndpoint, currentSuspended)
	}
}

func TestHostingCancellationCompensationOutageKeepsDeploymentNonterminal(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	_, token, runnerID, job := createAndClaimHostingJob(t,
		"project_01JCANCELCOMP", "deployment_01JCANCELCOMP")
	digest := "sha256:" + strings.Repeat("b", 64)
	endpoint := healthyHostingEndpointForTest(t)
	completion := hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
		ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, digest),
		RuntimeEndpoint:       endpoint,
		HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}
	observeHostingJobEndpointForTest(t, job.JobID, endpoint)
	fake.fail = true
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, completion); err == nil {
		t.Fatal("proxy outage did not leave a staged activation")
	}
	if _, _, err := cancelHostingDeployment(t.Context(), token, "deployment_01JCANCELCOMP",
		"cancel_01JCANCELCOMP"); err != nil {
		t.Fatal(err)
	}
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, completion); err == nil {
		t.Fatal("direct completion retry ignored failed cancellation compensation")
	}
	assertCancellationCompensationPendingForTest(t, job.JobID, runnerID)
	_, _, _, unrelatedJob := createAndClaimHostingJobAtFetching(t,
		"project_01JCANCELUNRELATED", "deployment_01JCANCELUNRELATED")
	if _, err := db.Exec(`UPDATE hosting_jobs SET lease_expires_at=? WHERE id=?`,
		formatSQLiteTime(time.Now().UTC().Add(-time.Minute)), unrelatedJob.JobID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err == nil {
		t.Fatal("reconciliation unexpectedly succeeded while cancellation compensation was unavailable")
	}
	assertCancellationCompensationPendingForTest(t, job.JobID, runnerID)
	var unrelatedStatus, unrelatedPhase string
	var unrelatedExpiryEvents int
	if err := db.QueryRow(`SELECT job.status, deployment.phase FROM hosting_jobs job
		JOIN hosting_deployments deployment ON deployment.id=job.hosting_deployment_id
		WHERE job.id=?`, unrelatedJob.JobID).Scan(&unrelatedStatus, &unrelatedPhase); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_events
		WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)
		  AND event_type='runner_lease_expired'`, unrelatedJob.JobID).Scan(&unrelatedExpiryEvents); err != nil {
		t.Fatal(err)
	}
	if unrelatedStatus != "queued" || unrelatedPhase != hostingPhaseQueued || unrelatedExpiryEvents != 1 {
		t.Fatalf("unrelated reconciliation status=%s phase=%s expiry events=%d",
			unrelatedStatus, unrelatedPhase, unrelatedExpiryEvents)
	}

	fake.fail = false
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, completion); err != nil {
		t.Fatal(err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JCANCELCOMP")
	if err != nil || deployment.Status != hostingStatusCancelled || deployment.Phase != hostingPhaseCancelled {
		t.Fatalf("deployment=%+v err=%v", deployment, err)
	}
	var unsettled, callbacks, cancelledEvents int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE hosting_deployment_id=(SELECT id FROM hosting_deployments WHERE external_deployment_id=?)
		  AND status IN ('pending','applied')`, "deployment_01JCANCELCOMP").Scan(&unsettled); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM callback_outbox
		WHERE hosting_deployment_id=(SELECT id FROM hosting_deployments WHERE external_deployment_id=?)`,
		"deployment_01JCANCELCOMP").Scan(&callbacks); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_events
		WHERE hosting_deployment_id=(SELECT id FROM hosting_deployments WHERE external_deployment_id=?)
		  AND event_type='deployment_cancelled'`, "deployment_01JCANCELCOMP").Scan(&cancelledEvents); err != nil {
		t.Fatal(err)
	}
	if unsettled != 0 || callbacks != 1 || cancelledEvents != 1 {
		t.Fatalf("unsettled=%d callbacks=%d cancelled events=%d", unsettled, callbacks, cancelledEvents)
	}
}

func assertCancellationCompensationPendingForTest(t *testing.T, jobID, runnerID int64) {
	t.Helper()
	var jobStatus, phase, releaseStatus, compensationStatus string
	var cancelRequested sql.NullString
	var activateGeneration, compensationGeneration, currentGeneration int64
	if err := db.QueryRow(`SELECT job.status, job.cancel_requested_at, deployment.phase, release.status
		FROM hosting_jobs job
		JOIN hosting_deployments deployment ON deployment.id=job.hosting_deployment_id
		JOIN hosting_releases release ON release.hosting_deployment_id=deployment.id
		WHERE job.id=?`, jobID).Scan(&jobStatus, &cancelRequested, &phase, &releaseStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT route_generation FROM hosting_proxy_operations
		WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)
		  AND operation_type='activate'`, jobID).Scan(&activateGeneration); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT route_generation, status FROM hosting_proxy_operations
		WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)
		  AND operation_type='compensate'`, jobID).Scan(&compensationGeneration, &compensationStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT project.route_generation FROM hosting_projects project
		JOIN hosting_deployments deployment ON deployment.hosting_project_id=project.id
		JOIN hosting_jobs job ON job.hosting_deployment_id=deployment.id WHERE job.id=?`, jobID).Scan(
		&currentGeneration); err != nil {
		t.Fatal(err)
	}
	var callbacks, cancelledEvents int
	if err := db.QueryRow(`SELECT COUNT(*) FROM callback_outbox
		WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)`,
		jobID).Scan(&callbacks); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_events
		WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)
		  AND event_type='deployment_cancelled'`, jobID).Scan(&cancelledEvents); err != nil {
		t.Fatal(err)
	}
	var freeCPU, capacityCPU, requiredCPU int64
	if err := db.QueryRow(`SELECT runner.free_cpu_millis, runner.capacity_cpu_millis,
		job.required_cpu_millis FROM hosting_runners runner JOIN hosting_jobs job ON job.hosting_runner_id=runner.id
		WHERE runner.id=? AND job.id=?`, runnerID, jobID).Scan(&freeCPU, &capacityCPU, &requiredCPU); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "running" || !cancelRequested.Valid || phase != hostingPhaseCancelling ||
		releaseStatus != "healthy" || compensationStatus != "pending" ||
		compensationGeneration <= activateGeneration || currentGeneration != compensationGeneration ||
		callbacks != 0 || cancelledEvents != 0 || freeCPU != capacityCPU-requiredCPU {
		t.Fatalf("job=%s cancel=%v phase=%s release=%s generations=%d/%d/%d compensation=%s callbacks=%d events=%d cpu=%d/%d required=%d",
			jobStatus, cancelRequested.Valid, phase, releaseStatus, activateGeneration,
			compensationGeneration, currentGeneration, compensationStatus, callbacks, cancelledEvents,
			freeCPU, capacityCPU, requiredCPU)
	}
}

func TestHostingLeaseExpiryWaitsForKillSwitchRouteFenceAfterRestart(t *testing.T) {
	t.Run("build", func(t *testing.T) {
		withTempDB(t)
		fake := withFakeProxy(t)
		project, _, runnerID, job := createAndClaimHostingJob(t,
			"project_01JCANCELEXPIRE", "deployment_01JCANCELEXPIRE")
		digest := "sha256:" + strings.Repeat("d", 64)
		endpoint := healthyHostingEndpointForTest(t)
		completion := hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, digest),
			RuntimeEndpoint:       endpoint,
			HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}
		observeHostingJobEndpointForTest(t, job.JobID, endpoint)
		fake.fail = true
		if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
			job.LeaseToken, completion); err == nil {
			t.Fatal("proxy outage did not leave activation pending")
		}
		operator, err := createServiceToken("build-expiry-kill", []string{serviceScopeProjectsWrite})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := setHostingExecutionKillSwitch(t.Context(), operator, project.ExternalProjectID,
			hostingKillSwitchRequest{Enabled: true, Reason: "expiry fence incident"},
			"build_expiry_kill_01", "request-build-expiry-kill"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`UPDATE hosting_proxy_operations SET status='failed'
			WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)
			  AND operation_type='activate'`, job.JobID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`UPDATE hosting_jobs SET lease_expires_at=? WHERE id=?`,
			formatSQLiteTime(time.Now().UTC().Add(-time.Minute)), job.JobID); err != nil {
			t.Fatal(err)
		}
		restartHostingDatabaseForCancellationTest(t)
		if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		var status, phase, suspendStatus string
		var callbacks int
		if err := db.QueryRow(`SELECT job.status, deployment.phase FROM hosting_jobs job
			JOIN hosting_deployments deployment ON deployment.id=job.hosting_deployment_id
			WHERE job.id=?`, job.JobID).Scan(&status, &phase); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations
			WHERE hosting_project_id=? AND operation_type='suspend' ORDER BY id DESC LIMIT 1`,
			project.ID).Scan(&suspendStatus); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM callback_outbox
			WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)`,
			job.JobID).Scan(&callbacks); err != nil {
			t.Fatal(err)
		}
		if status != "running" || phase != hostingPhaseCancelling || suspendStatus != "pending" || callbacks != 0 {
			t.Fatalf("before fence status=%s phase=%s suspend=%s callbacks=%d", status, phase,
				suspendStatus, callbacks)
		}
		fake.fail = false
		if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JCANCELEXPIRE")
		if err != nil || deployment.Status != hostingStatusCancelled || deployment.Phase != hostingPhaseCancelled {
			t.Fatalf("after fence deployment=%+v err=%v", deployment, err)
		}
	})

	t.Run("runtime recovery", func(t *testing.T) {
		withTempDB(t)
		fake := withFakeProxy(t)
		project, _, lostRunnerID, job := createAndClaimHostingJob(t,
			"project_01JRECEXPIRE", "deployment_01JRECEXPIRE")
		digest := "sha256:" + strings.Repeat("e", 64)
		artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
		if err := completeHostingJob(t.Context(), lostRunnerID, job.JobID, job.LeaseGeneration,
			job.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
				ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: healthyHostingEndpointForTest(t),
				HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
			t.Fatal(err)
		}
		limits, _ := hostingLimitsForProfile(project.ResourceProfile)
		recoveryRunnerID := insertHostingRunnerForTest(t, "recovery-expiry-target",
			hostingWorkloadLimits{CPUMillis: limits.CPUMillis * 2, RAMBytes: limits.RAMBytes * 2,
				DiskBytes: limits.DiskBytes * 2, PIDs: limits.PIDs * 2})
		if _, err := db.Exec(`UPDATE hosting_runners SET status='offline' WHERE id=?`, lostRunnerID); err != nil {
			t.Fatal(err)
		}
		if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		recovery, err := claimHostingRuntimeRecovery(t.Context(), recoveryRunnerID)
		if err != nil {
			t.Fatal(err)
		}
		restoredEndpoint := healthyHostingEndpointForTest(t)
		observeHostingRecoveryForTest(t, recoveryRunnerID, recovery, digest, restoredEndpoint)
		fake.fail = true
		if err := completeHostingRuntimeRecovery(t.Context(), recoveryRunnerID, recovery.JobID,
			recovery.LeaseGeneration, recovery.LeaseToken, hostingCompletionRequest{Status: "success",
				ReleaseDigest: digest, ReleaseArtifactDigest: artifactDigest,
				RuntimeEndpoint: restoredEndpoint,
				HealthEvidence:  map[string]any{"healthy": true, "attempts": float64(1)}}); err == nil {
			t.Fatal("proxy outage did not leave recovery activation pending")
		}
		operator, err := createServiceToken("recovery-expiry-kill", []string{serviceScopeProjectsWrite})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := setHostingExecutionKillSwitch(t.Context(), operator, project.ExternalProjectID,
			hostingKillSwitchRequest{Enabled: true, Reason: "recovery expiry fence incident"},
			"recovery_expiry_kill_01", "request-recovery-expiry-kill"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`UPDATE hosting_proxy_operations SET status='failed'
			WHERE hosting_runtime_recovery_id=? AND operation_type='recover'`, recovery.JobID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`UPDATE hosting_runtime_recoveries SET lease_expires_at=? WHERE id=?`,
			formatSQLiteTime(time.Now().UTC().Add(-time.Minute)), recovery.JobID); err != nil {
			t.Fatal(err)
		}
		restartHostingDatabaseForCancellationTest(t)
		if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		var recoveryStatus, suspendStatus string
		if err := db.QueryRow(`SELECT status FROM hosting_runtime_recoveries WHERE id=?`,
			recovery.JobID).Scan(&recoveryStatus); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations
			WHERE hosting_project_id=? AND operation_type='suspend' ORDER BY id DESC LIMIT 1`,
			project.ID).Scan(&suspendStatus); err != nil {
			t.Fatal(err)
		}
		if recoveryStatus != "running" || suspendStatus != "pending" {
			t.Fatalf("before recovery fence status=%s suspend=%s", recoveryStatus, suspendStatus)
		}
		fake.fail = false
		if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT status FROM hosting_runtime_recoveries WHERE id=?`,
			recovery.JobID).Scan(&recoveryStatus); err != nil || recoveryStatus != "cancelled" {
			t.Fatalf("after recovery fence status=%s err=%v", recoveryStatus, err)
		}
	})
}

func restartHostingDatabaseForCancellationTest(t *testing.T) {
	t.Helper()
	var sequence int
	var databaseName, databasePath string
	if err := db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &databaseName, &databasePath); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := initDB(databasePath); err != nil {
		t.Fatal(err)
	}
}

func TestHostingTerminalFailureUsesPersistedCancellationIntent(t *testing.T) {
	withTempDB(t)
	_, token, runnerID, job := createAndClaimHostingJobAtFetching(t,
		"project_01JCANCELFAIL", "deployment_01JCANCELFAIL")
	staleState, err := getHostingCompletionState(t.Context(), runnerID, job.JobID,
		job.LeaseGeneration, job.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cancelHostingDeployment(t.Context(), token, "deployment_01JCANCELFAIL",
		"cancel_01JCANCELFAIL"); err != nil {
		t.Fatal(err)
	}
	if err := finishHostingJobTerminal(t.Context(), staleState, hostingStatusFailed,
		hostingPhaseFailed, "build_failed", "stale failure"); err != nil {
		t.Fatal(err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JCANCELFAIL")
	if err != nil || deployment.Status != hostingStatusCancelled || deployment.FailureCode != "cancelled" {
		t.Fatalf("failure beat persisted cancellation: %+v err=%v", deployment, err)
	}
}

func TestHostingRuntimeRecoveryFailureUsesPersistedCancellationIntent(t *testing.T) {
	withTempDB(t)
	withFakeProxy(t)
	project, _, lostRunnerID, job := createAndClaimHostingJob(t,
		"project_01JRECCANCELFAIL", "deployment_01JRECCANCELFAIL")
	digest := "sha256:" + strings.Repeat("7", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	if err := completeHostingJob(t.Context(), lostRunnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: healthyHostingEndpointForTest(t),
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	capacity := hostingWorkloadLimits{CPUMillis: limits.CPUMillis * 2, RAMBytes: limits.RAMBytes * 2,
		DiskBytes: limits.DiskBytes * 2, PIDs: limits.PIDs * 2}
	recoveryRunnerID := insertHostingRunnerForTest(t, "recovery-cancel-failure", capacity)
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=NULL WHERE id=?`, lostRunnerID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	recovery, err := claimHostingRuntimeRecovery(t.Context(), recoveryRunnerID)
	if err != nil {
		t.Fatal(err)
	}
	staleState, err := authenticateHostingRecoveryLease(t.Context(), recoveryRunnerID, recovery.JobID,
		recovery.LeaseGeneration, recovery.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	operator, err := createServiceToken("recovery-cancel-failure-operator", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID,
		"suspended", "suspend_01JRECCANCELFAIL"); err != nil {
		t.Fatal(err)
	}
	var expiryBefore string
	if err := db.QueryRow(`SELECT lease_expires_at FROM hosting_runtime_recoveries WHERE id=?`,
		recovery.JobID).Scan(&expiryBefore); err != nil {
		t.Fatal(err)
	}
	heartbeat := hostingRecoveryHeartbeatForTest(t, recoveryRunnerID, recovery)
	if heartbeat.Code != http.StatusOK {
		t.Fatalf("recovery cancel heartbeat status=%d body=%s", heartbeat.Code, heartbeat.Body.String())
	}
	var heartbeatResponse map[string]bool
	if err := json.NewDecoder(heartbeat.Body).Decode(&heartbeatResponse); err != nil ||
		!heartbeatResponse["cancel_requested"] {
		t.Fatalf("recovery cancel heartbeat=%v err=%v", heartbeatResponse, err)
	}
	var expiryAfter string
	if err := db.QueryRow(`SELECT lease_expires_at FROM hosting_runtime_recoveries WHERE id=?`,
		recovery.JobID).Scan(&expiryAfter); err != nil {
		t.Fatal(err)
	}
	if expiryAfter != expiryBefore {
		t.Fatalf("cancelled recovery lease renewed from %q to %q", expiryBefore, expiryAfter)
	}
	if err := failHostingRuntimeRecovery(t.Context(), staleState, "runtime_start_failed", "stale failure"); err != nil {
		t.Fatal(err)
	}
	var status string
	var freeCPU int64
	if err := db.QueryRow(`SELECT status FROM hosting_runtime_recoveries WHERE id=?`, recovery.JobID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, recoveryRunnerID).Scan(&freeCPU); err != nil {
		t.Fatal(err)
	}
	if status != "cancelled" || freeCPU != capacity.CPUMillis {
		t.Fatalf("recovery failure beat cancellation: status=%q free_cpu=%d", status, freeCPU)
	}
}

func TestHostingCancelIdempotencyIsAtomicAndConflictsAcrossTargets(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JCANCELIDEM")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "cancel-idempotency", hostingWorkloadLimits{
		CPUMillis: limits.CPUMillis * 2, RAMBytes: limits.RAMBytes * 2,
		DiskBytes: limits.DiskBytes * 2, PIDs: limits.PIDs * 2})
	request := validHostingDeploymentRequest("deployment_01JCANCELIDEM1")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID,
		"create_01JCANCELIDEM1", request); err != nil {
		t.Fatal(err)
	}
	const workers = 16
	start := make(chan struct{})
	results := make(chan struct {
		replayed bool
		err      error
	}, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, replayed, err := cancelHostingDeployment(context.Background(), token,
				request.ExternalDeploymentID, "cancel_01JCANCELIDEM")
			results <- struct {
				replayed bool
				err      error
			}{replayed: replayed, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	firstResponses := 0
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if !result.replayed {
			firstResponses++
		}
	}
	if firstResponses != 1 {
		t.Fatalf("non-replayed cancellation responses=%d", firstResponses)
	}
	var events, callbacks, audits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_events WHERE event_type='deployment_cancelled'`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM callback_outbox`).Scan(&callbacks); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_audit_events
		WHERE event_type='hosting_deployment_cancel_requested'`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if events != 1 || callbacks != 1 || audits != 1 {
		t.Fatalf("idempotent side effects events=%d callbacks=%d audits=%d", events, callbacks, audits)
	}

	secondProject, _, _, err := upsertHostingProject(t.Context(), "project_01JCANCELIDEM2", validHostingManifest("node"))
	if err != nil {
		t.Fatal(err)
	}
	secondRequest := validHostingDeploymentRequest("deployment_01JCANCELIDEM2")
	secondRequest.ManifestDigest = secondProject.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, secondProject.ExternalProjectID,
		"create_01JCANCELIDEM2", secondRequest); err != nil {
		t.Fatal(err)
	}
	_, _, err = cancelHostingDeployment(t.Context(), token, secondRequest.ExternalDeploymentID,
		"cancel_01JCANCELIDEM")
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeIdempotencyConflict {
		t.Fatalf("cross-target cancel key conflict=%v", err)
	}
}
