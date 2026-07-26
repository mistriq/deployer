package app

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type runtimeOnlyLifecycleFixture struct {
	project                             *HostingProject
	token                               *ServiceToken
	runnerID                            int64
	targetDeployment, currentDeployment string
	targetDigest, currentDigest         string
	targetEndpoint, currentEndpoint     string
	fake                                *fakeProxyClient
}

func setupRuntimeOnlyLifecycleFixture(t *testing.T) *runtimeOnlyLifecycleFixture {
	t.Helper()
	withTempDB(t)
	fake := withFakeProxy(t)
	targetHealth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	currentHealth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(targetHealth.Close)
	t.Cleanup(currentHealth.Close)

	const targetDeployment = "deployment_01JRTONLYTARGET"
	project, token, runnerID, targetJob := createAndClaimHostingJob(t,
		"project_01JRTONLYLIFE", targetDeployment)
	if _, err := db.Exec(`UPDATE hosting_projects SET publication_mode=? WHERE id=?`,
		hostingPublicationRuntimeOnlyV1, project.ID); err != nil {
		t.Fatal(err)
	}
	project.PublicationMode = hostingPublicationRuntimeOnlyV1
	targetDigest := "sha256:" + strings.Repeat("a", 64)
	observeHostingJobEndpointForTest(t, targetJob.JobID, targetHealth.URL)
	if err := completeHostingJob(t.Context(), runnerID, targetJob.JobID, targetJob.LeaseGeneration,
		targetJob.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: targetDigest,
			ReleaseArtifactDigest: attachTestReleaseArtifact(t, targetJob.JobID, targetDigest),
			RuntimeEndpoint:       targetHealth.URL,
			HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}

	const currentDeployment = "deployment_01JRTONLYCURRENT"
	request := validHostingDeploymentRequest(currentDeployment)
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID,
		"create_01JRTONLYCURRENT", request); err != nil {
		t.Fatal(err)
	}
	currentJob, err := claimHostingJob(t.Context(), runnerID)
	if err != nil {
		t.Fatal(err)
	}
	advanceHostingJobToHealthCheckingForTest(t, runnerID, currentJob)
	currentDigest := "sha256:" + strings.Repeat("b", 64)
	observeHostingJobEndpointForTest(t, currentJob.JobID, currentHealth.URL)
	if err := completeHostingJob(t.Context(), runnerID, currentJob.JobID, currentJob.LeaseGeneration,
		currentJob.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: currentDigest,
			ReleaseArtifactDigest: attachTestReleaseArtifact(t, currentJob.JobID, currentDigest),
			RuntimeEndpoint:       currentHealth.URL,
			HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	return &runtimeOnlyLifecycleFixture{project: project, token: token, runnerID: runnerID,
		targetDeployment: targetDeployment, currentDeployment: currentDeployment,
		targetDigest: targetDigest, currentDigest: currentDigest,
		targetEndpoint: targetHealth.URL, currentEndpoint: currentHealth.URL, fake: fake}
}

func requireZeroRuntimeOnlyProxyWork(t *testing.T, fixture *runtimeOnlyLifecycleFixture) {
	t.Helper()
	var operations int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations WHERE hosting_project_id=?`,
		fixture.project.ID).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	fixture.fake.mu.Lock()
	calls := len(fixture.fake.activations) + len(fixture.fake.suspensions)
	fixture.fake.mu.Unlock()
	if operations != 0 || calls != 0 {
		t.Fatalf("runtime-only proxy operations=%d calls=%d", operations, calls)
	}
}

func confirmRuntimeOnlyStopForTest(t *testing.T, fixture *runtimeOnlyLifecycleFixture) int64 {
	t.Helper()
	var sessionID, instanceID string
	var sequence int64
	var capacity capacityDTO
	if err := db.QueryRow(`SELECT runner.active_session_id, runner.last_heartbeat_sequence,
		runner.capacity_cpu_millis, runner.capacity_ram_bytes, runner.capacity_disk_bytes,
		runner.capacity_pids, release.runtime_instance_id
		FROM hosting_runners runner JOIN hosting_releases release
		ON release.runtime_runner_id=runner.id
		WHERE runner.id=? AND release.hosting_project_id=? AND release.status='active'`,
		fixture.runnerID, fixture.project.ID).Scan(&sessionID, &sequence, &capacity.CPUMillis,
		&capacity.RAMBytes, &capacity.DiskBytes, &capacity.PIDs, &instanceID); err != nil {
		t.Fatal(err)
	}
	inventory := []hostingObservedRuntime{{ExternalProjectID: fixture.project.ExternalProjectID,
		ExternalDeploymentID: fixture.currentDeployment, ReleaseDigest: fixture.currentDigest,
		RuntimeInstanceID: instanceID, RuntimeEndpoint: fixture.currentEndpoint, State: "running"}}
	if response := postHostingInventoryHeartbeat(t, fixture.runnerID, capacity, sessionID,
		sequence+1, inventory); response.Code != http.StatusOK {
		t.Fatalf("runtime cleanup instruction heartbeat=%d body=%s", response.Code, response.Body.String())
	}
	if response := postHostingInventoryHeartbeat(t, fixture.runnerID, capacity, sessionID,
		sequence+2, []hostingObservedRuntime{}); response.Code != http.StatusOK {
		t.Fatalf("runtime cleanup confirmation heartbeat=%d body=%s", response.Code, response.Body.String())
	}
	var recoveryID int64
	if err := db.QueryRow(`SELECT id FROM hosting_runtime_recoveries
		WHERE hosting_release_id=(SELECT id FROM hosting_releases
		 WHERE hosting_project_id=? AND status='active') AND status='queued'`,
		fixture.project.ID).Scan(&recoveryID); err != nil {
		t.Fatal(err)
	}
	return recoveryID
}

func requireOneFreshRuntimeOnlyRecovery(t *testing.T, fixture *runtimeOnlyLifecycleFixture,
	priorRecoveryID int64) int64 {
	t.Helper()
	var recoveryID int64
	var activeCount, totalCount int
	if err := db.QueryRow(`SELECT id FROM hosting_runtime_recoveries
		WHERE hosting_release_id=(SELECT id FROM hosting_releases
		 WHERE hosting_project_id=? AND status='active')
		AND status IN ('queued','leased','running')`, fixture.project.ID).Scan(&recoveryID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_runtime_recoveries
		WHERE hosting_release_id=(SELECT id FROM hosting_releases
		 WHERE hosting_project_id=? AND status='active')
		AND status IN ('queued','leased','running')`, fixture.project.ID).Scan(&activeCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_runtime_recoveries
		WHERE hosting_release_id=(SELECT id FROM hosting_releases
		 WHERE hosting_project_id=? AND status='active')`, fixture.project.ID).Scan(&totalCount); err != nil {
		t.Fatal(err)
	}
	if recoveryID == priorRecoveryID || activeCount != 1 || totalCount != 2 {
		t.Fatalf("fresh recovery=%d prior=%d active=%d total=%d",
			recoveryID, priorRecoveryID, activeCount, totalCount)
	}
	var routeRevision string
	if err := db.QueryRow(`SELECT route_revision FROM hosting_releases
		WHERE hosting_project_id=? AND status='active'`, fixture.project.ID).Scan(&routeRevision); err != nil {
		t.Fatal(err)
	}
	if routeRevision != "" {
		t.Fatalf("runtime-only recovery created route revision %q", routeRevision)
	}
	requireZeroRuntimeOnlyProxyWork(t, fixture)
	return recoveryID
}

func TestRuntimeOnlyRollbackAndAlreadyActiveReplayNeverUseProxy(t *testing.T) {
	fixture := setupRuntimeOnlyLifecycleFixture(t)
	result, err := rollbackHostingReleaseResult(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeployment, fixture.targetDigest,
		"runtime_rollback_success_01")
	if err != nil {
		t.Fatal(err)
	}
	if result.Release == nil || result.Release.Digest != fixture.targetDigest ||
		result.Release.PreviousRelease != fixture.currentDigest || result.Release.RouteRevision != "" ||
		result.Release.PublicationMode != hostingPublicationRuntimeOnlyV1 {
		t.Fatalf("rollback result=%+v", result.Release)
	}
	replayed, err := rollbackHostingReleaseResult(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeployment, fixture.targetDigest,
		"runtime_rollback_success_01")
	if err != nil || !replayed.Replayed {
		t.Fatalf("replay=%+v err=%v", replayed, err)
	}
	already, err := rollbackHostingReleaseResult(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeployment, fixture.targetDigest,
		"runtime_rollback_already_active_01")
	if err != nil || already.Release.PreviousRelease == fixture.targetDigest {
		t.Fatalf("already active result=%+v err=%v", already, err)
	}
	var runtimeOperations int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_runtime_rollbacks WHERE hosting_project_id=?`,
		fixture.project.ID).Scan(&runtimeOperations); err != nil || runtimeOperations != 1 {
		t.Fatalf("runtime rollback operations=%d err=%v", runtimeOperations, err)
	}
	requireZeroRuntimeOnlyProxyWork(t, fixture)
}

func TestRuntimeOnlyRollbackStorageFailureRestartsAndReplaysAtomically(t *testing.T) {
	fixture := setupRuntimeOnlyLifecycleFixture(t)
	requestHash := hashHostingOperation(hostingRollbackOperation, fixture.project.ExternalProjectID,
		fixture.targetDeployment, fixture.targetDigest)
	operationID, replay, err := stageHostingRuntimeRollback(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeployment, fixture.targetDigest,
		"runtime_rollback_restart_01", requestHash)
	if err != nil || replay != nil {
		t.Fatalf("stage operation=%q replay=%+v err=%v", operationID, replay, err)
	}
	if _, err := db.Exec(`CREATE TRIGGER fail_runtime_rollback_event BEFORE INSERT ON hosting_events
		WHEN NEW.event_type='release_rolled_back' BEGIN SELECT RAISE(ABORT, 'event unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := runHostingRuntimeRollback(t.Context(), operationID); err == nil {
		t.Fatal("storage failure unexpectedly committed rollback")
	}
	var activeDigest, status string
	if err := db.QueryRow(`SELECT release_digest FROM hosting_releases WHERE hosting_project_id=? AND status='active'`,
		fixture.project.ID).Scan(&activeDigest); err != nil || activeDigest != fixture.currentDigest {
		t.Fatalf("active after rollback failure=%q err=%v", activeDigest, err)
	}
	if err := db.QueryRow(`SELECT status FROM hosting_runtime_rollbacks WHERE operation_id=?`, operationID).Scan(&status); err != nil || status != "pending" {
		t.Fatalf("operation after failure=%q err=%v", status, err)
	}
	if _, err := db.Exec(`DROP TRIGGER fail_runtime_rollback_event`); err != nil {
		t.Fatal(err)
	}
	restartHostingDatabaseForCancellationTest(t)
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT release_digest FROM hosting_releases WHERE hosting_project_id=? AND status='active'`,
		fixture.project.ID).Scan(&activeDigest); err != nil || activeDigest != fixture.targetDigest {
		t.Fatalf("active after restart=%q err=%v", activeDigest, err)
	}
	var events int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_events WHERE event_type='release_rolled_back'
		AND json_extract(metadata_json, '$.operation_id')=?`, operationID).Scan(&events); err != nil || events != 1 {
		t.Fatalf("rollback events=%d err=%v", events, err)
	}
	requireZeroRuntimeOnlyProxyWork(t, fixture)
}

func TestRuntimeOnlyRollbackFencesNewerActiveRelease(t *testing.T) {
	fixture := setupRuntimeOnlyLifecycleFixture(t)
	requestHash := hashHostingOperation(hostingRollbackOperation, fixture.project.ExternalProjectID,
		fixture.targetDeployment, fixture.targetDigest)
	operationID, _, err := stageHostingRuntimeRollback(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeployment, fixture.targetDigest,
		"runtime_rollback_fence_01", requestHash)
	if err != nil {
		t.Fatal(err)
	}
	now := formatSQLiteTime(time.Now().UTC())
	if _, err := db.Exec(`UPDATE hosting_releases SET status='inactive', deactivated_at=?
		WHERE hosting_project_id=? AND status='active'`, now, fixture.project.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_releases SET status='active', activated_at=?, deactivated_at=NULL
		WHERE hosting_project_id=? AND release_digest=?`, now, fixture.project.ID, fixture.targetDigest); err != nil {
		t.Fatal(err)
	}
	result, err := runHostingRuntimeRollback(t.Context(), operationID)
	if err != nil || result.StatusCode != http.StatusConflict {
		t.Fatalf("superseded result=%+v err=%v", result, err)
	}
	var activeDigest string
	if err := db.QueryRow(`SELECT release_digest FROM hosting_releases WHERE hosting_project_id=? AND status='active'`,
		fixture.project.ID).Scan(&activeDigest); err != nil || activeDigest != fixture.targetDigest {
		t.Fatalf("newer active=%q err=%v", activeDigest, err)
	}
	requireZeroRuntimeOnlyProxyWork(t, fixture)
}

func TestRuntimeOnlySuspendConfirmsRunnerCleanupThenRecoversWithoutProxy(t *testing.T) {
	fixture := setupRuntimeOnlyLifecycleFixture(t)
	if _, _, err := setHostingProjectDesiredState(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, "suspended", "runtime_suspend_confirm_01"); err != nil {
		t.Fatal(err)
	}
	var sessionID, instanceID string
	var sequence int64
	var capacity capacityDTO
	if err := db.QueryRow(`SELECT runner.active_session_id, runner.last_heartbeat_sequence,
		runner.capacity_cpu_millis, runner.capacity_ram_bytes, runner.capacity_disk_bytes, runner.capacity_pids,
		release.runtime_instance_id FROM hosting_runners runner JOIN hosting_releases release
		ON release.runtime_runner_id=runner.id WHERE runner.id=? AND release.status='active'`,
		fixture.runnerID).Scan(&sessionID, &sequence, &capacity.CPUMillis, &capacity.RAMBytes,
		&capacity.DiskBytes, &capacity.PIDs, &instanceID); err != nil {
		t.Fatal(err)
	}
	inventory := []hostingObservedRuntime{{ExternalProjectID: fixture.project.ExternalProjectID,
		ExternalDeploymentID: fixture.currentDeployment, ReleaseDigest: fixture.currentDigest,
		RuntimeInstanceID: instanceID, RuntimeEndpoint: fixture.currentEndpoint, State: "running"}}
	first := postHostingInventoryHeartbeat(t, fixture.runnerID, capacity, sessionID, sequence+1, inventory)
	if first.Code != http.StatusOK {
		t.Fatalf("cleanup heartbeat status=%d body=%s", first.Code, first.Body.String())
	}
	var heartbeat hostingHeartbeatResponse
	if err := json.Unmarshal(first.Body.Bytes(), &heartbeat); err != nil {
		t.Fatal(err)
	}
	for _, retained := range heartbeat.RetainedReleases {
		if retained.ReleaseDigest == fixture.currentDigest && retained.RuntimeInstanceID == instanceID {
			t.Fatalf("suspended runtime was retained: %+v", retained)
		}
	}
	var stopStatus string
	if err := db.QueryRow(`SELECT status FROM hosting_runtime_stops WHERE hosting_release_id=(
		SELECT id FROM hosting_releases WHERE hosting_project_id=? AND status='active')`,
		fixture.project.ID).Scan(&stopStatus); err != nil || stopStatus != "pending" {
		t.Fatalf("stop after cleanup instruction=%q err=%v", stopStatus, err)
	}
	second := postHostingInventoryHeartbeat(t, fixture.runnerID, capacity, sessionID, sequence+2,
		[]hostingObservedRuntime{})
	if second.Code != http.StatusOK {
		t.Fatalf("cleanup confirmation status=%d body=%s", second.Code, second.Body.String())
	}
	var owner sql.NullInt64
	var failureCode string
	if err := db.QueryRow(`SELECT runtime_runner_id, runtime_failure_code FROM hosting_releases
		WHERE hosting_project_id=? AND status='active'`, fixture.project.ID).Scan(&owner, &failureCode); err != nil {
		t.Fatal(err)
	}
	if owner.Valid || failureCode != "execution_disabled" {
		t.Fatalf("suspended owner=%+v failure=%q", owner, failureCode)
	}
	if _, _, err := setHostingProjectDesiredState(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, "active", "runtime_resume_restore_01"); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	recovery, err := claimHostingRuntimeRecovery(t.Context(), fixture.runnerID)
	if err != nil {
		t.Fatal(err)
	}
	recoveryEndpoint := healthyHostingEndpointForTest(t)
	observeHostingRecoveryForTest(t, fixture.runnerID, recovery, fixture.currentDigest, recoveryEndpoint)
	completion := hostingCompletionRequest{Status: "success", ReleaseDigest: fixture.currentDigest,
		ReleaseArtifactDigest: recovery.Recipe.ReleaseArtifactDigest, RuntimeEndpoint: recoveryEndpoint,
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(2)}}
	if _, err := db.Exec(`CREATE TRIGGER fail_runtime_recovery_event BEFORE INSERT ON hosting_events
		WHEN NEW.event_type='runtime_recovered' BEGIN SELECT RAISE(ABORT, 'event unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if err := completeHostingRuntimeRecovery(t.Context(), fixture.runnerID, recovery.JobID,
		recovery.LeaseGeneration, recovery.LeaseToken, completion); err == nil {
		t.Fatal("recovery storage failure unexpectedly committed")
	}
	var stagedStatus, stagedEndpoint string
	if err := db.QueryRow(`SELECT recovery.status, release.runtime_endpoint
		FROM hosting_runtime_recoveries recovery JOIN hosting_releases release
		ON release.id=recovery.hosting_release_id WHERE recovery.id=?`, recovery.JobID).Scan(
		&stagedStatus, &stagedEndpoint); err != nil || stagedStatus != "running" ||
		stagedEndpoint != fixture.currentEndpoint {
		t.Fatalf("staged recovery=%s endpoint=%q err=%v", stagedStatus, stagedEndpoint, err)
	}
	if _, err := db.Exec(`DROP TRIGGER fail_runtime_recovery_event`); err != nil {
		t.Fatal(err)
	}
	restartHostingDatabaseForCancellationTest(t)
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := completeHostingRuntimeRecovery(t.Context(), fixture.runnerID, recovery.JobID,
		recovery.LeaseGeneration, recovery.LeaseToken, completion); err != nil {
		t.Fatalf("exact recovery replay: %v", err)
	}
	var recoveryStatus, routeRevision, restoredFailure, healthEvidenceJSON string
	if err := db.QueryRow(`SELECT recovery.status, release.route_revision, release.runtime_failure_code,
		release.health_evidence_json
		FROM hosting_runtime_recoveries recovery JOIN hosting_releases release
		ON release.id=recovery.hosting_release_id WHERE recovery.id=?`, recovery.JobID).Scan(
		&recoveryStatus, &routeRevision, &restoredFailure, &healthEvidenceJSON); err != nil {
		t.Fatal(err)
	}
	var restoredEvidence map[string]any
	if err := json.Unmarshal([]byte(healthEvidenceJSON), &restoredEvidence); err != nil {
		t.Fatal(err)
	}
	if recoveryStatus != "succeeded" || routeRevision != "" || restoredFailure != "" ||
		restoredEvidence["attempts"] != float64(2) {
		t.Fatalf("recovery=%s route=%q failure=%q evidence=%v", recoveryStatus,
			routeRevision, restoredFailure, restoredEvidence)
	}
	requireZeroRuntimeOnlyProxyWork(t, fixture)
}

func TestRuntimeOnlyStopSurvivesDatabaseAndRunnerSessionRestart(t *testing.T) {
	fixture := setupRuntimeOnlyLifecycleFixture(t)
	if _, _, err := setHostingProjectDesiredState(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, "suspended", "runtime_suspend_restart_01"); err != nil {
		t.Fatal(err)
	}
	var instanceID string
	var capacity capacityDTO
	if err := db.QueryRow(`SELECT runner.capacity_cpu_millis, runner.capacity_ram_bytes,
		runner.capacity_disk_bytes, runner.capacity_pids, release.runtime_instance_id
		FROM hosting_runners runner JOIN hosting_releases release ON release.runtime_runner_id=runner.id
		WHERE runner.id=? AND release.status='active'`, fixture.runnerID).Scan(&capacity.CPUMillis,
		&capacity.RAMBytes, &capacity.DiskBytes, &capacity.PIDs, &instanceID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET last_seen=? WHERE id=?`,
		formatSQLiteTime(time.Now().UTC().Add(-hostingRunnerSessionTakeoverAfter-time.Second)),
		fixture.runnerID); err != nil {
		t.Fatal(err)
	}
	restartHostingDatabaseForCancellationTest(t)
	newSession := strings.TrimPrefix(hashHostingOperation("runtime-stop-new-session", fixture.currentDigest),
		"sha256:")[:48]
	inventory := []hostingObservedRuntime{{ExternalProjectID: fixture.project.ExternalProjectID,
		ExternalDeploymentID: fixture.currentDeployment, ReleaseDigest: fixture.currentDigest,
		RuntimeInstanceID: instanceID, RuntimeEndpoint: fixture.currentEndpoint, State: "running"}}
	first := postHostingInventoryHeartbeat(t, fixture.runnerID, capacity, newSession, 1, inventory)
	if first.Code != http.StatusOK {
		t.Fatalf("takeover cleanup heartbeat status=%d body=%s", first.Code, first.Body.String())
	}
	var storedSession, status string
	var requestedSequence int64
	if err := db.QueryRow(`SELECT runner_session_id, requested_sequence, status
		FROM hosting_runtime_stops WHERE hosting_release_id=(SELECT id FROM hosting_releases
		WHERE hosting_project_id=? AND status='active')`, fixture.project.ID).Scan(
		&storedSession, &requestedSequence, &status); err != nil {
		t.Fatal(err)
	}
	if storedSession != newSession || requestedSequence != 1 || status != "pending" {
		t.Fatalf("takeover stop session=%q sequence=%d status=%q", storedSession, requestedSequence, status)
	}
	second := postHostingInventoryHeartbeat(t, fixture.runnerID, capacity, newSession, 2,
		[]hostingObservedRuntime{})
	if second.Code != http.StatusOK {
		t.Fatalf("takeover confirmation status=%d body=%s", second.Code, second.Body.String())
	}
	if err := db.QueryRow(`SELECT status FROM hosting_runtime_stops WHERE hosting_release_id=(
		SELECT id FROM hosting_releases WHERE hosting_project_id=? AND status='active')`,
		fixture.project.ID).Scan(&status); err != nil || status != "committed" {
		t.Fatalf("confirmed takeover stop=%q err=%v", status, err)
	}
	requireZeroRuntimeOnlyProxyWork(t, fixture)
}

func TestRuntimeOnlySecondSuspendRequeuesCancelledQueuedRecoveryOnResume(t *testing.T) {
	fixture := setupRuntimeOnlyLifecycleFixture(t)
	if _, _, err := setHostingProjectDesiredState(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, "suspended", "runtime_repeat_suspend_first_01"); err != nil {
		t.Fatal(err)
	}
	priorRecoveryID := confirmRuntimeOnlyStopForTest(t, fixture)
	if _, _, err := setHostingProjectDesiredState(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, "active", "runtime_repeat_resume_first_01"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := setHostingProjectDesiredState(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, "suspended", "runtime_repeat_suspend_second_01"); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM hosting_runtime_recoveries WHERE id=?`,
		priorRecoveryID).Scan(&status); err != nil || status != "cancelled" {
		t.Fatalf("prior queued recovery status=%q err=%v", status, err)
	}
	restartHostingDatabaseForCancellationTest(t)
	if _, _, err := setHostingProjectDesiredState(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, "active", "runtime_repeat_resume_second_01"); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	requireOneFreshRuntimeOnlyRecovery(t, fixture, priorRecoveryID)
}

func TestRuntimeOnlyCancelledLeasedOrRunningRecoveryRequeuesOnlyAfterExpiry(t *testing.T) {
	for _, initialStatus := range []string{"leased", "running"} {
		t.Run(initialStatus, func(t *testing.T) {
			fixture := setupRuntimeOnlyLifecycleFixture(t)
			if _, _, err := setHostingProjectDesiredState(t.Context(), fixture.token,
				fixture.project.ExternalProjectID, "suspended", "runtime_fenced_suspend_first_01"); err != nil {
				t.Fatal(err)
			}
			priorRecoveryID := confirmRuntimeOnlyStopForTest(t, fixture)
			if _, _, err := setHostingProjectDesiredState(t.Context(), fixture.token,
				fixture.project.ExternalProjectID, "active", "runtime_fenced_resume_first_01"); err != nil {
				t.Fatal(err)
			}
			if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			recovery, err := claimHostingRuntimeRecovery(t.Context(), fixture.runnerID)
			if err != nil {
				t.Fatal(err)
			}
			if recovery.JobID != priorRecoveryID {
				t.Fatalf("claimed recovery=%d want=%d", recovery.JobID, priorRecoveryID)
			}
			if initialStatus == "running" {
				response := hostingRecoveryHeartbeatForTest(t, fixture.runnerID, recovery)
				if response.Code != http.StatusOK {
					t.Fatalf("start recovery heartbeat=%d body=%s", response.Code, response.Body.String())
				}
			}
			if _, _, err := setHostingProjectDesiredState(t.Context(), fixture.token,
				fixture.project.ExternalProjectID, "suspended", "runtime_fenced_suspend_second_01"); err != nil {
				t.Fatal(err)
			}
			var status string
			var cancelRequested sql.NullString
			if err := db.QueryRow(`SELECT status, cancel_requested_at FROM hosting_runtime_recoveries
				WHERE id=?`, priorRecoveryID).Scan(&status, &cancelRequested); err != nil {
				t.Fatal(err)
			}
			if status != initialStatus || !cancelRequested.Valid {
				t.Fatalf("cancel fence status=%q requested=%+v want=%q", status, cancelRequested, initialStatus)
			}
			if _, _, err := setHostingProjectDesiredState(t.Context(), fixture.token,
				fixture.project.ExternalProjectID, "active", "runtime_fenced_resume_second_01"); err != nil {
				t.Fatal(err)
			}
			restartHostingDatabaseForCancellationTest(t)
			if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			var activeCount, totalCount int
			if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_runtime_recoveries
				WHERE hosting_release_id=(SELECT id FROM hosting_releases
				 WHERE hosting_project_id=? AND status='active')
				AND status IN ('queued','leased','running')`, fixture.project.ID).Scan(&activeCount); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_runtime_recoveries
				WHERE hosting_release_id=(SELECT id FROM hosting_releases
				 WHERE hosting_project_id=? AND status='active')`, fixture.project.ID).Scan(&totalCount); err != nil {
				t.Fatal(err)
			}
			if activeCount != 1 || totalCount != 1 {
				t.Fatalf("recovery escaped cancellation fence before expiry: active=%d total=%d", activeCount, totalCount)
			}
			if _, err := db.Exec(`UPDATE hosting_runtime_recoveries SET lease_expires_at=? WHERE id=?`,
				formatSQLiteTime(time.Now().UTC().Add(-time.Second)), priorRecoveryID); err != nil {
				t.Fatal(err)
			}
			if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT status FROM hosting_runtime_recoveries WHERE id=?`,
				priorRecoveryID).Scan(&status); err != nil || status != "cancelled" {
				t.Fatalf("expired cancellation status=%q err=%v", status, err)
			}
			requireOneFreshRuntimeOnlyRecovery(t, fixture, priorRecoveryID)
		})
	}
}

func TestRuntimeOnlyProjectKillCallbackFailureRollsBackAndReplays(t *testing.T) {
	fixture := setupRuntimeOnlyLifecycleFixture(t)
	queuedID := "deployment_01JRTKILLCALLBACK"
	request := validHostingDeploymentRequest(queuedID)
	request.ManifestDigest = fixture.project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), fixture.token, fixture.project.ExternalProjectID,
		"create_01JRTKILLCALLBACK", request); err != nil {
		t.Fatal(err)
	}
	installFailingCallbackInsertTrigger(t)
	input := hostingKillSwitchRequest{Enabled: true, Reason: "runtime maintenance"}
	if _, _, err := setHostingExecutionKillSwitch(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, input, "runtime_kill_callback_01", "request-runtime-kill"); err == nil {
		t.Fatal("callback insertion failure unexpectedly committed kill switch")
	}
	var reason, jobStatus string
	var stops, receipts int
	if err := db.QueryRow(`SELECT kill_switch_reason FROM hosting_projects WHERE id=?`,
		fixture.project.ID).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT job.status FROM hosting_jobs job JOIN hosting_deployments deployment
		ON deployment.id=job.hosting_deployment_id WHERE deployment.external_deployment_id=?`, queuedID).Scan(
		&jobStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_runtime_stops WHERE status='pending'`).Scan(&stops); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_idempotency WHERE idempotency_key=?`,
		"runtime_kill_callback_01").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if reason != "" || jobStatus != "queued" || stops != 0 || receipts != 0 {
		t.Fatalf("rolled back reason=%q job=%q stops=%d receipts=%d", reason, jobStatus, stops, receipts)
	}
	removeFailingCallbackInsertTrigger(t)
	response, replayed, err := setHostingExecutionKillSwitch(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, input, "runtime_kill_callback_01", "request-runtime-kill")
	if err != nil || replayed || !response.Enabled {
		t.Fatalf("retry response=%+v replayed=%v err=%v", response, replayed, err)
	}
	if err := db.QueryRow(`SELECT job.status FROM hosting_jobs job JOIN hosting_deployments deployment
		ON deployment.id=job.hosting_deployment_id WHERE deployment.external_deployment_id=?`, queuedID).Scan(
		&jobStatus); err != nil || jobStatus != "cancelled" {
		t.Fatalf("retry job=%q err=%v", jobStatus, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_runtime_stops WHERE status='pending'`).Scan(&stops); err != nil || stops != 1 {
		t.Fatalf("retry stops=%d err=%v", stops, err)
	}
	requireZeroRuntimeOnlyProxyWork(t, fixture)
}

func TestMixedGlobalKillRoutesOnlyProxyProjects(t *testing.T) {
	fixture := setupRuntimeOnlyLifecycleFixture(t)
	proxyProject, _ := provisionDeploymentTestProject(t, "project_01JMIXEDPROXY")
	admin, err := createServiceToken("mixed-global-kill", []string{serviceScopeHostingAdmin})
	if err != nil {
		t.Fatal(err)
	}
	input := hostingKillSwitchRequest{Enabled: true, Reason: "global maintenance"}
	if _, _, err := setHostingExecutionKillSwitch(t.Context(), admin, "", input,
		"mixed_global_kill_enable_01", "request-mixed-kill"); err != nil {
		t.Fatal(err)
	}
	var runtimeProxyRows, proxyRows, runtimeStops int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations WHERE hosting_project_id=?`,
		fixture.project.ID).Scan(&runtimeProxyRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations WHERE hosting_project_id=?`,
		proxyProject.ID).Scan(&proxyRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_runtime_stops stop JOIN hosting_releases release
		ON release.id=stop.hosting_release_id WHERE release.hosting_project_id=? AND stop.status='pending'`,
		fixture.project.ID).Scan(&runtimeStops); err != nil {
		t.Fatal(err)
	}
	if runtimeProxyRows != 0 || proxyRows == 0 || runtimeStops != 1 {
		t.Fatalf("runtime proxy=%d proxy rows=%d runtime stops=%d", runtimeProxyRows, proxyRows, runtimeStops)
	}
	var runtimeGeneration int64
	if err := db.QueryRow(`SELECT route_generation FROM hosting_projects WHERE id=?`, fixture.project.ID).Scan(
		&runtimeGeneration); err != nil || runtimeGeneration != 0 {
		t.Fatalf("runtime route generation=%d err=%v", runtimeGeneration, err)
	}
}
