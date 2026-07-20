package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeProxyClient struct {
	mu          sync.Mutex
	activations []proxyActivationRequest
	suspensions []proxySuspendRequest
	fail        bool
	reject      bool
}

func (fake *fakeProxyClient) Activate(_ context.Context, request proxyActivationRequest) (*proxyActivationResponse, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.activations = append(fake.activations, request)
	if fake.fail {
		return nil, errors.New("adapter unavailable")
	}
	if fake.reject {
		return nil, &hostingAPIError{Code: errCodeProxyRejected, Message: "adapter rejected activation", StatusCode: http.StatusBadGateway}
	}
	return &proxyActivationResponse{RouteRevision: "route-rev-" + request.ReleaseDigest[len(request.ReleaseDigest)-8:], ActiveReleaseDigest: request.ReleaseDigest}, nil
}

func (fake *fakeProxyClient) SetSuspended(_ context.Context, request proxySuspendRequest) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.suspensions = append(fake.suspensions, request)
	if fake.fail {
		return errors.New("adapter unavailable")
	}
	return nil
}

func withFakeProxy(t *testing.T) *fakeProxyClient {
	t.Helper()
	fake := &fakeProxyClient{}
	old := hostingProxyClientFactory
	hostingProxyClientFactory = func(AppConfig) (hostingProxyClient, error) { return fake, nil }
	t.Cleanup(func() { hostingProxyClientFactory = old })
	return fake
}

func createAndClaimHostingJob(t *testing.T, projectExternalID, deploymentExternalID string) (*HostingProject, *ServiceToken, int64, *hostingClaimedJob) {
	t.Helper()
	project, token := provisionDeploymentTestProject(t, projectExternalID)
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	capacity := hostingWorkloadLimits{
		CPUMillis: limits.CPUMillis * 3,
		RAMBytes:  limits.RAMBytes * 3,
		DiskBytes: limits.DiskBytes * 3,
		PIDs:      limits.PIDs * 3,
	}
	runnerID := insertHostingRunnerForTest(t, "runner-"+deploymentExternalID, capacity)
	request := validHostingDeploymentRequest(deploymentExternalID)
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_"+deploymentExternalID, request); err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	job, err := claimHostingJob(t.Context(), runnerID)
	if err != nil {
		t.Fatalf("claim hosting job: %v", err)
	}
	return project, token, runnerID, job
}

func attachTestReleaseArtifact(t *testing.T, jobID int64, releaseDigest string) string {
	t.Helper()
	content := []byte("test docker image archive for " + releaseDigest)
	hash := sha256.Sum256(content)
	artifactDigest := "sha256:" + hex.EncodeToString(hash[:])
	path := managedArtifactPath("hosting-release-" + strings.TrimPrefix(artifactDigest, "sha256:") + ".tar")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_jobs SET release_upload_digest=?, release_upload_release_digest=?,
		release_upload_path=?, release_upload_size=? WHERE id=?`, artifactDigest, releaseDigest, path, len(content), jobID); err != nil {
		t.Fatal(err)
	}
	var storedArtifact, storedRelease, storedPath string
	var storedSize int64
	if err := db.QueryRow(`SELECT release_upload_digest, release_upload_release_digest, release_upload_path,
		release_upload_size FROM hosting_jobs WHERE id=?`, jobID).Scan(&storedArtifact, &storedRelease, &storedPath, &storedSize); err != nil {
		t.Fatal(err)
	}
	if storedArtifact != artifactDigest || storedRelease != releaseDigest || storedPath != path || storedSize != int64(len(content)) || !isManagedArtifactPath(path) {
		t.Fatalf("release artifact fixture mismatch artifact=%q release=%q path=%q size=%d managed=%v",
			storedArtifact, storedRelease, storedPath, storedSize, isManagedArtifactPath(path))
	}
	return artifactDigest
}

func TestHostingRunnerHealthyCandidateRetainsRuntimeCapacity(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JACTIVE", "deployment_01JACTIVE")
	releaseDigest := "sha256:" + strings.Repeat("d", 64)
	completion := hostingCompletionRequest{
		Status:                "success",
		ReleaseDigest:         releaseDigest,
		ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, releaseDigest),
		RuntimeEndpoint:       "http://10.10.0.5:3000",
		HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(3), "status_code": float64(200)},
	}
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, completion); err != nil {
		t.Fatalf("complete healthy job: %v", err)
	}
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, completion); err != nil {
		t.Fatalf("duplicate completion must be idempotent: %v", err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JACTIVE")
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if deployment.Status != hostingStatusActive || deployment.Phase != hostingPhaseActive || deployment.ReleaseDigest != releaseDigest {
		t.Fatalf("deployment was not activated: %+v", deployment)
	}
	var releaseStatus, jobStatus string
	if err := db.QueryRow(`SELECT status FROM hosting_releases WHERE release_digest=?`, releaseDigest).Scan(&releaseStatus); err != nil {
		t.Fatalf("read release: %v", err)
	}
	if err := db.QueryRow(`SELECT status FROM hosting_jobs WHERE id=?`, job.JobID).Scan(&jobStatus); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if releaseStatus != "active" || jobStatus != "succeeded" {
		t.Fatalf("release=%q job=%q", releaseStatus, jobStatus)
	}
	limits, _ := hostingLimitsForProfile("starter")
	var freeCPU int64
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, runnerID).Scan(&freeCPU); err != nil {
		t.Fatalf("read capacity: %v", err)
	}
	if freeCPU != limits.CPUMillis*2 {
		t.Fatalf("active runtime reservation was not retained: %d", freeCPU)
	}
	if len(fake.activations) != 1 || fake.activations[0].ReleaseDigest != releaseDigest {
		t.Fatalf("unexpected proxy activations: %#v", fake.activations)
	}
	var callbacks int
	if err := db.QueryRow(`SELECT COUNT(*) FROM callback_outbox`).Scan(&callbacks); err != nil || callbacks != 1 {
		t.Fatalf("terminal callback count=%d err=%v", callbacks, err)
	}
}

func TestHostingHeartbeatCannotEraseOutstandingReservations(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JHEARTB")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	capacity := hostingWorkloadLimits{CPUMillis: limits.CPUMillis * 3, RAMBytes: limits.RAMBytes * 3, DiskBytes: limits.DiskBytes * 3, PIDs: limits.PIDs * 3}
	runnerID := insertHostingRunnerForTest(t, "heartbeat-capacity", capacity)
	request := validHostingDeploymentRequest("deployment_01JHEARTB")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JHEARTB", request); err != nil {
		t.Fatal(err)
	}
	runner := &HostingRunner{ID: runnerID}
	payload, _ := json.Marshal(hostingHeartbeatRequest{Free: capacityDTO{CPUMillis: capacity.CPUMillis, RAMBytes: capacity.RAMBytes, DiskBytes: capacity.DiskBytes, PIDs: capacity.PIDs}, ProtocolVersion: "v1", ManifestVersions: []string{"v1"}, RuntimeVersions: []string{"20", "22"}})
	httpRequest := httptest.NewRequest(http.MethodPost, "/api/hosting-agent/v1/heartbeat", bytes.NewReader(payload))
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest = httpRequest.WithContext(context.WithValue(httpRequest.Context(), hostingRunnerContextKey{}, runner))
	recorder := httptest.NewRecorder()
	handleHostingAgentHeartbeat(recorder, httpRequest)
	if recorder.Code != http.StatusOK {
		t.Fatalf("heartbeat status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var freeCPU int64
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, runnerID).Scan(&freeCPU); err != nil {
		t.Fatal(err)
	}
	if freeCPU != capacity.CPUMillis-limits.CPUMillis {
		t.Fatalf("heartbeat erased reservation: free=%d", freeCPU)
	}
}

func TestHostingReleaseArtifactUploadIsLeaseFencedAndDigestVerified(t *testing.T) {
	withTempDB(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JUPLOAD", "deployment_01JUPLOAD")
	content := []byte("docker image archive")
	hash := sha256.Sum256(content)
	artifactDigest := "sha256:" + hex.EncodeToString(hash[:])
	releaseDigest := "sha256:" + strings.Repeat("7", 64)
	request := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/api/hosting-agent/v1/jobs/%d/release-artifact", job.JobID), bytes.NewReader(content))
	request.ContentLength = int64(len(content))
	request.Header.Set("Content-Type", "application/x-tar")
	request.Header.Set("X-Deployer-Lease-Generation", fmt.Sprint(job.LeaseGeneration))
	request.Header.Set("X-Deployer-Lease-Token", job.LeaseToken)
	request.Header.Set("X-Deployer-Release-Digest", releaseDigest)
	request.Header.Set("X-Deployer-Artifact-Digest", artifactDigest)
	request = request.WithContext(context.WithValue(request.Context(), hostingRunnerContextKey{}, &HostingRunner{ID: runnerID}))
	recorder := httptest.NewRecorder()
	handleHostingJobReleaseArtifact(recorder, request, job.JobID)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("upload status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var storedDigest, storedRelease, storedPath string
	var storedSize int64
	if err := db.QueryRow(`SELECT release_upload_digest, release_upload_release_digest,
		release_upload_path, release_upload_size FROM hosting_jobs WHERE id=?`, job.JobID).Scan(
		&storedDigest, &storedRelease, &storedPath, &storedSize); err != nil {
		t.Fatal(err)
	}
	if storedDigest != artifactDigest || storedRelease != releaseDigest || storedSize != int64(len(content)) || !isManagedArtifactPath(storedPath) {
		t.Fatalf("stored upload digest=%q release=%q path=%q size=%d", storedDigest, storedRelease, storedPath, storedSize)
	}
	badRequest := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/api/hosting-agent/v1/jobs/%d/release-artifact", job.JobID), bytes.NewReader(content))
	badRequest.ContentLength = int64(len(content))
	badRequest.Header = request.Header.Clone()
	badRequest.Header.Set("X-Deployer-Artifact-Digest", "sha256:"+strings.Repeat("0", 64))
	badRequest = badRequest.WithContext(context.WithValue(badRequest.Context(), hostingRunnerContextKey{}, &HostingRunner{ID: runnerID}))
	badRecorder := httptest.NewRecorder()
	handleHostingJobReleaseArtifact(badRecorder, badRequest, job.JobID)
	if badRecorder.Code != http.StatusConflict {
		t.Fatalf("different artifact replay status=%d body=%s", badRecorder.Code, badRecorder.Body.String())
	}
}

func TestContentIdenticalRedeploymentCreatesDistinctReleaseInstances(t *testing.T) {
	withTempDB(t)
	withFakeProxy(t)
	project, token, runnerID, firstJob := createAndClaimHostingJob(t, "project_01JSAMEIM", "deployment_01JSAME01")
	digest := "sha256:" + strings.Repeat("8", 64)
	firstArtifact := attachTestReleaseArtifact(t, firstJob.JobID, digest)
	if err := completeHostingJob(t.Context(), runnerID, firstJob.JobID, firstJob.LeaseGeneration, firstJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: digest, ReleaseArtifactDigest: firstArtifact,
		RuntimeEndpoint: "http://10.60.0.1:3000", HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
	}); err != nil {
		t.Fatal(err)
	}
	request := validHostingDeploymentRequest("deployment_01JSAME02")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JSAME02", request); err != nil {
		t.Fatal(err)
	}
	secondJob, err := claimHostingJob(t.Context(), runnerID)
	if err != nil {
		t.Fatal(err)
	}
	secondArtifact := attachTestReleaseArtifact(t, secondJob.JobID, digest)
	if secondArtifact != firstArtifact {
		t.Fatal("identical image content produced different fixture artifacts")
	}
	if err := completeHostingJob(t.Context(), runnerID, secondJob.JobID, secondJob.LeaseGeneration, secondJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: digest, ReleaseArtifactDigest: secondArtifact,
		RuntimeEndpoint: "http://10.60.0.2:3000", HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
	}); err != nil {
		t.Fatal(err)
	}
	var releases, active int
	if err := db.QueryRow(`SELECT COUNT(*), SUM(CASE WHEN status='active' THEN 1 ELSE 0 END)
		FROM hosting_releases WHERE hosting_project_id=? AND release_digest=?`, project.ID, digest).Scan(&releases, &active); err != nil {
		t.Fatal(err)
	}
	if releases != 2 || active != 1 {
		t.Fatalf("content-identical release instances=%d active=%d", releases, active)
	}
}

func TestExpiredLeaseRecoversWhileRunnerRemainsOnlineAndFencesCompletion(t *testing.T) {
	withTempDB(t)
	withFakeProxy(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JONLREC", "deployment_01JONLREC")
	if _, err := db.Exec(`UPDATE hosting_jobs SET lease_expires_at=? WHERE id=?`, formatSQLiteTime(time.Now().UTC().Add(-time.Minute)), job.JobID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var status, tokenHash string
	if err := db.QueryRow(`SELECT status, lease_token_hash FROM hosting_jobs WHERE id=?`, job.JobID).Scan(&status, &tokenHash); err != nil {
		t.Fatal(err)
	}
	if status != "queued" || tokenHash != "" {
		t.Fatalf("expired online lease was not reclaimed: status=%q token=%q", status, tokenHash)
	}
	err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, hostingCompletionRequest{Status: "failed", FailureCode: "runner_lost"})
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeJobForbidden {
		t.Fatalf("stale completion was not fenced: %v", err)
	}
}

func TestHostingPhaseReportsCannotRegress(t *testing.T) {
	withTempDB(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JPHASES", "deployment_01JPHASES")
	report := func(phase string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(hostingPhaseRequest{Phase: phase})
		request := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/hosting-agent/v1/jobs/%d/phase", job.JobID), bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Deployer-Lease-Generation", fmt.Sprint(job.LeaseGeneration))
		request.Header.Set("X-Deployer-Lease-Token", job.LeaseToken)
		request = request.WithContext(context.WithValue(request.Context(), hostingRunnerContextKey{}, &HostingRunner{ID: runnerID}))
		recorder := httptest.NewRecorder()
		handleHostingJobPhase(recorder, request, job.JobID)
		return recorder
	}
	if recorder := report(hostingPhaseBuilding); recorder.Code != http.StatusNoContent {
		t.Fatalf("building phase status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if recorder := report(hostingPhaseFetching); recorder.Code != http.StatusConflict {
		t.Fatalf("regressed phase status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestHostingRestartReconcilesPendingActivationAndPreservesDurableState(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JREOPEN", "deployment_01JREOPEN")
	digest := "sha256:" + strings.Repeat("9", 64)
	releaseArtifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	state, err := getHostingCompletionState(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := stageHealthyHostingRelease(t.Context(), state, hostingCompletionRequest{
		Status: "success", ReleaseDigest: digest, ReleaseArtifactDigest: releaseArtifactDigest, RuntimeEndpoint: "http://10.44.0.2:3000",
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
	}); err != nil {
		t.Fatal(err)
	}
	project, cancelToken := provisionDeploymentTestProject(t, "project_01JCBOPEN")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "callback-reopen", limits)
	request := validHostingDeploymentRequest("deployment_01JCBOPEN")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), cancelToken, project.ExternalProjectID, "create_01JCBOPEN", request); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cancelHostingDeployment(t.Context(), cancelToken, request.ExternalDeploymentID, "cancel_01JCBOPEN"); err != nil {
		t.Fatal(err)
	}
	claimedCallback, err := claimHostingCallback(t.Context(), time.Now().UTC())
	if err != nil {
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
		t.Fatalf("reopen hosting DB: %v", err)
	}
	var callbackStatus, claimHash string
	if err := db.QueryRow(`SELECT status, claim_token_hash FROM callback_outbox WHERE id=?`, claimedCallback.ID).Scan(&callbackStatus, &claimHash); err != nil {
		t.Fatal(err)
	}
	if callbackStatus != "delivering" || claimHash != hashToken(claimedCallback.ClaimToken) {
		t.Fatalf("callback claim was not durable: status=%q hash=%q", callbackStatus, claimHash)
	}
	var idempotencyResponses int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_idempotency WHERE response_body IS NOT NULL`).Scan(&idempotencyResponses); err != nil || idempotencyResponses < 2 {
		t.Fatalf("idempotency responses after reopen=%d err=%v", idempotencyResponses, err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatalf("reconcile after reopen: %v", err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JREOPEN")
	if err != nil || deployment.Status != hostingStatusActive || deployment.ReleaseDigest != digest {
		t.Fatalf("pending activation was not recovered: %+v err=%v", deployment, err)
	}
	if len(fake.activations) != 1 || fake.activations[0].ReleaseDigest != digest {
		t.Fatalf("reconciled activations=%#v", fake.activations)
	}
	var freeCPU int64
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, runnerID).Scan(&freeCPU); err != nil || freeCPU <= 0 {
		t.Fatalf("capacity invariant after reopen free=%d err=%v", freeCPU, err)
	}
}

func TestHealthFailurePreservesPreviousActiveRelease(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, token, runnerID, firstJob := createAndClaimHostingJob(t, "project_01JHEALTH", "deployment_01JGOOD")
	firstDigest := "sha256:" + strings.Repeat("e", 64)
	if err := completeHostingJob(t.Context(), runnerID, firstJob.JobID, firstJob.LeaseGeneration, firstJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: firstDigest, ReleaseArtifactDigest: attachTestReleaseArtifact(t, firstJob.JobID, firstDigest), RuntimeEndpoint: "http://10.11.0.5:3000",
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
	}); err != nil {
		t.Fatalf("activate first release: %v", err)
	}
	request := validHostingDeploymentRequest("deployment_01JBADHLT")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_deployment_01JBADHLT", request); err != nil {
		t.Fatalf("create second deployment: %v", err)
	}
	secondJob, err := claimHostingJob(t.Context(), runnerID)
	if err != nil {
		t.Fatalf("claim second job: %v", err)
	}
	secondDigest := "sha256:" + strings.Repeat("f", 64)
	if err := completeHostingJob(t.Context(), runnerID, secondJob.JobID, secondJob.LeaseGeneration, secondJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: secondDigest, ReleaseArtifactDigest: attachTestReleaseArtifact(t, secondJob.JobID, secondDigest), RuntimeEndpoint: "http://10.11.0.6:3000",
		HealthEvidence: map[string]any{"healthy": false, "attempts": float64(5)},
	}); err != nil {
		t.Fatalf("record health failure: %v", err)
	}
	var activeDigest string
	if err := db.QueryRow(`SELECT release_digest FROM hosting_releases WHERE hosting_project_id=? AND status='active'`, project.ID).Scan(&activeDigest); err != nil {
		t.Fatalf("read active release: %v", err)
	}
	if activeDigest != firstDigest {
		t.Fatalf("health failure changed active release to %q", activeDigest)
	}
	failed, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JBADHLT")
	if err != nil || failed.Status != hostingStatusFailed || failed.FailureCode != "health_check_failed" {
		t.Fatalf("unexpected failed deployment: %+v err=%v", failed, err)
	}
	if len(fake.activations) != 1 {
		t.Fatalf("unhealthy candidate reached proxy: %#v", fake.activations)
	}
}

func TestDurableCancelBeforeCompletionNeverActivatesCandidate(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	_, token, runnerID, job := createAndClaimHostingJob(t, "project_01JCANCEL", "deployment_01JCANCEL")
	cancelled, _, err := cancelHostingDeployment(t.Context(), token, "deployment_01JCANCEL", "cancel_deployment_01JCANCEL")
	if err != nil {
		t.Fatalf("request cancel: %v", err)
	}
	if cancelled.Phase != hostingPhaseCancelling || cancelled.CancelRequestedAt == nil {
		t.Fatalf("cancellation intent was not durable: %+v", cancelled)
	}
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: "sha256:" + strings.Repeat("a", 64), RuntimeEndpoint: "http://10.12.0.5:3000",
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
	}); err != nil {
		t.Fatalf("complete cancelled job: %v", err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JCANCEL")
	if err != nil || deployment.Status != hostingStatusCancelled {
		t.Fatalf("deployment was not cancelled: %+v err=%v", deployment, err)
	}
	if len(fake.activations) != 0 {
		t.Fatalf("cancelled candidate was activated: %#v", fake.activations)
	}
}

func TestExpiredRunnerLeaseRejectsStaleCompletionAndRequeues(t *testing.T) {
	withTempDB(t)
	withFakeProxy(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JLEASEX", "deployment_01JLEASEX")
	past := formatSQLiteTime(time.Now().UTC().Add(-time.Minute))
	if _, err := db.Exec(`UPDATE hosting_jobs SET lease_expires_at=? WHERE id=?`, past, job.JobID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=? WHERE id=?`, past, runnerID); err != nil {
		t.Fatalf("mark runner offline: %v", err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatalf("reconcile runner loss: %v", err)
	}
	err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, hostingCompletionRequest{
		Status: "failed", FailureCode: "runner_lost", FailureMessage: "late completion",
	})
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeJobForbidden {
		t.Fatalf("stale owner completion was not rejected: %v", err)
	}
	var status string
	var assigned sql.NullInt64
	if err := db.QueryRow(`SELECT status, hosting_runner_id FROM hosting_jobs WHERE id=?`, job.JobID).Scan(&status, &assigned); err != nil {
		t.Fatalf("read reconciled job: %v", err)
	}
	if status != "queued" || assigned.Valid {
		t.Fatalf("runner-loss job status=%q assigned=%v", status, assigned)
	}
}

func TestExpiredRunnerLeaseReassignsToCompatibleRunner(t *testing.T) {
	withTempDB(t)
	withFakeProxy(t)
	_, _, failedRunnerID, job := createAndClaimHostingJob(t, "project_01JREASSN", "deployment_01JREASSN")
	limits, _ := hostingLimitsForProfile("starter")
	replacementRunnerID := insertHostingRunnerForTest(t, "replacement-runner", limits)
	past := formatSQLiteTime(time.Now().UTC().Add(-time.Minute))
	if _, err := db.Exec(`UPDATE hosting_jobs SET lease_expires_at=? WHERE id=?`, past, job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=? WHERE id=?`, past, failedRunnerID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatalf("reconcile runner loss: %v", err)
	}
	var status string
	var assigned int64
	if err := db.QueryRow(`SELECT status, hosting_runner_id FROM hosting_jobs WHERE id=?`, job.JobID).Scan(&status, &assigned); err != nil {
		t.Fatal(err)
	}
	if status != "queued" || assigned != replacementRunnerID {
		t.Fatalf("reassigned job status=%q runner=%d, want queued runner=%d", status, assigned, replacementRunnerID)
	}
	replacementJob, err := claimHostingJob(t.Context(), replacementRunnerID)
	if err != nil {
		t.Fatalf("replacement runner could not claim: %v", err)
	}
	if replacementJob.JobID != job.JobID || replacementJob.LeaseGeneration <= job.LeaseGeneration {
		t.Fatalf("replacement lease = %+v, previous = %+v", replacementJob, job)
	}
}

func TestRepeatedRunnerLossTerminatesWithStableFailure(t *testing.T) {
	withTempDB(t)
	withFakeProxy(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JLOSTXX", "deployment_01JLOSTXX")
	past := formatSQLiteTime(time.Now().UTC().Add(-time.Minute))
	if _, err := db.Exec(`UPDATE hosting_jobs SET lease_expires_at=?, attempts=? WHERE id=?`, past, hostingMaxJobAttempts, job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=? WHERE id=?`, past, runnerID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatalf("reconcile repeated runner loss: %v", err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JLOSTXX")
	if err != nil {
		t.Fatal(err)
	}
	if deployment.Status != hostingStatusFailed || deployment.FailureCode != "runner_lost" || deployment.FinishedAt == nil {
		t.Fatalf("deployment = %+v", deployment)
	}
	var callbacks int
	if err := db.QueryRow(`SELECT COUNT(*) FROM callback_outbox WHERE hosting_deployment_id=?`, deployment.ID).Scan(&callbacks); err != nil || callbacks != 1 {
		t.Fatalf("callbacks=%d err=%v", callbacks, err)
	}
}

func TestProxyActivationFailurePreservesPreviousRelease(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, token, runnerID, firstJob := createAndClaimHostingJob(t, "project_01JPROXYF", "deployment_01JPROXY1")
	firstDigest := "sha256:" + strings.Repeat("1", 64)
	if err := completeHostingJob(t.Context(), runnerID, firstJob.JobID, firstJob.LeaseGeneration, firstJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: firstDigest, ReleaseArtifactDigest: attachTestReleaseArtifact(t, firstJob.JobID, firstDigest), RuntimeEndpoint: "http://10.20.0.1:3000",
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
	}); err != nil {
		t.Fatal(err)
	}
	request := validHostingDeploymentRequest("deployment_01JPROXY2")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JPROXY2", request); err != nil {
		t.Fatal(err)
	}
	secondJob, err := claimHostingJob(t.Context(), runnerID)
	if err != nil {
		t.Fatal(err)
	}
	fake.reject = true
	secondDigest := "sha256:" + strings.Repeat("2", 64)
	if err := completeHostingJob(t.Context(), runnerID, secondJob.JobID, secondJob.LeaseGeneration, secondJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: secondDigest, ReleaseArtifactDigest: attachTestReleaseArtifact(t, secondJob.JobID, secondDigest), RuntimeEndpoint: "http://10.20.0.2:3000",
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
	}); err != nil {
		t.Fatalf("proxy failure should be persisted as terminal state: %v", err)
	}
	var active, failedStatus string
	if err := db.QueryRow(`SELECT release_digest FROM hosting_releases WHERE hosting_project_id=? AND status='active'`, project.ID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM hosting_releases WHERE release_digest=?`, secondDigest).Scan(&failedStatus); err != nil {
		t.Fatal(err)
	}
	if active != firstDigest || failedStatus != "failed" {
		t.Fatalf("active=%q candidate=%q", active, failedStatus)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), request.ExternalDeploymentID)
	if err != nil || deployment.Status != hostingStatusFailed || deployment.FailureCode != "proxy_activation_failed" {
		t.Fatalf("deployment=%+v err=%v", deployment, err)
	}
}

func TestRollbackSelectedReleaseIsIdempotent(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	healthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	t.Cleanup(healthServer.Close)
	project, token, runnerID, firstJob := createAndClaimHostingJob(t, "project_01JROLLBK", "deployment_01JROLLB1")
	firstDigest := "sha256:" + strings.Repeat("3", 64)
	if err := completeHostingJob(t.Context(), runnerID, firstJob.JobID, firstJob.LeaseGeneration, firstJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: firstDigest, ReleaseArtifactDigest: attachTestReleaseArtifact(t, firstJob.JobID, firstDigest), RuntimeEndpoint: healthServer.URL,
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
	}); err != nil {
		t.Fatal(err)
	}
	request := validHostingDeploymentRequest("deployment_01JROLLB2")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JROLLB2", request); err != nil {
		t.Fatal(err)
	}
	secondJob, err := claimHostingJob(t.Context(), runnerID)
	if err != nil {
		t.Fatal(err)
	}
	secondDigest := "sha256:" + strings.Repeat("4", 64)
	if err := completeHostingJob(t.Context(), runnerID, secondJob.JobID, secondJob.LeaseGeneration, secondJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: secondDigest, ReleaseArtifactDigest: attachTestReleaseArtifact(t, secondJob.JobID, secondDigest), RuntimeEndpoint: "http://10.30.0.2:3000",
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
	}); err != nil {
		t.Fatal(err)
	}
	release, replayed, err := rollbackHostingRelease(t.Context(), token, project.ExternalProjectID, firstDigest, "rollback_01JROLLBK")
	if err != nil || replayed || release.Status != "active" || release.PreviousRelease != secondDigest {
		t.Fatalf("rollback release=%+v replayed=%v err=%v", release, replayed, err)
	}
	if _, replayed, err := rollbackHostingRelease(t.Context(), token, project.ExternalProjectID, firstDigest, "rollback_01JROLLBK"); err != nil || !replayed {
		t.Fatalf("rollback replayed=%v err=%v", replayed, err)
	}
	if len(fake.activations) != 3 || fake.activations[2].ExpectedPreviousReleaseDigest != secondDigest {
		t.Fatalf("proxy activations=%#v", fake.activations)
	}
}

func TestSuspendIntentPersistsAndReconcilesAfterProxyFailure(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _ := provisionDeploymentTestProject(t, "project_01JSUSPND")
	operator, err := createServiceToken("suspend-operator", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	fake.fail = true
	if _, _, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID, "suspended", "suspend_failure_01"); err == nil {
		t.Fatal("proxy failure was accepted")
	}
	stored, err := getHostingProjectByExternalID(project.ExternalProjectID)
	if err != nil || stored.DesiredState != "suspended" {
		t.Fatalf("durable suspension intent was not retained: %+v err=%v", stored, err)
	}
	fake.fail = false
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatalf("reconcile suspension: %v", err)
	}
	if _, replayed, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID, "suspended", "suspend_success_01"); err != nil || replayed {
		t.Fatalf("suspend replayed=%v err=%v", replayed, err)
	}
	if _, replayed, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID, "suspended", "suspend_success_01"); err != nil || !replayed {
		t.Fatalf("suspend replay replayed=%v err=%v", replayed, err)
	}
	if _, _, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID, "active", "resume_success_01"); err != nil {
		t.Fatal(err)
	}
	stored, err = getHostingProjectByExternalID(project.ExternalProjectID)
	if err != nil || stored.DesiredState != "active" {
		t.Fatalf("resume state=%+v err=%v", stored, err)
	}
	if len(fake.suspensions) != 4 || !fake.suspensions[1].Suspended || !fake.suspensions[2].Suspended || fake.suspensions[3].Suspended {
		t.Fatalf("proxy suspension calls=%#v", fake.suspensions)
	}
}

func TestSuspendDuringBuildPreventsLateActivation(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, runnerID, job := createAndClaimHostingJob(t, "project_01JSUSRUN", "deployment_01JSUSRUN")
	operator, err := createServiceToken("suspend-running-operator", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID, "suspended", "suspend_running_01"); err != nil {
		t.Fatal(err)
	}
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: "sha256:" + strings.Repeat("9", 64), RuntimeEndpoint: "http://10.40.0.1:3000",
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
	}); err != nil {
		t.Fatal(err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JSUSRUN")
	if err != nil || deployment.Status != hostingStatusCancelled {
		t.Fatalf("deployment=%+v err=%v", deployment, err)
	}
	if len(fake.activations) != 0 || len(fake.suspensions) != 1 || !fake.suspensions[0].Suspended {
		t.Fatalf("proxy activations=%#v suspensions=%#v", fake.activations, fake.suspensions)
	}
}

func TestHostingJobClaimIsAtomicUnderConcurrency(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JCLAIMX")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	runnerID := insertHostingRunnerForTest(t, "claim-concurrency-runner", limits)
	request := validHostingDeploymentRequest("deployment_01JCLAIMX")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JCLAIMX", request); err != nil {
		t.Fatal(err)
	}
	const workers = 32
	results := make(chan error, workers)
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := claimHostingJob(context.Background(), runnerID)
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
			continue
		}
		if !errors.Is(err, errNoHostingJob) {
			t.Fatalf("unexpected claim error: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful claims=%d, want 1", successes)
	}
}

func TestHostingLeaseMutationsRejectExpiryAndRedactLogs(t *testing.T) {
	withTempDB(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JLESCAS", "deployment_01JLESCAS")
	runner := &HostingRunner{ID: runnerID}
	callLog := func(message string) *httptest.ResponseRecorder {
		payload, _ := json.Marshal(hostingLogRequest{Stream: "build", Message: message})
		req := httptest.NewRequest(http.MethodPost, "/api/hosting-agent/v1/jobs/1/logs", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Deployer-Lease-Generation", itoa(job.LeaseGeneration))
		req.Header.Set("X-Deployer-Lease-Token", job.LeaseToken)
		req = req.WithContext(context.WithValue(req.Context(), hostingRunnerContextKey{}, runner))
		rec := httptest.NewRecorder()
		handleHostingJobLogs(rec, req, job.JobID)
		return rec
	}
	secret := "super-secret-runner-value"
	if rec := callLog("Authorization: Bearer " + secret); rec.Code != http.StatusNoContent {
		t.Fatalf("valid log = %d body=%s", rec.Code, rec.Body.String())
	}
	var stored string
	if err := db.QueryRow(`SELECT message FROM hosting_logs`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stored, secret) || !strings.Contains(stored, "[REDACTED]") {
		t.Fatalf("stored log was not redacted: %q", stored)
	}
	past := formatSQLiteTime(time.Now().UTC().Add(-time.Minute))
	if _, err := db.Exec(`UPDATE hosting_jobs SET lease_expires_at=? WHERE id=?`, past, job.JobID); err != nil {
		t.Fatal(err)
	}
	if rec := callLog("stale owner write"); rec.Code != http.StatusForbidden {
		t.Fatalf("expired log = %d body=%s", rec.Code, rec.Body.String())
	}
	heartbeat := httptest.NewRequest(http.MethodPost, "/api/hosting-agent/v1/jobs/1/heartbeat", nil)
	heartbeat.Header.Set("X-Deployer-Lease-Generation", itoa(job.LeaseGeneration))
	heartbeat.Header.Set("X-Deployer-Lease-Token", job.LeaseToken)
	heartbeat = heartbeat.WithContext(context.WithValue(heartbeat.Context(), hostingRunnerContextKey{}, runner))
	recorder := httptest.NewRecorder()
	handleHostingJobLeaseHeartbeat(recorder, heartbeat, job.JobID)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expired heartbeat = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_logs`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("log count=%d err=%v", count, err)
	}
}

func TestHostingRunnerCredentialsAreSeparateHashedAndAudited(t *testing.T) {
	withTempDB(t)
	runner, err := createHostingRunner(t.Context(), hostingRunnerInput{
		Name: "dedicated-hosting-runner", Labels: []string{"linux", "hosting"},
		ProtocolVersion: hostingRunnerProtocolVersion, ManifestVersions: []string{hostingManifestVersion}, RuntimeVersions: []string{"20", "22"},
		Capacity: capacityDTO{CPUMillis: 2000, RAMBytes: 2 << 30, DiskBytes: 10 << 30, PIDs: 512},
		Reserve:  capacityDTO{CPUMillis: 250, RAMBytes: 256 << 20, DiskBytes: 1 << 30, PIDs: 32},
	}, "runner-create-request")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(runner.Token, "htr_") {
		t.Fatalf("token = %q", runner.Token)
	}
	var storedHash string
	if err := db.QueryRow(`SELECT token_hash FROM hosting_runners WHERE id=?`, runner.ID).Scan(&storedHash); err != nil {
		t.Fatal(err)
	}
	if storedHash == runner.Token || !strings.HasPrefix(storedHash, "sha256:") {
		t.Fatalf("stored hosting runner token = %q", storedHash)
	}
	passed := false
	handler := hostingRunnerAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		passed = true
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodPost, "/api/hosting-agent/v1/poll", nil)
	req.Header.Set("Authorization", "Bearer "+runner.Token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent || !passed {
		t.Fatalf("hosting runner auth = %d body=%s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, "/api/hosting-agent/v1/poll", nil)
	req.Header.Set("Authorization", "Bearer invalid")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid hosting runner auth = %d", rec.Code)
	}
	req = httptest.NewRequest(http.MethodPost, "/api/hosting-runners/"+itoa(runner.ID)+"/rotate", nil)
	req = req.WithContext(context.WithValue(req.Context(), requestIDContextKey{}, "runner-rotate-request"))
	rec = httptest.NewRecorder()
	handleAPIHostingRunner(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("rotate = %d body=%s", rec.Code, rec.Body.String())
	}
	var rotated map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&rotated); err != nil {
		t.Fatal(err)
	}
	if rotated["token"] == runner.Token {
		t.Fatal("rotation returned the existing credential")
	}
	passed = false
	req = httptest.NewRequest(http.MethodPost, "/api/hosting-agent/v1/poll", nil)
	req.Header.Set("Authorization", "Bearer "+runner.Token)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || passed {
		t.Fatal("old hosting runner credential remained valid")
	}
	var audits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_audit_events WHERE event_type IN ('hosting_runner_created','hosting_runner_credential_rotated')`).Scan(&audits); err != nil || audits != 2 {
		t.Fatalf("runner audits=%d err=%v", audits, err)
	}
}
