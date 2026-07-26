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
	"sync/atomic"
	"testing"
	"time"
)

type fakeProxyClient struct {
	mu               sync.Mutex
	activations      []proxyActivationRequest
	suspensions      []proxySuspendRequest
	fail             bool
	reject           bool
	rejectOnce       bool
	activateStarted  chan struct{}
	activateContinue chan struct{}
	activateApplied  chan struct{}
	activateReturn   chan struct{}
	suspendStarted   chan struct{}
	suspendContinue  chan struct{}
	suspendedOps     map[string]struct{}
	routeGenerations map[string]int64
	currentSuspended bool
	currentEndpoint  string
}

type blockingRequestBody struct {
	reader  *bytes.Reader
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (body *blockingRequestBody) Read(buffer []byte) (int, error) {
	body.once.Do(func() { close(body.started) })
	<-body.release
	return body.reader.Read(buffer)
}

func (*blockingRequestBody) Close() error { return nil }

func (fake *fakeProxyClient) Activate(_ context.Context, request proxyActivationRequest) (*proxyActivationResponse, error) {
	fake.mu.Lock()
	fake.activations = append(fake.activations, request)
	fail := fake.fail
	reject := fake.reject
	if fake.rejectOnce {
		reject = true
		fake.rejectOnce = false
	}
	started := fake.activateStarted
	continued := fake.activateContinue
	fake.mu.Unlock()
	if started != nil {
		started <- struct{}{}
		<-continued
	}
	if fail {
		return nil, errors.New("adapter unavailable")
	}
	if reject {
		return nil, &hostingAPIError{Code: errCodeProxyRejected, Message: "adapter rejected activation", StatusCode: http.StatusBadGateway}
	}
	fake.mu.Lock()
	if fake.routeGenerations == nil {
		fake.routeGenerations = make(map[string]int64)
	}
	if request.RouteGeneration < fake.routeGenerations[request.ExternalProjectID] {
		fake.mu.Unlock()
		return &proxyActivationResponse{RouteRevision: "route-rev-stale", ActiveReleaseDigest: request.ReleaseDigest}, nil
	}
	fake.routeGenerations[request.ExternalProjectID] = request.RouteGeneration
	fake.currentSuspended = false
	fake.currentEndpoint = request.RuntimeEndpoint
	applied := fake.activateApplied
	returned := fake.activateReturn
	fake.mu.Unlock()
	if applied != nil {
		applied <- struct{}{}
		<-returned
	}
	return &proxyActivationResponse{RouteRevision: "route-rev-" + request.ReleaseDigest[len(request.ReleaseDigest)-8:], ActiveReleaseDigest: request.ReleaseDigest}, nil
}

func (fake *fakeProxyClient) SetSuspended(_ context.Context, request proxySuspendRequest) error {
	fake.mu.Lock()
	fake.suspensions = append(fake.suspensions, request)
	fail := fake.fail
	started := fake.suspendStarted
	continued := fake.suspendContinue
	fake.mu.Unlock()
	if started != nil && request.Suspended {
		started <- struct{}{}
		<-continued
	}
	if fail {
		return errors.New("adapter unavailable")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.suspendedOps == nil {
		fake.suspendedOps = make(map[string]struct{})
	}
	if fake.routeGenerations == nil {
		fake.routeGenerations = make(map[string]int64)
	}
	if _, alreadyApplied := fake.suspendedOps[request.OperationID]; alreadyApplied {
		return nil
	}
	fake.suspendedOps[request.OperationID] = struct{}{}
	if request.RouteGeneration < fake.routeGenerations[request.ExternalProjectID] {
		return nil
	}
	fake.routeGenerations[request.ExternalProjectID] = request.RouteGeneration
	fake.currentSuspended = request.Suspended
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

func createAndClaimHostingJobAtFetching(t *testing.T, projectExternalID, deploymentExternalID string) (*HostingProject, *ServiceToken, int64, *hostingClaimedJob) {
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

func createAndClaimHostingJob(t *testing.T, projectExternalID, deploymentExternalID string) (*HostingProject, *ServiceToken, int64, *hostingClaimedJob) {
	t.Helper()
	project, token, runnerID, job := createAndClaimHostingJobAtFetching(t, projectExternalID, deploymentExternalID)
	advanceHostingJobToHealthCheckingForTest(t, runnerID, job)
	return project, token, runnerID, job
}

func reportHostingJobPhaseForTest(t *testing.T, runnerID int64, job *hostingClaimedJob,
	phase string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(hostingPhaseRequest{Phase: phase})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/api/hosting-agent/v1/jobs/%d/phase", job.JobID), bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Deployer-Lease-Generation", fmt.Sprint(job.LeaseGeneration))
	request.Header.Set("X-Deployer-Lease-Token", job.LeaseToken)
	request = request.WithContext(context.WithValue(request.Context(), hostingRunnerContextKey{},
		&HostingRunner{ID: runnerID}))
	recorder := httptest.NewRecorder()
	handleHostingJobPhase(recorder, request, job.JobID)
	return recorder
}

func advanceHostingJobToHealthCheckingForTest(t *testing.T, runnerID int64, job *hostingClaimedJob) {
	t.Helper()
	for _, phase := range []string{hostingPhaseBuilding, hostingPhaseStarting, hostingPhaseHealth} {
		if recorder := reportHostingJobPhaseForTest(t, runnerID, job, phase); recorder.Code != http.StatusNoContent {
			t.Fatalf("advance hosting job to %s: status=%d body=%s", phase,
				recorder.Code, recorder.Body.String())
		}
	}
}

func healthyHostingEndpointForTest(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	_, _ = db.Exec(`UPDATE hosting_jobs SET runtime_observed_endpoint=?
		WHERE status IN ('leased','running') AND runtime_observed_at IS NOT NULL`, server.URL)
	return server.URL
}

func observeHostingJobEndpointForTest(t *testing.T, jobID int64, endpoint string) {
	t.Helper()
	if _, err := db.Exec(`UPDATE hosting_jobs SET runtime_observed_endpoint=? WHERE id=?`, endpoint, jobID); err != nil {
		t.Fatal(err)
	}
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
	var runnerID, generation int64
	var activeSession string
	if err := db.QueryRow(`SELECT job.hosting_runner_id, job.lease_generation, runner.active_session_id
		FROM hosting_jobs job
		JOIN hosting_runners runner ON runner.id=job.hosting_runner_id
		WHERE job.id=?`, jobID).Scan(&runnerID, &generation, &activeSession); err != nil {
		t.Fatal(err)
	}
	if activeSession == "" {
		activeSession = strings.TrimPrefix(hashHostingOperation("test.runner.session", fmt.Sprint(runnerID)), "sha256:")[:48]
		if _, err := db.Exec(`UPDATE hosting_runners SET active_session_id=?, last_heartbeat_sequence=1,
			operation_capabilities_json='["build","restore","runtime-inventory-v1"]' WHERE id=?`,
			activeSession, runnerID); err != nil {
			t.Fatal(err)
		}
	}
	expectedInstance := fmt.Sprintf("build-%d-%d", jobID, generation)
	if _, err := db.Exec(`UPDATE hosting_jobs SET release_upload_digest=?, release_upload_release_digest=?,
		release_upload_path=?, release_upload_size=?, runtime_observed_at=?, runtime_observed_session_id=?,
		runtime_observed_release_digest=?, runtime_observed_instance_id=? WHERE id=?`, artifactDigest,
		releaseDigest, path, len(content), formatSQLiteTime(time.Now().UTC()), activeSession,
		releaseDigest, expectedInstance, jobID); err != nil {
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

func observeHostingRecoveryForTest(t *testing.T, runnerID int64, recovery *hostingClaimedJob,
	releaseDigest, endpoint string) {
	t.Helper()
	var activeSession string
	if err := db.QueryRow(`SELECT active_session_id FROM hosting_runners WHERE id=?`, runnerID).Scan(&activeSession); err != nil {
		t.Fatal(err)
	}
	if activeSession == "" {
		activeSession = strings.TrimPrefix(hashHostingOperation("test.runner.session", fmt.Sprint(runnerID)), "sha256:")[:48]
		if _, err := db.Exec(`UPDATE hosting_runners SET active_session_id=?, last_heartbeat_sequence=1,
			operation_capabilities_json='["build","restore","runtime-inventory-v1"]' WHERE id=?`,
			activeSession, runnerID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`UPDATE hosting_runtime_recoveries SET runtime_observed_at=?,
		runtime_observed_session_id=?, runtime_observed_release_digest=?, runtime_observed_instance_id=?,
		runtime_observed_endpoint=?
		WHERE id=?`, formatSQLiteTime(time.Now().UTC()), activeSession, releaseDigest,
		fmt.Sprintf("restore-%d-%d", recovery.JobID, recovery.LeaseGeneration), endpoint, recovery.JobID); err != nil {
		t.Fatal(err)
	}
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
		RuntimeEndpoint:       healthyHostingEndpointForTest(t),
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

func TestInternalRuntimeEvidenceContractIsScopedAndAuthoritative(t *testing.T) {
	withTempDB(t)
	withFakeProxy(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JEVIDENCE", "deployment_01JEVIDENCE")
	releaseDigest := "sha256:" + strings.Repeat("e", 64)
	completion := hostingCompletionRequest{
		Status:                "success",
		ReleaseDigest:         releaseDigest,
		ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, releaseDigest),
		RuntimeEndpoint:       healthyHostingEndpointForTest(t),
		HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(2), "status_code": float64(200)},
	}
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, completion); err != nil {
		t.Fatalf("complete healthy job: %v", err)
	}
	reader, err := createServiceToken("runtime-evidence-reader", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatal(err)
	}
	denied, err := createServiceToken("runtime-evidence-denied", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	handler := serviceTokenAuthMiddleware(http.HandlerFunc(handleInternalAPI))

	request := httptest.NewRequest(http.MethodGet, "/api/internal/v1/deployments/deployment_01JEVIDENCE", nil)
	request.Header.Set("Authorization", "Bearer "+reader.Token)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("poll status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var deployment HostingDeployment
	if err := json.NewDecoder(recorder.Body).Decode(&deployment); err != nil {
		t.Fatal(err)
	}
	evidence, ok := deployment.HealthEvidence.(map[string]any)
	if deployment.RuntimeEndpoint != completion.RuntimeEndpoint || !ok || evidence["healthy"] != true || evidence["status_code"] != float64(200) {
		t.Fatalf("poll runtime evidence=%+v", deployment)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/internal/v1/projects/project_01JEVIDENCE/releases", nil)
	request.Header.Set("Authorization", "Bearer "+reader.Token)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("release list status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var releases []HostingRelease
	if err := json.NewDecoder(recorder.Body).Decode(&releases); err != nil {
		t.Fatal(err)
	}
	if len(releases) != 1 || releases[0].RuntimeEndpoint != completion.RuntimeEndpoint {
		t.Fatalf("release runtime evidence=%+v", releases)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/internal/v1/deployments/deployment_01JEVIDENCE", nil)
	request.Header.Set("Authorization", "Bearer "+denied.Token)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("insufficient scope status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	if _, err := db.Exec(`UPDATE hosting_releases SET runtime_endpoint='http://public.example.test:8080' WHERE release_digest=?`, releaseDigest); err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodGet, "/api/internal/v1/deployments/deployment_01JEVIDENCE", nil)
	request.Header.Set("Authorization", "Bearer "+reader.Token)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("invalid persisted endpoint status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestConflictingHostingJobCompletionsCannotRaceProxyActivation(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	fake.activateStarted = make(chan struct{})
	fake.activateContinue = make(chan struct{})
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JCOMPFN", "deployment_01JCOMPFN")
	releaseDigest := "sha256:" + strings.Repeat("c", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, releaseDigest)
	success := hostingCompletionRequest{
		Status:                "success",
		ReleaseDigest:         releaseDigest,
		ReleaseArtifactDigest: artifactDigest,
		RuntimeEndpoint:       healthyHostingEndpointForTest(t),
		HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)},
	}
	successResult := make(chan error, 1)
	go func() {
		successResult <- completeHostingJob(context.Background(), runnerID, job.JobID,
			job.LeaseGeneration, job.LeaseToken, success)
	}()
	<-fake.activateStarted

	conflict := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, hostingCompletionRequest{Status: "failed", FailureCode: "build_failed", FailureMessage: "late failure"})
	var apiErr *hostingAPIError
	if !errors.As(conflict, &apiErr) || apiErr.Code != errCodeIdempotencyConflict {
		t.Fatalf("conflicting completion error=%v", conflict)
	}
	fake.activateContinue <- struct{}{}
	if err := <-successResult; err != nil {
		t.Fatalf("successful completion: %v", err)
	}
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, success); err != nil {
		t.Fatalf("identical terminal replay: %v", err)
	}
	conflict = completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, hostingCompletionRequest{Status: "cancelled"})
	if !errors.As(conflict, &apiErr) || apiErr.Code != errCodeIdempotencyConflict {
		t.Fatalf("terminal conflicting replay error=%v", conflict)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JCOMPFN")
	if err != nil || deployment.Status != hostingStatusActive || deployment.ReleaseDigest != releaseDigest {
		t.Fatalf("deployment after completion race=%+v err=%v", deployment, err)
	}
	fake.mu.Lock()
	activationCount := len(fake.activations)
	fake.mu.Unlock()
	if activationCount != 1 {
		t.Fatalf("proxy activations=%d, want 1", activationCount)
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
	recorder := postHostingInventoryHeartbeat(t, runnerID, capacityDTO{CPUMillis: capacity.CPUMillis,
		RAMBytes: capacity.RAMBytes, DiskBytes: capacity.DiskBytes, PIDs: capacity.PIDs},
		strings.TrimPrefix(hashHostingOperation("test.runner.session", fmt.Sprint(runnerID)), "sha256:")[:48],
		2, []hostingObservedRuntime{})
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

func TestHostingRunnerPlacementReservesInventoryHeadroom(t *testing.T) {
	withTempDB(t)
	oldLimit := hostingRuntimeInventorySchedulingLimit
	hostingRuntimeInventorySchedulingLimit = 1
	t.Cleanup(func() { hostingRuntimeInventorySchedulingLimit = oldLimit })
	_, _, runnerID, _ := createAndClaimHostingJob(t, "project_01JINVCAP1", "deployment_01JINVCAP1")
	project, _ := provisionDeploymentTestProject(t, "project_01JINVCAP2")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = selectCompatibleHostingRunner(t.Context(), conn, project.Manifest, limits, "build", false, 0)
	var capacityErr *hostingAPIError
	if !errors.As(err, &capacityErr) || capacityErr.Code != errCodeRunnerCapacityUnavailable {
		t.Fatalf("runner %d exceeded inventory scheduling limit: %v", runnerID, err)
	}
}

func TestHostingBuildClaimRequiresActiveInventoryCapability(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JINVCAP")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	runnerID := insertHostingRunnerForTest(t, "inventory-required", limits)
	if _, err := db.Exec(`UPDATE hosting_runners SET operation_capabilities_json='["build"]' WHERE id=?`, runnerID); err != nil {
		t.Fatal(err)
	}
	if _, err := selectCompatibleHostingRunner(t.Context(), db, project.Manifest, limits, "build", false, 0); err == nil {
		t.Fatal("inventory-less runner was eligible for placement")
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET operation_capabilities_json='["build","runtime-inventory-v1"]',
		active_session_id='' WHERE id=?`, runnerID); err != nil {
		t.Fatal(err)
	}
	if _, err := selectCompatibleHostingRunner(t.Context(), db, project.Manifest, limits, "build", false, 0); err == nil {
		t.Fatal("runner without an active inventory session was eligible for placement")
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET active_session_id=? WHERE id=?`, strings.Repeat("a", 48), runnerID); err != nil {
		t.Fatal(err)
	}
	request := validHostingDeploymentRequest("deployment_01JINVCAP")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID,
		"create_01JINVCAP", request); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET operation_capabilities_json='["build"]' WHERE id=?`, runnerID); err != nil {
		t.Fatal(err)
	}
	if _, err := claimHostingJob(t.Context(), runnerID); !errors.Is(err, errNoHostingJob) {
		t.Fatalf("inventory-less runner claimed build: %v", err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET operation_capabilities_json='["build","runtime-inventory-v1"]',
		active_session_id='' WHERE id=?`, runnerID); err != nil {
		t.Fatal(err)
	}
	if _, err := claimHostingJob(t.Context(), runnerID); !errors.Is(err, errNoHostingJob) {
		t.Fatalf("runner without an active inventory session claimed build: %v", err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET active_session_id=? WHERE id=?`, strings.Repeat("a", 48), runnerID); err != nil {
		t.Fatal(err)
	}
	if _, err := claimHostingJob(t.Context(), runnerID); err != nil {
		t.Fatalf("active inventory runner could not claim build: %v", err)
	}
}

func TestQueuedHostingBuildReassignsWhenRunnerLosesInventoryEligibility(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JINVREASSIGN")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	initialCapacity := hostingWorkloadLimits{CPUMillis: limits.CPUMillis * 2,
		RAMBytes: limits.RAMBytes * 2, DiskBytes: limits.DiskBytes * 2, PIDs: limits.PIDs * 2}
	initialRunnerID := insertHostingRunnerForTest(t, "inventory-reassign-initial", initialCapacity)
	replacementRunnerID := insertHostingRunnerForTest(t, "inventory-reassign-replacement", limits)
	request := validHostingDeploymentRequest("deployment_01JINVREASSIGN")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID,
		"create_01JINVREASSIGN", request); err != nil {
		t.Fatal(err)
	}
	var jobID, assignedRunnerID int64
	if err := db.QueryRow(`SELECT job.id, job.hosting_runner_id FROM hosting_jobs job
		JOIN hosting_deployments deployment ON deployment.id=job.hosting_deployment_id
		WHERE deployment.external_deployment_id=?`, request.ExternalDeploymentID).Scan(
		&jobID, &assignedRunnerID); err != nil {
		t.Fatal(err)
	}
	if assignedRunnerID != initialRunnerID {
		t.Fatalf("initial assignment runner=%d want=%d", assignedRunnerID, initialRunnerID)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET operation_capabilities_json='["build"]'
		WHERE id=?`, initialRunnerID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT hosting_runner_id FROM hosting_jobs WHERE id=?`, jobID).Scan(&assignedRunnerID); err != nil {
		t.Fatal(err)
	}
	if assignedRunnerID != replacementRunnerID {
		t.Fatalf("reassigned runner=%d want=%d", assignedRunnerID, replacementRunnerID)
	}
	var freeCPU, freeRAM, freeDisk, freePIDs int64
	if err := db.QueryRow(`SELECT free_cpu_millis, free_ram_bytes, free_disk_bytes, free_pids
		FROM hosting_runners WHERE id=?`, initialRunnerID).Scan(&freeCPU, &freeRAM, &freeDisk, &freePIDs); err != nil {
		t.Fatal(err)
	}
	if freeCPU != initialCapacity.CPUMillis || freeRAM != initialCapacity.RAMBytes ||
		freeDisk != initialCapacity.DiskBytes || freePIDs != initialCapacity.PIDs {
		t.Fatalf("initial capacity was not restored: cpu=%d ram=%d disk=%d pids=%d",
			freeCPU, freeRAM, freeDisk, freePIDs)
	}
	if _, err := claimHostingJob(t.Context(), replacementRunnerID); err != nil {
		t.Fatalf("replacement runner could not claim reassigned build: %v", err)
	}
}

func postHostingInventoryHeartbeat(t *testing.T, runnerID int64, free capacityDTO, sessionID string,
	sequence int64, inventory []hostingObservedRuntime) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(hostingHeartbeatRequest{Free: free, ProtocolVersion: hostingRunnerProtocolVersion,
		ManifestVersions: []string{hostingManifestVersion}, RuntimeVersions: []string{"20", "22"},
		Operations: []string{"build", "restore", hostingRunnerInventoryOperation}, SessionID: sessionID,
		Sequence: sequence, RuntimeInventory: &inventory})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/hosting-agent/v1/heartbeat", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(context.WithValue(request.Context(), hostingRunnerContextKey{}, &HostingRunner{ID: runnerID}))
	recorder := httptest.NewRecorder()
	handleHostingAgentHeartbeat(recorder, request)
	return recorder
}

func reconcileHostingRuntimeInventoryAt(t *testing.T, runnerID int64, inventory []hostingObservedRuntime, now time.Time) {
	t.Helper()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	if err := queueMissingHostingRuntimes(t.Context(), conn, runnerID, inventory, now); err != nil {
		t.Fatal(err)
	}
	if err := placeHostingRuntimeRecoveries(t.Context(), conn, now); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(t.Context(), `COMMIT`); err != nil {
		t.Fatal(err)
	}
	committed = true
}

func TestHostingRuntimeInventoryRequiresCompleteFencedSessionSnapshots(t *testing.T) {
	withTempDB(t)
	capacity := hostingWorkloadLimits{CPUMillis: 1000, RAMBytes: 1 << 30, DiskBytes: 4 << 30, PIDs: 512}
	runnerID := insertHostingRunnerForTest(t, "inventory-session", capacity)
	free := capacityDTO{CPUMillis: capacity.CPUMillis, RAMBytes: capacity.RAMBytes,
		DiskBytes: capacity.DiskBytes, PIDs: capacity.PIDs}
	empty := make([]hostingObservedRuntime, 0)
	firstSession := strings.Repeat("a", 48)
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, firstSession, 1, empty); recorder.Code != http.StatusConflict {
		t.Fatalf("live legacy takeover status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=NULL WHERE id=?`, runnerID); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(hostingHeartbeatRequest{RuntimeInventory: &empty})
	if err != nil || !bytes.Contains(encoded, []byte(`"runtime_inventory":[]`)) {
		t.Fatalf("empty complete inventory was not encoded: %s err=%v", encoded, err)
	}
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, firstSession, 1, empty); recorder.Code != http.StatusOK {
		t.Fatalf("initial inventory heartbeat status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, firstSession, 1, empty); recorder.Code != http.StatusConflict {
		t.Fatalf("stale sequence status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	secondSession := strings.Repeat("b", 48)
	concurrent := postHostingInventoryHeartbeat(t, runnerID, free, secondSession, 1, empty)
	if concurrent.Code != http.StatusConflict {
		t.Fatalf("concurrent session status=%d body=%s", concurrent.Code, concurrent.Body.String())
	}
	var concurrentBody apiErrorResponse
	if err := json.Unmarshal(concurrent.Body.Bytes(), &concurrentBody); err != nil || concurrentBody.CleanupAuthorized {
		t.Fatalf("unaccepted session received cleanup authority: %+v err=%v", concurrentBody, err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET status='online', last_seen=? WHERE id=?`,
		formatSQLiteTime(time.Now().UTC().Add(-hostingRunnerSessionTakeoverAfter-time.Second)), runnerID); err != nil {
		t.Fatal(err)
	}
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, secondSession, 1, empty); recorder.Code != http.StatusOK {
		t.Fatalf("stale-session takeover status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	superseded := postHostingInventoryHeartbeat(t, runnerID, free, firstSession, 2, empty)
	if superseded.Code != http.StatusConflict {
		t.Fatalf("superseded session status=%d body=%s", superseded.Code, superseded.Body.String())
	}
	var supersededBody apiErrorResponse
	if err := json.Unmarshal(superseded.Body.Bytes(), &supersededBody); err != nil ||
		supersededBody.Code != errCodeRunnerSessionSuperseded || !supersededBody.CleanupAuthorized {
		t.Fatalf("superseded session error=%+v decode=%v", supersededBody, err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET last_seen=? WHERE id=?`,
		formatSQLiteTime(time.Now().UTC().Add(-hostingRunnerSessionTakeoverAfter-time.Second)), runnerID); err != nil {
		t.Fatal(err)
	}
	stillSuperseded := postHostingInventoryHeartbeat(t, runnerID, free, firstSession, 3, empty)
	if stillSuperseded.Code != http.StatusConflict {
		t.Fatalf("tombstoned session reclaimed stale runner status=%d body=%s",
			stillSuperseded.Code, stillSuperseded.Body.String())
	}
	var activeSession string
	if err := db.QueryRow(`SELECT active_session_id FROM hosting_runners WHERE id=?`, runnerID).Scan(&activeSession); err != nil {
		t.Fatal(err)
	}
	if activeSession != secondSession {
		t.Fatalf("tombstoned session reclaimed ownership: %q", activeSession)
	}
	retentionPayload, _ := json.Marshal(hostingSessionRetentionRequest{SessionID: firstSession})
	retentionRequest := httptest.NewRequest(http.MethodPost, "/api/hosting-agent/v1/session-retention", bytes.NewReader(retentionPayload))
	retentionRequest.Header.Set("Content-Type", "application/json")
	retentionRequest = retentionRequest.WithContext(context.WithValue(retentionRequest.Context(), hostingRunnerContextKey{}, &HostingRunner{ID: runnerID}))
	retentionRecorder := httptest.NewRecorder()
	handleHostingAgentSessionRetention(retentionRecorder, retentionRequest)
	if retentionRecorder.Code != http.StatusOK {
		t.Fatalf("durable session retention status=%d body=%s", retentionRecorder.Code, retentionRecorder.Body.String())
	}
	legacyPayload, _ := json.Marshal(hostingHeartbeatRequest{Free: free, ProtocolVersion: "v1",
		ManifestVersions: []string{"v1"}, RuntimeVersions: []string{"20", "22"}, Operations: []string{"build", "restore"}})
	legacyRequest := httptest.NewRequest(http.MethodPost, "/api/hosting-agent/v1/heartbeat", bytes.NewReader(legacyPayload))
	legacyRequest.Header.Set("Content-Type", "application/json")
	legacyRequest = legacyRequest.WithContext(context.WithValue(legacyRequest.Context(), hostingRunnerContextKey{}, &HostingRunner{ID: runnerID}))
	legacyRecorder := httptest.NewRecorder()
	handleHostingAgentHeartbeat(legacyRecorder, legacyRequest)
	if legacyRecorder.Code != http.StatusConflict {
		t.Fatalf("inventory downgrade status=%d body=%s", legacyRecorder.Code, legacyRecorder.Body.String())
	}
	legacyRuntime := []hostingObservedRuntime{{ReleaseDigest: "sha256:" + strings.Repeat("d", 64),
		ExternalProjectID: "project_01JLEGEMPTY", ExternalDeploymentID: "deployment_01JLEGEMPTY",
		RuntimeInstanceID: "", State: "running"}}
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, secondSession, 2, legacyRuntime); recorder.Code != http.StatusOK {
		t.Fatalf("adopted legacy empty-instance inventory status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	duplicate := []hostingObservedRuntime{{ReleaseDigest: "sha256:" + strings.Repeat("c", 64),
		ExternalProjectID: "project_01JINVLDX", ExternalDeploymentID: "deployment_01JINVLDX",
		RuntimeInstanceID: "build-1-1", RuntimeEndpoint: "http://127.0.0.1:3000", State: "running"}}
	duplicate = append(duplicate, duplicate[0])
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, secondSession, 3, duplicate); recorder.Code != http.StatusBadRequest {
		t.Fatalf("duplicate inventory status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestNewInventorySessionDoesNotTreatEnrollmentAsRuntimeAbsence(t *testing.T) {
	withTempDB(t)
	withFakeProxy(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JINVBOOT", "deployment_01JINVBOOT")
	digest := "sha256:" + strings.Repeat("1", 64)
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken,
		hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, digest), RuntimeEndpoint: healthyHostingEndpointForTest(t),
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=NULL WHERE id=?`, runnerID); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile("starter")
	free := capacityDTO{CPUMillis: limits.CPUMillis, RAMBytes: limits.RAMBytes,
		DiskBytes: limits.DiskBytes, PIDs: limits.PIDs}
	sessionID := strings.Repeat("e", 48)
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, sessionID, 1, []hostingObservedRuntime{}); recorder.Code != http.StatusOK {
		t.Fatalf("new session status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var missing sql.NullString
	if err := db.QueryRow(`SELECT runtime_missing_since FROM hosting_releases WHERE release_digest=?`, digest).Scan(&missing); err != nil {
		t.Fatal(err)
	}
	if missing.Valid {
		t.Fatalf("first accepted session snapshot started absence proof at %q", missing.String)
	}
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, sessionID, 2, []hostingObservedRuntime{}); recorder.Code != http.StatusOK {
		t.Fatalf("second session snapshot status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if err := db.QueryRow(`SELECT runtime_missing_since FROM hosting_releases WHERE release_digest=?`, digest).Scan(&missing); err != nil {
		t.Fatal(err)
	}
	if !missing.Valid {
		t.Fatal("second same-session snapshot did not begin durable absence proof")
	}
}

func TestHostingCompletionRequiresExactCurrentSessionRuntimeObservation(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, runnerID, job := createAndClaimHostingJob(t, "project_01JOBSERVE", "deployment_01JOBSERVE")
	digest := "sha256:" + strings.Repeat("6", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	healthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer healthServer.Close()
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=NULL WHERE id=?`, runnerID); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	free := capacityDTO{CPUMillis: limits.CPUMillis * 2, RAMBytes: limits.RAMBytes * 2,
		DiskBytes: limits.DiskBytes * 2, PIDs: limits.PIDs * 2}
	sessionID := strings.Repeat("d", 48)
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, sessionID, 1, []hostingObservedRuntime{}); recorder.Code != http.StatusOK {
		t.Fatalf("open inventory session status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	completion := hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
		ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: healthServer.URL,
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}
	var notObserved *hostingAPIError
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, completion); !errors.As(err, &notObserved) || notObserved.Code != errCodeReleaseNotHealthy {
		t.Fatalf("unobserved candidate err=%v", err)
	}
	instanceID := fmt.Sprintf("build-%d-%d", job.JobID, job.LeaseGeneration)
	wrongEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(wrongEndpoint.Close)
	wrongInventory := []hostingObservedRuntime{{ReleaseDigest: digest, ExternalProjectID: project.ExternalProjectID,
		ExternalDeploymentID: "deployment_01JOBSERVE", RuntimeInstanceID: instanceID,
		RuntimeEndpoint: wrongEndpoint.URL, State: "running"}}
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, sessionID, 2, wrongInventory); recorder.Code != http.StatusOK {
		t.Fatalf("observe wrong candidate endpoint status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, completion); !errors.As(err, &notObserved) || notObserved.Code != errCodeReleaseNotHealthy {
		t.Fatalf("endpoint-mismatched observation err=%v", err)
	}
	inventory := []hostingObservedRuntime{{ReleaseDigest: digest, ExternalProjectID: project.ExternalProjectID,
		ExternalDeploymentID: "deployment_01JOBSERVE", RuntimeInstanceID: instanceID,
		RuntimeEndpoint: healthServer.URL, State: "running"}}
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, sessionID, 3, inventory); recorder.Code != http.StatusOK {
		t.Fatalf("observe candidate status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, completion); err != nil {
		t.Fatalf("observed candidate completion: %v", err)
	}
	if len(fake.activations) != 1 || fake.activations[0].RuntimeEndpoint != healthServer.URL {
		t.Fatalf("activations=%+v", fake.activations)
	}
}

func TestHostingActivationCompensatesIfCandidateDisappearsAfterProxyCallStarts(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, runnerID, job := createAndClaimHostingJob(t, "project_01JOBCASXX", "deployment_01JOBCASXX")
	digest := "sha256:" + strings.Repeat("5", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	healthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer healthServer.Close()
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=NULL WHERE id=?`, runnerID); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	free := capacityDTO{CPUMillis: limits.CPUMillis * 2, RAMBytes: limits.RAMBytes * 2,
		DiskBytes: limits.DiskBytes * 2, PIDs: limits.PIDs * 2}
	sessionID := strings.Repeat("c", 48)
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, sessionID, 1, []hostingObservedRuntime{}); recorder.Code != http.StatusOK {
		t.Fatalf("open inventory session status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	instanceID := fmt.Sprintf("build-%d-%d", job.JobID, job.LeaseGeneration)
	inventory := []hostingObservedRuntime{{ReleaseDigest: digest, ExternalProjectID: project.ExternalProjectID,
		ExternalDeploymentID: "deployment_01JOBCASXX", RuntimeInstanceID: instanceID,
		RuntimeEndpoint: healthServer.URL, State: "running"}}
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, sessionID, 2, inventory); recorder.Code != http.StatusOK {
		t.Fatalf("observe candidate status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	fake.activateStarted = make(chan struct{}, 1)
	fake.activateContinue = make(chan struct{})
	completionResult := make(chan error, 1)
	go func() {
		completionResult <- completeHostingJob(context.Background(), runnerID, job.JobID,
			job.LeaseGeneration, job.LeaseToken, hostingCompletionRequest{Status: "success",
				ReleaseDigest: digest, ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: healthServer.URL,
				HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}})
	}()
	<-fake.activateStarted
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, sessionID, 3, []hostingObservedRuntime{}); recorder.Code != http.StatusOK {
		t.Fatalf("candidate-loss heartbeat status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	close(fake.activateContinue)
	if err := <-completionResult; err != nil {
		t.Fatalf("fenced completion should persist terminal failure: %v", err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JOBCASXX")
	if err != nil || deployment.Status != hostingStatusFailed || deployment.FailureCode != "health_check_failed" {
		t.Fatalf("fenced deployment=%+v err=%v", deployment, err)
	}
	if len(fake.activations) != 1 || len(fake.suspensions) != 1 || !fake.suspensions[0].Suspended || !fake.currentSuspended {
		t.Fatalf("candidate race activations=%+v suspensions=%+v", fake.activations, fake.suspensions)
	}
}

func TestHostingRuntimeInventorySelfHealsAndFencesReappearingInstance(t *testing.T) {
	withTempDB(t)
	withFakeProxy(t)
	project, _, runnerID, job := createAndClaimHostingJob(t, "project_01JINVENT", "deployment_01JINVENT")
	digest := "sha256:" + strings.Repeat("7", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	activeEndpoint := healthyHostingEndpointForTest(t)
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken,
		hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: activeEndpoint,
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	instanceID := fmt.Sprintf("build-%d-%d", job.JobID, job.LeaseGeneration)
	running := []hostingObservedRuntime{{ReleaseDigest: digest, ExternalProjectID: project.ExternalProjectID,
		ExternalDeploymentID: "deployment_01JINVENT", RuntimeInstanceID: instanceID,
		RuntimeEndpoint: activeEndpoint, State: "running"}}
	start := time.Now().UTC()
	reconcileHostingRuntimeInventoryAt(t, runnerID, nil, start)
	var missingCount, recoveryCount int
	if err := db.QueryRow(`SELECT runtime_missing_observations FROM hosting_releases WHERE release_digest=?`, digest).Scan(&missingCount); err != nil || missingCount != 1 {
		t.Fatalf("first missing observation=%d err=%v", missingCount, err)
	}
	checking, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JINVENT")
	if err != nil || checking.RuntimeStatus != "checking" || checking.RuntimeFailureCode != "" {
		t.Fatalf("checking runtime state=%+v err=%v", checking, err)
	}
	reconcileHostingRuntimeInventoryAt(t, runnerID, nil, start.Add(hostingRuntimeMissingGrace+time.Second))
	var recoveryID, recoveryRunnerID int64
	var allowOwner int
	if err := db.QueryRow(`SELECT id, hosting_runner_id, allow_owner_runner FROM hosting_runtime_recoveries WHERE status='queued'`).Scan(
		&recoveryID, &recoveryRunnerID, &allowOwner); err != nil {
		t.Fatal(err)
	}
	if recoveryRunnerID != runnerID || allowOwner != 1 {
		t.Fatalf("missing runtime recovery runner=%d allow_owner=%d", recoveryRunnerID, allowOwner)
	}
	recovering, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JINVENT")
	if err != nil || recovering.RuntimeStatus != "recovering" ||
		recovering.RuntimeFailureCode != "runtime_instance_lost" || recovering.RuntimeRecoveryStatus != "queued" {
		t.Fatalf("confirmed runtime state=%+v err=%v", recovering, err)
	}
	var capacityCPU, freeCPU int64
	if err := db.QueryRow(`SELECT capacity_cpu_millis, free_cpu_millis FROM hosting_runners WHERE id=?`, runnerID).Scan(&capacityCPU, &freeCPU); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	if freeCPU != capacityCPU-limits.CPUMillis {
		t.Fatalf("phantom reservation was not transferred exactly once: free=%d capacity=%d", freeCPU, capacityCPU)
	}
	reconcileHostingRuntimeInventoryAt(t, runnerID, running, start.Add(hostingRuntimeMissingGrace+2*time.Second))
	var firstRecoveryStatus string
	var restoredOwner sql.NullInt64
	if err := db.QueryRow(`SELECT status FROM hosting_runtime_recoveries WHERE id=?`, recoveryID).Scan(&firstRecoveryStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT runtime_runner_id FROM hosting_releases WHERE release_digest=?`, digest).Scan(&restoredOwner); err != nil {
		t.Fatal(err)
	}
	if firstRecoveryStatus != "cancelled" || !restoredOwner.Valid || restoredOwner.Int64 != runnerID {
		t.Fatalf("pre-claim reappearance recovery=%q owner=%v", firstRecoveryStatus, restoredOwner)
	}

	secondLoss := start.Add(2 * hostingRuntimeMissingGrace)
	reconcileHostingRuntimeInventoryAt(t, runnerID, nil, secondLoss)
	reconcileHostingRuntimeInventoryAt(t, runnerID, nil, secondLoss.Add(hostingRuntimeMissingGrace+time.Second))
	recovery, err := claimHostingRuntimeRecovery(t.Context(), runnerID)
	if err != nil {
		t.Fatal(err)
	}
	reconcileHostingRuntimeInventoryAt(t, runnerID, running, secondLoss.Add(hostingRuntimeMissingGrace+2*time.Second))
	var detachedOwner sql.NullInt64
	if err := db.QueryRow(`SELECT runtime_runner_id FROM hosting_releases WHERE release_digest=?`, digest).Scan(&detachedOwner); err != nil {
		t.Fatal(err)
	}
	if detachedOwner.Valid {
		t.Fatalf("old runtime reappearance after claim regained ownership: %v", detachedOwner)
	}
	recoveryEndpoint := healthyHostingEndpointForTest(t)
	observeHostingRecoveryForTest(t, runnerID, recovery, digest, recoveryEndpoint)
	if err := completeHostingRuntimeRecovery(t.Context(), runnerID, recovery.JobID,
		recovery.LeaseGeneration, recovery.LeaseToken, hostingCompletionRequest{Status: "success",
			ReleaseDigest: digest, ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: recoveryEndpoint,
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, runnerID).Scan(&freeCPU); err != nil {
		t.Fatal(err)
	}
	if freeCPU != capacityCPU-limits.CPUMillis {
		t.Fatalf("completed self-recovery reservation free=%d capacity=%d", freeCPU, capacityCPU)
	}
	available, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JINVENT")
	if err != nil || available.RuntimeStatus != "available" || available.RuntimeFailureCode != "" ||
		available.RuntimeRecoveryStatus != "succeeded" {
		t.Fatalf("restored runtime state=%+v err=%v", available, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_runtime_recoveries`).Scan(&recoveryCount); err != nil || recoveryCount != 2 {
		t.Fatalf("recovery history count=%d err=%v", recoveryCount, err)
	}
}

func TestHostingHeartbeatRetainsObservedCurrentLeaseCandidate(t *testing.T) {
	withTempDB(t)
	project, _, runnerID, job := createAndClaimHostingJob(t, "project_01JINFLGT", "deployment_01JINFLGT")
	var capacity capacityDTO
	if err := db.QueryRow(`SELECT capacity_cpu_millis, capacity_ram_bytes, capacity_disk_bytes, capacity_pids
		FROM hosting_runners WHERE id=?`, runnerID).Scan(&capacity.CPUMillis, &capacity.RAMBytes, &capacity.DiskBytes, &capacity.PIDs); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("8", 64)
	inventory := []hostingObservedRuntime{{ReleaseDigest: digest, ExternalProjectID: project.ExternalProjectID,
		ExternalDeploymentID: "deployment_01JINFLGT", RuntimeInstanceID: fmt.Sprintf("build-%d-%d", job.JobID, job.LeaseGeneration),
		RuntimeEndpoint: "http://127.0.0.1:3000", State: "running"}}
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=NULL WHERE id=?`, runnerID); err != nil {
		t.Fatal(err)
	}
	recorder := postHostingInventoryHeartbeat(t, runnerID, capacity, strings.Repeat("c", 48), 1, inventory)
	if recorder.Code != http.StatusOK {
		t.Fatalf("candidate inventory heartbeat status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response hostingHeartbeatResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.RetainedReleases) != 1 || response.RetainedReleases[0].ReleaseDigest != digest ||
		response.RetainedReleases[0].RuntimeInstanceID != inventory[0].RuntimeInstanceID {
		t.Fatalf("current lease candidate was not retained: %+v", response.RetainedReleases)
	}
	oldSeen := formatSQLiteTime(time.Now().UTC().Add(-30 * time.Second))
	if _, err := db.Exec(`UPDATE hosting_runners SET last_seen=? WHERE id=?`, oldSeen, runnerID); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/hosting-agent/v1/jobs/%d/heartbeat", job.JobID), nil)
	request.Header.Set("X-Deployer-Lease-Generation", fmt.Sprint(job.LeaseGeneration))
	request.Header.Set("X-Deployer-Lease-Token", job.LeaseToken)
	request = request.WithContext(context.WithValue(request.Context(), hostingRunnerContextKey{}, &HostingRunner{ID: runnerID}))
	leaseRecorder := httptest.NewRecorder()
	handleHostingJobLeaseHeartbeat(leaseRecorder, request, job.JobID)
	if leaseRecorder.Code != http.StatusOK {
		t.Fatalf("job lease heartbeat status=%d body=%s", leaseRecorder.Code, leaseRecorder.Body.String())
	}
	var seen string
	if err := db.QueryRow(`SELECT last_seen FROM hosting_runners WHERE id=?`, runnerID).Scan(&seen); err != nil {
		t.Fatal(err)
	}
	if seen != oldSeen {
		t.Fatalf("lease-only heartbeat refreshed inventory-fenced liveness: %q != %q", seen, oldSeen)
	}
}

func TestMissingInactiveHostingRuntimeReleasesPhantomCapacity(t *testing.T) {
	withTempDB(t)
	withFakeProxy(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JINACTV", "deployment_01JINACTV")
	digest := "sha256:" + strings.Repeat("4", 64)
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken,
		hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, digest), RuntimeEndpoint: healthyHostingEndpointForTest(t),
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_releases SET status='inactive' WHERE release_digest=?`, digest); err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC()
	reconcileHostingRuntimeInventoryAt(t, runnerID, nil, start)
	reconcileHostingRuntimeInventoryAt(t, runnerID, nil, start.Add(hostingRuntimeMissingGrace+time.Second))
	var owner sql.NullInt64
	var failure string
	if err := db.QueryRow(`SELECT runtime_runner_id, runtime_failure_code FROM hosting_releases WHERE release_digest=?`, digest).Scan(&owner, &failure); err != nil {
		t.Fatal(err)
	}
	var capacityCPU, freeCPU int64
	if err := db.QueryRow(`SELECT capacity_cpu_millis, free_cpu_millis FROM hosting_runners WHERE id=?`, runnerID).Scan(&capacityCPU, &freeCPU); err != nil {
		t.Fatal(err)
	}
	if owner.Valid || failure != "runtime_instance_lost" || freeCPU != capacityCPU {
		t.Fatalf("inactive loss owner=%v failure=%q free=%d capacity=%d", owner, failure, freeCPU, capacityCPU)
	}
}

func TestHostingRuntimeLossRestoresRetainedReleaseOnAnotherRunner(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, lostRunnerID, job := createAndClaimHostingJob(t, "project_01JRESTORX", "deployment_01JRESTORX")
	releaseDigest := "sha256:" + strings.Repeat("6", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, releaseDigest)
	completion := hostingCompletionRequest{Status: "success", ReleaseDigest: releaseDigest,
		ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: healthyHostingEndpointForTest(t),
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1), "status_code": float64(200)}}
	if err := completeHostingJob(t.Context(), lostRunnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, completion); err != nil {
		t.Fatal(err)
	}
	updatedManifest := project.Manifest
	updatedManifest.Runtime.HealthPath = "/new-health-contract"
	manifestJSON, manifestDigest, err := manifestJSONAndDigest(updatedManifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_projects SET manifest_json=?, manifest_digest=? WHERE id=?`,
		manifestJSON, manifestDigest, project.ID); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile("starter")
	capacity := hostingWorkloadLimits{CPUMillis: limits.CPUMillis * 2, RAMBytes: limits.RAMBytes * 2,
		DiskBytes: limits.DiskBytes * 2, PIDs: limits.PIDs * 2}
	recoveryRunnerID := insertHostingRunnerForTest(t, "runtime-recovery-target", capacity)
	if _, err := db.Exec(`UPDATE hosting_runners SET operation_capabilities_json='["build","runtime-inventory-v1"]' WHERE id=?`, recoveryRunnerID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=? WHERE id=?`,
		formatSQLiteTime(time.Now().UTC().Add(-2*hostingRunnerStaleAfter)), lostRunnerID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var assignedRecoveryRunner sql.NullInt64
	if err := db.QueryRow(`SELECT hosting_runner_id FROM hosting_runtime_recoveries`).Scan(&assignedRecoveryRunner); err != nil {
		t.Fatal(err)
	}
	if assignedRecoveryRunner.Valid {
		t.Fatalf("build-only runner received restore placement: %v", assignedRecoveryRunner)
	}
	if _, err := claimHostingRuntimeRecovery(t.Context(), recoveryRunnerID); !errors.Is(err, errNoHostingJob) {
		t.Fatalf("build-only runner claimed recovery: %v", err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET operation_capabilities_json='["build","restore","runtime-inventory-v1"]' WHERE id=?`, recoveryRunnerID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT hosting_runner_id FROM hosting_runtime_recoveries`).Scan(&assignedRecoveryRunner); err != nil {
		t.Fatal(err)
	}
	if !assignedRecoveryRunner.Valid || assignedRecoveryRunner.Int64 != recoveryRunnerID {
		t.Fatalf("restore-capable runner was not assigned: %v", assignedRecoveryRunner)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET operation_capabilities_json='["build","runtime-inventory-v1"]' WHERE id=?`, recoveryRunnerID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT hosting_runner_id FROM hosting_runtime_recoveries`).Scan(&assignedRecoveryRunner); err != nil {
		t.Fatal(err)
	}
	var unassignedFreeCPU int64
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, recoveryRunnerID).Scan(&unassignedFreeCPU); err != nil {
		t.Fatal(err)
	}
	if assignedRecoveryRunner.Valid || unassignedFreeCPU != capacity.CPUMillis {
		t.Fatalf("incompatible queued assignment was not released: runner=%v free CPU=%d", assignedRecoveryRunner, unassignedFreeCPU)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET operation_capabilities_json='["build","restore","runtime-inventory-v1"]' WHERE id=?`, recoveryRunnerID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	oldCapacity := capacityDTO{CPUMillis: limits.CPUMillis * 3, RAMBytes: limits.RAMBytes * 3,
		DiskBytes: limits.DiskBytes * 3, PIDs: limits.PIDs * 3}
	returningInventory := []hostingObservedRuntime{{ReleaseDigest: releaseDigest,
		ExternalProjectID: project.ExternalProjectID, ExternalDeploymentID: "deployment_01JRESTORX",
		RuntimeInstanceID: fmt.Sprintf("build-%d-%d", job.JobID, job.LeaseGeneration),
		RuntimeEndpoint:   completion.RuntimeEndpoint, State: "running"}}
	heartbeatRecorder := postHostingInventoryHeartbeat(t, lostRunnerID, oldCapacity,
		strings.Repeat("f", 48), 1, returningInventory)
	if heartbeatRecorder.Code != http.StatusOK {
		t.Fatalf("returning runner heartbeat status=%d body=%s", heartbeatRecorder.Code, heartbeatRecorder.Body.String())
	}
	var oldFreeCPU int64
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, lostRunnerID).Scan(&oldFreeCPU); err != nil {
		t.Fatal(err)
	}
	if oldFreeCPU != oldCapacity.CPUMillis-limits.CPUMillis {
		t.Fatalf("returning runner lost active runtime reservation: free CPU=%d", oldFreeCPU)
	}
	recovery, err := claimHostingRuntimeRecovery(t.Context(), recoveryRunnerID)
	if err != nil {
		t.Fatalf("claim runtime recovery: %v", err)
	}
	if recovery.Recipe.Operation != "restore" || recovery.Recipe.ReleaseDigest != releaseDigest ||
		recovery.Recipe.ReleaseArtifactDigest != artifactDigest || recovery.Recipe.Runtime.HealthPath != "/healthz" {
		t.Fatalf("recovery recipe = %+v", recovery.Recipe)
	}
	encodedRecipe, err := json.Marshal(recovery.Recipe)
	if err != nil {
		t.Fatal(err)
	}
	for _, buildOnlyField := range []string{`"repository"`, `"commit_sha"`, `"manifest_digest"`,
		`"artifact_digest"`, `"source_artifact_url"`} {
		if bytes.Contains(encodedRecipe, []byte(buildOnlyField)) {
			t.Fatalf("restore recipe contains build-only field %s: %s", buildOnlyField, encodedRecipe)
		}
	}
	artifactRequest := httptest.NewRequest(http.MethodGet, recovery.Recipe.ReleaseArtifactURL, nil)
	artifactRequest.Header.Set("X-Deployer-Lease-Generation", fmt.Sprint(recovery.LeaseGeneration))
	artifactRequest.Header.Set("X-Deployer-Lease-Token", recovery.LeaseToken)
	artifactRequest = artifactRequest.WithContext(context.WithValue(artifactRequest.Context(), hostingRunnerContextKey{}, &HostingRunner{ID: recoveryRunnerID}))
	artifactRecorder := httptest.NewRecorder()
	handleHostingRecoveryArtifact(artifactRecorder, artifactRequest, recovery.JobID)
	if artifactRecorder.Code != http.StatusOK || artifactRecorder.Header().Get("X-Deployer-Artifact-Digest") != artifactDigest {
		t.Fatalf("recovery artifact status=%d digest=%q", artifactRecorder.Code, artifactRecorder.Header().Get("X-Deployer-Artifact-Digest"))
	}
	artifactContent := append([]byte(nil), artifactRecorder.Body.Bytes()...)
	var artifactPath string
	if err := db.QueryRow(`SELECT artifact_path FROM hosting_release_artifacts WHERE artifact_digest=?`, artifactDigest).Scan(&artifactPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(artifactPath); err != nil {
		t.Fatal(err)
	}
	missingRequest := httptest.NewRequest(http.MethodGet, recovery.Recipe.ReleaseArtifactURL, nil)
	missingRequest.Header.Set("X-Deployer-Lease-Generation", fmt.Sprint(recovery.LeaseGeneration))
	missingRequest.Header.Set("X-Deployer-Lease-Token", recovery.LeaseToken)
	missingRequest = missingRequest.WithContext(context.WithValue(missingRequest.Context(), hostingRunnerContextKey{}, &HostingRunner{ID: recoveryRunnerID}))
	missingRecorder := httptest.NewRecorder()
	handleHostingRecoveryArtifact(missingRecorder, missingRequest, recovery.JobID)
	if missingRecorder.Code != http.StatusNotFound {
		t.Fatalf("missing recovery artifact status=%d body=%s", missingRecorder.Code, missingRecorder.Body.String())
	}
	if err := os.WriteFile(artifactPath, artifactContent, 0600); err != nil {
		t.Fatal(err)
	}
	restored := hostingCompletionRequest{Status: "success", ReleaseDigest: releaseDigest,
		ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: healthyHostingEndpointForTest(t),
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(2), "status_code": float64(200)}}
	observeHostingRecoveryForTest(t, recoveryRunnerID, recovery, releaseDigest, restored.RuntimeEndpoint)
	if err := completeHostingRuntimeRecovery(t.Context(), recoveryRunnerID, recovery.JobID,
		recovery.LeaseGeneration, recovery.LeaseToken, restored); err != nil {
		t.Fatalf("complete runtime recovery: %v", err)
	}
	if err := completeHostingRuntimeRecovery(t.Context(), recoveryRunnerID, recovery.JobID,
		recovery.LeaseGeneration, recovery.LeaseToken, restored); err != nil {
		t.Fatalf("duplicate recovery completion: %v", err)
	}
	conflicting := restored
	conflicting.RuntimeEndpoint = "http://10.70.0.3:3000"
	err = completeHostingRuntimeRecovery(t.Context(), recoveryRunnerID, recovery.JobID,
		recovery.LeaseGeneration, recovery.LeaseToken, conflicting)
	var conflict *hostingAPIError
	if !errors.As(err, &conflict) || conflict.Code != errCodeIdempotencyConflict {
		t.Fatalf("conflicting terminal recovery completion was accepted: %v", err)
	}
	var releaseRunnerID, generation int64
	var endpoint, recoveryStatus string
	if err := db.QueryRow(`SELECT runtime_runner_id, runtime_generation, runtime_endpoint
		FROM hosting_releases WHERE release_digest=?`, releaseDigest).Scan(&releaseRunnerID, &generation, &endpoint); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM hosting_runtime_recoveries WHERE id=?`, recovery.JobID).Scan(&recoveryStatus); err != nil {
		t.Fatal(err)
	}
	if releaseRunnerID != recoveryRunnerID || generation != 2 || endpoint != restored.RuntimeEndpoint || recoveryStatus != "succeeded" {
		t.Fatalf("restored release runner=%d generation=%d endpoint=%q recovery=%q", releaseRunnerID, generation, endpoint, recoveryStatus)
	}
	var oldJobStatus string
	if err := db.QueryRow(`SELECT status FROM hosting_jobs WHERE id=?`, job.JobID).Scan(&oldJobStatus); err != nil {
		t.Fatal(err)
	}
	if oldJobStatus != "succeeded" {
		t.Fatalf("immutable build outcome changed after runtime move: %q", oldJobStatus)
	}
	var freeCPU int64
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, recoveryRunnerID).Scan(&freeCPU); err != nil {
		t.Fatal(err)
	}
	if freeCPU != capacity.CPUMillis-limits.CPUMillis {
		t.Fatalf("recovery reservation free CPU=%d", freeCPU)
	}
	if _, err := db.Exec(`INSERT INTO hosting_runtime_recoveries
		(hosting_release_id, hosting_runner_id, status, lease_generation, lease_token_hash,
		 attempts, required_cpu_millis, required_ram_bytes, required_disk_bytes, required_pids,
		 created_at, completed_at)
		SELECT id, ?, 'succeeded', 77, '', 1, ?, ?, ?, ?, ?, ?
		FROM hosting_releases WHERE release_digest=?`, recoveryRunnerID, limits.CPUMillis,
		limits.RAMBytes, limits.DiskBytes, limits.PIDs, formatSQLiteTime(time.Now().UTC()),
		formatSQLiteTime(time.Now().UTC()), releaseDigest); err != nil {
		t.Fatalf("insert historical recovery: %v", err)
	}
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := recomputeHostingRunnerCapacity(t.Context(), conn, recoveryRunnerID); err != nil {
		conn.Close()
		t.Fatalf("recompute capacity with multi-hop history: %v", err)
	}
	conn.Close()
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, recoveryRunnerID).Scan(&freeCPU); err != nil {
		t.Fatal(err)
	}
	if freeCPU != capacity.CPUMillis-limits.CPUMillis {
		t.Fatalf("historical recovery was double-counted: free CPU=%d", freeCPU)
	}
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, lostRunnerID).Scan(&oldFreeCPU); err != nil {
		t.Fatal(err)
	}
	if oldFreeCPU != oldCapacity.CPUMillis {
		t.Fatalf("old runner reservation was not transferred after recovery: free CPU=%d", oldFreeCPU)
	}
	if len(fake.activations) != 2 || fake.activations[1].ExpectedPreviousReleaseDigest != releaseDigest {
		t.Fatalf("proxy recoveries = %#v", fake.activations)
	}
}

func TestHostingRuntimeRecoveryProxyIntentSurvivesDatabaseRestart(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	healthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer healthServer.Close()
	_, _, lostRunnerID, job := createAndClaimHostingJob(t, "project_01JRECRST", "deployment_01JRECRST")
	releaseDigest := "sha256:" + strings.Repeat("5", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, releaseDigest)
	if err := completeHostingJob(t.Context(), lostRunnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: releaseDigest, ReleaseArtifactDigest: artifactDigest,
		RuntimeEndpoint: healthyHostingEndpointForTest(t), HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
	}); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile("starter")
	recoveryRunnerID := insertHostingRunnerForTest(t, "runtime-restart-target", hostingWorkloadLimits{
		CPUMillis: limits.CPUMillis * 2, RAMBytes: limits.RAMBytes * 2,
		DiskBytes: limits.DiskBytes * 2, PIDs: limits.PIDs * 2,
	})
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
	observeHostingRecoveryForTest(t, recoveryRunnerID, recovery, releaseDigest, healthServer.URL)
	fake.fail = true
	err = completeHostingRuntimeRecovery(t.Context(), recoveryRunnerID, recovery.JobID,
		recovery.LeaseGeneration, recovery.LeaseToken, hostingCompletionRequest{
			Status: "success", ReleaseDigest: releaseDigest, ReleaseArtifactDigest: artifactDigest,
			RuntimeEndpoint: healthServer.URL, HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
		})
	if err == nil {
		t.Fatal("proxy outage did not interrupt recovery completion")
	}
	var operationStatus string
	if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations WHERE hosting_runtime_recovery_id=?`, recovery.JobID).Scan(&operationStatus); err != nil || operationStatus != "pending" {
		t.Fatalf("durable recovery proxy intent status=%q err=%v", operationStatus, err)
	}
	recoveryFree := capacityDTO{
		CPUMillis: limits.CPUMillis * 2, RAMBytes: limits.RAMBytes * 2,
		DiskBytes: limits.DiskBytes * 2, PIDs: limits.PIDs * 2,
	}
	recoveryInventory := []hostingObservedRuntime{{ReleaseDigest: releaseDigest,
		ExternalProjectID: "project_01JRECRST", ExternalDeploymentID: "deployment_01JRECRST",
		RuntimeInstanceID: fmt.Sprintf("restore-%d-%d", recovery.JobID, recovery.LeaseGeneration),
		RuntimeEndpoint:   healthServer.URL, State: "running"}}
	heartbeatRecorder := postHostingInventoryHeartbeat(t, recoveryRunnerID, recoveryFree,
		strings.TrimPrefix(hashHostingOperation("test.runner.session", fmt.Sprint(recoveryRunnerID)), "sha256:")[:48], 2, recoveryInventory)
	if heartbeatRecorder.Code != http.StatusOK {
		t.Fatalf("recovery candidate heartbeat status=%d body=%s", heartbeatRecorder.Code, heartbeatRecorder.Body.String())
	}
	var heartbeat hostingHeartbeatResponse
	if err := json.NewDecoder(heartbeatRecorder.Body).Decode(&heartbeat); err != nil {
		t.Fatal(err)
	}
	if len(heartbeat.RetainedReleases) != 1 || heartbeat.RetainedReleases[0].RuntimeInstanceID != fmt.Sprintf("restore-%d-%d", recovery.JobID, recovery.LeaseGeneration) {
		t.Fatalf("pending recovery was not retained: %+v", heartbeat.RetainedReleases)
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
	fake.fail = false
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var recoveryStatus, endpoint string
	if err := db.QueryRow(`SELECT status, runtime_endpoint FROM hosting_runtime_recoveries WHERE id=?`, recovery.JobID).Scan(&recoveryStatus, &endpoint); err != nil {
		t.Fatal(err)
	}
	if recoveryStatus != "succeeded" || endpoint != healthServer.URL {
		t.Fatalf("reconciled recovery status=%q endpoint=%q", recoveryStatus, endpoint)
	}
	if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations WHERE hosting_runtime_recovery_id=?`, recovery.JobID).Scan(&operationStatus); err != nil || operationStatus != "committed" {
		t.Fatalf("reconciled operation status=%q err=%v", operationStatus, err)
	}
}

func TestHostingRuntimeRecoveryProxyIntentFencesConcurrentFailure(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	_, _, lostRunnerID, job := createAndClaimHostingJob(t, "project_01JRECRCE", "deployment_01JRECRCE")
	releaseDigest := "sha256:" + strings.Repeat("b", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, releaseDigest)
	if err := completeHostingJob(t.Context(), lostRunnerID, job.JobID, job.LeaseGeneration, job.LeaseToken,
		hostingCompletionRequest{Status: "success", ReleaseDigest: releaseDigest,
			ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: healthyHostingEndpointForTest(t),
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile("starter")
	recoveryRunnerID := insertHostingRunnerForTest(t, "runtime-race-target", hostingWorkloadLimits{
		CPUMillis: limits.CPUMillis * 2, RAMBytes: limits.RAMBytes * 2,
		DiskBytes: limits.DiskBytes * 2, PIDs: limits.PIDs * 2,
	})
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
	fake.activateStarted = make(chan struct{}, 2)
	fake.activateContinue = make(chan struct{})
	successResult := make(chan error, 2)
	recoveryEndpoint := healthyHostingEndpointForTest(t)
	observeHostingRecoveryForTest(t, recoveryRunnerID, recovery, releaseDigest, recoveryEndpoint)
	for range 2 {
		go func() {
			successResult <- completeHostingRuntimeRecovery(context.Background(), recoveryRunnerID, recovery.JobID,
				recovery.LeaseGeneration, recovery.LeaseToken, hostingCompletionRequest{Status: "success",
					ReleaseDigest: releaseDigest, ReleaseArtifactDigest: artifactDigest,
					RuntimeEndpoint: recoveryEndpoint,
					HealthEvidence:  map[string]any{"healthy": true, "attempts": float64(1)}})
		}()
	}
	<-fake.activateStarted
	<-fake.activateStarted
	failureErr := completeHostingRuntimeRecovery(t.Context(), recoveryRunnerID, recovery.JobID,
		recovery.LeaseGeneration, recovery.LeaseToken, hostingCompletionRequest{
			Status: "failed", FailureCode: "runtime_start_failed", FailureMessage: "late failure",
		})
	var completionConflict *hostingAPIError
	if !errors.As(failureErr, &completionConflict) || completionConflict.Code != errCodeIdempotencyConflict {
		close(fake.activateContinue)
		t.Fatalf("concurrent failure was not fenced: %v", failureErr)
	}
	close(fake.activateContinue)
	for range 2 {
		if err := <-successResult; err != nil {
			t.Fatalf("idempotent concurrent success completion: %v", err)
		}
	}
	var recoveryStatus, operationStatus string
	if err := db.QueryRow(`SELECT status FROM hosting_runtime_recoveries WHERE id=?`, recovery.JobID).Scan(&recoveryStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations WHERE hosting_runtime_recovery_id=?`, recovery.JobID).Scan(&operationStatus); err != nil {
		t.Fatal(err)
	}
	if recoveryStatus != "succeeded" || operationStatus != "committed" {
		t.Fatalf("recovery=%q operation=%q", recoveryStatus, operationStatus)
	}
}

func TestHostingRuntimeRecoveryRejectsEndpointChangedDuringProxyDispatch(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, lostRunnerID, job := createAndClaimHostingJob(t,
		"project_01JRECENDPOINT", "deployment_01JRECENDPOINT")
	digest := "sha256:" + strings.Repeat("d", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	activeEndpoint := healthyHostingEndpointForTest(t)
	if err := completeHostingJob(t.Context(), lostRunnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: activeEndpoint,
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	recoveryCapacity := hostingWorkloadLimits{CPUMillis: limits.CPUMillis * 2,
		RAMBytes: limits.RAMBytes * 2, DiskBytes: limits.DiskBytes * 2, PIDs: limits.PIDs * 2}
	recoveryRunnerID := insertHostingRunnerForTest(t, "recovery-endpoint-swap", recoveryCapacity)
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
	endpointAServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(endpointAServer.Close)
	endpointBServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(endpointBServer.Close)
	observeHostingRecoveryForTest(t, recoveryRunnerID, recovery, digest, endpointAServer.URL)
	fake.activateStarted = make(chan struct{}, 1)
	fake.activateContinue = make(chan struct{})
	completionResult := make(chan error, 1)
	go func() {
		completionResult <- completeHostingRuntimeRecovery(context.Background(), recoveryRunnerID,
			recovery.JobID, recovery.LeaseGeneration, recovery.LeaseToken, hostingCompletionRequest{
				Status: "success", ReleaseDigest: digest, ReleaseArtifactDigest: artifactDigest,
				RuntimeEndpoint: endpointAServer.URL,
				HealthEvidence:  map[string]any{"healthy": true, "attempts": float64(1)}})
	}()
	<-fake.activateStarted
	var sessionID string
	if err := db.QueryRow(`SELECT active_session_id FROM hosting_runners WHERE id=?`, recoveryRunnerID).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	free := capacityDTO{CPUMillis: recoveryCapacity.CPUMillis, RAMBytes: recoveryCapacity.RAMBytes,
		DiskBytes: recoveryCapacity.DiskBytes, PIDs: recoveryCapacity.PIDs}
	inventory := []hostingObservedRuntime{{ReleaseDigest: digest,
		ExternalProjectID: project.ExternalProjectID, ExternalDeploymentID: "deployment_01JRECENDPOINT",
		RuntimeInstanceID: fmt.Sprintf("restore-%d-%d", recovery.JobID, recovery.LeaseGeneration),
		RuntimeEndpoint:   endpointBServer.URL, State: "running"}}
	if recorder := postHostingInventoryHeartbeat(t, recoveryRunnerID, free, sessionID, 2, inventory); recorder.Code != http.StatusOK {
		t.Fatalf("recovery endpoint-change heartbeat status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	close(fake.activateContinue)
	completionErr := <-completionResult
	var apiErr *hostingAPIError
	if !errors.As(completionErr, &apiErr) || apiErr.Code != errCodeReleaseNotHealthy {
		t.Fatalf("endpoint-change completion error=%v", completionErr)
	}
	var recoveryStatus, operationStatus string
	if err := db.QueryRow(`SELECT status FROM hosting_runtime_recoveries WHERE id=?`, recovery.JobID).Scan(&recoveryStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations
		WHERE hosting_runtime_recovery_id=?`, recovery.JobID).Scan(&operationStatus); err != nil {
		t.Fatal(err)
	}
	if recoveryStatus != "failed" || operationStatus != "failed" || !fake.currentSuspended {
		t.Fatalf("recovery endpoint swap committed: recovery=%q operation=%q suspended=%v",
			recoveryStatus, operationStatus, fake.currentSuspended)
	}
}

func TestExpiredHostingRuntimeRecoveryLeaseIsReassignedAndFenced(t *testing.T) {
	withTempDB(t)
	withFakeProxy(t)
	_, _, lostRunnerID, job := createAndClaimHostingJob(t, "project_01JRCFENC", "deployment_01JRCFENC")
	releaseDigest := "sha256:" + strings.Repeat("4", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, releaseDigest)
	if err := completeHostingJob(t.Context(), lostRunnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: releaseDigest, ReleaseArtifactDigest: artifactDigest,
		RuntimeEndpoint: healthyHostingEndpointForTest(t), HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
	}); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile("starter")
	recoveryRunnerID := insertHostingRunnerForTest(t, "runtime-fencing-target", hostingWorkloadLimits{
		CPUMillis: limits.CPUMillis * 2, RAMBytes: limits.RAMBytes * 2,
		DiskBytes: limits.DiskBytes * 2, PIDs: limits.PIDs * 2,
	})
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline' WHERE id=?`, lostRunnerID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	first, err := claimHostingRuntimeRecovery(t.Context(), recoveryRunnerID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_runtime_recoveries SET lease_expires_at=?,
		completion_fingerprint='crash-window-fingerprint' WHERE id=?`,
		formatSQLiteTime(time.Now().UTC().Add(-time.Minute)), first.JobID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	second, err := claimHostingRuntimeRecovery(t.Context(), recoveryRunnerID)
	if err != nil {
		t.Fatal(err)
	}
	if second.JobID != first.JobID || second.LeaseGeneration != first.LeaseGeneration+1 || second.LeaseToken == first.LeaseToken {
		t.Fatalf("reassigned recovery first=%+v second=%+v", first, second)
	}
	err = completeHostingRuntimeRecovery(t.Context(), recoveryRunnerID, first.JobID,
		first.LeaseGeneration, first.LeaseToken, hostingCompletionRequest{Status: "failed", FailureCode: "runner_lost"})
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeJobForbidden {
		t.Fatalf("stale recovery completion was not fenced: %v", err)
	}
	failedCompletion := hostingCompletionRequest{Status: "failed", FailureCode: "runtime_start_failed",
		FailureMessage: "retryable start failure"}
	startFailures := make(chan struct{})
	failureResults := make(chan error, 8)
	for range 8 {
		go func() {
			<-startFailures
			failureResults <- completeHostingRuntimeRecovery(context.Background(), recoveryRunnerID,
				second.JobID, second.LeaseGeneration, second.LeaseToken, failedCompletion)
		}()
	}
	close(startFailures)
	for range 8 {
		if err := <-failureResults; err != nil {
			t.Fatalf("concurrent retryable recovery failure: %v", err)
		}
	}
	if err := completeHostingRuntimeRecovery(t.Context(), recoveryRunnerID, second.JobID,
		second.LeaseGeneration, second.LeaseToken, failedCompletion); err != nil {
		t.Fatalf("replay accepted retryable recovery failure: %v", err)
	}
	conflictingFailure := failedCompletion
	conflictingFailure.FailureCode = "health_check_failed"
	err = completeHostingRuntimeRecovery(t.Context(), recoveryRunnerID, second.JobID,
		second.LeaseGeneration, second.LeaseToken, conflictingFailure)
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeIdempotencyConflict {
		t.Fatalf("conflicting accepted retryable failure was not fenced: %v", err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	third, err := claimHostingRuntimeRecovery(t.Context(), recoveryRunnerID)
	if err != nil {
		t.Fatalf("claim recovery after failed generation: %v", err)
	}
	if third.LeaseGeneration != second.LeaseGeneration+1 {
		t.Fatalf("retry generation=%d want=%d", third.LeaseGeneration, second.LeaseGeneration+1)
	}
	thirdEndpoint := healthyHostingEndpointForTest(t)
	observeHostingRecoveryForTest(t, recoveryRunnerID, third, releaseDigest, thirdEndpoint)
	if err := completeHostingRuntimeRecovery(t.Context(), recoveryRunnerID, third.JobID,
		third.LeaseGeneration, third.LeaseToken, hostingCompletionRequest{Status: "success",
			ReleaseDigest: releaseDigest, ReleaseArtifactDigest: artifactDigest,
			RuntimeEndpoint: thirdEndpoint,
			HealthEvidence:  map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatalf("complete recovery after failed generation: %v", err)
	}
}

func TestHostingRuntimeRecoveryHonorsDurableProjectSuspension(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, lostRunnerID, job := createAndClaimHostingJob(t, "project_01JRCSUSP", "deployment_01JRCSUSP")
	releaseDigest := "sha256:" + strings.Repeat("3", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, releaseDigest)
	if err := completeHostingJob(t.Context(), lostRunnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: releaseDigest, ReleaseArtifactDigest: artifactDigest,
		RuntimeEndpoint: healthyHostingEndpointForTest(t), HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
	}); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile("starter")
	capacity := hostingWorkloadLimits{CPUMillis: limits.CPUMillis * 2, RAMBytes: limits.RAMBytes * 2,
		DiskBytes: limits.DiskBytes * 2, PIDs: limits.PIDs * 2}
	recoveryRunnerID := insertHostingRunnerForTest(t, "runtime-suspend-target", capacity)
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
	restored := hostingCompletionRequest{Status: "success", ReleaseDigest: releaseDigest,
		ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: healthyHostingEndpointForTest(t),
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}
	observeHostingRecoveryForTest(t, recoveryRunnerID, recovery, releaseDigest, restored.RuntimeEndpoint)
	fake.activateStarted = make(chan struct{}, 1)
	fake.activateContinue = make(chan struct{})
	completionResult := make(chan error, 1)
	go func() {
		completionResult <- completeHostingRuntimeRecovery(context.Background(), recoveryRunnerID, recovery.JobID,
			recovery.LeaseGeneration, recovery.LeaseToken, restored)
	}()
	<-fake.activateStarted
	operator, err := createServiceToken("runtime-recovery-suspender", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID,
		"suspended", "suspend_runtime_recovery_01"); err != nil {
		t.Fatal(err)
	}
	var cancelRequested sql.NullString
	if err := db.QueryRow(`SELECT cancel_requested_at FROM hosting_runtime_recoveries WHERE id=?`, recovery.JobID).Scan(&cancelRequested); err != nil || !cancelRequested.Valid {
		t.Fatalf("recovery cancellation intent valid=%v err=%v", cancelRequested.Valid, err)
	}
	if err := reconcileHostingRecoveryProxyOperations(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID,
		"active", "resume_runtime_recovery_01"); err != nil {
		t.Fatal(err)
	}
	close(fake.activateContinue)
	var fenced *hostingAPIError
	if err := <-completionResult; !errors.As(err, &fenced) || fenced.Code != errCodeProjectSuspended {
		t.Fatalf("in-flight activation was not fenced and compensated: %v", err)
	}
	if err := completeHostingRuntimeRecovery(t.Context(), recoveryRunnerID, recovery.JobID,
		recovery.LeaseGeneration, recovery.LeaseToken, restored); err != nil {
		t.Fatalf("terminal recovery replay after cancellation: %v", err)
	}
	var status string
	var operationStatus string
	var freeCPU int64
	if err := db.QueryRow(`SELECT status FROM hosting_runtime_recoveries WHERE id=?`, recovery.JobID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, recoveryRunnerID).Scan(&freeCPU); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations WHERE hosting_runtime_recovery_id=?`,
		recovery.JobID).Scan(&operationStatus); err != nil {
		t.Fatal(err)
	}
	if status != "cancelled" || operationStatus != "failed" || freeCPU != capacity.CPUMillis {
		t.Fatalf("cancelled recovery status=%q operation=%q free CPU=%d", status, operationStatus, freeCPU)
	}
	if err := reconcileHostingDesiredStateOperations(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(fake.activations) != 2 || len(fake.suspensions) != 2 ||
		!fake.suspensions[0].Suspended || !fake.suspensions[1].Suspended ||
		fake.suspensions[1].RouteGeneration <= fake.suspensions[0].RouteGeneration || !fake.currentSuspended {
		t.Fatalf("late recovery activation=%#v suspensions=%#v", fake.activations, fake.suspensions)
	}
}

func TestHostingRuntimeLossWithSecretReferencesIssuesFreshRecoveryIdentity(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, lostRunnerID, job := createAndClaimHostingJob(t, "project_01JRECSEC", "deployment_01JRECSEC")
	appConfig.HostingWorkloadIdentitySecret = strings.Repeat("identity-secret-", 3)
	releaseDigest := "sha256:" + strings.Repeat("c", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, releaseDigest)
	if err := completeHostingJob(t.Context(), lostRunnerID, job.JobID, job.LeaseGeneration, job.LeaseToken,
		hostingCompletionRequest{Status: "success", ReleaseDigest: releaseDigest,
			ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: healthyHostingEndpointForTest(t),
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	secretReference := "opaque-reference-that-must-not-leak"
	encodedSecrets, err := json.Marshal([]HostingSecretReference{{Provider: "control-plane",
		Reference: secretReference, Name: "DATABASE_URL", ExpiresAt: time.Now().UTC().Add(time.Minute)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_jobs SET secret_refs_json=? WHERE id=?`, string(encodedSecrets), job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=? WHERE id=?`,
		formatSQLiteTime(time.Now().UTC().Add(-2*hostingRunnerStaleAfter)), lostRunnerID); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	recoveryRunnerID := insertHostingRunnerForTest(t, "secret-recovery-runner", limits)
	if _, err := db.Exec(`UPDATE hosting_runners SET operation_capabilities_json='["build","restore","runtime-inventory-v1"]' WHERE id=?`, recoveryRunnerID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var incompatibleAssignment sql.NullInt64
	if err := db.QueryRow(`SELECT hosting_runner_id FROM hosting_runtime_recoveries LIMIT 1`).Scan(&incompatibleAssignment); err != nil {
		t.Fatal(err)
	}
	if incompatibleAssignment.Valid {
		t.Fatalf("secret recovery assigned to legacy runner %d", incompatibleAssignment.Int64)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET operation_capabilities_json='["build","restore","runtime-secrets-v1","runtime-inventory-v1"]' WHERE id=?`, recoveryRunnerID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var releaseStatus string
	if err := db.QueryRow(`SELECT status, runtime_runner_id FROM hosting_releases WHERE release_digest=?`,
		releaseDigest).Scan(&releaseStatus, new(sql.NullInt64)); err != nil {
		t.Fatal(err)
	}
	var recoveryCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_runtime_recoveries`).Scan(&recoveryCount); err != nil {
		t.Fatal(err)
	}
	recovery, err := claimHostingRuntimeRecovery(t.Context(), recoveryRunnerID)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/hosting-agent/v1/recoveries/%d/workload-identity", recovery.JobID), nil)
	request.Header.Set("X-Deployer-Lease-Generation", fmt.Sprint(recovery.LeaseGeneration))
	request.Header.Set("X-Deployer-Lease-Token", recovery.LeaseToken)
	recorder := httptest.NewRecorder()
	handleHostingRecoveryWorkloadIdentity(recorder, request, &HostingRunner{ID: recoveryRunnerID}, recovery.JobID)
	if recorder.Code != http.StatusOK {
		t.Fatalf("identity response=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var identity hostingWorkloadIdentityResponse
	if err := json.NewDecoder(recorder.Body).Decode(&identity); err != nil {
		t.Fatal(err)
	}
	claims, err := parseHostingWorkloadIdentityForTest(identity.Token, appConfig.HostingWorkloadIdentitySecret)
	if err != nil {
		t.Fatal(err)
	}
	if releaseStatus != "active" || recoveryCount != 1 || claims.Operation != "restore" ||
		claims.RunnerID != recoveryRunnerID || claims.WorkID != recovery.JobID ||
		strings.Contains(identity.Token, secretReference) || len(fake.suspensions) != 0 {
		t.Fatalf("release=%q recoveries=%d claims=%+v suspensions=%#v", releaseStatus, recoveryCount, claims, fake.suspensions)
	}
	if err := completeHostingRuntimeRecovery(t.Context(), recoveryRunnerID, recovery.JobID,
		recovery.LeaseGeneration, recovery.LeaseToken, hostingCompletionRequest{Status: "failed",
			FailureCode: "secret_reference_unavailable", FailureMessage: "broker unavailable"}); err != nil {
		t.Fatalf("commit secret redemption failure: %v", err)
	}
	var recoveryStatus, failureCode string
	if err := db.QueryRow(`SELECT status FROM hosting_runtime_recoveries WHERE id=?`, recovery.JobID).Scan(&recoveryStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT failure_code FROM hosting_events WHERE event_type='runtime_recovery_queued' ORDER BY id DESC LIMIT 1`).Scan(&failureCode); err != nil {
		t.Fatal(err)
	}
	if recoveryStatus != "queued" || failureCode != "secret_reference_unavailable" {
		t.Fatalf("secret failure status=%q code=%q", recoveryStatus, failureCode)
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

func TestHostingReleaseArtifactAttachmentIsCASUnderConcurrency(t *testing.T) {
	withTempDB(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JARTCAS", "deployment_01JARTCAS")
	type candidate struct {
		release, artifact, path string
		size                    int64
	}
	candidates := []candidate{
		{release: "sha256:" + strings.Repeat("a", 64), artifact: "sha256:" + strings.Repeat("b", 64), path: managedArtifactPath("hosting-release-" + strings.Repeat("b", 64) + ".tar"), size: 101},
		{release: "sha256:" + strings.Repeat("c", 64), artifact: "sha256:" + strings.Repeat("d", 64), path: managedArtifactPath("hosting-release-" + strings.Repeat("d", 64) + ".tar"), size: 202},
	}
	type outcome struct {
		candidate candidate
		err       error
	}
	const workers = 32
	start := make(chan struct{})
	results := make(chan outcome, workers)
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		input := candidates[i%len(candidates)]
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			err := attachHostingReleaseArtifact(context.Background(), runnerID, job.JobID,
				job.LeaseGeneration, job.LeaseToken, input.release, input.artifact, input.path, input.size)
			results <- outcome{candidate: input, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	var stored candidate
	if err := db.QueryRow(`SELECT release_upload_release_digest, release_upload_digest,
		release_upload_path, release_upload_size FROM hosting_jobs WHERE id=?`, job.JobID).Scan(
		&stored.release, &stored.artifact, &stored.path, &stored.size); err != nil {
		t.Fatal(err)
	}
	for result := range results {
		if result.candidate == stored {
			if result.err != nil {
				t.Fatalf("identical CAS replay failed: %v", result.err)
			}
			continue
		}
		var apiErr *hostingAPIError
		if !errors.As(result.err, &apiErr) || apiErr.Code != errCodeConflict {
			t.Fatalf("losing CAS result=%+v err=%v", result.candidate, result.err)
		}
	}
}

func TestContentIdenticalRedeploymentCreatesDistinctReleaseInstances(t *testing.T) {
	withTempDB(t)
	withFakeProxy(t)
	healthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	t.Cleanup(healthServer.Close)
	project, token, runnerID, firstJob := createAndClaimHostingJob(t, "project_01JSAMEIM", "deployment_01JSAME01")
	digest := "sha256:" + strings.Repeat("8", 64)
	firstArtifact := attachTestReleaseArtifact(t, firstJob.JobID, digest)
	observeHostingJobEndpointForTest(t, firstJob.JobID, healthServer.URL)
	if err := completeHostingJob(t.Context(), runnerID, firstJob.JobID, firstJob.LeaseGeneration, firstJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: digest, ReleaseArtifactDigest: firstArtifact,
		RuntimeEndpoint: healthServer.URL, HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
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
	advanceHostingJobToHealthCheckingForTest(t, runnerID, secondJob)
	secondArtifact := attachTestReleaseArtifact(t, secondJob.JobID, digest)
	observeHostingJobEndpointForTest(t, secondJob.JobID, healthServer.URL)
	if secondArtifact != firstArtifact {
		t.Fatal("identical image content produced different fixture artifacts")
	}
	if err := completeHostingJob(t.Context(), runnerID, secondJob.JobID, secondJob.LeaseGeneration, secondJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: digest, ReleaseArtifactDigest: secondArtifact,
		RuntimeEndpoint: healthServer.URL, HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
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
	if _, _, err := rollbackHostingRelease(t.Context(), token, project.ExternalProjectID,
		"deployment_01JSAME01", digest, "rollback_01JSAME"); err != nil {
		t.Fatalf("rollback exact content-identical release: %v", err)
	}
	var activeDeployment string
	if err := db.QueryRow(`SELECT deployment.external_deployment_id FROM hosting_releases release
		JOIN hosting_deployments deployment ON deployment.id=release.hosting_deployment_id
		WHERE release.hosting_project_id=? AND release.status='active'`, project.ID).Scan(&activeDeployment); err != nil {
		t.Fatal(err)
	}
	if activeDeployment != "deployment_01JSAME01" {
		t.Fatalf("active content-identical release deployment=%q", activeDeployment)
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
	_, _, runnerID, job := createAndClaimHostingJobAtFetching(t, "project_01JPHASES", "deployment_01JPHASES")
	if recorder := reportHostingJobPhaseForTest(t, runnerID, job, hostingPhaseBuilding); recorder.Code != http.StatusNoContent {
		t.Fatalf("building phase status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if recorder := reportHostingJobPhaseForTest(t, runnerID, job, hostingPhaseFetching); recorder.Code != http.StatusConflict {
		t.Fatalf("regressed phase status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestHostingSuccessfulCompletionRequiresHealthCheckingPhase(t *testing.T) {
	for _, test := range []struct {
		name           string
		advanceThrough []string
		wantCode       string
	}{
		{name: "fetching_source", wantCode: errCodeConflict},
		{name: "building", advanceThrough: []string{hostingPhaseBuilding}, wantCode: errCodeConflict},
		{name: "starting_candidate", advanceThrough: []string{hostingPhaseBuilding, hostingPhaseStarting}, wantCode: errCodeConflict},
		{name: "health_checking", advanceThrough: []string{hostingPhaseBuilding, hostingPhaseStarting, hostingPhaseHealth}},
	} {
		t.Run(test.name, func(t *testing.T) {
			withTempDB(t)
			withFakeProxy(t)
			_, _, runnerID, job := createAndClaimHostingJobAtFetching(t,
				"project_01JPHASEOK"+strings.ToUpper(test.name[:1]),
				"deployment_01JPHASEOK"+strings.ToUpper(test.name[:1]))
			for _, phase := range test.advanceThrough {
				if recorder := reportHostingJobPhaseForTest(t, runnerID, job, phase); recorder.Code != http.StatusNoContent {
					t.Fatalf("advance to %s: status=%d body=%s", phase, recorder.Code, recorder.Body.String())
				}
			}
			digest := "sha256:" + strings.Repeat("a", 64)
			err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
				job.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
					ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, digest),
					RuntimeEndpoint:       healthyHostingEndpointForTest(t),
					HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}})
			if test.wantCode == "" {
				if err != nil {
					t.Fatalf("health-checking completion: %v", err)
				}
				return
			}
			var apiErr *hostingAPIError
			if !errors.As(err, &apiErr) || apiErr.Code != test.wantCode {
				t.Fatalf("completion from %s err=%v, want %s", test.name, err, test.wantCode)
			}
		})
	}
}

func TestHostingCandidateRequiresControlPlaneHealth(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JNOSESS", "deployment_01JNOSESS")
	unhealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(unhealthy.Close)
	oldTimeout := hostingActivationReconcileHealthTimeout
	hostingActivationReconcileHealthTimeout = 100 * time.Millisecond
	t.Cleanup(func() { hostingActivationReconcileHealthTimeout = oldTimeout })
	digest := "sha256:" + strings.Repeat("b", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	observeHostingJobEndpointForTest(t, job.JobID, unhealthy.URL)
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: artifactDigest,
			RuntimeEndpoint:       unhealthy.URL,
			HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatalf("persist no-session health failure: %v", err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JNOSESS")
	if err != nil {
		t.Fatal(err)
	}
	if deployment.Status != hostingStatusFailed || deployment.FailureCode != "health_check_failed" {
		t.Fatalf("unhealthy no-session deployment=%+v", deployment)
	}
	if len(fake.activations) != 0 {
		t.Fatalf("unhealthy candidate reached proxy: %+v", fake.activations)
	}
	var operationStatus string
	if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations
		WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)
		AND operation_type='activate'`, job.JobID).Scan(&operationStatus); err != nil {
		t.Fatal(err)
	}
	if operationStatus != "failed" {
		t.Fatalf("terminal health failure left activation intent %q", operationStatus)
	}
}

func TestHostingCandidateRequiresAuthoritativeInventorySession(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JREQSESS", "deployment_01JREQSESS")
	digest := "sha256:" + strings.Repeat("c", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	if _, err := db.Exec(`UPDATE hosting_runners SET active_session_id='' WHERE id=?`, runnerID); err != nil {
		t.Fatal(err)
	}
	err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: artifactDigest,
			RuntimeEndpoint:       healthyHostingEndpointForTest(t),
			HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}})
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeReleaseNotHealthy {
		t.Fatalf("completion without inventory session err=%v", err)
	}
	if len(fake.activations) != 0 {
		t.Fatalf("candidate without inventory identity reached proxy: %+v", fake.activations)
	}
	var operations int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)`,
		job.JobID).Scan(&operations); err != nil || operations != 0 {
		t.Fatalf("unbound candidate operations=%d err=%v", operations, err)
	}
}

func TestHostingCandidateEndpointMustMatchAuthoritativeInventory(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JEPBIND", "deployment_01JEPBIND")
	digest := "sha256:" + strings.Repeat("2", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	observedEndpoint := healthyHostingEndpointForTest(t)
	otherEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(otherEndpoint.Close)
	err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: otherEndpoint.URL,
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}})
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeReleaseNotHealthy {
		t.Fatalf("completion endpoint mismatch observed=%q supplied=%q err=%v",
			observedEndpoint, otherEndpoint.URL, err)
	}
	if len(fake.activations) != 0 {
		t.Fatalf("endpoint-mismatched candidate reached proxy: %+v", fake.activations)
	}
}

func TestHostingHealthCheckRejectsRedirects(t *testing.T) {
	var redirectedRequests atomic.Int64
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectedRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(redirectTarget.Close)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectTarget.URL, http.StatusFound)
	}))
	t.Cleanup(redirector.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, _, err := checkHostingCandidateHealth(ctx, redirector.URL); err == nil {
		t.Fatal("redirect response was accepted as healthy")
	}
	if got := redirectedRequests.Load(); got != 0 {
		t.Fatalf("health checker followed workload redirect %d times", got)
	}
}

func TestHostingRecoveryCandidateRequiresAuthoritativeInventorySession(t *testing.T) {
	var healthRequests atomic.Int64
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		healthRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(health.Close)
	now := time.Now().UTC()
	state := &hostingRuntimeRecoveryState{ID: 42, LeaseGeneration: 3, RunnerStatus: "online",
		RunnerLastSeen: sql.NullString{String: formatSQLiteTime(now), Valid: true},
		ReleaseDigest:  "sha256:" + strings.Repeat("f", 64)}
	if err := verifyHostingRecoveryCandidate(t.Context(), state, health.URL); err == nil {
		t.Fatal("recovery without an inventory session was accepted")
	}
	if healthRequests.Load() != 0 {
		t.Fatal("unbound recovery reached the HTTP health gate")
	}
	state.RunnerActiveSession = strings.Repeat("a", 48)
	state.RuntimeObservedAt = sql.NullString{String: formatSQLiteTime(now), Valid: true}
	state.RuntimeObservedSession = state.RunnerActiveSession
	state.RuntimeObservedDigest = state.ReleaseDigest
	state.RuntimeObservedInstance = "restore-42-3"
	state.RuntimeObservedEndpoint = health.URL
	if err := verifyHostingRecoveryCandidate(t.Context(), state, health.URL); err != nil {
		t.Fatalf("observed recovery candidate: %v", err)
	}
	if healthRequests.Load() != 1 {
		t.Fatalf("observed recovery health requests=%d", healthRequests.Load())
	}
}

func TestHostingSuccessfulCompletionExactRetryResumesStagedActivation(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JCOMPRET", "deployment_01JCOMPRET")
	digest := "sha256:" + strings.Repeat("d", 64)
	completion := hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
		ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, digest),
		RuntimeEndpoint:       healthyHostingEndpointForTest(t),
		HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}
	fake.fail = true
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, completion); err == nil {
		t.Fatal("transient adapter failure unexpectedly completed")
	}
	fake.fail = false
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, completion); err != nil {
		t.Fatalf("exact staged completion retry: %v", err)
	}
	var operations int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)
		AND operation_type='activate'`, job.JobID).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if operations != 1 || len(fake.activations) != 2 {
		t.Fatalf("operations=%d adapter attempts=%d", operations, len(fake.activations))
	}
}

func TestHostingConcurrentIdenticalSuccessCompletionsShareActivationIntent(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JCOMPDUP", "deployment_01JCOMPDUP")
	digest := "sha256:" + strings.Repeat("e", 64)
	completion := hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
		ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, digest),
		RuntimeEndpoint:       healthyHostingEndpointForTest(t),
		HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}
	fake.activateStarted = make(chan struct{}, 2)
	fake.activateContinue = make(chan struct{})
	results := make(chan error, 2)
	go func() {
		results <- completeHostingJob(context.Background(), runnerID, job.JobID,
			job.LeaseGeneration, job.LeaseToken, completion)
	}()
	select {
	case <-fake.activateStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("first completion did not reach activation")
	}
	go func() {
		results <- completeHostingJob(context.Background(), runnerID, job.JobID,
			job.LeaseGeneration, job.LeaseToken, completion)
	}()
	select {
	case <-fake.activateStarted:
	case <-time.After(3 * time.Second):
		close(fake.activateContinue)
		t.Fatal("identical completion did not adopt staged activation")
	}
	close(fake.activateContinue)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("identical completion result: %v", err)
		}
	}
	var operations int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)
		AND operation_type='activate'`, job.JobID).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if operations != 1 {
		t.Fatalf("durable activation intents=%d", operations)
	}
}

func TestHostingHealthFailureFencesConcurrentAmbiguousActivation(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JHLTRACE", "deployment_01JHLTRACE")
	var healthRequests atomic.Int64
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if healthRequests.Add(1) == 1 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(health.Close)
	oldTimeout := hostingActivationReconcileHealthTimeout
	hostingActivationReconcileHealthTimeout = 100 * time.Millisecond
	t.Cleanup(func() { hostingActivationReconcileHealthTimeout = oldTimeout })
	digest := "sha256:" + strings.Repeat("1", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	observeHostingJobEndpointForTest(t, job.JobID, health.URL)
	completion := hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
		ReleaseArtifactDigest: artifactDigest,
		RuntimeEndpoint:       health.URL,
		HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}
	started := make(chan struct{}, 1)
	continued := make(chan struct{})
	fake.activateStarted = started
	fake.activateContinue = continued
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- completeHostingJob(context.Background(), runnerID, job.JobID,
			job.LeaseGeneration, job.LeaseToken, completion)
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first completion did not dispatch activation")
	}
	fake.mu.Lock()
	fake.activateStarted = nil
	fake.activateContinue = nil
	fake.mu.Unlock()
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, completion); err != nil {
		close(continued)
		t.Fatalf("concurrent health failure did not converge: %v", err)
	}
	close(continued)
	if err := <-firstResult; err != nil {
		t.Fatalf("superseded activation completion: %v", err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JHLTRACE")
	if err != nil {
		t.Fatal(err)
	}
	if deployment.Status != hostingStatusFailed || deployment.FailureCode != "health_check_failed" {
		t.Fatalf("deployment after mixed health race=%+v", deployment)
	}
	fake.mu.Lock()
	currentSuspended := fake.currentSuspended
	fake.mu.Unlock()
	if !currentSuspended {
		t.Fatal("ambiguous candidate activation was not fenced by suspension")
	}
	var failedActivations, committedCompensations int
	if err := db.QueryRow(`SELECT
		SUM(CASE WHEN operation_type='activate' AND status='failed' THEN 1 ELSE 0 END),
		SUM(CASE WHEN operation_type='compensate' AND status='committed' THEN 1 ELSE 0 END)
		FROM hosting_proxy_operations
		WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)`,
		job.JobID).Scan(&failedActivations, &committedCompensations); err != nil {
		t.Fatal(err)
	}
	if failedActivations != 1 || committedCompensations != 1 {
		t.Fatalf("activation failures=%d committed compensations=%d", failedActivations, committedCompensations)
	}
}

func TestHostingRestartReconcilesPendingActivationAndPreservesDurableState(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	healthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer healthServer.Close()
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JREOPEN", "deployment_01JREOPEN")
	digest := "sha256:" + strings.Repeat("9", 64)
	releaseArtifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	observeHostingJobEndpointForTest(t, job.JobID, healthServer.URL)
	state, err := getHostingCompletionState(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := stageHealthyHostingRelease(t.Context(), state, hostingCompletionRequest{
		Status: "success", ReleaseDigest: digest, ReleaseArtifactDigest: releaseArtifactDigest, RuntimeEndpoint: healthServer.URL,
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

func TestHostingRestartRefusesDeadPendingCandidateActivation(t *testing.T) {
	for _, test := range []struct {
		name          string
		offlineRunner bool
	}{
		{name: "offline runner", offlineRunner: true},
		{name: "unhealthy endpoint"},
	} {
		t.Run(test.name, func(t *testing.T) {
			withTempDB(t)
			fake := withFakeProxy(t)
			healthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer healthServer.Close()
			_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JDEADPX", "deployment_01JDEADPX")
			digest := "sha256:" + strings.Repeat("3", 64)
			artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
			observeHostingJobEndpointForTest(t, job.JobID, healthServer.URL)
			state, err := getHostingCompletionState(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken)
			if err != nil {
				t.Fatal(err)
			}
			if err := stageHealthyHostingRelease(t.Context(), state, hostingCompletionRequest{Status: "success",
				ReleaseDigest: digest, ReleaseArtifactDigest: artifactDigest,
				RuntimeEndpoint: healthServer.URL,
				HealthEvidence:  map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
				t.Fatal(err)
			}
			if test.offlineRunner {
				if _, err := db.Exec(`UPDATE hosting_runners SET status='offline' WHERE id=?`, runnerID); err != nil {
					t.Fatal(err)
				}
			}
			oldTimeout := hostingActivationReconcileHealthTimeout
			hostingActivationReconcileHealthTimeout = 100 * time.Millisecond
			t.Cleanup(func() { hostingActivationReconcileHealthTimeout = oldTimeout })
			if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JDEADPX")
			if err != nil {
				t.Fatal(err)
			}
			if deployment.Status != hostingStatusFailed || deployment.FailureCode != "health_check_failed" || len(fake.activations) != 0 {
				t.Fatalf("dead candidate deployment=%+v activations=%#v", deployment, fake.activations)
			}
		})
	}
}

func TestHostingActivationRejectsEndpointChangedDuringProxyDispatch(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, runnerID, job := createAndClaimHostingJob(t,
		"project_01JENDPOINTSWAP", "deployment_01JENDPOINTSWAP")
	digest := "sha256:" + strings.Repeat("4", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	endpointA := healthyHostingEndpointForTest(t)
	endpointBServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(endpointBServer.Close)
	fake.activateStarted = make(chan struct{}, 1)
	fake.activateContinue = make(chan struct{})
	completionResult := make(chan error, 1)
	go func() {
		completionResult <- completeHostingJob(context.Background(), runnerID, job.JobID,
			job.LeaseGeneration, job.LeaseToken, hostingCompletionRequest{Status: "success",
				ReleaseDigest: digest, ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: endpointA,
				HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}})
	}()
	<-fake.activateStarted
	var sessionID string
	var free capacityDTO
	if err := db.QueryRow(`SELECT active_session_id, capacity_cpu_millis, capacity_ram_bytes,
		capacity_disk_bytes, capacity_pids FROM hosting_runners WHERE id=?`, runnerID).Scan(
		&sessionID, &free.CPUMillis, &free.RAMBytes, &free.DiskBytes, &free.PIDs); err != nil {
		t.Fatal(err)
	}
	inventory := []hostingObservedRuntime{{ReleaseDigest: digest,
		ExternalProjectID: project.ExternalProjectID, ExternalDeploymentID: "deployment_01JENDPOINTSWAP",
		RuntimeInstanceID: fmt.Sprintf("build-%d-%d", job.JobID, job.LeaseGeneration),
		RuntimeEndpoint:   endpointBServer.URL, State: "running"}}
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, sessionID, 2, inventory); recorder.Code != http.StatusOK {
		t.Fatalf("endpoint-change heartbeat status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	close(fake.activateContinue)
	if err := <-completionResult; err != nil {
		t.Fatal(err)
	}
	var deploymentStatus, failureCode, operationStatus string
	if err := db.QueryRow(`SELECT status, failure_code FROM hosting_deployments
		WHERE external_deployment_id=?`, "deployment_01JENDPOINTSWAP").Scan(&deploymentStatus, &failureCode); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations
		WHERE hosting_deployment_id=(SELECT id FROM hosting_deployments WHERE external_deployment_id=?)`,
		"deployment_01JENDPOINTSWAP").Scan(&operationStatus); err != nil {
		t.Fatal(err)
	}
	if deploymentStatus != hostingStatusFailed || failureCode != "health_check_failed" ||
		operationStatus != "failed" || !fake.currentSuspended {
		t.Fatalf("endpoint swap committed: deployment=%q failure=%q operation=%q suspended=%v",
			deploymentStatus, failureCode, operationStatus, fake.currentSuspended)
	}
}

func TestHostingRestartCompensatesToExactPreviousRuntimeEndpoint(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	firstEndpoint := healthyHostingEndpointForTest(t)
	secondEndpoint := healthyHostingEndpointForTest(t)
	project, token, runnerID, firstJob := createAndClaimHostingJob(t,
		"project_01JEXACTEP", "deployment_01JEXACTE1")
	sharedDigest := "sha256:" + strings.Repeat("6", 64)
	firstArtifact := attachTestReleaseArtifact(t, firstJob.JobID, sharedDigest)
	observeHostingJobEndpointForTest(t, firstJob.JobID, firstEndpoint)
	if err := completeHostingJob(t.Context(), runnerID, firstJob.JobID, firstJob.LeaseGeneration,
		firstJob.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: sharedDigest,
			ReleaseArtifactDigest: firstArtifact,
			RuntimeEndpoint:       firstEndpoint,
			HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	request := validHostingDeploymentRequest("deployment_01JEXACTE2")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID,
		"create_01JEXACTE2", request); err != nil {
		t.Fatal(err)
	}
	secondJob, err := claimHostingJob(t.Context(), runnerID)
	if err != nil {
		t.Fatal(err)
	}
	advanceHostingJobToHealthCheckingForTest(t, runnerID, secondJob)
	secondArtifact := attachTestReleaseArtifact(t, secondJob.JobID, sharedDigest)
	observeHostingJobEndpointForTest(t, secondJob.JobID, secondEndpoint)
	if err := completeHostingJob(t.Context(), runnerID, secondJob.JobID, secondJob.LeaseGeneration,
		secondJob.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: sharedDigest,
			ReleaseArtifactDigest: secondArtifact,
			RuntimeEndpoint:       secondEndpoint,
			HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	request = validHostingDeploymentRequest("deployment_01JEXACTE3")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID,
		"create_01JEXACTE3", request); err != nil {
		t.Fatal(err)
	}
	candidateJob, err := claimHostingJob(t.Context(), runnerID)
	if err != nil {
		t.Fatal(err)
	}
	advanceHostingJobToHealthCheckingForTest(t, runnerID, candidateJob)
	candidateDigest := "sha256:" + strings.Repeat("7", 64)
	candidateArtifact := attachTestReleaseArtifact(t, candidateJob.JobID, candidateDigest)
	unhealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(unhealthy.Close)
	observeHostingJobEndpointForTest(t, candidateJob.JobID, unhealthy.URL)
	state, err := getHostingCompletionState(t.Context(), runnerID, candidateJob.JobID,
		candidateJob.LeaseGeneration, candidateJob.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := stageHealthyHostingRelease(t.Context(), state, hostingCompletionRequest{Status: "success",
		ReleaseDigest: candidateDigest, ReleaseArtifactDigest: candidateArtifact,
		RuntimeEndpoint: unhealthy.URL,
		HealthEvidence:  map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	var persistedPreviousEndpoint string
	if err := db.QueryRow(`SELECT expected_previous_runtime_endpoint FROM hosting_proxy_operations
		WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)
		AND operation_type='activate'`, candidateJob.JobID).Scan(&persistedPreviousEndpoint); err != nil {
		t.Fatal(err)
	}
	if persistedPreviousEndpoint != secondEndpoint {
		t.Fatalf("persisted previous endpoint=%q want %q", persistedPreviousEndpoint, secondEndpoint)
	}
	if _, err := db.Exec(`UPDATE hosting_proxy_operations SET expected_previous_runtime_endpoint=''
		WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)
		AND operation_type='activate'`, candidateJob.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM schema_migrations WHERE id='039_hosting_activation_previous_endpoint'`); err != nil {
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
	if err := db.QueryRow(`SELECT expected_previous_runtime_endpoint FROM hosting_proxy_operations
		WHERE hosting_deployment_id=(SELECT hosting_deployment_id FROM hosting_jobs WHERE id=?)
		AND operation_type='activate'`, candidateJob.JobID).Scan(&persistedPreviousEndpoint); err != nil {
		t.Fatal(err)
	}
	if persistedPreviousEndpoint != secondEndpoint {
		t.Fatalf("backfilled previous endpoint=%q want %q", persistedPreviousEndpoint, secondEndpoint)
	}
	oldTimeout := hostingActivationReconcileHealthTimeout
	hostingActivationReconcileHealthTimeout = 100 * time.Millisecond
	t.Cleanup(func() { hostingActivationReconcileHealthTimeout = oldTimeout })
	initialActivations := len(fake.activations)
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if len(fake.activations) != initialActivations+1 || fake.activations[len(fake.activations)-1].RuntimeEndpoint != secondEndpoint {
		t.Fatalf("restart compensation activations=%+v want endpoint %q", fake.activations, secondEndpoint)
	}
}

func TestHealthFailurePreservesPreviousActiveRelease(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, token, runnerID, firstJob := createAndClaimHostingJob(t, "project_01JHEALTH", "deployment_01JGOOD")
	firstDigest := "sha256:" + strings.Repeat("e", 64)
	if err := completeHostingJob(t.Context(), runnerID, firstJob.JobID, firstJob.LeaseGeneration, firstJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: firstDigest, ReleaseArtifactDigest: attachTestReleaseArtifact(t, firstJob.JobID, firstDigest), RuntimeEndpoint: healthyHostingEndpointForTest(t),
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
	advanceHostingJobToHealthCheckingForTest(t, runnerID, secondJob)
	secondDigest := "sha256:" + strings.Repeat("f", 64)
	if err := completeHostingJob(t.Context(), runnerID, secondJob.JobID, secondJob.LeaseGeneration, secondJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: secondDigest, ReleaseArtifactDigest: attachTestReleaseArtifact(t, secondJob.JobID, secondDigest), RuntimeEndpoint: healthyHostingEndpointForTest(t),
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
		Status: "success", ReleaseDigest: "sha256:" + strings.Repeat("a", 64), RuntimeEndpoint: healthyHostingEndpointForTest(t),
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
		Status: "success", ReleaseDigest: firstDigest, ReleaseArtifactDigest: attachTestReleaseArtifact(t, firstJob.JobID, firstDigest), RuntimeEndpoint: healthyHostingEndpointForTest(t),
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
	advanceHostingJobToHealthCheckingForTest(t, runnerID, secondJob)
	fake.reject = true
	secondDigest := "sha256:" + strings.Repeat("2", 64)
	if err := completeHostingJob(t.Context(), runnerID, secondJob.JobID, secondJob.LeaseGeneration, secondJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: secondDigest, ReleaseArtifactDigest: attachTestReleaseArtifact(t, secondJob.JobID, secondDigest), RuntimeEndpoint: healthyHostingEndpointForTest(t),
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
	firstArtifact := attachTestReleaseArtifact(t, firstJob.JobID, firstDigest)
	observeHostingJobEndpointForTest(t, firstJob.JobID, healthServer.URL)
	if err := completeHostingJob(t.Context(), runnerID, firstJob.JobID, firstJob.LeaseGeneration, firstJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: firstDigest, ReleaseArtifactDigest: firstArtifact, RuntimeEndpoint: healthServer.URL,
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
	advanceHostingJobToHealthCheckingForTest(t, runnerID, secondJob)
	secondDigest := "sha256:" + strings.Repeat("4", 64)
	if err := completeHostingJob(t.Context(), runnerID, secondJob.JobID, secondJob.LeaseGeneration, secondJob.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: secondDigest, ReleaseArtifactDigest: attachTestReleaseArtifact(t, secondJob.JobID, secondDigest), RuntimeEndpoint: healthyHostingEndpointForTest(t),
		HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)},
	}); err != nil {
		t.Fatal(err)
	}
	legacyBody, _ := json.Marshal(map[string]string{"release_digest": firstDigest})
	legacyRequest := httptest.NewRequest(http.MethodPost, "/api/internal/v1/projects/"+project.ExternalProjectID+"/rollback", bytes.NewReader(legacyBody))
	legacyRequest.Header.Set("Content-Type", "application/json")
	legacyRequest.Header.Set(idempotencyKeyHeader, "rollback_legacy_digest_only")
	legacyRequest = legacyRequest.WithContext(context.WithValue(legacyRequest.Context(), serviceTokenContextKey{}, token))
	legacyRecorder := httptest.NewRecorder()
	handleInternalRollback(legacyRecorder, legacyRequest, project.ExternalProjectID)
	assertAPIErrorCode(t, legacyRecorder, http.StatusBadRequest, errCodeValidation)
	release, replayed, err := rollbackHostingRelease(t.Context(), token, project.ExternalProjectID,
		"deployment_01JROLLB1", firstDigest, "rollback_01JROLLBK")
	if err != nil || replayed || release.Status != "active" || release.PreviousRelease != secondDigest {
		t.Fatalf("rollback release=%+v replayed=%v err=%v", release, replayed, err)
	}
	if _, replayed, err := rollbackHostingRelease(t.Context(), token, project.ExternalProjectID,
		"deployment_01JROLLB1", firstDigest, "rollback_01JROLLBK"); err != nil || !replayed {
		t.Fatalf("rollback replayed=%v err=%v", replayed, err)
	}
	if len(fake.activations) != 3 || fake.activations[2].ExpectedPreviousReleaseDigest != secondDigest {
		t.Fatalf("proxy activations=%#v", fake.activations)
	}
}

func TestRollbackCompensatesWhenExactRuntimeDisappearsDuringProxyActivation(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	targetHealth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer targetHealth.Close()
	currentHealth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer currentHealth.Close()
	project, token, runnerID, firstJob := createAndClaimHostingJob(t, "project_01JROLLRACE", "deployment_01JROLLR1")
	firstDigest := "sha256:" + strings.Repeat("7", 64)
	firstArtifact := attachTestReleaseArtifact(t, firstJob.JobID, firstDigest)
	observeHostingJobEndpointForTest(t, firstJob.JobID, targetHealth.URL)
	if err := completeHostingJob(t.Context(), runnerID, firstJob.JobID, firstJob.LeaseGeneration, firstJob.LeaseToken,
		hostingCompletionRequest{Status: "success", ReleaseDigest: firstDigest,
			ReleaseArtifactDigest: firstArtifact, RuntimeEndpoint: targetHealth.URL,
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	request := validHostingDeploymentRequest("deployment_01JROLLR2")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JROLLR2", request); err != nil {
		t.Fatal(err)
	}
	secondJob, err := claimHostingJob(t.Context(), runnerID)
	if err != nil {
		t.Fatal(err)
	}
	advanceHostingJobToHealthCheckingForTest(t, runnerID, secondJob)
	secondDigest := "sha256:" + strings.Repeat("8", 64)
	secondArtifact := attachTestReleaseArtifact(t, secondJob.JobID, secondDigest)
	observeHostingJobEndpointForTest(t, secondJob.JobID, currentHealth.URL)
	if err := completeHostingJob(t.Context(), runnerID, secondJob.JobID, secondJob.LeaseGeneration, secondJob.LeaseToken,
		hostingCompletionRequest{Status: "success", ReleaseDigest: secondDigest,
			ReleaseArtifactDigest: secondArtifact, RuntimeEndpoint: currentHealth.URL,
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=NULL WHERE id=?`, runnerID); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	free := capacityDTO{CPUMillis: limits.CPUMillis, RAMBytes: limits.RAMBytes,
		DiskBytes: limits.DiskBytes, PIDs: limits.PIDs}
	sessionID := strings.Repeat("9", 48)
	inventory := []hostingObservedRuntime{
		{ReleaseDigest: firstDigest, ExternalProjectID: project.ExternalProjectID,
			ExternalDeploymentID: "deployment_01JROLLR1",
			RuntimeInstanceID:    fmt.Sprintf("build-%d-%d", firstJob.JobID, firstJob.LeaseGeneration),
			RuntimeEndpoint:      targetHealth.URL, State: "running"},
		{ReleaseDigest: secondDigest, ExternalProjectID: project.ExternalProjectID,
			ExternalDeploymentID: "deployment_01JROLLR2",
			RuntimeInstanceID:    fmt.Sprintf("build-%d-%d", secondJob.JobID, secondJob.LeaseGeneration),
			RuntimeEndpoint:      currentHealth.URL, State: "running"},
	}
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, sessionID, 1, inventory); recorder.Code != http.StatusOK {
		t.Fatalf("rollback inventory session status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	fake.activateStarted = make(chan struct{}, 1)
	fake.activateContinue = make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, _, rollbackErr := rollbackHostingRelease(context.Background(), token, project.ExternalProjectID,
			"deployment_01JROLLR1", firstDigest, "rollback_runtime_race_01")
		result <- rollbackErr
	}()
	<-fake.activateStarted
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, sessionID, 2, []hostingObservedRuntime{}); recorder.Code != http.StatusOK {
		t.Fatalf("rollback loss heartbeat status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	close(fake.activateContinue)
	var fenced *hostingAPIError
	if err := <-result; !errors.As(err, &fenced) || fenced.Code != errCodeReleaseNotHealthy {
		t.Fatalf("rollback race was not fenced: %v", err)
	}
	if len(fake.activations) != 3 || len(fake.suspensions) != 1 || !fake.suspensions[0].Suspended || !fake.currentSuspended {
		t.Fatalf("rollback compensation activations=%+v suspensions=%+v", fake.activations, fake.suspensions)
	}
	var activeDigest string
	if err := db.QueryRow(`SELECT release_digest FROM hosting_releases WHERE hosting_project_id=? AND status='active'`, project.ID).Scan(&activeDigest); err != nil || activeDigest != secondDigest {
		t.Fatalf("active digest=%q err=%v", activeDigest, err)
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
	if _, _, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID, "suspended", "suspend_failure_01"); err != nil {
		t.Fatalf("durable desired state was not accepted during proxy outage: %v", err)
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
	var failedResume, committedCompensation int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='resume' AND status='failed'`,
		project.ID).Scan(&failedResume); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='compensate' AND status='committed'
		  AND desired_state='resume_after_runtime'`, project.ID).Scan(&committedCompensation); err != nil {
		t.Fatal(err)
	}
	if len(fake.suspensions) != 4 || !fake.suspensions[0].Suspended ||
		!fake.suspensions[1].Suspended || !fake.suspensions[2].Suspended ||
		!fake.suspensions[3].Suspended || failedResume != 1 ||
		committedCompensation != 1 || !fake.currentSuspended {
		t.Fatalf("proxy suspension calls=%#v", fake.suspensions)
	}
}

func TestResumeCompensatesRuntimeLossAndRemainsPendingUntilAvailable(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, runnerID, job := createAndClaimHostingJob(t, "project_01JRESUMEC", "deployment_01JRESUMEC")
	digest := "sha256:" + strings.Repeat("a", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	activeEndpoint := healthyHostingEndpointForTest(t)
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken,
		hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: activeEndpoint,
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=NULL WHERE id=?`, runnerID); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	free := capacityDTO{CPUMillis: limits.CPUMillis, RAMBytes: limits.RAMBytes,
		DiskBytes: limits.DiskBytes, PIDs: limits.PIDs}
	sessionID := strings.Repeat("b", 48)
	running := []hostingObservedRuntime{{ReleaseDigest: digest, ExternalProjectID: project.ExternalProjectID,
		ExternalDeploymentID: "deployment_01JRESUMEC",
		RuntimeInstanceID:    fmt.Sprintf("build-%d-%d", job.JobID, job.LeaseGeneration),
		RuntimeEndpoint:      activeEndpoint, State: "running"}}
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, sessionID, 1, running); recorder.Code != http.StatusOK {
		t.Fatalf("resume inventory session status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	operator, err := createServiceToken("resume-race-operator", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID,
		"suspended", "suspend_resume_race_01"); err != nil {
		t.Fatal(err)
	}
	fake.activateApplied = make(chan struct{}, 1)
	fake.activateReturn = make(chan struct{})
	resumeResult := make(chan error, 1)
	go func() {
		_, _, resumeErr := setHostingProjectDesiredState(context.Background(), operator, project.ExternalProjectID,
			"active", "resume_runtime_race_01")
		resumeResult <- resumeErr
	}()
	<-fake.activateApplied
	if recorder := postHostingInventoryHeartbeat(t, runnerID, free, sessionID, 2, []hostingObservedRuntime{}); recorder.Code != http.StatusOK {
		t.Fatalf("resume-loss heartbeat status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	close(fake.activateReturn)
	if err := <-resumeResult; err != nil {
		t.Fatalf("durable resume race result: %v", err)
	}
	if !fake.currentSuspended {
		t.Fatal("runtime loss during resume left routing unsuspended")
	}
	var pendingResume, failedResume, committedCompensation int
	if err := db.QueryRow(`SELECT
		SUM(CASE WHEN operation_type='resume' AND status='pending' THEN 1 ELSE 0 END),
		SUM(CASE WHEN operation_type='resume' AND status='failed' THEN 1 ELSE 0 END),
		SUM(CASE WHEN operation_type='compensate' AND status='committed' THEN 1 ELSE 0 END)
		FROM hosting_proxy_operations WHERE hosting_project_id=?`, project.ID).Scan(
		&pendingResume, &failedResume, &committedCompensation); err != nil {
		t.Fatal(err)
	}
	if pendingResume != 0 || failedResume != 1 || committedCompensation != 1 {
		t.Fatalf("resume operations pending=%d failed=%d compensation=%d",
			pendingResume, failedResume, committedCompensation)
	}
}

func TestAppliedResumeCommitsAfterRestartWithoutRedispatch(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, runnerID, job := createAndClaimHostingJob(t,
		"project_01JRESAPPLIED", "deployment_01JRESAPPLIED")
	digest := "sha256:" + strings.Repeat("4", 64)
	endpoint := healthyHostingEndpointForTest(t)
	observeHostingJobEndpointForTest(t, job.JobID, endpoint)
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, digest),
			RuntimeEndpoint:       endpoint,
			HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	operator, err := createServiceToken("applied-resume-operator", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID,
		"suspended", "applied_resume_suspend_01"); err != nil {
		t.Fatal(err)
	}
	fake.fail = true
	if _, _, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID,
		"active", "applied_resume_active_01"); err != nil {
		t.Fatal(err)
	}
	var operationID string
	var generation int64
	if err := db.QueryRow(`SELECT operation_id, route_generation FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='resume' AND status='pending'`,
		project.ID).Scan(&operationID, &generation); err != nil {
		t.Fatal(err)
	}
	fake.fail = false
	activation, err := fake.Activate(t.Context(), proxyActivationRequest{OperationID: operationID,
		ExternalProjectID: project.ExternalProjectID, ReleaseDigest: digest,
		RuntimeEndpoint: endpoint, RouteGeneration: generation})
	if err != nil {
		t.Fatal(err)
	}
	if err := markHostingProxyOperationApplied(t.Context(), operationID, activation.RouteRevision); err != nil {
		t.Fatal(err)
	}
	beforeRestartActivations := len(fake.activations)
	restartHostingDatabaseForCancellationTest(t)
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations WHERE operation_id=?`,
		operationID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "committed" || len(fake.activations) != beforeRestartActivations ||
		fake.currentEndpoint != endpoint || fake.currentSuspended {
		t.Fatalf("resume status=%s activations=%d/%d endpoint=%q suspended=%v", status,
			len(fake.activations), beforeRestartActivations, fake.currentEndpoint, fake.currentSuspended)
	}
}

func TestResumeAfterSuspensionOutageStaysFencedUntilRuntimeAvailable(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, runnerID, job := createAndClaimHostingJob(t,
		"project_01JRESOUTAGE", "deployment_01JRESOUTAGE")
	digest := "sha256:" + strings.Repeat("5", 64)
	endpoint := healthyHostingEndpointForTest(t)
	observeHostingJobEndpointForTest(t, job.JobID, endpoint)
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, digest),
			RuntimeEndpoint:       endpoint,
			HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	operator, err := createServiceToken("resume-outage-operator", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	fake.fail = true
	if _, _, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID,
		"suspended", "resume_outage_suspend_01"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET status='offline', last_seen=NULL WHERE id=?`,
		runnerID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID,
		"active", "resume_outage_active_01"); err != nil {
		t.Fatal(err)
	}
	restartHostingDatabaseForCancellationTest(t)
	fake.fail = false
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var projectDesired, compensationStatus, compensationMarker string
	var pendingResume int
	if err := db.QueryRow(`SELECT desired_state FROM hosting_projects WHERE id=?`, project.ID).Scan(
		&projectDesired); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status, desired_state FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='compensate' ORDER BY id DESC LIMIT 1`,
		project.ID).Scan(&compensationStatus, &compensationMarker); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='resume' AND status='pending'`,
		project.ID).Scan(&pendingResume); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fencedEndpoint, fencedSuspended := fake.currentEndpoint, fake.currentSuspended
	fake.mu.Unlock()
	if projectDesired != "active" || compensationStatus != "committed" ||
		compensationMarker != "resume_after_runtime" || pendingResume != 0 ||
		fencedEndpoint != endpoint || !fencedSuspended {
		t.Fatalf("desired=%s compensation=%s/%s pending=%d endpoint=%q suspended=%v",
			projectDesired, compensationStatus, compensationMarker, pendingResume,
			fencedEndpoint, fencedSuspended)
	}
	var sessionID, deploymentExternalID, instanceID string
	var sequence int64
	var capacity capacityDTO
	if err := db.QueryRow(`SELECT runner.active_session_id, runner.last_heartbeat_sequence,
		runner.capacity_cpu_millis, runner.capacity_ram_bytes, runner.capacity_disk_bytes, runner.capacity_pids,
		deployment.external_deployment_id, release.runtime_instance_id
		FROM hosting_runners runner
		JOIN hosting_releases release ON release.runtime_runner_id=runner.id
		JOIN hosting_deployments deployment ON deployment.id=release.hosting_deployment_id
		WHERE runner.id=? AND release.hosting_project_id=? AND release.status='active'`, runnerID, project.ID).Scan(
		&sessionID, &sequence, &capacity.CPUMillis, &capacity.RAMBytes, &capacity.DiskBytes,
		&capacity.PIDs, &deploymentExternalID, &instanceID); err != nil {
		t.Fatal(err)
	}
	inventory := []hostingObservedRuntime{{ExternalProjectID: project.ExternalProjectID,
		ExternalDeploymentID: deploymentExternalID, ReleaseDigest: digest, RuntimeInstanceID: instanceID,
		RuntimeEndpoint: endpoint, State: "running"}}
	if recorder := postHostingInventoryHeartbeat(t, runnerID, capacity, sessionID, sequence+1, inventory); recorder.Code != http.StatusOK {
		t.Fatalf("runner return heartbeat status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var committedResume int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='resume' AND status='committed'`,
		project.ID).Scan(&committedResume); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	resumedEndpoint, resumedSuspended := fake.currentEndpoint, fake.currentSuspended
	fake.mu.Unlock()
	if committedResume != 1 || resumedEndpoint != endpoint || resumedSuspended {
		t.Fatalf("committed resumes=%d endpoint=%q suspended=%v", committedResume,
			resumedEndpoint, resumedSuspended)
	}
}

func TestCommittedResumeCompensationMarkerRecoversPendingRetry(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _ := provisionDeploymentTestProject(t, "project_01JRESMARK")
	resumeID := hashHostingOperation("resume-marker-source", project.ExternalProjectID)
	now := formatSQLiteTime(time.Now().UTC())
	if _, err := db.Exec(`INSERT INTO hosting_proxy_operations
		(operation_id, hosting_project_id, operation_type, route_generation, desired_state, status, created_at, updated_at)
		VALUES (?, ?, 'resume', 1, 'active', 'pending', ?, ?)`, resumeID, project.ID, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_projects SET route_generation=1 WHERE id=?`, project.ID); err != nil {
		t.Fatal(err)
	}
	compensationID := hashHostingOperation("suspend-unavailable-resume", resumeID)
	if err := stageHostingResumeCompensation(t.Context(), project.ID, resumeID, compensationID); err != nil {
		t.Fatal(err)
	}
	if err := executeHostingCompensation(t.Context(), fake, compensationID, "", project.ID, 0,
		project.ExternalProjectID, "", "", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO hosting_proxy_operations
		(operation_id, hosting_project_id, operation_type, route_generation, desired_state, status, created_at, updated_at)
		VALUES (?, ?, 'resume', 1, 'active', 'pending', ?, ?)`,
		hashHostingOperation("stale-pending-resume", project.ExternalProjectID), project.ID, now, now); err != nil {
		t.Fatal(err)
	}
	// Simulate a process crash after the external suspension and durable commit,
	// but before the follow-up resume intent was staged.
	if err := reconcileHostingCompensationOperations(t.Context()); err != nil {
		t.Fatal(err)
	}
	var pendingResume, currentPendingResume, markedCompensation int
	if err := db.QueryRow(`SELECT
		SUM(CASE WHEN operation_type='resume' AND status='pending' THEN 1 ELSE 0 END),
		SUM(CASE WHEN operation_type='resume' AND status='pending'
		  AND route_generation=(SELECT route_generation FROM hosting_projects WHERE id=?) THEN 1 ELSE 0 END),
		SUM(CASE WHEN operation_type='compensate' AND desired_state='resume_after_runtime' THEN 1 ELSE 0 END)
		FROM hosting_proxy_operations WHERE hosting_project_id=?`, project.ID, project.ID).Scan(
		&pendingResume, &currentPendingResume, &markedCompensation); err != nil {
		t.Fatal(err)
	}
	if pendingResume != 1 || currentPendingResume != 0 || markedCompensation != 1 || !fake.currentSuspended {
		t.Fatalf("recovered marker pending resumes=%d current=%d marked compensations=%d suspended=%v",
			pendingResume, currentPendingResume, markedCompensation, fake.currentSuspended)
	}
}

func TestDesiredStateRouteGenerationFencesLateSuspend(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	fake.suspendStarted = make(chan struct{})
	fake.suspendContinue = make(chan struct{})
	suspendStarted, suspendContinue := fake.suspendStarted, fake.suspendContinue
	project, token := provisionDeploymentTestProject(t, "project_01JSTATEGEN")
	suspendResult := make(chan error, 1)
	go func() {
		_, _, err := setHostingProjectDesiredState(context.Background(), token, project.ExternalProjectID,
			"suspended", "suspend_01JSTATEGEN")
		suspendResult <- err
	}()
	<-suspendStarted
	fake.mu.Lock()
	fake.suspendStarted = nil
	fake.suspendContinue = nil
	fake.mu.Unlock()
	if _, _, err := setHostingProjectDesiredState(t.Context(), token, project.ExternalProjectID,
		"active", "resume_01JSTATEGEN"); err != nil {
		t.Fatalf("resume while suspend is delayed: %v", err)
	}
	suspendContinue <- struct{}{}
	if err := <-suspendResult; err != nil {
		t.Fatalf("superseded suspend request: %v", err)
	}
	fake.mu.Lock()
	suspended := fake.currentSuspended
	requests := append([]proxySuspendRequest(nil), fake.suspensions...)
	fake.mu.Unlock()
	if !suspended {
		t.Fatal("project without an active release was unsuspended")
	}
	if len(requests) != 2 || requests[0].RouteGeneration != 1 || requests[1].RouteGeneration != 3 {
		t.Fatalf("ordered suspension requests=%+v", requests)
	}
	var desired string
	var generation int64
	if err := db.QueryRow(`SELECT desired_state, route_generation FROM hosting_projects WHERE id=?`, project.ID).Scan(&desired, &generation); err != nil {
		t.Fatal(err)
	}
	if desired != "active" || generation != 3 {
		t.Fatalf("desired state=%q generation=%d", desired, generation)
	}
	var failed, committed, pending int
	if err := db.QueryRow(`SELECT
		SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END),
		SUM(CASE WHEN status='committed' THEN 1 ELSE 0 END),
		SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END)
		FROM hosting_proxy_operations WHERE hosting_project_id=? AND operation_type IN ('suspend','resume')`, project.ID).Scan(&failed, &committed, &pending); err != nil {
		t.Fatal(err)
	}
	if failed != 2 || committed != 0 || pending != 0 {
		t.Fatalf("desired-state operations failed=%d committed=%d pending=%d", failed, committed, pending)
	}
}

func TestHostingRouteCompensationSurvivesAdapterFailureAndSuspendsUnavailableFallback(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _ := provisionDeploymentTestProject(t, "project_01JCOMPSAG")
	digest := "sha256:" + strings.Repeat("e", 64)
	operationID := hashHostingOperation("test-compensation", project.ExternalProjectID)
	fake.fail = true
	err := executeHostingCompensation(t.Context(), fake, operationID, "", project.ID, 0,
		project.ExternalProjectID, "sha256:"+strings.Repeat("f", 64), digest, "http://10.88.0.1:3000")
	if err == nil {
		t.Fatal("adapter failure unexpectedly committed compensation")
	}
	var status string
	var generation int64
	if err := db.QueryRow(`SELECT status, route_generation FROM hosting_proxy_operations WHERE operation_id=?`,
		operationID).Scan(&status, &generation); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || generation <= 0 {
		t.Fatalf("durable compensation status=%q generation=%d", status, generation)
	}
	fake.fail = false
	if err := reconcileHostingCompensationOperations(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations WHERE operation_id=?`, operationID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "committed" || !fake.currentSuspended || fake.currentEndpoint != "" {
		t.Fatalf("reconciled compensation status=%q suspended=%v endpoint=%q",
			status, fake.currentSuspended, fake.currentEndpoint)
	}
}

func TestHostingCompensationSuspendsIfExactFallbackDisappearsDuringProxyActivation(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, runnerID, job := createAndClaimHostingJob(t, "project_01JCOMPLOS", "deployment_01JCOMPLOS")
	digest := "sha256:" + strings.Repeat("7", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	endpoint := healthyHostingEndpointForTest(t)
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken,
		hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: endpoint,
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	fake.activateStarted = make(chan struct{}, 1)
	fake.activateContinue = make(chan struct{})
	operationID := hashHostingOperation("test-fallback-loss", project.ExternalProjectID)
	result := make(chan error, 1)
	go func() {
		result <- executeHostingCompensation(context.Background(), fake, operationID, "", project.ID,
			job.JobID, project.ExternalProjectID, "sha256:"+strings.Repeat("8", 64), digest, endpoint)
	}()
	<-fake.activateStarted
	start := time.Now().UTC()
	reconcileHostingRuntimeInventoryAt(t, runnerID, nil, start)
	reconcileHostingRuntimeInventoryAt(t, runnerID, nil, start.Add(hostingRuntimeMissingGrace+time.Second))
	close(fake.activateContinue)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations WHERE operation_id=?`, operationID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || !fake.currentSuspended {
		t.Fatalf("stale fallback compensation status=%q suspended=%v activations=%+v suspensions=%+v",
			status, fake.currentSuspended, fake.activations, fake.suspensions)
	}
}

func TestHostingCompensationIdentityIncludesSourceRoutingIntent(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _ := provisionDeploymentTestProject(t, "project_01JCOMPIDS")
	state := &hostingCompletionState{ProjectID: project.ID, ExternalProjectID: project.ExternalProjectID,
		ExternalDeploymentID: "deployment_01JCOMPIDS"}
	candidate := "sha256:" + strings.Repeat("9", 64)
	for index := 1; index <= 2; index++ {
		sourceID := hashHostingOperation("rollback-source", fmt.Sprint(index))
		now := formatSQLiteTime(time.Now().UTC())
		if _, err := db.Exec(`UPDATE hosting_projects SET route_generation=? WHERE id=?`, index*2-1, project.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO hosting_proxy_operations
			(operation_id, hosting_project_id, operation_type, release_digest, route_generation, status, created_at, updated_at)
			VALUES (?, ?, 'rollback', ?, ?, 'pending', ?, ?)`, sourceID, project.ID, candidate, index*2-1, now, now); err != nil {
			t.Fatal(err)
		}
		compensated, err := compensateHostingActivationIfCurrent(t.Context(), fake, sourceID, state, candidate)
		if err != nil || !compensated {
			t.Fatalf("source %d compensation applied=%v err=%v", index, compensated, err)
		}
	}
	var compensations int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations WHERE hosting_project_id=?
		AND operation_type='compensate'`, project.ID).Scan(&compensations); err != nil {
		t.Fatal(err)
	}
	if compensations != 2 || len(fake.suspensions) != 2 ||
		fake.suspensions[0].OperationID == fake.suspensions[1].OperationID {
		t.Fatalf("compensations=%d suspensions=%+v", compensations, fake.suspensions)
	}
}

func TestSupersededPendingCompensationCannotStageNewerSuspension(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, runnerID, job := createAndClaimHostingJob(t, "project_01JCOMPNEW", "deployment_01JCOMPNEW")
	digest := "sha256:" + strings.Repeat("2", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, digest)
	endpoint := healthyHostingEndpointForTest(t)
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken,
		hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: artifactDigest, RuntimeEndpoint: endpoint,
			HealthEvidence: map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	operationID := hashHostingOperation("pending-compensation", project.ExternalProjectID)
	fake.fail = true
	if err := executeHostingCompensation(t.Context(), fake, operationID, "", project.ID, job.JobID,
		project.ExternalProjectID, "sha256:"+strings.Repeat("3", 64), digest, endpoint); err == nil {
		t.Fatal("adapter failure did not leave compensation pending")
	}
	fake.fail = false
	var newerGeneration int64
	if err := db.QueryRow(`UPDATE hosting_projects SET route_generation=route_generation+1 WHERE id=? RETURNING route_generation`,
		project.ID).Scan(&newerGeneration); err != nil {
		t.Fatal(err)
	}
	now := formatSQLiteTime(time.Now().UTC())
	if _, err := db.Exec(`INSERT INTO hosting_proxy_operations
		(operation_id, hosting_project_id, operation_type, route_generation, status, created_at, updated_at)
		VALUES (?, ?, 'rollback', ?, 'committed', ?, ?)`, hashHostingOperation("newer-route"), project.ID,
		newerGeneration, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_releases SET runtime_missing_since=? WHERE release_digest=?`, now, digest); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingCompensationOperations(t.Context()); err != nil {
		t.Fatal(err)
	}
	var generation int64
	var status string
	if err := db.QueryRow(`SELECT route_generation FROM hosting_projects WHERE id=?`, project.ID).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations WHERE operation_id=?`, operationID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if generation != newerGeneration || status != "failed" || len(fake.suspensions) != 0 {
		t.Fatalf("generation=%d newer=%d status=%q suspensions=%+v", generation, newerGeneration, status, fake.suspensions)
	}
}

func TestUnavailableResumeCannotFenceNewerRoutingIntent(t *testing.T) {
	withTempDB(t)
	project, _ := provisionDeploymentTestProject(t, "project_01JRESFENC")
	now := formatSQLiteTime(time.Now().UTC())
	resumeID := hashHostingOperation("resume-source", project.ExternalProjectID)
	if _, err := db.Exec(`UPDATE hosting_projects SET route_generation=2 WHERE id=?`, project.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO hosting_proxy_operations
		(operation_id, hosting_project_id, operation_type, route_generation, desired_state, status, created_at, updated_at)
		VALUES (?, ?, 'resume', 1, 'active', 'pending', ?, ?),
		       (?, ?, 'rollback', 2, '', 'committed', ?, ?)`, resumeID, project.ID, now, now,
		hashHostingOperation("newer-resume-route"), project.ID, now, now); err != nil {
		t.Fatal(err)
	}
	compensationID := hashHostingOperation("suspend-unavailable-resume", resumeID)
	err := stageHostingResumeCompensation(t.Context(), project.ID, resumeID, compensationID)
	if !errors.Is(err, errHostingProxyOperationSuperseded) {
		t.Fatalf("stale resume compensation err=%v", err)
	}
	var generation, compensations int
	if err := db.QueryRow(`SELECT route_generation FROM hosting_projects WHERE id=?`, project.ID).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations WHERE operation_id=?`, compensationID).Scan(&compensations); err != nil {
		t.Fatal(err)
	}
	if generation != 2 || compensations != 0 {
		t.Fatalf("stale resume generation=%d compensations=%d", generation, compensations)
	}
}

func TestSupersededStagedResumeCompensationCannotCreateRetry(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _ := provisionDeploymentTestProject(t, "project_01JRESSTAL")
	now := formatSQLiteTime(time.Now().UTC())
	resumeID := hashHostingOperation("staged-resume-source", project.ExternalProjectID)
	if _, err := db.Exec(`UPDATE hosting_projects SET route_generation=1 WHERE id=?`, project.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO hosting_proxy_operations
		(operation_id, hosting_project_id, operation_type, route_generation, desired_state, status, created_at, updated_at)
		VALUES (?, ?, 'resume', 1, 'active', 'pending', ?, ?)`, resumeID, project.ID, now, now); err != nil {
		t.Fatal(err)
	}
	compensationID := hashHostingOperation("suspend-unavailable-resume", resumeID)
	if err := stageHostingResumeCompensation(t.Context(), project.ID, resumeID, compensationID); err != nil {
		t.Fatal(err)
	}
	if err := executeHostingCompensation(t.Context(), fake, compensationID, "", project.ID, 0,
		project.ExternalProjectID, "", "", ""); err != nil {
		t.Fatal(err)
	}
	var newerGeneration int64
	if err := db.QueryRow(`UPDATE hosting_projects SET route_generation=route_generation+1 WHERE id=? RETURNING route_generation`,
		project.ID).Scan(&newerGeneration); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO hosting_proxy_operations
		(operation_id, hosting_project_id, operation_type, route_generation, status, created_at, updated_at)
		VALUES (?, ?, 'rollback', ?, 'committed', ?, ?)`, hashHostingOperation("newer-staged-route"), project.ID,
		newerGeneration, now, now); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingCompensationOperations(t.Context()); err != nil {
		t.Fatal(err)
	}
	var pendingResume int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations WHERE hosting_project_id=?
		AND operation_type='resume' AND status='pending'`, project.ID).Scan(&pendingResume); err != nil {
		t.Fatal(err)
	}
	if pendingResume != 0 || len(fake.suspensions) != 1 {
		t.Fatalf("stale compensation pending resumes=%d suspensions=%+v", pendingResume, fake.suspensions)
	}
}

func TestSuspendDuringBuildPreventsLateActivation(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, runnerID, job := createAndClaimHostingJobAtFetching(t, "project_01JSUSRUN", "deployment_01JSUSRUN")
	operator, err := createServiceToken("suspend-running-operator", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := setHostingProjectDesiredState(t.Context(), operator, project.ExternalProjectID, "suspended", "suspend_running_01"); err != nil {
		t.Fatal(err)
	}
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration, job.LeaseToken, hostingCompletionRequest{
		Status: "success", ReleaseDigest: "sha256:" + strings.Repeat("9", 64), RuntimeEndpoint: healthyHostingEndpointForTest(t),
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

func TestRecoveryLogWriteRechecksLeaseAfterBlockedBodyDecode(t *testing.T) {
	withTempDB(t)
	project, _, runnerID, job := createAndClaimHostingJob(t,
		"project_01JRECOVERYLOGRACE", "deployment_01JRECOVERYLOGRACE")
	releaseDigest := "sha256:" + strings.Repeat("d", 64)
	artifactDigest := attachTestReleaseArtifact(t, job.JobID, releaseDigest)
	var deploymentID int64
	var cpu, ram, disk, pids int64
	var artifactPath string
	var artifactSize int64
	if err := db.QueryRow(`SELECT hosting_deployment_id, required_cpu_millis, required_ram_bytes,
		required_disk_bytes, required_pids, release_upload_path, release_upload_size
		FROM hosting_jobs WHERE id=?`, job.JobID).Scan(
		&deploymentID, &cpu, &ram, &disk, &pids, &artifactPath, &artifactSize); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := db.Exec(`INSERT INTO hosting_release_artifacts
		(artifact_digest, release_digest, artifact_path, size_bytes, created_at) VALUES (?, ?, ?, ?, ?)`,
		artifactDigest, releaseDigest, artifactPath, artifactSize, formatSQLiteTime(now)); err != nil {
		t.Fatal(err)
	}
	releaseResult, err := db.Exec(`INSERT INTO hosting_releases
		(hosting_project_id, hosting_deployment_id, release_digest, release_artifact_digest,
		 commit_sha, artifact_digest, status, runtime_endpoint, runtime_runner_id,
		 runtime_generation, runtime_instance_id, runtime_manifest_json, created_at, activated_at)
		VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, 1, ?, '{}', ?, ?)`, project.ID, deploymentID,
		releaseDigest, artifactDigest, strings.Repeat("a", 40), "sha256:"+strings.Repeat("c", 64),
		"http://127.0.0.1:43210", runnerID, "build-race-instance", formatSQLiteTime(now), formatSQLiteTime(now))
	if err != nil {
		t.Fatal(err)
	}
	releaseID, err := releaseResult.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	leaseToken := "recovery-log-race-token"
	recoveryResult, err := db.Exec(`INSERT INTO hosting_runtime_recoveries
		(hosting_release_id, hosting_runner_id, status, lease_generation, lease_token_hash,
		 lease_expires_at, attempts, required_cpu_millis, required_ram_bytes,
		 required_disk_bytes, required_pids, runtime_owner_runner_id, created_at, started_at)
		VALUES (?, ?, 'leased', 1, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)`, releaseID, runnerID,
		hashToken(leaseToken), formatSQLiteTime(now.Add(time.Minute)), cpu, ram, disk, pids,
		runnerID, formatSQLiteTime(now), formatSQLiteTime(now))
	if err != nil {
		t.Fatal(err)
	}
	recoveryID, err := recoveryResult.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"stream":"runtime","message":"late log"}`)
	body := &blockingRequestBody{reader: bytes.NewReader(payload), started: make(chan struct{}), release: make(chan struct{})}
	request := httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/api/hosting-agent/v1/recoveries/%d/logs", recoveryID), body)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Deployer-Lease-Generation", "1")
	request.Header.Set("X-Deployer-Lease-Token", leaseToken)
	request = request.WithContext(context.WithValue(request.Context(), hostingRunnerContextKey{},
		&HostingRunner{ID: runnerID}))
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handleHostingRecoveryLogs(recorder, request, recoveryID)
		close(done)
	}()
	select {
	case <-body.started:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery log handler did not reach body decoding")
	}
	if _, err := db.Exec(`UPDATE hosting_runtime_recoveries SET lease_expires_at=? WHERE id=?`,
		formatSQLiteTime(time.Now().UTC().Add(-time.Second)), recoveryID); err != nil {
		t.Fatal(err)
	}
	close(body.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery log handler did not finish")
	}
	var logs int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_logs WHERE hosting_deployment_id=?`, deploymentID).Scan(&logs); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != http.StatusForbidden || logs != 0 {
		t.Fatalf("status=%d body=%s logs=%d", recorder.Code, recorder.Body.String(), logs)
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
	if len(runner.Operations) != 1 || runner.Operations[0] != "build" {
		t.Fatalf("legacy runner operation default = %#v", runner.Operations)
	}
	if _, err := createHostingRunner(t.Context(), hostingRunnerInput{
		Name: "unsupported-operation-runner", ProtocolVersion: hostingRunnerProtocolVersion,
		ManifestVersions: []string{hostingManifestVersion}, RuntimeVersions: []string{"20"},
		Operations: []string{"build", "arbitrary"},
		Capacity:   capacityDTO{CPUMillis: 2000, RAMBytes: 2 << 30, DiskBytes: 10 << 30, PIDs: 512},
		Reserve:    capacityDTO{CPUMillis: 250, RAMBytes: 256 << 20, DiskBytes: 1 << 30, PIDs: 32},
	}, "invalid-runner-operation"); err == nil {
		t.Fatal("unknown runner operation was accepted")
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

func TestHostingRunnerClassAndHeartbeatLabelsStayHostingScoped(t *testing.T) {
	withTempDB(t)
	runner, err := createHostingRunner(t.Context(), hostingRunnerInput{
		Name: "class-label-runner", Labels: []string{"linux"}, ExecutionClass: "hosting",
		ProtocolVersion: hostingRunnerProtocolVersion, ManifestVersions: []string{hostingManifestVersion}, RuntimeVersions: []string{"22"},
		Capacity: capacityDTO{CPUMillis: 2000, RAMBytes: 2 << 30, DiskBytes: 10 << 30, PIDs: 512},
		Reserve:  capacityDTO{CPUMillis: 250, RAMBytes: 256 << 20, DiskBytes: 1 << 30, PIDs: 32},
	}, "class-label-request")
	if err != nil || runner.ExecutionClass != "hosting" {
		t.Fatalf("runner class=%q err=%v", runner.ExecutionClass, err)
	}
	if _, err := createHostingRunner(t.Context(), hostingRunnerInput{
		Name: "legacy-class-runner", ExecutionClass: "legacy", ProtocolVersion: hostingRunnerProtocolVersion,
		ManifestVersions: []string{hostingManifestVersion}, RuntimeVersions: []string{"22"},
		Capacity: capacityDTO{CPUMillis: 2000, RAMBytes: 2 << 30, DiskBytes: 10 << 30, PIDs: 512},
		Reserve:  capacityDTO{CPUMillis: 250, RAMBytes: 256 << 20, DiskBytes: 1 << 30, PIDs: 32},
	}, "legacy-class-request"); err == nil {
		t.Fatal("non-hosting execution class was accepted")
	}
	sessionID := strings.TrimPrefix(hashHostingOperation("test.runner.session", fmt.Sprint(runner.ID)), "sha256:")[:48]
	payload, err := json.Marshal(hostingHeartbeatRequest{Free: runner.Capacity, Labels: []string{"linux", "arm64"},
		ProtocolVersion: hostingRunnerProtocolVersion, ManifestVersions: []string{hostingManifestVersion}, RuntimeVersions: []string{"22"},
		Operations: []string{"build", hostingRunnerInventoryOperation}, SessionID: sessionID, Sequence: 1,
		RuntimeInventory: &[]hostingObservedRuntime{}})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/hosting-agent/v1/heartbeat", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(context.WithValue(req.Context(), hostingRunnerContextKey{}, &HostingRunner{ID: runner.ID}))
	rec := httptest.NewRecorder()
	handleHostingAgentHeartbeat(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("heartbeat status=%d body=%s", rec.Code, rec.Body.String())
	}
	var labels string
	if err := db.QueryRow(`SELECT execution_class, labels_json FROM hosting_runners WHERE id=?`, runner.ID).Scan(&runner.ExecutionClass, &labels); err != nil {
		t.Fatal(err)
	}
	if runner.ExecutionClass != "hosting" || labels != `["linux","arm64"]` {
		t.Fatalf("stored class/labels=%q/%s", runner.ExecutionClass, labels)
	}
}

func TestHostingRunnerHeartbeatPublishesFreeCapacityAndDrainState(t *testing.T) {
	withTempDB(t)
	runner, err := createHostingRunner(t.Context(), hostingRunnerInput{
		Name: "capacity-drain-runner", Labels: []string{"linux"}, ProtocolVersion: hostingRunnerProtocolVersion,
		ManifestVersions: []string{hostingManifestVersion}, RuntimeVersions: []string{"22"},
		Capacity: capacityDTO{CPUMillis: 2000, RAMBytes: 2 << 30, DiskBytes: 10 << 30, PIDs: 512},
		Reserve:  capacityDTO{CPUMillis: 250, RAMBytes: 256 << 20, DiskBytes: 1 << 30, PIDs: 32},
	}, "capacity-drain-request")
	if err != nil {
		t.Fatal(err)
	}
	free := capacityDTO{CPUMillis: 1500, RAMBytes: 1 << 30, DiskBytes: 8 << 30, PIDs: 400}
	sessionID := strings.TrimPrefix(hashHostingOperation("test.runner.session", fmt.Sprint(runner.ID)), "sha256:")[:48]
	payload, _ := json.Marshal(hostingHeartbeatRequest{Free: free, Draining: true, Labels: []string{"linux"},
		ProtocolVersion: hostingRunnerProtocolVersion, ManifestVersions: []string{hostingManifestVersion}, RuntimeVersions: []string{"22"},
		Operations: []string{"build", hostingRunnerInventoryOperation}, SessionID: sessionID, Sequence: 1,
		RuntimeInventory: &[]hostingObservedRuntime{}})
	req := httptest.NewRequest(http.MethodPost, "/api/hosting-agent/v1/heartbeat", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(context.WithValue(req.Context(), hostingRunnerContextKey{}, &HostingRunner{ID: runner.ID}))
	rec := httptest.NewRecorder()
	handleHostingAgentHeartbeat(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("heartbeat status=%d body=%s", rec.Code, rec.Body.String())
	}
	runners, err := listHostingRunners(t.Context())
	if err != nil || len(runners) != 1 {
		t.Fatalf("list runners=%+v err=%v", runners, err)
	}
	got := runners[0]
	if !got.Draining || got.Status != "online" || got.Free != free {
		t.Fatalf("runner heartbeat state=%+v", got)
	}
}

func TestHostingServiceAndLegacyCredentialsCannotCrossExecutionBoundaries(t *testing.T) {
	withTempDB(t)
	hostingRunner, err := createHostingRunner(t.Context(), hostingRunnerInput{
		Name: "credential-boundary-hosting", ProtocolVersion: hostingRunnerProtocolVersion,
		ManifestVersions: []string{hostingManifestVersion}, RuntimeVersions: []string{"22"},
		Capacity: capacityDTO{CPUMillis: 2000, RAMBytes: 2 << 30, DiskBytes: 10 << 30, PIDs: 512},
		Reserve:  capacityDTO{CPUMillis: 250, RAMBytes: 256 << 20, DiskBytes: 1 << 30, PIDs: 32},
	}, "credential-boundary-hosting")
	if err != nil {
		t.Fatal(err)
	}
	legacyRunner := &Runner{Name: "credential-boundary-legacy", Labels: "linux"}
	if err := createRunner(legacyRunner); err != nil {
		t.Fatal(err)
	}
	serviceToken, err := createServiceToken("credential-boundary-service", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatal(err)
	}

	credentials := map[string]string{
		"hosting": hostingRunner.Token,
		"legacy":  legacyRunner.Token,
		"service": serviceToken.Token,
	}
	for name, credential := range credentials {
		t.Run(name, func(t *testing.T) {
			hostingPassed := false
			hostingHandler := hostingRunnerAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hostingPassed = true
				w.WriteHeader(http.StatusNoContent)
			}))
			req := httptest.NewRequest(http.MethodPost, "/api/hosting-agent/v1/poll", nil)
			req.Header.Set("Authorization", "Bearer "+credential)
			rec := httptest.NewRecorder()
			hostingHandler.ServeHTTP(rec, req)
			if name == "hosting" {
				if rec.Code != http.StatusNoContent || !hostingPassed {
					t.Fatalf("hosting auth rejected hosting credential: status=%d body=%s", rec.Code, rec.Body.String())
				}
			} else if rec.Code != http.StatusUnauthorized || hostingPassed {
				t.Fatalf("hosting auth accepted %s credential: status=%d", name, rec.Code)
			}

			servicePassed := false
			serviceHandler := serviceTokenAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				servicePassed = true
				w.WriteHeader(http.StatusNoContent)
			}))
			req = httptest.NewRequest(http.MethodGet, "/api/internal/v1/deployments/example", nil)
			req.Header.Set("Authorization", "Bearer "+credential)
			rec = httptest.NewRecorder()
			serviceHandler.ServeHTTP(rec, req)
			if name == "service" {
				if rec.Code != http.StatusNoContent || !servicePassed {
					t.Fatalf("service auth rejected service credential: status=%d body=%s", rec.Code, rec.Body.String())
				}
			} else if rec.Code != http.StatusUnauthorized || servicePassed {
				t.Fatalf("service auth accepted %s credential: status=%d", name, rec.Code)
			}

			req = httptest.NewRequest(http.MethodGet, "/api/agent/poll", nil)
			req.Header.Set("Authorization", "Bearer "+credential)
			authenticatedLegacy, authErr := authenticateAgent(req)
			if name == "legacy" {
				if authErr != nil || authenticatedLegacy.ID != legacyRunner.ID {
					t.Fatalf("legacy auth rejected legacy credential: runner=%+v err=%v", authenticatedLegacy, authErr)
				}
			} else if authErr == nil {
				t.Fatalf("legacy auth accepted %s credential as runner %+v", name, authenticatedLegacy)
			}
		})
	}

	for table, want := range map[string]int{"hosting_runners": 1, "runners": 1, "service_tokens": 1} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != want {
			t.Fatalf("%s count=%d want=%d err=%v", table, count, want, err)
		}
	}
}
