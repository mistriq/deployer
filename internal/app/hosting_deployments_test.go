package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func insertHostingRunnerForTest(t *testing.T, name string, free hostingWorkloadLimits) int64 {
	t.Helper()
	now := formatSQLiteTime(time.Now().UTC())
	result, err := db.Exec(`INSERT INTO hosting_runners
		(name, token_hash, labels_json, protocol_version, manifest_versions_json, runtime_versions_json, operation_capabilities_json,
		 capacity_cpu_millis, capacity_ram_bytes, capacity_disk_bytes, capacity_pids,
		 free_cpu_millis, free_ram_bytes, free_disk_bytes, free_pids,
		 reported_free_cpu_millis, reported_free_ram_bytes, reported_free_disk_bytes, reported_free_pids,
		 reserve_cpu_millis, reserve_ram_bytes, reserve_disk_bytes, reserve_pids,
		 draining, status, last_seen, created_at)
		VALUES (?, ?, '["linux"]', 'v1', '["v1"]', '["20","22"]', '["build","restore"]',
		 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, 'online', ?, ?)`,
		name, hashToken("runner-"+name), free.CPUMillis, free.RAMBytes, free.DiskBytes, free.PIDs,
		free.CPUMillis, free.RAMBytes, free.DiskBytes, free.PIDs,
		free.CPUMillis, free.RAMBytes, free.DiskBytes, free.PIDs, now, now)
	if err != nil {
		t.Fatalf("insert hosting runner: %v", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("hosting runner ID: %v", err)
	}
	return id
}

func validHostingDeploymentRequest(externalID string) hostingDeploymentCreateRequest {
	return hostingDeploymentCreateRequest{
		ExternalDeploymentID: externalID,
		CommitSHA:            strings.Repeat("a", 40),
		ManifestDigest:       "sha256:" + strings.Repeat("b", 64),
		ArtifactDigest:       "sha256:" + strings.Repeat("c", 64),
		SourceReference: HostingSourceReference{
			Provider:  "control-plane",
			Reference: "source_01JTESTREFERENCE",
			ExpiresAt: time.Now().UTC().Add(30 * time.Minute),
		},
	}
}

func provisionDeploymentTestProject(t *testing.T, externalID string) (*HostingProject, *ServiceToken) {
	t.Helper()
	withHostingConfig(t)
	oldPreparer := prepareHostingSource
	prepareHostingSource = func(context.Context, *HostingProject, HostingSourceReference, string, string) (string, string, error) {
		return "/managed/test-source.tar", "sha256:" + strings.Repeat("c", 64), nil
	}
	t.Cleanup(func() { prepareHostingSource = oldPreparer })
	project, _, _, err := upsertHostingProject(t.Context(), externalID, validHostingManifest("node"))
	if err != nil {
		t.Fatalf("provision hosting project: %v", err)
	}
	token, err := createServiceToken("deployment-writer-"+externalID, []string{serviceScopeDeploymentsRead, serviceScopeDeploymentsWrite})
	if err != nil {
		t.Fatalf("create service token: %v", err)
	}
	return project, token
}

func TestInternalDeploymentCreationIsHostingOnlyAndPayloadIdempotent(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JDEPLOY")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	runnerID := insertHostingRunnerForTest(t, "hosting-one", limits)

	payload := validHostingDeploymentRequest("deployment_01JDEPLOY")
	payload.ManifestDigest = project.ManifestDigest
	body, _ := json.Marshal(payload)
	handler := serviceTokenAuthMiddleware(http.HandlerFunc(handleInternalAPI))
	request := httptest.NewRequest(http.MethodPost, "/api/internal/v1/projects/"+project.ExternalProjectID+"/deployments", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token.Token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "idem_01JDEPLOY")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("create deployment = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var created HostingDeployment
	if err := json.NewDecoder(recorder.Body).Decode(&created); err != nil {
		t.Fatalf("decode deployment: %v", err)
	}
	if created.ExternalDeploymentID != payload.ExternalDeploymentID || created.CommitSHA != payload.CommitSHA || created.Status != hostingStatusQueued || created.Phase != hostingPhaseQueued {
		t.Fatalf("unexpected deployment: %+v", created)
	}

	request = httptest.NewRequest(http.MethodPost, "/api/internal/v1/projects/"+project.ExternalProjectID+"/deployments", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token.Token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "idem_01JDEPLOY")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted || recorder.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay deployment = %d replay=%q body=%s", recorder.Code, recorder.Header().Get("Idempotency-Replayed"), recorder.Body.String())
	}

	changed := payload
	changed.ArtifactDigest = "sha256:" + strings.Repeat("d", 64)
	changedBody, _ := json.Marshal(changed)
	request = httptest.NewRequest(http.MethodPost, "/api/internal/v1/projects/"+project.ExternalProjectID+"/deployments", bytes.NewReader(changedBody))
	request.Header.Set("Authorization", "Bearer "+token.Token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "idem_01JDEPLOY")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	assertAPIErrorCode(t, recorder, http.StatusConflict, errCodeIdempotencyConflict)

	for table, expected := range map[string]int{"hosting_deployments": 1, "hosting_jobs": 1, "hosting_idempotency": 1, "hosting_events": 1, "builds": 0, "jobs": 0} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != expected {
			t.Fatalf("%s count = %d, want %d", table, count, expected)
		}
	}
	var freeCPU int64
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, runnerID).Scan(&freeCPU); err != nil {
		t.Fatalf("read runner capacity: %v", err)
	}
	if freeCPU != 0 {
		t.Fatalf("runner capacity was not reserved exactly once: %d", freeCPU)
	}
}

func TestHostingDeploymentConcurrentDuplicateCreatesOneAtomicGraph(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JCONCURR")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "hosting-concurrent", limits)
	request := validHostingDeploymentRequest("deployment_01JCONCURR")
	request.ManifestDigest = project.ManifestDigest

	const workers = 32
	var wait sync.WaitGroup
	errorsChannel := make(chan error, workers)
	ids := make(chan string, workers)
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := createHostingDeployment(context.Background(), token, project.ExternalProjectID, "idem_01JCONCURR", request)
			if err != nil {
				errorsChannel <- err
				return
			}
			ids <- result.Deployment.ExternalDeploymentID
		}()
	}
	wait.Wait()
	close(errorsChannel)
	close(ids)
	for err := range errorsChannel {
		t.Errorf("duplicate deployment: %v", err)
	}
	for id := range ids {
		if id != request.ExternalDeploymentID {
			t.Errorf("unexpected deployment identity %q", id)
		}
	}
	for table, expected := range map[string]int{"hosting_deployments": 1, "hosting_jobs": 1, "hosting_idempotency": 1, "hosting_events": 1} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != expected {
			t.Fatalf("%s count = %d, want %d", table, count, expected)
		}
	}
}

func TestHostingDeploymentRefusesCapacityWithoutPartialState(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JNOCAPAC")
	request := validHostingDeploymentRequest("deployment_01JNOCAPAC")
	request.ManifestDigest = project.ManifestDigest
	_, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "idem_01JNOCAPAC", request)
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeRunnerCapacityUnavailable {
		t.Fatalf("expected capacity error, got %v", err)
	}
	for _, table := range []string{"hosting_deployments", "hosting_jobs", "hosting_idempotency", "hosting_events"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("%s contains partial state after refused placement", table)
		}
	}
}

func TestHostingDeploymentRejectsArtifactDigestMismatchWithoutState(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JARTMIS")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "artifact-mismatch-runner", limits)
	request := validHostingDeploymentRequest("deployment_01JARTMIS")
	request.ManifestDigest = project.ManifestDigest
	request.ArtifactDigest = "sha256:" + strings.Repeat("d", 64)
	_, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JARTMIS", request)
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeArtifactDigestMismatch {
		t.Fatalf("artifact mismatch error = %v", err)
	}
	for _, table := range []string{"hosting_deployments", "hosting_jobs", "hosting_idempotency"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s count=%d err=%v", table, count, err)
		}
	}
}

func TestHostingDeploymentRedeemsBoundSourceBeforeCreatingState(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	project, _, _, err := upsertHostingProject(t.Context(), "project_01JSOURCE", validHostingManifest("node"))
	if err != nil {
		t.Fatal(err)
	}
	token, err := createServiceToken("source-deployment-writer", []string{serviceScopeDeploymentsWrite})
	if err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "source-runner", limits)
	body := sourceBrokerTestArchive(t, "exact source from authenticated broker")
	var brokerCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		brokerCalls++
		writeSourceBrokerResponse(w, r, body)
	}))
	defer server.Close()
	appConfig.HostingSourceBrokerURL = server.URL
	appConfig.HostingSourceBrokerToken = "broker-token"
	appConfig.HostingSourceBrokerTimeout = time.Second
	appConfig.HostingSourceMaxBytes = 1 << 20

	request := validHostingDeploymentRequest("deployment_01JSOURCE")
	request.ManifestDigest = project.ManifestDigest
	request.ArtifactDigest = sourceBrokerTestDigest(body)
	result, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JSOURCE", request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Deployment.ArtifactDigest != request.ArtifactDigest {
		t.Fatalf("unexpected deployment result: %+v", result.Deployment)
	}
	var sourcePath, recipeJSON, secretJSON, responseJSON, auditJSON string
	if err := db.QueryRow(`SELECT source_artifact_path, recipe_json, secret_refs_json FROM hosting_jobs`).Scan(&sourcePath, &recipeJSON, &secretJSON); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT response_body FROM hosting_idempotency`).Scan(&responseJSON); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT metadata_json FROM hosting_audit_events WHERE event_type='hosting_deployment_created'`).Scan(&auditJSON); err != nil {
		t.Fatal(err)
	}
	stored, err := os.ReadFile(sourcePath)
	if err != nil || string(stored) != string(body) {
		t.Fatalf("stored source=%q err=%v", stored, err)
	}
	for name, persisted := range map[string]string{"recipe": recipeJSON, "secret refs": secretJSON, "idempotency response": responseJSON, "audit": auditJSON} {
		if strings.Contains(persisted, request.SourceReference.Reference) {
			t.Fatalf("source reference leaked into %s", name)
		}
	}
	if _, err := db.Exec(`UPDATE hosting_deployments SET status='failed', phase='failed' WHERE external_deployment_id=?`, request.ExternalDeploymentID); err != nil {
		t.Fatal(err)
	}
	_, err = createHostingDeployment(t.Context(), token, project.ExternalProjectID, "different_01JSOURCE", request)
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeExternalDeploymentConflict || brokerCalls != 1 {
		t.Fatalf("existing identity preflight err=%v broker_calls=%d", err, brokerCalls)
	}
}

func TestHostingDeploymentSourceBrokerFailureLeavesNoPartialStateOrCapacity(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	project, _, _, err := upsertHostingProject(t.Context(), "project_01JSRCFAIL", validHostingManifest("node"))
	if err != nil {
		t.Fatal(err)
	}
	token, err := createServiceToken("source-failure-writer", []string{serviceScopeDeploymentsWrite})
	if err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	runnerID := insertHostingRunnerForTest(t, "source-failure-runner", limits)
	brokerStatus := http.StatusForbidden
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "reference unavailable", brokerStatus)
	}))
	defer server.Close()
	appConfig.HostingSourceBrokerURL = server.URL
	appConfig.HostingSourceBrokerToken = "broker-token"
	appConfig.HostingSourceBrokerTimeout = time.Second
	appConfig.HostingSourceMaxBytes = 1 << 20

	request := validHostingDeploymentRequest("deployment_01JSRCFAIL")
	request.ManifestDigest = project.ManifestDigest
	_, err = createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JSRCFAIL", request)
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeSourceFetchFailed || apiErr.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("source broker failure=%v", err)
	}
	brokerStatus = http.StatusServiceUnavailable
	request.ExternalDeploymentID = "deployment_01JSRCTEMP"
	request.SourceReference.Reference = "source_01JTEMPORARY"
	_, err = createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JSRCTEMP", request)
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeSourceFetchFailed || apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("transient source broker failure=%v", err)
	}
	for _, table := range []string{"hosting_deployments", "hosting_jobs", "hosting_idempotency", "hosting_events"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s count=%d err=%v", table, count, err)
		}
	}
	var freeCPU int64
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, runnerID).Scan(&freeCPU); err != nil || freeCPU != limits.CPUMillis {
		t.Fatalf("runner capacity changed after source failure: free=%d err=%v", freeCPU, err)
	}
}

func TestHostingDeploymentConcurrentReplayCrossesSourceExpiryWithOneRedemption(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	project, _, _, err := upsertHostingProject(t.Context(), "project_01JSRCIDEM", validHostingManifest("node"))
	if err != nil {
		t.Fatal(err)
	}
	token, err := createServiceToken("source-idempotency-writer", []string{serviceScopeDeploymentsWrite})
	if err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "source-idempotency-runner", limits)
	body := sourceBrokerTestArchive(t, "concurrent immutable source")
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		once.Do(func() { close(started) })
		<-release
		writeSourceBrokerResponse(w, r, body)
	}))
	defer server.Close()
	appConfig.HostingSourceBrokerURL = server.URL
	appConfig.HostingSourceBrokerToken = "broker-token"
	appConfig.HostingSourceBrokerTimeout = time.Second
	appConfig.HostingSourceMaxBytes = 1 << 20
	appConfig.ArtifactRetentionHours = 1

	request := validHostingDeploymentRequest("deployment_01JSRCIDEM")
	request.ManifestDigest = project.ManifestDigest
	request.ArtifactDigest = sourceBrokerTestDigest(body)
	request.SourceReference.ExpiresAt = time.Now().UTC().Add(250 * time.Millisecond)
	type outcome struct {
		result *hostingCreateResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	create := func() {
		result, err := createHostingDeployment(context.Background(), token, project.ExternalProjectID, "create_01JSRCIDEM", request)
		outcomes <- outcome{result: result, err: err}
	}
	go create()
	<-started
	go create()
	cleanupDone := make(chan struct{})
	go func() {
		cleanupRuntimeState(appConfig)
		close(cleanupDone)
	}()
	select {
	case <-cleanupDone:
		t.Fatal("artifact cleanup was not fenced while source publication was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	time.Sleep(time.Until(request.SourceReference.ExpiresAt.Add(25 * time.Millisecond)))
	close(release)
	for i := 0; i < 2; i++ {
		outcome := <-outcomes
		if outcome.err != nil || outcome.result == nil || outcome.result.Deployment.ExternalDeploymentID != request.ExternalDeploymentID {
			t.Fatalf("concurrent replay outcome=%+v", outcome)
		}
	}
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("artifact cleanup did not resume after deployment commit")
	}
	if calls != 1 {
		t.Fatalf("source broker redemptions=%d, want 1", calls)
	}
	var path string
	if err := db.QueryRow(`SELECT source_artifact_path FROM hosting_jobs`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("committed source artifact was lost during cleanup: %v", err)
	}
}

func TestInternalDeploymentCannotReadOrExecuteLegacyObjects(t *testing.T) {
	withTempDB(t)
	legacy := &Project{Name: "trusted", RepoPath: t.TempDir(), DeployDir: t.TempDir(), DeployMode: "files", PostDeploy: "touch forbidden"}
	if err := createProject(legacy); err != nil {
		t.Fatalf("create legacy project: %v", err)
	}
	build, err := createBuild(legacy.ID, "manual")
	if err != nil {
		t.Fatalf("create legacy build: %v", err)
	}
	token, err := createServiceToken("hosting-boundary", []string{serviceScopeDeploymentsRead, serviceScopeDeploymentsWrite})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	handler := serviceTokenAuthMiddleware(http.HandlerFunc(handleInternalAPI))
	request := httptest.NewRequest(http.MethodGet, "/api/internal/v1/deployments/"+itoa(build.ID), nil)
	request.Header.Set("Authorization", "Bearer "+token.Token)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	assertAPIErrorCode(t, recorder, http.StatusNotFound, errCodeDeploymentNotFound)

	request = httptest.NewRequest(http.MethodPost, "/api/internal/v1/projects/"+itoa(legacy.ID)+"/deployments", bytes.NewReader([]byte(`{}`)))
	request.Header.Set("Authorization", "Bearer "+token.Token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "idem_legacy_01")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	assertAPIErrorCode(t, recorder, http.StatusNotFound, errCodeProjectNotFound)
}

func TestHostingMigrationsCreateDurableLifecycleSchema(t *testing.T) {
	withTempDB(t)
	for table, columns := range map[string][]string{
		"hosting_projects":                             {"desired_state", "kill_switch_reason", "runner_selector_json", "route_generation"},
		"hosting_deployments":                          {"phase", "failure_code", "cancel_requested_at", "request_hash"},
		"hosting_idempotency":                          {"issuer_token_id", "operation", "request_hash", "response_body"},
		"hosting_jobs":                                 {"lease_generation", "lease_expires_at", "cancel_requested_at", "completion_fingerprint"},
		"hosting_releases":                             {"health_evidence_json", "route_revision", "runtime_endpoint", "runtime_runner_id", "runtime_generation", "runtime_instance_id", "runtime_manifest_json"},
		"hosting_runners":                              {"operation_capabilities_json"},
		"hosting_runtime_recoveries":                   {"hosting_release_id", "hosting_runner_id", "lease_generation", "lease_token_hash", "lease_expires_at", "completion_fingerprint"},
		"hosting_runtime_recovery_completion_receipts": {"hosting_runtime_recovery_id", "lease_generation", "hosting_runner_id", "lease_token_hash", "completion_fingerprint", "accepted_at"},
		"hosting_proxy_operations":                     {"hosting_runtime_recovery_id", "operation_type", "status", "expected_previous_runtime_endpoint", "route_generation", "desired_state"},
		"callback_outbox":                              {"event_id", "payload_hash", "next_attempt_at"},
		"service_token_credentials":                    {"token_hash", "expires_at", "revoked_at"},
		"service_token_audit_events":                   {"event_type", "actor", "metadata_json"},
	} {
		for _, column := range columns {
			exists, err := columnExists(table, column)
			if err != nil || !exists {
				t.Fatalf("expected %s.%s, exists=%v err=%v", table, column, exists, err)
			}
		}
	}
	for _, migrationID := range []string{"017_hosting_lifecycle", "018_hosting_idempotency_events", "019_hosting_runners_jobs", "020_hosting_releases_callbacks", "021_service_credentials_audit", "022_hosting_release_runtime_endpoint", "023_callback_outbox_leases", "024_hosting_job_source_artifact", "025_hosting_audit_events", "026_hosting_recovery_invariants", "027_hosting_release_artifacts", "028_hosting_runtime_recovery", "029_hosting_runtime_capabilities", "030_hosting_recovery_completion_fingerprint", "031_hosting_recovery_completion_receipts", "032_hosting_job_completion_fingerprint", "033_hosting_release_runtime_snapshot", "034_hosting_proxy_previous_runtime", "035_hosting_proxy_route_generation"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE id=?`, migrationID).Scan(&count); err != nil || count != 1 {
			t.Fatalf("migration %s count=%d err=%v", migrationID, count, err)
		}
	}
}

func TestMigrationsAreIdempotentUnderConcurrentStartup(t *testing.T) {
	withTempDB(t)
	const workers = 12
	errorsChannel := make(chan error, workers)
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := applyMigrations(); err != nil {
				errorsChannel <- err
				return
			}
			errorsChannel <- applyHostingMigrations()
		}()
	}
	wait.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatalf("concurrent migration: %v", err)
		}
	}
	var duplicates int
	if err := db.QueryRow(`SELECT COUNT(*) FROM (SELECT id FROM schema_migrations GROUP BY id HAVING COUNT(*)<>1)`).Scan(&duplicates); err != nil {
		t.Fatal(err)
	}
	if duplicates != 0 {
		t.Fatalf("migration IDs with duplicate records=%d", duplicates)
	}
}
