package app

import (
	"bytes"
	"context"
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
		VALUES (?, ?, '["linux"]', 'v1', '["v1"]', '["20","22"]', '["build","restore","runtime-secrets-v1"]',
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
	firstBody := recorder.Body.String()
	firstLocation := recorder.Header().Get("Location")
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
	if recorder.Body.String() != firstBody || recorder.Header().Get("Location") != firstLocation {
		t.Fatalf("replay changed original response: first_location=%q replay_location=%q first_body=%q replay_body=%q",
			firstLocation, recorder.Header().Get("Location"), firstBody, recorder.Body.String())
	}
	var storedResponse string
	if err := db.QueryRow(`SELECT response_body FROM hosting_idempotency
		WHERE issuer_token_id=? AND operation='hosting.deployment.create' AND idempotency_key=?`,
		token.ID, "idem_01JDEPLOY").Scan(&storedResponse); err != nil {
		t.Fatal(err)
	}
	if storedResponse != firstBody {
		t.Fatalf("stored response differs from first wire response: stored=%q first=%q", storedResponse, firstBody)
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

func TestInternalDeploymentRejectsExecutionEscapeFieldsAndBadRequestEnvelopes(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JINPUTBOUND")
	handler := serviceTokenAuthMiddleware(http.HandlerFunc(handleInternalAPI))
	base := validHostingDeploymentRequest("deployment_01JINPUTBOUND")
	base.ManifestDigest = project.ManifestDigest
	encoded, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	for field, value := range map[string]any{
		"command":      "curl attacker.invalid",
		"dockerfile":   "FROM malicious",
		"compose":      map[string]any{"services": map[string]any{}},
		"host_path":    "/etc",
		"host_mounts":  []string{"/:/host"},
		"host_port":    443,
		"privileged":   true,
		"capabilities": []string{"SYS_ADMIN"},
		"build_args":   map[string]string{"TOKEN": "plaintext"},
		"environment":  map[string]string{"TOKEN": "plaintext"},
		"post_deploy":  "touch /owned",
	} {
		t.Run(field, func(t *testing.T) {
			payload := make(map[string]any, len(document)+1)
			for key, existing := range document {
				payload[key] = existing
			}
			payload[field] = value
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/api/internal/v1/projects/"+project.ExternalProjectID+"/deployments", bytes.NewReader(body))
			request.Header.Set("Authorization", "Bearer "+token.Token)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set(idempotencyKeyHeader, "idem_01JINPUTBOUND")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			assertAPIErrorCode(t, recorder, http.StatusBadRequest, errCodeValidation)
		})
	}

	for name, testCase := range map[string]struct {
		contentType string
		body        []byte
		status      int
		code        string
	}{
		"missing content type": {body: encoded, status: http.StatusUnsupportedMediaType, code: errCodeUnsupportedMediaType},
		"oversized":            {contentType: "application/json", body: bytes.Repeat([]byte(" "), (32<<10)+1), status: http.StatusRequestEntityTooLarge, code: errCodePayloadTooLarge},
		"malformed":            {contentType: "application/json", body: []byte(`{"external_deployment_id":`), status: http.StatusBadRequest, code: errCodeValidation},
		"multiple values":      {contentType: "application/json", body: append(append([]byte(nil), encoded...), []byte(` {}`)...), status: http.StatusBadRequest, code: errCodeValidation},
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/internal/v1/projects/"+project.ExternalProjectID+"/deployments", bytes.NewReader(testCase.body))
			request.Header.Set("Authorization", "Bearer "+token.Token)
			if testCase.contentType != "" {
				request.Header.Set("Content-Type", testCase.contentType)
			}
			request.Header.Set(idempotencyKeyHeader, "idem_01JINPUTBOUND")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			assertAPIErrorCode(t, recorder, testCase.status, testCase.code)
			if recorder.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("error content type=%q", recorder.Header().Get("Content-Type"))
			}
		})
	}

	var deployments, idempotency int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_deployments`).Scan(&deployments); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_idempotency`).Scan(&idempotency); err != nil {
		t.Fatal(err)
	}
	if deployments != 0 || idempotency != 0 {
		t.Fatalf("rejected requests persisted deployments=%d idempotency=%d", deployments, idempotency)
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

func TestHostingDeploymentConcurrentIdempotencyConflictDoesNoDuplicateWork(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JHASHRACE")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "hosting-hash-race", limits)
	var sourceCalls atomic.Int64
	oldPreparer := prepareHostingSource
	prepareHostingSource = func(context.Context, *HostingProject, HostingSourceReference, string, string) (string, string, error) {
		sourceCalls.Add(1)
		return "/managed/hash-race.tar", "sha256:" + strings.Repeat("c", 64), nil
	}
	t.Cleanup(func() { prepareHostingSource = oldPreparer })

	requests := []hostingDeploymentCreateRequest{
		validHostingDeploymentRequest("deployment_01JHASHR1"),
		validHostingDeploymentRequest("deployment_01JHASHR2"),
	}
	for index := range requests {
		requests[index].ManifestDigest = project.ManifestDigest
	}
	results := make(chan error, len(requests))
	var wait sync.WaitGroup
	for index := range requests {
		request := requests[index]
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := createHostingDeployment(context.Background(), token, project.ExternalProjectID, "idem_01JHASHRACE", request)
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	accepted, conflicts := 0, 0
	for err := range results {
		if err == nil {
			accepted++
			continue
		}
		var apiErr *hostingAPIError
		if errors.As(err, &apiErr) && apiErr.Code == errCodeIdempotencyConflict {
			conflicts++
			continue
		}
		t.Fatalf("unexpected concurrent result: %v", err)
	}
	if accepted != 1 || conflicts != 1 || sourceCalls.Load() != 1 {
		t.Fatalf("accepted=%d conflicts=%d source_calls=%d", accepted, conflicts, sourceCalls.Load())
	}
	for table, expected := range map[string]int{"hosting_deployments": 1, "hosting_jobs": 1, "hosting_idempotency": 1} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != expected {
			t.Fatalf("%s count=%d want=%d err=%v", table, count, expected, err)
		}
	}
}

func TestHostingDeploymentRejectsManifestChangedDuringSourcePreparation(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	project, _, _, err := upsertHostingProject(t.Context(), "project_01JMANIFESTRACE", validHostingManifest("node"))
	if err != nil {
		t.Fatalf("provision hosting project: %v", err)
	}
	token, err := createServiceToken("deployment-writer-manifest-race", []string{serviceScopeDeploymentsWrite})
	if err != nil {
		t.Fatalf("create service token: %v", err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	runnerID := insertHostingRunnerForTest(t, "hosting-manifest-race", limits)

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseSource := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseSource()
	oldPreparer := prepareHostingSource
	prepareHostingSource = func(context.Context, *HostingProject, HostingSourceReference, string, string) (string, string, error) {
		close(started)
		<-release
		return "/managed/manifest-race.tar", "sha256:" + strings.Repeat("c", 64), nil
	}
	t.Cleanup(func() { prepareHostingSource = oldPreparer })

	request := validHostingDeploymentRequest("deployment_01JMANIFESTRACE")
	request.ManifestDigest = project.ManifestDigest
	result := make(chan error, 1)
	go func() {
		_, err := createHostingDeployment(context.Background(), token, project.ExternalProjectID, "idem_01JMANIFESTRACE", request)
		result <- err
	}()
	<-started

	updatedManifest := validHostingManifest("node")
	updatedManifest.ResourceProfile = "standard"
	updated, created, replayed, err := upsertHostingProject(t.Context(), project.ExternalProjectID, updatedManifest)
	if err != nil {
		t.Fatalf("update project while source is in flight: %v", err)
	}
	if created || replayed || updated.ManifestDigest == project.ManifestDigest {
		t.Fatalf("project update did not commit a distinct manifest: created=%v replayed=%v project=%+v", created, replayed, updated)
	}
	releaseSource()

	err = <-result
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeManifestDigestMismatch || apiErr.StatusCode != http.StatusConflict {
		t.Fatalf("stale deployment error=%v", err)
	}
	for _, table := range []string{"hosting_deployments", "hosting_jobs", "hosting_idempotency", "hosting_events"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("stale deployment created %d rows in %s", count, table)
		}
	}
	var freeCPU int64
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, runnerID).Scan(&freeCPU); err != nil {
		t.Fatal(err)
	}
	if freeCPU != limits.CPUMillis {
		t.Fatalf("stale deployment reserved runner capacity: free_cpu_millis=%d want=%d", freeCPU, limits.CPUMillis)
	}
}

func TestHostingDeploymentSerializesDifferentKeysPerProject(t *testing.T) {
	for _, separateIssuers := range []bool{false, true} {
		name := "same issuer"
		if separateIssuers {
			name = "separate issuers"
		}
		t.Run(name, func(t *testing.T) {
			withTempDB(t)
			project, firstToken := provisionDeploymentTestProject(t, "project_01JPROJECTRACE")
			secondToken := firstToken
			if separateIssuers {
				var err error
				secondToken, err = createServiceToken("deployment-writer-project-race-second", []string{serviceScopeDeploymentsWrite})
				if err != nil {
					t.Fatal(err)
				}
			}
			limits, _ := hostingLimitsForProfile(project.ResourceProfile)
			insertHostingRunnerForTest(t, "hosting-project-race", limits)
			var sourceCalls atomic.Int64
			oldPreparer := prepareHostingSource
			prepareHostingSource = func(context.Context, *HostingProject, HostingSourceReference, string, string) (string, string, error) {
				sourceCalls.Add(1)
				return "/managed/project-race.tar", "sha256:" + strings.Repeat("c", 64), nil
			}
			t.Cleanup(func() { prepareHostingSource = oldPreparer })
			requests := []hostingDeploymentCreateRequest{
				validHostingDeploymentRequest("deployment_01JPROJECTR1"),
				validHostingDeploymentRequest("deployment_01JPROJECTR2"),
			}
			for index := range requests {
				requests[index].ManifestDigest = project.ManifestDigest
			}
			tokens := []*ServiceToken{firstToken, secondToken}
			results := make(chan error, 2)
			var wait sync.WaitGroup
			for index := range requests {
				request, token := requests[index], tokens[index]
				wait.Add(1)
				go func(index int) {
					defer wait.Done()
					key := fmt.Sprintf("idem_01JPROJECTRACE_%d", index)
					if separateIssuers {
						key = "idem_01JPROJECTRACE"
					}
					_, err := createHostingDeployment(context.Background(), token, project.ExternalProjectID, key, request)
					results <- err
				}(index)
			}
			wait.Wait()
			close(results)
			accepted, conflicts := 0, 0
			for err := range results {
				if err == nil {
					accepted++
					continue
				}
				var apiErr *hostingAPIError
				if errors.As(err, &apiErr) && apiErr.Code == errCodeExternalDeploymentConflict {
					conflicts++
					continue
				}
				t.Fatalf("unexpected concurrent result: %v", err)
			}
			if accepted != 1 || conflicts != 1 || sourceCalls.Load() != 1 {
				t.Fatalf("accepted=%d conflicts=%d source_calls=%d", accepted, conflicts, sourceCalls.Load())
			}
		})
	}
}

func TestHostingDeploymentReplaySurvivesRotationExpiryAndRestart(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JREPLAYDB")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "hosting-replay-db", limits)
	var sourceCalls atomic.Int64
	oldPreparer := prepareHostingSource
	prepareHostingSource = func(context.Context, *HostingProject, HostingSourceReference, string, string) (string, string, error) {
		sourceCalls.Add(1)
		return "/managed/replay-db.tar", "sha256:" + strings.Repeat("c", 64), nil
	}
	t.Cleanup(func() { prepareHostingSource = oldPreparer })
	request := validHostingDeploymentRequest("deployment_01JREPLAYDB")
	request.ManifestDigest = project.ManifestDigest
	request.SourceReference.ExpiresAt = time.Now().UTC().Add(150 * time.Millisecond)
	first, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "idem_01JREPLAYDB", request)
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := rotateServiceToken(token.ID)
	if err != nil {
		t.Fatal(err)
	}
	if rotated.ID != token.ID {
		t.Fatalf("rotation changed issuer identity: before=%d after=%d", token.ID, rotated.ID)
	}
	if _, err := db.Exec(`UPDATE hosting_deployments SET status='running', phase='building' WHERE id=?`, first.Deployment.ID); err != nil {
		t.Fatal(err)
	}
	var databaseSequence int
	var databaseName, databasePath string
	if err := db.QueryRow(`PRAGMA database_list`).Scan(&databaseSequence, &databaseName, &databasePath); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(request.SourceReference.ExpiresAt.Add(10 * time.Millisecond)))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := initDB(databasePath); err != nil {
		t.Fatal(err)
	}
	replayed, err := createHostingDeployment(t.Context(), rotated, project.ExternalProjectID, "idem_01JREPLAYDB", request)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || replayed.StatusCode != http.StatusAccepted || replayed.Deployment.Status != hostingStatusQueued {
		t.Fatalf("replay=%+v", replayed)
	}
	if sourceCalls.Load() != 1 {
		t.Fatalf("source redemptions=%d, want 1", sourceCalls.Load())
	}
	var issuerID int64
	var operation, requestHash, responseBody string
	var responseStatus int
	if err := db.QueryRow(`SELECT issuer_token_id, operation, request_hash, response_status, response_body
		FROM hosting_idempotency WHERE idempotency_key='idem_01JREPLAYDB'`).Scan(
		&issuerID, &operation, &requestHash, &responseStatus, &responseBody); err != nil {
		t.Fatal(err)
	}
	if issuerID != token.ID || operation != "hosting.deployment.create" || !validSHA256Digest(requestHash) || responseStatus != http.StatusAccepted {
		t.Fatalf("stored idempotency issuer=%d operation=%q hash=%q status=%d", issuerID, operation, requestHash, responseStatus)
	}
	var stored HostingDeployment
	if err := json.Unmarshal([]byte(responseBody), &stored); err != nil || stored.Status != hostingStatusQueued ||
		stored.ExternalDeploymentID != first.Deployment.ExternalDeploymentID {
		t.Fatalf("stored original response=%+v err=%v", stored, err)
	}
}

func TestHostingDeploymentCanonicalizesSecretDefaultsAndReferenceTimes(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JCANONICAL")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "hosting-canonical", limits)
	appConfig.HostingWorkloadIdentitySecret = strings.Repeat("canonical-identity-key-", 2)
	var sourceCalls atomic.Int64
	oldPreparer := prepareHostingSource
	prepareHostingSource = func(context.Context, *HostingProject, HostingSourceReference, string, string) (string, string, error) {
		sourceCalls.Add(1)
		return "/managed/canonical.tar", "sha256:" + strings.Repeat("c", 64), nil
	}
	t.Cleanup(func() { prepareHostingSource = oldPreparer })

	request := validHostingDeploymentRequest("deployment_01JCANONICAL")
	request.ManifestDigest = project.ManifestDigest
	zone := time.FixedZone("request-offset", 2*60*60)
	request.SourceReference.ExpiresAt = request.SourceReference.ExpiresAt.In(zone)
	request.SecretReferences = []HostingSecretReference{{
		Provider: "control-plane", Reference: "reference-canonical-secret",
		ExpiresAt: time.Now().UTC().Add(20 * time.Minute).In(zone),
	}}
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "idem_01JCANONICAL", request); err != nil {
		t.Fatal(err)
	}
	equivalent := request
	equivalent.SourceReference.ExpiresAt = equivalent.SourceReference.ExpiresAt.UTC()
	equivalent.SecretReferences = append([]HostingSecretReference(nil), request.SecretReferences...)
	equivalent.SecretReferences[0].Name = "SECRET_1"
	equivalent.SecretReferences[0].ExpiresAt = equivalent.SecretReferences[0].ExpiresAt.UTC()
	result, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "idem_01JCANONICAL", equivalent)
	if err != nil || !result.Replayed {
		t.Fatalf("canonical replay=%+v err=%v", result, err)
	}
	if sourceCalls.Load() != 1 {
		t.Fatalf("source redemptions=%d, want 1", sourceCalls.Load())
	}
}

func TestHostingDeploymentReplaysPreCanonicalizationRowsAcrossUpgrade(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JLEGACYIDEM")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "hosting-legacy-idem", limits)
	appConfig.HostingWorkloadIdentitySecret = strings.Repeat("legacy-idempotency-key-", 2)
	var sourceCalls atomic.Int64
	oldPreparer := prepareHostingSource
	prepareHostingSource = func(context.Context, *HostingProject, HostingSourceReference, string, string) (string, string, error) {
		sourceCalls.Add(1)
		return "/managed/legacy-idem.tar", "sha256:" + strings.Repeat("c", 64), nil
	}
	t.Cleanup(func() { prepareHostingSource = oldPreparer })

	request := validHostingDeploymentRequest("deployment_01JLEGACYIDEM")
	request.ManifestDigest = project.ManifestDigest
	zone := time.FixedZone("legacy-request-offset", -5*60*60)
	request.SourceReference.ExpiresAt = request.SourceReference.ExpiresAt.In(zone)
	request.SecretReferences = []HostingSecretReference{{
		Provider: "control-plane", Reference: "reference-legacy-secret",
		ExpiresAt: time.Now().UTC().Add(20 * time.Minute).In(zone),
	}}
	legacyHash, err := hashHostingDeploymentRequest(project.ExternalProjectID, request)
	if err != nil {
		t.Fatal(err)
	}
	first, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "idem_01JLEGACYIDEM", request)
	if err != nil {
		t.Fatal(err)
	}
	legacyBody := strings.TrimSuffix(string(first.ResponseBody), "\n")
	if _, err := db.Exec(`UPDATE hosting_idempotency SET request_hash=?, response_body=?
		WHERE issuer_token_id=? AND operation='hosting.deployment.create' AND idempotency_key=?`,
		legacyHash, legacyBody, token.ID, "idem_01JLEGACYIDEM"); err != nil {
		t.Fatal(err)
	}
	replayed, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "idem_01JLEGACYIDEM", request)
	if err != nil || !replayed.Replayed || string(replayed.ResponseBody) != string(first.ResponseBody) {
		t.Fatalf("legacy rolling-upgrade replay=%+v err=%v", replayed, err)
	}
	if sourceCalls.Load() != 1 {
		t.Fatalf("source redemptions=%d, want 1", sourceCalls.Load())
	}
}

func TestHostingIdempotencyExpiryAllowsKeyReuse(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JIDEMEXP")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "hosting-idem-expiry", limits)
	first := validHostingDeploymentRequest("deployment_01JIDEMEXP1")
	first.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "idem_01JIDEMEXP", first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cancelHostingDeployment(t.Context(), token, first.ExternalDeploymentID, "idem_01JIDEMEXP"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_idempotency SET expires_at=?
		WHERE issuer_token_id=? AND operation='hosting.deployment.create' AND idempotency_key=?`,
		formatSQLiteTime(time.Now().UTC().Add(-time.Second)), token.ID, "idem_01JIDEMEXP"); err != nil {
		t.Fatal(err)
	}
	second := validHostingDeploymentRequest("deployment_01JIDEMEXP2")
	second.ManifestDigest = project.ManifestDigest
	result, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "idem_01JIDEMEXP", second)
	if err != nil || result.Replayed || result.Deployment.ExternalDeploymentID != second.ExternalDeploymentID {
		t.Fatalf("expired key reuse result=%+v err=%v", result, err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_idempotency
		WHERE issuer_token_id=? AND operation='hosting.deployment.create' AND idempotency_key=?`, token.ID, "idem_01JIDEMEXP").Scan(&count); err != nil || count != 1 {
		t.Fatalf("replacement idempotency rows=%d err=%v", count, err)
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

func TestHostingDeploymentValidatesSecretNamesAndDuplicates(t *testing.T) {
	base := validHostingDeploymentRequest("deployment_01JSECNAMES")
	base.SecretReferences = []HostingSecretReference{{
		Provider: "control-plane", Reference: "reference-database-url", Name: "DATABASE_URL", ExpiresAt: time.Now().Add(time.Minute),
	}}
	if err := validateHostingDeploymentRequest(&base); err != nil {
		t.Fatalf("valid secret reference rejected: %v", err)
	}
	for name, mutate := range map[string]func(*hostingDeploymentCreateRequest){
		"unsafe name": func(request *hostingDeploymentCreateRequest) {
			request.SecretReferences[0].Name = "../DATABASE_URL"
		},
		"duplicate name": func(request *hostingDeploymentCreateRequest) {
			request.SecretReferences = append(request.SecretReferences, HostingSecretReference{
				Provider: "control-plane", Reference: "reference-other-secret", Name: "DATABASE_URL", ExpiresAt: time.Now().Add(time.Minute),
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := base
			request.SecretReferences = append([]HostingSecretReference(nil), base.SecretReferences...)
			mutate(&request)
			if err := validateHostingDeploymentRequest(&request); err == nil {
				t.Fatal("invalid secret reference accepted")
			}
		})
	}
}

func TestStaticHostingDeploymentRejectsSecretsBeforeSourceRedemption(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	project, _, _, err := upsertHostingProject(t.Context(), "project_01JSTATICSEC", validHostingManifest("static"))
	if err != nil {
		t.Fatal(err)
	}
	token, err := createServiceToken("static-secret-writer", []string{serviceScopeDeploymentsWrite})
	if err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "static-secret-runner", limits)
	brokerCalls := 0
	oldPreparer := prepareHostingSource
	prepareHostingSource = func(context.Context, *HostingProject, HostingSourceReference, string, string) (string, string, error) {
		brokerCalls++
		return "", "", errors.New("must not be called")
	}
	t.Cleanup(func() { prepareHostingSource = oldPreparer })
	request := validHostingDeploymentRequest("deployment_01JSTATICSEC")
	request.ManifestDigest = project.ManifestDigest
	request.SecretReferences = []HostingSecretReference{{
		Provider: "control-plane", Reference: "reference-database-url", Name: "DATABASE_URL", ExpiresAt: time.Now().Add(time.Minute),
	}}
	_, err = createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JSTATICSEC", request)
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeInvalidDeployment || brokerCalls != 0 {
		t.Fatalf("static secret admission err=%v broker_calls=%d", err, brokerCalls)
	}
}

func TestNodeHostingDeploymentRejectsSecretsWithoutIdentitySigningBeforeSourceRedemption(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	project, _, _, err := upsertHostingProject(t.Context(), "project_01JNODESEC", validHostingManifest("node"))
	if err != nil {
		t.Fatal(err)
	}
	token, err := createServiceToken("node-secret-writer", []string{serviceScopeDeploymentsWrite})
	if err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "node-secret-runner", limits)
	brokerCalls := 0
	oldPreparer := prepareHostingSource
	prepareHostingSource = func(context.Context, *HostingProject, HostingSourceReference, string, string) (string, string, error) {
		brokerCalls++
		return "", "", errors.New("must not be called")
	}
	t.Cleanup(func() { prepareHostingSource = oldPreparer })
	appConfig.HostingWorkloadIdentitySecret = ""
	request := validHostingDeploymentRequest("deployment_01JNODESEC")
	request.ManifestDigest = project.ManifestDigest
	request.SecretReferences = []HostingSecretReference{{
		Provider: "control-plane", Reference: "reference-database-url", Name: "DATABASE_URL", ExpiresAt: time.Now().Add(time.Minute),
	}}
	_, err = createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JNODESEC", request)
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeSecretReferenceUnavailable || apiErr.StatusCode != http.StatusServiceUnavailable || brokerCalls != 0 {
		t.Fatalf("missing signing configuration err=%v broker_calls=%d", err, brokerCalls)
	}
}

func TestSecretHostingDeploymentRequiresSecretReadyRunnerBeforeSourceRedemption(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JSECCAP")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	runnerID := insertHostingRunnerForTest(t, "legacy-secret-runner", limits)
	if _, err := db.Exec(`UPDATE hosting_runners SET operation_capabilities_json='["build","restore"]' WHERE id=?`, runnerID); err != nil {
		t.Fatal(err)
	}
	appConfig.HostingWorkloadIdentitySecret = strings.Repeat("identity-signing-key-", 2)
	brokerCalls := 0
	oldPreparer := prepareHostingSource
	prepareHostingSource = func(context.Context, *HostingProject, HostingSourceReference, string, string) (string, string, error) {
		brokerCalls++
		return "", "", errors.New("must not be called")
	}
	t.Cleanup(func() { prepareHostingSource = oldPreparer })
	request := validHostingDeploymentRequest("deployment_01JSECCAP")
	request.ManifestDigest = project.ManifestDigest
	request.SecretReferences = []HostingSecretReference{{
		Provider: "control-plane", Reference: "reference-database-url", Name: "DATABASE_URL", ExpiresAt: time.Now().Add(time.Minute),
	}}
	_, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JSECCAP", request)
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != errCodeRunnerCapacityUnavailable || brokerCalls != 0 {
		t.Fatalf("secret capability admission err=%v broker_calls=%d", err, brokerCalls)
	}
}

func TestSecretHostingJobIsFencedAndReassignedAfterCapabilityRemoval(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JSECREASSIGN")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	firstRunnerID := insertHostingRunnerForTest(t, "secret-runner-first", limits)
	appConfig.HostingWorkloadIdentitySecret = strings.Repeat("identity-signing-key-", 2)
	request := validHostingDeploymentRequest("deployment_01JSECREASSIGN")
	request.ManifestDigest = project.ManifestDigest
	request.SecretReferences = []HostingSecretReference{{
		Provider: "control-plane", Reference: "reference-database-url", Name: "DATABASE_URL", ExpiresAt: time.Now().Add(time.Minute),
	}}
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JSECREASSIGN", request); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_runners SET operation_capabilities_json='["build","restore"]' WHERE id=?`, firstRunnerID); err != nil {
		t.Fatal(err)
	}
	if _, err := claimHostingJob(t.Context(), firstRunnerID); !errors.Is(err, errNoHostingJob) {
		t.Fatalf("legacy runner claimed secret job: %v", err)
	}
	secondRunnerID := insertHostingRunnerForTest(t, "secret-runner-second", limits)
	if err := reconcileHostingState(t.Context(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var assignedRunnerID int64
	if err := db.QueryRow(`SELECT hosting_runner_id FROM hosting_jobs LIMIT 1`).Scan(&assignedRunnerID); err != nil {
		t.Fatal(err)
	}
	if assignedRunnerID != secondRunnerID {
		t.Fatalf("secret job assigned runner=%d want=%d", assignedRunnerID, secondRunnerID)
	}
}

func TestLegacySecretReferenceNameDefaultsWithoutChangingRequestEncoding(t *testing.T) {
	type legacyReference struct {
		Provider  string    `json:"provider"`
		Reference string    `json:"reference"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	expires := time.Now().UTC().Add(time.Minute)
	legacy, err := json.Marshal(legacyReference{Provider: "control-plane", Reference: "reference-database-url", ExpiresAt: expires})
	if err != nil {
		t.Fatal(err)
	}
	current := HostingSecretReference{Provider: "control-plane", Reference: "reference-database-url", ExpiresAt: expires}
	encoded, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != string(legacy) {
		t.Fatalf("legacy request encoding changed: legacy=%s current=%s", legacy, encoded)
	}
	refs := []HostingSecretReference{current}
	if err := normalizeHostingSecretReferences(refs); err != nil || refs[0].Name != "SECRET_1" {
		t.Fatalf("legacy secret name normalization=%+v err=%v", refs, err)
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
	if strings.Contains(recipeJSON, project.DeployPath) || strings.Contains(recipeJSON, `"release_root"`) {
		t.Fatalf("hosting recipe leaked Deployer host path: %s", recipeJSON)
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
		"hosting_jobs":                                 {"lease_generation", "lease_expires_at", "cancel_requested_at", "completion_fingerprint", "runtime_observed_at", "runtime_observed_session_id", "runtime_observed_release_digest", "runtime_observed_instance_id"},
		"hosting_releases":                             {"health_evidence_json", "route_revision", "runtime_endpoint", "runtime_runner_id", "runtime_generation", "runtime_instance_id", "runtime_manifest_json", "runtime_observed_at", "runtime_observed_session_id", "runtime_missing_since", "runtime_missing_observations", "runtime_failure_code"},
		"hosting_runners":                              {"operation_capabilities_json", "active_session_id", "last_heartbeat_sequence"},
		"hosting_runtime_recoveries":                   {"hosting_release_id", "hosting_runner_id", "lease_generation", "lease_token_hash", "lease_expires_at", "completion_fingerprint", "runtime_owner_runner_id", "allow_owner_runner", "runtime_observed_at", "runtime_observed_session_id", "runtime_observed_release_digest", "runtime_observed_instance_id"},
		"hosting_runtime_recovery_completion_receipts": {"hosting_runtime_recovery_id", "lease_generation", "hosting_runner_id", "lease_token_hash", "completion_fingerprint", "accepted_at"},
		"hosting_proxy_operations":                     {"hosting_runtime_recovery_id", "operation_type", "status", "expected_previous_runtime_endpoint", "route_generation", "desired_state", "target_runtime_runner_id", "target_runtime_instance_id", "target_runtime_session_id"},
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
	for _, migrationID := range []string{"017_hosting_lifecycle", "018_hosting_idempotency_events", "019_hosting_runners_jobs", "020_hosting_releases_callbacks", "021_service_credentials_audit", "022_hosting_release_runtime_endpoint", "023_callback_outbox_leases", "024_hosting_job_source_artifact", "025_hosting_audit_events", "026_hosting_recovery_invariants", "027_hosting_release_artifacts", "028_hosting_runtime_recovery", "029_hosting_runtime_capabilities", "030_hosting_recovery_completion_fingerprint", "031_hosting_recovery_completion_receipts", "032_hosting_job_completion_fingerprint", "033_hosting_release_runtime_snapshot", "034_hosting_proxy_previous_runtime", "035_hosting_proxy_route_generation", "036_hosting_runtime_inventory", "037_hosting_runner_session_handoff"} {
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
