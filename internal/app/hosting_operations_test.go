package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRollbackAndKillSwitchRejectBadRequestEnvelopes(t *testing.T) {
	withTempDB(t)
	projectToken := &ServiceToken{ID: 1, Scopes: []string{serviceScopeDeploymentsWrite, serviceScopeProjectsWrite, serviceScopeHostingAdmin}}
	type endpointCase struct {
		serve       func(http.ResponseWriter, *http.Request)
		validBody   []byte
		unknownBody []byte
	}
	endpoints := map[string]endpointCase{
		"rollback": {
			serve:       func(w http.ResponseWriter, r *http.Request) { handleInternalRollback(w, r, "project_01JBOUNDARY") },
			validBody:   []byte(`{"external_deployment_id":"deployment_01JBOUNDARY","release_digest":"sha256:` + strings.Repeat("a", 64) + `"}`),
			unknownBody: []byte(`{"external_deployment_id":"deployment_01JBOUNDARY","release_digest":"sha256:` + strings.Repeat("a", 64) + `","command":"touch /owned"}`),
		},
		"kill switch": {
			serve:       handleInternalGlobalKillSwitch,
			validBody:   []byte(`{"enabled":true,"reason":"security response"}`),
			unknownBody: []byte(`{"enabled":true,"reason":"security response","privileged":true}`),
		},
	}
	for endpointName, endpoint := range endpoints {
		for envelopeName, testCase := range map[string]struct {
			contentType string
			body        []byte
			status      int
			code        string
		}{
			"missing content type": {body: endpoint.validBody, status: http.StatusUnsupportedMediaType, code: errCodeUnsupportedMediaType},
			"oversized":            {contentType: "application/json", body: bytes.Repeat([]byte(" "), (8<<10)+1), status: http.StatusRequestEntityTooLarge, code: errCodePayloadTooLarge},
			"malformed":            {contentType: "application/json", body: []byte(`{"broken":`), status: http.StatusBadRequest, code: errCodeValidation},
			"multiple values":      {contentType: "application/json", body: append(append([]byte(nil), endpoint.validBody...), []byte(` {}`)...), status: http.StatusBadRequest, code: errCodeValidation},
			"unknown field":        {contentType: "application/json", body: endpoint.unknownBody, status: http.StatusBadRequest, code: errCodeValidation},
		} {
			t.Run(endpointName+"/"+envelopeName, func(t *testing.T) {
				request := httptest.NewRequest(http.MethodPost, "/boundary", bytes.NewReader(testCase.body))
				request = request.WithContext(context.WithValue(request.Context(), serviceTokenContextKey{}, projectToken))
				request.Header.Set(idempotencyKeyHeader, "boundary_01JTEST")
				if endpointName == "kill switch" {
					request.Method = http.MethodPut
				}
				if testCase.contentType != "" {
					request.Header.Set("Content-Type", testCase.contentType)
				}
				recorder := httptest.NewRecorder()
				endpoint.serve(recorder, request)
				assertAPIErrorCode(t, recorder, testCase.status, testCase.code)
				if recorder.Header().Get("Content-Type") != "application/json" {
					t.Fatalf("error content type=%q", recorder.Header().Get("Content-Type"))
				}
			})
		}
	}
}

func TestProjectKillSwitchCancelsQueuedWorkAndIsIdempotent(t *testing.T) {
	withTempDB(t)
	project, deploymentToken := provisionDeploymentTestProject(t, "project_01JKILLPR")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	runnerID := insertHostingRunnerForTest(t, "kill-project-runner", limits)
	request := validHostingDeploymentRequest("deployment_01JKILLPR")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), deploymentToken, project.ExternalProjectID, "create_01JKILLPR", request); err != nil {
		t.Fatal(err)
	}
	operator, err := createServiceToken("project-kill-operator", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	input := hostingKillSwitchRequest{Enabled: true, Reason: "security incident response"}
	first, replayed, err := setHostingExecutionKillSwitch(t.Context(), operator, project.ExternalProjectID, input, "kill_01JKILLPR", "request-kill-1")
	if err != nil || replayed || !first.Enabled {
		t.Fatalf("first kill switch response=%+v replayed=%v err=%v", first, replayed, err)
	}
	second, replayed, err := setHostingExecutionKillSwitch(t.Context(), operator, project.ExternalProjectID, input, "kill_01JKILLPR", "request-kill-1")
	if err != nil || !replayed || second.Enabled != first.Enabled {
		t.Fatalf("replay response=%+v replayed=%v err=%v", second, replayed, err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), request.ExternalDeploymentID)
	if err != nil || deployment.Status != hostingStatusCancelled || deployment.FailureCode != "cancelled" {
		t.Fatalf("deployment=%+v err=%v", deployment, err)
	}
	var freeCPU int64
	if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, runnerID).Scan(&freeCPU); err != nil || freeCPU != limits.CPUMillis {
		t.Fatalf("free CPU=%d err=%v", freeCPU, err)
	}
	for table, want := range map[string]int{"callback_outbox": 1, "hosting_audit_events": 2} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != want {
			t.Fatalf("%s count=%d err=%v", table, count, err)
		}
	}
	blocked := validHostingDeploymentRequest("deployment_01JBLOCKD")
	blocked.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), deploymentToken, project.ExternalProjectID, "create_01JBLOCKD", blocked); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("kill-switched project accepted deployment: %v", err)
	}
}

func TestGlobalKillSwitchRequiresAdminScopeAndCancelsRunningLease(t *testing.T) {
	withTempDB(t)
	project, _, _, job := createAndClaimHostingJob(t, "project_01JKILLGL", "deployment_01JKILLGL")
	projectWriter, err := createServiceToken("not-global-admin", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	input := hostingKillSwitchRequest{Enabled: true, Reason: "capacity emergency shutdown"}
	body, _ := json.Marshal(input)
	handler := serviceTokenAuthMiddleware(http.HandlerFunc(handleInternalAPI))
	req := httptest.NewRequest(http.MethodPut, "/api/internal/v1/settings/kill-switch", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+projectWriter.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(idempotencyKeyHeader, "global_kill_denied")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assertAPIErrorCode(t, rec, http.StatusForbidden, errCodeInsufficientScope)

	admin, err := createServiceToken("global-admin", []string{serviceScopeHostingAdmin})
	if err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodPut, "/api/internal/v1/settings/kill-switch", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+admin.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(idempotencyKeyHeader, "global_kill_allowed")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("global kill switch = %d body=%s", rec.Code, rec.Body.String())
	}
	var cancelRequested string
	if err := db.QueryRow(`SELECT cancel_requested_at FROM hosting_jobs WHERE id=?`, job.JobID).Scan(&cancelRequested); err != nil || cancelRequested == "" {
		t.Fatalf("running job cancellation intent missing: %q err=%v", cancelRequested, err)
	}
	stored, err := getHostingProjectByExternalID(project.ExternalProjectID)
	if err != nil || stored.KillSwitchReason != "" {
		t.Fatalf("global switch must not mutate project switch: %+v err=%v", stored, err)
	}
	var enabled int
	if err := db.QueryRow(`SELECT global_kill_switch FROM hosting_settings WHERE id=1`).Scan(&enabled); err != nil || enabled != 1 {
		t.Fatalf("global switch=%d err=%v", enabled, err)
	}
}

func TestProjectKillSwitchFencesInFlightActivationAndResumesRouting(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, runnerID, job := createAndClaimHostingJob(t,
		"project_01JKILLRACE", "deployment_01JKILLRACE")
	digest := "sha256:" + strings.Repeat("c", 64)
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
	operator, err := createServiceToken("project-kill-race-operator", []string{serviceScopeProjectsWrite})
	if err != nil {
		close(fake.activateContinue)
		t.Fatal(err)
	}
	if _, replayed, err := setHostingExecutionKillSwitch(t.Context(), operator,
		project.ExternalProjectID, hostingKillSwitchRequest{Enabled: true, Reason: "incident routing fence"},
		"kill_race_enable_01", "request-kill-race-enable"); err != nil || replayed {
		close(fake.activateContinue)
		t.Fatalf("enable replayed=%v err=%v", replayed, err)
	}
	var activateGeneration, suspendGeneration, currentGeneration int64
	var suspendStatus string
	if err := db.QueryRow(`SELECT route_generation FROM hosting_proxy_operations
		WHERE hosting_deployment_id=(SELECT id FROM hosting_deployments WHERE external_deployment_id=?)
		  AND operation_type='activate'`, "deployment_01JKILLRACE").Scan(&activateGeneration); err != nil {
		close(fake.activateContinue)
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT route_generation, status FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='suspend' ORDER BY id DESC LIMIT 1`,
		project.ID).Scan(&suspendGeneration, &suspendStatus); err != nil {
		close(fake.activateContinue)
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT route_generation FROM hosting_projects WHERE id=?`, project.ID).Scan(
		&currentGeneration); err != nil {
		close(fake.activateContinue)
		t.Fatal(err)
	}
	fake.mu.Lock()
	suspended := fake.currentSuspended
	fake.mu.Unlock()
	if suspendGeneration <= activateGeneration || currentGeneration != suspendGeneration ||
		suspendStatus != "committed" || !suspended {
		close(fake.activateContinue)
		t.Fatalf("activate=%d suspend=%d/%s current=%d suspended=%v", activateGeneration,
			suspendGeneration, suspendStatus, currentGeneration, suspended)
	}
	if _, replayed, err := setHostingProjectDesiredState(t.Context(), operator,
		project.ExternalProjectID, "active", "resume_while_killed_01"); err != nil || replayed {
		close(fake.activateContinue)
		t.Fatalf("desired-active while killed replayed=%v err=%v", replayed, err)
	}
	var generationWhileKilled int64
	var resumeWhileKilled int
	if err := db.QueryRow(`SELECT route_generation FROM hosting_projects WHERE id=?`, project.ID).Scan(
		&generationWhileKilled); err != nil {
		close(fake.activateContinue)
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='resume'`, project.ID).Scan(&resumeWhileKilled); err != nil {
		close(fake.activateContinue)
		t.Fatal(err)
	}
	fake.mu.Lock()
	stillSuspended := fake.currentSuspended
	fake.mu.Unlock()
	if generationWhileKilled != suspendGeneration || resumeWhileKilled != 0 || !stillSuspended {
		close(fake.activateContinue)
		t.Fatalf("resume while killed generation=%d want=%d operations=%d suspended=%v",
			generationWhileKilled, suspendGeneration, resumeWhileKilled, stillSuspended)
	}
	close(fake.activateContinue)
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	lateEndpoint, lateSuspended := fake.currentEndpoint, fake.currentSuspended
	fake.mu.Unlock()
	if lateEndpoint != "" || !lateSuspended {
		t.Fatalf("late activation endpoint=%q suspended=%v", lateEndpoint, lateSuspended)
	}
	if _, replayed, err := setHostingExecutionKillSwitch(t.Context(), operator,
		project.ExternalProjectID, hostingKillSwitchRequest{Enabled: false, Reason: "incident resolved"},
		"kill_race_disable_01", "request-kill-race-disable"); err != nil || replayed {
		t.Fatalf("disable replayed=%v err=%v", replayed, err)
	}
	fake.mu.Lock()
	resumed := !fake.currentSuspended
	fake.mu.Unlock()
	var resumeStatus string
	if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='resume' ORDER BY id DESC LIMIT 1`,
		project.ID).Scan(&resumeStatus); err != nil {
		t.Fatal(err)
	}
	if resumed || resumeStatus != "failed" {
		t.Fatalf("resumed=%v resume status=%s", resumed, resumeStatus)
	}
}

func TestGlobalKillSwitchResumesOnlyProjectsWithoutLocalKill(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	first, _ := provisionDeploymentTestProject(t, "project_01JKILLMULTIA")
	second, _ := provisionDeploymentTestProject(t, "project_01JKILLMULTIB")
	projectOperator, err := createServiceToken("multi-project-kill-operator",
		[]string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	admin, err := createServiceToken("multi-project-kill-admin", []string{serviceScopeHostingAdmin})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := setHostingExecutionKillSwitch(t.Context(), projectOperator, second.ExternalProjectID,
		hostingKillSwitchRequest{Enabled: true, Reason: "project incident"},
		"multi_project_local_enable_01", "request-multi-local-enable"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := setHostingExecutionKillSwitch(t.Context(), admin, "",
		hostingKillSwitchRequest{Enabled: true, Reason: "global incident"},
		"multi_project_global_enable_01", "request-multi-global-enable"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := setHostingExecutionKillSwitch(t.Context(), admin, "",
		hostingKillSwitchRequest{Enabled: false, Reason: "global incident resolved"},
		"multi_project_global_disable_01", "request-multi-global-disable"); err != nil {
		t.Fatal(err)
	}

	fake.mu.Lock()
	requests := append([]proxySuspendRequest(nil), fake.suspensions...)
	fake.mu.Unlock()
	countRequests := func(projectID string, suspended bool) int {
		count := 0
		for _, request := range requests {
			if request.ExternalProjectID == projectID && request.Suspended == suspended {
				count++
			}
		}
		return count
	}
	if countRequests(first.ExternalProjectID, true) != 2 ||
		countRequests(first.ExternalProjectID, false) != 0 ||
		countRequests(second.ExternalProjectID, true) != 2 ||
		countRequests(second.ExternalProjectID, false) != 0 {
		t.Fatalf("unexpected global/local kill route requests: %+v", requests)
	}
	var firstResume, secondResume, pending int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='resume'`, first.ID).Scan(&firstResume); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='resume'`, second.ID).Scan(&secondResume); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE status IN ('pending','applied')`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if firstResume != 1 || secondResume != 0 || pending != 0 {
		t.Fatalf("resume operations first=%d second=%d pending=%d", firstResume, secondResume, pending)
	}

	if _, _, err := setHostingExecutionKillSwitch(t.Context(), projectOperator, second.ExternalProjectID,
		hostingKillSwitchRequest{Enabled: false, Reason: "project incident resolved"},
		"multi_project_local_disable_01", "request-multi-local-disable"); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	requests = append([]proxySuspendRequest(nil), fake.suspensions...)
	fake.mu.Unlock()
	localResumeRequests := 0
	for _, request := range requests {
		if request.ExternalProjectID == second.ExternalProjectID && !request.Suspended {
			localResumeRequests++
		}
	}
	var secondResumeStatus string
	if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='resume' ORDER BY id DESC LIMIT 1`,
		second.ID).Scan(&secondResumeStatus); err != nil {
		t.Fatal(err)
	}
	if localResumeRequests != 0 || secondResumeStatus != "failed" {
		t.Fatalf("local kill disable resume requests=%d status=%s all=%+v",
			localResumeRequests, secondResumeStatus, requests)
	}
}

func TestKillSwitchIdempotencyKeyCanBeReusedAfterReceiptExpiry(t *testing.T) {
	for _, testCase := range []struct {
		name              string
		externalProjectID string
		operation         string
		scope             string
	}{
		{name: "project", externalProjectID: "project_01JKILLEXPIRE",
			operation: "hosting.execution.kill_switch.project", scope: serviceScopeProjectsWrite},
		{name: "global", operation: "hosting.execution.kill_switch.global", scope: serviceScopeHostingAdmin},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			withTempDB(t)
			withFakeProxy(t)
			projectID := testCase.externalProjectID
			if projectID == "" {
				projectID = "project_01JKILLEXPIREGLOBAL"
			}
			project, _ := provisionDeploymentTestProject(t, projectID)
			operator, err := createServiceToken("kill-expiry-"+testCase.name, []string{testCase.scope})
			if err != nil {
				t.Fatal(err)
			}
			key := "kill_expired_reuse_" + testCase.name
			input := hostingKillSwitchRequest{Enabled: true, Reason: "repeat incident command"}
			if _, replayed, err := setHostingExecutionKillSwitch(t.Context(), operator,
				testCase.externalProjectID, input, key, "request-expiry-first"); err != nil || replayed {
				t.Fatalf("first replayed=%v err=%v", replayed, err)
			}
			if _, err := db.Exec(`UPDATE hosting_idempotency SET expires_at=?
				WHERE issuer_token_id=? AND operation=? AND idempotency_key=?`,
				formatSQLiteTime(time.Now().UTC().Add(-time.Minute)), operator.ID,
				testCase.operation, key); err != nil {
				t.Fatal(err)
			}
			if _, replayed, err := setHostingExecutionKillSwitch(t.Context(), operator,
				testCase.externalProjectID, input, key, "request-expiry-second"); err != nil || replayed {
				t.Fatalf("expired reuse replayed=%v err=%v", replayed, err)
			}
			var operations, distinctOperations int
			if err := db.QueryRow(`SELECT COUNT(*), COUNT(DISTINCT operation_id)
				FROM hosting_proxy_operations WHERE hosting_project_id=? AND operation_type='suspend'`,
				project.ID).Scan(&operations, &distinctOperations); err != nil {
				t.Fatal(err)
			}
			if operations != 2 || distinctOperations != 2 {
				t.Fatalf("route operations=%d distinct=%d", operations, distinctOperations)
			}
		})
	}
}

func TestDesiredStateIdempotencyKeysCreateFreshRouteOperationsAfterReceiptExpiry(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, _, runnerID, job := createAndClaimHostingJob(t,
		"project_01JSTATEEXPIRE", "deployment_01JSTATEEXPIRE")
	digest := "sha256:" + strings.Repeat("9", 64)
	endpoint := healthyHostingEndpointForTest(t)
	observeHostingJobEndpointForTest(t, job.JobID, endpoint)
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, digest),
			RuntimeEndpoint:       endpoint,
			HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	operator, err := createServiceToken("desired-state-expiry-operator", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	type desiredExecution struct {
		desired, key, operationType string
	}
	executions := []desiredExecution{
		{desired: "suspended", key: "expired_suspend_key_01", operationType: "suspend"},
		{desired: "active", key: "expired_resume_key_01", operationType: "resume"},
	}
	for _, execution := range executions {
		if _, replayed, err := setHostingProjectDesiredState(t.Context(), operator,
			project.ExternalProjectID, execution.desired, execution.key); err != nil || replayed {
			t.Fatalf("first %s replayed=%v err=%v", execution.desired, replayed, err)
		}
		var firstID string
		var firstGeneration int64
		if err := db.QueryRow(`SELECT operation_id, route_generation FROM hosting_proxy_operations
			WHERE hosting_project_id=? AND operation_type=? ORDER BY id DESC LIMIT 1`,
			project.ID, execution.operationType).Scan(&firstID, &firstGeneration); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`UPDATE hosting_idempotency SET expires_at=?
			WHERE issuer_token_id=? AND operation=? AND idempotency_key=?`,
			formatSQLiteTime(time.Now().UTC().Add(-time.Minute)), operator.ID,
			"hosting.project."+execution.desired, execution.key); err != nil {
			t.Fatal(err)
		}
		if _, replayed, err := setHostingProjectDesiredState(t.Context(), operator,
			project.ExternalProjectID, execution.desired, execution.key); err != nil || replayed {
			t.Fatalf("expired %s reuse replayed=%v err=%v", execution.desired, replayed, err)
		}
		var latestID, status string
		var latestGeneration, projectGeneration, operations, distinctOperations int64
		if err := db.QueryRow(`SELECT operation.operation_id, operation.route_generation, operation.status,
			project.route_generation,
			(SELECT COUNT(*) FROM hosting_proxy_operations counted
			 WHERE counted.hosting_project_id=project.id AND counted.operation_type=?),
			(SELECT COUNT(DISTINCT counted.operation_id) FROM hosting_proxy_operations counted
			 WHERE counted.hosting_project_id=project.id AND counted.operation_type=?)
			FROM hosting_proxy_operations operation JOIN hosting_projects project
			  ON project.id=operation.hosting_project_id
			WHERE operation.hosting_project_id=? AND operation.operation_type=?
			ORDER BY operation.id DESC LIMIT 1`, execution.operationType, execution.operationType,
			project.ID, execution.operationType).Scan(&latestID, &latestGeneration, &status,
			&projectGeneration, &operations, &distinctOperations); err != nil {
			t.Fatal(err)
		}
		if latestID == firstID || latestGeneration <= firstGeneration || latestGeneration != projectGeneration ||
			status != "committed" || operations != 2 || distinctOperations != 2 {
			t.Fatalf("%s first=%s/%d latest=%s/%d project=%d status=%s operations=%d distinct=%d",
				execution.desired, firstID, firstGeneration, latestID, latestGeneration,
				projectGeneration, status, operations, distinctOperations)
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.suspensions) != 2 || len(fake.activations) != 3 ||
		fake.suspensions[0].OperationID == fake.suspensions[1].OperationID ||
		fake.activations[1].OperationID == fake.activations[2].OperationID ||
		fake.currentEndpoint != endpoint || fake.currentSuspended {
		t.Fatalf("adapter activations=%+v suspensions=%+v endpoint=%q suspended=%v",
			fake.activations, fake.suspensions, fake.currentEndpoint, fake.currentSuspended)
	}
}

func TestKillSwitchResumeRebindsAuthoritativeReleaseAfterAmbiguousActivation(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, deploymentToken, runnerID, firstJob := createAndClaimHostingJob(t,
		"project_01JKILLREBIND", "deployment_01JKILLREBINDA")
	firstDigest := "sha256:" + strings.Repeat("1", 64)
	firstEndpoint := healthyHostingEndpointForTest(t)
	firstCompletion := hostingCompletionRequest{Status: "success", ReleaseDigest: firstDigest,
		ReleaseArtifactDigest: attachTestReleaseArtifact(t, firstJob.JobID, firstDigest),
		RuntimeEndpoint:       firstEndpoint,
		HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}
	observeHostingJobEndpointForTest(t, firstJob.JobID, firstEndpoint)
	if err := completeHostingJob(t.Context(), runnerID, firstJob.JobID, firstJob.LeaseGeneration,
		firstJob.LeaseToken, firstCompletion); err != nil {
		t.Fatal(err)
	}

	request := validHostingDeploymentRequest("deployment_01JKILLREBINDB")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), deploymentToken, project.ExternalProjectID,
		"create_01JKILLREBINDB", request); err != nil {
		t.Fatal(err)
	}
	secondJob, err := claimHostingJob(t.Context(), runnerID)
	if err != nil {
		t.Fatal(err)
	}
	advanceHostingJobToHealthCheckingForTest(t, runnerID, secondJob)
	secondDigest := "sha256:" + strings.Repeat("2", 64)
	secondEndpoint := healthyHostingEndpointForTest(t)
	secondCompletion := hostingCompletionRequest{Status: "success", ReleaseDigest: secondDigest,
		ReleaseArtifactDigest: attachTestReleaseArtifact(t, secondJob.JobID, secondDigest),
		RuntimeEndpoint:       secondEndpoint,
		HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}
	observeHostingJobEndpointForTest(t, secondJob.JobID, secondEndpoint)
	fake.activateApplied = make(chan struct{}, 1)
	fake.activateReturn = make(chan struct{})
	completed := make(chan error, 1)
	go func() {
		completed <- completeHostingJob(context.Background(), runnerID, secondJob.JobID,
			secondJob.LeaseGeneration, secondJob.LeaseToken, secondCompletion)
	}()
	select {
	case <-fake.activateApplied:
	case <-time.After(3 * time.Second):
		close(fake.activateReturn)
		t.Fatal("candidate activation was not applied before its response barrier")
	}
	fake.mu.Lock()
	appliedEndpoint := fake.currentEndpoint
	fake.mu.Unlock()
	if appliedEndpoint != secondEndpoint {
		close(fake.activateReturn)
		t.Fatalf("ambiguous adapter endpoint=%q want candidate %q", appliedEndpoint, secondEndpoint)
	}
	operator, err := createServiceToken("kill-rebind-operator", []string{serviceScopeProjectsWrite})
	if err != nil {
		close(fake.activateReturn)
		t.Fatal(err)
	}
	if _, _, err := setHostingExecutionKillSwitch(t.Context(), operator, project.ExternalProjectID,
		hostingKillSwitchRequest{Enabled: true, Reason: "ambiguous activation incident"},
		"kill_rebind_enable_01", "request-kill-rebind-enable"); err != nil {
		close(fake.activateReturn)
		t.Fatal(err)
	}
	close(fake.activateReturn)
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.activateApplied = nil
	fake.activateReturn = nil
	hiddenEndpoint, suspended := fake.currentEndpoint, fake.currentSuspended
	fake.mu.Unlock()
	if hiddenEndpoint != secondEndpoint || !suspended {
		t.Fatalf("suspended ambiguous route endpoint=%q suspended=%v", hiddenEndpoint, suspended)
	}
	if _, _, err := setHostingExecutionKillSwitch(t.Context(), operator, project.ExternalProjectID,
		hostingKillSwitchRequest{Enabled: false, Reason: "ambiguous activation resolved"},
		"kill_rebind_disable_01", "request-kill-rebind-disable"); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	resumedEndpoint, resumedSuspended := fake.currentEndpoint, fake.currentSuspended
	fake.mu.Unlock()
	var activeDigest, cancelledStatus, resumeStatus string
	if err := db.QueryRow(`SELECT release_digest FROM hosting_releases
		WHERE hosting_project_id=? AND status='active'`, project.ID).Scan(&activeDigest); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM hosting_deployments WHERE external_deployment_id=?`,
		"deployment_01JKILLREBINDB").Scan(&cancelledStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='resume' ORDER BY id DESC LIMIT 1`,
		project.ID).Scan(&resumeStatus); err != nil {
		t.Fatal(err)
	}
	if resumedEndpoint != firstEndpoint || resumedSuspended || activeDigest != firstDigest ||
		cancelledStatus != hostingStatusCancelled || resumeStatus != "committed" {
		t.Fatalf("resume endpoint=%q suspended=%v active=%s cancelled=%s resume=%s",
			resumedEndpoint, resumedSuspended, activeDigest, cancelledStatus, resumeStatus)
	}
}

func TestHealthyActivationSupersedesPendingResumeWithoutActiveRelease(t *testing.T) {
	withTempDB(t)
	fake := withFakeProxy(t)
	project, deploymentToken := provisionDeploymentTestProject(t, "project_01JRESUMENEW")
	operator, err := createServiceToken("pending-resume-operator", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := setHostingExecutionKillSwitch(t.Context(), operator, project.ExternalProjectID,
		hostingKillSwitchRequest{Enabled: true, Reason: "pre-deployment incident"},
		"pending_resume_enable_01", "request-pending-resume-enable"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := setHostingExecutionKillSwitch(t.Context(), operator, project.ExternalProjectID,
		hostingKillSwitchRequest{Enabled: false, Reason: "pre-deployment incident resolved"},
		"pending_resume_disable_01", "request-pending-resume-disable"); err != nil {
		t.Fatal(err)
	}
	var resumeGeneration, routeFenceGeneration int64
	var resumeStatus string
	if err := db.QueryRow(`SELECT route_generation, status FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='resume'`, project.ID).Scan(
		&resumeGeneration, &resumeStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT route_generation FROM hosting_projects WHERE id=?`, project.ID).Scan(
		&routeFenceGeneration); err != nil {
		t.Fatal(err)
	}
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	runnerID := insertHostingRunnerForTest(t, "pending-resume-runner", limits)
	request := validHostingDeploymentRequest("deployment_01JRESUMENEW")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), deploymentToken, project.ExternalProjectID,
		"create_01JRESUMENEW", request); err != nil {
		t.Fatal(err)
	}
	job, err := claimHostingJob(t.Context(), runnerID)
	if err != nil {
		t.Fatal(err)
	}
	advanceHostingJobToHealthCheckingForTest(t, runnerID, job)
	digest := "sha256:" + strings.Repeat("3", 64)
	endpoint := healthyHostingEndpointForTest(t)
	observeHostingJobEndpointForTest(t, job.JobID, endpoint)
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, digest),
			RuntimeEndpoint:       endpoint,
			HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	var projectGeneration int64
	if err := db.QueryRow(`SELECT route_generation FROM hosting_projects WHERE id=?`, project.ID).Scan(
		&projectGeneration); err != nil {
		t.Fatal(err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), request.ExternalDeploymentID)
	fake.mu.Lock()
	activeEndpoint, suspended := fake.currentEndpoint, fake.currentSuspended
	fake.mu.Unlock()
	if err != nil || deployment.Status != hostingStatusActive || resumeStatus != "failed" ||
		projectGeneration <= routeFenceGeneration || routeFenceGeneration <= resumeGeneration ||
		activeEndpoint != endpoint || suspended {
		t.Fatalf("deployment=%+v err=%v generations=%d/%d/%d resume=%s endpoint=%q suspended=%v",
			deployment, err, resumeGeneration, routeFenceGeneration, projectGeneration,
			resumeStatus, activeEndpoint, suspended)
	}
}
