package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type hostingRollbackTestFixture struct {
	project             *HostingProject
	token               *ServiceToken
	targetDeploymentID  string
	targetDigest        string
	currentDeploymentID string
	currentDigest       string
	targetHealthy       *atomic.Bool
	targetHealthCalls   *atomic.Int64
}

func setupHostingRollbackTestFixture(t *testing.T) (*hostingRollbackTestFixture, *fakeProxyClient) {
	t.Helper()
	withTempDB(t)
	fake := withFakeProxy(t)

	targetHealthy := &atomic.Bool{}
	targetHealthy.Store(true)
	targetHealthCalls := &atomic.Int64{}
	targetHealth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetHealthCalls.Add(1)
		if !targetHealthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(targetHealth.Close)
	currentHealth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(currentHealth.Close)

	const targetDeploymentID = "deployment_01JRBIDEMTARGET"
	project, token, runnerID, targetJob := createAndClaimHostingJob(t,
		"project_01JRBIDEMPOTENT", targetDeploymentID)
	targetDigest := "sha256:" + strings.Repeat("3", 64)
	targetArtifact := attachTestReleaseArtifact(t, targetJob.JobID, targetDigest)
	observeHostingJobEndpointForTest(t, targetJob.JobID, targetHealth.URL)
	if err := completeHostingJob(t.Context(), runnerID, targetJob.JobID, targetJob.LeaseGeneration,
		targetJob.LeaseToken, hostingCompletionRequest{
			Status:                "success",
			ReleaseDigest:         targetDigest,
			ReleaseArtifactDigest: targetArtifact,
			RuntimeEndpoint:       targetHealth.URL,
			HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)},
		}); err != nil {
		t.Fatalf("complete rollback target: %v", err)
	}

	const currentDeploymentID = "deployment_01JRBIDEMCURRENT"
	request := validHostingDeploymentRequest(currentDeploymentID)
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID,
		"create_01JRBIDEMCURRENT", request); err != nil {
		t.Fatalf("create current deployment: %v", err)
	}
	currentJob, err := claimHostingJob(t.Context(), runnerID)
	if err != nil {
		t.Fatalf("claim current deployment: %v", err)
	}
	advanceHostingJobToHealthCheckingForTest(t, runnerID, currentJob)
	currentDigest := "sha256:" + strings.Repeat("4", 64)
	currentArtifact := attachTestReleaseArtifact(t, currentJob.JobID, currentDigest)
	observeHostingJobEndpointForTest(t, currentJob.JobID, currentHealth.URL)
	if err := completeHostingJob(t.Context(), runnerID, currentJob.JobID, currentJob.LeaseGeneration,
		currentJob.LeaseToken, hostingCompletionRequest{
			Status:                "success",
			ReleaseDigest:         currentDigest,
			ReleaseArtifactDigest: currentArtifact,
			RuntimeEndpoint:       currentHealth.URL,
			HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)},
		}); err != nil {
		t.Fatalf("complete current deployment: %v", err)
	}

	return &hostingRollbackTestFixture{
		project:             project,
		token:               token,
		targetDeploymentID:  targetDeploymentID,
		targetDigest:        targetDigest,
		currentDeploymentID: currentDeploymentID,
		currentDigest:       currentDigest,
		targetHealthy:       targetHealthy,
		targetHealthCalls:   targetHealthCalls,
	}, fake
}

func hostingRollbackRouteGenerationForTest(t *testing.T, projectID int64) int64 {
	t.Helper()
	var generation int64
	if err := db.QueryRow(`SELECT route_generation FROM hosting_projects WHERE id=?`, projectID).Scan(&generation); err != nil {
		t.Fatalf("read route generation: %v", err)
	}
	return generation
}

func hostingRollbackActivationCountForTest(fake *fakeProxyClient) int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return len(fake.activations)
}

func requireHostingRollbackAPIError(t *testing.T, err error, code string) *hostingAPIError {
	t.Helper()
	var apiErr *hostingAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != code {
		t.Fatalf("rollback error=%v, want code %q", err, code)
	}
	return apiErr
}

func TestHostingRollbackConcurrentExactRetryOwnsOneIntent(t *testing.T) {
	fixture, fake := setupHostingRollbackTestFixture(t)
	initialGeneration := hostingRollbackRouteGenerationForTest(t, fixture.project.ID)
	initialActivations := hostingRollbackActivationCountForTest(fake)

	fake.activateStarted = make(chan struct{}, 2)
	fake.activateContinue = make(chan struct{})
	type rollbackOutcome struct {
		result *hostingRollbackResult
		err    error
	}
	firstDone := make(chan rollbackOutcome, 1)
	const key = "rollback_concurrent_exact_01"
	go func() {
		result, err := rollbackHostingReleaseResult(context.Background(), fixture.token,
			fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key)
		firstDone <- rollbackOutcome{result: result, err: err}
	}()

	select {
	case <-fake.activateStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("first rollback did not reach the proxy")
	}

	duplicateDone := make(chan rollbackOutcome, 1)
	go func() {
		result, err := rollbackHostingReleaseResult(context.Background(), fixture.token,
			fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key)
		duplicateDone <- rollbackOutcome{result: result, err: err}
	}()
	var duplicate rollbackOutcome
	select {
	case duplicate = <-duplicateDone:
	case <-time.After(3 * time.Second):
		close(fake.activateContinue)
		<-firstDone
		t.Fatal("duplicate rollback did not return while the owner was blocked")
	}
	requireHostingRollbackAPIError(t, duplicate.err, errCodeIdempotencyInProgress)
	if duplicate.result != nil {
		t.Fatalf("in-progress duplicate returned result %+v", duplicate.result)
	}

	if got := hostingRollbackRouteGenerationForTest(t, fixture.project.ID); got != initialGeneration+1 {
		t.Fatalf("route generation=%d, want %d", got, initialGeneration+1)
	}
	var operations, receipts, audits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='rollback'`, fixture.project.ID).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_idempotency
		WHERE issuer_token_id=? AND operation=? AND idempotency_key=?`, fixture.token.ID,
		hostingRollbackOperation, key).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_audit_events
		WHERE issuer_token_id=? AND hosting_project_id=?
		  AND event_type='hosting_release_rollback_requested'`, fixture.token.ID,
		fixture.project.ID).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if operations != 1 || receipts != 1 || audits != 1 {
		t.Fatalf("operations=%d receipts=%d audits=%d, want one each", operations, receipts, audits)
	}
	if got := hostingRollbackActivationCountForTest(fake); got != initialActivations+1 {
		t.Fatalf("proxy activations=%d, want %d", got, initialActivations+1)
	}

	close(fake.activateContinue)
	first := <-firstDone
	if first.err != nil || first.result == nil || first.result.StatusCode != http.StatusOK || first.result.Replayed {
		t.Fatalf("owner result=%+v err=%v", first.result, first.err)
	}
	replay, err := rollbackHostingReleaseResult(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key)
	if err != nil || replay == nil || replay.StatusCode != http.StatusOK || !replay.Replayed {
		t.Fatalf("completed replay=%+v err=%v", replay, err)
	}
	if !bytes.Equal(first.result.ResponseBody, replay.ResponseBody) {
		t.Fatalf("replay body differs:\nfirst=%s\nreplay=%s", first.result.ResponseBody, replay.ResponseBody)
	}
	if got := hostingRollbackRouteGenerationForTest(t, fixture.project.ID); got != initialGeneration+1 {
		t.Fatalf("replay changed route generation to %d", got)
	}
	if got := hostingRollbackActivationCountForTest(fake); got != initialActivations+1 {
		t.Fatalf("replay changed proxy activation count to %d", got)
	}
}

func TestHostingRollbackPendingKeyRejectsChangedTargetWithoutWork(t *testing.T) {
	fixture, fake := setupHostingRollbackTestFixture(t)
	initialGeneration := hostingRollbackRouteGenerationForTest(t, fixture.project.ID)
	initialActivations := hostingRollbackActivationCountForTest(fake)
	fake.activateStarted = make(chan struct{}, 2)
	fake.activateContinue = make(chan struct{})

	firstDone := make(chan error, 1)
	const key = "rollback_pending_changed_01"
	go func() {
		_, err := rollbackHostingReleaseResult(context.Background(), fixture.token,
			fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key)
		firstDone <- err
	}()
	select {
	case <-fake.activateStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("first rollback did not reach the proxy")
	}

	_, err := rollbackHostingReleaseResult(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.currentDeploymentID, fixture.currentDigest, key)
	requireHostingRollbackAPIError(t, err, errCodeIdempotencyConflict)
	if got := hostingRollbackRouteGenerationForTest(t, fixture.project.ID); got != initialGeneration+1 {
		t.Fatalf("changed request route generation=%d, want %d", got, initialGeneration+1)
	}
	if got := hostingRollbackActivationCountForTest(fake); got != initialActivations+1 {
		t.Fatalf("changed request proxy activations=%d, want %d", got, initialActivations+1)
	}
	var operations, receipts int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='rollback'`, fixture.project.ID).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_idempotency
		WHERE issuer_token_id=? AND operation=? AND idempotency_key=?`, fixture.token.ID,
		hostingRollbackOperation, key).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if operations != 1 || receipts != 1 {
		t.Fatalf("operations=%d receipts=%d, want one each", operations, receipts)
	}

	close(fake.activateContinue)
	if err := <-firstDone; err != nil {
		t.Fatalf("owner rollback: %v", err)
	}
}

func TestHostingRollbackTransientProxyFailureReconcilesReceiptAndEvent(t *testing.T) {
	fixture, fake := setupHostingRollbackTestFixture(t)
	initialGeneration := hostingRollbackRouteGenerationForTest(t, fixture.project.ID)
	initialActivations := hostingRollbackActivationCountForTest(fake)
	fake.mu.Lock()
	fake.fail = true
	fake.mu.Unlock()

	const key = "rollback_transient_reconcile_01"
	result, err := rollbackHostingReleaseResult(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key)
	if err == nil || result != nil {
		t.Fatalf("transient rollback result=%+v err=%v", result, err)
	}
	if got := hostingRollbackRouteGenerationForTest(t, fixture.project.ID); got != initialGeneration+1 {
		t.Fatalf("failed attempt route generation=%d, want %d", got, initialGeneration+1)
	}
	var operationID, operationStatus string
	var responseStatus any
	if err := db.QueryRow(`SELECT receipt.operation_reference, operation.status, receipt.response_status
		FROM hosting_idempotency receipt JOIN hosting_proxy_operations operation
		  ON operation.operation_id=receipt.operation_reference
		WHERE receipt.issuer_token_id=? AND receipt.operation=? AND receipt.idempotency_key=?`,
		fixture.token.ID, hostingRollbackOperation, key).Scan(&operationID, &operationStatus, &responseStatus); err != nil {
		t.Fatal(err)
	}
	if operationID == "" || operationStatus != "pending" || responseStatus != nil {
		t.Fatalf("operation=%q status=%q response=%v", operationID, operationStatus, responseStatus)
	}

	_, err = rollbackHostingReleaseResult(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key)
	requireHostingRollbackAPIError(t, err, errCodeIdempotencyInProgress)
	if got := hostingRollbackActivationCountForTest(fake); got != initialActivations+1 {
		t.Fatalf("in-progress retry changed proxy calls to %d", got)
	}

	fake.mu.Lock()
	fake.fail = false
	fake.mu.Unlock()
	if err := reconcileHostingRollbackOperations(t.Context()); err != nil {
		t.Fatalf("reconcile rollback: %v", err)
	}
	if got := hostingRollbackRouteGenerationForTest(t, fixture.project.ID); got != initialGeneration+1 {
		t.Fatalf("reconciliation changed route generation to %d", got)
	}
	if got := hostingRollbackActivationCountForTest(fake); got != initialActivations+2 {
		t.Fatalf("reconciliation proxy activations=%d, want %d", got, initialActivations+2)
	}

	var storedBody string
	if err := db.QueryRow(`SELECT operation.status, receipt.response_status, receipt.response_body
		FROM hosting_proxy_operations operation JOIN hosting_idempotency receipt
		  ON receipt.operation_reference=operation.operation_id
		WHERE operation.operation_id=?`, operationID).Scan(&operationStatus, &responseStatus, &storedBody); err != nil {
		t.Fatal(err)
	}
	if operationStatus != "committed" || responseStatus != int64(http.StatusOK) || storedBody == "" {
		t.Fatalf("status=%q response_status=%v body=%q", operationStatus, responseStatus, storedBody)
	}
	var events int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_events WHERE hosting_project_id=?
		AND event_type='release_rolled_back'`, fixture.project.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("rollback events=%d, want 1", events)
	}
	replay, err := rollbackHostingReleaseResult(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key)
	if err != nil || replay == nil || !replay.Replayed || replay.StatusCode != http.StatusOK {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if string(replay.ResponseBody) != storedBody {
		t.Fatalf("replay body differs from durable body:\nreplay=%s\nstored=%s", replay.ResponseBody, storedBody)
	}
}

func TestHostingRollbackAppliedRestartRechecksHealth(t *testing.T) {
	fixture, fake := setupHostingRollbackTestFixture(t)
	initialActivations := hostingRollbackActivationCountForTest(fake)
	const key = "rollback_applied_rehealth_01"
	requestHash := hashHostingOperation(hostingRollbackOperation, fixture.project.ExternalProjectID,
		fixture.targetDeploymentID, fixture.targetDigest)
	operation, replay, err := stageHostingRollbackOperation(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key, requestHash)
	if err != nil || replay != nil || operation == nil {
		t.Fatalf("stage operation=%+v replay=%+v err=%v", operation, replay, err)
	}
	if err := markHostingProxyOperationApplied(t.Context(), operation.OperationID, "route-rev-crash-window"); err != nil {
		t.Fatalf("mark operation applied: %v", err)
	}
	fixture.targetHealthy.Store(false)
	healthCallsBefore := fixture.targetHealthCalls.Load()
	if err := reconcileHostingRollbackOperations(t.Context()); err != nil {
		t.Fatalf("reconcile applied rollback: %v", err)
	}
	if fixture.targetHealthCalls.Load() <= healthCallsBefore {
		t.Fatal("applied rollback was not checked by the HTTP health gate")
	}

	var sourceStatus, sourceError string
	var receiptStatus int
	if err := db.QueryRow(`SELECT operation.status, operation.last_error_code, receipt.response_status
		FROM hosting_proxy_operations operation JOIN hosting_idempotency receipt
		  ON receipt.operation_reference=operation.operation_id
		WHERE operation.operation_id=?`, operation.OperationID).Scan(
		&sourceStatus, &sourceError, &receiptStatus); err != nil {
		t.Fatal(err)
	}
	if sourceStatus != "failed" || sourceError != errCodeReleaseNotHealthy || receiptStatus != http.StatusConflict {
		t.Fatalf("source status=%q error=%q receipt=%d", sourceStatus, sourceError, receiptStatus)
	}
	var activeDigest string
	if err := db.QueryRow(`SELECT release_digest FROM hosting_releases
		WHERE hosting_project_id=? AND status='active'`, fixture.project.ID).Scan(&activeDigest); err != nil {
		t.Fatal(err)
	}
	if activeDigest != fixture.currentDigest {
		t.Fatalf("active release=%q, want prior release %q", activeDigest, fixture.currentDigest)
	}
	var rollbackEvents int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_events WHERE hosting_project_id=?
		AND event_type='release_rolled_back'`, fixture.project.ID).Scan(&rollbackEvents); err != nil {
		t.Fatal(err)
	}
	if rollbackEvents != 0 {
		t.Fatalf("unhealthy applied rollback emitted %d success events", rollbackEvents)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.activations) != initialActivations+1 {
		t.Fatalf("proxy activations=%d, want one compensation after fixture", len(fake.activations))
	}
	if fake.activations[len(fake.activations)-1].ReleaseDigest != fixture.currentDigest {
		t.Fatalf("last activation targeted %q, want compensation %q",
			fake.activations[len(fake.activations)-1].ReleaseDigest, fixture.currentDigest)
	}
}

func TestHostingRollbackPendingDispatchCrashCompensatesBeforeFailure(t *testing.T) {
	fixture, fake := setupHostingRollbackTestFixture(t)
	initialActivations := hostingRollbackActivationCountForTest(fake)
	const key = "rollback_pending_dispatch_crash_01"
	requestHash := hashHostingOperation(hostingRollbackOperation, fixture.project.ExternalProjectID,
		fixture.targetDeploymentID, fixture.targetDigest)
	operation, replay, err := stageHostingRollbackOperation(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key, requestHash)
	if err != nil || replay != nil || operation == nil {
		t.Fatalf("stage operation=%+v replay=%+v err=%v", operation, replay, err)
	}
	if _, err := fake.Activate(t.Context(), proxyActivationRequest{
		OperationID:                   operation.OperationID,
		ExternalProjectID:             operation.ExternalProjectID,
		ReleaseDigest:                 operation.ReleaseDigest,
		RuntimeEndpoint:               operation.RuntimeEndpoint,
		ExpectedPreviousReleaseDigest: operation.PreviousReleaseDigest,
		RouteGeneration:               operation.RouteGeneration,
	}); err != nil {
		t.Fatalf("simulate ambiguous applied dispatch: %v", err)
	}
	// Simulate process loss before markHostingProxyOperationApplied persisted the
	// adapter response. A later terminal health decision must restore the prior route.
	fixture.targetHealthy.Store(false)
	if err := reconcileHostingRollbackOperations(t.Context()); err != nil {
		t.Fatalf("reconcile ambiguous pending rollback: %v", err)
	}

	var operationStatus, operationError string
	var receiptStatus int
	if err := db.QueryRow(`SELECT operation.status, operation.last_error_code, receipt.response_status
		FROM hosting_proxy_operations operation JOIN hosting_idempotency receipt
		  ON receipt.operation_reference=operation.operation_id
		WHERE operation.operation_id=?`, operation.OperationID).Scan(
		&operationStatus, &operationError, &receiptStatus); err != nil {
		t.Fatal(err)
	}
	if operationStatus != "failed" || operationError != errCodeReleaseNotHealthy ||
		receiptStatus != http.StatusConflict {
		t.Fatalf("operation status=%q error=%q receipt=%d", operationStatus,
			operationError, receiptStatus)
	}
	fake.mu.Lock()
	currentEndpoint := fake.currentEndpoint
	fake.mu.Unlock()
	if currentEndpoint != operation.PreviousRuntimeEndpoint {
		t.Fatalf("proxy endpoint=%q, want restored prior endpoint %q",
			currentEndpoint, operation.PreviousRuntimeEndpoint)
	}
	if got := hostingRollbackActivationCountForTest(fake); got != initialActivations+2 {
		t.Fatalf("proxy activations=%d, want ambiguous dispatch plus compensation", got)
	}
	var activeDigest string
	if err := db.QueryRow(`SELECT release_digest FROM hosting_releases
		WHERE hosting_project_id=? AND status='active'`, fixture.project.ID).Scan(&activeDigest); err != nil {
		t.Fatal(err)
	}
	if activeDigest != fixture.currentDigest {
		t.Fatalf("active release=%q, want prior release %q", activeDigest, fixture.currentDigest)
	}
}

func TestHostingRollbackRejectedNewerIntentCompensatesOlderAmbiguousDispatch(t *testing.T) {
	fixture, fake := setupHostingRollbackTestFixture(t)
	initialActivations := hostingRollbackActivationCountForTest(fake)
	requestHash := hashHostingOperation(hostingRollbackOperation, fixture.project.ExternalProjectID,
		fixture.targetDeploymentID, fixture.targetDigest)
	first, replay, err := stageHostingRollbackOperation(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest,
		"rollback_overlap_first_01", requestHash)
	if err != nil || replay != nil {
		t.Fatalf("stage first=%+v replay=%+v err=%v", first, replay, err)
	}
	if _, err := fake.Activate(t.Context(), proxyActivationRequest{
		OperationID: first.OperationID, ExternalProjectID: first.ExternalProjectID,
		ReleaseDigest: first.ReleaseDigest, RuntimeEndpoint: first.RuntimeEndpoint,
		ExpectedPreviousReleaseDigest: first.PreviousReleaseDigest,
		RouteGeneration:               first.RouteGeneration,
	}); err != nil {
		t.Fatalf("apply first ambiguous dispatch: %v", err)
	}
	second, replay, err := stageHostingRollbackOperation(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest,
		"rollback_overlap_second_01", requestHash)
	if err != nil || replay != nil {
		t.Fatalf("stage second=%+v replay=%+v err=%v", second, replay, err)
	}
	fake.mu.Lock()
	fake.rejectOnce = true
	fake.mu.Unlock()
	secondResult, err := runHostingRollbackOperation(t.Context(), second.OperationID)
	if err != nil || secondResult == nil || secondResult.StatusCode != http.StatusBadGateway {
		t.Fatalf("second result=%+v err=%v", secondResult, err)
	}
	firstResult, err := runHostingRollbackOperation(t.Context(), first.OperationID)
	if err != nil || firstResult == nil || firstResult.StatusCode != http.StatusConflict {
		t.Fatalf("first result=%+v err=%v", firstResult, err)
	}
	fake.mu.Lock()
	currentEndpoint := fake.currentEndpoint
	fake.mu.Unlock()
	if currentEndpoint != second.PreviousRuntimeEndpoint {
		t.Fatalf("proxy endpoint=%q, want prior endpoint %q", currentEndpoint,
			second.PreviousRuntimeEndpoint)
	}
	if got := hostingRollbackActivationCountForTest(fake); got != initialActivations+3 {
		t.Fatalf("proxy activations=%d, want first dispatch, rejected second, and compensation", got)
	}
	var activeDigest string
	if err := db.QueryRow(`SELECT release_digest FROM hosting_releases
		WHERE hosting_project_id=? AND status='active'`, fixture.project.ID).Scan(&activeDigest); err != nil {
		t.Fatal(err)
	}
	if activeDigest != fixture.currentDigest {
		t.Fatalf("active release=%q, want prior release %q", activeDigest, fixture.currentDigest)
	}
}

func TestHostingRollbackWaitsForPendingCompensationBeforeTerminalReceipt(t *testing.T) {
	fixture, fake := setupHostingRollbackTestFixture(t)
	requestHash := hashHostingOperation(hostingRollbackOperation, fixture.project.ExternalProjectID,
		fixture.targetDeploymentID, fixture.targetDigest)
	operation, replay, err := stageHostingRollbackOperation(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest,
		"rollback_pending_compensation_01", requestHash)
	if err != nil || replay != nil {
		t.Fatalf("stage operation=%+v replay=%+v err=%v", operation, replay, err)
	}
	if _, err := fake.Activate(t.Context(), proxyActivationRequest{
		OperationID: operation.OperationID, ExternalProjectID: operation.ExternalProjectID,
		ReleaseDigest: operation.ReleaseDigest, RuntimeEndpoint: operation.RuntimeEndpoint,
		ExpectedPreviousReleaseDigest: operation.PreviousReleaseDigest,
		RouteGeneration:               operation.RouteGeneration,
	}); err != nil {
		t.Fatalf("apply ambiguous dispatch: %v", err)
	}
	fixture.targetHealthy.Store(false)
	fake.mu.Lock()
	fake.fail = true
	fake.mu.Unlock()
	if err := reconcileHostingRollbackOperations(t.Context()); err != nil {
		t.Fatalf("first rollback reconcile: %v", err)
	}
	assertPendingReceipt := func(stage string) {
		t.Helper()
		var status any
		if err := db.QueryRow(`SELECT response_status FROM hosting_idempotency
			WHERE operation_reference=?`, operation.OperationID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != nil {
			t.Fatalf("%s response_status=%v, want pending", stage, status)
		}
	}
	assertPendingReceipt("after failed compensation")
	if err := reconcileHostingRollbackOperations(t.Context()); err != nil {
		t.Fatalf("rollback reconcile while compensation pending: %v", err)
	}
	assertPendingReceipt("after source generation mismatch")

	fake.mu.Lock()
	fake.fail = false
	fake.mu.Unlock()
	if err := reconcileHostingCompensationOperations(t.Context()); err != nil {
		t.Fatalf("reconcile compensation: %v", err)
	}
	if err := reconcileHostingRollbackOperations(t.Context()); err != nil {
		t.Fatalf("finalize rollback after compensation: %v", err)
	}
	var receiptStatus int
	var operationStatus, operationError string
	if err := db.QueryRow(`SELECT operation.status, operation.last_error_code, receipt.response_status
		FROM hosting_proxy_operations operation JOIN hosting_idempotency receipt
		  ON receipt.operation_reference=operation.operation_id
		WHERE operation.operation_id=?`, operation.OperationID).Scan(
		&operationStatus, &operationError, &receiptStatus); err != nil {
		t.Fatal(err)
	}
	if operationStatus != "failed" || operationError != errCodeReleaseNotHealthy ||
		receiptStatus != http.StatusConflict {
		t.Fatalf("operation status=%q error=%q receipt=%d", operationStatus,
			operationError, receiptStatus)
	}
}

func TestHostingRollbackRememberedTerminalIntentFencesInflightSuccess(t *testing.T) {
	fixture, fake := setupHostingRollbackTestFixture(t)
	initialGeneration := hostingRollbackRouteGenerationForTest(t, fixture.project.ID)
	requestHash := hashHostingOperation(hostingRollbackOperation, fixture.project.ExternalProjectID,
		fixture.targetDeploymentID, fixture.targetDigest)
	operation, replay, err := stageHostingRollbackOperation(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest,
		"rollback_terminal_intent_fence_01", requestHash)
	if err != nil || replay != nil {
		t.Fatalf("stage operation=%+v replay=%+v err=%v", operation, replay, err)
	}
	fake.activateStarted = make(chan struct{}, 1)
	fake.activateContinue = make(chan struct{})
	type outcome struct {
		result *hostingRollbackResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := runHostingRollbackOperation(context.Background(), operation.OperationID)
		done <- outcome{result: result, err: err}
	}()
	select {
	case <-fake.activateStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("rollback did not block in proxy activation")
	}
	remembered, err := rememberHostingRollbackTerminalIntent(t.Context(), operation.OperationID,
		errCodeReleaseNotHealthy)
	if err != nil || remembered != errCodeReleaseNotHealthy {
		t.Fatalf("remembered=%q err=%v", remembered, err)
	}
	close(fake.activateContinue)
	completed := <-done
	if completed.err != nil || completed.result == nil ||
		completed.result.StatusCode != http.StatusConflict {
		t.Fatalf("inflight result=%+v err=%v", completed.result, completed.err)
	}
	var operationStatus, operationError string
	var receiptStatus int
	if err := db.QueryRow(`SELECT operation.status, operation.last_error_code, receipt.response_status
		FROM hosting_proxy_operations operation JOIN hosting_idempotency receipt
		  ON receipt.operation_reference=operation.operation_id
		WHERE operation.operation_id=?`, operation.OperationID).Scan(
		&operationStatus, &operationError, &receiptStatus); err != nil {
		t.Fatal(err)
	}
	if operationStatus != "failed" || operationError != errCodeReleaseNotHealthy ||
		receiptStatus != http.StatusConflict {
		t.Fatalf("operation status=%q error=%q receipt=%d", operationStatus,
			operationError, receiptStatus)
	}
	if got := hostingRollbackRouteGenerationForTest(t, fixture.project.ID); got != initialGeneration+2 {
		t.Fatalf("route generation=%d, want source plus compensation", got)
	}
	fake.mu.Lock()
	currentEndpoint := fake.currentEndpoint
	fake.mu.Unlock()
	if currentEndpoint != operation.PreviousRuntimeEndpoint {
		t.Fatalf("proxy endpoint=%q, want prior endpoint %q", currentEndpoint,
			operation.PreviousRuntimeEndpoint)
	}
	var activeDigest string
	if err := db.QueryRow(`SELECT release_digest FROM hosting_releases
		WHERE hosting_project_id=? AND status='active'`, fixture.project.ID).Scan(&activeDigest); err != nil {
		t.Fatal(err)
	}
	if activeDigest != fixture.currentDigest {
		t.Fatalf("active release=%q, want prior release %q", activeDigest, fixture.currentDigest)
	}
}

func TestHostingRollbackSupersededCompensationSettlesReceiptAtomically(t *testing.T) {
	fixture, _ := setupHostingRollbackTestFixture(t)
	requestHash := hashHostingOperation(hostingRollbackOperation, fixture.project.ExternalProjectID,
		fixture.targetDeploymentID, fixture.targetDigest)
	first, replay, err := stageHostingRollbackOperation(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest,
		"rollback_superseded_source_01", requestHash)
	if err != nil || replay != nil {
		t.Fatalf("stage first=%+v replay=%+v err=%v", first, replay, err)
	}
	if _, replay, err := stageHostingRollbackOperation(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest,
		"rollback_superseding_source_01", requestHash); err != nil || replay != nil {
		t.Fatalf("stage superseding replay=%+v err=%v", replay, err)
	}
	result, err := compensateAndSettleHostingRollback(t.Context(), first,
		hostingRollbackTerminalError(errCodeReleaseNotHealthy))
	if err != nil || result == nil || result.StatusCode != http.StatusConflict {
		t.Fatalf("settle superseded result=%+v err=%v", result, err)
	}
	var operationStatus, operationError string
	var receiptStatus int
	if err := db.QueryRow(`SELECT operation.status, operation.last_error_code, receipt.response_status
		FROM hosting_proxy_operations operation JOIN hosting_idempotency receipt
		  ON receipt.operation_reference=operation.operation_id
		WHERE operation.operation_id=?`, first.OperationID).Scan(
		&operationStatus, &operationError, &receiptStatus); err != nil {
		t.Fatal(err)
	}
	if operationStatus != "failed" || operationError != errCodeReleaseNotHealthy ||
		receiptStatus != http.StatusConflict {
		t.Fatalf("operation status=%q error=%q receipt=%d", operationStatus,
			operationError, receiptStatus)
	}
}

func TestHostingRollbackRejectedFallbackCompensationFailsClosed(t *testing.T) {
	fixture, fake := setupHostingRollbackTestFixture(t)
	requestHash := hashHostingOperation(hostingRollbackOperation, fixture.project.ExternalProjectID,
		fixture.targetDeploymentID, fixture.targetDigest)
	operation, replay, err := stageHostingRollbackOperation(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest,
		"rollback_rejected_compensation_01", requestHash)
	if err != nil || replay != nil {
		t.Fatalf("stage operation=%+v replay=%+v err=%v", operation, replay, err)
	}
	if _, err := fake.Activate(t.Context(), proxyActivationRequest{
		OperationID: operation.OperationID, ExternalProjectID: operation.ExternalProjectID,
		ReleaseDigest: operation.ReleaseDigest, RuntimeEndpoint: operation.RuntimeEndpoint,
		ExpectedPreviousReleaseDigest: operation.PreviousReleaseDigest,
		RouteGeneration:               operation.RouteGeneration,
	}); err != nil {
		t.Fatalf("apply ambiguous dispatch: %v", err)
	}
	fixture.targetHealthy.Store(false)
	fake.mu.Lock()
	fake.rejectOnce = true
	fake.mu.Unlock()
	if err := reconcileHostingRollbackOperations(t.Context()); err != nil {
		t.Fatalf("reconcile rejected compensation: %v", err)
	}
	fake.mu.Lock()
	suspended := fake.currentSuspended
	fake.mu.Unlock()
	if !suspended {
		t.Fatal("rejected fallback activation did not fail closed through corrective suspension")
	}
	var operationStatus, operationError string
	var receiptStatus int
	if err := db.QueryRow(`SELECT operation.status, operation.last_error_code, receipt.response_status
		FROM hosting_proxy_operations operation JOIN hosting_idempotency receipt
		  ON receipt.operation_reference=operation.operation_id
		WHERE operation.operation_id=?`, operation.OperationID).Scan(
		&operationStatus, &operationError, &receiptStatus); err != nil {
		t.Fatal(err)
	}
	if operationStatus != "failed" || operationError != errCodeReleaseNotHealthy ||
		receiptStatus != http.StatusConflict {
		t.Fatalf("operation status=%q error=%q receipt=%d", operationStatus,
			operationError, receiptStatus)
	}
}

func TestHostingRollbackCompletedReceiptExpiryCreatesFreshOperation(t *testing.T) {
	fixture, fake := setupHostingRollbackTestFixture(t)
	initialGeneration := hostingRollbackRouteGenerationForTest(t, fixture.project.ID)
	initialActivations := hostingRollbackActivationCountForTest(fake)
	const key = "rollback_completed_expiry_01"
	first, err := rollbackHostingReleaseResult(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key)
	if err != nil || first == nil || first.Replayed || first.StatusCode != http.StatusOK {
		t.Fatalf("first rollback=%+v err=%v", first, err)
	}
	var firstOperationID string
	if err := db.QueryRow(`SELECT operation_reference FROM hosting_idempotency
		WHERE issuer_token_id=? AND operation=? AND idempotency_key=?`, fixture.token.ID,
		hostingRollbackOperation, key).Scan(&firstOperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_idempotency SET expires_at=?
		WHERE issuer_token_id=? AND operation=? AND idempotency_key=?`,
		formatSQLiteTime(time.Now().UTC().Add(-time.Minute)), fixture.token.ID,
		hostingRollbackOperation, key); err != nil {
		t.Fatal(err)
	}

	second, err := rollbackHostingReleaseResult(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key)
	if err != nil || second == nil || second.Replayed || second.StatusCode != http.StatusOK {
		t.Fatalf("post-expiry rollback=%+v err=%v", second, err)
	}
	var secondOperationID string
	if err := db.QueryRow(`SELECT operation_reference FROM hosting_idempotency
		WHERE issuer_token_id=? AND operation=? AND idempotency_key=?`, fixture.token.ID,
		hostingRollbackOperation, key).Scan(&secondOperationID); err != nil {
		t.Fatal(err)
	}
	if firstOperationID == "" || secondOperationID == "" || firstOperationID == secondOperationID {
		t.Fatalf("operation IDs first=%q second=%q, want distinct epochs", firstOperationID, secondOperationID)
	}
	if got := hostingRollbackRouteGenerationForTest(t, fixture.project.ID); got != initialGeneration+2 {
		t.Fatalf("route generation=%d, want %d", got, initialGeneration+2)
	}
	if got := hostingRollbackActivationCountForTest(fake); got != initialActivations+2 {
		t.Fatalf("proxy activations=%d, want %d", got, initialActivations+2)
	}
	var operations int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_proxy_operations
		WHERE hosting_project_id=? AND operation_type='rollback'`, fixture.project.ID).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if operations != 2 {
		t.Fatalf("rollback operations=%d, want 2", operations)
	}
}

func TestHostingRollbackAdoptsReconciledLegacyOperationWithoutDuplicateEvent(t *testing.T) {
	fixture, fake := setupHostingRollbackTestFixture(t)
	initialActivations := hostingRollbackActivationCountForTest(fake)
	const key = "rollback_legacy_committed_01"
	legacyOperationID := hashHostingOperation(fmt.Sprint(fixture.token.ID), hostingRollbackOperation, key)
	var deploymentID, runnerID int64
	var endpoint, runtimeInstance, runtimeSession string
	if err := db.QueryRow(`SELECT release.hosting_deployment_id, release.runtime_runner_id,
		release.runtime_endpoint, release.runtime_instance_id, runner.active_session_id
		FROM hosting_releases release JOIN hosting_runners runner ON runner.id=release.runtime_runner_id
		WHERE release.hosting_project_id=? AND release.release_digest=?`, fixture.project.ID,
		fixture.targetDigest).Scan(&deploymentID, &runnerID, &endpoint, &runtimeInstance, &runtimeSession); err != nil {
		t.Fatal(err)
	}
	var previousEndpoint string
	if err := db.QueryRow(`SELECT runtime_endpoint FROM hosting_releases
		WHERE hosting_project_id=? AND release_digest=?`, fixture.project.ID,
		fixture.currentDigest).Scan(&previousEndpoint); err != nil {
		t.Fatal(err)
	}
	generation := hostingRollbackRouteGenerationForTest(t, fixture.project.ID)
	now := formatSQLiteTime(time.Now().UTC())
	if _, err := db.Exec(`INSERT INTO hosting_proxy_operations
		(operation_id, hosting_project_id, hosting_deployment_id, operation_type, release_digest,
		 runtime_endpoint, expected_previous_release_digest, expected_previous_runtime_endpoint,
		 target_runtime_runner_id, target_runtime_instance_id, target_runtime_session_id,
		 route_revision, route_generation, status, created_at, updated_at)
		VALUES (?, ?, ?, 'rollback', ?, ?, ?, ?, ?, ?, ?, '', ?, 'pending', ?, ?)`,
		legacyOperationID, fixture.project.ID, deploymentID, fixture.targetDigest, endpoint,
		fixture.currentDigest, previousEndpoint, runnerID, runtimeInstance, runtimeSession,
		generation, now, now); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHostingRollbackOperations(t.Context()); err != nil {
		t.Fatalf("reconcile receipt-less legacy operation: %v", err)
	}
	var rollbackEvents int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_events WHERE hosting_project_id=?
		AND event_type='release_rolled_back'`, fixture.project.ID).Scan(&rollbackEvents); err != nil {
		t.Fatal(err)
	}
	if rollbackEvents != 1 {
		t.Fatalf("legacy reconciliation recorded %d events, want 1", rollbackEvents)
	}
	if _, err := db.Exec(`UPDATE hosting_releases SET status='inactive', deactivated_at=?
		WHERE hosting_project_id=? AND release_digest=?`, formatSQLiteTime(time.Now().UTC()),
		fixture.project.ID, fixture.targetDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_releases SET status='active', deactivated_at=NULL
		WHERE hosting_project_id=? AND release_digest=?`, fixture.project.ID,
		fixture.currentDigest); err != nil {
		t.Fatal(err)
	}

	result, err := rollbackHostingReleaseResult(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key)
	if err != nil || result == nil || result.StatusCode != http.StatusOK || result.Release == nil ||
		result.Release.Status != "active" || result.Release.DeactivatedAt != nil {
		t.Fatalf("legacy adoption result=%+v err=%v", result, err)
	}
	if got := hostingRollbackActivationCountForTest(fake); got != initialActivations+1 {
		t.Fatalf("legacy adoption proxy activations=%d, want one reconciliation call", got-initialActivations)
	}
	var receiptStatus int
	if err := db.QueryRow(`SELECT response_status FROM hosting_idempotency
		WHERE operation_reference=?`, legacyOperationID).Scan(&receiptStatus); err != nil {
		t.Fatal(err)
	}
	if receiptStatus != http.StatusOK {
		t.Fatalf("legacy receipt status=%d", receiptStatus)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_events WHERE hosting_project_id=?
		AND event_type='release_rolled_back'`, fixture.project.ID).Scan(&rollbackEvents); err != nil {
		t.Fatal(err)
	}
	if rollbackEvents != 1 {
		t.Fatalf("legacy committed adoption recovered %d events, want 1", rollbackEvents)
	}
}

func TestHostingRollbackIssuerRotationReplayAndCrossIssuerIsolation(t *testing.T) {
	fixture, fake := setupHostingRollbackTestFixture(t)
	initialGeneration := hostingRollbackRouteGenerationForTest(t, fixture.project.ID)
	initialActivations := hostingRollbackActivationCountForTest(fake)
	const key = "rollback_rotated_issuer_01"
	first, err := rollbackHostingReleaseResult(t.Context(), fixture.token,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key)
	if err != nil || first == nil || first.StatusCode != http.StatusOK || first.Replayed {
		t.Fatalf("first rollback=%+v err=%v", first, err)
	}
	rotated, err := rotateServiceToken(fixture.token.ID)
	if err != nil {
		t.Fatalf("rotate service token: %v", err)
	}
	if rotated.ID != fixture.token.ID {
		t.Fatalf("rotation changed issuer identity from %d to %d", fixture.token.ID, rotated.ID)
	}
	replay, err := rollbackHostingReleaseResult(t.Context(), rotated,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key)
	if err != nil || replay == nil || !replay.Replayed || replay.StatusCode != http.StatusOK {
		t.Fatalf("rotated replay=%+v err=%v", replay, err)
	}
	if !bytes.Equal(first.ResponseBody, replay.ResponseBody) {
		t.Fatalf("rotated replay body differs:\nfirst=%s\nreplay=%s", first.ResponseBody, replay.ResponseBody)
	}
	if got := hostingRollbackRouteGenerationForTest(t, fixture.project.ID); got != initialGeneration+1 {
		t.Fatalf("rotated replay changed generation to %d", got)
	}
	if got := hostingRollbackActivationCountForTest(fake); got != initialActivations+1 {
		t.Fatalf("rotated replay changed proxy calls to %d", got)
	}

	otherIssuer, err := createServiceToken("rollback-cross-issuer",
		[]string{serviceScopeDeploymentsRead, serviceScopeDeploymentsWrite})
	if err != nil {
		t.Fatalf("create second issuer: %v", err)
	}
	isolated, err := rollbackHostingReleaseResult(t.Context(), otherIssuer,
		fixture.project.ExternalProjectID, fixture.targetDeploymentID, fixture.targetDigest, key)
	if err != nil || isolated == nil || isolated.Replayed || isolated.StatusCode != http.StatusOK {
		t.Fatalf("cross-issuer rollback=%+v err=%v", isolated, err)
	}
	if got := hostingRollbackRouteGenerationForTest(t, fixture.project.ID); got != initialGeneration+2 {
		t.Fatalf("cross-issuer generation=%d, want %d", got, initialGeneration+2)
	}
	if got := hostingRollbackActivationCountForTest(fake); got != initialActivations+2 {
		t.Fatalf("cross-issuer proxy calls=%d, want %d", got, initialActivations+2)
	}
	var receipts int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_idempotency
		WHERE operation=? AND idempotency_key=?`, hostingRollbackOperation, key).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 2 {
		t.Fatalf("issuer-scoped receipts=%d, want 2", receipts)
	}
}

func TestHostingRollbackHandlerReplaysExactTerminalError(t *testing.T) {
	fixture, fake := setupHostingRollbackTestFixture(t)
	initialGeneration := hostingRollbackRouteGenerationForTest(t, fixture.project.ID)
	initialActivations := hostingRollbackActivationCountForTest(fake)
	fake.mu.Lock()
	fake.rejectOnce = true
	fake.mu.Unlock()

	body, err := json.Marshal(rollbackRequest{
		ExternalDeploymentID: fixture.targetDeploymentID,
		ReleaseDigest:        fixture.targetDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	serve := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost,
			"/api/internal/v1/projects/"+fixture.project.ExternalProjectID+"/rollback",
			bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(idempotencyKeyHeader, "rollback_terminal_replay_01")
		request = request.WithContext(context.WithValue(request.Context(), serviceTokenContextKey{}, fixture.token))
		recorder := httptest.NewRecorder()
		handleInternalRollback(recorder, request, fixture.project.ExternalProjectID)
		return recorder
	}

	first := serve()
	if first.Code != http.StatusBadGateway || first.Header().Get("Idempotency-Replayed") != "" {
		t.Fatalf("first status=%d replay=%q body=%s", first.Code,
			first.Header().Get("Idempotency-Replayed"), first.Body.String())
	}
	firstBody := first.Body.String()
	assertAPIErrorCode(t, first, http.StatusBadGateway, errCodeProxyRejected)

	replay := serve()
	if replay.Code != http.StatusBadGateway || replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay status=%d replay=%q body=%s", replay.Code,
			replay.Header().Get("Idempotency-Replayed"), replay.Body.String())
	}
	if replay.Body.String() != firstBody {
		t.Fatalf("terminal replay body differs:\nfirst=%s\nreplay=%s", firstBody, replay.Body.String())
	}
	if got := hostingRollbackRouteGenerationForTest(t, fixture.project.ID); got != initialGeneration+2 {
		t.Fatalf("terminal replay changed route generation to %d", got)
	}
	if got := hostingRollbackActivationCountForTest(fake); got != initialActivations+2 {
		t.Fatalf("terminal replay changed proxy calls to %d", got)
	}
}

func TestHostingRollbackHandlerReplaysExactSuccess(t *testing.T) {
	fixture, fake := setupHostingRollbackTestFixture(t)
	initialGeneration := hostingRollbackRouteGenerationForTest(t, fixture.project.ID)
	initialActivations := hostingRollbackActivationCountForTest(fake)
	body, err := json.Marshal(rollbackRequest{
		ExternalDeploymentID: fixture.targetDeploymentID,
		ReleaseDigest:        fixture.targetDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	serve := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost,
			"/api/internal/v1/projects/"+fixture.project.ExternalProjectID+"/rollback",
			bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(idempotencyKeyHeader, "rollback_success_replay_01")
		request = request.WithContext(context.WithValue(request.Context(), serviceTokenContextKey{}, fixture.token))
		recorder := httptest.NewRecorder()
		handleInternalRollback(recorder, request, fixture.project.ExternalProjectID)
		return recorder
	}

	first := serve()
	if first.Code != http.StatusOK || first.Header().Get("Idempotency-Replayed") != "" {
		t.Fatalf("first status=%d replay=%q body=%s", first.Code,
			first.Header().Get("Idempotency-Replayed"), first.Body.String())
	}
	firstBody := first.Body.String()
	var release HostingRelease
	if err := json.Unmarshal(first.Body.Bytes(), &release); err != nil || release.Status != "active" ||
		release.RouteRevision == "" || release.ActivatedAt == nil {
		t.Fatalf("first release=%+v err=%v", release, err)
	}
	replay := serve()
	if replay.Code != http.StatusOK || replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay status=%d replay=%q body=%s", replay.Code,
			replay.Header().Get("Idempotency-Replayed"), replay.Body.String())
	}
	if replay.Body.String() != firstBody {
		t.Fatalf("success replay body differs:\nfirst=%s\nreplay=%s", firstBody, replay.Body.String())
	}
	if got := hostingRollbackRouteGenerationForTest(t, fixture.project.ID); got != initialGeneration+1 {
		t.Fatalf("success replay changed route generation to %d", got)
	}
	if got := hostingRollbackActivationCountForTest(fake); got != initialActivations+1 {
		t.Fatalf("success replay changed proxy calls to %d", got)
	}
}
