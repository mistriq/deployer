package app

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func createCancelledDeploymentWithCallback(t *testing.T, projectID, deploymentID string) *HostingDeployment {
	t.Helper()
	project, token := provisionDeploymentTestProject(t, projectID)
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "runner-"+projectID, limits)
	request := validHostingDeploymentRequest(deploymentID)
	request.ManifestDigest = project.ManifestDigest
	result, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_"+deploymentID, request)
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	cancelled, _, err := cancelHostingDeployment(t.Context(), token, deploymentID, "cancel_"+deploymentID)
	if err != nil {
		t.Fatalf("cancel deployment: %v", err)
	}
	if cancelled.Status != hostingStatusCancelled {
		t.Fatalf("deployment was not cancelled: %+v", cancelled)
	}
	return result.Deployment
}

func TestCallbackOutboxSignsDeliversAndPollingReconciles(t *testing.T) {
	withTempDB(t)
	secret := strings.Repeat("callback-secret-", 3)
	var deliveries atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		timestamp := r.Header.Get(callbackTimestampHeader)
		eventID := r.Header.Get(callbackEventIDHeader)
		if !verifyHostingCallbackSignature(secret, timestamp, eventID, body, r.Header.Get(callbackSignatureHeader)) {
			t.Errorf("callback signature did not verify")
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("decode callback payload: %v", err)
		}
		if payload["status"] != hostingStatusCancelled || payload["external_deployment_id"] != "deployment_01JCALLBK" {
			t.Errorf("unexpected callback payload: %#v", payload)
		}
		deliveries.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	oldConfig := appConfig
	t.Cleanup(func() { appConfig = oldConfig })
	appConfig.CallbackURL = server.URL
	appConfig.CallbackSigningSecret = secret
	appConfig.CallbackMaxAttempts = 3
	appConfig.CallbackTimeout = time.Second
	createCancelledDeploymentWithCallback(t, "project_01JCALLBK", "deployment_01JCALLBK")

	delivered, err := deliverHostingCallbackOnce(context.Background(), time.Now().UTC())
	if err != nil || !delivered {
		t.Fatalf("deliver callback: delivered=%v err=%v", delivered, err)
	}
	delivered, err = deliverHostingCallbackOnce(context.Background(), time.Now().UTC())
	if err != nil || delivered {
		t.Fatalf("duplicate callback claim: delivered=%v err=%v", delivered, err)
	}
	if deliveries.Load() != 1 {
		t.Fatalf("callback deliveries = %d, want 1", deliveries.Load())
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JCALLBK")
	if err != nil {
		t.Fatalf("poll deployment: %v", err)
	}
	if deployment.CallbackState != "delivered" {
		t.Fatalf("polling callback state = %q", deployment.CallbackState)
	}
	var status string
	var attempts int
	if err := db.QueryRow(`SELECT status, attempts FROM callback_outbox`).Scan(&status, &attempts); err != nil {
		t.Fatalf("read callback outbox: %v", err)
	}
	if status != "delivered" || attempts != 1 {
		t.Fatalf("callback outbox status=%q attempts=%d", status, attempts)
	}
}

func TestCallbackFailureDeadLettersWithoutLosingDeploymentState(t *testing.T) {
	withTempDB(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	oldConfig := appConfig
	t.Cleanup(func() { appConfig = oldConfig })
	appConfig.CallbackURL = server.URL
	appConfig.CallbackSigningSecret = strings.Repeat("dead-letter-secret", 2)
	appConfig.CallbackMaxAttempts = 1
	appConfig.CallbackTimeout = time.Second
	createCancelledDeploymentWithCallback(t, "project_01JDEADLT", "deployment_01JDEADLT")

	processed, err := deliverHostingCallbackOnce(t.Context(), time.Now().UTC())
	if err != nil || !processed {
		t.Fatalf("process callback failure: processed=%v err=%v", processed, err)
	}
	var outboxStatus string
	if err := db.QueryRow(`SELECT status FROM callback_outbox`).Scan(&outboxStatus); err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if outboxStatus != "dead_letter" {
		t.Fatalf("outbox status = %q", outboxStatus)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JDEADLT")
	if err != nil {
		t.Fatalf("poll deployment: %v", err)
	}
	if deployment.Status != hostingStatusCancelled || deployment.CallbackState != "dead_letter" {
		t.Fatalf("terminal deployment state was lost: %+v", deployment)
	}
}

func TestCallbackSignatureBindsTimestampEventAndPayload(t *testing.T) {
	secret := strings.Repeat("signature-secret", 3)
	payload := []byte(`{"status":"failed"}`)
	signature := signHostingCallback(secret, "1710000000", "evt_12345678", payload)
	if !verifyHostingCallbackSignature(secret, "1710000000", "evt_12345678", payload, signature) {
		t.Fatal("valid callback signature was rejected")
	}
	if verifyHostingCallbackSignature(secret, "1710000001", "evt_12345678", payload, signature) ||
		verifyHostingCallbackSignature(secret, "1710000000", "evt_other", payload, signature) ||
		verifyHostingCallbackSignature(secret, "1710000000", "evt_12345678", []byte(`{"status":"active"}`), signature) {
		t.Fatal("callback signature did not bind all replay-sensitive fields")
	}
}

func TestCallbackStaleWorkerCannotCommitReclaimedDelivery(t *testing.T) {
	withTempDB(t)
	createCancelledDeploymentWithCallback(t, "project_01JCBFENC", "deployment_01JCBFENC")
	now := time.Now().UTC()
	first, err := claimHostingCallback(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE callback_outbox SET locked_at=? WHERE id=?`, formatSQLiteTime(now.Add(-callbackLeaseTimeout-time.Second)), first.ID); err != nil {
		t.Fatal(err)
	}
	second, err := claimHostingCallback(t.Context(), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if first.ClaimToken == second.ClaimToken {
		t.Fatal("reclaimed callback reused the claim token")
	}
	if err := markHostingCallbackDelivered(t.Context(), first, now.Add(2*time.Second)); err == nil {
		t.Fatal("stale callback worker committed a reclaimed delivery")
	}
	if err := markHostingCallbackDelivered(t.Context(), second, now.Add(3*time.Second)); err != nil {
		t.Fatalf("current callback worker could not commit: %v", err)
	}
}
