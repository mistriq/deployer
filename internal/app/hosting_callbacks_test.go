package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

func TestCallbackAcceptsEverySuccessfulHTTPStatus(t *testing.T) {
	for _, statusCode := range []int{http.StatusOK, http.StatusAccepted, http.StatusNoContent} {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			withTempDB(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(statusCode)
			}))
			defer server.Close()
			oldConfig := appConfig
			t.Cleanup(func() { appConfig = oldConfig })
			appConfig.CallbackURL = server.URL
			appConfig.CallbackSigningSecret = strings.Repeat("successful-status-secret", 2)
			appConfig.CallbackTimeout = time.Second
			identifier := strings.ReplaceAll(http.StatusText(statusCode), " ", "")
			createCancelledDeploymentWithCallback(t, "project_01JCB2XX"+identifier,
				"deployment_01JCB2XX"+identifier)
			if processed, err := deliverHostingCallbackOnce(t.Context(), time.Now().UTC()); err != nil || !processed {
				t.Fatalf("status %d processed=%v err=%v", statusCode, processed, err)
			}
			var status string
			if err := db.QueryRow(`SELECT status FROM callback_outbox`).Scan(&status); err != nil || status != "delivered" {
				t.Fatalf("status %d callback=%s err=%v", statusCode, status, err)
			}
		})
	}
}

func TestCallbackRedirectIsNotFollowedOrAcknowledged(t *testing.T) {
	withTempDB(t)
	var redirectedRequests atomic.Int32
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectedRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer redirectTarget.Close()
	redirectSource := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectTarget.URL, http.StatusTemporaryRedirect)
	}))
	defer redirectSource.Close()
	oldConfig := appConfig
	t.Cleanup(func() { appConfig = oldConfig })
	appConfig.CallbackURL = redirectSource.URL
	appConfig.CallbackSigningSecret = strings.Repeat("redirect-secret", 3)
	appConfig.CallbackMaxAttempts = 3
	appConfig.CallbackTimeout = time.Second
	createCancelledDeploymentWithCallback(t, "project_01JCBREDIRECT", "deployment_01JCBREDIRECT")

	processed, err := deliverHostingCallbackOnce(t.Context(), time.Now().UTC())
	if err != nil || !processed {
		t.Fatalf("redirect callback processed=%v err=%v", processed, err)
	}
	var status, errorCode string
	var attempts int
	if err := db.QueryRow(`SELECT status, attempts, last_error_code FROM callback_outbox`).Scan(
		&status, &attempts, &errorCode); err != nil {
		t.Fatal(err)
	}
	if redirectedRequests.Load() != 0 || status != "pending" || attempts != 1 ||
		errorCode != "callback_delivery_failed" {
		t.Fatalf("redirected=%d status=%s attempts=%d error=%s",
			redirectedRequests.Load(), status, attempts, errorCode)
	}
}

func TestCallbackEnvelopeCorruptionIsNeverSignedOrSent(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		tamper func(*testing.T)
	}{
		{name: "payload", tamper: func(t *testing.T) {
			if _, err := db.Exec(`UPDATE callback_outbox SET payload_json='{"status":"active"}'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "event id", tamper: func(t *testing.T) {
			if _, err := db.Exec(`UPDATE callback_outbox SET event_id=?`, "evt_"+strings.Repeat("a", 32)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "payload hash", tamper: func(t *testing.T) {
			if _, err := db.Exec(`UPDATE callback_outbox SET payload_hash=?`, "sha256:"+strings.Repeat("d", 64)); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			withTempDB(t)
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			oldConfig := appConfig
			t.Cleanup(func() { appConfig = oldConfig })
			appConfig.CallbackURL = server.URL
			appConfig.CallbackSigningSecret = strings.Repeat("corruption-secret", 3)
			appConfig.CallbackMaxAttempts = 12
			appConfig.CallbackTimeout = time.Second
			deployment := createCancelledDeploymentWithCallback(t,
				"project_01JCBCORRUPT"+strings.ReplaceAll(testCase.name, " ", ""),
				"deployment_01JCBCORRUPT"+strings.ReplaceAll(testCase.name, " ", ""))
			testCase.tamper(t)

			processed, err := deliverHostingCallbackOnce(t.Context(), time.Now().UTC())
			if err != nil || !processed {
				t.Fatalf("corrupt callback processed=%v err=%v", processed, err)
			}
			var status, errorCode, callbackState string
			var attempts int
			if err := db.QueryRow(`SELECT outbox.status, outbox.attempts, outbox.last_error_code,
				deployment.callback_state FROM callback_outbox outbox JOIN hosting_deployments deployment
				  ON deployment.id=outbox.hosting_deployment_id WHERE deployment.id=?`, deployment.ID).Scan(
				&status, &attempts, &errorCode, &callbackState); err != nil {
				t.Fatal(err)
			}
			if requests.Load() != 0 || status != "dead_letter" || callbackState != "dead_letter" ||
				attempts != 1 || errorCode != "callback_payload_corrupt" {
				t.Fatalf("requests=%d status=%s callback=%s attempts=%d error=%s",
					requests.Load(), status, callbackState, attempts, errorCode)
			}
		})
	}
}

func TestCallbackAmbiguousAcknowledgementRedeliversIdenticalEvent(t *testing.T) {
	withTempDB(t)
	secret := strings.Repeat("ambiguous-callback-secret", 2)
	type attempt struct {
		body, timestamp, eventID, signature string
	}
	var mu sync.Mutex
	var attempts []attempt
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		attempts = append(attempts, attempt{body: string(body), timestamp: r.Header.Get(callbackTimestampHeader),
			eventID: r.Header.Get(callbackEventIDHeader), signature: r.Header.Get(callbackSignatureHeader)})
		current := len(attempts)
		mu.Unlock()
		if current == 1 {
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack callback response: %v", err)
				return
			}
			_ = connection.Close()
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	oldConfig := appConfig
	t.Cleanup(func() { appConfig = oldConfig })
	appConfig.CallbackURL = server.URL
	appConfig.CallbackSigningSecret = secret
	appConfig.CallbackMaxAttempts = 3
	appConfig.CallbackTimeout = time.Second
	createCancelledDeploymentWithCallback(t, "project_01JCBAMBIG", "deployment_01JCBAMBIG")

	if processed, err := deliverHostingCallbackOnce(t.Context(), time.Now().UTC()); err != nil || !processed {
		t.Fatalf("ambiguous callback processed=%v err=%v", processed, err)
	}
	var nextAttempt string
	if err := db.QueryRow(`SELECT next_attempt_at FROM callback_outbox`).Scan(&nextAttempt); err != nil {
		t.Fatal(err)
	}
	if processed, err := deliverHostingCallbackOnce(t.Context(), parseSQLiteTime(nextAttempt).Add(time.Second)); err != nil || !processed {
		t.Fatalf("callback retry processed=%v err=%v", processed, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 2 || attempts[0].body != attempts[1].body || attempts[0].eventID != attempts[1].eventID ||
		attempts[0].timestamp == attempts[1].timestamp || attempts[0].signature == attempts[1].signature {
		t.Fatalf("callback attempts=%+v", attempts)
	}
	for _, delivery := range attempts {
		if !verifyHostingCallbackSignature(secret, delivery.timestamp, delivery.eventID,
			[]byte(delivery.body), delivery.signature) {
			t.Fatalf("invalid retry signature: %+v", delivery)
		}
	}
	var status string
	var count int
	if err := db.QueryRow(`SELECT status, attempts FROM callback_outbox`).Scan(&status, &count); err != nil {
		t.Fatal(err)
	}
	if status != "delivered" || count != 2 {
		t.Fatalf("callback status=%s attempts=%d", status, count)
	}
}

func TestCallbackBackoffStartsAfterFailureAndDeadLettersAtBound(t *testing.T) {
	withTempDB(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(75 * time.Millisecond)
		http.Error(w, "retry", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	oldConfig := appConfig
	t.Cleanup(func() { appConfig = oldConfig })
	appConfig.CallbackURL = server.URL
	appConfig.CallbackSigningSecret = strings.Repeat("backoff-secret", 3)
	appConfig.CallbackMaxAttempts = 4
	appConfig.CallbackTimeout = time.Second
	createCancelledDeploymentWithCallback(t, "project_01JCBBACKOFF", "deployment_01JCBBACKOFF")

	attemptStarted := time.Now().UTC()
	if processed, err := deliverHostingCallbackOnce(t.Context(), attemptStarted); err != nil || !processed {
		t.Fatalf("first callback processed=%v err=%v", processed, err)
	}
	var nextText string
	if err := db.QueryRow(`SELECT next_attempt_at FROM callback_outbox`).Scan(&nextText); err != nil {
		t.Fatal(err)
	}
	next := parseSQLiteTime(nextText)
	if next.Before(attemptStarted.Add(callbackBackoff(1) + 50*time.Millisecond)) {
		t.Fatalf("first retry deadline=%s attempt-start=%s", next, attemptStarted)
	}
	restartHostingDatabaseForCancellationTest(t)
	if _, err := claimHostingCallback(t.Context(), next.Add(-time.Millisecond)); err != sql.ErrNoRows {
		t.Fatalf("callback was claimable before retry deadline: %v", err)
	}
	for attemptNumber := 2; attemptNumber <= appConfig.CallbackMaxAttempts; attemptNumber++ {
		record, err := claimHostingCallback(t.Context(), next.Add(time.Millisecond))
		if err != nil {
			t.Fatalf("claim attempt %d: %v", attemptNumber, err)
		}
		completed := next.Add(time.Second)
		if err := rescheduleHostingCallback(t.Context(), record, io.ErrUnexpectedEOF, completed); err != nil {
			t.Fatalf("reschedule attempt %d: %v", attemptNumber, err)
		}
		var status string
		var finalized sql.NullString
		if err := db.QueryRow(`SELECT status, next_attempt_at, finalized_at FROM callback_outbox`).Scan(
			&status, &nextText, &finalized); err != nil {
			t.Fatal(err)
		}
		next = parseSQLiteTime(nextText)
		if attemptNumber < appConfig.CallbackMaxAttempts {
			if status != "pending" || finalized.Valid || next.Sub(completed) != callbackBackoff(attemptNumber) {
				t.Fatalf("attempt %d status=%s finalized=%v delay=%s", attemptNumber,
					status, finalized.Valid, next.Sub(completed))
			}
		} else if status != "dead_letter" || !finalized.Valid {
			t.Fatalf("final attempt status=%s finalized=%v", status, finalized.Valid)
		}
	}
	if callbackBackoff(100) != time.Hour {
		t.Fatalf("callback backoff cap=%s", callbackBackoff(100))
	}
}

func TestCallbackRecoveryMigrationReclaimsLegacyRowsAndNormalizesHistoricalTerminals(t *testing.T) {
	withTempDB(t)
	first := createCancelledDeploymentWithCallback(t, "project_01JCBMIGRATEA", "deployment_01JCBMIGRATEA")
	second := createCancelledDeploymentWithCallback(t, "project_01JCBMIGRATEB", "deployment_01JCBMIGRATEB")
	var legacyPayload, createdAt string
	if err := db.QueryRow(`SELECT payload_json, created_at FROM callback_outbox
		WHERE hosting_deployment_id=?`, first.ID).Scan(&legacyPayload, &createdAt); err != nil {
		t.Fatal(err)
	}
	var legacyEnvelope map[string]any
	if err := json.Unmarshal([]byte(legacyPayload), &legacyEnvelope); err != nil {
		t.Fatal(err)
	}
	delete(legacyEnvelope, "metadata")
	legacyBytes, err := json.Marshal(legacyEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	legacyHash := sha256.Sum256(legacyBytes)
	if _, err := db.Exec(`UPDATE callback_outbox SET status='delivering', attempts=1, locked_at=NULL,
		claim_token_hash='', payload_json=?, payload_hash=? WHERE hosting_deployment_id=?`,
		string(legacyBytes), "sha256:"+hex.EncodeToString(legacyHash[:]), first.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM callback_outbox WHERE hosting_deployment_id=?`, second.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM schema_migrations WHERE id='041_callback_recovery_and_retention'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE callback_outbox DROP COLUMN finalized_at`); err != nil {
		t.Fatal(err)
	}
	if err := applyHostingMigrations(); err != nil {
		t.Fatal(err)
	}
	var firstStatus, firstClaim, migratedPayload, migratedHash, secondCallback string
	if err := db.QueryRow(`SELECT status, claim_token_hash, payload_json, payload_hash FROM callback_outbox
		WHERE hosting_deployment_id=?`, first.ID).Scan(&firstStatus, &firstClaim, &migratedPayload, &migratedHash); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT callback_state FROM hosting_deployments WHERE id=?`, second.ID).Scan(
		&secondCallback); err != nil {
		t.Fatal(err)
	}
	var secondRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM callback_outbox WHERE hosting_deployment_id=?`, second.ID).Scan(
		&secondRows); err != nil {
		t.Fatal(err)
	}
	if firstStatus != "pending" || firstClaim != "" || migratedPayload != string(legacyBytes) ||
		migratedHash != "sha256:"+hex.EncodeToString(legacyHash[:]) || secondCallback != "dead_letter" || secondRows != 0 {
		t.Fatalf("legacy callback=%s/%q payload_preserved=%v hash=%q historical=%s rows=%d",
			firstStatus, firstClaim, migratedPayload == string(legacyBytes), migratedHash,
			secondCallback, secondRows)
	}
	var deliveredBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deliveredBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	oldConfig := appConfig
	t.Cleanup(func() { appConfig = oldConfig })
	appConfig.CallbackURL = server.URL
	appConfig.CallbackSigningSecret = strings.Repeat("legacy-callback-secret", 2)
	appConfig.CallbackTimeout = time.Second
	if processed, err := deliverHostingCallbackOnce(t.Context(), time.Now().UTC()); err != nil || !processed {
		t.Fatalf("deliver migrated legacy callback: processed=%v err=%v", processed, err)
	}
	var deliveredStatus string
	var attempts int
	if err := db.QueryRow(`SELECT status, attempts FROM callback_outbox WHERE hosting_deployment_id=?`,
		first.ID).Scan(&deliveredStatus, &attempts); err != nil {
		t.Fatal(err)
	}
	if deliveredStatus != "delivered" || attempts != 2 || !bytes.Equal(deliveredBody, legacyBytes) {
		t.Fatalf("status=%s attempts=%d exact_legacy_body=%v", deliveredStatus, attempts,
			bytes.Equal(deliveredBody, legacyBytes))
	}
}

func TestCallbackMigrationQuarantinesLegacySecretShapedIdentityButKeepsPolling(t *testing.T) {
	withTempDB(t)
	deployment := createCancelledDeploymentWithCallback(t,
		"project_01JCBLEGACYSECRET", "deployment_01JCBLEGACYSECRET")
	legacyID := "dpl_" + strings.Repeat("q", 48)
	if _, err := db.Exec(`UPDATE hosting_deployments SET external_deployment_id=? WHERE id=?`, legacyID,
		deployment.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM schema_migrations WHERE id='041_callback_recovery_and_retention'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE callback_outbox DROP COLUMN finalized_at`); err != nil {
		t.Fatal(err)
	}
	if err := applyHostingMigrations(); err != nil {
		t.Fatal(err)
	}
	var outboxStatus, errorCode, callbackState string
	if err := db.QueryRow(`SELECT callback.status, callback.last_error_code, deployment.callback_state
		FROM callback_outbox callback JOIN hosting_deployments deployment
		  ON deployment.id=callback.hosting_deployment_id WHERE deployment.id=?`, deployment.ID).Scan(
		&outboxStatus, &errorCode, &callbackState); err != nil {
		t.Fatal(err)
	}
	if outboxStatus != "dead_letter" || callbackState != "dead_letter" || errorCode != "callback_identity_invalid" {
		t.Fatalf("outbox=%s callback=%s error=%s", outboxStatus, callbackState, errorCode)
	}
	readToken, err := createServiceToken("legacy-identity-poller", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/internal/v1/deployments/"+legacyID, nil)
	request = request.WithContext(context.WithValue(request.Context(), serviceTokenContextKey{}, readToken))
	recorder := httptest.NewRecorder()
	handleInternalDeployment(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"callback_state":"dead_letter"`) {
		t.Fatalf("legacy poll status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestMalformedCallbackTimestampDeadLettersWithoutBlockingFollowingDelivery(t *testing.T) {
	withTempDB(t)
	first := createCancelledDeploymentWithCallback(t, "project_01JCBADTIMEA", "deployment_01JCBADTIMEA")
	second := createCancelledDeploymentWithCallback(t, "project_01JCBADTIMEB", "deployment_01JCBADTIMEB")
	if _, err := db.Exec(`UPDATE callback_outbox SET created_at='not-a-timestamp'
		WHERE hosting_deployment_id=?`, first.ID); err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	oldConfig := appConfig
	t.Cleanup(func() { appConfig = oldConfig })
	appConfig.CallbackURL = server.URL
	appConfig.CallbackSigningSecret = strings.Repeat("timestamp-corruption-secret", 2)
	appConfig.CallbackTimeout = time.Second
	for range 2 {
		if processed, err := deliverHostingCallbackOnce(t.Context(), time.Now().UTC()); err != nil || !processed {
			t.Fatalf("processed=%v err=%v", processed, err)
		}
	}
	var firstStatus, firstCode, secondStatus string
	if err := db.QueryRow(`SELECT status, last_error_code FROM callback_outbox
		WHERE hosting_deployment_id=?`, first.ID).Scan(&firstStatus, &firstCode); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM callback_outbox WHERE hosting_deployment_id=?`, second.ID).Scan(
		&secondStatus); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 || firstStatus != "dead_letter" || firstCode != "callback_payload_corrupt" ||
		secondStatus != "delivered" {
		t.Fatalf("requests=%d first=%s/%s second=%s", requests.Load(), firstStatus, firstCode, secondStatus)
	}
}

func TestConcurrentCallbackDispatchersDeliverEachEventOnce(t *testing.T) {
	withTempDB(t)
	const callbackCount = 4
	var mu sync.Mutex
	deliveries := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		deliveries[r.Header.Get(callbackEventIDHeader)]++
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	oldConfig := appConfig
	t.Cleanup(func() { appConfig = oldConfig })
	appConfig.CallbackURL = server.URL
	appConfig.CallbackSigningSecret = strings.Repeat("concurrent-callback-secret", 2)
	appConfig.CallbackTimeout = time.Second
	for index := 0; index < callbackCount; index++ {
		suffix := string(rune('A' + index))
		createCancelledDeploymentWithCallback(t, "project_01JCBCONCURRENT"+suffix,
			"deployment_01JCBCONCURRENT"+suffix)
	}
	var workers sync.WaitGroup
	errCh := make(chan error, 16)
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				processed, err := deliverHostingCallbackOnce(context.Background(), time.Now().UTC())
				if err != nil {
					errCh <- err
					return
				}
				if !processed {
					return
				}
			}
		}()
	}
	workers.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(deliveries) != callbackCount {
		t.Fatalf("delivered events=%v", deliveries)
	}
	for eventID, count := range deliveries {
		if eventID == "" || count != 1 {
			t.Fatalf("event %q deliveries=%d", eventID, count)
		}
	}
	var deliveredRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM callback_outbox WHERE status='delivered'`).Scan(&deliveredRows); err != nil || deliveredRows != callbackCount {
		t.Fatalf("delivered rows=%d err=%v", deliveredRows, err)
	}
}

func TestTerminalCallbackMetadataAndPollingRedactFailureSecrets(t *testing.T) {
	withTempDB(t)
	_, _, runnerID, job := createAndClaimHostingJobAtFetching(t,
		"project_01JCBREDACT", "deployment_01JCBREDACT")
	secret := "database-password-in-callback"
	failure := hostingCompletionRequest{Status: "failed", FailureCode: "build_failed",
		FailureMessage: "DATABASE_URL=postgres://app:" + secret + "@db.internal/app"}
	if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
		job.LeaseToken, failure); err != nil {
		t.Fatal(err)
	}
	var payload string
	if err := db.QueryRow(`SELECT payload_json FROM callback_outbox`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JCBREDACT")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(payload+deployment.FailureMessage, secret) || !strings.Contains(payload, "[REDACTED]") {
		t.Fatalf("callback=%s failure=%q", payload, deployment.FailureMessage)
	}
}

func installFailingCallbackInsertTrigger(t *testing.T) {
	t.Helper()
	if _, err := db.Exec(`CREATE TRIGGER fail_callback_outbox_insert BEFORE INSERT ON callback_outbox
		BEGIN SELECT RAISE(ABORT, 'injected callback outbox failure'); END`); err != nil {
		t.Fatal(err)
	}
}

func removeFailingCallbackInsertTrigger(t *testing.T) {
	t.Helper()
	if _, err := db.Exec(`DROP TRIGGER fail_callback_outbox_insert`); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalTransitionsRollbackWhenCallbackEnqueueFails(t *testing.T) {
	t.Run("queued cancellation", func(t *testing.T) {
		withTempDB(t)
		project, token := provisionDeploymentTestProject(t, "project_01JCBTXCANCEL")
		limits, _ := hostingLimitsForProfile(project.ResourceProfile)
		runnerID := insertHostingRunnerForTest(t, "callback-tx-cancel-runner", limits)
		request := validHostingDeploymentRequest("deployment_01JCBTXCANCEL")
		request.ManifestDigest = project.ManifestDigest
		if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID,
			"create_01JCBTXCANCEL", request); err != nil {
			t.Fatal(err)
		}
		installFailingCallbackInsertTrigger(t)
		if _, _, err := cancelHostingDeployment(t.Context(), token, request.ExternalDeploymentID,
			"cancel_01JCBTXCANCEL"); err == nil {
			t.Fatal("callback insertion failure did not roll back cancellation")
		}
		var jobStatus, deploymentStatus, phase string
		var callbacks, terminalEvents int
		var freeCPU int64
		if err := db.QueryRow(`SELECT job.status, deployment.status, deployment.phase
			FROM hosting_jobs job JOIN hosting_deployments deployment
			  ON deployment.id=job.hosting_deployment_id
			WHERE deployment.external_deployment_id=?`, request.ExternalDeploymentID).Scan(
			&jobStatus, &deploymentStatus, &phase); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM callback_outbox`).Scan(&callbacks); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_events WHERE event_type='deployment_cancelled'`).Scan(
			&terminalEvents); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT free_cpu_millis FROM hosting_runners WHERE id=?`, runnerID).Scan(&freeCPU); err != nil {
			t.Fatal(err)
		}
		if jobStatus != "queued" || deploymentStatus != hostingStatusQueued || phase != hostingPhaseQueued ||
			callbacks != 0 || terminalEvents != 0 || freeCPU != 0 {
			t.Fatalf("job=%s deployment=%s/%s callbacks=%d events=%d free=%d",
				jobStatus, deploymentStatus, phase, callbacks, terminalEvents, freeCPU)
		}
		removeFailingCallbackInsertTrigger(t)
		if _, _, err := cancelHostingDeployment(t.Context(), token, request.ExternalDeploymentID,
			"cancel_01JCBTXCANCEL"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("failed build", func(t *testing.T) {
		withTempDB(t)
		_, _, runnerID, job := createAndClaimHostingJobAtFetching(t,
			"project_01JCBTXFAIL", "deployment_01JCBTXFAIL")
		failure := hostingCompletionRequest{Status: "failed", FailureCode: "build_failed",
			FailureMessage: "build failed"}
		installFailingCallbackInsertTrigger(t)
		if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
			job.LeaseToken, failure); err == nil {
			t.Fatal("callback insertion failure did not roll back failed completion")
		}
		var jobStatus, deploymentStatus, phase string
		var callbacks, terminalEvents int
		if err := db.QueryRow(`SELECT job.status, deployment.status, deployment.phase
			FROM hosting_jobs job JOIN hosting_deployments deployment
			  ON deployment.id=job.hosting_deployment_id WHERE job.id=?`, job.JobID).Scan(
			&jobStatus, &deploymentStatus, &phase); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM callback_outbox`).Scan(&callbacks); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_events WHERE event_type='deployment_failed'`).Scan(
			&terminalEvents); err != nil {
			t.Fatal(err)
		}
		if jobStatus != "leased" || deploymentStatus != hostingStatusRunning || phase != hostingPhaseFetching ||
			callbacks != 0 || terminalEvents != 0 {
			t.Fatalf("job=%s deployment=%s/%s callbacks=%d events=%d",
				jobStatus, deploymentStatus, phase, callbacks, terminalEvents)
		}
		removeFailingCallbackInsertTrigger(t)
		if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
			job.LeaseToken, failure); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("active release", func(t *testing.T) {
		withTempDB(t)
		withFakeProxy(t)
		_, _, runnerID, job := createAndClaimHostingJob(t,
			"project_01JCBTXACTIVE", "deployment_01JCBTXACTIVE")
		digest := "sha256:" + strings.Repeat("c", 64)
		endpoint := healthyHostingEndpointForTest(t)
		observeHostingJobEndpointForTest(t, job.JobID, endpoint)
		completion := hostingCompletionRequest{Status: "success", ReleaseDigest: digest,
			ReleaseArtifactDigest: attachTestReleaseArtifact(t, job.JobID, digest),
			RuntimeEndpoint:       endpoint,
			HealthEvidence:        map[string]any{"healthy": true, "attempts": float64(1)}}
		installFailingCallbackInsertTrigger(t)
		if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
			job.LeaseToken, completion); err == nil {
			t.Fatal("callback insertion failure did not roll back active completion")
		}
		var jobStatus, deploymentStatus, phase string
		var callbacks, terminalEvents int
		if err := db.QueryRow(`SELECT job.status, deployment.status, deployment.phase
			FROM hosting_jobs job JOIN hosting_deployments deployment
			  ON deployment.id=job.hosting_deployment_id WHERE job.id=?`, job.JobID).Scan(
			&jobStatus, &deploymentStatus, &phase); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM callback_outbox`).Scan(&callbacks); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_events WHERE event_type='release_activated'`).Scan(
			&terminalEvents); err != nil {
			t.Fatal(err)
		}
		if jobStatus != "running" || deploymentStatus != hostingStatusRunning || phase != hostingPhaseActivating ||
			callbacks != 0 || terminalEvents != 0 {
			t.Fatalf("job=%s deployment=%s/%s callbacks=%d events=%d",
				jobStatus, deploymentStatus, phase, callbacks, terminalEvents)
		}
		removeFailingCallbackInsertTrigger(t)
		if err := completeHostingJob(t.Context(), runnerID, job.JobID, job.LeaseGeneration,
			job.LeaseToken, completion); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAuthenticatedDeploymentPollingRepairsTerminalCallbackState(t *testing.T) {
	withTempDB(t)
	project, _ := provisionDeploymentTestProject(t, "project_01JCBPOLL")
	readToken, err := createServiceToken("poll-reader", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatal(err)
	}
	deniedToken, err := createServiceToken("poll-denied", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	now := formatSQLiteTime(time.Now().UTC())
	type pollCase struct {
		externalID, status, phase, failureCode, callbackState, releaseDigest string
	}
	cases := []pollCase{
		{externalID: "deployment_01JCBPOLLA", status: hostingStatusActive, phase: hostingPhaseActive,
			callbackState: "pending", releaseDigest: "sha256:" + strings.Repeat("a", 64)},
		{externalID: "deployment_01JCBPOLLF", status: hostingStatusFailed, phase: hostingPhaseFailed,
			failureCode: "build_failed", callbackState: "dead_letter"},
		{externalID: "deployment_01JCBPOLLC", status: hostingStatusCancelled, phase: hostingPhaseCancelled,
			failureCode: "cancelled", callbackState: "delivered"},
	}
	for _, testCase := range cases {
		if _, err := db.Exec(`INSERT INTO hosting_deployments
			(hosting_project_id, external_deployment_id, commit_sha, manifest_digest, artifact_digest,
			 status, phase, failure_code, failure_message, release_digest, callback_state,
			 finished_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, project.ID, testCase.externalID,
			strings.Repeat("b", 40), project.ManifestDigest, "sha256:"+strings.Repeat("c", 64),
			testCase.status, testCase.phase, testCase.failureCode,
			"DATABASE_URL=postgres://app:poll-secret@db.internal/app", testCase.releaseDigest,
			testCase.callbackState, now, now, now); err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodGet,
			"/api/internal/v1/deployments/"+testCase.externalID, nil)
		request = request.WithContext(context.WithValue(request.Context(), serviceTokenContextKey{}, readToken))
		recorder := httptest.NewRecorder()
		handleInternalDeployment(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("poll %s status=%d body=%s", testCase.status, recorder.Code, recorder.Body.String())
		}
		var deployment HostingDeployment
		if err := json.Unmarshal(recorder.Body.Bytes(), &deployment); err != nil {
			t.Fatal(err)
		}
		if deployment.Status != testCase.status || deployment.Phase != testCase.phase ||
			deployment.FailureCode != testCase.failureCode || deployment.CallbackState != testCase.callbackState ||
			deployment.ReleaseDigest != testCase.releaseDigest ||
			strings.Contains(deployment.FailureMessage, "poll-secret") || deployment.LogReference == "" ||
			deployment.EventReference == "" || deployment.FinishedAt == nil {
			t.Fatalf("poll %s response=%+v", testCase.status, deployment)
		}
	}
	for _, authCase := range []struct {
		name   string
		token  *ServiceToken
		status int
		code   string
	}{
		{name: "missing token", status: http.StatusUnauthorized, code: errCodeServiceTokenRequired},
		{name: "insufficient scope", token: deniedToken, status: http.StatusForbidden,
			code: errCodeInsufficientScope},
	} {
		t.Run(authCase.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet,
				"/api/internal/v1/deployments/"+cases[0].externalID, nil)
			if authCase.token != nil {
				request = request.WithContext(context.WithValue(request.Context(), serviceTokenContextKey{}, authCase.token))
			}
			recorder := httptest.NewRecorder()
			handleInternalDeployment(recorder, request)
			var response apiErrorResponse
			_ = json.Unmarshal(recorder.Body.Bytes(), &response)
			if recorder.Code != authCase.status || response.Code != authCase.code {
				t.Fatalf("status=%d code=%s body=%s", recorder.Code, response.Code, recorder.Body.String())
			}
		})
	}
	handler := serviceTokenAuthMiddleware(http.HandlerFunc(handleInternalAPI))
	assertError := func(method, path, credential string, status int, code string) {
		t.Helper()
		request := httptest.NewRequest(method, path, nil)
		if credential != "" {
			request.Header.Set("Authorization", "Bearer "+credential)
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		var response apiErrorResponse
		_ = json.Unmarshal(recorder.Body.Bytes(), &response)
		if recorder.Code != status || response.Code != code {
			t.Fatalf("%s %s status=%d code=%s body=%s", method, path, recorder.Code,
				response.Code, recorder.Body.String())
		}
	}
	basePath := "/api/internal/v1/deployments/" + cases[0].externalID
	assertError(http.MethodGet, basePath, "invalid-service-token", http.StatusUnauthorized,
		errCodeInvalidServiceToken)
	assertError(http.MethodGet, "/api/internal/v1/deployments/deployment_01JCBPOLLMISSING",
		readToken.Token, http.StatusNotFound, errCodeDeploymentNotFound)
	assertError(http.MethodPost, basePath, readToken.Token, http.StatusMethodNotAllowed,
		errCodeMethodNotAllowed)
	if _, err := db.Exec(`ALTER TABLE hosting_deployments RENAME TO hosting_deployments_poll_failure`); err != nil {
		t.Fatal(err)
	}
	assertError(http.MethodGet, basePath, readToken.Token, http.StatusInternalServerError, errCodeInternal)
}

func TestCallbackRequestTimeoutLeavesLeaseSafetyMargin(t *testing.T) {
	if got := hostingCallbackRequestTimeout(0); got != 15*time.Second {
		t.Fatalf("default timeout=%s", got)
	}
	if got := hostingCallbackRequestTimeout(5 * time.Second); got != 5*time.Second {
		t.Fatalf("configured timeout=%s", got)
	}
	if got := hostingCallbackRequestTimeout(callbackLeaseTimeout - time.Nanosecond); got != callbackLeaseTimeout/2 {
		t.Fatalf("capped timeout=%s", got)
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
