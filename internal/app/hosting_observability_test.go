package app

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHostingEventTimelineReportsPhaseDurationsAndStableFailureCodes(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JTIMELINE")
	now := time.Now().UTC().Truncate(time.Second)
	result, err := db.Exec(`INSERT INTO hosting_deployments
		(hosting_project_id, external_deployment_id, commit_sha, manifest_digest, artifact_digest,
		 status, phase, callback_state, created_at, updated_at)
		VALUES (?, 'deployment_01JTIMELINE', ?, ?, ?, 'failed', 'failed', 'pending', ?, ?)`,
		project.ID, strings.Repeat("a", 40), project.ManifestDigest, "sha256:"+strings.Repeat("b", 64),
		formatSQLiteTime(now), formatSQLiteTime(now))
	if err != nil {
		t.Fatal(err)
	}
	deploymentID, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := recordHostingEvent(t.Context(), conn, project.ID, deploymentID, "deployment_created", hostingPhaseQueued, "", nil, now); err != nil {
		t.Fatal(err)
	}
	if err := recordHostingEvent(t.Context(), conn, project.ID, deploymentID, "phase_changed", hostingPhaseBuilding, "", nil, now.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := recordHostingEvent(t.Context(), conn, project.ID, deploymentID, "deployment_failed", hostingPhaseFailed, "build_failed", nil, now.Add(19*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := recordHostingEvent(t.Context(), conn, project.ID, deploymentID, "invalid", hostingPhaseFailed, "arbitrary text", nil, now); err == nil {
		t.Fatal("arbitrary failure code was accepted")
	}

	handler := serviceTokenAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalDeploymentEvents(w, r, "deployment_01JTIMELINE")
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/internal/v1/deployments/deployment_01JTIMELINE/events", nil)
	req.Header.Set("Authorization", "Bearer "+token.Token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("timeline status=%d body=%s", rec.Code, rec.Body.String())
	}
	var events []hostingEventResponse
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0].PhaseDurationSeconds != 7 || events[1].PhaseDurationSeconds != 12 ||
		events[2].PhaseDurationSeconds != 0 || events[2].FailureCode != "build_failed" {
		t.Fatalf("timeline events=%+v", events)
	}
}

func TestCollectHostingMetricsReportsQueueCapacityAndCallbacks(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JMETRIC")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "metrics-runner", limits)
	request := validHostingDeploymentRequest("deployment_01JMETRIC")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JMETRIC", request); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	insertTerminal := func(externalID, status, failureCode string, started time.Time) int64 {
		t.Helper()
		result, err := db.Exec(`INSERT INTO hosting_deployments
			(hosting_project_id, external_deployment_id, commit_sha, manifest_digest, artifact_digest,
			 status, phase, failure_code, callback_state, started_at, finished_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)`, project.ID, externalID,
			strings.Repeat("a", 40), project.ManifestDigest, "sha256:"+strings.Repeat("b", 64), status, status,
			failureCode, formatSQLiteTime(started), formatSQLiteTime(now), formatSQLiteTime(started), formatSQLiteTime(now))
		if err != nil {
			t.Fatal(err)
		}
		id, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	activeID := insertTerminal("deployment_01JMETRICSUCCESS", "active", "", now.Add(-20*time.Second))
	failedID := insertTerminal("deployment_01JMETRICFAILED", "failed", "build_failed", now.Add(-10*time.Second))
	if _, err := db.Exec(`INSERT INTO callback_outbox
		(event_id, hosting_deployment_id, payload_json, payload_hash, status, attempts, next_attempt_at, created_at)
		VALUES (?, ?, '{}', ?, 'pending', 0, ?, ?)`, "evt_"+strings.Repeat("a", 32), failedID,
		"sha256:"+strings.Repeat("c", 64), formatSQLiteTime(now), formatSQLiteTime(now.Add(-30*time.Second))); err != nil {
		t.Fatal(err)
	}
	if activeID == 0 {
		t.Fatal("missing successful deployment ID")
	}
	metrics, err := collectHostingMetrics(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Queue.QueuedJobs != 1 || metrics.Queue.OldestAgeSeconds < 0 {
		t.Fatalf("queue metrics = %+v", metrics.Queue)
	}
	if metrics.Runners.Online != 1 || metrics.Runners.Capacity.CPUMillis != limits.CPUMillis || metrics.Runners.Free.CPUMillis != 0 {
		t.Fatalf("runner metrics = %+v", metrics.Runners)
	}
	if metrics.Callbacks.Pending != 1 || metrics.Callbacks.OldestLagSeconds < 25 || metrics.Deployments.Terminal != 2 ||
		metrics.Deployments.Successful != 1 || metrics.Deployments.Failed != 1 || metrics.Deployments.SuccessRate != 0.5 ||
		metrics.Deployments.FailureCodes["build_failed"] != 1 || metrics.Deployments.AverageDurationSeconds <= 0 {
		t.Fatalf("terminal metrics = %+v callbacks=%+v", metrics.Deployments, metrics.Callbacks)
	}
}

func TestCleanupHostingRecordsAppliesConfiguredRetention(t *testing.T) {
	withTempDB(t)
	project, token := provisionDeploymentTestProject(t, "project_01JRETAIN")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "retention-runner", limits)
	request := validHostingDeploymentRequest("deployment_01JRETAIN")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JRETAIN", request); err != nil {
		t.Fatal(err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), request.ExternalDeploymentID)
	if err != nil {
		t.Fatal(err)
	}
	old := formatSQLiteTime(time.Now().UTC().Add(-48 * time.Hour))
	if _, err := db.Exec(`INSERT INTO hosting_logs (hosting_deployment_id, stream, message, created_at) VALUES (?, 'system', 'old', ?)`, deployment.ID, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_events SET created_at=? WHERE hosting_deployment_id=?`, old, deployment.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO hosting_audit_events (issuer_token_id, hosting_project_id, event_type, reason, created_at) VALUES (?, ?, 'test', 'old', ?)`, token.ID, project.ID, old); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	insertCallback := func(externalID, status string, finalizedAt any) int64 {
		t.Helper()
		result, err := db.Exec(`INSERT INTO hosting_deployments
			(hosting_project_id, external_deployment_id, commit_sha, manifest_digest, artifact_digest,
			 status, phase, callback_state, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, 'cancelled', 'cancelled', ?, ?, ?)`, project.ID, externalID,
			strings.Repeat("a", 40), project.ManifestDigest, "sha256:"+strings.Repeat("b", 64),
			status, old, old)
		if err != nil {
			t.Fatal(err)
		}
		deploymentID, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		lockedAt := any(nil)
		if status == "delivering" {
			lockedAt = old
		}
		eventRune := string(externalID[len(externalID)-1])
		if _, err := db.Exec(`INSERT INTO callback_outbox
			(event_id, hosting_deployment_id, payload_json, payload_hash, status, attempts,
			 next_attempt_at, created_at, finalized_at, locked_at)
			VALUES (?, ?, '{}', ?, ?, 1, ?, ?, ?, ?)`, "evt_"+strings.Repeat(eventRune, 32),
			deploymentID, "sha256:"+strings.Repeat("c", 64), status, old, old, finalizedAt, lockedAt); err != nil {
			t.Fatal(err)
		}
		return deploymentID
	}
	insertCallback("deployment_01JRETAINP", "pending", nil)
	insertCallback("deployment_01JRETAIND", "delivering", nil)
	insertCallback("deployment_01JRETAINR", "delivered", formatSQLiteTime(now))
	oldDeliveredID := insertCallback("deployment_01JRETAINO", "delivered", old)
	insertCallback("deployment_01JRETAINX", "dead_letter", old)
	if err := cleanupHostingRecords(t.Context(), AppConfig{
		HostingLogRetentionDays: 1, HostingEventRetentionDays: 1, HostingReleaseRetentionDays: 1,
		HostingCallbackRetentionDays: 1, HostingAuditRetentionDays: 1,
	}, now); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"hosting_logs", "hosting_events"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s count=%d err=%v", table, count, err)
		}
	}
	var oldAudits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_audit_events WHERE reason='old'`).Scan(&oldAudits); err != nil || oldAudits != 0 {
		t.Fatalf("old hosting audit count=%d err=%v", oldAudits, err)
	}
	var jobs int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_jobs`).Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("active hosting job was removed: count=%d err=%v", jobs, err)
	}
	var retainedCallbacks, removedCallbacks int
	if err := db.QueryRow(`SELECT COUNT(*) FROM callback_outbox
		WHERE status IN ('pending','delivering','delivered')`).Scan(&retainedCallbacks); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM callback_outbox
		WHERE hosting_deployment_id=? OR status='dead_letter'`, oldDeliveredID).Scan(&removedCallbacks); err != nil {
		t.Fatal(err)
	}
	if retainedCallbacks != 3 || removedCallbacks != 0 {
		t.Fatalf("retained callbacks=%d removed callbacks still present=%d", retainedCallbacks, removedCallbacks)
	}
	pollState, err := getHostingDeploymentByExternalID(t.Context(), "deployment_01JRETAINO")
	if err != nil || pollState.Status != hostingStatusCancelled || pollState.CallbackState != "delivered" {
		t.Fatalf("poll state after callback retention=%+v err=%v", pollState, err)
	}
}

func TestCleanupHostingRecordsPreservesExpiredIncompleteIdempotencyReceipts(t *testing.T) {
	withTempDB(t)
	token, err := createServiceToken("idempotency-retention", []string{serviceScopeDeploymentsWrite})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	createdAt := formatSQLiteTime(now.Add(-48 * time.Hour))
	expiresAt := formatSQLiteTime(now.Add(-24 * time.Hour))
	pendingReference := hashHostingOperation("rollback-pending-retention")
	completedReference := hashHostingOperation("rollback-completed-retention")
	if _, err := db.Exec(`INSERT INTO hosting_idempotency
		(issuer_token_id, operation, idempotency_key, request_hash, response_status, response_body,
		 operation_reference, created_at, expires_at)
		VALUES (?, 'hosting.project.rollback', 'rollback_pending_retention', ?, NULL, NULL, ?, ?, ?),
		       (?, 'hosting.project.rollback', 'rollback_complete_retention', ?, 200, '{}', ?, ?, ?)`,
		token.ID, hashHostingOperation("pending-request"), pendingReference, createdAt, expiresAt,
		token.ID, hashHostingOperation("completed-request"), completedReference, createdAt, expiresAt); err != nil {
		t.Fatal(err)
	}

	if err := cleanupHostingRecords(t.Context(), AppConfig{}, now); err != nil {
		t.Fatal(err)
	}

	var pending, completed int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_idempotency WHERE operation_reference=?`, pendingReference).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_idempotency WHERE operation_reference=?`, completedReference).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if pending != 1 || completed != 0 {
		t.Fatalf("expired idempotency receipts pending=%d completed=%d", pending, completed)
	}
}

func TestCleanupHostingRecordsRemovesExpiredInactiveReleaseArtifactsOnly(t *testing.T) {
	withTempDB(t)
	oldConfig := appConfig
	oldStorage := artifactStorage
	t.Cleanup(func() {
		appConfig = oldConfig
		artifactStorage = oldStorage
	})
	appConfig.ArtifactDir = filepath.Join(t.TempDir(), "artifacts")
	appConfig.SnapshotDir = filepath.Join(t.TempDir(), "snapshots")
	configureArtifactStorage(appConfig)
	if err := currentArtifactStorage().Ensure(); err != nil {
		t.Fatal(err)
	}
	project, token := provisionDeploymentTestProject(t, "project_01JRELEASEGC")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "release-gc-runner", limits)
	request := validHostingDeploymentRequest("deployment_01JRELEASEGC")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JRELEASEGC", request); err != nil {
		t.Fatal(err)
	}
	deployment, err := getHostingDeploymentByExternalID(t.Context(), request.ExternalDeploymentID)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	old := formatSQLiteTime(now.Add(-48 * time.Hour))
	activeDeployment, err := db.Exec(`INSERT INTO hosting_deployments
		(hosting_project_id, external_deployment_id, commit_sha, manifest_digest, artifact_digest,
		 status, phase, callback_state, created_at, updated_at)
		VALUES (?, 'deployment_01JRELEASEGCA', ?, ?, ?, 'active', 'active', 'delivered', ?, ?)`,
		project.ID, request.CommitSHA, project.ManifestDigest, request.ArtifactDigest, old, old)
	if err != nil {
		t.Fatal(err)
	}
	activeDeploymentID, err := activeDeployment.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	insertRelease := func(deploymentID int64, status, letter string) (string, string) {
		t.Helper()
		releaseDigest := "sha256:" + strings.Repeat(letter, 64)
		artifactDigest := "sha256:" + strings.Repeat(strings.ToUpper(letter), 64)
		path := managedArtifactPath("hosting-release-" + letter + ".tar")
		if err := os.WriteFile(path, []byte(status), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO hosting_release_artifacts
			(artifact_digest, release_digest, artifact_path, size_bytes, created_at) VALUES (?, ?, ?, ?, ?)`,
			artifactDigest, releaseDigest, path, int64(len(status)), old); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO hosting_releases
			(hosting_project_id, hosting_deployment_id, release_digest, release_artifact_digest, commit_sha,
			 artifact_digest, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, project.ID, deploymentID,
			releaseDigest, artifactDigest, request.CommitSHA, request.ArtifactDigest, status, old); err != nil {
			t.Fatal(err)
		}
		return artifactDigest, path
	}
	expiredArtifact, expiredPath := insertRelease(deployment.ID, "inactive", "a")
	activeArtifact, activePath := insertRelease(activeDeploymentID, "active", "b")
	if err := cleanupHostingRecords(t.Context(), AppConfig{HostingReleaseRetentionDays: 1}, now); err != nil {
		t.Fatal(err)
	}
	var expiredRows, activeRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_release_artifacts WHERE artifact_digest=?`, expiredArtifact).Scan(&expiredRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_release_artifacts WHERE artifact_digest=?`, activeArtifact).Scan(&activeRows); err != nil {
		t.Fatal(err)
	}
	if expiredRows != 0 || activeRows != 1 {
		t.Fatalf("release artifact records expired=%d active=%d", expiredRows, activeRows)
	}
	if _, err := os.Stat(expiredPath); !os.IsNotExist(err) {
		t.Fatalf("expired artifact remains: %v", err)
	}
	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("active artifact was removed: %v", err)
	}
}

func TestProtectActiveHostingArtifactsRefreshesReferencedSourceAndRelease(t *testing.T) {
	withTempDB(t)
	oldConfig := appConfig
	oldStorage := artifactStorage
	t.Cleanup(func() {
		appConfig = oldConfig
		artifactStorage = oldStorage
	})
	appConfig.ArtifactDir = filepath.Join(t.TempDir(), "artifacts")
	appConfig.SnapshotDir = filepath.Join(t.TempDir(), "snapshots")
	configureArtifactStorage(appConfig)
	if err := currentArtifactStorage().Ensure(); err != nil {
		t.Fatal(err)
	}
	project, token := provisionDeploymentTestProject(t, "project_01JARTPRO")
	limits, _ := hostingLimitsForProfile(project.ResourceProfile)
	insertHostingRunnerForTest(t, "artifact-protection-runner", limits)
	request := validHostingDeploymentRequest("deployment_01JARTPRO")
	request.ManifestDigest = project.ManifestDigest
	if _, err := createHostingDeployment(t.Context(), token, project.ExternalProjectID, "create_01JARTPRO", request); err != nil {
		t.Fatal(err)
	}
	path := managedArtifactPath("hosting-source-protected.tar")
	if err := os.WriteFile(path, []byte("source"), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-72 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hosting_jobs SET source_artifact_path=?`, path); err != nil {
		t.Fatal(err)
	}
	releasePath := managedArtifactPath("hosting-release-protected.tar")
	if err := os.WriteFile(releasePath, []byte("release"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(releasePath, old, old); err != nil {
		t.Fatal(err)
	}
	releaseArtifactDigest := "sha256:" + strings.Repeat("e", 64)
	releaseDigest := "sha256:" + strings.Repeat("f", 64)
	var deploymentID int64
	if err := db.QueryRow(`SELECT id FROM hosting_deployments WHERE external_deployment_id=?`, request.ExternalDeploymentID).Scan(&deploymentID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO hosting_release_artifacts
		(artifact_digest, release_digest, artifact_path, size_bytes, created_at) VALUES (?, ?, ?, ?, ?)`,
		releaseArtifactDigest, releaseDigest, releasePath, 7, formatSQLiteTime(time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO hosting_releases
		(hosting_project_id, hosting_deployment_id, release_digest, release_artifact_digest, commit_sha,
		 artifact_digest, status, created_at) VALUES (?, ?, ?, ?, ?, ?, 'active', ?)`, project.ID,
		deploymentID, releaseDigest, releaseArtifactDigest, request.CommitSHA, request.ArtifactDigest,
		formatSQLiteTime(time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	if err := protectActiveHostingArtifacts(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(info.ModTime()) > time.Minute {
		t.Fatalf("active source artifact mtime was not refreshed: %v", info.ModTime())
	}
	releaseInfo, err := os.Stat(releasePath)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(releaseInfo.ModTime()) > time.Minute {
		t.Fatalf("retained release artifact mtime was not refreshed: %v", releaseInfo.ModTime())
	}
}
