package app

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
	if metrics.Callbacks.Pending != 0 || metrics.Deployments.Terminal != 0 {
		t.Fatalf("unexpected terminal metrics = %+v callbacks=%+v", metrics.Deployments, metrics.Callbacks)
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
	if err := cleanupHostingRecords(t.Context(), AppConfig{
		HostingLogRetentionDays: 1, HostingEventRetentionDays: 1, HostingReleaseRetentionDays: 1,
		HostingCallbackRetentionDays: 1, HostingAuditRetentionDays: 1,
	}, time.Now().UTC()); err != nil {
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
}

func TestProtectActiveHostingArtifactsRefreshesReferencedSource(t *testing.T) {
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
	protectActiveHostingArtifacts()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(info.ModTime()) > time.Minute {
		t.Fatalf("active source artifact mtime was not refreshed: %v", info.ModTime())
	}
}
