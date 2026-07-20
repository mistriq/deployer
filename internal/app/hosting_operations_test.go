package app

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
