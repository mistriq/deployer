package app

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRedactSecrets(t *testing.T) {
	input := strings.Join([]string{
		"Authorization: Bearer abc123",
		"DEPLOYER_TOKEN=secret-token",
		"https://example.test/api?token=query-secret",
		"deployer agent --token flag-secret",
		`DATABASE_URL=postgres://app:database-password@db.internal/app`,
		`{"api_key":"json-secret"}`,
		"service token dpl_1234567890abcdefghij",
		"github token github_pat_1234567890abcdefghij",
		"cloud access AKIA1234567890ABCDEF",
		"-----BEGIN PRIVATE KEY-----\nprivate-material\n-----END PRIVATE KEY-----",
	}, "\n")

	got := redactSecrets(input)
	for _, secret := range []string{"abc123", "secret-token", "query-secret", "flag-secret", "database-password", "json-secret", "dpl_1234567890abcdefghij", "github_pat_1234567890abcdefghij", "AKIA1234567890ABCDEF", "private-material"} {
		if strings.Contains(got, secret) {
			t.Fatalf("expected %q to be redacted from %q", secret, got)
		}
	}
	if count := strings.Count(got, "[REDACTED]"); count < 9 {
		t.Fatalf("expected at least 9 redactions, got %d in %q", count, got)
	}
}

func TestJSONErrorsRedactDynamicSecretValues(t *testing.T) {
	recorder := httptest.NewRecorder()
	jsonErrorCode(recorder, errCodeValidation,
		"upstream rejected Authorization: Bearer callback-error-secret", http.StatusBadRequest)
	if recorder.Code != http.StatusBadRequest || strings.Contains(recorder.Body.String(), "callback-error-secret") ||
		!strings.Contains(recorder.Body.String(), "[REDACTED]") {
		t.Fatalf("redacted error status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestHostingStructuredEventsRedactMetadataBeforePersistence(t *testing.T) {
	withTempDB(t)
	project, _ := provisionDeploymentTestProject(t, "project_01JCEVENTREDACT")
	secret := "structured-event-secret"
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := recordHostingEvent(t.Context(), conn, project.ID, 0, "redaction_test", "queued", "",
		map[string]any{"diagnostic": "Authorization: Bearer " + secret}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var metadata string
	if err := db.QueryRow(`SELECT metadata_json FROM hosting_events WHERE event_type='redaction_test'`).Scan(&metadata); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(metadata, secret) || !strings.Contains(metadata, "[REDACTED]") {
		t.Fatalf("event metadata was not redacted: %s", metadata)
	}
}

func TestUpdateBuildRedactsAuthoritativeErrorAndLogPersistence(t *testing.T) {
	withTempDB(t)
	project := &Project{Name: "redaction", RepoPath: t.TempDir(), DeployDir: t.TempDir(), DeployMode: "files"}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	build, err := createBuild(project.ID, "test")
	if err != nil {
		t.Fatalf("create build: %v", err)
	}
	build.Status = "failed"
	build.ErrorMessage = "command failed DATABASE_URL=postgres://app:database-password@db.internal/app"
	build.Log = `runner returned {"token":"json-secret"}`
	if err := updateBuild(build); err != nil {
		t.Fatalf("update build: %v", err)
	}
	stored, err := getBuild(build.ID)
	if err != nil {
		t.Fatalf("get build: %v", err)
	}
	for _, secret := range []string{"database-password", "json-secret"} {
		if strings.Contains(stored.ErrorMessage+stored.Log, secret) {
			t.Fatalf("persisted build contains secret %q: %+v", secret, stored)
		}
	}
}
