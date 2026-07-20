package app

import (
	"strings"
	"testing"
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
