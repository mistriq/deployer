package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigReadsEnvironment(t *testing.T) {
	artifactDir := filepath.Join(t.TempDir(), "artifacts")
	snapshotDir := filepath.Join(t.TempDir(), "snapshots")

	t.Setenv("DEPLOYER_ADDR", "127.0.0.1:9191")
	t.Setenv("DEPLOYER_DB_PATH", filepath.Join(t.TempDir(), "deployer.db"))
	t.Setenv("DEPLOYER_ADMIN_USER", "operator")
	t.Setenv("DEPLOYER_ADMIN_PASSWORD", "secret")
	t.Setenv("DEPLOYER_PUBLIC_URL", "https://deployer.example.com/")
	t.Setenv("DEPLOYER_ARTIFACT_DIR", artifactDir)
	t.Setenv("DEPLOYER_SNAPSHOT_DIR", snapshotDir)
	t.Setenv("DEPLOYER_LOG_RETENTION_DAYS", "14")
	t.Setenv("DEPLOYER_ARTIFACT_RETENTION_HOURS", "8")
	t.Setenv("DEPLOYER_DOCKER_PRUNE", "true")
	t.Setenv("DEPLOYER_SERVER_READ_TIMEOUT", "10s")
	t.Setenv("DEPLOYER_SERVER_WRITE_TIMEOUT", "2m")
	t.Setenv("DEPLOYER_DOCKER_BUILD_TIMEOUT", "20m")
	t.Setenv("DEPLOYER_SCP_TIMEOUT", "3m")
	t.Setenv("DEPLOYER_SSH_TIMEOUT", "90s")
	t.Setenv("DEPLOYER_HEALTH_CHECK_TIMEOUT", "15s")
	t.Setenv("DEPLOYER_DEMO_MODE", "true")

	cfg := loadConfig()
	if cfg.Addr != "127.0.0.1:9191" || cfg.AdminUser != "operator" || cfg.AdminPassword != "secret" {
		t.Fatalf("unexpected auth/listen config: %+v", cfg)
	}
	if cfg.PublicURL != "https://deployer.example.com" {
		t.Fatalf("expected public URL to be trimmed, got %q", cfg.PublicURL)
	}
	if cfg.ArtifactDir != artifactDir || cfg.SnapshotDir != snapshotDir {
		t.Fatalf("unexpected runtime dirs: %+v", cfg)
	}
	if cfg.LogRetentionDays != 14 || cfg.ArtifactRetentionHours != 8 || !cfg.DockerPrune {
		t.Fatalf("unexpected retention/prune config: %+v", cfg)
	}
	if cfg.ServerReadTimeout != 10*time.Second || cfg.ServerWriteTimeout != 2*time.Minute ||
		cfg.DockerBuildTimeout != 20*time.Minute || cfg.SCPTimeout != 3*time.Minute ||
		cfg.SSHTimeout != 90*time.Second || cfg.HealthCheckTimeout != 15*time.Second {
		t.Fatalf("unexpected timeout config: %+v", cfg)
	}
	if !cfg.DemoMode {
		t.Fatal("expected demo mode to be enabled")
	}
}

func TestConfigHelpersUseDefaultsAndParseValues(t *testing.T) {
	const missing = "DEPLOYER_TEST_MISSING"
	_ = os.Unsetenv(missing)
	if got := getenvDefault(missing, "fallback"); got != "fallback" {
		t.Fatalf("expected string fallback, got %q", got)
	}
	if got := getenvIntDefault(missing, 7); got != 7 {
		t.Fatalf("expected int fallback, got %d", got)
	}
	if got := getenvDurationDefault(missing, 5*time.Second); got != 5*time.Second {
		t.Fatalf("expected duration fallback, got %s", got)
	}
	if got := getenvBoolDefault(missing, true); !got {
		t.Fatal("expected bool fallback")
	}

	t.Setenv("DEPLOYER_TEST_STRING", " value ")
	t.Setenv("DEPLOYER_TEST_INT", "12")
	t.Setenv("DEPLOYER_TEST_DURATION", "45s")
	t.Setenv("DEPLOYER_TEST_BOOL_ON", "on")
	t.Setenv("DEPLOYER_TEST_BOOL_OFF", "no")

	if got := getenvDefault("DEPLOYER_TEST_STRING", "fallback"); got != "value" {
		t.Fatalf("expected trimmed string value, got %q", got)
	}
	if got := getenvIntDefault("DEPLOYER_TEST_INT", 0); got != 12 {
		t.Fatalf("expected parsed int, got %d", got)
	}
	if got := getenvDurationDefault("DEPLOYER_TEST_DURATION", time.Second); got != 45*time.Second {
		t.Fatalf("expected parsed duration, got %s", got)
	}
	if got := getenvBoolDefault("DEPLOYER_TEST_BOOL_ON", false); !got {
		t.Fatal("expected on to parse true")
	}
	if got := getenvBoolDefault("DEPLOYER_TEST_BOOL_OFF", true); got {
		t.Fatal("expected no to parse false")
	}
}

func TestRetentionDuration(t *testing.T) {
	if got := retentionDuration(0); got != 0 {
		t.Fatalf("expected disabled retention duration, got %s", got)
	}
	if got := retentionDuration(3); got != 3*time.Hour {
		t.Fatalf("expected hourly retention duration, got %s", got)
	}
}
