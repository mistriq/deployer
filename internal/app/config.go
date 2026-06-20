package app

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type AppConfig struct {
	Addr                   string
	DBPath                 string
	PublicURL              string
	ArtifactDir            string
	SnapshotDir            string
	LogRetentionDays       int
	ArtifactRetentionHours int
	DockerPrune            bool
	ServerReadTimeout      time.Duration
	ServerWriteTimeout     time.Duration
	DockerBuildTimeout     time.Duration
	SCPTimeout             time.Duration
	SSHTimeout             time.Duration
	HealthCheckTimeout     time.Duration
	DemoMode               bool
}

type SecurityStatus struct {
	Label string
	Title string
	Class string
}

func securityStatus() SecurityStatus {
	return SecurityStatus{
		Label: "External auth",
		Title: "Interactive authentication is delegated to the upstream authorization gateway.",
		Class: "external-auth",
	}
}

func loadConfig() AppConfig {
	cfg := AppConfig{
		Addr:                   getenvDefault("DEPLOYER_ADDR", "127.0.0.1:9090"),
		DBPath:                 getenvDefault("DEPLOYER_DB_PATH", "deployer.db"),
		PublicURL:              strings.TrimRight(os.Getenv("DEPLOYER_PUBLIC_URL"), "/"),
		ArtifactDir:            getenvDefault("DEPLOYER_ARTIFACT_DIR", filepath.Join(os.TempDir(), "deployer-artifacts")),
		SnapshotDir:            getenvDefault("DEPLOYER_SNAPSHOT_DIR", filepath.Join(os.TempDir(), "deployer-snapshots")),
		LogRetentionDays:       getenvIntDefault("DEPLOYER_LOG_RETENTION_DAYS", 30),
		ArtifactRetentionHours: getenvIntDefault("DEPLOYER_ARTIFACT_RETENTION_HOURS", 24),
		DockerPrune:            getenvBoolDefault("DEPLOYER_DOCKER_PRUNE", false),
		ServerReadTimeout:      getenvDurationDefault("DEPLOYER_SERVER_READ_TIMEOUT", 30*time.Second),
		ServerWriteTimeout:     getenvDurationDefault("DEPLOYER_SERVER_WRITE_TIMEOUT", 5*time.Minute),
		DockerBuildTimeout:     getenvDurationDefault("DEPLOYER_DOCKER_BUILD_TIMEOUT", 15*time.Minute),
		SCPTimeout:             getenvDurationDefault("DEPLOYER_SCP_TIMEOUT", 10*time.Minute),
		SSHTimeout:             getenvDurationDefault("DEPLOYER_SSH_TIMEOUT", 5*time.Minute),
		HealthCheckTimeout:     getenvDurationDefault("DEPLOYER_HEALTH_CHECK_TIMEOUT", 60*time.Second),
		DemoMode:               getenvBoolDefault("DEPLOYER_DEMO_MODE", false),
	}
	return cfg
}

func getenvDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func getenvIntDefault(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		logFatal("config_error", key+" must be a non-negative integer", err, map[string]interface{}{
			"key": key,
		})
	}
	return n
}

func getenvDurationDefault(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		logFatal("config_error", key+" must be a non-negative duration, for example 30s or 5m", err, map[string]interface{}{
			"key": key,
		})
	}
	return d
}

func getenvBoolDefault(key string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return fallback
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		logFatal("config_error", key+" must be true or false", nil, map[string]interface{}{
			"key": key,
		})
	}
	return fallback
}

func retentionDuration(hours int) time.Duration {
	if hours <= 0 {
		return 0
	}
	return time.Duration(hours) * time.Hour
}
