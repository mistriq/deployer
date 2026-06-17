package app

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type AppConfig struct {
	Addr                   string
	DBPath                 string
	AdminUser              string
	AdminPassword          string
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

func securityStatus(cfg AppConfig) SecurityStatus {
	if cfg.AdminPassword == "" {
		return SecurityStatus{
			Label: "Local-only",
			Title: "Admin authentication is disabled; only loopback clients are accepted.",
			Class: "local-only",
		}
	}
	if isPublicListenAddr(cfg.Addr) {
		return SecurityStatus{
			Label: "Auth enabled",
			Title: "Admin authentication is required; the server is listening on a public interface.",
			Class: "auth-enabled",
		}
	}
	return SecurityStatus{
		Label: "Auth enabled",
		Title: "Admin authentication is required; the server is listening on loopback.",
		Class: "auth-enabled",
	}
}

func loadConfig() AppConfig {
	cfg := AppConfig{
		Addr:                   getenvDefault("DEPLOYER_ADDR", "127.0.0.1:9090"),
		DBPath:                 getenvDefault("DEPLOYER_DB_PATH", "deployer.db"),
		AdminUser:              getenvDefault("DEPLOYER_ADMIN_USER", "admin"),
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
	cfg.AdminPassword = os.Getenv("DEPLOYER_ADMIN_PASSWORD")
	if cfg.AdminPassword == "" && isPublicListenAddr(cfg.Addr) {
		logFatal("config_error", "DEPLOYER_ADMIN_PASSWORD is required when DEPLOYER_ADDR is not loopback-only", nil, map[string]interface{}{
			"addr": cfg.Addr,
		})
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

func isPublicListenAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	host = strings.Trim(host, "[]")
	if host == "" || host == "0.0.0.0" || host == "::" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && !ip.IsLoopback()
}

func isLoopbackRemoteAddr(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
