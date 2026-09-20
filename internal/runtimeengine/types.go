// Package runtimeengine implements the scoped, server-side Runtime Engine API.
package runtimeengine

import (
	"context"
	"net/http"
)

type Config struct {
	BaseURL, Token string
	Sandbox        bool
	HTTPClient     *http.Client
}
type API interface {
	Capabilities(context.Context) (Capabilities, error)
	UpsertProject(context.Context, ProjectRequest) (Project, error)
	ReplaceEnvironment(context.Context, string, map[string]string) error
	Deploy(context.Context, DeploymentRequest, string) (Deployment, error)
	Deployment(context.Context, string) (Deployment, error)
	VerifyActive(context.Context, string, string) (Verification, error)
	Rollback(context.Context, string, string, string) (Deployment, error)
	Logs(context.Context, string, int) (string, error)
	ReleaseLogs(context.Context, string, int) (string, error)
}
type Capabilities struct {
	ManifestVersion        int      `json:"manifest_version"`
	SupportedManifestKinds []string `json:"supported_manifest_kinds"`
	Limits                 Limits   `json:"limits"`
}
type Limits struct{ MaxCPUMillis, MaxMemoryMB, MaxPidsLimit, MaxDiskMB, MaxEnvVars, MaxTmpfsMounts, MaxTmpfsMB, MaxHealthRetries, MaxHealthTimeoutMS, MaxHealthGraceMS int }
type Manifest struct {
	Version       int       `json:"version"`
	Kind          string    `json:"kind"`
	PolicyVersion string    `json:"policy_version"`
	Runtime       Runtime   `json:"runtime"`
	Health        Health    `json:"health"`
	Resources     Resources `json:"resources"`
	Network       Network   `json:"network"`
}
type Runtime struct {
	Port         int      `json:"port"`
	User         string   `json:"user"`
	ReadOnlyRoot bool     `json:"read_only_root"`
	Tmpfs        []Tmpfs  `json:"tmpfs"`
	EnvNames     []string `json:"env_names"`
}
type Tmpfs struct {
	Path   string `json:"path"`
	SizeMB int    `json:"size_mb"`
}
type Health struct {
	Path            string `json:"path"`
	ExpectStatusMin int    `json:"expect_status_min"`
	ExpectStatusMax int    `json:"expect_status_max"`
	TimeoutMS       int    `json:"timeout_ms"`
	IntervalMS      int    `json:"interval_ms"`
	Retries         int    `json:"retries"`
	GracePeriodMS   int    `json:"grace_period_ms"`
}
type Resources struct {
	CPUMillis int `json:"cpu_millis"`
	MemoryMB  int `json:"memory_mb"`
	PidsLimit int `json:"pids_limit"`
	DiskMB    int `json:"disk_mb"`
}
type Network struct {
	Websocket     bool  `json:"websocket"`
	EgressAllowed bool  `json:"egress_allowed"`
	MaxBodyBytes  int64 `json:"max_body_bytes"`
	RateLimitRPS  int   `json:"rate_limit_rps"`
}
type ProjectRequest struct {
	ProjectID string   `json:"project_id"`
	Slug      string   `json:"slug"`
	Manifest  Manifest `json:"manifest"`
}
type Project struct {
	ID              string `json:"id"`
	ProjectID       string `json:"project_id"`
	ActiveReleaseID string `json:"active_release_id"`
	RouteID         string `json:"route_id"`
}
type Artifact struct {
	Image  string `json:"image"`
	Digest string `json:"digest"`
}
type DeploymentRequest struct {
	ProjectID            string   `json:"project_id"`
	ExternalDeploymentID string   `json:"external_deployment_id,omitempty"`
	Artifact             Artifact `json:"artifact"`
}
type Deployment struct {
	ID          string `json:"id"`
	ProjectID   string `json:"project_id"`
	ReleaseID   string `json:"release_id"`
	Status      string `json:"status"`
	Phase       string `json:"phase"`
	FailureCode string `json:"failure_code,omitempty"`
}
type Release struct {
	ID           string `json:"id"`
	ProjectID    string `json:"project_id"`
	Status       string `json:"status"`
	Healthy      bool   `json:"healthy"`
	HealthStatus string `json:"health_status"`
}
type Route struct {
	ID              string `json:"id"`
	ProjectID       string `json:"project_id"`
	ReleaseID       string `json:"release_id"`
	ActiveReleaseID string `json:"active_release_id"`
	Status          string `json:"status"`
	DesiredRevision int64  `json:"desired_revision"`
	AppliedRevision int64  `json:"applied_revision"`
	Hostname        string `json:"hostname"`
}
type Verification struct {
	Active    bool
	ReleaseID string
	Route     Route
}
