// Package runtimeengine implements the scoped, server-side Runtime Engine API.
package runtimeengine

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type Config struct {
	BaseURL, Token string
	Sandbox        bool
	HTTPClient     *http.Client
}

// Protocols a Runtime target can speak. "internal-v1" is the original /api/internal/v1 API.
const (
	ProtocolInternalV1 = "internal-v1"
	ProtocolRunyer     = "runyer"
)

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
	// SMTPSubmission opens outgoing ports 465 and 587 (sending through a mailbox provider) on runyer 0.6.1+.
	// Port 25 and internal ranges stay closed. Needs EgressAllowed. Omitted when false, so older runtimes
	// get the same manifest as before.
	SMTPSubmission bool `json:"smtp_submission,omitempty"`
}
type ProjectRequest struct {
	ProjectID string   `json:"project_id"`
	Slug      string   `json:"slug"`
	Manifest  Manifest `json:"manifest"`
	// Hostname replaces the Runtime's own host name, e.g. with the customer's domain, so the Runtime's
	// address is neither served nor given a certificate (runyer only). Empty keeps the Runtime's choice.
	Hostname string `json:"hostname,omitempty"`
	// Aliases are custom domains served next to the host name (runyer only).
	Aliases []string `json:"aliases,omitempty"`
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
	// FailureMessage is runyer's own explanation of a failure (health check, start, pull…).
	FailureMessage string       `json:"failure_message,omitempty"`
	Events         []PhaseEvent `json:"events,omitempty"`
}
type PhaseEvent struct {
	Phase   string    `json:"phase"`
	At      time.Time `json:"at"`
	Code    string    `json:"code,omitempty"`
	Message string    `json:"message,omitempty"`
}
type LogEntry struct {
	ID     string    `json:"id,omitempty"`
	At     time.Time `json:"at"`
	Stream string    `json:"stream"`
	Text   string    `json:"text"`
}
type Release struct {
	DeploymentID                        string    `json:"deployment_id,omitempty"`
	RouteRevision                       int64     `json:"route_revision,omitempty"`
	ActivatedAt                         time.Time `json:"activated_at,omitempty"`
	healthyPresent, healthStatusPresent bool
	ID                                  string `json:"id"`
	ProjectID                           string `json:"project_id"`
	Status                              string `json:"status"`
	Healthy                             bool   `json:"healthy"`
	HealthStatus                        string `json:"health_status"`
}
type Route struct {
	ID              string   `json:"id"`
	ProjectID       string   `json:"project_id"`
	ReleaseID       string   `json:"release_id"`
	ActiveReleaseID string   `json:"active_release_id"`
	Status          string   `json:"status"`
	DesiredRevision int64    `json:"desired_revision"`
	AppliedRevision int64    `json:"applied_revision"`
	Hostname        string   `json:"hostname"`
	Aliases         []string `json:"aliases,omitempty"`
	HTTPS           bool     `json:"https,omitempty"`
	CertStatus      string   `json:"cert_status,omitempty"`
}
type Verification struct {
	Active    bool
	ReleaseID string
	Route     Route
}

func (d *Deployment) UnmarshalJSON(b []byte) error {
	type plain Deployment
	var v plain
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*d = Deployment(v)
	if d.Status == "" {
		d.Status = d.Phase
	}
	return nil
}
func (r *Release) UnmarshalJSON(b []byte) error {
	type plain Release
	var v plain
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return err
	}
	*r = Release(v)
	_, r.healthyPresent = fields["healthy"]
	_, r.healthStatusPresent = fields["health_status"]
	return nil
}
func (r *Route) UnmarshalJSON(b []byte) error {
	type plain Route
	var v struct {
		plain
		RouteID string `json:"route_id"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*r = Route(v.plain)
	if r.ID == "" {
		r.ID = v.RouteID
	}
	return nil
}
