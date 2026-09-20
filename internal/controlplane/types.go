package controlplane

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/mistriq/deployer/internal/ocibuild"
	"github.com/mistriq/deployer/internal/runtimeengine"
)

type ProjectSpec struct {
	Repository string                 `json:"repository"`
	Ref        string                 `json:"ref"`
	Build      ocibuild.BuildSpec     `json:"build"`
	Manifest   runtimeengine.Manifest `json:"manifest"`
}
type Project struct {
	ID          string            `json:"project_id"`
	Tenant      string            `json:"tenant_id"`
	Revision    int               `json:"revision"`
	EnvRevision int               `json:"env_revision"`
	Spec        ProjectSpec       `json:"spec"`
	Env         map[string]string `json:"env"`
}
type Job struct {
	ID                  string             `json:"deployment_id"`
	Tenant              string             `json:"tenant_id"`
	ProjectID           string             `json:"project_id"`
	RequestID           string             `json:"request_id"`
	State               string             `json:"state"`
	Stage               string             `json:"stage"`
	ErrorCode           string             `json:"error_code,omitempty"`
	Message             string             `json:"message"`
	CreatedAt           time.Time          `json:"created_at"`
	UpdatedAt           time.Time          `json:"updated_at"`
	Commit              string             `json:"commit,omitempty"`
	BuildID             string             `json:"build_id"`
	RuntimeID           string             `json:"runtime_deployment_id,omitempty"`
	ReleaseID           string             `json:"runtime_release_id,omitempty"`
	SiteURL             string             `json:"site_url,omitempty"`
	Artifact            *ocibuild.Artifact `json:"artifact,omitempty"`
	Snapshot            Project            `json:"snapshot"`
	RollbackOf          string             `json:"rollback_of,omitempty"`
	VerificationStarted time.Time          `json:"verification_started,omitempty"`
	RuntimeTail         []string           `json:"runtime_tail,omitempty"`
}

func (j Job) public() any {
	return struct {
		ID        string    `json:"deployment_id"`
		ProjectID string    `json:"project_id"`
		RequestID string    `json:"request_id"`
		State     string    `json:"state"`
		Stage     string    `json:"stage"`
		ErrorCode string    `json:"error_code,omitempty"`
		Message   string    `json:"message"`
		CreatedAt time.Time `json:"created_at"`
		UpdatedAt time.Time `json:"updated_at"`
		Commit    string    `json:"commit,omitempty"`
		BuildID   string    `json:"build_id"`
		RuntimeID string    `json:"runtime_deployment_id,omitempty"`
		ReleaseID string    `json:"runtime_release_id,omitempty"`
		SiteURL   string    `json:"site_url,omitempty"`
	}{j.ID, j.ProjectID, j.RequestID, j.State, j.Stage, j.ErrorCode, j.Message, j.CreatedAt, j.UpdatedAt, j.Commit, j.BuildID, j.RuntimeID, j.ReleaseID, j.SiteURL}
}

type LogLine struct {
	Cursor int64     `json:"cursor"`
	Time   time.Time `json:"time"`
	Source string    `json:"source"`
	Stream string    `json:"stream"`
	Text   string    `json:"text"`
}
type Runtime interface {
	Capabilities(context.Context) (runtimeengine.Capabilities, error)
	UpsertProject(context.Context, runtimeengine.ProjectRequest) (runtimeengine.Project, error)
	ReplaceEnvironment(context.Context, string, map[string]string) error
	Deploy(context.Context, runtimeengine.DeploymentRequest, string) (runtimeengine.Deployment, error)
	VerifyActive(context.Context, string, string) (runtimeengine.Verification, error)
	Logs(context.Context, string, int) (string, error)
	ReleaseLogs(context.Context, string, int) (string, error)
}
type Builder interface {
	Resolve(context.Context, string, string) (string, error)
	Build(context.Context, ocibuild.Request, func(string)) (ocibuild.Artifact, error)
}
type Config struct {
	Tokens         map[string]string
	ImagePrefix    string
	Credentials    ocibuild.RegistryCredentials
	PollInterval   time.Duration
	ServiceSecrets []string
}
type Service struct {
	workerMu sync.Mutex
	logMu    sync.Mutex
	Store    *Store
	Runtime  Runtime
	Builder  Builder
	Config   Config
}

func terminal(s string) bool {
	return s == "online" || s == "build_failed" || s == "deployment_failed" || s == "cancelled"
}
func raw(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
