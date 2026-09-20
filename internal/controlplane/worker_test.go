package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mistriq/deployer/internal/ocibuild"
	"github.com/mistriq/deployer/internal/runtimeengine"
)

type fakeBuilder struct {
	calls          int
	publishFailure bool
}

func (b *fakeBuilder) Resolve(context.Context, string, string) (string, error) {
	return strings.Repeat("a", 40), nil
}
func (b *fakeBuilder) Build(_ context.Context, r ocibuild.Request, log func(string)) (ocibuild.Artifact, error) {
	b.calls++
	log("build environment-secret-123 output")
	if r.OnPhase != nil {
		r.OnPhase("publishing")
	}
	if b.publishFailure {
		return ocibuild.Artifact{}, errors.New("registry authentication environment-secret-123")
	}
	digest := "sha256:" + strings.Repeat("b", 64)
	return ocibuild.Artifact{CommitSHA: r.Commit, ImageRef: r.ImageRepository + "@" + digest, Digest: digest}, nil
}

type fakeRuntime struct {
	requests       []runtimeengine.DeploymentRequest
	keys           []string
	env            map[string]string
	submitFailures int
	verifyError    error
	active         bool
}

func (r *fakeRuntime) Capabilities(context.Context) (runtimeengine.Capabilities, error) {
	return runtimeengine.Capabilities{ManifestVersion: 1, SupportedManifestKinds: []string{"node-http"}}, nil
}
func (r *fakeRuntime) UpsertProject(_ context.Context, p runtimeengine.ProjectRequest) (runtimeengine.Project, error) {
	return runtimeengine.Project{ID: p.ProjectID}, nil
}
func (r *fakeRuntime) ReplaceEnvironment(_ context.Context, _ string, env map[string]string) error {
	r.env = env
	return nil
}
func (r *fakeRuntime) Deploy(_ context.Context, p runtimeengine.DeploymentRequest, key string) (runtimeengine.Deployment, error) {
	r.requests = append(r.requests, p)
	r.keys = append(r.keys, key)
	if r.submitFailures > 0 {
		r.submitFailures--
		return runtimeengine.Deployment{}, &runtimeengine.Error{Code: "UNAVAILABLE", HTTPStatus: 503, Retryable: true}
	}
	return runtimeengine.Deployment{ID: "runtime-deploy"}, nil
}
func (r *fakeRuntime) VerifyActive(context.Context, string, string) (runtimeengine.Verification, error) {
	return runtimeengine.Verification{Active: r.active, ReleaseID: "release-1", Route: runtimeengine.Route{Hostname: "app.example.com"}}, r.verifyError
}
func (r *fakeRuntime) Logs(context.Context, string, int) (string, error) {
	return "runtime environment-secret-123 line", nil
}
func (r *fakeRuntime) ReleaseLogs(context.Context, string, int) (string, error) {
	return "runtime environment-secret-123 line", nil
}
func step(t *testing.T, s *Service, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if e := s.Step(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
}
func workerFixture(t *testing.T) (*Service, *fakeBuilder, *fakeRuntime, string) {
	t.Helper()
	s := testService(t)
	setupProject(t, s)
	b := &fakeBuilder{}
	r := &fakeRuntime{active: true}
	s.Builder = b
	s.Runtime = r
	return s, b, r, enqueue(t, s, "deployment-worker")
}
func TestWorkerPublishFailureNeverSubmitsArtifact(t *testing.T) {
	s, b, r, id := workerFixture(t)
	b.publishFailure = true
	step(t, s, 5)
	j := s.Store.snapshot().Jobs[id]
	if j.State != "build_failed" || j.ErrorCode != "REGISTRY_PUBLISH_FAILED" {
		t.Fatalf("unexpected state: %+v", j)
	}
	if len(r.requests) != 0 || j.Artifact != nil {
		t.Fatal("failed publish reached runtime")
	}
	w := request(t, s, "GET", "/internal/v1/deployments/"+id+"/logs?source=build", "token-a", nil)
	requireStatus(t, w, 200)
	if strings.Contains(w.Body.String(), "environment-secret-123") {
		t.Fatal("build logs exposed environment secret")
	}
}
func TestWorkerRetriesSubmissionAcrossRestartWithSameIdempotencyKey(t *testing.T) {
	s, b, r, id := workerFixture(t)
	r.submitFailures = 1
	step(t, s, 4)
	j := s.Store.snapshot().Jobs[id]
	if terminal(j.State) || j.RuntimeID != "" {
		t.Fatalf("transient submission must remain pending: %+v", j)
	}
	path := s.Store.path
	s.Store.Close()
	reopened, e := OpenStore(path, []byte{7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7})
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	s.Store = reopened
	step(t, s, 3)
	j = s.Store.snapshot().Jobs[id]
	if j.State != "online" || j.ReleaseID != "release-1" || j.SiteURL != "https://app.example.com" {
		t.Fatalf("not online after retry: %+v", j)
	}
	if b.calls != 1 || len(r.keys) != 2 || r.keys[0] != id || r.keys[1] != id {
		t.Fatalf("replay changed immutable identity: builds=%d keys=%v", b.calls, r.keys)
	}
	if r.requests[0] != r.requests[1] {
		t.Fatal("submission changed across restart")
	}
	w := request(t, s, "GET", "/internal/v1/deployments/"+id+"/logs?source=runtime", "token-a", nil)
	requireStatus(t, w, 200)
	if strings.Contains(w.Body.String(), "environment-secret-123") {
		t.Fatal("runtime logs exposed secret")
	}
}
func TestWorkerWaitsForVerifiedHealthAndPropagatesHealthFailure(t *testing.T) {
	s, _, r, id := workerFixture(t)
	r.active = false
	step(t, s, 6)
	j := s.Store.snapshot().Jobs[id]
	if j.State == "online" || j.SiteURL != "" {
		t.Fatal("unverified deployment advertised as online")
	}
	r.verifyError = &runtimeengine.Error{Code: "HEALTH_FAILED", HTTPStatus: 409}
	step(t, s, 1)
	j = s.Store.snapshot().Jobs[id]
	if j.State != "deployment_failed" || j.ErrorCode != "HEALTH_FAILED" || j.SiteURL != "" {
		t.Fatalf("health failure not terminal: %+v", j)
	}
}
func TestRollbackUsesHistoricalArtifactAndEnvironment(t *testing.T) {
	s, b, r, id := workerFixture(t)
	step(t, s, 6)
	old := s.Store.snapshot().Jobs[id]
	if old.State != "online" {
		t.Fatal("fixture did not deploy")
	}
	requireStatus(t, request(t, s, "PUT", "/internal/v1/projects/demo/env", "token-a", map[string]any{"request_id": "rotate-env", "env": map[string]string{"TOKEN": "new-secret"}}), 200)
	w := request(t, s, "POST", "/internal/v1/projects/demo/rollback", "token-a", map[string]string{"request_id": "rollback-history", "release_id": old.ReleaseID})
	requireStatus(t, w, 202)
	var response map[string]string
	json.Unmarshal(w.Body.Bytes(), &response)
	rollbackID := response["deployment_id"]
	step(t, s, 4)
	j := s.Store.snapshot().Jobs[rollbackID]
	if j.State != "online" || j.RollbackOf != id {
		t.Fatalf("rollback incomplete: %+v", j)
	}
	if b.calls != 1 {
		t.Fatal("rollback rebuilt historical release")
	}
	if r.env["TOKEN"] != "environment-secret-123" {
		t.Fatal("rollback used present environment")
	}
	if len(r.requests) != 2 || r.requests[1].Artifact.Digest != old.Artifact.Digest || r.keys[1] != rollbackID {
		t.Fatal("rollback lost immutable artifact or distinct request identity")
	}
}
