package controlplane

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/mistriq/deployer/internal/ocibuild"
	"github.com/mistriq/deployer/internal/runtimeengine"
)

// This fixture exercises the real HTTP adapter and service boundary. Builds are
// injected deterministic artifacts; this test does not execute Docker or registry pushes.
type sandboxRuntime struct {
	mu              sync.Mutex
	project, active string
	env             map[string]string
	deployments     map[string]runtimeengine.Deployment
	submissions     []runtimeengine.DeploymentRequest
	failNext        bool
	violations      []string
}

func (f *sandboxRuntime) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	send := func(v any) { json.NewEncoder(w).Encode(v) }
	if r.Header.Get("Authorization") != "Bearer runtime-test-token" {
		f.violations = append(f.violations, "missing runtime authentication")
		w.WriteHeader(401)
		return
	}
	if r.Method != "GET" && r.Header.Get("X-Socen-Sandbox") != "true" {
		f.violations = append(f.violations, "mutation missing sandbox header")
		w.WriteHeader(403)
		return
	}
	p := strings.TrimPrefix(r.URL.Path, "/api/internal/v1")
	switch {
	case p == "/meta/capabilities":
		send(runtimeengine.Capabilities{ManifestVersion: 1, SupportedManifestKinds: []string{"static", "node-http"}})
	case p == "/projects" && r.Method == "POST":
		var body runtimeengine.ProjectRequest
		json.NewDecoder(r.Body).Decode(&body)
		f.project = body.ProjectID
		send(runtimeengine.Project{ID: f.project, ActiveReleaseID: f.active})
	case strings.HasSuffix(p, "/env") && r.Method == "PUT":
		var body struct {
			Env map[string]string `json:"env"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		f.env = body.Env
		send(map[string]bool{"ok": true})
	case p == "/deployments" && r.Method == "POST":
		var body runtimeengine.DeploymentRequest
		json.NewDecoder(r.Body).Decode(&body)
		key := r.Header.Get("Idempotency-Key")
		if key == "" || key != body.ExternalDeploymentID {
			f.violations = append(f.violations, "invalid deployment idempotency key")
			w.WriteHeader(400)
			return
		}
		f.submissions = append(f.submissions, body)
		n := len(f.submissions)
		id := fmt.Sprintf("runtime-%d", n)
		release := fmt.Sprintf("release-%d", n)
		dep := runtimeengine.Deployment{ID: id, ProjectID: f.project, ReleaseID: release, Status: "active"}
		if f.failNext {
			dep.Status = "failed"
			dep.FailureCode = "HEALTH_CHECK_FAILED"
			f.failNext = false
		} else {
			f.active = release
		}
		f.deployments[id] = dep
		send(dep)
	case strings.HasSuffix(p, "/logs"):
		send(map[string]string{"logs": "started environment-secret-123 runtime-test-token\nhealthy"})
	case strings.HasPrefix(p, "/deployments/"):
		send(f.deployments[strings.TrimPrefix(p, "/deployments/")])
	case strings.HasPrefix(p, "/releases/"):
		send(runtimeengine.Release{ID: strings.TrimPrefix(p, "/releases/"), ProjectID: f.project, Healthy: true, Status: "active"})
	case p == "/projects/"+f.project:
		send(runtimeengine.Project{ID: f.project, ActiveReleaseID: f.active})
	case p == "/routes":
		send(map[string]any{"items": []runtimeengine.Route{{ID: "route-1", ProjectID: f.project, ReleaseID: f.active, Status: "active", DesiredRevision: 1, AppliedRevision: 1, Hostname: "sandbox.example.com"}}})
	default:
		f.violations = append(f.violations, r.Method+" "+p)
		w.WriteHeader(404)
	}
}
func TestPortalHTTPToRuntimeSandboxLifecycle(t *testing.T) {
	for _, kind := range []string{"static", "node-http"} {
		t.Run(kind, func(t *testing.T) {
			fixture := &sandboxRuntime{deployments: map[string]runtimeengine.Deployment{}}
			upstream := httptest.NewServer(fixture)
			defer upstream.Close()
			client, e := runtimeengine.New(runtimeengine.Config{BaseURL: upstream.URL, Token: "runtime-test-token", Sandbox: true})
			if e != nil {
				t.Fatal(e)
			}
			s := testService(t)
			builder := &fakeBuilder{}
			s.Builder = builder
			s.Runtime = client
			portal := httptest.NewServer(s)
			defer portal.Close()
			call := func(method, path string, body any, status int) []byte {
				t.Helper()
				b, _ := json.Marshal(body)
				r, e := http.NewRequest(method, portal.URL+path, bytes.NewReader(b))
				if e != nil {
					t.Fatal(e)
				}
				r.Header.Set("Authorization", "Bearer token-a")
				resp, e := portal.Client().Do(r)
				if e != nil {
					t.Fatal(e)
				}
				defer resp.Body.Close()
				out, e := io.ReadAll(resp.Body)
				if e != nil {
					t.Fatal(e)
				}
				if resp.StatusCode != status {
					t.Fatalf("%s %s status %d: %s", method, path, resp.StatusCode, out)
				}
				if strings.Contains(string(out), "environment-secret-123") || strings.Contains(string(out), "runtime-test-token") {
					t.Fatalf("secret exposed via %s", path)
				}
				return out
			}
			manifest := testManifest()
			manifest.Kind = kind
			spec := ocibuild.BuildSpec{Kind: kind, Port: 3000, StartCommand: "node server.js"}
			if kind == "static" {
				manifest.Runtime.Port = 8080
				spec.Port = 8080
				spec.OutputDir = "dist"
				spec.StartCommand = ""
			}
			call("PUT", "/internal/v1/projects/demo", map[string]any{"request_id": "setup", "repository": "https://github.com/example/app.git", "ref": "main", "build": spec, "manifest": manifest}, 200)
			call("PUT", "/internal/v1/projects/demo/env", map[string]any{"request_id": "secrets", "env": map[string]string{"TOKEN": "environment-secret-123"}}, 200)
			submit := func(endpoint, req, release string) string {
				t.Helper()
				payload := map[string]string{"request_id": req}
				if release != "" {
					payload["release_id"] = release
				}
				var response map[string]string
				json.Unmarshal(call("POST", "/internal/v1/projects/demo/"+endpoint, payload, 202), &response)
				return response["deployment_id"]
			}
			finish := func(id, want string) Job {
				t.Helper()
				for i := 0; i < 12; i++ {
					step(t, s, 1)
					var job Job
					if e := json.Unmarshal(call("GET", "/internal/v1/deployments/"+id, nil, 200), &job); e != nil {
						t.Fatal(e)
					}
					if terminal(job.State) {
						if job.State != want {
							t.Fatalf("deployment %s state=%s expected %s", id, job.State, want)
						}
						return job
					}
				}
				t.Fatal("deployment did not terminate")
				return Job{}
			}
			first := finish(submit("deployments", "deploy-first", ""), "online")
			if first.SiteURL != "https://sandbox.example.com" || first.ReleaseID == "" {
				t.Fatal("missing verified public URL/release")
			}
			for _, source := range []string{"build", "runtime"} {
				out := call("GET", "/internal/v1/deployments/"+first.ID+"/logs?source="+source, nil, 200)
				if !strings.Contains(string(out), "REDACTED") {
					t.Fatalf("%s log redaction was not exercised", source)
				}
			}
			fixture.mu.Lock()
			fixture.failNext = true
			fixture.mu.Unlock()
			failed := finish(submit("deployments", "deploy-failed", ""), "deployment_failed")
			if failed.ErrorCode != "HEALTH_CHECK_FAILED" || failed.SiteURL != "" {
				t.Fatal("health failure not safely exposed")
			}
			fixture.mu.Lock()
			active := fixture.active
			fixture.mu.Unlock()
			if active != first.ReleaseID {
				t.Fatal("failed deployment changed active route mapping")
			}
			rolled := finish(submit("rollback", "restore-first", first.ReleaseID), "online")
			if rolled.ReleaseID == first.ReleaseID {
				t.Fatal("rollback must create a new runtime release from old artifact")
			}
			if builder.calls != 2 {
				t.Fatal("historical rollback ran a fresh build")
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if len(fixture.violations) > 0 {
				t.Fatalf("sandbox contract violations: %v", fixture.violations)
			}
			if len(fixture.submissions) != 3 || fixture.submissions[0].Artifact != fixture.submissions[2].Artifact {
				t.Fatal("rollback artifact differs from historical artifact")
			}
			if fixture.active != rolled.ReleaseID {
				t.Fatal("rollback did not restore an active mapping")
			}
		})
	}
}
