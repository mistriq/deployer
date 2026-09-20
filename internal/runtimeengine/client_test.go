package runtimeengine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func manifest() Manifest {
	return Manifest{Version: 1, Kind: "node-http", PolicyVersion: "policy-2026-07", Runtime: Runtime{Port: 3000, User: "10001:10001", ReadOnlyRoot: true, EnvNames: []string{"DATABASE_URL"}}, Health: Health{Path: "/healthz", ExpectStatusMin: 200, ExpectStatusMax: 299, TimeoutMS: 2000, IntervalMS: 500, Retries: 20, GracePeriodMS: 1000}, Resources: Resources{CPUMillis: 500, MemoryMB: 512, PidsLimit: 128, DiskMB: 1024}, Network: Network{MaxBodyBytes: 10485760, RateLimitRPS: 50}}
}
func client(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	c, e := New(Config{BaseURL: s.URL, Token: "secret-token", Sandbox: true})
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func TestManifestPolicy(t *testing.T) {
	if e := manifest().Validate(); e != nil {
		t.Fatal(e)
	}
	for name, edit := range map[string]func(*Manifest){"root": func(m *Manifest) { m.Runtime.User = "0:0" }, "writable": func(m *Manifest) { m.Runtime.ReadOnlyRoot = false }, "cpu": func(m *Manifest) { m.Resources.CPUMillis = 2001 }, "env-duplicate": func(m *Manifest) { m.Runtime.EnvNames = []string{"A", "A"} }, "mount-escape": func(m *Manifest) { m.Runtime.Tmpfs = []Tmpfs{{Path: "/tmp/../etc", SizeMB: 1}} }, "health-host": func(m *Manifest) { m.Health.Path = "https://evil.test" }} {
		t.Run(name, func(t *testing.T) {
			m := manifest()
			edit(&m)
			if m.Validate() == nil {
				t.Fatal("accepted unsafe manifest")
			}
		})
	}
}
func TestConfigRejectsUnsafeOrigin(t *testing.T) {
	for _, u := range []string{"http://runtime.example", "https://user:pass@runtime.example", "https://runtime.example?token=secret", "https://runtime.example/elsewhere"} {
		if _, e := New(Config{BaseURL: u, Token: "x"}); e == nil {
			t.Fatal("accepted", u)
		}
	}
}
func TestHeadersEnvironmentReplacementAndIdempotency(t *testing.T) {
	calls := 0
	c := client(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer secret-token" || r.Header.Get("X-Socen-Sandbox") != "true" {
			t.Fatal("missing security headers")
		}
		if r.Method == "PUT" {
			var b map[string]map[string]string
			if e := json.NewDecoder(r.Body).Decode(&b); e != nil || len(b["env"]) != 0 {
				t.Fatal("empty full replacement must be sent")
			}
			w.WriteHeader(204)
			return
		}
		if r.Header.Get("Idempotency-Key") != "dep-key" {
			t.Fatal("missing idempotency")
		}
		w.Write([]byte(`{"deployment":{"id":"dep_1","project_id":"prj_1"}}`))
	})
	if e := c.ReplaceEnvironment(context.Background(), "prj_1", map[string]string{}); e != nil {
		t.Fatal(e)
	}
	_, e := c.Deploy(context.Background(), DeploymentRequest{ProjectID: "prj_1", Artifact: Artifact{Image: "registry.example/app", Digest: "sha256:" + strings.Repeat("a", 64)}}, "dep-key")
	if e != nil || calls != 2 {
		t.Fatal(e, calls)
	}
}
func TestRedirectNeverForwardsCredential(t *testing.T) {
	called := false
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	defer dest.Close()
	c := client(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, dest.URL, 302) })
	if _, e := c.Capabilities(context.Background()); e == nil {
		t.Fatal("redirect accepted")
	}
	if called {
		t.Fatal("followed redirect")
	}
}
func TestErrorsNeverExposeRemoteMessages(t *testing.T) {
	c := client(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		w.Write([]byte(`{"error":{"code":"NODE_DRAINING","message":"secret-password","retryable":true}}`))
	})
	_, e := c.Capabilities(context.Background())
	var re *Error
	if !errors.As(e, &re) || re.Code != "NODE_DRAINING" || !re.Retryable || strings.Contains(e.Error(), "secret") {
		t.Fatal(e)
	}
}
func TestActivationRequiresHealthAndExactRoute(t *testing.T) {
	for _, tc := range []struct {
		name    string
		healthy bool
		release string
		applied int
		want    bool
	}{{"healthy", true, "rel_1", 4, true}, {"unhealthy", false, "rel_1", 4, false}, {"other-release", true, "rel_old", 4, false}, {"diverged", true, "rel_1", 3, false}} {
		t.Run(tc.name, func(t *testing.T) {
			c := client(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/internal/v1/deployments/dep_1":
					json.NewEncoder(w).Encode(Deployment{ID: "dep_1", ProjectID: "prj_1", ReleaseID: "rel_1", Status: "active"})
				case "/api/internal/v1/releases/rel_1":
					json.NewEncoder(w).Encode(Release{ID: "rel_1", ProjectID: "prj_1", Healthy: tc.healthy})
				case "/api/internal/v1/projects/prj_1":
					json.NewEncoder(w).Encode(Project{ID: "prj_1", ActiveReleaseID: "rel_1"})
				case "/api/internal/v1/routes":
					json.NewEncoder(w).Encode(map[string]any{"items": []Route{{ID: "route_1", ProjectID: "prj_1", ReleaseID: tc.release, Status: "active", DesiredRevision: 4, AppliedRevision: int64(tc.applied)}}})
				default:
					t.Fatal(r.URL.Path)
				}
			})
			v, e := c.VerifyActive(context.Background(), "prj_1", "dep_1")
			if e != nil || v.Active != tc.want {
				t.Fatal(v, e)
			}
		})
	}
}
func TestRollbackRejectsOtherProject(t *testing.T) {
	mutated := false
	c := client(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			mutated = true
		}
		json.NewEncoder(w).Encode(Release{ID: "rel_1", ProjectID: "prj_other"})
	})
	_, e := c.Rollback(context.Background(), "prj_1", "rel_1", "key")
	if e == nil || mutated {
		t.Fatal("cross-project rollback")
	}
}
func TestRejectPathInjection(t *testing.T) {
	c := client(t, func(w http.ResponseWriter, r *http.Request) { t.Fatal("invalid request reached server") })
	for _, s := range []string{"../projects", "x/y", "x?token=y", "x%2Fy"} {
		if _, e := c.Deployment(context.Background(), s); e == nil {
			t.Fatal(s)
		}
	}
}

func TestTerminalDeploymentStopsVerification(t *testing.T) {
	c := client(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(Deployment{ID: "dep_1", ProjectID: "prj_1", Status: "failed", FailureCode: "HEALTH_CHECK_FAILED"})
	})
	_, e := c.VerifyActive(context.Background(), "prj_1", "dep_1")
	var re *Error
	if !errors.As(e, &re) || re.Code != "HEALTH_CHECK_FAILED" || re.Retryable {
		t.Fatal(e)
	}
}
func TestCapabilitiesLowerLimits(t *testing.T) {
	c := Capabilities{ManifestVersion: 1, SupportedManifestKinds: []string{"node-http"}, Limits: Limits{MaxMemoryMB: 256}}
	if ValidateCapabilities(manifest(), c) == nil {
		t.Fatal("accepted manifest beyond node capacity")
	}
}
func TestLogsRedactServiceToken(t *testing.T) {
	c := client(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"logs":"never print secret-token"}`)) })
	logs, e := c.ReleaseLogs(context.Background(), "rel_1", 200)
	if e != nil || strings.Contains(logs, "secret-token") {
		t.Fatal(logs, e)
	}
}
