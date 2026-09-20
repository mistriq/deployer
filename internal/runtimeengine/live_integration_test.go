package runtimeengine

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// sandboxGuard is deliberately independent of Client's sandbox setting: an
// accidental future header regression must never turn a live test into a VPS deploy.
type sandboxGuard struct {
	next     http.RoundTripper
	projects map[string]bool
}

func (g sandboxGuard) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "https" || r.URL.Host != "scr.socen.eu" || !strings.HasPrefix(r.URL.Path, "/api/internal/v1/") || r.Header.Get("X-Socen-Sandbox") != "true" {
		return nil, errors.New("sandbox test refused unsafe target or missing header")
	}
	if r.Method != "GET" {
		allowed := false
		if r.Method == "PUT" {
			for project := range g.projects {
				if r.URL.Path == "/api/internal/v1/projects/"+project+"/env" {
					allowed = true
				}
			}
		} else if r.Method == "POST" && (r.URL.Path == "/api/internal/v1/projects" || r.URL.Path == "/api/internal/v1/deployments") && r.Body != nil {
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err != nil {
				return nil, errors.New("cannot validate sandbox request body")
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			var target struct {
				ProjectID string `json:"project_id"`
			}
			allowed = json.Unmarshal(body, &target) == nil && g.projects[target.ProjectID]
		}
		if !allowed {
			return nil, errors.New("sandbox test refused mutation outside fixture allowlist")
		}
	}
	return g.next.RoundTrip(r)
}
func TestSandboxGuardRejectsUnsafeRequests(t *testing.T) {
	for _, tc := range []struct{ url, method, header string }{
		{"https://scr.socen.eu/api/internal/v1/deployments", "POST", ""},
		{"https://other.invalid/api/internal/v1/deployments", "POST", "true"},
		{"http://scr.socen.eu/api/internal/v1/deployments", "POST", "true"},
		{"https://scr.socen.eu/api/internal/v1/node/stop", "POST", "true"},
		{"https://scr.socen.eu/api/internal/v1/projects/real/env", "PUT", "true"},
	} {
		r, _ := http.NewRequest(tc.method, tc.url, nil)
		r.Header.Set("X-Socen-Sandbox", tc.header)
		if _, err := (sandboxGuard{}).RoundTrip(r); err == nil {
			t.Fatal("unsafe request accepted")
		}
	}
}

// TestLiveSandbox is opt-in, never uses the non-sandbox node, never resets shared
// sandbox data, and does not emit credentials, env values, or raw response bodies.
// Its images are sandbox artifacts; actual Docker builds have a separate test.
func TestLiveSandbox(t *testing.T) {
	if os.Getenv("RUN_RUNTIME_SANDBOX_TESTS") != "1" {
		t.Skip("set RUN_RUNTIME_SANDBOX_TESTS=1 and RUNTIME_TOKEN_FILE to explicitly test scr.socen.eu sandbox")
	}
	path := os.Getenv("RUNTIME_TOKEN_FILE")
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		t.Fatal("RUNTIME_TOKEN_FILE must reference a private regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("cannot read token file")
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		t.Fatal("token file is empty")
	}
	nonce := make([]byte, 8)
	if _, err = rand.Read(nonce); err != nil {
		t.Fatal("cannot generate test identity")
	}
	suffix := hex.EncodeToString(nonce)
	projects := map[string]bool{"prj_codex_static_" + suffix: true, "prj_codex_node_http_" + suffix: true}
	artifact := Artifact{Image: os.Getenv("RUNTIME_SANDBOX_ARTIFACT_IMAGE"), Digest: os.Getenv("RUNTIME_SANDBOX_ARTIFACT_DIGEST")}
	if artifact.Validate() != nil {
		t.Fatal("set a confirmed sandbox artifact image and digest after authenticated read-only inspection")
	}
	c, err := New(Config{BaseURL: "https://scr.socen.eu", Token: token, Sandbox: true, HTTPClient: &http.Client{Transport: sandboxGuard{next: http.DefaultTransport, projects: projects}, Timeout: 20 * time.Second}})
	if err != nil {
		t.Fatal("invalid sandbox client configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	caps, err := c.Capabilities(ctx)
	if err != nil {
		t.Fatalf("sandbox capabilities: %v", err)
	}

	for _, kind := range []string{"static", "node-http"} {
		t.Run(kind, func(t *testing.T) {
			projectID := "prj_codex_" + strings.ReplaceAll(kind, "-", "_") + "_" + suffix
			m := manifest()
			m.Kind = kind
			m.Runtime.EnvNames = []string{}
			m.Runtime.Tmpfs = []Tmpfs{{Path: "/tmp", SizeMB: 64}}
			if kind == "static" {
				m.Runtime.Port = 8080
			}
			if err := m.ValidateCapabilities(caps); err != nil {
				t.Fatal("sandbox capabilities do not support test manifest")
			}
			request := ProjectRequest{ProjectID: projectID, Slug: "codex-" + kind + "-" + suffix, Manifest: m}
			first, err := c.UpsertProject(ctx, request)
			if err != nil {
				t.Fatalf("sandbox upsert: %v", err)
			}
			second, err := c.UpsertProject(ctx, request)
			if err != nil || first.ID != second.ID {
				t.Fatal("sandbox project upsert was not stable")
			}
			if err := c.ReplaceEnvironment(ctx, projectID, map[string]string{}); err != nil {
				t.Fatalf("sandbox env replacement: %v", err)
			}
			depReq := DeploymentRequest{ProjectID: projectID, ExternalDeploymentID: "dep_codex_" + strings.ReplaceAll(kind, "-", "_") + "_" + suffix, Artifact: artifact}
			dep, err := c.Deploy(ctx, depReq, depReq.ExternalDeploymentID)
			if err != nil {
				t.Fatalf("sandbox deployment: %v", err)
			}
			repeated, err := c.Deploy(ctx, depReq, depReq.ExternalDeploymentID)
			if err != nil || dep.ID != repeated.ID {
				t.Fatal("sandbox duplicate submission did not return same deployment")
			}
			var verified Verification
			for {
				verified, err = c.VerifyActive(ctx, projectID, dep.ID)
				if err != nil {
					t.Fatalf("sandbox activation verification: %v", err)
				}
				if verified.Active {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("sandbox activation not confirmed before deadline")
				case <-time.After(time.Second):
				}
			}
			if _, err = c.Logs(ctx, dep.ID, 20); err != nil {
				t.Fatalf("sandbox deployment logs: %v", err)
			}
			if _, err = c.ReleaseLogs(ctx, verified.ReleaseID, 20); err != nil {
				t.Fatalf("sandbox release logs: %v", err)
			}
			// Admission failure is a real sandbox request, deliberately bypassing local
			// artifact validation to prove server rejection; it must not displace release.
			_, err = c.call(ctx, "POST", "/deployments", DeploymentRequest{ProjectID: projectID, Artifact: Artifact{Image: depReq.Artifact.Image, Digest: "invalid"}}, "bad_"+depReq.ExternalDeploymentID)
			var remote *Error
			if !errors.As(err, &remote) || !((remote.HTTPStatus == 400 && remote.Code == "MANIFEST_INVALID") || (remote.HTTPStatus == 422 && remote.Code == "ARTIFACT_DIGEST_MISMATCH")) {
				t.Fatal("sandbox did not return the expected digest/manifest validation failure")
			}
			still, err := c.VerifyActive(ctx, projectID, dep.ID)
			if err != nil || !still.Active || still.ReleaseID != verified.ReleaseID {
				t.Fatal("failed admission displaced previous active sandbox release")
			}
			t.Logf("sandbox=%s project=%s deployment=%s release=%s: upsert/env/idempotency/active route/logs/admission failure PASS", (&url.URL{Scheme: "https", Host: "scr.socen.eu"}).String(), projectID, dep.ID, verified.ReleaseID)
		})
	}
}

func TestSandboxGuardRejectsUnownedProjectBody(t *testing.T) {
	for _, route := range []string{"projects", "deployments"} {
		r, _ := http.NewRequest("POST", "https://scr.socen.eu/api/internal/v1/"+route, strings.NewReader(`{"project_id":"prj_someone_else"}`))
		r.Header.Set("X-Socen-Sandbox", "true")
		guard := sandboxGuard{projects: map[string]bool{"prj_codex_ours": true}}
		if _, err := guard.RoundTrip(r); err == nil {
			t.Fatal("unowned fixture mutation accepted")
		}
	}
}
