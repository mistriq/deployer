package runtimeengine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testDigest = "sha256:" + "ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12"

func runyerManifest() Manifest {
	return Manifest{Version: 1, Kind: "static", PolicyVersion: "policy-2026-07",
		Runtime:   Runtime{Port: 8080, User: "10001:10001", ReadOnlyRoot: true, Tmpfs: []Tmpfs{{Path: "/tmp", SizeMB: 64}}},
		Health:    Health{Path: "/", ExpectStatusMin: 200, ExpectStatusMax: 299, TimeoutMS: 2000, IntervalMS: 1000, Retries: 3},
		Resources: Resources{CPUMillis: 250, MemoryMB: 256, PidsLimit: 64, DiskMB: 512},
		Network:   Network{MaxBodyBytes: 1048576, RateLimitRPS: 50}}
}

type fakeRunyer struct {
	t        *testing.T
	phase    string
	active   string
	route    map[string]any
	requests []string
	bodies   map[string]map[string]any
	keys     map[string]string
}

func (f *fakeRunyer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer tk_test" {
		w.WriteHeader(401)
		_, _ = io.WriteString(w, `{"error":"UNAUTHENTICATED","http":401}`)
		return
	}
	f.requests = append(f.requests, r.Method+" "+r.URL.Path)
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.bodies[r.Method+" "+r.URL.Path] = body
	f.keys[r.URL.Path] = r.Header.Get("Idempotency-Key")
	reply := func(status int, v any) { w.WriteHeader(status); _ = json.NewEncoder(w).Encode(v) }
	switch {
	case r.URL.Path == "/api/instance":
		reply(200, map[string]any{"name": "runyer"})
	case r.Method == "POST" && r.URL.Path == "/api/projects":
		reply(200, map[string]any{"id": body["id"], "slug": body["slug"], "aliases": body["aliases"]})
	case r.URL.Path == "/api/projects/prj_1/env":
		reply(200, map[string]any{"version": 2})
	case r.URL.Path == "/api/projects/prj_1/deploy":
		reply(202, map[string]any{"id": "dep_1", "project_id": "prj_1", "phase": "queued", "external_id": body["external_id"]})
	case r.URL.Path == "/api/deployments/dep_1":
		reply(200, map[string]any{"id": "dep_1", "project_id": "prj_1", "phase": f.phase, "release_id": "rel_1", "failure_code": map[bool]string{true: "HEALTH_CHECK_FAILED"}[f.phase == "failed"]})
	case r.Method == "DELETE" && r.URL.Path == "/api/projects/prj_1":
		reply(200, map[string]any{"deleted": true})
	case r.Method == "DELETE" && r.URL.Path == "/api/projects/prj_kept":
		reply(403, map[string]any{"error": "forbidden"})
	case r.URL.Path == "/api/projects/prj_kept/suspend":
		reply(200, map[string]any{"status": "suspended"})
	case r.URL.Path == "/api/projects/prj_1":
		reply(200, map[string]any{"id": "prj_1", "hostname": "site.apps.example.com", "active_release_id": f.active, "route": f.route})
	case r.URL.Path == "/api/projects/prj_1/rollback":
		reply(200, map[string]any{"id": "dep_2", "project_id": "prj_1", "phase": "queued"})
	case r.URL.Path == "/api/deployments/dep_1/logs":
		_, _ = io.WriteString(w, `{"items":[{"at":"2026-10-07T10:00:00Z","stream":"stderr","text":"boom tk_test"},{"time":"2026-10-07T10:00:01Z","line":"ok"}]}`)
	case r.URL.Path == "/api/releases/rel_1/logs":
		_, _ = io.WriteString(w, `{"logs":"a\nb\n"}`)
	default:
		reply(404, map[string]any{"error": "NOT_FOUND", "http": 404})
	}
}

func newFake(t *testing.T) (*fakeRunyer, *Runyer) {
	f := &fakeRunyer{t: t, phase: "deploying", bodies: map[string]map[string]any{}, keys: map[string]string{}}
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	client, err := NewRunyer(Config{BaseURL: server.URL, Token: "tk_test"})
	if err != nil {
		t.Fatal(err)
	}
	return f, client
}

func TestRunyerConfig(t *testing.T) {
	for _, bad := range []string{"http://runyer.example.com", "https://user@x.example.com", "https://x.example.com/other", "ftp://x"} {
		if _, err := NewRunyer(Config{BaseURL: bad, Token: "tk"}); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
	for _, good := range []string{"https://runyer.example.com", "https://runyer.example.com/api", "http://127.0.0.1:9000"} {
		if _, err := NewRunyer(Config{BaseURL: good, Token: "tk"}); err != nil {
			t.Errorf("%s rejected", good)
		}
	}
}

func TestRunyerDeploymentLifecycle(t *testing.T) {
	f, c := newFake(t)
	ctx := context.Background()
	if _, err := c.Capabilities(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UpsertProject(ctx, ProjectRequest{ProjectID: "prj_1", Slug: "one", Manifest: runyerManifest(), Aliases: []string{"www.client.cz"}}); err != nil {
		t.Fatal(err)
	}
	if aliases := f.bodies["POST /api/projects"]["aliases"].([]any); len(aliases) != 1 || aliases[0] != "www.client.cz" {
		t.Fatalf("aliases not sent: %v", f.bodies["POST /api/projects"])
	}
	if _, sent := f.bodies["POST /api/projects"]["hostname"]; sent {
		t.Fatalf("no host name was asked for, runyer must keep its own: %v", f.bodies["POST /api/projects"])
	}
	if _, err := c.UpsertProject(ctx, ProjectRequest{ProjectID: "prj_1", Slug: "one", Manifest: runyerManifest(), Hostname: "client.cz", Aliases: []string{"www.client.cz"}}); err != nil {
		t.Fatal(err)
	}
	if host := f.bodies["POST /api/projects"]["hostname"]; host != "client.cz" {
		t.Fatalf("custom host name not sent: %v", f.bodies["POST /api/projects"])
	}
	if _, err := c.UpsertProject(ctx, ProjectRequest{ProjectID: "prj_1", Slug: "one", Manifest: runyerManifest(), Hostname: "Not A Host"}); err == nil {
		t.Fatal("invalid host name accepted")
	}
	if _, err := c.UpsertProject(ctx, ProjectRequest{ProjectID: "prj_1", Slug: "one", Manifest: runyerManifest(), Aliases: []string{"Not A Host"}}); err == nil {
		t.Fatal("invalid alias accepted")
	}
	if err := c.ReplaceEnvironment(ctx, "prj_1", map[string]string{"A": "1"}); err != nil {
		t.Fatal(err)
	}
	dep, err := c.Deploy(ctx, DeploymentRequest{ProjectID: "prj_1", ExternalDeploymentID: "job_1", Artifact: Artifact{Image: "registry.example.com/molo/prj_1", Digest: testDigest}}, "job_1")
	if err != nil || dep.ID != "dep_1" || f.keys["/api/projects/prj_1/deploy"] != "job_1" || f.bodies["POST /api/projects/prj_1/deploy"]["external_id"] != "job_1" {
		t.Fatalf("deploy: %v %+v %v", err, dep, f.keys)
	}

	if v, err := c.VerifyActive(ctx, "prj_1", "dep_1"); err != nil || v.Active {
		t.Fatalf("deploying must not verify: %v %+v", err, v)
	}
	f.phase, f.active = "active", "rel_0"
	if v, err := c.VerifyActive(ctx, "prj_1", "dep_1"); err != nil || v.Active {
		t.Fatalf("active phase with another active release must not verify: %v %+v", err, v)
	}
	f.active = "rel_1"
	f.route = map[string]any{"route_id": "rte_1", "release_id": "rel_1", "status": "active", "hostname": "site.apps.example.com", "aliases": []string{"www.client.cz"}, "https": true, "cert_status": "valid"}
	v, err := c.VerifyActive(ctx, "prj_1", "dep_1")
	if err != nil || !v.Active || v.ReleaseID != "rel_1" || v.Route.Hostname != "site.apps.example.com" || !v.Route.HTTPS || len(v.Route.Aliases) != 1 {
		t.Fatalf("active deployment not verified: %v %+v", err, v)
	}
	f.route["status"] = "maintenance"
	if v, _ := c.VerifyActive(ctx, "prj_1", "dep_1"); v.Active {
		t.Fatal("route in maintenance must not verify")
	}
	f.phase = "failed"
	var remote *Error
	if _, err := c.VerifyActive(ctx, "prj_1", "dep_1"); !errors.As(err, &remote) || remote.Code != "HEALTH_CHECK_FAILED" {
		t.Fatalf("failure code lost: %v", err)
	}

	if err := c.DeleteProject(ctx, "prj_1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := c.DeleteProject(ctx, "prj_gone"); err != nil {
		t.Fatalf("deleting a missing project must succeed: %v", err)
	}
	if err := c.DeleteProject(ctx, "prj_kept"); !errors.Is(err, ErrOffboardPending) || f.bodies["POST /api/projects/prj_kept/suspend"]["reason"] == "" {
		t.Fatalf("an operator token must suspend the project instead: %v", err)
	}
	if d, err := c.Rollback(ctx, "prj_1", "rel_1", "rb_1"); err != nil || d.ID != "dep_2" || f.bodies["POST /api/projects/prj_1/rollback"]["release_id"] != "rel_1" {
		t.Fatalf("rollback: %v %+v", err, d)
	}
}

func TestRunyerLogsAndErrors(t *testing.T) {
	_, c := newFake(t)
	ctx := context.Background()
	entries, err := c.LogEntries(ctx, "dep_1", 100)
	if err != nil || len(entries) != 2 || entries[0].Stream != "stderr" || strings.Contains(entries[0].Text, "tk_test") || entries[1].Text != "ok" || entries[1].At.IsZero() {
		t.Fatalf("logs: %v %+v", err, entries)
	}
	if text, err := c.ReleaseLogs(ctx, "rel_1", 10); err != nil || text != "a\nb" {
		t.Fatalf("release logs: %v %q", err, text)
	}
	_, err = c.Deployment(ctx, "dep_missing")
	var remote *Error
	if !errors.As(err, &remote) || remote.Code != "NOT_FOUND" || remote.HTTPStatus != 404 || remote.Retryable {
		t.Fatalf("error shape: %v", err)
	}
	bad, _ := NewRunyer(Config{BaseURL: c.base[:len(c.base)-4], Token: "tk_wrong"})
	if _, err := bad.Capabilities(ctx); !errors.As(err, &remote) || remote.Code != "UNAUTHENTICATED" {
		t.Fatalf("auth error: %v", err)
	}
}

func TestRunyerPruneReleases(t *testing.T) {
	var deleted []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tk_test" {
			w.WriteHeader(401)
			return
		}
		switch {
		case r.Method == "GET" && r.URL.Path == "/api/projects/prj_demo/releases":
			_, _ = io.WriteString(w, `[{"id":"rel_live","status":"active"},{"id":"rel_old","status":"superseded"},{"id":"rel_busy","status":"superseded"},{"id":"rel_gone","status":"retired"},{"id":"rel_failed","status":"failed"}]`)
		case r.Method == "DELETE" && r.URL.Path == "/api/releases/rel_busy":
			w.WriteHeader(409)
			_, _ = io.WriteString(w, `{"error":"RELEASE_BUSY","http":409}`)
		case r.Method == "DELETE" && strings.HasPrefix(r.URL.Path, "/api/releases/"):
			deleted = append(deleted, strings.TrimPrefix(r.URL.Path, "/api/releases/"))
			_, _ = io.WriteString(w, `{"ok":true}`)
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	client, err := NewRunyer(Config{BaseURL: server.URL, Token: "tk_test"})
	if err != nil {
		t.Fatal(err)
	}
	n, err := client.PruneReleases(context.Background(), "prj_demo")
	if err != nil || n != 1 || len(deleted) != 1 || deleted[0] != "rel_old" {
		t.Fatalf("only the superseded release is deleted, a busy one is skipped: n=%d err=%v deleted=%v", n, err, deleted)
	}
	if _, err := client.PruneReleases(context.Background(), "../x"); err == nil {
		t.Fatal("an invalid project id is refused")
	}
}
