package controlplane

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mistriq/deployer/internal/ocibuild"
	"github.com/mistriq/deployer/internal/runtimeengine"
)

func testManifest() runtimeengine.Manifest {
	return runtimeengine.Manifest{Version: 1, Kind: "node-http", PolicyVersion: "v1", Runtime: runtimeengine.Runtime{Port: 3000, User: "1000:1000", ReadOnlyRoot: true, EnvNames: []string{"TOKEN"}}, Health: runtimeengine.Health{Path: "/health", ExpectStatusMin: 200, ExpectStatusMax: 299, TimeoutMS: 1000, IntervalMS: 1000, Retries: 3}, Resources: runtimeengine.Resources{CPUMillis: 100, MemoryMB: 128, PidsLimit: 32, DiskMB: 256}, Network: runtimeengine.Network{MaxBodyBytes: 1024, RateLimitRPS: 100}}
}
func testService(t *testing.T) *Service {
	t.Helper()
	s, e := OpenStore(filepath.Join(t.TempDir(), "state"), bytes.Repeat([]byte{7}, 32))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return &Service{Store: s, Config: Config{Tokens: map[string]string{"token-a": "tenant-a", "token-b": "tenant-b"}, ImagePrefix: "registry.example/apps"}}
}
func request(t *testing.T, s *Service, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var b []byte
	if body != nil {
		var e error
		b, e = json.Marshal(body)
		if e != nil {
			t.Fatal(e)
		}
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(b))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}
func requireStatus(t *testing.T, w *httptest.ResponseRecorder, status int) {
	t.Helper()
	if w.Code != status {
		t.Fatalf("status=%d want=%d: %s", w.Code, status, w.Body.String())
	}
}
func setupProject(t *testing.T, s *Service) {
	t.Helper()
	requireStatus(t, request(t, s, "PUT", "/internal/v1/projects/demo", "token-a", map[string]any{"request_id": "project-1", "repository": "https://github.com/example/app.git", "ref": "main", "build": ocibuild.BuildSpec{Kind: "node-http", StartCommand: "node server.js", Port: 3000}, "manifest": testManifest()}), 200)
	requireStatus(t, request(t, s, "PUT", "/internal/v1/projects/demo/env", "token-a", map[string]any{"request_id": "env-1", "env": map[string]string{"TOKEN": "environment-secret-123"}}), 200)
}
func enqueue(t *testing.T, s *Service, req string) string {
	t.Helper()
	w := request(t, s, "POST", "/internal/v1/projects/demo/deployments", "token-a", map[string]string{"request_id": req})
	requireStatus(t, w, 202)
	var v map[string]string
	if e := json.Unmarshal(w.Body.Bytes(), &v); e != nil {
		t.Fatal(e)
	}
	return v["deployment_id"]
}

func TestAPIAuthenticationTenantIsolationAndIdempotency(t *testing.T) {
	s := testService(t)
	setupProject(t, s)
	requireStatus(t, request(t, s, "POST", "/internal/v1/projects/demo/deployments", "", map[string]string{"request_id": "x"}), 401)
	requireStatus(t, request(t, s, "PUT", "/internal/v1/projects/demo/env", "token-b", map[string]any{"request_id": "other", "env": map[string]string{"TOKEN": "changed"}}), 404)
	job := enqueue(t, s, "deploy-1")
	if again := enqueue(t, s, "deploy-1"); again != job {
		t.Fatal("retry created duplicate deployment")
	}
	if len(s.Store.snapshot().Jobs) != 1 {
		t.Fatal("duplicate job persisted")
	}
	requireStatus(t, request(t, s, "POST", "/internal/v1/projects/demo/deployments", "token-a", map[string]string{"request_id": "deploy-1", "release_id": "different"}), 409)
	for _, suffix := range []string{"", "/logs?source=build", "/logs?source=runtime"} {
		requireStatus(t, request(t, s, "GET", "/internal/v1/deployments/"+job+suffix, "token-b", nil), 404)
	}
	w := request(t, s, "GET", "/internal/v1/deployments/"+job, "token-a", nil)
	requireStatus(t, w, 200)
	for _, secret := range []string{"environment-secret-123", "snapshot", "registry.example"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatalf("public deployment exposed %q", secret)
		}
	}
}
func TestStoreEncryptedRestartPreservesReceiptAndSnapshot(t *testing.T) {
	s := testService(t)
	setupProject(t, s)
	job := enqueue(t, s, "deploy-persistent")
	path := s.Store.path
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	for _, v := range []string{"environment-secret-123", "github.com", "tenant-a"} {
		if bytes.Contains(b, []byte(v)) {
			t.Fatalf("plaintext %q persisted", v)
		}
	}
	if _, e := OpenStore(path, bytes.Repeat([]byte{7}, 32)); e == nil {
		t.Fatal("second writer acquired store")
	}
	s.Store.Close()
	if _, e := OpenStore(path, bytes.Repeat([]byte{8}, 32)); e == nil {
		t.Fatal("incorrect encryption key accepted")
	}
	reopened, e := OpenStore(path, bytes.Repeat([]byte{7}, 32))
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	s.Store = reopened
	if got := enqueue(t, s, "deploy-persistent"); got != job {
		t.Fatal("idempotency receipt lost after restart")
	}
	if s.Store.snapshot().Jobs[job].Snapshot.Env["TOKEN"] != "environment-secret-123" {
		t.Fatal("encrypted environment snapshot lost")
	}
	requireStatus(t, request(t, s, "PUT", "/internal/v1/projects/demo/env", "token-a", map[string]any{"request_id": "env-2", "env": map[string]string{"TOKEN": "new-value"}}), 200)
	if s.Store.snapshot().Jobs[job].Snapshot.Env["TOKEN"] != "environment-secret-123" {
		t.Fatal("queued snapshot mutated by later environment update")
	}
}
func TestAPIStrictPayloadAndRollbackTarget(t *testing.T) {
	s := testService(t)
	setupProject(t, s)
	requireStatus(t, request(t, s, "POST", "/internal/v1/projects/demo/rollback", "token-a", map[string]string{"request_id": "rollback-1"}), 400)
	requireStatus(t, request(t, s, "POST", "/internal/v1/projects/demo/rollback", "token-a", map[string]string{"request_id": "rollback-2", "release_id": "missing"}), 404)
	requireStatus(t, request(t, s, "POST", "/internal/v1/projects/demo/deployments", "token-a", map[string]string{"request_id": "deploy-invalid", "unexpected": "x"}), 400)
	if len(s.Store.snapshot().Jobs) != 0 {
		t.Fatal("invalid request created job")
	}
}
