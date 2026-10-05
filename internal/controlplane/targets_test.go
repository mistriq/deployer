package controlplane

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/mistriq/deployer/internal/ocibuild"
)

func putTarget(t *testing.T, s *Service, requestID, target string, status int) {
	t.Helper()
	body := map[string]any{"request_id": requestID, "repository": "https://github.com/example/app.git", "ref": "main", "build": ocibuild.BuildSpec{Kind: "node-http", StartCommand: "node server.js", Port: 3000}, "manifest": testManifest()}
	if target != "" {
		body["runtime_target_id"] = target
	}
	requireStatus(t, request(t, s, "PUT", "/internal/v1/projects/demo", "token-a", body), status)
}

func TestTargetDefaultPinnedAndMigrationRejected(t *testing.T) {
	s := testService(t)
	s.Runtimes = map[string]Runtime{"node-a": &fakeRuntime{}, "node-b": &fakeRuntime{}}
	s.Config.DefaultRuntimeTarget = "node-a"
	setupProject(t, s)
	s.Config.DefaultRuntimeTarget = "node-b"
	putTarget(t, s, "update", "", 200)
	if got := s.Store.snapshot().Projects["demo"].Spec.RuntimeTargetID; got != "node-a" {
		t.Fatalf("default moved existing project to %s", got)
	}
	putTarget(t, s, "unknown", "missing", 400)
	putTarget(t, s, "move-before-job", "node-b", 200)
	job := enqueue(t, s, "job")
	if got := s.Store.snapshot().Jobs[job].Snapshot.Spec.RuntimeTargetID; got != "node-b" {
		t.Fatal(got)
	}
	if err := s.fail(job, "build_failed", "SOURCE_FAILED"); err != nil {
		t.Fatal(err)
	}
	putTarget(t, s, "move-after-failed-job", "node-a", 409)
	putTarget(t, s, "same-target", "", 200)
	w := request(t, s, "GET", "/internal/v1/deployments/"+job, "token-a", nil)
	if !strings.Contains(w.Body.String(), `"runtime_target_id":"node-b"`) {
		t.Fatal(w.Body.String())
	}
}

func TestWorkerTargetSnapshotAndRemovedTargetFailClosed(t *testing.T) {
	s := testService(t)
	a, b := &fakeRuntime{active: true}, &fakeRuntime{active: true}
	s.Runtimes = map[string]Runtime{"node-a": a, "node-b": b}
	s.Config.DefaultRuntimeTarget = "node-a"
	s.Builder = &fakeBuilder{}
	setupProject(t, s)
	job := enqueue(t, s, "job")
	s.Config.DefaultRuntimeTarget = "node-b"
	// Restart from encrypted state with a different configured default.
	path := s.Store.path
	if err := s.Store.Close(); err != nil {
		t.Fatal(err)
	}
	restored, err := OpenStore(path, bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	s.Store = restored
	t.Cleanup(func() { restored.Close() })
	delete(s.Runtimes, "node-a")
	step(t, s, 1)
	if got := s.Store.snapshot().Jobs[job]; got.ErrorCode != "RUNTIME_TARGET_UNAVAILABLE" || got.Commit != "" {
		t.Fatalf("removed target not blocked: %+v", got)
	}
	s.Runtimes["node-a"] = a
	step(t, s, 5)
	j := s.Store.snapshot().Jobs[job]
	if j.State != "online" || len(a.requests) != 1 || len(b.requests) != 0 || a.env == nil {
		t.Fatalf("wrong routing: state=%s a=%d b=%d", j.State, len(a.requests), len(b.requests))
	}
	delete(s.Runtimes, "node-a")
	if err := s.collectLogs(context.Background(), j); err == nil {
		t.Fatal("removed target logs did not fail closed")
	}
	s.Runtimes["node-a"] = a
	requireStatus(t, request(t, s, "POST", "/internal/v1/projects/demo/rollback", "token-a", map[string]string{"request_id": "rollback", "release_id": j.ReleaseID}), 202)
	step(t, s, 3)
	if len(a.requests) != 2 || len(b.requests) != 0 {
		t.Fatal("rollback switched target")
	}
}

func TestHistoricalTargetDoesNotFollowNewDefault(t *testing.T) {
	s, b, legacy, job := workerFixture(t)
	_ = b
	if err := s.Store.update(func(d *database) error {
		d.Projects["demo"].Spec.RuntimeTargetID = ""
		d.Jobs[job].Snapshot.Spec.RuntimeTargetID = ""
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	other := &fakeRuntime{active: true}
	s.Runtimes = map[string]Runtime{"node-new": other}
	s.Config.DefaultRuntimeTarget = "node-new"
	putTarget(t, s, "legacy-update", "", 200)
	step(t, s, 5)
	if len(legacy.requests) != 1 || len(other.requests) != 0 {
		t.Fatal("historical deployment redirected")
	}
	if s.Store.snapshot().Projects["demo"].Spec.RuntimeTargetID != "default" {
		t.Fatal("legacy update not pinned")
	}
}
