package controlplane

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestEnvironmentRejectsRuntimeIncompatibleNamesAndValues(t *testing.T) {
	s := testService(t)
	setupProject(t, s)
	before := s.Store.snapshot()
	for _, tc := range []struct {
		name string
		env  map[string]string
	}{{"nul", map[string]string{"TOKEN": "before\x00after"}}, {"long-name", map[string]string{strings.Repeat("A", 129): "value"}}} {
		t.Run(tc.name, func(t *testing.T) {
			requireStatus(t, request(t, s, "PUT", "/internal/v1/projects/demo/env", "token-a", map[string]any{"request_id": tc.name, "env": tc.env}), 400)
			after := s.Store.snapshot()
			if !reflect.DeepEqual(before, after) {
				t.Fatal("rejected environment request changed durable state")
			}
		})
	}
}
func TestPostRenameSyncFailureRetainsCommittedReceiptAndPoisonsWriter(t *testing.T) {
	s := testService(t)
	setupProject(t, s)
	s.Store.syncDirectory = func(string) error { return errors.New("injected directory sync failure") }
	payload := map[string]string{"request_id": "uncertain-commit"}
	requireStatus(t, request(t, s, "POST", "/internal/v1/projects/demo/deployments", "token-a", payload), 503)
	committed := s.Store.snapshot()
	if len(committed.Jobs) != 1 {
		t.Fatal("post-rename error rolled back in-memory job")
	}
	var id string
	for id = range committed.Jobs {
	}
	if !s.Store.poisoned {
		t.Fatal("uncertain durability did not poison writer")
	}
	requireStatus(t, request(t, s, "POST", "/internal/v1/projects/demo/deployments", "token-a", map[string]string{"request_id": "must-not-write"}), 503)
	if !reflect.DeepEqual(committed, s.Store.snapshot()) {
		t.Fatal("poisoned writer mutated memory")
	}
	path := s.Store.path
	s.Store.Close()
	reopened, e := OpenStore(path, bytes.Repeat([]byte{7}, 32))
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	if !reflect.DeepEqual(committed, reopened.snapshot()) {
		t.Fatal("reopened state differs from committed in-memory state")
	}
	s.Store = reopened
	if got := enqueue(t, s, "uncertain-commit"); got != id {
		t.Fatal("retry after restart lost committed receipt")
	}
	if len(s.Store.snapshot().Jobs) != 1 {
		t.Fatal("uncertain commit produced duplicate job")
	}
}
func TestQueueFairnessPreservesProjectFIFOAndUnconfirmedActivation(t *testing.T) {
	s, _, r, first := workerFixture(t)
	r.active = false
	step(t, s, 4)
	second := enqueue(t, s, "same-project-later")
	var otherID = "other-project-job"
	if e := s.Store.update(func(d *database) error {
		j := d.Jobs[first]
		j.VerificationStarted = time.Now().Add(-16 * time.Minute)
		j.CreatedAt = time.Now().Add(-3 * time.Hour)
		j.UpdatedAt = time.Now().Add(-3 * time.Hour)
		d.Jobs[second].CreatedAt = time.Now().Add(-2 * time.Hour)
		d.Jobs[second].UpdatedAt = time.Now().Add(-4 * time.Hour)
		other := *d.Jobs[second]
		other.ID = otherID
		other.ProjectID = "other-project"
		other.Snapshot.ID = "other-project"
		other.CreatedAt = time.Now().Add(-time.Hour)
		other.UpdatedAt = time.Now().Add(-2 * time.Hour)
		d.Jobs[otherID] = &other
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	step(t, s, 1)
	d := s.Store.snapshot()
	j := d.Jobs[first]
	if terminal(j.State) || j.ErrorCode != "ACTIVATION_UNCONFIRMED" || j.RuntimeID != "runtime-deploy" {
		t.Fatalf("unconfirmed activation lost pending identity: %+v", j)
	}
	step(t, s, 1)
	d = s.Store.snapshot()
	if d.Jobs[otherID].Commit == "" {
		t.Fatal("pending oldest project starved another project")
	}
	if d.Jobs[second].Commit != "" || d.Jobs[second].State != "queued" {
		t.Fatal("later same-project job bypassed unconfirmed activation")
	}
	step(t, s, 10)
	d = s.Store.snapshot()
	if d.Jobs[second].Commit != "" {
		t.Fatal("continued reconciliation freed same-project queue")
	}
	if len(r.requests) != 2 {
		t.Fatalf("wanted only initial + independent project submissions, got %d", len(r.requests))
	}
	r.active = true
	step(t, s, 4)
	d = s.Store.snapshot()
	if d.Jobs[first].State != "online" || d.Jobs[first].ErrorCode != "" {
		t.Fatal("unconfirmed candidate did not recover when verification succeeded")
	}
	if d.Jobs[second].Commit == "" {
		t.Fatal("same-project queue did not resume after confirmed activation")
	}
}
