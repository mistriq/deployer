package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mistriq/deployer/internal/runtimeengine"
)

type structuredLogRuntime struct {
	fakeRuntime
	entries      []runtimeengine.LogEntry
	releaseCalls int
}

func (r *structuredLogRuntime) LogEntries(context.Context, string, int) ([]runtimeengine.LogEntry, error) {
	return append([]runtimeengine.LogEntry(nil), r.entries...), nil
}
func (r *structuredLogRuntime) ReleaseLogEntries(context.Context, string, int) ([]runtimeengine.LogEntry, error) {
	r.releaseCalls++
	return append([]runtimeengine.LogEntry(nil), r.entries...), nil
}
func TestStructuredLogIdentityTimestampStreamAndRestartReplay(t *testing.T) {
	s := testService(t)
	setupProject(t, s)
	id := enqueue(t, s, "structured-log-job")
	at := time.Date(2026, 9, 20, 10, 11, 12, 123456789, time.UTC)
	r := &structuredLogRuntime{entries: []runtimeengine.LogEntry{{At: at, Stream: "stderr", Text: "repeat environment-secret-123"}, {At: at.Add(time.Nanosecond), Stream: "stderr", Text: "repeat environment-secret-123"}}}
	s.Runtime = r
	if e := s.change(id, func(j *Job) { j.RuntimeID = "runtime-structured" }); e != nil {
		t.Fatal(e)
	}
	collect := func() {
		t.Helper()
		if e := s.collectLogs(context.Background(), s.Store.snapshot().Jobs[id]); e != nil {
			t.Fatal(e)
		}
	}
	collect()
	d := s.Store.snapshot()
	lines := d.Logs[id+"|runtime"]
	if len(lines) != 2 || lines[0].Cursor != 1 || lines[1].Cursor != 2 {
		t.Fatal("identical text at different timestamps was lost")
	}
	if !lines[0].Time.Equal(at) || !lines[1].Time.Equal(at.Add(time.Nanosecond)) || lines[0].Stream != "stderr" {
		t.Fatal("source timestamp or stream lost")
	}
	serialized, _ := json.Marshal(d)
	if strings.Contains(string(serialized), "repeat environment-secret-123") {
		t.Fatal("raw log secret persisted")
	}
	for _, key := range d.Jobs[id].RuntimeTail {
		if strings.Contains(key, "environment-secret-123") {
			t.Fatal("secret persisted in overlap identity")
		}
	}
	path := s.Store.path
	s.Store.Close()
	store, e := OpenStore(path, bytes.Repeat([]byte{7}, 32))
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	s.Store = store
	collect()
	if len(s.Store.snapshot().Logs[id+"|runtime"]) != 2 {
		t.Fatal("restart duplicated stable log snapshot")
	}
	r.entries = []runtimeengine.LogEntry{r.entries[1], {At: at.Add(2 * time.Nanosecond), Stream: "stdout", Text: "repeat environment-secret-123"}}
	collect()
	lines = s.Store.snapshot().Logs[id+"|runtime"]
	if len(lines) != 3 || lines[2].Cursor != 3 || lines[2].Stream != "stdout" {
		t.Fatal("overlapping snapshot lost repeated entry or cursor")
	}
	if e := s.change(id, func(j *Job) { j.ReleaseID = "release-structured" }); e != nil {
		t.Fatal(e)
	}
	w := request(t, s, "GET", "/internal/v1/deployments/"+id+"/logs?source=runtime&cursor=2", "token-a", nil)
	requireStatus(t, w, 200)
	if r.releaseCalls != 1 || strings.Contains(w.Body.String(), "environment-secret-123") {
		t.Fatal("public structured logs leaked secret or ignored release endpoint")
	}
	var page struct {
		Items []LogLine `json:"items"`
		Next  string    `json:"next_cursor"`
	}
	if e := json.Unmarshal(w.Body.Bytes(), &page); e != nil {
		t.Fatal(e)
	}
	if len(page.Items) != 1 || page.Next != "3" || !page.Items[0].Time.Equal(at.Add(2*time.Nanosecond)) {
		t.Fatal("public cursor replay or timestamp changed")
	}
}
func TestStructuredLogStableIDAndHistoricalSecretRedaction(t *testing.T) {
	s := testService(t)
	setupProject(t, s)
	id := enqueue(t, s, "id-logs")
	requireStatus(t, request(t, s, "PUT", "/internal/v1/projects/demo/env", "token-a", map[string]any{"request_id": "rotate-structured", "env": map[string]string{"TOKEN": "rotated-secret"}}), 200)
	r := &structuredLogRuntime{entries: []runtimeengine.LogEntry{{ID: "id-environment-secret-123", At: time.Now(), Stream: "stderr", Text: "environment-secret-123"}}}
	s.Runtime = r
	job := s.Store.snapshot().Jobs[id]
	if e := s.collectLogs(context.Background(), job); e != nil {
		t.Fatal(e)
	}
	r.entries[0].At = r.entries[0].At.Add(time.Second)
	r.entries[0].Text = "same source entry updated"
	if e := s.collectLogs(context.Background(), job); e != nil {
		t.Fatal(e)
	}
	d := s.Store.snapshot()
	if len(d.Logs[id+"|runtime"]) != 1 {
		t.Fatal("stable upstream identity duplicated")
	}
	if strings.Contains(d.Logs[id+"|runtime"][0].Text, "environment-secret-123") || strings.Contains(strings.Join(d.Jobs[id].RuntimeTail, ""), "environment-secret-123") {
		t.Fatal("historical secret exposed in log or ID")
	}
}
