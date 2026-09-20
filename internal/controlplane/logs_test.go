package controlplane

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

func TestLogRedactionIncludesHistoricalAndEncodedSecrets(t *testing.T) {
	s := testService(t)
	setupProject(t, s)
	enqueue(t, s, "historical-env")
	requireStatus(t, request(t, s, "PUT", "/internal/v1/projects/demo/env", "token-a", map[string]any{"request_id": "env-rotated", "env": map[string]string{"TOKEN": "replacement-secret"}}), 200)
	s.Config.ServiceSecrets = []string{"service secret/+", "first-private-line\nsecond-private-line"}
	s.Config.Credentials.Password = "registry-password"
	values := []string{"environment-secret-123", "replacement-secret", "service secret/+", "first-private-line\nsecond-private-line", "registry-password", "token-a"}
	for _, line := range []string{"first-private-line", "second-private-line"} {
		if strings.Contains(s.redact(line), line) {
			t.Fatal("multiline secret fragment retained")
		}
	}
	for _, v := range values {
		for _, encoded := range []string{v, url.QueryEscape(v), base64.StdEncoding.EncodeToString([]byte(v))} {
			redacted := s.redact("before " + encoded + " after")
			if strings.Contains(redacted, encoded) {
				t.Errorf("log retained secret representation %q", encoded)
			}
		}
	}
}
func TestLogCursorPaginationAndRetentionGap(t *testing.T) {
	s := testService(t)
	setupProject(t, s)
	id := enqueue(t, s, "log-job")
	if e := s.Store.update(func(d *database) error {
		for i := 0; i < 2005; i++ {
			appendLine(d, id, "build", "stdout", fmt.Sprintf("line %d", i))
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	type page struct {
		Items     []LogLine `json:"items"`
		Next      string    `json:"next_cursor"`
		Truncated bool      `json:"truncated"`
	}
	get := func(cursor string) page {
		t.Helper()
		w := request(t, s, "GET", "/internal/v1/deployments/"+id+"/logs?source=build&cursor="+cursor, "token-a", nil)
		requireStatus(t, w, 200)
		var p page
		if e := json.Unmarshal(w.Body.Bytes(), &p); e != nil {
			t.Fatal(e)
		}
		return p
	}
	first := get("0")
	if !first.Truncated || len(first.Items) != 200 || first.Items[0].Cursor != 6 || first.Next != "205" {
		t.Fatalf("invalid first page %+v", first)
	}
	second := get(first.Next)
	if second.Truncated || second.Items[0].Cursor != 206 {
		t.Fatal("cursor replay duplicated or skipped persisted logs")
	}
	empty := get("2005")
	if len(empty.Items) != 0 || empty.Next != "2005" {
		t.Fatal("empty page changed cursor")
	}
	requireStatus(t, request(t, s, "GET", "/internal/v1/deployments/"+id+"/logs?source=build&cursor=-1", "token-a", nil), 400)
}
