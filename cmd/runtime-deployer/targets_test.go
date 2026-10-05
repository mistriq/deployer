package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestRuntimeCatalogueClients(t *testing.T) {
	for _, sandbox := range []bool{true, false} {
		t.Run(map[bool]string{true: "sandbox", false: "production"}[sandbox], func(t *testing.T) {
			dir := t.TempDir()
			seen := map[string]int{}
			var seenMu sync.Mutex
			makeNode := func(id, token string) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("Authorization") != "Bearer "+token {
						t.Error("wrong target credential")
					}
					want := ""
					if sandbox {
						want = "true"
					}
					if r.Header.Get("X-Socen-Sandbox") != want {
						t.Error("wrong environment header")
					}
					if r.URL.Path != "/api/internal/v1/meta/capabilities" {
						t.Error("unexpected target path")
					}
					seenMu.Lock()
					seen[id]++
					seenMu.Unlock()
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"node_id":"` + id + `","manifest_version":1}`))
				}))
			}
			a := makeNode("default", "node-a-secret")
			defer a.Close()
			b := makeNode("node-b", "node-b-secret")
			defer b.Close()
			ap := filepath.Join(dir, "a")
			bp := filepath.Join(dir, "b")
			if err := os.WriteFile(ap, []byte("node-a-secret"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(bp, []byte("node-b-secret"), 0600); err != nil {
				t.Fatal(err)
			}
			cp := filepath.Join(dir, "targets.json")
			data, _ := json.Marshal(targetCatalogue{DefaultTargetID: "node-b", Targets: map[string]targetProfile{"default": {a.URL, ap}, "node-b": {b.URL, bp}}})
			if err := os.WriteFile(cp, data, 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("RUNTIME_TARGETS_FILE", cp)
			t.Setenv("RUNTIME_TOKEN_FILE", "/missing/legacy-token")
			clients, def, secrets, err := loadRuntimeTargets(sandbox, "registry.example/apps")
			if err != nil {
				t.Fatal(err)
			}
			if def != "node-b" || len(secrets) != 2 {
				t.Fatal("default or redaction secrets lost")
			}
			for _, id := range []string{"default", "node-b"} {
				_, _ = clients[id].Capabilities(context.Background())
			}
			seenMu.Lock()
			defer seenMu.Unlock()
			if seen["default"] != 1 || seen["node-b"] != 1 {
				t.Fatal("target routing failed")
			}
		})
	}
}

func TestRuntimeCatalogueRejectsInvalidConfiguration(t *testing.T) {
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte("do-not-print-this-token"), 0600); err != nil {
		t.Fatal(err)
	}
	good := `{"default_target_id":"default","targets":{"default":{"base_url":"https://runtime.example","token_file":` + string(mustJSON(token)) + `}}}`
	for _, tc := range []struct {
		name, body string
		mode       os.FileMode
	}{
		{"missing default", strings.Replace(good, `"default_target_id":"default"`, `"default_target_id":"missing"`, 1), 0600},
		{"unknown field", strings.Replace(good, `"base_url"`, `"unexpected"`, 1), 0600},
		{"trailing data", good + ` {}`, 0600},
		{"public file", good, 0644},
		{"invalid ID", strings.ReplaceAll(good, `"default"`, `"bad/id"`), 0600},
		{"relative token", strings.Replace(good, string(mustJSON(token)), `"relative-token"`, 1), 0600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "targets.json")
			if err := os.WriteFile(p, []byte(tc.body), tc.mode); err != nil {
				t.Fatal(err)
			}
			t.Setenv("RUNTIME_TARGETS_FILE", p)
			_, _, _, err := loadRuntimeTargets(false, "registry.example/apps")
			if err == nil {
				t.Fatal("invalid configuration accepted")
			}
			if strings.Contains(err.Error(), "do-not-print-this-token") {
				t.Fatal("credential disclosed")
			}
		})
	}
}
func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
