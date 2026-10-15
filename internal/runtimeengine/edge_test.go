package runtimeengine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRunyerEdge(t *testing.T) {
	body := `{}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/projects/prj_1/edge" || r.Header.Get("Authorization") != "Bearer tk_test" {
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"error":"NOT_FOUND","http":404}`))
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	c, err := NewRunyer(Config{BaseURL: server.URL, Token: "tk_test"})
	if err != nil {
		t.Fatal(err)
	}
	read := func(v any) Edge {
		b, _ := json.Marshal(v)
		body = string(b)
		e, err := c.Edge(context.Background(), "prj_1")
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	route := map[string]any{"hostname": "site.yo.example.com", "aliases": []string{"www.client.cz"}, "status": "active", "https": false, "cert_status": "pending"}

	if e := read(map[string]any{"route": nil}); e.RouteStatus != "none" || len(e.Hosts) != 0 {
		t.Fatalf("no route: %+v", e)
	}
	e := read(map[string]any{"route": route, "site": map[string]any{"domains": []string{"site.yo.example.com", "www.client.cz"}, "https": false, "tls": map[string]any{"mode": "auto"}, "cert": map[string]any{"status": "pending", "running": true}}})
	if len(e.Hosts) != 2 || !e.Hosts[0].Primary || e.Hosts[0].HTTPS || e.Hosts[0].CertStatus != "issuing" {
		t.Fatalf("issuing: %+v", e)
	}
	e = read(map[string]any{"route": route, "site": map[string]any{"domains": []string{"site.yo.example.com", "www.client.cz"}, "https": true, "cert": map[string]any{"status": "valid", "domains": []string{"site.yo.example.com"}, "not_after": 1800000000}}})
	if !e.Hosts[0].HTTPS || e.Hosts[0].CertNotAfter != 1800000000 || e.Hosts[1].HTTPS || e.Hosts[1].CertStatus != "pending" {
		t.Fatalf("alias not covered by the certificate must stay pending: %+v", e)
	}
	e = read(map[string]any{"route": route, "site": map[string]any{"https": false, "cert": map[string]any{"status": "failed", "last_error": "acme: NXDOMAIN", "next_attempt": 1700000000}}})
	if e.Hosts[0].CertStatus != "failed" || e.Hosts[0].CertError != "acme: NXDOMAIN" || e.Hosts[0].CertNextAttempt != 1700000000 {
		t.Fatalf("failed: %+v", e)
	}
	e = read(map[string]any{"route": map[string]any{"hostname": "site.yo.example.com", "status": "suspended", "https": true, "cert_status": "valid"}})
	if e.RouteStatus != "suspended" || !e.Hosts[0].HTTPS {
		t.Fatalf("route-only fallback: %+v", e)
	}
	e = read(map[string]any{"route": map[string]any{"hostname": "site.yo.example.com", "https": true, "cert_status": "weird"}})
	if e.Hosts[0].CertStatus != "unknown" || e.Hosts[0].HTTPS {
		t.Fatalf("unknown status must not count as secure: %+v", e)
	}
	if certCovers([]string{"*.client.cz"}, "a.b.client.cz") || !certCovers([]string{"*.client.cz"}, "www.client.cz") {
		t.Fatal("wildcard matching")
	}
}
