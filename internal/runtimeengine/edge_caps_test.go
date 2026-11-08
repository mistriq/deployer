package runtimeengine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRunyerEdgeCaps(t *testing.T) {
	var sent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/api/edge/modules":
			_, _ = w.Write([]byte(`{"modules":[{"id":"basic_auth","per_site":true,"site_fields":[{"key":"users","type":"text"}]}]}`))
		case r.Method == "GET" && r.URL.Path == "/api/projects/prj_1/edge":
			_, _ = w.Write([]byte(`{"route":{"hostname":"a.test"},"site":{"caps":{"basic_auth":{"on":true,"config":{"realm":"x"}}}}}`))
		case r.Method == "GET" && r.URL.Path == "/api/projects/prj_2/edge":
			_, _ = w.Write([]byte(`{"route":null,"site":null}`))
		case r.Method == "PUT" && r.URL.Path == "/api/projects/prj_1/edge/caps":
			b, _ := io.ReadAll(r.Body)
			sent = string(b)
			_, _ = w.Write([]byte(`{}`))
		case r.Method == "PUT" && r.URL.Path == "/api/projects/prj_3/edge/caps":
			w.WriteHeader(422)
			_, _ = w.Write([]byte(`{"error":"invalid","http":422}`))
		default:
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"error":"NOT_FOUND","http":404}`))
		}
	}))
	t.Cleanup(server.Close)
	c, err := NewRunyer(Config{BaseURL: server.URL, Token: "tk_test"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	modules, err := c.EdgeModules(ctx)
	if err != nil || !json.Valid(modules) {
		t.Fatalf("modules: %v %s", err, modules)
	}
	caps, err := c.EdgeCaps(ctx, "prj_1")
	if err != nil || string(caps) != `{"basic_auth":{"on":true,"config":{"realm":"x"}}}` {
		t.Fatalf("caps: %v %s", err, caps)
	}
	if empty, err := c.EdgeCaps(ctx, "prj_2"); err != nil || string(empty) != "{}" {
		t.Fatalf("no site: %v %s", err, empty)
	}
	if err := c.SetEdgeCaps(ctx, "prj_1", json.RawMessage(`{"basic_auth":{"on":false}}`)); err != nil || sent != `{"caps":{"basic_auth":{"on":false}}}` {
		t.Fatalf("set: %v %s", err, sent)
	}
	var remote *Error
	if err := c.SetEdgeCaps(ctx, "prj_3", json.RawMessage(`{"x":{"on":true}}`)); !errors.As(err, &remote) || remote.HTTPStatus != 422 {
		t.Fatalf("refusal: %v", err)
	}
}
