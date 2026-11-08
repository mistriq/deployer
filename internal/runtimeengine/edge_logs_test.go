package runtimeengine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestRunyerAccessLogAndRouteCaps(t *testing.T) {
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/projects/prj_1/edge/logs":
			query = r.URL.RawQuery
			_, _ = w.Write([]byte(`[{"seq":7,"time":1700000000000,"host":"a.test","method":"GET","uri":"/x","status":404,"ip":"1.2.3.4"}]`))
		case "/api/projects/prj_2/edge/logs":
			_, _ = w.Write([]byte(`{"items":[{"seq":1,"status":200}]}`))
		case "/api/projects/prj_1/edge":
			_, _ = w.Write([]byte(`{"route":{"caps":{"basic_auth":{"on":true}}},"site":null}`))
		default:
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"error":"not_found","http":404}`))
		}
	}))
	t.Cleanup(server.Close)
	c, err := NewRunyer(Config{BaseURL: server.URL, Token: "tk_test"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	items, err := c.AccessLog(ctx, "prj_1", url.Values{"status": {"4xx"}, "limit": {"50"}})
	if err != nil || len(items) != 1 || items[0]["uri"] != "/x" || query != "limit=50&status=4xx" {
		t.Fatalf("access log: %v %v %q", err, items, query)
	}
	if wrapped, err := c.AccessLog(ctx, "prj_2", nil); err != nil || len(wrapped) != 1 {
		t.Fatalf("wrapped: %v %v", err, wrapped)
	}
	if caps, err := c.EdgeCaps(ctx, "prj_1"); err != nil || string(caps) != `{"basic_auth":{"on":true}}` {
		t.Fatalf("route caps: %v %s", err, caps)
	}
	var remote *Error
	if _, err := c.AccessLog(ctx, "prj_3", nil); err == nil || !asError(err, &remote) || remote.Code != "NOT_FOUND" {
		t.Fatalf("lowercase code: %v", err)
	}
	detail := failureDetail(Deployment{FailureMessage: "GET / -> connection refused", Events: []PhaseEvent{{Phase: "pulling"}, {Phase: "health_check", Code: "health_check_failed", Message: "no answer on :8080 after 30 s"}}})
	if len(detail) != 2 || detail[0] != "runyer: GET / -> connection refused" || detail[1] != "runyer health_check [health_check_failed]: no answer on :8080 after 30 s" {
		t.Fatalf("detail: %q", detail)
	}
}

func asError(err error, target **Error) bool {
	e, ok := err.(*Error)
	if ok {
		*target = e
	}
	return ok
}
