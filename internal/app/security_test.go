package app

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminRequestsAreDelegatedToUpstreamAuth(t *testing.T) {
	handler := wrapHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/projects", nil)
	req.RemoteAddr = "203.0.113.10:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected admin request to pass local middleware, got %d", rec.Code)
	}
	if got := rec.Header().Get("WWW-Authenticate"); got != "" {
		t.Fatalf("did not expect Basic Auth challenge, got %q", got)
	}
}

func TestCSRFMiddlewareRequiresHeaderForBrowserWrites(t *testing.T) {
	handler := wrapHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/projects", nil)
	req.RemoteAddr = "203.0.113.10:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected missing CSRF header to be forbidden, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/projects", nil)
	req.RemoteAddr = "203.0.113.10:1234"
	req.Header.Set(csrfHeader, "1")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected CSRF header to allow request, got %d", rec.Code)
	}
}

func TestCSRFExemptionIsLimitedToVersionedPrivateAPI(t *testing.T) {
	handler := wrapHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	for _, path := range []string{"/api/internal/v2/projects/example", "/api/internal-preview/projects/example"} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("unversioned private-like path %q bypassed CSRF with status %d", path, rec.Code)
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/api/internal/v1/projects/example", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("versioned private path did not receive its bearer-middleware exemption: %d", rec.Code)
	}
}

func TestMiddlewareSetsRequestID(t *testing.T) {
	handler := wrapHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/projects", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	req.Header.Set(requestIDHeader, "req-test-123")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected request to pass, got %d", rec.Code)
	}
	if got := rec.Header().Get(requestIDHeader); got != "req-test-123" {
		t.Fatalf("expected request ID to be echoed, got %q", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/projects", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if got := rec.Header().Get(requestIDHeader); got == "" {
		t.Fatal("expected generated request ID")
	}
}

func TestPanicRecoveryMiddlewareReturnsCodedJSONError(t *testing.T) {
	handler := wrapHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/projects", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected internal server error, got %d", rec.Code)
	}
	if rec.Header().Get(requestIDHeader) == "" {
		t.Fatal("expected request ID header")
	}
	var got apiErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if got.Code != errCodeInternal || got.Error != "internal server error" {
		t.Fatalf("unexpected error response: %+v", got)
	}
}

func TestLoggingResponseWriterPreservesFlusher(t *testing.T) {
	handler := requestLoggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := w.(http.Flusher); !ok {
			t.Fatal("expected wrapped response writer to preserve http.Flusher")
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/builds/1/stream", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected no content, got %d", rec.Code)
	}
}

func TestRequestLoggingOmitsQueriesAndRedactsDecodedPaths(t *testing.T) {
	var output bytes.Buffer
	oldOutput := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&output)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldOutput)
		log.SetFlags(oldFlags)
	})
	secret := "dpl_" + strings.Repeat("a", 48)
	rawEncodedSecret := "%64pl_" + strings.Repeat("a", 48)
	handler := requestLoggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/internal/v1/diagnostic/"+rawEncodedSecret+"?note="+secret, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("request status = %d", rec.Code)
	}
	logLine := output.String()
	for _, leaked := range []string{secret, rawEncodedSecret, "?note="} {
		if strings.Contains(logLine, leaked) {
			t.Fatalf("request log leaked %q: %s", leaked, logLine)
		}
	}
	if !strings.Contains(logLine, "[REDACTED]") {
		t.Fatalf("expected decoded path redaction in %s", logLine)
	}
}

func TestSecurityStatusReflectsAuthenticationMode(t *testing.T) {
	status := securityStatus()
	if status.Label != "External auth" || status.Class != "external-auth" {
		t.Fatalf("expected external auth status, got %+v", status)
	}
	if status.Title == "" {
		t.Fatal("expected external auth status title")
	}
}
