package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func TestSecurityStatusReflectsAuthenticationMode(t *testing.T) {
	status := securityStatus()
	if status.Label != "External auth" || status.Class != "external-auth" {
		t.Fatalf("expected external auth status, got %+v", status)
	}
	if status.Title == "" {
		t.Fatal("expected external auth status title")
	}
}
