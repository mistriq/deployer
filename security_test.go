package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminAuthMiddlewareRequiresBasicAuth(t *testing.T) {
	handler := wrapHTTPHandler(AppConfig{
		AdminUser:     "admin",
		AdminPassword: "secret",
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/projects", nil)
	req.RemoteAddr = "203.0.113.10:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized without credentials, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/projects", nil)
	req.RemoteAddr = "203.0.113.10:1234"
	req.SetBasicAuth("admin", "secret")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected authorized request, got %d", rec.Code)
	}
}

func TestAdminAuthMiddlewareAllowsLoopbackWhenPasswordUnset(t *testing.T) {
	handler := wrapHTTPHandler(AppConfig{}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/projects", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected loopback request to be allowed, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/projects", nil)
	req.RemoteAddr = "203.0.113.10:1234"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected non-loopback request to be forbidden, got %d", rec.Code)
	}
}

func TestCSRFMiddlewareRequiresHeaderForBrowserWrites(t *testing.T) {
	handler := wrapHTTPHandler(AppConfig{
		AdminUser:     "admin",
		AdminPassword: "secret",
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/projects", nil)
	req.RemoteAddr = "203.0.113.10:1234"
	req.SetBasicAuth("admin", "secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected missing CSRF header to be forbidden, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/projects", nil)
	req.RemoteAddr = "203.0.113.10:1234"
	req.SetBasicAuth("admin", "secret")
	req.Header.Set(csrfHeader, "1")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected CSRF header to allow request, got %d", rec.Code)
	}
}

func TestMiddlewareSetsRequestID(t *testing.T) {
	handler := wrapHTTPHandler(AppConfig{}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	handler := wrapHTTPHandler(AppConfig{}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	localOnly := securityStatus(AppConfig{Addr: "127.0.0.1:9090"})
	if localOnly.Label != "Local-only" || localOnly.Class != "local-only" {
		t.Fatalf("expected local-only status, got %+v", localOnly)
	}
	if !strings.Contains(localOnly.Title, "disabled") {
		t.Fatalf("expected local-only title to mention disabled auth, got %q", localOnly.Title)
	}

	authenticated := securityStatus(AppConfig{Addr: "0.0.0.0:9090", AdminPassword: "secret"})
	if authenticated.Label != "Auth enabled" || authenticated.Class != "auth-enabled" {
		t.Fatalf("expected authenticated status, got %+v", authenticated)
	}
	if !strings.Contains(authenticated.Title, "public interface") {
		t.Fatalf("expected public listen title, got %q", authenticated.Title)
	}
}
