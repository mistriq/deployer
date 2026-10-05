package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const csrfHeader = "X-Deployer-CSRF"
const requestIDHeader = "X-Request-ID"

type requestIDContextKey struct{}
type mcpBearerContextKey struct{}

func wrapHTTPHandler(next http.Handler) http.Handler {
	handler := securityHeaders(csrfMiddleware(next))
	handler = mcpBearerAuthMiddleware(handler)
	handler = panicRecoveryMiddleware(handler)
	handler = requestLoggingMiddleware(handler)
	return requestIDMiddleware(handler)
}

func mcpBearerAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := strings.TrimSpace(r.Header.Get("Authorization"))
		if auth == "" || !strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/api/agent/") {
			next.ServeHTTP(w, r)
			return
		}
		if !strings.HasPrefix(auth, "Bearer ") {
			jsonErrorCode(w, "invalid_bearer", "invalid bearer credential", http.StatusUnauthorized)
			return
		}
		token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
		readToken, writeToken := strings.TrimSpace(os.Getenv("DEPLOYER_MCP_READ_TOKEN")), strings.TrimSpace(os.Getenv("DEPLOYER_MCP_WRITE_TOKEN"))
		write := secureTokenEqual(token, writeToken)
		read := write || secureTokenEqual(token, readToken)
		if !read {
			jsonErrorCode(w, "invalid_bearer", "invalid bearer credential", http.StatusUnauthorized)
			return
		}
		if requiresCSRF(r) && !write {
			jsonErrorCode(w, "write_scope_required", "write bearer credential required", http.StatusForbidden)
			return
		}
		ctx := context.WithValue(r.Context(), mcpBearerContextKey{}, true)
		machineRequest := r.WithContext(ctx)
		if r.Method == http.MethodGet && !strings.HasSuffix(r.URL.Path, "/artifact") && !strings.HasSuffix(r.URL.Path, "/stream") {
			serveRedactedMachineJSON(next, w, machineRequest)
			return
		}
		next.ServeHTTP(w, machineRequest)
	})
}

type bufferedHTTPResponse struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (w *bufferedHTTPResponse) Header() http.Header { return w.header }
func (w *bufferedHTTPResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *bufferedHTTPResponse) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}
func serveRedactedMachineJSON(next http.Handler, w http.ResponseWriter, r *http.Request) {
	capture := &bufferedHTTPResponse{header: make(http.Header)}
	next.ServeHTTP(capture, r)
	for key, values := range capture.header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	status := capture.status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	body := capture.body.Bytes()
	if strings.Contains(capture.header.Get("Content-Type"), "application/json") {
		body = redactJSON(body)
	}
	_, _ = w.Write(body)
}

func secureTokenEqual(got, want string) bool {
	if got == "" || want == "" || len(got) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := sanitizeRequestID(r.Header.Get(requestIDHeader))
		if requestID == "" {
			requestID = newRequestID()
		}
		w.Header().Set(requestIDHeader, requestID)
		ctx := context.WithValue(r.Context(), requestIDContextKey{}, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func requestLoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		logStructured("info", "http_request", map[string]interface{}{
			"request_id": requestIDFromContext(r.Context()),
			"method":     r.Method,
			"path":       r.URL.RequestURI(),
			"status":     recorder.status,
			"bytes":      recorder.bytes,
			"duration":   time.Since(start).Round(time.Millisecond).String(),
			"remote":     r.RemoteAddr,
		})
	})
}

func panicRecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logStructured("error", "http_panic", map[string]interface{}{
					"request_id": requestIDFromContext(r.Context()),
					"panic":      fmt.Sprint(recovered),
				})
				jsonErrorCode(w, errCodeInternal, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int
	wrote  bool
}

func (w *statusResponseWriter) WriteHeader(status int) {
	if w.wrote {
		return
	}
	w.status = status
	w.wrote = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusResponseWriter) Write(data []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(data)
	w.bytes += n
	return n, err
}

func (w *statusResponseWriter) Flush() {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *statusResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func sanitizeRequestID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return ""
	}
	for _, ch := range value {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' {
			continue
		}
		return ""
	}
	return value
}

func newRequestID() string {
	var data [16]byte
	if _, err := rand.Read(data[:]); err == nil {
		return hex.EncodeToString(data[:])
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

func requestIDFromContext(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDContextKey{}).(string)
	return requestID
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
		next.ServeHTTP(w, r)
	})
}

func csrfMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requiresCSRF(r) && r.Header.Get(csrfHeader) != "1" {
			jsonErrorCode(w, errCodeCSRFRequired, "missing CSRF header", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func requiresCSRF(r *http.Request) bool {
	switch r.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		return false
	}
	if strings.HasPrefix(r.URL.Path, "/api/agent/") {
		return false
	}
	if machine, _ := r.Context().Value(mcpBearerContextKey{}).(bool); machine {
		return false
	}
	return true
}
