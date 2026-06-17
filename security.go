package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const csrfHeader = "X-Deployer-CSRF"
const requestIDHeader = "X-Request-ID"

type requestIDContextKey struct{}

func wrapHTTPHandler(cfg AppConfig, next http.Handler) http.Handler {
	handler := securityHeaders(csrfMiddleware(adminAuthMiddleware(cfg, next)))
	handler = panicRecoveryMiddleware(handler)
	handler = requestLoggingMiddleware(handler)
	return requestIDMiddleware(handler)
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
	return true
}

func adminAuthMiddleware(cfg AppConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/agent/") {
			next.ServeHTTP(w, r)
			return
		}

		if isAgentAccessiblePath(r.URL.Path) && hasBearerToken(r) {
			if _, err := authenticateAgent(r); err != nil {
				jsonErrorCode(w, errCodeInvalidAgentToken, "invalid agent token", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		if cfg.AdminPassword == "" {
			if isLoopbackRemoteAddr(r.RemoteAddr) {
				next.ServeHTTP(w, r)
				return
			}
			jsonErrorCode(w, errCodeAdminAuthNotConfigured, "admin authentication is not configured", http.StatusForbidden)
			return
		}

		user, pass, ok := r.BasicAuth()
		userOK := subtle.ConstantTimeCompare([]byte(user), []byte(cfg.AdminUser)) == 1
		passOK := subtle.ConstantTimeCompare([]byte(pass), []byte(cfg.AdminPassword)) == 1
		if !ok || !userOK || !passOK {
			w.Header().Set("WWW-Authenticate", `Basic realm="Deployer"`)
			jsonErrorCode(w, errCodeAuthenticationRequired, "authentication required", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func isAgentAccessiblePath(path string) bool {
	return path == "/api/version" || path == "/download/deployer"
}

func hasBearerToken(r *http.Request) bool {
	return agentTokenFromRequest(r) != ""
}
