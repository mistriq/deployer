package app

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestServiceTokenLifecycle(t *testing.T) {
	withTempDB(t)

	created, err := createServiceToken("hosting-control-plane", []string{
		serviceScopeDeploymentsWrite,
		serviceScopeDeploymentsRead,
		serviceScopeDeploymentsRead,
	})
	if err != nil {
		t.Fatalf("create service token: %v", err)
	}
	if !strings.HasPrefix(created.Token, "dpl_") {
		t.Fatalf("expected one-time plaintext token, got %q", created.Token)
	}
	if len(created.Scopes) != 2 || created.Scopes[0] != serviceScopeDeploymentsRead {
		t.Fatalf("expected normalized scopes, got %#v", created.Scopes)
	}

	authenticated, err := authenticateServiceToken(created.Token)
	if err != nil {
		t.Fatalf("authenticate service token: %v", err)
	}
	if authenticated.ID != created.ID || authenticated.LastUsedAt == nil {
		t.Fatalf("unexpected authenticated token: %+v", authenticated)
	}

	tokens, err := listServiceTokens()
	if err != nil {
		t.Fatalf("list service tokens: %v", err)
	}
	if len(tokens) != 1 || tokens[0].Token != "" {
		t.Fatalf("stored token must never be returned, got %+v", tokens)
	}

	rotated, err := rotateServiceToken(created.ID)
	if err != nil {
		t.Fatalf("rotate service token: %v", err)
	}
	if rotated.Token == "" || rotated.Token == created.Token {
		t.Fatal("expected a new one-time plaintext token")
	}
	if _, err := authenticateServiceToken(created.Token); err != nil {
		t.Fatalf("expected old token to remain valid during rotation overlap: %v", err)
	}
	if _, err := authenticateServiceToken(rotated.Token); err != nil {
		t.Fatalf("authenticate rotated token: %v", err)
	}

	revoked, err := revokeServiceToken(created.ID)
	if err != nil || !revoked {
		t.Fatalf("revoke service token: revoked=%v err=%v", revoked, err)
	}
	if _, err := authenticateServiceToken(rotated.Token); err == nil {
		t.Fatal("expected revoked token to stop working")
	}
	if _, err := authenticateServiceToken(created.Token); err == nil {
		t.Fatal("expected revocation to invalidate overlapping old credential")
	}
	var auditCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM service_token_audit_events WHERE service_token_id=? AND event_type IN ('created','rotated','revoked')`, created.ID).Scan(&auditCount); err != nil {
		t.Fatalf("count token audit events: %v", err)
	}
	if auditCount != 3 {
		t.Fatalf("expected create/rotate/revoke audit events, got %d", auditCount)
	}
}

func TestServiceTokenMiddlewareEnforcesScopes(t *testing.T) {
	withTempDB(t)

	token, err := createServiceToken("read-only", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatalf("create service token: %v", err)
	}
	readHandler := serviceTokenAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !requireServiceTokenScope(w, r, serviceScopeDeploymentsRead) {
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/internal/v1/deployments/1", nil)
	rec := httptest.NewRecorder()
	readHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected missing token to be unauthorized, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/internal/v1/deployments/1", nil)
	req.Header.Set("Authorization", "Bearer "+token.Token)
	rec = httptest.NewRecorder()
	readHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected read token to pass, got %d body=%s", rec.Code, rec.Body.String())
	}

	writeHandler := serviceTokenAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !requireServiceTokenScope(w, r, serviceScopeDeploymentsWrite) {
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	req = httptest.NewRequest(http.MethodPost, "/api/internal/v1/projects/project_01JHOSTING/deployments", nil)
	req.Header.Set("Authorization", "Bearer "+token.Token)
	rec = httptest.NewRecorder()
	writeHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected read-only token to be forbidden, got %d", rec.Code)
	}
	var authenticatedAudits, deniedAudits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM service_token_audit_events WHERE service_token_id=? AND event_type='authenticated'`, token.ID).Scan(&authenticatedAudits); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM service_token_audit_events WHERE service_token_id=? AND event_type='scope_denied'`, token.ID).Scan(&deniedAudits); err != nil {
		t.Fatal(err)
	}
	if authenticatedAudits != 2 || deniedAudits != 1 {
		t.Fatalf("authenticated audits=%d denied audits=%d", authenticatedAudits, deniedAudits)
	}
}

func TestServiceTokenRevocationWinsAgainstConcurrentAuthentication(t *testing.T) {
	withTempDB(t)
	token, err := createServiceToken("concurrent-revocation", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatal(err)
	}
	const workers = 16
	start := make(chan struct{})
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, _ = authenticateServiceToken(token.Token)
		}()
	}
	close(start)
	revoked, err := revokeServiceToken(token.ID)
	if err != nil || !revoked {
		t.Fatalf("revoke=%v err=%v", revoked, err)
	}
	wait.Wait()
	for i := 0; i < workers; i++ {
		if _, err := authenticateServiceToken(token.Token); err == nil {
			t.Fatal("authentication succeeded after revocation committed")
		}
	}
}
