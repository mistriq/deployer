package app

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestServiceTokenLifecycle(t *testing.T) {
	withTempDB(t)
	oldConfig := appConfig
	appConfig.ServiceTokenOverlap = 2 * time.Hour
	t.Cleanup(func() { appConfig = oldConfig })

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
	var legacyHash, credentialHash string
	if err := db.QueryRow(`SELECT token_hash FROM service_tokens WHERE id=?`, created.ID).Scan(&legacyHash); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT token_hash FROM service_token_credentials WHERE service_token_id=?`, created.ID).Scan(&credentialHash); err != nil {
		t.Fatal(err)
	}
	for _, stored := range []string{legacyHash, credentialHash} {
		if stored == created.Token || !strings.HasPrefix(stored, "sha256:") || strings.Contains(stored, created.Token) {
			t.Fatalf("plaintext service token persisted: %q", stored)
		}
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

func TestServiceTokenZeroOverlapExpiresPreviousCredentialImmediately(t *testing.T) {
	withTempDB(t)
	oldConfig := appConfig
	appConfig.ServiceTokenOverlap = 0
	t.Cleanup(func() { appConfig = oldConfig })

	created, err := createServiceToken("zero-overlap", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := rotateServiceToken(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authenticateServiceToken(created.Token); !errors.Is(err, errInvalidServiceToken) {
		t.Fatalf("expected previous credential to expire immediately, got %v", err)
	}
	if _, err := authenticateServiceToken(rotated.Token); err != nil {
		t.Fatalf("new credential must remain active: %v", err)
	}
	var overlap int64
	if err := db.QueryRow(`SELECT json_extract(metadata_json, '$.overlap_seconds') FROM service_token_audit_events WHERE service_token_id=? AND event_type='rotated'`, created.ID).Scan(&overlap); err != nil {
		t.Fatal(err)
	}
	if overlap != 0 {
		t.Fatalf("expected audited zero overlap, got %d", overlap)
	}
}

func TestServiceTokenRejectsNonCanonicalNames(t *testing.T) {
	withTempDB(t)
	for _, name := range []string{" leading", "trailing ", "control\nname", strings.Repeat("é", 101)} {
		if _, err := createServiceToken(name, []string{serviceScopeDeploymentsRead}); err == nil {
			t.Fatalf("accepted non-canonical service token name %q", name)
		} else {
			var validation serviceTokenValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("name %q returned non-validation error: %v", name, err)
			}
		}
	}
}

func TestServiceTokenLifecycleAuditFailuresRollbackMutations(t *testing.T) {
	withTempDB(t)
	if _, err := db.Exec(`CREATE TRIGGER reject_service_created BEFORE INSERT ON service_token_audit_events
		WHEN NEW.event_type='created' BEGIN SELECT RAISE(ABORT, 'reject created audit'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := createServiceToken("rolled-back-create", []string{serviceScopeDeploymentsRead}); err == nil {
		t.Fatal("expected create audit failure")
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM service_tokens WHERE name='rolled-back-create'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed create escaped transaction: count=%d err=%v", count, err)
	}
	if _, err := db.Exec(`DROP TRIGGER reject_service_created`); err != nil {
		t.Fatal(err)
	}

	token, err := createServiceToken("audit-rollbacks", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER reject_service_rotated BEFORE INSERT ON service_token_audit_events
		WHEN NEW.event_type='rotated' BEGIN SELECT RAISE(ABORT, 'reject rotated audit'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := rotateServiceToken(token.ID); err == nil {
		t.Fatal("expected rotate audit failure")
	}
	listed, err := listServiceTokens()
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].CredentialGeneration != token.CredentialGeneration {
		t.Fatalf("failed rotation changed current generation: created=%+v listed=%+v", token, listed)
	}
	if _, err := authenticateServiceToken(token.Token); err != nil {
		t.Fatalf("failed rotation invalidated original credential: %v", err)
	}
	if _, err := db.Exec(`DROP TRIGGER reject_service_rotated`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`CREATE TRIGGER reject_service_revoked BEFORE INSERT ON service_token_audit_events
		WHEN NEW.event_type='revoked' BEGIN SELECT RAISE(ABORT, 'reject revoked audit'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := revokeServiceToken(token.ID); err == nil {
		t.Fatal("expected revoke audit failure")
	}
	if _, err := authenticateServiceToken(token.Token); err != nil {
		t.Fatalf("failed revocation invalidated credential: %v", err)
	}
}

func TestServiceTokenMultipleRotationsDoNotExtendOlderCredential(t *testing.T) {
	withTempDB(t)
	oldConfig := appConfig
	t.Cleanup(func() { appConfig = oldConfig })
	appConfig.ServiceTokenOverlap = 2 * time.Hour

	created, err := createServiceToken("bounded-overlap", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatal(err)
	}
	first, err := rotateServiceToken(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	var originalExpiry string
	if err := db.QueryRow(`SELECT expires_at FROM service_token_credentials WHERE token_hash=?`, hashToken(created.Token)).Scan(&originalExpiry); err != nil {
		t.Fatal(err)
	}
	appConfig.ServiceTokenOverlap = 4 * time.Hour
	if _, err := rotateServiceToken(created.ID); err != nil {
		t.Fatal(err)
	}
	var unchangedExpiry, firstExpiry string
	if err := db.QueryRow(`SELECT expires_at FROM service_token_credentials WHERE token_hash=?`, hashToken(created.Token)).Scan(&unchangedExpiry); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT expires_at FROM service_token_credentials WHERE token_hash=?`, hashToken(first.Token)).Scan(&firstExpiry); err != nil {
		t.Fatal(err)
	}
	if unchangedExpiry != originalExpiry {
		t.Fatalf("older credential expiry was extended: before=%s after=%s", originalExpiry, unchangedExpiry)
	}
	if parseSQLiteTime(firstExpiry).Sub(parseSQLiteTime(originalExpiry)) < time.Hour {
		t.Fatalf("expected the newly superseded credential to receive the later overlap: original=%s first=%s", originalExpiry, firstExpiry)
	}
}

func TestServiceTokenRotationOverlapStartsAfterWriterWait(t *testing.T) {
	withTempDB(t)
	oldConfig := appConfig
	appConfig.ServiceTokenOverlap = 500 * time.Millisecond
	t.Cleanup(func() { appConfig = oldConfig })
	created, err := createServiceToken("contended-overlap", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatal(err)
	}
	lockConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lockConn.Close()
	if _, err := lockConn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	type rotationResult struct {
		token *ServiceToken
		err   error
	}
	result := make(chan rotationResult, 1)
	go func() {
		token, err := rotateServiceToken(created.ID)
		result <- rotationResult{token: token, err: err}
	}()
	time.Sleep(750 * time.Millisecond)
	if _, err := lockConn.ExecContext(context.Background(), `ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	rotated := <-result
	if rotated.err != nil {
		t.Fatal(rotated.err)
	}
	if _, err := authenticateServiceToken(created.Token); err != nil {
		t.Fatalf("old credential lost its post-commit overlap: %v", err)
	}
	var expiry string
	if err := db.QueryRow(`SELECT expires_at FROM service_token_credentials WHERE token_hash=?`, hashToken(created.Token)).Scan(&expiry); err != nil {
		t.Fatal(err)
	}
	if remaining := time.Until(parseSQLiteTime(expiry)); remaining < 250*time.Millisecond {
		t.Fatalf("rotation wait consumed overlap; only %s remained", remaining)
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

func TestServiceTokenAuthenticationAuditIsAtomicWithLastUsed(t *testing.T) {
	withTempDB(t)
	token, err := createServiceToken("audit-atomicity", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER reject_service_auth_audit BEFORE INSERT ON service_token_audit_events
		WHEN NEW.event_type='authenticated' BEGIN SELECT RAISE(ABORT, 'reject authentication audit'); END`); err != nil {
		t.Fatal(err)
	}
	handler := serviceTokenAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/internal/v1/capabilities", nil)
	req.Header.Set("Authorization", "Bearer "+token.Token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected audit failure to reject request, got %d body=%s", rec.Code, rec.Body.String())
	}
	var tokenLastUsed, credentialLastUsed sql.NullString
	if err := db.QueryRow(`SELECT t.last_used_at, c.last_used_at FROM service_tokens t
		JOIN service_token_credentials c ON c.service_token_id=t.id WHERE t.id=?`, token.ID).Scan(&tokenLastUsed, &credentialLastUsed); err != nil {
		t.Fatal(err)
	}
	if tokenLastUsed.Valid || credentialLastUsed.Valid {
		t.Fatalf("last-used changes escaped the failed audit transaction: token=%v credential=%v", tokenLastUsed, credentialLastUsed)
	}
}

func TestInvalidServiceTokensDoNotWaitForWriterReservation(t *testing.T) {
	withTempDB(t)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), `ROLLBACK`)

	for _, raw := range []string{"malformed", "dpl_" + strings.Repeat("0", 48)} {
		started := time.Now()
		if _, err := authenticateServiceToken(raw); !errors.Is(err, errInvalidServiceToken) {
			t.Fatalf("expected invalid token for %q, got %v", raw, err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("invalid credential waited on the writer reservation for %s", elapsed)
		}
	}
}

func TestServiceTokenConcurrentCreateIsDeterministic(t *testing.T) {
	withTempDB(t)
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			_, err := createServiceToken("duplicate-name", []string{serviceScopeDeploymentsRead})
			results <- err
		}()
	}
	close(start)
	var created, conflicts int
	for i := 0; i < 2; i++ {
		err := <-results
		switch {
		case err == nil:
			created++
		case errors.Is(err, errServiceTokenNameExists):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent create error: %v", err)
		}
	}
	if created != 1 || conflicts != 1 {
		t.Fatalf("created=%d conflicts=%d", created, conflicts)
	}
}

func TestServiceTokenCredentialMigrationBackfillsLegacyRows(t *testing.T) {
	withTempDB(t)
	now := time.Now().UTC().Add(-time.Hour)
	revokedAt := now.Add(30 * time.Minute)
	rows := []struct {
		name       string
		rawToken   string
		lastUsedAt any
		revokedAt  any
	}{
		{"legacy-active", "dpl_" + strings.Repeat("a", 48), formatSQLiteTime(now), nil},
		{"legacy-revoked", "dpl_" + strings.Repeat("b", 48), nil, formatSQLiteTime(revokedAt)},
	}
	for _, row := range rows {
		if _, err := db.Exec(`INSERT INTO service_tokens (name, token_hash, scopes, created_at, last_used_at, revoked_at)
			VALUES (?, ?, ?, ?, ?, ?)`, row.name, hashToken(row.rawToken), `["deployments:read"]`, formatSQLiteTime(now), row.lastUsedAt, row.revokedAt); err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range []string{
		`DROP TABLE service_token_audit_events`,
		`DROP TABLE service_token_credentials`,
		`DROP TABLE hosting_settings`,
		`DELETE FROM schema_migrations WHERE id='021_service_credentials_audit'`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("prepare legacy schema with %q: %v", statement, err)
		}
	}
	if err := applyHostingMigrations(); err != nil {
		t.Fatalf("apply credential migration: %v", err)
	}
	for _, row := range rows {
		var tokenHash, created string
		var lastUsed, revoked sql.NullString
		if err := db.QueryRow(`SELECT c.token_hash, c.created_at, c.last_used_at, c.revoked_at
			FROM service_token_credentials c JOIN service_tokens t ON t.id=c.service_token_id WHERE t.name=?`, row.name).
			Scan(&tokenHash, &created, &lastUsed, &revoked); err != nil {
			t.Fatal(err)
		}
		if tokenHash != hashToken(row.rawToken) || created != formatSQLiteTime(now) {
			t.Fatalf("incorrect credential backfill for %s: hash=%q created=%q", row.name, tokenHash, created)
		}
		if (row.lastUsedAt != nil) != lastUsed.Valid || (row.revokedAt != nil) != revoked.Valid {
			t.Fatalf("nullable lifecycle fields not preserved for %s: last=%v revoked=%v", row.name, lastUsed, revoked)
		}
	}
}

func TestServiceTokenConcurrentRotateAndRevokeAreSerialized(t *testing.T) {
	withTempDB(t)
	oldConfig := appConfig
	appConfig.ServiceTokenOverlap = time.Hour
	t.Cleanup(func() { appConfig = oldConfig })
	token, err := createServiceToken("rotate-revoke", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatal(err)
	}
	const rotations = 8
	start := make(chan struct{})
	rotated := make(chan *ServiceToken, rotations)
	errorsSeen := make(chan error, rotations)
	var wait sync.WaitGroup
	for i := 0; i < rotations; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			credential, err := rotateServiceToken(token.ID)
			rotated <- credential
			errorsSeen <- err
		}()
	}
	close(start)
	revoked, err := revokeServiceToken(token.ID)
	if err != nil || !revoked {
		t.Fatalf("revoke=%v err=%v", revoked, err)
	}
	wait.Wait()
	close(rotated)
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil && !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, errServiceTokenGenerationConflict) {
			t.Fatalf("unexpected serialized rotation error: %v", err)
		}
	}
	for credential := range rotated {
		if credential == nil {
			continue
		}
		if _, err := authenticateServiceToken(credential.Token); !errors.Is(err, errInvalidServiceToken) {
			t.Fatalf("rotated credential remained active after revocation: %v", err)
		}
	}
	if _, err := authenticateServiceToken(token.Token); !errors.Is(err, errInvalidServiceToken) {
		t.Fatalf("original credential remained active after revocation: %v", err)
	}
}

func TestServiceTokenWrappedRoutesEnforceBoundaryAndAuditLifecycle(t *testing.T) {
	withTempDB(t)
	oldConfig := appConfig
	appConfig.ServiceTokenOverlap = time.Hour
	t.Cleanup(func() { appConfig = oldConfig })

	mux := http.NewServeMux()
	mux.HandleFunc("/api/service-tokens", handleAPIServiceTokens)
	mux.HandleFunc("/api/service-tokens/", handleAPIServiceToken)
	mux.Handle("/api/internal/v1/", serviceTokenAuthMiddleware(http.HandlerFunc(handleInternalAPI)))
	handler := wrapHTTPHandler(mux)
	body := []byte(`{"name":"wrapped-boundary","scopes":["deployments:read","hosting:admin"]}`)

	req := httptest.NewRequest(http.MethodPost, "/api/service-tokens", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("admin write without CSRF = %d body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/api/service-tokens", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(csrfHeader, "1")
	req.Header.Set(requestIDHeader, "req-service-create")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create = %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("Pragma") != "no-cache" {
		t.Fatalf("one-time secret response is cacheable: %v", rec.Header())
	}
	var token ServiceToken
	if err := json.NewDecoder(rec.Body).Decode(&token); err != nil {
		t.Fatal(err)
	}
	if token.CredentialGeneration <= 0 || rec.Header().Get(serviceTokenGenerationHeader) != formatServiceTokenGeneration(token.CredentialGeneration) {
		t.Fatalf("missing credential generation: token=%+v headers=%v", token, rec.Header())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/internal/v1/capabilities", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("private route without bearer = %d body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPut, "/api/internal/v1/settings/kill-switch", nil)
	req.Header.Set("Authorization", "Bearer "+token.Token)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("valid private bearer should bypass browser CSRF and reach handler, got %d body=%s", rec.Code, rec.Body.String())
	}

	rotatePath := "/api/service-tokens/" + strconv.FormatInt(token.ID, 10) + "/rotate"
	req = httptest.NewRequest(http.MethodPost, rotatePath, nil)
	req.Header.Set(serviceTokenIfGenerationHeader, formatServiceTokenGeneration(token.CredentialGeneration))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("rotation without CSRF = %d body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, rotatePath, nil)
	req.Header.Set(csrfHeader, "1")
	req.Header.Set(requestIDHeader, "req-service-rotate")
	req.Header.Set(serviceTokenIfGenerationHeader, formatServiceTokenGeneration(token.CredentialGeneration))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("rotate = %d headers=%v body=%s", rec.Code, rec.Header(), rec.Body.String())
	}
	var rotatedToken ServiceToken
	if err := json.NewDecoder(rec.Body).Decode(&rotatedToken); err != nil {
		t.Fatal(err)
	}
	if rotatedToken.CredentialGeneration <= token.CredentialGeneration || rec.Header().Get(serviceTokenGenerationHeader) != formatServiceTokenGeneration(rotatedToken.CredentialGeneration) {
		t.Fatalf("rotated generation is missing or stale: created=%+v rotated=%+v headers=%v", token, rotatedToken, rec.Header())
	}

	revokePath := "/api/service-tokens/" + strconv.FormatInt(token.ID, 10)
	req = httptest.NewRequest(http.MethodDelete, revokePath, nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("revocation without CSRF = %d body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodDelete, revokePath, nil)
	req.Header.Set(csrfHeader, "1")
	req.Header.Set(requestIDHeader, "req-service-revoke")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("revoke = %d body=%s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodGet, revokePath, nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound || rec.Header().Get(serviceTokenGenerationHeader) != "" {
		t.Fatalf("revoked token confirmed as current: status=%d headers=%v body=%s", rec.Code, rec.Header(), rec.Body.String())
	}

	want := map[string]string{
		"created": "req-service-create",
		"rotated": "req-service-rotate",
		"revoked": "req-service-revoke",
	}
	rows, err := db.Query(`SELECT event_type, request_id FROM service_token_audit_events
		WHERE service_token_id=? AND event_type IN ('created','rotated','revoked')`, token.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventType, requestID string
		if err := rows.Scan(&eventType, &requestID); err != nil {
			t.Fatal(err)
		}
		if requestID != want[eventType] {
			t.Fatalf("%s request_id=%q want=%q", eventType, requestID, want[eventType])
		}
		delete(want, eventType)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(want) != 0 {
		t.Fatalf("missing lifecycle audits: %v", want)
	}
}

func TestConcurrentServiceTokenRotationsRequireCurrentGeneration(t *testing.T) {
	withTempDB(t)
	oldConfig := appConfig
	appConfig.ServiceTokenOverlap = time.Hour
	t.Cleanup(func() { appConfig = oldConfig })
	created, err := createServiceToken("generation-fenced-rotation", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/service-tokens/", handleAPIServiceToken)
	handler := wrapHTTPHandler(mux)
	path := "/api/service-tokens/" + strconv.FormatInt(created.ID, 10) + "/rotate"

	missing := httptest.NewRequest(http.MethodPost, path, nil)
	missing.Header.Set(csrfHeader, "1")
	missingRecorder := httptest.NewRecorder()
	handler.ServeHTTP(missingRecorder, missing)
	if missingRecorder.Code != http.StatusPreconditionRequired {
		t.Fatalf("missing generation = %d body=%s", missingRecorder.Code, missingRecorder.Body.String())
	}

	start := make(chan struct{})
	responses := make(chan *httptest.ResponseRecorder, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			req := httptest.NewRequest(http.MethodPost, path, nil)
			req.Header.Set(csrfHeader, "1")
			req.Header.Set(serviceTokenIfGenerationHeader, formatServiceTokenGeneration(created.CredentialGeneration))
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			responses <- recorder
		}()
	}
	close(start)
	var success, failed int
	var rotated ServiceToken
	for i := 0; i < 2; i++ {
		response := <-responses
		switch response.Code {
		case http.StatusOK:
			success++
			if err := json.NewDecoder(response.Body).Decode(&rotated); err != nil {
				t.Fatal(err)
			}
		case http.StatusPreconditionFailed:
			failed++
		default:
			t.Fatalf("unexpected concurrent rotation status=%d body=%s", response.Code, response.Body.String())
		}
	}
	if success != 1 || failed != 1 {
		t.Fatalf("success=%d precondition_failed=%d", success, failed)
	}
	tokens, err := listServiceTokens()
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 || rotated.CredentialGeneration <= created.CredentialGeneration || tokens[0].CredentialGeneration != rotated.CredentialGeneration {
		t.Fatalf("rotation generation is not authoritative: created=%+v rotated=%+v listed=%+v", created, rotated, tokens)
	}

	metadataPath := "/api/service-tokens/" + strconv.FormatInt(created.ID, 10)
	metadataRequest := httptest.NewRequest(http.MethodGet, metadataPath, nil)
	metadataRecorder := httptest.NewRecorder()
	handler.ServeHTTP(metadataRecorder, metadataRequest)
	if metadataRecorder.Code != http.StatusOK || metadataRecorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("metadata read = %d headers=%v body=%s", metadataRecorder.Code, metadataRecorder.Header(), metadataRecorder.Body.String())
	}
	var observed ServiceToken
	if err := json.NewDecoder(metadataRecorder.Body).Decode(&observed); err != nil {
		t.Fatal(err)
	}
	if observed.Token != "" || observed.CredentialGeneration != rotated.CredentialGeneration {
		t.Fatalf("metadata exposed a secret or stale generation: %+v", observed)
	}

	secondRequest := httptest.NewRequest(http.MethodPost, path, nil)
	secondRequest.Header.Set(csrfHeader, "1")
	secondRequest.Header.Set(serviceTokenIfGenerationHeader, formatServiceTokenGeneration(observed.CredentialGeneration))
	secondRecorder := httptest.NewRecorder()
	handler.ServeHTTP(secondRecorder, secondRequest)
	if secondRecorder.Code != http.StatusOK {
		t.Fatalf("chained rotation = %d body=%s", secondRecorder.Code, secondRecorder.Body.String())
	}
	var secondRotation ServiceToken
	if err := json.NewDecoder(secondRecorder.Body).Decode(&secondRotation); err != nil {
		t.Fatal(err)
	}

	confirmationRequest := httptest.NewRequest(http.MethodGet, metadataPath, nil)
	confirmationRecorder := httptest.NewRecorder()
	handler.ServeHTTP(confirmationRecorder, confirmationRequest)
	var current ServiceToken
	if err := json.NewDecoder(confirmationRecorder.Body).Decode(&current); err != nil {
		t.Fatal(err)
	}
	if current.CredentialGeneration != secondRotation.CredentialGeneration || current.CredentialGeneration == rotated.CredentialGeneration {
		t.Fatalf("authoritative confirmation did not detect delayed superseded response: delayed=%+v second=%+v current=%+v", rotated, secondRotation, current)
	}
}

func TestServiceTokenAuditRedactsRequestPath(t *testing.T) {
	withTempDB(t)
	token, err := createServiceToken("audit-redaction", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatal(err)
	}
	handler := serviceTokenAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(token.Token, "/api/internal/v1/diagnostic/"+token.Token+"/"+strings.Repeat("segment/", 100), nil)
	req.Header.Set("Authorization", "Bearer "+token.Token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("request = %d body=%s", rec.Code, rec.Body.String())
	}
	deniedHandler := serviceTokenAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requireServiceTokenScope(w, r, serviceScopeDeploymentsWrite) {
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	req = httptest.NewRequest(token.Token, "/api/internal/v1/diagnostic/"+token.Token, nil)
	req.Header.Set("Authorization", "Bearer "+token.Token)
	rec = httptest.NewRecorder()
	deniedHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("scope denial = %d body=%s", rec.Code, rec.Body.String())
	}

	rows, err := db.Query(`SELECT event_type, metadata_json FROM service_token_audit_events
		WHERE service_token_id=? AND event_type IN ('authenticated','scope_denied') ORDER BY id`, token.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var checked int
	for rows.Next() {
		var eventType, metadata string
		if err := rows.Scan(&eventType, &metadata); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(metadata, token.Token) || !strings.Contains(metadata, "[REDACTED]") {
			t.Fatalf("%s audit metadata was not redacted: %s", eventType, metadata)
		}
		var fields map[string]string
		if err := json.Unmarshal([]byte(metadata), &fields); err != nil {
			t.Fatal(err)
		}
		if fields["method"] != "[REDACTED]" || len([]rune(fields["path"])) > 512 {
			t.Fatalf("%s audit metadata was not bounded: %#v", eventType, fields)
		}
		checked++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if checked != 3 {
		t.Fatalf("expected two authenticated and one scope-denied audit, got %d", checked)
	}
}

func TestInternalRoutesRequireTheirDocumentedServiceTokenScopes(t *testing.T) {
	withTempDB(t)
	tokens := make(map[string]*ServiceToken)
	for scope := range allowedServiceTokenScopes {
		token, err := createServiceToken("scope-matrix-"+strings.ReplaceAll(scope, ":", "-"), []string{scope})
		if err != nil {
			t.Fatal(err)
		}
		tokens[scope] = token
	}
	tests := []struct {
		method string
		path   string
		scope  string
	}{
		{http.MethodGet, "/api/internal/v1/capabilities", serviceScopeDeploymentsRead},
		{http.MethodGet, "/api/internal/v1/runners", serviceScopeDeploymentsRead},
		{http.MethodGet, "/api/internal/v1/metrics", serviceScopeDeploymentsRead},
		{http.MethodGet, "/api/internal/v1/projects/project_01JAUTHMATRIX/releases", serviceScopeDeploymentsRead},
		{http.MethodGet, "/api/internal/v1/deployments/deploy_01JAUTHMATRIX", serviceScopeDeploymentsRead},
		{http.MethodGet, "/api/internal/v1/deployments/deploy_01JAUTHMATRIX/events", serviceScopeDeploymentsRead},
		{http.MethodGet, "/api/internal/v1/deployments/deploy_01JAUTHMATRIX/logs", serviceScopeDeploymentsRead},
		{http.MethodGet, "/api/internal/v1/projects/project_01JAUTHMATRIX/runtime-health", serviceScopeDeploymentsRead},
		{http.MethodPost, "/api/internal/v1/projects/project_01JAUTHMATRIX/deployments", serviceScopeDeploymentsWrite},
		{http.MethodPost, "/api/internal/v1/projects/project_01JAUTHMATRIX/rollback", serviceScopeDeploymentsWrite},
		{http.MethodPost, "/api/internal/v1/deployments/deploy_01JAUTHMATRIX/cancel", serviceScopeDeploymentsWrite},
		{http.MethodPut, "/api/internal/v1/projects/project_01JAUTHMATRIX", serviceScopeProjectsWrite},
		{http.MethodPost, "/api/internal/v1/projects/project_01JAUTHMATRIX/suspend", serviceScopeProjectsWrite},
		{http.MethodPost, "/api/internal/v1/projects/project_01JAUTHMATRIX/resume", serviceScopeProjectsWrite},
		{http.MethodPut, "/api/internal/v1/projects/project_01JAUTHMATRIX/kill-switch", serviceScopeProjectsWrite},
		{http.MethodPut, "/api/internal/v1/settings/kill-switch", serviceScopeHostingAdmin},
	}
	handler := serviceTokenAuthMiddleware(http.HandlerFunc(handleInternalAPI))
	for _, test := range tests {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			wrongScope := serviceScopeHostingAdmin
			if test.scope == serviceScopeHostingAdmin {
				wrongScope = serviceScopeDeploymentsRead
			}
			req := httptest.NewRequest(test.method, test.path, nil)
			req.Header.Set("Authorization", "Bearer "+tokens[wrongScope].Token)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("wrong scope %q returned %d body=%s", wrongScope, rec.Code, rec.Body.String())
			}

			req = httptest.NewRequest(test.method, test.path, nil)
			req.Header.Set("Authorization", "Bearer "+tokens[test.scope].Token)
			rec = httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
				t.Fatalf("documented scope %q was rejected with %d body=%s", test.scope, rec.Code, rec.Body.String())
			}
		})
	}
}
