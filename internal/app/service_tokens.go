package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	serviceScopeDeploymentsRead  = "deployments:read"
	serviceScopeDeploymentsWrite = "deployments:write"
	serviceScopeProjectsWrite    = "projects:write"
	serviceScopeHostingAdmin     = "hosting:admin"
)

var allowedServiceTokenScopes = map[string]struct{}{
	serviceScopeDeploymentsRead:  {},
	serviceScopeDeploymentsWrite: {},
	serviceScopeProjectsWrite:    {},
	serviceScopeHostingAdmin:     {},
}

type ServiceToken struct {
	ID           int64      `json:"id"`
	Name         string     `json:"name"`
	Scopes       []string   `json:"scopes"`
	CreatedAt    time.Time  `json:"created_at"`
	LastUsedAt   *time.Time `json:"last_used_at,omitempty"`
	RevokedAt    *time.Time `json:"revoked_at,omitempty"`
	Token        string     `json:"token,omitempty"`
	rawToken     string
	credentialID int64
}

type serviceTokenContextKey struct{}

func createServiceToken(name string, scopes []string) (*ServiceToken, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if len(name) > 100 {
		return nil, fmt.Errorf("name is too large")
	}
	normalizedScopes, err := normalizeServiceTokenScopes(scopes)
	if err != nil {
		return nil, err
	}
	rawToken := "dpl_" + generateToken()
	scopesJSON, err := json.Marshal(normalizedScopes)
	if err != nil {
		return nil, fmt.Errorf("encode scopes: %w", err)
	}
	now := time.Now().UTC()
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`INSERT INTO service_tokens (name, token_hash, scopes, created_at) VALUES (?, ?, ?, ?)`,
		name, hashToken(rawToken), string(scopesJSON), formatSQLiteTime(now),
	)
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	credential, err := tx.Exec(`INSERT INTO service_token_credentials (service_token_id, token_hash, created_at) VALUES (?, ?, ?)`, id, hashToken(rawToken), formatSQLiteTime(now))
	if err != nil {
		return nil, err
	}
	credentialID, err := credential.LastInsertId()
	if err != nil {
		return nil, err
	}
	if err := insertServiceTokenAudit(tx, id, credentialID, "created", "trusted-admin", "", map[string]any{"scopes": normalizedScopes}, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &ServiceToken{
		ID:           id,
		Name:         name,
		Scopes:       normalizedScopes,
		CreatedAt:    now,
		Token:        rawToken,
		credentialID: credentialID,
	}, nil
}

func normalizeServiceTokenScopes(scopes []string) ([]string, error) {
	unique := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if _, ok := allowedServiceTokenScopes[scope]; !ok {
			return nil, fmt.Errorf("unsupported scope %q", scope)
		}
		unique[scope] = struct{}{}
	}
	if len(unique) == 0 {
		return nil, fmt.Errorf("at least one scope is required")
	}
	normalized := make([]string, 0, len(unique))
	for scope := range unique {
		normalized = append(normalized, scope)
	}
	sort.Strings(normalized)
	return normalized, nil
}

func listServiceTokens() ([]ServiceToken, error) {
	rows, err := db.Query(`SELECT id, name, scopes, created_at, last_used_at, revoked_at FROM service_tokens ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tokens := make([]ServiceToken, 0)
	for rows.Next() {
		token, err := scanServiceToken(rows)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, *token)
	}
	return tokens, rows.Err()
}

type serviceTokenScanner interface {
	Scan(dest ...interface{}) error
}

func scanServiceToken(scanner serviceTokenScanner) (*ServiceToken, error) {
	var token ServiceToken
	var scopesJSON, createdAt string
	var lastUsedAt, revokedAt sql.NullString
	if err := scanner.Scan(&token.ID, &token.Name, &scopesJSON, &createdAt, &lastUsedAt, &revokedAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(scopesJSON), &token.Scopes); err != nil {
		return nil, fmt.Errorf("decode service token scopes: %w", err)
	}
	token.CreatedAt = parseSQLiteTime(createdAt)
	if lastUsedAt.Valid {
		value := parseSQLiteTime(lastUsedAt.String)
		token.LastUsedAt = &value
	}
	if revokedAt.Valid {
		value := parseSQLiteTime(revokedAt.String)
		token.RevokedAt = &value
	}
	return &token, nil
}

func authenticateServiceToken(rawToken string) (*ServiceToken, error) {
	rawToken = strings.TrimSpace(rawToken)
	if rawToken == "" {
		return nil, fmt.Errorf("service token is required")
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	row := conn.QueryRowContext(context.Background(), `SELECT t.id, t.name, t.scopes, t.created_at, t.last_used_at, t.revoked_at, c.id
		FROM service_tokens t JOIN service_token_credentials c ON c.service_token_id=t.id
		WHERE c.token_hash=? AND t.revoked_at IS NULL AND c.revoked_at IS NULL
		  AND (c.expires_at IS NULL OR c.expires_at>?)`, hashToken(rawToken), formatSQLiteTime(time.Now().UTC()))
	var credentialID int64
	token, err := scanServiceTokenWithCredential(row, &credentialID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("invalid service token")
		}
		return nil, err
	}
	now := time.Now().UTC()
	result, err := conn.ExecContext(context.Background(), `UPDATE service_token_credentials SET last_used_at=?
		WHERE id=? AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>?)
		  AND EXISTS (SELECT 1 FROM service_tokens t WHERE t.id=service_token_id AND t.revoked_at IS NULL)`,
		formatSQLiteTime(now), credentialID, formatSQLiteTime(now))
	if err != nil {
		return nil, err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		return nil, fmt.Errorf("service token became inactive")
	}
	if _, err := conn.ExecContext(context.Background(), `UPDATE service_tokens SET last_used_at=? WHERE id=? AND revoked_at IS NULL`, formatSQLiteTime(now), token.ID); err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(context.Background(), `COMMIT`); err != nil {
		return nil, err
	}
	committed = true
	token.LastUsedAt = &now
	token.rawToken = rawToken
	token.credentialID = credentialID
	return token, nil
}

func scanServiceTokenWithCredential(scanner serviceTokenScanner, credentialID *int64) (*ServiceToken, error) {
	var token ServiceToken
	var scopesJSON, createdAt string
	var lastUsedAt, revokedAt sql.NullString
	if err := scanner.Scan(&token.ID, &token.Name, &scopesJSON, &createdAt, &lastUsedAt, &revokedAt, credentialID); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(scopesJSON), &token.Scopes); err != nil {
		return nil, fmt.Errorf("decode service token scopes: %w", err)
	}
	token.CreatedAt = parseSQLiteTime(createdAt)
	if lastUsedAt.Valid {
		value := parseSQLiteTime(lastUsedAt.String)
		token.LastUsedAt = &value
	}
	if revokedAt.Valid {
		value := parseSQLiteTime(revokedAt.String)
		token.RevokedAt = &value
	}
	return &token, nil
}

func rotateServiceToken(id int64) (*ServiceToken, error) {
	rawToken := "dpl_" + generateToken()
	now := time.Now().UTC()
	overlap := appConfig.ServiceTokenOverlap
	if overlap <= 0 {
		overlap = 24 * time.Hour
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var active int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM service_tokens WHERE id=? AND revoked_at IS NULL`, id).Scan(&active); err != nil {
		return nil, err
	}
	if active == 0 {
		return nil, sql.ErrNoRows
	}
	if _, err := tx.Exec(`UPDATE service_token_credentials SET expires_at=?
		WHERE service_token_id=? AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>?)`,
		formatSQLiteTime(now.Add(overlap)), id, formatSQLiteTime(now.Add(overlap))); err != nil {
		return nil, err
	}
	credential, err := tx.Exec(`INSERT INTO service_token_credentials (service_token_id, token_hash, created_at) VALUES (?, ?, ?)`, id, hashToken(rawToken), formatSQLiteTime(now))
	if err != nil {
		return nil, err
	}
	credentialID, err := credential.LastInsertId()
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`UPDATE service_tokens SET token_hash=?, last_used_at=NULL WHERE id=?`, hashToken(rawToken), id); err != nil {
		return nil, err
	}
	if err := insertServiceTokenAudit(tx, id, credentialID, "rotated", "trusted-admin", "", map[string]any{"overlap_seconds": int64(overlap.Seconds())}, now); err != nil {
		return nil, err
	}
	row := tx.QueryRow(`SELECT id, name, scopes, created_at, last_used_at, revoked_at FROM service_tokens WHERE id=?`, id)
	token, err := scanServiceToken(row)
	if err != nil {
		return nil, err
	}
	token.Token = rawToken
	token.credentialID = credentialID
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return token, nil
}

func revokeServiceToken(id int64) (bool, error) {
	now := time.Now().UTC()
	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`UPDATE service_tokens SET revoked_at=? WHERE id=? AND revoked_at IS NULL`, formatSQLiteTime(now), id)
	if err != nil {
		return false, err
	}
	count, err := res.RowsAffected()
	if err != nil || count == 0 {
		return false, err
	}
	if _, err := tx.Exec(`UPDATE service_token_credentials SET revoked_at=? WHERE service_token_id=? AND revoked_at IS NULL`, formatSQLiteTime(now), id); err != nil {
		return false, err
	}
	if err := insertServiceTokenAudit(tx, id, 0, "revoked", "trusted-admin", "", nil, now); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

type sqlExecutor interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func insertServiceTokenAudit(executor sqlExecutor, tokenID, credentialID int64, eventType, actor, requestID string, metadata map[string]any, now time.Time) error {
	if metadata == nil {
		metadata = map[string]any{}
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	var credential any
	if credentialID != 0 {
		credential = credentialID
	}
	_, err = executor.Exec(`INSERT INTO service_token_audit_events
		(service_token_id, credential_id, event_type, actor, request_id, metadata_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, tokenID, credential, eventType, actor, requestID, string(encoded), formatSQLiteTime(now))
	return err
}

func serviceTokenAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
		if !strings.HasPrefix(authHeader, "Bearer ") {
			w.Header().Set("WWW-Authenticate", "Bearer")
			jsonErrorCode(w, errCodeServiceTokenRequired, "service token is required", http.StatusUnauthorized)
			return
		}
		token, err := authenticateServiceToken(strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer ")))
		if err != nil {
			w.Header().Set("WWW-Authenticate", "Bearer")
			jsonErrorCode(w, errCodeInvalidServiceToken, "invalid service token", http.StatusUnauthorized)
			return
		}
		if err := insertServiceTokenAudit(db, token.ID, token.credentialID, "authenticated", "service-token", requestIDFromContext(r.Context()), map[string]any{
			"method": r.Method,
			"path":   r.URL.Path,
		}, time.Now().UTC()); err != nil {
			jsonErrorCode(w, errCodeInternal, "service token audit failed", http.StatusInternalServerError)
			return
		}
		ctx := context.WithValue(r.Context(), serviceTokenContextKey{}, token)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func requireServiceTokenScope(w http.ResponseWriter, r *http.Request, required string) bool {
	token, _ := r.Context().Value(serviceTokenContextKey{}).(*ServiceToken)
	if token == nil {
		jsonErrorCode(w, errCodeServiceTokenRequired, "service token is required", http.StatusUnauthorized)
		return false
	}
	for _, scope := range token.Scopes {
		if scope == required {
			return true
		}
	}
	if err := insertServiceTokenAudit(db, token.ID, token.credentialID, "scope_denied", "service-token", requestIDFromContext(r.Context()), map[string]any{
		"method":         r.Method,
		"path":           r.URL.Path,
		"required_scope": required,
	}, time.Now().UTC()); err != nil {
		jsonErrorCode(w, errCodeInternal, "service token audit failed", http.StatusInternalServerError)
		return false
	}
	jsonErrorCode(w, errCodeInsufficientScope, "service token does not have required scope", http.StatusForbidden)
	return false
}

func handleAPIServiceTokens(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		tokens, err := listServiceTokens()
		if err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		jsonResponse(w, tokens)
	case http.MethodPost:
		if !requireJSONContentType(w, r) {
			return
		}
		var payload struct {
			Name   string   `json:"name"`
			Scopes []string `json:"scopes"`
		}
		if !decodeInternalJSON(w, r, 16<<10, &payload) {
			return
		}
		token, err := createServiceToken(payload.Name, payload.Scopes)
		if err != nil {
			jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		jsonResponse(w, token)
	default:
		jsonMethodNotAllowed(w, http.MethodGet, http.MethodPost)
	}
}

func handleAPIServiceToken(w http.ResponseWriter, r *http.Request) {
	id, suffix, ok := parseIDPath(r.URL.Path, "/api/service-tokens/")
	if !ok {
		http.NotFound(w, r)
		return
	}
	if suffix == "rotate" {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		token, err := rotateServiceToken(id)
		if err != nil {
			if err == sql.ErrNoRows {
				jsonErrorCode(w, errCodeServiceTokenNotFound, "service token not found", http.StatusNotFound)
				return
			}
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		jsonResponse(w, token)
		return
	}
	if suffix != "" {
		http.NotFound(w, r)
		return
	}
	if !requireMethod(w, r, http.MethodDelete) {
		return
	}
	revoked, err := revokeServiceToken(id)
	if err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !revoked {
		jsonErrorCode(w, errCodeServiceTokenNotFound, "service token not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
