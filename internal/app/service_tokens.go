package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	serviceScopeDeploymentsRead    = "deployments:read"
	serviceScopeDeploymentsWrite   = "deployments:write"
	serviceScopeProjectsWrite      = "projects:write"
	serviceScopeHostingAdmin       = "hosting:admin"
	serviceTokenGenerationHeader   = "X-Deployer-Credential-Generation"
	serviceTokenIfGenerationHeader = "X-Deployer-If-Credential-Generation"
)

var allowedServiceTokenScopes = map[string]struct{}{
	serviceScopeDeploymentsRead:  {},
	serviceScopeDeploymentsWrite: {},
	serviceScopeProjectsWrite:    {},
	serviceScopeHostingAdmin:     {},
}

var (
	errInvalidServiceToken            = errors.New("invalid service token")
	errServiceTokenNameExists         = errors.New("service token name already exists")
	errServiceTokenGenerationConflict = errors.New("service token credential generation changed")
)

type serviceTokenValidationError string

func (err serviceTokenValidationError) Error() string {
	return string(err)
}

type ServiceToken struct {
	ID                   int64      `json:"id"`
	Name                 string     `json:"name"`
	Scopes               []string   `json:"scopes"`
	CreatedAt            time.Time  `json:"created_at"`
	LastUsedAt           *time.Time `json:"last_used_at,omitempty"`
	RevokedAt            *time.Time `json:"revoked_at,omitempty"`
	Token                string     `json:"token,omitempty"`
	CredentialGeneration int64      `json:"credential_generation"`
	rawToken             string
	credentialID         int64
}

type serviceTokenContextKey struct{}

func createServiceToken(name string, scopes []string) (*ServiceToken, error) {
	return createServiceTokenContext(context.Background(), name, scopes, "")
}

func createServiceTokenContext(ctx context.Context, name string, scopes []string, requestID string) (*ServiceToken, error) {
	trimmedName := strings.TrimSpace(name)
	if trimmedName == "" {
		return nil, serviceTokenValidationError("name is required")
	}
	if name != trimmedName {
		return nil, serviceTokenValidationError("name must not have leading or trailing whitespace")
	}
	if utf8.RuneCountInString(name) > 100 {
		return nil, serviceTokenValidationError("name is too large")
	}
	for _, ch := range name {
		if unicode.IsControl(ch) {
			return nil, serviceTokenValidationError("name must not contain control characters")
		}
	}
	normalizedScopes, err := normalizeServiceTokenScopes(scopes)
	if err != nil {
		return nil, serviceTokenValidationError(err.Error())
	}
	scopesJSON, err := json.Marshal(normalizedScopes)
	if err != nil {
		return nil, fmt.Errorf("encode scopes: %w", err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	var existing int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM service_tokens WHERE name=?`, name).Scan(&existing); err != nil {
		return nil, err
	}
	if existing != 0 {
		return nil, errServiceTokenNameExists
	}
	now := time.Now().UTC()
	rawToken := "dpl_" + generateToken()
	res, err := conn.ExecContext(ctx, `INSERT INTO service_tokens (name, token_hash, scopes, created_at) VALUES (?, ?, ?, ?)`,
		name, hashToken(rawToken), string(scopesJSON), formatSQLiteTime(now),
	)
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	credential, err := conn.ExecContext(ctx, `INSERT INTO service_token_credentials (service_token_id, token_hash, created_at) VALUES (?, ?, ?)`, id, hashToken(rawToken), formatSQLiteTime(now))
	if err != nil {
		return nil, err
	}
	credentialID, err := credential.LastInsertId()
	if err != nil {
		return nil, err
	}
	if err := insertServiceTokenAudit(ctx, conn, id, credentialID, "created", "trusted-admin", requestID, map[string]any{"scopes": normalizedScopes}, now); err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, err
	}
	committed = true
	return &ServiceToken{
		ID:                   id,
		Name:                 name,
		Scopes:               normalizedScopes,
		CreatedAt:            now,
		Token:                rawToken,
		CredentialGeneration: credentialID,
		credentialID:         credentialID,
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
	rows, err := db.Query(`SELECT t.id, t.name, t.scopes, t.created_at, t.last_used_at, t.revoked_at,
		COALESCE((SELECT c.id FROM service_token_credentials c WHERE c.service_token_id=t.id AND c.token_hash=t.token_hash), 0)
		FROM service_tokens t ORDER BY t.id`)
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

func getServiceToken(id int64) (*ServiceToken, error) {
	row := db.QueryRow(`SELECT t.id, t.name, t.scopes, t.created_at, t.last_used_at, t.revoked_at,
		COALESCE((SELECT c.id FROM service_token_credentials c WHERE c.service_token_id=t.id AND c.token_hash=t.token_hash), 0)
		FROM service_tokens t WHERE t.id=? AND t.revoked_at IS NULL`, id)
	return scanServiceToken(row)
}

type serviceTokenScanner interface {
	Scan(dest ...interface{}) error
}

func scanServiceToken(scanner serviceTokenScanner) (*ServiceToken, error) {
	var token ServiceToken
	var scopesJSON, createdAt string
	var lastUsedAt, revokedAt sql.NullString
	if err := scanner.Scan(&token.ID, &token.Name, &scopesJSON, &createdAt, &lastUsedAt, &revokedAt, &token.CredentialGeneration); err != nil {
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
	return authenticateServiceTokenContext(context.Background(), rawToken, nil)
}

type serviceTokenAuthenticationAudit struct {
	RequestID string
	Method    string
	Path      string
}

func authenticateServiceTokenContext(ctx context.Context, rawToken string, audit *serviceTokenAuthenticationAudit) (*ServiceToken, error) {
	rawToken = strings.TrimSpace(rawToken)
	if !validServiceTokenSecret(rawToken) {
		return nil, errInvalidServiceToken
	}
	candidateAt := time.Now().UTC()
	tokenHash := hashToken(rawToken)
	// Reject unknown or inactive credentials without taking SQLite's global
	// writer reservation. The credential is revalidated after BEGIN IMMEDIATE
	// before any authoritative usage or audit state is changed.
	if _, _, err := lookupActiveServiceToken(ctx, db, tokenHash, candidateAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errInvalidServiceToken
		}
		return nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	now := time.Now().UTC()
	token, credentialID, err := lookupActiveServiceToken(ctx, conn, tokenHash, now)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errInvalidServiceToken
		}
		return nil, err
	}
	result, err := conn.ExecContext(ctx, `UPDATE service_token_credentials SET last_used_at=?
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
	if _, err := conn.ExecContext(ctx, `UPDATE service_tokens SET last_used_at=? WHERE id=? AND revoked_at IS NULL`, formatSQLiteTime(now), token.ID); err != nil {
		return nil, err
	}
	if audit != nil {
		if err := insertServiceTokenAudit(ctx, conn, token.ID, credentialID, "authenticated", "service-token", audit.RequestID, map[string]any{
			"method": serviceTokenAuditMethod(audit.Method),
			"path":   serviceTokenAuditPath(audit.Path),
		}, now); err != nil {
			return nil, err
		}
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, err
	}
	committed = true
	token.LastUsedAt = &now
	token.rawToken = rawToken
	token.credentialID = credentialID
	return token, nil
}

type serviceTokenQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func lookupActiveServiceToken(ctx context.Context, queryer serviceTokenQueryer, tokenHash string, now time.Time) (*ServiceToken, int64, error) {
	row := queryer.QueryRowContext(ctx, `SELECT t.id, t.name, t.scopes, t.created_at, t.last_used_at, t.revoked_at, c.id
		FROM service_tokens t JOIN service_token_credentials c ON c.service_token_id=t.id
		WHERE c.token_hash=? AND t.revoked_at IS NULL AND c.revoked_at IS NULL
		  AND (c.expires_at IS NULL OR c.expires_at>?)`, tokenHash, formatSQLiteTime(now))
	var credentialID int64
	token, err := scanServiceTokenWithCredential(row, &credentialID)
	return token, credentialID, err
}

func validServiceTokenSecret(rawToken string) bool {
	if len(rawToken) != len("dpl_")+48 || !strings.HasPrefix(rawToken, "dpl_") {
		return false
	}
	for _, ch := range rawToken[len("dpl_"):] {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
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
	ctx := context.Background()
	generation, err := currentServiceTokenGeneration(ctx, id)
	if err != nil {
		return nil, err
	}
	return rotateServiceTokenContext(ctx, id, generation, "")
}

func currentServiceTokenGeneration(ctx context.Context, id int64) (int64, error) {
	var generation int64
	err := db.QueryRowContext(ctx, `SELECT c.id FROM service_tokens t
		JOIN service_token_credentials c ON c.service_token_id=t.id AND c.token_hash=t.token_hash
		WHERE t.id=? AND t.revoked_at IS NULL AND c.revoked_at IS NULL`, id).Scan(&generation)
	return generation, err
}

func rotateServiceTokenContext(ctx context.Context, id, expectedGeneration int64, requestID string) (*ServiceToken, error) {
	overlap := appConfig.ServiceTokenOverlap
	if overlap < 0 {
		return nil, fmt.Errorf("service token rotation overlap must not be negative")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	var currentGeneration int64
	if err := conn.QueryRowContext(ctx, `SELECT c.id FROM service_tokens t
		JOIN service_token_credentials c ON c.service_token_id=t.id AND c.token_hash=t.token_hash
		WHERE t.id=? AND t.revoked_at IS NULL AND c.revoked_at IS NULL`, id).Scan(&currentGeneration); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, err
	}
	if currentGeneration != expectedGeneration {
		return nil, errServiceTokenGenerationConflict
	}
	now := time.Now().UTC()
	rawToken := "dpl_" + generateToken()
	expiresAt := now.Add(overlap)
	if _, err := conn.ExecContext(ctx, `UPDATE service_token_credentials SET expires_at=?
		WHERE service_token_id=? AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>?)`,
		formatSQLiteTime(expiresAt), id, formatSQLiteTime(expiresAt)); err != nil {
		return nil, err
	}
	credential, err := conn.ExecContext(ctx, `INSERT INTO service_token_credentials (service_token_id, token_hash, created_at) VALUES (?, ?, ?)`, id, hashToken(rawToken), formatSQLiteTime(now))
	if err != nil {
		return nil, err
	}
	credentialID, err := credential.LastInsertId()
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `UPDATE service_tokens SET token_hash=?, last_used_at=NULL WHERE id=?`, hashToken(rawToken), id); err != nil {
		return nil, err
	}
	if err := insertServiceTokenAudit(ctx, conn, id, credentialID, "rotated", "trusted-admin", requestID, map[string]any{"overlap_seconds": int64(overlap.Seconds())}, now); err != nil {
		return nil, err
	}
	row := conn.QueryRowContext(ctx, `SELECT t.id, t.name, t.scopes, t.created_at, t.last_used_at, t.revoked_at,
		COALESCE((SELECT c.id FROM service_token_credentials c WHERE c.service_token_id=t.id AND c.token_hash=t.token_hash), 0)
		FROM service_tokens t WHERE t.id=?`, id)
	token, err := scanServiceToken(row)
	if err != nil {
		return nil, err
	}
	token.Token = rawToken
	token.CredentialGeneration = credentialID
	token.credentialID = credentialID
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, err
	}
	committed = true
	return token, nil
}

func revokeServiceToken(id int64) (bool, error) {
	return revokeServiceTokenContext(context.Background(), id, "")
}

func revokeServiceTokenContext(ctx context.Context, id int64, requestID string) (bool, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return false, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	now := time.Now().UTC()
	res, err := conn.ExecContext(ctx, `UPDATE service_tokens SET revoked_at=? WHERE id=? AND revoked_at IS NULL`, formatSQLiteTime(now), id)
	if err != nil {
		return false, err
	}
	count, err := res.RowsAffected()
	if err != nil || count == 0 {
		return false, err
	}
	if _, err := conn.ExecContext(ctx, `UPDATE service_token_credentials SET revoked_at=? WHERE service_token_id=? AND revoked_at IS NULL`, formatSQLiteTime(now), id); err != nil {
		return false, err
	}
	if err := insertServiceTokenAudit(ctx, conn, id, 0, "revoked", "trusted-admin", requestID, nil, now); err != nil {
		return false, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return false, err
	}
	committed = true
	return true, nil
}

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func insertServiceTokenAudit(ctx context.Context, executor sqlExecutor, tokenID, credentialID int64, eventType, actor, requestID string, metadata map[string]any, now time.Time) error {
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
	_, err = executor.ExecContext(ctx, `INSERT INTO service_token_audit_events
		(service_token_id, credential_id, event_type, actor, request_id, metadata_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, tokenID, credential, eventType, actor, requestID, string(encoded), formatSQLiteTime(now))
	return err
}

func serviceTokenAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawToken, ok := parseBearerCredential(r.Header.Get("Authorization"))
		if !ok {
			w.Header().Set("WWW-Authenticate", "Bearer")
			jsonErrorCode(w, errCodeServiceTokenRequired, "service token is required", http.StatusUnauthorized)
			return
		}
		token, err := authenticateServiceTokenContext(r.Context(), rawToken, &serviceTokenAuthenticationAudit{
			RequestID: requestIDFromContext(r.Context()),
			Method:    r.Method,
			Path:      r.URL.Path,
		})
		if err != nil {
			if errors.Is(err, errInvalidServiceToken) {
				w.Header().Set("WWW-Authenticate", "Bearer")
				jsonErrorCode(w, errCodeInvalidServiceToken, "invalid service token", http.StatusUnauthorized)
				return
			}
			jsonErrorCode(w, errCodeInternal, "service token authentication failed", http.StatusInternalServerError)
			return
		}
		ctx := context.WithValue(r.Context(), serviceTokenContextKey{}, token)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func parseBearerCredential(header string) (string, bool) {
	fields := strings.Fields(header)
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") || fields[1] == "" {
		return "", false
	}
	return fields[1], true
}

func serviceTokenAuditPath(path string) string {
	const maxRunes = 512
	redacted := redactSecrets(path)
	runes := []rune(redacted)
	if len(runes) <= maxRunes {
		return redacted
	}
	return string(runes[:maxRunes])
}

func serviceTokenAuditMethod(method string) string {
	const maxRunes = 32
	redacted := redactSecrets(method)
	runes := []rune(redacted)
	if len(runes) <= maxRunes {
		return redacted
	}
	return string(runes[:maxRunes])
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
	if err := insertServiceTokenAudit(r.Context(), db, token.ID, token.credentialID, "scope_denied", "service-token", requestIDFromContext(r.Context()), map[string]any{
		"method":         serviceTokenAuditMethod(r.Method),
		"path":           serviceTokenAuditPath(r.URL.Path),
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
			jsonErrorCode(w, errCodeInternal, "list service tokens failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")
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
		token, err := createServiceTokenContext(r.Context(), payload.Name, payload.Scopes, requestIDFromContext(r.Context()))
		if err != nil {
			var validation serviceTokenValidationError
			switch {
			case errors.As(err, &validation):
				jsonErrorCode(w, errCodeValidation, validation.Error(), http.StatusBadRequest)
			case errors.Is(err, errServiceTokenNameExists):
				jsonErrorCode(w, errCodeConflict, "service token name already exists", http.StatusConflict)
			default:
				jsonErrorCode(w, errCodeInternal, "create service token failed", http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set(serviceTokenGenerationHeader, formatServiceTokenGeneration(token.CredentialGeneration))
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
		expectedGeneration, ok := parseServiceTokenGenerationPrecondition(w, r)
		if !ok {
			return
		}
		token, err := rotateServiceTokenContext(r.Context(), id, expectedGeneration, requestIDFromContext(r.Context()))
		if err != nil {
			if errors.Is(err, errServiceTokenGenerationConflict) {
				jsonErrorCode(w, errCodePreconditionFailed, "service token credential generation changed", http.StatusPreconditionFailed)
				return
			}
			if errors.Is(err, sql.ErrNoRows) {
				jsonErrorCode(w, errCodeServiceTokenNotFound, "service token not found", http.StatusNotFound)
				return
			}
			jsonErrorCode(w, errCodeInternal, "rotate service token failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set(serviceTokenGenerationHeader, formatServiceTokenGeneration(token.CredentialGeneration))
		jsonResponse(w, token)
		return
	}
	if suffix != "" {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		token, err := getServiceToken(id)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				jsonErrorCode(w, errCodeServiceTokenNotFound, "service token not found", http.StatusNotFound)
				return
			}
			jsonErrorCode(w, errCodeInternal, "read service token failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set(serviceTokenGenerationHeader, formatServiceTokenGeneration(token.CredentialGeneration))
		jsonResponse(w, token)
	case http.MethodDelete:
		revoked, err := revokeServiceTokenContext(r.Context(), id, requestIDFromContext(r.Context()))
		if err != nil {
			jsonErrorCode(w, errCodeInternal, "revoke service token failed", http.StatusInternalServerError)
			return
		}
		if !revoked {
			jsonErrorCode(w, errCodeServiceTokenNotFound, "service token not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		jsonMethodNotAllowed(w, http.MethodGet, http.MethodDelete)
	}
}

func formatServiceTokenGeneration(generation int64) string {
	return strconv.FormatInt(generation, 10)
}

func parseServiceTokenGenerationPrecondition(w http.ResponseWriter, r *http.Request) (int64, bool) {
	value := strings.TrimSpace(r.Header.Get(serviceTokenIfGenerationHeader))
	if value == "" {
		jsonErrorCode(w, errCodePreconditionRequired, serviceTokenIfGenerationHeader+" is required", http.StatusPreconditionRequired)
		return 0, false
	}
	generation, err := strconv.ParseInt(value, 10, 64)
	if err != nil || generation <= 0 {
		jsonErrorCode(w, errCodeValidation, serviceTokenIfGenerationHeader+" must be a positive integer", http.StatusBadRequest)
		return 0, false
	}
	return generation, true
}
