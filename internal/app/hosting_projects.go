package app

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	hostingManifestVersion = "v1"
	maxManifestBodyBytes   = 64 << 10
	manifestSignatureTTL   = 5 * time.Minute
)

const (
	manifestTimestampHeader = "X-Deployer-Timestamp"
	manifestSignatureHeader = "X-Deployer-Signature"
)

var (
	externalIDPattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$`)
	externalIDSecretPattern = regexp.MustCompile(`(?:(?:dpl_|htr_|wli_|ghp_|gho_|ghu_|ghs_|github_pat_|sk_live_|xox[baprs]-)[A-Za-z0-9_.-]{8,}|AKIA[A-Z0-9]{16})`)
	repositoryNamePattern   = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
	packageScriptPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:_-]{0,63}$`)
	outputDirectoryPattern  = regexp.MustCompile(`^[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*$`)
	healthPathPattern       = regexp.MustCompile(`^/(?:[A-Za-z0-9._~!$&'()*+,;=:@/-]|%[A-Fa-f0-9]{2})*$`)
)

func validHostingExternalIDSyntax(value string) bool {
	return externalIDPattern.MatchString(value)
}

func validHostingExternalIDForAdmission(value string) bool {
	return validHostingExternalIDSyntax(value) && !externalIDSecretPattern.MatchString(value)
}

type HostingProjectManifest struct {
	SchemaVersion   string                    `json:"schema_version"`
	Repository      HostingRepositoryManifest `json:"repository"`
	Runtime         HostingRuntimeManifest    `json:"runtime"`
	ResourceProfile string                    `json:"resource_profile"`
}

type HostingRepositoryManifest struct {
	InstallationID int64  `json:"installation_id"`
	RepositoryID   int64  `json:"repository_id"`
	FullName       string `json:"full_name"`
}

type HostingRuntimeManifest struct {
	Kind            string `json:"kind"`
	NodeVersion     string `json:"node_version,omitempty"`
	PackageManager  string `json:"package_manager"`
	BuildScript     string `json:"build_script,omitempty"`
	StartScript     string `json:"start_script,omitempty"`
	OutputDirectory string `json:"output_directory,omitempty"`
	Port            int    `json:"port,omitempty"`
	HealthPath      string `json:"health_path,omitempty"`
}

type hostingProjectUpsertRequest struct {
	ExternalProjectID string                 `json:"-"`
	Manifest          HostingProjectManifest `json:"manifest"`
}

type HostingProject struct {
	ID                       int64                  `json:"id"`
	ExternalProjectID        string                 `json:"external_project_id"`
	ManifestVersion          string                 `json:"manifest_version"`
	Manifest                 HostingProjectManifest `json:"manifest"`
	ManifestDigest           string                 `json:"manifest_digest"`
	RepositoryInstallationID int64                  `json:"repository_installation_id"`
	RepositoryID             int64                  `json:"repository_id"`
	RepositoryFullName       string                 `json:"repository_full_name"`
	RuntimeKind              string                 `json:"runtime_kind"`
	ResourceProfile          string                 `json:"resource_profile"`
	RepoPath                 string                 `json:"-"`
	DeployPath               string                 `json:"-"`
	RunnerID                 int64                  `json:"runner_id"`
	DesiredState             string                 `json:"desired_state"`
	KillSwitchReason         string                 `json:"kill_switch_reason,omitempty"`
	CreatedAt                time.Time              `json:"created_at"`
	UpdatedAt                time.Time              `json:"updated_at"`
}

type internalProjectResponse struct {
	ExternalProjectID string                 `json:"external_project_id"`
	ManifestDigest    string                 `json:"manifest_digest"`
	Manifest          HostingProjectManifest `json:"manifest"`
	Created           bool                   `json:"created"`
	Replayed          bool                   `json:"replayed"`
}

func decodeSignedManifestRequest(w http.ResponseWriter, r *http.Request, externalProjectID string) (*hostingProjectUpsertRequest, bool) {
	token, _ := r.Context().Value(serviceTokenContextKey{}).(*ServiceToken)
	if token == nil || token.rawToken == "" {
		jsonErrorCode(w, errCodeServiceTokenRequired, "service token is required", http.StatusUnauthorized)
		return nil, false
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		jsonErrorCode(w, errCodeUnsupportedMediaType, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return nil, false
	}

	timestampText := strings.TrimSpace(r.Header.Get(manifestTimestampHeader))
	signatureText := strings.TrimSpace(r.Header.Get(manifestSignatureHeader))
	if timestampText == "" || signatureText == "" {
		jsonErrorCode(w, errCodeManifestSignatureRequired, "signed manifest headers are required", http.StatusUnauthorized)
		return nil, false
	}
	timestampUnix, err := strconv.ParseInt(timestampText, 10, 64)
	if err != nil {
		jsonErrorCode(w, errCodeInvalidManifestSignature, "manifest timestamp is invalid", http.StatusUnauthorized)
		return nil, false
	}
	signedAt := time.Unix(timestampUnix, 0)
	if delta := time.Since(signedAt); delta > manifestSignatureTTL || delta < -manifestSignatureTTL {
		jsonErrorCode(w, errCodeManifestSignatureExpired, "manifest signature timestamp is outside the allowed window", http.StatusUnauthorized)
		return nil, false
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxManifestBodyBytes))
	if err != nil {
		jsonErrorCode(w, errCodePayloadTooLarge, "manifest body exceeds 64 KiB", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	expectedMAC := hmac.New(sha256.New, []byte(token.rawToken))
	expectedMAC.Write([]byte(timestampText))
	expectedMAC.Write([]byte("."))
	expectedMAC.Write(body)
	provided, err := decodeManifestSignature(signatureText)
	if err != nil || !hmac.Equal(provided, expectedMAC.Sum(nil)) {
		jsonErrorCode(w, errCodeInvalidManifestSignature, "manifest signature is invalid", http.StatusUnauthorized)
		return nil, false
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var payload hostingProjectUpsertRequest
	if err := decoder.Decode(&payload); err != nil {
		jsonErrorCode(w, errCodeInvalidManifest, "manifest body is invalid: "+err.Error(), http.StatusBadRequest)
		return nil, false
	}
	if err := ensureJSONEOF(decoder); err != nil {
		jsonErrorCode(w, errCodeInvalidManifest, err.Error(), http.StatusBadRequest)
		return nil, false
	}
	payload.ExternalProjectID = externalProjectID
	if err := validateHostingProjectRequest(&payload); err != nil {
		jsonErrorCode(w, errCodeInvalidManifest, err.Error(), http.StatusBadRequest)
		return nil, false
	}
	return &payload, true
}

func decodeManifestSignature(value string) ([]byte, error) {
	encoded, ok := strings.CutPrefix(value, "sha256=")
	if !ok {
		return nil, fmt.Errorf("signature must use sha256")
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != sha256.Size {
		return nil, fmt.Errorf("signature is invalid")
	}
	return decoded, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("manifest body must contain exactly one JSON object")
	}
	return nil
}

func validateHostingProjectRequest(payload *hostingProjectUpsertRequest) error {
	payload.ExternalProjectID = strings.TrimSpace(payload.ExternalProjectID)
	if !validHostingExternalIDForAdmission(payload.ExternalProjectID) {
		return fmt.Errorf("external_project_id must contain 8-128 safe characters and must not match a credential format")
	}
	manifest := &payload.Manifest
	manifest.Repository.FullName = strings.TrimSpace(manifest.Repository.FullName)
	manifest.Runtime.Kind = strings.TrimSpace(manifest.Runtime.Kind)
	manifest.Runtime.NodeVersion = strings.TrimSpace(manifest.Runtime.NodeVersion)
	manifest.Runtime.PackageManager = strings.TrimSpace(manifest.Runtime.PackageManager)
	manifest.Runtime.BuildScript = strings.TrimSpace(manifest.Runtime.BuildScript)
	manifest.Runtime.StartScript = strings.TrimSpace(manifest.Runtime.StartScript)
	manifest.Runtime.OutputDirectory = strings.TrimSpace(manifest.Runtime.OutputDirectory)
	manifest.Runtime.HealthPath = strings.TrimSpace(manifest.Runtime.HealthPath)

	if manifest.SchemaVersion != hostingManifestVersion {
		return fmt.Errorf("manifest schema_version must be %q", hostingManifestVersion)
	}
	if manifest.Repository.InstallationID <= 0 || manifest.Repository.RepositoryID <= 0 {
		return fmt.Errorf("repository installation_id and repository_id must be positive")
	}
	if !repositoryNamePattern.MatchString(manifest.Repository.FullName) {
		return fmt.Errorf("repository full_name must be in owner/repository form")
	}
	if manifest.ResourceProfile != "starter" && manifest.ResourceProfile != "standard" {
		return fmt.Errorf("resource_profile must be starter or standard")
	}
	return validateHostingRuntimeManifest(&manifest.Runtime)
}

func validateHostingRuntimeManifest(runtime *HostingRuntimeManifest) error {
	if runtime == nil {
		return fmt.Errorf("runtime is required")
	}
	runtime.Kind = strings.TrimSpace(runtime.Kind)
	runtime.NodeVersion = strings.TrimSpace(runtime.NodeVersion)
	runtime.PackageManager = strings.TrimSpace(runtime.PackageManager)
	runtime.BuildScript = strings.TrimSpace(runtime.BuildScript)
	runtime.StartScript = strings.TrimSpace(runtime.StartScript)
	runtime.OutputDirectory = strings.TrimSpace(runtime.OutputDirectory)
	runtime.HealthPath = strings.TrimSpace(runtime.HealthPath)
	if runtime.Kind != "static" && runtime.Kind != "node" {
		return fmt.Errorf("runtime kind must be static or node")
	}
	if runtime.NodeVersion != "20" && runtime.NodeVersion != "22" {
		return fmt.Errorf("node_version must be 20 or 22")
	}
	if runtime.PackageManager != "npm" && runtime.PackageManager != "pnpm" && runtime.PackageManager != "yarn" {
		return fmt.Errorf("package_manager must be npm, pnpm, or yarn")
	}
	if runtime.BuildScript != "" && !packageScriptPattern.MatchString(runtime.BuildScript) {
		return fmt.Errorf("build_script must be a package.json script name, not a shell command")
	}

	switch runtime.Kind {
	case "static":
		if runtime.BuildScript == "" {
			return fmt.Errorf("static runtime requires build_script")
		}
		if err := validateHostingOutputDirectory(runtime.OutputDirectory); err != nil {
			return err
		}
		if runtime.StartScript != "" || runtime.Port != 0 || runtime.HealthPath != "" {
			return fmt.Errorf("static runtime must not define start_script, port, or health_path")
		}
	case "node":
		if !packageScriptPattern.MatchString(runtime.StartScript) {
			return fmt.Errorf("node runtime requires a package.json start_script name")
		}
		if runtime.OutputDirectory != "" {
			return fmt.Errorf("node runtime must not define output_directory")
		}
		if runtime.Port < 1024 || runtime.Port > 65535 {
			return fmt.Errorf("node runtime port must be between 1024 and 65535")
		}
		if err := validateHealthPath(runtime.HealthPath); err != nil {
			return err
		}
	}
	return nil
}

func validateHostingOutputDirectory(value string) error {
	if len(value) > 256 || !outputDirectoryPattern.MatchString(value) {
		return fmt.Errorf("output_directory must contain only safe relative path segments")
	}
	clean, err := cleanRelativeDeployPath(value)
	if err != nil {
		return fmt.Errorf("output_directory is unsafe: %w", err)
	}
	if clean != value {
		return fmt.Errorf("output_directory must be a canonical relative path")
	}
	return nil
}

func validateHealthPath(value string) error {
	if value == "" || strings.HasPrefix(value, "//") || !healthPathPattern.MatchString(value) {
		return fmt.Errorf("health_path must be an absolute HTTP path")
	}
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("health_path must be an absolute HTTP path without query or fragment")
	}
	return nil
}

func manifestJSONAndDigest(manifest HostingProjectManifest) (string, string, error) {
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return "", "", fmt.Errorf("encode hosting manifest: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return string(encoded), "sha256:" + hex.EncodeToString(digest[:]), nil
}

func hostingProjectFromManifest(externalProjectID string, manifest HostingProjectManifest) (*HostingProject, error) {
	_, digest, err := manifestJSONAndDigest(manifest)
	if err != nil {
		return nil, err
	}
	deployPath, err := deriveHostingPath(appConfig.HostingDeployRoot, externalProjectID)
	if err != nil {
		return nil, fmt.Errorf("derive hosting deploy path: %w", err)
	}
	project := &HostingProject{
		ExternalProjectID:        externalProjectID,
		ManifestVersion:          manifest.SchemaVersion,
		Manifest:                 manifest,
		ManifestDigest:           digest,
		RepositoryInstallationID: manifest.Repository.InstallationID,
		RepositoryID:             manifest.Repository.RepositoryID,
		RepositoryFullName:       manifest.Repository.FullName,
		RuntimeKind:              manifest.Runtime.Kind,
		ResourceProfile:          manifest.ResourceProfile,
		RepoPath:                 "",
		DeployPath:               deployPath,
	}
	return project, nil
}

func deriveHostingPath(root, externalProjectID string) (string, error) {
	root = filepath.Clean(strings.TrimSpace(root))
	if !filepath.IsAbs(root) || root == string(filepath.Separator) {
		return "", fmt.Errorf("hosting root must be an absolute non-root path")
	}
	path := filepath.Join(root, externalProjectID)
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("derived path escapes hosting root")
	}
	return path, nil
}

func upsertHostingProject(ctx context.Context, externalProjectID string, manifest HostingProjectManifest) (*HostingProject, bool, bool, error) {
	return upsertHostingProjectAudited(ctx, externalProjectID, manifest, nil, "")
}

func upsertHostingProjectAudited(ctx context.Context, externalProjectID string, manifest HostingProjectManifest, token *ServiceToken, requestID string) (*HostingProject, bool, bool, error) {
	payload := hostingProjectUpsertRequest{ExternalProjectID: externalProjectID, Manifest: manifest}
	if err := validateHostingProjectRequest(&payload); err != nil {
		return nil, false, false, err
	}
	externalProjectID = payload.ExternalProjectID
	manifest = payload.Manifest
	project, err := hostingProjectFromManifest(externalProjectID, manifest)
	if err != nil {
		return nil, false, false, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, false, false, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, false, false, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()

	var existingID int64
	var existingDigest string
	err = conn.QueryRowContext(ctx, `SELECT id, manifest_digest FROM hosting_projects WHERE external_project_id=?`, externalProjectID).Scan(&existingID, &existingDigest)
	created := false
	replayed := false
	switch {
	case err == sql.ErrNoRows:
		created = true
		now := time.Now().UTC()
		manifestJSON, _, _ := manifestJSONAndDigest(project.Manifest)
		res, insertErr := conn.ExecContext(ctx, `INSERT INTO hosting_projects (external_project_id, manifest_version, manifest_json, manifest_digest, repository_installation_id, repository_id, repository_full_name, runtime_kind, resource_profile, repo_path, deploy_path, runner_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			project.ExternalProjectID, project.ManifestVersion, manifestJSON, project.ManifestDigest, project.RepositoryInstallationID, project.RepositoryID, project.RepositoryFullName, project.RuntimeKind, project.ResourceProfile, project.RepoPath, project.DeployPath, project.RunnerID, formatSQLiteTime(now), formatSQLiteTime(now),
		)
		if insertErr != nil {
			return nil, false, false, insertErr
		}
		project.ID, err = res.LastInsertId()
		if err != nil {
			return nil, false, false, err
		}
	case err != nil:
		return nil, false, false, err
	case existingDigest == project.ManifestDigest:
		project.ID = existingID
		replayed = true
	default:
		project.ID = existingID
		manifestJSON, _, _ := manifestJSONAndDigest(project.Manifest)
		_, err = conn.ExecContext(ctx, `UPDATE hosting_projects SET manifest_version=?, manifest_json=?, manifest_digest=?, repository_installation_id=?, repository_id=?, repository_full_name=?, runtime_kind=?, resource_profile=?, deploy_path=?, runner_id=?, updated_at=? WHERE id=?`,
			project.ManifestVersion, manifestJSON, project.ManifestDigest, project.RepositoryInstallationID, project.RepositoryID, project.RepositoryFullName, project.RuntimeKind, project.ResourceProfile, project.DeployPath, project.RunnerID, formatSQLiteTime(time.Now()), project.ID,
		)
		if err != nil {
			return nil, false, false, err
		}
	}
	if token != nil && !replayed {
		eventType := "hosting_project_updated"
		if created {
			eventType = "hosting_project_created"
		}
		metadata, _ := json.Marshal(map[string]any{"external_project_id": externalProjectID, "manifest_digest": project.ManifestDigest})
		if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_audit_events
			(issuer_token_id, hosting_project_id, event_type, reason, request_id, metadata_json, created_at)
			VALUES (?, ?, ?, 'control-plane manifest upsert', ?, ?, ?)`, token.ID, project.ID, eventType,
			requestID, string(metadata), formatSQLiteTime(time.Now().UTC())); err != nil {
			return nil, false, false, err
		}
	}
	stored, err := getHostingProjectByIDOn(ctx, conn, project.ID)
	if err != nil {
		return nil, false, false, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, false, false, err
	}
	committed = true
	return stored, created, replayed, nil
}

func getHostingProjectByExternalID(externalProjectID string) (*HostingProject, error) {
	var id int64
	if err := db.QueryRow(`SELECT id FROM hosting_projects WHERE external_project_id=?`, externalProjectID).Scan(&id); err != nil {
		return nil, err
	}
	return getHostingProjectByID(id)
}

func getHostingProjectByID(id int64) (*HostingProject, error) {
	return getHostingProjectByIDOn(context.Background(), db, id)
}

type hostingProjectQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getHostingProjectByIDOn(ctx context.Context, querier hostingProjectQuerier, id int64) (*HostingProject, error) {
	var project HostingProject
	var manifestJSON, createdAt, updatedAt string
	err := querier.QueryRowContext(ctx, `SELECT id, external_project_id, manifest_version, manifest_json, manifest_digest, repository_installation_id, repository_id, repository_full_name, runtime_kind, resource_profile, repo_path, deploy_path, runner_id, desired_state, kill_switch_reason, created_at, updated_at FROM hosting_projects WHERE id=?`, id).Scan(
		&project.ID, &project.ExternalProjectID, &project.ManifestVersion, &manifestJSON, &project.ManifestDigest, &project.RepositoryInstallationID, &project.RepositoryID, &project.RepositoryFullName, &project.RuntimeKind, &project.ResourceProfile, &project.RepoPath, &project.DeployPath, &project.RunnerID, &project.DesiredState, &project.KillSwitchReason, &createdAt, &updatedAt,
	)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(manifestJSON), &project.Manifest); err != nil {
		return nil, fmt.Errorf("decode stored hosting manifest: %w", err)
	}
	project.CreatedAt = parseSQLiteTime(createdAt)
	project.UpdatedAt = parseSQLiteTime(updatedAt)
	return &project, nil
}
