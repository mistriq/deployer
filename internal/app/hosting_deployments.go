package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	hostingRunnerProtocolVersion      = "v1"
	hostingRunnerSecretOperation      = "runtime-secrets-v1"
	hostingRunnerInventoryOperation   = "runtime-inventory-v1"
	hostingMaxRuntimeInventoryEntries = 1024
	hostingIdempotencyTTL             = 24 * time.Hour
	hostingRunnerStaleAfter           = 60 * time.Second
	hostingRunnerSessionTakeoverAfter = 45 * time.Second
	hostingAgentJobHeartbeatInterval  = 10 * time.Second
)

var hostingDeploymentAdmissionLocks [64]sync.Mutex
var hostingProjectAdmissionLocks [64]sync.Mutex
var hostingRuntimeInventorySchedulingLimit = 1000

type HostingSecretReference struct {
	Provider  string    `json:"provider"`
	Reference string    `json:"reference"`
	Name      string    `json:"name,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
}

type HostingSourceReference struct {
	Provider  string    `json:"provider"`
	Reference string    `json:"reference"`
	ExpiresAt time.Time `json:"expires_at"`
}

type hostingDeploymentCreateRequest struct {
	ExternalDeploymentID string                   `json:"external_deployment_id"`
	CommitSHA            string                   `json:"commit_sha"`
	ManifestDigest       string                   `json:"manifest_digest"`
	ArtifactDigest       string                   `json:"artifact_digest"`
	SourceReference      HostingSourceReference   `json:"source_reference"`
	SecretReferences     []HostingSecretReference `json:"secret_references,omitempty"`
}

type hostingWorkloadLimits struct {
	CPUMillis int64 `json:"cpu_millis"`
	RAMBytes  int64 `json:"ram_bytes"`
	DiskBytes int64 `json:"disk_bytes"`
	PIDs      int64 `json:"pids"`
}

type hostingJobRecipe struct {
	Operation             string                     `json:"operation"`
	SchemaVersion         string                     `json:"schema_version"`
	ExternalProjectID     string                     `json:"external_project_id"`
	ExternalDeploymentID  string                     `json:"external_deployment_id"`
	Repository            *HostingRepositoryManifest `json:"repository,omitempty"`
	CommitSHA             string                     `json:"commit_sha,omitempty"`
	ManifestDigest        string                     `json:"manifest_digest,omitempty"`
	ArtifactDigest        string                     `json:"artifact_digest,omitempty"`
	Runtime               HostingRuntimeManifest     `json:"runtime"`
	Limits                hostingWorkloadLimits      `json:"limits"`
	SourceArtifactURL     string                     `json:"source_artifact_url,omitempty"`
	ReleaseDigest         string                     `json:"release_digest,omitempty"`
	ReleaseArtifactDigest string                     `json:"release_artifact_digest,omitempty"`
	ReleaseArtifactURL    string                     `json:"release_artifact_url,omitempty"`
}

type hostingCreateResult struct {
	Deployment   *HostingDeployment
	ResponseBody []byte
	StatusCode   int
	Replayed     bool
}

type hostingAPIError struct {
	Code       string
	Message    string
	StatusCode int
	Err        error
}

func (e *hostingAPIError) Error() string {
	if e.Err != nil {
		return e.Message + ": " + e.Err.Error()
	}
	return e.Message
}

func (e *hostingAPIError) Unwrap() error { return e.Err }

func validateHostingDeploymentRequestShape(request *hostingDeploymentCreateRequest) error {
	request.ExternalDeploymentID = strings.TrimSpace(request.ExternalDeploymentID)
	request.CommitSHA = strings.ToLower(strings.TrimSpace(request.CommitSHA))
	request.ManifestDigest = strings.ToLower(strings.TrimSpace(request.ManifestDigest))
	request.ArtifactDigest = strings.ToLower(strings.TrimSpace(request.ArtifactDigest))
	request.SourceReference.Provider = strings.TrimSpace(request.SourceReference.Provider)
	request.SourceReference.Reference = strings.TrimSpace(request.SourceReference.Reference)
	if !externalIDPattern.MatchString(request.ExternalDeploymentID) {
		return fmt.Errorf("external_deployment_id must contain 8-128 safe characters")
	}
	if len(request.CommitSHA) != 40 || !isLowerHex(request.CommitSHA) {
		return fmt.Errorf("commit_sha must be a full 40-character lowercase hexadecimal SHA")
	}
	if !validSHA256Digest(request.ManifestDigest) {
		return fmt.Errorf("manifest_digest must be a sha256 digest")
	}
	if !validSHA256Digest(request.ArtifactDigest) {
		return fmt.Errorf("artifact_digest must be a sha256 digest")
	}
	if request.SourceReference.Provider != "control-plane" {
		return fmt.Errorf("source_reference uses an unsupported provider")
	}
	if len(request.SourceReference.Reference) < 8 || len(request.SourceReference.Reference) > 256 || !safeSourceReference(request.SourceReference.Reference) {
		return fmt.Errorf("source_reference is invalid")
	}
	if len(request.SecretReferences) > 64 {
		return fmt.Errorf("at most 64 secret references are allowed")
	}
	seenSecretNames := make(map[string]struct{}, len(request.SecretReferences))
	for i := range request.SecretReferences {
		secret := &request.SecretReferences[i]
		secret.Provider = strings.TrimSpace(secret.Provider)
		secret.Reference = strings.TrimSpace(secret.Reference)
		secret.Name = strings.TrimSpace(secret.Name)
		if secret.Provider != "control-plane" {
			return fmt.Errorf("secret reference %d uses an unsupported provider", i)
		}
		if len(secret.Reference) < 8 || len(secret.Reference) > 256 || !safeOpaqueReference(secret.Reference) {
			return fmt.Errorf("secret reference %d is invalid", i)
		}
		if secret.Name != "" && !validHostingSecretName(secret.Name) {
			return fmt.Errorf("secret reference %d name is invalid", i)
		}
		if _, duplicate := seenSecretNames[secret.Name]; secret.Name != "" && duplicate {
			return fmt.Errorf("secret reference %d duplicates name %q", i, secret.Name)
		}
		if secret.Name != "" {
			seenSecretNames[secret.Name] = struct{}{}
		}
	}
	return nil
}

func normalizeHostingSecretReferences(refs []HostingSecretReference) error {
	seen := make(map[string]struct{}, len(refs))
	for index := range refs {
		if refs[index].Name == "" {
			refs[index].Name = fmt.Sprintf("SECRET_%d", index+1)
		}
		if !validHostingSecretName(refs[index].Name) {
			return fmt.Errorf("secret reference %d name is invalid", index)
		}
		if _, duplicate := seen[refs[index].Name]; duplicate {
			return fmt.Errorf("secret reference %d duplicates name %q", index, refs[index].Name)
		}
		seen[refs[index].Name] = struct{}{}
	}
	return nil
}

func validateHostingDeploymentReferenceExpiry(request *hostingDeploymentCreateRequest, now time.Time) error {
	now = now.UTC()
	if !request.SourceReference.ExpiresAt.After(now) || request.SourceReference.ExpiresAt.After(now.Add(time.Hour)) {
		return fmt.Errorf("source_reference must expire within one hour")
	}
	for i := range request.SecretReferences {
		if !request.SecretReferences[i].ExpiresAt.After(now) || request.SecretReferences[i].ExpiresAt.After(now.Add(time.Hour)) {
			return fmt.Errorf("secret reference %d must expire within one hour", i)
		}
	}
	return nil
}

func validateHostingDeploymentRequest(request *hostingDeploymentCreateRequest) error {
	if err := validateHostingDeploymentRequestShape(request); err != nil {
		return err
	}
	return validateHostingDeploymentReferenceExpiry(request, time.Now())
}

func isLowerHex(value string) bool {
	for _, char := range value {
		if (char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') {
			continue
		}
		return false
	}
	return value != ""
}

func validSHA256Digest(value string) bool {
	return len(value) == len("sha256:")+sha256.Size*2 && strings.HasPrefix(value, "sha256:") && isLowerHex(strings.TrimPrefix(value, "sha256:"))
}

func safeOpaqueReference(value string) bool {
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || strings.ContainsRune("._:/-", char) {
			continue
		}
		return false
	}
	return true
}

func safeSourceReference(value string) bool {
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || strings.ContainsRune("._-", char) {
			continue
		}
		return false
	}
	return value != ""
}

func hostingLimitsForProfile(profile string) (hostingWorkloadLimits, error) {
	switch profile {
	case "starter":
		return hostingWorkloadLimits{CPUMillis: 500, RAMBytes: 512 << 20, DiskBytes: 2 << 30, PIDs: 128}, nil
	case "standard":
		return hostingWorkloadLimits{CPUMillis: 1000, RAMBytes: 1 << 30, DiskBytes: 5 << 30, PIDs: 256}, nil
	default:
		return hostingWorkloadLimits{}, fmt.Errorf("unsupported resource profile %q", profile)
	}
}

func createHostingDeployment(ctx context.Context, token *ServiceToken, externalProjectID, idempotencyKey string, request hostingDeploymentCreateRequest) (*hostingCreateResult, error) {
	if token == nil {
		return nil, &hostingAPIError{Code: errCodeServiceTokenRequired, Message: "service token is required", StatusCode: 401}
	}
	if !validIdempotencyKey(idempotencyKey) {
		return nil, &hostingAPIError{Code: errCodeInvalidIdempotencyKey, Message: "Idempotency-Key must contain 8-128 safe characters", StatusCode: 400}
	}
	if !externalIDPattern.MatchString(externalProjectID) {
		return nil, &hostingAPIError{Code: errCodeProjectNotFound, Message: "project not found", StatusCode: 404}
	}
	request.SecretReferences = append([]HostingSecretReference(nil), request.SecretReferences...)
	if err := validateHostingDeploymentRequestShape(&request); err != nil {
		return nil, &hostingAPIError{Code: errCodeInvalidDeployment, Message: err.Error(), StatusCode: 400}
	}
	legacyRequest := request
	legacyRequest.SecretReferences = append([]HostingSecretReference(nil), request.SecretReferences...)
	legacyHashText, err := hashHostingDeploymentRequest(externalProjectID, legacyRequest)
	if err != nil {
		return nil, err
	}
	if err := normalizeHostingSecretReferences(request.SecretReferences); err != nil {
		return nil, &hostingAPIError{Code: errCodeInvalidDeployment, Message: err.Error(), StatusCode: 400}
	}
	request.SourceReference.ExpiresAt = request.SourceReference.ExpiresAt.UTC()
	for index := range request.SecretReferences {
		request.SecretReferences[index].ExpiresAt = request.SecretReferences[index].ExpiresAt.UTC()
	}
	requestHashText, err := hashHostingDeploymentRequest(externalProjectID, request)
	if err != nil {
		return nil, err
	}
	operation := "hosting.deployment.create"
	idempotencyLockHash := sha256.Sum256([]byte(fmt.Sprintf("%d\n%s\n%s", token.ID, operation, idempotencyKey)))
	admissionLock := &hostingDeploymentAdmissionLocks[int(idempotencyLockHash[0])%len(hostingDeploymentAdmissionLocks)]
	admissionLock.Lock()
	defer admissionLock.Unlock()
	projectLockHash := sha256.Sum256([]byte(externalProjectID))
	projectLock := &hostingProjectAdmissionLocks[int(projectLockHash[0])%len(hostingProjectAdmissionLocks)]
	projectLock.Lock()
	defer projectLock.Unlock()
	if replay, err := loadIdempotentResponse(ctx, token.ID, operation, idempotencyKey, requestHashText, legacyHashText); err != nil || replay != nil {
		if err != nil {
			return nil, err
		}
		var deployment HostingDeployment
		if err := json.Unmarshal(replay.Body, &deployment); err != nil {
			return nil, err
		}
		return &hostingCreateResult{Deployment: &deployment, ResponseBody: hostingDeploymentWireResponse(replay.Body), StatusCode: replay.StatusCode, Replayed: true}, nil
	}
	if err := validateHostingDeploymentReferenceExpiry(&request, time.Now()); err != nil {
		return nil, &hostingAPIError{Code: errCodeInvalidDeployment, Message: err.Error(), StatusCode: 400}
	}
	preflightProject, err := getHostingProjectByExternalID(externalProjectID)
	if err == sql.ErrNoRows {
		return nil, &hostingAPIError{Code: errCodeProjectNotFound, Message: "project not found", StatusCode: 404}
	}
	if err != nil {
		return nil, err
	}
	if preflightProject.ManifestDigest != request.ManifestDigest {
		return nil, &hostingAPIError{Code: errCodeManifestDigestMismatch, Message: "manifest_digest does not match the provisioned project", StatusCode: 409}
	}
	if len(request.SecretReferences) > 0 && preflightProject.RuntimeKind != "node" {
		return nil, &hostingAPIError{Code: errCodeInvalidDeployment, Message: "secret references require a node runtime", StatusCode: 400}
	}
	if len(request.SecretReferences) > 0 && len(appConfig.HostingWorkloadIdentitySecret) < 32 {
		return nil, &hostingAPIError{Code: errCodeSecretReferenceUnavailable, Message: "workload identity signing is not configured", StatusCode: http.StatusServiceUnavailable}
	}
	if err := preflightHostingDeploymentAdmission(ctx, preflightProject, request.ExternalDeploymentID, len(request.SecretReferences) > 0); err != nil {
		return nil, err
	}
	artifactLifecycleMu.RLock()
	defer artifactLifecycleMu.RUnlock()
	sourceArtifactPath, sourceDigest, err := prepareHostingSource(ctx, preflightProject, request.SourceReference, request.CommitSHA, request.ArtifactDigest)
	if err != nil {
		if errors.Is(err, errHostingSourceArtifactMismatch) {
			return nil, &hostingAPIError{Code: errCodeArtifactDigestMismatch, Message: "artifact_digest does not match the exact source artifact", StatusCode: 409, Err: err}
		}
		if errors.Is(err, errHostingSourceBrokerUnavailable) {
			return nil, &hostingAPIError{Code: errCodeSourceFetchFailed, Message: "source broker is temporarily unavailable", StatusCode: 503, Err: err}
		}
		return nil, &hostingAPIError{Code: errCodeSourceFetchFailed, Message: "exact source commit could not be prepared", StatusCode: 422, Err: err}
	}
	if sourceDigest != request.ArtifactDigest {
		return nil, &hostingAPIError{Code: errCodeArtifactDigestMismatch, Message: "artifact_digest does not match the exact source artifact", StatusCode: 409}
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

	if replay, err := findHostingIdempotencyReplay(ctx, conn, token.ID, operation, idempotencyKey, requestHashText, legacyHashText); err != nil || replay != nil {
		if err != nil {
			return nil, err
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return nil, err
		}
		committed = true
		return replay, nil
	}

	project, err := getHostingProjectOnConn(ctx, conn, externalProjectID)
	if err == sql.ErrNoRows {
		return nil, &hostingAPIError{Code: errCodeProjectNotFound, Message: "project not found", StatusCode: 404}
	}
	if err != nil {
		return nil, err
	}
	if project.DesiredState != "active" {
		return nil, &hostingAPIError{Code: errCodeProjectSuspended, Message: "project is suspended", StatusCode: 409}
	}
	if project.KillSwitchReason != "" {
		return nil, &hostingAPIError{Code: errCodeExecutionDisabled, Message: "project execution is disabled", StatusCode: 423}
	}
	var globalDisabled int
	var globalReason string
	if err := conn.QueryRowContext(ctx, `SELECT global_kill_switch, kill_switch_reason FROM hosting_settings WHERE id=1`).Scan(&globalDisabled, &globalReason); err != nil {
		return nil, err
	}
	if globalDisabled != 0 {
		return nil, &hostingAPIError{Code: errCodeExecutionDisabled, Message: "hosting execution is disabled", StatusCode: 423}
	}
	if request.ManifestDigest != project.ManifestDigest {
		return nil, &hostingAPIError{Code: errCodeManifestDigestMismatch, Message: "manifest_digest does not match the provisioned project", StatusCode: 409}
	}

	limits, err := hostingLimitsForProfile(project.ResourceProfile)
	if err != nil {
		return nil, err
	}
	runnerID, err := reserveHostingRunner(ctx, conn, project, limits, "build", len(request.SecretReferences) > 0, 0)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	result, err := conn.ExecContext(ctx, `INSERT INTO hosting_deployments
		(hosting_project_id, external_deployment_id, commit_sha, manifest_digest, artifact_digest,
		 status, issuer_token_id, phase, callback_state, request_hash, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, 'queued', ?, 'queued', 'pending', ?, ?, ?)`,
		project.ID, request.ExternalDeploymentID, request.CommitSHA, request.ManifestDigest,
		request.ArtifactDigest, token.ID, requestHashText, formatSQLiteTime(now), formatSQLiteTime(now))
	if err != nil {
		if isSQLiteUniqueConstraint(err) {
			return nil, &hostingAPIError{Code: errCodeExternalDeploymentConflict, Message: "deployment identity or active project work conflicts", StatusCode: 409, Err: err}
		}
		return nil, err
	}
	deploymentID, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	recipe := hostingJobRecipe{
		Operation:            "build",
		SchemaVersion:        hostingRunnerProtocolVersion,
		ExternalProjectID:    project.ExternalProjectID,
		ExternalDeploymentID: request.ExternalDeploymentID,
		Repository:           &project.Manifest.Repository,
		CommitSHA:            request.CommitSHA,
		ManifestDigest:       request.ManifestDigest,
		ArtifactDigest:       request.ArtifactDigest,
		Runtime:              project.Manifest.Runtime,
		Limits:               limits,
	}
	recipeJSON, err := json.Marshal(recipe)
	if err != nil {
		return nil, err
	}
	secretJSON, err := json.Marshal(request.SecretReferences)
	if err != nil {
		return nil, err
	}
	jobResult, err := conn.ExecContext(ctx, `INSERT INTO hosting_jobs
		(hosting_deployment_id, hosting_runner_id, status, recipe_json, secret_refs_json,
		 source_artifact_path, required_cpu_millis, required_ram_bytes, required_disk_bytes, required_pids, created_at)
		VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?)`, deploymentID, runnerID, string(recipeJSON), string(secretJSON), sourceArtifactPath,
		limits.CPUMillis, limits.RAMBytes, limits.DiskBytes, limits.PIDs, formatSQLiteTime(now))
	if err != nil {
		return nil, err
	}
	jobID, err := jobResult.LastInsertId()
	if err != nil {
		return nil, err
	}
	recipe.SourceArtifactURL = "/api/hosting-agent/v1/jobs/" + fmt.Sprint(jobID) + "/source"
	recipeJSON, err = json.Marshal(recipe)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_jobs SET recipe_json=? WHERE id=?`, string(recipeJSON), jobID); err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_events
		(hosting_project_id, hosting_deployment_id, event_type, phase, metadata_json, created_at)
		VALUES (?, ?, 'deployment_created', 'queued', '{}', ?)`, project.ID, deploymentID, formatSQLiteTime(now)); err != nil {
		return nil, err
	}
	auditMetadata, _ := json.Marshal(map[string]any{
		"external_deployment_id": request.ExternalDeploymentID,
		"commit_sha":             request.CommitSHA,
		"artifact_digest":        request.ArtifactDigest,
	})
	if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_audit_events
		(issuer_token_id, hosting_project_id, event_type, reason, request_id, metadata_json, created_at)
		VALUES (?, ?, 'hosting_deployment_created', 'control-plane deployment request', ?, ?, ?)`, token.ID,
		project.ID, requestIDFromContext(ctx), redactSecrets(string(auditMetadata)), formatSQLiteTime(now)); err != nil {
		return nil, err
	}

	deployment := &HostingDeployment{
		ID:                   deploymentID,
		ExternalDeploymentID: request.ExternalDeploymentID,
		ExternalProjectID:    project.ExternalProjectID,
		CommitSHA:            request.CommitSHA,
		ManifestDigest:       request.ManifestDigest,
		ArtifactDigest:       request.ArtifactDigest,
		Status:               hostingStatusQueued,
		Phase:                hostingPhaseQueued,
		CallbackState:        "pending",
		CreatedAt:            now,
		UpdatedAt:            now,
		LogReference:         "/api/internal/v1/deployments/" + request.ExternalDeploymentID + "/logs",
		EventReference:       "/api/internal/v1/deployments/" + request.ExternalDeploymentID + "/events",
	}
	responseBody, err := json.Marshal(deployment)
	if err != nil {
		return nil, err
	}
	responseBody = hostingDeploymentWireResponse(responseBody)
	if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_idempotency
		(issuer_token_id, operation, idempotency_key, request_hash, response_status, response_body, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, token.ID, operation, idempotencyKey, requestHashText, 202,
		string(responseBody), formatSQLiteTime(now), formatSQLiteTime(now.Add(hostingIdempotencyTTL))); err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, err
	}
	committed = true
	return &hostingCreateResult{Deployment: deployment, ResponseBody: responseBody, StatusCode: 202}, nil
}

func hashHostingDeploymentRequest(externalProjectID string, request hostingDeploymentCreateRequest) (string, error) {
	requestBody, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	requestHash := sha256.Sum256(append([]byte(externalProjectID+"\n"), requestBody...))
	return "sha256:" + hex.EncodeToString(requestHash[:]), nil
}

func preflightHostingDeploymentAdmission(ctx context.Context, project *HostingProject, externalDeploymentID string, requireSecrets bool) error {
	if project.DesiredState != "active" {
		return &hostingAPIError{Code: errCodeProjectSuspended, Message: "project is suspended", StatusCode: 409}
	}
	if project.KillSwitchReason != "" {
		return &hostingAPIError{Code: errCodeExecutionDisabled, Message: "project execution is disabled", StatusCode: 423}
	}
	var globalDisabled int
	if err := db.QueryRowContext(ctx, `SELECT global_kill_switch FROM hosting_settings WHERE id=1`).Scan(&globalDisabled); err != nil {
		return err
	}
	if globalDisabled != 0 {
		return &hostingAPIError{Code: errCodeExecutionDisabled, Message: "hosting execution is disabled", StatusCode: 423}
	}
	var existingIdentity int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM hosting_deployments WHERE external_deployment_id=?`, externalDeploymentID).Scan(&existingIdentity); err != nil {
		return err
	}
	if existingIdentity != 0 {
		return &hostingAPIError{Code: errCodeExternalDeploymentConflict, Message: "external deployment identity already exists", StatusCode: 409}
	}
	var activeWork int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM hosting_deployments WHERE hosting_project_id=? AND status IN ('queued','running')`, project.ID).Scan(&activeWork); err != nil {
		return err
	}
	if activeWork != 0 {
		return &hostingAPIError{Code: errCodeExternalDeploymentConflict, Message: "project already has active deployment work", StatusCode: 409}
	}
	limits, err := hostingLimitsForProfile(project.ResourceProfile)
	if err != nil {
		return err
	}
	_, err = selectCompatibleHostingRunner(ctx, db, project.Manifest, limits, "build", requireSecrets, 0)
	return err
}

type hostingProjectState struct {
	ID                int64
	ExternalProjectID string
	Manifest          HostingProjectManifest
	ManifestDigest    string
	ResourceProfile   string
	DeployPath        string
	DesiredState      string
	KillSwitchReason  string
}

func getHostingProjectOnConn(ctx context.Context, conn *sql.Conn, externalID string) (*hostingProjectState, error) {
	var project hostingProjectState
	var manifestJSON string
	err := conn.QueryRowContext(ctx, `SELECT id, external_project_id, manifest_json, manifest_digest,
		resource_profile, deploy_path, desired_state, kill_switch_reason
		FROM hosting_projects WHERE external_project_id=?`, externalID).Scan(
		&project.ID, &project.ExternalProjectID, &manifestJSON, &project.ManifestDigest,
		&project.ResourceProfile, &project.DeployPath, &project.DesiredState, &project.KillSwitchReason)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(manifestJSON), &project.Manifest); err != nil {
		return nil, fmt.Errorf("decode stored manifest: %w", err)
	}
	return &project, nil
}

func findHostingIdempotencyReplay(ctx context.Context, conn *sql.Conn, issuerID int64, operation, key string, requestHashes ...string) (*hostingCreateResult, error) {
	replay, err := findGenericIdempotencyReplay(ctx, conn, issuerID, operation, key, requestHashes...)
	if err != nil || replay == nil {
		return nil, err
	}
	var deployment HostingDeployment
	if err := json.Unmarshal(replay.Body, &deployment); err != nil {
		return nil, fmt.Errorf("decode idempotent response: %w", err)
	}
	return &hostingCreateResult{Deployment: &deployment, ResponseBody: hostingDeploymentWireResponse(replay.Body), StatusCode: replay.StatusCode, Replayed: true}, nil
}

func hostingDeploymentWireResponse(body []byte) []byte {
	if len(body) > 0 && body[len(body)-1] == '\n' {
		return body
	}
	result := make([]byte, len(body), len(body)+1)
	copy(result, body)
	return append(result, '\n')
}

type hostingRunnerQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func selectCompatibleHostingRunner(ctx context.Context, querier hostingRunnerQuerier, manifest HostingProjectManifest, limits hostingWorkloadLimits, operation string, requireSecrets bool, excludedRunnerID int64) (int64, error) {
	cutoff := formatSQLiteTime(time.Now().UTC().Add(-hostingRunnerStaleAfter))
	rows, err := querier.QueryContext(ctx, `SELECT id, manifest_versions_json, runtime_versions_json, operation_capabilities_json
		FROM hosting_runners
		WHERE execution_class='hosting' AND status='online' AND draining=0 AND last_seen>=? AND id<>?
		  AND active_session_id<>''
		  AND free_cpu_millis-reserve_cpu_millis>=?
		  AND free_ram_bytes-reserve_ram_bytes>=?
		  AND free_disk_bytes-reserve_disk_bytes>=?
		  AND free_pids-reserve_pids>=?
		  AND ((SELECT COUNT(*) FROM hosting_releases release
		          WHERE release.runtime_runner_id=hosting_runners.id AND release.status IN ('healthy','active','inactive'))
		     + (SELECT COUNT(*) FROM hosting_jobs job
		          WHERE job.hosting_runner_id=hosting_runners.id AND job.status IN ('queued','leased','running'))
		     + (SELECT COUNT(*) FROM hosting_runtime_recoveries recovery
		          WHERE recovery.hosting_runner_id=hosting_runners.id AND recovery.status IN ('queued','leased','running'))) < ?
		ORDER BY free_cpu_millis DESC, id`, cutoff, excludedRunnerID, limits.CPUMillis, limits.RAMBytes,
		limits.DiskBytes, limits.PIDs, hostingRuntimeInventorySchedulingLimit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var selected int64
	for rows.Next() {
		var id int64
		var manifestsJSON, runtimesJSON, operationsJSON string
		if err := rows.Scan(&id, &manifestsJSON, &runtimesJSON, &operationsJSON); err != nil {
			return 0, err
		}
		if jsonStringListContains(manifestsJSON, manifest.SchemaVersion) &&
			jsonStringListContains(runtimesJSON, manifest.Runtime.NodeVersion) &&
			jsonStringListContains(operationsJSON, operation) &&
			jsonStringListContains(operationsJSON, hostingRunnerInventoryOperation) &&
			(!requireSecrets || jsonStringListContains(operationsJSON, hostingRunnerSecretOperation)) {
			selected = id
			break
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if selected == 0 {
		return 0, &hostingAPIError{Code: errCodeRunnerCapacityUnavailable, Message: "no compatible hosting runner has sufficient reserved capacity", StatusCode: 503}
	}
	return selected, nil
}

func reserveHostingRunner(ctx context.Context, conn *sql.Conn, project *hostingProjectState, limits hostingWorkloadLimits, operation string, requireSecrets bool, excludedRunnerID int64) (int64, error) {
	selected, err := selectCompatibleHostingRunner(ctx, conn, project.Manifest, limits, operation, requireSecrets, excludedRunnerID)
	if err != nil {
		return 0, err
	}
	result, err := conn.ExecContext(ctx, `UPDATE hosting_runners SET
		free_cpu_millis=free_cpu_millis-?, free_ram_bytes=free_ram_bytes-?,
		free_disk_bytes=free_disk_bytes-?, free_pids=free_pids-?
		WHERE id=? AND status='online' AND draining=0 AND active_session_id<>''
		  AND EXISTS (SELECT 1 FROM json_each(operation_capabilities_json)
		    WHERE value=?)
		  AND free_cpu_millis-reserve_cpu_millis>=?
		  AND free_ram_bytes-reserve_ram_bytes>=?
		  AND free_disk_bytes-reserve_disk_bytes>=?
		  AND free_pids-reserve_pids>=?
		  AND ((SELECT COUNT(*) FROM hosting_releases release
		          WHERE release.runtime_runner_id=hosting_runners.id AND release.status IN ('healthy','active','inactive'))
		     + (SELECT COUNT(*) FROM hosting_jobs job
		          WHERE job.hosting_runner_id=hosting_runners.id AND job.status IN ('queued','leased','running'))
		     + (SELECT COUNT(*) FROM hosting_runtime_recoveries recovery
		          WHERE recovery.hosting_runner_id=hosting_runners.id AND recovery.status IN ('queued','leased','running'))) < ?`,
		limits.CPUMillis, limits.RAMBytes, limits.DiskBytes, limits.PIDs, selected, hostingRunnerInventoryOperation, limits.CPUMillis,
		limits.RAMBytes, limits.DiskBytes, limits.PIDs, hostingRuntimeInventorySchedulingLimit)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected != 1 {
		return 0, &hostingAPIError{Code: errCodeRunnerCapacityUnavailable, Message: "hosting runner capacity changed before placement", StatusCode: 503}
	}
	return selected, nil
}

func jsonStringListContains(encoded, wanted string) bool {
	var values []string
	if json.Unmarshal([]byte(encoded), &values) != nil {
		return false
	}
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func isSQLiteUniqueConstraint(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unique constraint") || strings.Contains(message, "constraint failed")
}

func generateLeaseToken() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

var errNoHostingJob = errors.New("no hosting job available")
