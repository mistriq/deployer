package app

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const (
	hostingWorkloadIdentityIssuer   = "deployer"
	hostingWorkloadIdentityAudience = "light-apps-hosting-secret-broker"
)

var hostingSecretNamePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)

type hostingWorkloadIdentityClaims struct {
	Version              string `json:"version"`
	Issuer               string `json:"issuer"`
	Audience             string `json:"audience"`
	TokenID              string `json:"token_id"`
	RunnerID             int64  `json:"runner_id"`
	WorkID               int64  `json:"work_id"`
	LeaseGeneration      int64  `json:"lease_generation"`
	Operation            string `json:"operation"`
	ExternalProjectID    string `json:"external_project_id"`
	ExternalDeploymentID string `json:"external_deployment_id"`
	SecretRefsDigest     string `json:"secret_references_digest"`
	IssuedAt             int64  `json:"issued_at"`
	ExpiresAt            int64  `json:"expires_at"`
}

type hostingWorkloadIdentityResponse struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

type hostingSecretReferenceDigestValue struct {
	Provider  string `json:"provider"`
	Reference string `json:"reference"`
	Name      string `json:"name"`
	ExpiresAt string `json:"expires_at"`
}

func validHostingSecretName(name string) bool {
	return hostingSecretNamePattern.MatchString(name)
}

func hostingSecretReferencesDigest(refs []HostingSecretReference) (string, error) {
	canonical := make([]hostingSecretReferenceDigestValue, len(refs))
	for index, ref := range refs {
		canonical[index] = hostingSecretReferenceDigestValue{
			Provider: ref.Provider, Reference: ref.Reference, Name: ref.Name,
			ExpiresAt: ref.ExpiresAt.UTC().Format(time.RFC3339Nano),
		}
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func mintHostingWorkloadIdentity(runnerID, workID, generation int64, operation, externalProjectID,
	externalDeploymentID string, refs []HostingSecretReference, now, leaseExpiresAt time.Time) (*hostingWorkloadIdentityResponse, error) {
	if len(appConfig.HostingWorkloadIdentitySecret) < 32 {
		return nil, fmt.Errorf("workload identity signing is not configured")
	}
	if runnerID <= 0 || workID <= 0 || generation <= 0 || len(refs) == 0 ||
		(operation != "build" && operation != "restore") {
		return nil, fmt.Errorf("workload identity claims are invalid")
	}
	refsDigest, err := hostingSecretReferencesDigest(refs)
	if err != nil {
		return nil, err
	}
	tokenIDBytes := make([]byte, 16)
	if _, err := rand.Read(tokenIDBytes); err != nil {
		return nil, err
	}
	now = now.UTC()
	expires := now.Add(90 * time.Second)
	if leaseExpiresAt.UTC().Before(expires) {
		expires = leaseExpiresAt.UTC()
	}
	expires = time.Unix(expires.Unix(), 0).UTC()
	if !expires.After(now) {
		return nil, fmt.Errorf("workload lease expires too soon")
	}
	claims := hostingWorkloadIdentityClaims{
		Version: "v1", Issuer: hostingWorkloadIdentityIssuer, Audience: hostingWorkloadIdentityAudience,
		TokenID: hex.EncodeToString(tokenIDBytes), RunnerID: runnerID, WorkID: workID,
		LeaseGeneration: generation, Operation: operation, ExternalProjectID: externalProjectID,
		ExternalDeploymentID: externalDeploymentID, SecretRefsDigest: refsDigest,
		IssuedAt: now.Unix(), ExpiresAt: expires.Unix(),
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return nil, err
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(appConfig.HostingWorkloadIdentitySecret))
	mac.Write([]byte(encodedPayload))
	token := "wli_" + encodedPayload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return &hostingWorkloadIdentityResponse{Token: token, ExpiresAt: expires}, nil
}

func handleHostingJobWorkloadIdentity(w http.ResponseWriter, r *http.Request, runner *HostingRunner, jobID int64) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	generation, leaseToken, ok := leaseCredentials(r)
	if !ok {
		jsonErrorCode(w, errCodeJobForbidden, "valid job lease is required", http.StatusForbidden)
		return
	}
	now := time.Now().UTC()
	var externalProjectID, externalDeploymentID, refsJSON, leaseExpires string
	err := db.QueryRowContext(r.Context(), `SELECT project.external_project_id, deployment.external_deployment_id, job.secret_refs_json
		, job.lease_expires_at
		FROM hosting_jobs job JOIN hosting_deployments deployment ON deployment.id=job.hosting_deployment_id
		JOIN hosting_projects project ON project.id=deployment.hosting_project_id
		WHERE job.id=? AND job.hosting_runner_id=? AND job.lease_generation=? AND job.lease_token_hash=?
		  AND job.status IN ('leased','running') AND job.cancel_requested_at IS NULL AND job.lease_expires_at>?`,
		jobID, runner.ID, generation, hashToken(leaseToken), formatSQLiteTime(now)).Scan(
		&externalProjectID, &externalDeploymentID, &refsJSON, &leaseExpires)
	if err != nil {
		jsonErrorCode(w, errCodeJobForbidden, "valid uncancelled job lease is required", http.StatusForbidden)
		return
	}
	issueHostingWorkloadIdentity(w, runner.ID, jobID, generation, "build", externalProjectID, externalDeploymentID, refsJSON, parseSQLiteTime(leaseExpires), now)
}

func handleHostingRecoveryWorkloadIdentity(w http.ResponseWriter, r *http.Request, runner *HostingRunner, recoveryID int64) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	generation, leaseToken, ok := leaseCredentials(r)
	if !ok {
		jsonErrorCode(w, errCodeJobForbidden, "valid recovery lease is required", http.StatusForbidden)
		return
	}
	now := time.Now().UTC()
	var externalProjectID, externalDeploymentID, refsJSON, leaseExpires string
	err := db.QueryRowContext(r.Context(), `SELECT project.external_project_id, deployment.external_deployment_id,
		job.secret_refs_json, recovery.lease_expires_at
		FROM hosting_runtime_recoveries recovery
		JOIN hosting_releases release ON release.id=recovery.hosting_release_id
		JOIN hosting_deployments deployment ON deployment.id=release.hosting_deployment_id
		JOIN hosting_projects project ON project.id=release.hosting_project_id
		JOIN hosting_jobs job ON job.hosting_deployment_id=deployment.id
		WHERE recovery.id=? AND recovery.hosting_runner_id=? AND recovery.lease_generation=?
		  AND recovery.lease_token_hash=? AND recovery.status IN ('leased','running')
		  AND recovery.cancel_requested_at IS NULL AND recovery.lease_expires_at>?`,
		recoveryID, runner.ID, generation, hashToken(leaseToken), formatSQLiteTime(now)).Scan(
		&externalProjectID, &externalDeploymentID, &refsJSON, &leaseExpires)
	if err != nil {
		jsonErrorCode(w, errCodeJobForbidden, "valid uncancelled recovery lease is required", http.StatusForbidden)
		return
	}
	issueHostingWorkloadIdentity(w, runner.ID, recoveryID, generation, "restore", externalProjectID, externalDeploymentID, refsJSON, parseSQLiteTime(leaseExpires), now)
}

func issueHostingWorkloadIdentity(w http.ResponseWriter, runnerID, workID, generation int64, operation,
	externalProjectID, externalDeploymentID, refsJSON string, leaseExpiresAt, now time.Time) {
	var refs []HostingSecretReference
	if err := json.Unmarshal([]byte(refsJSON), &refs); err != nil || len(refs) == 0 {
		jsonErrorCode(w, errCodeSecretReferenceUnavailable, "workload has no redeemable secret references", http.StatusConflict)
		return
	}
	if err := normalizeHostingSecretReferences(refs); err != nil {
		jsonErrorCode(w, errCodeSecretReferenceUnavailable, "workload secret references are invalid", http.StatusConflict)
		return
	}
	identity, err := mintHostingWorkloadIdentity(runnerID, workID, generation, operation,
		externalProjectID, externalDeploymentID, refs, now, leaseExpiresAt)
	if err != nil {
		jsonErrorCode(w, errCodeSecretReferenceUnavailable, "workload identity is unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	jsonResponse(w, identity)
}

func parseHostingWorkloadIdentityForTest(token, secret string) (*hostingWorkloadIdentityClaims, error) {
	token = strings.TrimPrefix(token, "wli_")
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid token")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(parts[0]))
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || !hmac.Equal(signature, mac.Sum(nil)) {
		return nil, fmt.Errorf("invalid signature")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, err
	}
	var claims hostingWorkloadIdentityClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, err
	}
	return &claims, nil
}
