package app

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

const (
	errCodeAuthenticationRequired     = "authentication_required"
	errCodeInvalidAgentToken          = "invalid_agent_token"
	errCodeServiceTokenRequired       = "service_token_required"
	errCodeInvalidServiceToken        = "invalid_service_token"
	errCodeServiceTokenNotFound       = "service_token_not_found"
	errCodeInsufficientScope          = "insufficient_scope"
	errCodePreconditionRequired       = "precondition_required"
	errCodePreconditionFailed         = "precondition_failed"
	errCodeInvalidIdempotencyKey      = "invalid_idempotency_key"
	errCodeIdempotencyConflict        = "idempotency_conflict"
	errCodeIdempotencyInProgress      = "idempotency_in_progress"
	errCodeInvalidDeployment          = "invalid_deployment"
	errCodeExternalDeploymentConflict = "external_deployment_conflict"
	errCodeDeploymentNotFound         = "deployment_not_found"
	errCodeManifestDigestMismatch     = "manifest_digest_mismatch"
	errCodeArtifactDigestMismatch     = "artifact_digest_mismatch"
	errCodeSourceFetchFailed          = "source_fetch_failed"
	errCodeSecretReferenceUnavailable = "secret_reference_unavailable"
	errCodeProjectSuspended           = "project_suspended"
	errCodeExecutionDisabled          = "execution_disabled"
	errCodeRunnerCapacityUnavailable  = "runner_capacity_unavailable"
	errCodeReleaseNotFound            = "release_not_found"
	errCodeReleaseNotHealthy          = "release_not_healthy"
	errCodeProxyUnavailable           = "proxy_unavailable"
	errCodeProxyRejected              = "proxy_rejected"
	errCodeUnsupportedMediaType       = "unsupported_media_type"
	errCodeManifestSignatureRequired  = "manifest_signature_required"
	errCodeInvalidManifestSignature   = "invalid_manifest_signature"
	errCodeManifestSignatureExpired   = "manifest_signature_expired"
	errCodeInvalidManifest            = "invalid_manifest"
	errCodeProvisionFailed            = "provision_failed"
	errCodeCSRFRequired               = "csrf_required"
	errCodeForbidden                  = "forbidden"
	errCodeValidation                 = "validation_error"
	errCodeMethodNotAllowed           = "method_not_allowed"
	errCodeNotFound                   = "not_found"
	errCodeConflict                   = "conflict"
	errCodeProjectBusy                = "project_busy"
	errCodeBuildNotRunning            = "build_not_running"
	errCodeBuildCancelFailed          = "build_cancel_failed"
	errCodePayloadTooLarge            = "payload_too_large"
	errCodeInternal                   = "internal_error"
	errCodeProjectNotFound            = "project_not_found"
	errCodeBuildNotFound              = "build_not_found"
	errCodeRunnerNotFound             = "runner_not_found"
	errCodeJobNotFound                = "job_not_found"
	errCodeJobForbidden               = "job_forbidden"
	errCodeArtifactNotFound           = "artifact_not_found"
	errCodeArtifactUnavailable        = "artifact_unavailable"
	errCodeArtifactUnmanaged          = "artifact_unmanaged"
	errCodeArtifactTooLarge           = "artifact_too_large"
	errCodeSnapshotUnavailable        = "snapshot_unavailable"
	errCodeSnapshotUnmanaged          = "snapshot_unmanaged"
	errCodeInvalidAgentCompletion     = "invalid_agent_completion"
	errCodeRunnerSessionSuperseded    = "runner_session_superseded"
)

type apiErrorResponse struct {
	Error             string                   `json:"error"`
	Code              string                   `json:"code"`
	CleanupAuthorized bool                     `json:"cleanup_authorized,omitempty"`
	RetainedReleases  []hostingRetainedRelease `json:"retained_releases,omitempty"`
}

func jsonError(w http.ResponseWriter, msg string, status int) {
	jsonErrorCode(w, defaultErrorCode(status), msg, status)
}

func jsonErrorCode(w http.ResponseWriter, code, msg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	logOperationalError("encode JSON error", json.NewEncoder(w).Encode(apiErrorResponse{
		Error: msg,
		Code:  code,
	}))
}

func defaultErrorCode(status int) string {
	switch status {
	case http.StatusBadRequest:
		return errCodeValidation
	case http.StatusUnauthorized:
		return errCodeAuthenticationRequired
	case http.StatusForbidden:
		return errCodeForbidden
	case http.StatusNotFound:
		return errCodeNotFound
	case http.StatusMethodNotAllowed:
		return errCodeMethodNotAllowed
	case http.StatusConflict:
		return errCodeConflict
	case http.StatusRequestEntityTooLarge:
		return errCodePayloadTooLarge
	case http.StatusUnsupportedMediaType:
		return errCodeUnsupportedMediaType
	default:
		if status >= 500 {
			return errCodeInternal
		}
		return errCodeValidation
	}
}

func agentAuthErrorCode(err error) string {
	if err != nil && strings.Contains(err.Error(), "invalid token") {
		return errCodeInvalidAgentToken
	}
	return errCodeAuthenticationRequired
}

func deployConflictErrorCode(err error) string {
	if err == nil {
		return errCodeConflict
	}
	message := err.Error()
	if strings.Contains(message, "already building") || strings.Contains(message, "already running") {
		return errCodeProjectBusy
	}
	return errCodeConflict
}

func requireMethod(w http.ResponseWriter, r *http.Request, allowed ...string) bool {
	for _, method := range allowed {
		if r.Method == method {
			return true
		}
	}
	jsonMethodNotAllowed(w, allowed...)
	return false
}

func jsonMethodNotAllowed(w http.ResponseWriter, allowed ...string) {
	if len(allowed) > 0 {
		w.Header().Set("Allow", strings.Join(allowed, ", "))
	}
	jsonErrorCode(w, errCodeMethodNotAllowed, "method not allowed", http.StatusMethodNotAllowed)
}

func parseIDPath(path, prefix string) (int64, string, bool) {
	rest, ok := strings.CutPrefix(path, prefix)
	if !ok || rest == "" || strings.HasPrefix(rest, "/") || strings.HasSuffix(rest, "/") {
		return 0, "", false
	}
	idText, suffix, _ := strings.Cut(rest, "/")
	if idText == "" {
		return 0, "", false
	}
	id, err := strconv.ParseInt(idText, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return id, suffix, true
}
