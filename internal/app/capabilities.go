package app

import "net/http"

const apiVersion = "v1"

type capabilitiesResponse struct {
	Service        string             `json:"service"`
	Version        string             `json:"version"`
	APIVersion     string             `json:"api_version"`
	DeployModes    []string           `json:"deploy_modes"`
	BuildTriggers  []string           `json:"build_triggers"`
	BuildStatuses  []string           `json:"build_statuses"`
	JobStatuses    []string           `json:"job_statuses"`
	ErrorCodes     []string           `json:"error_codes"`
	RuntimeErrors  []string           `json:"runtime_error_codes"`
	EndpointGroups []string           `json:"endpoint_groups"`
	Auth           capabilitiesAuth   `json:"auth"`
	Features       map[string]bool    `json:"features"`
	Limits         capabilitiesLimits `json:"limits"`
}

type capabilitiesAuth struct {
	ExternalAdminAuth bool   `json:"external_admin_auth"`
	LocalPasswordAuth bool   `json:"local_password_auth"`
	AgentBearerAuth   bool   `json:"agent_bearer_auth"`
	CSRFHeader        string `json:"csrf_header"`
}

type capabilitiesLimits struct {
	MaxPersistedBuildLogBytes     int   `json:"max_persisted_build_log_bytes"`
	MaxAgentLogSendBytes          int64 `json:"max_agent_log_send_bytes"`
	MaxSnapshotUploadBytes        int64 `json:"max_snapshot_upload_bytes"`
	MaxAgentArtifactDownloadBytes int64 `json:"max_agent_artifact_download_bytes"`
	LogRetentionDays              int   `json:"log_retention_days"`
	ArtifactRetentionHours        int   `json:"artifact_retention_hours"`
}

func handleAPICapabilities(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	jsonResponse(w, newCapabilitiesResponse(appConfig))
}

func newCapabilitiesResponse(cfg AppConfig) capabilitiesResponse {
	return capabilitiesResponse{
		Service:       "deployer",
		Version:       buildVersion,
		APIVersion:    apiVersion,
		DeployModes:   []string{"docker", "files"},
		BuildTriggers: []string{"manual", "snapshot"},
		BuildStatuses: []string{"running", "success", "failed", "cancelled"},
		JobStatuses:   []string{"pending", "running", "completed", "failed", "cancelled"},
		ErrorCodes: []string{
			errCodeAuthenticationRequired,
			errCodeInvalidAgentToken,
			errCodeCSRFRequired,
			errCodeForbidden,
			errCodeValidation,
			errCodeMethodNotAllowed,
			errCodeNotFound,
			errCodeConflict,
			errCodeProjectBusy,
			errCodeBuildNotRunning,
			errCodeBuildCancelFailed,
			errCodePayloadTooLarge,
			errCodeInternal,
			errCodeProjectNotFound,
			errCodeBuildNotFound,
			errCodeRunnerNotFound,
			errCodeJobNotFound,
			errCodeJobForbidden,
			errCodeArtifactNotFound,
			errCodeArtifactUnavailable,
			errCodeArtifactUnmanaged,
			errCodeArtifactTooLarge,
			errCodeSnapshotUnavailable,
			errCodeSnapshotUnmanaged,
			errCodeInvalidAgentCompletion,
		},
		RuntimeErrors: runtimeErrorCodes,
		EndpointGroups: []string{
			"projects",
			"builds",
			"runners",
			"agent",
			"version",
			"capabilities",
		},
		Auth: capabilitiesAuth{
			ExternalAdminAuth: true,
			LocalPasswordAuth: false,
			AgentBearerAuth:   true,
			CSRFHeader:        csrfHeader,
		},
		Features: map[string]bool{
			"artifact_downloads":      true,
			"artifact_retention":      cfg.ArtifactRetentionHours > 0,
			"build_annotations":       true,
			"build_events":            true,
			"build_failure_summaries": true,
			"build_history_charts":    true,
			"build_release_notes":     true,
			"auto_update":             true,
			"content_security_policy": true,
			"csrf_protection":         true,
			"deploy_previews":         true,
			"docker_deploys":          true,
			"files_deploys":           true,
			"health_checks":           true,
			"log_redaction":           true,
			"log_retention":           cfg.LogRetentionDays > 0,
			"project_cloning":         true,
			"project_runbooks":        true,
			"project_import_export":   true,
			"repo_autodetection":      true,
			"project_summaries":       true,
			"remote_snapshots":        true,
			"runner_token_hashing":    true,
			"runner_token_rotation":   true,
			"sse_build_logs":          true,
		},
		Limits: capabilitiesLimits{
			MaxPersistedBuildLogBytes:     maxPersistedBuildLogBytes,
			MaxAgentLogSendBytes:          1 << 20,
			MaxSnapshotUploadBytes:        maxSnapshotUploadBytes,
			MaxAgentArtifactDownloadBytes: maxAgentArtifactDownloadBytes,
			LogRetentionDays:              cfg.LogRetentionDays,
			ArtifactRetentionHours:        cfg.ArtifactRetentionHours,
		},
	}
}
