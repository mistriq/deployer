package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleAPICapabilitiesReturnsMachineReadableMetadata(t *testing.T) {
	oldVersion := buildVersion
	oldConfig := appConfig
	t.Cleanup(func() {
		buildVersion = oldVersion
		appConfig = oldConfig
	})

	buildVersion = "1.2.3-test"
	appConfig = AppConfig{
		AdminPassword:          "secret",
		LogRetentionDays:       14,
		ArtifactRetentionHours: 48,
	}

	req := httptest.NewRequest(http.MethodGet, "/api/capabilities", nil)
	rr := httptest.NewRecorder()
	handleAPICapabilities(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}

	var got capabilitiesResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode capabilities response: %v", err)
	}

	if got.Service != "deployer" || got.Version != "1.2.3-test" || got.APIVersion != "v1" {
		t.Fatalf("unexpected service metadata: %+v", got)
	}
	if !containsString(got.DeployModes, "docker") || !containsString(got.DeployModes, "files") {
		t.Fatalf("expected docker and files deploy modes, got %#v", got.DeployModes)
	}
	if !containsString(got.EndpointGroups, "capabilities") || !containsString(got.EndpointGroups, "agent") {
		t.Fatalf("expected endpoint groups to include capabilities and agent, got %#v", got.EndpointGroups)
	}
	if !containsString(got.ErrorCodes, errCodeProjectBusy) ||
		!containsString(got.ErrorCodes, errCodeArtifactUnavailable) ||
		!containsString(got.ErrorCodes, errCodeMethodNotAllowed) {
		t.Fatalf("expected stable error codes to be listed, got %#v", got.ErrorCodes)
	}
	if !containsString(got.RuntimeErrors, runtimeErrArtifactFailed) ||
		!containsString(got.RuntimeErrors, runtimeErrHealthCheckFailed) ||
		!containsString(got.RuntimeErrors, runtimeErrRunnerOffline) {
		t.Fatalf("expected runtime error codes to be listed, got %#v", got.RuntimeErrors)
	}
	if !got.Auth.AdminAuthConfigured || !got.Auth.AgentBearerAuth || got.Auth.CSRFHeader != csrfHeader {
		t.Fatalf("unexpected auth metadata: %+v", got.Auth)
	}
	if !got.Features["runner_token_rotation"] || !got.Features["sse_build_logs"] ||
		!got.Features["project_import_export"] || !got.Features["build_history_charts"] ||
		!got.Features["build_release_notes"] || !got.Features["project_cloning"] {
		t.Fatalf("expected core features to be enabled, got %#v", got.Features)
	}
	if got.Limits.MaxPersistedBuildLogBytes != maxPersistedBuildLogBytes ||
		got.Limits.MaxSnapshotUploadBytes != maxSnapshotUploadBytes ||
		got.Limits.MaxAgentArtifactDownloadBytes != maxAgentArtifactDownloadBytes ||
		got.Limits.LogRetentionDays != 14 ||
		got.Limits.ArtifactRetentionHours != 48 {
		t.Fatalf("unexpected limits: %+v", got.Limits)
	}
}

func TestHandleAPICapabilitiesRejectsNonGET(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/capabilities", nil)
	rr := httptest.NewRecorder()
	handleAPICapabilities(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", rr.Code)
	}
	var got apiErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if got.Code != errCodeMethodNotAllowed {
		t.Fatalf("expected method_not_allowed code, got %+v", got)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
