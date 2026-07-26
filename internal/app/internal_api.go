package app

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
)

const idempotencyKeyHeader = "Idempotency-Key"

func handleInternalAPI(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case path == "/api/internal/v1/capabilities":
		handleInternalCapabilities(w, r)
	case path == "/api/internal/v1/runners":
		handleInternalRunners(w, r)
	case path == "/api/internal/v1/settings/kill-switch":
		handleInternalGlobalKillSwitch(w, r)
	case path == "/api/internal/v1/metrics":
		handleInternalHostingMetrics(w, r)
	case strings.HasPrefix(path, "/api/internal/v1/projects/"):
		handleInternalProject(w, r)
	case strings.HasPrefix(path, "/api/internal/v1/deployments/"):
		handleInternalDeployment(w, r)
	default:
		jsonErrorCode(w, errCodeNotFound, "internal API resource not found", http.StatusNotFound)
	}
}

func handleInternalProjectUpsert(w http.ResponseWriter, r *http.Request, externalProjectID string) {
	if !requireMethod(w, r, http.MethodPut) || !requireServiceTokenScope(w, r, serviceScopeProjectsWrite) {
		return
	}
	if !requireJSONContentType(w, r) {
		return
	}
	payload, ok := decodeSignedManifestRequest(w, r, externalProjectID)
	if !ok {
		return
	}
	token, _ := r.Context().Value(serviceTokenContextKey{}).(*ServiceToken)
	project, created, replayed, err := upsertHostingProjectAudited(r.Context(), payload.ExternalProjectID, payload.DefaultHostname, payload.Manifest, token, requestIDFromContext(r.Context()))
	if err != nil {
		jsonErrorCode(w, errCodeProvisionFailed, "project provisioning failed", http.StatusInternalServerError)
		return
	}
	response := internalProjectResponse{
		DefaultHostname:   project.DefaultHostname,
		ExternalProjectID: project.ExternalProjectID,
		ManifestDigest:    project.ManifestDigest,
		Manifest:          project.Manifest,
		Created:           created,
		Replayed:          replayed,
	}
	w.Header().Set("Content-Type", "application/json")
	if created {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	jsonResponse(w, response)
}

func handleInternalProject(w http.ResponseWriter, r *http.Request) {
	externalProjectID, suffix, ok := parseResourcePath(r.URL.Path, "/api/internal/v1/projects/")
	if !ok || !validHostingExternalIDSyntax(externalProjectID) {
		jsonErrorCode(w, errCodeProjectNotFound, "project not found", http.StatusNotFound)
		return
	}
	switch suffix {
	case "":
		handleInternalProjectUpsert(w, r, externalProjectID)
	case "deployments":
		handleInternalDeploymentCreate(w, r, externalProjectID)
	case "releases":
		handleInternalReleases(w, r, externalProjectID)
	case "rollback":
		handleInternalRollback(w, r, externalProjectID)
	case "suspend":
		handleInternalDesiredState(w, r, externalProjectID, "suspended")
	case "resume":
		handleInternalDesiredState(w, r, externalProjectID, "active")
	case "kill-switch":
		handleInternalProjectKillSwitch(w, r, externalProjectID)
	default:
		jsonErrorCode(w, errCodeNotFound, "internal project operation not found", http.StatusNotFound)
	}
}

func handleInternalDeploymentCreate(w http.ResponseWriter, r *http.Request, externalProjectID string) {
	if !requireMethod(w, r, http.MethodPost) || !requireServiceTokenScope(w, r, serviceScopeDeploymentsWrite) {
		return
	}
	if !requireJSONContentType(w, r) {
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get(idempotencyKeyHeader))
	var payload hostingDeploymentCreateRequest
	if !decodeInternalJSON(w, r, 32<<10, &payload) {
		return
	}
	token, _ := r.Context().Value(serviceTokenContextKey{}).(*ServiceToken)
	result, err := createHostingDeployment(r.Context(), token, externalProjectID, idempotencyKey, payload)
	if err != nil {
		writeHostingAPIError(w, err)
		return
	}
	if result.Replayed {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	w.Header().Set("Location", "/api/internal/v1/deployments/"+result.Deployment.ExternalDeploymentID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(result.StatusCode)
	_, err = w.Write(result.ResponseBody)
	logOperationalError("write hosting deployment response", err)
}

func handleInternalDeployment(w http.ResponseWriter, r *http.Request) {
	externalDeploymentID, suffix, ok := parseResourcePath(r.URL.Path, "/api/internal/v1/deployments/")
	if !ok || !validHostingExternalIDSyntax(externalDeploymentID) {
		jsonErrorCode(w, errCodeDeploymentNotFound, "deployment not found", http.StatusNotFound)
		return
	}
	switch suffix {
	case "":
		if !requireMethod(w, r, http.MethodGet) || !requireServiceTokenScope(w, r, serviceScopeDeploymentsRead) {
			return
		}
		deployment, err := getHostingDeploymentByExternalID(r.Context(), externalDeploymentID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				jsonErrorCode(w, errCodeDeploymentNotFound, "deployment not found", http.StatusNotFound)
				return
			}
			jsonErrorCode(w, errCodeInternal, "read deployment state failed", http.StatusInternalServerError)
			return
		}
		jsonResponse(w, deployment)
	case "cancel":
		handleInternalDeploymentCancel(w, r, externalDeploymentID)
	case "events":
		handleInternalDeploymentEvents(w, r, externalDeploymentID)
	case "logs":
		handleInternalDeploymentLogs(w, r, externalDeploymentID)
	default:
		jsonErrorCode(w, errCodeNotFound, "internal deployment operation not found", http.StatusNotFound)
	}
}

func parseResourcePath(path, prefix string) (string, string, bool) {
	rest, ok := strings.CutPrefix(path, prefix)
	if !ok || rest == "" || strings.HasPrefix(rest, "/") || strings.HasSuffix(rest, "/") {
		return "", "", false
	}
	identifier, suffix, _ := strings.Cut(rest, "/")
	if identifier == "" || strings.Contains(suffix, "/") {
		return "", "", false
	}
	return identifier, suffix, true
}

func validIdempotencyKey(value string) bool {
	if len(value) < 8 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || strings.ContainsRune("._:-", char) {
			continue
		}
		return false
	}
	return true
}

func requireJSONContentType(w http.ResponseWriter, r *http.Request) bool {
	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType != "application/json" {
		jsonErrorCode(w, errCodeUnsupportedMediaType, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return false
	}
	return true
}

func decodeInternalJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, destination any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBytes))
	if err != nil {
		jsonErrorCode(w, errCodePayloadTooLarge, "request body exceeds the allowed size", http.StatusRequestEntityTooLarge)
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		jsonErrorCode(w, errCodeValidation, "invalid JSON request body", http.StatusBadRequest)
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		jsonErrorCode(w, errCodeValidation, "request body must contain exactly one JSON object", http.StatusBadRequest)
		return false
	}
	return true
}

func writeHostingAPIError(w http.ResponseWriter, err error) {
	var apiErr *hostingAPIError
	if errors.As(err, &apiErr) {
		jsonErrorCode(w, apiErr.Code, apiErr.Message, apiErr.StatusCode)
		return
	}
	jsonErrorCode(w, errCodeInternal, "hosting operation failed", http.StatusInternalServerError)
}
