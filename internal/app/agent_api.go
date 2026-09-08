package app

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"
)

const maxSnapshotUploadBytes int64 = 4 << 30 // 4 GiB
const maxAgentArtifactDownloadBytes int64 = 8 << 30

// authenticateAgent validates the agent token and returns the runner
func authenticateAgent(r *http.Request) (*Runner, error) {
	token := agentTokenFromRequest(r)
	if token == "" {
		return nil, fmt.Errorf("missing token")
	}
	runner, err := getRunnerByToken(token)
	if err != nil {
		return nil, fmt.Errorf("invalid token")
	}
	return runner, nil
}

func agentTokenFromRequest(r *http.Request) string {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(auth) >= 7 && strings.EqualFold(auth[:7], "Bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	return ""
}

// handleAgentPoll — long-poll endpoint for agents to pick up jobs
// GET /api/agent/poll
func handleAgentPoll(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	if err := ensureRunnerOperationsSchema(); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	runner, err := authenticateAgent(r)
	if err != nil {
		jsonErrorCode(w, agentAuthErrorCode(err), err.Error(), http.StatusUnauthorized)
		return
	}

	// Update heartbeat
	logOperationalError("update runner heartbeat", updateRunnerHeartbeatContext(r.Context(), runner.ID))
	if queued, err := runnerHasQueuedOperation(runner.ID); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	} else if queued {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Long-poll: check for pending jobs every 2s, timeout after 30s
	ctx := r.Context()
	deadline := time.After(30 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			w.WriteHeader(http.StatusNoContent)
			return
		default:
		}

		job, err := claimPendingDeploymentForRunner(ctx, runner.ID)
		if err == nil && job != nil {
			logOperationalError("update runner heartbeat", updateRunnerHeartbeatContext(ctx, runner.ID))

			w.Header().Set("Content-Type", "application/json")
			logOperationalError("encode agent job", json.NewEncoder(w).Encode(job))
			return
		}

		// Wait 2 seconds before checking again
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}

		// Update heartbeat while polling
		logOperationalError("update runner heartbeat", updateRunnerHeartbeatContext(ctx, runner.ID))
	}
}

// handleAgentOperationPoll is intentionally separate from deployment polling so
// older agents keep the existing Job response format.
func handleAgentOperationPoll(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	if err := ensureRunnerOperationsSchema(); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	runner, err := authenticateAgent(r)
	if err != nil {
		jsonErrorCode(w, agentAuthErrorCode(err), err.Error(), http.StatusUnauthorized)
		return
	}
	logOperationalError("update runner heartbeat", updateRunnerHeartbeatContext(r.Context(), runner.ID))
	op, err := claimRunnerOperation(r.Context(), runner.ID)
	if err == sql.ErrNoRows {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, op)
}

func handleAgentOperationComplete(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	runner, err := authenticateAgent(r)
	if err != nil {
		jsonErrorCode(w, agentAuthErrorCode(err), err.Error(), http.StatusUnauthorized)
		return
	}
	id, suffix, ok := parseIDPath(r.URL.Path, "/api/agent/operations/complete/")
	if !ok || suffix != "" {
		jsonErrorCode(w, errCodeValidation, "invalid operation id", http.StatusBadRequest)
		return
	}
	var result struct {
		Status  string `json:"status"`
		Summary string `json:"summary"`
		Log     string `json:"log"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, maxOperationLogBytes+4096)).Decode(&result); err != nil {
		jsonErrorCode(w, errCodeValidation, "invalid JSON", http.StatusBadRequest)
		return
	}
	if err := completeRunnerOperation(runner.ID, id, result.Status, result.Summary, result.Log); err != nil {
		if err == sql.ErrNoRows {
			jsonErrorCode(w, errCodeJobNotFound, "operation not found", http.StatusNotFound)
		} else {
			jsonError(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleAgentArtifact — serve the docker image tar to the agent
// GET /api/agent/artifact/{build_id}
func handleAgentArtifact(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	runner, err := authenticateAgent(r)
	if err != nil {
		jsonErrorCode(w, agentAuthErrorCode(err), err.Error(), http.StatusUnauthorized)
		return
	}

	buildID, suffix, ok := parseIDPath(r.URL.Path, "/api/agent/artifact/")
	if !ok || suffix != "" {
		jsonErrorCode(w, errCodeValidation, "invalid build_id", http.StatusBadRequest)
		return
	}

	// Get the job and verify it belongs to this runner
	job, err := getJobByBuildID(buildID)
	if err != nil {
		jsonErrorCode(w, errCodeJobNotFound, "job not found", http.StatusNotFound)
		return
	}
	if job.RunnerID != runner.ID {
		jsonErrorCode(w, errCodeJobForbidden, "job does not belong to this runner", http.StatusForbidden)
		return
	}
	if !isManagedArtifactPath(job.ArtifactPath) {
		jsonErrorCode(w, errCodeArtifactUnmanaged, "artifact path is not managed", http.StatusInternalServerError)
		return
	}

	// Serve the tar file
	info, err := managedArtifactInfo(job.ArtifactPath)
	if err != nil {
		jsonErrorCode(w, errCodeArtifactNotFound, "artifact not found", http.StatusNotFound)
		return
	}
	if info.Size() > maxAgentArtifactDownloadBytes {
		jsonErrorCode(w, errCodeArtifactTooLarge, "artifact exceeds download limit", http.StatusRequestEntityTooLarge)
		return
	}

	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": safeDownloadName(job.ImageName) + ".tar"}))
	if err := serveManagedArtifact(w, r, job.ArtifactPath); err != nil {
		logOperationalError("serve agent artifact", err)
	}
}

// handleAgentSnapshotUpload receives a project archive uploaded by an agent.
// POST /api/agent/snapshot/{build_id}
func handleAgentSnapshotUpload(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	runner, err := authenticateAgent(r)
	if err != nil {
		jsonErrorCode(w, agentAuthErrorCode(err), err.Error(), http.StatusUnauthorized)
		return
	}

	buildID, suffix, ok := parseIDPath(r.URL.Path, "/api/agent/snapshot/")
	if !ok || suffix != "" {
		jsonErrorCode(w, errCodeValidation, "invalid build_id", http.StatusBadRequest)
		return
	}

	job, err := getJobByBuildID(buildID)
	if err != nil {
		jsonErrorCode(w, errCodeJobNotFound, "job not found", http.StatusNotFound)
		return
	}
	if job.RunnerID != runner.ID || job.Mode != "snapshot" {
		jsonErrorCode(w, errCodeJobForbidden, "unauthorized", http.StatusForbidden)
		return
	}
	if job.ArtifactPath == "" {
		jsonErrorCode(w, errCodeSnapshotUnavailable, "snapshot path not configured", http.StatusInternalServerError)
		return
	}
	if !isManagedArtifactPath(job.ArtifactPath) {
		jsonErrorCode(w, errCodeSnapshotUnmanaged, "snapshot path is not managed", http.StatusInternalServerError)
		return
	}

	tmpPath, out, err := prepareManagedArtifactUpload(job.ArtifactPath)
	if err != nil {
		jsonErrorCode(w, errCodeSnapshotUnavailable, "create snapshot: "+err.Error(), http.StatusInternalServerError)
		return
	}

	limitedBody := http.MaxBytesReader(w, r.Body, maxSnapshotUploadBytes)
	_, copyErr := io.Copy(out, limitedBody)
	closeErr := out.Close()
	if copyErr != nil {
		abortManagedArtifactUpload(tmpPath)
		jsonErrorCode(w, errCodeValidation, "save snapshot: "+copyErr.Error(), http.StatusBadRequest)
		return
	}
	if closeErr != nil {
		abortManagedArtifactUpload(tmpPath)
		jsonErrorCode(w, errCodeSnapshotUnavailable, "close snapshot: "+closeErr.Error(), http.StatusInternalServerError)
		return
	}
	if err := commitManagedArtifactUpload(tmpPath, job.ArtifactPath); err != nil {
		abortManagedArtifactUpload(tmpPath)
		jsonErrorCode(w, errCodeSnapshotUnavailable, "finalize snapshot: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	logOperationalError("encode snapshot upload response", json.NewEncoder(w).Encode(map[string]string{"status": "ok"}))
}

// handleAgentLog — receive log lines from the agent
// POST /api/agent/log/{build_id}
func handleAgentLog(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	runner, err := authenticateAgent(r)
	if err != nil {
		jsonErrorCode(w, agentAuthErrorCode(err), err.Error(), http.StatusUnauthorized)
		return
	}

	buildID, suffix, ok := parseIDPath(r.URL.Path, "/api/agent/log/")
	if !ok || suffix != "" {
		jsonErrorCode(w, errCodeValidation, "invalid build_id", http.StatusBadRequest)
		return
	}

	// Verify job belongs to this runner
	job, err := getJobByBuildID(buildID)
	if err != nil || job.RunnerID != runner.ID {
		jsonErrorCode(w, errCodeJobForbidden, "unauthorized", http.StatusForbidden)
		return
	}

	// Read log lines from body
	body, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024)) // 1MB max
	if err != nil {
		jsonErrorCode(w, errCodeValidation, "read error", http.StatusBadRequest)
		return
	}

	logText := redactSecrets(string(body))
	lines := strings.Split(logText, "\n")
	for _, line := range lines {
		if line != "" {
			broker.Publish(buildID, line)
		}
	}

	logOperationalError("append build log", appendBuildLog(buildID, logText))

	w.WriteHeader(http.StatusOK)
}

// handleAgentComplete — agent reports final build status
// POST /api/agent/complete/{build_id}
func handleAgentComplete(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	runner, err := authenticateAgent(r)
	if err != nil {
		jsonErrorCode(w, agentAuthErrorCode(err), err.Error(), http.StatusUnauthorized)
		return
	}

	buildID, suffix, ok := parseIDPath(r.URL.Path, "/api/agent/complete/")
	if !ok || suffix != "" {
		jsonErrorCode(w, errCodeValidation, "invalid build_id", http.StatusBadRequest)
		return
	}

	// Verify job belongs to this runner
	job, err := getJobByBuildID(buildID)
	if err != nil || job.RunnerID != runner.ID {
		jsonErrorCode(w, errCodeJobForbidden, "unauthorized", http.StatusForbidden)
		return
	}

	// Parse status from body
	var result struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
		jsonErrorCode(w, errCodeValidation, "invalid JSON", http.StatusBadRequest)
		return
	}
	if result.Status != "success" && result.Status != "failed" {
		jsonErrorCode(w, errCodeInvalidAgentCompletion, "status must be success or failed", http.StatusBadRequest)
		return
	}

	// Update build
	build, err := getBuild(buildID)
	if err != nil {
		jsonErrorCode(w, errCodeBuildNotFound, "build not found", http.StatusNotFound)
		return
	}
	if build.Status != "running" {
		if build.Status == "cancelled" {
			logOperationalError("cancel completed job", updateUnfinishedJobStatusByBuildIDWithError(buildID, "cancelled", runtimeErrCancelled))
		}
		if job.ArtifactPath != "" && (job.Mode != "snapshot" || result.Status != "success") {
			removeManagedArtifact(job.ArtifactPath)
		}
		broker.Close(buildID)
		w.WriteHeader(http.StatusOK)
		logOperationalError("encode ignored completion response", json.NewEncoder(w).Encode(map[string]string{"status": "ignored"}))
		return
	}

	now := time.Now()
	build.Status = result.Status
	build.ErrorMessage = result.Error
	build.ErrorCode = ""
	if result.Status == "failed" {
		build.ErrorCode = classifyRuntimeError(result.Error)
	}
	build.FinishedAt = &now
	dur := int(now.Sub(build.StartedAt).Seconds())
	build.DurationSeconds = &dur
	logOperationalError("update completed build", updateBuild(build))

	// Update job status
	if result.Status == "success" {
		logOperationalError("update completed job", updateJobStatus(job.ID, "completed"))
	} else {
		logOperationalError("update failed job", updateJobStatusWithError(job.ID, "failed", build.ErrorCode))
	}
	clearRunnerDeploymentTask(runner.ID, job.ID)

	// Close SSE stream
	broker.Close(buildID)

	// Cleanup artifact
	if job.ArtifactPath != "" && job.Mode != "snapshot" {
		removeManagedArtifact(job.ArtifactPath)
	} else if job.ArtifactPath != "" && job.Mode == "snapshot" && result.Status != "success" {
		removeManagedArtifact(job.ArtifactPath)
	}

	logStructured("info", "agent_build_completed", map[string]interface{}{
		"build_id": buildID,
		"status":   result.Status,
	})

	w.WriteHeader(http.StatusOK)
	logOperationalError("encode completion response", json.NewEncoder(w).Encode(map[string]string{"status": "ok"}))
}

// handleAgentHeartbeat — keepalive from agent
// POST /api/agent/heartbeat
func handleAgentHeartbeat(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	if err := ensureRunnerOperationsSchema(); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	runner, err := authenticateAgent(r)
	if err != nil {
		jsonErrorCode(w, agentAuthErrorCode(err), err.Error(), http.StatusUnauthorized)
		return
	}
	if r.ContentLength != 0 {
		var telemetry RunnerTelemetry
		if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&telemetry); err != nil {
			jsonErrorCode(w, errCodeValidation, "invalid telemetry", http.StatusBadRequest)
			return
		}
		if err := saveRunnerTelemetry(runner.ID, telemetry); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	logOperationalError("update runner heartbeat", updateRunnerHeartbeat(runner.ID))
	w.WriteHeader(http.StatusOK)
}

// ========== Runner Management API ==========

// handleAPIRunners — list/create runners
// GET /api/runners — list all
// POST /api/runners — create new runner
func handleAPIRunners(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		runners, err := listRunners()
		if err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if runners == nil {
			runners = []Runner{}
		}
		// Don't expose tokens in list
		for i := range runners {
			runners[i].Token = ""
		}
		jsonResponse(w, runners)

	case "POST":
		var runner Runner
		if err := json.NewDecoder(r.Body).Decode(&runner); err != nil {
			jsonErrorCode(w, errCodeValidation, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		if runner.Name == "" {
			jsonErrorCode(w, errCodeValidation, "name is required", http.StatusBadRequest)
			return
		}
		if err := createRunner(&runner); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Return with token (only time it's visible)
		w.WriteHeader(http.StatusCreated)
		jsonResponse(w, runner)

	default:
		jsonMethodNotAllowed(w, http.MethodGet, http.MethodPost)
	}
}

// handleAPIRunner — get/delete runner
func handleAPIRunner(w http.ResponseWriter, r *http.Request) {
	id, suffix, ok := parseIDPath(r.URL.Path, "/api/runners/")
	if !ok {
		http.NotFound(w, r)
		return
	}

	if suffix == "rotate" {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		runner, err := rotateRunnerToken(id)
		if err != nil {
			if err == sql.ErrNoRows {
				jsonErrorCode(w, errCodeRunnerNotFound, "runner not found", http.StatusNotFound)
			} else {
				jsonError(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		jsonResponse(w, runner)
		return
	}
	if suffix == "history" {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		handleAPIRunnerHistory(w, r, id)
		return
	}
	if suffix == "operations" {
		switch r.Method {
		case http.MethodGet:
			ops, err := listRunnerOperations(id, 50)
			if err != nil {
				jsonError(w, err.Error(), http.StatusInternalServerError)
				return
			}
			jsonResponse(w, ops)
		case http.MethodPost:
			var request struct {
				ProjectID int64  `json:"project_id"`
				Kind      string `json:"kind"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				jsonErrorCode(w, errCodeValidation, "invalid JSON", http.StatusBadRequest)
				return
			}
			op, err := queueRunnerOperation(id, request.ProjectID, request.Kind)
			if err != nil {
				jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			jsonResponse(w, op)
		default:
			jsonMethodNotAllowed(w, http.MethodGet, http.MethodPost)
		}
		return
	}
	if suffix != "" {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case "GET":
		runner, err := getRunner(id)
		if err != nil {
			if err == sql.ErrNoRows {
				jsonErrorCode(w, errCodeRunnerNotFound, "runner not found", http.StatusNotFound)
			} else {
				jsonError(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		runner.Token = ""
		jsonResponse(w, runner)

	case "DELETE":
		if err := deleteRunner(id); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		jsonMethodNotAllowed(w, http.MethodGet, http.MethodDelete)
	}
}

func handleAPIRunnerOperation(w http.ResponseWriter, r *http.Request) {
	id, suffix, ok := parseIDPath(r.URL.Path, "/api/runner-operations/")
	if !ok {
		http.NotFound(w, r)
		return
	}
	if suffix == "" && r.Method == http.MethodGet {
		op, err := getRunnerOperation(id)
		if err == sql.ErrNoRows {
			jsonErrorCode(w, errCodeJobNotFound, "operation not found", http.StatusNotFound)
			return
		}
		if err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		jsonResponse(w, op)
		return
	}
	if suffix == "cancel" && r.Method == http.MethodPost {
		ok, err := cancelPendingRunnerOperation(id)
		if err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			jsonErrorCode(w, errCodeValidation, "operation is not pending", http.StatusBadRequest)
			return
		}
		jsonResponse(w, map[string]string{"status": "cancelled"})
		return
	}
	jsonMethodNotAllowed(w, http.MethodPost)
}

// handleRunnersPage — HTML page for runner management
func handleRunnersPage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/runners" && r.URL.Path != "/runners/" {
		id, suffix, ok := parseIDPath(r.URL.Path, "/runners/")
		if !ok || suffix != "" {
			http.NotFound(w, r)
			return
		}
		detail, err := getRunnerDetail(id)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		projects, _ := listProjects()
		tmpl.ExecuteTemplate(w, "runner_detail.html", map[string]interface{}{"Detail": detail, "Projects": projects})
		return
	}
	runners, err := listRunners()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if runners == nil {
		runners = []Runner{}
	}
	rows := make([]runnerPageRow, 0, len(runners))
	for _, runner := range runners {
		history, _ := newRunnerBuildHistory(runner.ID, 12)
		rows = append(rows, runnerPageRow{Runner: runner, History: history})
	}
	tmpl.ExecuteTemplate(w, "runners.html", map[string]interface{}{
		"RunnerRows": rows,
		"PublicURL":  appConfig.PublicURL,
	})
}

func safeDownloadName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "artifact"
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	name := strings.Trim(b.String(), ".-")
	if name == "" {
		return "artifact"
	}
	return name
}
