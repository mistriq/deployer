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
	"time"
)

type hostingEventResponse struct {
	ID                   int64          `json:"id"`
	EventType            string         `json:"event_type"`
	Phase                string         `json:"phase"`
	FailureCode          string         `json:"failure_code,omitempty"`
	Metadata             map[string]any `json:"metadata"`
	CreatedAt            time.Time      `json:"created_at"`
	PhaseDurationSeconds float64        `json:"phase_duration_seconds"`
}

type hostingLogResponse struct {
	ID        int64     `json:"id"`
	Stream    string    `json:"stream"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

type internalCapabilitiesResponse struct {
	APIVersion             string                           `json:"api_version"`
	ManifestVersions       []string                         `json:"manifest_versions"`
	RunnerProtocolVersions []string                         `json:"runner_protocol_versions"`
	NodeVersions           []string                         `json:"node_versions"`
	RuntimeKinds           []string                         `json:"runtime_kinds"`
	ResourceProfiles       map[string]hostingWorkloadLimits `json:"resource_profiles"`
	FailureCodes           []string                         `json:"failure_codes"`
	RunnerOperations       []string                         `json:"runner_operations"`
}

type internalRunnerResponse struct {
	ID               int64       `json:"id"`
	Name             string      `json:"name"`
	Labels           []string    `json:"labels"`
	ProtocolVersion  string      `json:"protocol_version"`
	ManifestVersions []string    `json:"manifest_versions"`
	RuntimeVersions  []string    `json:"runtime_versions"`
	Operations       []string    `json:"operations"`
	Capacity         capacityDTO `json:"capacity"`
	Free             capacityDTO `json:"free"`
	Reserve          capacityDTO `json:"reserve"`
	Draining         bool        `json:"draining"`
	Status           string      `json:"status"`
	LastSeen         *time.Time  `json:"last_seen,omitempty"`
}

type capacityDTO struct {
	CPUMillis int64 `json:"cpu_millis"`
	RAMBytes  int64 `json:"ram_bytes"`
	DiskBytes int64 `json:"disk_bytes"`
	PIDs      int64 `json:"pids"`
}

func handleInternalCapabilities(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) || !requireServiceTokenScope(w, r, serviceScopeDeploymentsRead) {
		return
	}
	starter, _ := hostingLimitsForProfile("starter")
	standard, _ := hostingLimitsForProfile("standard")
	jsonResponse(w, internalCapabilitiesResponse{
		APIVersion:             "v1",
		ManifestVersions:       []string{hostingManifestVersion},
		RunnerProtocolVersions: []string{hostingRunnerProtocolVersion},
		NodeVersions:           []string{"20", "22"},
		RuntimeKinds:           []string{"static", "node"},
		RunnerOperations:       []string{"build", "restore", hostingRunnerSecretOperation, hostingRunnerInventoryOperation},
		ResourceProfiles:       map[string]hostingWorkloadLimits{"starter": starter, "standard": standard},
		FailureCodes: []string{
			"artifact_digest_mismatch", "artifact_unavailable", "build_failed", "build_timeout", "cancelled", "health_check_failed",
			"proxy_activation_failed", "release_persistence_failed", "runner_lost", "runtime_start_failed",
			"runtime_instance_lost", "secret_reference_unavailable", "source_fetch_failed", "workload_policy_violation",
		},
	})
}

func handleInternalRunners(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) || !requireServiceTokenScope(w, r, serviceScopeDeploymentsRead) {
		return
	}
	rows, err := db.QueryContext(r.Context(), `SELECT id, name, labels_json, protocol_version,
		manifest_versions_json, runtime_versions_json, operation_capabilities_json,
		capacity_cpu_millis, capacity_ram_bytes, capacity_disk_bytes, capacity_pids,
		free_cpu_millis, free_ram_bytes, free_disk_bytes, free_pids,
		reserve_cpu_millis, reserve_ram_bytes, reserve_disk_bytes, reserve_pids,
		draining, status, last_seen FROM hosting_runners ORDER BY name`)
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "list hosting runners failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	runners := make([]internalRunnerResponse, 0)
	for rows.Next() {
		var runner internalRunnerResponse
		var labelsJSON, manifestsJSON, runtimesJSON, operationsJSON string
		var draining int
		var lastSeen sql.NullString
		if err := rows.Scan(&runner.ID, &runner.Name, &labelsJSON, &runner.ProtocolVersion,
			&manifestsJSON, &runtimesJSON, &operationsJSON,
			&runner.Capacity.CPUMillis, &runner.Capacity.RAMBytes, &runner.Capacity.DiskBytes, &runner.Capacity.PIDs,
			&runner.Free.CPUMillis, &runner.Free.RAMBytes, &runner.Free.DiskBytes, &runner.Free.PIDs,
			&runner.Reserve.CPUMillis, &runner.Reserve.RAMBytes, &runner.Reserve.DiskBytes, &runner.Reserve.PIDs,
			&draining, &runner.Status, &lastSeen); err != nil {
			jsonErrorCode(w, errCodeInternal, "read hosting runner failed", http.StatusInternalServerError)
			return
		}
		if json.Unmarshal([]byte(labelsJSON), &runner.Labels) != nil ||
			json.Unmarshal([]byte(manifestsJSON), &runner.ManifestVersions) != nil ||
			json.Unmarshal([]byte(runtimesJSON), &runner.RuntimeVersions) != nil ||
			json.Unmarshal([]byte(operationsJSON), &runner.Operations) != nil {
			jsonErrorCode(w, errCodeInternal, "hosting runner capabilities are invalid", http.StatusInternalServerError)
			return
		}
		runner.Draining = draining != 0
		runner.LastSeen = nullableSQLiteTime(lastSeen)
		runners = append(runners, runner)
	}
	if err := rows.Err(); err != nil {
		jsonErrorCode(w, errCodeInternal, "list hosting runners failed", http.StatusInternalServerError)
		return
	}
	jsonResponse(w, runners)
}

func handleInternalDeploymentEvents(w http.ResponseWriter, r *http.Request, externalDeploymentID string) {
	if !requireMethod(w, r, http.MethodGet) || !requireServiceTokenScope(w, r, serviceScopeDeploymentsRead) {
		return
	}
	rows, err := db.QueryContext(r.Context(), `SELECT e.id, e.event_type, e.phase, e.failure_code,
		e.metadata_json, e.created_at FROM hosting_events e
		JOIN hosting_deployments d ON d.id=e.hosting_deployment_id
		WHERE d.external_deployment_id=? ORDER BY e.id`, externalDeploymentID)
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "read deployment events failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	events := make([]hostingEventResponse, 0)
	for rows.Next() {
		var event hostingEventResponse
		var metadataJSON, createdAt string
		if err := rows.Scan(&event.ID, &event.EventType, &event.Phase, &event.FailureCode, &metadataJSON, &createdAt); err != nil {
			jsonErrorCode(w, errCodeInternal, "read deployment event failed", http.StatusInternalServerError)
			return
		}
		metadataJSON = redactSecrets(metadataJSON)
		if err := json.Unmarshal([]byte(metadataJSON), &event.Metadata); err != nil {
			event.Metadata = map[string]any{"redacted": true}
		}
		event.CreatedAt = parseSQLiteTime(createdAt)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		jsonErrorCode(w, errCodeInternal, "read deployment events failed", http.StatusInternalServerError)
		return
	}
	if len(events) == 0 {
		if _, err := getHostingDeploymentByExternalID(r.Context(), externalDeploymentID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				jsonErrorCode(w, errCodeInternal, "read deployment state failed", http.StatusInternalServerError)
				return
			}
			jsonErrorCode(w, errCodeDeploymentNotFound, "deployment not found", http.StatusNotFound)
			return
		}
	}
	for index := range events {
		if index+1 >= len(events) {
			continue
		}
		events[index].PhaseDurationSeconds = maxFloat(0, events[index+1].CreatedAt.Sub(events[index].CreatedAt).Seconds())
	}
	jsonResponse(w, events)
}

func handleInternalDeploymentLogs(w http.ResponseWriter, r *http.Request, externalDeploymentID string) {
	if !requireMethod(w, r, http.MethodGet) || !requireServiceTokenScope(w, r, serviceScopeDeploymentsRead) {
		return
	}
	rows, err := db.QueryContext(r.Context(), `SELECT l.id, l.stream, l.message, l.created_at
		FROM hosting_logs l JOIN hosting_deployments d ON d.id=l.hosting_deployment_id
		WHERE d.external_deployment_id=? ORDER BY l.id`, externalDeploymentID)
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "read deployment logs failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	logs := make([]hostingLogResponse, 0)
	for rows.Next() {
		var entry hostingLogResponse
		var createdAt string
		if err := rows.Scan(&entry.ID, &entry.Stream, &entry.Message, &createdAt); err != nil {
			jsonErrorCode(w, errCodeInternal, "read deployment log failed", http.StatusInternalServerError)
			return
		}
		entry.Message = redactSecrets(entry.Message)
		entry.CreatedAt = parseSQLiteTime(createdAt)
		logs = append(logs, entry)
	}
	if err := rows.Err(); err != nil {
		jsonErrorCode(w, errCodeInternal, "read deployment logs failed", http.StatusInternalServerError)
		return
	}
	if len(logs) == 0 {
		if _, err := getHostingDeploymentByExternalID(r.Context(), externalDeploymentID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				jsonErrorCode(w, errCodeInternal, "read deployment state failed", http.StatusInternalServerError)
				return
			}
			jsonErrorCode(w, errCodeDeploymentNotFound, "deployment not found", http.StatusNotFound)
			return
		}
	}
	jsonResponse(w, logs)
}

func handleInternalDeploymentCancel(w http.ResponseWriter, r *http.Request, externalDeploymentID string) {
	if !requireMethod(w, r, http.MethodPost) || !requireServiceTokenScope(w, r, serviceScopeDeploymentsWrite) {
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get(idempotencyKeyHeader))
	if !validIdempotencyKey(idempotencyKey) {
		jsonErrorCode(w, errCodeInvalidIdempotencyKey, "Idempotency-Key must contain 8-128 safe characters", http.StatusBadRequest)
		return
	}
	token, _ := r.Context().Value(serviceTokenContextKey{}).(*ServiceToken)
	deployment, replayed, err := cancelHostingDeployment(r.Context(), token, externalDeploymentID, idempotencyKey)
	if err != nil {
		writeHostingAPIError(w, err)
		return
	}
	if replayed {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	jsonResponse(w, deployment)
}

func cancelHostingDeployment(ctx context.Context, token *ServiceToken, externalID, key string) (*HostingDeployment, bool, error) {
	requestHash := hashHostingOperation("cancel", externalID)
	operation := "hosting.deployment.cancel"
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, false, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	if replay, err := findGenericIdempotencyReplay(ctx, conn, token.ID, operation, key, requestHash); err != nil || replay != nil {
		if err != nil {
			return nil, false, err
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return nil, false, err
		}
		committed = true
		var deployment HostingDeployment
		if replay.StatusCode != http.StatusOK {
			return nil, false, fmt.Errorf("invalid cancel idempotency response status %d", replay.StatusCode)
		}
		if err := json.Unmarshal(replay.Body, &deployment); err != nil {
			return nil, false, err
		}
		return &deployment, true, nil
	}

	var deploymentID, projectID int64
	var status string
	err = conn.QueryRowContext(ctx, `SELECT id, hosting_project_id, status FROM hosting_deployments WHERE external_deployment_id=?`, externalID).Scan(&deploymentID, &projectID, &status)
	if err == sql.ErrNoRows {
		return nil, false, &hostingAPIError{Code: errCodeDeploymentNotFound, Message: "deployment not found", StatusCode: 404}
	}
	if err != nil {
		return nil, false, err
	}
	now := time.Now().UTC()
	var compensationIntent *hostingCompensationIntent
	if !isHostingTerminalStatus(status) {
		var jobStatus string
		var runnerID sql.NullInt64
		var limits hostingWorkloadLimits
		err := conn.QueryRowContext(ctx, `SELECT status, hosting_runner_id, required_cpu_millis, required_ram_bytes,
			required_disk_bytes, required_pids FROM hosting_jobs WHERE hosting_deployment_id=?`, deploymentID).Scan(
			&jobStatus, &runnerID, &limits.CPUMillis, &limits.RAMBytes, &limits.DiskBytes, &limits.PIDs)
		if err != nil {
			return nil, false, err
		}
		if jobStatus == "queued" {
			result, err := conn.ExecContext(ctx, `UPDATE hosting_jobs SET status='cancelled',
				cancel_requested_at=?, completed_at=?, hosting_runner_id=NULL
				WHERE hosting_deployment_id=? AND status='queued'`, formatSQLiteTime(now), formatSQLiteTime(now), deploymentID)
			if err != nil {
				return nil, false, err
			}
			if affected, _ := result.RowsAffected(); affected != 1 {
				return nil, false, errHostingStateConflict
			}
			if runnerID.Valid {
				if err := restoreHostingRunnerCapacity(ctx, conn, runnerID.Int64, limits); err != nil {
					return nil, false, err
				}
			}
			result, err = conn.ExecContext(ctx, `UPDATE hosting_deployments SET status='cancelled',
				phase='cancelled', failure_code='cancelled', cancel_requested_at=?, finished_at=?, updated_at=?
				WHERE id=? AND ((status='queued' AND phase='queued') OR (status='running' AND phase='queued'))`,
				formatSQLiteTime(now), formatSQLiteTime(now), formatSQLiteTime(now), deploymentID)
			if err != nil {
				return nil, false, err
			}
			if affected, _ := result.RowsAffected(); affected != 1 {
				return nil, false, errHostingStateConflict
			}
			if err := recordHostingEvent(ctx, conn, projectID, deploymentID, "deployment_cancelled", hostingPhaseCancelled, "cancelled", nil, now); err != nil {
				return nil, false, err
			}
			if err := enqueueTerminalCallback(ctx, conn, deploymentID, now); err != nil {
				return nil, false, err
			}
		} else {
			if _, err := conn.ExecContext(ctx, `UPDATE hosting_jobs SET cancel_requested_at=? WHERE hosting_deployment_id=? AND status IN ('leased','running')`, formatSQLiteTime(now), deploymentID); err != nil {
				return nil, false, err
			}
			if _, err := conn.ExecContext(ctx, `UPDATE hosting_deployments SET phase='cancelling', cancel_requested_at=?, updated_at=? WHERE id=? AND status='running'`, formatSQLiteTime(now), formatSQLiteTime(now), deploymentID); err != nil {
				return nil, false, err
			}
			if err := recordHostingEvent(ctx, conn, projectID, deploymentID, "cancellation_requested", hostingPhaseCancelling, "", nil, now); err != nil {
				return nil, false, err
			}
			compensationIntent, err = stageHostingDeploymentCancellationCompensation(ctx, conn,
				deploymentID, now)
			if err != nil {
				return nil, false, err
			}
		}
	}
	deployment, err := getHostingDeploymentOnConn(ctx, conn, externalID)
	if err != nil {
		return nil, false, err
	}
	response, err := json.Marshal(deployment)
	if err != nil {
		return nil, false, err
	}
	if err := storeGenericIdempotency(ctx, conn, token.ID, operation, key, requestHash, http.StatusOK, response, now); err != nil {
		return nil, false, err
	}
	metadata, _ := json.Marshal(map[string]any{"external_deployment_id": externalID, "result_status": deployment.Status})
	if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_audit_events
		(issuer_token_id, hosting_project_id, event_type, reason, request_id, metadata_json, created_at)
		VALUES (?, ?, 'hosting_deployment_cancel_requested', 'control-plane cancellation', ?, ?, ?)`,
		token.ID, projectID, requestIDFromContext(ctx), redactSecrets(string(metadata)), formatSQLiteTime(now)); err != nil {
		return nil, false, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, false, err
	}
	committed = true
	if err := dispatchHostingCompensationIntent(ctx, compensationIntent); err != nil {
		logOperationalError("dispatch durable deployment cancellation compensation", err)
	}
	return deployment, false, nil
}

func getHostingDeploymentOnConn(ctx context.Context, conn *sql.Conn, externalID string) (*HostingDeployment, error) {
	return scanHostingDeployment(conn.QueryRowContext(ctx, `SELECT d.id, d.external_deployment_id, p.external_project_id,
		d.commit_sha, d.manifest_digest, d.artifact_digest, d.status, d.phase, d.failure_code,
		d.failure_message, d.release_digest, d.previous_release_digest, d.callback_state,
		d.cancel_requested_at, d.started_at, d.finished_at, d.created_at, d.updated_at, p.publication_mode
		FROM hosting_deployments d JOIN hosting_projects p ON p.id=d.hosting_project_id
		WHERE d.external_deployment_id=?`, externalID))
}

func restoreHostingRunnerCapacity(ctx context.Context, conn *sql.Conn, runnerID int64, limits hostingWorkloadLimits) error {
	_, err := conn.ExecContext(ctx, `UPDATE hosting_runners SET
		free_cpu_millis=MIN(capacity_cpu_millis, free_cpu_millis+?),
		free_ram_bytes=MIN(capacity_ram_bytes, free_ram_bytes+?),
		free_disk_bytes=MIN(capacity_disk_bytes, free_disk_bytes+?),
		free_pids=MIN(capacity_pids, free_pids+?) WHERE id=?`, limits.CPUMillis, limits.RAMBytes, limits.DiskBytes, limits.PIDs, runnerID)
	return err
}

func recordHostingEvent(ctx context.Context, conn *sql.Conn, projectID, deploymentID int64, eventType, phase, failureCode string, metadata map[string]any, now time.Time) error {
	if !validHostingEventFailureCode(failureCode) {
		return fmt.Errorf("unknown hosting failure code %q", failureCode)
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	encoded = []byte(redactSecrets(string(encoded)))
	var deployment any
	if deploymentID != 0 {
		deployment = deploymentID
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO hosting_events
		(hosting_project_id, hosting_deployment_id, event_type, phase, failure_code, metadata_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, projectID, deployment, eventType, phase, failureCode, string(encoded), formatSQLiteTime(now))
	return err
}

func validHostingEventFailureCode(code string) bool {
	if code == "" || code == "runtime_instance_lost" {
		return true
	}
	_, ok := allowedHostingFailureCodes[code]
	return ok
}

func enqueueTerminalCallback(ctx context.Context, conn *sql.Conn, deploymentID int64, now time.Time) (resultErr error) {
	defer func() {
		if resultErr != nil {
			resultErr = fmt.Errorf("%w: %v", errHostingCallbackEnqueue, resultErr)
		}
	}()
	deployment, err := getHostingDeploymentByIDOnConn(ctx, conn, deploymentID)
	if err != nil {
		return err
	}
	eventID, encoded, payloadHash, err := buildHostingTerminalCallback(deployment, now)
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO callback_outbox
		(event_id, hosting_deployment_id, payload_json, payload_hash, status, attempts, next_attempt_at, created_at)
		VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)
		ON CONFLICT(hosting_deployment_id) DO NOTHING`, eventID, deploymentID, string(encoded),
		payloadHash, formatSQLiteTime(now), formatSQLiteTime(now))
	return err
}

func buildHostingTerminalCallback(deployment *HostingDeployment, timestamp time.Time) (string, []byte, string, error) {
	eventIdentity := sha256.Sum256([]byte(deployment.ExternalDeploymentID + "\x00" + deployment.Status + "\x00" + deployment.Phase))
	eventID := "evt_" + hex.EncodeToString(eventIdentity[:16])
	payload := map[string]any{
		"event_id": eventID, "timestamp": timestamp, "external_project_id": deployment.ExternalProjectID,
		"external_deployment_id": deployment.ExternalDeploymentID, "phase": deployment.Phase,
		"status": deployment.Status, "failure_code": deployment.FailureCode,
		"artifact_digest": deployment.ArtifactDigest, "release_digest": deployment.ReleaseDigest,
		"metadata": map[string]string{"failure_message": redactSecrets(deployment.FailureMessage),
			"previous_release_digest": deployment.PreviousRelease},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", nil, "", err
	}
	hash := sha256.Sum256(encoded)
	return eventID, encoded, "sha256:" + hex.EncodeToString(hash[:]), nil
}

func buildLegacyHostingTerminalCallback(deployment *HostingDeployment, timestamp time.Time) (string, []byte, string, error) {
	eventIdentity := sha256.Sum256([]byte(deployment.ExternalDeploymentID + "\x00" + deployment.Status + "\x00" + deployment.Phase))
	eventID := "evt_" + hex.EncodeToString(eventIdentity[:16])
	payload := map[string]any{
		"event_id": eventID, "timestamp": timestamp, "external_project_id": deployment.ExternalProjectID,
		"external_deployment_id": deployment.ExternalDeploymentID, "phase": deployment.Phase,
		"status": deployment.Status, "failure_code": deployment.FailureCode,
		"artifact_digest": deployment.ArtifactDigest, "release_digest": deployment.ReleaseDigest,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", nil, "", err
	}
	hash := sha256.Sum256(encoded)
	return eventID, encoded, "sha256:" + hex.EncodeToString(hash[:]), nil
}

func getHostingDeploymentByIDOnConn(ctx context.Context, conn *sql.Conn, id int64) (*HostingDeployment, error) {
	return scanHostingDeployment(conn.QueryRowContext(ctx, `SELECT d.id, d.external_deployment_id, p.external_project_id,
		d.commit_sha, d.manifest_digest, d.artifact_digest, d.status, d.phase, d.failure_code,
		d.failure_message, d.release_digest, d.previous_release_digest, d.callback_state,
		d.cancel_requested_at, d.started_at, d.finished_at, d.created_at, d.updated_at, p.publication_mode
		FROM hosting_deployments d JOIN hosting_projects p ON p.id=d.hosting_project_id WHERE d.id=?`, id))
}

func getHostingDeploymentByID(ctx context.Context, id int64) (*HostingDeployment, error) {
	return scanHostingDeployment(db.QueryRowContext(ctx, `SELECT d.id, d.external_deployment_id, p.external_project_id,
		d.commit_sha, d.manifest_digest, d.artifact_digest, d.status, d.phase, d.failure_code,
		d.failure_message, d.release_digest, d.previous_release_digest, d.callback_state,
		d.cancel_requested_at, d.started_at, d.finished_at, d.created_at, d.updated_at, p.publication_mode
		FROM hosting_deployments d JOIN hosting_projects p ON p.id=d.hosting_project_id WHERE d.id=?`, id))
}

func randomEventID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return "evt_" + hex.EncodeToString(value), nil
}

func hashHostingOperation(parts ...string) string {
	digest := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return "sha256:" + hex.EncodeToString(digest[:])
}

type hostingIdempotencyResponse struct {
	StatusCode int
	Body       []byte
}

func findGenericIdempotencyReplay(ctx context.Context, conn *sql.Conn, tokenID int64, operation, key string, hashes ...string) (*hostingIdempotencyResponse, error) {
	var storedHash string
	var status sql.NullInt64
	var response sql.NullString
	var expiresAt string
	err := conn.QueryRowContext(ctx, `SELECT request_hash, response_status, response_body, expires_at FROM hosting_idempotency
		WHERE issuer_token_id=? AND operation=? AND idempotency_key=?`, tokenID, operation, key).Scan(&storedHash, &status, &response, &expiresAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if status.Valid && response.Valid && !parseSQLiteTime(expiresAt).After(now) {
		_, err := conn.ExecContext(ctx, `DELETE FROM hosting_idempotency
			WHERE issuer_token_id=? AND operation=? AND idempotency_key=?
			  AND response_status IS NOT NULL AND response_body IS NOT NULL AND expires_at<=?`,
			tokenID, operation, key, formatSQLiteTime(now))
		return nil, err
	}
	if !hostingIdempotencyHashMatches(storedHash, hashes) {
		return nil, &hostingAPIError{Code: errCodeIdempotencyConflict, Message: "Idempotency-Key was already used with a different request", StatusCode: 409}
	}
	if !status.Valid || !response.Valid {
		return nil, &hostingAPIError{Code: errCodeIdempotencyInProgress, Message: "the original request is still being processed", StatusCode: 409}
	}
	if status.Int64 < 100 || status.Int64 > 599 {
		return nil, fmt.Errorf("invalid stored idempotency response status %d", status.Int64)
	}
	return &hostingIdempotencyResponse{StatusCode: int(status.Int64), Body: []byte(response.String)}, nil
}

func storeGenericIdempotency(ctx context.Context, conn *sql.Conn, tokenID int64, operation, key, hash string, status int, response []byte, now time.Time) error {
	if status < 100 || status > 599 || len(response) == 0 {
		return fmt.Errorf("invalid idempotency response status or body")
	}
	_, err := conn.ExecContext(ctx, `INSERT INTO hosting_idempotency
		(issuer_token_id, operation, idempotency_key, request_hash, response_status, response_body, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, tokenID, operation, key, hash, status, string(response),
		formatSQLiteTime(now), formatSQLiteTime(now.Add(hostingIdempotencyTTL)))
	return err
}

func handleInternalReleases(w http.ResponseWriter, r *http.Request, externalProjectID string) {
	if !requireMethod(w, r, http.MethodGet) || !requireServiceTokenScope(w, r, serviceScopeDeploymentsRead) {
		return
	}
	project, err := getHostingProjectByExternalID(externalProjectID)
	if err == sql.ErrNoRows {
		jsonErrorCode(w, errCodeProjectNotFound, "project not found", http.StatusNotFound)
		return
	}
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "read project failed", http.StatusInternalServerError)
		return
	}
	releases, err := listHostingReleases(r.Context(), project.ID)
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "list releases failed", http.StatusInternalServerError)
		return
	}
	jsonResponse(w, releases)
}

func handleInternalDesiredState(w http.ResponseWriter, r *http.Request, externalProjectID, desired string) {
	if !requireMethod(w, r, http.MethodPost) || !requireServiceTokenScope(w, r, serviceScopeProjectsWrite) {
		return
	}
	key := strings.TrimSpace(r.Header.Get(idempotencyKeyHeader))
	if !validIdempotencyKey(key) {
		jsonErrorCode(w, errCodeInvalidIdempotencyKey, "Idempotency-Key must contain 8-128 safe characters", http.StatusBadRequest)
		return
	}
	token, _ := r.Context().Value(serviceTokenContextKey{}).(*ServiceToken)
	response, replayed, err := setHostingProjectDesiredState(r.Context(), token, externalProjectID, desired, key)
	if err != nil {
		writeHostingAPIError(w, err)
		return
	}
	if replayed {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	jsonResponse(w, response)
}

func setHostingProjectDesiredState(ctx context.Context, token *ServiceToken, externalID, desired, key string) (map[string]string, bool, error) {
	operation := "hosting.project." + desired
	hash := hashHostingOperation(operation, externalID)
	if replay, err := loadIdempotentResponse(ctx, token.ID, operation, key, hash); err != nil || replay != nil {
		if err != nil {
			return nil, false, err
		}
		var response map[string]string
		if replay.StatusCode != http.StatusOK {
			return nil, false, fmt.Errorf("invalid desired-state idempotency response status %d", replay.StatusCode)
		}
		if err := json.Unmarshal(replay.Body, &response); err != nil {
			return nil, false, err
		}
		return response, true, nil
	}
	now := time.Now().UTC()
	var operationID string
	response := map[string]string{"external_project_id": externalID, "desired_state": desired}
	encoded, _ := json.Marshal(response)
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, false, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	if replay, err := findGenericIdempotencyReplay(ctx, conn, token.ID, operation, key, hash); err != nil || replay != nil {
		if err != nil {
			return nil, false, err
		}
		var stored map[string]string
		if replay.StatusCode != http.StatusOK {
			return nil, false, fmt.Errorf("invalid desired-state idempotency response status %d", replay.StatusCode)
		}
		if err := json.Unmarshal(replay.Body, &stored); err != nil {
			return nil, false, err
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return nil, false, err
		}
		committed = true
		return stored, true, nil
	}
	var projectID int64
	var projectKillReason, publicationMode string
	var globalKill int
	if err := conn.QueryRowContext(ctx, `SELECT project.id, project.kill_switch_reason, project.publication_mode,
		settings.global_kill_switch FROM hosting_projects project
		JOIN hosting_settings settings ON settings.id=1 WHERE project.external_project_id=?`,
		externalID).Scan(&projectID, &projectKillReason, &publicationMode, &globalKill); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, &hostingAPIError{Code: errCodeProjectNotFound, Message: "project not found", StatusCode: 404}
		}
		return nil, false, err
	}
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_projects SET desired_state=?, updated_at=? WHERE id=?`, desired, formatSQLiteTime(now), projectID); err != nil {
		return nil, false, err
	}
	if err := recordHostingEvent(ctx, conn, projectID, 0, "project_"+desired, "", "", nil, now); err != nil {
		return nil, false, err
	}
	if desired == "suspended" {
		if err := cancelHostingJobsForKillSwitch(ctx, conn, sql.NullInt64{Int64: projectID, Valid: true}, "project suspended", now); err != nil {
			return nil, false, err
		}
		if publicationMode == hostingPublicationRuntimeOnlyV1 {
			if _, err := suspendRuntimeOnlyWorkloads(ctx, conn,
				sql.NullInt64{Int64: projectID, Valid: true}, "project suspended", now); err != nil {
				return nil, false, err
			}
		}
	}
	if desired == "active" && publicationMode == hostingPublicationRuntimeOnlyV1 {
		if _, err := ensureResumableRuntimeOnlyRecoveries(ctx, conn,
			sql.NullInt64{Int64: projectID, Valid: true}, now); err != nil {
			return nil, false, err
		}
	}
	routeChange := publicationMode == hostingPublicationProxyV1 &&
		(desired == "suspended" || (projectKillReason == "" && globalKill == 0))
	var routeGeneration int64
	if routeChange {
		routeGeneration, err = nextHostingRouteGeneration(ctx, conn, projectID)
		if err != nil {
			return nil, false, err
		}
		operationID = hashHostingOperation("desired-state-route", fmt.Sprint(token.ID), operation,
			externalID, key, fmt.Sprint(routeGeneration))
		operationType := "resume"
		if desired == "suspended" {
			operationType = "suspend"
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_proxy_operations
			(operation_id, hosting_project_id, operation_type, route_generation, desired_state, status, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)`, operationID,
			projectID, operationType, routeGeneration, desired, formatSQLiteTime(now), formatSQLiteTime(now)); err != nil {
			return nil, false, err
		}
	}
	metadata, _ := json.Marshal(map[string]any{"desired_state": desired, "external_project_id": externalID})
	if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_audit_events
		(issuer_token_id, hosting_project_id, event_type, reason, request_id, metadata_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, token.ID, projectID, "hosting_project_"+desired,
		"control-plane desired state change", requestIDFromContext(ctx), redactSecrets(string(metadata)), formatSQLiteTime(now)); err != nil {
		return nil, false, err
	}
	if err := storeGenericIdempotency(ctx, conn, token.ID, operation, key, hash, http.StatusOK, encoded, now); err != nil {
		return nil, false, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, false, err
	}
	committed = true
	if !routeChange {
		return response, false, nil
	}
	var resumeDigest, resumeEndpoint string
	if desired == "active" {
		var available bool
		resumeDigest, resumeEndpoint, available, err = hostingProjectActiveRoute(ctx, projectID)
		if err != nil {
			logOperationalError("inspect hosting runtime after durable resume acceptance", err)
			return response, false, nil
		}
		if !available {
			if err := fenceUnavailableHostingResume(ctx, nil, projectID, externalID, operationID); err != nil {
				logOperationalError("fence unavailable runtime after durable resume acceptance", err)
			}
			return response, false, nil
		}
	}
	proxy, err := hostingProxyClientFactory(appConfig)
	if err != nil {
		_ = markHostingProxyOperationError(ctx, operationID, errCodeProxyUnavailable)
		return response, false, nil
	}
	var proxyErr error
	if desired == "active" {
		var activation *proxyActivationResponse
		activation, proxyErr = proxy.Activate(ctx, proxyActivationRequest{OperationID: operationID,
			ExternalProjectID: externalID, ReleaseDigest: resumeDigest, RuntimeEndpoint: resumeEndpoint,
			RouteGeneration: routeGeneration})
		if proxyErr == nil {
			proxyErr = markHostingProxyOperationApplied(ctx, operationID, activation.RouteRevision)
		}
	} else {
		proxyErr = proxy.SetSuspended(ctx, proxySuspendRequest{OperationID: operationID,
			ExternalProjectID: externalID, Suspended: true, RouteGeneration: routeGeneration})
	}
	if proxyErr != nil {
		code := errCodeProxyUnavailable
		var apiErr *hostingAPIError
		if errorsAsHosting(proxyErr, &apiErr) {
			code = apiErr.Code
		}
		_ = markHostingProxyOperationError(ctx, operationID, code)
		return response, false, nil
	}
	if desired == "active" {
		resumeState, commitErr := commitHostingResumeOperation(ctx, projectID, operationID)
		if commitErr != nil {
			logOperationalError("commit proxy resume after durable acceptance", commitErr)
			return response, false, nil
		}
		if resumeState == hostingResumeUnavailable {
			if err := fenceUnavailableHostingResume(ctx, proxy, projectID, externalID, operationID); err != nil {
				logOperationalError("fence unavailable proxy resume after durable acceptance", err)
			}
			return response, false, nil
		}
		if resumeState == hostingResumeSuperseded {
			if err := markHostingProxyOperationFailed(ctx, operationID, errCodeConflict); err != nil {
				logOperationalError("mark superseded proxy resume after durable acceptance", err)
			}
		}
		return response, false, nil
	}
	current, err := hostingProxyOperationIsCurrent(ctx, operationID)
	if err != nil {
		logOperationalError("inspect proxy suspension after durable acceptance", err)
		return response, false, nil
	}
	if !current {
		if err := markHostingProxyOperationFailed(ctx, operationID, errCodeConflict); err != nil {
			logOperationalError("mark superseded proxy suspension after durable acceptance", err)
		}
		return response, false, nil
	}
	if err := markHostingProxyOperationCommitted(ctx, operationID); err != nil {
		logOperationalError("commit proxy suspension after durable acceptance", err)
	}
	return response, false, nil
}

type hostingResumeCommitState string

const (
	hostingResumeCommitted   hostingResumeCommitState = "committed"
	hostingResumeUnavailable hostingResumeCommitState = "unavailable"
	hostingResumeSuperseded  hostingResumeCommitState = "superseded"
)

func commitHostingResumeOperation(ctx context.Context, projectID int64, operationID string) (hostingResumeCommitState, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return "", err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	var operationGeneration, currentGeneration int64
	var operationStatus, operationDesired, projectDesired, projectKillReason string
	var globalKill int
	err = conn.QueryRowContext(ctx, `SELECT operation.route_generation, operation.status,
		operation.desired_state, project.route_generation, project.desired_state,
		project.kill_switch_reason, settings.global_kill_switch
		FROM hosting_proxy_operations operation
		JOIN hosting_projects project ON project.id=operation.hosting_project_id
		JOIN hosting_settings settings ON settings.id=1
		WHERE operation.operation_id=? AND operation.hosting_project_id=? AND operation.operation_type='resume'`,
		operationID, projectID).Scan(&operationGeneration, &operationStatus, &operationDesired,
		&currentGeneration, &projectDesired, &projectKillReason, &globalKill)
	if err != nil {
		return "", err
	}
	if operationGeneration != currentGeneration ||
		(operationDesired != "" && operationDesired != "active") || projectDesired != "active" ||
		projectKillReason != "" || globalKill != 0 ||
		(operationStatus != "pending" && operationStatus != "applied") {
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return "", err
		}
		committed = true
		return hostingResumeSuperseded, nil
	}
	available, err := hostingProjectRuntimeAvailableOn(ctx, conn, projectID)
	if err != nil {
		return "", err
	}
	if !available {
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return "", err
		}
		committed = true
		return hostingResumeUnavailable, nil
	}
	result, err := conn.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='committed',
		last_error_code='', updated_at=? WHERE operation_id=? AND status IN ('pending','applied')
		  AND route_generation=(SELECT route_generation FROM hosting_projects WHERE id=?)`,
		formatSQLiteTime(time.Now().UTC()), operationID, projectID)
	if err != nil {
		return "", err
	}
	if affected, err := result.RowsAffected(); err != nil {
		return "", err
	} else if affected != 1 {
		return "", errHostingStateConflict
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return "", err
	}
	committed = true
	return hostingResumeCommitted, nil
}

func fenceUnavailableHostingResume(ctx context.Context, proxy hostingProxyClient, projectID int64,
	externalProjectID, resumeOperationID string) error {
	compensationID := hashHostingOperation("suspend-unavailable-resume", resumeOperationID)
	if err := stageHostingResumeCompensation(ctx, projectID, resumeOperationID, compensationID); err != nil {
		if errors.Is(err, errHostingProxyOperationSuperseded) {
			return nil
		}
		return err
	}
	if proxy == nil {
		var err error
		proxy, err = hostingProxyClientFactory(appConfig)
		if err != nil {
			_ = markHostingProxyOperationError(ctx, compensationID, errCodeProxyUnavailable)
			return nil
		}
	}
	compensationErr := executeHostingCompensation(ctx, proxy, compensationID, "", projectID, 0,
		externalProjectID, "", "", "")
	if compensationErr != nil {
		// The corrective suspension is durable and its reconciler will stage
		// the resume retry only after the adapter accepts the suspension.
		return nil
	}
	return nil
}

func stageHostingResumeCompensation(ctx context.Context, projectID int64, resumeOperationID, compensationID string) error {
	now := time.Now().UTC()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	var sourceCurrent int
	err = conn.QueryRowContext(ctx, `SELECT CASE
		WHEN resume.route_generation=project.route_generation
		 AND resume.status IN ('pending','applied') AND resume.desired_state='active'
		 AND project.desired_state='active' THEN 1 ELSE 0 END
		FROM hosting_proxy_operations resume JOIN hosting_projects project
		  ON project.id=resume.hosting_project_id
		WHERE resume.operation_id=? AND resume.hosting_project_id=? AND resume.operation_type='resume'`,
		resumeOperationID, projectID).Scan(&sourceCurrent)
	if err == sql.ErrNoRows || (err == nil && sourceCurrent == 0) {
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return err
		}
		committed = true
		return errHostingProxyOperationSuperseded
	}
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_proxy_operations SET status='failed',
		last_error_code=?, updated_at=? WHERE operation_id=? AND status IN ('pending','applied')`,
		errCodeReleaseNotHealthy, formatSQLiteTime(now), resumeOperationID); err != nil {
		return err
	}
	var existing int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM hosting_proxy_operations WHERE operation_id=?`, compensationID).Scan(&existing); err != nil {
		return err
	}
	if existing == 0 {
		generation, err := nextHostingRouteGeneration(ctx, conn, projectID)
		if err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_proxy_operations
			(operation_id, hosting_project_id, operation_type, route_generation, desired_state,
			 status, created_at, updated_at)
			VALUES (?, ?, 'compensate', ?, 'resume_after_runtime', 'pending', ?, ?)`,
			compensationID, projectID, generation, formatSQLiteTime(now), formatSQLiteTime(now)); err != nil {
			return err
		}
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return err
	}
	committed = true
	return nil
}

func stageHostingResumeRetry(ctx context.Context, projectID int64, seed string) error {
	now := time.Now().UTC()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	var desired string
	var compensationCurrent int
	if err := conn.QueryRowContext(ctx, `SELECT project.desired_state, CASE
		WHEN compensation.route_generation=project.route_generation
		 AND compensation.status='committed' AND compensation.desired_state='resume_after_runtime'
		THEN 1 ELSE 0 END
		FROM hosting_projects project LEFT JOIN hosting_proxy_operations compensation
		  ON compensation.operation_id=? AND compensation.hosting_project_id=project.id
		WHERE project.id=?`, seed, projectID).Scan(&desired, &compensationCurrent); err != nil {
		return err
	}
	if desired != "active" || compensationCurrent == 0 {
		if _, err := conn.ExecContext(ctx, `UPDATE hosting_proxy_operations SET desired_state='', updated_at=?
			WHERE operation_id=? AND operation_type='compensate' AND desired_state='resume_after_runtime'`,
			formatSQLiteTime(now), seed); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return err
		}
		committed = true
		return nil
	}
	generation, err := nextHostingRouteGeneration(ctx, conn, projectID)
	if err != nil {
		return err
	}
	operationID := hashHostingOperation("resume-after-runtime-available", seed)
	if _, err := conn.ExecContext(ctx, `INSERT INTO hosting_proxy_operations
		(operation_id, hosting_project_id, operation_type, route_generation, desired_state,
		 status, created_at, updated_at) VALUES (?, ?, 'resume', ?, 'active', 'pending', ?, ?)
		ON CONFLICT(operation_id) DO NOTHING`, operationID, projectID, generation,
		formatSQLiteTime(now), formatSQLiteTime(now)); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_proxy_operations SET desired_state='', updated_at=?
		WHERE operation_id=? AND operation_type='compensate' AND desired_state='resume_after_runtime'`,
		formatSQLiteTime(now), seed); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return err
	}
	committed = true
	return nil
}

func hostingProjectRuntimeAvailableOn(ctx context.Context, querier hostingRouteGenerationQuerier, projectID int64) (bool, error) {
	_, _, available, err := hostingProjectActiveRouteOn(ctx, querier, projectID)
	return available, err
}

func hostingProjectActiveRoute(ctx context.Context, projectID int64) (string, string, bool, error) {
	return hostingProjectActiveRouteOn(ctx, db, projectID)
}

func hostingProjectActiveRouteOn(ctx context.Context, querier hostingRouteGenerationQuerier,
	projectID int64) (string, string, bool, error) {
	var digest, endpoint string
	err := querier.QueryRowContext(ctx, `SELECT release_digest, runtime_endpoint FROM hosting_releases
		WHERE hosting_project_id=? AND status='active'`, projectID).Scan(&digest, &endpoint)
	if err == sql.ErrNoRows {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, err
	}
	var runnerStatus, failureCode, activeSession, observedSession string
	var runnerLastSeen, observedAt, missingSince sql.NullString
	err = querier.QueryRowContext(ctx, `SELECT runner.status, runner.last_seen, runner.active_session_id,
		release.runtime_failure_code, release.runtime_observed_at, release.runtime_observed_session_id,
		release.runtime_missing_since
		FROM hosting_releases release
		JOIN hosting_runners runner ON runner.id=release.runtime_runner_id
		WHERE release.hosting_project_id=? AND release.status='active'
		  AND NOT EXISTS (SELECT 1 FROM hosting_runtime_recoveries recovery
		    WHERE recovery.hosting_release_id=release.id AND recovery.status IN ('queued','leased','running'))`,
		projectID).Scan(&runnerStatus, &runnerLastSeen, &activeSession, &failureCode,
		&observedAt, &observedSession, &missingSince)
	if err == sql.ErrNoRows {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, err
	}
	now := time.Now().UTC()
	if failureCode != "" || missingSince.Valid || runnerStatus != "online" || !runnerLastSeen.Valid ||
		parseSQLiteTime(runnerLastSeen.String).Before(now.Add(-hostingRunnerStaleAfter)) {
		return "", "", false, nil
	}
	if activeSession != "" && (!observedAt.Valid || observedSession != activeSession ||
		parseSQLiteTime(observedAt.String).Before(now.Add(-hostingRunnerStaleAfter))) {
		return "", "", false, nil
	}
	return digest, endpoint, true, nil
}

func loadIdempotentResponse(ctx context.Context, tokenID int64, operation, key string, hashes ...string) (*hostingIdempotencyResponse, error) {
	var storedHash string
	var status sql.NullInt64
	var response sql.NullString
	var expiresAt string
	err := db.QueryRowContext(ctx, `SELECT request_hash, response_status, response_body, expires_at FROM hosting_idempotency
		WHERE issuer_token_id=? AND operation=? AND idempotency_key=?`,
		tokenID, operation, key).Scan(&storedHash, &status, &response, &expiresAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if status.Valid && response.Valid && !parseSQLiteTime(expiresAt).After(now) {
		if _, err := db.ExecContext(ctx, `DELETE FROM hosting_idempotency
			WHERE issuer_token_id=? AND operation=? AND idempotency_key=?
			  AND response_status IS NOT NULL AND response_body IS NOT NULL AND expires_at<=?`,
			tokenID, operation, key, formatSQLiteTime(now)); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if !hostingIdempotencyHashMatches(storedHash, hashes) {
		return nil, &hostingAPIError{Code: errCodeIdempotencyConflict, Message: "Idempotency-Key was already used with a different request", StatusCode: 409}
	}
	if !status.Valid || !response.Valid {
		return nil, &hostingAPIError{Code: errCodeIdempotencyInProgress, Message: "the original request is still being processed", StatusCode: 409}
	}
	if status.Int64 < 100 || status.Int64 > 599 {
		return nil, fmt.Errorf("invalid stored idempotency response status %d", status.Int64)
	}
	return &hostingIdempotencyResponse{StatusCode: int(status.Int64), Body: []byte(response.String)}, nil
}

func hostingIdempotencyHashMatches(stored string, accepted []string) bool {
	for _, candidate := range accepted {
		if candidate != "" && stored == candidate {
			return true
		}
	}
	return false
}

type rollbackRequest struct {
	ExternalDeploymentID string `json:"external_deployment_id"`
	ReleaseDigest        string `json:"release_digest"`
}

func handleInternalRollback(w http.ResponseWriter, r *http.Request, externalProjectID string) {
	if !requireMethod(w, r, http.MethodPost) || !requireServiceTokenScope(w, r, serviceScopeDeploymentsWrite) {
		return
	}
	if !requireJSONContentType(w, r) {
		return
	}
	key := strings.TrimSpace(r.Header.Get(idempotencyKeyHeader))
	if !validIdempotencyKey(key) {
		jsonErrorCode(w, errCodeInvalidIdempotencyKey, "Idempotency-Key must contain 8-128 safe characters", http.StatusBadRequest)
		return
	}
	var request rollbackRequest
	if !decodeInternalJSON(w, r, 8<<10, &request) {
		return
	}
	request.ReleaseDigest = strings.ToLower(strings.TrimSpace(request.ReleaseDigest))
	if !validSHA256Digest(request.ReleaseDigest) {
		jsonErrorCode(w, errCodeValidation, "release_digest must be a sha256 digest", http.StatusBadRequest)
		return
	}
	request.ExternalDeploymentID = strings.TrimSpace(request.ExternalDeploymentID)
	if !validHostingExternalIDSyntax(request.ExternalDeploymentID) {
		jsonErrorCode(w, errCodeValidation, "external_deployment_id is invalid", http.StatusBadRequest)
		return
	}
	token, _ := r.Context().Value(serviceTokenContextKey{}).(*ServiceToken)
	result, err := rollbackHostingReleaseResult(r.Context(), token, externalProjectID,
		request.ExternalDeploymentID, request.ReleaseDigest, key)
	if err != nil {
		writeHostingAPIError(w, err)
		return
	}
	if result.Replayed {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(result.StatusCode)
	_, _ = w.Write(result.ResponseBody)
}
