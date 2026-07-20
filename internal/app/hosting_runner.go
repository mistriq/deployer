package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	hostingJobLeaseDuration = 2 * time.Minute
	maxHostingLogChunkBytes = 64 << 10
)

type HostingRunner struct {
	ID               int64       `json:"id"`
	Name             string      `json:"name"`
	Token            string      `json:"token,omitempty"`
	Labels           []string    `json:"labels"`
	ProtocolVersion  string      `json:"protocol_version"`
	ManifestVersions []string    `json:"manifest_versions"`
	RuntimeVersions  []string    `json:"runtime_versions"`
	Operations       []string    `json:"operations"`
	Capacity         capacityDTO `json:"capacity"`
	Reserve          capacityDTO `json:"reserve"`
	Draining         bool        `json:"draining"`
	Status           string      `json:"status"`
	LastSeen         *time.Time  `json:"last_seen,omitempty"`
	CreatedAt        time.Time   `json:"created_at"`
}

type hostingRunnerContextKey struct{}

type hostingRunnerInput struct {
	Name             string      `json:"name"`
	Labels           []string    `json:"labels"`
	ProtocolVersion  string      `json:"protocol_version"`
	ManifestVersions []string    `json:"manifest_versions"`
	RuntimeVersions  []string    `json:"runtime_versions"`
	Operations       []string    `json:"operations,omitempty"`
	Capacity         capacityDTO `json:"capacity"`
	Reserve          capacityDTO `json:"reserve"`
}

func handleAPIHostingRunners(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		runners, err := listHostingRunners(r.Context())
		if err != nil {
			jsonErrorCode(w, errCodeInternal, "list hosting runners failed", http.StatusInternalServerError)
			return
		}
		jsonResponse(w, runners)
	case http.MethodPost:
		if !requireJSONContentType(w, r) {
			return
		}
		var input hostingRunnerInput
		if !decodeInternalJSON(w, r, 32<<10, &input) {
			return
		}
		runner, err := createHostingRunner(r.Context(), input, requestIDFromContext(r.Context()))
		if err != nil {
			jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
		jsonResponse(w, runner)
	default:
		jsonMethodNotAllowed(w, http.MethodGet, http.MethodPost)
	}
}

func handleAPIHostingRunner(w http.ResponseWriter, r *http.Request) {
	id, suffix, ok := parseIDPath(r.URL.Path, "/api/hosting-runners/")
	if !ok || suffix != "rotate" {
		http.NotFound(w, r)
		return
	}
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	token := "htr_" + generateToken()
	tx, err := db.BeginTx(r.Context(), nil)
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "rotate hosting runner failed", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(r.Context(), `UPDATE hosting_runners SET token_hash=? WHERE id=?`, hashToken(token), id)
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "rotate hosting runner failed", http.StatusInternalServerError)
		return
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		jsonErrorCode(w, errCodeRunnerNotFound, "hosting runner not found", http.StatusNotFound)
		return
	}
	metadata, _ := json.Marshal(map[string]any{"hosting_runner_id": id})
	if _, err := tx.ExecContext(r.Context(), `INSERT INTO hosting_audit_events
		(event_type, reason, request_id, metadata_json, created_at)
		VALUES ('hosting_runner_credential_rotated', 'trusted-admin rotation', ?, ?, ?)`,
		requestIDFromContext(r.Context()), string(metadata), formatSQLiteTime(time.Now().UTC())); err != nil {
		jsonErrorCode(w, errCodeInternal, "audit hosting runner rotation failed", http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(); err != nil {
		jsonErrorCode(w, errCodeInternal, "rotate hosting runner failed", http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]any{"id": id, "token": token})
}

func createHostingRunner(ctx context.Context, input hostingRunnerInput, requestID string) (*HostingRunner, error) {
	input.Name = strings.TrimSpace(input.Name)
	if input.Name == "" || len(input.Name) > 100 {
		return nil, fmt.Errorf("name must contain 1-100 characters")
	}
	if input.ProtocolVersion != hostingRunnerProtocolVersion || !hostingStringListContains(input.ManifestVersions, hostingManifestVersion) {
		return nil, fmt.Errorf("runner must support protocol and manifest version v1")
	}
	if !hostingStringListContains(input.RuntimeVersions, "20") && !hostingStringListContains(input.RuntimeVersions, "22") {
		return nil, fmt.Errorf("runner must support an allowlisted Node runtime")
	}
	if len(input.Operations) == 0 {
		input.Operations = []string{"build"}
	}
	input.Operations = normalizeStringList(input.Operations)
	for _, operation := range input.Operations {
		if operation != "build" && operation != "restore" {
			return nil, fmt.Errorf("runner operation must be build or restore")
		}
	}
	if !hostingStringListContains(input.Operations, "build") {
		return nil, fmt.Errorf("runner must support the build operation")
	}
	if err := validateCapacity(input.Capacity, input.Reserve); err != nil {
		return nil, err
	}
	labels, _ := json.Marshal(normalizeStringList(input.Labels))
	manifests, _ := json.Marshal(normalizeStringList(input.ManifestVersions))
	runtimes, _ := json.Marshal(normalizeStringList(input.RuntimeVersions))
	operations, _ := json.Marshal(input.Operations)
	token := "htr_" + generateToken()
	now := time.Now().UTC()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO hosting_runners
		(name, token_hash, labels_json, protocol_version, manifest_versions_json, runtime_versions_json, operation_capabilities_json,
		 capacity_cpu_millis, capacity_ram_bytes, capacity_disk_bytes, capacity_pids,
		 free_cpu_millis, free_ram_bytes, free_disk_bytes, free_pids,
		 reported_free_cpu_millis, reported_free_ram_bytes, reported_free_disk_bytes, reported_free_pids,
		 reserve_cpu_millis, reserve_ram_bytes, reserve_disk_bytes, reserve_pids, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		input.Name, hashToken(token), string(labels), input.ProtocolVersion, string(manifests), string(runtimes), string(operations),
		input.Capacity.CPUMillis, input.Capacity.RAMBytes, input.Capacity.DiskBytes, input.Capacity.PIDs,
		input.Capacity.CPUMillis, input.Capacity.RAMBytes, input.Capacity.DiskBytes, input.Capacity.PIDs,
		input.Capacity.CPUMillis, input.Capacity.RAMBytes, input.Capacity.DiskBytes, input.Capacity.PIDs,
		input.Reserve.CPUMillis, input.Reserve.RAMBytes, input.Reserve.DiskBytes, input.Reserve.PIDs, formatSQLiteTime(now))
	if err != nil {
		return nil, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}
	metadata, _ := json.Marshal(map[string]any{"hosting_runner_id": id, "name": input.Name})
	if _, err := tx.ExecContext(ctx, `INSERT INTO hosting_audit_events
		(event_type, reason, request_id, metadata_json, created_at)
		VALUES ('hosting_runner_created', 'trusted-admin registration', ?, ?, ?)`, requestID, string(metadata), formatSQLiteTime(now)); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &HostingRunner{ID: id, Name: input.Name, Token: token, Labels: normalizeStringList(input.Labels), ProtocolVersion: input.ProtocolVersion, ManifestVersions: normalizeStringList(input.ManifestVersions), RuntimeVersions: normalizeStringList(input.RuntimeVersions), Operations: input.Operations, Capacity: input.Capacity, Reserve: input.Reserve, Status: "offline", CreatedAt: now}, nil
}

func validateCapacity(capacity, reserve capacityDTO) error {
	values := []int64{capacity.CPUMillis, capacity.RAMBytes, capacity.DiskBytes, capacity.PIDs}
	reserves := []int64{reserve.CPUMillis, reserve.RAMBytes, reserve.DiskBytes, reserve.PIDs}
	for i := range values {
		if values[i] <= 0 || reserves[i] < 0 || reserves[i] >= values[i] {
			return fmt.Errorf("runner capacity must be positive and reserve must be smaller than capacity")
		}
	}
	return nil
}

func normalizeStringList(values []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func hostingStringListContains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func listHostingRunners(ctx context.Context) ([]HostingRunner, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, name, labels_json, protocol_version, manifest_versions_json,
		runtime_versions_json, operation_capabilities_json, capacity_cpu_millis, capacity_ram_bytes, capacity_disk_bytes, capacity_pids,
		reserve_cpu_millis, reserve_ram_bytes, reserve_disk_bytes, reserve_pids, draining, status, last_seen, created_at
		FROM hosting_runners ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]HostingRunner, 0)
	for rows.Next() {
		var runner HostingRunner
		var labels, manifests, runtimes, operations, created string
		var drain int
		var seen sql.NullString
		if err := rows.Scan(&runner.ID, &runner.Name, &labels, &runner.ProtocolVersion, &manifests, &runtimes, &operations,
			&runner.Capacity.CPUMillis, &runner.Capacity.RAMBytes, &runner.Capacity.DiskBytes, &runner.Capacity.PIDs,
			&runner.Reserve.CPUMillis, &runner.Reserve.RAMBytes, &runner.Reserve.DiskBytes, &runner.Reserve.PIDs,
			&drain, &runner.Status, &seen, &created); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(labels), &runner.Labels)
		_ = json.Unmarshal([]byte(manifests), &runner.ManifestVersions)
		_ = json.Unmarshal([]byte(runtimes), &runner.RuntimeVersions)
		_ = json.Unmarshal([]byte(operations), &runner.Operations)
		runner.Draining = drain != 0
		runner.LastSeen = nullableSQLiteTime(seen)
		runner.CreatedAt = parseSQLiteTime(created)
		result = append(result, runner)
	}
	return result, rows.Err()
}

func hostingRunnerAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := strings.TrimSpace(r.Header.Get("Authorization"))
		if !strings.HasPrefix(auth, "Bearer ") {
			jsonErrorCode(w, errCodeAuthenticationRequired, "hosting runner token is required", http.StatusUnauthorized)
			return
		}
		var runner HostingRunner
		var labels, manifests, runtimes, operations, created string
		var seen sql.NullString
		err := db.QueryRowContext(r.Context(), `SELECT id, name, labels_json, protocol_version, manifest_versions_json,
			runtime_versions_json, operation_capabilities_json, draining, status, last_seen, created_at FROM hosting_runners WHERE token_hash=?`, hashToken(strings.TrimSpace(strings.TrimPrefix(auth, "Bearer ")))).Scan(
			&runner.ID, &runner.Name, &labels, &runner.ProtocolVersion, &manifests, &runtimes, &operations, &runner.Draining, &runner.Status, &seen, &created)
		if err != nil {
			jsonErrorCode(w, errCodeInvalidAgentToken, "invalid hosting runner token", http.StatusUnauthorized)
			return
		}
		runner.LastSeen = nullableSQLiteTime(seen)
		_ = json.Unmarshal([]byte(operations), &runner.Operations)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), hostingRunnerContextKey{}, &runner)))
	})
}

type hostingHeartbeatRequest struct {
	Free             capacityDTO `json:"free"`
	Draining         bool        `json:"draining"`
	ProtocolVersion  string      `json:"protocol_version"`
	ManifestVersions []string    `json:"manifest_versions"`
	RuntimeVersions  []string    `json:"runtime_versions"`
	Operations       []string    `json:"operations,omitempty"`
}

type hostingHeartbeatResponse struct {
	RetainedReleases []hostingRetainedRelease `json:"retained_releases"`
}

type hostingRetainedRelease struct {
	ReleaseDigest        string `json:"release_digest"`
	ExternalProjectID    string `json:"external_project_id"`
	ExternalDeploymentID string `json:"external_deployment_id"`
	RuntimeInstanceID    string `json:"runtime_instance_id,omitempty"`
	Status               string `json:"status"`
}

func handleHostingAgentHeartbeat(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) || !requireJSONContentType(w, r) {
		return
	}
	runner := r.Context().Value(hostingRunnerContextKey{}).(*HostingRunner)
	var input hostingHeartbeatRequest
	if !decodeInternalJSON(w, r, 16<<10, &input) {
		return
	}
	if input.Free.CPUMillis < 0 || input.Free.RAMBytes < 0 || input.Free.DiskBytes < 0 || input.Free.PIDs < 0 {
		jsonErrorCode(w, errCodeValidation, "runner free capacity must be non-negative", http.StatusBadRequest)
		return
	}
	if input.ProtocolVersion != hostingRunnerProtocolVersion || !hostingStringListContains(input.ManifestVersions, hostingManifestVersion) {
		jsonErrorCode(w, errCodeValidation, "unsupported hosting runner protocol or manifest version", http.StatusBadRequest)
		return
	}
	if len(input.Operations) == 0 {
		input.Operations = []string{"build"}
	}
	input.Operations = normalizeStringList(input.Operations)
	for _, operation := range input.Operations {
		if operation != "build" && operation != "restore" {
			jsonErrorCode(w, errCodeValidation, "hosting runner operation must be build or restore", http.StatusBadRequest)
			return
		}
	}
	if !hostingStringListContains(input.Operations, "build") {
		jsonErrorCode(w, errCodeValidation, "hosting runner must advertise the build operation", http.StatusBadRequest)
		return
	}
	manifests, _ := json.Marshal(normalizeStringList(input.ManifestVersions))
	runtimes, _ := json.Marshal(normalizeStringList(input.RuntimeVersions))
	operations, _ := json.Marshal(input.Operations)
	now := time.Now().UTC()
	conn, err := db.Conn(r.Context())
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "update hosting runner heartbeat failed", http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	if _, err := conn.ExecContext(r.Context(), `BEGIN IMMEDIATE`); err != nil {
		jsonErrorCode(w, errCodeInternal, "update hosting runner heartbeat failed", http.StatusInternalServerError)
		return
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	result, err := conn.ExecContext(r.Context(), `UPDATE hosting_runners SET
		reported_free_cpu_millis=MIN(capacity_cpu_millis, ?), reported_free_ram_bytes=MIN(capacity_ram_bytes, ?),
		reported_free_disk_bytes=MIN(capacity_disk_bytes, ?), reported_free_pids=MIN(capacity_pids, ?),
		draining=?, protocol_version=?, manifest_versions_json=?, runtime_versions_json=?, operation_capabilities_json=?, status='online', last_seen=? WHERE id=?`,
		input.Free.CPUMillis, input.Free.RAMBytes, input.Free.DiskBytes, input.Free.PIDs, input.Draining,
		input.ProtocolVersion, string(manifests), string(runtimes), string(operations), formatSQLiteTime(now), runner.ID)
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "update hosting runner heartbeat failed", http.StatusInternalServerError)
		return
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		jsonErrorCode(w, errCodeRunnerNotFound, "hosting runner not found", http.StatusNotFound)
		return
	}
	if err := recomputeHostingRunnerCapacity(r.Context(), conn, runner.ID); err != nil {
		jsonErrorCode(w, errCodeInternal, "reconcile hosting runner capacity failed", http.StatusInternalServerError)
		return
	}
	rows, err := conn.QueryContext(r.Context(), `SELECT DISTINCT rel.release_digest, p.external_project_id,
			d.external_deployment_id,
			CASE
				WHEN recovery_op.id IS NOT NULL THEN 'active'
				WHEN activation.id IS NOT NULL AND job.hosting_runner_id=? THEN 'healthy'
				ELSE rel.status
			END,
			CASE
				WHEN recovery_op.id IS NOT NULL THEN 'restore-' || recovery.id || '-' || recovery.lease_generation
				WHEN activation.id IS NOT NULL AND job.hosting_runner_id=? THEN 'build-' || job.id || '-' || job.lease_generation
				ELSE rel.runtime_instance_id
		END
		FROM hosting_releases rel
		JOIN hosting_deployments d ON d.id=rel.hosting_deployment_id
		JOIN hosting_projects p ON p.id=rel.hosting_project_id
		LEFT JOIN hosting_jobs job ON job.hosting_deployment_id=rel.hosting_deployment_id
		LEFT JOIN hosting_proxy_operations activation ON activation.hosting_deployment_id=rel.hosting_deployment_id
			AND activation.operation_type='activate' AND activation.status IN ('pending','applied')
		LEFT JOIN hosting_runtime_recoveries recovery ON recovery.hosting_release_id=rel.id
			AND recovery.hosting_runner_id=? AND recovery.status='running'
		LEFT JOIN hosting_proxy_operations recovery_op ON recovery_op.hosting_runtime_recovery_id=recovery.id
			AND recovery_op.operation_type='recover' AND recovery_op.status IN ('pending','applied')
		WHERE (rel.runtime_runner_id=? AND rel.status IN ('healthy','active','inactive'))
		   OR (job.hosting_runner_id=? AND rel.status='healthy' AND activation.id IS NOT NULL)
		   OR (recovery.runtime_endpoint<>'' AND recovery_op.id IS NOT NULL)
		ORDER BY rel.id`, runner.ID, runner.ID, runner.ID, runner.ID, runner.ID)
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "list retained hosting releases failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	response := hostingHeartbeatResponse{RetainedReleases: make([]hostingRetainedRelease, 0)}
	for rows.Next() {
		var release hostingRetainedRelease
		if err := rows.Scan(&release.ReleaseDigest, &release.ExternalProjectID, &release.ExternalDeploymentID,
			&release.Status, &release.RuntimeInstanceID); err != nil {
			jsonErrorCode(w, errCodeInternal, "read retained hosting release failed", http.StatusInternalServerError)
			return
		}
		response.RetainedReleases = append(response.RetainedReleases, release)
	}
	if err := rows.Err(); err != nil {
		jsonErrorCode(w, errCodeInternal, "list retained hosting releases failed", http.StatusInternalServerError)
		return
	}
	if err := rows.Close(); err != nil {
		jsonErrorCode(w, errCodeInternal, "close retained hosting releases failed", http.StatusInternalServerError)
		return
	}
	if _, err := conn.ExecContext(r.Context(), `COMMIT`); err != nil {
		jsonErrorCode(w, errCodeInternal, "commit hosting runner heartbeat failed", http.StatusInternalServerError)
		return
	}
	committed = true
	jsonResponse(w, response)
}

func recomputeHostingRunnerCapacity(ctx context.Context, conn *sql.Conn, runnerID int64) error {
	_, err := conn.ExecContext(ctx, `UPDATE hosting_runners SET
		free_cpu_millis=MAX(0, MIN(reported_free_cpu_millis, capacity_cpu_millis-COALESCE((
			SELECT SUM(required_cpu_millis) FROM hosting_jobs WHERE hosting_runner_id=hosting_runners.id
			AND (status IN ('queued','leased','running') OR (status='succeeded' AND EXISTS (
				SELECT 1 FROM hosting_releases release WHERE release.hosting_deployment_id=hosting_jobs.hosting_deployment_id
				AND release.runtime_runner_id=hosting_runners.id AND release.status IN ('healthy','active','inactive')
				AND (release.runtime_instance_id='' OR release.runtime_instance_id='build-' || hosting_jobs.id || '-' || hosting_jobs.lease_generation))))), 0)
			-COALESCE((SELECT SUM(required_cpu_millis) FROM hosting_runtime_recoveries
			WHERE hosting_runner_id=hosting_runners.id AND (status IN ('queued','leased','running') OR (status='succeeded'
				AND EXISTS (SELECT 1 FROM hosting_releases release WHERE release.id=hosting_runtime_recoveries.hosting_release_id
				AND release.runtime_runner_id=hosting_runners.id AND release.status IN ('healthy','active','inactive')
				AND release.runtime_instance_id='restore-' || hosting_runtime_recoveries.id || '-' || hosting_runtime_recoveries.lease_generation)))), 0))),
		free_ram_bytes=MAX(0, MIN(reported_free_ram_bytes, capacity_ram_bytes-COALESCE((
			SELECT SUM(required_ram_bytes) FROM hosting_jobs WHERE hosting_runner_id=hosting_runners.id
			AND (status IN ('queued','leased','running') OR (status='succeeded' AND EXISTS (
				SELECT 1 FROM hosting_releases release WHERE release.hosting_deployment_id=hosting_jobs.hosting_deployment_id
				AND release.runtime_runner_id=hosting_runners.id AND release.status IN ('healthy','active','inactive')
				AND (release.runtime_instance_id='' OR release.runtime_instance_id='build-' || hosting_jobs.id || '-' || hosting_jobs.lease_generation))))), 0)
			-COALESCE((SELECT SUM(required_ram_bytes) FROM hosting_runtime_recoveries
			WHERE hosting_runner_id=hosting_runners.id AND (status IN ('queued','leased','running') OR (status='succeeded'
				AND EXISTS (SELECT 1 FROM hosting_releases release WHERE release.id=hosting_runtime_recoveries.hosting_release_id
				AND release.runtime_runner_id=hosting_runners.id AND release.status IN ('healthy','active','inactive')
				AND release.runtime_instance_id='restore-' || hosting_runtime_recoveries.id || '-' || hosting_runtime_recoveries.lease_generation)))), 0))),
		free_disk_bytes=MAX(0, MIN(reported_free_disk_bytes, capacity_disk_bytes-COALESCE((
			SELECT SUM(required_disk_bytes) FROM hosting_jobs WHERE hosting_runner_id=hosting_runners.id
			AND (status IN ('queued','leased','running') OR (status='succeeded' AND EXISTS (
				SELECT 1 FROM hosting_releases release WHERE release.hosting_deployment_id=hosting_jobs.hosting_deployment_id
				AND release.runtime_runner_id=hosting_runners.id AND release.status IN ('healthy','active','inactive')
				AND (release.runtime_instance_id='' OR release.runtime_instance_id='build-' || hosting_jobs.id || '-' || hosting_jobs.lease_generation))))), 0)
			-COALESCE((SELECT SUM(required_disk_bytes) FROM hosting_runtime_recoveries
			WHERE hosting_runner_id=hosting_runners.id AND (status IN ('queued','leased','running') OR (status='succeeded'
				AND EXISTS (SELECT 1 FROM hosting_releases release WHERE release.id=hosting_runtime_recoveries.hosting_release_id
				AND release.runtime_runner_id=hosting_runners.id AND release.status IN ('healthy','active','inactive')
				AND release.runtime_instance_id='restore-' || hosting_runtime_recoveries.id || '-' || hosting_runtime_recoveries.lease_generation)))), 0))),
		free_pids=MAX(0, MIN(reported_free_pids, capacity_pids-COALESCE((
			SELECT SUM(required_pids) FROM hosting_jobs WHERE hosting_runner_id=hosting_runners.id
			AND (status IN ('queued','leased','running') OR (status='succeeded' AND EXISTS (
				SELECT 1 FROM hosting_releases release WHERE release.hosting_deployment_id=hosting_jobs.hosting_deployment_id
				AND release.runtime_runner_id=hosting_runners.id AND release.status IN ('healthy','active','inactive')
				AND (release.runtime_instance_id='' OR release.runtime_instance_id='build-' || hosting_jobs.id || '-' || hosting_jobs.lease_generation))))), 0)
			-COALESCE((SELECT SUM(required_pids) FROM hosting_runtime_recoveries
			WHERE hosting_runner_id=hosting_runners.id AND (status IN ('queued','leased','running') OR (status='succeeded'
				AND EXISTS (SELECT 1 FROM hosting_releases release WHERE release.id=hosting_runtime_recoveries.hosting_release_id
				AND release.runtime_runner_id=hosting_runners.id AND release.status IN ('healthy','active','inactive')
				AND release.runtime_instance_id='restore-' || hosting_runtime_recoveries.id || '-' || hosting_runtime_recoveries.lease_generation)))), 0)))
		WHERE id=?`, runnerID)
	return err
}

type hostingClaimedJob struct {
	JobID           int64                    `json:"job_id"`
	LeaseGeneration int64                    `json:"lease_generation"`
	LeaseToken      string                   `json:"lease_token"`
	LeaseExpiresAt  time.Time                `json:"lease_expires_at"`
	Recipe          hostingJobRecipe         `json:"recipe"`
	SecretRefs      []HostingSecretReference `json:"secret_references"`
}

func handleHostingAgentPoll(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	runner := r.Context().Value(hostingRunnerContextKey{}).(*HostingRunner)
	recovery, err := claimHostingRuntimeRecovery(r.Context(), runner.ID)
	if err == nil {
		jsonResponse(w, recovery)
		return
	}
	if err != errNoHostingJob {
		jsonErrorCode(w, errCodeInternal, "claim hosting runtime recovery failed", http.StatusInternalServerError)
		return
	}
	job, err := claimHostingJob(r.Context(), runner.ID)
	if err == errNoHostingJob {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "claim hosting job failed", http.StatusInternalServerError)
		return
	}
	jsonResponse(w, job)
}

func claimHostingJob(ctx context.Context, runnerID int64) (*hostingClaimedJob, error) {
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
	var jobID, deploymentID, projectID, generation int64
	var recipeJSON, secretsJSON string
	err = conn.QueryRowContext(ctx, `SELECT j.id, j.hosting_deployment_id, d.hosting_project_id,
		j.lease_generation, j.recipe_json, j.secret_refs_json FROM hosting_jobs j
		JOIN hosting_deployments d ON d.id=j.hosting_deployment_id
		JOIN hosting_projects p ON p.id=d.hosting_project_id
		JOIN hosting_runners r ON r.id=j.hosting_runner_id
		JOIN hosting_settings s ON s.id=1
		WHERE j.hosting_runner_id=? AND j.status='queued' AND j.cancel_requested_at IS NULL
		  AND p.desired_state='active' AND p.kill_switch_reason='' AND s.global_kill_switch=0
		  AND r.status='online' AND r.draining=0 ORDER BY j.id LIMIT 1`, runnerID).Scan(&jobID, &deploymentID, &projectID, &generation, &recipeJSON, &secretsJSON)
	if err == sql.ErrNoRows {
		return nil, errNoHostingJob
	}
	if err != nil {
		return nil, err
	}
	leaseToken, err := generateLeaseToken()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	expires := now.Add(hostingJobLeaseDuration)
	generation++
	result, err := conn.ExecContext(ctx, `UPDATE hosting_jobs SET status='leased', lease_generation=?, lease_token_hash=?,
		lease_expires_at=?, completion_fingerprint='', attempts=attempts+1,
		started_at=COALESCE(started_at, ?) WHERE id=? AND status='queued'`, generation,
		hashToken(leaseToken), formatSQLiteTime(expires), formatSQLiteTime(now), jobID)
	if err != nil {
		return nil, err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return nil, errNoHostingJob
	}
	if _, err := conn.ExecContext(ctx, `UPDATE hosting_deployments SET status='running', phase='fetching_source',
		started_at=COALESCE(started_at, ?), updated_at=? WHERE id=? AND status IN ('queued','running')`, formatSQLiteTime(now), formatSQLiteTime(now), deploymentID); err != nil {
		return nil, err
	}
	if err := recordHostingEvent(ctx, conn, projectID, deploymentID, "job_leased", hostingPhaseFetching, "", map[string]any{"runner_id": runnerID, "attempt": generation}, now); err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, err
	}
	committed = true
	var recipe hostingJobRecipe
	var secrets []HostingSecretReference
	if err := json.Unmarshal([]byte(recipeJSON), &recipe); err != nil {
		return nil, err
	}
	if recipe.Operation == "" {
		recipe.Operation = "build"
	}
	if err := json.Unmarshal([]byte(secretsJSON), &secrets); err != nil {
		return nil, err
	}
	if secrets == nil {
		secrets = make([]HostingSecretReference, 0)
	}
	return &hostingClaimedJob{JobID: jobID, LeaseGeneration: generation, LeaseToken: leaseToken, LeaseExpiresAt: expires, Recipe: recipe, SecretRefs: secrets}, nil
}

func handleHostingAgentJob(w http.ResponseWriter, r *http.Request) {
	jobID, suffix, ok := parseIDPath(r.URL.Path, "/api/hosting-agent/v1/jobs/")
	if !ok {
		jsonErrorCode(w, errCodeJobNotFound, "hosting job not found", http.StatusNotFound)
		return
	}
	switch suffix {
	case "source":
		handleHostingJobSource(w, r, jobID)
	case "release-artifact":
		handleHostingJobReleaseArtifact(w, r, jobID)
	case "heartbeat":
		handleHostingJobLeaseHeartbeat(w, r, jobID)
	case "phase":
		handleHostingJobPhase(w, r, jobID)
	case "logs":
		handleHostingJobLogs(w, r, jobID)
	case "complete":
		handleHostingJobComplete(w, r, jobID)
	default:
		jsonErrorCode(w, errCodeNotFound, "hosting job operation not found", http.StatusNotFound)
	}
}

func handleHostingJobReleaseArtifact(w http.ResponseWriter, r *http.Request, jobID int64) {
	if !requireMethod(w, r, http.MethodPut) {
		return
	}
	if mediaType := strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]); mediaType != "application/x-tar" {
		jsonErrorCode(w, errCodeUnsupportedMediaType, "release artifact must use application/x-tar", http.StatusUnsupportedMediaType)
		return
	}
	runner := r.Context().Value(hostingRunnerContextKey{}).(*HostingRunner)
	generation, token, ok := leaseCredentials(r)
	if !ok {
		jsonErrorCode(w, errCodeJobForbidden, "valid job lease is required", http.StatusForbidden)
		return
	}
	if _, _, err := authenticateHostingJobLease(r.Context(), runner.ID, jobID, generation, token); err != nil {
		jsonErrorCode(w, errCodeJobForbidden, "valid job lease is required", http.StatusForbidden)
		return
	}
	releaseDigest := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Deployer-Release-Digest")))
	artifactDigest := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Deployer-Artifact-Digest")))
	if !validSHA256Digest(releaseDigest) || !validSHA256Digest(artifactDigest) {
		jsonErrorCode(w, errCodeValidation, "valid release and artifact digests are required", http.StatusBadRequest)
		return
	}
	var maximumBytes int64
	var existingDigest, existingPath string
	var existingSize int64
	if err := db.QueryRowContext(r.Context(), `SELECT required_disk_bytes, release_upload_digest,
		release_upload_path, release_upload_size FROM hosting_jobs WHERE id=?`, jobID).Scan(
		&maximumBytes, &existingDigest, &existingPath, &existingSize); err != nil {
		jsonErrorCode(w, errCodeJobNotFound, "hosting job not found", http.StatusNotFound)
		return
	}
	if existingDigest != "" {
		if existingDigest == artifactDigest && existingPath != "" && existingSize > 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		jsonErrorCode(w, errCodeConflict, "a different release artifact is already attached to this lease", http.StatusConflict)
		return
	}
	if maximumBytes <= 0 || r.ContentLength <= 0 || r.ContentLength > maximumBytes {
		jsonErrorCode(w, errCodePayloadTooLarge, "release artifact exceeds the job disk limit", http.StatusRequestEntityTooLarge)
		return
	}
	if err := currentArtifactStorage().Ensure(); err != nil {
		jsonErrorCode(w, errCodeInternal, "prepare release artifact storage failed", http.StatusInternalServerError)
		return
	}
	path := managedArtifactPath("hosting-release-" + strings.TrimPrefix(artifactDigest, "sha256:") + ".tar")
	temporaryPath, output, err := prepareManagedArtifactUpload(path)
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "prepare release artifact upload failed", http.StatusInternalServerError)
		return
	}
	committed := false
	defer func() {
		if !committed {
			abortManagedArtifactUpload(temporaryPath)
		}
	}()
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(output, hash), io.LimitReader(r.Body, maximumBytes+1))
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil || written != r.ContentLength || written > maximumBytes {
		jsonErrorCode(w, errCodeArtifactUnavailable, "release artifact upload was incomplete", http.StatusBadRequest)
		return
	}
	actualDigest := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	if actualDigest != artifactDigest {
		jsonErrorCode(w, errCodeArtifactDigestMismatch, "release artifact digest mismatch", http.StatusConflict)
		return
	}
	if err := commitManagedArtifactUpload(temporaryPath, path); err != nil {
		jsonErrorCode(w, errCodeInternal, "commit release artifact upload failed", http.StatusInternalServerError)
		return
	}
	committed = true
	if err := attachHostingReleaseArtifact(r.Context(), runner.ID, jobID, generation, token,
		releaseDigest, artifactDigest, path, written); err != nil {
		var apiErr *hostingAPIError
		if errorsAsHosting(err, &apiErr) {
			writeHostingAPIError(w, apiErr)
		} else {
			jsonErrorCode(w, errCodeInternal, "attach release artifact failed", http.StatusInternalServerError)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func attachHostingReleaseArtifact(ctx context.Context, runnerID, jobID, generation int64, leaseToken,
	releaseDigest, artifactDigest, path string, size int64) error {
	now := formatSQLiteTime(time.Now().UTC())
	result, err := db.ExecContext(ctx, `UPDATE hosting_jobs SET release_upload_digest=?,
		release_upload_release_digest=?, release_upload_path=?, release_upload_size=?
		WHERE id=? AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=?
		AND status IN ('leased','running') AND lease_expires_at>? AND release_upload_digest=''`,
		artifactDigest, releaseDigest, path, size, jobID, runnerID, generation, hashToken(leaseToken), now)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected == 1 {
		return nil
	}
	var storedRelease, storedArtifact, storedPath, status, expires string
	var storedSize int64
	err = db.QueryRowContext(ctx, `SELECT release_upload_release_digest, release_upload_digest,
		release_upload_path, release_upload_size, status, COALESCE(lease_expires_at, '')
		FROM hosting_jobs WHERE id=? AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=?`,
		jobID, runnerID, generation, hashToken(leaseToken)).Scan(&storedRelease, &storedArtifact,
		&storedPath, &storedSize, &status, &expires)
	if err != nil || (status != "leased" && status != "running") || expires == "" || !parseSQLiteTime(expires).After(time.Now().UTC()) {
		return &hostingAPIError{Code: errCodeJobForbidden, Message: "hosting job lease changed during upload", StatusCode: http.StatusForbidden, Err: err}
	}
	if storedRelease == releaseDigest && storedArtifact == artifactDigest && storedPath == path && storedSize == size {
		return nil
	}
	return &hostingAPIError{Code: errCodeConflict, Message: "a different release artifact is already attached to this lease", StatusCode: http.StatusConflict}
}

func authenticateHostingJobLease(ctx context.Context, runnerID, jobID, generation int64, leaseToken string) (int64, int64, error) {
	var deploymentID, projectID int64
	err := db.QueryRowContext(ctx, `SELECT j.hosting_deployment_id, d.hosting_project_id FROM hosting_jobs j
		JOIN hosting_deployments d ON d.id=j.hosting_deployment_id
		WHERE j.id=? AND j.hosting_runner_id=? AND j.lease_generation=? AND j.lease_token_hash=?
		  AND j.status IN ('leased','running') AND j.lease_expires_at>?`, jobID, runnerID, generation,
		hashToken(leaseToken), formatSQLiteTime(time.Now().UTC())).Scan(&deploymentID, &projectID)
	return deploymentID, projectID, err
}

func leaseCredentials(r *http.Request) (int64, string, bool) {
	generation, err := strconv.ParseInt(r.Header.Get("X-Deployer-Lease-Generation"), 10, 64)
	token := strings.TrimSpace(r.Header.Get("X-Deployer-Lease-Token"))
	return generation, token, err == nil && generation > 0 && token != ""
}

func handleHostingJobSource(w http.ResponseWriter, r *http.Request, jobID int64) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	runner := r.Context().Value(hostingRunnerContextKey{}).(*HostingRunner)
	generation, token, ok := leaseCredentials(r)
	if !ok {
		jsonErrorCode(w, errCodeJobForbidden, "valid job lease is required", http.StatusForbidden)
		return
	}
	if _, _, err := authenticateHostingJobLease(r.Context(), runner.ID, jobID, generation, token); err != nil {
		jsonErrorCode(w, errCodeJobForbidden, "valid job lease is required", http.StatusForbidden)
		return
	}
	var path string
	if err := db.QueryRowContext(r.Context(), `SELECT source_artifact_path FROM hosting_jobs WHERE id=?`, jobID).Scan(&path); err != nil || !isManagedArtifactPath(path) {
		jsonErrorCode(w, errCodeArtifactUnavailable, "source artifact is unavailable", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/x-tar")
	if err := serveManagedArtifact(w, r, path); err != nil {
		logOperationalError("serve hosting source artifact", err)
	}
}

func handleHostingJobLeaseHeartbeat(w http.ResponseWriter, r *http.Request, jobID int64) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	runner := r.Context().Value(hostingRunnerContextKey{}).(*HostingRunner)
	generation, token, ok := leaseCredentials(r)
	if !ok {
		jsonErrorCode(w, errCodeJobForbidden, "valid job lease is required", http.StatusForbidden)
		return
	}
	result, err := db.ExecContext(r.Context(), `UPDATE hosting_jobs SET lease_expires_at=? WHERE id=? AND hosting_runner_id=?
		AND lease_generation=? AND lease_token_hash=? AND status IN ('leased','running') AND lease_expires_at>?`,
		formatSQLiteTime(time.Now().UTC().Add(hostingJobLeaseDuration)), jobID, runner.ID, generation, hashToken(token), formatSQLiteTime(time.Now().UTC()))
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "extend hosting job lease failed", http.StatusInternalServerError)
		return
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		jsonErrorCode(w, errCodeJobForbidden, "valid job lease is required", http.StatusForbidden)
		return
	}
	if _, err := db.ExecContext(r.Context(), `UPDATE hosting_runners SET status='online', last_seen=? WHERE id=?`, formatSQLiteTime(time.Now().UTC()), runner.ID); err != nil {
		jsonErrorCode(w, errCodeInternal, "update hosting runner liveness failed", http.StatusInternalServerError)
		return
	}
	var cancelRequested sql.NullString
	if err := db.QueryRowContext(r.Context(), `SELECT cancel_requested_at FROM hosting_jobs WHERE id=?`, jobID).Scan(&cancelRequested); err != nil {
		jsonErrorCode(w, errCodeInternal, "read hosting job cancellation failed", http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]bool{"cancel_requested": cancelRequested.Valid})
}

type hostingPhaseRequest struct {
	Phase string `json:"phase"`
}

func handleHostingJobPhase(w http.ResponseWriter, r *http.Request, jobID int64) {
	if !requireMethod(w, r, http.MethodPost) || !requireJSONContentType(w, r) {
		return
	}
	runner := r.Context().Value(hostingRunnerContextKey{}).(*HostingRunner)
	generation, token, ok := leaseCredentials(r)
	if !ok {
		jsonErrorCode(w, errCodeJobForbidden, "valid job lease is required", http.StatusForbidden)
		return
	}
	deploymentID, projectID, err := authenticateHostingJobLease(r.Context(), runner.ID, jobID, generation, token)
	if err != nil {
		jsonErrorCode(w, errCodeJobForbidden, "valid job lease is required", http.StatusForbidden)
		return
	}
	var input hostingPhaseRequest
	if !decodeInternalJSON(w, r, 4<<10, &input) {
		return
	}
	allowed := map[string]struct{}{hostingPhaseFetching: {}, hostingPhaseBuilding: {}, hostingPhaseStarting: {}, hostingPhaseHealth: {}}
	if _, ok := allowed[input.Phase]; !ok {
		jsonErrorCode(w, errCodeValidation, "unsupported hosting job phase", http.StatusBadRequest)
		return
	}
	now := time.Now().UTC()
	conn, err := db.Conn(r.Context())
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "update phase failed", http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	if _, err := conn.ExecContext(r.Context(), `BEGIN IMMEDIATE`); err != nil {
		jsonErrorCode(w, errCodeInternal, "update phase failed", http.StatusInternalServerError)
		return
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	var currentPhase string
	if err := conn.QueryRowContext(r.Context(), `SELECT phase FROM hosting_deployments WHERE id=? AND status='running'`, deploymentID).Scan(&currentPhase); err != nil {
		jsonErrorCode(w, errCodeConflict, "hosting deployment is no longer running", http.StatusConflict)
		return
	}
	if !validHostingPhaseTransition(currentPhase, input.Phase) {
		jsonErrorCode(w, errCodeConflict, "hosting job phase transition is out of order", http.StatusConflict)
		return
	}
	result, err := conn.ExecContext(r.Context(), `UPDATE hosting_jobs SET status='running', lease_expires_at=?
		WHERE id=? AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=?
		  AND status IN ('leased','running') AND lease_expires_at>?`, formatSQLiteTime(now.Add(hostingJobLeaseDuration)),
		jobID, runner.ID, generation, hashToken(token), formatSQLiteTime(now))
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "update phase failed", http.StatusInternalServerError)
		return
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		jsonErrorCode(w, errCodeJobForbidden, "valid job lease is required", http.StatusForbidden)
		return
	}
	if currentPhase != input.Phase {
		if _, err := conn.ExecContext(r.Context(), `UPDATE hosting_deployments SET phase=?, updated_at=? WHERE id=? AND status='running'`, input.Phase, formatSQLiteTime(now), deploymentID); err != nil {
			jsonErrorCode(w, errCodeInternal, "update phase failed", http.StatusInternalServerError)
			return
		}
		if _, err := conn.ExecContext(r.Context(), `INSERT INTO hosting_events (hosting_project_id, hosting_deployment_id, event_type, phase, metadata_json, created_at) VALUES (?, ?, 'phase_changed', ?, '{}', ?)`, projectID, deploymentID, input.Phase, formatSQLiteTime(now)); err != nil {
			jsonErrorCode(w, errCodeInternal, "record phase failed", http.StatusInternalServerError)
			return
		}
	}
	if _, err := conn.ExecContext(r.Context(), `COMMIT`); err != nil {
		jsonErrorCode(w, errCodeInternal, "update phase failed", http.StatusInternalServerError)
		return
	}
	committed = true
	w.WriteHeader(http.StatusNoContent)
}

func validHostingPhaseTransition(current, next string) bool {
	if current == next {
		return true
	}
	allowed := map[string]string{
		hostingPhaseFetching: hostingPhaseBuilding,
		hostingPhaseBuilding: hostingPhaseStarting,
		hostingPhaseStarting: hostingPhaseHealth,
	}
	return allowed[current] == next
}

type hostingLogRequest struct {
	Stream  string `json:"stream"`
	Message string `json:"message"`
}

func handleHostingJobLogs(w http.ResponseWriter, r *http.Request, jobID int64) {
	if !requireMethod(w, r, http.MethodPost) || !requireJSONContentType(w, r) {
		return
	}
	runner := r.Context().Value(hostingRunnerContextKey{}).(*HostingRunner)
	generation, token, ok := leaseCredentials(r)
	if !ok {
		jsonErrorCode(w, errCodeJobForbidden, "valid job lease is required", http.StatusForbidden)
		return
	}
	deploymentID, _, err := authenticateHostingJobLease(r.Context(), runner.ID, jobID, generation, token)
	if err != nil {
		jsonErrorCode(w, errCodeJobForbidden, "valid job lease is required", http.StatusForbidden)
		return
	}
	var input hostingLogRequest
	if !decodeInternalJSON(w, r, maxHostingLogChunkBytes, &input) {
		return
	}
	if input.Stream != "build" && input.Stream != "runtime" && input.Stream != "system" {
		jsonErrorCode(w, errCodeValidation, "invalid log stream", http.StatusBadRequest)
		return
	}
	input.Message = redactSecrets(input.Message)
	if len(input.Message) > maxHostingLogChunkBytes {
		input.Message = input.Message[len(input.Message)-maxHostingLogChunkBytes:]
	}
	result, err := db.ExecContext(r.Context(), `INSERT INTO hosting_logs (hosting_deployment_id, stream, message, created_at)
		SELECT hosting_deployment_id, ?, ?, ? FROM hosting_jobs WHERE id=? AND hosting_deployment_id=?
		  AND hosting_runner_id=? AND lease_generation=? AND lease_token_hash=?
		  AND status IN ('leased','running') AND lease_expires_at>?`, input.Stream, input.Message,
		formatSQLiteTime(time.Now().UTC()), jobID, deploymentID, runner.ID, generation, hashToken(token), formatSQLiteTime(time.Now().UTC()))
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "persist hosting log failed", http.StatusInternalServerError)
		return
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		jsonErrorCode(w, errCodeJobForbidden, "valid job lease is required", http.StatusForbidden)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type hostingCompletionRequest struct {
	Status                string         `json:"status"`
	FailureCode           string         `json:"failure_code,omitempty"`
	FailureMessage        string         `json:"failure_message,omitempty"`
	ReleaseDigest         string         `json:"release_digest,omitempty"`
	ReleaseArtifactDigest string         `json:"release_artifact_digest,omitempty"`
	RuntimeEndpoint       string         `json:"runtime_endpoint,omitempty"`
	HealthEvidence        map[string]any `json:"health_evidence,omitempty"`
}

func handleHostingJobComplete(w http.ResponseWriter, r *http.Request, jobID int64) {
	if !requireMethod(w, r, http.MethodPost) || !requireJSONContentType(w, r) {
		return
	}
	runner := r.Context().Value(hostingRunnerContextKey{}).(*HostingRunner)
	generation, token, ok := leaseCredentials(r)
	if !ok {
		jsonErrorCode(w, errCodeJobForbidden, "valid job lease is required", http.StatusForbidden)
		return
	}
	var input hostingCompletionRequest
	if !decodeInternalJSON(w, r, 32<<10, &input) {
		return
	}
	if err := completeHostingJob(r.Context(), runner.ID, jobID, generation, token, input); err != nil {
		writeHostingAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func validateRuntimeEndpoint(value string) error {
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("runtime_endpoint must be an absolute HTTP(S) URL without credentials, query, or fragment")
	}
	host := parsed.Hostname()
	ip := net.ParseIP(host)
	if ip == nil || (!ip.IsPrivate() && !ip.IsLoopback()) {
		return fmt.Errorf("runtime_endpoint host must be a private or loopback IP address")
	}
	return nil
}
