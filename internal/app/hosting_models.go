package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	hostingStatusQueued    = "queued"
	hostingStatusRunning   = "running"
	hostingStatusActive    = "active"
	hostingStatusFailed    = "failed"
	hostingStatusCancelled = "cancelled"
)

const (
	hostingPhaseQueued      = "queued"
	hostingPhaseFetching    = "fetching_source"
	hostingPhaseBuilding    = "building"
	hostingPhaseStarting    = "starting_candidate"
	hostingPhaseHealth      = "health_checking"
	hostingPhaseActivating  = "activating"
	hostingPhaseActive      = "active"
	hostingPhaseFailed      = "failed"
	hostingPhaseCancelling  = "cancelling"
	hostingPhaseCancelled   = "cancelled"
	hostingPhaseRollingBack = "rolling_back"
)

var hostingTerminalStatuses = map[string]struct{}{
	hostingStatusActive:    {},
	hostingStatusFailed:    {},
	hostingStatusCancelled: {},
}

type HostingDeployment struct {
	ID                    int64      `json:"-"`
	ExternalDeploymentID  string     `json:"external_deployment_id"`
	ExternalProjectID     string     `json:"external_project_id"`
	CommitSHA             string     `json:"commit_sha"`
	ManifestDigest        string     `json:"manifest_digest"`
	ArtifactDigest        string     `json:"artifact_digest"`
	Status                string     `json:"status"`
	Phase                 string     `json:"phase"`
	FailureCode           string     `json:"failure_code,omitempty"`
	FailureMessage        string     `json:"failure_message,omitempty"`
	ReleaseDigest         string     `json:"release_digest,omitempty"`
	PreviousRelease       string     `json:"previous_release_digest,omitempty"`
	PublicationMode       string     `json:"publication_mode"`
	RuntimeEndpoint       string     `json:"runtime_endpoint,omitempty"`
	HealthEvidence        any        `json:"health_evidence,omitempty"`
	RuntimeStatus         string     `json:"runtime_status,omitempty"`
	RuntimeFailureCode    string     `json:"runtime_failure_code,omitempty"`
	RuntimeRecoveryID     int64      `json:"runtime_recovery_id,omitempty"`
	RuntimeRecoveryStatus string     `json:"runtime_recovery_status,omitempty"`
	CallbackState         string     `json:"callback_state"`
	CancelRequestedAt     *time.Time `json:"cancel_requested_at,omitempty"`
	StartedAt             *time.Time `json:"started_at,omitempty"`
	FinishedAt            *time.Time `json:"finished_at,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
	LogReference          string     `json:"log_reference"`
	EventReference        string     `json:"event_reference"`
}

type HostingRelease struct {
	Digest               string     `json:"digest"`
	ExternalDeploymentID string     `json:"external_deployment_id"`
	CommitSHA            string     `json:"commit_sha"`
	ArtifactDigest       string     `json:"artifact_digest"`
	Status               string     `json:"status"`
	PublicationMode      string     `json:"publication_mode"`
	HealthEvidence       any        `json:"health_evidence,omitempty"`
	RouteRevision        string     `json:"route_revision,omitempty"`
	RuntimeEndpoint      string     `json:"runtime_endpoint,omitempty"`
	PreviousRelease      string     `json:"previous_release_digest,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
	ActivatedAt          *time.Time `json:"activated_at,omitempty"`
	DeactivatedAt        *time.Time `json:"deactivated_at,omitempty"`
}

type hostingMigration struct {
	id         string
	statements []string
	migrate    func(context.Context, *sql.Conn) error
}

func applyHostingMigrations() error {
	migrations := []hostingMigration{
		{
			id: "017_hosting_lifecycle",
			statements: []string{
				`ALTER TABLE hosting_projects ADD COLUMN desired_state TEXT NOT NULL DEFAULT 'active' CHECK (desired_state IN ('active', 'suspended'))`,
				`ALTER TABLE hosting_projects ADD COLUMN kill_switch_reason TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_projects ADD COLUMN runner_selector_json TEXT NOT NULL DEFAULT '{}'`,
				`ALTER TABLE hosting_deployments ADD COLUMN issuer_token_id INTEGER REFERENCES service_tokens(id) ON DELETE RESTRICT`,
				`ALTER TABLE hosting_deployments ADD COLUMN phase TEXT NOT NULL DEFAULT 'queued'`,
				`ALTER TABLE hosting_deployments ADD COLUMN failure_code TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_deployments ADD COLUMN failure_message TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_deployments ADD COLUMN release_digest TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_deployments ADD COLUMN previous_release_digest TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_deployments ADD COLUMN callback_state TEXT NOT NULL DEFAULT 'pending'`,
				`ALTER TABLE hosting_deployments ADD COLUMN request_hash TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_deployments ADD COLUMN cancel_requested_at DATETIME`,
				`ALTER TABLE hosting_deployments ADD COLUMN started_at DATETIME`,
				`ALTER TABLE hosting_deployments ADD COLUMN finished_at DATETIME`,
				`CREATE UNIQUE INDEX idx_hosting_deployments_one_active_work ON hosting_deployments(hosting_project_id) WHERE status IN ('queued', 'running')`,
				`CREATE INDEX idx_hosting_deployments_project_created ON hosting_deployments(hosting_project_id, created_at DESC)`,
			},
		},
		{
			id: "018_hosting_idempotency_events",
			statements: []string{
				`CREATE TABLE hosting_idempotency (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					issuer_token_id INTEGER NOT NULL REFERENCES service_tokens(id) ON DELETE RESTRICT,
					operation TEXT NOT NULL,
					idempotency_key TEXT NOT NULL,
					request_hash TEXT NOT NULL,
					response_status INTEGER,
					response_body TEXT,
					created_at DATETIME NOT NULL,
					expires_at DATETIME NOT NULL,
					UNIQUE (issuer_token_id, operation, idempotency_key)
				)`,
				`CREATE INDEX idx_hosting_idempotency_expiry ON hosting_idempotency(expires_at)`,
				`CREATE TABLE hosting_events (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					hosting_project_id INTEGER NOT NULL REFERENCES hosting_projects(id) ON DELETE RESTRICT,
					hosting_deployment_id INTEGER REFERENCES hosting_deployments(id) ON DELETE CASCADE,
					event_type TEXT NOT NULL,
					phase TEXT NOT NULL,
					failure_code TEXT NOT NULL DEFAULT '',
					metadata_json TEXT NOT NULL DEFAULT '{}',
					created_at DATETIME NOT NULL
				)`,
				`CREATE INDEX idx_hosting_events_deployment ON hosting_events(hosting_deployment_id, id)`,
			},
		},
		{
			id: "019_hosting_runners_jobs",
			statements: []string{
				`CREATE TABLE hosting_runners (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					name TEXT NOT NULL UNIQUE,
					token_hash TEXT NOT NULL UNIQUE,
					execution_class TEXT NOT NULL DEFAULT 'hosting' CHECK (execution_class = 'hosting'),
					labels_json TEXT NOT NULL DEFAULT '[]',
					protocol_version TEXT NOT NULL,
					manifest_versions_json TEXT NOT NULL DEFAULT '[]',
					runtime_versions_json TEXT NOT NULL DEFAULT '[]',
					capacity_cpu_millis INTEGER NOT NULL DEFAULT 0,
					capacity_ram_bytes INTEGER NOT NULL DEFAULT 0,
					capacity_disk_bytes INTEGER NOT NULL DEFAULT 0,
					capacity_pids INTEGER NOT NULL DEFAULT 0,
					free_cpu_millis INTEGER NOT NULL DEFAULT 0,
					free_ram_bytes INTEGER NOT NULL DEFAULT 0,
					free_disk_bytes INTEGER NOT NULL DEFAULT 0,
					free_pids INTEGER NOT NULL DEFAULT 0,
					reserve_cpu_millis INTEGER NOT NULL DEFAULT 0,
					reserve_ram_bytes INTEGER NOT NULL DEFAULT 0,
					reserve_disk_bytes INTEGER NOT NULL DEFAULT 0,
					reserve_pids INTEGER NOT NULL DEFAULT 0,
					draining INTEGER NOT NULL DEFAULT 0 CHECK (draining IN (0, 1)),
					status TEXT NOT NULL DEFAULT 'offline' CHECK (status IN ('online', 'offline')),
					last_seen DATETIME,
					created_at DATETIME NOT NULL
				)`,
				`CREATE TABLE hosting_jobs (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					hosting_deployment_id INTEGER NOT NULL UNIQUE REFERENCES hosting_deployments(id) ON DELETE CASCADE,
					hosting_runner_id INTEGER REFERENCES hosting_runners(id) ON DELETE RESTRICT,
					status TEXT NOT NULL CHECK (status IN ('queued', 'leased', 'running', 'succeeded', 'failed', 'cancelled')),
					recipe_json TEXT NOT NULL,
					secret_refs_json TEXT NOT NULL DEFAULT '[]',
					required_cpu_millis INTEGER NOT NULL,
					required_ram_bytes INTEGER NOT NULL,
					required_disk_bytes INTEGER NOT NULL,
					required_pids INTEGER NOT NULL,
					lease_generation INTEGER NOT NULL DEFAULT 0,
					lease_token_hash TEXT NOT NULL DEFAULT '',
					lease_expires_at DATETIME,
					attempts INTEGER NOT NULL DEFAULT 0,
					cancel_requested_at DATETIME,
					created_at DATETIME NOT NULL,
					started_at DATETIME,
					completed_at DATETIME
				)`,
				`CREATE INDEX idx_hosting_jobs_schedulable ON hosting_jobs(status, created_at)`,
				`CREATE INDEX idx_hosting_jobs_runner_lease ON hosting_jobs(hosting_runner_id, lease_expires_at)`,
			},
		},
		{
			id: "020_hosting_releases_callbacks",
			statements: []string{
				`CREATE TABLE hosting_releases (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					hosting_project_id INTEGER NOT NULL REFERENCES hosting_projects(id) ON DELETE RESTRICT,
					hosting_deployment_id INTEGER NOT NULL UNIQUE REFERENCES hosting_deployments(id) ON DELETE RESTRICT,
					release_digest TEXT NOT NULL UNIQUE,
					commit_sha TEXT NOT NULL,
					artifact_digest TEXT NOT NULL,
					status TEXT NOT NULL CHECK (status IN ('candidate', 'healthy', 'active', 'inactive', 'failed')),
					health_evidence_json TEXT NOT NULL DEFAULT '{}',
					route_revision TEXT NOT NULL DEFAULT '',
					previous_release_digest TEXT NOT NULL DEFAULT '',
					created_at DATETIME NOT NULL,
					activated_at DATETIME,
					deactivated_at DATETIME
				)`,
				`CREATE UNIQUE INDEX idx_hosting_releases_one_active ON hosting_releases(hosting_project_id) WHERE status='active'`,
				`CREATE INDEX idx_hosting_releases_project_created ON hosting_releases(hosting_project_id, created_at DESC)`,
				`CREATE TABLE callback_outbox (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					event_id TEXT NOT NULL UNIQUE,
					hosting_deployment_id INTEGER NOT NULL REFERENCES hosting_deployments(id) ON DELETE CASCADE,
					payload_json TEXT NOT NULL,
					payload_hash TEXT NOT NULL,
					status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'delivering', 'delivered', 'dead_letter')),
					attempts INTEGER NOT NULL DEFAULT 0,
					next_attempt_at DATETIME NOT NULL,
					last_error_code TEXT NOT NULL DEFAULT '',
					created_at DATETIME NOT NULL,
					delivered_at DATETIME
				)`,
				`CREATE INDEX idx_callback_outbox_pending ON callback_outbox(status, next_attempt_at)`,
			},
		},
		{
			id: "021_service_credentials_audit",
			statements: []string{
				`CREATE TABLE service_token_credentials (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					service_token_id INTEGER NOT NULL REFERENCES service_tokens(id) ON DELETE CASCADE,
					token_hash TEXT NOT NULL UNIQUE,
					created_at DATETIME NOT NULL,
					last_used_at DATETIME,
					expires_at DATETIME,
					revoked_at DATETIME
				)`,
				`INSERT INTO service_token_credentials (service_token_id, token_hash, created_at, last_used_at, revoked_at)
				 SELECT id, token_hash, created_at, last_used_at, revoked_at FROM service_tokens`,
				`CREATE INDEX idx_service_token_credentials_owner ON service_token_credentials(service_token_id, revoked_at, expires_at)`,
				`CREATE TABLE service_token_audit_events (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					service_token_id INTEGER REFERENCES service_tokens(id) ON DELETE SET NULL,
					credential_id INTEGER REFERENCES service_token_credentials(id) ON DELETE SET NULL,
					event_type TEXT NOT NULL,
					actor TEXT NOT NULL,
					request_id TEXT NOT NULL DEFAULT '',
					metadata_json TEXT NOT NULL DEFAULT '{}',
					created_at DATETIME NOT NULL
				)`,
				`CREATE INDEX idx_service_token_audit_token ON service_token_audit_events(service_token_id, id)`,
				`CREATE TABLE hosting_settings (
					id INTEGER PRIMARY KEY CHECK (id = 1),
					global_kill_switch INTEGER NOT NULL DEFAULT 0 CHECK (global_kill_switch IN (0, 1)),
					kill_switch_reason TEXT NOT NULL DEFAULT '',
					updated_at DATETIME NOT NULL
				)`,
				`INSERT INTO hosting_settings (id, global_kill_switch, kill_switch_reason, updated_at) VALUES (1, 0, '', strftime('%Y-%m-%dT%H:%M:%fZ','now'))`,
			},
		},
		{
			id: "022_hosting_release_runtime_endpoint",
			statements: []string{
				`ALTER TABLE hosting_releases ADD COLUMN runtime_endpoint TEXT NOT NULL DEFAULT ''`,
			},
		},
		{
			id: "023_callback_outbox_leases",
			statements: []string{
				`ALTER TABLE callback_outbox ADD COLUMN locked_at DATETIME`,
			},
		},
		{
			id: "024_hosting_job_source_artifact",
			statements: []string{
				`ALTER TABLE hosting_jobs ADD COLUMN source_artifact_path TEXT NOT NULL DEFAULT ''`,
				`CREATE TABLE hosting_logs (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					hosting_deployment_id INTEGER NOT NULL REFERENCES hosting_deployments(id) ON DELETE CASCADE,
					stream TEXT NOT NULL CHECK (stream IN ('build', 'runtime', 'system')),
					message TEXT NOT NULL,
					created_at DATETIME NOT NULL
				)`,
				`CREATE INDEX idx_hosting_logs_deployment ON hosting_logs(hosting_deployment_id, id)`,
			},
		},
		{
			id: "025_hosting_audit_events",
			statements: []string{
				`CREATE TABLE hosting_audit_events (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					issuer_token_id INTEGER REFERENCES service_tokens(id) ON DELETE SET NULL,
					hosting_project_id INTEGER REFERENCES hosting_projects(id) ON DELETE SET NULL,
					event_type TEXT NOT NULL,
					reason TEXT NOT NULL,
					request_id TEXT NOT NULL DEFAULT '',
					metadata_json TEXT NOT NULL DEFAULT '{}',
					created_at DATETIME NOT NULL
				)`,
				`CREATE INDEX idx_hosting_audit_created ON hosting_audit_events(created_at, id)`,
				`CREATE INDEX idx_hosting_audit_project ON hosting_audit_events(hosting_project_id, id)`,
			},
		},
		{
			id: "026_hosting_recovery_invariants",
			statements: []string{
				`ALTER TABLE hosting_runners ADD COLUMN reported_free_cpu_millis INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE hosting_runners ADD COLUMN reported_free_ram_bytes INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE hosting_runners ADD COLUMN reported_free_disk_bytes INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE hosting_runners ADD COLUMN reported_free_pids INTEGER NOT NULL DEFAULT 0`,
				`UPDATE hosting_runners SET reported_free_cpu_millis=free_cpu_millis,
					reported_free_ram_bytes=free_ram_bytes, reported_free_disk_bytes=free_disk_bytes,
					reported_free_pids=free_pids`,
				`ALTER TABLE callback_outbox ADD COLUMN claim_token_hash TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_jobs ADD COLUMN release_upload_digest TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_jobs ADD COLUMN release_upload_release_digest TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_jobs ADD COLUMN release_upload_path TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_jobs ADD COLUMN release_upload_size INTEGER NOT NULL DEFAULT 0`,
				`DELETE FROM callback_outbox WHERE id NOT IN (
					SELECT MIN(id) FROM callback_outbox GROUP BY hosting_deployment_id
				)`,
				`CREATE UNIQUE INDEX idx_callback_outbox_terminal_deployment ON callback_outbox(hosting_deployment_id)`,
				`CREATE TABLE hosting_proxy_operations (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					operation_id TEXT NOT NULL UNIQUE,
					hosting_project_id INTEGER NOT NULL REFERENCES hosting_projects(id) ON DELETE RESTRICT,
					hosting_deployment_id INTEGER REFERENCES hosting_deployments(id) ON DELETE RESTRICT,
					operation_type TEXT NOT NULL CHECK (operation_type IN ('activate','suspend','resume','rollback','compensate')),
					release_digest TEXT NOT NULL DEFAULT '',
					runtime_endpoint TEXT NOT NULL DEFAULT '',
					expected_previous_release_digest TEXT NOT NULL DEFAULT '',
					status TEXT NOT NULL CHECK (status IN ('pending','applied','committed','failed')),
					route_revision TEXT NOT NULL DEFAULT '',
					last_error_code TEXT NOT NULL DEFAULT '',
					created_at DATETIME NOT NULL,
					updated_at DATETIME NOT NULL
				)`,
				`CREATE INDEX idx_hosting_proxy_operations_reconcile ON hosting_proxy_operations(status, updated_at)`,
			},
		},
		{
			id: "027_hosting_release_artifacts",
			statements: []string{
				`ALTER TABLE hosting_releases RENAME TO hosting_releases_pre_artifacts`,
				`CREATE TABLE hosting_release_artifacts (
					artifact_digest TEXT PRIMARY KEY,
					release_digest TEXT NOT NULL,
					artifact_path TEXT NOT NULL UNIQUE,
					size_bytes INTEGER NOT NULL CHECK (size_bytes > 0),
					created_at DATETIME NOT NULL
				)`,
				`CREATE INDEX idx_hosting_release_artifacts_release ON hosting_release_artifacts(release_digest)`,
				`CREATE TABLE hosting_releases (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					hosting_project_id INTEGER NOT NULL REFERENCES hosting_projects(id) ON DELETE RESTRICT,
					hosting_deployment_id INTEGER NOT NULL UNIQUE REFERENCES hosting_deployments(id) ON DELETE RESTRICT,
					release_digest TEXT NOT NULL,
					release_artifact_digest TEXT REFERENCES hosting_release_artifacts(artifact_digest) ON DELETE RESTRICT,
					commit_sha TEXT NOT NULL,
					artifact_digest TEXT NOT NULL,
					status TEXT NOT NULL CHECK (status IN ('candidate', 'healthy', 'active', 'inactive', 'failed')),
					health_evidence_json TEXT NOT NULL DEFAULT '{}',
					route_revision TEXT NOT NULL DEFAULT '',
					previous_release_digest TEXT NOT NULL DEFAULT '',
					runtime_endpoint TEXT NOT NULL DEFAULT '',
					created_at DATETIME NOT NULL,
					activated_at DATETIME,
					deactivated_at DATETIME
				)`,
				`INSERT INTO hosting_releases
					(id, hosting_project_id, hosting_deployment_id, release_digest, commit_sha, artifact_digest,
					 status, health_evidence_json, route_revision, previous_release_digest, runtime_endpoint,
					 created_at, activated_at, deactivated_at)
				 SELECT id, hosting_project_id, hosting_deployment_id, release_digest, commit_sha, artifact_digest,
					status, health_evidence_json, route_revision, previous_release_digest, runtime_endpoint,
					created_at, activated_at, deactivated_at FROM hosting_releases_pre_artifacts`,
				`DROP TABLE hosting_releases_pre_artifacts`,
				`CREATE UNIQUE INDEX idx_hosting_releases_one_active ON hosting_releases(hosting_project_id) WHERE status='active'`,
				`CREATE INDEX idx_hosting_releases_project_created ON hosting_releases(hosting_project_id, created_at DESC)`,
				`CREATE INDEX idx_hosting_releases_content ON hosting_releases(release_digest)`,
			},
		},
		{
			id: "028_hosting_runtime_recovery",
			statements: []string{
				`ALTER TABLE hosting_releases ADD COLUMN runtime_runner_id INTEGER REFERENCES hosting_runners(id) ON DELETE RESTRICT`,
				`ALTER TABLE hosting_releases ADD COLUMN runtime_generation INTEGER NOT NULL DEFAULT 0`,
				`UPDATE hosting_releases SET runtime_runner_id=(SELECT hosting_runner_id FROM hosting_jobs
					WHERE hosting_jobs.hosting_deployment_id=hosting_releases.hosting_deployment_id)
					WHERE status IN ('healthy','active','inactive')`,
				`CREATE TABLE hosting_runtime_recoveries (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					hosting_release_id INTEGER NOT NULL REFERENCES hosting_releases(id) ON DELETE CASCADE,
					hosting_runner_id INTEGER REFERENCES hosting_runners(id) ON DELETE RESTRICT,
					status TEXT NOT NULL CHECK (status IN ('queued','leased','running','succeeded','failed','cancelled')),
					lease_generation INTEGER NOT NULL DEFAULT 0,
					lease_token_hash TEXT NOT NULL DEFAULT '',
					lease_expires_at DATETIME,
					attempts INTEGER NOT NULL DEFAULT 0,
					required_cpu_millis INTEGER NOT NULL,
					required_ram_bytes INTEGER NOT NULL,
					required_disk_bytes INTEGER NOT NULL,
					required_pids INTEGER NOT NULL,
					cancel_requested_at DATETIME,
					runtime_endpoint TEXT NOT NULL DEFAULT '',
					health_evidence_json TEXT NOT NULL DEFAULT '{}',
					created_at DATETIME NOT NULL,
					started_at DATETIME,
					completed_at DATETIME
				)`,
				`CREATE UNIQUE INDEX idx_hosting_runtime_recovery_one_active ON hosting_runtime_recoveries(hosting_release_id)
					WHERE status IN ('queued','leased','running')`,
				`CREATE INDEX idx_hosting_runtime_recovery_runner ON hosting_runtime_recoveries(hosting_runner_id, status, lease_expires_at)`,
				`ALTER TABLE hosting_proxy_operations RENAME TO hosting_proxy_operations_pre_recovery`,
				`CREATE TABLE hosting_proxy_operations (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					operation_id TEXT NOT NULL UNIQUE,
					hosting_project_id INTEGER NOT NULL REFERENCES hosting_projects(id) ON DELETE RESTRICT,
					hosting_deployment_id INTEGER REFERENCES hosting_deployments(id) ON DELETE RESTRICT,
					hosting_runtime_recovery_id INTEGER REFERENCES hosting_runtime_recoveries(id) ON DELETE RESTRICT,
					operation_type TEXT NOT NULL CHECK (operation_type IN ('activate','suspend','resume','rollback','compensate','recover')),
					release_digest TEXT NOT NULL DEFAULT '',
					runtime_endpoint TEXT NOT NULL DEFAULT '',
					expected_previous_release_digest TEXT NOT NULL DEFAULT '',
					status TEXT NOT NULL CHECK (status IN ('pending','applied','committed','failed')),
					route_revision TEXT NOT NULL DEFAULT '',
					last_error_code TEXT NOT NULL DEFAULT '',
					created_at DATETIME NOT NULL,
					updated_at DATETIME NOT NULL
				)`,
				`INSERT INTO hosting_proxy_operations
					(id, operation_id, hosting_project_id, hosting_deployment_id, operation_type, release_digest,
					 runtime_endpoint, expected_previous_release_digest, status, route_revision, last_error_code,
					 created_at, updated_at)
				 SELECT id, operation_id, hosting_project_id, hosting_deployment_id, operation_type, release_digest,
					runtime_endpoint, expected_previous_release_digest, status, route_revision, last_error_code,
					created_at, updated_at FROM hosting_proxy_operations_pre_recovery`,
				`DROP TABLE hosting_proxy_operations_pre_recovery`,
				`CREATE INDEX idx_hosting_proxy_operations_reconcile ON hosting_proxy_operations(status, updated_at)`,
			},
		},
		{
			id: "029_hosting_runtime_capabilities",
			statements: []string{
				`ALTER TABLE hosting_runners ADD COLUMN operation_capabilities_json TEXT NOT NULL DEFAULT '["build"]'`,
				`ALTER TABLE hosting_releases ADD COLUMN runtime_instance_id TEXT NOT NULL DEFAULT ''`,
			},
		},
		{
			id: "030_hosting_recovery_completion_fingerprint",
			statements: []string{
				`ALTER TABLE hosting_runtime_recoveries ADD COLUMN completion_fingerprint TEXT NOT NULL DEFAULT ''`,
			},
		},
		{
			id: "031_hosting_recovery_completion_receipts",
			statements: []string{
				`CREATE TABLE hosting_runtime_recovery_completion_receipts (
					hosting_runtime_recovery_id INTEGER NOT NULL REFERENCES hosting_runtime_recoveries(id) ON DELETE CASCADE,
					lease_generation INTEGER NOT NULL,
					hosting_runner_id INTEGER NOT NULL REFERENCES hosting_runners(id) ON DELETE RESTRICT,
					lease_token_hash TEXT NOT NULL,
					completion_fingerprint TEXT NOT NULL,
					accepted_at DATETIME NOT NULL,
					PRIMARY KEY (hosting_runtime_recovery_id, lease_generation)
				)`,
			},
		},
		{
			id: "032_hosting_job_completion_fingerprint",
			statements: []string{
				`ALTER TABLE hosting_jobs ADD COLUMN completion_fingerprint TEXT NOT NULL DEFAULT ''`,
			},
		},
		{
			id: "033_hosting_release_runtime_snapshot",
			statements: []string{
				`ALTER TABLE hosting_releases ADD COLUMN runtime_manifest_json TEXT NOT NULL DEFAULT '{}'`,
				`UPDATE hosting_releases SET runtime_manifest_json=COALESCE((
					SELECT json_extract(job.recipe_json, '$.runtime') FROM hosting_jobs job
					WHERE job.hosting_deployment_id=hosting_releases.hosting_deployment_id
				), '{}')`,
			},
		},
		{
			id: "034_hosting_proxy_previous_runtime",
			statements: []string{
				`ALTER TABLE hosting_proxy_operations ADD COLUMN expected_previous_runtime_endpoint TEXT NOT NULL DEFAULT ''`,
			},
		},
		{
			id: "035_hosting_proxy_route_generation",
			statements: []string{
				`ALTER TABLE hosting_projects ADD COLUMN route_generation INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE hosting_proxy_operations ADD COLUMN route_generation INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE hosting_proxy_operations ADD COLUMN desired_state TEXT NOT NULL DEFAULT ''`,
				`UPDATE hosting_proxy_operations SET route_generation=(SELECT COUNT(*)
					FROM hosting_proxy_operations earlier WHERE earlier.hosting_project_id=hosting_proxy_operations.hosting_project_id
					AND earlier.id<=hosting_proxy_operations.id)`,
				`UPDATE hosting_projects SET route_generation=COALESCE((SELECT MAX(operation.route_generation)
					FROM hosting_proxy_operations operation WHERE operation.hosting_project_id=hosting_projects.id), 0)`,
			},
		},
		{
			id: "036_hosting_runtime_inventory",
			statements: []string{
				`ALTER TABLE hosting_runners ADD COLUMN active_session_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_runners ADD COLUMN last_heartbeat_sequence INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE hosting_jobs ADD COLUMN runtime_observed_at DATETIME`,
				`ALTER TABLE hosting_jobs ADD COLUMN runtime_observed_session_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_jobs ADD COLUMN runtime_observed_release_digest TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_jobs ADD COLUMN runtime_observed_instance_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_releases ADD COLUMN runtime_observed_at DATETIME`,
				`ALTER TABLE hosting_releases ADD COLUMN runtime_observed_session_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_releases ADD COLUMN runtime_missing_since DATETIME`,
				`ALTER TABLE hosting_releases ADD COLUMN runtime_missing_observations INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE hosting_releases ADD COLUMN runtime_failure_code TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_runtime_recoveries ADD COLUMN runtime_owner_runner_id INTEGER REFERENCES hosting_runners(id) ON DELETE RESTRICT`,
				`ALTER TABLE hosting_runtime_recoveries ADD COLUMN allow_owner_runner INTEGER NOT NULL DEFAULT 0 CHECK (allow_owner_runner IN (0, 1))`,
				`ALTER TABLE hosting_runtime_recoveries ADD COLUMN runtime_observed_at DATETIME`,
				`ALTER TABLE hosting_runtime_recoveries ADD COLUMN runtime_observed_session_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_runtime_recoveries ADD COLUMN runtime_observed_release_digest TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_runtime_recoveries ADD COLUMN runtime_observed_instance_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_proxy_operations ADD COLUMN target_runtime_runner_id INTEGER REFERENCES hosting_runners(id) ON DELETE RESTRICT`,
				`ALTER TABLE hosting_proxy_operations ADD COLUMN target_runtime_instance_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_proxy_operations ADD COLUMN target_runtime_session_id TEXT NOT NULL DEFAULT ''`,
			},
		},
		{
			id: "037_hosting_runner_session_handoff",
			statements: []string{
				`CREATE TABLE IF NOT EXISTS hosting_runner_superseded_sessions (
					hosting_runner_id INTEGER NOT NULL REFERENCES hosting_runners(id) ON DELETE CASCADE,
					session_id TEXT NOT NULL CHECK (length(session_id)=48),
					superseded_at DATETIME NOT NULL,
					PRIMARY KEY (hosting_runner_id, session_id)
				)`,
			},
		},
		{
			id: "038_hosting_idempotency_operation_reference",
			statements: []string{
				`ALTER TABLE hosting_idempotency ADD COLUMN operation_reference TEXT NOT NULL DEFAULT ''`,
				`CREATE UNIQUE INDEX idx_hosting_idempotency_operation_reference
					ON hosting_idempotency(operation_reference) WHERE operation_reference<>''`,
			},
		},
		{
			id: "039_hosting_activation_previous_endpoint",
			statements: []string{
				`UPDATE hosting_proxy_operations SET expected_previous_runtime_endpoint=COALESCE((
					SELECT release.runtime_endpoint FROM hosting_releases release
					WHERE release.hosting_project_id=hosting_proxy_operations.hosting_project_id
					  AND release.release_digest=hosting_proxy_operations.expected_previous_release_digest
					  AND release.status='active' LIMIT 1
				), '')
				WHERE operation_type='activate' AND status IN ('pending','applied')
				  AND expected_previous_release_digest<>'' AND expected_previous_runtime_endpoint=''`,
			},
		},
		{
			id: "040_hosting_inventory_runtime_endpoint",
			statements: []string{
				`ALTER TABLE hosting_jobs ADD COLUMN runtime_observed_endpoint TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE hosting_runtime_recoveries ADD COLUMN runtime_observed_endpoint TEXT NOT NULL DEFAULT ''`,
				`UPDATE hosting_jobs SET runtime_observed_at=NULL, runtime_observed_session_id='',
					runtime_observed_release_digest='', runtime_observed_instance_id='', runtime_observed_endpoint=''`,
				`UPDATE hosting_runtime_recoveries SET runtime_observed_at=NULL, runtime_observed_session_id='',
					runtime_observed_release_digest='', runtime_observed_instance_id='', runtime_observed_endpoint=''`,
				`UPDATE hosting_releases SET runtime_observed_at=NULL, runtime_observed_session_id=''`,
			},
		},
		{
			id: "041_hosting_default_hostname",
			statements: []string{
				`ALTER TABLE hosting_projects ADD COLUMN default_hostname TEXT NOT NULL DEFAULT ''`,
			},
		},
		{
			id: "041_callback_recovery_and_retention",
			statements: []string{
				`ALTER TABLE callback_outbox ADD COLUMN finalized_at DATETIME`,
				`UPDATE callback_outbox SET finalized_at=delivered_at
					WHERE status='delivered' AND finalized_at IS NULL`,
				`UPDATE callback_outbox SET finalized_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
					WHERE status='dead_letter' AND finalized_at IS NULL`,
				`UPDATE callback_outbox SET status='pending', locked_at=NULL, claim_token_hash=''
					WHERE status='delivering' AND locked_at IS NULL`,
				`UPDATE hosting_deployments SET callback_state='dead_letter'
					WHERE status IN ('active','failed','cancelled') AND callback_state='pending'
					  AND NOT EXISTS (SELECT 1 FROM callback_outbox callback
					    WHERE callback.hosting_deployment_id=hosting_deployments.id)`,
			},
			migrate: quarantineLegacyUnsafeHostingCallbackIdentities,
		},
		{
			id: "042_hosting_publication_mode",
			statements: []string{
				`ALTER TABLE hosting_projects ADD COLUMN publication_mode TEXT NOT NULL DEFAULT 'proxy_v1'
					CHECK (publication_mode IN ('proxy_v1','runtime_only_v1'))`,
			},
		},
		{
			id: "043_hosting_runtime_only_rollbacks",
			statements: []string{
				`CREATE TABLE hosting_runtime_rollbacks (
					operation_id TEXT PRIMARY KEY,
					hosting_project_id INTEGER NOT NULL REFERENCES hosting_projects(id) ON DELETE RESTRICT,
					hosting_deployment_id INTEGER NOT NULL REFERENCES hosting_deployments(id) ON DELETE RESTRICT,
					hosting_release_id INTEGER NOT NULL REFERENCES hosting_releases(id) ON DELETE RESTRICT,
					expected_previous_release_digest TEXT NOT NULL DEFAULT '',
					target_runtime_runner_id INTEGER NOT NULL REFERENCES hosting_runners(id) ON DELETE RESTRICT,
					target_runtime_instance_id TEXT NOT NULL,
					target_runtime_session_id TEXT NOT NULL,
					target_runtime_endpoint TEXT NOT NULL,
					status TEXT NOT NULL CHECK (status IN ('pending','committed','failed')),
					last_error_code TEXT NOT NULL DEFAULT '',
					created_at DATETIME NOT NULL,
					updated_at DATETIME NOT NULL
				)`,
				`CREATE INDEX idx_hosting_runtime_rollbacks_pending
					ON hosting_runtime_rollbacks(status, updated_at)`,
				`CREATE TABLE hosting_runtime_stops (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					hosting_release_id INTEGER NOT NULL REFERENCES hosting_releases(id) ON DELETE RESTRICT,
					hosting_runner_id INTEGER NOT NULL REFERENCES hosting_runners(id) ON DELETE RESTRICT,
					runtime_instance_id TEXT NOT NULL,
					runner_session_id TEXT NOT NULL,
					requested_sequence INTEGER NOT NULL,
					status TEXT NOT NULL CHECK (status IN ('pending','committed')),
					reason TEXT NOT NULL DEFAULT '',
					created_at DATETIME NOT NULL,
					completed_at DATETIME
				)`,
				`CREATE UNIQUE INDEX idx_hosting_runtime_stops_one_pending
					ON hosting_runtime_stops(hosting_release_id) WHERE status='pending'`,
			},
		},
		{
			id: "044_hosting_observation_pages",
			statements: []string{
				`ALTER TABLE hosting_logs ADD COLUMN truncated INTEGER NOT NULL DEFAULT 0 CHECK (truncated IN (0, 1))`,
				`ALTER TABLE hosting_logs ADD COLUMN dropped_bytes INTEGER NOT NULL DEFAULT 0 CHECK (dropped_bytes >= 0)`,
				`CREATE TABLE hosting_stream_expiry_watermarks (
					hosting_deployment_id INTEGER NOT NULL REFERENCES hosting_deployments(id) ON DELETE CASCADE,
					stream TEXT NOT NULL CHECK (stream IN ('events','logs')),
					expired_through_id INTEGER NOT NULL CHECK (expired_through_id > 0),
					expired_through_at DATETIME NOT NULL,
					updated_at DATETIME NOT NULL,
					PRIMARY KEY (hosting_deployment_id, stream)
				)`,
			},
		},
	}

	for _, migration := range migrations {
		if err := applyHostingMigration(migration); err != nil {
			return err
		}
	}
	return nil
}

func applyHostingMigration(migration hostingMigration) error {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open migration connection: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("begin migration %s: %w", migration.id, err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()

	var applied int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE id=?`, migration.id).Scan(&applied); err != nil {
		return fmt.Errorf("check migration %s: %w", migration.id, err)
	}
	if applied != 0 {
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return fmt.Errorf("commit migration check %s: %w", migration.id, err)
		}
		committed = true
		return nil
	}
	for _, statement := range migration.statements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("apply migration %s: %w", migration.id, err)
		}
	}
	if migration.migrate != nil {
		if err := migration.migrate(ctx, conn); err != nil {
			return fmt.Errorf("apply migration %s data transform: %w", migration.id, err)
		}
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO schema_migrations (id, applied_at) VALUES (?, ?)`, migration.id, formatSQLiteTime(time.Now())); err != nil {
		return fmt.Errorf("record migration %s: %w", migration.id, err)
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("commit migration %s: %w", migration.id, err)
	}
	committed = true
	return nil
}

func quarantineLegacyUnsafeHostingCallbackIdentities(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, `SELECT callback.id, callback.hosting_deployment_id,
		project.external_project_id, deployment.external_deployment_id
		FROM callback_outbox callback
		JOIN hosting_deployments deployment ON deployment.id=callback.hosting_deployment_id
		JOIN hosting_projects project ON project.id=deployment.hosting_project_id
		WHERE callback.status IN ('pending','delivering') ORDER BY callback.id`)
	if err != nil {
		return err
	}
	type callbackIdentity struct {
		id, deploymentID                        int64
		externalProjectID, externalDeploymentID string
	}
	callbacks := make([]callbackIdentity, 0)
	for rows.Next() {
		var callback callbackIdentity
		if err := rows.Scan(&callback.id, &callback.deploymentID, &callback.externalProjectID,
			&callback.externalDeploymentID); err != nil {
			rows.Close()
			return err
		}
		callbacks = append(callbacks, callback)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	now := formatSQLiteTime(time.Now().UTC())
	for _, callback := range callbacks {
		if validHostingExternalIDForAdmission(callback.externalProjectID) &&
			validHostingExternalIDForAdmission(callback.externalDeploymentID) {
			continue
		}
		if _, err := conn.ExecContext(ctx, `UPDATE callback_outbox SET status='dead_letter',
			finalized_at=?, locked_at=NULL, claim_token_hash='', last_error_code='callback_identity_invalid'
			WHERE id=?`, now, callback.id); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `UPDATE hosting_deployments
			SET callback_state='dead_letter', updated_at=? WHERE id=?`, now, callback.deploymentID); err != nil {
			return err
		}
	}
	return nil
}

func scanHostingDeployment(scanner interface{ Scan(...any) error }) (*HostingDeployment, error) {
	var deployment HostingDeployment
	var cancelRequested, startedAt, finishedAt sql.NullString
	var createdAt, updatedAt string
	err := scanner.Scan(
		&deployment.ID,
		&deployment.ExternalDeploymentID,
		&deployment.ExternalProjectID,
		&deployment.CommitSHA,
		&deployment.ManifestDigest,
		&deployment.ArtifactDigest,
		&deployment.Status,
		&deployment.Phase,
		&deployment.FailureCode,
		&deployment.FailureMessage,
		&deployment.ReleaseDigest,
		&deployment.PreviousRelease,
		&deployment.CallbackState,
		&cancelRequested,
		&startedAt,
		&finishedAt,
		&createdAt,
		&updatedAt,
		&deployment.PublicationMode,
	)
	if err != nil {
		return nil, err
	}
	deployment.CancelRequestedAt = nullableSQLiteTime(cancelRequested)
	deployment.StartedAt = nullableSQLiteTime(startedAt)
	deployment.FinishedAt = nullableSQLiteTime(finishedAt)
	deployment.CreatedAt = parseSQLiteTime(createdAt)
	deployment.UpdatedAt = parseSQLiteTime(updatedAt)
	deployment.FailureMessage = redactSecrets(deployment.FailureMessage)
	deployment.LogReference = "/api/internal/v1/deployments/" + deployment.ExternalDeploymentID + "/logs"
	deployment.EventReference = "/api/internal/v1/deployments/" + deployment.ExternalDeploymentID + "/events"
	return &deployment, nil
}

func nullableSQLiteTime(value sql.NullString) *time.Time {
	if !value.Valid {
		return nil
	}
	parsed := parseSQLiteTime(value.String)
	return &parsed
}

func getHostingDeploymentByExternalID(ctx context.Context, externalID string) (*HostingDeployment, error) {
	deployment, err := scanHostingDeployment(db.QueryRowContext(ctx, `SELECT d.id, d.external_deployment_id, p.external_project_id,
		d.commit_sha, d.manifest_digest, d.artifact_digest, d.status, d.phase, d.failure_code,
		d.failure_message, d.release_digest, d.previous_release_digest, d.callback_state,
		d.cancel_requested_at, d.started_at, d.finished_at, d.created_at, d.updated_at, p.publication_mode
		FROM hosting_deployments d
		JOIN hosting_projects p ON p.id=d.hosting_project_id
		WHERE d.external_deployment_id=?`, externalID))
	if err != nil {
		return nil, err
	}
	var runtimeEndpoint, healthEvidenceJSON string
	err = db.QueryRowContext(ctx, `SELECT runtime_endpoint, health_evidence_json
		FROM hosting_releases WHERE hosting_deployment_id=? ORDER BY id DESC LIMIT 1`, deployment.ID).Scan(
		&runtimeEndpoint, &healthEvidenceJSON)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if err == nil {
		if runtimeEndpoint != "" {
			if err := validateRuntimeEndpoint(runtimeEndpoint); err != nil {
				return nil, fmt.Errorf("invalid persisted hosting runtime endpoint: %w", err)
			}
			deployment.RuntimeEndpoint = runtimeEndpoint
		}
		if err := decodeNonEmptyHealthEvidence(healthEvidenceJSON, &deployment.HealthEvidence); err != nil {
			return nil, fmt.Errorf("decode deployment health evidence: %w", err)
		}
	}
	if deployment.Status == hostingStatusActive {
		evidence, ok := deployment.HealthEvidence.(map[string]any)
		if deployment.RuntimeEndpoint == "" || !ok || !validHealthEvidence(evidence) {
			return nil, fmt.Errorf("active hosting deployment is missing validated runtime health evidence")
		}
	}
	if deployment.Status == hostingStatusActive {
		var releaseStatus, desiredState, killReason, runnerStatus, runtimeFailureCode string
		var globalKill int
		var activeRecoveryID, latestRecoveryID sql.NullInt64
		var runnerLastSeen, missingSince, latestRecoveryStatus sql.NullString
		err := db.QueryRowContext(ctx, `SELECT release.status, project.desired_state,
			project.kill_switch_reason, settings.global_kill_switch,
			COALESCE(runner.status, ''), runner.last_seen, release.runtime_failure_code,
			release.runtime_missing_since,
			(SELECT recovery.id FROM hosting_runtime_recoveries recovery
			 WHERE recovery.hosting_release_id=release.id AND recovery.status IN ('queued','leased','running')
			   AND recovery.cancel_requested_at IS NULL
			 ORDER BY recovery.id DESC LIMIT 1),
			(SELECT recovery.id FROM hosting_runtime_recoveries recovery
			 WHERE recovery.hosting_release_id=release.id ORDER BY recovery.id DESC LIMIT 1),
			(SELECT recovery.status FROM hosting_runtime_recoveries recovery
			 WHERE recovery.hosting_release_id=release.id ORDER BY recovery.id DESC LIMIT 1)
			FROM hosting_releases release
			JOIN hosting_projects project ON project.id=release.hosting_project_id
			JOIN hosting_settings settings ON settings.id=1
			LEFT JOIN hosting_runners runner ON runner.id=release.runtime_runner_id
			WHERE release.hosting_deployment_id=?
			ORDER BY release.id DESC LIMIT 1`, deployment.ID).Scan(&releaseStatus, &desiredState,
			&killReason, &globalKill, &runnerStatus, &runnerLastSeen, &runtimeFailureCode,
			&missingSince, &activeRecoveryID, &latestRecoveryID, &latestRecoveryStatus)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		runnerLive := runnerStatus == "online" && runnerLastSeen.Valid &&
			!parseSQLiteTime(runnerLastSeen.String).Before(time.Now().UTC().Add(-hostingRunnerStaleAfter))
		deployment.RuntimeFailureCode = runtimeFailureCode
		if latestRecoveryID.Valid {
			deployment.RuntimeRecoveryID = latestRecoveryID.Int64
			deployment.RuntimeRecoveryStatus = latestRecoveryStatus.String
		}
		switch {
		case desiredState != "active" || killReason != "" || globalKill != 0:
			deployment.RuntimeStatus = "unavailable"
		case activeRecoveryID.Valid:
			deployment.RuntimeStatus = "recovering"
		case releaseStatus == "failed" || !runnerLive:
			deployment.RuntimeStatus = "unavailable"
			if deployment.RuntimeFailureCode == "" && !runnerLive {
				deployment.RuntimeFailureCode = "runner_lost"
			}
		case missingSince.Valid:
			deployment.RuntimeStatus = "checking"
		case releaseStatus != "":
			deployment.RuntimeStatus = "available"
		}
	}
	return deployment, nil
}

func listHostingReleases(ctx context.Context, projectID int64) ([]HostingRelease, error) {
	rows, err := db.QueryContext(ctx, `SELECT r.release_digest, d.external_deployment_id, r.commit_sha,
		r.artifact_digest, r.status, r.health_evidence_json, r.route_revision, r.runtime_endpoint,
		r.previous_release_digest, r.created_at, r.activated_at, r.deactivated_at, p.publication_mode
		FROM hosting_releases r
		JOIN hosting_deployments d ON d.id=r.hosting_deployment_id
		JOIN hosting_projects p ON p.id=r.hosting_project_id
		WHERE r.hosting_project_id=? ORDER BY r.id DESC`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]HostingRelease, 0)
	for rows.Next() {
		var release HostingRelease
		var evidence, createdAt string
		var activatedAt, deactivatedAt sql.NullString
		if err := rows.Scan(&release.Digest, &release.ExternalDeploymentID, &release.CommitSHA,
			&release.ArtifactDigest, &release.Status, &evidence, &release.RouteRevision, &release.RuntimeEndpoint,
			&release.PreviousRelease, &createdAt, &activatedAt, &deactivatedAt, &release.PublicationMode); err != nil {
			return nil, err
		}
		if err := decodeNonEmptyHealthEvidence(evidence, &release.HealthEvidence); err != nil {
			return nil, fmt.Errorf("decode health evidence: %w", err)
		}
		if release.RuntimeEndpoint != "" {
			if err := validateRuntimeEndpoint(release.RuntimeEndpoint); err != nil {
				return nil, fmt.Errorf("invalid persisted hosting runtime endpoint: %w", err)
			}
		}
		release.CreatedAt = parseSQLiteTime(createdAt)
		release.ActivatedAt = nullableSQLiteTime(activatedAt)
		release.DeactivatedAt = nullableSQLiteTime(deactivatedAt)
		if release.Status == "healthy" || release.Status == "active" || release.Status == "inactive" {
			evidence, ok := release.HealthEvidence.(map[string]any)
			if release.RuntimeEndpoint == "" || !ok || !validHealthEvidence(evidence) {
				return nil, fmt.Errorf("%s hosting release is missing validated runtime health evidence", release.Status)
			}
		}
		if release.Status == "active" {
			if release.ActivatedAt == nil {
				return nil, fmt.Errorf("active hosting release is missing activation time")
			}
			switch release.PublicationMode {
			case hostingPublicationProxyV1:
				if release.RouteRevision == "" {
					return nil, fmt.Errorf("active proxy release is missing route revision")
				}
			case hostingPublicationRuntimeOnlyV1:
				if release.RouteRevision != "" {
					return nil, fmt.Errorf("active runtime-only release has a route revision")
				}
			default:
				return nil, fmt.Errorf("active hosting release has invalid publication mode %q", release.PublicationMode)
			}
		}
		result = append(result, release)
	}
	return result, rows.Err()
}

func decodeNonEmptyHealthEvidence(encoded string, destination *any) error {
	var evidence map[string]any
	if err := json.Unmarshal([]byte(encoded), &evidence); err != nil {
		return err
	}
	if len(evidence) != 0 {
		*destination = evidence
	}
	return nil
}

func isHostingTerminalStatus(status string) bool {
	_, ok := hostingTerminalStatuses[status]
	return ok
}

var errHostingStateConflict = errors.New("hosting state conflict")
var errHostingProxyOperationSuperseded = errors.New("hosting proxy operation superseded")
var errHostingCallbackEnqueue = errors.New("hosting callback enqueue failed")
