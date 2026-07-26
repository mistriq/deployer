package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func hostingContractSection(t *testing.T, document, start, end string) string {
	t.Helper()
	startIndex := strings.Index(document, start)
	if startIndex < 0 {
		t.Fatalf("OpenAPI is missing section %q", start)
	}
	section := document[startIndex:]
	if end != "" {
		endIndex := strings.Index(section[len(start):], end)
		if endIndex < 0 {
			t.Fatalf("OpenAPI section %q is missing boundary %q", start, end)
		}
		section = section[:len(start)+endIndex]
	}
	return section
}

func TestOpenAPIDocumentsEveryHostingContractBoundary(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "docs", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	document := string(content)
	required := []string{
		"/api/internal/v1/projects/{externalProjectId}:",
		"/api/internal/v1/projects/{externalProjectId}/deployments:",
		"/api/internal/v1/projects/{externalProjectId}/releases:",
		"/api/internal/v1/projects/{externalProjectId}/rollback:",
		"/api/internal/v1/projects/{externalProjectId}/suspend:",
		"/api/internal/v1/projects/{externalProjectId}/resume:",
		"/api/internal/v1/projects/{externalProjectId}/kill-switch:",
		"/api/internal/v1/deployments/{externalDeploymentId}:",
		"/api/internal/v1/deployments/{externalDeploymentId}/cancel:",
		"/api/internal/v1/deployments/{externalDeploymentId}/events:",
		"/api/internal/v1/deployments/{externalDeploymentId}/logs:",
		"/api/internal/v1/capabilities:",
		"/api/internal/v1/runners:",
		"/api/internal/v1/metrics:",
		"/api/internal/v1/settings/kill-switch:",
		"/api/hosting-agent/v1/heartbeat:",
		"/api/hosting-agent/v1/session-retention:",
		"/api/hosting-agent/v1/poll:",
		"/api/hosting-agent/v1/jobs/{hostingJobId}/source:",
		"/api/hosting-agent/v1/jobs/{hostingJobId}/heartbeat:",
		"/api/hosting-agent/v1/jobs/{hostingJobId}/workload-identity:",
		"/api/hosting-agent/v1/jobs/{hostingJobId}/phase:",
		"/api/hosting-agent/v1/jobs/{hostingJobId}/logs:",
		"/api/hosting-agent/v1/jobs/{hostingJobId}/complete:",
		"/api/hosting-agent/v1/recoveries/{hostingRecoveryId}/artifact:",
		"/api/hosting-agent/v1/recoveries/{hostingRecoveryId}/heartbeat:",
		"/api/hosting-agent/v1/recoveries/{hostingRecoveryId}/workload-identity:",
		"/api/hosting-agent/v1/recoveries/{hostingRecoveryId}/logs:",
		"/api/hosting-agent/v1/recoveries/{hostingRecoveryId}/complete:",
		"Idempotency-Key", "X-Deployer-Lease-Generation", "X-Deployer-Lease-Token",
		"hosting:admin", "workload_policy_violation", "idempotency_conflict",
		"idempotency_in_progress", "HostingRollbackRelease", "Pending rollback receipts do not expire",
		"retry only the same", "completed 200 or terminal error replay", "ambiguous transport error or 5xx",
		"HostingRestoreJobRecipe", "release_artifact_url", "artifact_unavailable",
		"X-Deployer-Event-ID", "outside a five-minute window", "at-least-once",
		"source-broker-openapi.yaml", "HostingSourceReference", "source_reference",
		"secret-broker-openapi.yaml", "HostingWorkloadIdentity", "secret_reference_unavailable",
		"runtime-secrets-v1", "runtime-inventory-v1", "HostingObservedRuntime",
		"HostingPublicationMode", "publication_mode", "runtime_only_v1", "proxy_v1",
		"ineligible for new build", "sole published IPv4 binding", "runtime_endpoint",
		"runtime_instance_lost", "runtime_failure_code", "runtime_recovery_status",
		"runner_session_superseded", "runtime_instance_missing_observed",
		"runtime-adoptions.json", "cleanup_authorized", "512 KiB", "maxItems: 1024",
	}
	for _, value := range required {
		if !strings.Contains(document, value) {
			t.Errorf("OpenAPI is missing %q", value)
		}
	}
	phaseSection := hostingContractSection(t, document,
		"  /api/hosting-agent/v1/jobs/{hostingJobId}/phase:",
		"  /api/hosting-agent/v1/jobs/{hostingJobId}/logs:")
	for _, value := range []string{
		"fetching_source, building, starting_candidate,",
		"Repeating the current phase is idempotent",
		"Skips,", "regressions", "return 409",
		`        "204":`, `        "400":`, `        "401":`, `        "403":`,
		`        "405":`, `        "409":`, `        "413":`, `        "415":`, `        "500":`,
	} {
		if !strings.Contains(phaseSection, value) {
			t.Errorf("hosting phase OpenAPI contract is missing %q", value)
		}
	}
	cancelSection := hostingContractSection(t, document,
		"  /api/internal/v1/deployments/{externalDeploymentId}/cancel:",
		"  /api/internal/v1/deployments/{externalDeploymentId}/events:")
	for _, value := range []string{"queued job is atomically terminalized", "running/cancelling",
		"cancellation wins", "atomically persists a newer", "route-generation compensation",
		"routing intents to settle", "original accepted response snapshot", "Idempotency-Replayed=true",
		"authoritative", "current state"} {
		if !strings.Contains(cancelSection, value) {
			t.Errorf("hosting cancellation OpenAPI contract is missing %q", value)
		}
	}
	resumeSection := hostingContractSection(t, document,
		"  /api/internal/v1/projects/{externalProjectId}/resume:",
		"  /api/internal/v1/projects/{externalProjectId}/kill-switch:")
	for _, value := range []string{"exact authoritative active", "never a bare unsuspend",
		"remains suspended", "kill switch is enabled", "without admitting a resume route",
		"convergence may complete asynchronously"} {
		if !strings.Contains(resumeSection, value) {
			t.Errorf("project resume OpenAPI contract is missing %q", value)
		}
	}
	projectKillSection := hostingContractSection(t, document,
		"  /api/internal/v1/projects/{externalProjectId}/kill-switch:",
		"  /api/internal/v1/deployments/{externalDeploymentId}:")
	for _, value := range []string{"atomically cancels queued work", "newer",
		"generation-fenced proxy suspension", "neither the project nor global kill",
		"exact authoritative active", "rather than blindly exposing",
		"convergence may complete asynchronously"} {
		if !strings.Contains(projectKillSection, value) {
			t.Errorf("project kill-switch OpenAPI contract is missing %q", value)
		}
	}
	pollSection := hostingContractSection(t, document,
		"  /api/internal/v1/deployments/{externalDeploymentId}:",
		"  /api/internal/v1/deployments/{externalDeploymentId}/cancel:")
	for _, value := range []string{"authoritative when a", "callback_state=dead_letter",
		"immutable digests", "runtime_endpoint", "health_evidence",
		"deployment_not_found", "insufficient_scope"} {
		if !strings.Contains(pollSection, value) {
			t.Errorf("deployment polling OpenAPI contract is missing %q", value)
		}
	}
	releasesSection := hostingContractSection(t, document,
		"  /api/internal/v1/projects/{externalProjectId}/releases:",
		"  /api/internal/v1/projects/{externalProjectId}/rollback:")
	for _, value := range []string{"runner-authoritative endpoint", "health gating", "HostingRelease"} {
		if !strings.Contains(releasesSection, value) {
			t.Errorf("release-list OpenAPI contract is missing %q", value)
		}
	}
	callbackWebhook := hostingContractSection(t, document, "webhooks:", "components:")
	for _, value := range []string{"header Unix timestamp", "exact raw body", "refreshing the header",
		"Any 2xx", "Redirects", "attempt completion", "callback_payload_corrupt",
		"callback_identity_invalid", "older Deployer version", `"2XX"`} {
		if !strings.Contains(callbackWebhook, value) {
			t.Errorf("callback webhook OpenAPI contract is missing %q", value)
		}
	}
	callbackSchema := hostingContractSection(t, document, "    HostingCallback:", "    HostingMetrics:")
	for _, value := range []string{"additionalProperties: false", "HostingCallbackMetadata",
		"failure_message", "previous_release_digest", "status: {const: active}",
		"status: {const: failed}", "status: {const: cancelled}", "Immutable terminal-event time"} {
		if !strings.Contains(callbackSchema, value) {
			t.Errorf("HostingCallback schema is missing %q", value)
		}
	}
	for _, value := range []string{"x-new-admission-forbidden-pattern", "legacy stored values remain readable"} {
		if !strings.Contains(document, value) {
			t.Errorf("external identifier compatibility contract is missing %q", value)
		}
	}
	globalKillSection := hostingContractSection(t, document,
		"  /api/internal/v1/settings/kill-switch:",
		"  /api/hosting-agent/v1/heartbeat:")
	for _, value := range []string{"atomically cancels queued work", "one newer",
		"generation-fenced proxy suspension for every desired-active project",
		"have no project kill switch", "exact authoritative active",
		"blindly exposing", "convergence may complete asynchronously"} {
		if !strings.Contains(globalKillSection, value) {
			t.Errorf("global kill-switch OpenAPI contract is missing %q", value)
		}
	}
	jobHeartbeatSection := hostingContractSection(t, document,
		"  /api/hosting-agent/v1/jobs/{hostingJobId}/heartbeat:",
		"  /api/hosting-agent/v1/jobs/{hostingJobId}/workload-identity:")
	for _, value := range []string{"cancel_requested=true without extending the lease",
		"persist an exact cancelled completion", "terminalizes the deployment as cancelled",
		"current-generation routing fence"} {
		if !strings.Contains(jobHeartbeatSection, value) {
			t.Errorf("hosting job heartbeat cancellation contract is missing %q", value)
		}
	}
	recoveryHeartbeatSection := hostingContractSection(t, document,
		"  /api/hosting-agent/v1/recoveries/{hostingRecoveryId}/heartbeat:",
		"  /api/hosting-agent/v1/recoveries/{hostingRecoveryId}/workload-identity:")
	for _, value := range []string{"cancel_requested=true without", "persist an exact",
		"cancelled completion", "restores capacity", "current-generation routing fence"} {
		if !strings.Contains(recoveryHeartbeatSection, value) {
			t.Errorf("hosting recovery heartbeat cancellation contract is missing %q", value)
		}
	}
	completionSection := hostingContractSection(t, document,
		"  /api/hosting-agent/v1/jobs/{hostingJobId}/complete:",
		"  /api/hosting-agent/v1/recoveries/{hostingRecoveryId}/artifact:")
	for _, value := range []string{
		"initial success completion", "exact replay of an already durable staged",
		"existing activating intent", "health_checking",
		"current fenced", "exact match between the observed", "endpoint and completion runtime_endpoint",
		"without following redirects", "exact prior runtime endpoint",
		"fresh HTTP", "health check", "previous active release unchanged",
		"persisted candidate and activation intent", "retry the exact completion",
		`        "204":`, `        "400":`, `        "401":`, `        "403":`,
		`        "405":`, `        "409":`, `        "413":`, `        "415":`,
		`        "500":`, `        "502":`, `        "503":`,
	} {
		if !strings.Contains(completionSection, value) {
			t.Errorf("hosting completion OpenAPI contract is missing %q", value)
		}
	}
	deploymentSchema := hostingContractSection(t, document,
		"    HostingDeployment:", "    HostingRelease:")
	for _, value := range []string{
		"Stable status/phase pairs", "status: {const: queued}",
		"status: {const: running}", "status: {const: active}",
		"status: {const: failed}", "status: {const: cancelled}",
		"required: [release_digest, runtime_endpoint, health_evidence]",
		"enum: [queued, fetching_source, building, starting_candidate, health_checking, activating, cancelling]",
	} {
		if !strings.Contains(deploymentSchema, value) {
			t.Errorf("HostingDeployment schema is missing %q", value)
		}
	}
	if strings.Contains(deploymentSchema, "rolling_back") {
		t.Error("HostingDeployment phase must not advertise event-only rolling_back")
	}
	releaseSchema := hostingContractSection(t, document,
		"    HostingRelease:", "    HostingRollbackRelease:")
	if !strings.Contains(releaseSchema, "required: [runtime_endpoint, health_evidence]") {
		t.Error("HostingRelease schema must require runtime evidence after health gating")
	}
	eventSchema := hostingContractSection(t, document, "    HostingEvent:", "    HostingLog:")
	if !strings.Contains(eventSchema, "rolling_back") {
		t.Error("HostingEvent phase must retain rolling_back")
	}
	recoveryCompletionSection := hostingContractSection(t, document,
		"  /api/hosting-agent/v1/recoveries/{hostingRecoveryId}/complete:",
		"  /api/agent/poll:")
	for _, value := range []string{"exact restore-instance", "current fenced runner inventory session",
		"exact match between the observed endpoint", "fresh Deployer HTTP health check", "does not follow redirects",
		"generation-fenced recovery activation"} {
		if !strings.Contains(recoveryCompletionSection, value) {
			t.Errorf("hosting recovery completion contract is missing %q", value)
		}
	}
	readmeContent, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{
		"An exact retry while pending returns `409 idempotency_in_progress`",
		"Pending rollback receipts do not",
		"expire. Once reconciliation records a completed success or terminal error",
		"replays the completed response instead of creating another routing intent",
		"`starting_candidate`, and `health_checking` in order",
		"repeating the current",
		"initial successful", "accepted only from `health_checking`",
		"exact replay of an", "existing `activating` intent",
		"current fenced inventory session",
		"managed container's sole private port", "completion endpoint must match that observation exactly",
		"Inventory-less runners are ineligible",
		"fresh HTTP health gate that rejects redirects",
		"exact prior runtime endpoint",
		"Deployment cancellation is immediate and terminal for queued work",
		"cancel_requested=true", "without extending the lease",
		"Exact-key cancel replays return the original accepted",
		"Cancellation wins over stale",
		"atomically persists a newer route-generation compensation",
		"cannot become terminal until both",
		"one newer suspension intent per desired-active",
		"never a bare unsuspend", "no active release",
		"previous active",
		"release unchanged",
		"expired uncancelled leases are fenced and requeued",
		"repeated lease",
		"loss terminates with `runner_lost`",
		"pending activation is freshly revalidated",
		"ambiguous adapter `503` remains durable",
		"header Unix timestamp", "retry preserves the event ID and exact raw",
		"redirects are not followed", "immutable event enqueued by an older",
	} {
		if !strings.Contains(string(readmeContent), value) {
			t.Errorf("README is missing hosting contract %q", value)
		}
	}
	securityContent, err := os.ReadFile(filepath.Join("..", "..", "SECURITY.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"Redirects are deliberately not followed", "callback_identity_invalid",
		"Syntactically valid legacy IDs remain pollable"} {
		if !strings.Contains(string(securityContent), value) {
			t.Errorf("SECURITY is missing callback contract %q", value)
		}
	}
	runbookContent, err := os.ReadFile(filepath.Join("..", "..", "docs", "HOSTING_RUNBOOK.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"polling-only `dead_letter`", "exact legacy body",
		"after delivery or dead-letter"} {
		if !strings.Contains(string(runbookContent), value) {
			t.Errorf("hosting runbook is missing callback contract %q", value)
		}
	}
	brokerContract, err := os.ReadFile(filepath.Join("..", "..", "docs", "source-broker-openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"/api/internal/v1/source-artifacts/redeem:", "sourceBrokerBearer", "Authorization", "application/x-tar", "Content-Length", sourceRepositoryInstallationHeader, sourceRepositoryIDHeader, sourceRepositoryNameHeader, sourceCommitSHAHeader, sourceArtifactDigestHeader} {
		if !strings.Contains(string(brokerContract), value) {
			t.Errorf("source broker OpenAPI is missing %q", value)
		}
	}
	secretBrokerContract, err := os.ReadFile(filepath.Join("..", "..", "docs", "secret-broker-openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"/api/internal/v1/workload-secrets/redeem:", "workloadIdentityBearer", "WorkloadIdentityClaims", "secret_references_digest", "lease_generation", "time.RFC3339Nano", "application/json", "wli_"} {
		if !strings.Contains(string(secretBrokerContract), value) {
			t.Errorf("secret broker OpenAPI is missing %q", value)
		}
	}
}

func TestOpenAPIDocumentsNoBuildStaticRuntime(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "docs", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	section := hostingContractSection(t, string(content), "    HostingStaticRuntime:", "    HostingNodeRuntime:")
	if strings.Contains(section, "required: [kind, node_version, package_manager, build_script, output_directory]") {
		t.Fatal("OpenAPI still requires build fields for every static runtime")
	}
	for _, required := range []string{
		"required: [kind, output_directory]",
		"Omit together with build_script to publish output_directory directly without Node or a build stage.",
		"No-build static site served directly from the repository checkout",
		"required: [node_version, package_manager, build_script]",
		"(?:\\.|(?!\\.deployer",
	} {
		if !strings.Contains(section, required) {
			t.Errorf("no-build static OpenAPI schema is missing %q", required)
		}
	}
}

func TestHostingRunbookCoversServiceIdentityBackupAndRunnerRecovery(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "docs", "HOSTING_RUNBOOK.md"))
	if err != nil {
		t.Fatal(err)
	}
	document := string(content)
	for _, required := range []string{
		"## Service identities and rotation",
		"credential_generation",
		"sqlite3 /var/lib/deployer/deployer.db \".backup",
		"PRAGMA integrity_check",
		"## Service identity, stale-job, and runner-loss verification",
		"never paste a `dpl_` token",
		"Do not edit `hosting_jobs`",
		"`runner_lost` code",
		"fenced heartbeat and inventory session",
	} {
		if !strings.Contains(document, required) {
			t.Errorf("hosting runbook is missing %q", required)
		}
	}
}

func TestHostingOperationsDocumentationMatchesStructuredObservabilityContract(t *testing.T) {
	files := map[string][]string{
		"README.md": {
			"phase_duration_seconds", "stable `failure_codes`", "online\nschedulable runners", "pending/delivering callbacks",
		},
		"SECURITY.md": {
			"stable failure codes", "Pending and delivering callback rows", "inactive/failed release artifacts",
		},
		"CHANGELOG.md": {
			"deployment phase-duration timelines", "stable failure-code outcome metrics", "release archives",
		},
		"docs/openapi.yaml": {
			"per-phase elapsed durations", "stable failure-code counts", "online schedulable runner capacity",
		},
	}
	for path, required := range files {
		content, err := os.ReadFile(filepath.Join("..", "..", path))
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, value := range required {
			if !strings.Contains(string(content), value) {
				t.Errorf("%s is missing %q", path, value)
			}
		}
	}
}
