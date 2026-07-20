package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
		"/api/hosting-agent/v1/poll:",
		"/api/hosting-agent/v1/jobs/{hostingJobId}/source:",
		"/api/hosting-agent/v1/jobs/{hostingJobId}/heartbeat:",
		"/api/hosting-agent/v1/jobs/{hostingJobId}/phase:",
		"/api/hosting-agent/v1/jobs/{hostingJobId}/logs:",
		"/api/hosting-agent/v1/jobs/{hostingJobId}/complete:",
		"/api/hosting-agent/v1/recoveries/{hostingRecoveryId}/artifact:",
		"/api/hosting-agent/v1/recoveries/{hostingRecoveryId}/heartbeat:",
		"/api/hosting-agent/v1/recoveries/{hostingRecoveryId}/logs:",
		"/api/hosting-agent/v1/recoveries/{hostingRecoveryId}/complete:",
		"Idempotency-Key", "X-Deployer-Lease-Generation", "X-Deployer-Lease-Token",
		"hosting:admin", "workload_policy_violation", "idempotency_conflict",
		"HostingRestoreJobRecipe", "release_artifact_url", "artifact_unavailable",
		"X-Deployer-Event-ID", "outside a five-minute window", "at-least-once",
	}
	for _, value := range required {
		if !strings.Contains(document, value) {
			t.Errorf("OpenAPI is missing %q", value)
		}
	}
}
