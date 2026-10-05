package app

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func setupRunnerOperationTest(t *testing.T) (*Runner, *Project) {
	t.Helper()
	withTempDB(t)
	if err := ensureRunnerOperationsSchema(); err != nil {
		t.Fatalf("ensure runner operation schema: %v", err)
	}
	runner := &Runner{Name: "operations-runner"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	if err := updateRunnerHeartbeat(runner.ID); err != nil {
		t.Fatalf("runner heartbeat: %v", err)
	}
	if err := saveRunnerTelemetry(runner.ID, RunnerTelemetry{DockerAvailable: true, DockerVersion: "test"}); err != nil {
		t.Fatalf("save telemetry: %v", err)
	}
	project := &Project{Name: "operations-project", RepoPath: t.TempDir(), ImageName: "operations-image", DeployDir: t.TempDir(), RunnerID: runner.ID}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	return runner, project
}

func TestRunnerOperationDockerComposeLifecycle(t *testing.T) {
	if err := exec.Command("docker", "version").Run(); err != nil {
		t.Skip("Docker unavailable")
	}
	runner, project := setupRunnerOperationTest(t)
	project.DeployDir = t.TempDir()
	project.ComposeFile = "compose.yaml"
	project.ComposeServices = "app"
	name := fmt.Sprintf("deployer-stage4-%d", time.Now().UnixNano())
	if err := os.WriteFile(filepath.Join(project.DeployDir, project.ComposeFile), []byte("name: "+name+"\nservices:\n  app:\n    image: alpine:3.21\n    command: [\"sh\", \"-c\", \"while true; do echo ready; sleep 1; done\"]\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := updateProject(project); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	t.Cleanup(func() {
		c := exec.Command("docker", "compose", "-f", project.ComposeFile, "down", "--volumes", "--remove-orphans")
		c.Dir = project.DeployDir
		_ = c.Run()
	})
	cmd := exec.CommandContext(ctx, "docker", "compose", "-f", project.ComposeFile, "up", "-d")
	cmd.Dir = project.DeployDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("start disposable compose: %v: %s", err, out)
	}
	psCmd := exec.Command("docker", "compose", "-f", project.ComposeFile, "ps", "-q", "app")
	psCmd.Dir = project.DeployDir
	containerID, err := psCmd.Output()
	if err != nil || strings.TrimSpace(string(containerID)) == "" {
		t.Fatalf("find disposable container: %v %q", err, containerID)
	}
	startedBefore, err := exec.Command("docker", "inspect", "-f", "{{.State.StartedAt}}", strings.TrimSpace(string(containerID))).Output()
	if err != nil {
		t.Fatal(err)
	}

	oldClient := agentControlClient
	t.Cleanup(func() { agentControlClient = oldClient })
	mux := http.NewServeMux()
	mux.HandleFunc("/api/agent/operations/poll", handleAgentOperationPoll)
	mux.HandleFunc("/api/agent/operations/complete/", handleAgentOperationComplete)
	server := httptest.NewServer(mux)
	defer server.Close()
	agentControlClient = server.Client()
	for _, kind := range []string{operationComposeStatus, operationComposeLogs, operationComposeRestart, operationComposeStop} {
		if _, err := queueRunnerOperation(runner.ID, project.ID, kind); err != nil {
			t.Fatalf("queue %s: %v", kind, err)
		}
		op, err := agentPollOperation(server.URL, runner.Token)
		if err != nil {
			t.Fatalf("poll %s: %v", kind, err)
		}
		executeRunnerOperation(server.URL, runner.Token, op)
		ops, err := listRunnerOperations(runner.ID, 1)
		if err != nil || len(ops) != 1 || ops[0].Status != "succeeded" {
			t.Fatalf("%s result: %+v err=%v", kind, ops, err)
		}
		if kind == operationComposeLogs && !strings.Contains(ops[0].Log, "ready") {
			t.Fatalf("expected disposable logs, got %q", ops[0].Log)
		}
		if kind == operationComposeRestart {
			startedAfter, err := exec.Command("docker", "inspect", "-f", "{{.State.StartedAt}}", strings.TrimSpace(string(containerID))).Output()
			if err != nil || string(startedAfter) == string(startedBefore) {
				t.Fatalf("restart not observed: before=%q after=%q err=%v", startedBefore, startedAfter, err)
			}
		}
		if kind == operationComposeStop {
			state, err := exec.Command("docker", "inspect", "-f", "{{.State.Status}}", strings.TrimSpace(string(containerID))).Output()
			if err != nil || strings.TrimSpace(string(state)) != "exited" {
				t.Fatalf("stop not observed: %q err=%v", state, err)
			}
		}
	}
}

func TestRunnerOperationEndToEndWithDisposableHealthCheck(t *testing.T) {
	runner, project := setupRunnerOperationTest(t)
	healthy := false
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !healthy {
			http.Error(w, "unhealthy", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer health.Close()
	project.HealthURL = health.URL
	if err := updateProject(project); err != nil {
		t.Fatalf("set health URL: %v", err)
	}
	op, err := queueRunnerOperation(runner.ID, project.ID, operationHealthCheck)
	if err != nil {
		t.Fatalf("queue operation: %v", err)
	}

	oldClient := agentControlClient
	t.Cleanup(func() { agentControlClient = oldClient })
	mux := http.NewServeMux()
	mux.HandleFunc("/api/agent/operations/poll", handleAgentOperationPoll)
	mux.HandleFunc("/api/agent/operations/complete/", handleAgentOperationComplete)
	server := httptest.NewServer(mux)
	defer server.Close()
	agentControlClient = server.Client()

	picked, err := agentPollOperation(server.URL, runner.Token)
	if err != nil {
		t.Fatalf("poll operation: %v", err)
	}
	if picked == nil || picked.ID != op.ID || picked.Kind != operationHealthCheck {
		t.Fatalf("unexpected operation: %+v", picked)
	}
	executeRunnerOperation(server.URL, runner.Token, picked)

	ops, err := listRunnerOperations(runner.ID, 5)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 1 || ops[0].Status != "failed" {
		t.Fatalf("expected unhealthy operation failure, got %+v", ops)
	}
	healthy = true
	if _, err := queueRunnerOperation(runner.ID, project.ID, operationHealthCheck); err != nil {
		t.Fatalf("queue recovered health operation: %v", err)
	}
	picked, err = agentPollOperation(server.URL, runner.Token)
	if err != nil {
		t.Fatalf("poll recovered operation: %v", err)
	}
	executeRunnerOperation(server.URL, runner.Token, picked)
	ops, err = listRunnerOperations(runner.ID, 5)
	if err != nil || len(ops) != 2 || ops[0].Status != "succeeded" || ops[0].ResultSummary != "completed" {
		t.Fatalf("unexpected completed operation: %+v", ops)
	}
}

func TestRunnerOperationBlocksDeploymentPollingUntilHandled(t *testing.T) {
	runner, project := setupRunnerOperationTest(t)
	if _, err := queueRunnerOperation(runner.ID, project.ID, operationComposeStatus); err != nil {
		t.Fatalf("queue operation: %v", err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/agent/poll", nil)
	request.Header.Set("Authorization", "Bearer "+runner.Token)
	recorder := httptest.NewRecorder()
	handleAgentPoll(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("expected operation queue to block legacy deployment poll, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestRunnerOperationRejectsProjectOnOtherRunner(t *testing.T) {
	runner, project := setupRunnerOperationTest(t)
	other := &Runner{Name: "other-runner"}
	if err := createRunner(other); err != nil {
		t.Fatalf("create other runner: %v", err)
	}
	if _, err := queueRunnerOperation(other.ID, project.ID, operationComposeRestart); err == nil {
		t.Fatal("expected project runner mismatch to be rejected")
	}
	if _, err := queueRunnerOperation(runner.ID, project.ID, "sh"); err == nil {
		t.Fatal("expected arbitrary operation to be rejected")
	}
}

func TestRunnerOperationRejectsLegacyHeartbeatWithoutTelemetry(t *testing.T) {
	withTempDB(t)
	if err := ensureRunnerOperationsSchema(); err != nil {
		t.Fatal(err)
	}
	runner := &Runner{Name: "legacy-agent"}
	if err := createRunner(runner); err != nil {
		t.Fatal(err)
	}
	if err := updateRunnerHeartbeat(runner.ID); err != nil {
		t.Fatal(err)
	}
	project := &Project{Name: "legacy-health", RepoPath: t.TempDir(), ImageName: "legacy", DeployDir: t.TempDir(), RunnerID: runner.ID, HealthURL: "https://example.test/health"}
	if err := createProject(project); err != nil {
		t.Fatal(err)
	}
	if _, err := queueRunnerOperation(runner.ID, project.ID, operationHealthCheck); err == nil {
		t.Fatal("legacy runner without operation telemetry must not receive an operation")
	}
}

func TestRunnerOperationPendingCancellation(t *testing.T) {
	runner, project := setupRunnerOperationTest(t)
	op, err := queueRunnerOperation(runner.ID, project.ID, operationComposeStop)
	if err != nil {
		t.Fatalf("queue operation: %v", err)
	}
	ok, err := cancelPendingRunnerOperation(op.ID)
	if err != nil || !ok {
		t.Fatalf("cancel pending operation: ok=%v err=%v", ok, err)
	}
	if ok, err := cancelPendingRunnerOperation(op.ID); err != nil || ok {
		t.Fatalf("cancel completed cancellation: ok=%v err=%v", ok, err)
	}
}

func TestRunnerOperationSurvivesControlServerSchemaRestart(t *testing.T) {
	runner, project := setupRunnerOperationTest(t)
	if _, err := queueRunnerOperation(runner.ID, project.ID, operationComposeStatus); err != nil {
		t.Fatalf("queue operation: %v", err)
	}
	op, err := claimRunnerOperation(t.Context(), runner.ID)
	if err != nil {
		t.Fatalf("claim operation: %v", err)
	}
	runnerOperationsSchema.Lock()
	runnerOperationsSchema.database = nil
	runnerOperationsSchema.Unlock()
	if err := ensureRunnerOperationsSchema(); err != nil {
		t.Fatalf("reopen operation schema: %v", err)
	}
	ops, err := listRunnerOperations(runner.ID, 5)
	if err != nil || len(ops) != 1 || ops[0].ID != op.ID || ops[0].Status != "running" {
		t.Fatalf("running operation was not preserved: %+v err=%v", ops, err)
	}
	if next, err := claimPendingDeploymentForRunner(t.Context(), runner.ID); err != sql.ErrNoRows || next != nil {
		t.Fatalf("expected active operation to keep deployment blocked, job=%+v err=%v", next, err)
	}
}
