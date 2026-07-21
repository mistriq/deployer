package app

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type hostingAgentConfig struct {
	ServerURL               string
	Token                   string
	WorkRoot                string
	RuntimeBindAddress      string
	BuildNetwork            string
	BuildTimeout            time.Duration
	SecretBrokerURL         string
	SecretBrokerTimeout     time.Duration
	SecretMemoryRoot        string
	SessionID               string
	PriorAcceptedSessionIDs []string
	HeartbeatSequence       *atomic.Int64
	SessionAccepted         *atomic.Bool
	HeartbeatMu             *sync.Mutex
	Draining                bool
	Labels                  []string
}

var errHostingAgentSessionSuperseded = errors.New("hosting agent session was superseded")
var errHostingAgentSessionDraining = errors.New("hosting agent superseded session is draining")

type hostingAgentAPIError struct {
	StatusCode        int
	Code              string
	Message           string
	CleanupAuthorized bool
	RetainedReleases  []hostingRetainedRelease
}

func (err *hostingAgentAPIError) Error() string {
	return fmt.Sprintf("hosting agent API returned HTTP %d (%s): %s", err.StatusCode, err.Code, err.Message)
}

func runHostingAgent() {
	flags := flag.NewFlagSet("hosting-agent", flag.ExitOnError)
	serverURL := flags.String("server", os.Getenv("DEPLOYER_SERVER"), "Deployer server URL")
	token := flags.String("token", os.Getenv("DEPLOYER_TOKEN"), "Dedicated hosting runner token")
	workRoot := flags.String("work-root", getenvDefault("DEPLOYER_HOSTING_AGENT_WORK_ROOT", filepath.Join(os.TempDir(), "deployer-hosting-agent")), "Hosting runner work directory")
	runtimeBindAddress := flags.String("runtime-bind-address", getenvDefault("DEPLOYER_HOSTING_AGENT_RUNTIME_BIND_ADDRESS", "127.0.0.1"), "Private IPv4 address used for candidate ports")
	buildNetwork := flags.String("build-network", os.Getenv("DEPLOYER_HOSTING_AGENT_BUILD_NETWORK"), "Docker network restricted to approved package mirrors")
	buildTimeout := flags.Duration("build-timeout", getenvDurationDefault("DEPLOYER_HOSTING_AGENT_BUILD_TIMEOUT", 15*time.Minute), "Maximum customer build duration")
	secretBrokerURL := flags.String("secret-broker", os.Getenv("DEPLOYER_HOSTING_SECRET_BROKER_URL"), "Private workload secret broker URL")
	secretBrokerTimeout := flags.Duration("secret-broker-timeout", getenvDurationDefault("DEPLOYER_HOSTING_SECRET_BROKER_TIMEOUT", 15*time.Second), "Workload secret redemption timeout")
	draining := flags.Bool("draining", getenvBoolDefault("DEPLOYER_HOSTING_AGENT_DRAINING", false), "Advertise drain state and do not claim new work")
	labels := flags.String("labels", getenvDefault("DEPLOYER_HOSTING_AGENT_LABELS", "linux,hosting"), "Comma-separated hosting runner labels")
	flags.Parse(os.Args[2:])
	if *serverURL == "" || *token == "" || !filepath.IsAbs(*workRoot) {
		fmt.Fprintln(os.Stderr, "Usage: deployer hosting-agent --server URL --token TOKEN --work-root ABSOLUTE_PATH")
		os.Exit(1)
	}
	configuredLabels := normalizeStringList(strings.Split(*labels, ","))
	if len(configuredLabels) == 0 {
		logFatal("hosting_agent_config_error", "at least one runner label is required", nil, nil)
	}
	*serverURL = strings.TrimRight(*serverURL, "/")
	if err := validateServerURL(*serverURL); err != nil {
		logFatal("hosting_agent_config_error", "invalid server URL", err, nil)
	}
	if err := validateHostingRuntimeBindAddress(*runtimeBindAddress); err != nil {
		logFatal("hosting_agent_config_error", "invalid runtime bind address", err, nil)
	}
	if !validHostingDockerObjectName(*buildNetwork) || *buildTimeout < time.Minute || *buildTimeout > time.Hour {
		logFatal("hosting_agent_config_error", "build network is required and build timeout must be between 1m and 1h", nil, nil)
	}
	*secretBrokerURL = strings.TrimRight(strings.TrimSpace(*secretBrokerURL), "/")
	if *secretBrokerURL != "" {
		parsed, err := url.Parse(*secretBrokerURL)
		if err != nil || !validPrivateServiceURL(parsed) || (parsed.Path != "" && parsed.Path != "/") || *secretBrokerTimeout <= 0 || *secretBrokerTimeout > time.Minute {
			logFatal("hosting_agent_config_error", "secret broker must use HTTPS (or loopback HTTP) and timeout must be at most one minute", err, nil)
		}
	}
	if err := os.MkdirAll(*workRoot, 0750); err != nil {
		logFatal("hosting_agent_config_error", "prepare work root", err, nil)
	}
	canonicalWorkRoot, err := canonicalHostingAgentWorkRoot(*workRoot)
	if err != nil {
		logFatal("hosting_agent_config_error", "resolve work root", err, nil)
	}
	agentLock, err := acquireHostingAgentInstanceLock(canonicalWorkRoot)
	if err != nil {
		logFatal("hosting_agent_config_error", "acquire exclusive work-root ownership", err, nil)
	}
	defer agentLock.Close()
	if err := requireHostingContainerRuntime(*buildNetwork); err != nil {
		logFatal("hosting_agent_runtime_error", "container runtime does not satisfy hosting policy", err, nil)
	}
	secretMemoryRoot := hostingAgentSecretMemoryRoot(canonicalWorkRoot)
	if err := reconcileHostingSecretDirectories(secretMemoryRoot, *secretBrokerURL != ""); err != nil {
		logFatal("hosting_agent_runtime_error", "reconcile runtime secret directories", err, nil)
	}
	config := hostingAgentConfig{ServerURL: *serverURL, Token: *token, WorkRoot: canonicalWorkRoot, RuntimeBindAddress: *runtimeBindAddress, BuildNetwork: *buildNetwork, BuildTimeout: *buildTimeout,
		SecretBrokerURL: *secretBrokerURL, SecretBrokerTimeout: *secretBrokerTimeout, SecretMemoryRoot: secretMemoryRoot,
		SessionID: generateToken(), HeartbeatSequence: new(atomic.Int64), SessionAccepted: new(atomic.Bool),
		HeartbeatMu: new(sync.Mutex), Draining: *draining}
	config.Labels = configuredLabels
	config.PriorAcceptedSessionIDs, err = loadHostingAcceptedSessions(canonicalWorkRoot)
	if err != nil {
		logFatal("hosting_agent_runtime_error", "load accepted runner session", err, nil)
	}
	for {
		if err := flushHostingAgentCompletions(context.Background(), config); err != nil {
			logOperationalError("retry hosting job completions", err)
		}
		if err := sendHostingAgentHeartbeat(context.Background(), config); err != nil {
			if errors.Is(err, errHostingAgentSessionDraining) {
				if config.SessionAccepted != nil && config.SessionAccepted.Load() {
					config.PriorAcceptedSessionIDs = appendUniqueHostingSession(
						config.PriorAcceptedSessionIDs, config.SessionID)
					config.SessionID = generateToken()
					config.HeartbeatSequence.Store(0)
					config.SessionAccepted.Store(false)
				}
				logOperationalInfo("superseded hosting runner session is retaining routed runtimes until handoff")
				time.Sleep(5 * time.Second)
				continue
			}
			if errors.Is(err, errHostingAgentSessionSuperseded) {
				logOperationalInfo("superseded hosting runner session completed autonomous runtime cleanup")
				return
			}
			logOperationalError("hosting runner heartbeat", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if config.Draining {
			time.Sleep(5 * time.Second)
			continue
		}
		job, err := pollHostingAgentJob(context.Background(), config)
		if err != nil {
			logOperationalError("hosting runner poll", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if job == nil {
			time.Sleep(2 * time.Second)
			continue
		}
		executeHostingAgentJob(config, job)
	}
}

func canonicalHostingAgentWorkRoot(workRoot string) (string, error) {
	absoluteRoot, err := filepath.Abs(workRoot)
	if err != nil {
		return "", fmt.Errorf("resolve absolute work root: %w", err)
	}
	canonicalRoot, err := filepath.EvalSymlinks(absoluteRoot)
	if err != nil {
		return "", fmt.Errorf("resolve work-root symlinks: %w", err)
	}
	info, err := os.Stat(canonicalRoot)
	if err != nil {
		return "", fmt.Errorf("inspect work root: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("work root is not a directory")
	}
	return filepath.Clean(canonicalRoot), nil
}

func hostingAgentSecretMemoryRoot(workRoot string) string {
	digest := sha256.Sum256([]byte(filepath.Clean(workRoot)))
	return filepath.Join("/dev/shm/deployer-hosting-secrets", hex.EncodeToString(digest[:16]))
}

func hostingAgentSecretNamespace(secretRoot string) string {
	digest := sha256.Sum256([]byte(filepath.Clean(secretRoot)))
	return hex.EncodeToString(digest[:16])
}

func hostingAgentRuntimeNamespace(workRoot string) string {
	digest := sha256.Sum256([]byte(filepath.Clean(workRoot)))
	return hex.EncodeToString(digest[:16])
}

func requireHostingContainerRuntime(buildNetwork string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Version}}").CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) == "" {
		return fmt.Errorf("Docker engine is required: %w", err)
	}
	output, err = exec.CommandContext(ctx, "docker", "network", "inspect", "--format", `{{.Internal}} {{index .Labels "light-apps.hosting.restricted-egress"}}`, buildNetwork).CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "true true" {
		return fmt.Errorf("build network must be internal and labeled light-apps.hosting.restricted-egress=true")
	}
	return nil
}

func validHostingDockerObjectName(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for index, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '_' || character == '-' || (character == '.' && index > 0) {
			continue
		}
		return false
	}
	return true
}

func detectHostingAgentCapacity(workRoot string) (capacityDTO, capacityDTO, error) {
	capacity := capacityDTO{CPUMillis: int64(runtime.NumCPU()) * 1000}
	free := capacityDTO{CPUMillis: capacity.CPUMillis}
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return capacityDTO{}, capacityDTO{}, err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		value, _ := strconv.ParseInt(fields[1], 10, 64)
		switch fields[0] {
		case "MemTotal:":
			capacity.RAMBytes = value << 10
		case "MemAvailable:":
			free.RAMBytes = value << 10
		}
	}
	file.Close()
	if err := scanner.Err(); err != nil {
		return capacityDTO{}, capacityDTO{}, err
	}
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(workRoot, &filesystem); err != nil {
		return capacityDTO{}, capacityDTO{}, err
	}
	capacity.DiskBytes = int64(filesystem.Blocks) * int64(filesystem.Bsize)
	free.DiskBytes = int64(filesystem.Bavail) * int64(filesystem.Bsize)
	pidMax, err := os.ReadFile("/proc/sys/kernel/pid_max")
	if err != nil {
		return capacityDTO{}, capacityDTO{}, err
	}
	capacity.PIDs, err = strconv.ParseInt(strings.TrimSpace(string(pidMax)), 10, 64)
	if err != nil {
		return capacityDTO{}, capacityDTO{}, err
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return capacityDTO{}, capacityDTO{}, err
	}
	var used int64
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err == nil {
			used++
		}
	}
	free.PIDs = capacity.PIDs - used
	return capacity, free, nil
}

func sendHostingAgentHeartbeat(ctx context.Context, config hostingAgentConfig) error {
	return publishHostingAgentHeartbeat(ctx, config, true)
}

func publishHostingAgentHeartbeat(ctx context.Context, config hostingAgentConfig, reconcile bool) error {
	if config.HeartbeatMu != nil {
		config.HeartbeatMu.Lock()
		defer config.HeartbeatMu.Unlock()
	}
	_, free, err := detectHostingAgentCapacity(config.WorkRoot)
	if err != nil {
		return err
	}
	namespace := hostingAgentRuntimeNamespace(config.WorkRoot)
	adoptions, err := loadHostingRuntimeAdoptions(config.WorkRoot)
	if err != nil {
		return fmt.Errorf("load legacy runtime adoptions: %w", err)
	}
	containers, err := listHostingAgentRuntimes(ctx, namespace, func(containerID string, runtime hostingObservedRuntime) bool {
		adopted, ok := adoptions[containerID]
		return ok && hostingObservedRuntimeIdentity(adopted.Runtime) == hostingObservedRuntimeIdentity(runtime)
	})
	if err != nil {
		return err
	}
	if config.SessionAccepted == nil || !config.SessionAccepted.Load() {
		priorRetained, priorAuthorized, retentionErr := hostingAgentPriorSessionRetention(ctx, config)
		if retentionErr != nil {
			return retentionErr
		}
		if priorAuthorized {
			cleanupErr := reconcileSupersededHostingAgentSession(ctx, config, containers, priorRetained,
				errHostingAgentSessionSuperseded)
			if !errors.Is(cleanupErr, errHostingAgentSessionDraining) {
				return cleanupErr
			}
		}
		if !hostingStringListContains(config.PriorAcceptedSessionIDs, config.SessionID) {
			pendingSessions := append(append([]string(nil), config.PriorAcceptedSessionIDs...), config.SessionID)
			if err := persistHostingAcceptedSessions(config.WorkRoot, pendingSessions); err != nil {
				return fmt.Errorf("persist pending runner session: %w", err)
			}
		}
	}
	inventory := make([]hostingObservedRuntime, len(containers))
	for index := range containers {
		inventory[index] = containers[index].Runtime
	}
	sequence := int64(1)
	if config.HeartbeatSequence != nil {
		sequence = config.HeartbeatSequence.Add(1)
	}
	payload := hostingHeartbeatRequest{Free: free, Draining: config.Draining, Labels: normalizeStringList(config.Labels), ProtocolVersion: hostingRunnerProtocolVersion,
		ManifestVersions: []string{hostingManifestVersion}, RuntimeVersions: []string{"20", "22"},
		Operations: hostingAgentOperations(config), SessionID: config.SessionID, Sequence: sequence,
		RuntimeInventory: &inventory}
	var response hostingHeartbeatResponse
	if err := hostingAgentJSON(ctx, config, http.MethodPost, "/api/hosting-agent/v1/heartbeat", payload, &response, nil); err != nil {
		var apiErr *hostingAgentAPIError
		if errors.As(err, &apiErr) && apiErr.Code == errCodeRunnerSessionSuperseded {
			retained := apiErr.RetainedReleases
			authorized := apiErr.CleanupAuthorized
			if !authorized {
				priorRetained, priorAuthorized, retentionErr := hostingAgentPriorSessionRetention(ctx, config)
				if retentionErr != nil {
					return retentionErr
				}
				authorized = priorAuthorized
				retained = priorRetained
			}
			if !authorized {
				return err
			}
			return reconcileSupersededHostingAgentSession(ctx, config, containers, retained, err)
		}
		return err
	}
	if config.SessionAccepted == nil || !config.SessionAccepted.Load() {
		if err := persistHostingAcceptedSessions(config.WorkRoot, []string{config.SessionID}); err != nil {
			return fmt.Errorf("persist accepted runner session: %w", err)
		}
	}
	if config.SessionAccepted != nil {
		config.SessionAccepted.Store(true)
	}
	if !reconcile {
		return nil
	}
	return reconcileHostingAgentReleases(ctx, config, response.RetainedReleases)
}

func appendUniqueHostingSession(sessions []string, sessionID string) []string {
	if hostingStringListContains(sessions, sessionID) {
		return sessions
	}
	return append(sessions, sessionID)
}

func hostingAgentPriorSessionRetention(ctx context.Context, config hostingAgentConfig) ([]hostingRetainedRelease, bool, error) {
	for _, sessionID := range config.PriorAcceptedSessionIDs {
		if sessionID == config.SessionID {
			continue
		}
		var response hostingHeartbeatResponse
		err := hostingAgentJSON(ctx, config, http.MethodPost, "/api/hosting-agent/v1/session-retention",
			hostingSessionRetentionRequest{SessionID: sessionID}, &response, nil)
		if err == nil {
			return response.RetainedReleases, true, nil
		}
		var apiErr *hostingAgentAPIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
			return nil, false, err
		}
	}
	return nil, false, nil
}

func reconcileSupersededHostingAgentSession(ctx context.Context, config hostingAgentConfig,
	containers []hostingManagedRuntime, retained []hostingRetainedRelease, cause error) error {
	if err := reconcileHostingAgentReleases(ctx, config, retained); err != nil {
		return err
	}
	if config.SecretMemoryRoot != "" {
		if err := reconcileHostingSecretDirectories(config.SecretMemoryRoot,
			config.SecretBrokerURL != ""); err != nil {
			return err
		}
	}
	if hostingAgentInventoryContainsRetainedRuntime(containers, retained) {
		return fmt.Errorf("%w: %v", errHostingAgentSessionDraining, cause)
	}
	if err := clearHostingAcceptedSession(config.WorkRoot); err != nil {
		return err
	}
	return fmt.Errorf("%w: %v", errHostingAgentSessionSuperseded, cause)
}

func hostingAgentOperations(config hostingAgentConfig) []string {
	operations := []string{"build", "restore", hostingRunnerInventoryOperation}
	if config.SecretBrokerURL != "" {
		operations = append(operations, hostingRunnerSecretOperation)
	}
	return operations
}

func pollHostingAgentJob(ctx context.Context, config hostingAgentConfig) (*hostingClaimedJob, error) {
	var job hostingClaimedJob
	status, err := hostingAgentJSONStatus(ctx, config, http.MethodPost, "/api/hosting-agent/v1/poll", nil, &job, nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNoContent {
		return nil, nil
	}
	return &job, nil
}

func hostingAgentJSON(ctx context.Context, config hostingAgentConfig, method, path string, body, response any, headers map[string]string) error {
	_, err := hostingAgentJSONStatus(ctx, config, method, path, body, response, headers)
	return err
}

func hostingAgentJSONStatus(ctx context.Context, config hostingAgentConfig, method, path string, body, response any, headers map[string]string) (int, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, config.ServerURL+path, reader)
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", "Bearer "+config.Token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	httpResponse, err := agentControlClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
		limited, _ := io.ReadAll(io.LimitReader(httpResponse.Body, 1<<20))
		responseError := apiErrorResponse{}
		if json.Unmarshal(limited, &responseError) == nil && responseError.Code != "" {
			return httpResponse.StatusCode, &hostingAgentAPIError{StatusCode: httpResponse.StatusCode,
				Code: responseError.Code, Message: redactSecrets(responseError.Error),
				CleanupAuthorized: responseError.CleanupAuthorized, RetainedReleases: responseError.RetainedReleases}
		}
		return httpResponse.StatusCode, &hostingAgentAPIError{StatusCode: httpResponse.StatusCode,
			Code: defaultErrorCode(httpResponse.StatusCode), Message: redactSecrets(string(limited))}
	}
	if response != nil && httpResponse.StatusCode != http.StatusNoContent {
		decoder := json.NewDecoder(io.LimitReader(httpResponse.Body, 1<<20))
		if err := decoder.Decode(response); err != nil {
			return httpResponse.StatusCode, err
		}
	}
	return httpResponse.StatusCode, nil
}

func hostingAcceptedSessionPath(workRoot string) string {
	return filepath.Join(workRoot, "accepted-session")
}

func loadHostingAcceptedSessions(workRoot string) ([]string, error) {
	encoded, err := os.ReadFile(hostingAcceptedSessionPath(workRoot))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	lines := strings.Fields(string(encoded))
	if len(lines) == 0 || len(lines) > 64 {
		return nil, fmt.Errorf("accepted runner session file is invalid")
	}
	sessions := make([]string, 0, len(lines))
	seen := make(map[string]struct{}, len(lines))
	for _, sessionID := range lines {
		if !validHostingRunnerSessionID(sessionID) {
			return nil, fmt.Errorf("accepted runner session file is invalid")
		}
		if _, duplicate := seen[sessionID]; duplicate {
			continue
		}
		seen[sessionID] = struct{}{}
		sessions = append(sessions, sessionID)
	}
	return sessions, nil
}

func persistHostingAcceptedSession(workRoot, sessionID string) error {
	return persistHostingAcceptedSessions(workRoot, []string{sessionID})
}

func persistHostingAcceptedSessions(workRoot string, sessionIDs []string) error {
	if len(sessionIDs) == 0 || len(sessionIDs) > 64 {
		return fmt.Errorf("accepted runner session list is invalid")
	}
	for _, sessionID := range sessionIDs {
		if !validHostingRunnerSessionID(sessionID) {
			return fmt.Errorf("accepted runner session is invalid")
		}
	}
	path := hostingAcceptedSessionPath(workRoot)
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(strings.Join(sessionIDs, "\n") + "\n"); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func clearHostingAcceptedSession(workRoot string) error {
	err := os.Remove(hostingAcceptedSessionPath(workRoot))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func hostingAgentInventoryContainsRetainedRuntime(containers []hostingManagedRuntime,
	retained []hostingRetainedRelease) bool {
	retainedIdentities := make(map[string]struct{}, len(retained))
	for _, release := range retained {
		retainedIdentities[hostingObservedRuntimeIdentity(hostingObservedRuntime{
			ReleaseDigest: release.ReleaseDigest, ExternalProjectID: release.ExternalProjectID,
			ExternalDeploymentID: release.ExternalDeploymentID, RuntimeInstanceID: release.RuntimeInstanceID,
		})] = struct{}{}
	}
	for _, container := range containers {
		if _, ok := retainedIdentities[hostingObservedRuntimeIdentity(container.Runtime)]; ok {
			return true
		}
	}
	return false
}

func executeHostingAgentJob(config hostingAgentConfig, job *hostingClaimedJob) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	headers := hostingLeaseHeaders(job)
	done := make(chan struct{})
	var cancellationRequested atomic.Bool
	var controlPlaneUnavailable atomic.Bool
	go func() {
		defer close(done)
		ticker := time.NewTicker(hostingAgentJobHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := publishHostingAgentHeartbeat(ctx, config, true); err != nil {
					controlPlaneUnavailable.Store(true)
					cancel()
					return
				}
				var state map[string]bool
				if err := hostingAgentJSON(ctx, config, http.MethodPost, hostingAgentWorkPath(job, "heartbeat"), nil, &state, headers); err != nil {
					controlPlaneUnavailable.Store(true)
					cancel()
					return
				}
				if state["cancel_requested"] {
					cancellationRequested.Store(true)
					cancel()
					return
				}
			}
		}
	}()
	completion := runHostingWorkload(ctx, config, job)
	cancel()
	<-done
	if cancellationRequested.Load() {
		completion.Status = "cancelled"
		completion.FailureCode = "cancelled"
		completion.FailureMessage = "hosting job cancelled"
	} else if controlPlaneUnavailable.Load() && completion.Status == "failed" {
		completion.FailureCode = "runner_lost"
		completion.FailureMessage = "hosting runner lost its control-plane lease"
	}
	if err := queueHostingAgentCompletion(config, job, completion); err != nil {
		logOperationalError("persist hosting job completion", err)
		return
	}
	if err := flushHostingAgentCompletions(context.Background(), config); err != nil {
		logOperationalError("complete hosting job", err)
	}
}

type hostingAgentCompletionEnvelope struct {
	Operation       string                   `json:"operation,omitempty"`
	JobID           int64                    `json:"job_id"`
	LeaseGeneration int64                    `json:"lease_generation"`
	LeaseToken      string                   `json:"lease_token"`
	Completion      hostingCompletionRequest `json:"completion"`
}

func hostingAgentCompletionDir(config hostingAgentConfig) string {
	return filepath.Join(config.WorkRoot, "completion-outbox")
}

func queueHostingAgentCompletion(config hostingAgentConfig, job *hostingClaimedJob, completion hostingCompletionRequest) error {
	directory := hostingAgentCompletionDir(config)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	envelope := hostingAgentCompletionEnvelope{Operation: job.Recipe.Operation, JobID: job.JobID, LeaseGeneration: job.LeaseGeneration, LeaseToken: job.LeaseToken, Completion: completion}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	operation := hostingAgentOperation(job)
	path := filepath.Join(directory, fmt.Sprintf("%s-%d-%d.json", operation, job.JobID, job.LeaseGeneration))
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err := file.Write(encoded); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func flushHostingAgentCompletions(ctx context.Context, config hostingAgentConfig) error {
	directory := hostingAgentCompletionDir(config)
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		encoded, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var envelope hostingAgentCompletionEnvelope
		if err := json.Unmarshal(encoded, &envelope); err != nil || envelope.JobID <= 0 || envelope.LeaseGeneration <= 0 || envelope.LeaseToken == "" {
			return fmt.Errorf("invalid persisted completion %s", entry.Name())
		}
		headers := map[string]string{
			"X-Deployer-Lease-Generation": strconv.FormatInt(envelope.LeaseGeneration, 10),
			"X-Deployer-Lease-Token":      envelope.LeaseToken,
		}
		group := "jobs"
		if envelope.Operation == "restore" {
			group = "recoveries"
		}
		status, deliveryErr := hostingAgentJSONStatus(ctx, config, http.MethodPost,
			fmt.Sprintf("/api/hosting-agent/v1/%s/%d/complete", group, envelope.JobID), envelope.Completion, nil, headers)
		if deliveryErr != nil {
			if status == http.StatusBadRequest || status == http.StatusForbidden || status == http.StatusNotFound {
				logOperationalError("discard permanently fenced hosting completion", deliveryErr)
				if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
				continue
			}
			return deliveryErr
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func hostingAgentOperation(job *hostingClaimedJob) string {
	if job != nil && job.Recipe.Operation == "restore" {
		return "restore"
	}
	return "build"
}

func hostingAgentWorkPath(job *hostingClaimedJob, suffix string) string {
	group := "jobs"
	if hostingAgentOperation(job) == "restore" {
		group = "recoveries"
	}
	return fmt.Sprintf("/api/hosting-agent/v1/%s/%d/%s", group, job.JobID, suffix)
}

func hostingAgentInstanceID(job *hostingClaimedJob) string {
	return fmt.Sprintf("%s-%d-%d", hostingAgentOperation(job), job.JobID, job.LeaseGeneration)
}

func hostingLeaseHeaders(job *hostingClaimedJob) map[string]string {
	return map[string]string{
		"X-Deployer-Lease-Generation": strconv.FormatInt(job.LeaseGeneration, 10),
		"X-Deployer-Lease-Token":      job.LeaseToken,
	}
}

func runHostingWorkload(ctx context.Context, config hostingAgentConfig, job *hostingClaimedJob) hostingCompletionRequest {
	if hostingAgentOperation(job) == "restore" {
		return runHostingRestoreWorkload(ctx, config, job)
	}
	fail := func(code string, err error) hostingCompletionRequest {
		return hostingCompletionRequest{Status: "failed", FailureCode: code, FailureMessage: redactSecrets(err.Error())}
	}
	workDir := filepath.Join(config.WorkRoot, fmt.Sprintf("job-%d-%d", job.JobID, job.LeaseGeneration))
	if err := os.RemoveAll(workDir); err != nil {
		return fail("workload_policy_violation", err)
	}
	if err := os.Mkdir(workDir, 0750); err != nil {
		return fail("workload_policy_violation", err)
	}
	defer os.RemoveAll(workDir)
	sourceTar := filepath.Join(workDir, "source.tar")
	if err := downloadHostingSource(ctx, config, job, sourceTar); err != nil {
		return fail("source_fetch_failed", err)
	}
	if err := verifyFileDigest(sourceTar, job.Recipe.ArtifactDigest); err != nil {
		return fail("artifact_digest_mismatch", err)
	}
	sourceDir := filepath.Join(workDir, "source")
	if err := os.Mkdir(sourceDir, 0750); err != nil {
		return fail("workload_policy_violation", err)
	}
	if err := extractHostingSource(sourceTar, sourceDir); err != nil {
		return fail("workload_policy_violation", err)
	}
	if err := writeGeneratedHostingRecipe(sourceDir, job.Recipe); err != nil {
		return fail("workload_policy_violation", err)
	}
	if err := reportHostingPhase(ctx, config, job, hostingPhaseBuilding); err != nil {
		return fail("build_failed", err)
	}
	imageTag := "deployer-hosting:" + safeFileName(job.Recipe.ExternalDeploymentID)
	buildTimeout := config.BuildTimeout
	if buildTimeout <= 0 {
		buildTimeout = 15 * time.Minute
	}
	buildCtx, cancelBuild := context.WithTimeout(ctx, buildTimeout)
	defer cancelBuild()
	buildArgs := []string{"build", "--pull", "--no-cache", "--network", config.BuildNetwork,
		"--memory", strconv.FormatInt(job.Recipe.Limits.RAMBytes, 10),
		"--memory-swap", strconv.FormatInt(job.Recipe.Limits.RAMBytes, 10),
		"--cpu-period", "100000", "--cpu-quota", strconv.FormatInt(job.Recipe.Limits.CPUMillis*100, 10),
		"--ulimit", fmt.Sprintf("nproc=%d:%d", job.Recipe.Limits.PIDs, job.Recipe.Limits.PIDs),
		"--ulimit", "nofile=4096:4096", "--shm-size", "64m",
		"-f", ".deployer/Dockerfile", "-t", imageTag, "."}
	if err := runHostingCommand(buildCtx, config, job, sourceDir, "docker", buildArgs...); err != nil {
		if errors.Is(buildCtx.Err(), context.DeadlineExceeded) {
			return fail("build_timeout", fmt.Errorf("build exceeded %s", buildTimeout))
		}
		return fail("build_failed", err)
	}
	imageIDBytes, err := exec.CommandContext(ctx, "docker", "image", "inspect", "--format", "{{.Id}}", imageTag).Output()
	if err != nil {
		return fail("build_failed", err)
	}
	releaseDigest := strings.TrimSpace(string(imageIDBytes))
	if !validSHA256Digest(releaseDigest) {
		return fail("build_failed", fmt.Errorf("container runtime returned invalid image digest"))
	}
	releaseArtifactPath := filepath.Join(workDir, "release-image.tar")
	if err := runHostingCommand(ctx, config, job, "", "docker", "image", "save", "-o", releaseArtifactPath, imageTag); err != nil {
		return fail("release_persistence_failed", err)
	}
	releaseArtifactHash, err := fileSHA256(releaseArtifactPath)
	if err != nil {
		return fail("release_persistence_failed", err)
	}
	releaseArtifactDigest := "sha256:" + releaseArtifactHash
	if err := uploadHostingReleaseArtifact(ctx, config, job, releaseDigest, releaseArtifactDigest, releaseArtifactPath); err != nil {
		return fail("release_persistence_failed", err)
	}
	if err := reportHostingPhase(ctx, config, job, hostingPhaseStarting); err != nil {
		return fail("runtime_start_failed", err)
	}
	return startHostingRuntime(ctx, config, job, imageTag, releaseDigest, releaseArtifactDigest, true)
}

func runHostingRestoreWorkload(ctx context.Context, config hostingAgentConfig, job *hostingClaimedJob) hostingCompletionRequest {
	fail := func(code string, err error) hostingCompletionRequest {
		return hostingCompletionRequest{Status: "failed", FailureCode: code, FailureMessage: redactSecrets(err.Error())}
	}
	if !validSHA256Digest(job.Recipe.ReleaseDigest) || !validSHA256Digest(job.Recipe.ReleaseArtifactDigest) || job.Recipe.ReleaseArtifactURL == "" {
		return fail("artifact_digest_mismatch", fmt.Errorf("recovery artifact identity is invalid"))
	}
	workDir := filepath.Join(config.WorkRoot, fmt.Sprintf("restore-%d-%d", job.JobID, job.LeaseGeneration))
	if err := os.RemoveAll(workDir); err != nil {
		return fail("workload_policy_violation", err)
	}
	if err := os.Mkdir(workDir, 0750); err != nil {
		return fail("workload_policy_violation", err)
	}
	defer os.RemoveAll(workDir)
	artifactPath := filepath.Join(workDir, "release-image.tar")
	if err := downloadHostingArtifact(ctx, config, job, job.Recipe.ReleaseArtifactURL, artifactPath, "release"); err != nil {
		return fail("artifact_unavailable", err)
	}
	if err := verifyFileDigest(artifactPath, job.Recipe.ReleaseArtifactDigest); err != nil {
		return fail("artifact_digest_mismatch", err)
	}
	if err := runHostingCommand(ctx, config, job, "", "docker", "image", "load", "--input", artifactPath); err != nil {
		return fail("release_persistence_failed", err)
	}
	imageID, err := exec.CommandContext(ctx, "docker", "image", "inspect", "--format", "{{.Id}}", job.Recipe.ReleaseDigest).Output()
	if err != nil || strings.TrimSpace(string(imageID)) != job.Recipe.ReleaseDigest {
		return fail("artifact_digest_mismatch", fmt.Errorf("loaded image identity does not match retained release"))
	}
	return startHostingRuntime(ctx, config, job, job.Recipe.ReleaseDigest, job.Recipe.ReleaseDigest,
		job.Recipe.ReleaseArtifactDigest, false)
}

func startHostingRuntime(ctx context.Context, config hostingAgentConfig, job *hostingClaimedJob, imageRef,
	releaseDigest, releaseArtifactDigest string, reportPhases bool) hostingCompletionRequest {
	fail := func(code string, err error) hostingCompletionRequest {
		return hostingCompletionRequest{Status: "failed", FailureCode: code, FailureMessage: redactSecrets(err.Error())}
	}
	instanceID := hostingAgentInstanceID(job)
	containerName := "deployer-hosting-" + safeFileName(job.Recipe.ExternalDeploymentID) + "-" + safeFileName(instanceID)
	agentNamespace := hostingAgentRuntimeNamespace(config.WorkRoot)
	if err := removeStaleHostingContainer(ctx, containerName, agentNamespace, releaseDigest,
		job.Recipe.ExternalProjectID, job.Recipe.ExternalDeploymentID, instanceID); err != nil {
		return fail("workload_policy_violation", err)
	}
	secretDirectory, err := prepareHostingRuntimeSecrets(ctx, config, job, instanceID)
	if err != nil {
		return fail("secret_reference_unavailable", err)
	}
	keepContainer := false
	defer func() {
		if !keepContainer && secretDirectory != "" {
			_ = removeHostingSecretDirectory(secretDirectory)
		}
	}()
	containerPort := job.Recipe.Runtime.Port
	if job.Recipe.Runtime.Kind == "static" {
		containerPort = 8080
	}
	runArgs := []string{"run", "-d", "--name", containerName, "--restart", "unless-stopped",
		"--label", "light-apps.hosting.managed=true", "--label", "light-apps.hosting.release=" + releaseDigest,
		"--label", "light-apps.hosting.agent-namespace=" + agentNamespace,
		"--label", "light-apps.hosting.project=" + job.Recipe.ExternalProjectID,
		"--label", "light-apps.hosting.deployment=" + job.Recipe.ExternalDeploymentID,
		"--label", "light-apps.hosting.instance=" + instanceID,
		"--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--pids-limit", strconv.FormatInt(job.Recipe.Limits.PIDs, 10), "--memory", strconv.FormatInt(job.Recipe.Limits.RAMBytes, 10), "--cpus", fmt.Sprintf("%.3f", float64(job.Recipe.Limits.CPUMillis)/1000), "--storage-opt", "size=" + strconv.FormatInt(job.Recipe.Limits.DiskBytes, 10), "--tmpfs", "/tmp:rw,noexec,nosuid,size=64m", "-p", fmt.Sprintf("%s::%d", config.RuntimeBindAddress, containerPort)}
	if secretDirectory != "" {
		runArgs = append(runArgs, "--label", "light-apps.hosting.secrets=true",
			"--label", "light-apps.hosting.secret-namespace="+hostingAgentSecretNamespace(filepath.Dir(secretDirectory)),
			"--mount", "type=bind,source="+secretDirectory+",target=/run/secrets/deployer,readonly,bind-propagation=rprivate", "--env", "DEPLOYER_SECRETS_DIR=/run/secrets/deployer")
	}
	runArgs = append(runArgs, imageRef)
	if err := runHostingCommand(ctx, config, job, "", "docker", runArgs...); err != nil {
		return fail("runtime_start_failed", err)
	}
	defer func() {
		if !keepContainer {
			_ = exec.Command("docker", "rm", "-f", containerName).Run()
		}
	}()
	portOutput, err := exec.CommandContext(ctx, "docker", "port", containerName, fmt.Sprintf("%d/tcp", containerPort)).Output()
	if err != nil {
		return fail("runtime_start_failed", err)
	}
	hostPort, err := parseDockerBoundPort(string(portOutput), config.RuntimeBindAddress)
	if err != nil {
		return fail("runtime_start_failed", err)
	}
	healthPath := job.Recipe.Runtime.HealthPath
	if healthPath == "" {
		healthPath = "/"
	}
	endpoint := fmt.Sprintf("http://%s:%d", config.RuntimeBindAddress, hostPort)
	if reportPhases {
		if err := reportHostingPhase(ctx, config, job, hostingPhaseHealth); err != nil {
			return fail("health_check_failed", err)
		}
	}
	attempts, statusCode, err := checkHostingCandidateHealth(ctx, endpoint+healthPath)
	if err != nil {
		return hostingCompletionRequest{Status: "failed", FailureCode: "health_check_failed", FailureMessage: redactSecrets(err.Error()), HealthEvidence: map[string]any{"healthy": false, "attempts": attempts, "status_code": statusCode}}
	}
	// The control plane must observe the exact immutable runtime identity before it
	// accepts completion. This also refreshes the candidate's lease while the
	// completion outbox is pending.
	if config.SessionID != "" {
		if err := publishHostingAgentHeartbeat(ctx, config, true); err != nil {
			return fail("runner_lost", fmt.Errorf("publish exact runtime inventory: %w", err))
		}
	}
	keepContainer = true
	if secretDirectory != "" {
		go cleanupHostingSecretsAfterContainerExit(containerName, secretDirectory)
	}
	return hostingCompletionRequest{Status: "success", ReleaseDigest: releaseDigest, ReleaseArtifactDigest: releaseArtifactDigest, RuntimeEndpoint: endpoint, HealthEvidence: map[string]any{"healthy": true, "attempts": attempts, "status_code": statusCode}}
}

func removeStaleHostingContainer(ctx context.Context, containerName, agentNamespace, releaseDigest,
	externalProjectID, externalDeploymentID, instanceID string) error {
	output, err := exec.CommandContext(ctx, "docker", "inspect", "--format",
		`{{index .Config.Labels "light-apps.hosting.managed"}}\t{{index .Config.Labels "light-apps.hosting.agent-namespace"}}\t{{index .Config.Labels "light-apps.hosting.release"}}\t{{index .Config.Labels "light-apps.hosting.project"}}\t{{index .Config.Labels "light-apps.hosting.deployment"}}\t{{index .Config.Labels "light-apps.hosting.instance"}}`,
		containerName).CombinedOutput()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 && strings.Contains(strings.ToLower(string(output)), "no such") {
			return nil
		}
		return fmt.Errorf("inspect existing hosting container: %w", err)
	}
	fields := strings.Split(strings.TrimSpace(string(output)), "\t")
	if len(fields) != 6 || fields[0] != "true" || fields[1] != agentNamespace || fields[2] != releaseDigest ||
		fields[3] != externalProjectID || fields[4] != externalDeploymentID || fields[5] != instanceID {
		return fmt.Errorf("refusing to replace container with mismatched ownership labels")
	}
	if err := exec.CommandContext(ctx, "docker", "rm", "-f", containerName).Run(); err != nil {
		return fmt.Errorf("remove stale managed hosting container: %w", err)
	}
	return nil
}

func uploadHostingReleaseArtifact(ctx context.Context, config hostingAgentConfig, job *hostingClaimedJob, releaseDigest, artifactDigest, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut,
		config.ServerURL+fmt.Sprintf("/api/hosting-agent/v1/jobs/%d/release-artifact", job.JobID), file)
	if err != nil {
		return err
	}
	request.ContentLength = info.Size()
	request.Header.Set("Authorization", "Bearer "+config.Token)
	request.Header.Set("Content-Type", "application/x-tar")
	request.Header.Set("X-Deployer-Release-Digest", releaseDigest)
	request.Header.Set("X-Deployer-Artifact-Digest", artifactDigest)
	for key, value := range hostingLeaseHeaders(job) {
		request.Header.Set(key, value)
	}
	response, err := agentArtifactClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		limited, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return fmt.Errorf("release artifact upload returned HTTP %d: %s", response.StatusCode, redactSecrets(string(limited)))
	}
	return nil
}

type hostingManagedRuntime struct {
	ContainerID string                 `json:"container_id"`
	Runtime     hostingObservedRuntime `json:"runtime"`
	Legacy      bool                   `json:"legacy"`
}

func listHostingAgentRuntimes(ctx context.Context, namespace string,
	legacyFilter func(string, hostingObservedRuntime) bool) ([]hostingManagedRuntime, error) {
	if len(namespace) != 32 {
		return nil, fmt.Errorf("invalid hosting agent namespace")
	}
	commandCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(commandCtx, "docker", "ps", "-a",
		"--filter", "label=light-apps.hosting.managed=true",
		"--format", `{{.ID}}\t{{.Label "light-apps.hosting.agent-namespace"}}\t{{.Label "light-apps.hosting.release"}}\t{{.Label "light-apps.hosting.project"}}\t{{.Label "light-apps.hosting.deployment"}}\t{{.Label "light-apps.hosting.instance"}}\t{{.State}}\t{{.Ports}}`).Output()
	if err != nil {
		return nil, fmt.Errorf("list managed hosting containers: %w", err)
	}
	containers := make([]hostingManagedRuntime, 0)
	seen := make(map[string]int)
	for _, line := range strings.Split(string(output), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) == 7 {
			fields = append(fields, "")
		}
		if len(fields) != 8 || strings.TrimSpace(fields[0]) == "" {
			return nil, fmt.Errorf("container runtime returned invalid managed container metadata")
		}
		containerNamespace := strings.TrimSpace(fields[1])
		if containerNamespace != "" && containerNamespace != namespace {
			continue
		}
		runtime := hostingObservedRuntime{ReleaseDigest: strings.TrimSpace(fields[2]),
			ExternalProjectID: strings.TrimSpace(fields[3]), ExternalDeploymentID: strings.TrimSpace(fields[4]),
			RuntimeInstanceID: strings.TrimSpace(fields[5]), State: strings.TrimSpace(fields[6])}
		legacy := containerNamespace == ""
		if endpoint, endpointErr := parseHostingPublishedRuntimeEndpoint(fields[7]); endpointErr == nil {
			runtime.RuntimeEndpoint = endpoint
		} else if !legacy || runtime.RuntimeInstanceID != "" {
			return nil, fmt.Errorf("container runtime returned invalid published endpoint: %w", endpointErr)
		}
		if err := validateHostingRuntimeInventory([]hostingObservedRuntime{runtime}); err != nil &&
			!(legacy && validLegacyHostingRuntime(runtime)) {
			return nil, fmt.Errorf("container runtime returned invalid managed container metadata: %w", err)
		}
		if !legacy && runtime.RuntimeInstanceID == "" {
			return nil, fmt.Errorf("container runtime returned empty namespaced runtime instance")
		}
		containerID := strings.TrimSpace(fields[0])
		if legacy && (legacyFilter == nil || !legacyFilter(containerID, runtime)) {
			continue
		}
		identity := hostingObservedRuntimeIdentity(runtime)
		if existingIndex, duplicate := seen[identity]; duplicate {
			if containers[existingIndex].Legacy && !legacy {
				containers[existingIndex] = hostingManagedRuntime{ContainerID: strings.TrimSpace(fields[0]), Runtime: runtime}
				continue
			}
			if !containers[existingIndex].Legacy && legacy {
				continue
			}
			if containers[existingIndex].Legacy && legacy {
				continue
			}
			return nil, fmt.Errorf("multiple managed containers claim runtime identity")
		}
		seen[identity] = len(containers)
		containers = append(containers, hostingManagedRuntime{ContainerID: containerID, Runtime: runtime, Legacy: legacy})
		if len(containers) > hostingMaxRuntimeInventoryEntries {
			return nil, fmt.Errorf("managed runtime inventory exceeds %d entries", hostingMaxRuntimeInventoryEntries)
		}
	}
	return containers, nil
}

func parseHostingPublishedRuntimeEndpoint(ports string) (string, error) {
	var endpoint string
	for _, entry := range strings.Split(ports, ",") {
		entry = strings.TrimSpace(entry)
		arrow := strings.Index(entry, "->")
		if arrow < 0 {
			continue
		}
		host, portText, err := net.SplitHostPort(strings.TrimSpace(entry[:arrow]))
		ip := net.ParseIP(host)
		port, portErr := strconv.Atoi(portText)
		if err != nil || ip == nil || ip.To4() == nil || (!ip.IsPrivate() && !ip.IsLoopback()) ||
			portErr != nil || port < 1024 || port > 65535 || endpoint != "" {
			return "", fmt.Errorf("managed runtime must have one private IPv4 port binding")
		}
		endpoint = "http://" + net.JoinHostPort(ip.String(), strconv.Itoa(port))
	}
	if endpoint == "" {
		return "", fmt.Errorf("managed runtime has no published endpoint")
	}
	return endpoint, nil
}

func validLegacyHostingRuntime(runtime hostingObservedRuntime) bool {
	if runtime.RuntimeInstanceID != "" || !validSHA256Digest(runtime.ReleaseDigest) ||
		!validHostingExternalIDSyntax(runtime.ExternalProjectID) ||
		!validHostingExternalIDSyntax(runtime.ExternalDeploymentID) {
		return false
	}
	switch runtime.State {
	case "created", "restarting", "running", "removing", "paused", "exited", "dead":
		return true
	default:
		return false
	}
}

type hostingRuntimeAdoptionFile struct {
	Version  int                     `json:"version"`
	Runtimes []hostingManagedRuntime `json:"runtimes"`
}

func loadHostingRuntimeAdoptions(workRoot string) (map[string]hostingManagedRuntime, error) {
	adoptions := make(map[string]hostingManagedRuntime)
	file, err := os.Open(filepath.Join(workRoot, "runtime-adoptions.json"))
	if errors.Is(err, os.ErrNotExist) {
		return adoptions, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	var stored hostingRuntimeAdoptionFile
	if err := decoder.Decode(&stored); err != nil || stored.Version != 1 || len(stored.Runtimes) > hostingMaxRuntimeInventoryEntries {
		return nil, fmt.Errorf("invalid runtime adoption registry")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("invalid runtime adoption registry")
	}
	for _, runtime := range stored.Runtimes {
		if runtime.ContainerID == "" || !runtime.Legacy ||
			(validateHostingRuntimeInventory([]hostingObservedRuntime{runtime.Runtime}) != nil && !validLegacyHostingRuntime(runtime.Runtime)) {
			return nil, fmt.Errorf("invalid runtime adoption registry")
		}
		if _, duplicate := adoptions[runtime.ContainerID]; duplicate {
			return nil, fmt.Errorf("duplicate runtime adoption registry entry")
		}
		adoptions[runtime.ContainerID] = runtime
	}
	return adoptions, nil
}

func saveHostingRuntimeAdoptions(workRoot string, adoptions map[string]hostingManagedRuntime) error {
	stored := hostingRuntimeAdoptionFile{Version: 1, Runtimes: make([]hostingManagedRuntime, 0, len(adoptions))}
	for _, runtime := range adoptions {
		stored.Runtimes = append(stored.Runtimes, runtime)
	}
	temporary, err := os.CreateTemp(workRoot, ".runtime-adoptions-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0600); err != nil {
		temporary.Close()
		return err
	}
	if err := json.NewEncoder(temporary).Encode(stored); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, filepath.Join(workRoot, "runtime-adoptions.json"))
}

func reconcileHostingAgentReleases(ctx context.Context, config hostingAgentConfig, retained []hostingRetainedRelease) error {
	namespace := hostingAgentRuntimeNamespace(config.WorkRoot)
	retainedSet := make(map[string]struct{}, len(retained))
	legacyRetainedSet := make(map[string]struct{})
	for _, release := range retained {
		if !validSHA256Digest(release.ReleaseDigest) || !validHostingExternalIDSyntax(release.ExternalProjectID) || !validHostingExternalIDSyntax(release.ExternalDeploymentID) {
			return fmt.Errorf("control plane returned invalid retained release identity")
		}
		base := release.ExternalProjectID + "\x00" + release.ExternalDeploymentID + "\x00" + release.ReleaseDigest
		if release.RuntimeInstanceID == "" {
			legacyRetainedSet[base] = struct{}{}
		} else {
			retainedSet[base+"\x00"+release.RuntimeInstanceID] = struct{}{}
		}
	}
	adoptions, err := loadHostingRuntimeAdoptions(config.WorkRoot)
	if err != nil {
		return fmt.Errorf("load legacy runtime adoptions: %w", err)
	}
	containers, err := listHostingAgentRuntimes(ctx, namespace, func(containerID string, runtime hostingObservedRuntime) bool {
		if _, adopted := adoptions[containerID]; adopted {
			return true
		}
		base := runtime.ExternalProjectID + "\x00" + runtime.ExternalDeploymentID + "\x00" + runtime.ReleaseDigest
		if runtime.RuntimeInstanceID == "" {
			_, retained := legacyRetainedSet[base]
			return retained
		}
		_, retained := retainedSet[hostingObservedRuntimeIdentity(runtime)]
		return retained
	})
	if err != nil {
		return err
	}
	found := make(map[string]struct{}, len(containers))
	for _, container := range containers {
		found[container.ContainerID] = struct{}{}
		containerID := container.ContainerID
		base := container.Runtime.ExternalProjectID + "\x00" + container.Runtime.ExternalDeploymentID + "\x00" + container.Runtime.ReleaseDigest
		identity := hostingObservedRuntimeIdentity(container.Runtime)
		_, exact := retainedSet[identity]
		_, legacy := legacyRetainedSet[base]
		if exact || (legacy && container.Runtime.RuntimeInstanceID == "") {
			if container.Legacy {
				adoptions[containerID] = container
			}
			continue
		}
		if container.Legacy {
			adopted, owned := adoptions[containerID]
			if !owned || hostingObservedRuntimeIdentity(adopted.Runtime) != identity {
				continue
			}
		}
		inspectCtx, cancelInspect := context.WithTimeout(ctx, 10*time.Second)
		imageOutput, _ := exec.CommandContext(inspectCtx, "docker", "inspect", "--format", "{{.Image}}", containerID).Output()
		cancelInspect()
		removeCtx, cancelRemove := context.WithTimeout(ctx, 10*time.Second)
		err := exec.CommandContext(removeCtx, "docker", "rm", "-f", containerID).Run()
		cancelRemove()
		if err != nil {
			return fmt.Errorf("remove expired managed hosting container: %w", err)
		}
		imageID := strings.TrimSpace(string(imageOutput))
		if validSHA256Digest(imageID) {
			imageCtx, cancelImage := context.WithTimeout(ctx, 10*time.Second)
			_ = exec.CommandContext(imageCtx, "docker", "image", "rm", imageID).Run()
			cancelImage()
		}
		delete(adoptions, containerID)
	}
	for containerID := range adoptions {
		if _, exists := found[containerID]; !exists {
			delete(adoptions, containerID)
		}
	}
	if err := saveHostingRuntimeAdoptions(config.WorkRoot, adoptions); err != nil {
		return fmt.Errorf("persist legacy runtime adoptions: %w", err)
	}
	return nil
}

func downloadHostingSource(ctx context.Context, config hostingAgentConfig, job *hostingClaimedJob, destination string) error {
	return downloadHostingArtifact(ctx, config, job, job.Recipe.SourceArtifactURL, destination, "source")
}

func downloadHostingArtifact(ctx context.Context, config hostingAgentConfig, job *hostingClaimedJob, artifactURL, destination, kind string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, config.ServerURL+artifactURL, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+config.Token)
	for key, value := range hostingLeaseHeaders(job) {
		request.Header.Set(key, value)
	}
	response, err := agentArtifactClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%s download returned HTTP %d", kind, response.StatusCode)
	}
	if response.ContentLength > maxAgentArtifactDownloadBytes {
		return fmt.Errorf("%s artifact exceeds size limit", kind)
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	written, copyErr := io.Copy(file, io.LimitReader(response.Body, maxAgentArtifactDownloadBytes+1))
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	if written > maxAgentArtifactDownloadBytes {
		_ = os.Remove(destination)
		return fmt.Errorf("%s artifact exceeds size limit", kind)
	}
	return closeErr
}

func verifyFileDigest(path, expected string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	actual := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	if actual != expected {
		return fmt.Errorf("artifact digest mismatch")
	}
	return nil
}

func extractHostingSource(archivePath, destination string) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := tar.NewReader(file)
	var extractedBytes int64
	var entries int
	for {
		header, err := reader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		entries++
		if entries > 100000 {
			return fmt.Errorf("archive contains too many entries")
		}
		clean := filepath.Clean(header.Name)
		if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return fmt.Errorf("archive path is unsafe")
		}
		target := filepath.Join(destination, clean)
		relative, err := filepath.Rel(destination, target)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("archive path escapes destination")
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if header.Size < 0 || header.Size > maxAgentArtifactDownloadBytes-extractedBytes {
				return fmt.Errorf("archive expanded size exceeds limit")
			}
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
			if err != nil {
				return err
			}
			written, copyErr := io.Copy(output, io.LimitReader(reader, header.Size))
			closeErr := output.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			if written != header.Size {
				return fmt.Errorf("archive entry was truncated")
			}
			extractedBytes += written
		default:
			return fmt.Errorf("archive contains forbidden non-regular entry")
		}
	}
}

func writeGeneratedHostingRecipe(sourceDir string, recipe hostingJobRecipe) error {
	runtime := recipe.Runtime
	if err := validateHostingRuntimeManifest(&runtime); err != nil {
		return fmt.Errorf("invalid hosting runtime recipe: %w", err)
	}
	recipe.Runtime = runtime
	directory := filepath.Join(sourceDir, ".deployer")
	if err := os.Mkdir(directory, 0750); err != nil {
		return err
	}
	install, runBuild, command, err := hostingPackageCommands(recipe.Runtime)
	if err != nil {
		return err
	}
	var dockerfile string
	if recipe.Runtime.Kind == "static" {
		dockerfile = fmt.Sprintf("FROM node:%s-bookworm-slim AS build\nWORKDIR /app\nCOPY . .\nRUN %s\nRUN %s\nFROM nginxinc/nginx-unprivileged:1.27-alpine\nCOPY .deployer/nginx.conf /etc/nginx/conf.d/default.conf\nCOPY --from=build /app/%s /usr/share/nginx/html\nEXPOSE 8080\n", recipe.Runtime.NodeVersion, install, runBuild, recipe.Runtime.OutputDirectory)
		nginx := "server { listen 8080; server_name _; root /usr/share/nginx/html; location / { try_files $uri $uri/ /index.html; } }\n"
		if err := os.WriteFile(filepath.Join(directory, "nginx.conf"), []byte(nginx), 0640); err != nil {
			return err
		}
	} else {
		buildStep := ""
		if recipe.Runtime.BuildScript != "" {
			buildStep = "RUN " + runBuild + "\n"
		}
		dockerfile = fmt.Sprintf("FROM node:%s-bookworm-slim\nWORKDIR /app\nCOPY . .\nRUN %s\n%sENV NODE_ENV=production\nENV PORT=%d\nUSER node\nEXPOSE %d\nCMD %s\n", recipe.Runtime.NodeVersion, install, buildStep, recipe.Runtime.Port, recipe.Runtime.Port, command)
	}
	return os.WriteFile(filepath.Join(directory, "Dockerfile"), []byte(dockerfile), 0640)
}

func hostingPackageCommands(runtime HostingRuntimeManifest) (string, string, string, error) {
	script := runtime.BuildScript
	switch runtime.PackageManager {
	case "npm":
		return "npm ci --ignore-scripts", "npm run " + script, fmt.Sprintf(`["npm","run",%q]`, runtime.StartScript), nil
	case "pnpm":
		return "corepack enable && pnpm install --frozen-lockfile --ignore-scripts", "pnpm run " + script, fmt.Sprintf(`["pnpm","run",%q]`, runtime.StartScript), nil
	case "yarn":
		return "corepack enable && yarn install --immutable --mode=skip-builds", "yarn run " + script, fmt.Sprintf(`["yarn","run",%q]`, runtime.StartScript), nil
	default:
		return "", "", "", fmt.Errorf("unsupported package manager")
	}
}

func reportHostingPhase(ctx context.Context, config hostingAgentConfig, job *hostingClaimedJob, phase string) error {
	return hostingAgentJSON(ctx, config, http.MethodPost, hostingAgentWorkPath(job, "phase"), hostingPhaseRequest{Phase: phase}, nil, hostingLeaseHeaders(job))
}

func runHostingCommand(ctx context.Context, config hostingAgentConfig, job *hostingClaimedJob, directory, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = directory
	pipe, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	command.Stderr = command.Stdout
	if err := command.Start(); err != nil {
		return err
	}
	scanner := bufio.NewScanner(pipe)
	buffer := make([]byte, 64<<10)
	scanner.Buffer(buffer, 1<<20)
	for scanner.Scan() {
		message := redactSecrets(scanner.Text())
		stream := "build"
		if hostingAgentOperation(job) == "restore" {
			stream = "system"
		}
		_ = hostingAgentJSON(ctx, config, http.MethodPost, hostingAgentWorkPath(job, "logs"), hostingLogRequest{Stream: stream, Message: message}, nil, hostingLeaseHeaders(job))
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return command.Wait()
}

func parseLoopbackDockerPort(output string) (int, error) {
	return parseDockerBoundPort(output, "127.0.0.1")
}

func validateHostingRuntimeBindAddress(value string) error {
	ip := net.ParseIP(strings.TrimSpace(value))
	if ip == nil || ip.To4() == nil || ip.IsUnspecified() || (!ip.IsPrivate() && !ip.IsLoopback()) {
		return fmt.Errorf("runtime bind address must be a private or loopback IPv4 address")
	}
	return nil
}

func parseDockerBoundPort(output, expectedAddress string) (int, error) {
	line := strings.TrimSpace(strings.Split(output, "\n")[0])
	host, portText, err := net.SplitHostPort(line)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).Equal(net.ParseIP(expectedAddress)) {
		return 0, fmt.Errorf("container runtime did not bind the configured private address")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1024 || port > 65535 {
		return 0, fmt.Errorf("container runtime returned invalid host port")
	}
	return port, nil
}

func checkHostingCandidateHealth(ctx context.Context, target string) (int, int, error) {
	client := &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	lastStatus := 0
	for attempt := 1; attempt <= 10; attempt++ {
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		response, err := client.Do(request)
		if err == nil {
			lastStatus = response.StatusCode
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
			response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				return attempt, response.StatusCode, nil
			}
		}
		select {
		case <-ctx.Done():
			return attempt, lastStatus, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return 10, lastStatus, fmt.Errorf("candidate health check failed")
}
