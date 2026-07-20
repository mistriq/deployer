package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

const linuxTmpfsMagic = 0x01021994

const maxHostingSecretBrokerResponseBytes int64 = 1 << 20

var hostingSecretInstancePattern = regexp.MustCompile(`^(?:build|restore)-[1-9][0-9]*-[1-9][0-9]*$`)

var errHostingSecretBrokerRedirect = errors.New("secret broker redirects are not allowed")

type hostingSecretBrokerRequest struct {
	SecretReferences []HostingSecretReference `json:"secret_references"`
}

type hostingSecretValue struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type hostingSecretBrokerResponse struct {
	Values []hostingSecretValue `json:"values"`
}

func prepareHostingRuntimeSecrets(ctx context.Context, config hostingAgentConfig, job *hostingClaimedJob, instanceID string) (string, error) {
	if len(job.SecretRefs) == 0 {
		return "", nil
	}
	if job.Recipe.Runtime.Kind != "node" || strings.TrimSpace(config.SecretBrokerURL) == "" {
		return "", fmt.Errorf("runtime secret broker is unavailable")
	}
	encoded, err := json.Marshal(hostingSecretBrokerRequest{SecretReferences: job.SecretRefs})
	if err != nil {
		return "", err
	}
	timeout := config.SecretBrokerTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	client := &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error {
		return errHostingSecretBrokerRedirect
	}}
	var payload *hostingSecretBrokerResponse
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		var identity hostingWorkloadIdentityResponse
		if err := hostingAgentJSON(ctx, config, http.MethodPost, hostingAgentWorkPath(job, "workload-identity"), nil, &identity, hostingLeaseHeaders(job)); err != nil {
			lastErr = fmt.Errorf("obtain workload identity: %w", err)
		} else {
			now := time.Now()
			if !strings.HasPrefix(identity.Token, "wli_") || !identity.ExpiresAt.After(now) || identity.ExpiresAt.After(now.Add(2*time.Minute)) {
				return "", fmt.Errorf("Deployer returned an invalid workload identity")
			}
			var transient bool
			payload, transient, lastErr = redeemHostingRuntimeSecrets(ctx, client, config.SecretBrokerURL, identity.Token, encoded, job.SecretRefs)
			if lastErr == nil {
				break
			}
			if !transient {
				return "", lastErr
			}
		}
		if attempt < 2 {
			timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return "", ctx.Err()
			case <-timer.C:
			}
		}
	}
	if lastErr != nil {
		return "", lastErr
	}
	if payload == nil {
		return "", fmt.Errorf("secret broker returned no workload secrets")
	}
	root := config.SecretMemoryRoot
	if root == "" {
		root = "/dev/shm/deployer-hosting-secrets"
	}
	if err := requireHostingSecretTmpfs(root); err != nil {
		return "", err
	}
	directory := filepath.Join(root, safeFileName(instanceID))
	if err := removeHostingSecretDirectory(directory); err != nil {
		return "", err
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return "", err
	}
	committed := false
	defer func() {
		if !committed {
			_ = removeHostingSecretDirectory(directory)
		}
	}()
	for _, value := range payload.Values {
		if err := os.WriteFile(filepath.Join(directory, value.Name), []byte(value.Value), 0444); err != nil {
			return "", err
		}
	}
	if err := os.Chmod(directory, 0555); err != nil {
		return "", err
	}
	committed = true
	return directory, nil
}

func redeemHostingRuntimeSecrets(ctx context.Context, client *http.Client, brokerURL, identityToken string,
	encoded []byte, refs []HostingSecretReference) (*hostingSecretBrokerResponse, bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, brokerURL+"/api/internal/v1/workload-secrets/redeem", bytes.NewReader(encoded))
	if err != nil {
		return nil, false, err
	}
	request.Header.Set("Authorization", "Bearer "+identityToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return nil, !errors.Is(err, errHostingSecretBrokerRedirect), fmt.Errorf("redeem workload secrets: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
		transient := response.StatusCode == http.StatusRequestTimeout || response.StatusCode == http.StatusConflict ||
			response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
		return nil, transient, fmt.Errorf("secret broker rejected workload identity with status %d", response.StatusCode)
	}
	if !cacheControlHasDirective(response.Header.Get("Cache-Control"), "no-store") {
		return nil, false, fmt.Errorf("secret broker response is cacheable")
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return nil, false, fmt.Errorf("secret broker returned an invalid content type")
	}
	if response.ContentLength > maxHostingSecretBrokerResponseBytes {
		return nil, false, fmt.Errorf("secret broker response exceeds the workload limit")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxHostingSecretBrokerResponseBytes+1))
	if err != nil {
		return nil, true, fmt.Errorf("read secret broker response: %w", err)
	}
	if int64(len(body)) > maxHostingSecretBrokerResponseBytes {
		return nil, false, fmt.Errorf("secret broker response exceeds the workload limit")
	}
	var payload hostingSecretBrokerResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return nil, true, fmt.Errorf("decode secret broker response: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, true, fmt.Errorf("decode secret broker response: %w", err)
	}
	if err := validateHostingSecretValues(refs, payload.Values); err != nil {
		return nil, false, err
	}
	return &payload, false, nil
}

func cacheControlHasDirective(value, wanted string) bool {
	for _, directive := range strings.Split(value, ",") {
		parts := strings.SplitN(directive, "=", 2)
		name := strings.TrimSpace(parts[0])
		if strings.EqualFold(name, wanted) && len(parts) == 1 {
			return true
		}
	}
	return false
}

func validateHostingSecretValues(refs []HostingSecretReference, values []hostingSecretValue) error {
	if len(values) != len(refs) {
		return fmt.Errorf("secret broker returned an incomplete secret set")
	}
	expected := make([]string, 0, len(refs))
	actual := make([]string, 0, len(values))
	var total int
	seen := make(map[string]struct{}, len(values))
	for _, ref := range refs {
		expected = append(expected, ref.Name)
	}
	for _, value := range values {
		if !validHostingSecretName(value.Name) || len(value.Value) == 0 || len(value.Value) > 64<<10 {
			return fmt.Errorf("secret broker returned an invalid secret value")
		}
		if _, duplicate := seen[value.Name]; duplicate {
			return fmt.Errorf("secret broker returned a duplicate secret name")
		}
		seen[value.Name] = struct{}{}
		total += len(value.Value)
		if total > 256<<10 {
			return fmt.Errorf("secret broker response exceeds the workload limit")
		}
		actual = append(actual, value.Name)
	}
	sort.Strings(expected)
	sort.Strings(actual)
	if strings.Join(expected, "\x00") != strings.Join(actual, "\x00") {
		return fmt.Errorf("secret broker returned a mismatched secret set")
	}
	return nil
}

func requireHostingSecretTmpfs(root string) error {
	if !filepath.IsAbs(root) {
		return fmt.Errorf("secret memory root must be absolute")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("secret memory root must be a real directory")
	}
	if err := os.Chmod(root, 0700); err != nil {
		return err
	}
	var stats syscall.Statfs_t
	if err := syscall.Statfs(root, &stats); err != nil {
		return err
	}
	if uint64(stats.Type) != uint64(linuxTmpfsMagic) {
		return fmt.Errorf("secret memory root must reside on tmpfs")
	}
	return nil
}

func removeHostingSecretDirectory(directory string) error {
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return os.Remove(directory)
	}
	if err := os.Chmod(directory, 0700); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.RemoveAll(directory)
}

func reconcileHostingSecretDirectories(root string, create bool) error {
	namespace := hostingAgentSecretNamespace(root)
	output, err := execHostingSecretContainers(namespace)
	if err != nil {
		return fmt.Errorf("list managed secret containers: %w", err)
	}
	containers := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 3 || strings.TrimSpace(fields[0]) == "" || !hostingSecretInstancePattern.MatchString(strings.TrimSpace(fields[1])) || strings.TrimSpace(fields[2]) != namespace {
			return fmt.Errorf("managed secret container has invalid identity metadata")
		}
		instanceID := strings.TrimSpace(fields[1])
		if _, duplicate := containers[instanceID]; duplicate {
			return fmt.Errorf("multiple managed containers claim secret instance %q", instanceID)
		}
		containers[instanceID] = strings.TrimSpace(fields[0])
	}
	if _, err := os.Lstat(root); errors.Is(err, os.ErrNotExist) && !create && len(containers) == 0 {
		return nil
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := requireHostingSecretTmpfs(root); err != nil {
		return err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	foundDirectories := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		directory := filepath.Join(root, entry.Name())
		if !entry.IsDir() || !hostingSecretInstancePattern.MatchString(entry.Name()) {
			if err := removeHostingSecretDirectory(directory); err != nil {
				return err
			}
			continue
		}
		foundDirectories[entry.Name()] = struct{}{}
		containerID, exists := containers[entry.Name()]
		if !exists {
			if err := removeHostingSecretDirectory(directory); err != nil {
				return err
			}
			continue
		}
		source, err := hostingContainerSecretSource(containerID)
		if err != nil {
			return fmt.Errorf("inspect managed secret mount: %w", err)
		}
		if filepath.Clean(source) != filepath.Clean(directory) {
			return fmt.Errorf("managed secret container %q has an unexpected bind source", entry.Name())
		}
		go cleanupHostingSecretsAfterContainerExit(containerID, directory)
	}
	for instanceID := range containers {
		if _, exists := foundDirectories[instanceID]; !exists {
			return fmt.Errorf("managed secret container %q has lost its tmpfs material", instanceID)
		}
	}
	return nil
}

func cleanupHostingSecretsAfterContainerExit(containerName, directory string) {
	for {
		time.Sleep(30 * time.Second)
		exists, err := hostingContainerExists(containerName)
		if err != nil {
			continue
		}
		if !exists {
			if err := removeHostingSecretDirectory(directory); err == nil {
				return
			}
		}
	}
}

var hostingContainerExists = func(containerName string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "inspect", containerName).CombinedOutput()
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 && strings.Contains(strings.ToLower(string(output)), "no such") {
		return false, nil
	}
	return false, err
}

var execHostingSecretContainers = func(namespace string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "ps", "-a",
		"--filter", "label=light-apps.hosting.managed=true",
		"--filter", "label=light-apps.hosting.secrets=true",
		"--filter", "label=light-apps.hosting.secret-namespace="+namespace,
		"--format", `{{.ID}}\t{{.Label "light-apps.hosting.instance"}}\t{{.Label "light-apps.hosting.secret-namespace"}}`).Output()
	return string(output), err
}

var hostingContainerSecretSource = func(containerID string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "inspect", "--format",
		`{{range .Mounts}}{{if eq .Destination "/run/secrets/deployer"}}{{.Source}}{{end}}{{end}}`, containerID).Output()
	return strings.TrimSpace(string(output)), err
}
