package app

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/subtle"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	agentControlClient  = &http.Client{Timeout: getenvDurationDefault("DEPLOYER_AGENT_CONTROL_TIMEOUT", 45*time.Second)}
	agentArtifactClient = &http.Client{Timeout: getenvDurationDefault("DEPLOYER_AGENT_ARTIFACT_TIMEOUT", 30*time.Minute)}
	agentCommands       = systemCommands
)

const agentHTTPAttempts = 3

func agentRetryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}
	return time.Duration(attempt) * time.Second
}

type agentJobReporter struct {
	serverURL string
	token     string
	buildID   int64
	logErr    error
}

func newAgentJobReporter(serverURL, token string, buildID int64) *agentJobReporter {
	return &agentJobReporter{
		serverURL: serverURL,
		token:     token,
		buildID:   buildID,
	}
}

func (r *agentJobReporter) sendLog(line string) {
	if err := agentSendLog(r.serverURL, r.token, r.buildID, line+"\n"); err != nil {
		logStructured("error", "agent_send_log_failed", map[string]interface{}{
			"build_id": r.buildID,
			"error":    err,
		})
		if r.logErr == nil {
			r.logErr = err
		}
	}
}

func (r *agentJobReporter) sendLogf(format string, args ...interface{}) {
	r.sendLog(fmt.Sprintf(format, args...))
}

func (r *agentJobReporter) reportComplete(status, errMsg string) {
	if status == "success" && r.logErr != nil {
		status = "failed"
		errMsg = "send build log: " + r.logErr.Error()
	}
	agentComplete(r.serverURL, r.token, r.buildID, status, errMsg)
}

func runAgent() {
	fs := flag.NewFlagSet("agent", flag.ExitOnError)
	serverURL := fs.String("server", os.Getenv("DEPLOYER_SERVER"), "Deployer server URL (e.g. https://deployer.example.com)")
	token := fs.String("token", os.Getenv("DEPLOYER_TOKEN"), "Agent authentication token")
	autoUpdate := fs.Bool("auto-update", getenvBoolDefault("DEPLOYER_AGENT_AUTO_UPDATE", false), "Enable checksum-verified self-update from the server")
	fs.Parse(os.Args[2:])

	if *serverURL == "" || *token == "" {
		fmt.Println("Usage: deployer agent --server URL --token TOKEN")
		fmt.Println()
		fmt.Println("Example:")
		fmt.Println("  deployer agent --server https://deployer.example.com --token TOKEN")
		os.Exit(1)
	}

	// Remove trailing slash
	*serverURL = strings.TrimRight(*serverURL, "/")
	if err := validateServerURL(*serverURL); err != nil {
		logFatal("agent_config_error", "invalid server URL", err, map[string]interface{}{
			"server": *serverURL,
		})
	}

	logStructured("info", "agent_starting", map[string]interface{}{
		"version": buildVersion,
		"server":  *serverURL,
	})

	// Test connection
	if err := agentHeartbeat(*serverURL, *token); err != nil {
		logFatal("agent_connection_error", "cannot connect to server", err, map[string]interface{}{
			"server": *serverURL,
		})
	}
	logStructured("info", "agent_connected", map[string]interface{}{
		"server": *serverURL,
	})

	if *autoUpdate {
		agentCheckUpdate(*serverURL, *token)
	}

	// Start heartbeat goroutine (also checks for updates every 60s)
	go func() {
		tick := 0
		for {
			time.Sleep(15 * time.Second)
			if err := agentHeartbeat(*serverURL, *token); err != nil {
				logStructured("error", "agent_heartbeat_failed", map[string]interface{}{"error": err})
			}
			tick++
			if *autoUpdate && tick%4 == 0 { // every 4th heartbeat = every 60s
				agentCheckUpdate(*serverURL, *token)
			}
		}
	}()

	// Main poll loop
	for {
		job, err := agentPoll(*serverURL, *token)
		if err != nil {
			logStructured("error", "agent_poll_failed", map[string]interface{}{
				"error":       err,
				"retry_after": "5s",
			})
			time.Sleep(5 * time.Second)
			continue
		}
		if job == nil {
			time.Sleep(2 * time.Second)
			continue
		}

		logStructured("info", "agent_job_received", map[string]interface{}{
			"job_id":   job.ID,
			"build_id": job.BuildID,
			"mode":     job.Mode,
		})
		if job.Mode == "snapshot" {
			executeSnapshotJob(*serverURL, *token, job)
		} else if job.Mode == "files" {
			executeFilesJob(*serverURL, *token, job)
		} else {
			executeJob(*serverURL, *token, job)
		}

		// Check for update after completing the job (not before!)
		if *autoUpdate {
			agentCheckUpdate(*serverURL, *token)
		}
	}
}

func validateServerURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return fmt.Errorf("scheme must be http or https")
	}
	if u.Host == "" {
		return fmt.Errorf("host is required")
	}
	if u.Scheme == "http" && !isLocalServerHost(u.Hostname()) {
		logStructured("warn", "agent_plaintext_http", map[string]interface{}{
			"host": u.Hostname(),
		})
	}
	return nil
}

func isLocalServerHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func newAgentRequest(method, reqURL, token string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, reqURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return req, nil
}

func agentHeartbeat(serverURL, token string) error {
	req, err := newAgentRequest(http.MethodPost, serverURL+"/api/agent/heartbeat", token, nil)
	if err != nil {
		return err
	}
	resp, err := agentControlClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func agentPoll(serverURL, token string) (*Job, error) {
	client := &http.Client{Timeout: 35 * time.Second} // slightly longer than server's 30s
	req, err := newAgentRequest(http.MethodGet, serverURL+"/api/agent/poll", token, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 204 {
		return nil, nil // No job
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	var job Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, fmt.Errorf("decode job: %w", err)
	}
	return &job, nil
}

func executeJob(serverURL, token string, job *Job) {
	reporter := newAgentJobReporter(serverURL, token, job.BuildID)
	sendLog := reporter.sendLog
	sendLogf := reporter.sendLogf
	reportComplete := reporter.reportComplete

	var stepStart time.Time
	startGroup := func(name string) {
		stepStart = time.Now()
		sendLog("##[group]" + name)
	}
	endGroup := func() {
		dur := int(time.Since(stepStart).Seconds())
		sendLog(fmt.Sprintf("##[endgroup:%d]", dur))
	}

	// Download artifact
	startGroup("Download Image")
	safeName := strings.ReplaceAll(job.ImageName, "/", "-")
	tarPath := filepath.Join(os.TempDir(), fmt.Sprintf("%s-%d.tar", safeName, job.BuildID))
	if err := agentDownloadArtifact(serverURL, token, job.BuildID, tarPath); err != nil {
		sendLogf("Download failed: %s", err)
		endGroup()
		reportComplete("failed", "download artifact: "+err.Error())
		return
	}

	fi, _ := os.Stat(tarPath)
	if fi != nil {
		sendLogf("Downloaded %.1f MB", float64(fi.Size())/(1024*1024))
	}
	defer os.Remove(tarPath)
	endGroup()

	// Docker load
	startGroup("Docker Load")
	if err := agentRunCommand(serverURL, token, job.BuildID, "", "docker", "load", "-i", tarPath); err != nil {
		sendLogf("Docker load failed: %s", err)
		endGroup()
		reportComplete("failed", "docker load: "+err.Error())
		return
	}
	endGroup()

	// Restart services
	startGroup("Restart Services")
	services := strings.Fields(job.ComposeServices)
	downArgs := append([]string{"compose", "-f", job.ComposeFile, "down"}, services...)
	if err := agentRunCommand(serverURL, token, job.BuildID, job.DeployDir, "docker", downArgs...); err != nil {
		sendLogf("Compose down failed: %s (continuing...)", err)
	}

	upArgs := append([]string{"compose", "-f", job.ComposeFile, "up", "-d"}, services...)
	if err := agentRunCommand(serverURL, token, job.BuildID, job.DeployDir, "docker", upArgs...); err != nil {
		sendLogf("Compose up failed: %s", err)
		endGroup()
		reportComplete("failed", "docker compose up: "+err.Error())
		return
	}
	endGroup()

	// Health check
	if job.HealthContainer != "" || job.HealthURL != "" {
		startGroup("Health Check")
		if err := agentHealthCheck(job, sendLogf); err != nil {
			sendLogf("Health check failed: %s (deploy may still be OK)", err)
		} else {
			sendLogf("Health check passed")
		}
		endGroup()
	}

	// Cleanup old images and build cache after successful deploy
	startGroup("Cleanup")
	if getenvBoolDefault("DEPLOYER_DOCKER_PRUNE", false) {
		sendLogf("Pruning old Docker images and build cache...")
		_ = agentRunCommandSilent(job.BuildID, "docker", "image", "prune", "-af")
		_ = agentRunCommandSilent(job.BuildID, "docker", "builder", "prune", "-af")
		sendLogf("Cleanup done")
	} else {
		sendLogf("Docker prune skipped")
	}
	endGroup()

	sendLogf("Deploy completed!")
	reportComplete("success", "")
}

func executeSnapshotJob(serverURL, token string, job *Job) {
	reporter := newAgentJobReporter(serverURL, token, job.BuildID)
	sendLog := reporter.sendLog
	sendLogf := reporter.sendLogf
	reportComplete := reporter.reportComplete

	var stepStart time.Time
	startGroup := func(name string) {
		stepStart = time.Now()
		sendLog("##[group]" + name)
	}
	endGroup := func() {
		dur := int(time.Since(stepStart).Seconds())
		sendLog(fmt.Sprintf("##[endgroup:%d]", dur))
	}

	startGroup("Package Remote State")
	if job.DeployDir == "" {
		sendLogf("Missing deploy directory")
		endGroup()
		reportComplete("failed", "missing deploy directory")
		return
	}
	if fi, err := os.Stat(job.DeployDir); err != nil {
		sendLogf("Deploy directory is not readable: %s", err)
		endGroup()
		reportComplete("failed", "stat deploy dir: "+err.Error())
		return
	} else if !fi.IsDir() {
		sendLogf("Deploy path is not a directory: %s", job.DeployDir)
		endGroup()
		reportComplete("failed", "deploy path is not a directory")
		return
	}

	archivePath := filepath.Join(os.TempDir(), fmt.Sprintf("deployer-snapshot-%d.tar.gz", job.BuildID))
	ignorePatterns := loadIgnorePatterns(job.DeployDir)
	sendLogf("Packaging %s", job.DeployDir)
	if len(ignorePatterns) > 0 {
		sendLogf("Ignore patterns: %v", ignorePatterns)
	}
	if err := packageFiles(job.DeployDir, archivePath, ignorePatterns); err != nil {
		sendLogf("Package failed: %s", err)
		endGroup()
		reportComplete("failed", "package snapshot: "+err.Error())
		return
	}

	fi, _ := os.Stat(archivePath)
	if fi != nil {
		sendLogf("Archive: %.1f MB", float64(fi.Size())/(1024*1024))
	}
	defer os.Remove(archivePath)
	endGroup()

	startGroup("Upload Snapshot")
	if err := agentUploadSnapshot(serverURL, token, job.BuildID, archivePath); err != nil {
		sendLogf("Upload failed: %s", err)
		endGroup()
		reportComplete("failed", "upload snapshot: "+err.Error())
		return
	}
	sendLogf("Snapshot uploaded")
	endGroup()

	reportComplete("success", "")
}

func executeFilesJob(serverURL, token string, job *Job) {
	reporter := newAgentJobReporter(serverURL, token, job.BuildID)
	sendLog := reporter.sendLog
	sendLogf := reporter.sendLogf
	reportComplete := reporter.reportComplete

	var stepStart time.Time
	startGroup := func(name string) {
		stepStart = time.Now()
		sendLog("##[group]" + name)
	}
	endGroup := func() {
		dur := int(time.Since(stepStart).Seconds())
		sendLog(fmt.Sprintf("##[endgroup:%d]", dur))
	}

	// Step 1: Download archive
	startGroup("Download Archive")
	archivePath := filepath.Join(os.TempDir(), fmt.Sprintf("files-%d.tar.gz", job.BuildID))
	if err := agentDownloadArtifact(serverURL, token, job.BuildID, archivePath); err != nil {
		sendLogf("Download failed: %s", err)
		endGroup()
		reportComplete("failed", "download archive: "+err.Error())
		return
	}

	fi, _ := os.Stat(archivePath)
	if fi != nil {
		sendLogf("Downloaded %.1f MB", float64(fi.Size())/(1024*1024))
	}
	defer os.Remove(archivePath)
	endGroup()

	// Step 2: Backup preserve files
	preservePaths, err := parsePreservePaths(job.Preserve)
	if err != nil {
		sendLogf("%s", err)
		reportComplete("failed", err.Error())
		return
	}

	preserveDir := ""
	if len(preservePaths) > 0 {
		startGroup("Preserve Backup")
		preserveDir, err = os.MkdirTemp("", "preserve-*")
		if err != nil {
			sendLogf("Failed to create preserve temp dir: %s", err)
			endGroup()
			reportComplete("failed", "preserve backup: "+err.Error())
			return
		}
		defer os.RemoveAll(preserveDir)

		preserved := backupPreservePaths(job.DeployDir, preserveDir, preservePaths, sendLogf)
		sendLogf("Backed up %d items", preserved)
		endGroup()
	}

	// Step 3: Extract files
	startGroup("Extract Files")
	sendLogf("Extracting to %s", job.DeployDir)

	// Ensure deploy dir exists
	if err := os.MkdirAll(job.DeployDir, 0755); err != nil {
		sendLogf("Failed to create deploy dir: %s", err)
		endGroup()
		reportComplete("failed", "create deploy dir: "+err.Error())
		return
	}

	// Extract tar.gz directly to deploy dir
	if err := extractTarGz(archivePath, job.DeployDir); err != nil {
		sendLogf("Extract failed: %s", err)
		endGroup()
		reportComplete("failed", "extract files: "+err.Error())
		return
	}
	sendLogf("Files extracted successfully")
	endGroup()

	// Step 4: Restore preserve files
	if preserveDir != "" && len(preservePaths) > 0 {
		startGroup("Preserve Restore")
		restored := restorePreservePaths(job.DeployDir, preserveDir, preservePaths, sendLogf)
		sendLogf("Restored %d items", restored)
		endGroup()
	}

	// Step 5: Set permissions
	if job.Permissions != "" {
		startGroup("Set Permissions")
		perms, err := validatePermissionsConfig(job.Permissions)
		if err != nil {
			sendLogf("Invalid permissions config: %s", err)
		} else {
			// Set owner
			if perms.Owner != "" {
				sendLogf("Setting owner: %s", perms.Owner)
				if err := agentCommands.Run(context.Background(), "", "chown", "-R", perms.Owner, job.DeployDir); err != nil {
					sendLogf("  chown failed: %s", err)
				}
			}
			// Set file permissions
			for pattern, mode := range perms.Files {
				matches, _ := filepath.Glob(filepath.Join(job.DeployDir, pattern))
				for _, m := range matches {
					if err := agentCommands.Run(context.Background(), "", "chmod", mode, m); err != nil {
						sendLogf("  chmod %s %s failed: %s", mode, m, err)
					}
				}
				sendLogf("  %s → %s (%d files)", pattern, mode, len(matches))
			}
			// Set directory permissions
			for pattern, mode := range perms.Dirs {
				matches, _ := filepath.Glob(filepath.Join(job.DeployDir, pattern))
				for _, m := range matches {
					if err := agentCommands.Run(context.Background(), "", "chmod", mode, m); err != nil {
						sendLogf("  chmod %s %s failed: %s", mode, m, err)
					}
				}
				sendLogf("  %s → %s (%d dirs)", pattern, mode, len(matches))
			}
		}
		endGroup()
	}

	// Step 6: Post-deploy command
	if job.PostDeploy != "" {
		startGroup("Post-deploy Command")
		sendLogf("Running: %s", job.PostDeploy)
		if err := agentRunCommand(serverURL, token, job.BuildID, job.DeployDir, "sh", "-c", job.PostDeploy); err != nil {
			sendLogf("Post-deploy command failed: %s", err)
			endGroup()
			reportComplete("failed", "post-deploy: "+err.Error())
			return
		}
		sendLogf("Post-deploy command completed")
		endGroup()
	}

	// Step 7: Health check
	if job.HealthContainer != "" || job.HealthURL != "" {
		startGroup("Health Check")
		if err := agentHealthCheck(job, sendLogf); err != nil {
			sendLogf("Health check failed: %s (deploy may still be OK)", err)
		} else {
			sendLogf("Health check passed")
		}
		endGroup()
	}

	sendLogf("Deploy completed!")
	reportComplete("success", "")
}

func parsePreservePaths(raw string) ([]string, error) {
	var paths []string
	for _, line := range strings.Split(raw, "\n") {
		path, err := cleanRelativeDeployPath(line)
		if err != nil {
			if strings.TrimSpace(line) == "" {
				continue
			}
			return nil, fmt.Errorf("invalid preserve path %q: %w", line, err)
		}
		paths = append(paths, path)
	}
	return paths, nil
}

func backupPreservePaths(deployDir, preserveDir string, preservePaths []string, sendLogf func(string, ...interface{})) int {
	preserved := 0
	for _, p := range preservePaths {
		src := filepath.Join(deployDir, p)
		if _, err := os.Lstat(src); os.IsNotExist(err) {
			sendLogf("  %s (not found, skipping)", p)
			continue
		} else if err != nil {
			sendLogf("  %s (stat failed: %s)", p, err)
			continue
		}
		dst := filepath.Join(preserveDir, p)
		if err := copyPreservePath(src, dst); err != nil {
			sendLogf("  %s (backup failed: %s)", p, err)
		} else {
			sendLogf("  %s (backed up)", p)
			preserved++
		}
	}
	return preserved
}

func restorePreservePaths(deployDir, preserveDir string, preservePaths []string, sendLogf func(string, ...interface{})) int {
	restored := 0
	for _, p := range preservePaths {
		src := filepath.Join(preserveDir, p)
		if _, err := os.Lstat(src); os.IsNotExist(err) {
			continue
		} else if err != nil {
			sendLogf("  %s (restore stat failed: %s)", p, err)
			continue
		}
		dst := filepath.Join(deployDir, p)
		if err := os.RemoveAll(dst); err != nil {
			sendLogf("  %s (restore remove failed: %s)", p, err)
			continue
		}
		if err := copyPreservePath(src, dst); err != nil {
			sendLogf("  %s (restore failed: %s)", p, err)
		} else {
			sendLogf("  %s (restored)", p)
			restored++
		}
	}
	return restored
}

func copyPreservePath(src, dst string) error {
	info, err := os.Lstat(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	mode := info.Mode()
	if mode&os.ModeSymlink != 0 {
		target, err := os.Readlink(src)
		if err != nil {
			return err
		}
		return os.Symlink(target, dst)
	}
	if info.IsDir() {
		return copyPreserveDir(src, dst, mode.Perm())
	}
	if !mode.IsRegular() {
		return fmt.Errorf("unsupported file type %s", mode.Type())
	}
	return copyPreserveFile(src, dst, mode.Perm())
}

func copyPreserveDir(src, dst string, mode os.FileMode) error {
	if err := os.MkdirAll(dst, mode); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := copyPreservePath(filepath.Join(src, entry.Name()), filepath.Join(dst, entry.Name())); err != nil {
			return err
		}
	}
	return os.Chmod(dst, mode)
}

func copyPreserveFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Chmod(dst, mode)
}

func agentDownloadArtifact(serverURL, token string, buildID int64, destPath string) error {
	var lastErr error
	for attempt := 0; attempt < agentHTTPAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(agentRetryDelay(attempt))
		}

		req, err := newAgentRequest(http.MethodGet, fmt.Sprintf("%s/api/agent/artifact/%d", serverURL, buildID), token, nil)
		if err != nil {
			return err
		}
		resp, err := agentArtifactClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		err = saveAgentArtifactResponse(resp, destPath)
		resp.Body.Close()
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

func saveAgentArtifactResponse(resp *http.Response, destPath string) error {
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	if resp.ContentLength > maxAgentArtifactDownloadBytes {
		return fmt.Errorf("artifact exceeds download limit")
	}
	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	written, copyErr := io.Copy(f, io.LimitReader(resp.Body, maxAgentArtifactDownloadBytes+1))
	closeErr := f.Close()
	if copyErr != nil {
		os.Remove(destPath)
		return copyErr
	}
	if closeErr != nil {
		os.Remove(destPath)
		return closeErr
	}
	if written > maxAgentArtifactDownloadBytes {
		os.Remove(destPath)
		return fmt.Errorf("artifact exceeds download limit")
	}
	return nil
}

func agentUploadSnapshot(serverURL, token string, buildID int64, archivePath string) error {
	var lastErr error
	for attempt := 0; attempt < agentHTTPAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(agentRetryDelay(attempt))
		}

		f, err := os.Open(archivePath)
		if err != nil {
			return err
		}
		url := fmt.Sprintf("%s/api/agent/snapshot/%d", serverURL, buildID)
		req, err := newAgentRequest(http.MethodPost, url, token, f)
		if err != nil {
			f.Close()
			return err
		}
		req.Header.Set("Content-Type", "application/gzip")

		resp, err := agentArtifactClient.Do(req)
		f.Close()
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		lastErr = fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	return lastErr
}

func agentRunCommand(serverURL, token string, buildID int64, dir string, name string, args ...string) error {
	cmd, output, err := agentCommands.Start(context.Background(), dir, name, args...)
	if err != nil {
		return err
	}
	defer output.Close()

	// Read output and send logs in batches
	reader := bufio.NewReader(output)
	var batch strings.Builder
	lastSend := time.Now()
	lineCount := 0
	var readErr error
	var logErr error

	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			line = strings.TrimRight(line, "\r\n")
		}
		if line != "" {
			batch.WriteString(line + "\n")
			lineCount++
		}

		// Send batch every 500ms or every 20 lines
		if batch.Len() > 0 && (time.Since(lastSend) > 500*time.Millisecond || lineCount >= 20) {
			if err := agentSendLog(serverURL, token, buildID, batch.String()); err != nil {
				logStructured("error", "agent_send_log_failed", map[string]interface{}{
					"build_id": buildID,
					"error":    err,
				})
				if logErr == nil {
					logErr = err
				}
			}
			batch.Reset()
			lineCount = 0
			lastSend = time.Now()
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			readErr = err
			break
		}
	}

	// Send remaining
	if batch.Len() > 0 {
		if err := agentSendLog(serverURL, token, buildID, batch.String()); err != nil {
			logStructured("error", "agent_send_log_failed", map[string]interface{}{
				"build_id": buildID,
				"error":    err,
			})
			if logErr == nil {
				logErr = err
			}
		}
	}

	waitErr := cmd.Wait()
	if readErr != nil {
		if waitErr != nil {
			return fmt.Errorf("read command output: %w; command wait: %v", readErr, waitErr)
		}
		return fmt.Errorf("read command output: %w", readErr)
	}
	if waitErr != nil {
		return waitErr
	}
	if logErr != nil {
		return fmt.Errorf("send build log: %w", logErr)
	}
	return nil
}

func agentRunCommandSilent(buildID int64, name string, args ...string) error {
	return agentCommands.Run(context.Background(), "", name, args...)
}

func extractTarGz(archivePath, destDir string) error {
	base, err := filepath.Abs(destDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(base, 0755); err != nil {
		return err
	}

	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer f.Close()

	gzReader, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	tarReader := tar.NewReader(gzReader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		target, err := safeArchiveTarget(base, header.Name)
		if err != nil {
			return err
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(header.Mode)&0755); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			mode := os.FileMode(header.Mode) & 0777
			if mode == 0 {
				mode = 0644
			}
			out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
			if err != nil {
				return err
			}
			if _, err := io.Copy(out, tarReader); err != nil {
				out.Close()
				return err
			}
			if err := out.Close(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("archive entry %q has unsupported type %d", header.Name, header.Typeflag)
		}
	}
}

func safeArchiveTarget(base, name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("archive contains an empty path")
	}
	clean := filepath.Clean(name)
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("archive path %q is unsafe", name)
	}
	target := filepath.Join(base, clean)
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("archive path %q escapes destination", name)
	}
	return target, nil
}

func agentSendLog(serverURL, token string, buildID int64, logText string) error {
	url := fmt.Sprintf("%s/api/agent/log/%d", serverURL, buildID)
	var lastErr error
	for attempt := 0; attempt < agentHTTPAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(agentRetryDelay(attempt))
		}
		req, err := newAgentRequest(http.MethodPost, url, token, bytes.NewBufferString(logText))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "text/plain")
		resp, err := agentControlClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		lastErr = fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	return lastErr
}

func agentComplete(serverURL, token string, buildID int64, status, errMsg string) {
	payload, _ := json.Marshal(map[string]string{
		"status": status,
		"error":  errMsg,
	})
	url := fmt.Sprintf("%s/api/agent/complete/%d", serverURL, buildID)
	var lastErr error
	for attempt := 0; attempt < agentHTTPAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(agentRetryDelay(attempt))
		}
		req, err := newAgentRequest(http.MethodPost, url, token, bytes.NewBuffer(payload))
		if err != nil {
			logStructured("error", "agent_completion_request_failed", map[string]interface{}{
				"build_id": buildID,
				"error":    err,
			})
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := agentControlClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			logStructured("info", "agent_completion_reported", map[string]interface{}{
				"build_id": buildID,
				"status":   status,
			})
			return
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		lastErr = fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	logStructured("error", "agent_completion_failed", map[string]interface{}{
		"build_id": buildID,
		"error":    lastErr,
	})
}

func agentHealthCheck(job *Job, logf func(string, ...interface{})) error {
	// Health check by container inspect
	if job.HealthContainer != "" {
		for i := 0; i < 30; i++ {
			out, err := agentCommands.Output(context.Background(), "", "docker", "inspect", "--format={{.State.Health.Status}}", job.HealthContainer)
			if err == nil && strings.TrimSpace(string(out)) == "healthy" {
				return nil
			}
			if i%5 == 0 {
				logf("   Waiting for container %s... (%ds)", job.HealthContainer, i*2)
			}
			time.Sleep(2 * time.Second)
		}
		return fmt.Errorf("container %s not healthy after 60s", job.HealthContainer)
	}

	// Health check by URL (curl locally)
	if job.HealthURL != "" {
		for i := 0; i < 30; i++ {
			if err := agentCommands.Run(context.Background(), "", "curl", "-sf", job.HealthURL); err == nil {
				return nil
			}
			if i%5 == 0 {
				logf("   Waiting for %s... (%ds)", job.HealthURL, i*2)
			}
			time.Sleep(2 * time.Second)
		}
		return fmt.Errorf("health URL %s not responding after 60s", job.HealthURL)
	}

	return nil
}

// agentCheckUpdate checks if the server has a newer version and self-updates
func agentCheckUpdate(serverURL, token string) {
	req, err := newAgentRequest(http.MethodGet, serverURL+"/api/version", token, nil)
	if err != nil {
		return
	}
	resp, err := agentControlClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return
	}

	var result struct {
		Version        string `json:"version"`
		ChecksumSHA256 string `json:"checksum_sha256"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return
	}

	serverVersion := result.Version
	if serverVersion == "" || serverVersion == buildVersion {
		return
	}
	if result.ChecksumSHA256 == "" {
		logStructured("warn", "agent_update_skipped", map[string]interface{}{
			"reason": "missing_checksum",
		})
		return
	}

	logStructured("info", "agent_update_available", map[string]interface{}{
		"current_version": buildVersion,
		"server_version":  serverVersion,
	})

	dlReq, err := newAgentRequest(http.MethodGet, serverURL+"/download/deployer", token, nil)
	if err != nil {
		logStructured("error", "agent_update_request_failed", map[string]interface{}{"error": err})
		return
	}
	dlResp, err := agentArtifactClient.Do(dlReq)
	if err != nil {
		logStructured("error", "agent_update_download_failed", map[string]interface{}{"error": err})
		return
	}
	defer dlResp.Body.Close()

	if dlResp.StatusCode != 200 {
		logStructured("error", "agent_update_download_failed", map[string]interface{}{
			"status": dlResp.StatusCode,
		})
		return
	}

	exePath, err := os.Executable()
	if err != nil {
		logStructured("error", "agent_update_failed", map[string]interface{}{
			"step":  "executable_path",
			"error": err,
		})
		return
	}

	tmpPath := filepath.Join(filepath.Dir(exePath), filepath.Base(exePath)+".new")
	f, err := os.Create(tmpPath)
	if err != nil {
		logStructured("error", "agent_update_failed", map[string]interface{}{
			"step":  "create_temp",
			"error": err,
		})
		return
	}

	if _, err := io.Copy(f, dlResp.Body); err != nil {
		f.Close()
		os.Remove(tmpPath)
		logStructured("error", "agent_update_failed", map[string]interface{}{
			"step":  "copy_download",
			"error": err,
		})
		return
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		logStructured("error", "agent_update_failed", map[string]interface{}{
			"step":  "close_temp",
			"error": err,
		})
		return
	}

	checksum, err := fileSHA256(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		logStructured("error", "agent_update_failed", map[string]interface{}{
			"step":  "checksum",
			"error": err,
		})
		return
	}
	if subtle.ConstantTimeCompare([]byte(checksum), []byte(result.ChecksumSHA256)) != 1 {
		os.Remove(tmpPath)
		logStructured("error", "agent_update_failed", map[string]interface{}{
			"step": "checksum_mismatch",
		})
		return
	}

	if err := installVerifiedUpdate(tmpPath, exePath); err != nil {
		os.Remove(tmpPath)
		logStructured("error", "agent_update_failed", map[string]interface{}{
			"step":  "install",
			"error": err,
		})
		return
	}

	logStructured("info", "agent_updated", map[string]interface{}{
		"server_version": serverVersion,
	})
	os.Exit(0)
}

func installVerifiedUpdate(tmpPath, exePath string) error {
	if err := os.Chmod(tmpPath, 0755); err != nil {
		return fmt.Errorf("chmod update: %w", err)
	}

	dir := filepath.Dir(exePath)
	base := filepath.Base(exePath)
	pendingPath := filepath.Join(dir, base+".pending")
	backupPath := filepath.Join(dir, base+".old")

	if err := os.Remove(pendingPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove old pending update: %w", err)
	}
	if err := os.Remove(backupPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove old backup: %w", err)
	}
	if err := os.Rename(tmpPath, pendingPath); err != nil {
		return fmt.Errorf("stage update: %w", err)
	}
	if err := os.Rename(exePath, backupPath); err != nil {
		_ = os.Remove(pendingPath)
		return fmt.Errorf("backup current executable: %w", err)
	}
	if err := os.Rename(pendingPath, exePath); err != nil {
		_ = os.Rename(backupPath, exePath)
		_ = os.Remove(pendingPath)
		return fmt.Errorf("activate update: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync update directory: %w", err)
	}
	return nil
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
