package app

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Builder struct {
	broker   *SSEBroker
	mu       sync.Mutex
	cancels  map[int64]context.CancelFunc // buildID -> cancel
	wg       sync.WaitGroup
	commands commandExecutor
}

func NewBuilder(broker *SSEBroker) *Builder {
	return &Builder{
		broker:   broker,
		cancels:  make(map[int64]context.CancelFunc),
		commands: systemCommands,
	}
}

func (b *Builder) commandExecutor() commandExecutor {
	if b.commands != nil {
		return b.commands
	}
	return systemCommands
}

func (b *Builder) Deploy(project *Project, triggeredBy string) (int64, error) {
	if err := validateProject(project); err != nil {
		return 0, err
	}
	if isProjectBuilding(project.ID) {
		return 0, fmt.Errorf("project %s is already building", project.Name)
	}

	build, err := createBuild(project.ID, triggeredBy)
	if err != nil {
		return 0, fmt.Errorf("create build: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.mu.Lock()
	b.cancels[build.ID] = cancel
	b.mu.Unlock()

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.runPipeline(ctx, project, build)
	}()

	return build.ID, nil
}

func (b *Builder) FetchRemoteSnapshot(project *Project, triggeredBy string) (int64, error) {
	if err := validateSnapshotProject(project); err != nil {
		return 0, err
	}
	if isProjectBuilding(project.ID) {
		return 0, fmt.Errorf("project %s is already running", project.Name)
	}

	build, err := createBuild(project.ID, triggeredBy)
	if err != nil {
		return 0, fmt.Errorf("create build: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.mu.Lock()
	b.cancels[build.ID] = cancel
	b.mu.Unlock()

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.runSnapshotPipeline(ctx, project, build)
	}()

	return build.ID, nil
}

func (b *Builder) Cancel(buildID int64) error {
	b.mu.Lock()
	cancel, ok := b.cancels[buildID]
	b.mu.Unlock()
	if !ok {
		return fmt.Errorf("build %d not found or already finished", buildID)
	}
	cancel()
	return nil
}

func (b *Builder) Shutdown(ctx context.Context) {
	b.mu.Lock()
	for _, cancel := range b.cancels {
		cancel()
	}
	b.mu.Unlock()

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		logOperationalError("wait for builds to stop", ctx.Err())
	}
}

func (b *Builder) runSnapshotPipeline(ctx context.Context, project *Project, build *Build) {
	var logBuf strings.Builder
	startTime := time.Now()
	agentHandled := false

	defer func() {
		if r := recover(); r != nil {
			logStructured("error", "snapshot_pipeline_panic", map[string]interface{}{
				"build_id": build.ID,
				"panic":    fmt.Sprint(r),
			})
			build.Status = "failed"
			build.ErrorMessage = fmt.Sprintf("panic: %v", r)
			build.ErrorCode = runtimeErrPanic
			agentHandled = false
		}

		if !agentHandled {
			now := time.Now()
			build.FinishedAt = &now
			dur := int(now.Sub(startTime).Seconds())
			build.DurationSeconds = &dur
			build.Log = logBuf.String()
			logOperationalError("update snapshot build", updateBuild(build))
		}
		b.broker.Close(build.ID)

		b.mu.Lock()
		delete(b.cancels, build.ID)
		b.mu.Unlock()
	}()

	log := func(msg string) {
		msg = redactSecrets(msg)
		logBuf.WriteString(msg + "\n")
		b.broker.Publish(build.ID, msg)
		build.Log = logBuf.String()
		logOperationalError("update snapshot log", updateBuild(build))
	}
	logf := func(format string, args ...interface{}) {
		log(fmt.Sprintf(format, args...))
	}

	var groupStartTime time.Time
	startGroup := func(name string) {
		groupStartTime = time.Now()
		log("##[group]" + name)
	}
	endGroup := func() {
		dur := int(time.Since(groupStartTime).Seconds())
		log(fmt.Sprintf("##[endgroup:%d]", dur))
	}
	fail := func(step string, err error) {
		build.Status = "failed"
		build.ErrorMessage = fmt.Sprintf("%s: %s", step, err)
		build.ErrorCode = classifyRuntimeError(build.ErrorMessage)
		logf("FAILED at step '%s': %s", step, err)
	}

	logStructured("info", "snapshot_started", map[string]interface{}{
		"build_id":   build.ID,
		"project":    project.Name,
		"runner_id":  project.RunnerID,
		"deploy_dir": project.DeployDir,
	})
	logf("Requesting remote snapshot for %s", project.Name)

	startGroup("Request Remote Snapshot")
	safeName := strings.ReplaceAll(project.Name, " ", "-")
	safeName = strings.ReplaceAll(safeName, "/", "-")
	if err := currentArtifactStorage().Ensure(); err != nil {
		endGroup()
		fail("create snapshot directory", err)
		return
	}
	artifactPath := managedSnapshotPath(fmt.Sprintf("%s-%d.tar.gz", safeName, build.ID))
	if _, err := createSnapshotJob(build.ID, project.RunnerID, project, artifactPath); err != nil {
		endGroup()
		fail("create snapshot job", err)
		return
	}
	logf("Remote path: %s", project.DeployDir)
	logf("Snapshot will be stored at %s", artifactPath)
	endGroup()

	startGroup("Agent Snapshot")
	logf("Waiting for agent to package and upload the project...")
	for {
		select {
		case <-ctx.Done():
			build.Status = "cancelled"
			build.ErrorMessage = "cancelled by user"
			build.ErrorCode = runtimeErrCancelled
			log("Snapshot cancelled by user")
			endGroup()
			logOperationalError("cancel snapshot job", updateUnfinishedJobStatusByBuildIDWithError(build.ID, "cancelled", runtimeErrCancelled))
			removeManagedArtifact(artifactPath)
			return
		case <-time.After(3 * time.Second):
		}

		currentBuild, err := getBuild(build.ID)
		if err != nil {
			continue
		}
		if currentBuild.Status != "running" {
			agentHandled = true
			return
		}
	}
}

func (b *Builder) runPipeline(ctx context.Context, project *Project, build *Build) {
	var logBuf strings.Builder
	startTime := time.Now()
	agentHandled := false // when true, agent already updated the build — defer must not overwrite

	defer func() {
		if r := recover(); r != nil {
			logStructured("error", "deploy_pipeline_panic", map[string]interface{}{
				"build_id": build.ID,
				"panic":    fmt.Sprint(r),
			})
			build.Status = "failed"
			build.ErrorMessage = fmt.Sprintf("panic: %v", r)
			build.ErrorCode = runtimeErrPanic
			agentHandled = false // force update on panic
		}

		if !agentHandled {
			now := time.Now()
			build.FinishedAt = &now
			dur := int(now.Sub(startTime).Seconds())
			build.DurationSeconds = &dur
			build.Log = logBuf.String()
			logOperationalError("update build", updateBuild(build))
		}
		b.broker.Close(build.ID)

		b.mu.Lock()
		delete(b.cancels, build.ID)
		b.mu.Unlock()
	}()

	log := func(msg string) {
		msg = redactSecrets(msg)
		logBuf.WriteString(msg + "\n")
		b.broker.Publish(build.ID, msg)
		// Periodically save log to DB so API can return it
		build.Log = logBuf.String()
		logOperationalError("update build log", updateBuild(build))
	}

	logf := func(format string, args ...interface{}) {
		log(fmt.Sprintf(format, args...))
	}

	var groupStartTime time.Time
	startGroup := func(name string) {
		groupStartTime = time.Now()
		log("##[group]" + name)
	}
	endGroup := func() {
		dur := int(time.Since(groupStartTime).Seconds())
		log(fmt.Sprintf("##[endgroup:%d]", dur))
	}

	fail := func(step string, err error) {
		build.Status = "failed"
		build.ErrorMessage = fmt.Sprintf("%s: %s", step, err)
		build.ErrorCode = classifyRuntimeError(build.ErrorMessage)
		logf("FAILED at step '%s': %s", step, err)
	}

	// Check for cancellation
	checkCancel := func() bool {
		select {
		case <-ctx.Done():
			build.Status = "cancelled"
			build.ErrorMessage = "cancelled by user"
			build.ErrorCode = runtimeErrCancelled
			log("Build cancelled by user")
			return true
		default:
			return false
		}
	}

	logStructured("info", "deploy_started", map[string]interface{}{
		"build_id":  build.ID,
		"project":   project.Name,
		"runner_id": project.RunnerID,
		"mode":      project.DeployMode,
	})

	logf("Starting deploy for %s", project.Name)

	// Step 1: Git pull (optional)
	if project.GitPullBeforeBuild {
		startGroup("Git Pull")
		if err := b.runStep(ctx, &logBuf, build.ID, project.RepoPath, "git", "pull"); err != nil {
			endGroup()
			if checkCancel() {
				return
			}
			fail("git pull", err)
			return
		}
		endGroup()
	} else {
		startGroup("Git Pull (skipped)")
		endGroup()
	}

	if checkCancel() {
		return
	}

	// Step 2: Get commit SHA
	startGroup("Get Commit SHA")
	sha, err := b.runCapture(ctx, project.RepoPath, "git", "rev-parse", "--short", "HEAD")
	if err != nil {
		fail("get commit SHA", err)
		return
	}
	sha = strings.TrimSpace(sha)
	build.CommitSHA = sha
	logf("Commit: %s", sha)
	endGroup()

	if checkCancel() {
		return
	}

	// Determine deploy mode
	isFilesMode := project.DeployMode == "files"
	useAgent := project.RunnerID > 0
	isLocal := !useAgent && (project.SSHHost == "" || project.SSHHost == "localhost")

	var artifactPath string
	artifactHandedToAgent := false
	defer func() {
		if !artifactHandedToAgent && artifactPath != "" {
			removeManagedArtifact(artifactPath)
		}
	}()

	if isFilesMode {
		// FILES MODE — package files into tar.gz
		startGroup("Package Files")
		safeName := strings.ReplaceAll(project.Name, " ", "-")
		safeName = strings.ReplaceAll(safeName, "/", "-")
		artifactPath = managedArtifactPath(fmt.Sprintf("%s-%s.tar.gz", safeName, sha))

		ignorePatterns := loadIgnorePatterns(project.RepoPath)
		logf("Packaging files from %s", project.RepoPath)
		if len(ignorePatterns) > 0 {
			logf("Ignore patterns: %v", ignorePatterns)
		}

		if err := packageFiles(project.RepoPath, artifactPath, ignorePatterns); err != nil {
			endGroup()
			if checkCancel() {
				return
			}
			fail("package files", err)
			return
		}

		fi, _ := managedArtifactInfo(artifactPath)
		if fi != nil {
			logf("Archive: %.1f MB", float64(fi.Size())/(1024*1024))
		}
		endGroup()
	} else {
		// DOCKER MODE — build image
		startGroup("Docker Build")
		dockerArgs := []string{"build", "--no-cache", "--progress=plain", "-t", project.ImageName, "-f", project.DockerfilePath}
		for k, v := range project.BuildArgs {
			v = strings.ReplaceAll(v, "${GIT_SHA}", sha)
			dockerArgs = append(dockerArgs, "--build-arg", fmt.Sprintf("%s=%s", k, v))
		}
		dockerArgs = append(dockerArgs, ".")

		buildCtx, buildCancel := context.WithTimeout(ctx, appConfig.DockerBuildTimeout)
		defer buildCancel()
		if err := b.runStep(buildCtx, &logBuf, build.ID, project.RepoPath, "docker", dockerArgs...); err != nil {
			endGroup()
			if checkCancel() {
				return
			}
			fail("docker build", err)
			return
		}
		endGroup()
	}

	if checkCancel() {
		return
	}

	if useAgent {
		if !isFilesMode {
			// DOCKER AGENT DEPLOY — docker save, then hand off to agent
			safeName := strings.ReplaceAll(project.ImageName, "/", "-")
			artifactPath = managedArtifactPath(fmt.Sprintf("%s-%s.tar", safeName, sha))
			startGroup("Docker Save")
			logf("Saving image to %s", artifactPath)
			if err := b.runStep(ctx, &logBuf, build.ID, "", "docker", "save", "-o", artifactPath, project.ImageName); err != nil {
				endGroup()
				if checkCancel() {
					return
				}
				fail("docker save", err)
				return
			}
			endGroup()

			if checkCancel() {
				return
			}

			// Cleanup local Docker after save
			startGroup("Cleanup")
			if appConfig.DockerPrune {
				logf("Pruning local Docker images and build cache...")
				logOperationalError("docker image prune", b.runStepSilent(ctx, "", "docker", "image", "prune", "-af"))
				logOperationalError("docker builder prune", b.runStepSilent(ctx, "", "docker", "builder", "prune", "-af"))
				logf("Cleanup done")
			} else {
				logf("Docker prune skipped")
			}
			endGroup()

			if checkCancel() {
				return
			}
		}

		// Create job for agent (both docker and files mode)
		startGroup("Agent Deploy")
		logf("Handing off to agent...")
		_, err := createJob(build.ID, project.RunnerID, project, artifactPath)
		if err != nil {
			endGroup()
			if artifactPath != "" {
				removeManagedArtifact(artifactPath)
			}
			fail("create agent job", err)
			return
		}
		artifactHandedToAgent = true
		logf("Waiting for agent to pick up the job...")

		// Wait for agent to complete
		for {
			select {
			case <-ctx.Done():
				build.Status = "cancelled"
				build.ErrorMessage = "cancelled by user"
				build.ErrorCode = runtimeErrCancelled
				log("Build cancelled by user")
				endGroup()
				logOperationalError("cancel agent job", updateUnfinishedJobStatusByBuildIDWithError(build.ID, "cancelled", runtimeErrCancelled))
				removeManagedArtifact(artifactPath)
				return
			case <-time.After(3 * time.Second):
			}

			currentBuild, err := getBuild(build.ID)
			if err != nil {
				continue
			}
			if currentBuild.Status != "running" {
				// Agent already updated the build — don't let defer overwrite
				agentHandled = true
				return
			}
		}

	} else if isLocal {
		// LOCAL DEPLOY — no SCP/SSH needed
		startGroup("Local Deploy")
		deployDir := project.DeployDir
		if deployDir == "" {
			deployDir = project.RepoPath
		}
		services := project.ComposeServices

		// Compose down
		downArgs := append([]string{"compose", "down"}, strings.Fields(services)...)
		if err := b.runStep(ctx, &logBuf, build.ID, deployDir, "docker", downArgs...); err != nil {
			endGroup()
			if checkCancel() {
				return
			}
			fail("docker compose down", err)
			return
		}

		if checkCancel() {
			endGroup()
			return
		}

		// Compose up
		upArgs := append([]string{"compose", "up", "-d"}, strings.Fields(services)...)
		if err := b.runStep(ctx, &logBuf, build.ID, deployDir, "docker", upArgs...); err != nil {
			endGroup()
			if checkCancel() {
				return
			}
			fail("docker compose up", err)
			return
		}
		endGroup()
	} else {
		// REMOTE DEPLOY — SCP + SSH

		// Docker save
		safeName := strings.ReplaceAll(project.ImageName, "/", "-")
		tarPath := managedArtifactPath(fmt.Sprintf("%s-%s.tar", safeName, sha))
		startGroup("Docker Save")
		logf("Saving image to %s", tarPath)
		if err := b.runStep(ctx, &logBuf, build.ID, "", "docker", "save", "-o", tarPath, project.ImageName); err != nil {
			endGroup()
			if checkCancel() {
				return
			}
			fail("docker save", err)
			return
		}
		endGroup()
		defer removeManagedArtifact(tarPath)

		if checkCancel() {
			return
		}

		// SCP to remote
		startGroup("SCP Transfer")
		logf("Uploading to %s", project.SSHHost)
		scpCtx, scpCancel := context.WithTimeout(ctx, appConfig.SCPTimeout)
		defer scpCancel()
		if err := b.runStep(scpCtx, &logBuf, build.ID, "", "scp", tarPath, project.SSHHost+":/tmp/"); err != nil {
			endGroup()
			if checkCancel() {
				return
			}
			fail("scp", err)
			return
		}
		endGroup()

		if checkCancel() {
			return
		}

		// SSH deploy
		startGroup("SSH Deploy")
		deployScript := buildDeployScript(project, sha)
		sshCtx, sshCancel := context.WithTimeout(ctx, appConfig.SSHTimeout)
		defer sshCancel()
		if err := b.runStep(sshCtx, &logBuf, build.ID, "", "ssh", project.SSHHost, deployScript); err != nil {
			endGroup()
			if checkCancel() {
				return
			}
			fail("ssh deploy", err)
			return
		}
		endGroup()
	}

	if checkCancel() {
		return
	}

	// Health check (if configured) — skip for agent deploys (agent does its own)
	if !useAgent {
		if project.HealthURL != "" {
			startGroup("Health Check")
			logf("Checking %s", project.HealthURL)
			if err := b.healthCheck(ctx, project, isLocal); err != nil {
				logf("Health check failed: %s (deploy may still be OK)", err)
			} else {
				logf("Health check passed")
			}
			endGroup()
		}
	}

	build.Status = "success"
	logf("Deploy completed successfully in %ds", int(time.Since(startTime).Seconds()))
}

func buildDeployScript(project *Project, sha string) string {
	safeName := strings.ReplaceAll(project.ImageName, "/", "-")
	tarFile := fmt.Sprintf("/tmp/%s-%s.tar", safeName, sha)
	services := shellJoinFields(project.ComposeServices)

	script := fmt.Sprintf(`set -e
cd %s
echo "Loading Docker image..."
docker load -i %s
echo "Restarting services: %s"
docker compose down %s
docker compose up -d %s
echo "Cleaning up..."
rm -f %s
echo "Deploy complete!"`,
		shellQuote(project.DeployDir), shellQuote(tarFile), project.ComposeServices, services, services, shellQuote(tarFile))

	return script
}

func shellJoinFields(value string) string {
	fields := strings.Fields(value)
	quoted := make([]string, 0, len(fields))
	for _, field := range fields {
		quoted = append(quoted, shellQuote(field))
	}
	return strings.Join(quoted, " ")
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func (b *Builder) runStep(ctx context.Context, logBuf *strings.Builder, buildID int64, dir string, name string, args ...string) error {
	cmd, output, err := b.commandExecutor().Start(ctx, dir, name, args...)
	if err != nil {
		return err
	}
	defer output.Close()

	reader := bufio.NewReader(output)
	lineCount := 0
	var readErr error
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			line = strings.TrimRight(line, "\r\n")
		}
		if line != "" {
			line = redactSecrets(line)
			logBuf.WriteString(line + "\n")
			b.broker.Publish(buildID, line)
			lineCount++
			// Save to DB every 20 lines so API can return progress
			if lineCount%20 == 0 {
				_, err := db.Exec("UPDATE builds SET log=? WHERE id=?", logBuf.String(), buildID)
				logOperationalError("update build log", err)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			readErr = err
			break
		}
	}
	// Final save
	_, err = db.Exec("UPDATE builds SET log=? WHERE id=?", logBuf.String(), buildID)
	logOperationalError("update build log", err)

	waitErr := cmd.Wait()
	if readErr != nil {
		if waitErr != nil {
			return fmt.Errorf("read command output: %w; command wait: %v", readErr, waitErr)
		}
		return fmt.Errorf("read command output: %w", readErr)
	}
	return waitErr
}

func (b *Builder) runStepSilent(ctx context.Context, dir string, name string, args ...string) error {
	return b.commandExecutor().Run(ctx, dir, name, args...)
}

func (b *Builder) runCapture(ctx context.Context, dir string, name string, args ...string) (string, error) {
	out, err := b.commandExecutor().CombinedOutput(ctx, dir, name, args...)
	if err != nil {
		output := strings.TrimSpace(string(out))
		if output != "" {
			return string(out), fmt.Errorf("%w: %s", err, output)
		}
	}
	return string(out), err
}

func (b *Builder) healthCheck(ctx context.Context, project *Project, isLocal bool) error {
	// Try health check up to 30 times with 2s interval (60s total)
	checkCtx, cancel := context.WithTimeout(ctx, appConfig.HealthCheckTimeout)
	defer cancel()
	attempts := int(appConfig.HealthCheckTimeout / (2 * time.Second))
	if attempts < 1 {
		attempts = 1
	}

	// If health_container is set, use docker inspect
	if project.HealthContainer != "" {
		for i := 0; i < attempts; i++ {
			select {
			case <-checkCtx.Done():
				return fmt.Errorf("health check timed out")
			default:
			}

			inspectCmd := fmt.Sprintf("docker inspect --format='{{.State.Health.Status}}' %s 2>/dev/null || echo 'starting'", shellQuote(project.HealthContainer))
			var out []byte
			var err error
			if isLocal {
				out, err = b.commandExecutor().Output(checkCtx, "", "docker", "inspect", "--format={{.State.Health.Status}}", project.HealthContainer)
			} else {
				out, err = b.commandExecutor().Output(checkCtx, "", "ssh", project.SSHHost, inspectCmd)
			}
			if err == nil && strings.TrimSpace(string(out)) == "healthy" {
				return nil
			}
			time.Sleep(2 * time.Second)
		}
		return fmt.Errorf("container %s not healthy after %s", project.HealthContainer, appConfig.HealthCheckTimeout)
	}

	// Fallback: curl the health URL
	if project.HealthURL != "" {
		for i := 0; i < attempts; i++ {
			select {
			case <-checkCtx.Done():
				return fmt.Errorf("health check timed out")
			default:
			}

			curlCmd := fmt.Sprintf("curl -sf %s > /dev/null 2>&1", shellQuote(project.HealthURL))
			var err error
			if isLocal {
				err = b.commandExecutor().Run(checkCtx, "", "curl", "-sf", project.HealthURL)
			} else {
				err = b.commandExecutor().Run(checkCtx, "", "ssh", project.SSHHost, curlCmd)
			}
			if err == nil {
				return nil
			}
			time.Sleep(2 * time.Second)
		}
		return fmt.Errorf("health URL %s not responding after %s", project.HealthURL, appConfig.HealthCheckTimeout)
	}

	return nil
}

func buildDockerArgsJSON(buildArgs map[string]string) string {
	if len(buildArgs) == 0 {
		return "{}"
	}
	b, _ := json.Marshal(buildArgs)
	return string(b)
}

// loadIgnorePatterns reads .deployignore, falls back to .dockerignore
func loadIgnorePatterns(repoPath string) []string {
	// Always ignore these
	alwaysIgnore := []string{".git", ".deployignore", ".dockerignore"}

	patterns := alwaysIgnore

	ignoreFile := filepath.Join(repoPath, ".deployignore")
	data, err := os.ReadFile(ignoreFile)
	if err != nil {
		ignoreFile = filepath.Join(repoPath, ".dockerignore")
		data, err = os.ReadFile(ignoreFile)
		if err != nil {
			return patterns
		}
	}

	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		patterns = append(patterns, line)
	}
	return patterns
}

// shouldIgnore checks if a path matches any ignore pattern
func shouldIgnore(relPath string, patterns []string) bool {
	// Check each component of the path
	parts := strings.Split(relPath, string(filepath.Separator))
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		pattern = strings.TrimSuffix(pattern, "/")
		if strings.HasPrefix(pattern, "/") {
			anchored := strings.TrimPrefix(pattern, "/")
			if matched, _ := filepath.Match(anchored, relPath); matched {
				return true
			}
			if strings.HasPrefix(relPath, anchored+string(filepath.Separator)) {
				return true
			}
			continue
		}

		// Match against full relative path
		if matched, _ := filepath.Match(pattern, relPath); matched {
			return true
		}
		// Match against the base name
		base := filepath.Base(relPath)
		if matched, _ := filepath.Match(pattern, base); matched {
			return true
		}
		// Match against each path component (for directory patterns like "node_modules")
		for _, part := range parts {
			if matched, _ := filepath.Match(pattern, part); matched {
				return true
			}
		}
	}
	return false
}

// packageFiles creates a tar.gz archive of the repo directory
func packageFiles(repoPath, destPath string, ignorePatterns []string) error {
	outFile, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("create archive: %w", err)
	}
	defer outFile.Close()

	gzWriter := gzip.NewWriter(outFile)
	defer gzWriter.Close()

	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	return filepath.Walk(repoPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(repoPath, path)
		if err != nil {
			return err
		}

		// Skip root
		if relPath == "." {
			return nil
		}

		// Check ignore patterns
		if shouldIgnore(relPath, ignorePatterns) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		// Create tar header
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = relPath

		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing to package symlink %s", relPath)
		}

		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		// Write file content
		if !info.IsDir() {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()
			if _, err := io.Copy(tarWriter, f); err != nil {
				return err
			}
		}

		return nil
	})
}
