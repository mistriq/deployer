package app

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateProjectRejectsMissingRequiredFields(t *testing.T) {
	project := &Project{
		DeployMode: "docker",
	}

	if err := validateProject(project); err == nil {
		t.Fatal("expected validation error for missing name, repo_path, and deploy_dir")
	}
}

func TestValidateProjectRejectsDockerWithoutImage(t *testing.T) {
	project := &Project{
		Name:       "Dashboard",
		RepoPath:   "/srv/repos/dashboard",
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "docker",
	}

	if err := validateProject(project); err == nil {
		t.Fatal("expected validation error for missing docker image")
	}
}

func TestValidateProjectAllowsFilesWithoutImageAndAppliesDefaults(t *testing.T) {
	project := &Project{
		Name:       " Dashboard ",
		RepoPath:   " /srv/repos/dashboard ",
		DeployDir:  " /srv/apps/dashboard ",
		DeployMode: "files",
	}

	if err := validateProject(project); err != nil {
		t.Fatalf("expected files project to validate: %v", err)
	}
	if project.Name != "Dashboard" {
		t.Fatalf("expected project name to be trimmed, got %q", project.Name)
	}
	if project.DockerfilePath != "Dockerfile" {
		t.Fatalf("expected default dockerfile path, got %q", project.DockerfilePath)
	}
	if project.ComposeFile != "docker-compose.yml" {
		t.Fatalf("expected default compose file, got %q", project.ComposeFile)
	}
	if project.ComposeServices != "app worker" {
		t.Fatalf("expected default compose services, got %q", project.ComposeServices)
	}
	if project.BuildArgs == nil {
		t.Fatal("expected build args map to be initialized")
	}
}

func TestValidateProjectRejectsDangerousDeployDir(t *testing.T) {
	project := &Project{
		Name:       "Dashboard",
		RepoPath:   "/srv/repos/dashboard",
		DeployDir:  "/",
		DeployMode: "files",
	}

	if err := validateProject(project); err == nil {
		t.Fatal("expected validation error for root deploy_dir")
	}
}

func TestValidateProjectRejectsUnsafePreservePath(t *testing.T) {
	project := &Project{
		Name:       "Dashboard",
		RepoPath:   "/srv/repos/dashboard",
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "files",
		Preserve:   "../secrets",
	}

	if err := validateProject(project); err == nil {
		t.Fatal("expected validation error for unsafe preserve path")
	}
}

func TestFormatSQLiteTimeUsesUTC(t *testing.T) {
	loc := time.FixedZone("Test", 2*60*60)
	input := time.Date(2026, 6, 16, 12, 0, 0, 123456789, loc)
	got := formatSQLiteTime(input)
	if got != "2026-06-16T10:00:00.123456789Z" {
		t.Fatalf("unexpected formatted time %q", got)
	}
}

func TestParseSQLiteTimeSupportsNewAndLegacyFormats(t *testing.T) {
	rfc := parseSQLiteTime("2026-06-16T10:00:00.123456789Z")
	if rfc.IsZero() || rfc.Location() != time.UTC {
		t.Fatalf("expected RFC3339 UTC time, got %v", rfc)
	}
	legacy := parseSQLiteTime("2026-06-16 10:00:00")
	if legacy.IsZero() {
		t.Fatal("expected legacy timestamp to parse")
	}
}

func TestHashTokenDoesNotReturnPlaintext(t *testing.T) {
	token := "abc123def456"
	hash := hashToken(token)

	if hash == token {
		t.Fatal("expected token hash to differ from plaintext token")
	}
	if hash == "" {
		t.Fatal("expected non-empty token hash")
	}
}

func TestGetRunnerByTokenMatchesHashedToken(t *testing.T) {
	withTempDB(t)

	runner := &Runner{Name: "test-runner"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}

	matched, err := getRunnerByToken(runner.Token)
	if err != nil {
		t.Fatalf("get runner by token: %v", err)
	}
	if matched.ID != runner.ID {
		t.Fatalf("expected runner %d, got %d", runner.ID, matched.ID)
	}

	stored, err := getRunner(runner.ID)
	if err != nil {
		t.Fatalf("get runner: %v", err)
	}
	if !strings.HasPrefix(stored.Token, "sha256:") {
		t.Fatalf("expected stored token to be hashed, got %q", stored.Token)
	}
	if stored.Token == runner.Token {
		t.Fatal("expected stored token not to equal plaintext token")
	}
}

func TestRotateRunnerTokenInvalidatesOldToken(t *testing.T) {
	withTempDB(t)

	runner := &Runner{Name: "test-runner"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	oldToken := runner.Token

	rotated, err := rotateRunnerToken(runner.ID)
	if err != nil {
		t.Fatalf("rotate runner token: %v", err)
	}
	if rotated.Token == "" || rotated.Token == oldToken {
		t.Fatalf("expected a new plaintext token, got %q", rotated.Token)
	}

	if _, err := getRunnerByToken(oldToken); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected old token to be rejected, got %v", err)
	}
	if matched, err := getRunnerByToken(rotated.Token); err != nil {
		t.Fatalf("expected new token to authenticate: %v", err)
	} else if matched.ID != runner.ID {
		t.Fatalf("expected runner %d, got %d", runner.ID, matched.ID)
	}
}

func TestMigratePlaintextRunnerTokens(t *testing.T) {
	withTempDB(t)

	const plaintext = "legacy-runner-token"
	res, err := db.Exec(`INSERT INTO runners (name, token) VALUES (?, ?)`, "legacy", plaintext)
	if err != nil {
		t.Fatalf("insert legacy runner: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}

	if err := migratePlaintextRunnerTokens(); err != nil {
		t.Fatalf("migrate runner tokens: %v", err)
	}

	stored, err := getRunner(id)
	if err != nil {
		t.Fatalf("get migrated runner: %v", err)
	}
	if !strings.HasPrefix(stored.Token, "sha256:") {
		t.Fatalf("expected migrated token to be hashed, got %q", stored.Token)
	}
	if _, err := getRunnerByToken(plaintext); err != nil {
		t.Fatalf("expected migrated token to authenticate: %v", err)
	}
}

func TestProjectCRUDLifecycle(t *testing.T) {
	withTempDB(t)

	project := &Project{
		Name:               "Dashboard",
		RepoPath:           "/srv/repos/dashboard",
		DockerfilePath:     "deploy/Dockerfile",
		ComposeFile:        "deploy/compose.yml",
		ImageName:          "dashboard",
		SSHHost:            "deploy@example.com",
		DeployDir:          "/srv/apps/dashboard",
		HealthURL:          "https://example.com/health",
		HealthContainer:    "app",
		BuildArgs:          map[string]string{"APP_ENV": "production", "CACHE": "true"},
		ComposeServices:    "app worker",
		GitPullBeforeBuild: true,
		RunnerID:           42,
		DeployMode:         "docker",
		PostDeploy:         "php artisan migrate --force",
		Permissions:        `{"owner":"www-data:www-data","dirs":{"storage":"0775"}}`,
		Preserve:           "storage\n.env",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	if project.ID == 0 {
		t.Fatal("expected project ID to be populated")
	}
	if project.CreatedAt.IsZero() || project.CreatedAt.Location() != time.UTC {
		t.Fatalf("expected UTC created_at, got %v", project.CreatedAt)
	}

	got, err := getProject(project.ID)
	if err != nil {
		t.Fatalf("get project: %v", err)
	}
	if got.Name != project.Name || got.RepoPath != project.RepoPath || got.DeployMode != "docker" {
		t.Fatalf("unexpected project after create: %+v", got)
	}
	if got.BuildArgs["APP_ENV"] != "production" || got.BuildArgs["CACHE"] != "true" {
		t.Fatalf("unexpected build args after create: %#v", got.BuildArgs)
	}
	if got.Permissions != project.Permissions || got.Preserve != project.Preserve {
		t.Fatalf("expected permissions and preserve settings to round trip: %+v", got)
	}

	project.Name = "Dashboard API"
	project.DeployMode = "files"
	project.ImageName = ""
	project.HealthURL = ""
	project.HealthContainer = ""
	project.BuildArgs = map[string]string{"APP_ENV": "staging"}
	project.GitPullBeforeBuild = false
	project.PostDeploy = "systemctl reload php-fpm"
	if err := updateProject(project); err != nil {
		t.Fatalf("update project: %v", err)
	}

	updated, err := getProject(project.ID)
	if err != nil {
		t.Fatalf("get updated project: %v", err)
	}
	if updated.Name != "Dashboard API" || updated.DeployMode != "files" || updated.ImageName != "" {
		t.Fatalf("unexpected project after update: %+v", updated)
	}
	if updated.GitPullBeforeBuild {
		t.Fatal("expected git_pull_before_build update to persist")
	}
	if updated.BuildArgs["APP_ENV"] != "staging" || len(updated.BuildArgs) != 1 {
		t.Fatalf("unexpected build args after update: %#v", updated.BuildArgs)
	}

	projects, err := listProjects()
	if err != nil {
		t.Fatalf("list projects: %v", err)
	}
	if len(projects) != 1 || projects[0].ID != project.ID {
		t.Fatalf("expected updated project in list, got %+v", projects)
	}

	if err := deleteProject(project.ID); err != nil {
		t.Fatalf("delete project: %v", err)
	}
	if _, err := getProject(project.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected deleted project to be absent, got %v", err)
	}
}

func TestRunnerCRUDLifecycle(t *testing.T) {
	withTempDB(t)

	runner := &Runner{Name: "deploy-runner", Labels: "prod linux"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	plaintextToken := runner.Token
	if runner.ID == 0 || plaintextToken == "" {
		t.Fatalf("expected ID and plaintext token after create: %+v", runner)
	}
	if runner.Status != "offline" {
		t.Fatalf("expected new runner to be offline, got %q", runner.Status)
	}

	stored, err := getRunner(runner.ID)
	if err != nil {
		t.Fatalf("get runner: %v", err)
	}
	if stored.Name != runner.Name || stored.Labels != runner.Labels {
		t.Fatalf("unexpected runner after create: %+v", stored)
	}
	if !strings.HasPrefix(stored.Token, "sha256:") || stored.Token == plaintextToken {
		t.Fatalf("expected stored token hash, got %q", stored.Token)
	}

	runners, err := listRunners()
	if err != nil {
		t.Fatalf("list runners: %v", err)
	}
	if len(runners) != 1 || runners[0].ID != runner.ID {
		t.Fatalf("expected runner in list, got %+v", runners)
	}

	if err := updateRunnerHeartbeat(runner.ID); err != nil {
		t.Fatalf("update heartbeat: %v", err)
	}
	online, err := getRunner(runner.ID)
	if err != nil {
		t.Fatalf("get heartbeat runner: %v", err)
	}
	if online.Status != "online" || online.LastSeen == nil || online.LastSeen.IsZero() {
		t.Fatalf("expected online runner with last_seen, got %+v", online)
	}

	oldSeen := formatSQLiteTime(time.Now().Add(-2 * time.Minute))
	if _, err := db.Exec(`UPDATE runners SET last_seen=? WHERE id=?`, oldSeen, runner.ID); err != nil {
		t.Fatalf("age runner heartbeat: %v", err)
	}
	if err := markStaleRunners(); err != nil {
		t.Fatalf("mark stale runners: %v", err)
	}
	stale, err := getRunner(runner.ID)
	if err != nil {
		t.Fatalf("get stale runner: %v", err)
	}
	if stale.Status != "offline" {
		t.Fatalf("expected stale runner to be offline, got %q", stale.Status)
	}

	if _, err := rotateRunnerToken(runner.ID); err != nil {
		t.Fatalf("rotate runner token: %v", err)
	}
	if _, err := getRunnerByToken(plaintextToken); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected rotated token to invalidate old token, got %v", err)
	}

	if err := deleteRunner(runner.ID); err != nil {
		t.Fatalf("delete runner: %v", err)
	}
	if _, err := getRunner(runner.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected deleted runner to be absent, got %v", err)
	}
}

func TestBuildCRUDLifecycle(t *testing.T) {
	withTempDB(t)

	project := &Project{
		Name:       "Dashboard",
		RepoPath:   "/srv/repos/dashboard",
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "files",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}

	build, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create build: %v", err)
	}
	if build.ID == 0 || build.Status != "running" || build.TriggeredBy != "manual" {
		t.Fatalf("unexpected build after create: %+v", build)
	}
	if !isProjectBuilding(project.ID) {
		t.Fatal("expected project to have a running build")
	}

	duration := 12
	finishedAt := time.Now().UTC()
	build.Status = "success"
	build.CommitSHA = "abcdef123456"
	build.FinishedAt = &finishedAt
	build.DurationSeconds = &duration
	build.Log = "build finished"
	if err := updateBuild(build); err != nil {
		t.Fatalf("update build: %v", err)
	}
	if isProjectBuilding(project.ID) {
		t.Fatal("expected project running guard to clear after build update")
	}

	got, err := getBuild(build.ID)
	if err != nil {
		t.Fatalf("get build: %v", err)
	}
	if got.ProjectName != project.Name || got.Status != "success" || got.CommitSHA != "abcdef123456" {
		t.Fatalf("unexpected build after update: %+v", got)
	}
	if got.DurationSeconds == nil || *got.DurationSeconds != duration {
		t.Fatalf("expected duration to round trip, got %+v", got.DurationSeconds)
	}
	if got.FinishedAt == nil || got.FinishedAt.IsZero() {
		t.Fatalf("expected finished_at to round trip, got %+v", got.FinishedAt)
	}

	last, err := getLastBuild(project.ID)
	if err != nil {
		t.Fatalf("get last build: %v", err)
	}
	if last.ID != build.ID {
		t.Fatalf("expected last build %d, got %d", build.ID, last.ID)
	}

	builds, err := listBuilds(project.ID, 10)
	if err != nil {
		t.Fatalf("list builds: %v", err)
	}
	if len(builds) != 1 || builds[0].ID != build.ID {
		t.Fatalf("expected build in list, got %+v", builds)
	}

	cancelled, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create build to cancel: %v", err)
	}
	ok, err := cancelRunningBuild(cancelled.ID, "user cancelled")
	if err != nil {
		t.Fatalf("cancel running build: %v", err)
	}
	if !ok {
		t.Fatal("expected running build to be cancelled")
	}
	gotCancelled, err := getBuild(cancelled.ID)
	if err != nil {
		t.Fatalf("get cancelled build: %v", err)
	}
	if gotCancelled.Status != "cancelled" || gotCancelled.ErrorMessage != "user cancelled" {
		t.Fatalf("unexpected cancelled build: %+v", gotCancelled)
	}
	if gotCancelled.FinishedAt == nil || gotCancelled.DurationSeconds == nil {
		t.Fatalf("expected cancellation to set finish time and duration: %+v", gotCancelled)
	}
}

func TestRuntimeErrorCodesPersistForBuildsAndJobs(t *testing.T) {
	withTempDB(t)

	runner := &Runner{Name: "deploy-runner"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	project := &Project{
		Name:       "Dashboard",
		RepoPath:   "/srv/repos/dashboard",
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "files",
		RunnerID:   runner.ID,
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}

	failedBuild, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create failed build: %v", err)
	}
	failedBuild.Status = "failed"
	failedBuild.ErrorMessage = "health check: health URL not responding"
	if err := updateBuild(failedBuild); err != nil {
		t.Fatalf("update failed build: %v", err)
	}
	gotFailed, err := getBuild(failedBuild.ID)
	if err != nil {
		t.Fatalf("get failed build: %v", err)
	}
	if gotFailed.ErrorCode != runtimeErrHealthCheckFailed {
		t.Fatalf("expected health error code, got %+v", gotFailed)
	}

	cancelBuild, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create cancelled build: %v", err)
	}
	job, err := createJob(cancelBuild.ID, runner.ID, project, "/tmp/artifact.tar.gz")
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := updateJobStatus(job.ID, "running"); err != nil {
		t.Fatalf("mark job running: %v", err)
	}
	ok, err := cancelRunningBuild(cancelBuild.ID, "cancelled by user")
	if err != nil || !ok {
		t.Fatalf("cancel build ok=%v err=%v", ok, err)
	}
	gotCancelled, err := getBuild(cancelBuild.ID)
	if err != nil {
		t.Fatalf("get cancelled build: %v", err)
	}
	if gotCancelled.ErrorCode != runtimeErrCancelled {
		t.Fatalf("expected cancelled error code, got %+v", gotCancelled)
	}
	gotJob, err := getJobByBuildID(cancelBuild.ID)
	if err != nil {
		t.Fatalf("get cancelled job: %v", err)
	}
	if gotJob.Status != "cancelled" || gotJob.ErrorCode != runtimeErrCancelled {
		t.Fatalf("expected cancelled job error code, got %+v", gotJob)
	}
}

func TestBuildAnnotationLifecycle(t *testing.T) {
	withTempDB(t)

	project := &Project{
		Name:       "Dashboard",
		RepoPath:   "/srv/repos/dashboard",
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "files",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	build, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create build: %v", err)
	}

	annotation, err := createBuildAnnotation(build.ID, "rollback candidate")
	if err != nil {
		t.Fatalf("create annotation: %v", err)
	}
	if annotation.ID == 0 || annotation.Note != "rollback candidate" || annotation.BuildID != build.ID || annotation.CreatedAt.IsZero() {
		t.Fatalf("unexpected annotation: %+v", annotation)
	}
	if _, err := createBuildAnnotation(build.ID, " "); err == nil {
		t.Fatal("expected blank annotation to be rejected")
	}

	annotations, err := listBuildAnnotations(build.ID)
	if err != nil {
		t.Fatalf("list annotations: %v", err)
	}
	if len(annotations) != 1 || annotations[0].ID != annotation.ID {
		t.Fatalf("unexpected annotations: %+v", annotations)
	}

	ok, err := deleteBuildAnnotation(build.ID, annotation.ID)
	if err != nil {
		t.Fatalf("delete annotation: %v", err)
	}
	if !ok {
		t.Fatal("expected annotation to be deleted")
	}
	annotations, err = listBuildAnnotations(build.ID)
	if err != nil {
		t.Fatalf("list annotations after delete: %v", err)
	}
	if len(annotations) != 0 {
		t.Fatalf("expected no annotations after delete, got %+v", annotations)
	}
}

func TestJobLifecycle(t *testing.T) {
	withTempDB(t)

	runner := &Runner{Name: "deploy-runner"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	project := &Project{
		Name:            "Dashboard",
		RepoPath:        "/srv/repos/dashboard",
		DockerfilePath:  "Dockerfile",
		ComposeFile:     "compose.prod.yml",
		ImageName:       "dashboard",
		DeployDir:       "/srv/apps/dashboard",
		HealthURL:       "https://example.com/health",
		HealthContainer: "app",
		ComposeServices: "app worker",
		RunnerID:        runner.ID,
		DeployMode:      "docker",
		PostDeploy:      "systemctl reload nginx",
		Permissions:     `{"files":{"public/*":"0644"}}`,
		Preserve:        "storage",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	build, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create build: %v", err)
	}
	job, err := createJob(build.ID, runner.ID, project, "/tmp/dashboard.tar")
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if job.ID == 0 || job.Status != "pending" || job.CreatedAt.IsZero() {
		t.Fatalf("unexpected job after create: %+v", job)
	}

	pending, err := getPendingJob(runner.ID)
	if err != nil {
		t.Fatalf("get pending job: %v", err)
	}
	if pending.ID != job.ID || pending.Mode != "docker" || pending.PostDeploy != project.PostDeploy {
		t.Fatalf("unexpected pending job: %+v", pending)
	}
	if pending.ArtifactPath != "/tmp/dashboard.tar" || pending.Permissions != project.Permissions || pending.Preserve != project.Preserve {
		t.Fatalf("expected deployment metadata to round trip: %+v", pending)
	}

	claimed, err := claimPendingJob(runner.ID)
	if err != nil {
		t.Fatalf("claim pending job: %v", err)
	}
	if claimed.ID != job.ID || claimed.Status != "running" {
		t.Fatalf("unexpected claimed job: %+v", claimed)
	}
	if _, err := getPendingJob(runner.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected no pending jobs after claim, got %v", err)
	}

	if err := updateJobStatus(job.ID, "completed"); err != nil {
		t.Fatalf("complete job: %v", err)
	}
	completed, err := getJobByBuildID(build.ID)
	if err != nil {
		t.Fatalf("get job by build: %v", err)
	}
	if completed.ID != job.ID || completed.Status != "completed" {
		t.Fatalf("unexpected completed job: %+v", completed)
	}

	build.Status = "success"
	finishedAt := time.Now().UTC()
	duration := 1
	build.FinishedAt = &finishedAt
	build.DurationSeconds = &duration
	if err := updateBuild(build); err != nil {
		t.Fatalf("finish build: %v", err)
	}

	snapshotBuild, err := createBuild(project.ID, "snapshot")
	if err != nil {
		t.Fatalf("create snapshot build: %v", err)
	}
	snapshot, err := createSnapshotJob(snapshotBuild.ID, runner.ID, project, "/tmp/snapshot.tar")
	if err != nil {
		t.Fatalf("create snapshot job: %v", err)
	}
	if snapshot.Mode != "snapshot" || snapshot.ImageName != project.Name || snapshot.DeployDir != project.DeployDir {
		t.Fatalf("unexpected snapshot job: %+v", snapshot)
	}
}

func TestCleanupOldBuildLogs(t *testing.T) {
	withTempDB(t)

	project := &Project{
		Name:       "Dashboard",
		RepoPath:   "/srv/repos/dashboard",
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "files",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	oldBuild, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create old build: %v", err)
	}
	if _, err := db.Exec(`UPDATE builds SET status='success', finished_at=datetime('now', '-40 days'), log='old log' WHERE id=?`, oldBuild.ID); err != nil {
		t.Fatalf("update old build: %v", err)
	}
	newBuild, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create new build: %v", err)
	}
	if _, err := db.Exec(`UPDATE builds SET status='success', finished_at=datetime('now'), log='new log' WHERE id=?`, newBuild.ID); err != nil {
		t.Fatalf("update new build: %v", err)
	}

	affected, err := cleanupOldBuildLogs(30)
	if err != nil {
		t.Fatalf("cleanup old build logs: %v", err)
	}
	if affected != 1 {
		t.Fatalf("expected 1 cleaned build, got %d", affected)
	}

	gotOld, err := getBuild(oldBuild.ID)
	if err != nil {
		t.Fatalf("get old build: %v", err)
	}
	if gotOld.Log != "" {
		t.Fatalf("expected old build log to be cleared, got %q", gotOld.Log)
	}
	gotNew, err := getBuild(newBuild.ID)
	if err != nil {
		t.Fatalf("get new build: %v", err)
	}
	if gotNew.Log != "new log" {
		t.Fatalf("expected new build log to remain, got %q", gotNew.Log)
	}
}

func TestTrimBuildLog(t *testing.T) {
	shortLog := "hello"
	if got := trimBuildLog(shortLog); got != shortLog {
		t.Fatalf("expected short log unchanged, got %q", got)
	}

	longLog := strings.Repeat("a", maxPersistedBuildLogBytes) + "tail"
	got := trimBuildLog(longLog)
	if len(got) != maxPersistedBuildLogBytes {
		t.Fatalf("expected trimmed log length %d, got %d", maxPersistedBuildLogBytes, len(got))
	}
	if !strings.HasSuffix(got, "tail") {
		t.Fatal("expected trimmed log to keep newest content")
	}
}

func TestValidatePermissionsConfig(t *testing.T) {
	valid := `{"owner":"www-data:www-data","files":{"*.php":"0644","storage/*":"0660"},"dirs":{"storage":"0775"}}`
	if _, err := validatePermissionsConfig(valid); err != nil {
		t.Fatalf("expected valid permissions config: %v", err)
	}

	cases := []string{
		`{"owner":"-bad"}`,
		`{"files":{"../secret":"0644"}}`,
		`{"files":{"[":"0644"}}`,
		`{"dirs":{"storage":"0999"}}`,
	}
	for _, raw := range cases {
		if _, err := validatePermissionsConfig(raw); err == nil {
			t.Fatalf("expected invalid permissions config for %s", raw)
		}
	}
}

func TestClaimPendingJobClaimsOnlyOnce(t *testing.T) {
	withTempDB(t)

	runner := &Runner{Name: "test-runner"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	project := &Project{
		Name:       "Dashboard",
		RepoPath:   "/srv/repos/dashboard",
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "docker",
		ImageName:  "dashboard",
		RunnerID:   runner.ID,
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	build, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create build: %v", err)
	}
	if _, err := createJob(build.ID, runner.ID, project, "/tmp/artifact.tar"); err != nil {
		t.Fatalf("create job: %v", err)
	}

	job, err := claimPendingJob(runner.ID)
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}
	if job.Status != "running" {
		t.Fatalf("expected claimed job to be running, got %q", job.Status)
	}

	if _, err := claimPendingJob(runner.ID); err != sql.ErrNoRows {
		t.Fatalf("expected second claim to find no rows, got %v", err)
	}
}

func TestInitDBMigratesOlderSchema(t *testing.T) {
	oldDB := db
	if oldDB != nil {
		t.Cleanup(func() { db = oldDB })
	} else {
		t.Cleanup(func() { db = nil })
	}

	path := filepath.Join(t.TempDir(), "old.db")
	preDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open old db: %v", err)
	}
	_, err = preDB.Exec(`
		CREATE TABLE projects (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			repo_path TEXT NOT NULL,
			dockerfile_path TEXT DEFAULT 'Dockerfile',
			compose_file TEXT DEFAULT 'docker-compose.yml',
			image_name TEXT NOT NULL,
			ssh_host TEXT DEFAULT '',
			deploy_dir TEXT NOT NULL,
			health_url TEXT DEFAULT '',
			health_container TEXT DEFAULT '',
			build_args TEXT DEFAULT '{}',
			compose_services TEXT DEFAULT 'app worker',
			git_pull_before_build BOOLEAN DEFAULT 1,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			build_id INTEGER NOT NULL,
			runner_id INTEGER NOT NULL,
			status TEXT DEFAULT 'pending',
			artifact_path TEXT DEFAULT '',
			deploy_dir TEXT DEFAULT '',
			compose_file TEXT DEFAULT 'docker-compose.yml',
			compose_services TEXT DEFAULT '',
			image_name TEXT DEFAULT '',
			health_url TEXT DEFAULT '',
			health_container TEXT DEFAULT '',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			picked_at DATETIME,
			completed_at DATETIME
		);
	`)
	if err != nil {
		preDB.Close()
		t.Fatalf("create old schema: %v", err)
	}
	if err := preDB.Close(); err != nil {
		t.Fatalf("close old db: %v", err)
	}

	if err := initDB(path); err != nil {
		t.Fatalf("init old db: %v", err)
	}
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	for table, columns := range map[string][]string{
		"projects": {"runner_id", "deploy_mode", "post_deploy", "permissions", "preserve"},
		"builds":   {"error_code"},
		"jobs":     {"mode", "post_deploy", "permissions", "preserve", "error_code"},
	} {
		for _, column := range columns {
			exists, err := columnExists(table, column)
			if err != nil {
				t.Fatalf("check column %s.%s: %v", table, column, err)
			}
			if !exists {
				t.Fatalf("expected column %s.%s to exist", table, column)
			}
		}
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if count != 12 {
		t.Fatalf("expected 12 migration records, got %d", count)
	}
}

func TestInitDBPreservesRunningAgentJobsOnRestart(t *testing.T) {
	oldDB := db
	t.Cleanup(func() { db = oldDB })

	path := filepath.Join(t.TempDir(), "restart.db")
	if err := initDB(path); err != nil {
		t.Fatalf("init db: %v", err)
	}

	runner := &Runner{Name: "agent-runner"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}
	agentProject := &Project{
		Name:       "Agent",
		RepoPath:   "/srv/repos/agent",
		DeployDir:  "/srv/apps/agent",
		DeployMode: "files",
		RunnerID:   runner.ID,
	}
	if err := createProject(agentProject); err != nil {
		t.Fatalf("create agent project: %v", err)
	}
	agentBuild, err := createBuild(agentProject.ID, "manual")
	if err != nil {
		t.Fatalf("create agent build: %v", err)
	}
	agentJob, err := createJob(agentBuild.ID, runner.ID, agentProject, "")
	if err != nil {
		t.Fatalf("create agent job: %v", err)
	}
	if err := updateJobStatus(agentJob.ID, "running"); err != nil {
		t.Fatalf("mark agent job running: %v", err)
	}

	localProject := &Project{
		Name:       "Local",
		RepoPath:   "/srv/repos/local",
		DeployDir:  "/srv/apps/local",
		DeployMode: "files",
	}
	if err := createProject(localProject); err != nil {
		t.Fatalf("create local project: %v", err)
	}
	localBuild, err := createBuild(localProject.ID, "manual")
	if err != nil {
		t.Fatalf("create local build: %v", err)
	}

	orphanProject := &Project{
		Name:       "Orphan",
		RepoPath:   "/srv/repos/orphan",
		DeployDir:  "/srv/apps/orphan",
		DeployMode: "files",
		RunnerID:   runner.ID,
	}
	if err := createProject(orphanProject); err != nil {
		t.Fatalf("create orphan project: %v", err)
	}
	orphanBuild, err := createBuild(orphanProject.ID, "manual")
	if err != nil {
		t.Fatalf("create orphan build: %v", err)
	}
	finishedAt := time.Now().UTC()
	orphanBuild.Status = "failed"
	orphanBuild.FinishedAt = &finishedAt
	if err := updateBuild(orphanBuild); err != nil {
		t.Fatalf("finish orphan build: %v", err)
	}
	orphanJob, err := createJob(orphanBuild.ID, runner.ID, orphanProject, "")
	if err != nil {
		t.Fatalf("create orphan job: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db before restart: %v", err)
	}
	if err := initDB(path); err != nil {
		t.Fatalf("re-init db: %v", err)
	}
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	gotAgentBuild, err := getBuild(agentBuild.ID)
	if err != nil {
		t.Fatalf("get agent build: %v", err)
	}
	if gotAgentBuild.Status != "running" {
		t.Fatalf("expected agent build to remain running, got %q", gotAgentBuild.Status)
	}
	gotAgentJob, err := getJobByBuildID(agentBuild.ID)
	if err != nil {
		t.Fatalf("get agent job: %v", err)
	}
	if gotAgentJob.Status != "running" {
		t.Fatalf("expected agent job to remain running, got %q", gotAgentJob.Status)
	}

	gotLocalBuild, err := getBuild(localBuild.ID)
	if err != nil {
		t.Fatalf("get local build: %v", err)
	}
	if gotLocalBuild.Status != "cancelled" || gotLocalBuild.ErrorMessage != "server restarted" {
		t.Fatalf("expected local build to be cancelled after restart, got %+v", gotLocalBuild)
	}

	gotOrphanJob, err := getJobByBuildID(orphanJob.BuildID)
	if err != nil {
		t.Fatalf("get orphan job: %v", err)
	}
	if gotOrphanJob.Status != "failed" {
		t.Fatalf("expected orphan job to fail after restart, got %q", gotOrphanJob.Status)
	}
}

func columnExists(table, column string) (bool, error) {
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func withTempDB(t *testing.T) {
	t.Helper()

	oldDB := db
	if oldDB != nil {
		t.Cleanup(func() { db = oldDB })
	} else {
		t.Cleanup(func() { db = nil })
	}

	path := filepath.Join(t.TempDir(), "deployer-test.db")
	if err := initDB(path); err != nil {
		t.Fatalf("init temp db: %v", err)
	}
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})
}
