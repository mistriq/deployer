package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type Project struct {
	ID                 int64             `json:"id"`
	Name               string            `json:"name"`
	RepoPath           string            `json:"repo_path"`
	DockerfilePath     string            `json:"dockerfile_path"`
	ComposeFile        string            `json:"compose_file"`
	ImageName          string            `json:"image_name"`
	SSHHost            string            `json:"ssh_host"`
	DeployDir          string            `json:"deploy_dir"`
	HealthURL          string            `json:"health_url"`
	HealthContainer    string            `json:"health_container"`
	BuildArgs          map[string]string `json:"build_args"`
	ComposeServices    string            `json:"compose_services"`
	GitPullBeforeBuild bool              `json:"git_pull_before_build"`
	RunnerID           int64             `json:"runner_id"`
	DeployMode         string            `json:"deploy_mode"`
	PostDeploy         string            `json:"post_deploy"`
	Permissions        string            `json:"permissions"`
	Preserve           string            `json:"preserve"`
	CreatedAt          time.Time         `json:"created_at"`
	LastBuild          *Build            `json:"last_build,omitempty"`
}

type Build struct {
	ID              int64      `json:"id"`
	ProjectID       int64      `json:"project_id"`
	Status          string     `json:"status"`
	CommitSHA       string     `json:"commit_sha"`
	StartedAt       time.Time  `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
	DurationSeconds *int       `json:"duration_seconds,omitempty"`
	Log             string     `json:"log"`
	ErrorMessage    string     `json:"error_message,omitempty"`
	ErrorCode       string     `json:"error_code,omitempty"`
	TriggeredBy     string     `json:"triggered_by"`
	ProjectName     string     `json:"project_name,omitempty"`
}

type Runner struct {
	ID        int64      `json:"id"`
	Name      string     `json:"name"`
	Token     string     `json:"token,omitempty"`
	Labels    string     `json:"labels"`
	Status    string     `json:"status"`
	LastSeen  *time.Time `json:"last_seen,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

type Job struct {
	ID              int64      `json:"id"`
	BuildID         int64      `json:"build_id"`
	RunnerID        int64      `json:"runner_id"`
	Status          string     `json:"status"`
	ArtifactPath    string     `json:"artifact_path"`
	DeployDir       string     `json:"deploy_dir"`
	ComposeFile     string     `json:"compose_file"`
	ComposeServices string     `json:"compose_services"`
	ImageName       string     `json:"image_name"`
	HealthURL       string     `json:"health_url"`
	HealthContainer string     `json:"health_container"`
	Mode            string     `json:"mode"`
	PostDeploy      string     `json:"post_deploy"`
	Permissions     string     `json:"permissions"`
	Preserve        string     `json:"preserve"`
	ErrorCode       string     `json:"error_code,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	PickedAt        *time.Time `json:"picked_at,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
}

type BuildAnnotation struct {
	ID        int64     `json:"id"`
	BuildID   int64     `json:"build_id"`
	Note      string    `json:"note"`
	CreatedAt time.Time `json:"created_at"`
}

var db *sql.DB

const sqliteLegacyTimeFormat = "2006-01-02 15:04:05"
const maxPersistedBuildLogBytes = 4 << 20

const (
	runtimeErrArtifactFailed    = "artifact_failed"
	runtimeErrCancelled         = "cancelled"
	runtimeErrCommandFailed     = "command_failed"
	runtimeErrHealthCheckFailed = "health_check_failed"
	runtimeErrPanic             = "panic"
	runtimeErrRunnerOffline     = "runner_offline"
	runtimeErrServerRestarted   = "server_restarted"
	runtimeErrValidationFailed  = "validation_failed"
)

var runtimeErrorCodes = []string{
	runtimeErrArtifactFailed,
	runtimeErrCancelled,
	runtimeErrCommandFailed,
	runtimeErrHealthCheckFailed,
	runtimeErrPanic,
	runtimeErrRunnerOffline,
	runtimeErrServerRestarted,
	runtimeErrValidationFailed,
}

var ownerPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+(:[A-Za-z0-9_.-]+)?$`)

type permissionsConfig struct {
	Owner string            `json:"owner"`
	Files map[string]string `json:"files"`
	Dirs  map[string]string `json:"dirs"`
}

// parseSQLiteTime parses various time formats from SQLite
func parseSQLiteTime(s string) time.Time {
	// Preferred storage format for new writes.
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}
	// Legacy SQLite format.
	if t, err := time.Parse(sqliteLegacyTimeFormat, s); err == nil {
		return t
	}
	// Try with timezone
	if t, err := time.Parse("2006-01-02 15:04:05-07:00", s); err == nil {
		return t
	}
	// Try Go default format (from time.Time.String())
	if t, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", s); err == nil {
		return t
	}
	return time.Time{}
}

// formatSQLiteTime formats time for SQLite storage
func formatSQLiteTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func initDB(path string) error {
	var err error
	db, err = sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}

	// Enable WAL mode for better concurrent access
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("set WAL: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		return fmt.Errorf("enable foreign keys: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		return fmt.Errorf("set busy timeout: %w", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS projects (
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
		runner_id INTEGER DEFAULT 0,
		created_at DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
	);

	CREATE TABLE IF NOT EXISTS builds (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		project_id INTEGER NOT NULL REFERENCES projects(id),
		status TEXT NOT NULL DEFAULT 'running',
		commit_sha TEXT DEFAULT '',
		started_at DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
		finished_at DATETIME,
		duration_seconds INTEGER,
		log TEXT DEFAULT '',
		error_message TEXT DEFAULT '',
		error_code TEXT DEFAULT '',
		triggered_by TEXT DEFAULT 'manual'
	);

	CREATE TABLE IF NOT EXISTS runners (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		token TEXT NOT NULL UNIQUE,
		labels TEXT DEFAULT '',
		status TEXT DEFAULT 'offline',
		last_seen DATETIME,
		created_at DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
	);

	CREATE TABLE IF NOT EXISTS jobs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		build_id INTEGER NOT NULL REFERENCES builds(id),
		runner_id INTEGER NOT NULL REFERENCES runners(id),
		status TEXT DEFAULT 'pending',
		artifact_path TEXT DEFAULT '',
		deploy_dir TEXT DEFAULT '',
		compose_file TEXT DEFAULT 'docker-compose.yml',
		compose_services TEXT DEFAULT '',
		image_name TEXT DEFAULT '',
		health_url TEXT DEFAULT '',
		health_container TEXT DEFAULT '',
		error_code TEXT DEFAULT '',
		created_at DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
		picked_at DATETIME,
		completed_at DATETIME
	);

	CREATE TABLE IF NOT EXISTS build_annotations (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		build_id INTEGER NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
		note TEXT NOT NULL,
		created_at DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
	);
	`
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	if err := applyMigrations(); err != nil {
		return err
	}

	if err := migratePlaintextRunnerTokens(); err != nil {
		return err
	}

	// Cleanup: local server-side builds cannot be resumed after restart, but
	// agent-backed jobs keep enough persisted state for agents to reconnect.
	res, err := db.Exec(`UPDATE builds
		SET status='cancelled', error_message='server restarted', error_code=?
		WHERE status='running'
		  AND id NOT IN (SELECT build_id FROM jobs WHERE status IN ('pending', 'running'))`, runtimeErrServerRestarted)
	if err != nil {
		return fmt.Errorf("cleanup running builds: %w", err)
	}
	if res != nil {
		if n, _ := res.RowsAffected(); n > 0 {
			logStructured("info", "orphaned_builds_cleaned", map[string]interface{}{"count": n})
		}
	}
	if _, err := db.Exec(`UPDATE jobs
		SET status='failed', error_code=?, completed_at=?
		WHERE status IN ('pending', 'running')
		  AND build_id NOT IN (SELECT id FROM builds WHERE status='running')`, runtimeErrServerRestarted, formatSQLiteTime(time.Now())); err != nil {
		return fmt.Errorf("cleanup unfinished jobs: %w", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_builds_one_running_per_project ON builds(project_id) WHERE status='running'`); err != nil {
		return fmt.Errorf("create running build guard: %w", err)
	}

	return nil
}

func applyMigrations() error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		id TEXT PRIMARY KEY,
		applied_at DATETIME NOT NULL
	)`); err != nil {
		return fmt.Errorf("create schema migrations table: %w", err)
	}

	tableMigrations := []struct {
		id  string
		sql string
	}{
		{"010_build_annotations", `CREATE TABLE IF NOT EXISTS build_annotations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			build_id INTEGER NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
			note TEXT NOT NULL,
			created_at DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		)`},
	}
	for _, migration := range tableMigrations {
		applied, err := migrationApplied(migration.id)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		if _, err := db.Exec(migration.sql); err != nil {
			return fmt.Errorf("apply migration %s: %w", migration.id, err)
		}
		if err := recordMigration(migration.id); err != nil {
			return err
		}
	}

	migrations := []struct {
		id         string
		table      string
		column     string
		definition string
	}{
		{"001_projects_runner_id", "projects", "runner_id", "runner_id INTEGER DEFAULT 0"},
		{"002_projects_deploy_mode", "projects", "deploy_mode", "deploy_mode TEXT DEFAULT 'docker'"},
		{"003_projects_post_deploy", "projects", "post_deploy", "post_deploy TEXT DEFAULT ''"},
		{"004_projects_permissions", "projects", "permissions", "permissions TEXT DEFAULT ''"},
		{"005_projects_preserve", "projects", "preserve", "preserve TEXT DEFAULT ''"},
		{"006_jobs_mode", "jobs", "mode", "mode TEXT DEFAULT 'docker'"},
		{"007_jobs_post_deploy", "jobs", "post_deploy", "post_deploy TEXT DEFAULT ''"},
		{"008_jobs_permissions", "jobs", "permissions", "permissions TEXT DEFAULT ''"},
		{"009_jobs_preserve", "jobs", "preserve", "preserve TEXT DEFAULT ''"},
		{"011_builds_error_code", "builds", "error_code", "error_code TEXT DEFAULT ''"},
		{"012_jobs_error_code", "jobs", "error_code", "error_code TEXT DEFAULT ''"},
	}
	for _, migration := range migrations {
		applied, err := migrationApplied(migration.id)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		if err := addColumnIfMissing(migration.table, migration.column, migration.definition); err != nil {
			return err
		}
		if err := recordMigration(migration.id); err != nil {
			return err
		}
	}
	return nil
}

func migrationApplied(id string) (bool, error) {
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE id=?`, id).Scan(&count); err != nil {
		return false, fmt.Errorf("check migration %s: %w", id, err)
	}
	return count > 0, nil
}

func recordMigration(id string) error {
	_, err := db.Exec(`INSERT INTO schema_migrations (id, applied_at) VALUES (?, ?)`, id, formatSQLiteTime(time.Now()))
	if err != nil {
		return fmt.Errorf("record migration %s: %w", id, err)
	}
	return nil
}

func addColumnIfMissing(table, column, definition string) error {
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return fmt.Errorf("inspect %s schema: %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk); err != nil {
			return fmt.Errorf("scan %s schema: %w", table, err)
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s schema: %w", table, err)
	}

	if _, err := db.Exec("ALTER TABLE " + table + " ADD COLUMN " + definition); err != nil {
		return fmt.Errorf("add %s.%s: %w", table, column, err)
	}
	return nil
}

func migratePlaintextRunnerTokens() error {
	rows, err := db.Query(`SELECT id, token FROM runners`)
	if err != nil {
		return fmt.Errorf("list runner tokens: %w", err)
	}
	defer rows.Close()

	type runnerToken struct {
		id    int64
		token string
	}
	var tokens []runnerToken
	for rows.Next() {
		var token runnerToken
		if err := rows.Scan(&token.id, &token.token); err != nil {
			return fmt.Errorf("scan runner token: %w", err)
		}
		tokens = append(tokens, token)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate runner tokens: %w", err)
	}

	for _, token := range tokens {
		if isHashedToken(token.token) {
			continue
		}
		if _, err := db.Exec(`UPDATE runners SET token=? WHERE id=?`, hashToken(token.token), token.id); err != nil {
			return fmt.Errorf("hash runner token %d: %w", token.id, err)
		}
	}
	return nil
}

// ========== Project CRUD ==========

func normalizeProject(p *Project) {
	p.Name = strings.TrimSpace(p.Name)
	p.RepoPath = strings.TrimSpace(p.RepoPath)
	p.DockerfilePath = strings.TrimSpace(p.DockerfilePath)
	p.ComposeFile = strings.TrimSpace(p.ComposeFile)
	p.ImageName = strings.TrimSpace(p.ImageName)
	p.SSHHost = strings.TrimSpace(p.SSHHost)
	p.DeployDir = strings.TrimSpace(p.DeployDir)
	p.HealthURL = strings.TrimSpace(p.HealthURL)
	p.HealthContainer = strings.TrimSpace(p.HealthContainer)
	p.ComposeServices = strings.TrimSpace(p.ComposeServices)
	p.DeployMode = strings.TrimSpace(p.DeployMode)

	if p.DockerfilePath == "" {
		p.DockerfilePath = "Dockerfile"
	}
	if p.ComposeFile == "" {
		p.ComposeFile = "docker-compose.yml"
	}
	if p.ComposeServices == "" {
		p.ComposeServices = "app worker"
	}
	if p.DeployMode == "" {
		p.DeployMode = "docker"
	}
	if p.BuildArgs == nil {
		p.BuildArgs = make(map[string]string)
	}
}

func validateProject(p *Project) error {
	normalizeProject(p)

	if p.Name == "" {
		return fmt.Errorf("name is required")
	}
	if p.RepoPath == "" {
		return fmt.Errorf("repo_path is required")
	}
	if p.DeployDir == "" {
		return fmt.Errorf("deploy_dir is required")
	}
	if err := validateDeployDir(p.DeployDir); err != nil {
		return err
	}
	if p.DeployMode != "docker" && p.DeployMode != "files" {
		return fmt.Errorf("deploy_mode must be docker or files")
	}
	if p.DeployMode == "docker" && p.ImageName == "" {
		return fmt.Errorf("image_name is required for docker mode")
	}
	if p.SSHHost != "" && !isSafeSSHHost(p.SSHHost) {
		return fmt.Errorf("ssh_host is invalid")
	}
	if p.ImageName != "" && hasShellSensitiveChars(p.ImageName) {
		return fmt.Errorf("image_name contains unsupported shell-sensitive characters")
	}
	if err := validateRelativePathField("dockerfile_path", p.DockerfilePath); err != nil {
		return err
	}
	if err := validateRelativePathField("compose_file", p.ComposeFile); err != nil {
		return err
	}
	for _, service := range strings.Fields(p.ComposeServices) {
		if hasShellSensitiveChars(service) {
			return fmt.Errorf("compose_services contains unsupported shell-sensitive characters")
		}
	}
	if p.HealthURL != "" {
		if err := validateHTTPURL("health_url", p.HealthURL); err != nil {
			return err
		}
	}
	if p.HealthContainer != "" && hasShellSensitiveChars(p.HealthContainer) {
		return fmt.Errorf("health_container contains unsupported shell-sensitive characters")
	}
	for _, line := range strings.Split(p.Preserve, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if _, err := cleanRelativeDeployPath(line); err != nil {
			return fmt.Errorf("preserve path %q is unsafe: %w", line, err)
		}
	}
	if p.Permissions != "" {
		if _, err := validatePermissionsConfig(p.Permissions); err != nil {
			return err
		}
	}
	if strings.ContainsRune(p.PostDeploy, 0) {
		return fmt.Errorf("post_deploy contains an invalid NUL byte")
	}
	if len(p.PostDeploy) > 16*1024 {
		return fmt.Errorf("post_deploy is too large")
	}

	return nil
}

func validateDeployDir(path string) error {
	clean := filepath.Clean(path)
	if clean == "." || clean == string(filepath.Separator) {
		return fmt.Errorf("deploy_dir must not be %q", path)
	}
	return nil
}

func validateRelativePathField(name, value string) error {
	if value == "" {
		return nil
	}
	if filepath.IsAbs(value) {
		return fmt.Errorf("%s must be relative", name)
	}
	if _, err := cleanRelativeDeployPath(value); err != nil {
		return fmt.Errorf("%s is unsafe: %w", name, err)
	}
	return nil
}

func cleanRelativeDeployPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	path = strings.Trim(path, "/")
	if path == "" || path == "." {
		return "", fmt.Errorf("empty path")
	}
	clean := filepath.Clean(path)
	if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path traversal is not allowed")
	}
	return clean, nil
}

func validateHTTPURL(name, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("%s is invalid: %w", name, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%s must use http or https", name)
	}
	if u.Host == "" {
		return fmt.Errorf("%s must include a host", name)
	}
	return nil
}

func hasShellSensitiveChars(value string) bool {
	return strings.ContainsAny(value, " \t\r\n;&|`$<>\\\"'")
}

func isSafeSSHHost(value string) bool {
	if value == "" || strings.HasPrefix(value, "-") || strings.ContainsAny(value, " \t\r\n;&|`$<>\\\"'") {
		return false
	}
	return true
}

func validatePermissionsConfig(raw string) (*permissionsConfig, error) {
	var permissions permissionsConfig
	if err := json.Unmarshal([]byte(raw), &permissions); err != nil {
		return nil, fmt.Errorf("permissions must be valid JSON: %w", err)
	}
	if permissions.Owner != "" {
		if strings.HasPrefix(permissions.Owner, "-") || !ownerPattern.MatchString(permissions.Owner) {
			return nil, fmt.Errorf("permissions owner is invalid")
		}
	}
	if err := validatePermissionPatterns("permissions.files", permissions.Files); err != nil {
		return nil, err
	}
	if err := validatePermissionPatterns("permissions.dirs", permissions.Dirs); err != nil {
		return nil, err
	}
	return &permissions, nil
}

func validatePermissionPatterns(name string, patterns map[string]string) error {
	for pattern, mode := range patterns {
		if _, err := cleanRelativeDeployPath(pattern); err != nil {
			return fmt.Errorf("%s pattern %q is unsafe: %w", name, pattern, err)
		}
		if _, err := filepath.Match(pattern, ""); err != nil {
			return fmt.Errorf("%s pattern %q is invalid: %w", name, pattern, err)
		}
		if !isOctalMode(mode) {
			return fmt.Errorf("%s mode %q is invalid", name, mode)
		}
	}
	return nil
}

func isOctalMode(mode string) bool {
	if len(mode) != 3 && len(mode) != 4 {
		return false
	}
	for _, ch := range mode {
		if ch < '0' || ch > '7' {
			return false
		}
	}
	return true
}

func validateSnapshotProject(p *Project) error {
	normalizeProject(p)

	if p.Name == "" {
		return fmt.Errorf("name is required")
	}
	if p.DeployDir == "" {
		return fmt.Errorf("deploy_dir is required")
	}
	if p.RunnerID <= 0 {
		return fmt.Errorf("project %s has no agent runner configured", p.Name)
	}

	return nil
}

func createProject(p *Project) error {
	if err := validateProject(p); err != nil {
		return err
	}
	argsJSON, _ := json.Marshal(p.BuildArgs)

	now := time.Now()
	res, err := db.Exec(`INSERT INTO projects (name, repo_path, dockerfile_path, compose_file, image_name, ssh_host, deploy_dir, health_url, health_container, build_args, compose_services, git_pull_before_build, runner_id, deploy_mode, post_deploy, permissions, preserve, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		p.Name, p.RepoPath, p.DockerfilePath, p.ComposeFile, p.ImageName, p.SSHHost, p.DeployDir, p.HealthURL, p.HealthContainer, string(argsJSON), p.ComposeServices, p.GitPullBeforeBuild, p.RunnerID, p.DeployMode, p.PostDeploy, p.Permissions, p.Preserve, formatSQLiteTime(now),
	)
	if err != nil {
		return err
	}
	p.ID, err = res.LastInsertId()
	if err != nil {
		return err
	}
	p.CreatedAt = now.UTC()
	return nil
}

func updateProject(p *Project) error {
	if err := validateProject(p); err != nil {
		return err
	}
	argsJSON, _ := json.Marshal(p.BuildArgs)
	_, err := db.Exec(`UPDATE projects SET name=?, repo_path=?, dockerfile_path=?, compose_file=?, image_name=?, ssh_host=?, deploy_dir=?, health_url=?, health_container=?, build_args=?, compose_services=?, git_pull_before_build=?, runner_id=?, deploy_mode=?, post_deploy=?, permissions=?, preserve=? WHERE id=?`,
		p.Name, p.RepoPath, p.DockerfilePath, p.ComposeFile, p.ImageName, p.SSHHost, p.DeployDir, p.HealthURL, p.HealthContainer, string(argsJSON), p.ComposeServices, p.GitPullBeforeBuild, p.RunnerID, p.DeployMode, p.PostDeploy, p.Permissions, p.Preserve, p.ID,
	)
	return err
}

func deleteProject(id int64) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM jobs WHERE build_id IN (SELECT id FROM builds WHERE project_id=?)", id); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM builds WHERE project_id=?", id); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM projects WHERE id=?", id); err != nil {
		return err
	}
	return tx.Commit()
}

func getProject(id int64) (*Project, error) {
	p := &Project{}
	var argsJSON string
	var createdAt string
	err := db.QueryRow(`SELECT id, name, repo_path, dockerfile_path, compose_file, image_name, ssh_host, deploy_dir, health_url, health_container, build_args, compose_services, git_pull_before_build, runner_id, deploy_mode, post_deploy, permissions, preserve, created_at FROM projects WHERE id=?`, id).Scan(
		&p.ID, &p.Name, &p.RepoPath, &p.DockerfilePath, &p.ComposeFile, &p.ImageName, &p.SSHHost, &p.DeployDir, &p.HealthURL, &p.HealthContainer, &argsJSON, &p.ComposeServices, &p.GitPullBeforeBuild, &p.RunnerID, &p.DeployMode, &p.PostDeploy, &p.Permissions, &p.Preserve, &createdAt,
	)
	if err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(argsJSON), &p.BuildArgs)
	if p.BuildArgs == nil {
		p.BuildArgs = make(map[string]string)
	}
	p.CreatedAt = parseSQLiteTime(createdAt)
	return p, nil
}

func listProjects() ([]Project, error) {
	rows, err := db.Query(`SELECT id, name, repo_path, dockerfile_path, compose_file, image_name, ssh_host, deploy_dir, health_url, health_container, build_args, compose_services, git_pull_before_build, runner_id, deploy_mode, post_deploy, permissions, preserve, created_at FROM projects ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var projects []Project
	for rows.Next() {
		var p Project
		var argsJSON, createdAt string
		if err := rows.Scan(&p.ID, &p.Name, &p.RepoPath, &p.DockerfilePath, &p.ComposeFile, &p.ImageName, &p.SSHHost, &p.DeployDir, &p.HealthURL, &p.HealthContainer, &argsJSON, &p.ComposeServices, &p.GitPullBeforeBuild, &p.RunnerID, &p.DeployMode, &p.PostDeploy, &p.Permissions, &p.Preserve, &createdAt); err != nil {
			return nil, err
		}
		json.Unmarshal([]byte(argsJSON), &p.BuildArgs)
		if p.BuildArgs == nil {
			p.BuildArgs = make(map[string]string)
		}
		p.CreatedAt = parseSQLiteTime(createdAt)
		projects = append(projects, p)
	}
	return projects, nil
}

func listProjectsWithLastBuild() ([]Project, error) {
	projects, err := listProjects()
	if err != nil {
		return nil, err
	}
	for i := range projects {
		build, err := getLastBuild(projects[i].ID)
		if err == nil && build != nil {
			projects[i].LastBuild = build
		}
	}
	return projects, nil
}

// ========== Build CRUD ==========

func createBuild(projectID int64, triggeredBy string) (*Build, error) {
	b := &Build{
		ProjectID:   projectID,
		Status:      "running",
		StartedAt:   time.Now(),
		TriggeredBy: triggeredBy,
	}
	res, err := db.Exec(`INSERT INTO builds (project_id, status, started_at, triggered_by) VALUES (?, ?, ?, ?)`,
		b.ProjectID, b.Status, formatSQLiteTime(b.StartedAt), b.TriggeredBy,
	)
	if err != nil {
		return nil, err
	}
	b.ID, err = res.LastInsertId()
	if err != nil {
		return nil, err
	}
	return b, nil
}

func updateBuild(b *Build) error {
	b.Log = trimBuildLog(b.Log)
	b.ErrorCode = normalizedBuildErrorCode(b.Status, b.ErrorMessage, b.ErrorCode)
	var finishedStr *string
	if b.FinishedAt != nil && !b.FinishedAt.IsZero() {
		s := formatSQLiteTime(*b.FinishedAt)
		finishedStr = &s
	}
	_, err := db.Exec(`UPDATE builds SET status=?, commit_sha=?, finished_at=?, duration_seconds=?, log=?, error_message=?, error_code=? WHERE id=?`,
		b.Status, b.CommitSHA, finishedStr, b.DurationSeconds, b.Log, b.ErrorMessage, b.ErrorCode, b.ID,
	)
	return err
}

func trimBuildLog(logText string) string {
	if len(logText) <= maxPersistedBuildLogBytes {
		return logText
	}
	return logText[len(logText)-maxPersistedBuildLogBytes:]
}

func normalizedBuildErrorCode(status, message, existing string) string {
	switch status {
	case "success", "running":
		return ""
	case "cancelled":
		if existing != "" {
			return existing
		}
		return runtimeErrCancelled
	case "failed":
		if existing != "" {
			return existing
		}
		return classifyRuntimeError(message)
	default:
		return existing
	}
}

func classifyRuntimeError(message string) string {
	text := strings.ToLower(strings.TrimSpace(message))
	switch {
	case text == "":
		return ""
	case strings.Contains(text, "cancel"):
		return runtimeErrCancelled
	case strings.Contains(text, "server restarted"):
		return runtimeErrServerRestarted
	case strings.Contains(text, "runner offline") || strings.Contains(text, "runner is offline"):
		return runtimeErrRunnerOffline
	case strings.Contains(text, "health"):
		return runtimeErrHealthCheckFailed
	case strings.Contains(text, "artifact") ||
		strings.Contains(text, "archive") ||
		strings.Contains(text, "snapshot") ||
		strings.Contains(text, "download") ||
		strings.Contains(text, "upload") ||
		strings.Contains(text, "extract") ||
		strings.Contains(text, "package files") ||
		strings.Contains(text, "package snapshot"):
		return runtimeErrArtifactFailed
	case strings.Contains(text, "panic"):
		return runtimeErrPanic
	case strings.Contains(text, "invalid") ||
		strings.Contains(text, "missing") ||
		strings.Contains(text, "not configured") ||
		strings.Contains(text, "not a directory"):
		return runtimeErrValidationFailed
	default:
		return runtimeErrCommandFailed
	}
}

func cleanupOldBuildLogs(retentionDays int) (int64, error) {
	if retentionDays <= 0 {
		return 0, nil
	}
	cutoffModifier := fmt.Sprintf("-%d days", retentionDays)
	res, err := db.Exec(`UPDATE builds SET log='' WHERE log <> '' AND finished_at IS NOT NULL AND datetime(finished_at) < datetime('now', ?)`, cutoffModifier)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func cancelRunningBuild(buildID int64, message string) (bool, error) {
	now := formatSQLiteTime(time.Now())
	res, err := db.Exec(`
		UPDATE builds
		SET status='cancelled',
			finished_at=?,
			duration_seconds=CAST(strftime('%s', ?) - strftime('%s', started_at) AS INTEGER),
			error_message=?,
			error_code=?
		WHERE id=? AND status='running'`,
		now, now, message, runtimeErrCancelled, buildID,
	)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n > 0 {
		if err := updateUnfinishedJobStatusByBuildIDWithError(buildID, "cancelled", runtimeErrCancelled); err != nil {
			return true, err
		}
	}
	return n > 0, nil
}

func getBuild(id int64) (*Build, error) {
	return getBuildContext(context.Background(), id)
}

func getBuildContext(ctx context.Context, id int64) (*Build, error) {
	b := &Build{}
	var startedAt string
	var finishedAt sql.NullString
	var durSeconds sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT b.id, b.project_id, b.status, b.commit_sha, b.started_at, b.finished_at, b.duration_seconds, b.log, b.error_message, b.error_code, b.triggered_by, p.name FROM builds b JOIN projects p ON p.id = b.project_id WHERE b.id=?`, id).Scan(
		&b.ID, &b.ProjectID, &b.Status, &b.CommitSHA, &startedAt, &finishedAt, &durSeconds, &b.Log, &b.ErrorMessage, &b.ErrorCode, &b.TriggeredBy, &b.ProjectName,
	)
	if err != nil {
		return nil, err
	}
	b.StartedAt = parseSQLiteTime(startedAt)
	if finishedAt.Valid {
		t := parseSQLiteTime(finishedAt.String)
		b.FinishedAt = &t
	}
	if durSeconds.Valid {
		d := int(durSeconds.Int64)
		b.DurationSeconds = &d
	}
	return b, nil
}

func getLastBuild(projectID int64) (*Build, error) {
	b := &Build{}
	var startedAt string
	var finishedAt sql.NullString
	var durSeconds sql.NullInt64
	err := db.QueryRow(`SELECT id, project_id, status, commit_sha, started_at, finished_at, duration_seconds, log, error_message, error_code, triggered_by FROM builds WHERE project_id=? ORDER BY id DESC LIMIT 1`, projectID).Scan(
		&b.ID, &b.ProjectID, &b.Status, &b.CommitSHA, &startedAt, &finishedAt, &durSeconds, &b.Log, &b.ErrorMessage, &b.ErrorCode, &b.TriggeredBy,
	)
	if err != nil {
		return nil, err
	}
	b.StartedAt = parseSQLiteTime(startedAt)
	if finishedAt.Valid {
		t := parseSQLiteTime(finishedAt.String)
		b.FinishedAt = &t
	}
	if durSeconds.Valid {
		d := int(durSeconds.Int64)
		b.DurationSeconds = &d
	}
	return b, nil
}

func listBuilds(projectID int64, limit int) ([]Build, error) {
	if limit == 0 {
		limit = 50
	}
	rows, err := db.Query(`SELECT id, project_id, status, commit_sha, started_at, finished_at, duration_seconds, error_message, error_code, triggered_by FROM builds WHERE project_id=? ORDER BY id DESC LIMIT ?`, projectID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var builds []Build
	for rows.Next() {
		var b Build
		var startedAt string
		var finishedAt sql.NullString
		var durSeconds sql.NullInt64
		if err := rows.Scan(&b.ID, &b.ProjectID, &b.Status, &b.CommitSHA, &startedAt, &finishedAt, &durSeconds, &b.ErrorMessage, &b.ErrorCode, &b.TriggeredBy); err != nil {
			return nil, err
		}
		b.StartedAt = parseSQLiteTime(startedAt)
		if finishedAt.Valid {
			t := parseSQLiteTime(finishedAt.String)
			b.FinishedAt = &t
		}
		if durSeconds.Valid {
			d := int(durSeconds.Int64)
			b.DurationSeconds = &d
		}
		builds = append(builds, b)
	}
	return builds, nil
}

func isProjectBuilding(projectID int64) bool {
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM builds WHERE project_id=? AND status='running'`, projectID).Scan(&count)
	return count > 0
}

func createBuildAnnotation(buildID int64, note string) (*BuildAnnotation, error) {
	note = strings.TrimSpace(note)
	if note == "" {
		return nil, fmt.Errorf("note is required")
	}
	if len(note) > 4096 {
		return nil, fmt.Errorf("note is too large")
	}
	now := time.Now().UTC()
	res, err := db.Exec(`INSERT INTO build_annotations (build_id, note, created_at) VALUES (?, ?, ?)`, buildID, note, formatSQLiteTime(now))
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	return &BuildAnnotation{
		ID:        id,
		BuildID:   buildID,
		Note:      note,
		CreatedAt: now,
	}, nil
}

func listBuildAnnotations(buildID int64) ([]BuildAnnotation, error) {
	rows, err := db.Query(`SELECT id, build_id, note, created_at FROM build_annotations WHERE build_id=? ORDER BY id`, buildID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var annotations []BuildAnnotation
	for rows.Next() {
		var annotation BuildAnnotation
		var createdAt string
		if err := rows.Scan(&annotation.ID, &annotation.BuildID, &annotation.Note, &createdAt); err != nil {
			return nil, err
		}
		annotation.CreatedAt = parseSQLiteTime(createdAt)
		annotations = append(annotations, annotation)
	}
	return annotations, rows.Err()
}

func deleteBuildAnnotation(buildID, annotationID int64) (bool, error) {
	res, err := db.Exec(`DELETE FROM build_annotations WHERE build_id=? AND id=?`, buildID, annotationID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ========== Runner CRUD ==========

func generateToken() string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("generate token: %v", err))
	}
	return hex.EncodeToString(b)
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func isHashedToken(token string) bool {
	return strings.HasPrefix(token, "sha256:")
}

func tokenMatches(stored, supplied string) bool {
	if isHashedToken(stored) {
		expected := hashToken(supplied)
		return subtle.ConstantTimeCompare([]byte(stored), []byte(expected)) == 1
	}
	return subtle.ConstantTimeCompare([]byte(stored), []byte(supplied)) == 1
}

func createRunner(r *Runner) error {
	if r.Token == "" {
		r.Token = generateToken()
	}
	tokenHash := hashToken(r.Token)
	now := time.Now()
	res, err := db.Exec(`INSERT INTO runners (name, token, labels, created_at) VALUES (?, ?, ?, ?)`,
		r.Name, tokenHash, r.Labels, formatSQLiteTime(now),
	)
	if err != nil {
		return err
	}
	r.ID, err = res.LastInsertId()
	if err != nil {
		return err
	}
	r.Status = "offline"
	r.CreatedAt = now.UTC()
	return nil
}

func getRunner(id int64) (*Runner, error) {
	r := &Runner{}
	var lastSeen sql.NullString
	var createdAt string
	err := db.QueryRow(`SELECT id, name, token, labels, status, last_seen, created_at FROM runners WHERE id=?`, id).Scan(
		&r.ID, &r.Name, &r.Token, &r.Labels, &r.Status, &lastSeen, &createdAt,
	)
	if err != nil {
		return nil, err
	}
	if lastSeen.Valid {
		t := parseSQLiteTime(lastSeen.String)
		r.LastSeen = &t
	}
	r.CreatedAt = parseSQLiteTime(createdAt)
	return r, nil
}

func getRunnerByToken(token string) (*Runner, error) {
	rows, err := db.Query(`SELECT id, name, token, labels, status, last_seen, created_at FROM runners ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var matched *Runner
	for rows.Next() {
		r := &Runner{}
		var lastSeen sql.NullString
		var createdAt string
		if err := rows.Scan(&r.ID, &r.Name, &r.Token, &r.Labels, &r.Status, &lastSeen, &createdAt); err != nil {
			return nil, err
		}
		if lastSeen.Valid {
			t := parseSQLiteTime(lastSeen.String)
			r.LastSeen = &t
		}
		r.CreatedAt = parseSQLiteTime(createdAt)
		if tokenMatches(r.Token, token) {
			matched = r
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if matched == nil {
		return nil, sql.ErrNoRows
	}
	return matched, nil
}

func listRunners() ([]Runner, error) {
	rows, err := db.Query(`SELECT id, name, token, labels, status, last_seen, created_at FROM runners ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runners []Runner
	for rows.Next() {
		var r Runner
		var lastSeen sql.NullString
		var createdAt string
		if err := rows.Scan(&r.ID, &r.Name, &r.Token, &r.Labels, &r.Status, &lastSeen, &createdAt); err != nil {
			return nil, err
		}
		if lastSeen.Valid {
			t := parseSQLiteTime(lastSeen.String)
			r.LastSeen = &t
		}
		r.CreatedAt = parseSQLiteTime(createdAt)
		runners = append(runners, r)
	}
	return runners, nil
}

func deleteRunner(id int64) error {
	_, err := db.Exec("DELETE FROM runners WHERE id=?", id)
	return err
}

func rotateRunnerToken(id int64) (*Runner, error) {
	token := generateToken()
	res, err := db.Exec(`UPDATE runners SET token=? WHERE id=?`, hashToken(token), id)
	if err != nil {
		return nil, err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return nil, err
	} else if affected != 1 {
		return nil, sql.ErrNoRows
	}
	runner, err := getRunner(id)
	if err != nil {
		return nil, err
	}
	runner.Token = token
	return runner, nil
}

func updateRunnerHeartbeat(id int64) error {
	return updateRunnerHeartbeatContext(context.Background(), id)
}

func updateRunnerHeartbeatContext(ctx context.Context, id int64) error {
	_, err := db.ExecContext(ctx, `UPDATE runners SET status='online', last_seen=? WHERE id=?`, formatSQLiteTime(time.Now()), id)
	return err
}

func markStaleRunners() error {
	_, err := db.Exec(`UPDATE runners SET status='offline' WHERE datetime(last_seen) < datetime('now', '-60 seconds') AND status='online'`)
	return err
}

// ========== Job CRUD ==========

func createJob(buildID, runnerID int64, project *Project, artifactPath string) (*Job, error) {
	mode := project.DeployMode
	if mode == "" {
		mode = "docker"
	}
	j := &Job{
		BuildID:         buildID,
		RunnerID:        runnerID,
		Status:          "pending",
		ArtifactPath:    artifactPath,
		DeployDir:       project.DeployDir,
		ComposeFile:     project.ComposeFile,
		ComposeServices: project.ComposeServices,
		ImageName:       project.ImageName,
		HealthURL:       project.HealthURL,
		HealthContainer: project.HealthContainer,
		Mode:            mode,
		PostDeploy:      project.PostDeploy,
		Permissions:     project.Permissions,
		Preserve:        project.Preserve,
	}
	now := time.Now()
	res, err := db.Exec(`INSERT INTO jobs (build_id, runner_id, status, artifact_path, deploy_dir, compose_file, compose_services, image_name, health_url, health_container, mode, post_deploy, permissions, preserve, error_code, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.BuildID, j.RunnerID, j.Status, j.ArtifactPath, j.DeployDir, j.ComposeFile, j.ComposeServices, j.ImageName, j.HealthURL, j.HealthContainer, j.Mode, j.PostDeploy, j.Permissions, j.Preserve, j.ErrorCode, formatSQLiteTime(now),
	)
	if err != nil {
		return nil, err
	}
	j.ID, err = res.LastInsertId()
	if err != nil {
		return nil, err
	}
	j.CreatedAt = now.UTC()
	return j, nil
}

func createSnapshotJob(buildID, runnerID int64, project *Project, artifactPath string) (*Job, error) {
	j := &Job{
		BuildID:      buildID,
		RunnerID:     runnerID,
		Status:       "pending",
		ArtifactPath: artifactPath,
		DeployDir:    project.DeployDir,
		ImageName:    project.Name,
		Mode:         "snapshot",
	}
	now := time.Now()
	res, err := db.Exec(`INSERT INTO jobs (build_id, runner_id, status, artifact_path, deploy_dir, image_name, mode, error_code, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.BuildID, j.RunnerID, j.Status, j.ArtifactPath, j.DeployDir, j.ImageName, j.Mode, j.ErrorCode, formatSQLiteTime(now),
	)
	if err != nil {
		return nil, err
	}
	j.ID, err = res.LastInsertId()
	if err != nil {
		return nil, err
	}
	j.CreatedAt = now.UTC()
	return j, nil
}

func getPendingJob(runnerID int64) (*Job, error) {
	j := &Job{}
	err := db.QueryRow(`SELECT id, build_id, runner_id, status, artifact_path, deploy_dir, compose_file, compose_services, image_name, health_url, health_container, mode, post_deploy, permissions, preserve, error_code FROM jobs WHERE runner_id=? AND status='pending' ORDER BY id ASC LIMIT 1`, runnerID).Scan(
		&j.ID, &j.BuildID, &j.RunnerID, &j.Status, &j.ArtifactPath, &j.DeployDir, &j.ComposeFile, &j.ComposeServices, &j.ImageName, &j.HealthURL, &j.HealthContainer, &j.Mode, &j.PostDeploy, &j.Permissions, &j.Preserve, &j.ErrorCode,
	)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func claimPendingJob(runnerID int64) (*Job, error) {
	return claimPendingJobContext(context.Background(), runnerID)
}

func claimPendingJobContext(ctx context.Context, runnerID int64) (*Job, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var jobID int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM jobs WHERE runner_id=? AND status='pending' ORDER BY id ASC LIMIT 1`, runnerID).Scan(&jobID); err != nil {
		return nil, err
	}

	res, err := tx.ExecContext(ctx, `UPDATE jobs SET status='running', picked_at=? WHERE id=? AND status='pending'`, formatSQLiteTime(time.Now()), jobID)
	if err != nil {
		return nil, err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return nil, err
	} else if affected != 1 {
		return nil, sql.ErrNoRows
	}

	j := &Job{}
	err = tx.QueryRowContext(ctx, `SELECT id, build_id, runner_id, status, artifact_path, deploy_dir, compose_file, compose_services, image_name, health_url, health_container, mode, post_deploy, permissions, preserve, error_code FROM jobs WHERE id=?`, jobID).Scan(
		&j.ID, &j.BuildID, &j.RunnerID, &j.Status, &j.ArtifactPath, &j.DeployDir, &j.ComposeFile, &j.ComposeServices, &j.ImageName, &j.HealthURL, &j.HealthContainer, &j.Mode, &j.PostDeploy, &j.Permissions, &j.Preserve, &j.ErrorCode,
	)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return j, nil
}

func getJobByBuildID(buildID int64) (*Job, error) {
	j := &Job{}
	err := db.QueryRow(`SELECT id, build_id, runner_id, status, artifact_path, deploy_dir, compose_file, compose_services, image_name, health_url, health_container, mode, post_deploy, permissions, preserve, error_code FROM jobs WHERE build_id=? ORDER BY id DESC LIMIT 1`, buildID).Scan(
		&j.ID, &j.BuildID, &j.RunnerID, &j.Status, &j.ArtifactPath, &j.DeployDir, &j.ComposeFile, &j.ComposeServices, &j.ImageName, &j.HealthURL, &j.HealthContainer, &j.Mode, &j.PostDeploy, &j.Permissions, &j.Preserve, &j.ErrorCode,
	)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func updateJobStatus(jobID int64, status string) error {
	return updateJobStatusWithError(jobID, status, "")
}

func updateJobStatusWithError(jobID int64, status, errorCode string) error {
	if status == "running" {
		_, err := db.Exec(`UPDATE jobs SET status=?, error_code='', picked_at=? WHERE id=?`, status, formatSQLiteTime(time.Now()), jobID)
		return err
	}
	if status == "completed" || status == "failed" || status == "cancelled" {
		if errorCode == "" && status == "cancelled" {
			errorCode = runtimeErrCancelled
		}
		_, err := db.Exec(`UPDATE jobs SET status=?, error_code=?, completed_at=? WHERE id=?`, status, errorCode, formatSQLiteTime(time.Now()), jobID)
		return err
	}
	_, err := db.Exec(`UPDATE jobs SET status=?, error_code=? WHERE id=?`, status, errorCode, jobID)
	return err
}

func updateUnfinishedJobStatusByBuildID(buildID int64, status string) error {
	errorCode := ""
	if status == "cancelled" {
		errorCode = runtimeErrCancelled
	}
	return updateUnfinishedJobStatusByBuildIDWithError(buildID, status, errorCode)
}

func updateUnfinishedJobStatusByBuildIDWithError(buildID int64, status, errorCode string) error {
	if status == "completed" || status == "failed" || status == "cancelled" {
		_, err := db.Exec(`UPDATE jobs SET status=?, error_code=?, completed_at=? WHERE build_id=? AND status IN ('pending', 'running')`, status, errorCode, formatSQLiteTime(time.Now()), buildID)
		return err
	}
	_, err := db.Exec(`UPDATE jobs SET status=?, error_code=? WHERE build_id=? AND status IN ('pending', 'running')`, status, errorCode, buildID)
	return err
}
