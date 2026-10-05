package app

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"
)

var runnerOperationsSchema struct {
	sync.Mutex
	database *database
}

const (
	operationHealthCheck    = "health_check"
	operationComposeStatus  = "compose_status"
	operationComposeRestart = "compose_restart"
	operationComposeStop    = "compose_stop"
	operationComposeLogs    = "compose_logs"
	maxOperationLogBytes    = 256 << 10
)

type RunnerTelemetry struct {
	RunnerID        int64     `json:"runner_id"`
	AgentVersion    string    `json:"agent_version"`
	Hostname        string    `json:"hostname"`
	OSName          string    `json:"os_name"`
	UptimeSeconds   int64     `json:"uptime_seconds"`
	DiskTotalBytes  int64     `json:"disk_total_bytes"`
	DiskFreeBytes   int64     `json:"disk_free_bytes"`
	DockerAvailable bool      `json:"docker_available"`
	DockerVersion   string    `json:"docker_version"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type RunnerOperation struct {
	ID              int64      `json:"id"`
	RunnerID        int64      `json:"runner_id"`
	ProjectID       int64      `json:"project_id"`
	Kind            string     `json:"kind"`
	Status          string     `json:"status"`
	DeployDir       string     `json:"deploy_dir"`
	ComposeFile     string     `json:"compose_file"`
	ComposeServices string     `json:"compose_services"`
	HealthURL       string     `json:"health_url"`
	HealthContainer string     `json:"health_container"`
	RequestedBy     string     `json:"requested_by"`
	ResultSummary   string     `json:"result_summary,omitempty"`
	Log             string     `json:"log,omitempty"`
	RequestedAt     time.Time  `json:"requested_at"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
}

type runnerDetail struct {
	Runner          Runner            `json:"runner"`
	Telemetry       *RunnerTelemetry  `json:"telemetry,omitempty"`
	Operations      []RunnerOperation `json:"operations"`
	DeploymentQueue []Job             `json:"deployment_queue"`
	ActiveTaskKind  string            `json:"active_task_kind,omitempty"`
	ActiveTaskID    int64             `json:"active_task_id,omitempty"`
}

func validRunnerOperationKind(kind string) bool {
	switch kind {
	case operationHealthCheck, operationComposeStatus, operationComposeRestart, operationComposeStop, operationComposeLogs:
		return true
	}
	return false
}

func ensureRunnerOperationsSchema() error {
	runnerOperationsSchema.Lock()
	defer runnerOperationsSchema.Unlock()
	if runnerOperationsSchema.database == db {
		return nil
	}
	if db.dialect == "postgres" {
		// PostgreSQL uses the embedded, versioned migration. Never apply the
		// SQLite compatibility DDL to a PostgreSQL connection.
		runnerOperationsSchema.database = db
		return nil
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS runner_telemetry (runner_id INTEGER PRIMARY KEY REFERENCES runners(id) ON DELETE CASCADE, agent_version TEXT NOT NULL DEFAULT '', hostname TEXT NOT NULL DEFAULT '', os_name TEXT NOT NULL DEFAULT '', uptime_seconds INTEGER NOT NULL DEFAULT 0, disk_total_bytes INTEGER NOT NULL DEFAULT 0, disk_free_bytes INTEGER NOT NULL DEFAULT 0, docker_available BOOLEAN NOT NULL DEFAULT 0, docker_version TEXT NOT NULL DEFAULT '', updated_at DATETIME NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS runner_operations (id INTEGER PRIMARY KEY AUTOINCREMENT, runner_id INTEGER NOT NULL REFERENCES runners(id), project_id INTEGER NOT NULL REFERENCES projects(id), kind TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', deploy_dir TEXT NOT NULL DEFAULT '', compose_file TEXT NOT NULL DEFAULT '', compose_services TEXT NOT NULL DEFAULT '', health_url TEXT NOT NULL DEFAULT '', health_container TEXT NOT NULL DEFAULT '', requested_by TEXT NOT NULL DEFAULT 'admin', result_summary TEXT NOT NULL DEFAULT '', log TEXT NOT NULL DEFAULT '', requested_at DATETIME NOT NULL, started_at DATETIME, completed_at DATETIME)`,
		`CREATE INDEX IF NOT EXISTS idx_runner_operations_queue ON runner_operations(runner_id, status, requested_at)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	for _, column := range []struct{ name, definition string }{{"active_task_kind", "TEXT NOT NULL DEFAULT ''"}, {"active_task_id", "INTEGER NOT NULL DEFAULT 0"}} {
		if _, err := db.Exec(`ALTER TABLE runners ADD COLUMN ` + column.name + ` ` + column.definition); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return err
		}
	}
	runnerOperationsSchema.database = db
	return nil
}

func saveRunnerTelemetry(runnerID int64, value RunnerTelemetry) error {
	now := time.Now().UTC()
	value.RunnerID = runnerID
	value.UpdatedAt = now
	_, err := db.Exec(`INSERT INTO runner_telemetry (runner_id,agent_version,hostname,os_name,uptime_seconds,disk_total_bytes,disk_free_bytes,docker_available,docker_version,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(runner_id) DO UPDATE SET agent_version=excluded.agent_version,hostname=excluded.hostname,os_name=excluded.os_name,uptime_seconds=excluded.uptime_seconds,disk_total_bytes=excluded.disk_total_bytes,disk_free_bytes=excluded.disk_free_bytes,docker_available=excluded.docker_available,docker_version=excluded.docker_version,updated_at=excluded.updated_at`, runnerID, value.AgentVersion, value.Hostname, value.OSName, value.UptimeSeconds, value.DiskTotalBytes, value.DiskFreeBytes, value.DockerAvailable, value.DockerVersion, formatSQLiteTime(now))
	return err
}

func queueRunnerOperation(runnerID, projectID int64, kind string) (*RunnerOperation, error) {
	if !validRunnerOperationKind(kind) {
		return nil, fmt.Errorf("unsupported operation %q", kind)
	}
	runner, err := getRunner(runnerID)
	if err != nil {
		return nil, err
	}
	project, err := getProject(projectID)
	if err != nil {
		return nil, err
	}
	if project.RunnerID != runner.ID {
		return nil, fmt.Errorf("project is not assigned to runner")
	}
	if runner.Status != "online" || runner.LastSeen == nil || time.Since(*runner.LastSeen) > time.Minute {
		return nil, fmt.Errorf("runner is offline or heartbeat is stale")
	}
	if kind == operationHealthCheck && project.HealthURL == "" && project.HealthContainer == "" {
		return nil, fmt.Errorf("project has no health check configured")
	}
	var docker bool
	var telemetryUpdated string
	if err := db.QueryRow(`SELECT docker_available,updated_at FROM runner_telemetry WHERE runner_id=?`, runnerID).Scan(&docker, &telemetryUpdated); err != nil {
		return nil, fmt.Errorf("runner does not report operation capability")
	}
	if updated := parseSQLiteTime(telemetryUpdated); updated.IsZero() || time.Since(updated) > time.Minute {
		return nil, fmt.Errorf("runner telemetry is stale")
	}
	if kind != operationHealthCheck && !docker {
		return nil, fmt.Errorf("runner does not report Docker support")
	}
	op := &RunnerOperation{RunnerID: runnerID, ProjectID: projectID, Kind: kind, Status: "pending", DeployDir: project.DeployDir, ComposeFile: project.ComposeFile, ComposeServices: project.ComposeServices, HealthURL: project.HealthURL, HealthContainer: project.HealthContainer, RequestedBy: "admin", RequestedAt: time.Now().UTC()}
	op.ID, err = insertID(`INSERT INTO runner_operations (runner_id,project_id,kind,status,deploy_dir,compose_file,compose_services,health_url,health_container,requested_by,result_summary,log,requested_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`, op.RunnerID, op.ProjectID, op.Kind, op.Status, op.DeployDir, op.ComposeFile, op.ComposeServices, op.HealthURL, op.HealthContainer, op.RequestedBy, "", "", formatSQLiteTime(op.RequestedAt))
	if err != nil {
		return nil, err
	}
	return op, nil
}

func scanRunnerOperation(row interface{ Scan(...any) error }) (*RunnerOperation, error) {
	op := &RunnerOperation{}
	var requested, started, completed string
	var startedOK, completedOK sql.NullString
	err := row.Scan(&op.ID, &op.RunnerID, &op.ProjectID, &op.Kind, &op.Status, &op.DeployDir, &op.ComposeFile, &op.ComposeServices, &op.HealthURL, &op.HealthContainer, &op.RequestedBy, &op.ResultSummary, &op.Log, &requested, &startedOK, &completedOK)
	if err != nil {
		return nil, err
	}
	op.RequestedAt = parseSQLiteTime(requested)
	if startedOK.Valid {
		started = parseSQLiteTime(startedOK.String).Format(time.RFC3339Nano)
		t := parseSQLiteTime(started)
		op.StartedAt = &t
	}
	if completedOK.Valid {
		completed = parseSQLiteTime(completedOK.String).Format(time.RFC3339Nano)
		t := parseSQLiteTime(completed)
		op.CompletedAt = &t
	}
	return op, nil
}

const runnerOperationColumns = `id,runner_id,project_id,kind,status,deploy_dir,compose_file,compose_services,health_url,health_container,requested_by,result_summary,log,requested_at,started_at,completed_at`

func getRunnerOperation(id int64) (*RunnerOperation, error) {
	return scanRunnerOperation(db.QueryRow(`SELECT `+runnerOperationColumns+` FROM runner_operations WHERE id=?`, id))
}

func listRunnerOperations(runnerID int64, limit int) ([]RunnerOperation, error) {
	rows, err := db.Query(`SELECT `+runnerOperationColumns+` FROM runner_operations WHERE runner_id=? ORDER BY id DESC LIMIT ?`, runnerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RunnerOperation
	for rows.Next() {
		op, err := scanRunnerOperation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *op)
	}
	return out, rows.Err()
}

func listRunnerDeploymentQueue(runnerID int64) ([]Job, error) {
	rows, err := db.Query(`SELECT id,build_id,runner_id,status,artifact_path,deploy_dir,compose_file,compose_services,image_name,health_url,health_container,mode,post_deploy,permissions,preserve,error_code,created_at,picked_at,completed_at FROM jobs WHERE runner_id=? AND status IN ('pending','running') ORDER BY id`, runnerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		var j Job
		var created string
		var picked, completed sql.NullString
		if err := rows.Scan(&j.ID, &j.BuildID, &j.RunnerID, &j.Status, &j.ArtifactPath, &j.DeployDir, &j.ComposeFile, &j.ComposeServices, &j.ImageName, &j.HealthURL, &j.HealthContainer, &j.Mode, &j.PostDeploy, &j.Permissions, &j.Preserve, &j.ErrorCode, &created, &picked, &completed); err != nil {
			return nil, err
		}
		j.CreatedAt = parseSQLiteTime(created)
		out = append(out, j)
	}
	return out, rows.Err()
}

func getRunnerDetail(id int64) (*runnerDetail, error) {
	r, err := getRunner(id)
	if err != nil {
		return nil, err
	}
	detail := &runnerDetail{Runner: *r}
	if err := db.QueryRow(`SELECT active_task_kind,active_task_id FROM runners WHERE id=?`, id).Scan(&detail.ActiveTaskKind, &detail.ActiveTaskID); err != nil {
		return nil, err
	}
	detail.Operations, err = listRunnerOperations(id, 20)
	if err != nil {
		return nil, err
	}
	detail.DeploymentQueue, err = listRunnerDeploymentQueue(id)
	if err != nil {
		return nil, err
	}
	var t RunnerTelemetry
	var updated string
	err = db.QueryRow(`SELECT runner_id,agent_version,hostname,os_name,uptime_seconds,disk_total_bytes,disk_free_bytes,docker_available,docker_version,updated_at FROM runner_telemetry WHERE runner_id=?`, id).Scan(&t.RunnerID, &t.AgentVersion, &t.Hostname, &t.OSName, &t.UptimeSeconds, &t.DiskTotalBytes, &t.DiskFreeBytes, &t.DockerAvailable, &t.DockerVersion, &updated)
	if err == nil {
		t.UpdatedAt = parseSQLiteTime(updated)
		detail.Telemetry = &t
	} else if err != sql.ErrNoRows {
		return nil, err
	}
	return detail, nil
}

func cancelPendingRunnerOperation(id int64) (bool, error) {
	res, err := db.Exec(`UPDATE runner_operations SET status='cancelled',completed_at=?,result_summary='cancelled by user' WHERE id=? AND status='pending'`, formatSQLiteTime(time.Now()), id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func claimRunnerOperation(ctx context.Context, runnerID int64) (*RunnerOperation, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var active string
	if err := tx.QueryRowContext(ctx, `SELECT active_task_kind FROM runners WHERE id=?`, runnerID).Scan(&active); err != nil {
		return nil, err
	}
	if active != "" {
		return nil, sql.ErrNoRows
	}
	var running bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE runner_id=? AND status='running')`, runnerID).Scan(&running); err != nil || running {
		if err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}
	var id int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM runner_operations WHERE runner_id=? AND status='pending' ORDER BY id LIMIT 1`, runnerID).Scan(&id); err != nil {
		return nil, err
	}
	now := formatSQLiteTime(time.Now())
	res, err := tx.ExecContext(ctx, `UPDATE runners SET active_task_kind='operation',active_task_id=? WHERE id=? AND active_task_kind=''`, id, runnerID)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return nil, sql.ErrNoRows
	}
	res, err = tx.ExecContext(ctx, `UPDATE runner_operations SET status='running',started_at=? WHERE id=? AND status='pending'`, now, id)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, sql.ErrNoRows
	}
	op, err := scanRunnerOperation(tx.QueryRowContext(ctx, `SELECT `+runnerOperationColumns+` FROM runner_operations WHERE id=?`, id))
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return op, nil
}

func completeRunnerOperation(runnerID, id int64, status, summary, log string) error {
	if status != "succeeded" && status != "failed" {
		return fmt.Errorf("invalid operation status")
	}
	if len(log) > maxOperationLogBytes {
		log = log[:maxOperationLogBytes]
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current string
	if err := tx.QueryRowContext(context.Background(), `SELECT status FROM runner_operations WHERE id=? AND runner_id=?`, id, runnerID).Scan(&current); err != nil {
		return err
	}
	if current == status {
		return tx.Commit()
	} // completion retry after a lost response
	if current != "running" {
		return sql.ErrNoRows
	}
	now := formatSQLiteTime(time.Now())
	res, err := tx.Exec(`UPDATE runner_operations SET status=?,result_summary=?,log=?,completed_at=? WHERE id=? AND runner_id=? AND status='running'`, status, summary, log, now, id, runnerID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return sql.ErrNoRows
	}
	if _, err = tx.Exec(`UPDATE runners SET active_task_kind='',active_task_id=0 WHERE id=? AND active_task_kind='operation' AND active_task_id=?`, runnerID, id); err != nil {
		return err
	}
	return tx.Commit()
}

func runnerHasQueuedOperation(runnerID int64) (bool, error) {
	var yes bool
	err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM runner_operations WHERE runner_id=? AND status IN ('pending','running'))`, runnerID).Scan(&yes)
	return yes, err
}

// claimPendingDeploymentForRunner shares the runner active-task guard with
// operations. It intentionally duplicates the small job claim query so legacy
// Job JSON remains unchanged while both task classes are serialized per runner.
func claimPendingDeploymentForRunner(ctx context.Context, runnerID int64) (*Job, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var active string
	if err := tx.QueryRowContext(ctx, `SELECT active_task_kind FROM runners WHERE id=?`, runnerID).Scan(&active); err != nil {
		return nil, err
	}
	if active != "" {
		return nil, sql.ErrNoRows
	}
	var operations bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runner_operations WHERE runner_id=? AND status IN ('pending','running'))`, runnerID).Scan(&operations); err != nil {
		return nil, err
	}
	if operations {
		return nil, sql.ErrNoRows
	}
	var id int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM jobs WHERE runner_id=? AND status='pending' ORDER BY id LIMIT 1`, runnerID).Scan(&id); err != nil {
		return nil, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE runners SET active_task_kind='deployment',active_task_id=? WHERE id=? AND active_task_kind=''`, id, runnerID)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, sql.ErrNoRows
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET status='running',picked_at=? WHERE id=? AND status='pending'`, formatSQLiteTime(time.Now()), id); err != nil {
		return nil, err
	}
	row := tx.QueryRowContext(ctx, `SELECT id,build_id,runner_id,status,artifact_path,deploy_dir,compose_file,compose_services,image_name,health_url,health_container,mode,post_deploy,permissions,preserve,error_code FROM jobs WHERE id=?`, id)
	job := &Job{}
	if err := row.Scan(&job.ID, &job.BuildID, &job.RunnerID, &job.Status, &job.ArtifactPath, &job.DeployDir, &job.ComposeFile, &job.ComposeServices, &job.ImageName, &job.HealthURL, &job.HealthContainer, &job.Mode, &job.PostDeploy, &job.Permissions, &job.Preserve, &job.ErrorCode); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return job, nil
}

func clearRunnerDeploymentTask(runnerID, jobID int64) {
	_, _ = db.Exec(`UPDATE runners SET active_task_kind='',active_task_id=0 WHERE id=? AND active_task_kind='deployment' AND active_task_id=?`, runnerID, jobID)
}
