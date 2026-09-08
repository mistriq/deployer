package app

import (
	"archive/tar"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func postgresTestURL(t *testing.T) string {
	t.Helper()
	u := getenvDefault("DEPLOYER_TEST_POSTGRES_URL", "")
	if u == "" {
		t.Skip("DEPLOYER_TEST_POSTGRES_URL is not set")
	}
	return u
}

func TestPostgresDisposableFilesDeploymentEndToEnd(t *testing.T) {
	u := isolatedPostgres(t, postgresTestURL(t))
	oldDB, oldBroker, oldConfig := db, broker, appConfig
	oldControl, oldArtifact := agentControlClient, agentArtifactClient
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
		db = oldDB
		broker = oldBroker
		appConfig = oldConfig
		agentControlClient = oldControl
		agentArtifactClient = oldArtifact
	})
	if err := initPostgres(u); err != nil {
		t.Fatal(err)
	}
	broker = NewSSEBroker()
	appConfig = AppConfig{ArtifactDir: filepath.Join(t.TempDir(), "artifacts"), SnapshotDir: filepath.Join(t.TempDir(), "snapshots")}
	if err := ensureRuntimeDirs(appConfig); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	if err := os.MkdirAll(filepath.Join(target, "uploads"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "uploads", "customer.txt"), []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}
	r := &Runner{Name: "rehearsal-runner"}
	if err := createRunner(r); err != nil {
		t.Fatal(err)
	}
	p := &Project{Name: "rehearsal", RepoPath: "/tmp/disposable-repo", DeployDir: target, DeployMode: "files", RunnerID: r.ID, Preserve: "uploads"}
	if err := createProject(p); err != nil {
		t.Fatal(err)
	}
	b, err := createBuild(p.ID, "rehearsal")
	if err != nil {
		t.Fatal(err)
	}
	artifact := managedArtifactPath("rehearsal.tar.gz")
	if err := writeTestTarGz(artifact, func(tw *tar.Writer) error {
		body := []byte("new release")
		if err := tw.WriteHeader(&tar.Header{Name: "index.html", Mode: 0644, Size: int64(len(body))}); err != nil {
			return err
		}
		_, err := tw.Write(body)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := createJob(b.ID, r.ID, p, artifact); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/agent/poll", handleAgentPoll)
	mux.HandleFunc("/api/agent/artifact/", handleAgentArtifact)
	mux.HandleFunc("/api/agent/log/", handleAgentLog)
	mux.HandleFunc("/api/agent/complete/", handleAgentComplete)
	server := httptest.NewServer(wrapHTTPHandler(mux))
	defer server.Close()
	agentControlClient = server.Client()
	agentArtifactClient = server.Client()
	resp := agentRequest(t, server.URL, r.Token, http.MethodGet, "/api/agent/poll", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("poll=%d", resp.StatusCode)
	}
	var job Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	executeFilesJob(server.URL, r.Token, &job)
	if got, err := os.ReadFile(filepath.Join(target, "index.html")); err != nil || string(got) != "new release" {
		t.Fatalf("release missing: %q %v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(target, "uploads", "customer.txt")); err != nil || string(got) != "keep" {
		t.Fatalf("mutable data changed: %q %v", got, err)
	}
	completed, err := getBuild(b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != "success" {
		t.Fatalf("build=%+v", completed)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := initPostgres(u); err != nil {
		t.Fatal(err)
	}
	if _, err := getRunnerByToken(r.Token); err != nil {
		t.Fatalf("runner auth after restart: %v", err)
	}
	reloaded, err := getBuild(b.ID)
	if err != nil || reloaded.Status != "success" {
		t.Fatalf("history after restart: %+v %v", reloaded, err)
	}
}

func isolatedPostgres(t *testing.T, rawURL string) string {
	t.Helper()
	d, err := sql.Open("pgx", rawURL)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	schema := fmt.Sprintf("deployer_test_%d", time.Now().UnixNano())
	if _, err := d.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		d, err := sql.Open("pgx", rawURL)
		if err == nil {
			_, _ = d.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`)
			d.Close()
		}
	})
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

func TestPostgresOperationAndDeploymentClaimsSerializePerRunner(t *testing.T) {
	u := isolatedPostgres(t, postgresTestURL(t))
	old := db
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
		db = old
	})
	if err := initPostgres(u); err != nil {
		t.Fatal(err)
	}
	if err := ensureRunnerOperationsSchema(); err != nil {
		t.Fatal(err)
	}
	r := &Runner{Name: "operation-claim-runner"}
	if err := createRunner(r); err != nil {
		t.Fatal(err)
	}
	if err := updateRunnerHeartbeat(r.ID); err != nil {
		t.Fatal(err)
	}
	if err := saveRunnerTelemetry(r.ID, RunnerTelemetry{DockerAvailable: true}); err != nil {
		t.Fatal(err)
	}
	p := &Project{Name: "operation-claim-project", RepoPath: "/tmp/repo", ImageName: "image", DeployDir: "/tmp/target", RunnerID: r.ID, HealthURL: "https://example.test/health"}
	if err := createProject(p); err != nil {
		t.Fatal(err)
	}
	b, err := createBuild(p.ID, "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := createJob(b.ID, r.ID, p, "/tmp/artifact"); err != nil {
		t.Fatal(err)
	}
	if _, err := queueRunnerOperation(r.ID, p.ID, operationHealthCheck); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var opWon, deployWon int
	var mu sync.Mutex
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if op, err := claimRunnerOperation(context.Background(), r.ID); err == nil && op != nil {
				mu.Lock()
				opWon++
				mu.Unlock()
			}
		}()
		go func() {
			defer wg.Done()
			if job, err := claimPendingDeploymentForRunner(context.Background(), r.ID); err == nil && job != nil {
				mu.Lock()
				deployWon++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if opWon != 1 || deployWon != 0 {
		t.Fatalf("claims must serialize operation before deployment: op=%d deploy=%d", opWon, deployWon)
	}
	if err := completeRunnerOperation(r.ID, 1, "succeeded", "done", ""); err != nil {
		t.Fatal(err)
	}
	job, err := claimPendingDeploymentForRunner(context.Background(), r.ID)
	if err != nil || job == nil {
		t.Fatalf("deployment did not resume after op completion: job=%+v err=%v", job, err)
	}
	clearRunnerDeploymentTask(r.ID, job.ID)
	if _, err := queueRunnerOperation(r.ID, p.ID, operationHealthCheck); err != nil {
		t.Fatal(err)
	}
	var claimed, cancelled bool
	wg = sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := claimRunnerOperation(context.Background(), r.ID)
		mu.Lock()
		claimed = err == nil
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		ok, _ := cancelPendingRunnerOperation(2)
		mu.Lock()
		cancelled = ok
		mu.Unlock()
	}()
	wg.Wait()
	if claimed && cancelled {
		t.Fatal("pending operation was both claimed and cancelled")
	}
}

func TestPostgresConcurrentClaimsHeartbeatsAndLogs(t *testing.T) {
	u := isolatedPostgres(t, postgresTestURL(t))
	old := db
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
		db = old
	})
	if err := initPostgres(u); err != nil {
		t.Fatal(err)
	}
	r := &Runner{Name: "concurrent"}
	if err := createRunner(r); err != nil {
		t.Fatal(err)
	}
	p := &Project{Name: "concurrent", RepoPath: "/tmp/repo", DeployDir: "/tmp/deploy", DeployMode: "files", RunnerID: r.ID}
	if err := createProject(p); err != nil {
		t.Fatal(err)
	}
	b, err := createBuild(p.ID, "test")
	if err != nil {
		t.Fatal(err)
	}
	j, err := createJob(b.ID, r.ID, p, "")
	if err != nil {
		t.Fatal(err)
	}

	const workers = 24
	var wg sync.WaitGroup
	wg.Add(workers * 3)
	claimed := make(chan int64, workers)
	errs := make(chan error, workers*3)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			job, err := claimPendingJobContext(context.Background(), r.ID)
			if err == nil {
				claimed <- job.ID
			} else if err != sql.ErrNoRows {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			if err := updateRunnerHeartbeat(r.ID); err != nil {
				errs <- err
			}
		}()
		go func(n int) {
			defer wg.Done()
			err := appendBuildLog(b.ID, fmt.Sprintf("line-%d\n", n))
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(claimed)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent operation: %v", err)
	}
	ids := []int64{}
	for id := range claimed {
		ids = append(ids, id)
	}
	if len(ids) != 1 || ids[0] != j.ID {
		t.Fatalf("job claimed %d times: %v", len(ids), ids)
	}
	got, err := getBuild(b.ID)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < workers; i++ {
		if !strings.Contains(got.Log, fmt.Sprintf("line-%d\n", i)) {
			t.Errorf("missing concurrent log line %d", i)
		}
	}
	if err := updateJobStatus(j.ID, "completed"); err != nil {
		t.Fatal(err)
	}
	b.Log = got.Log
	finished := time.Now().UTC()
	duration := 1
	b.Status, b.FinishedAt, b.DurationSeconds = "success", &finished, &duration
	if err := updateBuild(b); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := initPostgres(u); err != nil {
		t.Fatal(err)
	}
	reloaded, err := getBuild(b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Status != "success" || reloaded.FinishedAt == nil {
		t.Fatalf("deployment lifecycle did not survive restart: %+v", reloaded)
	}
}

func TestSQLiteImportPreservesRepresentativeData(t *testing.T) {
	u := isolatedPostgres(t, postgresTestURL(t))
	old := db
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
		db = old
	})
	path := filepath.Join(t.TempDir(), "source.db")
	if err := initDB(path); err != nil {
		t.Fatal(err)
	}
	r := &Runner{Name: "import-runner", Token: "compatibility-secret", Labels: "prod"}
	if err := createRunner(r); err != nil {
		t.Fatal(err)
	}
	p := &Project{Name: "import-project", RepoPath: "/srv/repo", DeployDir: "/srv/deploy", DeployMode: "files", RunnerID: r.ID, Preserve: "uploads\nrecords.db", BuildArgs: map[string]string{"MODE": "prod"}}
	if err := createProject(p); err != nil {
		t.Fatal(err)
	}
	b, err := createBuild(p.ID, "import-test")
	if err != nil {
		t.Fatal(err)
	}
	b.Status = "failed"
	b.CommitSHA = "abc123"
	now := time.Now().UTC()
	b.FinishedAt = &now
	b.Log = "representative log"
	b.ErrorMessage = "expected"
	if err := updateBuild(b); err != nil {
		t.Fatal(err)
	}
	if _, err := createBuildAnnotation(b.ID, "keep this note"); err != nil {
		t.Fatal(err)
	}
	if _, err := createJob(b.ID, r.ID, p, "/tmp/artifact"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	var report bytes.Buffer
	if err := ImportSQLite(context.Background(), path, u, &report); err != nil {
		t.Fatal(err)
	}
	if err := initPostgres(u); err != nil {
		t.Fatal(err)
	}
	gotP, err := getProject(p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotP.Name != p.Name || gotP.Preserve != p.Preserve || gotP.RunnerID != r.ID || gotP.BuildArgs["MODE"] != "prod" {
		t.Fatalf("project changed: %+v", gotP)
	}
	gotR, err := getRunnerByToken("compatibility-secret")
	if err != nil {
		t.Fatal(err)
	}
	if gotR.ID != r.ID || !isHashedToken(gotR.Token) {
		t.Fatalf("runner authentication changed: %+v", gotR)
	}
	gotB, err := getBuild(b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotB.CommitSHA != b.CommitSHA || gotB.Log != b.Log || gotB.ErrorMessage != b.ErrorMessage {
		t.Fatalf("build history changed: %+v", gotB)
	}
	annotations, err := listBuildAnnotations(b.ID)
	if err != nil || len(annotations) != 1 || annotations[0].Note != "keep this note" {
		t.Fatalf("annotations changed: %+v %v", annotations, err)
	}
	if report.Len() == 0 {
		t.Fatal("missing verification report")
	}
}
