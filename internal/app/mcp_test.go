package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMCPProtocolListsReadToolsAndUsesBearerAuth(t *testing.T) {
	var authorization string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `[{"id":1,"name":"site","build_args":{"TOKEN":"super-secret"}}]`)
	}))
	defer api.Close()
	s := &mcpServer{baseURL: api.URL, readToken: "read-secret", client: api.Client()}
	input := strings.NewReader("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"projects_list\",\"arguments\":{}}}\n")
	var output bytes.Buffer
	if err := s.serve(context.Background(), input, &output); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d protocol responses: %s", len(lines), output.String())
	}
	if !strings.Contains(lines[0], mcpProtocolVersion) || !strings.Contains(lines[1], "deployment_preview") {
		t.Fatalf("missing MCP handshake/tools: %s", output.String())
	}
	if strings.Contains(lines[1], "deployment_trigger") {
		t.Fatalf("write tool exposed without write credential: %s", lines[1])
	}
	if strings.Contains(lines[2], "super-secret") || !strings.Contains(lines[2], "[REDACTED]") {
		t.Fatalf("secret was not redacted: %s", lines[2])
	}
	if authorization != "Bearer read-secret" {
		t.Fatalf("Authorization = %q", authorization)
	}
}

func TestMCPWriteToolForwardsWriteTokenAndIdempotencyKey(t *testing.T) {
	var authorization, key string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization, key = r.Header.Get("Authorization"), r.Header.Get("Idempotency-Key")
		io.WriteString(w, `{"build_id":42}`)
	}))
	defer api.Close()
	s := &mcpServer{baseURL: api.URL, readToken: "read", writeToken: "write", client: api.Client()}
	result, err := s.call(context.Background(), mcpCallParams{Name: "deployment_trigger", Arguments: map[string]interface{}{"project_id": float64(7), "idempotency_key": "deploy-abc-123"}})
	if err != nil {
		t.Fatal(err)
	}
	if authorization != "Bearer write" || key != "deploy-abc-123" {
		t.Fatalf("auth=%q key=%q", authorization, key)
	}
	encoded, _ := json.Marshal(result.StructuredContent)
	if !strings.Contains(string(encoded), `"build_id":42`) {
		t.Fatalf("unexpected result: %s", encoded)
	}
}

func TestMCPWrappedAPIEndToEndPreviewTriggerFollowFailure(t *testing.T) {
	u := isolatedPostgres(t, postgresTestURL(t))
	oldDB := db
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
		db = oldDB
	})
	if err := initPostgres(u); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEPLOYER_MCP_READ_TOKEN", "real-read-token")
	t.Setenv("DEPLOYER_MCP_WRITE_TOKEN", "real-write-token")
	project := &Project{Name: "mcp-real-api", RepoPath: t.TempDir(), DeployDir: t.TempDir(), DeployMode: "files"}
	if err := createProject(project); err != nil {
		t.Fatal(err)
	}
	oldBuilder, oldBroker := builder, broker
	broker = NewSSEBroker()
	builder = NewBuilder(broker)
	testBuilder := builder
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		testBuilder.Shutdown(ctx)
		builder = oldBuilder
		broker = oldBroker
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/api/projects", handleAPIProjects)
	mux.HandleFunc("/api/projects/", handleAPIProject)
	mux.HandleFunc("/api/builds/", handleAPIBuild)
	api := httptest.NewServer(wrapHTTPHandler(mux))
	defer api.Close()
	s := &mcpServer{baseURL: api.URL, readToken: "real-read-token", writeToken: "real-write-token", client: api.Client()}
	preview, err := s.call(context.Background(), mcpCallParams{Name: "deployment_preview", Arguments: map[string]interface{}{"project_id": float64(project.ID)}})
	if err != nil || preview.IsError {
		t.Fatalf("preview err=%v result=%+v", err, preview)
	}
	args := map[string]interface{}{"project_id": float64(project.ID), "idempotency_key": "real-api-deploy-123"}
	trigger, err := s.call(context.Background(), mcpCallParams{Name: "deployment_trigger", Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	triggerRetry, err := s.call(context.Background(), mcpCallParams{Name: "deployment_trigger", Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	first := trigger.StructuredContent.(map[string]interface{})["build_id"]
	second := triggerRetry.StructuredContent.(map[string]interface{})["build_id"]
	if first != second {
		t.Fatalf("idempotent IDs differ: %v %v", first, second)
	}
	buildID := int64(first.(float64))
	deadline := time.Now().Add(5 * time.Second)
	for {
		build, err := getBuild(buildID)
		if err != nil {
			t.Fatal(err)
		}
		if build.Status != "running" {
			if build.Status != "failed" {
				t.Fatalf("status=%s", build.Status)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("build did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	follow, err := s.call(context.Background(), mcpCallParams{Name: "build_get", Arguments: map[string]interface{}{"build_id": float64(buildID), "log_bytes": float64(4096)}})
	if err != nil || follow.StructuredContent.(map[string]interface{})["status"] != "failed" {
		t.Fatalf("follow err=%v result=%+v", err, follow)
	}
	diagnosis, err := s.call(context.Background(), mcpCallParams{Name: "build_failure_summary", Arguments: map[string]interface{}{"build_id": float64(buildID)}})
	if err != nil || diagnosis.StructuredContent.(map[string]interface{})["has_failure"] != true {
		t.Fatalf("diagnosis err=%v result=%+v", err, diagnosis)
	}
}

func TestMCPWrappedAPIRequestsSnapshotAndAgentCompletesArtifact(t *testing.T) {
	u := isolatedPostgres(t, postgresTestURL(t))
	oldDB := db
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
		db = oldDB
	})
	if err := initPostgres(u); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEPLOYER_MCP_READ_TOKEN", "snapshot-read-token")
	t.Setenv("DEPLOYER_MCP_WRITE_TOKEN", "snapshot-write-token")
	oldConfig := appConfig
	oldArtifactStorage := artifactStorage
	appConfig.SnapshotDir = t.TempDir()
	appConfig.ArtifactDir = t.TempDir()
	configureArtifactStorage(appConfig)
	t.Cleanup(func() { appConfig = oldConfig; artifactStorage = oldArtifactStorage })
	runner := &Runner{Name: "snapshot-runner"}
	if err := createRunner(runner); err != nil {
		t.Fatal(err)
	}
	deployDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(deployDir, "state.txt"), []byte("preserved state"), 0600); err != nil {
		t.Fatal(err)
	}
	project := &Project{Name: "mcp-snapshot", RepoPath: t.TempDir(), DeployDir: deployDir, DeployMode: "files", RunnerID: runner.ID}
	if err := createProject(project); err != nil {
		t.Fatal(err)
	}
	oldBuilder, oldBroker := builder, broker
	broker = NewSSEBroker()
	builder = NewBuilder(broker)
	testBuilder := builder
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		testBuilder.Shutdown(ctx)
		builder = oldBuilder
		broker = oldBroker
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/api/projects/", handleAPIProject)
	mux.HandleFunc("/api/builds/", handleAPIBuild)
	mux.HandleFunc("/api/agent/log/", handleAgentLog)
	mux.HandleFunc("/api/agent/snapshot/", handleAgentSnapshotUpload)
	mux.HandleFunc("/api/agent/complete/", handleAgentComplete)
	api := httptest.NewServer(wrapHTTPHandler(mux))
	defer api.Close()
	s := &mcpServer{baseURL: api.URL, readToken: "snapshot-read-token", writeToken: "snapshot-write-token", client: api.Client()}
	result, err := s.call(context.Background(), mcpCallParams{Name: "snapshot_request", Arguments: map[string]interface{}{"project_id": float64(project.ID), "idempotency_key": "snapshot-real-api-123"}})
	if err != nil {
		t.Fatal(err)
	}
	buildID := int64(result.StructuredContent.(map[string]interface{})["build_id"].(float64))
	var job *Job
	deadline := time.Now().Add(5 * time.Second)
	for {
		job, err = getJobByBuildID(buildID)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			build, _ := getBuild(buildID)
			t.Fatalf("snapshot job not created; build=%+v query_error=%v", build, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	executeSnapshotJob(api.URL, runner.Token, job)
	build, err := getBuild(buildID)
	if err != nil {
		t.Fatal(err)
	}
	if build.Status != "success" {
		t.Fatalf("snapshot status=%s error=%s", build.Status, build.ErrorMessage)
	}
	job, err = getJobByBuildID(buildID)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(job.ArtifactPath)
	if err != nil || info.Size() == 0 {
		t.Fatalf("snapshot artifact=%q info=%v err=%v", job.ArtifactPath, info, err)
	}
}

func TestMCPBuildLogIsBoundedAndRedacted(t *testing.T) {
	raw := []byte(`{"id":5,"log":"prefix Authorization: Bearer secret-token suffix"}`)
	bounded := boundBuildLog(redactJSON(raw), 48)
	if strings.Contains(string(bounded), "secret-token") || !strings.Contains(string(bounded), "[REDACTED]") {
		t.Fatalf("unsafe output: %s", bounded)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(bounded, &got); err != nil {
		t.Fatal(err)
	}
	if len([]byte(got["log"].(string))) > 48 {
		t.Fatalf("log exceeded bound: %q", got["log"])
	}
}

func TestMCPIdempotentBuildReturnsExistingBuild(t *testing.T) {
	withTempDB(t)
	project := &Project{Name: "mcp-idempotency", RepoPath: t.TempDir(), DeployDir: t.TempDir(), DeployMode: "files"}
	if err := createProject(project); err != nil {
		t.Fatal(err)
	}
	calls := 0
	start := func(trigger string) (int64, error) {
		calls++
		build, err := createBuild(project.ID, trigger)
		if err != nil {
			return 0, err
		}
		return build.ID, nil
	}
	first, err := mcpIdempotentBuild("deploy", project, "stable-key-123", start)
	if err != nil {
		t.Fatal(err)
	}
	second, err := mcpIdempotentBuild("deploy", project, "stable-key-123", start)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || calls != 1 {
		t.Fatalf("first=%d second=%d calls=%d", first, second, calls)
	}
	var stored string
	if err := db.QueryRow(`SELECT idempotency_key FROM mcp_idempotency WHERE project_id=?`, project.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored == "stable-key-123" || len(stored) != 64 {
		t.Fatalf("idempotency key was not hashed: %q", stored)
	}
}

func TestMCPIdempotentBuildRecoversExpiredReservation(t *testing.T) {
	withTempDB(t)
	project := &Project{Name: "mcp-recovery", RepoPath: t.TempDir(), DeployDir: t.TempDir(), DeployMode: "files"}
	if err := createProject(project); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(mcpIdempotencySchema); err != nil {
		t.Fatal(err)
	}
	keyBytes := sha256.Sum256([]byte("recovery-key-123"))
	key := fmt.Sprintf("%x", keyBytes[:])
	if _, err := db.Exec(`INSERT INTO mcp_idempotency(action,project_id,idempotency_key,build_id,created_unix) VALUES(?,?,?,0,?)`, "deploy", project.ID, key, time.Now().Add(-time.Minute).Unix()); err != nil {
		t.Fatal(err)
	}
	calls := 0
	got, err := mcpIdempotentBuild("deploy", project, "recovery-key-123", func(trigger string) (int64, error) {
		calls++
		b, e := createBuild(project.ID, trigger)
		if e != nil {
			return 0, e
		}
		return b.ID, nil
	})
	if err != nil || got < 1 || calls != 1 {
		t.Fatalf("got=%d calls=%d err=%v", got, calls, err)
	}
}

func TestMCPIdempotentBuildRecoversBuildAfterRecordingCrash(t *testing.T) {
	withTempDB(t)
	project := &Project{Name: "mcp-post-build-crash", RepoPath: t.TempDir(), DeployDir: t.TempDir(), DeployMode: "files"}
	if err := createProject(project); err != nil {
		t.Fatal(err)
	}
	rawKey := "post-build-crash-key"
	keyBytes := sha256.Sum256([]byte(rawKey))
	key := fmt.Sprintf("%x", keyBytes[:])
	markerBytes := sha256.Sum256([]byte("deploy\x00" + strconv.FormatInt(project.ID, 10) + "\x00" + key))
	marker := "mcp:deploy:" + fmt.Sprintf("%x", markerBytes[:12])
	build, err := createBuild(project.ID, marker)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(mcpIdempotencySchema); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO mcp_idempotency(action,project_id,idempotency_key,build_id,created_unix) VALUES(?,?,?,0,?)`, "deploy", project.ID, key, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	calls := 0
	got, err := mcpIdempotentBuild("deploy", project, rawKey, func(string) (int64, error) { calls++; return 0, nil })
	if err != nil {
		t.Fatal(err)
	}
	if got != build.ID || calls != 0 {
		t.Fatalf("got=%d want=%d calls=%d", got, build.ID, calls)
	}
}

func TestMCPPostgresAdvisoryLockIndependentHolders(t *testing.T) {
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
	unlockFirst, err := lockMCPIdempotency("deploy", 77, "same-key")
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan func(), 1)
	failed := make(chan error, 1)
	go func() {
		unlock, err := lockMCPIdempotency("deploy", 77, "same-key")
		if err != nil {
			failed <- err
			return
		}
		acquired <- unlock
	}()
	select {
	case <-acquired:
		t.Fatal("second independent PostgreSQL holder acquired key concurrently")
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(100 * time.Millisecond):
	}
	unlockFirst()
	select {
	case unlock := <-acquired:
		unlock()
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("second holder did not acquire after release")
	}
}

func TestMCPIdempotentBuildConcurrentRetryCreatesOneBuild(t *testing.T) {
	withTempDB(t)
	project := &Project{Name: "mcp-concurrent", RepoPath: t.TempDir(), DeployDir: t.TempDir(), DeployMode: "files"}
	if err := createProject(project); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	calls := 0
	ids := make(chan int64, 12)
	errs := make(chan error, 12)
	start := func(trigger string) (int64, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		b, e := createBuild(project.ID, trigger)
		if e != nil {
			return 0, e
		}
		return b.ID, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, e := mcpIdempotentBuild("deploy", project, "concurrent-key-123", start)
			if e != nil {
				errs <- e
				return
			}
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	var want int64
	for id := range ids {
		if want == 0 {
			want = id
		} else if id != want {
			t.Errorf("duplicate build IDs %d and %d", want, id)
		}
	}
	for err := range errs {
		if !strings.Contains(err.Error(), "still starting") {
			t.Errorf("unexpected retry error: %v", err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("start called %d times", calls)
	}
}
