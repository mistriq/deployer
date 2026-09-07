package app

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSafeArchiveTargetRejectsTraversal(t *testing.T) {
	base := t.TempDir()
	if _, err := safeArchiveTarget(base, "../secret.txt"); err == nil {
		t.Fatal("expected traversal path to be rejected")
	}
}

func TestSafeArchiveTargetRejectsAbsolutePath(t *testing.T) {
	base := t.TempDir()
	if _, err := safeArchiveTarget(base, "/etc/passwd"); err == nil {
		t.Fatal("expected absolute path to be rejected")
	}
}

func TestExtractTarGzRejectsSymlink(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "archive.tar.gz")
	if err := writeTestTarGz(archivePath, func(tw *tar.Writer) error {
		return tw.WriteHeader(&tar.Header{
			Name:     "link",
			Typeflag: tar.TypeSymlink,
			Linkname: "target",
			Mode:     0777,
		})
	}); err != nil {
		t.Fatalf("write test archive: %v", err)
	}

	if err := extractTarGz(archivePath, t.TempDir()); err == nil {
		t.Fatal("expected symlink archive entry to be rejected")
	}
}

func TestExtractTarGzWritesRegularFile(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "archive.tar.gz")
	if err := writeTestTarGz(archivePath, func(tw *tar.Writer) error {
		content := []byte("hello")
		if err := tw.WriteHeader(&tar.Header{
			Name: "dir/file.txt",
			Mode: 0644,
			Size: int64(len(content)),
		}); err != nil {
			return err
		}
		_, err := tw.Write(content)
		return err
	}); err != nil {
		t.Fatalf("write test archive: %v", err)
	}

	dest := t.TempDir()
	if err := extractTarGz(archivePath, dest); err != nil {
		t.Fatalf("extract archive: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "dir", "file.txt"))
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("unexpected file content %q", got)
	}
}

func TestAuthenticateAgentRequiresBearerToken(t *testing.T) {
	withTempDB(t)

	runner := &Runner{Name: "test-runner"}
	if err := createRunner(runner); err != nil {
		t.Fatalf("create runner: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/agent/poll", nil)
	req.Header.Set("Authorization", "Basic "+runner.Token)
	if _, err := authenticateAgent(req); err == nil {
		t.Fatal("expected non-bearer token to be rejected")
	}

	req = httptest.NewRequest(http.MethodGet, "/api/agent/poll", nil)
	req.Header.Set("Authorization", "Bearer "+runner.Token)
	authenticated, err := authenticateAgent(req)
	if err != nil {
		t.Fatalf("expected bearer token to authenticate: %v", err)
	}
	if authenticated.ID != runner.ID {
		t.Fatalf("expected runner %d, got %d", runner.ID, authenticated.ID)
	}
}

func TestValidateServerURL(t *testing.T) {
	cases := []struct {
		name    string
		rawURL  string
		wantErr bool
	}{
		{name: "https remote", rawURL: "https://deployer.example.com"},
		{name: "http localhost", rawURL: "http://localhost:9090"},
		{name: "http loopback", rawURL: "http://127.0.0.1:9090"},
		{name: "http remote warns but validates", rawURL: "http://192.0.2.10:9090"},
		{name: "missing host", rawURL: "https://", wantErr: true},
		{name: "unsupported scheme", rawURL: "ftp://localhost", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateServerURL(tc.rawURL)
			if tc.wantErr && err == nil {
				t.Fatal("expected validation error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}

func TestNewAgentRequestUsesBearerToken(t *testing.T) {
	req, err := newAgentRequest(http.MethodPost, "https://deployer.example.com/api/agent/heartbeat", "runner-token", nil)
	if err != nil {
		t.Fatalf("new agent request: %v", err)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer runner-token" {
		t.Fatalf("unexpected authorization header %q", got)
	}
}

func TestAgentHeartbeatHandlesSuccessAndFailure(t *testing.T) {
	oldClient := agentControlClient
	t.Cleanup(func() { agentControlClient = oldClient })

	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/agent/heartbeat" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("unexpected authorization header %q", got)
		}
		if requests == 1 {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "denied", http.StatusUnauthorized)
	}))
	defer server.Close()
	agentControlClient = server.Client()

	if err := agentHeartbeat(server.URL, "token"); err != nil {
		t.Fatalf("expected heartbeat to succeed: %v", err)
	}
	if err := agentHeartbeat(server.URL, "token"); err == nil || !strings.Contains(err.Error(), "status 401") {
		t.Fatalf("expected status error, got %v", err)
	}
}

func TestAgentPollHandlesNoContentAndJob(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/agent/poll" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("unexpected authorization header %q", got)
		}
		if requests == 1 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		json.NewEncoder(w).Encode(Job{ID: 7, BuildID: 11, Mode: "files"})
	}))
	defer server.Close()

	job, err := agentPoll(server.URL, "token")
	if err != nil {
		t.Fatalf("poll no content: %v", err)
	}
	if job != nil {
		t.Fatalf("expected no job, got %+v", job)
	}

	job, err = agentPoll(server.URL, "token")
	if err != nil {
		t.Fatalf("poll job: %v", err)
	}
	if job == nil || job.ID != 7 || job.BuildID != 11 || job.Mode != "files" {
		t.Fatalf("unexpected job: %+v", job)
	}
}

func TestAgentRetryDelay(t *testing.T) {
	if got := agentRetryDelay(0); got != 0 {
		t.Fatalf("expected first retry delay to be zero, got %s", got)
	}
	if got := agentRetryDelay(2); got != 2*time.Second {
		t.Fatalf("expected retry delay to scale by seconds, got %s", got)
	}
}

func TestAgentDownloadArtifactWritesResponse(t *testing.T) {
	oldClient := agentArtifactClient
	t.Cleanup(func() { agentArtifactClient = oldClient })

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/agent/artifact/42" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("unexpected authorization header %q", got)
		}
		w.Header().Set("Content-Length", "7")
		_, _ = w.Write([]byte("archive"))
	}))
	defer server.Close()
	agentArtifactClient = server.Client()

	dest := filepath.Join(t.TempDir(), "artifact.tar")
	if err := agentDownloadArtifact(server.URL, "token", 42, dest); err != nil {
		t.Fatalf("download artifact: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read downloaded artifact: %v", err)
	}
	if string(got) != "archive" {
		t.Fatalf("unexpected artifact content %q", got)
	}
}

func TestSaveAgentArtifactResponseRejectsErrorsAndLargeContent(t *testing.T) {
	dir := t.TempDir()
	statusDest := filepath.Join(dir, "status.tar")
	resp := &http.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       io.NopCloser(strings.NewReader("artifact unavailable")),
	}
	if err := saveAgentArtifactResponse(resp, statusDest); err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("expected status error, got %v", err)
	}
	if _, err := os.Stat(statusDest); !os.IsNotExist(err) {
		t.Fatalf("expected no file for error response, got %v", err)
	}

	largeDest := filepath.Join(dir, "large.tar")
	resp = &http.Response{
		StatusCode:    http.StatusOK,
		ContentLength: maxAgentArtifactDownloadBytes + 1,
		Body:          io.NopCloser(strings.NewReader("too large")),
	}
	if err := saveAgentArtifactResponse(resp, largeDest); err == nil || !strings.Contains(err.Error(), "download limit") {
		t.Fatalf("expected size limit error, got %v", err)
	}
	if _, err := os.Stat(largeDest); !os.IsNotExist(err) {
		t.Fatalf("expected no file for oversized response, got %v", err)
	}
}

func TestAgentUploadSnapshotPostsArchive(t *testing.T) {
	oldClient := agentArtifactClient
	t.Cleanup(func() { agentArtifactClient = oldClient })

	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/agent/snapshot/99" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("unexpected authorization header %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/gzip" {
			t.Fatalf("unexpected content type %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read upload body: %v", err)
		}
		gotBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	agentArtifactClient = server.Client()

	archivePath := filepath.Join(t.TempDir(), "snapshot.tar.gz")
	if err := os.WriteFile(archivePath, []byte("snapshot"), 0644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	if err := agentUploadSnapshot(server.URL, "token", 99, archivePath); err != nil {
		t.Fatalf("upload snapshot: %v", err)
	}
	if gotBody != "snapshot" {
		t.Fatalf("unexpected uploaded body %q", gotBody)
	}
}

func TestAgentSendLogAndComplete(t *testing.T) {
	oldClient := agentControlClient
	t.Cleanup(func() { agentControlClient = oldClient })

	var logBody string
	var completeStatus string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("unexpected authorization header %q", got)
		}
		switch r.URL.Path {
		case "/api/agent/log/5":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read log body: %v", err)
			}
			logBody = string(body)
			w.WriteHeader(http.StatusOK)
		case "/api/agent/complete/5":
			var payload map[string]string
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode completion payload: %v", err)
			}
			completeStatus = payload["status"]
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	agentControlClient = server.Client()

	if err := agentSendLog(server.URL, "token", 5, "hello\n"); err != nil {
		t.Fatalf("send log: %v", err)
	}
	agentComplete(server.URL, "token", 5, "success", "")

	if logBody != "hello\n" {
		t.Fatalf("unexpected log body %q", logBody)
	}
	if completeStatus != "success" {
		t.Fatalf("unexpected completion status %q", completeStatus)
	}
}

func TestAgentRunCommandReturnsLogSendFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "log unavailable", http.StatusInternalServerError)
	}))
	defer server.Close()

	err := agentRunCommand(server.URL, "token", 1, "", "sh", "-c", "printf hello")
	if err == nil {
		t.Fatal("expected log send failure")
	}
	if !strings.Contains(err.Error(), "send build log") {
		t.Fatalf("expected send build log error, got %v", err)
	}
}

func TestParsePreservePathsSkipsBlankAndRejectsUnsafe(t *testing.T) {
	paths, err := parsePreservePaths("storage\n\n .env \n")
	if err != nil {
		t.Fatalf("parse preserve paths: %v", err)
	}
	if len(paths) != 2 || paths[0] != "storage" || paths[1] != ".env" {
		t.Fatalf("unexpected preserve paths: %#v", paths)
	}

	if _, err := parsePreservePaths("storage\n../secret"); err == nil {
		t.Fatal("expected unsafe preserve path to be rejected")
	}
}

func TestBackupAndRestorePreservePaths(t *testing.T) {
	deployDir := t.TempDir()
	preserveDir := t.TempDir()
	var logs []string
	logf := func(format string, args ...interface{}) {
		logs = append(logs, fmt.Sprintf(format, args...))
	}

	if err := os.WriteFile(filepath.Join(deployDir, ".env"), []byte("old-secret"), 0600); err != nil {
		t.Fatalf("write old env: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(deployDir, "storage", "cache"), 0750); err != nil {
		t.Fatalf("create old storage: %v", err)
	}
	if err := os.WriteFile(filepath.Join(deployDir, "storage", "cache", "data.txt"), []byte("old-data"), 0640); err != nil {
		t.Fatalf("write old data: %v", err)
	}

	paths := []string{".env", "storage", "missing.json"}
	if preserved, err := backupPreservePaths(deployDir, preserveDir, paths, logf); err != nil || preserved != 2 {
		t.Fatalf("expected two preserved paths, got %d; logs=%v", preserved, logs)
	}

	if err := os.WriteFile(filepath.Join(deployDir, ".env"), []byte("archive-secret"), 0644); err != nil {
		t.Fatalf("write archive env: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(deployDir, "storage")); err != nil {
		t.Fatalf("remove old storage: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(deployDir, "storage", "cache"), 0755); err != nil {
		t.Fatalf("create archive storage: %v", err)
	}
	if err := os.WriteFile(filepath.Join(deployDir, "storage", "cache", "data.txt"), []byte("archive-data"), 0644); err != nil {
		t.Fatalf("write archive data: %v", err)
	}

	if restored, err := restorePreservePaths(deployDir, preserveDir, paths, logf); err != nil || restored != 2 {
		t.Fatalf("expected two restored paths, got %d; logs=%v", restored, logs)
	}

	env, err := os.ReadFile(filepath.Join(deployDir, ".env"))
	if err != nil {
		t.Fatalf("read restored env: %v", err)
	}
	if string(env) != "old-secret" {
		t.Fatalf("expected preserved env content, got %q", env)
	}
	data, err := os.ReadFile(filepath.Join(deployDir, "storage", "cache", "data.txt"))
	if err != nil {
		t.Fatalf("read restored data: %v", err)
	}
	if string(data) != "old-data" {
		t.Fatalf("expected preserved storage content, got %q", data)
	}
	if _, err := os.Stat(filepath.Join(deployDir, "missing.json")); !os.IsNotExist(err) {
		t.Fatalf("expected missing preserve path to remain absent, got %v", err)
	}
}

func TestExecuteFilesJobPreservesRecoveryBackupAfterExtractionFailure(t *testing.T) {
	oldControlClient, oldArtifactClient := agentControlClient, agentArtifactClient
	t.Cleanup(func() {
		agentControlClient = oldControlClient
		agentArtifactClient = oldArtifactClient
	})

	deployDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(deployDir, ".env"), []byte("keep-me"), 0600); err != nil {
		t.Fatalf("write preserved file: %v", err)
	}

	var logs []string
	var completion struct {
		Status string
		Error  string
	}
	archivePath := filepath.Join(t.TempDir(), "partially-valid.tar.gz")
	if err := writeTestTarGz(archivePath, func(tw *tar.Writer) error {
		content := []byte("archive-secret")
		if err := tw.WriteHeader(&tar.Header{Name: ".env", Mode: 0644, Size: int64(len(content))}); err != nil {
			return err
		}
		if _, err := tw.Write(content); err != nil {
			return err
		}
		return tw.WriteHeader(&tar.Header{Name: "unsafe-link", Typeflag: tar.TypeSymlink, Linkname: "target", Mode: 0777})
	}); err != nil {
		t.Fatalf("write partially valid archive: %v", err)
	}
	archive, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("read partially valid archive: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/agent/artifact/91":
			_, _ = w.Write(archive)
		case "/api/agent/log/91":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read log: %v", err)
			}
			logs = append(logs, string(body))
		case "/api/agent/complete/91":
			if err := json.NewDecoder(r.Body).Decode(&completion); err != nil {
				t.Fatalf("decode completion: %v", err)
			}
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	agentControlClient = server.Client()
	agentArtifactClient = server.Client()

	executeFilesJob(server.URL, "token", &Job{
		BuildID:   91,
		DeployDir: deployDir,
		Mode:      "files",
		Preserve:  ".env\nmissing.json",
	})

	if completion.Status != "failed" || !strings.Contains(completion.Error, "extract files") {
		t.Fatalf("unexpected completion: %+v", completion)
	}
	joinedLogs := strings.Join(logs, "")
	const marker = "Recovery backup retained at "
	idx := strings.Index(joinedLogs, marker)
	if idx < 0 {
		t.Fatalf("expected retained recovery backup log, got %q", joinedLogs)
	}
	backupDir := strings.TrimSpace(joinedLogs[idx+len(marker):])
	backupDir = strings.Split(backupDir, "\n")[0]
	t.Cleanup(func() { _ = os.RemoveAll(backupDir) })
	got, err := os.ReadFile(filepath.Join(backupDir, ".env"))
	if err != nil {
		t.Fatalf("read retained recovery backup: %v", err)
	}
	if string(got) != "keep-me" {
		t.Fatalf("unexpected retained content %q", got)
	}
	if got, err := os.ReadFile(filepath.Join(deployDir, ".env")); err != nil || string(got) != "archive-secret" {
		t.Fatalf("expected partial extraction to replace live file, got %q err=%v", got, err)
	}
}

func TestExecuteFilesJobAbortsWhenPreserveBackupFails(t *testing.T) {
	oldControlClient, oldArtifactClient := agentControlClient, agentArtifactClient
	t.Cleanup(func() {
		agentControlClient = oldControlClient
		agentArtifactClient = oldArtifactClient
	})

	deployDir := t.TempDir()
	socketPath := filepath.Join(deployDir, "state.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("create preserved socket: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	archivePath := filepath.Join(t.TempDir(), "files.tar.gz")
	if err := writeTestTarGz(archivePath, func(tw *tar.Writer) error {
		content := []byte("must-not-extract")
		if err := tw.WriteHeader(&tar.Header{Name: "new-file.txt", Mode: 0644, Size: int64(len(content))}); err != nil {
			return err
		}
		_, err := tw.Write(content)
		return err
	}); err != nil {
		t.Fatalf("write archive: %v", err)
	}
	archive, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}

	var completion struct {
		Status string
		Error  string
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/agent/artifact/93":
			_, _ = w.Write(archive)
		case "/api/agent/log/93":
			w.WriteHeader(http.StatusOK)
		case "/api/agent/complete/93":
			if err := json.NewDecoder(r.Body).Decode(&completion); err != nil {
				t.Fatalf("decode completion: %v", err)
			}
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	agentControlClient = server.Client()
	agentArtifactClient = server.Client()

	executeFilesJob(server.URL, "token", &Job{BuildID: 93, DeployDir: deployDir, Mode: "files", Preserve: "state.sock"})

	if completion.Status != "failed" || !strings.Contains(completion.Error, "preserve backup") {
		t.Fatalf("unexpected completion: %+v", completion)
	}
	if _, err := os.Stat(filepath.Join(deployDir, "new-file.txt")); !os.IsNotExist(err) {
		t.Fatalf("archive was extracted despite preserve backup failure: %v", err)
	}
}

func TestExecuteFilesJobReportsRestoreFailureAndRetainsRecoveryBackup(t *testing.T) {
	oldControlClient, oldArtifactClient := agentControlClient, agentArtifactClient
	oldRestore := agentRestorePreservePaths
	t.Cleanup(func() {
		agentControlClient = oldControlClient
		agentArtifactClient = oldArtifactClient
		agentRestorePreservePaths = oldRestore
	})

	archivePath := filepath.Join(t.TempDir(), "files.tar.gz")
	if err := writeTestTarGz(archivePath, func(tw *tar.Writer) error {
		content := []byte("new-release")
		if err := tw.WriteHeader(&tar.Header{Name: "index.html", Mode: 0644, Size: int64(len(content))}); err != nil {
			return err
		}
		_, err := tw.Write(content)
		return err
	}); err != nil {
		t.Fatalf("write archive: %v", err)
	}
	archive, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}

	deployDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(deployDir, ".env"), []byte("keep-me"), 0600); err != nil {
		t.Fatalf("write preserved file: %v", err)
	}
	agentRestorePreservePaths = func(_ string, _ string, _ []string, _ func(string, ...interface{})) (int, error) {
		return 0, fmt.Errorf("restore permission denied")
	}

	var logs []string
	var completion struct {
		Status string
		Error  string
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/agent/artifact/92":
			_, _ = w.Write(archive)
		case "/api/agent/log/92":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read log: %v", err)
			}
			logs = append(logs, string(body))
		case "/api/agent/complete/92":
			if err := json.NewDecoder(r.Body).Decode(&completion); err != nil {
				t.Fatalf("decode completion: %v", err)
			}
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	agentControlClient = server.Client()
	agentArtifactClient = server.Client()

	executeFilesJob(server.URL, "token", &Job{BuildID: 92, DeployDir: deployDir, Mode: "files", Preserve: ".env"})

	if completion.Status != "failed" || !strings.Contains(completion.Error, "preserve restore: restore permission denied") {
		t.Fatalf("unexpected completion: %+v", completion)
	}
	joinedLogs := strings.Join(logs, "")
	if !strings.Contains(joinedLogs, "Files extracted successfully") || !strings.Contains(joinedLogs, "Preserve restore failed: restore permission denied") {
		t.Fatalf("expected extraction and restore failure logs, got %q", joinedLogs)
	}
	const marker = "Recovery backup retained at "
	idx := strings.Index(joinedLogs, marker)
	if idx < 0 {
		t.Fatalf("expected retained recovery backup log, got %q", joinedLogs)
	}
	backupDir := strings.Split(strings.TrimSpace(joinedLogs[idx+len(marker):]), "\n")[0]
	t.Cleanup(func() { _ = os.RemoveAll(backupDir) })
	if got, err := os.ReadFile(filepath.Join(backupDir, ".env")); err != nil || string(got) != "keep-me" {
		t.Fatalf("expected retained preserved file, got %q err=%v", got, err)
	}
}

func TestRestorePreservePathsReportsFailure(t *testing.T) {
	preserveDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(preserveDir, ".env"), []byte("keep-me"), 0600); err != nil {
		t.Fatalf("write preserve source: %v", err)
	}
	deployFile := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(deployFile, []byte("file"), 0644); err != nil {
		t.Fatalf("write deploy file: %v", err)
	}
	var logs []string
	logf := func(format string, args ...interface{}) { logs = append(logs, fmt.Sprintf(format, args...)) }

	restored, err := restorePreservePaths(deployFile, preserveDir, []string{".env", "optional.json"}, logf)
	if err == nil || restored != 0 {
		t.Fatalf("expected restore failure and no restored paths, got restored=%d err=%v", restored, err)
	}
	if !strings.Contains(strings.Join(logs, "\n"), "restore remove failed") {
		t.Fatalf("expected restore failure log, got %v", logs)
	}
}

func TestInstallVerifiedUpdateStagesAndBacksUpExecutable(t *testing.T) {
	dir := t.TempDir()
	exePath := filepath.Join(dir, "deployer")
	tmpPath := filepath.Join(dir, "deployer.new")

	if err := os.WriteFile(exePath, []byte("old-binary"), 0755); err != nil {
		t.Fatalf("write current executable: %v", err)
	}
	if err := os.WriteFile(tmpPath, []byte("new-binary"), 0644); err != nil {
		t.Fatalf("write update executable: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "deployer.old"), []byte("stale-backup"), 0644); err != nil {
		t.Fatalf("write stale backup: %v", err)
	}

	if err := installVerifiedUpdate(tmpPath, exePath); err != nil {
		t.Fatalf("install verified update: %v", err)
	}

	got, err := os.ReadFile(exePath)
	if err != nil {
		t.Fatalf("read installed executable: %v", err)
	}
	if string(got) != "new-binary" {
		t.Fatalf("expected new executable content, got %q", got)
	}
	backup, err := os.ReadFile(filepath.Join(dir, "deployer.old"))
	if err != nil {
		t.Fatalf("read backup executable: %v", err)
	}
	if string(backup) != "old-binary" {
		t.Fatalf("expected old executable backup, got %q", backup)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("expected temp update to be moved away, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "deployer.pending")); !os.IsNotExist(err) {
		t.Fatalf("expected pending update to be cleaned up, got %v", err)
	}
	info, err := os.Stat(exePath)
	if err != nil {
		t.Fatalf("stat installed executable: %v", err)
	}
	if info.Mode().Perm() != 0755 {
		t.Fatalf("expected executable mode 0755, got %o", info.Mode().Perm())
	}
}

func writeTestTarGz(path string, writeEntries func(*tar.Writer) error) error {
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	tarWriter := tar.NewWriter(gzWriter)
	if err := writeEntries(tarWriter); err != nil {
		tarWriter.Close()
		gzWriter.Close()
		return err
	}
	if err := tarWriter.Close(); err != nil {
		gzWriter.Close()
		return err
	}
	if err := gzWriter.Close(); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0644)
}
