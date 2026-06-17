package app

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestShellQuote(t *testing.T) {
	cases := map[string]string{
		"":                 "''",
		"simple":           "'simple'",
		"/srv/apps/site":   "'/srv/apps/site'",
		"it's complicated": "'it'\"'\"'s complicated'",
	}
	for input, want := range cases {
		if got := shellQuote(input); got != want {
			t.Fatalf("shellQuote(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestBuildDeployScriptQuotesDynamicValues(t *testing.T) {
	project := &Project{
		ImageName:       "registry.example.com/app",
		DeployDir:       "/srv/apps/site with spaces",
		ComposeServices: "web worker",
	}

	script := buildDeployScript(project, "abc123")
	for _, want := range []string{
		"cd '/srv/apps/site with spaces'",
		"docker load -i '/tmp/registry.example.com-app-abc123.tar'",
		"docker compose down 'web' 'worker'",
		"docker compose up -d 'web' 'worker'",
		"rm -f '/tmp/registry.example.com-app-abc123.tar'",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected deploy script to contain %q, got:\n%s", want, script)
		}
	}
}

func TestBuilderCancelAndShutdownCancelTrackedBuilds(t *testing.T) {
	builder := NewBuilder(NewSSEBroker())

	ctx, cancel := context.WithCancel(context.Background())
	builder.cancels[42] = cancel

	if err := builder.Cancel(42); err != nil {
		t.Fatalf("cancel tracked build: %v", err)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("expected cancel to cancel build context")
	}
	if err := builder.Cancel(99); err == nil {
		t.Fatal("expected missing build cancel to fail")
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	builder.cancels[43] = cancel2
	builder.Shutdown(context.Background())
	select {
	case <-ctx2.Done():
	default:
		t.Fatal("expected shutdown to cancel remaining build context")
	}
}

func TestBuilderRunStepPersistsRedactedOutput(t *testing.T) {
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

	fakeCommands := &fakeCommandExecutor{
		startOutput: "token=secret123\nplain line\n",
	}
	builder := NewBuilder(NewSSEBroker())
	builder.commands = fakeCommands
	var logBuf strings.Builder
	if err := builder.runStep(context.Background(), &logBuf, build.ID, "/srv/repo", "deploy", "--token", "secret123"); err != nil {
		t.Fatalf("run step: %v", err)
	}
	if fakeCommands.waitCount != 1 {
		t.Fatalf("expected command wait once, got %d", fakeCommands.waitCount)
	}
	if len(fakeCommands.startCalls) != 1 || fakeCommands.startCalls[0].dir != "/srv/repo" || fakeCommands.startCalls[0].name != "deploy" {
		t.Fatalf("unexpected command start calls: %#v", fakeCommands.startCalls)
	}
	if !strings.Contains(logBuf.String(), "token=[REDACTED]") || strings.Contains(logBuf.String(), "secret123") {
		t.Fatalf("expected redacted log buffer, got %q", logBuf.String())
	}
	got, err := getBuild(build.ID)
	if err != nil {
		t.Fatalf("get build: %v", err)
	}
	if !strings.Contains(got.Log, "plain line") || strings.Contains(got.Log, "secret123") {
		t.Fatalf("expected persisted redacted log, got %q", got.Log)
	}
}

func TestBuilderRunCaptureIncludesCommandOutputOnError(t *testing.T) {
	builder := NewBuilder(NewSSEBroker())
	fakeCommands := &fakeCommandExecutor{
		combinedOutput: []byte("failure\n"),
		combinedErr:    errors.New("exit status 7"),
	}
	builder.commands = fakeCommands
	out, err := builder.runCapture(context.Background(), "/srv/repo", "git", "rev-parse", "--short", "HEAD")
	if err == nil {
		t.Fatal("expected command error")
	}
	if !strings.Contains(out, "failure") || !strings.Contains(err.Error(), "failure") {
		t.Fatalf("expected output in return and error, out=%q err=%v", out, err)
	}
	if len(fakeCommands.combinedCalls) != 1 || fakeCommands.combinedCalls[0].dir != "/srv/repo" || fakeCommands.combinedCalls[0].name != "git" {
		t.Fatalf("unexpected combined output calls: %#v", fakeCommands.combinedCalls)
	}
}

func TestBuilderRunStepSilentAndHealthCheckNoop(t *testing.T) {
	fakeCommands := &fakeCommandExecutor{}
	builder := NewBuilder(NewSSEBroker())
	builder.commands = fakeCommands
	if err := builder.runStepSilent(context.Background(), "/srv/repo", "cleanup", "--all"); err != nil {
		t.Fatalf("run silent step: %v", err)
	}
	if len(fakeCommands.runCalls) != 1 || fakeCommands.runCalls[0].dir != "/srv/repo" || fakeCommands.runCalls[0].name != "cleanup" {
		t.Fatalf("unexpected run calls: %#v", fakeCommands.runCalls)
	}
	if err := builder.healthCheck(context.Background(), &Project{}, true); err != nil {
		t.Fatalf("expected empty health check to succeed: %v", err)
	}
	if len(fakeCommands.runCalls) != 1 || len(fakeCommands.outputCalls) != 0 {
		t.Fatalf("expected empty health check to skip commands, run=%#v output=%#v", fakeCommands.runCalls, fakeCommands.outputCalls)
	}
}

func TestBuilderHealthCheckUsesCommandExecutor(t *testing.T) {
	oldConfig := appConfig
	appConfig.HealthCheckTimeout = time.Second
	t.Cleanup(func() {
		appConfig = oldConfig
	})

	fakeCommands := &fakeCommandExecutor{output: []byte("healthy\n")}
	builder := NewBuilder(NewSSEBroker())
	builder.commands = fakeCommands

	project := &Project{HealthContainer: "web"}
	if err := builder.healthCheck(context.Background(), project, true); err != nil {
		t.Fatalf("health check: %v", err)
	}
	if len(fakeCommands.outputCalls) != 1 || fakeCommands.outputCalls[0].name != "docker" {
		t.Fatalf("unexpected health check command calls: %#v", fakeCommands.outputCalls)
	}
}

func TestBuildDockerArgsJSON(t *testing.T) {
	if got := buildDockerArgsJSON(nil); got != "{}" {
		t.Fatalf("empty build args = %q, want {}", got)
	}
	got := buildDockerArgsJSON(map[string]string{"APP_ENV": "production"})
	if got != `{"APP_ENV":"production"}` {
		t.Fatalf("unexpected build args JSON %q", got)
	}
}

func TestLoadIgnorePatternsPrefersDeployIgnore(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".dockerignore"), []byte("docker-only\n"), 0644); err != nil {
		t.Fatalf("write dockerignore: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, ".deployignore"), []byte("# comment\n\nbuild\n*.tmp\n"), 0644); err != nil {
		t.Fatalf("write deployignore: %v", err)
	}

	patterns := loadIgnorePatterns(root)
	joined := strings.Join(patterns, ",")
	for _, want := range []string{".git", ".deployignore", ".dockerignore", "build", "*.tmp"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected pattern %q in %#v", want, patterns)
		}
	}
	if strings.Contains(joined, "docker-only") {
		t.Fatalf("expected .deployignore to take precedence, got %#v", patterns)
	}
}

func TestLoadIgnorePatternsFallsBackToDockerIgnore(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".dockerignore"), []byte("node_modules\n"), 0644); err != nil {
		t.Fatalf("write dockerignore: %v", err)
	}
	patterns := loadIgnorePatterns(root)
	if !strings.Contains(strings.Join(patterns, ","), "node_modules") {
		t.Fatalf("expected dockerignore pattern, got %#v", patterns)
	}
}

func TestPackageFilesRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.txt")
	if err := os.WriteFile(target, []byte("target"), 0644); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Symlink("target.txt", filepath.Join(root, "link.txt")); err != nil {
		t.Fatalf("create symlink: %v", err)
	}

	err := packageFiles(root, filepath.Join(t.TempDir(), "archive.tar.gz"), nil)
	if err == nil {
		t.Fatal("expected packageFiles to reject symlink")
	}
	if !strings.Contains(err.Error(), "refusing to package symlink") {
		t.Fatalf("expected symlink error, got %v", err)
	}
}

func TestPackageFilesHonorsIgnorePatterns(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "keep.txt"), []byte("keep"), 0644); err != nil {
		t.Fatalf("write keep file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "skip.log"), []byte("skip"), 0644); err != nil {
		t.Fatalf("write skip file: %v", err)
	}

	archivePath := filepath.Join(t.TempDir(), "archive.tar.gz")
	if err := packageFiles(root, archivePath, []string{"*.log"}); err != nil {
		t.Fatalf("package files: %v", err)
	}

	entries, err := readTarGzEntries(archivePath)
	if err != nil {
		t.Fatalf("read archive entries: %v", err)
	}
	if !entries["keep.txt"] {
		t.Fatal("expected keep.txt in archive")
	}
	if entries["skip.log"] {
		t.Fatal("expected skip.log to be ignored")
	}
}

func readTarGzEntries(path string) (map[string]bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gzReader, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gzReader.Close()

	entries := make(map[string]bool)
	tarReader := tar.NewReader(gzReader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
		entries[header.Name] = true
	}
}

type commandCall struct {
	dir  string
	name string
	args []string
}

type fakeCommandExecutor struct {
	startOutput    string
	startErr       error
	waitErr        error
	waitCount      int
	runErr         error
	combinedOutput []byte
	combinedErr    error
	output         []byte
	outputErr      error

	startCalls    []commandCall
	runCalls      []commandCall
	combinedCalls []commandCall
	outputCalls   []commandCall
}

func (f *fakeCommandExecutor) Start(ctx context.Context, dir string, name string, args ...string) (runningCommand, io.ReadCloser, error) {
	f.startCalls = append(f.startCalls, newCommandCall(dir, name, args))
	if f.startErr != nil {
		return nil, nil, f.startErr
	}
	return &fakeRunningCommand{err: f.waitErr, waitCount: &f.waitCount}, io.NopCloser(strings.NewReader(f.startOutput)), nil
}

func (f *fakeCommandExecutor) Run(ctx context.Context, dir string, name string, args ...string) error {
	f.runCalls = append(f.runCalls, newCommandCall(dir, name, args))
	return f.runErr
}

func (f *fakeCommandExecutor) CombinedOutput(ctx context.Context, dir string, name string, args ...string) ([]byte, error) {
	f.combinedCalls = append(f.combinedCalls, newCommandCall(dir, name, args))
	return f.combinedOutput, f.combinedErr
}

func (f *fakeCommandExecutor) Output(ctx context.Context, dir string, name string, args ...string) ([]byte, error) {
	f.outputCalls = append(f.outputCalls, newCommandCall(dir, name, args))
	return f.output, f.outputErr
}

type fakeRunningCommand struct {
	err       error
	waitCount *int
}

func (c *fakeRunningCommand) Wait() error {
	if c.waitCount != nil {
		*c.waitCount = *c.waitCount + 1
	}
	return c.err
}

func newCommandCall(dir string, name string, args []string) commandCall {
	copiedArgs := append([]string(nil), args...)
	return commandCall{dir: dir, name: name, args: copiedArgs}
}
