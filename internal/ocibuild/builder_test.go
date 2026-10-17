package ocibuild

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type runnerFunc func(context.Context, Command) ([]byte, error)

func (f runnerFunc) Run(ctx context.Context, c Command) ([]byte, error) { return f(ctx, c) }
func repoFixture(t *testing.T) (string, string) {
	t.Helper()
	d := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		c := exec.Command("git", args...)
		c.Dir = d
		c.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.org", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.org")
		out, e := c.CombinedOutput()
		if e != nil {
			t.Fatalf("git: %s %v", out, e)
		}
		return strings.TrimSpace(string(out))
	}
	git("init")
	os.WriteFile(filepath.Join(d, "Dockerfile"), []byte("FROM scratch\n"), 0600)
	git("add", ".")
	git("commit", "-m", "initial")
	sha := git("rev-parse", "HEAD")
	os.WriteFile(filepath.Join(d, "untracked-secret"), []byte("secret"), 0600)
	os.WriteFile(filepath.Join(d, "Dockerfile"), []byte("dirty"), 0600)
	return d, sha
}
func TestBuildPinsCommitAndPublishesDigest(t *testing.T) {
	repo, sha := repoFixture(t)
	tmp := t.TempDir()
	digest := "sha256:" + strings.Repeat("a", 64)
	calls := 0
	b := Builder{AllowLocalRepositories: true, TempDir: tmp, CLIPluginDirs: []string{"/trusted/plugins"}}
	b.Runner = runnerFunc(func(ctx context.Context, c Command) ([]byte, error) {
		for _, e := range c.Env {
			if strings.HasPrefix(e, "SENTINEL_SECRET=") {
				t.Fatal("ambient secret propagated")
			}
		}
		if c.Name != "docker" {
			return (SystemRunner{}).Run(ctx, c)
		}
		calls++
		args := strings.Join(c.Args, " ")
		if !strings.Contains(args, "--push") || !strings.Contains(args, ":"+sha) {
			t.Fatal(args)
		}
		source := c.Args[len(c.Args)-1]
		if _, e := os.Stat(filepath.Join(source, "untracked-secret")); !os.IsNotExist(e) {
			t.Fatal("untracked file leaked")
		}
		data, _ := os.ReadFile(filepath.Join(source, "Dockerfile"))
		if string(data) != "FROM scratch\n" {
			t.Fatalf("dirty working tree leaked: %s", data)
		}
		if _, e := os.Stat(filepath.Join(source, ".git")); !os.IsNotExist(e) {
			t.Fatal("git metadata leaked")
		}
		for _, e := range c.Env {
			if strings.HasPrefix(e, "DOCKER_CONFIG=") {
				p := strings.TrimPrefix(e, "DOCKER_CONFIG=")
				st, err := os.Stat(filepath.Join(p, "config.json"))
				if err != nil || st.Mode().Perm() != 0600 {
					t.Fatal("unsafe Docker credential file")
				}
				data, err := os.ReadFile(filepath.Join(p, "config.json"))
				if err != nil {
					t.Fatal(err)
				}
				var config map[string]json.RawMessage
				if err = json.Unmarshal(data, &config); err != nil {
					t.Fatal(err)
				}
				if len(config) != 2 || string(config["cliPluginsExtraDirs"]) != `["/trusted/plugins"]` || strings.Contains(string(data), "HOST_AUTH_SENTINEL") || config["credsStore"] != nil {
					t.Fatalf("unexpected config fields: %v", config)
				}
			}
		}
		for i, a := range c.Args {
			if a == "--metadata-file" {
				data, _ := json.Marshal(map[string]string{"containerimage.digest": digest})
				return nil, os.WriteFile(c.Args[i+1], data, 0600)
			}
		}
		t.Fatal("missing metadata")
		return nil, nil
	})
	hostConfig := t.TempDir()
	os.WriteFile(filepath.Join(hostConfig, "config.json"), []byte(`{"auths":{"host.registry":{"auth":"HOST_AUTH_SENTINEL"}},"credsStore":"desktop"}`), 0600)
	t.Setenv("DOCKER_CONFIG", hostConfig)
	t.Setenv("SENTINEL_SECRET", "sensitive")
	resolved, e := b.Resolve(context.Background(), repo, "HEAD")
	if e != nil || resolved != sha {
		t.Fatalf("resolve: %s %v", resolved, e)
	}
	var phases []string
	a, e := b.Build(context.Background(), Request{Repository: repo, Commit: resolved, ImageRepository: "registry.example/app", Spec: BuildSpec{Kind: "dockerfile"}, Credentials: RegistryCredentials{"registry.example", "user", "secret"}, OnPhase: func(s string) { phases = append(phases, s) }}, nil)
	if e != nil {
		t.Fatal(e)
	}
	if a.ImageRef != "registry.example/app@"+digest || calls != 1 {
		t.Fatalf("artifact: %+v", a)
	}
	entries, _ := os.ReadDir(tmp)
	if len(entries) != 0 {
		t.Fatal("build left temporary credentials behind")
	}
	if strings.Join(phases, ",") != "resolving,building,publishing" {
		t.Fatal(phases)
	}
}
func TestValidation(t *testing.T) {
	b := Builder{}
	for _, repo := range []string{"/tmp/repo", "http://example.org/repo", "https://user:password@example.org/repo", "https://example.org/repo?token=secret"} {
		if _, e := b.Resolve(context.Background(), repo, "main"); e == nil {
			t.Fatalf("accepted %s", repo)
		}
	}
	for _, s := range []BuildSpec{{Kind: "static", OutputDir: "../../secret"}, {Kind: "node-http", BuildCommand: "true\nCOPY /secret /"}, {Kind: "node-http", Port: 65536}} {
		if _, e := Dockerfile(s); e == nil {
			t.Fatalf("accepted %+v", s)
		}
	}
}
func TestGeneratedRecipes(t *testing.T) {
	for _, kind := range []string{"static", "node-http"} {
		s, e := Dockerfile(BuildSpec{Kind: kind})
		if e != nil {
			t.Fatal(e)
		}
		for _, want := range []string{"RUN npm ci", "RUN npm run build", "COPY --from=build"} {
			if !strings.Contains(s, want) {
				t.Fatal(s)
			}
		}
		if kind == "node-http" && !strings.Contains(s, "USER 10001:10001") {
			t.Fatal(s)
		}
	}
}
func TestMissingDigestFailsAndCleans(t *testing.T) {
	repo, sha := repoFixture(t)
	tmp := t.TempDir()
	b := Builder{AllowLocalRepositories: true, TempDir: tmp, Runner: runnerFunc(func(ctx context.Context, c Command) ([]byte, error) {
		if c.Name == "docker" {
			return []byte("pushed but no metadata"), nil
		}
		return (SystemRunner{}).Run(ctx, c)
	})}
	_, e := b.Build(context.Background(), Request{Repository: repo, Commit: sha, ImageRepository: "registry.example/app", Spec: BuildSpec{Kind: "dockerfile"}}, nil)
	if e == nil {
		t.Fatal("accepted unproven publication")
	}
	entries, _ := os.ReadDir(tmp)
	if len(entries) > 0 {
		t.Fatal("leaked temporary files")
	}
}

func TestStreamingAndCancellation(t *testing.T) {
	var lines []string
	out, err := (SystemRunner{}).Run(context.Background(), Command{Name: "sh", Args: []string{"-c", "printf 'first\\nsecond\\n'"}, Env: []string{"PATH=" + os.Getenv("PATH")}, Log: func(s string) { lines = append(lines, s) }})
	if err != nil || len(lines) != 2 || !strings.Contains(string(out), "second") {
		t.Fatalf("stream: %q %v %v", out, lines, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = (SystemRunner{}).Run(ctx, Command{Name: "git", Args: []string{"--version"}}); err == nil {
		t.Fatal("cancelled command succeeded")
	}
}

func TestBuildFailureStagesAndCode(t *testing.T) {
	repo, sha := repoFixture(t)
	for _, exporting := range []bool{false, true} {
		t.Run(fmt.Sprint(exporting), func(t *testing.T) {
			var phases []string
			b := Builder{AllowLocalRepositories: true, Runner: runnerFunc(func(ctx context.Context, c Command) ([]byte, error) {
				if c.Name != "docker" {
					return (SystemRunner{}).Run(ctx, c)
				}
				if len(phases) != 2 || phases[1] != "building" {
					t.Fatalf("build starts in wrong phase: %v", phases)
				}
				c.Log("#8 1.23 pushing layers")
				if len(phases) != 2 {
					t.Fatal("application output changed phase")
				}
				if exporting {
					c.Log("#12 exporting to image")
					c.Log("#12 pushing layers")
					if len(phases) != 3 || phases[2] != "publishing" {
						t.Fatalf("missing publishing phase: %v", phases)
					}
				}
				return nil, fmt.Errorf("exit status 1")
			})}
			_, err := b.Build(context.Background(), Request{Repository: repo, Commit: sha, ImageRepository: "registry.example/app", Spec: BuildSpec{Kind: "dockerfile"}, OnPhase: func(p string) { phases = append(phases, p) }}, nil)
			var pipeline *PipelineError
			if !errors.As(err, &pipeline) || pipeline.Code != "BUILD_PUBLISH_FAILED" {
				t.Fatalf("incorrect error: %v", err)
			}
			if !exporting && len(phases) != 2 {
				t.Fatalf("compilation failure labeled publishing: %v", phases)
			}
		})
	}
}

func TestBusyBuilderStoreIsRetried(t *testing.T) {
	repo, sha := repoFixture(t)
	busy := "#12 ERROR: mount callback failed on /var/lib/docker/containerd/daemon/tmpmounts/x: failed to open writer: ref moby/1/abc locked for 276ms (since now): unavailable"
	for _, tc := range []struct {
		name      string
		failures  int
		code      string
		restarted bool
	}{{"recovers on retry", 1, "", false}, {"restart before last attempt", 2, "", true}, {"stays busy", 3, "BUILDER_BUSY", true}} {
		t.Run(tc.name, func(t *testing.T) {
			restart := filepath.Join(t.TempDir(), "builder-restart")
			builds := 0
			var sawRestart atomic.Bool
			var logs []string
			// Like the systemd path unit on the host: restart the builder, then remove the request.
			stop := make(chan struct{})
			defer close(stop)
			go func() {
				for {
					select {
					case <-stop:
						return
					case <-time.After(time.Millisecond):
						if _, err := os.Stat(restart); err == nil {
							sawRestart.Store(true)
							os.Remove(restart)
						}
					}
				}
			}()
			b := Builder{AllowLocalRepositories: true, TempDir: t.TempDir(), BuilderRestartFile: restart, busyDelay: time.Millisecond, restartWait: 50 * time.Millisecond}
			b.Runner = runnerFunc(func(ctx context.Context, c Command) ([]byte, error) {
				if c.Name != "docker" {
					return (SystemRunner{}).Run(ctx, c)
				}
				if c.Args[0] == "version" {
					return []byte("29.8.2"), nil
				}
				builds++
				if builds <= tc.failures {
					c.Log(busy)
					return []byte(busy), fmt.Errorf("exit status 1")
				}
				for i, a := range c.Args {
					if a == "--metadata-file" {
						os.WriteFile(c.Args[i+1], []byte(`{"containerimage.digest":"sha256:`+strings.Repeat("b", 64)+`"}`), 0600)
					}
				}
				return nil, nil
			})
			_, err := b.Build(context.Background(), Request{Repository: repo, Commit: sha, ImageRepository: "registry.example/app", Spec: BuildSpec{Kind: "dockerfile"}}, func(l string) { logs = append(logs, l) })
			var pipeline *PipelineError
			if tc.code == "" && err != nil {
				t.Fatalf("expected success after retry: %v", err)
			}
			if tc.code != "" && (!errors.As(err, &pipeline) || pipeline.Code != tc.code) {
				t.Fatalf("expected %s, got %v", tc.code, err)
			}
			if sawRestart.Load() != tc.restarted {
				t.Fatalf("restart requested=%v, want %v", sawRestart.Load(), tc.restarted)
			}
			if !strings.Contains(strings.Join(logs, "\n"), "retrying (attempt 2 of 3)") {
				t.Fatal("retry not visible in the build log")
			}
		})
	}
}

func TestFailingDockerfileIsNotRetried(t *testing.T) {
	repo, sha := repoFixture(t)
	builds := 0
	b := Builder{AllowLocalRepositories: true, TempDir: t.TempDir(), busyDelay: time.Millisecond}
	b.Runner = runnerFunc(func(ctx context.Context, c Command) ([]byte, error) {
		if c.Name != "docker" {
			return (SystemRunner{}).Run(ctx, c)
		}
		builds++
		return []byte("ERROR: process \"/bin/sh -c npm run build\" did not complete successfully: exit code: 1"), fmt.Errorf("exit status 1")
	})
	_, err := b.Build(context.Background(), Request{Repository: repo, Commit: sha, ImageRepository: "registry.example/app", Spec: BuildSpec{Kind: "dockerfile"}}, nil)
	var pipeline *PipelineError
	if !errors.As(err, &pipeline) || pipeline.Code != "BUILD_PUBLISH_FAILED" || builds != 1 {
		t.Fatalf("a failing Dockerfile must fail at once: builds=%d err=%v", builds, err)
	}
}

func TestBadgeOnlyWhenAsked(t *testing.T) {
	plain, err := Dockerfile(BuildSpec{Kind: "static", OutputDir: "dist"})
	if err != nil || strings.Contains(plain, "molo-badge") {
		t.Fatalf("no badge unless asked: %v", err)
	}
	badged, err := Dockerfile(BuildSpec{Kind: "static", OutputDir: "dist", Badge: true})
	if err != nil {
		t.Fatal(err)
	}
	step := strings.Index(badged, "node /tmp/molo-badge.js /app/dist ")
	if step < 0 || step > strings.Index(badged, "FROM nginxinc") {
		t.Fatalf("the badge step must run in the build stage, on the output dir:\n%s", badged)
	}
	node, _ := Dockerfile(BuildSpec{Kind: "node-http", Badge: true})
	if strings.Contains(node, "molo-badge") {
		t.Fatal("apps are not badged by the static recipe")
	}
}

// Runs the injection script with Node on a sample output directory (skipped without node).
func TestBadgeScriptInjectsOnce(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not installed")
	}
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "sub"), 0o755)
	os.WriteFile(filepath.Join(dir, "index.html"), []byte("<!doctype html><html lang=\"cs\"><body><h1>Ahoj</h1></BODY></html>"), 0o644)
	os.WriteFile(filepath.Join(dir, "sub", "page.htm"), []byte("<p>no body tag</p>"), 0o644)
	os.WriteFile(filepath.Join(dir, "app.js"), []byte("console.log('</body>')"), 0o644)
	script := filepath.Join(t.TempDir(), "badge.js")
	os.WriteFile(script, []byte(badgeScript), 0o644)
	run := func() {
		if out, err := exec.Command("node", script, dir, base64.StdEncoding.EncodeToString([]byte(badgeHTML))).CombinedOutput(); err != nil {
			t.Fatalf("%s %v", out, err)
		}
	}
	run()
	run() // a second build step must not add a second badge
	index, _ := os.ReadFile(filepath.Join(dir, "index.html"))
	page, _ := os.ReadFile(filepath.Join(dir, "sub", "page.htm"))
	js, _ := os.ReadFile(filepath.Join(dir, "app.js"))
	if strings.Count(string(index), "data-molo-badge") != 1 || !strings.Contains(string(index), "Běží na") || !strings.HasSuffix(string(index), "</BODY></html>") {
		t.Fatalf("index not badged once before </body> in Czech: %s", index)
	}
	if !strings.Contains(string(page), "Running on") {
		t.Fatal("pages without </body> get the badge appended")
	}
	if strings.Contains(string(js), "molo") {
		t.Fatal("only HTML files are touched")
	}
}
