// Package ocibuild builds immutable Git revisions and publishes OCI images via
// Docker Buildx. It requires Git and Docker Buildx on PATH. Build output is never
// transported as an archive. Credentials live only in a temporary Docker config.
package ocibuild

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

type BuildSpec struct {
	Kind           string            `json:"kind"`
	ContextDir     string            `json:"context_dir"`
	Dockerfile     string            `json:"dockerfile"`
	InstallCommand string            `json:"install_command"`
	BuildCommand   string            `json:"build_command"`
	OutputDir      string            `json:"output_dir"`
	StartCommand   string            `json:"start_command"`
	Platform       string            `json:"platform"`
	Port           int               `json:"port"`
	BuildArgs      map[string]string `json:"build_args"`
}
type RegistryCredentials struct{ Registry, Username, Password string }
type Request struct {
	RepoPath, Repository, Commit, ImageRepository, JobID string
	OnPhase                                              func(string)
	Spec                                                 BuildSpec
	Credentials                                          RegistryCredentials
}
type Artifact struct{ CommitSHA, ImageRef, Digest string }

// PipelineError identifies a combined Buildx build/push failure without guessing
// whether compilation, image export, authentication, or registry transport failed.
type PipelineError struct {
	Code string
	Err  error
}

func (e *PipelineError) Error() string { return e.Code + ": " + e.Err.Error() }
func (e *PipelineError) Unwrap() error { return e.Err }

type Command struct {
	Name  string
	Args  []string
	Dir   string
	Env   []string
	Stdin string
	Log   func(string)
}
type Runner interface {
	Run(context.Context, Command) ([]byte, error)
}
type SystemRunner struct{}

func (SystemRunner) Run(ctx context.Context, c Command) ([]byte, error) {
	cmd := exec.CommandContext(ctx, c.Name, c.Args...)
	cmd.WaitDelay = 5 * time.Second
	cmd.Dir = c.Dir
	cmd.Env = c.Env
	cmd.Stdin = strings.NewReader(c.Stdin)
	var mu sync.Mutex
	var captured strings.Builder
	reader, writer := io.Pipe()
	cmd.Stdout = writer
	cmd.Stderr = writer
	done := make(chan struct{})
	go func() {
		defer close(done)
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 4096), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			mu.Lock()
			if captured.Len() < 1024*1024 {
				captured.WriteString(line + "\n")
			}
			mu.Unlock()
			if c.Log != nil {
				c.Log(line)
			}
		}
		io.Copy(io.Discard, reader)
	}()
	err := cmd.Run()
	writer.Close()
	<-done
	reader.Close()
	if ctx.Err() != nil {
		return []byte(captured.String()), ctx.Err()
	}
	return []byte(captured.String()), err
}

type Builder struct {
	AllowLocalRepositories bool
	// LocalRepositoryRoot, when set, restricts local repositories to its direct children.
	LocalRepositoryRoot string
	// CLIPluginDirs lists trusted Docker CLI plugin directories, not credentials.
	CLIPluginDirs []string
	Runner        Runner
	TempDir       string
	Timeout       time.Duration
}

var shaPattern = regexp.MustCompile(`^[a-f0-9]{40}([a-f0-9]{24})?$`)
var digestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
var imagePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*(:[0-9]+)?/[a-z0-9][a-z0-9._/-]*$`)

func (b *Builder) Build(ctx context.Context, r Request, log func(string)) (Artifact, error) {
	var artifact Artifact
	if r.Commit == "" || strings.HasPrefix(r.Commit, "-") || strings.ContainsAny(r.Commit, "\r\n\x00") {
		return artifact, fmt.Errorf("commit is required and must be a Git revision")
	}
	if !imagePattern.MatchString(r.ImageRepository) || strings.Contains(r.ImageRepository, "..") || strings.HasSuffix(r.ImageRepository, "/") {
		return artifact, fmt.Errorf("image repository must be a fully qualified registry/repository without tag")
	}
	if r.Spec.Kind != "static" && r.Spec.Kind != "node-http" && r.Spec.Kind != "dockerfile" {
		return artifact, fmt.Errorf("unsupported build kind")
	}
	timeout := b.Timeout
	if timeout <= 0 {
		timeout = 20 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	root, err := os.MkdirTemp(b.TempDir, "oci-build-")
	if err != nil {
		return artifact, err
	}
	defer os.RemoveAll(root)
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return artifact, err
	}
	config := filepath.Join(root, "docker")
	if err = os.Mkdir(config, 0700); err != nil {
		return artifact, err
	}
	auth := map[string]any{"auths": map[string]any{}}
	pluginDirs := b.CLIPluginDirs
	if len(pluginDirs) == 0 && runtime.GOOS == "darwin" {
		const desktopPlugins = "/Applications/Docker.app/Contents/Resources/cli-plugins"
		if info, e := os.Stat(desktopPlugins); e == nil && info.IsDir() {
			pluginDirs = []string{desktopPlugins}
		}
	}
	if len(pluginDirs) > 0 {
		auth["cliPluginsExtraDirs"] = pluginDirs
	}

	if r.Credentials.Password != "" || r.Credentials.Username != "" {
		registry := strings.Split(r.ImageRepository, "/")[0]
		if r.Credentials.Registry != registry || r.Credentials.Username == "" || r.Credentials.Password == "" {
			return artifact, fmt.Errorf("push credentials must match image registry and include username/password")
		}
		auth["auths"] = map[string]any{registry: map[string]string{"auth": base64.StdEncoding.EncodeToString([]byte(r.Credentials.Username + ":" + r.Credentials.Password))}}
	}
	data, _ := json.Marshal(auth)
	if err = os.WriteFile(filepath.Join(config, "config.json"), data, 0600); err != nil {
		return artifact, err
	}
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + root, "DOCKER_CONFIG=" + config, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "GIT_LFS_SKIP_SMUDGE=1", "LC_ALL=C"}
	runner := b.Runner
	if runner == nil {
		runner = SystemRunner{}
	}
	var commandProgress func(string)
	run := func(dir, name string, args ...string) (string, error) {
		output, e := runner.Run(ctx, Command{Name: name, Args: args, Dir: dir, Env: env, Log: func(line string) {
			if commandProgress != nil {
				commandProgress(line)
			}
			if log == nil {
				return
			}
			for _, secret := range []string{r.Credentials.Password, r.Credentials.Username, base64.StdEncoding.EncodeToString([]byte(r.Credentials.Username + ":" + r.Credentials.Password))} {
				if secret != "" {
					line = strings.ReplaceAll(line, secret, "[REDACTED]")
				}
			}
			log(line)
		}})
		if e != nil {
			return "", fmt.Errorf("%s command failed: %w", name, e)
		}
		return strings.TrimSpace(string(output)), nil
	}
	phase := func(s string) {
		if r.OnPhase != nil {
			r.OnPhase(s)
		}
		if log != nil {
			log(s)
		}
	}
	phase("resolving")
	repository := r.Repository
	if repository == "" {
		repository = r.RepoPath
	}
	if err = b.validateRepository(repository); err != nil {
		return artifact, err
	}
	if !shaPattern.MatchString(r.Commit) {
		return artifact, fmt.Errorf("Build requires a resolved full commit SHA; call Resolve first")
	}
	sha := r.Commit
	checkout := filepath.Join(root, "source")
	if _, err = run(root, "git", "-c", "core.hooksPath=/dev/null", "clone", "--no-local", "--no-checkout", "--", repository, checkout); err != nil {
		return artifact, err
	}
	if _, err = run(checkout, "git", "-c", "core.hooksPath=/dev/null", "checkout", "--detach", sha, "--"); err != nil {
		return artifact, err
	}
	if err = os.RemoveAll(filepath.Join(checkout, ".git")); err != nil {
		return artifact, err
	}
	contextDir, err := inside(checkout, r.Spec.ContextDir, true)
	if err != nil {
		return artifact, err
	}
	dockerfile := ""
	if r.Spec.Dockerfile != "" || r.Spec.Kind == "dockerfile" {
		f := r.Spec.Dockerfile
		if f == "" {
			f = "Dockerfile"
		}
		dockerfile, err = inside(contextDir, f, false)
		if err != nil {
			return artifact, err
		}
	} else {
		content, e := Dockerfile(r.Spec)
		if e != nil {
			return artifact, e
		}
		dockerfile = filepath.Join(root, "Dockerfile.generated")
		if err = os.WriteFile(dockerfile, []byte(content), 0600); err != nil {
			return artifact, err
		}
	}
	metadata := filepath.Join(root, "metadata.json")
	args := []string{"buildx", "build", "--push", "--progress=plain", "--metadata-file", metadata, "--tag", r.ImageRepository + ":" + sha, "--file", dockerfile}
	platform := r.Spec.Platform
	if platform == "" {
		platform = "linux/amd64"
	}
	if platform != "" {
		if !regexp.MustCompile(`^linux/(amd64|arm64)(/v[0-9]+)?$`).MatchString(platform) {
			return artifact, fmt.Errorf("unsupported platform")
		}
		args = append(args, "--platform", platform)
	}
	keys := make([]string, 0, len(r.Spec.BuildArgs))
	for k := range r.Spec.BuildArgs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(k) {
			return artifact, fmt.Errorf("invalid build argument")
		}
		args = append(args, "--build-arg", k+"="+r.Spec.BuildArgs[k])
	}
	args = append(args, contextDir)
	phase("building")
	publishedPhase := false
	commandProgress = func(line string) {
		// BuildKit progress prefixes export/push activity with a numbered vertex.
		// Avoid interpreting arbitrary application RUN output as a lifecycle marker.
		if !publishedPhase && regexp.MustCompile(`^#[0-9]+ (exporting to image|exporting to oci image format|pushing layers|pushing manifest)`).MatchString(line) {
			publishedPhase = true
			phase("publishing")
		}
	}
	// Buildx combines build and publication; only metadata from a successful push
	// constitutes an artifact. Streaming output redacts registry credentials.
	if _, err = run(root, "docker", args...); err != nil {
		return artifact, &PipelineError{Code: "BUILD_PUBLISH_FAILED", Err: err}
	}
	if !publishedPhase {
		phase("publishing")
	}
	data, err = os.ReadFile(metadata)
	if err != nil {
		return artifact, fmt.Errorf("read image metadata: %w", err)
	}
	var m struct {
		Digest string `json:"containerimage.digest"`
	}
	if err = json.Unmarshal(data, &m); err != nil {
		return artifact, fmt.Errorf("invalid image metadata")
	}
	if !digestPattern.MatchString(m.Digest) {
		return artifact, fmt.Errorf("missing or invalid published image digest")
	}
	return Artifact{CommitSHA: sha, Digest: m.Digest, ImageRef: r.ImageRepository + "@" + m.Digest}, nil
}
func inside(root, rel string, dir bool) (string, error) {
	if rel == "" {
		rel = "."
	}
	if filepath.IsAbs(rel) {
		return "", fmt.Errorf("build path must be relative")
	}
	p, err := filepath.EvalSymlinks(filepath.Join(root, rel))
	if err != nil {
		return "", err
	}
	r, err := filepath.Rel(root, p)
	if err != nil || r == ".." || strings.HasPrefix(r, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("build path escapes checkout")
	}
	s, err := os.Stat(p)
	if err != nil {
		return "", err
	}
	if s.IsDir() != dir {
		return "", fmt.Errorf("build path has incorrect type")
	}
	return p, nil
}

// Dockerfile returns a generated multi-stage recipe. Commands are intentional
// project-controlled shell code executed inside the build container, never host shell.
func Dockerfile(s BuildSpec) (string, error) {
	if s.Kind != "static" && s.Kind != "node-http" {
		return "", fmt.Errorf("unsupported generated build kind")
	}
	for _, v := range []string{s.InstallCommand, s.BuildCommand, s.StartCommand, s.OutputDir} {
		if strings.ContainsAny(v, "\r\n\x00") {
			return "", fmt.Errorf("build settings must be single-line")
		}
	}
	install := s.InstallCommand
	if install == "" {
		install = "npm ci"
	}
	build := s.BuildCommand
	if build == "" {
		build = "npm run build"
	}
	out := "FROM node:22-alpine AS build\nWORKDIR /app\nCOPY . .\nRUN " + install + "\nRUN " + build + "\n"
	if s.Kind == "static" {
		if s.Port != 0 && s.Port != 8080 {
			return "", fmt.Errorf("generated static images listen on port 8080")
		}
		dir := s.OutputDir
		if dir == "" {
			dir = "dist"
		}
		if filepath.IsAbs(dir) || dir == ".." || strings.HasPrefix(filepath.Clean(dir), "../") || !regexp.MustCompile(`^[A-Za-z0-9_./-]+$`).MatchString(dir) {
			return "", fmt.Errorf("invalid static output directory")
		}
		out += "FROM nginxinc/nginx-unprivileged:stable-alpine\nUSER 10001:10001\nCOPY --from=build /app/" + dir + " /usr/share/nginx/html\nEXPOSE 8080\nENTRYPOINT [\"nginx\"]\nCMD [\"-g\", \"daemon off;\"]\n"
	} else {
		port := s.Port
		if port == 0 {
			port = 3000
		}
		if port < 1 || port > 65535 {
			return "", fmt.Errorf("invalid HTTP port")
		}
		start := s.StartCommand
		if start == "" {
			start = "npm start"
		}
		cmd, _ := json.Marshal([]string{"sh", "-c", start})
		out += fmt.Sprintf("FROM node:22-alpine\nWORKDIR /app\nENV NODE_ENV=production\nENV HOME=/tmp\nENV NPM_CONFIG_CACHE=/tmp/.npm\nENV PORT=%d\nCOPY --from=build --chown=10001:10001 /app /app\nUSER 10001:10001\nEXPOSE %d\nCMD %s\n", port, port, cmd)
	}
	return out, nil
}

// Resolve pins a branch, tag or full SHA before a job is queued. HTTPS URLs may
// not embed credentials. Local repositories require explicit opt-in.
func (b *Builder) Resolve(ctx context.Context, repository, ref string) (string, error) {
	if err := b.validateRepository(repository); err != nil {
		return "", err
	}
	if ref == "" || strings.HasPrefix(ref, "-") || strings.ContainsAny(ref, "\r\n\x00") {
		return "", fmt.Errorf("invalid Git ref")
	}
	if shaPattern.MatchString(ref) {
		return ref, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	runner := b.Runner
	if runner == nil {
		runner = SystemRunner{}
	}
	refs := []string{ref}
	if strings.HasPrefix(ref, "refs/tags/") {
		refs = append(refs, ref+"^{}")
	}
	if !strings.HasPrefix(ref, "refs/") && ref != "HEAD" {
		refs = []string{"refs/heads/" + ref, "refs/tags/" + ref, "refs/tags/" + ref + "^{}"}
	}
	args := append([]string{"ls-remote", "--exit-code", "--", repository}, refs...)
	out, err := runner.Run(ctx, Command{Name: "git", Args: args, Env: []string{"PATH=" + os.Getenv("PATH"), "HOME=/nonexistent", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "LC_ALL=C"}})
	if err != nil {
		return "", fmt.Errorf("resolve Git revision: %w", err)
	}
	var found, peeled string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		parts := strings.Fields(line)
		if len(parts) != 2 || !shaPattern.MatchString(parts[0]) {
			continue
		}
		if strings.HasSuffix(parts[1], "^{}") {
			peeled = parts[0]
			continue
		}
		if found != "" && found != parts[0] {
			return "", fmt.Errorf("ambiguous Git revision; use full refs/heads or refs/tags name")
		}
		found = parts[0]
	}
	if peeled != "" {
		found = peeled
	}
	if found == "" {
		return "", fmt.Errorf("Git revision not found")
	}
	return found, nil
}
func (b *Builder) validateRepository(repository string) error {
	u, err := url.Parse(repository)
	if err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" {
		return nil
	}
	if b.AllowLocalRepositories && filepath.IsAbs(repository) && (b.LocalRepositoryRoot == "" || filepath.Dir(filepath.Clean(repository)) == b.LocalRepositoryRoot) {
		return nil
	}
	return fmt.Errorf("repository must be an HTTPS URL without embedded credentials")
}
