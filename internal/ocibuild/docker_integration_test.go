package ocibuild

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestDockerRegistryIntegration explicitly opts into disposable local Docker
// resources. It never logs credentials, changes daemon settings, or uses a remote
// registry. Registry htpasswd separates identities but does not enforce ACLs.
func TestDockerRegistryIntegration(t *testing.T) {
	if os.Getenv("OCI_DOCKER_INTEGRATION") != "1" {
		t.Skip("set OCI_DOCKER_INTEGRATION=1 to build, publish, and run examples on local Docker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	tmp := t.TempDir()
	fixtureConfig := filepath.Join(tmp, "fixture-config")
	if err := os.Mkdir(fixtureConfig, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixtureConfig, "config.json"), []byte(`{"auths":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	fixtureEnv := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + tmp, "DOCKER_CONFIG=" + fixtureConfig}
	makeRun := func(t *testing.T) func([]string, string, ...string) string {
		return func(env []string, name string, args ...string) string {
			t.Helper()
			c := exec.CommandContext(ctx, name, args...)
			c.WaitDelay = 5 * time.Second
			if name == "docker" && env == nil {
				env = fixtureEnv
			}
			if env != nil {
				c.Env = env
			}
			out, err := c.CombinedOutput()
			if err != nil {
				t.Fatalf("%s failed: %v\n%s", name, err, out)
			}
			return strings.TrimSpace(string(out))
		}
	}
	run := makeRun(t)
	run(nil, "docker", "info", "--format", "{{.ServerVersion}}")
	random := func() string {
		b := make([]byte, 24)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		return hex.EncodeToString(b)
	}
	pushPass, pullPass := random(), random()
	prefix := "oci-test-" + random()[:12]
	authPath := filepath.Join(tmp, "htpasswd")
	for i, pair := range [][2]string{{"test-push", pushPass}, {"test-pull", pullPass}} {
		args := []string{"-iB", authPath, pair[0]}
		if i == 0 {
			args[0] = "-ciB"
		}
		c := exec.CommandContext(ctx, "/usr/sbin/htpasswd", args...)
		c.Stdin = strings.NewReader(pair[1] + "\n")
		if err := c.Run(); err != nil {
			t.Fatalf("generate registry authentication: %v", err)
		}
	}
	os.Chmod(authPath, 0644)
	cleanup := func(name string) {
		t.Cleanup(func() {
			cctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			cmd := exec.CommandContext(cctx, "docker", "rm", "-fv", name)
			cmd.Env = fixtureEnv
			cmd.WaitDelay = 5 * time.Second
			cmd.Run()
		})
	}
	registryName := prefix + "-registry"
	cleanup(registryName)
	// Docker Desktop's daemon resolves localhost inside its Linux VM. Bind the
	// registry to VM loopback, never a public interface, so daemon and BuildKit
	// access exactly the same disposable endpoint without daemon configuration.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	registry := listener.Addr().String()
	listener.Close()
	run(nil, "docker", "run", "-d", "--name", registryName, "--label", "deployer.integration="+prefix, "--network", "host", "--mount", "type=bind,src="+authPath+",dst=/auth/htpasswd,readonly", "--env", "REGISTRY_HTTP_ADDR="+registry, "--env", "REGISTRY_AUTH=htpasswd", "--env", "REGISTRY_AUTH_HTPASSWD_REALM=Integration", "--env", "REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd", "registry:2")
	client := &http.Client{Timeout: 2 * time.Second}
	// Probe from the same network namespace; Desktop may not mirror VM loopback
	// onto macOS loopback. An unauthenticated registry must return HTTP 401.
	ready := false
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		c := exec.CommandContext(ctx, "docker", "exec", registryName, "wget", "-S", "-O", "/dev/null", "http://"+registry+"/v2/")
		c.Env = fixtureEnv
		c.WaitDelay = 5 * time.Second
		output, _ := c.CombinedOutput()
		if strings.Contains(string(output), "401") {
			ready = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !ready {
		t.Fatalf("registry failed to start: %s", run(nil, "docker", "logs", registryName))
	}
	// Work from a new committed repository, so the test never alters the current
	// checkout or accidentally packages its dirty/untracked application files.
	source := filepath.Join(tmp, "source")
	os.Mkdir(source, 0755)
	original, err := filepath.Abs("../../examples")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"runtime-static", "runtime-node"} {
		err = filepath.Walk(filepath.Join(original, kind), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(original, path)
			dest := filepath.Join(source, "examples", rel)
			if info.IsDir() {
				return os.MkdirAll(dest, 0755)
			}
			data, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			return os.WriteFile(dest, data, 0644)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	gitEnv := append(os.Environ(), "GIT_AUTHOR_NAME=Integration", "GIT_AUTHOR_EMAIL=integration@example.invalid", "GIT_COMMITTER_NAME=Integration", "GIT_COMMITTER_EMAIL=integration@example.invalid")
	run(gitEnv, "git", "-C", source, "init")
	run(gitEnv, "git", "-C", source, "add", ".")
	run(gitEnv, "git", "-C", source, "commit", "-m", "integration fixtures")
	sha := run(gitEnv, "git", "-C", source, "rev-parse", "HEAD")
	pullConfig := filepath.Join(tmp, "pull-config")
	os.Mkdir(pullConfig, 0700)
	credentials, _ := json.Marshal(map[string]any{"auths": map[string]any{registry: map[string]string{"auth": base64.StdEncoding.EncodeToString([]byte("test-pull:" + pullPass))}}})
	os.WriteFile(filepath.Join(pullConfig, "config.json"), credentials, 0600)
	pullEnv := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + tmp, "DOCKER_CONFIG=" + pullConfig}
	for _, kind := range []string{"runtime-static", "runtime-node"} {
		t.Run(kind, func(t *testing.T) {
			run := makeRun(t)
			var spec BuildSpec
			data, err := os.ReadFile(filepath.Join(original, kind, "build-spec.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(data, &spec); err != nil {
				t.Fatal(err)
			}
			imageRepository := registry + "/" + kind
			t.Cleanup(func() {
				cctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
				defer stop()
				cmd := exec.CommandContext(cctx, "docker", "image", "rm", imageRepository+":"+sha)
				cmd.Env = fixtureEnv
				cmd.WaitDelay = 5 * time.Second
				cmd.Run()
			})
			b := Builder{AllowLocalRepositories: true, Timeout: 10 * time.Minute}
			artifact, err := b.Build(ctx, Request{Repository: source, Commit: sha, ImageRepository: imageRepository, Spec: spec, Credentials: RegistryCredentials{Registry: registry, Username: "test-push", Password: pushPass}}, func(line string) {
				if strings.Contains(line, pushPass) || strings.Contains(line, pullPass) {
					t.Error("credential leaked into build output")
				}
				for _, secret := range []string{pushPass, pullPass, base64.StdEncoding.EncodeToString([]byte("test-push:" + pushPass)), base64.StdEncoding.EncodeToString([]byte("test-pull:" + pullPass))} {
					line = strings.ReplaceAll(line, secret, "[REDACTED]")
				}
				t.Log(line)
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("published immutable image %s", artifact.ImageRef)
			run(pullEnv, "docker", "pull", "--platform", "linux/amd64", artifact.ImageRef)
			name := prefix + "-" + kind
			t.Cleanup(func() {
				cctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
				defer stop()
				cmd := exec.CommandContext(cctx, "docker", "rm", "-fv", name)
				cmd.Env = fixtureEnv
				cmd.WaitDelay = 5 * time.Second
				cmd.Run()
				cmd = exec.CommandContext(cctx, "docker", "image", "rm", artifact.ImageRef)
				cmd.Env = fixtureEnv
				cmd.WaitDelay = 5 * time.Second
				cmd.Run()
			})
			run(nil, "docker", "run", "-d", "--name", name, "--label", "deployer.integration="+prefix, "--platform", "linux/amd64", "--user", "10001:10001", "--read-only", "--tmpfs", "/tmp:rw,noexec,nosuid,size=64m", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--cpus", "0.5", "--memory", "128m", "--pids-limit", "64", "--publish", fmt.Sprintf("127.0.0.1::%d", spec.Port), artifact.ImageRef)
			inspect := run(nil, "docker", "inspect", name)
			var instances []struct {
				Config     struct{ User string }
				HostConfig struct {
					ReadonlyRootfs bool
					CapDrop        []string
					SecurityOpt    []string
					Memory         int64
					NanoCPUs       int64
					PidsLimit      int
					Tmpfs          map[string]string
				}
			}
			if err = json.Unmarshal([]byte(inspect), &instances); err != nil || len(instances) != 1 {
				t.Fatalf("inspect failed: %v", err)
			}
			instance := instances[0]
			if instance.Config.User != "10001:10001" || !instance.HostConfig.ReadonlyRootfs || instance.HostConfig.Memory != 128*1024*1024 || instance.HostConfig.NanoCPUs != 500000000 || instance.HostConfig.PidsLimit != 64 || instance.HostConfig.Tmpfs["/tmp"] == "" || strings.Join(instance.HostConfig.CapDrop, ",") != "ALL" || !strings.Contains(strings.Join(instance.HostConfig.SecurityOpt, ","), "no-new-privileges") {
				t.Fatalf("container hardening mismatch: %+v", instance)
			}
			endpoint := run(nil, "docker", "port", name, fmt.Sprintf("%d/tcp", spec.Port))
			deadline := time.Now().Add(30 * time.Second)
			healthy := false
			for time.Now().Before(deadline) {
				response, e := client.Get("http://" + endpoint + "/healthz")
				if e == nil {
					response.Body.Close()
					if response.StatusCode == 200 {
						healthy = true
						break
					}
				}
				time.Sleep(200 * time.Millisecond)
			}
			if !healthy {
				logs := run(nil, "docker", "logs", name)
				t.Fatalf("health check failed: %s", logs)
			}
			response, err := client.Get("http://" + endpoint + "/definitely-not-a-route")
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != 404 {
				t.Fatalf("unknown route status %d", response.StatusCode)
			}
			t.Log("read-only UID10001 runtime: /healthz=200, unknown route=404")
		})
	}
}
