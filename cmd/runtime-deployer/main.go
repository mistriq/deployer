// runtime-deployer is the private Runtime Engine control plane. It does not load
// the legacy admin UI, runner API, or Compose pipeline.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/mistriq/deployer/internal/controlplane"
	"github.com/mistriq/deployer/internal/ocibuild"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func secret(k string) (string, error) {
	p := os.Getenv(k)
	if p == "" {
		return "", fmt.Errorf("%s is required", k)
	}
	info, err := os.Stat(p)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return "", fmt.Errorf("%s must point to a private regular file (0600)", k)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return "", fmt.Errorf("cannot read %s", k)
	}
	return strings.TrimSpace(string(b)), nil
}

// A remote Runtime pulls images from its own network, not the builder's.
// Keep loopback registries available for sandbox and local integration tests.
func checkRegistryLocation(image, runtimeURL string, sandbox bool) error {
	if sandbox {
		return nil
	}
	registry, err := url.Parse("https://" + strings.Split(image, "/")[0])
	if err != nil {
		return errors.New("invalid registry address")
	}
	runtime, err := url.Parse(runtimeURL)
	if err != nil {
		return errors.New("invalid Runtime address")
	}
	local := func(host string) bool {
		host = strings.TrimSuffix(strings.ToLower(host), ".")
		ip := net.ParseIP(host)
		return host == "localhost" || strings.HasSuffix(host, ".localhost") || (ip != nil && (ip.IsLoopback() || ip.IsUnspecified()))
	}
	if local(registry.Hostname()) && !local(runtime.Hostname()) {
		return errors.New("remote Runtime requires a registry address reachable from its server; REGISTRY_IMAGE_PREFIX points to a loopback or unspecified address")
	}
	return nil
}

func run() error {
	addr := env("DEPLOYER_LISTEN", "127.0.0.1:8091")
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return errors.New("invalid DEPLOYER_LISTEN")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("DEPLOYER_LISTEN must use a loopback IP; expose through a private TLS proxy")
	}
	keyHex, err := secret("DEPLOYER_STATE_KEY_FILE")
	if err != nil {
		return err
	}
	key, err := hex.DecodeString(keyHex)
	if err != nil || len(key) != 32 {
		return errors.New("DEPLOYER_STATE_KEY_FILE must contain 64 hex characters")
	}
	portalJSON, err := secret("PORTAL_TOKENS_FILE")
	if err != nil {
		return err
	}
	var tenants map[string]string
	if json.Unmarshal([]byte(portalJSON), &tenants) != nil || len(tenants) == 0 {
		return errors.New("PORTAL_TOKENS_FILE must contain a tenant-to-token JSON object")
	}
	tokens := map[string]string{}
	for tenant, t := range tenants {
		if tenant == "" || len(t) < 32 || strings.ContainsAny(t, "\r\n") || tokens[t] != "" {
			return errors.New("portal tokens require unique values of at least 32 characters")
		}
		tokens[t] = tenant
	}
	sandbox := env("RUNTIME_SANDBOX", "true")
	if sandbox != "true" && sandbox != "false" {
		return errors.New("RUNTIME_SANDBOX must be true or false")
	}
	image := os.Getenv("REGISTRY_IMAGE_PREFIX")
	if !strings.Contains(image, "/") || strings.ContainsAny(image, " @\r\n") {
		return errors.New("REGISTRY_IMAGE_PREFIX must be registry.example/team")
	}
	runtimes, defaultTarget, runtimeSecrets, err := loadRuntimeTargets(sandbox == "true", image)
	if err != nil {
		return err
	}
	creds := ocibuild.RegistryCredentials{Registry: strings.Split(image, "/")[0], Username: os.Getenv("REGISTRY_PUSH_USERNAME")}
	if os.Getenv("REGISTRY_PUSH_PASSWORD_FILE") != "" {
		creds.Password, err = secret("REGISTRY_PUSH_PASSWORD_FILE")
		if err != nil {
			return err
		}
	}
	if (creds.Password == "") != (creds.Username == "") {
		return errors.New("registry push username and password must both be configured")
	}
	path := env("DEPLOYER_STATE_PATH", "./runtime-state/state.enc")
	if !filepath.IsAbs(path) {
		path, err = filepath.Abs(path)
		if err != nil {
			return errors.New("invalid state path")
		}
	}
	store, err := controlplane.OpenStore(path, key)
	if err != nil {
		return fmt.Errorf("open state: %w", err)
	}
	defer store.Close()
	svc := &controlplane.Service{Store: store, Runtimes: runtimes, Builder: &ocibuild.Builder{}, Config: controlplane.Config{DefaultRuntimeTarget: defaultTarget, Tokens: tokens, ImagePrefix: image, Credentials: creds, ServiceSecrets: append(runtimeSecrets, keyHex)}}
	if len(os.Args) > 1 {
		if len(os.Args) == 2 && os.Args[1] == "--check-config" {
			fmt.Println("Configuration valid; no network calls made.")
			return nil
		}
		return errors.New("usage: runtime-deployer [--check-config]")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.New("cannot listen on DEPLOYER_LISTEN")
	}
	server := &http.Server{Handler: svc, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 45 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384}
	results := make(chan error, 2)
	go func() { results <- svc.Run(ctx) }()
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		results <- err
	}()
	fmt.Printf("Runtime Deployer listening on %s (sandbox=%s)\n", addr, sandbox)
	var first error
	received := false
	select {
	case <-ctx.Done():
	case first = <-results:
		received = true
	}
	cancel()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancelShutdown()
	_ = server.Shutdown(shutdown)
	pending := 2
	if received {
		pending--
	}
	for pending > 0 {
		select {
		case e := <-results:
			if first == nil {
				first = e
			}
			pending--
		case <-shutdown.Done():
			return errors.New("shutdown timed out; persisted jobs resume on restart")
		}
	}
	if first != nil {
		return errors.New("service stopped after an internal error; inspect encrypted state and configuration")
	}
	return nil
}
