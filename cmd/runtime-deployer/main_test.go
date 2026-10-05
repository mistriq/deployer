package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRegistryLocation(t *testing.T) {
	for _, tc := range []struct {
		name, registry, runtime string
		sandbox, rejected       bool
	}{
		{"remote IPv4", "127.0.0.1:5500/molo", "https://scr.socen.eu", false, true},
		{"remote IPv6", "[::1]:5500/molo", "https://scr.socen.eu", false, true},
		{"remote localhost", "LOCALHOST.:5500/molo", "https://scr.socen.eu", false, true},
		{"unspecified", "0.0.0.0:5500/molo", "https://scr.socen.eu", false, true},
		{"sandbox", "127.0.0.1:5500/molo", "https://scr.socen.eu", true, false},
		{"local integration", "127.0.0.1:5500/molo", "http://localhost:9510", false, false},
		{"remote registry", "ghcr.io/example", "https://scr.socen.eu", false, false},
		{"private network", "10.0.0.2:5000/apps", "https://scr.socen.eu", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkRegistryLocation(tc.registry, tc.runtime, tc.sandbox)
			if (err != nil) != tc.rejected {
				t.Fatalf("unexpected configuration result: %v", err)
			}
		})
	}
}

func TestConfigCheckAndPrivateCredentialFiles(t *testing.T) {
	dir := t.TempDir()
	write := func(name, value string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	t.Setenv("DEPLOYER_STATE_KEY_FILE", write("state-key", strings.Repeat("01", 32)))
	t.Setenv("PORTAL_TOKENS_FILE", write("portal", `{"org_test":"`+strings.Repeat("p", 32)+`"}`))
	tokenFile := write("runtime", strings.Repeat("r", 32))
	t.Setenv("RUNTIME_TOKEN_FILE", tokenFile)
	t.Setenv("RUNTIME_BASE_URL", "https://runtime.invalid")
	t.Setenv("RUNTIME_SANDBOX", "true")
	t.Setenv("REGISTRY_IMAGE_PREFIX", "registry.invalid/apps")
	t.Setenv("REGISTRY_PUSH_USERNAME", "")
	t.Setenv("REGISTRY_PUSH_PASSWORD_FILE", "")
	t.Setenv("DEPLOYER_LISTEN", "127.0.0.1:8091")
	t.Setenv("DEPLOYER_STATE_PATH", filepath.Join(dir, "state.enc"))
	args := os.Args
	os.Args = []string{"runtime-deployer", "--check-config"}
	defer func() { os.Args = args }()
	if err := run(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEPLOYER_LISTEN", "0.0.0.0:8091")
	if err := run(); err == nil {
		t.Fatal("public listener accepted")
	}
	t.Setenv("DEPLOYER_LISTEN", "127.0.0.1:8091")
	if err := os.Chmod(tokenFile, 0644); err != nil {
		t.Fatal(err)
	}
	if err := run(); err == nil || strings.Contains(err.Error(), strings.Repeat("r", 32)) {
		t.Fatal("unsafe credential permissions accepted or leaked")
	}
}
