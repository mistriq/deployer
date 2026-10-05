package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/mistriq/deployer/internal/controlplane"
	"github.com/mistriq/deployer/internal/runtimeengine"
)

type targetProfile struct {
	BaseURL   string `json:"base_url"`
	TokenFile string `json:"token_file"`
}
type targetCatalogue struct {
	DefaultTargetID string                   `json:"default_target_id"`
	Targets         map[string]targetProfile `json:"targets"`
}

var targetIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$`)

func privateContents(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("configuration must use a private regular file (0600)")
	}
	if info.Size() > 1024*1024 {
		return nil, errors.New("configuration file is too large")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("cannot read private configuration file")
	}
	return b, nil
}

// Target IDs are durable routing identities. Never reassign an existing ID to
// another node: historical jobs and rollback snapshots still reference it.
func loadRuntimeTargets(sandbox bool, image string) (map[string]controlplane.Runtime, string, []string, error) {
	catalogue := targetCatalogue{DefaultTargetID: "default", Targets: map[string]targetProfile{"default": {BaseURL: os.Getenv("RUNTIME_BASE_URL"), TokenFile: os.Getenv("RUNTIME_TOKEN_FILE")}}}
	if path := os.Getenv("RUNTIME_TARGETS_FILE"); path != "" {
		if !filepath.IsAbs(path) {
			return nil, "", nil, errors.New("RUNTIME_TARGETS_FILE must be an absolute path")
		}
		b, err := privateContents(path)
		if err != nil {
			return nil, "", nil, err
		}
		dec := json.NewDecoder(strings.NewReader(string(b)))
		dec.DisallowUnknownFields()
		catalogue = targetCatalogue{}
		if err = dec.Decode(&catalogue); err != nil {
			return nil, "", nil, errors.New("invalid RUNTIME_TARGETS_FILE JSON")
		}
		var extra any
		if dec.Decode(&extra) != io.EOF {
			return nil, "", nil, errors.New("invalid trailing RUNTIME_TARGETS_FILE data")
		}
	}
	if len(catalogue.Targets) == 0 || len(catalogue.Targets) > 128 || !targetIDPattern.MatchString(catalogue.DefaultTargetID) {
		return nil, "", nil, errors.New("Runtime targets require a valid default_target_id and 1–128 targets")
	}
	if _, ok := catalogue.Targets[catalogue.DefaultTargetID]; !ok {
		return nil, "", nil, errors.New("default Runtime target is not configured")
	}
	runtimes := make(map[string]controlplane.Runtime)
	secrets := []string{}
	for id, p := range catalogue.Targets {
		if !targetIDPattern.MatchString(id) || !filepath.IsAbs(p.TokenFile) {
			return nil, "", nil, errors.New("Runtime targets require valid IDs and absolute token_file paths")
		}
		b, err := privateContents(p.TokenFile)
		if err != nil {
			return nil, "", nil, err
		}
		token := strings.TrimSpace(string(b))
		client, err := runtimeengine.New(runtimeengine.Config{BaseURL: p.BaseURL, Token: token, Sandbox: sandbox})
		if err != nil {
			return nil, "", nil, errors.New("invalid Runtime target endpoint or token")
		}
		if err = checkRegistryLocation(image, p.BaseURL, sandbox); err != nil {
			return nil, "", nil, err
		}
		runtimes[id] = client
		secrets = append(secrets, token)
	}
	return runtimes, catalogue.DefaultTargetID, secrets, nil
}
