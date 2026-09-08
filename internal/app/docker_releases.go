package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

type dockerActivationSnapshot struct {
	ActivationID  int64             `json:"activation_id"`
	ImageArtifact string            `json:"image_artifact"`
	ImageSHA256   string            `json:"image_sha256"`
	ComposeFile   string            `json:"compose_file"`
	ConfigFiles   map[string]string `json:"config_files"`
}

type dockerReleaseCommand func(context.Context, string, ...string) error

func captureDockerActivation(deployDir string, activationID int64, imageArtifact, imageSHA256, composeFile string, configPaths []string) (string, error) {
	if activationID <= 0 {
		return "", fmt.Errorf("activation ID must be positive")
	}
	actualSHA, err := fileSHA256(imageArtifact)
	if err != nil {
		return "", err
	}
	if imageSHA256 == "" || actualSHA != imageSHA256 {
		return "", fmt.Errorf("image artifact checksum mismatch")
	}
	compose, err := cleanRelativeDeployPath(composeFile)
	if err != nil {
		return "", fmt.Errorf("compose file: %w", err)
	}
	all := append([]string{compose}, configPaths...)
	files := map[string]string{}
	for _, path := range all {
		clean, err := cleanRelativeDeployPath(path)
		if err != nil {
			return "", fmt.Errorf("config path %q: %w", path, err)
		}
		if _, seen := files[clean]; seen {
			continue
		}
		source := filepath.Join(deployDir, clean)
		info, err := os.Lstat(source)
		if err != nil {
			return "", fmt.Errorf("read config %s: %w", clean, err)
		}
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("config %s must be a regular file", clean)
		}
		body, err := os.ReadFile(source)
		if err != nil {
			return "", err
		}
		files[clean] = string(body)
	}
	layout, err := newFilesReleaseLayout(deployDir)
	if err != nil {
		return "", err
	}
	dir := filepath.Join(layout.root, "docker-activations", strconv.FormatInt(activationID, 10))
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	manifest := dockerActivationSnapshot{activationID, imageArtifact, imageSHA256, compose, files}
	body, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "snapshot.json")
	if err := writePrivateFileAtomic(path, body); err != nil {
		return "", err
	}
	return path, nil
}

func activateDockerSnapshot(ctx context.Context, deployDir, snapshotPath string, run dockerReleaseCommand) error {
	body, err := os.ReadFile(snapshotPath)
	if err != nil {
		return err
	}
	var snapshot dockerActivationSnapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return fmt.Errorf("decode Docker activation snapshot: %w", err)
	}
	compose, err := cleanRelativeDeployPath(snapshot.ComposeFile)
	if err != nil {
		return fmt.Errorf("stored compose file: %w", err)
	}
	snapshot.ComposeFile = compose
	sha, err := fileSHA256(snapshot.ImageArtifact)
	if err != nil {
		return err
	}
	if sha != snapshot.ImageSHA256 {
		return fmt.Errorf("retained image checksum mismatch")
	}
	if err := run(ctx, "", "docker", "load", "-i", snapshot.ImageArtifact); err != nil {
		return fmt.Errorf("load retained image: %w", err)
	}
	for path, content := range snapshot.ConfigFiles {
		clean, err := cleanRelativeDeployPath(path)
		if err != nil {
			return err
		}
		if err := writePrivateFileAtomic(filepath.Join(deployDir, clean), []byte(content)); err != nil {
			return fmt.Errorf("restore config %s: %w", clean, err)
		}
	}
	if err := run(ctx, deployDir, "docker", "compose", "-f", snapshot.ComposeFile, "up", "-d"); err != nil {
		return fmt.Errorf("activate retained Compose config: %w", err)
	}
	return nil
}

func writePrivateFileAtomic(path string, body []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".deployer-config-")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := temp.Chmod(0600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(body); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
