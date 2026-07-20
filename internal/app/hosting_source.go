package app

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var prepareHostingSource = prepareHostingGitSource

func prepareHostingGitSource(ctx context.Context, project *HostingProject, commitSHA string) (string, string, error) {
	if project == nil || !filepath.IsAbs(project.RepoPath) {
		return "", "", fmt.Errorf("hosting repository path is invalid")
	}
	info, err := os.Stat(project.RepoPath)
	if err != nil || !info.IsDir() {
		return "", "", fmt.Errorf("hosting repository is unavailable")
	}
	resolved, err := exec.CommandContext(ctx, "git", "-C", project.RepoPath, "rev-parse", "--verify", commitSHA+"^{commit}").Output()
	if err != nil {
		return "", "", fmt.Errorf("verify commit: %w", err)
	}
	if strings.TrimSpace(string(resolved)) != commitSHA {
		return "", "", fmt.Errorf("resolved commit identity does not match requested SHA")
	}
	if err := currentArtifactStorage().Ensure(); err != nil {
		return "", "", err
	}
	temporary, err := os.CreateTemp(appConfig.ArtifactDir, "hosting-source-*.tar")
	if err != nil {
		return "", "", err
	}
	temporaryPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		os.Remove(temporaryPath)
		return "", "", err
	}
	defer os.Remove(temporaryPath)
	if output, err := exec.CommandContext(ctx, "git", "-C", project.RepoPath, "archive", "--format=tar", "--output", temporaryPath, commitSHA).CombinedOutput(); err != nil {
		return "", "", fmt.Errorf("archive exact commit: %w: %s", err, redactSecrets(string(output)))
	}
	digest, err := fileSHA256(temporaryPath)
	if err != nil {
		return "", "", err
	}
	digest = "sha256:" + digest
	finalPath := managedArtifactPath("hosting-source-" + strings.TrimPrefix(digest, "sha256:") + ".tar")
	if _, err := os.Stat(finalPath); err == nil {
		return finalPath, digest, nil
	}
	if err := os.Rename(temporaryPath, finalPath); err != nil {
		return "", "", err
	}
	return finalPath, digest, nil
}
