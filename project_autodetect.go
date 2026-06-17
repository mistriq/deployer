package main

import (
	"database/sql"
	"errors"
	"net/http"
	"os"
	"path/filepath"
)

type repoAutodetectResponse struct {
	ProjectID        int64             `json:"project_id"`
	RepoPath         string            `json:"repo_path"`
	Dockerfiles      []string          `json:"dockerfiles"`
	ComposeFiles     []string          `json:"compose_files"`
	PackageManagers  []string          `json:"package_managers"`
	IgnoreFile       string            `json:"ignore_file,omitempty"`
	LikelyDeployMode string            `json:"likely_deploy_mode"`
	SuggestedFields  map[string]string `json:"suggested_fields"`
	Warnings         []string          `json:"warnings"`
}

func handleAPIProjectAutodetect(w http.ResponseWriter, r *http.Request, projectID int64) {
	result, err := newRepoAutodetect(projectID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			jsonErrorCode(w, errCodeProjectNotFound, "project not found", http.StatusNotFound)
			return
		}
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, result)
}

func newRepoAutodetect(projectID int64) (*repoAutodetectResponse, error) {
	project, err := getProject(projectID)
	if err != nil {
		return nil, err
	}
	result := detectRepo(project.RepoPath)
	result.ProjectID = project.ID
	return result, nil
}

func detectRepo(repoPath string) *repoAutodetectResponse {
	result := &repoAutodetectResponse{
		RepoPath:         repoPath,
		Dockerfiles:      []string{},
		ComposeFiles:     []string{},
		PackageManagers:  []string{},
		LikelyDeployMode: "files",
		SuggestedFields:  map[string]string{"deploy_mode": "files"},
		Warnings:         []string{},
	}
	if info, err := os.Stat(repoPath); err != nil || !info.IsDir() {
		result.Warnings = append(result.Warnings, "Repository path is not readable as a directory.")
		return result
	}

	result.Dockerfiles = existingRepoFiles(repoPath, []string{"Dockerfile", "docker/Dockerfile", ".docker/Dockerfile"})
	result.ComposeFiles = existingRepoFiles(repoPath, []string{"docker-compose.yml", "docker-compose.yaml", "compose.yml", "compose.yaml"})
	result.PackageManagers = detectPackageManagers(repoPath)
	if file := firstExistingRepoFile(repoPath, []string{".deployignore", ".dockerignore"}); file != "" {
		result.IgnoreFile = file
	}
	if len(result.Dockerfiles) > 0 || len(result.ComposeFiles) > 0 {
		result.LikelyDeployMode = "docker"
		result.SuggestedFields["deploy_mode"] = "docker"
	}
	if len(result.Dockerfiles) > 0 {
		result.SuggestedFields["dockerfile_path"] = result.Dockerfiles[0]
	}
	if len(result.ComposeFiles) > 0 {
		result.SuggestedFields["compose_file"] = result.ComposeFiles[0]
	}
	if result.IgnoreFile == "" && result.LikelyDeployMode == "files" {
		result.Warnings = append(result.Warnings, "No .deployignore or .dockerignore file was found for files mode packaging.")
	}
	return result
}

func detectPackageManagers(repoPath string) []string {
	checks := []struct {
		file string
		name string
	}{
		{"package-lock.json", "npm"},
		{"yarn.lock", "yarn"},
		{"pnpm-lock.yaml", "pnpm"},
		{"go.mod", "go"},
		{"Cargo.toml", "cargo"},
		{"requirements.txt", "pip"},
		{"pyproject.toml", "python"},
		{"Gemfile", "bundler"},
		{"composer.json", "composer"},
	}
	var managers []string
	for _, check := range checks {
		if repoFileExists(repoPath, check.file) {
			managers = append(managers, check.name)
		}
	}
	return managers
}

func existingRepoFiles(repoPath string, files []string) []string {
	var existing []string
	for _, file := range files {
		if repoFileExists(repoPath, file) {
			existing = append(existing, file)
		}
	}
	return existing
}

func firstExistingRepoFile(repoPath string, files []string) string {
	for _, file := range files {
		if repoFileExists(repoPath, file) {
			return file
		}
	}
	return ""
}

func repoFileExists(repoPath, name string) bool {
	info, err := os.Stat(filepath.Join(repoPath, name))
	return err == nil && !info.IsDir()
}
