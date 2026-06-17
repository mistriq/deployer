package app

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"
)

type deployPreviewResponse struct {
	ProjectID        int64                 `json:"project_id"`
	ProjectName      string                `json:"project_name"`
	RepoPath         string                `json:"repo_path"`
	CommitSHA        string                `json:"commit_sha,omitempty"`
	CommitError      string                `json:"commit_error,omitempty"`
	DeployMode       string                `json:"deploy_mode"`
	Transport        string                `json:"transport"`
	Runner           *runnerSummary        `json:"runner,omitempty"`
	DeployDir        string                `json:"deploy_dir"`
	Hooks            deployPreviewHooks    `json:"hooks"`
	HealthCheck      deployPreviewHealth   `json:"health_check"`
	ExpectedArtifact deployPreviewArtifact `json:"expected_artifact"`
	PlannedSteps     []string              `json:"planned_steps"`
	Warnings         []string              `json:"warnings"`
}

type deployPreviewHooks struct {
	GitPullBeforeBuild bool   `json:"git_pull_before_build"`
	PostDeploy         string `json:"post_deploy,omitempty"`
	PostDeployEnabled  bool   `json:"post_deploy_enabled"`
}

type deployPreviewHealth struct {
	Enabled   bool   `json:"enabled"`
	URL       string `json:"url,omitempty"`
	Container string `json:"container,omitempty"`
	Timeout   string `json:"timeout"`
}

type deployPreviewArtifact struct {
	Type          string `json:"type"`
	Extension     string `json:"extension,omitempty"`
	ServerManaged bool   `json:"server_managed"`
}

func handleAPIProjectPreview(w http.ResponseWriter, r *http.Request, projectID int64) {
	preview, err := newDeployPreview(projectID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			jsonErrorCode(w, errCodeProjectNotFound, "project not found", http.StatusNotFound)
			return
		}
		jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
		return
	}
	jsonResponse(w, preview)
}

func newDeployPreview(projectID int64) (*deployPreviewResponse, error) {
	project, err := getProject(projectID)
	if err != nil {
		return nil, err
	}
	if err := validateProject(project); err != nil {
		return nil, err
	}

	transport := deployTransport(project)
	commitSHA, commitErr := previewCommitSHA(project.RepoPath)
	preview := &deployPreviewResponse{
		ProjectID:   project.ID,
		ProjectName: project.Name,
		RepoPath:    project.RepoPath,
		CommitSHA:   commitSHA,
		DeployMode:  project.DeployMode,
		Transport:   transport,
		DeployDir:   project.DeployDir,
		Hooks: deployPreviewHooks{
			GitPullBeforeBuild: project.GitPullBeforeBuild,
			PostDeploy:         project.PostDeploy,
			PostDeployEnabled:  strings.TrimSpace(project.PostDeploy) != "",
		},
		HealthCheck: deployPreviewHealth{
			Enabled:   project.HealthURL != "" || project.HealthContainer != "",
			URL:       project.HealthURL,
			Container: project.HealthContainer,
			Timeout:   appConfig.HealthCheckTimeout.String(),
		},
		ExpectedArtifact: previewArtifact(project, transport),
		PlannedSteps:     previewSteps(project, transport),
		Warnings:         previewWarnings(project, transport),
	}
	if commitErr != nil {
		preview.CommitError = commitErr.Error()
	}

	if project.RunnerID > 0 {
		runner, err := getRunner(project.RunnerID)
		if err == nil {
			runner.Token = ""
			preview.Runner = summarizeRunner(*runner)
			if runner.Status == "offline" {
				preview.Warnings = append(preview.Warnings, "Runner is offline; the job will wait until the agent polls.")
			}
		} else if errors.Is(err, sql.ErrNoRows) {
			preview.Warnings = append(preview.Warnings, "Configured runner does not exist.")
		} else {
			return nil, err
		}
	}
	return preview, nil
}

func deployTransport(project *Project) string {
	if project.RunnerID > 0 {
		return "agent"
	}
	if project.SSHHost != "" && project.SSHHost != "localhost" {
		return "ssh"
	}
	return "local"
}

func previewCommitSHA(repoPath string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := systemCommands.CombinedOutput(ctx, "", "git", "-C", repoPath, "rev-parse", "--short", "HEAD")
	if err != nil {
		output := strings.TrimSpace(string(out))
		if output != "" {
			return "", errors.New(output)
		}
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func previewArtifact(project *Project, transport string) deployPreviewArtifact {
	if project.DeployMode == "files" {
		return deployPreviewArtifact{Type: "files_archive", Extension: ".tar.gz", ServerManaged: true}
	}
	if transport == "agent" || transport == "ssh" {
		return deployPreviewArtifact{Type: "docker_image_tar", Extension: ".tar", ServerManaged: true}
	}
	return deployPreviewArtifact{Type: "none", ServerManaged: false}
}

func previewSteps(project *Project, transport string) []string {
	steps := []string{"git_pull_skipped"}
	if project.GitPullBeforeBuild {
		steps[0] = "git_pull"
	}
	steps = append(steps, "get_commit_sha")
	if project.DeployMode == "files" {
		steps = append(steps, "package_files")
	} else {
		steps = append(steps, "docker_build")
	}

	switch transport {
	case "agent":
		if project.DeployMode == "docker" {
			steps = append(steps, "docker_save")
		}
		steps = append(steps, "create_agent_job", "wait_for_agent")
	case "ssh":
		if project.DeployMode == "docker" {
			steps = append(steps, "docker_save", "scp_artifact", "ssh_docker_compose_deploy")
		}
	case "local":
		steps = append(steps, "docker_compose_down", "docker_compose_up")
	}

	if transport != "agent" && (project.HealthURL != "" || project.HealthContainer != "") {
		steps = append(steps, "health_check")
	}
	return steps
}

func previewWarnings(project *Project, transport string) []string {
	var warnings []string
	if project.DeployMode == "files" && transport != "agent" {
		warnings = append(warnings, "Files mode should use an agent runner so files can be extracted on the target host.")
	}
	if project.PostDeploy != "" && transport != "agent" {
		warnings = append(warnings, "post_deploy is executed by agent jobs; this preview transport will not run it.")
	}
	if project.HealthURL == "" && project.HealthContainer == "" {
		warnings = append(warnings, "No health check is configured.")
	}
	return warnings
}
