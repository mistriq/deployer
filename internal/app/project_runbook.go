package app

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"
)

type projectRunbookResponse struct {
	ProjectID        int64           `json:"project_id"`
	ProjectName      string          `json:"project_name"`
	DeployMode       string          `json:"deploy_mode"`
	HowToDeploy      []string        `json:"how_to_deploy"`
	ImportantFiles   []string        `json:"important_files"`
	Runner           *runnerSummary  `json:"runner,omitempty"`
	Rollback         runbookRollback `json:"rollback"`
	FailureRecovery  []string        `json:"failure_recovery"`
	OperationalNotes []string        `json:"operational_notes"`
}

type runbookRollback struct {
	Supported bool     `json:"supported"`
	Notes     []string `json:"notes"`
}

func handleAPIProjectRunbook(w http.ResponseWriter, r *http.Request, projectID int64) {
	runbook, err := newProjectRunbook(projectID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			jsonErrorCode(w, errCodeProjectNotFound, "project not found", http.StatusNotFound)
			return
		}
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, runbook)
}

func newProjectRunbook(projectID int64) (*projectRunbookResponse, error) {
	project, err := getProject(projectID)
	if err != nil {
		return nil, err
	}
	preview, err := newDeployPreview(projectID)
	if err != nil {
		return nil, err
	}
	summary, err := newProjectSummary(projectID)
	if err != nil {
		return nil, err
	}

	runbook := &projectRunbookResponse{
		ProjectID:        project.ID,
		ProjectName:      project.Name,
		DeployMode:       project.DeployMode,
		HowToDeploy:      runbookDeploySteps(project, preview),
		ImportantFiles:   runbookImportantFiles(project),
		Runner:           summary.Runner,
		Rollback:         runbookRollbackPlan(project),
		FailureRecovery:  runbookFailureRecovery(project, summary),
		OperationalNotes: runbookOperationalNotes(project, preview, summary),
	}
	return runbook, nil
}

func runbookDeploySteps(project *Project, preview *deployPreviewResponse) []string {
	steps := []string{
		"Review the deploy preview for the current commit and planned artifact.",
	}
	if project.GitPullBeforeBuild {
		steps = append(steps, "The server will run git pull before reading the commit SHA.")
	}
	switch project.DeployMode {
	case "files":
		steps = append(steps, "The server packages repository files into a tar.gz archive.")
		if preview.Transport == "agent" {
			steps = append(steps, "The assigned runner downloads the archive, extracts it into deploy_dir, restores preserve paths, applies permissions, and runs post_deploy if configured.")
		}
	default:
		steps = append(steps, "The server builds the Docker image using the configured Dockerfile and build args.")
		if preview.Transport == "agent" {
			steps = append(steps, "The server saves the Docker image as a tar artifact and waits for the runner to load it and restart Docker Compose services.")
		} else if preview.Transport == "ssh" {
			steps = append(steps, "The server saves the Docker image, uploads it over SCP, then runs the remote Docker Compose deploy script over SSH.")
		} else {
			steps = append(steps, "The server restarts Docker Compose services locally after the image build.")
		}
	}
	if project.HealthURL != "" || project.HealthContainer != "" {
		steps = append(steps, "Health checks run after non-agent deploys and runner-side after agent deploys.")
	}
	steps = append(steps, "Watch the build log stream or structured build events until the build reaches a terminal status.")
	return steps
}

func runbookImportantFiles(project *Project) []string {
	files := []string{project.RepoPath}
	if project.DeployMode == "docker" {
		files = append(files, project.DockerfilePath, project.ComposeFile)
	}
	if project.DeployMode == "files" {
		files = append(files, ".deployignore or .dockerignore", project.DeployDir)
	}
	for _, preserve := range strings.Split(project.Preserve, "\n") {
		preserve = strings.TrimSpace(preserve)
		if preserve != "" {
			files = append(files, "preserve:"+preserve)
		}
	}
	if strings.TrimSpace(project.Permissions) != "" {
		files = append(files, "permissions configuration")
	}
	return dedupeStrings(files)
}

func runbookRollbackPlan(project *Project) runbookRollback {
	notes := []string{}
	if project.DeployMode == "files" {
		notes = append(notes, "Automatic file rollback is not implemented; take a snapshot before risky deploys and restore files manually if needed.")
	}
	if project.DeployMode == "docker" {
		notes = append(notes, "Automatic Docker rollback is not implemented; redeploy a known-good commit or retag/restart a known-good image manually.")
	}
	if project.RunnerID > 0 {
		notes = append(notes, "Runner jobs can fetch remote snapshots, but snapshot restore is not automated yet.")
	}
	return runbookRollback{Supported: false, Notes: notes}
}

func runbookFailureRecovery(project *Project, summary *projectSummaryResponse) []string {
	steps := []string{
		"Open the latest failed build and inspect the failure summary plus structured build events.",
		"Use the relevant log lines to identify the first failing step before rerunning the deploy.",
	}
	if summary.Runner != nil && summary.Runner.Status == "offline" {
		steps = append(steps, "The runner is offline; check the agent service, network path, and runner token.")
	}
	if project.HealthURL != "" {
		steps = append(steps, "If health_url failed, verify the endpoint from the target host and inspect service logs.")
	}
	if project.HealthContainer != "" {
		steps = append(steps, "If health_container failed, inspect Docker health status and container logs on the target host.")
	}
	if project.DeployMode == "files" {
		steps = append(steps, "For files deploy failures, check archive contents, preserve paths, post_deploy, ownership, and permissions.")
	} else {
		steps = append(steps, "For Docker deploy failures, check Docker build output, compose service names, image name, and compose logs.")
	}
	return steps
}

func runbookOperationalNotes(project *Project, preview *deployPreviewResponse, summary *projectSummaryResponse) []string {
	notes := []string{
		"Deploy preview transport: " + preview.Transport,
		"Expected artifact type: " + preview.ExpectedArtifact.Type,
	}
	if summary.LastBuild != nil {
		notes = append(notes, "Last build status: "+summary.LastBuild.Status)
	}
	if len(preview.Warnings) > 0 {
		notes = append(notes, preview.Warnings...)
	}
	if project.PostDeploy != "" {
		notes = append(notes, "post_deploy is privileged code and should be treated as trusted admin configuration.")
	}
	return dedupeStrings(notes)
}

func dedupeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
