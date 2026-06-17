package app

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"
)

const projectExportVersion = 1

type projectExportEnvelope struct {
	Kind          string              `json:"kind"`
	Version       int                 `json:"version"`
	ExportedAt    time.Time           `json:"exported_at"`
	Project       projectExportConfig `json:"project"`
	OmittedFields []string            `json:"omitted_fields,omitempty"`
}

type projectExportConfig struct {
	Name               string            `json:"name"`
	RepoPath           string            `json:"repo_path"`
	DockerfilePath     string            `json:"dockerfile_path,omitempty"`
	ComposeFile        string            `json:"compose_file,omitempty"`
	ImageName          string            `json:"image_name,omitempty"`
	SSHHost            string            `json:"ssh_host,omitempty"`
	DeployDir          string            `json:"deploy_dir"`
	HealthURL          string            `json:"health_url,omitempty"`
	HealthContainer    string            `json:"health_container,omitempty"`
	BuildArgs          map[string]string `json:"build_args,omitempty"`
	ComposeServices    string            `json:"compose_services,omitempty"`
	GitPullBeforeBuild bool              `json:"git_pull_before_build"`
	DeployMode         string            `json:"deploy_mode,omitempty"`
	Permissions        string            `json:"permissions,omitempty"`
	Preserve           string            `json:"preserve,omitempty"`
}

func handleAPIProjectExport(w http.ResponseWriter, r *http.Request, projectID int64) {
	export, err := newProjectExport(projectID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			jsonErrorCode(w, errCodeProjectNotFound, "project not found", http.StatusNotFound)
			return
		}
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, export)
}

func handleAPIProjectImport(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	project, err := decodeProjectImport(r)
	if err != nil {
		jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
		return
	}
	if err := createProject(project); err != nil {
		jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
	jsonResponse(w, project)
}

func newProjectExport(projectID int64) (*projectExportEnvelope, error) {
	project, err := getProject(projectID)
	if err != nil {
		return nil, err
	}
	config, omitted := exportProjectConfig(project)
	return &projectExportEnvelope{
		Kind:          "deployer.project",
		Version:       projectExportVersion,
		ExportedAt:    time.Now().UTC(),
		Project:       config,
		OmittedFields: omitted,
	}, nil
}

func exportProjectConfig(project *Project) (projectExportConfig, []string) {
	buildArgs, omitted := exportBuildArgs(project.BuildArgs)
	if project.RunnerID > 0 {
		omitted = append(omitted, "runner_id")
	}
	if strings.TrimSpace(project.PostDeploy) != "" {
		omitted = append(omitted, "post_deploy")
	}
	sort.Strings(omitted)

	return projectExportConfig{
		Name:               project.Name,
		RepoPath:           project.RepoPath,
		DockerfilePath:     project.DockerfilePath,
		ComposeFile:        project.ComposeFile,
		ImageName:          project.ImageName,
		SSHHost:            project.SSHHost,
		DeployDir:          project.DeployDir,
		HealthURL:          project.HealthURL,
		HealthContainer:    project.HealthContainer,
		BuildArgs:          buildArgs,
		ComposeServices:    project.ComposeServices,
		GitPullBeforeBuild: project.GitPullBeforeBuild,
		DeployMode:         project.DeployMode,
		Permissions:        project.Permissions,
		Preserve:           project.Preserve,
	}, omitted
}

func exportBuildArgs(buildArgs map[string]string) (map[string]string, []string) {
	if len(buildArgs) == 0 {
		return nil, nil
	}
	exported := make(map[string]string, len(buildArgs))
	omitted := []string{}
	for key, value := range buildArgs {
		if isSensitiveExportValue(key, value) {
			omitted = append(omitted, "build_args."+key)
			continue
		}
		exported[key] = value
	}
	if len(exported) == 0 {
		exported = nil
	}
	return exported, omitted
}

func isSensitiveExportValue(key, value string) bool {
	normalizedKey := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "-", "_"), ".", "_"))
	for _, marker := range []string{"secret", "token", "password", "passwd", "private_key", "credential", "api_key", "apikey", "access_key", "auth", "bearer"} {
		if strings.Contains(normalizedKey, marker) {
			return true
		}
	}
	return redactSecrets(value) != value
}

func decodeProjectImport(r *http.Request) (*Project, error) {
	var envelope projectExportEnvelope
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&envelope); err != nil {
		return nil, err
	}
	if envelope.Kind != "" && envelope.Kind != "deployer.project" {
		return nil, errors.New("kind must be deployer.project")
	}
	if envelope.Version != projectExportVersion {
		return nil, errors.New("unsupported project export version")
	}
	return envelope.Project.toProject(), nil
}

func (config projectExportConfig) toProject() *Project {
	buildArgs := config.BuildArgs
	if buildArgs == nil {
		buildArgs = map[string]string{}
	}
	return &Project{
		Name:               config.Name,
		RepoPath:           config.RepoPath,
		DockerfilePath:     config.DockerfilePath,
		ComposeFile:        config.ComposeFile,
		ImageName:          config.ImageName,
		SSHHost:            config.SSHHost,
		DeployDir:          config.DeployDir,
		HealthURL:          config.HealthURL,
		HealthContainer:    config.HealthContainer,
		BuildArgs:          buildArgs,
		ComposeServices:    config.ComposeServices,
		GitPullBeforeBuild: config.GitPullBeforeBuild,
		DeployMode:         config.DeployMode,
		Permissions:        config.Permissions,
		Preserve:           config.Preserve,
	}
}
