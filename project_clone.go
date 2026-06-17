package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

type projectCloneRequest struct {
	Name string `json:"name"`
}

func handleAPIProjectClone(w http.ResponseWriter, r *http.Request, projectID int64) {
	cloneName, err := decodeProjectCloneRequest(r)
	if err != nil {
		jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
		return
	}
	project, err := cloneProject(projectID, cloneName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			jsonErrorCode(w, errCodeProjectNotFound, "project not found", http.StatusNotFound)
			return
		}
		jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
	jsonResponse(w, project)
}

func decodeProjectCloneRequest(r *http.Request) (string, error) {
	if r.Body == nil {
		return "", nil
	}
	defer r.Body.Close()
	var payload projectCloneRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		if errors.Is(err, io.EOF) {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(payload.Name), nil
}

func cloneProject(projectID int64, cloneName string) (*Project, error) {
	source, err := getProject(projectID)
	if err != nil {
		return nil, err
	}
	clone := copyProjectConfig(source)
	if cloneName == "" {
		cloneName = source.Name + " Copy"
	}
	clone.Name = cloneName
	if err := createProject(clone); err != nil {
		return nil, err
	}
	return clone, nil
}

func copyProjectConfig(source *Project) *Project {
	buildArgs := make(map[string]string, len(source.BuildArgs))
	for key, value := range source.BuildArgs {
		buildArgs[key] = value
	}
	return &Project{
		Name:               source.Name,
		RepoPath:           source.RepoPath,
		DockerfilePath:     source.DockerfilePath,
		ComposeFile:        source.ComposeFile,
		ImageName:          source.ImageName,
		SSHHost:            source.SSHHost,
		DeployDir:          source.DeployDir,
		HealthURL:          source.HealthURL,
		HealthContainer:    source.HealthContainer,
		BuildArgs:          buildArgs,
		ComposeServices:    source.ComposeServices,
		GitPullBeforeBuild: source.GitPullBeforeBuild,
		RunnerID:           source.RunnerID,
		DeployMode:         source.DeployMode,
		PostDeploy:         source.PostDeploy,
		Permissions:        source.Permissions,
		Preserve:           source.Preserve,
	}
}
