package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleAPIBuildAnnotationsCreateListDelete(t *testing.T) {
	withTempDB(t)

	project := &Project{
		Name:       "Dashboard",
		RepoPath:   "/srv/repos/dashboard",
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "files",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	build, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create build: %v", err)
	}

	createReq := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/builds/%d/annotations", build.ID), strings.NewReader(`{"note":"deployed after dependency update"}`))
	createRec := httptest.NewRecorder()
	handleAPIBuild(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", createRec.Code, createRec.Body.String())
	}

	var created BuildAnnotation
	if err := json.NewDecoder(createRec.Body).Decode(&created); err != nil {
		t.Fatalf("decode created annotation: %v", err)
	}
	if created.BuildID != build.ID || created.Note != "deployed after dependency update" {
		t.Fatalf("unexpected created annotation: %+v", created)
	}

	listReq := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/builds/%d/annotations", build.ID), nil)
	listRec := httptest.NewRecorder()
	handleAPIBuild(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d body=%s", listRec.Code, listRec.Body.String())
	}
	var annotations []BuildAnnotation
	if err := json.NewDecoder(listRec.Body).Decode(&annotations); err != nil {
		t.Fatalf("decode annotations: %v", err)
	}
	if len(annotations) != 1 || annotations[0].ID != created.ID {
		t.Fatalf("unexpected annotations: %+v", annotations)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/builds/%d/annotations/%d", build.ID, created.ID), nil)
	deleteRec := httptest.NewRecorder()
	handleAPIBuild(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d body=%s", deleteRec.Code, deleteRec.Body.String())
	}
}

func TestHandleAPIBuildAnnotationsRejectsBlankNote(t *testing.T) {
	withTempDB(t)

	project := &Project{
		Name:       "Dashboard",
		RepoPath:   "/srv/repos/dashboard",
		DeployDir:  "/srv/apps/dashboard",
		DeployMode: "files",
	}
	if err := createProject(project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	build, err := createBuild(project.ID, "manual")
	if err != nil {
		t.Fatalf("create build: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/builds/%d/annotations", build.ID), strings.NewReader(`{"note":" "}`))
	rec := httptest.NewRecorder()
	handleAPIBuild(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var errorBody apiErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&errorBody); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if errorBody.Code != errCodeValidation {
		t.Fatalf("expected validation code, got %+v", errorBody)
	}
}
