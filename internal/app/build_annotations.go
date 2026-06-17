package app

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

type buildAnnotationRequest struct {
	Note string `json:"note"`
}

func handleAPIBuildAnnotations(w http.ResponseWriter, r *http.Request, buildID int64, path string) {
	if _, err := getBuild(buildID); err != nil {
		jsonErrorCode(w, errCodeBuildNotFound, "build not found", http.StatusNotFound)
		return
	}

	if path == "annotations" {
		switch r.Method {
		case http.MethodGet:
			annotations, err := listBuildAnnotations(buildID)
			if err != nil {
				jsonError(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if annotations == nil {
				annotations = []BuildAnnotation{}
			}
			jsonResponse(w, annotations)
		case http.MethodPost:
			var payload buildAnnotationRequest
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				jsonErrorCode(w, errCodeValidation, "invalid JSON: "+err.Error(), http.StatusBadRequest)
				return
			}
			annotation, err := createBuildAnnotation(buildID, payload.Note)
			if err != nil {
				jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
			jsonResponse(w, annotation)
		default:
			jsonMethodNotAllowed(w, http.MethodGet, http.MethodPost)
		}
		return
	}

	annotationIDText := strings.TrimPrefix(path, "annotations/")
	annotationID, err := strconv.ParseInt(annotationIDText, 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if !requireMethod(w, r, http.MethodDelete) {
		return
	}
	ok, err := deleteBuildAnnotation(buildID, annotationID)
	if err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !ok {
		jsonErrorCode(w, errCodeNotFound, "annotation not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
