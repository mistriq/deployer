package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestJSONErrorIncludesStableCode(t *testing.T) {
	rr := httptest.NewRecorder()
	jsonErrorCode(rr, errCodeProjectBusy, "project Dashboard is already building", http.StatusConflict)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d", rr.Code)
	}

	var got apiErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if got.Error != "project Dashboard is already building" || got.Code != errCodeProjectBusy {
		t.Fatalf("unexpected error response: %+v", got)
	}
}

func TestJSONErrorDefaultCodeIsStable(t *testing.T) {
	rr := httptest.NewRecorder()
	jsonError(rr, "invalid input", http.StatusBadRequest)

	var got apiErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if got.Code != errCodeValidation {
		t.Fatalf("expected validation code, got %q", got.Code)
	}
}

func TestRequireMethodReturnsJSONMethodError(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/capabilities", nil)
	rr := httptest.NewRecorder()

	if requireMethod(rr, req, http.MethodGet) {
		t.Fatal("expected method to be rejected")
	}
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", rr.Code)
	}
	if rr.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("unexpected Allow header %q", rr.Header().Get("Allow"))
	}
	var got apiErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if got.Code != errCodeMethodNotAllowed {
		t.Fatalf("expected method_not_allowed code, got %+v", got)
	}
}

func TestParseIDPathRejectsAmbiguousPaths(t *testing.T) {
	id, suffix, ok := parseIDPath("/api/projects/42/summary", "/api/projects/")
	if !ok || id != 42 || suffix != "summary" {
		t.Fatalf("unexpected parsed path id=%d suffix=%q ok=%v", id, suffix, ok)
	}

	for _, path := range []string{
		"/api/projects/",
		"/api/projects//42",
		"/api/projects/abc",
		"/api/projects/42/",
		"/other/42",
	} {
		if _, _, ok := parseIDPath(path, "/api/projects/"); ok {
			t.Fatalf("expected path %q to be rejected", path)
		}
	}
}
