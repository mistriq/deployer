package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeProjectRequestAcceptsValidJSONFields(t *testing.T) {
	req := httptest.NewRequest("POST", "/api/projects", strings.NewReader(`{
		"name": "Dashboard",
		"repo_path": "/srv/repos/dashboard",
		"deploy_dir": "/srv/apps/dashboard",
		"deploy_mode": "files",
		"build_args": {"APP_ENV": "production"},
		"permissions": "{\"owner\":\"www-data:www-data\"}"
	}`))

	project, err := decodeProjectRequest(req)
	if err != nil {
		t.Fatalf("decode project request: %v", err)
	}
	if project.BuildArgs["APP_ENV"] != "production" {
		t.Fatalf("expected build args to decode, got %#v", project.BuildArgs)
	}
	if project.Permissions != `{"owner":"www-data:www-data"}` {
		t.Fatalf("expected permissions string to decode, got %q", project.Permissions)
	}
}

func TestDecodeProjectRequestRejectsNonObjectBuildArgs(t *testing.T) {
	req := httptest.NewRequest("POST", "/api/projects", strings.NewReader(`{
		"name": "Dashboard",
		"repo_path": "/srv/repos/dashboard",
		"deploy_dir": "/srv/apps/dashboard",
		"deploy_mode": "files",
		"build_args": ["APP_ENV=production"]
	}`))

	_, err := decodeProjectRequest(req)
	if err == nil || !strings.Contains(err.Error(), "build_args must be a JSON object with string values") {
		t.Fatalf("expected build_args object error, got %v", err)
	}
}

func TestDecodeProjectRequestRejectsNonStringBuildArgValue(t *testing.T) {
	req := httptest.NewRequest("POST", "/api/projects", strings.NewReader(`{
		"name": "Dashboard",
		"repo_path": "/srv/repos/dashboard",
		"deploy_dir": "/srv/apps/dashboard",
		"deploy_mode": "files",
		"build_args": {"PORT": 3000}
	}`))

	_, err := decodeProjectRequest(req)
	if err == nil || !strings.Contains(err.Error(), "build_args.PORT must be a string value") {
		t.Fatalf("expected build_args value error, got %v", err)
	}
}

func TestDecodeProjectRequestRejectsNonStringPermissions(t *testing.T) {
	req := httptest.NewRequest("POST", "/api/projects", strings.NewReader(`{
		"name": "Dashboard",
		"repo_path": "/srv/repos/dashboard",
		"deploy_dir": "/srv/apps/dashboard",
		"deploy_mode": "files",
		"permissions": {"owner": "www-data"}
	}`))

	_, err := decodeProjectRequest(req)
	if err == nil || !strings.Contains(err.Error(), "permissions must be a JSON string containing the permissions object") {
		t.Fatalf("expected permissions string error, got %v", err)
	}
}

func TestCompactPath(t *testing.T) {
	cases := map[string]string{
		"/srv/repos/dashboard":       "dashboard",
		"/srv/repos/dashboard/":      "dashboard",
		`C:\Users\deploy\dashboard`:  "dashboard",
		`C:\Users\deploy\dashboard\`: "dashboard",
		"dashboard":                  "dashboard",
		" ":                          "",
	}
	for input, want := range cases {
		if got := compactPath(input); got != want {
			t.Fatalf("compactPath(%q) = %q, want %q", input, got, want)
		}
	}
}
