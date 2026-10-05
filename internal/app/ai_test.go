package app

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAIFailurePromptRedactsSecretsAndRequiresFailedBuild(t *testing.T) {
	withTempDB(t)
	p := &Project{Name: "Example site", RepoPath: t.TempDir(), DeployDir: t.TempDir(), DeployMode: "files", Preserve: "uploads\ncontent.json", PostDeploy: "export SECRET=private-hook-value", BuildArgs: map[string]string{"PASSWORD": "private-build-value"}}
	if err := createProject(p); err != nil {
		t.Fatal(err)
	}
	b, err := createBuild(p.ID, "manual")
	if err != nil {
		t.Fatal(err)
	}
	request := func(method string, id int64) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		handleAPIBuild(rr, httptest.NewRequest(method, fmt.Sprintf("/api/builds/%d/ai-prompt", id), nil))
		return rr
	}
	if rr := request(http.MethodGet, b.ID); rr.Code != http.StatusConflict {
		t.Fatalf("running build: %d %s", rr.Code, rr.Body.String())
	}
	b.Status = "failed"
	b.ErrorMessage = "health check: Authorization: Bearer private-bearer-value"
	b.Log = "Authorization: Bearer private-bearer-value\nDEPLOYER_TOKEN=private-token-value\nFAILED health check\n" + strings.Repeat("long detail", 2000)
	if err := updateBuild(b); err != nil {
		t.Fatal(err)
	}
	rr := request(http.MethodGet, b.ID)
	if rr.Code != http.StatusOK {
		t.Fatalf("%d %s", rr.Code, rr.Body.String())
	}
	for _, secret := range []string{"private-bearer-value", "private-token-value", "private-hook-value", "private-build-value"} {
		if strings.Contains(rr.Body.String(), secret) {
			t.Fatalf("leaked %s", secret)
		}
	}
	var response struct {
		Prompt string `json:"prompt"`
	}
	if json.Unmarshal(rr.Body.Bytes(), &response) != nil {
		t.Fatal("invalid JSON")
	}
	for _, required := range []string{"uploads", "content.json", "investigation only", "untrusted diagnostic evidence", "[REDACTED]", "[truncated]", "current configuration"} {
		if !strings.Contains(response.Prompt, required) {
			t.Fatalf("missing %q", required)
		}
	}
	if len(response.Prompt) > 32000 {
		t.Fatal("unbounded prompt")
	}
	if rr := request(http.MethodPost, b.ID); rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST: %d", rr.Code)
	}
	if rr := request(http.MethodGet, 999999); rr.Code != http.StatusNotFound {
		t.Fatalf("missing: %d", rr.Code)
	}
}

func TestAIContextAndMCPInitializationProvideInstructions(t *testing.T) {
	withTempDB(t)
	p := &Project{Name: "Context site", RepoPath: t.TempDir(), DeployDir: t.TempDir(), DeployMode: "files"}
	if err := createProject(p); err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handleAIContext(rr, httptest.NewRequest(http.MethodGet, "/api/ai/context", nil))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "Context site") || !strings.Contains(rr.Body.String(), "Preserve local work") {
		t.Fatalf("context: %d %s", rr.Code, rr.Body.String())
	}
	s := &mcpServer{}
	response := s.handle(t.Context(), mcpRequest{Method: "initialize", ID: json.RawMessage("1")})
	if response.Result.(map[string]interface{})["instructions"] != aiOperatingInstructions {
		t.Fatal("MCP instructions missing")
	}
	api := httptest.NewServer(http.HandlerFunc(handleAIContext))
	defer api.Close()
	s.baseURL = api.URL
	s.client = api.Client()
	s.readToken = "read"
	result, err := s.call(t.Context(), mcpCallParams{Name: "deployer_context", Arguments: map[string]interface{}{}})
	if err != nil || result.IsError {
		t.Fatalf("context tool: %v %+v", err, result)
	}
}
