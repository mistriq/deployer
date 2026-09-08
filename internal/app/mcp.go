package app

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const mcpProtocolVersion = "2025-06-18"

type mcpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type mcpResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *mcpError       `json:"error,omitempty"`
}

type mcpError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}
type mcpTool struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	InputSchema map[string]interface{} `json:"inputSchema"`
}
type mcpCallParams struct {
	Name      string                 `json:"name"`
	Arguments map[string]interface{} `json:"arguments"`
}
type mcpContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}
type mcpCallResult struct {
	Content           []mcpContent `json:"content"`
	StructuredContent interface{}  `json:"structuredContent,omitempty"`
	IsError           bool         `json:"isError,omitempty"`
}

type mcpServer struct {
	baseURL, readToken, writeToken string
	client                         *http.Client
}

// RunMCP serves the Model Context Protocol over newline-delimited stdio.
func RunMCP(ctx context.Context, in io.Reader, out io.Writer) error {
	base := strings.TrimRight(strings.TrimSpace(os.Getenv("DEPLOYER_MCP_URL")), "/")
	readToken := strings.TrimSpace(os.Getenv("DEPLOYER_MCP_READ_TOKEN"))
	if base == "" || readToken == "" {
		return errors.New("DEPLOYER_MCP_URL and DEPLOYER_MCP_READ_TOKEN are required")
	}
	s := &mcpServer{baseURL: base, readToken: readToken, writeToken: strings.TrimSpace(os.Getenv("DEPLOYER_MCP_WRITE_TOKEN")), client: &http.Client{Timeout: 30 * time.Second}}
	return s.serve(ctx, in, out)
}

func (s *mcpServer) serve(ctx context.Context, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	enc := json.NewEncoder(out)
	for scanner.Scan() {
		var req mcpRequest
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			_ = enc.Encode(mcpResponse{JSONRPC: "2.0", Error: &mcpError{Code: -32700, Message: "parse error"}})
			continue
		}
		if len(req.ID) == 0 { // notifications have no response
			continue
		}
		resp := s.handle(ctx, req)
		if err := enc.Encode(resp); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (s *mcpServer) handle(ctx context.Context, req mcpRequest) mcpResponse {
	r := mcpResponse{JSONRPC: "2.0", ID: req.ID}
	switch req.Method {
	case "initialize":
		r.Result = map[string]interface{}{"protocolVersion": mcpProtocolVersion, "capabilities": map[string]interface{}{"tools": map[string]interface{}{"listChanged": false}}, "serverInfo": map[string]string{"name": "deployer", "version": buildVersion}}
	case "ping":
		r.Result = map[string]interface{}{}
	case "tools/list":
		r.Result = map[string]interface{}{"tools": s.tools()}
	case "tools/call":
		var p mcpCallParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			r.Error = &mcpError{Code: -32602, Message: "invalid tool parameters"}
			break
		}
		result, err := s.call(ctx, p)
		if err != nil {
			result = mcpCallResult{Content: []mcpContent{{Type: "text", Text: redactSecrets(err.Error())}}, IsError: true}
		}
		r.Result = result
	default:
		r.Error = &mcpError{Code: -32601, Message: "method not found"}
	}
	return r
}

func objectSchema(required []string, props map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{"type": "object", "additionalProperties": false, "properties": props, "required": required}
}
func intProp(description string) map[string]interface{} {
	return map[string]interface{}{"type": "integer", "minimum": 1, "description": description}
}

func (s *mcpServer) tools() []mcpTool {
	noArgs := objectSchema([]string{}, map[string]interface{}{})
	projectID := objectSchema([]string{"project_id"}, map[string]interface{}{"project_id": intProp("Project ID")})
	buildID := objectSchema([]string{"build_id"}, map[string]interface{}{"build_id": intProp("Build ID")})
	runnerID := objectSchema([]string{"runner_id"}, map[string]interface{}{"runner_id": intProp("Runner ID")})
	operationID := objectSchema([]string{"operation_id"}, map[string]interface{}{"operation_id": intProp("Operation ID")})
	tools := []mcpTool{
		{Name: "projects_list", Description: "List deployment projects and their latest builds.", InputSchema: noArgs},
		{Name: "runners_list", Description: "List deployment runners.", InputSchema: noArgs},
		{Name: "runner_detail", Description: "Read runner telemetry, active task, deployment queue, and recent operations.", InputSchema: runnerID},
		{Name: "operation_get", Description: "Read a runner operation and its bounded output.", InputSchema: operationID},
		{Name: "project_summary", Description: "Read a project operational summary.", InputSchema: projectID},
		{Name: "project_config", Description: "Read a project configuration with secret values redacted.", InputSchema: projectID},
		{Name: "builds_recent", Description: "Inspect recent project builds.", InputSchema: projectID},
		{Name: "build_get", Description: "Read build status and bounded recent log output; call again to follow progress.", InputSchema: objectSchema([]string{"build_id"}, map[string]interface{}{"build_id": intProp("Build ID"), "log_bytes": map[string]interface{}{"type": "integer", "minimum": 0, "maximum": 65536, "default": 16384}})},
		{Name: "build_events", Description: "Read structured events for a build.", InputSchema: buildID},
		{Name: "build_failure_summary", Description: "Diagnose a failed build.", InputSchema: buildID},
		{Name: "project_runbook", Description: "Generate the operational runbook for a project.", InputSchema: projectID},
		{Name: "deployment_preview", Description: "Preview source version, target, preserve paths, hooks, and health checks without deploying.", InputSchema: projectID},
	}
	if s.writeToken != "" {
		writeProject := objectSchema([]string{"project_id", "idempotency_key"}, map[string]interface{}{"project_id": intProp("Project ID"), "idempotency_key": map[string]interface{}{"type": "string", "minLength": 8, "maxLength": 200, "description": "Stable unique key reused for retries"}})
		tools = append(tools,
			mcpTool{Name: "deployment_trigger", Description: "Start a deployment asynchronously and return its build ID. Retries with the same key return the same build.", InputSchema: writeProject},
			mcpTool{Name: "build_cancel", Description: "Cancel a running build.", InputSchema: buildID},
			mcpTool{Name: "snapshot_request", Description: "Request a remote snapshot asynchronously and return its build ID.", InputSchema: writeProject},
			mcpTool{Name: "operation_queue", Description: "Queue an authorized fixed runner operation.", InputSchema: objectSchema([]string{"runner_id", "project_id", "kind"}, map[string]interface{}{"runner_id": intProp("Runner ID"), "project_id": intProp("Project ID"), "kind": map[string]interface{}{"type": "string", "enum": []string{"health_check", "compose_status", "compose_restart", "compose_stop", "compose_logs"}}})},
			mcpTool{Name: "operation_cancel", Description: "Cancel a pending runner operation.", InputSchema: operationID},
		)
	}
	return tools
}

func (s *mcpServer) call(ctx context.Context, p mcpCallParams) (mcpCallResult, error) {
	id := func(name string) (int64, error) {
		v, ok := p.Arguments[name].(float64)
		if !ok || v < 1 || v != float64(int64(v)) {
			return 0, fmt.Errorf("%s must be a positive integer", name)
		}
		return int64(v), nil
	}
	var method, path, token, requestBody string
	method, token = http.MethodGet, s.readToken
	switch p.Name {
	case "projects_list":
		path = "/api/projects"
	case "runners_list":
		path = "/api/runners"
	case "runner_detail":
		v, e := id("runner_id")
		if e != nil {
			return mcpCallResult{}, e
		}
		path = "/api/runners/" + strconv.FormatInt(v, 10)
	case "operation_get":
		v, e := id("operation_id")
		if e != nil {
			return mcpCallResult{}, e
		}
		path = "/api/runner-operations/" + strconv.FormatInt(v, 10)
	case "project_summary", "project_config", "builds_recent", "project_runbook", "deployment_preview":
		v, e := id("project_id")
		if e != nil {
			return mcpCallResult{}, e
		}
		suffix := map[string]string{"project_summary": "summary", "project_config": "", "builds_recent": "history", "project_runbook": "runbook", "deployment_preview": "preview"}[p.Name]
		path = "/api/projects/" + strconv.FormatInt(v, 10)
		if suffix != "" {
			path += "/" + suffix
		}
	case "build_get", "build_events", "build_failure_summary":
		v, e := id("build_id")
		if e != nil {
			return mcpCallResult{}, e
		}
		suffix := map[string]string{"build_get": "", "build_events": "events", "build_failure_summary": "failure-summary"}[p.Name]
		path = "/api/builds/" + strconv.FormatInt(v, 10)
		if suffix != "" {
			path += "/" + suffix
		}
	case "deployment_trigger", "snapshot_request":
		if s.writeToken == "" {
			return mcpCallResult{}, errors.New("write access is not configured")
		}
		v, e := id("project_id")
		if e != nil {
			return mcpCallResult{}, e
		}
		key, ok := p.Arguments["idempotency_key"].(string)
		if !ok || len(strings.TrimSpace(key)) < 8 || len(strings.TrimSpace(key)) > 200 {
			return mcpCallResult{}, errors.New("idempotency_key must contain 8 to 200 characters")
		}
		action := map[string]string{"deployment_trigger": "deploy", "snapshot_request": "snapshot"}[p.Name]
		path = "/api/projects/" + strconv.FormatInt(v, 10) + "/" + action
		method, token = http.MethodPost, s.writeToken
	case "build_cancel":
		if s.writeToken == "" {
			return mcpCallResult{}, errors.New("write access is not configured")
		}
		v, e := id("build_id")
		if e != nil {
			return mcpCallResult{}, e
		}
		path = "/api/builds/" + strconv.FormatInt(v, 10) + "/cancel"
		method, token = http.MethodPost, s.writeToken
	case "operation_queue":
		if s.writeToken == "" {
			return mcpCallResult{}, errors.New("write access is not configured")
		}
		runner, e := id("runner_id")
		if e != nil {
			return mcpCallResult{}, e
		}
		project, e := id("project_id")
		if e != nil {
			return mcpCallResult{}, e
		}
		kind, ok := p.Arguments["kind"].(string)
		if !ok || !validRunnerOperationKind(kind) {
			return mcpCallResult{}, errors.New("invalid operation kind")
		}
		path = "/api/runners/" + strconv.FormatInt(runner, 10) + "/operations"
		method, token = http.MethodPost, s.writeToken
		requestBody = fmt.Sprintf(`{"project_id":%d,"kind":%q}`, project, kind)
	case "operation_cancel":
		if s.writeToken == "" {
			return mcpCallResult{}, errors.New("write access is not configured")
		}
		v, e := id("operation_id")
		if e != nil {
			return mcpCallResult{}, e
		}
		path = "/api/runner-operations/" + strconv.FormatInt(v, 10) + "/cancel"
		method, token = http.MethodPost, s.writeToken
	default:
		return mcpCallResult{}, fmt.Errorf("unknown tool %q", p.Name)
	}
	headers := map[string]string{}
	if requestBody != "" {
		headers["Content-Type"] = "application/json"
	}
	if key, ok := p.Arguments["idempotency_key"].(string); ok {
		headers["Idempotency-Key"] = strings.TrimSpace(key)
	}
	raw, err := s.requestWithBody(ctx, method, path, token, headers, requestBody)
	if err != nil {
		return mcpCallResult{}, err
	}
	redacted := redactJSON(raw)
	if p.Name == "build_get" {
		limit := 16384
		if n, ok := p.Arguments["log_bytes"].(float64); ok {
			limit = int(n)
		}
		redacted = boundBuildLog(redacted, limit)
	}
	var structured interface{}
	if err := json.Unmarshal(redacted, &structured); err != nil {
		return mcpCallResult{}, err
	}
	structured = boundMCPResult(p.Name, structured)
	pretty, _ := json.MarshalIndent(structured, "", "  ")
	if list, ok := structured.([]interface{}); ok {
		structured = map[string]interface{}{"items": list}
		pretty, _ = json.MarshalIndent(structured, "", "  ")
	}
	return mcpCallResult{Content: []mcpContent{{Type: "text", Text: string(pretty)}}, StructuredContent: structured}, nil
}

func (s *mcpServer) request(ctx context.Context, method, path, token string, headers map[string]string) ([]byte, error) {
	return s.requestWithBody(ctx, method, path, token, headers, "")
}

func (s *mcpServer) requestWithBody(ctx context.Context, method, path, token string, headers map[string]string, body string) ([]byte, error) {
	base, err := url.Parse(s.baseURL)
	if err != nil {
		return nil, err
	}
	rel, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	endpoint := base.ResolveReference(rel)
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, (6<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(responseBody) > 6<<20 {
		return nil, errors.New("Deployer API response exceeded 6 MiB safety limit")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("Deployer API %s: %s", resp.Status, strings.TrimSpace(string(redactJSON(responseBody))))
	}
	return responseBody, nil
}

func boundMCPResult(tool string, value interface{}) interface{} {
	if list, ok := value.([]interface{}); ok {
		if len(list) > 100 {
			list = list[:100]
		}
		return list
	}
	object, ok := value.(map[string]interface{})
	if !ok {
		return value
	}
	if tool == "build_events" {
		if events, ok := object["events"].([]interface{}); ok {
			if len(events) > 200 {
				events = events[len(events)-200:]
				object["events_truncated"] = true
			}
			for _, rawEvent := range events {
				if event, ok := rawEvent.(map[string]interface{}); ok {
					for _, field := range []string{"message", "error"} {
						if text, ok := event[field].(string); ok && len([]byte(text)) > 4096 {
							event[field] = string([]byte(text)[:4096])
							event[field+"_truncated"] = true
						}
					}
				}
			}
			object["events"] = events
		}
	}
	return object
}

func boundBuildLog(raw []byte, limit int) []byte {
	var value map[string]interface{}
	if json.Unmarshal(raw, &value) != nil {
		return raw
	}
	logText, ok := value["log"].(string)
	if !ok {
		return raw
	}
	if limit < 0 {
		limit = 0
	}
	if limit > 65536 {
		limit = 65536
	}
	b := []byte(logText)
	if len(b) > limit {
		b = b[len(b)-limit:]
		value["log_truncated"] = true
	}
	value["log"] = string(b)
	out, _ := json.Marshal(value)
	return out
}

func redactJSON(raw []byte) []byte {
	var value interface{}
	if json.Unmarshal(raw, &value) != nil {
		return []byte(redactSecrets(string(raw)))
	}
	redactJSONValue(&value)
	out, err := json.Marshal(value)
	if err != nil {
		return []byte(`{"error":"response redaction failed"}`)
	}
	return out
}
func redactJSONValue(ptr *interface{}) {
	switch value := (*ptr).(type) {
	case string:
		*ptr = redactSecrets(value)
	case []interface{}:
		for i := range value {
			redactJSONValue(&value[i])
		}
	case map[string]interface{}:
		for key, item := range value {
			lower := strings.ToLower(key)
			if lower == "token" || strings.Contains(lower, "secret") || strings.Contains(lower, "password") || strings.Contains(lower, "api_key") {
				value[key] = "[REDACTED]"
				continue
			}
			if lower == "build_args" {
				if args, ok := item.(map[string]interface{}); ok {
					for k := range args {
						args[k] = "[REDACTED]"
					}
				}
				continue
			}
			if lower == "post_deploy" && strings.TrimSpace(fmt.Sprint(item)) != "" {
				value[key] = "[REDACTED]"
				continue
			}
			redactJSONValue(&item)
			value[key] = item
		}
	}
}

const mcpIdempotencySchema = `CREATE TABLE IF NOT EXISTS mcp_idempotency (
	action TEXT NOT NULL,
	project_id BIGINT NOT NULL,
	idempotency_key TEXT NOT NULL,
	build_id BIGINT NOT NULL DEFAULT 0,
	created_unix BIGINT NOT NULL,
	PRIMARY KEY (action, project_id, idempotency_key)
)`

var mcpIdempotencyMu sync.Mutex

func mcpIdempotentBuild(action string, project *Project, key string, start func(string) (int64, error)) (int64, error) {
	mcpIdempotencyMu.Lock()
	defer mcpIdempotencyMu.Unlock()
	key = strings.TrimSpace(key)
	if key == "" {
		return start(map[string]string{"deploy": "manual", "snapshot": "snapshot"}[action])
	}
	if len(key) < 8 || len(key) > 200 {
		return 0, errors.New("Idempotency-Key must contain 8 to 200 characters")
	}
	keyBytes := sha256.Sum256([]byte(key))
	key = fmt.Sprintf("%x", keyBytes[:])
	if _, err := db.Exec(mcpIdempotencySchema); err != nil {
		return 0, fmt.Errorf("initialize MCP idempotency: %w", err)
	}
	unlock, err := lockMCPIdempotency(action, project.ID, key)
	if err != nil {
		return 0, err
	}
	defer unlock()
	var buildID, createdUnix int64
	err = db.QueryRow(`SELECT build_id, created_unix FROM mcp_idempotency WHERE action=? AND project_id=? AND idempotency_key=?`, action, project.ID, key).Scan(&buildID, &createdUnix)
	if err == nil && buildID > 0 {
		return buildID, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	markerBytes := sha256.Sum256([]byte(action + "\x00" + strconv.FormatInt(project.ID, 10) + "\x00" + key))
	marker := "mcp:" + action + ":" + fmt.Sprintf("%x", markerBytes[:12])
	if buildID == 0 {
		// A prior process may have created the build but died before recording its ID.
		if findErr := db.QueryRow(`SELECT id FROM builds WHERE project_id=? AND triggered_by=? ORDER BY id DESC LIMIT 1`, project.ID, marker).Scan(&buildID); findErr == nil {
			_, _ = db.Exec(`UPDATE mcp_idempotency SET build_id=? WHERE action=? AND project_id=? AND idempotency_key=?`, buildID, action, project.ID, key)
			return buildID, nil
		}
		// Holding the per-key lock proves that no live request owns this empty
		// reservation. It is safe to reclaim immediately after a process crash.
		_, _ = db.Exec(`DELETE FROM mcp_idempotency WHERE action=? AND project_id=? AND idempotency_key=? AND build_id=0 AND created_unix=?`, action, project.ID, key, createdUnix)
	}
	result, err := db.Exec(`INSERT INTO mcp_idempotency (action,project_id,idempotency_key,build_id,created_unix) VALUES (?,?,?,0,?) ON CONFLICT (action,project_id,idempotency_key) DO NOTHING`, action, project.ID, key, time.Now().Unix())
	if err != nil {
		return 0, err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return 0, errors.New("an operation with this idempotency key is still starting; retry shortly")
	}
	buildID, err = start(marker)
	if err != nil {
		_, _ = db.Exec(`DELETE FROM mcp_idempotency WHERE action=? AND project_id=? AND idempotency_key=? AND build_id=0`, action, project.ID, key)
		return 0, err
	}
	if _, err = db.Exec(`UPDATE mcp_idempotency SET build_id=? WHERE action=? AND project_id=? AND idempotency_key=?`, buildID, action, project.ID, key); err != nil {
		return 0, err
	}
	return buildID, nil
}

func lockMCPIdempotency(action string, projectID int64, key string) (func(), error) {
	if db.dialect != "postgres" {
		return func() {}, nil
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	lockKey := action + ":" + strconv.FormatInt(projectID, 10) + ":" + key
	if _, err = conn.ExecContext(context.Background(), `SELECT pg_advisory_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		conn.Close()
		return nil, err
	}
	return func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, lockKey)
		_ = conn.Close()
	}, nil
}
