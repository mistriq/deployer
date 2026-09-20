package runtimeengine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	base, token string
	sandbox     bool
	http        *http.Client
}
type Error struct {
	Code       string
	HTTPStatus int
	Retryable  bool
}

func (e *Error) Error() string {
	return fmt.Sprintf("runtime engine: %s (HTTP %d)", e.Code, e.HTTPStatus)
}
func New(cfg Config) (*Client, error) {
	u, e := url.Parse(cfg.BaseURL)
	if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.TrimSpace(cfg.Token) == "" || strings.ContainsAny(cfg.Token, "\r\n") {
		return nil, invalid()
	}
	ip := net.ParseIP(u.Hostname())
	loopback := u.Hostname() == "localhost" || (ip != nil && ip.IsLoopback())
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback) {
		return nil, invalid()
	}
	p := strings.TrimRight(u.Path, "/")
	if p != "" && p != "/api/internal/v1" {
		return nil, invalid()
	}
	u.Path = "/api/internal/v1"
	h := http.Client{Timeout: 30 * time.Second}
	if cfg.HTTPClient != nil {
		h = *cfg.HTTPClient
		if h.Timeout == 0 {
			h.Timeout = 30 * time.Second
		}
	}
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{base: strings.TrimRight(u.String(), "/"), token: cfg.Token, sandbox: cfg.Sandbox, http: &h}, nil
}

var knownCodes = map[string]bool{}

func init() {
	for _, s := range strings.Fields("ROUTE_APPLY_FAILED DEPLOY_SUPERSEDED DEPLOY_TIMEOUT NO_ROLLBACK_TARGET ARTIFACT_DIGEST_MISMATCH ROUTE_RELOAD_ROLLED_BACK DEPLOY_CANCELLED STORE_UNAVAILABLE INTERNAL_ERROR GLOBAL_STOP_ACTIVE NODE_CAPACITY_EXHAUSTED NETWORK_SETUP_FAILED CONTAINER_CREATE_FAILED CONTAINER_START_FAILED CONTAINER_EXITED_EARLY DRIVER_UNAVAILABLE HEALTH_CHECK_FAILED POLICY_VIOLATION PROJECT_UNKNOWN PROJECT_SUSPENDED NODE_DRAINING IMAGE_PULL_FAILED HEALTH_CHECK_TIMEOUT ROUTE_PROBE_FAILED ROUTE_HOSTNAME_CONFLICT MANIFEST_INVALID KILL_SWITCH_ACTIVE ARTIFACT_TOO_LARGE") {
		knownCodes[s] = true
	}
}
func (c *Client) call(ctx context.Context, method, p string, body any, key string) ([]byte, error) {
	var b []byte
	var err error
	if body != nil {
		b, err = json.Marshal(body)
		if err != nil {
			return nil, invalid()
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+p, bytes.NewReader(b))
	if err != nil {
		return nil, invalid()
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.sandbox {
		req.Header.Set("X-Socen-Sandbox", "true")
	}
	if key != "" {
		if !validID(key) {
			return nil, invalid()
		}
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &Error{Code: "TRANSPORT_ERROR", Retryable: true}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024+1))
	if err != nil || len(raw) > 2*1024*1024 {
		return nil, &Error{Code: "INVALID_RESPONSE", HTTPStatus: resp.StatusCode}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var v struct {
			Error struct {
				Code      string `json:"code"`
				Retryable *bool  `json:"retryable"`
			} `json:"error"`
		}
		_ = json.Unmarshal(raw, &v)
		code := "REMOTE_ERROR"
		if knownCodes[v.Error.Code] {
			code = v.Error.Code
		}
		retryable := resp.StatusCode == 429 || resp.StatusCode >= 500
		if v.Error.Retryable != nil {
			retryable = *v.Error.Retryable
		}
		return nil, &Error{Code: code, HTTPStatus: resp.StatusCode, Retryable: retryable}
	}
	return raw, nil
}
func decode(raw []byte, key string, out any) error {
	var envelope map[string]json.RawMessage
	if json.Unmarshal(raw, &envelope) == nil {
		if v, ok := envelope[key]; ok {
			raw = v
		} else if v, ok := envelope["data"]; ok {
			raw = v
		}
	}
	if json.Unmarshal(raw, out) != nil {
		return &Error{Code: "INVALID_RESPONSE"}
	}
	return nil
}
func (c *Client) Capabilities(ctx context.Context) (Capabilities, error) {
	var v Capabilities
	b, e := c.call(ctx, "GET", "/meta/capabilities", nil, "")
	if e == nil {
		e = decode(b, "capabilities", &v)
	}
	if e == nil && v.ManifestVersion == 0 {
		e = &Error{Code: "INVALID_RESPONSE"}
	}
	return v, e
}
func (c *Client) UpsertProject(ctx context.Context, r ProjectRequest) (Project, error) {
	var v Project
	if !validID(r.ProjectID) || !validID(r.Slug) {
		return v, invalid()
	}
	if e := r.Manifest.Validate(); e != nil {
		return v, e
	}
	b, e := c.call(ctx, "POST", "/projects", r, "")
	if e == nil {
		e = decode(b, "project", &v)
	}
	if v.ID == "" {
		v.ID = v.ProjectID
	}
	if e == nil && v.ID != r.ProjectID {
		e = &Error{Code: "INVALID_RESPONSE"}
	}
	return v, e
}
func (c *Client) ReplaceEnvironment(ctx context.Context, id string, env map[string]string) error {
	if !validID(id) || env == nil || len(env) > 100 {
		return invalid()
	}
	total := 0
	for k, v := range env {
		if !envName.MatchString(k) || strings.ContainsRune(v, 0) {
			return invalid()
		}
		total += len(k) + len(v)
	}
	if total > 1024*1024 {
		return invalid()
	}
	_, e := c.call(ctx, "PUT", "/projects/"+id+"/env", map[string]any{"env": env}, "")
	return e
}
func (c *Client) Deploy(ctx context.Context, r DeploymentRequest, key string) (Deployment, error) {
	var v Deployment
	if !validID(r.ProjectID) || key == "" || (r.ExternalDeploymentID != "" && !validID(r.ExternalDeploymentID)) {
		return v, invalid()
	}
	if e := r.Artifact.Validate(); e != nil {
		return v, e
	}
	b, e := c.call(ctx, "POST", "/deployments", r, key)
	if e == nil {
		e = decode(b, "deployment", &v)
	}
	if e == nil && (!validID(v.ID) || (v.ProjectID != "" && v.ProjectID != r.ProjectID)) {
		e = &Error{Code: "INVALID_RESPONSE"}
	}
	return v, e
}
func (c *Client) Deployment(ctx context.Context, id string) (Deployment, error) {
	var v Deployment
	if !validID(id) {
		return v, invalid()
	}
	b, e := c.call(ctx, "GET", "/deployments/"+id, nil, "")
	if e == nil {
		e = decode(b, "deployment", &v)
	}
	if e == nil && v.ID != id {
		e = &Error{Code: "INVALID_RESPONSE"}
	}
	if v.FailureCode != "" && !knownCodes[v.FailureCode] {
		v.FailureCode = "REMOTE_ERROR"
	}
	return v, e
}
func (c *Client) release(ctx context.Context, id string) (Release, error) {
	var v Release
	if !validID(id) {
		return v, invalid()
	}
	b, e := c.call(ctx, "GET", "/releases/"+id, nil, "")
	if e == nil {
		e = decode(b, "release", &v)
	}
	if e == nil && v.ID != id {
		e = &Error{Code: "INVALID_RESPONSE"}
	}
	return v, e
}

// VerifyActive requires independent release health, project active-release mapping,
// and a route whose desired/applied revisions agree for that exact release.
func (c *Client) VerifyActive(ctx context.Context, projectID, deploymentID string) (Verification, error) {
	var v Verification
	if !validID(projectID) {
		return v, invalid()
	}
	d, e := c.Deployment(ctx, deploymentID)
	if e != nil {
		return v, e
	}
	if d.ProjectID != projectID {
		return v, &Error{Code: "IDENTITY_MISMATCH"}
	}
	switch d.Status {
	case "failed", "cancelled", "canceled", "superseded":
		code := d.FailureCode
		if code == "" {
			code = "DEPLOY_FAILED"
		}
		return v, &Error{Code: code}
	}
	if d.Status != "active" {
		return v, nil
	}
	if !validID(d.ReleaseID) {
		return v, &Error{Code: "INVALID_RESPONSE"}
	}
	r, e := c.release(ctx, d.ReleaseID)
	if e != nil {
		return v, e
	}
	if r.ProjectID != projectID {
		return v, &Error{Code: "IDENTITY_MISMATCH"}
	}
	if (r.healthyPresent && !r.Healthy) || (r.healthStatusPresent && r.HealthStatus != "" && r.HealthStatus != "healthy") {
		return v, nil
	}
	explicitHealth := r.Healthy || r.HealthStatus == "healthy"
	fallbackHealth := r.Status == "active" && r.DeploymentID == d.ID && !r.ActivatedAt.IsZero() && r.RouteRevision > 0 && d.Phase == "active" && healthPhasesComplete(d.Events)
	if !explicitHealth && !fallbackHealth {
		return v, nil
	}
	b, e := c.call(ctx, "GET", "/projects/"+projectID, nil, "")
	if e != nil {
		return v, e
	}
	var p Project
	if e = decode(b, "project", &p); e != nil {
		return v, e
	}
	if p.ID == "" {
		p.ID = p.ProjectID
	}
	if p.ID != projectID {
		return v, &Error{Code: "IDENTITY_MISMATCH"}
	}
	if p.ActiveReleaseID != d.ReleaseID {
		return v, nil
	}
	b, e = c.call(ctx, "GET", "/routes", nil, "")
	if e != nil {
		return v, e
	}
	var routes []Route
	if e = decode(b, "items", &routes); e != nil {
		if e = decode(b, "routes", &routes); e != nil {
			return v, e
		}
	}
	for _, route := range routes {
		releaseID := route.ReleaseID
		if releaseID == "" {
			releaseID = route.ActiveReleaseID
		}
		if route.ProjectID == projectID && releaseID == d.ReleaseID && route.Status == "active" && route.DesiredRevision > 0 && route.AppliedRevision == route.DesiredRevision && (explicitHealth || r.RouteRevision == route.DesiredRevision) {
			return Verification{Active: true, ReleaseID: d.ReleaseID, Route: route}, nil
		}
	}
	return v, nil
}

// Rollback must not be automatically retried after an ambiguous transport result:
// the published rollback contract does not promise idempotency support.
func (c *Client) Rollback(ctx context.Context, projectID, releaseID, key string) (Deployment, error) {
	var v Deployment
	if !validID(projectID) || !validID(releaseID) || !validID(key) {
		return v, invalid()
	}
	r, e := c.release(ctx, releaseID)
	if e != nil {
		return v, e
	}
	if r.ProjectID != projectID {
		return v, &Error{Code: "IDENTITY_MISMATCH"}
	}
	b, e := c.call(ctx, "POST", "/projects/"+projectID+"/rollback", map[string]string{"release_id": releaseID}, key)
	if e == nil {
		e = decode(b, "deployment", &v)
	}
	if e == nil && (!validID(v.ID) || (v.ProjectID != "" && v.ProjectID != projectID)) {
		e = &Error{Code: "INVALID_RESPONSE"}
	}
	return v, e
}
func (c *Client) Logs(ctx context.Context, id string, tail int) (string, error) {
	return c.logs(ctx, "deployments", id, tail)
}
func (c *Client) ReleaseLogs(ctx context.Context, id string, tail int) (string, error) {
	return c.logs(ctx, "releases", id, tail)
}

func healthPhasesComplete(events []PhaseEvent) bool {
	wanted := []string{"health_check", "activating", "active"}
	next := 0
	var previous time.Time
	for _, event := range events {
		if event.Phase == "failed" || event.Phase == "cancelled" || event.Phase == "superseded" {
			return false
		}
		if next < len(wanted) && event.Phase == wanted[next] {
			if event.At.IsZero() || (!previous.IsZero() && event.At.Before(previous)) {
				return false
			}
			previous = event.At
			next++
		}
	}
	return next == len(wanted)
}
func (c *Client) LogEntries(ctx context.Context, id string, tail int) ([]LogEntry, error) {
	return c.logEntries(ctx, "deployments", id, tail)
}
func (c *Client) ReleaseLogEntries(ctx context.Context, id string, tail int) ([]LogEntry, error) {
	return c.logEntries(ctx, "releases", id, tail)
}
func (c *Client) logs(ctx context.Context, kind, id string, tail int) (string, error) {
	entries, e := c.logEntries(ctx, kind, id, tail)
	if e != nil {
		return "", e
	}
	lines := make([]string, len(entries))
	for i, v := range entries {
		lines[i] = v.Text
	}
	return strings.Join(lines, "\n"), nil
}
func (c *Client) logEntries(ctx context.Context, kind, id string, tail int) ([]LogEntry, error) {
	if !validID(id) || tail < 1 || tail > 1000 {
		return nil, invalid()
	}
	b, e := c.call(ctx, "GET", "/"+kind+"/"+id+"/logs?tail="+strconv.Itoa(tail), nil, "")
	if e != nil {
		return nil, e
	}
	var raw map[string]json.RawMessage
	if json.Unmarshal(b, &raw) != nil {
		return nil, &Error{Code: "INVALID_RESPONSE"}
	}
	entries := []LogEntry{}
	if items, ok := raw["items"]; ok {
		if string(items) == "null" || json.Unmarshal(items, &entries) != nil {
			return nil, &Error{Code: "INVALID_RESPONSE"}
		}
		var fields []map[string]json.RawMessage
		if json.Unmarshal(items, &fields) != nil {
			return nil, &Error{Code: "INVALID_RESPONSE"}
		}
		for _, field := range fields {
			var text string
			value, ok := field["text"]
			if !ok || string(value) == "null" || json.Unmarshal(value, &text) != nil {
				return nil, &Error{Code: "INVALID_RESPONSE"}
			}
		}
		for _, entry := range entries {
			if entry.At.IsZero() || (entry.Stream != "stdout" && entry.Stream != "stderr" && entry.Stream != "system") {
				return nil, &Error{Code: "INVALID_RESPONSE"}
			}
		}
	} else if logs, ok := raw["logs"]; ok {
		var text string
		if string(logs) == "null" || json.Unmarshal(logs, &text) != nil {
			return nil, &Error{Code: "INVALID_RESPONSE"}
		}
		if text != "" {
			for _, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
				entries = append(entries, LogEntry{Stream: "stdout", Text: line})
			}
		}
	} else {
		return nil, &Error{Code: "INVALID_RESPONSE"}
	}
	for i := range entries {
		entries[i].Text = strings.ReplaceAll(entries[i].Text, c.token, "[REDACTED]")
	}
	return entries, nil
}
func ValidateCapabilities(m Manifest, c Capabilities) error { return m.ValidateCapabilities(c) }

var _ API = (*Client)(nil)
