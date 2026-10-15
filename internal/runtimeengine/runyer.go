package runtimeengine

// Runyer is the client for the runyer API (OpenAPI "runyer API" 0.1.x). It implements the same
// interface as Client, so the control plane does not care which Runtime protocol a target speaks.
//
// Differences from the internal v1 protocol:
//   - Endpoints live under /api; deployments start with POST /api/projects/{id}/deploy.
//   - A deployment phase "active" already means healthy, switched at the edge (netyer) and proven
//     from outside, so verification checks the phase plus the project's active release.
//   - Custom domains are the project's `aliases`; host names take effect at the next activation.
//   - Errors are {"error": CODE, "http": status, "message"?, "retryable"?}.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Runyer struct {
	base, token string
	http        *http.Client
}

var remoteCode = regexp.MustCompile(`^[A-Z][A-Z0-9_]{1,63}$`)
var hostnamePattern = regexp.MustCompile(`^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$`)

// ValidHostname reports whether s is a lowercase DNS host name suitable as a project alias.
func ValidHostname(s string) bool { return len(s) <= 253 && hostnamePattern.MatchString(s) }

func NewRunyer(cfg Config) (*Runyer, error) {
	u, e := url.Parse(cfg.BaseURL)
	if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.TrimSpace(cfg.Token) == "" || strings.ContainsAny(cfg.Token, "\r\n") {
		return nil, invalid()
	}
	ip := net.ParseIP(u.Hostname())
	loopback := u.Hostname() == "localhost" || (ip != nil && ip.IsLoopback())
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback) {
		return nil, invalid()
	}
	if p := strings.TrimRight(u.Path, "/"); p != "" && p != "/api" {
		return nil, invalid()
	}
	u.Path = ""
	h := http.Client{Timeout: 30 * time.Second}
	if cfg.HTTPClient != nil {
		h = *cfg.HTTPClient
		if h.Timeout == 0 {
			h.Timeout = 30 * time.Second
		}
	}
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Runyer{base: strings.TrimRight(u.String(), "/") + "/api", token: cfg.Token, http: &h}, nil
}

func (c *Runyer) call(ctx context.Context, method, p string, body any, key string) ([]byte, error) {
	var b []byte
	if body != nil {
		var err error
		if b, err = json.Marshal(body); err != nil {
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
			Error     json.RawMessage `json:"error"`
			Retryable *bool           `json:"retryable"`
		}
		_ = json.Unmarshal(raw, &v)
		var code string
		if json.Unmarshal(v.Error, &code) != nil {
			// Tolerate the internal-v1 shape {"error": {"code": …}} from compatibility gateways.
			var nested struct {
				Code      string `json:"code"`
				Retryable *bool  `json:"retryable"`
			}
			_ = json.Unmarshal(v.Error, &nested)
			code, v.Retryable = nested.Code, nested.Retryable
		}
		if !remoteCode.MatchString(code) {
			code = "REMOTE_ERROR"
		}
		retryable := resp.StatusCode == 429 || resp.StatusCode >= 500
		if v.Retryable != nil {
			retryable = *v.Retryable
		}
		return nil, &Error{Code: code, HTTPStatus: resp.StatusCode, Retryable: retryable}
	}
	return raw, nil
}

// Capabilities: runyer validates manifests itself during provisioning (and rejects them with
// MANIFEST_INVALID / POLICY_VIOLATION), so no client-side limits are imposed beyond Manifest.Validate.
func (c *Runyer) Capabilities(ctx context.Context) (Capabilities, error) {
	if _, e := c.call(ctx, "GET", "/instance", nil, ""); e != nil {
		return Capabilities{}, e
	}
	return Capabilities{ManifestVersion: 1, SupportedManifestKinds: []string{"static", "node-http"}}, nil
}

type runyerProject struct {
	ID              string   `json:"id"`
	Slug            string   `json:"slug"`
	Hostname        string   `json:"hostname"`
	Aliases         []string `json:"aliases"`
	URL             string   `json:"url"`
	ActiveReleaseID string   `json:"active_release_id"`
	Route           *struct {
		Hostname        string   `json:"hostname"`
		Aliases         []string `json:"aliases"`
		Status          string   `json:"status"`
		HTTPS           bool     `json:"https"`
		CertStatus      string   `json:"cert_status"`
		ReleaseID       string   `json:"release_id"`
		RouteID         string   `json:"route_id"`
		DesiredRevision int64    `json:"desired_revision"`
		AppliedRevision int64    `json:"applied_revision"`
	} `json:"route"`
}

func (c *Runyer) project(ctx context.Context, id string) (runyerProject, error) {
	var p runyerProject
	b, e := c.call(ctx, "GET", "/projects/"+id, nil, "")
	if e == nil {
		e = decode(b, "project", &p)
	}
	if e == nil && p.ID != id {
		e = &Error{Code: "IDENTITY_MISMATCH"}
	}
	return p, e
}

func (c *Runyer) UpsertProject(ctx context.Context, r ProjectRequest) (Project, error) {
	var v Project
	if !validID(r.ProjectID) || !validID(r.Slug) || len(r.Aliases) > 20 {
		return v, invalid()
	}
	for _, alias := range r.Aliases {
		if !ValidHostname(alias) {
			return v, invalid()
		}
	}
	if e := r.Manifest.Validate(); e != nil {
		return v, e
	}
	// Provisioning is an idempotent upsert; aliases are always sent so removed domains are removed.
	aliases := r.Aliases
	if aliases == nil {
		aliases = []string{}
	}
	body := map[string]any{"id": r.ProjectID, "slug": r.Slug, "manifest": r.Manifest, "aliases": aliases}
	b, e := c.call(ctx, "POST", "/projects", body, "")
	var p runyerProject
	if e == nil {
		e = decode(b, "project", &p)
	}
	if e == nil && p.ID != r.ProjectID {
		e = &Error{Code: "INVALID_RESPONSE"}
	}
	return Project{ID: p.ID, ProjectID: p.ID, ActiveReleaseID: p.ActiveReleaseID}, e
}

func (c *Runyer) ReplaceEnvironment(ctx context.Context, id string, env map[string]string) error {
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

func (c *Runyer) Deploy(ctx context.Context, r DeploymentRequest, key string) (Deployment, error) {
	var v Deployment
	if !validID(r.ProjectID) || key == "" || (r.ExternalDeploymentID != "" && !validID(r.ExternalDeploymentID)) {
		return v, invalid()
	}
	if e := r.Artifact.Validate(); e != nil {
		return v, e
	}
	body := map[string]any{"artifact": r.Artifact}
	if r.ExternalDeploymentID != "" {
		body["external_id"] = r.ExternalDeploymentID
	}
	b, e := c.call(ctx, "POST", "/projects/"+r.ProjectID+"/deploy", body, key)
	if e == nil {
		e = decode(b, "deployment", &v)
	}
	if e == nil && (!validID(v.ID) || (v.ProjectID != "" && v.ProjectID != r.ProjectID)) {
		e = &Error{Code: "INVALID_RESPONSE"}
	}
	return v, e
}

func (c *Runyer) Deployment(ctx context.Context, id string) (Deployment, error) {
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
	if v.FailureCode != "" && !remoteCode.MatchString(v.FailureCode) {
		v.FailureCode = "REMOTE_ERROR"
	}
	return v, e
}

// VerifyActive: runyer reports "active" only after the edge switched and the public host name was
// proven from outside. The project's active release must also be this deployment's release.
func (c *Runyer) VerifyActive(ctx context.Context, projectID, deploymentID string) (Verification, error) {
	var v Verification
	if !validID(projectID) {
		return v, invalid()
	}
	d, e := c.Deployment(ctx, deploymentID)
	if e != nil {
		return v, e
	}
	if d.ProjectID != "" && d.ProjectID != projectID {
		return v, &Error{Code: "IDENTITY_MISMATCH"}
	}
	switch d.Phase {
	case "failed", "cancelled":
		code := d.FailureCode
		if code == "" {
			code = "DEPLOY_FAILED"
		}
		return v, &Error{Code: code}
	case "active":
	default:
		return v, nil
	}
	if !validID(d.ReleaseID) {
		return v, &Error{Code: "INVALID_RESPONSE"}
	}
	p, e := c.project(ctx, projectID)
	if e != nil {
		return v, e
	}
	if p.ActiveReleaseID != d.ReleaseID {
		return v, nil
	}
	route := Route{ProjectID: projectID, ReleaseID: d.ReleaseID, Status: "active", Hostname: p.Hostname, Aliases: p.Aliases}
	if p.Route != nil {
		route = Route{ID: p.Route.RouteID, ProjectID: projectID, ReleaseID: p.Route.ReleaseID, Status: p.Route.Status, DesiredRevision: p.Route.DesiredRevision, AppliedRevision: p.Route.AppliedRevision, Hostname: p.Route.Hostname, Aliases: p.Route.Aliases, HTTPS: p.Route.HTTPS, CertStatus: p.Route.CertStatus}
		if route.Hostname == "" {
			route.Hostname = p.Hostname
		}
		if route.Status != "" && route.Status != "active" {
			return v, nil
		}
	}
	if route.Hostname == "" && p.URL != "" {
		if u, err := url.Parse(p.URL); err == nil {
			route.Hostname = u.Hostname()
		}
	}
	return Verification{Active: true, ReleaseID: d.ReleaseID, Route: route}, nil
}

func (c *Runyer) Rollback(ctx context.Context, projectID, releaseID, key string) (Deployment, error) {
	var v Deployment
	if !validID(projectID) || !validID(releaseID) || !validID(key) {
		return v, invalid()
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

// ErrOffboardPending reports a project that was suspended instead of removed: runyer lets only
// admins offboard projects, so an operator token stops it serving and an admin removes it later.
var ErrOffboardPending = errors.New("runyer: project suspended, offboarding needs an admin")

// DeleteProject removes the project, its releases and its route. A project the
// node no longer knows counts as deleted, so the call can be retried safely.
func (c *Runyer) DeleteProject(ctx context.Context, projectID string) error {
	if !validID(projectID) {
		return invalid()
	}
	_, e := c.call(ctx, "DELETE", "/projects/"+projectID, nil, "")
	var remote *Error
	if errors.As(e, &remote) && remote.HTTPStatus == 404 {
		return nil
	}
	if errors.As(e, &remote) && remote.HTTPStatus == 403 {
		_, s := c.call(ctx, "POST", "/projects/"+projectID+"/suspend", map[string]string{"reason": "Deleted in Molo; waiting for an admin to offboard it."}, "")
		if s == nil || errors.As(s, &remote) && remote.HTTPStatus == 409 {
			return ErrOffboardPending
		}
		return s
	}
	return e
}

func (c *Runyer) Logs(ctx context.Context, id string, tail int) (string, error) {
	return joinLogs(c.LogEntries(ctx, id, tail))
}
func (c *Runyer) ReleaseLogs(ctx context.Context, id string, tail int) (string, error) {
	return joinLogs(c.ReleaseLogEntries(ctx, id, tail))
}
func (c *Runyer) LogEntries(ctx context.Context, id string, tail int) ([]LogEntry, error) {
	return c.logEntries(ctx, "deployments", id, tail)
}
func (c *Runyer) ReleaseLogEntries(ctx context.Context, id string, tail int) ([]LogEntry, error) {
	return c.logEntries(ctx, "releases", id, tail)
}

func joinLogs(entries []LogEntry, e error) (string, error) {
	if e != nil {
		return "", e
	}
	lines := make([]string, len(entries))
	for i, v := range entries {
		lines[i] = v.Text
	}
	return strings.Join(lines, "\n"), nil
}

// The OpenAPI document leaves the log body open ("object"); accept the shapes runyer and its
// predecessor use: {items|lines|entries: [{at|time|ts, stream, text|line|message}]} or {logs: "…"}.
func (c *Runyer) logEntries(ctx context.Context, kind, id string, tail int) ([]LogEntry, error) {
	if !validID(id) || tail < 1 || tail > 1000 {
		return nil, invalid()
	}
	b, e := c.call(ctx, "GET", "/"+kind+"/"+id+"/logs?tail="+strconv.Itoa(tail), nil, "")
	if e != nil {
		return nil, e
	}
	var body map[string]json.RawMessage
	if json.Unmarshal(b, &body) != nil {
		return nil, &Error{Code: "INVALID_RESPONSE"}
	}
	entries := []LogEntry{}
	if text, ok := body["logs"]; ok {
		var s string
		if json.Unmarshal(text, &s) != nil {
			return nil, &Error{Code: "INVALID_RESPONSE"}
		}
		for _, line := range strings.Split(strings.TrimSuffix(s, "\n"), "\n") {
			if line != "" {
				entries = append(entries, LogEntry{Stream: "stdout", Text: line})
			}
		}
	} else {
		var list []map[string]json.RawMessage
		found := false
		for _, key := range []string{"items", "lines", "entries"} {
			if raw, ok := body[key]; ok {
				found = true
				if string(raw) != "null" && json.Unmarshal(raw, &list) != nil {
					return nil, &Error{Code: "INVALID_RESPONSE"}
				}
				break
			}
		}
		if !found {
			return nil, &Error{Code: "INVALID_RESPONSE"}
		}
		for _, item := range list {
			entry := LogEntry{Stream: "stdout"}
			for _, key := range []string{"text", "line", "message"} {
				if json.Unmarshal(item[key], &entry.Text) == nil && entry.Text != "" {
					break
				}
			}
			for _, key := range []string{"at", "time", "ts"} {
				var at time.Time
				if json.Unmarshal(item[key], &at) == nil && !at.IsZero() {
					entry.At = at
					break
				}
			}
			var stream string
			if json.Unmarshal(item["stream"], &stream) == nil && (stream == "stdout" || stream == "stderr" || stream == "system") {
				entry.Stream = stream
			}
			entries = append(entries, entry)
		}
	}
	for i := range entries {
		entries[i].Text = strings.ReplaceAll(entries[i].Text, c.token, "[REDACTED]")
	}
	return entries, nil
}

var _ API = (*Runyer)(nil)
