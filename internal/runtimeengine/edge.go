package runtimeengine

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
)

// Edge is what the public is served right now: runyer's route and, behind it, the netyer site and its
// certificate. A deployment can be active while netyer is still issuing the certificate, so the
// portal reads this separately from the deployment state.
type Edge struct {
	RouteStatus string     `json:"route_status"`
	RouteError  string     `json:"route_error,omitempty"`
	Drift       string     `json:"drift,omitempty"`
	TLSMode     string     `json:"tls_mode,omitempty"`
	Hosts       []EdgeHost `json:"hosts"`
}

// EdgeHost is the state of one host name. CertStatus uses netyer's values (pending, issuing, valid,
// failed, expired, revoked), "off" when the site has no TLS, or "unknown".
type EdgeHost struct {
	Hostname        string `json:"hostname"`
	Primary         bool   `json:"primary,omitempty"`
	HTTPS           bool   `json:"https"`
	CertStatus      string `json:"cert_status"`
	CertError       string `json:"cert_error,omitempty"`
	CertNotAfter    int64  `json:"cert_not_after,omitempty"`
	CertRenewAt     int64  `json:"cert_renew_at,omitempty"`
	CertNextAttempt int64  `json:"cert_next_attempt,omitempty"`
}

var certStatuses = map[string]bool{"pending": true, "issuing": true, "valid": true, "failed": true, "expired": true, "revoked": true}

type netyerCert struct {
	Status      string   `json:"status"`
	Running     bool     `json:"running"`
	Domains     []string `json:"domains"`
	LastError   string   `json:"last_error"`
	NotAfter    int64    `json:"not_after"`
	RenewAt     int64    `json:"renew_at"`
	NextAttempt int64    `json:"next_attempt"`
}
type edgeRoute struct {
	Hostname     string   `json:"hostname"`
	Aliases      []string `json:"aliases"`
	Status       string   `json:"status"`
	HTTPS        bool     `json:"https"`
	CertStatus   string   `json:"cert_status"`
	CertError    string   `json:"cert_error"`
	CertNotAfter int64    `json:"cert_not_after"`
	Drift        string   `json:"drift"`
	LastError    string   `json:"last_error"`
	Message      string   `json:"message"`
}
type edgeSite struct {
	Domains []string    `json:"domains"`
	HTTPS   bool        `json:"https"`
	Enabled *bool       `json:"enabled"`
	Cert    *netyerCert `json:"cert"`
	TLS     *struct {
		Mode string `json:"mode"`
	} `json:"tls"`
}

// Edge reads GET /api/projects/{id}/edge ({route, site}). A project without a route yet has no hosts.
func (c *Runyer) Edge(ctx context.Context, projectID string) (Edge, error) {
	var out Edge
	if !validID(projectID) {
		return out, invalid()
	}
	b, e := c.call(ctx, "GET", "/projects/"+projectID+"/edge", nil, "")
	if e != nil {
		return out, e
	}
	var body struct {
		Route *edgeRoute `json:"route"`
		Site  *edgeSite  `json:"site"`
	}
	if json.Unmarshal(b, &body) != nil {
		return out, &Error{Code: "INVALID_RESPONSE"}
	}
	return edgeFrom(body.Route, body.Site), nil
}

func edgeFrom(route *edgeRoute, site *edgeSite) Edge {
	out := Edge{RouteStatus: "none", Hosts: []EdgeHost{}}
	if route == nil {
		return out
	}
	out.RouteStatus = route.Status
	if out.RouteStatus == "" {
		out.RouteStatus = "active"
	}
	out.Drift = route.Drift
	out.RouteError = route.LastError
	if site != nil && site.Enabled != nil && !*site.Enabled && out.RouteStatus == "active" {
		out.RouteStatus = "offline"
	}
	// The route's own certificate fields are the fallback when netyer's site is not included.
	status, certErr, notAfter := route.CertStatus, route.CertError, route.CertNotAfter
	var renewAt, nextAttempt int64
	var covered []string
	served := route.HTTPS
	if site != nil {
		served = site.HTTPS
		if site.TLS != nil {
			out.TLSMode = site.TLS.Mode
		}
		if site.Cert != nil {
			status, certErr, notAfter = site.Cert.Status, site.Cert.LastError, site.Cert.NotAfter
			renewAt, nextAttempt, covered = site.Cert.RenewAt, site.Cert.NextAttempt, site.Cert.Domains
			if site.Cert.Running && status != "valid" {
				status = "issuing"
			}
		}
	}
	switch {
	case out.TLSMode == "off":
		status = "off"
	case status == "" && site != nil && site.Cert == nil:
		status = "pending"
	case !certStatuses[status]:
		status = "unknown"
	}
	hosts := []string{}
	if route.Hostname != "" {
		hosts = append(hosts, strings.ToLower(route.Hostname))
	}
	for _, alias := range route.Aliases {
		hosts = append(hosts, strings.ToLower(alias))
	}
	for i, host := range hosts {
		h := EdgeHost{Hostname: host, Primary: i == 0 && route.Hostname != "", CertStatus: status, CertError: certErr, CertNotAfter: notAfter, CertRenewAt: renewAt, CertNextAttempt: nextAttempt}
		// A certificate that does not name this host yet (a new alias) is still being issued for it.
		if status == "valid" && covered != nil && !certCovers(covered, host) {
			h.CertStatus, h.CertError, h.CertNotAfter, h.CertRenewAt = "pending", "", 0, 0
		}
		if site != nil && site.Domains != nil && !certCovers(site.Domains, host) && h.CertStatus != "off" {
			h.CertStatus = "pending"
		}
		h.HTTPS = served && h.CertStatus == "valid"
		out.Hosts = append(out.Hosts, h)
	}
	return out
}

func certCovers(names []string, host string) bool {
	for _, name := range names {
		name = strings.ToLower(name)
		if name == host {
			return true
		}
		if strings.HasPrefix(name, "*.") {
			if i := strings.IndexByte(host, '.'); i > 0 && host[i+1:] == name[2:] {
				return true
			}
		}
	}
	return false
}

// EdgeModules reads netyer's per-site modules through runyer (GET /api/edge/modules): ids, names and the site
// fields of each, enough for the portal to draw a form.
func (c *Runyer) EdgeModules(ctx context.Context) (json.RawMessage, error) {
	b, e := c.call(ctx, "GET", "/edge/modules", nil, "")
	if e != nil {
		return nil, e
	}
	if !json.Valid(b) {
		return nil, &Error{Code: "INVALID_RESPONSE"}
	}
	return json.RawMessage(b), nil
}

// EdgeCaps reads the switches and settings of a project's netyer site (site.caps of GET /api/projects/{id}/edge).
// A project without a site yet has none.
func (c *Runyer) EdgeCaps(ctx context.Context, projectID string) (json.RawMessage, error) {
	if !validID(projectID) {
		return nil, invalid()
	}
	b, e := c.call(ctx, "GET", "/projects/"+projectID+"/edge", nil, "")
	if e != nil {
		return nil, e
	}
	var body struct {
		Route *struct {
			Caps json.RawMessage `json:"caps"`
		} `json:"route"`
		Site *struct {
			Caps json.RawMessage `json:"caps"`
		} `json:"site"`
	}
	if json.Unmarshal(b, &body) != nil {
		return nil, &Error{Code: "INVALID_RESPONSE"}
	}
	present := func(raw json.RawMessage) bool { return len(raw) > 0 && string(raw) != "null" }
	// netyer's site is the truth; runyer ≥ 0.7 also keeps the caps on the route.
	if body.Site != nil && present(body.Site.Caps) {
		return body.Site.Caps, nil
	}
	if body.Route != nil && present(body.Route.Caps) {
		return body.Route.Caps, nil
	}
	return json.RawMessage("{}"), nil
}

// SetEdgeCaps sends PUT /api/projects/{id}/edge/caps {caps: {<module id>: {on, config}}}. netyer validates the
// settings; a refusal (422) changes nothing. Needs a runyer token with operator access.
func (c *Runyer) SetEdgeCaps(ctx context.Context, projectID string, caps json.RawMessage) error {
	if !validID(projectID) || !json.Valid(caps) {
		return invalid()
	}
	_, e := c.call(ctx, "PUT", "/projects/"+projectID+"/edge/caps", map[string]json.RawMessage{"caps": caps}, "")
	return e
}

// AccessLog is netyer's access log of a project's site (GET /api/projects/{id}/edge/logs), newest first.
// query holds only the filters netyer documents: status (404, 4xx, error, blocked), ip (prefix), method, q (text),
// since (unix ms), before (seq) and limit.
func (c *Runyer) AccessLog(ctx context.Context, projectID string, query url.Values) ([]map[string]any, error) {
	if !validID(projectID) {
		return nil, invalid()
	}
	p := "/projects/" + projectID + "/edge/logs"
	if encoded := query.Encode(); encoded != "" {
		p += "?" + encoded
	}
	b, e := c.call(ctx, "GET", p, nil, "")
	if e != nil {
		return nil, e
	}
	var list []map[string]any
	if json.Unmarshal(b, &list) == nil {
		return list, nil
	}
	var wrapped map[string]json.RawMessage
	if json.Unmarshal(b, &wrapped) != nil {
		return nil, &Error{Code: "INVALID_RESPONSE"}
	}
	for _, key := range []string{"items", "entries", "logs", "lines"} {
		if raw, ok := wrapped[key]; ok {
			if json.Unmarshal(raw, &list) != nil {
				return nil, &Error{Code: "INVALID_RESPONSE"}
			}
			return list, nil
		}
	}
	return []map[string]any{}, nil
}
