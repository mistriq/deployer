package runtimeengine

import (
	"context"
	"encoding/json"
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
