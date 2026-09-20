package runtimeengine

import (
	"errors"
	"net/url"
	"path"
	"regexp"
	"strings"
)

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
var digest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var user = regexp.MustCompile(`^[1-9][0-9]{0,9}:[1-9][0-9]{0,9}$`)
var imageName = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]{0,254}$`)

func invalid() error        { return errors.New("runtime engine: invalid configuration or request") }
func validID(s string) bool { return identifier.MatchString(s) }
func (a Artifact) Validate() error {
	if !digest.MatchString(a.Digest) || !imageName.MatchString(a.Image) || strings.Contains(a.Image, "..") || strings.Contains(a.Image, "://") || strings.Contains(a.Image, "//") {
		return invalid()
	}
	return nil
}
func (m Manifest) Validate() error {
	if m.Version != 1 || (m.Kind != "static" && m.Kind != "node-http") || !validID(m.PolicyVersion) {
		return invalid()
	}
	r := m.Runtime
	if r.Port < 1024 || r.Port > 65535 || !user.MatchString(r.User) || !r.ReadOnlyRoot || len(r.Tmpfs) > 4 || len(r.EnvNames) > 100 {
		return invalid()
	}
	seen := map[string]bool{}
	for _, n := range r.EnvNames {
		if !envName.MatchString(n) || seen[n] {
			return invalid()
		}
		seen[n] = true
	}
	seen = map[string]bool{}
	for _, t := range r.Tmpfs {
		if (t.Path != "/tmp" && t.Path != "/run" && !strings.HasPrefix(t.Path, "/tmp/")) || path.Clean(t.Path) != t.Path || t.SizeMB < 1 || t.SizeMB > 256 || seen[t.Path] {
			return invalid()
		}
		seen[t.Path] = true
	}
	h := m.Health
	u, e := url.ParseRequestURI(h.Path)
	if e != nil || u.IsAbs() || u.Host != "" || !strings.HasPrefix(h.Path, "/") || strings.HasPrefix(h.Path, "//") || h.ExpectStatusMin < 200 || h.ExpectStatusMax > 399 || h.ExpectStatusMax < h.ExpectStatusMin || h.TimeoutMS < 1 || h.TimeoutMS > 10000 || h.IntervalMS < 1 || h.IntervalMS > 120000 || h.Retries < 1 || h.Retries > 30 || h.GracePeriodMS < 0 || h.GracePeriodMS > 120000 {
		return invalid()
	}
	s := m.Resources
	if s.CPUMillis < 1 || s.CPUMillis > 2000 || s.MemoryMB < 1 || s.MemoryMB > 2048 || s.PidsLimit < 1 || s.PidsLimit > 512 || s.DiskMB < 1 || s.DiskMB > 4096 || m.Network.MaxBodyBytes < 1 || m.Network.MaxBodyBytes > 1073741824 || m.Network.RateLimitRPS < 1 || m.Network.RateLimitRPS > 100000 {
		return invalid()
	}
	return nil
}

// ValidateCapabilities rejects nodes unable to support the requested manifest and quotas.
func (m Manifest) ValidateCapabilities(c Capabilities) error {
	if err := m.Validate(); err != nil {
		return err
	}
	if c.ManifestVersion != m.Version {
		return invalid()
	}
	found := false
	for _, k := range c.SupportedManifestKinds {
		if k == m.Kind {
			found = true
		}
	}
	if !found {
		return invalid()
	}
	l := c.Limits
	for _, p := range [][2]int{{m.Resources.CPUMillis, l.MaxCPUMillis}, {m.Resources.MemoryMB, l.MaxMemoryMB}, {m.Resources.PidsLimit, l.MaxPidsLimit}, {m.Resources.DiskMB, l.MaxDiskMB}, {len(m.Runtime.EnvNames), l.MaxEnvVars}, {len(m.Runtime.Tmpfs), l.MaxTmpfsMounts}, {m.Health.Retries, l.MaxHealthRetries}, {m.Health.TimeoutMS, l.MaxHealthTimeoutMS}, {m.Health.GracePeriodMS, l.MaxHealthGraceMS}} {
		if p[1] > 0 && p[0] > p[1] {
			return invalid()
		}
	}
	for _, t := range m.Runtime.Tmpfs {
		if l.MaxTmpfsMB > 0 && t.SizeMB > l.MaxTmpfsMB {
			return invalid()
		}
	}
	return nil
}
