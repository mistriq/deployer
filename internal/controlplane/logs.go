package controlplane

import (
	"context"
	"encoding/base64"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

var logPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(bearer\s+)[^\s]+`),
	regexp.MustCompile(`(?i)((?:password|passwd|secret|token|api[_-]?key|authorization)\s*[=:]\s*)[^\s,;]+`),
	regexp.MustCompile(`(https?://)[^/@\s]+:[^/@\s]+@`),
}

func (s *Service) redact(text string) string {
	d := s.Store.snapshot()
	values := append([]string{}, s.Config.ServiceSecrets...)
	values = append(values, s.Config.Credentials.Password)
	for token := range s.Config.Tokens {
		values = append(values, token)
	}
	for _, p := range d.Projects {
		for _, v := range p.Env {
			values = append(values, v)
		}
	}
	for _, j := range d.Jobs {
		for _, v := range j.Snapshot.Env {
			values = append(values, v)
		}
		for _, v := range j.Snapshot.Spec.Build.BuildArgs {
			values = append(values, v)
		}
	}
	expanded := []string{}
	for _, v := range values {
		if v != "" {
			expanded = append(expanded, v, url.QueryEscape(v), base64.StdEncoding.EncodeToString([]byte(v)))
			for _, line := range strings.Split(v, "\n") {
				if line != "" {
					expanded = append(expanded, line)
				}
			}
		}
	}
	sort.Slice(expanded, func(i, j int) bool { return len(expanded[i]) > len(expanded[j]) })
	for _, v := range expanded {
		text = strings.ReplaceAll(text, v, "[REDACTED]")
	}
	for _, p := range logPatterns {
		text = p.ReplaceAllString(text, "${1}[REDACTED]")
	}
	return text
}
func (s *Service) appendLog(id, source, stream, text string) error {
	text = s.redact(text)
	if len(text) > 16384 {
		text = text[:16384] + " [truncated]"
	}
	return s.Store.update(func(d *database) error { appendLine(d, id, source, stream, text); return nil })
}
func appendLine(d *database, id, source, stream, text string) {
	key := id + "|" + source
	lines := d.Logs[key]
	next := int64(1)
	if len(lines) > 0 {
		next = lines[len(lines)-1].Cursor + 1
	}
	lines = append(lines, LogLine{Cursor: next, Time: time.Now().UTC(), Source: source, Stream: stream, Text: text})
	if len(lines) > 2000 {
		lines = lines[len(lines)-2000:]
	}
	d.Logs[key] = lines
}

// Runtime currently exposes bounded tail snapshots, not a durable source cursor.
// Persist overlapping snapshots and explicitly mark discontinuities. This avoids
// claiming lossless replay that the upstream contract cannot provide.
func (s *Service) collectLogs(ctx context.Context, j *Job) error {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	var text string
	var err error
	if j.ReleaseID != "" {
		text, err = s.Runtime.ReleaseLogs(ctx, j.ReleaseID, 1000)
	} else {
		text, err = s.Runtime.Logs(ctx, j.RuntimeID, 1000)
	}
	if err != nil {
		return err
	}
	text = s.redact(text)
	current := []string{}
	if text != "" {
		current = strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	}
	return s.Store.update(func(d *database) error {
		stored := d.Jobs[j.ID]
		prev := stored.RuntimeTail
		overlap := 0
		for n := min(len(prev), len(current)); n > 0; n-- {
			same := true
			for i := 0; i < n; i++ {
				if prev[len(prev)-n+i] != current[i] {
					same = false
					break
				}
			}
			if same {
				overlap = n
				break
			}
		}
		if len(prev) > 0 && len(current) > 0 && overlap == 0 {
			appendLine(d, j.ID, "runtime", "system", "[upstream log window changed; some lines may be unavailable]")
		}
		for _, line := range current[overlap:] {
			if len(line) > 16384 {
				line = line[:16384] + " [truncated]"
			}
			appendLine(d, j.ID, "runtime", "stdout", line)
		}
		stored.RuntimeTail = current
		return nil
	})
}
