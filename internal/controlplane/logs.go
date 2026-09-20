package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"

	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/mistriq/deployer/internal/runtimeengine"
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
	appendLineAt(d, id, source, stream, text, time.Now().UTC())
}
func appendLineAt(d *database, id, source, stream, text string, at time.Time) {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	key := id + "|" + source
	lines := d.Logs[key]
	next := int64(1)
	if len(lines) > 0 {
		next = lines[len(lines)-1].Cursor + 1
	}
	lines = append(lines, LogLine{Cursor: next, Time: at, Source: source, Stream: stream, Text: text})
	if len(lines) > 2000 {
		lines = lines[len(lines)-2000:]
	}
	d.Logs[key] = lines
}

// structuredRuntimeLogs is optional so injected runtimes that expose only text
// remain compatible. Actual Runtime entries preserve their source timestamps.
type structuredRuntimeLogs interface {
	LogEntries(context.Context, string, int) ([]runtimeengine.LogEntry, error)
	ReleaseLogEntries(context.Context, string, int) ([]runtimeengine.LogEntry, error)
}

// Bounded upstream snapshots are reconciled by stable identity, not text alone.
// Hash identities before persistence so even upstream IDs cannot retain secrets.
func logIdentity(entry runtimeengine.LogEntry) string {
	var parts []string
	if entry.ID != "" {
		parts = []string{"id", entry.ID}
	} else {
		parts = []string{"entry", entry.At.UTC().Format(time.RFC3339Nano), entry.Stream, entry.Text}
	}
	raw, _ := json.Marshal(parts)
	sum := sha256.Sum256(raw)
	return "entry:" + hex.EncodeToString(sum[:])
}
func (s *Service) collectLogs(ctx context.Context, j *Job) error {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	var entries []runtimeengine.LogEntry
	var err error
	structured, ok := s.Runtime.(structuredRuntimeLogs)
	if ok {
		if j.ReleaseID != "" {
			entries, err = structured.ReleaseLogEntries(ctx, j.ReleaseID, 1000)
		} else {
			entries, err = structured.LogEntries(ctx, j.RuntimeID, 1000)
		}
	} else {
		var text string
		if j.ReleaseID != "" {
			text, err = s.Runtime.ReleaseLogs(ctx, j.ReleaseID, 1000)
		} else {
			text, err = s.Runtime.Logs(ctx, j.RuntimeID, 1000)
		}
		if text != "" {
			for _, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
				entries = append(entries, runtimeengine.LogEntry{Text: line, Stream: "stdout"})
			}
		}
	}
	if err != nil {
		return err
	}
	current := make([]string, len(entries))
	for i := range entries {
		entries[i].Text = s.redact(entries[i].Text)
		// Stream labels are metadata but are still untrusted input.
		entries[i].Stream = s.redact(entries[i].Stream)
		if ok {
			current[i] = logIdentity(entries[i])
		} else {
			current[i] = entries[i].Text
		}
		if len(entries[i].Text) > 16384 {
			entries[i].Text = entries[i].Text[:16384] + " [truncated]"
		}
	}
	return s.Store.update(func(d *database) error {
		stored := d.Jobs[j.ID]
		if stored == nil {
			return errors.New("log job missing")
		}
		if len(current) == 0 {
			return nil
		}
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
		if len(prev) > 0 && overlap == 0 {
			appendLine(d, j.ID, "runtime", "system", "[upstream log window changed; some lines may be unavailable]")
		}
		for _, entry := range entries[overlap:] {
			appendLineAt(d, j.ID, "runtime", entry.Stream, entry.Text, entry.At)
		}
		stored.RuntimeTail = current
		return nil
	})
}
