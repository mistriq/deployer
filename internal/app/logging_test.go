package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"testing"
)

func TestLogStructuredWritesJSONAndRedactsSecrets(t *testing.T) {
	var buf bytes.Buffer
	oldOutput := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldOutput)
		log.SetFlags(oldFlags)
	})

	logStructured("info", "test_event", map[string]interface{}{
		"path":  "/api/agent/poll?token=secret123",
		"error": errors.New("Authorization: Bearer live-token"),
		"count": 2,
	})

	var got map[string]interface{}
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got); err != nil {
		t.Fatalf("expected JSON log, got %q: %v", buf.String(), err)
	}
	if got["level"] != "info" || got["event"] != "test_event" {
		t.Fatalf("unexpected structured fields: %#v", got)
	}
	if got["count"].(float64) != 2 {
		t.Fatalf("expected numeric field to remain numeric, got %#v", got["count"])
	}
	logLine := buf.String()
	if strings.Contains(logLine, "secret123") || strings.Contains(logLine, "live-token") {
		t.Fatalf("expected secrets to be redacted, got %q", logLine)
	}
	if !strings.Contains(logLine, "[REDACTED]") {
		t.Fatalf("expected redaction marker in log, got %q", logLine)
	}
}

func TestLogOperationalErrorSkipsNil(t *testing.T) {
	var buf bytes.Buffer
	oldOutput := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldOutput)
		log.SetFlags(oldFlags)
	})

	logOperationalError("noop", nil)
	if buf.Len() != 0 {
		t.Fatalf("expected nil operational error to skip logging, got %q", buf.String())
	}
}

func TestLogStructuredRedactsNestedValuesAndMarshalFallback(t *testing.T) {
	var buf bytes.Buffer
	oldOutput := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldOutput)
		log.SetFlags(oldFlags)
	})

	logStructured("info", "nested_event", map[string]interface{}{
		"items": []interface{}{
			"--token nested-secret",
			map[string]interface{}{"auth": "Authorization: Bearer nested-live-token"},
		},
	})
	if strings.Contains(buf.String(), "nested-secret") || strings.Contains(buf.String(), "nested-live-token") {
		t.Fatalf("expected nested secrets to be redacted, got %q", buf.String())
	}

	buf.Reset()
	logStructured("info", "token=fallback-secret", map[string]interface{}{
		"bad": make(chan int),
	})
	var got map[string]interface{}
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got); err != nil {
		t.Fatalf("expected fallback JSON log, got %q: %v", buf.String(), err)
	}
	if got["event"] != "log_marshal_failed" {
		t.Fatalf("expected marshal fallback event, got %#v", got)
	}
	if strings.Contains(buf.String(), "fallback-secret") {
		t.Fatalf("expected fallback event secret to be redacted, got %q", buf.String())
	}
}
