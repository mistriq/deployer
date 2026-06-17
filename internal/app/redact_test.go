package app

import (
	"strings"
	"testing"
)

func TestRedactSecrets(t *testing.T) {
	input := strings.Join([]string{
		"Authorization: Bearer abc123",
		"DEPLOYER_TOKEN=secret-token",
		"https://example.test/api?token=query-secret",
		"deployer agent --token flag-secret",
	}, "\n")

	got := redactSecrets(input)
	for _, secret := range []string{"abc123", "secret-token", "query-secret", "flag-secret"} {
		if strings.Contains(got, secret) {
			t.Fatalf("expected %q to be redacted from %q", secret, got)
		}
	}
	if count := strings.Count(got, "[REDACTED]"); count != 4 {
		t.Fatalf("expected 4 redactions, got %d in %q", count, got)
	}
}
