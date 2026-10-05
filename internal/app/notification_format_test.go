package app

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"
)

type discordTestEmbed struct {
	Title, Description, URL, Timestamp string
	Color                              int
	Fields                             []struct {
		Name, Value string
		Inline      bool
	}
	Footer struct{ Text string }
}

func decodeDiscordNotification(t *testing.T, raw []byte) discordTestEmbed {
	t.Helper()
	var payload struct {
		Content         string
		Embeds          []discordTestEmbed
		AllowedMentions struct{ Parse []string } `json:"allowed_mentions"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Embeds) != 1 || payload.Content != "" || payload.AllowedMentions.Parse == nil || len(payload.AllowedMentions.Parse) != 0 {
		t.Fatalf("expected one embed, no duplicated content and disabled mentions: %s", raw)
	}
	return payload.Embeds[0]
}

func TestDiscordNotificationsRenderDeploymentOutcomes(t *testing.T) {
	for _, tc := range []struct {
		status, label string
		color         int
	}{
		{"success", "succeeded", 0x22c55e},
		{"failed", "failed", 0xef4444},
		{"cancelled", "cancelled", 0xf59e0b},
	} {
		t.Run(tc.status, func(t *testing.T) {
			raw, _ := json.Marshal(map[string]interface{}{
				"project_name": "My website", "status": tc.status, "build_id": 42,
				"commit_sha": "0123456789abcdef0123456789abcdef01234567", "duration_seconds": 125,
				"triggered_by": "manual", "build_url": "https://deployer.example.com/builds/42",
				"finished_at": "2026-10-05T09:30:00+02:00",
			})
			formatted, err := notificationPayload("discord", raw)
			if err != nil {
				t.Fatal(err)
			}
			embed := decodeDiscordNotification(t, formatted)
			if embed.Color != tc.color || !strings.Contains(embed.Title, tc.label) || !strings.Contains(embed.Description, "My website") {
				t.Fatalf("missing outcome or project: %+v", embed)
			}
			if embed.URL != "https://deployer.example.com/builds/42" || embed.Timestamp != "2026-10-05T07:30:00Z" {
				t.Fatalf("incorrect link/completion timestamp: %+v", embed)
			}
			fields := map[string]string{}
			for _, field := range embed.Fields {
				fields[field.Name] = field.Value
				if !field.Inline {
					t.Fatal("deployment details are not compact inline fields")
				}
			}
			if fields["Build"] != "#42" || fields["Duration"] != "2m 5s" || fields["Revision"] != "0123456789ab" || fields["Triggered by"] != "manual" {
				t.Fatalf("deployment context missing: %+v", fields)
			}
		})
	}
}

func TestRichNotificationsBoundUntrustedValuesAndKeepLinksSafe(t *testing.T) {
	notificationTestConfig(t)
	raw, _ := json.Marshal(map[string]interface{}{
		"project_name": "@everyone **fake** [link](https://evil.example) " + strings.Repeat("💥", 1000),
		"status":       "failed", "commit_sha": strings.Repeat("`", 4000),
		"triggered_by": "<!channel>\n" + strings.Repeat("*", 4000),
		"build_url":    "javascript:alert(1)", "finished_at": "invalid timestamp",
	})
	formatted, err := notificationPayload("discord", raw)
	if err != nil {
		t.Fatal(err)
	}
	embed := decodeDiscordNotification(t, formatted)
	if embed.URL != "" || embed.Timestamp != "" || strings.Contains(embed.Description, "**fake**") || !strings.Contains(embed.Description, `\*\*fake\*\*`) {
		t.Fatalf("unsafe link, timestamp or project markup: %+v", embed)
	}
	// Enforce Discord's per-field and 6000-character aggregate limits using
	// stricter byte counts, including expanded escaping and multibyte text.
	total := len(embed.Title) + len(embed.Description) + len(embed.Footer.Text)
	if len(embed.Title) > 256 || len(embed.Description) > 4096 || len(embed.Fields) > 25 {
		t.Fatal("Discord embed exceeds published limits")
	}
	for _, field := range embed.Fields {
		if len(field.Name) > 256 || len(field.Value) > 1024 || !utf8.ValidString(field.Value) {
			t.Fatal("Discord field exceeds limits or contains broken UTF-8")
		}
		total += len(field.Name) + len(field.Value)
	}
	if total > 6000 || !utf8.ValidString(embed.Description) {
		t.Fatal("Discord aggregate limit or UTF-8 violation")
	}
	formatted, err = notificationPayload("slack", raw)
	if err != nil {
		t.Fatal(err)
	}
	var slack struct {
		Text   string
		Mrkdwn bool
		Blocks []struct {
			Type   string
			Text   struct{ Type, Text string }
			Fields []struct{ Type, Text string }
		}
	}
	if err := json.Unmarshal(formatted, &slack); err != nil {
		t.Fatal(err)
	}
	if slack.Mrkdwn || len(slack.Blocks) < 3 || slack.Blocks[0].Type != "header" {
		t.Fatal("Slack heading missing or fallback markup enabled")
	}
	for _, block := range slack.Blocks {
		if block.Text.Type == "mrkdwn" {
			t.Fatal("unsafe build URL became a Slack link")
		}
		for _, field := range block.Fields {
			if field.Type != "plain_text" || len(field.Text) > 2000 {
				t.Fatal("Slack project data became markup or exceeded field limits")
			}
		}
	}
	if !strings.Contains(slack.Text, "@everyone") {
		t.Fatal("plain project text unexpectedly lost")
	}
}

func TestNotificationTestsAndLegacyQueuedPayloads(t *testing.T) {
	notificationTestConfig(t)
	formatted, err := notificationPayload("discord", json.RawMessage(`{"title":"Deployer test notification","status":"test","build_url":"/notifications"}`))
	if err != nil {
		t.Fatal(err)
	}
	embed := decodeDiscordNotification(t, formatted)
	if embed.Color != 0x5865f2 || !strings.Contains(embed.Title, "connected") || len(embed.Fields) != 0 || embed.URL != "https://deployer.example.com/notifications" {
		t.Fatalf("test notification has fake deployment context or a broken link: %+v", embed)
	}
	legacy := json.RawMessage(`{"title":"Existing queued notification","build_url":"https://example.com/builds/1"}`)
	formatted, err = notificationPayload("discord", legacy)
	if err != nil || !strings.Contains(string(formatted), "Existing queued notification") {
		t.Fatalf("legacy delivery cannot be formatted: %s %v", formatted, err)
	}
	webhook, err := notificationPayload("webhook", legacy)
	if err != nil || string(webhook) != string(legacy) {
		t.Fatal("generic webhook payload changed during formatting")
	}
	formatted, err = notificationPayload("slack", json.RawMessage(`{"status":"test","build_url":"/notifications"}`))
	if err != nil || !strings.Contains(string(formatted), "Notification settings") || !strings.Contains(string(formatted), "https://deployer.example.com/notifications") {
		t.Fatalf("Slack test link missing: %s %v", formatted, err)
	}
}

func TestDiscordDeliverySendsEmbedWithConfirmation(t *testing.T) {
	notificationTestConfig(t)
	endpoint, err := encryptNotificationEndpoint("https://discord.com/api/webhooks/123/example-test-token")
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: notificationRoundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.Query().Get("wait") != "true" || r.Header.Get("Idempotency-Key") != "deployer-notification-42" {
			t.Error("Discord confirmation or delivery identity missing")
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		embed := decodeDiscordNotification(t, raw)
		if embed.Color != 0xef4444 || embed.URL != "https://deployer.example.com/builds/42" {
			t.Error("actual webhook request did not contain the failure card")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"id":"123"}`)), Header: make(http.Header)}, nil
	})}
	delivery := &notificationDelivery{ID: 42, Kind: "discord", EndpointCipher: endpoint, Payload: json.RawMessage(`{"status":"failed","project_name":"My website","build_id":42,"build_url":"https://deployer.example.com/builds/42"}`)}
	if code, err := deliverNotification(t.Context(), client, delivery); err != nil || code != 200 {
		t.Fatalf("Discord delivery failed: %d %v", code, err)
	}
}
