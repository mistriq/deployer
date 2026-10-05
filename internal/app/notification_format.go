package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type notificationMessage struct {
	Title           string `json:"title"`
	ProjectName     string `json:"project_name"`
	ProjectID       int64  `json:"project_id"`
	BuildID         int64  `json:"build_id"`
	Status          string `json:"status"`
	BuildURL        string `json:"build_url"`
	CommitSHA       string `json:"commit_sha"`
	TriggeredBy     string `json:"triggered_by"`
	DurationSeconds *int64 `json:"duration_seconds"`
	FinishedAt      string `json:"finished_at"`
}

type notificationField struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func notificationPayload(kind string, raw json.RawMessage) ([]byte, error) {
	if kind == "webhook" {
		return raw, nil
	}
	var p notificationMessage
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	switch kind {
	case "discord":
		return discordNotificationPayload(p)
	case "slack":
		return slackNotificationPayload(p)
	default:
		return nil, errors.New("unsupported notification format")
	}
}

func notificationOutcome(status string) (string, string, int) {
	switch status {
	case "success":
		return "✅ Deployment succeeded", "The deployment completed successfully.", 0x22c55e
	case "failed":
		return "❌ Deployment failed", "Open the build for logs and the AI diagnostic prompt.", 0xef4444
	case "cancelled":
		return "⏹️ Deployment cancelled", "The deployment was cancelled.", 0xf59e0b
	case "test":
		return "🔔 Notifications connected", "Your channel is ready to receive deployment alerts.", 0x5865f2
	default:
		return "🔔 Deployment update", "", 0x5865f2
	}
}

func (p notificationMessage) projectLabel() string {
	if strings.TrimSpace(p.ProjectName) != "" {
		return notificationText(p.ProjectName, 256)
	}
	if strings.TrimSpace(p.Title) != "" {
		return notificationText(p.Title, 256)
	}
	return "Deployment notification"
}

func (p notificationMessage) fields() []notificationField {
	fields := []notificationField{}
	if p.Status == "test" {
		return fields
	}
	if p.BuildID > 0 {
		fields = append(fields, notificationField{"Build", fmt.Sprintf("#%d", p.BuildID)})
	}
	if p.DurationSeconds != nil && *p.DurationSeconds >= 0 {
		seconds := *p.DurationSeconds
		duration := fmt.Sprintf("%ds", seconds%60)
		if seconds >= 3600 {
			duration = fmt.Sprintf("%dh %dm %ds", seconds/3600, seconds/60%60, seconds%60)
		} else if seconds >= 60 {
			duration = fmt.Sprintf("%dm %ds", seconds/60, seconds%60)
		}
		fields = append(fields, notificationField{"Duration", duration})
	}
	if revision := strings.TrimSpace(p.CommitSHA); revision != "" {
		// Keep real commit identifiers compact without hiding arbitrary text as a
		// seemingly valid hash. Project data is never trusted as chat markup.
		isHash := len(revision) == 40 || len(revision) == 64
		for _, r := range revision {
			if !strings.ContainsRune("0123456789abcdefABCDEF", r) {
				isHash = false
				break
			}
		}
		if isHash {
			revision = revision[:12]
		}
		fields = append(fields, notificationField{"Revision", notificationText(revision, 128)})
	}
	if trigger := notificationText(p.TriggeredBy, 128); trigger != "" {
		fields = append(fields, notificationField{"Triggered by", trigger})
	}
	return fields
}

func discordNotificationPayload(p notificationMessage) ([]byte, error) {
	title, description, color := notificationOutcome(p.Status)
	if p.Status != "test" {
		description = "**" + discordNotificationText(p.projectLabel(), 512) + "**\n" + description
	}
	embed := map[string]interface{}{
		"title": title, "description": description, "color": color,
		"footer": map[string]string{"text": "Deployer"},
	}
	if link := notificationLink(p.BuildURL); link != "" {
		embed["url"] = link
	}
	if when, err := time.Parse(time.RFC3339, p.FinishedAt); err == nil {
		embed["timestamp"] = when.UTC().Format(time.RFC3339)
	}
	fields := []map[string]interface{}{}
	for _, field := range p.fields() {
		fields = append(fields, map[string]interface{}{
			"name": field.Name, "value": discordNotificationText(field.Value, 1024), "inline": true,
		})
	}
	if len(fields) > 0 {
		embed["fields"] = fields
	}
	return json.Marshal(map[string]interface{}{
		"embeds":           []interface{}{embed},
		"allowed_mentions": map[string]interface{}{"parse": []string{}},
	})
}

func slackNotificationPayload(p notificationMessage) ([]byte, error) {
	title, description, _ := notificationOutcome(p.Status)
	plain := func(text string) map[string]interface{} {
		return map[string]interface{}{"type": "plain_text", "text": text, "emoji": true}
	}
	body := description
	if p.Status != "test" {
		body = p.projectLabel() + "\n" + description
	}
	blocks := []interface{}{
		map[string]interface{}{"type": "header", "text": plain(title)},
		map[string]interface{}{"type": "section", "text": plain(body)},
	}
	fields := []interface{}{}
	for _, field := range p.fields() {
		fields = append(fields, plain(field.Name+"\n"+field.Value))
	}
	if len(fields) > 0 {
		blocks = append(blocks, map[string]interface{}{"type": "section", "fields": fields})
	}
	if link := notificationLink(p.BuildURL); link != "" {
		label := "View build"
		if p.Status == "test" {
			label = "Notification settings"
		}
		// Only this application-generated link uses mrkdwn. All project data is
		// plain_text, preventing mentions and injected formatting.
		link = strings.NewReplacer("&", "&amp;", "<", "%3C", ">", "%3E", "|", "%7C").Replace(link)
		blocks = append(blocks, map[string]interface{}{"type": "section", "text": map[string]interface{}{
			"type": "mrkdwn", "text": "<" + link + "|" + label + ">", "verbatim": true,
		}})
	}
	blocks = append(blocks, map[string]interface{}{"type": "context", "elements": []interface{}{plain("Deployer")}})
	return json.Marshal(map[string]interface{}{
		"text": title + " · " + p.projectLabel(), "mrkdwn": false,
		"unfurl_links": false, "unfurl_media": false, "blocks": blocks,
	})
}

func notificationLink(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || raw == "" {
		return ""
	}
	if !u.IsAbs() {
		base, err := url.Parse(appConfig.PublicURL)
		if err != nil || !base.IsAbs() {
			return ""
		}
		u = base.ResolveReference(u)
	}
	if (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" || u.User != nil || len(u.String()) > 2048 {
		return ""
	}
	return u.String()
}

func discordNotificationText(text string, limit int) string {
	escaped := strings.NewReplacer("\\", "\\\\", "`", "\\`", "*", "\\*", "_", "\\_", "~", "\\~", "|", "\\|", "<", "\\<", ">", "\\>", "[", "\\[", "]", "\\]").Replace(text)
	return notificationText(escaped, limit)
}

func notificationText(text string, limit int) string {
	text = strings.TrimSpace(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, text))
	// A byte bound is conservative for both Unicode character and UTF-16
	// limits. Preserve valid UTF-8 and include the ellipsis within the bound.
	if len(text) > limit {
		end := limit - len("…")
		for end > 0 && !utf8.RuneStart(text[end]) {
			end--
		}
		text = text[:end] + "…"
	}
	return text
}
