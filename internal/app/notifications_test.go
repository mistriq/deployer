package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func notificationTestConfig(t *testing.T) {
	t.Helper()
	old := appConfig
	appConfig.NotificationKey = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{42}, 32))
	appConfig.PublicURL = "https://deployer.example.com"
	t.Cleanup(func() { appConfig = old })
}

func TestNotificationEncryptionAndAddressPolicy(t *testing.T) {
	notificationTestConfig(t)
	endpoint := "https://hooks.example.com/a-private-token"
	encrypted, err := encryptNotificationEndpoint(endpoint)
	if err != nil || strings.Contains(encrypted, "a-private-token") {
		t.Fatalf("encryption: %v %s", err, encrypted)
	}
	plain, err := decryptNotificationEndpoint(encrypted)
	if err != nil || plain != endpoint {
		t.Fatalf("decrypt: %v %s", err, plain)
	}
	raw, _ := base64.StdEncoding.DecodeString(encrypted)
	raw[len(raw)-1] ^= 1
	if _, err := decryptNotificationEndpoint(base64.StdEncoding.EncodeToString(raw)); err == nil {
		t.Fatal("tampered ciphertext accepted")
	}
	for _, address := range []string{"127.0.0.1", "::1", "10.0.0.1", "172.16.1.1", "192.168.1.1", "169.254.169.254", "100.64.0.1", "0.0.0.0", "2001:db8::1", "::ffff:127.0.0.1"} {
		if publicNotificationIP(net.ParseIP(address)) {
			t.Fatalf("private/reserved address accepted: %s", address)
		}
	}
	if !publicNotificationIP(net.ParseIP("1.1.1.1")) {
		t.Fatal("public address rejected")
	}
	for _, endpoint := range []string{"http://example.com/hook", "https://user:password@example.com/hook", "https://example.com:8443/hook"} {
		c := notificationChannel{Name: "Test", Kind: "webhook", Events: []string{"failed"}, Endpoint: endpoint}
		if validateNotificationChannel(&c, false) == nil {
			t.Fatalf("invalid endpoint accepted: %s", endpoint)
		}
	}
	client := notificationHTTPClient()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := client.Transport.(*http.Transport).DialContext(ctx, "tcp", "127.0.0.1:443"); err == nil {
		t.Fatal("loopback dial accepted")
	}
}

func TestPersonalChannelsQueueRetriesAndBrowserOwnership(t *testing.T) {
	oldDB := db
	u := isolatedPostgres(t, postgresTestURL(t))
	if err := initPostgres(u); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(); db = oldDB })
	notificationTestConfig(t)
	p := &Project{Name: "A project", RepoPath: t.TempDir(), DeployDir: t.TempDir(), DeployMode: "files"}
	if err := createProject(p); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/notifications/channels", handleNotificationChannels)
	mux.HandleFunc("/api/notifications/channels/", handleNotificationChannels)
	mux.HandleFunc("/api/notifications/inbox", handleBrowserNotificationInbox)
	mux.HandleFunc("/api/notifications/ack", handleBrowserNotificationAck)
	handler := wrapHTTPHandler(mux)
	request := func(owner, method, path string, payload interface{}) *httptest.ResponseRecorder {
		var body io.Reader
		if payload != nil {
			raw, _ := json.Marshal(payload)
			body = bytes.NewReader(raw)
		}
		r := httptest.NewRequest(method, path, body)
		r.RemoteAddr = "127.0.0.1:43210"
		r.Header.Set("X-Deployer-User", owner)
		r.Header.Set(csrfHeader, "1")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if w := request("", http.MethodGet, "/api/notifications/channels", nil); w.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous: %d", w.Code)
	}
	external := httptest.NewRequest(http.MethodGet, "/api/notifications/channels", nil)
	external.Header.Set("X-Deployer-User", "alice")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, external)
	if w.Code != http.StatusUnauthorized {
		t.Fatal("untrusted identity header accepted")
	}
	create := func(owner string, c notificationChannel) int64 {
		w := request(owner, http.MethodPost, "/api/notifications/channels", c)
		if w.Code != http.StatusOK {
			t.Fatalf("create: %d %s", w.Code, w.Body.String())
		}
		var result struct {
			ID int64 `json:"id"`
		}
		json.Unmarshal(w.Body.Bytes(), &result)
		return result.ID
	}
	webhook := notificationChannel{Name: "Team hook", Kind: "webhook", Endpoint: "https://hooks.example.com/private-hook-token", Events: []string{"failed"}, Enabled: true}
	hookID := create("alice", webhook)
	device := "browser-device-123456789"
	browserID := create("alice", notificationChannel{Name: "Browser", Kind: "browser", BrowserID: device, Events: []string{"failed"}, Enabled: true})
	for i := 0; i < 8; i++ {
		create("bob", notificationChannel{Name: fmt.Sprintf("Channel %d", i), Kind: "browser", BrowserID: "other-browser-device-123456", Events: []string{"success"}, Enabled: true})
	}
	w = request("alice", http.MethodGet, "/api/notifications/channels", nil)
	if strings.Contains(w.Body.String(), "private-hook-token") || strings.Contains(w.Body.String(), "Channel 0") {
		t.Fatalf("channel data leak: %s", w.Body.String())
	}
	webhook.Endpoint = ""
	w = request("alice", http.MethodPut, fmt.Sprintf("/api/notifications/channels/%d", hookID), webhook)
	if w.Code != http.StatusOK {
		t.Fatalf("preserve URL: %s", w.Body.String())
	}
	w = request("bob", http.MethodPut, fmt.Sprintf("/api/notifications/channels/%d", hookID), webhook)
	if w.Code != http.StatusNotFound {
		t.Fatalf("cross-user update: %d", w.Code)
	}
	t.Setenv("DEPLOYER_MCP_READ_TOKEN", "machine-read")
	machine := httptest.NewRequest(http.MethodGet, "/api/notifications/channels", nil)
	machine.RemoteAddr = "127.0.0.1:43210"
	machine.Header.Set("X-Deployer-User", "alice")
	machine.Header.Set("Authorization", "Bearer machine-read")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, machine)
	if w.Code != http.StatusUnauthorized {
		t.Fatal("machine read accessed personal channels")
	}
	b, err := createBuild(p.ID, "manual")
	if err != nil {
		t.Fatal(err)
	}
	b.Status = "failed"
	now := time.Now().UTC()
	b.FinishedAt = &now
	b.ErrorMessage = "private failure details"
	if err := updateBuild(b); err != nil {
		t.Fatal(err)
	}
	if err := queueBuildNotifications(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := queueBuildNotifications(t.Context()); err != nil {
		t.Fatal(err)
	}
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM notification_deliveries WHERE build_id=?`, b.ID).Scan(&count)
	if count != 2 {
		t.Fatalf("expected exactly two routed deliveries, got %d", count)
	}
	var delivery *notificationDelivery
	var mu sync.Mutex
	var wg sync.WaitGroup
	claims := 0
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := claimNotificationDelivery(t.Context())
			if err != nil {
				t.Error(err)
				return
			}
			if d != nil {
				mu.Lock()
				claims++
				delivery = d
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if claims != 1 {
		t.Fatalf("claimed %d times", claims)
	}
	client := &http.Client{Transport: notificationRoundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/private-hook-token" || r.Header.Get("Idempotency-Key") == "" {
			t.Error("missing endpoint or durable delivery key")
		}
		raw, _ := io.ReadAll(r.Body)
		if strings.Contains(string(raw), "private failure details") {
			t.Error("failure details sent to webhook")
		}
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("private response token")), Header: make(http.Header)}, nil
	})}
	code, sendErr := deliverNotification(t.Context(), client, delivery)
	if code != 503 || sendErr == nil {
		t.Fatalf("send result: %d %v", code, sendErr)
	}
	if err := finishNotificationDelivery(t.Context(), delivery, code, sendErr); err != nil {
		t.Fatal(err)
	}
	var state, message string
	db.QueryRow(`SELECT status,last_error FROM notification_deliveries WHERE id=?`, delivery.ID).Scan(&state, &message)
	if state != "retry" || strings.Contains(message, "private response token") {
		t.Fatalf("retry state: %s %s", state, message)
	}
	current, _ := getBuild(b.ID)
	if current.Status != "failed" {
		t.Fatal("delivery changed build outcome")
	}
	w = request("alice", http.MethodGet, "/api/notifications/inbox?browser_id="+device, nil)
	var inbox []struct {
		ID int64 `json:"id"`
	}
	json.Unmarshal(w.Body.Bytes(), &inbox)
	if len(inbox) != 1 {
		t.Fatalf("browser inbox: %s", w.Body.String())
	}
	w = request("bob", http.MethodPost, "/api/notifications/ack", map[string]interface{}{"id": inbox[0].ID, "browser_id": device})
	if w.Code != http.StatusOK {
		t.Fatalf("ack: %d", w.Code)
	}
	db.QueryRow(`SELECT status FROM notification_deliveries WHERE id=?`, inbox[0].ID).Scan(&state)
	if state != "browser_pending" {
		t.Fatal("cross-user ack modified delivery")
	}
	request("alice", http.MethodPost, "/api/notifications/ack", map[string]interface{}{"id": inbox[0].ID, "browser_id": device})
	db.QueryRow(`SELECT status FROM notification_deliveries WHERE id=?`, inbox[0].ID).Scan(&state)
	if state != "delivered" {
		t.Fatal("browser ack did not persist")
	}
	if w := request("bob", http.MethodDelete, fmt.Sprintf("/api/notifications/channels/%d", browserID), nil); w.Code != http.StatusNotFound {
		t.Fatal("cross-user delete succeeded")
	}
	if _, err := db.Exec(`UPDATE notification_deliveries SET status='sending',attempts=5,lease_until=CURRENT_TIMESTAMP-INTERVAL '1 second' WHERE id=?`, delivery.ID); err != nil {
		t.Fatal(err)
	}
	if d, err := claimNotificationDelivery(t.Context()); err != nil || d != nil {
		t.Fatalf("attempt limit ignored: %+v %v", d, err)
	}
}

type notificationRoundTrip func(*http.Request) (*http.Response, error)

func (f notificationRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestNotificationProvidersPreventMentionsAndBoundRetries(t *testing.T) {
	raw := json.RawMessage(`{"title":"@everyone deploy failed","build_url":"https://example.com/builds/1"}`)
	discord, err := notificationPayload("discord", raw)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(discord), `"parse":[]`) {
		t.Fatal("Discord mentions enabled")
	}
	slack, err := notificationPayload("slack", raw)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(slack), `"type":"plain_text"`) {
		t.Fatal("Slack markup enabled")
	}
}
