package app

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type notificationChannel struct {
	ID                 int64    `json:"id"`
	Name               string   `json:"name"`
	Kind               string   `json:"kind"`
	BrowserID          string   `json:"browser_id"`
	ProjectIDs         []int64  `json:"project_ids"`
	Events             []string `json:"events"`
	Enabled            bool     `json:"enabled"`
	EndpointConfigured bool     `json:"endpoint_configured"`
	Endpoint           string   `json:"endpoint,omitempty"`
	endpointCipher     string
}

func handleNotificationsPage(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	projects, err := listProjects()
	if err != nil {
		http.Error(w, "Could not load notification settings", http.StatusInternalServerError)
		return
	}
	tmpl.ExecuteTemplate(w, "notifications.html", map[string]interface{}{"Projects": projects})
}

// Interactive identity is asserted by the loopback authorization gateway. It
// must overwrite X-Deployer-User with OIDC's stable sub claim. Machine tokens
// never grant access to another person's notification settings.
func notificationOwner(r *http.Request) (string, bool) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	ip := net.ParseIP(host)
	owner := strings.TrimSpace(r.Header.Get("X-Deployer-User"))
	machine, _ := r.Context().Value(mcpBearerContextKey{}).(bool)
	if err != nil || ip == nil || !ip.IsLoopback() || machine || r.Header.Get("Authorization") != "" || owner == "" || len(owner) > 512 {
		return "", false
	}
	return owner, true
}

func notificationCipher() (cipher.AEAD, error) {
	key, err := base64.StdEncoding.DecodeString(appConfig.NotificationKey)
	if err != nil || len(key) != 32 {
		return nil, errors.New("notification encryption key is not configured")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.New("notification encryption key is invalid")
	}
	return cipher.NewGCM(block)
}

func encryptNotificationEndpoint(endpoint string) (string, error) {
	if endpoint == "" {
		return "", nil
	}
	aead, err := notificationCipher()
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(aead.Seal(nonce, nonce, []byte(endpoint), []byte("deployer-notification-v1"))), nil
}

func decryptNotificationEndpoint(encoded string) (string, error) {
	aead, err := notificationCipher()
	if err != nil {
		return "", err
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(raw) < aead.NonceSize() {
		return "", errors.New("invalid encrypted notification endpoint")
	}
	plain, err := aead.Open(nil, raw[:aead.NonceSize()], raw[aead.NonceSize():], []byte("deployer-notification-v1"))
	if err != nil {
		return "", errors.New("could not decrypt notification endpoint")
	}
	return string(plain), nil
}

func validateNotificationChannel(c *notificationChannel, editing bool) error {
	c.Name = strings.TrimSpace(c.Name)
	if c.Name == "" || len(c.Name) > 160 {
		return errors.New("channel name must contain 1 to 160 characters")
	}
	if len(c.Events) == 0 {
		return errors.New("select at least one outcome")
	}
	for _, event := range c.Events {
		if event != "success" && event != "failed" && event != "cancelled" {
			return errors.New("unsupported notification outcome")
		}
	}
	for _, id := range c.ProjectIDs {
		if id < 1 {
			return errors.New("project IDs must be positive")
		}
	}
	if c.Kind == "browser" {
		if len(c.BrowserID) < 16 || len(c.BrowserID) > 100 {
			return errors.New("a browser device ID is required")
		}
		c.Endpoint = ""
		return nil
	}
	if c.Kind != "discord" && c.Kind != "slack" && c.Kind != "webhook" {
		return errors.New("unsupported notification channel")
	}
	c.BrowserID = ""
	if editing && c.Endpoint == "" {
		return nil
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || len(c.Endpoint) > 4096 {
		return errors.New("provide an HTTPS webhook URL without a username, password, or fragment")
	}
	if c.Kind == "discord" && (u.Hostname() != "discord.com" || !strings.HasPrefix(u.Path, "/api/webhooks/")) {
		return errors.New("provide a Discord webhook URL from discord.com/api/webhooks")
	}
	if c.Kind == "slack" && (u.Hostname() != "hooks.slack.com" || !strings.HasPrefix(u.Path, "/services/")) {
		return errors.New("provide a Slack incoming webhook URL from hooks.slack.com/services")
	}
	if u.Port() != "" && u.Port() != "443" {
		return errors.New("webhooks must use HTTPS port 443")
	}
	return nil
}

func handleNotificationChannels(w http.ResponseWriter, r *http.Request) {
	owner, ok := notificationOwner(r)
	if !ok {
		jsonErrorCode(w, errCodeAuthenticationRequired, "sign in through the authorization gateway to manage personal notifications", http.StatusUnauthorized)
		return
	}
	if db.dialect != "postgres" {
		jsonErrorCode(w, errCodeInternal, "notifications require PostgreSQL", http.StatusServiceUnavailable)
		return
	}
	id, suffix := int64(0), ""
	if r.URL.Path != "/api/notifications/channels" {
		var valid bool
		id, suffix, valid = parseIDPath(r.URL.Path, "/api/notifications/channels/")
		if !valid {
			http.NotFound(w, r)
			return
		}
	}
	if suffix == "deliveries" && id > 0 {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		handleNotificationHistory(w, r, owner, id)
		return
	}
	if suffix == "test" && id > 0 {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		var deliveryID int64
		err := db.QueryRow(`INSERT INTO notification_deliveries(channel_id,payload,status)
		 SELECT id,?, CASE WHEN kind='browser' THEN 'browser_pending' ELSE 'queued' END
		 FROM notification_channels WHERE id=? AND owner_id=? AND enabled=TRUE RETURNING id`,
			`{"title":"Deployer test notification","status":"test","project_name":"Notification test","build_url":"/notifications"}`, id, owner).Scan(&deliveryID)
		if errors.Is(err, sql.ErrNoRows) {
			jsonErrorCode(w, errCodeNotFound, "enabled channel not found", http.StatusNotFound)
			return
		}
		if err != nil {
			jsonErrorCode(w, errCodeInternal, "could not queue test notification", http.StatusInternalServerError)
			return
		}
		jsonResponse(w, map[string]interface{}{"delivery_id": deliveryID, "status": "queued"})
		return
	}
	if suffix != "" {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		if id != 0 {
			http.NotFound(w, r)
			return
		}
		rows, err := db.Query(`SELECT id,name,kind,browser_id,project_ids,events,enabled,endpoint_cipher FROM notification_channels WHERE owner_id=? ORDER BY id`, owner)
		if err != nil {
			jsonErrorCode(w, errCodeInternal, "could not load channels", http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		channels := []notificationChannel{}
		for rows.Next() {
			var c notificationChannel
			var projects, events []byte
			if err := rows.Scan(&c.ID, &c.Name, &c.Kind, &c.BrowserID, &projects, &events, &c.Enabled, &c.endpointCipher); err != nil {
				jsonErrorCode(w, errCodeInternal, "could not load channels", http.StatusInternalServerError)
				return
			}
			if json.Unmarshal(projects, &c.ProjectIDs) != nil || json.Unmarshal(events, &c.Events) != nil {
				jsonErrorCode(w, errCodeInternal, "invalid stored channel", http.StatusInternalServerError)
				return
			}
			c.EndpointConfigured = c.endpointCipher != ""
			channels = append(channels, c)
		}
		if rows.Err() != nil {
			jsonErrorCode(w, errCodeInternal, "could not load channels", http.StatusInternalServerError)
			return
		}
		jsonResponse(w, map[string]interface{}{"channels": channels, "encryption_configured": appConfig.NotificationKey != ""})
	case http.MethodPost, http.MethodPut:
		if (r.Method == http.MethodPost && id != 0) || (r.Method == http.MethodPut && id == 0) {
			jsonErrorCode(w, errCodeMethodNotAllowed, "unsupported method", http.StatusMethodNotAllowed)
			return
		}
		var c notificationChannel
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		if json.NewDecoder(r.Body).Decode(&c) != nil {
			jsonErrorCode(w, errCodeValidation, "invalid channel JSON", http.StatusBadRequest)
			return
		}
		if err := validateNotificationChannel(&c, id > 0); err != nil {
			jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
			return
		}
		if c.ProjectIDs == nil {
			c.ProjectIDs = []int64{}
		}
		for _, projectID := range c.ProjectIDs {
			if _, err := getProject(projectID); err != nil {
				jsonErrorCode(w, errCodeValidation, "selected project does not exist", http.StatusBadRequest)
				return
			}
		}
		projects, _ := json.Marshal(c.ProjectIDs)
		events, _ := json.Marshal(c.Events)
		var oldKind, encrypted string
		if id > 0 {
			err := db.QueryRow(`SELECT kind,endpoint_cipher FROM notification_channels WHERE id=? AND owner_id=?`, id, owner).Scan(&oldKind, &encrypted)
			if errors.Is(err, sql.ErrNoRows) {
				jsonErrorCode(w, errCodeNotFound, "channel not found", http.StatusNotFound)
				return
			}
			if err != nil {
				jsonErrorCode(w, errCodeInternal, "could not load channel", http.StatusInternalServerError)
				return
			}
			if oldKind != c.Kind {
				jsonErrorCode(w, errCodeValidation, "create a new channel to use a different channel type", http.StatusBadRequest)
				return
			}
		}
		if c.Kind == "browser" {
			encrypted = ""
		} else if c.Endpoint != "" {
			var err error
			encrypted, err = encryptNotificationEndpoint(c.Endpoint)
			if err != nil {
				jsonErrorCode(w, errCodeInternal, "notification encryption is not configured", http.StatusServiceUnavailable)
				return
			}
		}
		var err error
		if id == 0 {
			id, err = insertID(`INSERT INTO notification_channels(owner_id,name,kind,browser_id,project_ids,events,enabled,endpoint_cipher) VALUES(?,?,?,?,?,?,?,?)`, owner, c.Name, c.Kind, c.BrowserID, string(projects), string(events), c.Enabled, encrypted)
		} else {
			_, err = db.Exec(`UPDATE notification_channels SET name=?,kind=?,browser_id=?,project_ids=?,events=?,enabled=?,endpoint_cipher=?,updated_at=CURRENT_TIMESTAMP WHERE id=? AND owner_id=?`, c.Name, c.Kind, c.BrowserID, string(projects), string(events), c.Enabled, encrypted, id, owner)
		}
		if err != nil {
			jsonErrorCode(w, errCodeInternal, "could not save channel", http.StatusInternalServerError)
			return
		}
		jsonResponse(w, map[string]interface{}{"id": id})
	case http.MethodDelete:
		if id == 0 {
			jsonErrorCode(w, errCodeValidation, "channel ID is required", http.StatusBadRequest)
			return
		}
		result, err := db.Exec(`DELETE FROM notification_channels WHERE id=? AND owner_id=?`, id, owner)
		if err != nil {
			jsonErrorCode(w, errCodeInternal, "could not delete channel", http.StatusInternalServerError)
			return
		}
		count, _ := result.RowsAffected()
		if count == 0 {
			jsonErrorCode(w, errCodeNotFound, "channel not found", http.StatusNotFound)
			return
		}
		jsonResponse(w, map[string]string{"status": "deleted"})
	default:
		jsonErrorCode(w, errCodeMethodNotAllowed, "unsupported method", http.StatusMethodNotAllowed)
	}
}

func handleNotificationHistory(w http.ResponseWriter, r *http.Request, owner string, id int64) {
	rows, err := db.Query(`SELECT d.id,d.build_id,d.status,d.attempts,d.last_error,d.created_at,d.delivered_at
	 FROM notification_deliveries d JOIN notification_channels c ON c.id=d.channel_id WHERE c.id=? AND c.owner_id=? ORDER BY d.id DESC LIMIT 50`, id, owner)
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "could not load deliveries", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	items := []map[string]interface{}{}
	for rows.Next() {
		var deliveryID int64
		var buildID sql.NullInt64
		var status, lastError string
		var attempts int
		var created time.Time
		var delivered sql.NullTime
		if rows.Scan(&deliveryID, &buildID, &status, &attempts, &lastError, &created, &delivered) != nil {
			jsonErrorCode(w, errCodeInternal, "could not load deliveries", http.StatusInternalServerError)
			return
		}
		item := map[string]interface{}{"id": deliveryID, "status": status, "attempts": attempts, "last_error": lastError, "created_at": created}
		if buildID.Valid {
			item["build_id"] = buildID.Int64
		}
		if delivered.Valid {
			item["delivered_at"] = delivered.Time
		}
		items = append(items, item)
	}
	if rows.Err() != nil {
		jsonErrorCode(w, errCodeInternal, "could not load deliveries", http.StatusInternalServerError)
		return
	}
	jsonResponse(w, items)
}

func handleBrowserNotificationInbox(w http.ResponseWriter, r *http.Request) {
	owner, ok := notificationOwner(r)
	if !ok {
		jsonErrorCode(w, errCodeAuthenticationRequired, "interactive sign-in required", http.StatusUnauthorized)
		return
	}
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	browser := r.URL.Query().Get("browser_id")
	if len(browser) < 16 || len(browser) > 100 {
		jsonErrorCode(w, errCodeValidation, "browser ID is required", http.StatusBadRequest)
		return
	}
	rows, err := db.Query(`SELECT d.id,d.payload FROM notification_deliveries d JOIN notification_channels c ON c.id=d.channel_id
	 WHERE c.owner_id=? AND c.kind='browser' AND c.browser_id=? AND c.enabled=TRUE AND d.status='browser_pending' ORDER BY d.id LIMIT 50`, owner, browser)
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "could not load notifications", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	items := []map[string]interface{}{}
	for rows.Next() {
		var id int64
		var payload json.RawMessage
		if rows.Scan(&id, &payload) != nil {
			jsonErrorCode(w, errCodeInternal, "could not load notifications", http.StatusInternalServerError)
			return
		}
		items = append(items, map[string]interface{}{"id": id, "payload": payload})
	}
	if rows.Err() != nil {
		jsonErrorCode(w, errCodeInternal, "could not load notifications", http.StatusInternalServerError)
		return
	}
	jsonResponse(w, items)
}

func handleBrowserNotificationAck(w http.ResponseWriter, r *http.Request) {
	owner, ok := notificationOwner(r)
	if !ok {
		jsonErrorCode(w, errCodeAuthenticationRequired, "interactive sign-in required", http.StatusUnauthorized)
		return
	}
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var input struct {
		ID        int64  `json:"id"`
		BrowserID string `json:"browser_id"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if json.NewDecoder(r.Body).Decode(&input) != nil || input.ID < 1 || len(input.BrowserID) < 16 {
		jsonErrorCode(w, errCodeValidation, "invalid notification acknowledgement", http.StatusBadRequest)
		return
	}
	_, err := db.Exec(`UPDATE notification_deliveries d SET status='delivered',delivered_at=CURRENT_TIMESTAMP FROM notification_channels c
	 WHERE d.channel_id=c.id AND d.id=? AND c.owner_id=? AND c.kind='browser' AND c.browser_id=? AND d.status='browser_pending'`, input.ID, owner, input.BrowserID)
	if err != nil {
		jsonErrorCode(w, errCodeInternal, "could not acknowledge notification", http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]string{"status": "acknowledged"})
}

// Routing and marking are committed together. Failed queue writes can be
// retried after a restart and never alter a deployment's recorded outcome.
func queueBuildNotifications(ctx context.Context) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var id, projectID int64
	var name, status, commit, trigger string
	var seconds sql.NullInt64
	var finished sql.NullTime
	err = tx.QueryRowContext(ctx, `SELECT b.id,b.project_id,p.name,b.status,b.commit_sha,b.triggered_by,b.duration_seconds,b.finished_at
	 FROM builds b JOIN projects p ON p.id=b.project_id WHERE b.notifications_recorded=FALSE AND b.status IN ('success','failed','cancelled')
	 ORDER BY b.id LIMIT 1 FOR UPDATE OF b SKIP LOCKED`).Scan(&id, &projectID, &name, &status, &commit, &trigger, &seconds, &finished)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	baseURL := strings.TrimRight(appConfig.PublicURL, "/")
	payload, _ := json.Marshal(map[string]interface{}{"title": boundedAIText(name, 160) + ": " + status, "project_id": projectID, "project_name": boundedAIText(name, 160), "build_id": id, "status": status, "commit_sha": commit, "triggered_by": trigger, "duration_seconds": seconds.Int64, "build_url": baseURL + "/builds/" + strconv.FormatInt(id, 10)})
	when := time.Now().UTC()
	if finished.Valid {
		when = finished.Time
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO notification_deliveries(channel_id,build_id,payload,status)
	 SELECT id,?,?,CASE WHEN kind='browser' THEN 'browser_pending' ELSE 'queued' END FROM notification_channels
	 WHERE enabled=TRUE AND created_at<=? AND jsonb_exists(events,?) AND (jsonb_array_length(project_ids)=0 OR project_ids @> jsonb_build_array(?::bigint))
	 ON CONFLICT(channel_id,build_id) DO NOTHING`, id, string(redactJSON(payload)), when, status, projectID)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE builds SET notifications_recorded=TRUE WHERE id=?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

type notificationDelivery struct {
	ID                   int64
	Kind, EndpointCipher string
	Payload              json.RawMessage
	Attempts             int
}

func claimNotificationDelivery(ctx context.Context) (*notificationDelivery, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	d := &notificationDelivery{}
	err = tx.QueryRowContext(ctx, `SELECT d.id,c.kind,c.endpoint_cipher,d.payload,d.attempts FROM notification_deliveries d JOIN notification_channels c ON c.id=d.channel_id
	 WHERE c.enabled=TRUE AND c.kind<>'browser' AND d.attempts<5 AND ((d.status IN ('queued','retry') AND d.next_attempt_at<=CURRENT_TIMESTAMP) OR (d.status='sending' AND d.lease_until<CURRENT_TIMESTAMP))
	 ORDER BY d.id LIMIT 1 FOR UPDATE OF d SKIP LOCKED`).Scan(&d.ID, &d.Kind, &d.EndpointCipher, &d.Payload, &d.Attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	d.Attempts++
	_, err = tx.ExecContext(ctx, `UPDATE notification_deliveries SET status='sending',attempts=?,lease_until=CURRENT_TIMESTAMP+INTERVAL '1 minute' WHERE id=?`, d.Attempts, d.ID)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return d, nil
}

func publicNotificationIP(ip net.IP) bool {
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() {
		return false
	}
	for _, block := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001:db8::/32"} {
		_, network, _ := net.ParseCIDR(block)
		if network.Contains(ip) {
			return false
		}
	}
	return true
}

func notificationHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, errors.New("invalid webhook address")
		}
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil || len(ips) == 0 {
			return nil, errors.New("could not resolve webhook host")
		}
		for _, ip := range ips {
			if !publicNotificationIP(ip.IP) {
				return nil, errors.New("webhook host must resolve to a public address")
			}
		}
		var dialer net.Dialer
		for _, ip := range ips {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
			if err == nil {
				return conn, nil
			}
		}
		return nil, errors.New("could not connect to webhook host")
	}
	return &http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("webhook redirects are disabled") }}
}

func notificationPayload(kind string, raw json.RawMessage) ([]byte, error) {
	if kind == "webhook" {
		return raw, nil
	}
	var p struct {
		Title     string `json:"title"`
		BuildURL  string `json:"build_url"`
		CommitSHA string `json:"commit_sha"`
		Duration  int64  `json:"duration_seconds"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	text := p.Title
	if p.CommitSHA != "" {
		text += "\nRevision: " + boundedAIText(p.CommitSHA, 128)
	}
	if p.Duration > 0 {
		text += fmt.Sprintf("\nDuration: %ds", p.Duration)
	}
	if kind == "discord" {
		return json.Marshal(map[string]interface{}{"content": text + "\n" + p.BuildURL, "allowed_mentions": map[string]interface{}{"parse": []string{}}})
	}
	text += "\n" + p.BuildURL
	return json.Marshal(map[string]interface{}{"text": "Deployer notification", "unfurl_links": false, "unfurl_media": false, "blocks": []interface{}{map[string]interface{}{"type": "section", "text": map[string]interface{}{"type": "plain_text", "text": text}}}})
}

func deliverNotification(ctx context.Context, client *http.Client, d *notificationDelivery) (int, error) {
	endpoint, err := decryptNotificationEndpoint(d.EndpointCipher)
	if err != nil {
		return 0, err
	}
	c := notificationChannel{Name: "delivery", Kind: d.Kind, Endpoint: endpoint, Events: []string{"failed"}}
	if validateNotificationChannel(&c, false) != nil {
		return 0, errors.New("stored webhook URL is invalid")
	}
	if d.Kind == "discord" {
		u, _ := url.Parse(endpoint)
		query := u.Query()
		query.Set("wait", "true")
		u.RawQuery = query.Encode()
		endpoint = u.String()
	}
	payload, err := notificationPayload(d.Kind, d.Payload)
	if err != nil {
		return 0, errors.New("invalid notification payload")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(payload)))
	if err != nil {
		return 0, errors.New("invalid webhook request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", fmt.Sprintf("deployer-notification-%d", d.ID))
	resp, err := client.Do(req)
	if err != nil {
		return 0, errors.New("webhook delivery failed (network, timeout, or address policy)")
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}

func finishNotificationDelivery(ctx context.Context, d *notificationDelivery, statusCode int, deliveryErr error) error {
	status, message := "delivered", ""
	next := time.Now().UTC().Add(time.Duration(1<<min(d.Attempts, 6)) * 15 * time.Second)
	if deliveryErr != nil {
		status = "retry"
		message = deliveryErr.Error()
		if d.Attempts >= 5 || (statusCode >= 400 && statusCode < 500 && statusCode != 429 && statusCode != 408) {
			status = "failed"
		}
	}
	_, err := db.ExecContext(ctx, `UPDATE notification_deliveries SET status=?,last_error=?,next_attempt_at=?,lease_until=NULL,
	 delivered_at=CASE WHEN ?='delivered' THEN CURRENT_TIMESTAMP ELSE NULL END WHERE id=? AND attempts=? AND status='sending'`, status, message, next, status, d.ID, d.Attempts)
	return err
}

func runNotificationWorker(ctx context.Context) {
	client := notificationHTTPClient()
	defer client.CloseIdleConnections()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for i := 0; i < 25; i++ {
			var pending bool
			if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM builds WHERE notifications_recorded=FALSE AND status IN ('success','failed','cancelled'))`).Scan(&pending); err != nil {
				logOperationalError("find pending notification routing", err)
				break
			}
			if !pending {
				break
			}
			if err := queueBuildNotifications(ctx); err != nil {
				logOperationalError("queue build notification", err)
				break
			}
		}
		for i := 0; i < 10; i++ {
			d, err := claimNotificationDelivery(ctx)
			if err != nil {
				logOperationalError("claim notification delivery", err)
				break
			}
			if d == nil {
				break
			}
			code, deliveryErr := deliverNotification(ctx, client, d)
			logOperationalError("record notification delivery", finishNotificationDelivery(ctx, d, code, deliveryErr))
			if ctx.Err() != nil {
				return
			}
		}
		_, err := db.ExecContext(ctx, `UPDATE notification_deliveries SET status='failed',last_error='delivery attempt limit reached',lease_until=NULL WHERE status='sending' AND attempts>=5 AND lease_until<CURRENT_TIMESTAMP`)
		logOperationalError("recover expired notification deliveries", err)
		_, err = db.ExecContext(ctx, `DELETE FROM notification_deliveries WHERE created_at<CURRENT_TIMESTAMP-INTERVAL '30 days' AND status IN ('delivered','failed','browser_pending')`)
		logOperationalError("expire notification history", err)
	}
}
