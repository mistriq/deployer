package app

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	callbackTimestampHeader = "X-Deployer-Timestamp"
	callbackEventIDHeader   = "X-Deployer-Event-ID"
	callbackSignatureHeader = "X-Deployer-Signature"
	callbackLeaseTimeout    = 2 * time.Minute
)

type callbackOutboxRecord struct {
	ID                  int64
	EventID             string
	HostingDeploymentID int64
	Payload             []byte
	PayloadHash         string
	Attempts            int
	ClaimToken          string
	CreatedAt           time.Time
}

func runHostingCallbackDispatcher(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		for {
			delivered, err := deliverHostingCallbackOnce(ctx, time.Now().UTC())
			if err != nil {
				logOperationalError("deliver hosting callback", err)
				break
			}
			if !delivered {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func deliverHostingCallbackOnce(ctx context.Context, now time.Time) (bool, error) {
	record, err := claimHostingCallback(ctx, now)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	deliveryErr := sendHostingCallback(ctx, record, now)
	completedAt := time.Now().UTC()
	if deliveryErr == nil {
		if err := markHostingCallbackDelivered(ctx, record, completedAt); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := rescheduleHostingCallback(ctx, record, deliveryErr, completedAt); err != nil {
		return false, err
	}
	return true, nil
}

func claimHostingCallback(ctx context.Context, now time.Time) (*callbackOutboxRecord, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	staleBefore := formatSQLiteTime(now.Add(-callbackLeaseTimeout))
	if _, err := conn.ExecContext(ctx, `UPDATE callback_outbox SET status='pending', locked_at=NULL, claim_token_hash=''
		WHERE status='delivering' AND (locked_at IS NULL OR locked_at<?)`, staleBefore); err != nil {
		return nil, err
	}
	var record callbackOutboxRecord
	var createdAt string
	err = conn.QueryRowContext(ctx, `SELECT id, event_id, hosting_deployment_id, payload_json, payload_hash,
		attempts, created_at
		FROM callback_outbox WHERE status='pending' AND next_attempt_at<=? ORDER BY id LIMIT 1`, formatSQLiteTime(now)).Scan(
		&record.ID, &record.EventID, &record.HostingDeploymentID, &record.Payload, &record.PayloadHash,
		&record.Attempts, &createdAt)
	if err != nil {
		return nil, err
	}
	record.CreatedAt = parseSQLiteTime(createdAt)
	record.ClaimToken, err = generateLeaseToken()
	if err != nil {
		return nil, err
	}
	result, err := conn.ExecContext(ctx, `UPDATE callback_outbox SET status='delivering', attempts=attempts+1,
		locked_at=?, claim_token_hash=? WHERE id=? AND status='pending'`, formatSQLiteTime(now), hashToken(record.ClaimToken), record.ID)
	if err != nil {
		return nil, err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		return nil, fmt.Errorf("callback claim lost")
	}
	record.Attempts++
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, err
	}
	committed = true
	return &record, nil
}

func sendHostingCallback(ctx context.Context, record *callbackOutboxRecord, now time.Time) error {
	if err := validateHostingCallbackEnvelope(ctx, record); err != nil {
		return err
	}
	target, err := validateCallbackTarget(appConfig.CallbackURL)
	if err != nil {
		return &hostingAPIError{Code: "callback_not_configured", Message: "hosting callback target is unavailable", StatusCode: http.StatusServiceUnavailable, Err: err}
	}
	if len(appConfig.CallbackSigningSecret) < 32 {
		return &hostingAPIError{Code: "callback_signing_unavailable", Message: "hosting callback signing secret is unavailable", StatusCode: http.StatusServiceUnavailable}
	}
	timestamp := strconv.FormatInt(now.Unix(), 10)
	signature := signHostingCallback(appConfig.CallbackSigningSecret, timestamp, record.EventID, record.Payload)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(record.Payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(callbackTimestampHeader, timestamp)
	request.Header.Set(callbackEventIDHeader, record.EventID)
	request.Header.Set(callbackSignatureHeader, signature)
	timeout := hostingCallbackRequestTimeout(appConfig.CallbackTimeout)
	client := &http.Client{Timeout: timeout, CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("send callback: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("callback returned HTTP %d", response.StatusCode)
	}
	return nil
}

func hostingCallbackRequestTimeout(configured time.Duration) time.Duration {
	if configured <= 0 {
		configured = 15 * time.Second
	}
	if configured > callbackLeaseTimeout/2 {
		return callbackLeaseTimeout / 2
	}
	return configured
}

func validateHostingCallbackEnvelope(ctx context.Context, record *callbackOutboxRecord) error {
	actualHash := sha256.Sum256(record.Payload)
	if record.CreatedAt.IsZero() || record.PayloadHash != "sha256:"+hex.EncodeToString(actualHash[:]) {
		return &hostingAPIError{Code: "callback_payload_corrupt",
			Message: "callback payload cannot be verified", StatusCode: http.StatusInternalServerError}
	}
	deployment, err := getHostingDeploymentByID(ctx, record.HostingDeploymentID)
	if err != nil {
		return &hostingAPIError{Code: "callback_payload_corrupt",
			Message: "callback payload cannot be verified", StatusCode: http.StatusInternalServerError, Err: err}
	}
	if !validHostingExternalIDForAdmission(deployment.ExternalProjectID) ||
		!validHostingExternalIDForAdmission(deployment.ExternalDeploymentID) {
		return &hostingAPIError{Code: "callback_identity_invalid",
			Message: "callback identity cannot be delivered", StatusCode: http.StatusInternalServerError}
	}
	expectedEventID, expectedPayload, _, err := buildHostingTerminalCallback(deployment, record.CreatedAt)
	if err != nil || record.EventID != expectedEventID {
		return &hostingAPIError{Code: "callback_payload_corrupt",
			Message: "callback payload cannot be verified", StatusCode: http.StatusInternalServerError, Err: err}
	}
	if bytes.Equal(record.Payload, expectedPayload) {
		return nil
	}
	legacyEventID, legacyPayload, _, legacyErr := buildLegacyHostingTerminalCallback(deployment, record.CreatedAt)
	if legacyErr != nil || record.EventID != legacyEventID || !bytes.Equal(record.Payload, legacyPayload) {
		return &hostingAPIError{Code: "callback_payload_corrupt",
			Message: "callback payload cannot be verified", StatusCode: http.StatusInternalServerError, Err: legacyErr}
	}
	return nil
}

func validateCallbackTarget(value string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || !validPrivateServiceURL(parsed) {
		return nil, fmt.Errorf("callback URL must use HTTPS (or loopback HTTP) without credentials, query, or fragment")
	}
	return parsed, nil
}

func signHostingCallback(secret, timestamp, eventID string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write([]byte(eventID))
	mac.Write([]byte("."))
	mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func verifyHostingCallbackSignature(secret, timestamp, eventID string, payload []byte, signature string) bool {
	expected := signHostingCallback(secret, timestamp, eventID, payload)
	return hmac.Equal([]byte(expected), []byte(signature))
}

func markHostingCallbackDelivered(ctx context.Context, record *callbackOutboxRecord, now time.Time) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE callback_outbox SET status='delivered', delivered_at=?, finalized_at=?, locked_at=NULL,
		claim_token_hash='', last_error_code='' WHERE id=? AND status='delivering' AND claim_token_hash=?`,
		formatSQLiteTime(now), formatSQLiteTime(now), record.ID, hashToken(record.ClaimToken))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		return fmt.Errorf("callback delivery state changed")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE hosting_deployments SET callback_state='delivered', updated_at=? WHERE id=?`, formatSQLiteTime(now), record.HostingDeploymentID); err != nil {
		return err
	}
	return tx.Commit()
}

func rescheduleHostingCallback(ctx context.Context, record *callbackOutboxRecord, deliveryErr error, now time.Time) error {
	maxAttempts := appConfig.CallbackMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 12
	}
	status := "pending"
	next := now.Add(callbackBackoff(record.Attempts))
	callbackState := "pending"
	if record.Attempts >= maxAttempts {
		status = "dead_letter"
		callbackState = "dead_letter"
	}
	errorCode := "callback_delivery_failed"
	var apiErr *hostingAPIError
	if errorsAsHosting(deliveryErr, &apiErr) {
		errorCode = apiErr.Code
	}
	if errorCode == "callback_payload_corrupt" || errorCode == "callback_identity_invalid" {
		status = "dead_letter"
		callbackState = "dead_letter"
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var finalizedAt any
	if status == "dead_letter" {
		finalizedAt = formatSQLiteTime(now)
	}
	result, err := tx.ExecContext(ctx, `UPDATE callback_outbox SET status=?, next_attempt_at=?, finalized_at=?, locked_at=NULL,
		claim_token_hash='', last_error_code=? WHERE id=? AND status='delivering' AND claim_token_hash=?`,
		status, formatSQLiteTime(next), finalizedAt, errorCode, record.ID, hashToken(record.ClaimToken))
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("callback delivery claim changed")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE hosting_deployments SET callback_state=?, updated_at=? WHERE id=?`, callbackState, formatSQLiteTime(now), record.HostingDeploymentID); err != nil {
		return err
	}
	return tx.Commit()
}

func errorsAsHosting(err error, target **hostingAPIError) bool {
	for err != nil {
		if value, ok := err.(*hostingAPIError); ok {
			*target = value
			return true
		}
		type unwrapper interface{ Unwrap() error }
		if wrapped, ok := err.(unwrapper); ok {
			err = wrapped.Unwrap()
			continue
		}
		break
	}
	return false
}

func callbackBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 10 {
		shift = 10
	}
	delay := 5 * time.Second * time.Duration(1<<shift)
	if delay > time.Hour {
		return time.Hour
	}
	return delay
}
