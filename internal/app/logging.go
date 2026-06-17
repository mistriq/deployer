package app

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

func logStructured(level, event string, fields map[string]interface{}) {
	log.SetFlags(0)
	record := map[string]interface{}{
		"time":  time.Now().UTC().Format(time.RFC3339Nano),
		"level": level,
		"event": event,
	}
	for key, value := range fields {
		record[key] = redactLogValue(value)
	}
	payload, err := json.Marshal(record)
	if err != nil {
		log.Print(redactedLogMarshalFallback(level, event, err))
		return
	}
	log.Print(string(payload))
}

func redactLogValue(value interface{}) interface{} {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		return redactSecrets(v)
	case error:
		return redactSecrets(v.Error())
	case fmt.Stringer:
		return redactSecrets(v.String())
	case []string:
		redacted := make([]string, len(v))
		for i, item := range v {
			redacted[i] = redactSecrets(item)
		}
		return redacted
	case []interface{}:
		redacted := make([]interface{}, len(v))
		for i, item := range v {
			redacted[i] = redactLogValue(item)
		}
		return redacted
	case map[string]string:
		redacted := make(map[string]string, len(v))
		for key, item := range v {
			redacted[key] = redactSecrets(item)
		}
		return redacted
	case map[string]interface{}:
		redacted := make(map[string]interface{}, len(v))
		for key, item := range v {
			redacted[key] = redactLogValue(item)
		}
		return redacted
	default:
		return v
	}
}

func redactedLogMarshalFallback(level, event string, marshalErr error) string {
	record := map[string]string{
		"time":           time.Now().UTC().Format(time.RFC3339Nano),
		"level":          "error",
		"event":          "log_marshal_failed",
		"original_level": redactSecrets(level),
		"original_event": redactSecrets(event),
		"error":          redactSecrets(marshalErr.Error()),
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return `{"level":"error","event":"log_marshal_failed","error":"unable to encode log record"}`
	}
	return string(payload)
}

func logOperationalError(context string, err error) {
	if err != nil {
		logStructured("error", "operation_error", map[string]interface{}{
			"operation": context,
			"error":     err,
		})
	}
}

func logOperationalInfo(format string, args ...interface{}) {
	logStructured("info", "operation_info", map[string]interface{}{
		"message": fmt.Sprintf(format, args...),
	})
}

func logFatal(event, message string, err error, fields map[string]interface{}) {
	recordFields := make(map[string]interface{}, len(fields)+2)
	for key, value := range fields {
		recordFields[key] = value
	}
	if message != "" {
		recordFields["message"] = message
	}
	if err != nil {
		recordFields["error"] = err
	}
	logStructured("fatal", event, recordFields)
	os.Exit(1)
}
