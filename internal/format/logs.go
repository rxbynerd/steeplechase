package format

import (
	"fmt"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

// SeverityName maps numeric severity to a short string.
func SeverityName(sev logspb.SeverityNumber) string {
	switch {
	case sev >= logspb.SeverityNumber_SEVERITY_NUMBER_FATAL:
		return "FATAL"
	case sev >= logspb.SeverityNumber_SEVERITY_NUMBER_ERROR:
		return "ERROR"
	case sev >= logspb.SeverityNumber_SEVERITY_NUMBER_WARN:
		return "WARN"
	case sev >= logspb.SeverityNumber_SEVERITY_NUMBER_INFO:
		return "INFO"
	case sev >= logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG:
		return "DEBUG"
	case sev >= logspb.SeverityNumber_SEVERITY_NUMBER_TRACE:
		return "TRACE"
	default:
		return "UNSPECIFIED"
	}
}

// FormatLogs formats an OTLP log export into human-readable lines.
// Claude Code events have an event name attribute; regular logs use severity + body.
func FormatLogs(resourceLogs []*logspb.ResourceLogs) []string {
	var lines []string
	for _, rl := range resourceLogs {
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				lines = append(lines, formatLogRecord(lr))
			}
		}
	}
	return lines
}

func formatLogRecord(lr *logspb.LogRecord) string {
	ts := FormatTimestamp(lr.TimeUnixNano)

	// Check if this is an event (has a non-empty event.name attribute or non-empty SeverityText as event name)
	eventName := ""
	for _, kv := range lr.Attributes {
		if kv.Key == "event.name" {
			if kv.Value != nil {
				eventName = FormatAnyValue(kv.Value)
			}
			break
		}
	}

	if eventName != "" {
		// Format as an event
		attrs := FormatAttributes(filterAttributes(lr.Attributes, "event.name"))
		line := fmt.Sprintf("[EVENT]  %s %s", ts, eventName)
		if attrs != "" {
			line += " " + attrs
		}
		if lr.Body != nil {
			body := FormatAnyValue(lr.Body)
			if body != "" && body != "<nil>" {
				line += " body=" + body
			}
		}
		return line
	}

	// Format as a regular log
	sev := SeverityName(lr.SeverityNumber)
	body := ""
	if lr.Body != nil {
		body = FormatAnyValue(lr.Body)
	}
	attrs := FormatAttributes(lr.Attributes)
	line := fmt.Sprintf("[LOG]    %s %s %q", ts, sev, body)
	if attrs != "" {
		line += " " + attrs
	}
	return line
}

// filterAttributes returns attributes with the given key removed.
func filterAttributes(attrs []*commonpb.KeyValue, excludeKey string) []*commonpb.KeyValue {
	filtered := make([]*commonpb.KeyValue, 0, len(attrs))
	for _, kv := range attrs {
		if kv.Key != excludeKey {
			filtered = append(filtered, kv)
		}
	}
	return filtered
}
