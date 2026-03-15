package format

import (
	"encoding/hex"
	"fmt"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// FormatTraces formats an OTLP trace export into human-readable lines.
func FormatTraces(resourceSpans []*tracepb.ResourceSpans) []string {
	var lines []string
	for _, rs := range resourceSpans {
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				lines = append(lines, formatSpan(span))
			}
		}
	}
	return lines
}

func formatSpan(span *tracepb.Span) string {
	ts := FormatTimestamp(span.StartTimeUnixNano)
	traceID := hex.EncodeToString(span.TraceId)
	spanID := hex.EncodeToString(span.SpanId)
	attrs := FormatAttributes(span.Attributes)

	line := fmt.Sprintf("[TRACE]  %s %s trace=%s span=%s", ts, span.Name, traceID, spanID)
	if attrs != "" {
		line += " " + attrs
	}
	return line
}
