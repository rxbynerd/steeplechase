package format

import (
	"fmt"
	"strings"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// FormatTimestamp converts a Unix nanosecond timestamp to an ISO 8601 string.
func FormatTimestamp(nanos uint64) string {
	if nanos == 0 {
		return "<no-timestamp>"
	}
	t := time.Unix(0, int64(nanos)).UTC()
	return t.Format("2006-01-02T15:04:05.000Z")
}

// FormatAttributes formats a slice of KeyValue pairs as {key=value, ...}.
func FormatAttributes(attrs []*commonpb.KeyValue) string {
	if len(attrs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(attrs))
	for _, kv := range attrs {
		parts = append(parts, fmt.Sprintf("%s=%s", kv.Key, FormatAnyValue(kv.Value)))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// FormatAnyValue formats an OTLP AnyValue to a human-readable string.
func FormatAnyValue(v *commonpb.AnyValue) string {
	if v == nil {
		return "<nil>"
	}
	switch val := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", val.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", val.DoubleValue)
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", val.BoolValue)
	case *commonpb.AnyValue_BytesValue:
		return fmt.Sprintf("%x", val.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		if val.ArrayValue == nil {
			return "[]"
		}
		items := make([]string, 0, len(val.ArrayValue.Values))
		for _, item := range val.ArrayValue.Values {
			items = append(items, FormatAnyValue(item))
		}
		return "[" + strings.Join(items, ", ") + "]"
	case *commonpb.AnyValue_KvlistValue:
		if val.KvlistValue == nil {
			return "{}"
		}
		return FormatAttributes(val.KvlistValue.Values)
	default:
		return fmt.Sprintf("%v", v.Value)
	}
}
