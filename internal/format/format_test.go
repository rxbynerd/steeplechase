package format

import (
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestFormatTimestamp(t *testing.T) {
	tests := []struct {
		nanos uint64
		want  string
	}{
		{0, "<no-timestamp>"},
		{1710504600123000000, "2024-03-15T12:10:00.123Z"},
	}
	for _, tt := range tests {
		got := FormatTimestamp(tt.nanos)
		if got != tt.want {
			t.Errorf("FormatTimestamp(%d) = %q, want %q", tt.nanos, got, tt.want)
		}
	}
}

func TestFormatAttributes(t *testing.T) {
	attrs := []*commonpb.KeyValue{
		{Key: "model", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "claude-sonnet-4-6"}}},
		{Key: "count", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}}},
	}
	got := FormatAttributes(attrs)
	if !strings.Contains(got, "model=claude-sonnet-4-6") {
		t.Errorf("FormatAttributes missing model, got %q", got)
	}
	if !strings.Contains(got, "count=42") {
		t.Errorf("FormatAttributes missing count, got %q", got)
	}
	if FormatAttributes(nil) != "" {
		t.Error("FormatAttributes(nil) should be empty")
	}
}

func TestFormatAnyValue(t *testing.T) {
	tests := []struct {
		name string
		val  *commonpb.AnyValue
		want string
	}{
		{"nil", nil, "<nil>"},
		{"string", &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}}, "hello"},
		{"int", &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 123}}, "123"},
		{"double", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 3.14}}, "3.14"},
		{"bool", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}, "true"},
		{"array", &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{
			Values: []*commonpb.AnyValue{
				{Value: &commonpb.AnyValue_StringValue{StringValue: "a"}},
				{Value: &commonpb.AnyValue_StringValue{StringValue: "b"}},
			},
		}}}, "[a, b]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatAnyValue(tt.val)
			if got != tt.want {
				t.Errorf("FormatAnyValue = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatMetrics_Sum(t *testing.T) {
	rm := []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "claude-code"}}},
			},
		},
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Metrics: []*metricspb.Metric{{
				Name: "claude_code.token.usage",
				Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
					DataPoints: []*metricspb.NumberDataPoint{{
						TimeUnixNano: 1710504600123000000,
						Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 1523},
						Attributes: []*commonpb.KeyValue{
							{Key: "type", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "input"}}},
						},
					}},
				}},
			}},
		}},
	}}

	lines := FormatMetrics(rm)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	line := lines[0]
	if !strings.Contains(line, "[METRIC]") {
		t.Errorf("missing [METRIC] prefix: %s", line)
	}
	if !strings.Contains(line, "claude_code.token.usage") {
		t.Errorf("missing metric name: %s", line)
	}
	if !strings.Contains(line, "1523") {
		t.Errorf("missing value: %s", line)
	}
	if !strings.Contains(line, "type=input") {
		t.Errorf("missing attribute: %s", line)
	}
}

func TestFormatMetrics_Gauge(t *testing.T) {
	rm := []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Metrics: []*metricspb.Metric{{
				Name: "claude_code.cost.usage",
				Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
					DataPoints: []*metricspb.NumberDataPoint{{
						TimeUnixNano: 1710504600123000000,
						Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 0.0342},
					}},
				}},
			}},
		}},
	}}

	lines := FormatMetrics(rm)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	if !strings.Contains(lines[0], "0.0342") {
		t.Errorf("missing value: %s", lines[0])
	}
}

func TestFormatMetrics_Histogram(t *testing.T) {
	rm := []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Metrics: []*metricspb.Metric{{
				Name: "test.histogram",
				Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
					DataPoints: []*metricspb.HistogramDataPoint{{
						TimeUnixNano: 1710504600123000000,
						Count:        10,
						Sum:          func() *float64 { v := 42.5; return &v }(),
					}},
				}},
			}},
		}},
	}}

	lines := FormatMetrics(rm)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	if !strings.Contains(lines[0], "count=10") {
		t.Errorf("missing count: %s", lines[0])
	}
}

func TestFormatLogs_Event(t *testing.T) {
	rl := []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{{
				TimeUnixNano: 1710504600123000000,
				Attributes: []*commonpb.KeyValue{
					{Key: "event.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "claude_code.api_request"}}},
					{Key: "model", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "claude-sonnet-4-6"}}},
					{Key: "duration_ms", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 2341}}},
				},
			}},
		}},
	}}

	lines := FormatLogs(rl)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	line := lines[0]
	if !strings.Contains(line, "[EVENT]") {
		t.Errorf("missing [EVENT] prefix: %s", line)
	}
	if !strings.Contains(line, "claude_code.api_request") {
		t.Errorf("missing event name: %s", line)
	}
	if !strings.Contains(line, "model=claude-sonnet-4-6") {
		t.Errorf("missing model attribute: %s", line)
	}
	// event.name should be filtered out of attributes
	if strings.Contains(line, "event.name=") {
		t.Errorf("event.name should be filtered from attributes: %s", line)
	}
}

func TestFormatLogs_RegularLog(t *testing.T) {
	rl := []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{{
				TimeUnixNano:   1710504600123000000,
				SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
				Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello world"}},
			}},
		}},
	}}

	lines := FormatLogs(rl)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	line := lines[0]
	if !strings.Contains(line, "[LOG]") {
		t.Errorf("missing [LOG] prefix: %s", line)
	}
	if !strings.Contains(line, "INFO") {
		t.Errorf("missing severity: %s", line)
	}
	if !strings.Contains(line, "hello world") {
		t.Errorf("missing body: %s", line)
	}
}

func TestFormatTraces(t *testing.T) {
	rs := []*tracepb.ResourceSpans{{
		ScopeSpans: []*tracepb.ScopeSpans{{
			Spans: []*tracepb.Span{{
				Name:              "test-span",
				TraceId:           []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
				SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
				StartTimeUnixNano: 1710504600123000000,
			}},
		}},
	}}

	lines := FormatTraces(rs)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	line := lines[0]
	if !strings.Contains(line, "[TRACE]") {
		t.Errorf("missing [TRACE] prefix: %s", line)
	}
	if !strings.Contains(line, "test-span") {
		t.Errorf("missing span name: %s", line)
	}
}

func TestSeverityName(t *testing.T) {
	tests := []struct {
		sev  logspb.SeverityNumber
		want string
	}{
		{logspb.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED, "UNSPECIFIED"},
		{logspb.SeverityNumber_SEVERITY_NUMBER_TRACE, "TRACE"},
		{logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG, "DEBUG"},
		{logspb.SeverityNumber_SEVERITY_NUMBER_INFO, "INFO"},
		{logspb.SeverityNumber_SEVERITY_NUMBER_WARN, "WARN"},
		{logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, "ERROR"},
		{logspb.SeverityNumber_SEVERITY_NUMBER_FATAL, "FATAL"},
	}
	for _, tt := range tests {
		got := SeverityName(tt.sev)
		if got != tt.want {
			t.Errorf("SeverityName(%v) = %q, want %q", tt.sev, got, tt.want)
		}
	}
}

func TestFormatMetrics_Summary(t *testing.T) {
	rm := []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Metrics: []*metricspb.Metric{{
				Name: "test.summary",
				Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{
					DataPoints: []*metricspb.SummaryDataPoint{{
						TimeUnixNano: 1710504600123000000,
						Count:        50,
						Sum:          123.45,
					}},
				}},
			}},
		}},
	}}

	lines := FormatMetrics(rm)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	if !strings.Contains(lines[0], "count=50") {
		t.Errorf("missing count: %s", lines[0])
	}
	if !strings.Contains(lines[0], "sum=123.45") {
		t.Errorf("missing sum: %s", lines[0])
	}
}

func TestFormatMetrics_ExponentialHistogram(t *testing.T) {
	rm := []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Metrics: []*metricspb.Metric{{
				Name: "test.exp_histogram",
				Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
					DataPoints: []*metricspb.ExponentialHistogramDataPoint{{
						TimeUnixNano: 1710504600123000000,
						Count:        25,
						Sum:          func() *float64 { v := 99.9; return &v }(),
					}},
				}},
			}},
		}},
	}}

	lines := FormatMetrics(rm)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	if !strings.Contains(lines[0], "count=25") {
		t.Errorf("missing count: %s", lines[0])
	}
	if !strings.Contains(lines[0], "sum=99.9") {
		t.Errorf("missing sum: %s", lines[0])
	}
}

func TestFormatMetrics_NilData(t *testing.T) {
	rm := []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Metrics: []*metricspb.Metric{{
				Name: "test.nil",
				Data: nil,
			}},
		}},
	}}

	lines := FormatMetrics(rm)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	if !strings.Contains(lines[0], "unknown data type") {
		t.Errorf("expected unknown data type message: %s", lines[0])
	}
}

func TestFormatAnyValue_BytesAndKvlist(t *testing.T) {
	bytesVal := &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{0xde, 0xad}}}
	got := FormatAnyValue(bytesVal)
	if got != "dead" {
		t.Errorf("BytesValue = %q, want %q", got, "dead")
	}

	kvlistVal := &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{
		Values: []*commonpb.KeyValue{
			{Key: "k", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "v"}}},
		},
	}}}
	got = FormatAnyValue(kvlistVal)
	if !strings.Contains(got, "k=v") {
		t.Errorf("KvlistValue = %q, want to contain k=v", got)
	}
}

// Stirrup is the primary telemetry source for Steeplechase. The fixtures
// below mirror the OTLP shapes that stirrup-harness actually emits
// (counters under stirrup.harness.*, run-tagged spans, observable gauges
// for live context-token estimates) so changes to the formatter that
// only happen to keep the Claude Code fixtures green can't silently
// regress Stirrup output.

func TestFormatMetrics_Stirrup_TokensInputCounter(t *testing.T) {
	rm := []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "stirrup-harness"}}},
			},
		},
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Metrics: []*metricspb.Metric{{
				Name: "stirrup.harness.tokens.input",
				Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
					DataPoints: []*metricspb.NumberDataPoint{{
						TimeUnixNano: 1710504600123000000,
						Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 4096},
						Attributes: []*commonpb.KeyValue{
							{Key: "run.id", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "run-abc"}}},
							{Key: "run.mode", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "execution"}}},
						},
					}},
				}},
			}},
		}},
	}}

	lines := FormatMetrics(rm)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	line := lines[0]
	if !strings.Contains(line, "stirrup.harness.tokens.input") {
		t.Errorf("missing metric name: %s", line)
	}
	if !strings.Contains(line, "= 4096") {
		t.Errorf("missing value: %s", line)
	}
	if !strings.Contains(line, "run.id=run-abc") {
		t.Errorf("missing run.id attribute: %s", line)
	}
	if !strings.Contains(line, "run.mode=execution") {
		t.Errorf("missing run.mode attribute: %s", line)
	}
	if !strings.Contains(line, "service.name=stirrup-harness") {
		t.Errorf("expected resource attribute to be merged in: %s", line)
	}
}

func TestFormatMetrics_Stirrup_ContextTokensGauge(t *testing.T) {
	rm := []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Metrics: []*metricspb.Metric{{
				Name: "stirrup.harness.context_tokens",
				Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
					DataPoints: []*metricspb.NumberDataPoint{{
						TimeUnixNano: 1710504600123000000,
						Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 87234},
						Attributes: []*commonpb.KeyValue{
							{Key: "run.id", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "run-abc"}}},
							{Key: "run.mode", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "execution"}}},
						},
					}},
				}},
			}},
		}},
	}}

	lines := FormatMetrics(rm)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	if !strings.Contains(lines[0], "stirrup.harness.context_tokens") {
		t.Errorf("missing gauge name: %s", lines[0])
	}
	if !strings.Contains(lines[0], "= 87234") {
		t.Errorf("missing gauge value: %s", lines[0])
	}
}

func TestFormatTraces_Stirrup_TurnSpan(t *testing.T) {
	rs := []*tracepb.ResourceSpans{{
		ScopeSpans: []*tracepb.ScopeSpans{{
			Spans: []*tracepb.Span{{
				Name:              "turn[3]",
				TraceId:           []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
				SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
				StartTimeUnixNano: 1710504600123000000,
				Attributes: []*commonpb.KeyValue{
					{Key: "turn.number", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 3}}},
					{Key: "turn.tokens.input", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 1024}}},
					{Key: "turn.tokens.output", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 256}}},
					{Key: "turn.tool_calls", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 2}}},
					{Key: "turn.stop_reason", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "tool_use"}}},
				},
			}},
		}},
	}}

	lines := FormatTraces(rs)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	line := lines[0]
	if !strings.Contains(line, "[TRACE]") {
		t.Errorf("missing [TRACE] prefix: %s", line)
	}
	if !strings.Contains(line, "turn[3]") {
		t.Errorf("missing span name: %s", line)
	}
	if !strings.Contains(line, "turn.stop_reason=tool_use") {
		t.Errorf("missing turn.stop_reason: %s", line)
	}
}

func TestFormatTraces_Stirrup_ToolCallSpan(t *testing.T) {
	rs := []*tracepb.ResourceSpans{{
		ScopeSpans: []*tracepb.ScopeSpans{{
			Spans: []*tracepb.Span{{
				Name:              "tool_call",
				TraceId:           []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
				SpanId:            []byte{9, 10, 11, 12, 13, 14, 15, 16},
				StartTimeUnixNano: 1710504600123000000,
				Attributes: []*commonpb.KeyValue{
					{Key: "tool.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "edit_file"}}},
					{Key: "tool.success", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}},
					{Key: "tool.duration_ms", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 142}}},
				},
			}},
		}},
	}}

	lines := FormatTraces(rs)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	line := lines[0]
	if !strings.Contains(line, "tool.name=edit_file") {
		t.Errorf("missing tool.name: %s", line)
	}
	if !strings.Contains(line, "tool.success=true") {
		t.Errorf("missing tool.success: %s", line)
	}
}

func TestFormatLogs_EventWithBody(t *testing.T) {
	rl := []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{{
				TimeUnixNano: 1710504600123000000,
				Attributes: []*commonpb.KeyValue{
					{Key: "event.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "claude_code.user_prompt"}}},
				},
				Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello world"}},
			}},
		}},
	}}

	lines := FormatLogs(rl)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	if !strings.Contains(lines[0], "body=hello world") {
		t.Errorf("missing body in event: %s", lines[0])
	}
}
