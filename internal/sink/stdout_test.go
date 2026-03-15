package sink

import (
	"bytes"
	"context"
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestStdoutSink_ConsumeMetrics(t *testing.T) {
	var buf bytes.Buffer
	s := NewStdoutSink(&buf)

	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "claude_code.token.usage",
					Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						DataPoints: []*metricspb.NumberDataPoint{{
							TimeUnixNano: 1710504600123000000,
							Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 100},
						}},
					}},
				}},
			}},
		}},
	}

	err := s.ConsumeMetrics(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if !strings.Contains(output, "[METRIC]") {
		t.Errorf("expected [METRIC] in output, got %q", output)
	}
	if !strings.Contains(output, "claude_code.token.usage") {
		t.Errorf("expected metric name in output, got %q", output)
	}
}

func TestStdoutSink_ConsumeLogs(t *testing.T) {
	var buf bytes.Buffer
	s := NewStdoutSink(&buf)

	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					TimeUnixNano: 1710504600123000000,
					Attributes: []*commonpb.KeyValue{
						{Key: "event.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "claude_code.tool_result"}}},
					},
				}},
			}},
		}},
	}

	err := s.ConsumeLogs(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if !strings.Contains(output, "[EVENT]") {
		t.Errorf("expected [EVENT] in output, got %q", output)
	}
}

func TestStdoutSink_ConsumeTraces(t *testing.T) {
	var buf bytes.Buffer
	s := NewStdoutSink(&buf)

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					Name:              "test",
					TraceId:           make([]byte, 16),
					SpanId:            make([]byte, 8),
					StartTimeUnixNano: 1710504600123000000,
				}},
			}},
		}},
	}

	err := s.ConsumeTraces(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if !strings.Contains(output, "[TRACE]") {
		t.Errorf("expected [TRACE] in output, got %q", output)
	}
}

func TestStdoutSink_EmptyRequest(t *testing.T) {
	var buf bytes.Buffer
	s := NewStdoutSink(&buf)

	if err := s.ConsumeMetrics(context.Background(), &colmetricspb.ExportMetricsServiceRequest{}); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Errorf("expected empty output for empty request, got %q", buf.String())
	}
}

func TestStdoutSink_CancelledContext(t *testing.T) {
	var buf bytes.Buffer
	s := NewStdoutSink(&buf)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := s.ConsumeMetrics(ctx, &colmetricspb.ExportMetricsServiceRequest{})
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}

	err = s.ConsumeLogs(ctx, &collogspb.ExportLogsServiceRequest{})
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}

	err = s.ConsumeTraces(ctx, &coltracepb.ExportTraceServiceRequest{})
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}
