package sink

import (
	"context"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// Sink receives OTLP telemetry data. Implementations decide how to process it
// (print to stdout, forward to a backend, etc.).
type Sink interface {
	ConsumeMetrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) error
	ConsumeLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) error
	ConsumeTraces(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) error
}
