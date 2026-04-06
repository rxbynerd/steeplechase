package sink

import (
	"context"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// Sink receives OTLP telemetry data. Implementations decide how to process it
// (print to stdout, forward to a backend, etc.).
//
// Sinks must be safe for concurrent use: Consume* methods may be called from
// many goroutines at once by the receivers.
type Sink interface {
	ConsumeMetrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) error
	ConsumeLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) error
	ConsumeTraces(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) error

	// Name returns a short, stable identifier used for log and metric labels.
	// It should be unique within a running process so that multiple instances
	// of the same sink type can be distinguished.
	Name() string

	// Shutdown releases any resources held by the sink (network connections,
	// background workers, etc.). Implementations without resources to release
	// should return nil. Shutdown must be safe to call exactly once; callers
	// should not use the sink after Shutdown returns.
	Shutdown(ctx context.Context) error
}
