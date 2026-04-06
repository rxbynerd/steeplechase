package sink

import (
	"context"
	"sync/atomic"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/rxbynerd/steeplechase/internal/metrics"
)

// transport is the protocol-specific forwarder used by OTLPForwardSink. Two
// implementations exist: grpcTransport (internal/sink/otlp_forward_grpc.go)
// and httpTransport (internal/sink/otlp_forward_http.go).
//
// A transport is responsible for:
//   - Serializing a request and delivering it to the downstream.
//   - Returning a permanentError (via permanent()) for non-retryable outcomes
//     (e.g. HTTP 4xx, gRPC InvalidArgument).
//   - Returning a plain error for retryable outcomes (e.g. HTTP 5xx, gRPC
//     Unavailable, network errors).
//   - Releasing any underlying connection or client when Close is called.
type transport interface {
	SendMetrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) error
	SendLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) error
	SendTraces(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) error
	Close(ctx context.Context) error
}

// OTLPForwardSink forwards OTLP payloads to a downstream OTLP backend over
// either gRPC or HTTP. Each Consume call applies the configured retry policy
// on top of the transport.
//
// Retry counts for the most recent call per signal are stored so MeteredSink
// can publish them via the lastRetryCount interface.
type OTLPForwardSink struct {
	name      string
	transport transport
	retry     retryConfig

	lastMetricsRetries atomic.Int32
	lastLogsRetries    atomic.Int32
	lastTracesRetries  atomic.Int32
}

// NewOTLPForwardSink constructs a sink with the given transport and retry
// policy. The name is used for log and metric labels.
func NewOTLPForwardSink(name string, t transport, r retryConfig) *OTLPForwardSink {
	return &OTLPForwardSink{name: name, transport: t, retry: r}
}

// Name returns the sink identifier.
func (s *OTLPForwardSink) Name() string { return s.name }

// Shutdown closes the underlying transport.
func (s *OTLPForwardSink) Shutdown(ctx context.Context) error {
	return s.transport.Close(ctx)
}

func (s *OTLPForwardSink) ConsumeMetrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) error {
	attempts, err := s.retry.Do(ctx, func(ctx context.Context) error {
		return s.transport.SendMetrics(ctx, req)
	})
	s.lastMetricsRetries.Store(int32(attempts))
	return err
}

func (s *OTLPForwardSink) ConsumeLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) error {
	attempts, err := s.retry.Do(ctx, func(ctx context.Context) error {
		return s.transport.SendLogs(ctx, req)
	})
	s.lastLogsRetries.Store(int32(attempts))
	return err
}

func (s *OTLPForwardSink) ConsumeTraces(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) error {
	attempts, err := s.retry.Do(ctx, func(ctx context.Context) error {
		return s.transport.SendTraces(ctx, req)
	})
	s.lastTracesRetries.Store(int32(attempts))
	return err
}

// lastRetryCount implements the retryCountReporter interface so MeteredSink
// can publish steeplechase_sink_retries_total{} for forwarding sinks.
func (s *OTLPForwardSink) lastRetryCount(signal metrics.Signal) int {
	switch signal {
	case metrics.SignalMetrics:
		return int(s.lastMetricsRetries.Load())
	case metrics.SignalLogs:
		return int(s.lastLogsRetries.Load())
	case metrics.SignalTraces:
		return int(s.lastTracesRetries.Load())
	}
	return 0
}
