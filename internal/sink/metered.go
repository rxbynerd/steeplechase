package sink

import (
	"context"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/rxbynerd/steeplechase/internal/metrics"
)

// MeteredSink wraps any Sink and records Prometheus metrics on each Consume
// call using an injected metrics.Recorder. The sink delegates Name() and
// Shutdown() to its inner sink.
//
// The metrics observation happens in a deferred closure so panics do not lose
// the inflight gauge decrement. Retry counts exposed by OTLPForwardSink are
// wired through a side channel: MeteredSink treats the inner sink as a
// single-shot op with zero retries, so if you want retry observability you
// must place MeteredSink *outside* a retrying sink (which is what the main
// wiring does).
type MeteredSink struct {
	inner Sink
	rec   *metrics.Recorder
}

// NewMeteredSink wraps inner with Prometheus instrumentation.
func NewMeteredSink(inner Sink, rec *metrics.Recorder) *MeteredSink {
	return &MeteredSink{inner: inner, rec: rec}
}

// Name delegates to the inner sink so metric labels match the wrapped sink.
func (m *MeteredSink) Name() string { return m.inner.Name() }

// Shutdown delegates to the inner sink.
func (m *MeteredSink) Shutdown(ctx context.Context) error { return m.inner.Shutdown(ctx) }

// Inner returns the wrapped sink. Useful for tests.
func (m *MeteredSink) Inner() Sink { return m.inner }

func (m *MeteredSink) ConsumeMetrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) error {
	finish := m.rec.SinkStart(m.inner.Name(), metrics.SignalMetrics)
	retries := retryCountFromSink(m.inner, metrics.SignalMetrics)
	err := m.inner.ConsumeMetrics(ctx, req)
	finish(err, retries())
	return err
}

func (m *MeteredSink) ConsumeLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) error {
	finish := m.rec.SinkStart(m.inner.Name(), metrics.SignalLogs)
	retries := retryCountFromSink(m.inner, metrics.SignalLogs)
	err := m.inner.ConsumeLogs(ctx, req)
	finish(err, retries())
	return err
}

func (m *MeteredSink) ConsumeTraces(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) error {
	finish := m.rec.SinkStart(m.inner.Name(), metrics.SignalTraces)
	retries := retryCountFromSink(m.inner, metrics.SignalTraces)
	err := m.inner.ConsumeTraces(ctx, req)
	finish(err, retries())
	return err
}

// retryCountReporter is an optional interface that sinks performing their own
// retry loops can implement to publish the most recent attempt count for
// observation by MeteredSink. OTLPForwardSink implements this.
type retryCountReporter interface {
	lastRetryCount(signal metrics.Signal) int
}

// retryCountFromSink returns a function that, when invoked after the Consume
// call completes, returns the number of retries the sink performed on the most
// recent call. Sinks that don't report retries simply return 0.
func retryCountFromSink(s Sink, signal metrics.Signal) func() int {
	if rr, ok := s.(retryCountReporter); ok {
		return func() int { return rr.lastRetryCount(signal) }
	}
	return func() int { return 0 }
}
