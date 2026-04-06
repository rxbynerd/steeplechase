// Package sinktest provides reusable sink.Sink fakes for use in tests.
package sinktest

import (
	"context"
	"sync"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/rxbynerd/steeplechase/internal/sink"
)

// RecordingSink counts the number of times each Consume* method was called and
// retains the last request of each signal type. It is safe for concurrent use.
type RecordingSink struct {
	mu           sync.Mutex
	metricsCount int
	logsCount    int
	tracesCount  int
	lastMetrics  *colmetricspb.ExportMetricsServiceRequest
	lastLogs     *collogspb.ExportLogsServiceRequest
	lastTraces   *coltracepb.ExportTraceServiceRequest
	shutdowns    int
	id           string
}

// NewRecordingSink returns a RecordingSink identified by name. The name is
// returned from Name() and used for metric labels.
func NewRecordingSink(name string) *RecordingSink {
	return &RecordingSink{id: name}
}

func (s *RecordingSink) ConsumeMetrics(_ context.Context, req *colmetricspb.ExportMetricsServiceRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metricsCount++
	s.lastMetrics = req
	return nil
}

func (s *RecordingSink) ConsumeLogs(_ context.Context, req *collogspb.ExportLogsServiceRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logsCount++
	s.lastLogs = req
	return nil
}

func (s *RecordingSink) ConsumeTraces(_ context.Context, req *coltracepb.ExportTraceServiceRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tracesCount++
	s.lastTraces = req
	return nil
}

func (s *RecordingSink) Name() string {
	if s.id == "" {
		return "recording"
	}
	return s.id
}

func (s *RecordingSink) Shutdown(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shutdowns++
	return nil
}

// MetricsCount returns the number of ConsumeMetrics calls so far.
func (s *RecordingSink) MetricsCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metricsCount
}

// LogsCount returns the number of ConsumeLogs calls so far.
func (s *RecordingSink) LogsCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logsCount
}

// TracesCount returns the number of ConsumeTraces calls so far.
func (s *RecordingSink) TracesCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tracesCount
}

// ShutdownCount returns the number of Shutdown calls so far.
func (s *RecordingSink) ShutdownCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shutdowns
}

// LastMetrics returns the most recently received ExportMetricsServiceRequest
// or nil if none has been received.
func (s *RecordingSink) LastMetrics() *colmetricspb.ExportMetricsServiceRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastMetrics
}

// LastLogs returns the most recently received ExportLogsServiceRequest
// or nil if none has been received.
func (s *RecordingSink) LastLogs() *collogspb.ExportLogsServiceRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastLogs
}

// LastTraces returns the most recently received ExportTraceServiceRequest
// or nil if none has been received.
func (s *RecordingSink) LastTraces() *coltracepb.ExportTraceServiceRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastTraces
}

// ErrorSink is a Sink that always returns the configured error from every
// Consume* call. Useful for testing error propagation.
type ErrorSink struct {
	id  string
	Err error
}

// NewErrorSink returns an ErrorSink identified by name.
func NewErrorSink(name string, err error) *ErrorSink {
	return &ErrorSink{id: name, Err: err}
}

func (s *ErrorSink) ConsumeMetrics(_ context.Context, _ *colmetricspb.ExportMetricsServiceRequest) error {
	return s.Err
}
func (s *ErrorSink) ConsumeLogs(_ context.Context, _ *collogspb.ExportLogsServiceRequest) error {
	return s.Err
}
func (s *ErrorSink) ConsumeTraces(_ context.Context, _ *coltracepb.ExportTraceServiceRequest) error {
	return s.Err
}

func (s *ErrorSink) Name() string {
	if s.id == "" {
		return "error"
	}
	return s.id
}

func (s *ErrorSink) Shutdown(_ context.Context) error { return nil }

// SlowSink blocks for Delay before returning from each Consume* call. It
// respects ctx cancellation and returns ctx.Err() if ctx is canceled first.
// Useful for verifying fan-out parallelism.
type SlowSink struct {
	id    string
	Delay time.Duration
}

// NewSlowSink returns a SlowSink identified by name.
func NewSlowSink(name string, delay time.Duration) *SlowSink {
	return &SlowSink{id: name, Delay: delay}
}

func (s *SlowSink) wait(ctx context.Context) error {
	select {
	case <-time.After(s.Delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *SlowSink) ConsumeMetrics(ctx context.Context, _ *colmetricspb.ExportMetricsServiceRequest) error {
	return s.wait(ctx)
}
func (s *SlowSink) ConsumeLogs(ctx context.Context, _ *collogspb.ExportLogsServiceRequest) error {
	return s.wait(ctx)
}
func (s *SlowSink) ConsumeTraces(ctx context.Context, _ *coltracepb.ExportTraceServiceRequest) error {
	return s.wait(ctx)
}

func (s *SlowSink) Name() string {
	if s.id == "" {
		return "slow"
	}
	return s.id
}

func (s *SlowSink) Shutdown(_ context.Context) error { return nil }

// Compile-time assertions.
var (
	_ sink.Sink = (*RecordingSink)(nil)
	_ sink.Sink = (*ErrorSink)(nil)
	_ sink.Sink = (*SlowSink)(nil)
)
