package sink

import (
	"context"
	"fmt"
	"io"
	"sync"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"github.com/rxbynerd/steeplechase/internal/format"
)

// StdoutSink writes formatted telemetry to an io.Writer.
type StdoutSink struct {
	mu sync.Mutex
	w  io.Writer
}

// NewStdoutSink creates a new StdoutSink writing to the given writer.
func NewStdoutSink(w io.Writer) *StdoutSink {
	return &StdoutSink{w: w}
}

func (s *StdoutSink) ConsumeMetrics(_ context.Context, req *colmetricspb.ExportMetricsServiceRequest) error {
	lines := format.FormatMetrics(req.ResourceMetrics)
	s.writeLines(lines)
	return nil
}

func (s *StdoutSink) ConsumeLogs(_ context.Context, req *collogspb.ExportLogsServiceRequest) error {
	lines := format.FormatLogs(req.ResourceLogs)
	s.writeLines(lines)
	return nil
}

func (s *StdoutSink) ConsumeTraces(_ context.Context, req *coltracepb.ExportTraceServiceRequest) error {
	lines := format.FormatTraces(req.ResourceSpans)
	s.writeLines(lines)
	return nil
}

func (s *StdoutSink) writeLines(lines []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, line := range lines {
		fmt.Fprintln(s.w, line)
	}
}
