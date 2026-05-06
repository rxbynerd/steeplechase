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
// It is safe for concurrent use by multiple goroutines.
type StdoutSink struct {
	mu sync.Mutex
	w  io.Writer
}

// NewStdoutSink creates a new StdoutSink writing to the given writer.
func NewStdoutSink(w io.Writer) *StdoutSink {
	return &StdoutSink{w: w}
}

// Name returns the sink's identifier for use in log and metric labels.
func (s *StdoutSink) Name() string { return "stdout" }

// Shutdown is a no-op for StdoutSink; the underlying writer is owned by the caller.
func (s *StdoutSink) Shutdown(_ context.Context) error { return nil }

func (s *StdoutSink) ConsumeMetrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lines := format.FormatMetrics(req.ResourceMetrics)
	return s.writeLines(lines)
}

func (s *StdoutSink) ConsumeLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lines := format.FormatLogs(req.ResourceLogs)
	return s.writeLines(lines)
}

func (s *StdoutSink) ConsumeTraces(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lines := format.FormatTraces(req.ResourceSpans)
	return s.writeLines(lines)
}

func (s *StdoutSink) writeLines(lines []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, line := range lines {
		if _, err := fmt.Fprintln(s.w, line); err != nil {
			return fmt.Errorf("write failed: %w", err)
		}
	}
	return nil
}

// writeRunBlockLines satisfies the linesWriter contract used by RunBlockSink.
// It funnels rendered run-block lines through the same mutex-serialised
// writeLines path that line-mode emissions use, so a buffered flush cannot
// interleave with a concurrent line-mode write to the same writer.
//
// Lock order: callers (RunBlockSink) hold RunBlockSink.mu; this method
// acquires StdoutSink.mu internally. The order is therefore
// RunBlockSink.mu -> StdoutSink.mu and must never be inverted.
func (s *StdoutSink) writeRunBlockLines(_ context.Context, lines []string) error {
	if err := s.writeLines(lines); err != nil {
		return fmt.Errorf("runblock write: %w", err)
	}
	return nil
}
