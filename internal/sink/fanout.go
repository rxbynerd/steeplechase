package sink

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// FanoutSink fans each Consume* call out to a set of child sinks in parallel.
// Its delivery semantics are intentionally best-effort: a call returns nil
// unless *every* child sink failed, in which case it returns a joined error.
// Per-child failures are logged via the attached slog.Logger.
//
// This avoids the scenario where a single flaky backend causes the upstream
// OTLP client to retry the entire payload, resulting in duplicate delivery to
// the healthy sinks. Failures are surfaced via logs and the metered-sink
// metrics instead.
type FanoutSink struct {
	sinks  []Sink
	logger *slog.Logger
}

// NewFanoutSink constructs a FanoutSink over the given child sinks. A nil
// logger falls back to slog.Default().
func NewFanoutSink(sinks []Sink, logger *slog.Logger) *FanoutSink {
	if logger == nil {
		logger = slog.Default()
	}
	return &FanoutSink{sinks: sinks, logger: logger}
}

// Name returns the constant "fanout" label for metric purposes.
func (f *FanoutSink) Name() string { return "fanout" }

// Sinks returns the child sinks. Primarily useful for tests and introspection.
func (f *FanoutSink) Sinks() []Sink { return f.sinks }

func (f *FanoutSink) ConsumeMetrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) error {
	return f.fanout(ctx, "metrics", func(s Sink) error {
		return s.ConsumeMetrics(ctx, req)
	})
}

func (f *FanoutSink) ConsumeLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) error {
	return f.fanout(ctx, "logs", func(s Sink) error {
		return s.ConsumeLogs(ctx, req)
	})
}

func (f *FanoutSink) ConsumeTraces(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) error {
	return f.fanout(ctx, "traces", func(s Sink) error {
		return s.ConsumeTraces(ctx, req)
	})
}

// fanout runs call against every child sink in parallel and aggregates errors
// according to the best-effort policy.
func (f *FanoutSink) fanout(ctx context.Context, signal string, call func(Sink) error) error {
	if len(f.sinks) == 0 {
		return nil
	}

	errs := make([]error, len(f.sinks))
	var wg sync.WaitGroup
	wg.Add(len(f.sinks))
	for i, s := range f.sinks {
		i, s := i, s
		go func() {
			defer wg.Done()
			errs[i] = call(s)
		}()
	}
	wg.Wait()

	failed := 0
	var nonNil []error
	for i, err := range errs {
		if err == nil {
			continue
		}
		failed++
		nonNil = append(nonNil, err)
		f.logger.WarnContext(ctx, "sink failed",
			slog.String("sink", f.sinks[i].Name()),
			slog.String("signal", signal),
			slog.Any("err", err),
		)
	}

	if failed == len(f.sinks) {
		// Every child failed: surface a joined error so the receiver can
		// respond with an error to the upstream client.
		return errors.Join(nonNil...)
	}
	return nil
}

// Shutdown fans Shutdown out to every child sink in parallel and returns a
// joined error if any of them failed. Unlike Consume*, Shutdown propagates
// *any* failure, because operators need to notice resource-leak conditions.
func (f *FanoutSink) Shutdown(ctx context.Context) error {
	if len(f.sinks) == 0 {
		return nil
	}
	errs := make([]error, len(f.sinks))
	var wg sync.WaitGroup
	wg.Add(len(f.sinks))
	for i, s := range f.sinks {
		i, s := i, s
		go func() {
			defer wg.Done()
			errs[i] = s.Shutdown(ctx)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}
