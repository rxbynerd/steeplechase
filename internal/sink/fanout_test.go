package sink_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"github.com/rxbynerd/steeplechase/internal/sink"
	"github.com/rxbynerd/steeplechase/internal/sinktest"
)

func TestFanout_AllSucceed(t *testing.T) {
	a := sinktest.NewRecordingSink("a")
	b := sinktest.NewRecordingSink("b")
	c := sinktest.NewRecordingSink("c")

	f := sink.NewFanoutSink([]sink.Sink{a, b, c}, quietLogger())
	err := f.ConsumeMetrics(context.Background(), &colmetricspb.ExportMetricsServiceRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, s := range []*sinktest.RecordingSink{a, b, c} {
		if s.MetricsCount() != 1 {
			t.Errorf("sink %q received %d metrics, want 1", s.Name(), s.MetricsCount())
		}
	}
}

func TestFanout_PartialFailureIsBestEffort(t *testing.T) {
	good := sinktest.NewRecordingSink("good")
	bad := sinktest.NewErrorSink("bad", errors.New("nope"))
	alsoGood := sinktest.NewRecordingSink("also-good")

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	f := sink.NewFanoutSink([]sink.Sink{good, bad, alsoGood}, logger)
	err := f.ConsumeMetrics(context.Background(), &colmetricspb.ExportMetricsServiceRequest{})
	if err != nil {
		t.Errorf("partial failure should not bubble up, got %v", err)
	}
	if good.MetricsCount() != 1 || alsoGood.MetricsCount() != 1 {
		t.Errorf("healthy sinks should still receive the payload; good=%d alsoGood=%d",
			good.MetricsCount(), alsoGood.MetricsCount())
	}
	if !strings.Contains(logBuf.String(), `sink=bad`) {
		t.Errorf("expected log line mentioning the failing sink, got: %s", logBuf.String())
	}
}

func TestFanout_TotalFailurePropagates(t *testing.T) {
	a := sinktest.NewErrorSink("a", errors.New("a failed"))
	b := sinktest.NewErrorSink("b", errors.New("b failed"))

	f := sink.NewFanoutSink([]sink.Sink{a, b}, quietLogger())
	err := f.ConsumeMetrics(context.Background(), &colmetricspb.ExportMetricsServiceRequest{})
	if err == nil {
		t.Fatal("expected joined error when every sink failed")
	}
	if !strings.Contains(err.Error(), "a failed") || !strings.Contains(err.Error(), "b failed") {
		t.Errorf("joined error should contain every child error, got %v", err)
	}
}

func TestFanout_EmptyReturnsNil(t *testing.T) {
	f := sink.NewFanoutSink(nil, quietLogger())
	if err := f.ConsumeMetrics(context.Background(), &colmetricspb.ExportMetricsServiceRequest{}); err != nil {
		t.Errorf("empty fanout should be a no-op, got %v", err)
	}
}

func TestFanout_ParallelExecution(t *testing.T) {
	// Three slow sinks, each 60ms. Serial execution would take ~180ms;
	// parallel should finish well under ~120ms even on a loaded CI box.
	a := sinktest.NewSlowSink("a", 60*time.Millisecond)
	b := sinktest.NewSlowSink("b", 60*time.Millisecond)
	c := sinktest.NewSlowSink("c", 60*time.Millisecond)

	f := sink.NewFanoutSink([]sink.Sink{a, b, c}, quietLogger())

	start := time.Now()
	if err := f.ConsumeMetrics(context.Background(), &colmetricspb.ExportMetricsServiceRequest{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed >= 150*time.Millisecond {
		t.Errorf("fanout looks serial; took %v, expected <150ms", elapsed)
	}
}

func TestFanout_ContextCancellationReachesChildren(t *testing.T) {
	slow := sinktest.NewSlowSink("slow", 500*time.Millisecond)
	f := sink.NewFanoutSink([]sink.Sink{slow}, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	// Single-sink fanout, so "all failed" == child's ctx error propagates.
	err := f.ConsumeMetrics(ctx, &colmetricspb.ExportMetricsServiceRequest{})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected ctx cancellation to cause error")
	}
	if elapsed >= 200*time.Millisecond {
		t.Errorf("child did not observe cancellation; elapsed %v", elapsed)
	}
}

func TestFanout_Shutdown(t *testing.T) {
	a := sinktest.NewRecordingSink("a")
	b := sinktest.NewRecordingSink("b")
	f := sink.NewFanoutSink([]sink.Sink{a, b}, quietLogger())

	if err := f.Shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.ShutdownCount() != 1 || b.ShutdownCount() != 1 {
		t.Errorf("shutdown did not reach all children: a=%d b=%d",
			a.ShutdownCount(), b.ShutdownCount())
	}
}

// quietLogger discards log output so successful test runs don't spam stdout.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
}
