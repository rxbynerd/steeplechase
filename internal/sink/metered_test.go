package sink_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"github.com/rxbynerd/steeplechase/internal/metrics"
	"github.com/rxbynerd/steeplechase/internal/sink"
	"github.com/rxbynerd/steeplechase/internal/sinktest"
)

func TestMetered_Success(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := metrics.NewRecorder(reg)
	inner := sinktest.NewRecordingSink("stdout")

	m := sink.NewMeteredSink(inner, rec)
	if err := m.ConsumeMetrics(context.Background(), &colmetricspb.ExportMetricsServiceRequest{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if count := testutil.CollectAndCount(reg, "steeplechase_sink_success_total"); count == 0 {
		t.Error("expected success counter to have observations")
	}
	if inner.MetricsCount() != 1 {
		t.Errorf("inner sink got %d calls, want 1", inner.MetricsCount())
	}
}

func TestMetered_Failure(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := metrics.NewRecorder(reg)
	inner := sinktest.NewErrorSink("forward", errors.New("boom"))

	m := sink.NewMeteredSink(inner, rec)
	err := m.ConsumeLogs(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error to propagate from inner sink")
	}

	// failure_total{sink=forward,signal=logs,reason=other} must be 1.
	expected := `
# HELP steeplechase_sink_failure_total Count of OTLP requests that failed in each sink, per signal and reason.
# TYPE steeplechase_sink_failure_total counter
steeplechase_sink_failure_total{reason="other",signal="logs",sink="forward"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected), "steeplechase_sink_failure_total"); err != nil {
		t.Errorf("unexpected metric output: %v", err)
	}
}

func TestMetered_Delegates(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := metrics.NewRecorder(reg)
	inner := sinktest.NewRecordingSink("inner-name")
	m := sink.NewMeteredSink(inner, rec)

	if m.Name() != "inner-name" {
		t.Errorf("Name = %q, want delegation to inner", m.Name())
	}
	if err := m.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown returned %v", err)
	}
	if inner.ShutdownCount() != 1 {
		t.Errorf("Shutdown not delegated to inner (count=%d)", inner.ShutdownCount())
	}
}

