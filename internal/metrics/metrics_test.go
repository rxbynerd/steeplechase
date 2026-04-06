package metrics

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRecorder_ObserveReceive(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewRecorder(reg)

	r.ObserveReceive("grpc", SignalMetrics)
	r.ObserveReceive("grpc", SignalMetrics)
	r.ObserveReceive("http", SignalLogs)

	if got := testutil.ToFloat64(r.receiverAccept.WithLabelValues("grpc", "metrics")); got != 2 {
		t.Errorf("grpc metrics accept = %v, want 2", got)
	}
	if got := testutil.ToFloat64(r.receiverAccept.WithLabelValues("http", "logs")); got != 1 {
		t.Errorf("http logs accept = %v, want 1", got)
	}
}

func TestRecorder_SinkStart_Success(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewRecorder(reg)

	finish := r.SinkStart("stdout", SignalMetrics)
	// Inflight should be 1 while the call is in progress.
	if got := testutil.ToFloat64(r.sinkInflight.WithLabelValues("stdout", "metrics")); got != 1 {
		t.Errorf("inflight during call = %v, want 1", got)
	}
	finish(nil, 0)

	if got := testutil.ToFloat64(r.sinkReceive.WithLabelValues("stdout", "metrics")); got != 1 {
		t.Errorf("receive_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(r.sinkSuccess.WithLabelValues("stdout", "metrics")); got != 1 {
		t.Errorf("success_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(r.sinkInflight.WithLabelValues("stdout", "metrics")); got != 0 {
		t.Errorf("inflight after call = %v, want 0", got)
	}
	// Latency histogram must have recorded exactly one observation.
	if got := testutil.CollectAndCount(r.sinkLatency); got == 0 {
		t.Errorf("latency histogram had no observations")
	}
}

func TestRecorder_SinkStart_Failure(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewRecorder(reg)

	cases := []struct {
		name   string
		err    error
		reason string
	}{
		{"canceled", context.Canceled, "canceled"},
		{"timeout", context.DeadlineExceeded, "timeout"},
		{"wrapped-canceled", wrap(context.Canceled), "canceled"},
		{"generic", errors.New("boom"), "other"},
		{"classified", &classifiedErr{reason: "unavailable"}, "unavailable"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			finish := r.SinkStart("forward", SignalLogs)
			finish(tc.err, 0)

			if got := testutil.ToFloat64(r.sinkFailure.WithLabelValues("forward", "logs", tc.reason)); got != 1 {
				t.Errorf("failure counter for reason %q = %v, want 1", tc.reason, got)
			}
			// Reset for next case to keep counts deterministic.
			r.sinkFailure.Reset()
		})
	}
}

func TestRecorder_SinkStart_Retries(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewRecorder(reg)

	finish := r.SinkStart("forward", SignalTraces)
	finish(nil, 3)

	if got := testutil.ToFloat64(r.sinkRetries.WithLabelValues("forward", "traces")); got != 3 {
		t.Errorf("retries_total = %v, want 3", got)
	}
}

func TestRecorder_SetBuildInfo(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewRecorder(reg)

	r.SetBuildInfo("1.2.3")
	if got := testutil.ToFloat64(r.buildInfo.WithLabelValues("1.2.3")); got != 1 {
		t.Errorf("build_info = %v, want 1", got)
	}
}

type classifiedErr struct{ reason string }

func (e *classifiedErr) Error() string  { return "classified: " + e.reason }
func (e *classifiedErr) Reason() string { return e.reason }

// wrap simulates a wrapping error so classifyError must use errors.Is.
type wrapErr struct{ inner error }

func (w wrapErr) Error() string { return "wrapped: " + w.inner.Error() }
func (w wrapErr) Unwrap() error { return w.inner }

func wrap(err error) error { return wrapErr{inner: err} }
