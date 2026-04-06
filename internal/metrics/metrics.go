// Package metrics owns the Prometheus registry and metric definitions for
// Steeplechase. A single Recorder is constructed in main and shared between
// receivers, sinks, and the admin /metrics endpoint.
package metrics

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Signal identifies the telemetry type for metric labels.
type Signal string

const (
	SignalMetrics Signal = "metrics"
	SignalLogs    Signal = "logs"
	SignalTraces  Signal = "traces"
)

// Recorder owns all Steeplechase Prometheus metrics. It is safe for concurrent
// use by multiple goroutines.
type Recorder struct {
	reg prometheus.Registerer

	receiverAccept *prometheus.CounterVec

	sinkReceive  *prometheus.CounterVec
	sinkSuccess  *prometheus.CounterVec
	sinkFailure  *prometheus.CounterVec
	sinkLatency  *prometheus.HistogramVec
	sinkRetries  *prometheus.CounterVec
	sinkInflight *prometheus.GaugeVec

	buildInfo *prometheus.GaugeVec
}

// NewRecorder creates a Recorder and registers its metrics with reg. Panics if
// any metric fails to register (indicates a programming error such as a
// duplicate registration).
func NewRecorder(reg prometheus.Registerer) *Recorder {
	r := &Recorder{reg: reg}

	r.receiverAccept = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "steeplechase",
		Subsystem: "receiver",
		Name:      "accept_total",
		Help:      "Count of OTLP requests accepted by each receiver, per signal.",
	}, []string{"receiver", "signal"})

	r.sinkReceive = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "steeplechase",
		Subsystem: "sink",
		Name:      "receive_total",
		Help:      "Count of OTLP requests handed to each sink, per signal.",
	}, []string{"sink", "signal"})

	r.sinkSuccess = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "steeplechase",
		Subsystem: "sink",
		Name:      "success_total",
		Help:      "Count of OTLP requests successfully processed by each sink, per signal.",
	}, []string{"sink", "signal"})

	r.sinkFailure = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "steeplechase",
		Subsystem: "sink",
		Name:      "failure_total",
		Help:      "Count of OTLP requests that failed in each sink, per signal and reason.",
	}, []string{"sink", "signal", "reason"})

	r.sinkLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "steeplechase",
		Subsystem: "sink",
		Name:      "latency_seconds",
		Help:      "End-to-end latency of each sink Consume call, per signal.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"sink", "signal"})

	r.sinkRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "steeplechase",
		Subsystem: "sink",
		Name:      "retries_total",
		Help:      "Count of retry attempts issued by each sink, per signal.",
	}, []string{"sink", "signal"})

	r.sinkInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "steeplechase",
		Subsystem: "sink",
		Name:      "inflight",
		Help:      "Number of Consume calls currently executing in each sink, per signal.",
	}, []string{"sink", "signal"})

	r.buildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "steeplechase",
		Name:      "build_info",
		Help:      "Constant 1 gauge labelled with the running Steeplechase version.",
	}, []string{"version"})

	reg.MustRegister(
		r.receiverAccept,
		r.sinkReceive,
		r.sinkSuccess,
		r.sinkFailure,
		r.sinkLatency,
		r.sinkRetries,
		r.sinkInflight,
		r.buildInfo,
	)

	return r
}

// SetBuildInfo records the running version as a gauge. Call once at startup.
func (r *Recorder) SetBuildInfo(version string) {
	r.buildInfo.WithLabelValues(version).Set(1)
}

// ObserveReceive increments the receiver-level accept counter.
func (r *Recorder) ObserveReceive(receiver string, signal Signal) {
	r.receiverAccept.WithLabelValues(receiver, string(signal)).Inc()
}

// SinkStart marks a sink Consume call as begun: increments the receive counter,
// bumps the inflight gauge, and returns a finish func that must be called
// exactly once to record the outcome. The returned func decrements inflight,
// observes latency, increments success/failure counters, and adds any retry
// attempts.
//
// Typical use:
//
//	finish := rec.SinkStart("vector", metrics.SignalMetrics)
//	defer finish(err, retries)
func (r *Recorder) SinkStart(sink string, signal Signal) func(err error, retries int) {
	label := string(signal)
	r.sinkReceive.WithLabelValues(sink, label).Inc()
	r.sinkInflight.WithLabelValues(sink, label).Inc()
	start := time.Now()

	return func(err error, retries int) {
		r.sinkInflight.WithLabelValues(sink, label).Dec()
		r.sinkLatency.WithLabelValues(sink, label).Observe(time.Since(start).Seconds())
		if retries > 0 {
			r.sinkRetries.WithLabelValues(sink, label).Add(float64(retries))
		}
		if err == nil {
			r.sinkSuccess.WithLabelValues(sink, label).Inc()
			return
		}
		r.sinkFailure.WithLabelValues(sink, label, classifyError(err)).Inc()
	}
}

// classifyError maps an error to one of a small closed set of label values.
// Keeping the set small prevents label cardinality explosion.
func classifyError(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	// Interface check for sentinel types defined by sink implementations.
	// We use an interface instead of importing the sink package to avoid an
	// import cycle (sink imports metrics).
	var c classified
	if errors.As(err, &c) {
		return c.Reason()
	}
	return "other"
}

// classified is an optional interface that error values can implement to
// supply their own metrics reason label. Sink implementations define typed
// errors (permanentError, unavailableError) that satisfy this interface.
type classified interface {
	error
	Reason() string
}
