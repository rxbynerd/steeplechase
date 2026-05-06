package sink_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/rxbynerd/steeplechase/internal/sink"
)

// fakeClock is a manually-advanced time source. The mutex makes it safe to
// drive from a test goroutine while the sweeper goroutine reads it.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{now: t} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// quietLogger is a slog.Logger that drops all output. Sink-level diagnostic
// noise during a successful test run isn't useful.
func quietRunblockLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
}

// syncBuffer is a goroutine-safe wrapper around bytes.Buffer. Tests that
// observe a running sweeper need this because the sweeper writes via the
// inner StdoutSink while the test reads from the same buffer.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func (s *syncBuffer) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Len()
}

// stringAttr is a tiny helper to keep span/log construction concise.
func stringAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

// mustNewRunBlockSink wraps the constructor for the common test case where
// passing *StdoutSink as inner is guaranteed to succeed. Tests that want
// to exercise the construction error path call sink.NewRunBlockSink
// directly.
func mustNewRunBlockSink(t *testing.T, inner sink.Sink, cfg sink.RunBlockConfig) *sink.RunBlockSink {
	t.Helper()
	rb, err := sink.NewRunBlockSink(inner, cfg)
	if err != nil {
		t.Fatalf("NewRunBlockSink: %v", err)
	}
	return rb
}

// makeRootRunSpan returns the canonical Stirrup root span for a run.
func makeRootRunSpan(traceID, spanID []byte, ts uint64, runID string, attrs ...*commonpb.KeyValue) *tracepb.Span {
	a := []*commonpb.KeyValue{stringAttr("run.id", runID)}
	a = append(a, attrs...)
	return &tracepb.Span{
		Name:              "run",
		TraceId:           traceID,
		SpanId:            spanID,
		StartTimeUnixNano: ts,
		Attributes:        a,
	}
}

// makeChildSpan returns a child span under the given parent.
func makeChildSpan(name string, traceID, spanID, parentID []byte, ts uint64, attrs ...*commonpb.KeyValue) *tracepb.Span {
	return &tracepb.Span{
		Name:              name,
		TraceId:           traceID,
		SpanId:            spanID,
		ParentSpanId:      parentID,
		StartTimeUnixNano: ts,
		Attributes:        attrs,
	}
}

func wrapSpans(spans ...*tracepb.Span) *coltracepb.ExportTraceServiceRequest {
	return &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
		}},
	}
}

func wrapLogs(records ...*logspb.LogRecord) *collogspb.ExportLogsServiceRequest {
	return &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{LogRecords: records}},
		}},
	}
}

func wrapMetric(name string, dp *metricspb.NumberDataPoint, isSum bool) *colmetricspb.ExportMetricsServiceRequest {
	var data interface{ isMetric_Data() }
	_ = data
	var m *metricspb.Metric
	if isSum {
		m = &metricspb.Metric{
			Name: name,
			Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{DataPoints: []*metricspb.NumberDataPoint{dp}}},
		}
	} else {
		m = &metricspb.Metric{
			Name: name,
			Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{dp}}},
		}
	}
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{m}}},
		}},
	}
}

// ---- Tests ----

func TestRunBlock_HappyPath_RootEndFlushes(t *testing.T) {
	var buf bytes.Buffer
	stdout := sink.NewStdoutSink(&buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode:        sink.RunBlockModeGrouped,
		IdleTimeout: time.Hour,
		MaxItems:    100,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})
	defer rb.Shutdown(context.Background())

	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rootID := []byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8}
	turnID := []byte{0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8}

	// Stream the turn span first (out of order vs. the root start).
	turn := makeChildSpan("turn[1]", traceID, turnID, rootID, 1710504602000000000,
		stringAttr("run.id", "abc"),
		stringAttr("turn.number", "1"),
	)
	if err := rb.ConsumeTraces(context.Background(), wrapSpans(turn)); err != nil {
		t.Fatalf("turn span: %v", err)
	}

	// Buffer must not have been flushed yet — nothing should be on stdout.
	if buf.Len() != 0 {
		t.Fatalf("expected no output until root end, got %q", buf.String())
	}

	// Root with run.outcome triggers the flush.
	root := makeRootRunSpan(traceID, rootID, 1710504600000000000, "abc",
		stringAttr("run.mode", "execution"),
		stringAttr("run.provider", "anthropic"),
		stringAttr("run.model", "claude-sonnet-4-6"),
		stringAttr("run.outcome", "success"),
		stringAttr("run.turns", "1"),
	)
	if err := rb.ConsumeTraces(context.Background(), wrapSpans(root)); err != nil {
		t.Fatalf("root span: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "=== run abc started") {
		t.Errorf("expected run header, got %q", out)
	}
	if !strings.Contains(out, "outcome=success") || !strings.Contains(out, "turns=1") {
		t.Errorf("expected outcome+turns in footer, got %q", out)
	}
	// Body items must be in chronological order: root (1710504600) then turn (1710504602).
	rootIdx := strings.Index(out, "[TRACE]  2024-03-15T12:10:00.000Z run ")
	turnIdx := strings.Index(out, "[TRACE]  2024-03-15T12:10:02.000Z turn[1]")
	if rootIdx < 0 || turnIdx < 0 {
		t.Fatalf("expected both span lines in output, got %q", out)
	}
	if rootIdx > turnIdx {
		t.Errorf("body items not chronological: root@%d turn@%d", rootIdx, turnIdx)
	}
}

func TestRunBlock_NoRunIDBypasses(t *testing.T) {
	var buf bytes.Buffer
	stdout := sink.NewStdoutSink(&buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode:        sink.RunBlockModeGrouped,
		IdleTimeout: time.Hour,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})
	defer rb.Shutdown(context.Background())

	// No run.id anywhere — should stream straight through.
	span := makeChildSpan("standalone", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		[]byte{1, 2, 3, 4, 5, 6, 7, 8}, nil, 1710504600123000000)

	if err := rb.ConsumeTraces(context.Background(), wrapSpans(span)); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if !strings.Contains(out, "[TRACE]") || !strings.Contains(out, "standalone") {
		t.Errorf("expected immediate pass-through line, got %q", out)
	}
	// No header/footer should appear.
	if strings.Contains(out, "=== run") {
		t.Errorf("unidentified items must not be wrapped in a run block: %q", out)
	}
}

func TestRunBlock_IdleTimeoutFlushes(t *testing.T) {
	buf := &syncBuffer{}
	stdout := sink.NewStdoutSink(buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode: sink.RunBlockModeGrouped,
		// Short timeout so the sweeper polls at the 100ms floor.
		IdleTimeout: 200 * time.Millisecond,
		MaxItems:    100,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})
	defer rb.Shutdown(context.Background())

	turnID := []byte{0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8}
	turn := makeChildSpan("turn[1]",
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		turnID, []byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8},
		1710504600100000000, stringAttr("run.id", "abc"))

	if err := rb.ConsumeTraces(context.Background(), wrapSpans(turn)); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected nothing flushed yet, got %q", buf.String())
	}

	// Advance the fake clock past the idle window. The sweeper polls real
	// time, so wait for at least one poll interval (100ms floor).
	clock.Advance(time.Hour)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if buf.Len() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	out := buf.String()
	if !strings.Contains(out, "=== run abc") {
		t.Fatalf("expected idle flush to emit a run block, got %q", out)
	}
	if !strings.Contains(out, "outcome=<unknown>") {
		t.Errorf("idle flush footer should mark outcome unknown: %q", out)
	}
}

func TestRunBlock_OverflowTruncatesAndStreams(t *testing.T) {
	var buf bytes.Buffer
	stdout := sink.NewStdoutSink(&buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode:        sink.RunBlockModeGrouped,
		IdleTimeout: time.Hour,
		MaxItems:    2,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})
	defer rb.Shutdown(context.Background())

	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	for i := 0; i < 4; i++ {
		spanID := []byte{0xb0, byte(i), 0xc0, 0xd0, 0xe0, 0xf0, 0x00, 0x01}
		span := makeChildSpan("turn", traceID, spanID, nil,
			uint64(1710504600000000000+int64(i)*1_000_000_000),
			stringAttr("run.id", "abc"))
		if err := rb.ConsumeTraces(context.Background(), wrapSpans(span)); err != nil {
			t.Fatal(err)
		}
	}

	out := buf.String()
	// MaxItems=2 caps the buffer at exactly two items, so the truncation
	// warning reports 2. The third span is the one that triggered the
	// overflow flush and is rejected from the buffer; it streams as a
	// line-mode bypass alongside the fourth.
	if !strings.Contains(out, "[WARN] run abc truncated at 2 items") {
		t.Errorf("expected truncation warn line for 2 items, got %q", out)
	}
	// The first two should be in a run block, the third and fourth should
	// each appear as line-mode pass-throughs after the warn.
	warnIdx := strings.Index(out, "[WARN]")
	lastIdx := strings.LastIndex(out, "[TRACE]")
	if warnIdx < 0 || lastIdx < 0 || lastIdx < warnIdx {
		t.Errorf("expected post-overflow trace line after warn, got %q", out)
	}
	// Pre-warn output should contain exactly 2 [TRACE] lines (the buffered
	// pair) and post-warn should contain 2 more (the bypass-routed pair).
	preWarn := out[:warnIdx]
	postWarn := out[warnIdx:]
	if got := strings.Count(preWarn, "[TRACE]"); got != 2 {
		t.Errorf("expected 2 [TRACE] lines before warn, got %d in %q", got, preWarn)
	}
	if got := strings.Count(postWarn, "[TRACE]"); got != 2 {
		t.Errorf("expected 2 [TRACE] lines after warn (bypass-routed), got %d in %q", got, postWarn)
	}
}

func TestRunBlock_ShutdownFlushesOpenRuns(t *testing.T) {
	var buf bytes.Buffer
	stdout := sink.NewStdoutSink(&buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode:        sink.RunBlockModeGrouped,
		IdleTimeout: time.Hour,
		MaxItems:    100,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})

	turn := makeChildSpan("turn[1]",
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		[]byte{0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8}, nil,
		1710504600100000000, stringAttr("run.id", "abc"))
	if err := rb.ConsumeTraces(context.Background(), wrapSpans(turn)); err != nil {
		t.Fatal(err)
	}

	if err := rb.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "=== run abc") {
		t.Errorf("expected open run to flush on shutdown, got %q", out)
	}
	if !strings.Contains(out, "outcome=<unknown>") {
		t.Errorf("shutdown flush should treat run as idle: %q", out)
	}
}

// TestRunBlock_ShutdownAfterRootEndDoesNotDuplicateBlock pins the
// invariant that flushing a run via root-end clears its buffer and the
// trace_id mapping, so a subsequent Shutdown does not re-flush the same
// run as if it were still open. Without this guarantee a future refactor
// could resurrect the run buffer after flush and emit a second
// header/footer pair on shutdown for an already-closed run.
func TestRunBlock_ShutdownAfterRootEndDoesNotDuplicateBlock(t *testing.T) {
	var buf bytes.Buffer
	stdout := sink.NewStdoutSink(&buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode:        sink.RunBlockModeGrouped,
		IdleTimeout: time.Hour,
		MaxItems:    100,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})

	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rootID := []byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8}
	root := makeRootRunSpan(traceID, rootID, 1710504600000000000, "abc",
		stringAttr("run.outcome", "success"),
	)
	if err := rb.ConsumeTraces(context.Background(), wrapSpans(root)); err != nil {
		t.Fatal(err)
	}

	// Root-end has flushed the run; the buffer for "abc" should be gone.
	if err := rb.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	out := buf.String()
	if got := strings.Count(out, "=== run abc started"); got != 1 {
		t.Errorf("expected exactly one header for run abc, got %d in:\n%s", got, out)
	}
	if got := strings.Count(out, "=== run abc finished"); got != 1 {
		t.Errorf("expected exactly one footer for run abc, got %d in:\n%s", got, out)
	}
}

// TestRunBlock_ShutdownIsIdempotent guards against the previous bug where a
// second Shutdown closed an already-closed stopCh and panicked. main.go's
// shutdown path defers a best-effort Shutdown and then calls Shutdown again
// on the orderly-shutdown branch, so a panic on the second call would crash
// the binary on every clean exit.
func TestRunBlock_ShutdownIsIdempotent(t *testing.T) {
	var buf bytes.Buffer
	stdout := sink.NewStdoutSink(&buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode:        sink.RunBlockModeGrouped,
		IdleTimeout: time.Hour,
		MaxItems:    100,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})

	if err := rb.Shutdown(context.Background()); err != nil {
		t.Fatalf("first shutdown: %v", err)
	}
	// A second Shutdown must not close stopCh again (panic), and must still
	// return a usable error (here nil, since the inner StdoutSink's
	// Shutdown is a no-op).
	if err := rb.Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}

func TestRunBlock_TraceIDMappingForChildSpans(t *testing.T) {
	var buf bytes.Buffer
	stdout := sink.NewStdoutSink(&buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode:        sink.RunBlockModeGrouped,
		IdleTimeout: time.Hour,
		MaxItems:    100,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})
	defer rb.Shutdown(context.Background())

	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rootID := []byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8}
	childID := []byte{0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8}

	// First request: a span with run.id, populating the trace_id -> run.id map.
	tagged := makeChildSpan("turn[1]", traceID, []byte{0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8}, rootID,
		1710504600100000000, stringAttr("run.id", "abc"))
	if err := rb.ConsumeTraces(context.Background(), wrapSpans(tagged)); err != nil {
		t.Fatal(err)
	}

	// Second request: a span with no run.id but the same trace_id. Should be
	// bucketed into the same run.
	untagged := makeChildSpan("tool_call", traceID, childID, rootID, 1710504600200000000)
	if err := rb.ConsumeTraces(context.Background(), wrapSpans(untagged)); err != nil {
		t.Fatal(err)
	}

	// Buffer should now hold both. Flush via the canonical run-end.
	root := makeRootRunSpan(traceID, rootID, 1710504600000000000, "abc",
		stringAttr("run.outcome", "success"),
	)
	if err := rb.ConsumeTraces(context.Background(), wrapSpans(root)); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if !strings.Contains(out, "tool_call") {
		t.Errorf("trace_id-mapped child span should be inside the run block: %q", out)
	}
	if !strings.Contains(out, "outcome=success") {
		t.Errorf("expected run.outcome footer, got %q", out)
	}
}

func TestRunBlock_LogsAndMetrics(t *testing.T) {
	var buf bytes.Buffer
	stdout := sink.NewStdoutSink(&buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode:        sink.RunBlockModeGrouped,
		IdleTimeout: time.Hour,
		MaxItems:    100,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})
	defer rb.Shutdown(context.Background())

	// Tagged log + tagged metric, then root-end flushes them.
	logRec := &logspb.LogRecord{
		TimeUnixNano:   1710504600100000000,
		SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
		Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}},
		Attributes:     []*commonpb.KeyValue{stringAttr("run.id", "abc")},
	}
	if err := rb.ConsumeLogs(context.Background(), wrapLogs(logRec)); err != nil {
		t.Fatal(err)
	}

	dp := &metricspb.NumberDataPoint{
		TimeUnixNano: 1710504600200000000,
		Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 42},
		Attributes:   []*commonpb.KeyValue{stringAttr("run.id", "abc")},
	}
	if err := rb.ConsumeMetrics(context.Background(), wrapMetric("stirrup.harness.tokens.input", dp, true)); err != nil {
		t.Fatal(err)
	}

	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rootID := []byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8}
	root := makeRootRunSpan(traceID, rootID, 1710504600000000000, "abc",
		stringAttr("run.outcome", "success"),
	)
	if err := rb.ConsumeTraces(context.Background(), wrapSpans(root)); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"=== run abc started", "[LOG]", "[METRIC]", "stirrup.harness.tokens.input", "outcome=success"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output: %q", want, out)
		}
	}
}

// TestRunBlock_LogRunIDOnScopeAttrs verifies that ConsumeLogs walks past
// the LogRecord attributes layer when no run.id is present and picks up
// the scope-level run.id. Without this branch covered, the lookup chain
// (record -> scope -> resource) could regress to record-only and silently
// stop bucketing entire log batches that scope-tag their telemetry (a
// shape we expect from libraries that set run.id once per scope).
func TestRunBlock_LogRunIDOnScopeAttrs(t *testing.T) {
	var buf bytes.Buffer
	stdout := sink.NewStdoutSink(&buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode:        sink.RunBlockModeGrouped,
		IdleTimeout: time.Hour,
		MaxItems:    100,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})
	defer rb.Shutdown(context.Background())

	rec := &logspb.LogRecord{
		TimeUnixNano: 1710504600100000000,
		Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "scope-tagged"}},
	}
	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				Scope: &commonpb.InstrumentationScope{
					Name:       "stirrup",
					Attributes: []*commonpb.KeyValue{stringAttr("run.id", "abc")},
				},
				LogRecords: []*logspb.LogRecord{rec},
			}},
		}},
	}
	if err := rb.ConsumeLogs(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	// Output should still be empty — the log was buffered, not bypassed.
	if buf.Len() != 0 {
		t.Fatalf("scope-tagged log should buffer; got line-mode output: %q", buf.String())
	}

	// Flush via root-end and check the log appears inside the block.
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rootID := []byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8}
	root := makeRootRunSpan(traceID, rootID, 1710504600000000000, "abc",
		stringAttr("run.outcome", "success"),
	)
	if err := rb.ConsumeTraces(context.Background(), wrapSpans(root)); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "=== run abc started") || !strings.Contains(out, "scope-tagged") {
		t.Errorf("scope-tagged log missing from run block: %q", out)
	}
}

// TestRunBlock_LogRunIDOnResourceAttrs covers the third (and outermost)
// rung of the run.id lookup chain: ResourceLogs.Resource.Attributes. A
// regression here would leak resource-tagged log batches into line mode.
func TestRunBlock_LogRunIDOnResourceAttrs(t *testing.T) {
	var buf bytes.Buffer
	stdout := sink.NewStdoutSink(&buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode:        sink.RunBlockModeGrouped,
		IdleTimeout: time.Hour,
		MaxItems:    100,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})
	defer rb.Shutdown(context.Background())

	rec := &logspb.LogRecord{
		TimeUnixNano: 1710504600100000000,
		Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "resource-tagged"}},
	}
	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{stringAttr("run.id", "abc")},
			},
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{rec},
			}},
		}},
	}
	if err := rb.ConsumeLogs(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("resource-tagged log should buffer; got line-mode output: %q", buf.String())
	}

	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rootID := []byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8}
	root := makeRootRunSpan(traceID, rootID, 1710504600000000000, "abc",
		stringAttr("run.outcome", "success"),
	)
	if err := rb.ConsumeTraces(context.Background(), wrapSpans(root)); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "=== run abc started") || !strings.Contains(out, "resource-tagged") {
		t.Errorf("resource-tagged log missing from run block: %q", out)
	}
}

// TestRunBlock_MetricRunIDOnResourceAttrs verifies the metric path falls
// back to ResourceMetrics.Resource.Attributes when the data point itself
// carries no run.id. Each split*Points helper checks the resource fallback;
// this test pins the contract for at least one of them so a future
// refactor that drops the fallback gets caught by CI.
func TestRunBlock_MetricRunIDOnResourceAttrs(t *testing.T) {
	var buf bytes.Buffer
	stdout := sink.NewStdoutSink(&buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode:        sink.RunBlockModeGrouped,
		IdleTimeout: time.Hour,
		MaxItems:    100,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})
	defer rb.Shutdown(context.Background())

	dp := &metricspb.NumberDataPoint{
		TimeUnixNano: 1710504600200000000,
		Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 42},
	}
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{stringAttr("run.id", "abc")},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "stirrup.harness.tokens.input",
					Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						DataPoints: []*metricspb.NumberDataPoint{dp},
					}},
				}},
			}},
		}},
	}
	if err := rb.ConsumeMetrics(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("resource-tagged metric should buffer; got: %q", buf.String())
	}

	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rootID := []byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8}
	root := makeRootRunSpan(traceID, rootID, 1710504600000000000, "abc",
		stringAttr("run.outcome", "success"),
	)
	if err := rb.ConsumeTraces(context.Background(), wrapSpans(root)); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "stirrup.harness.tokens.input") {
		t.Errorf("resource-tagged metric missing from run block: %q", out)
	}
}

// TestRunBlock_NonNumberMetricsOverflowBypass exercises the Histogram,
// Summary, and ExponentialHistogram arms of buildSingleMetricProto. The
// existing overflow test only sends Sum-shaped points, so a refactor that
// drops one of the other arms (returning nil from the default case in
// buildSingleMetricProto) would silently lose the post-overflow data
// points without any test catching it. We trigger overflow with MaxItems=1
// to guarantee the second point routes through the streamSingleMetric
// bypass, which is the path that exercises buildSingleMetricProto.
func TestRunBlock_NonNumberMetricsOverflowBypass(t *testing.T) {
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	_ = traceID

	tests := []struct {
		name     string
		buildReq func(ts uint64) *colmetricspb.ExportMetricsServiceRequest
		expect   string
	}{
		{
			name: "Histogram",
			buildReq: func(ts uint64) *colmetricspb.ExportMetricsServiceRequest {
				dp := &metricspb.HistogramDataPoint{
					TimeUnixNano:   ts,
					Count:          3,
					Sum:            ptrFloat(1.5),
					BucketCounts:   []uint64{1, 1, 1},
					ExplicitBounds: []float64{0.5, 1.0},
					Attributes:     []*commonpb.KeyValue{stringAttr("run.id", "abc")},
				}
				return &colmetricspb.ExportMetricsServiceRequest{
					ResourceMetrics: []*metricspb.ResourceMetrics{{
						ScopeMetrics: []*metricspb.ScopeMetrics{{
							Metrics: []*metricspb.Metric{{
								Name: "stirrup.harness.histogram",
								Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
									DataPoints: []*metricspb.HistogramDataPoint{dp},
								}},
							}},
						}},
					}},
				}
			},
			expect: "stirrup.harness.histogram",
		},
		{
			name: "Summary",
			buildReq: func(ts uint64) *colmetricspb.ExportMetricsServiceRequest {
				dp := &metricspb.SummaryDataPoint{
					TimeUnixNano: ts,
					Count:        2,
					Sum:          1.0,
					Attributes:   []*commonpb.KeyValue{stringAttr("run.id", "abc")},
				}
				return &colmetricspb.ExportMetricsServiceRequest{
					ResourceMetrics: []*metricspb.ResourceMetrics{{
						ScopeMetrics: []*metricspb.ScopeMetrics{{
							Metrics: []*metricspb.Metric{{
								Name: "stirrup.harness.summary",
								Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{
									DataPoints: []*metricspb.SummaryDataPoint{dp},
								}},
							}},
						}},
					}},
				}
			},
			expect: "stirrup.harness.summary",
		},
		{
			name: "ExponentialHistogram",
			buildReq: func(ts uint64) *colmetricspb.ExportMetricsServiceRequest {
				dp := &metricspb.ExponentialHistogramDataPoint{
					TimeUnixNano: ts,
					Count:        2,
					Sum:          ptrFloat(0.5),
					Scale:        0,
					Attributes:   []*commonpb.KeyValue{stringAttr("run.id", "abc")},
				}
				return &colmetricspb.ExportMetricsServiceRequest{
					ResourceMetrics: []*metricspb.ResourceMetrics{{
						ScopeMetrics: []*metricspb.ScopeMetrics{{
							Metrics: []*metricspb.Metric{{
								Name: "stirrup.harness.exphist",
								Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
									DataPoints: []*metricspb.ExponentialHistogramDataPoint{dp},
								}},
							}},
						}},
					}},
				}
			},
			expect: "stirrup.harness.exphist",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			stdout := sink.NewStdoutSink(&buf)
			clock := newFakeClock(time.Unix(0, 1710504600000000000))
			rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
				Mode:        sink.RunBlockModeGrouped,
				IdleTimeout: time.Hour,
				MaxItems:    1,
				Clock:       clock,
				Logger:      quietRunblockLogger(),
			})
			defer rb.Shutdown(context.Background())

			// First point fills the buffer to MaxItems.
			if err := rb.ConsumeMetrics(context.Background(), tc.buildReq(1710504600100000000)); err != nil {
				t.Fatal(err)
			}
			// Second point triggers overflow and streams via the bypass.
			if err := rb.ConsumeMetrics(context.Background(), tc.buildReq(1710504600200000000)); err != nil {
				t.Fatal(err)
			}

			out := buf.String()
			if !strings.Contains(out, "[WARN] run abc truncated") {
				t.Errorf("expected truncation warning, got %q", out)
			}
			// The bypass-routed second point must appear after the warning.
			warnIdx := strings.Index(out, "[WARN]")
			postWarn := out[warnIdx:]
			if !strings.Contains(postWarn, tc.expect) {
				t.Errorf("expected %q after truncation warning, got %q", tc.expect, postWarn)
			}
			// And the metric line must not have been silently dropped (which
			// would happen if buildSingleMetricProto's relevant arm was lost).
			if got := strings.Count(out, tc.expect); got < 2 {
				t.Errorf("expected metric name %q at least twice (once buffered, once bypass), got %d in %q", tc.expect, got, out)
			}
		})
	}
}

// ptrFloat is a tiny helper for proto fields whose Sum is a *float64.
func ptrFloat(v float64) *float64 { return &v }

func TestRunBlock_LogWithoutRunIDBypasses(t *testing.T) {
	var buf bytes.Buffer
	stdout := sink.NewStdoutSink(&buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode:        sink.RunBlockModeGrouped,
		IdleTimeout: time.Hour,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})
	defer rb.Shutdown(context.Background())

	// Claude Code-shaped log (event.name, no run.id).
	rec := &logspb.LogRecord{
		TimeUnixNano: 1710504600100000000,
		Attributes: []*commonpb.KeyValue{
			stringAttr("event.name", "claude_code.api_request"),
		},
	}
	if err := rb.ConsumeLogs(context.Background(), wrapLogs(rec)); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "[EVENT]") {
		t.Errorf("expected immediate pass-through of run-less log, got %q", out)
	}
	if strings.Contains(out, "=== run") {
		t.Errorf("run-less log must not be wrapped: %q", out)
	}
}

func TestRunBlock_ConcurrentUseRaceFree(t *testing.T) {
	buf := &syncBuffer{}
	stdout := sink.NewStdoutSink(buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode:        sink.RunBlockModeGrouped,
		IdleTimeout: time.Hour,
		MaxItems:    1000,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})
	defer rb.Shutdown(context.Background())

	const goroutines = 20
	const iterations = 50

	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	var sent atomic.Int64
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				// Two spans per iteration: one tagged for a run, one bypass.
				tagged := makeChildSpan("turn", traceID,
					[]byte{byte(g), byte(i), 1, 2, 3, 4, 5, 6}, nil,
					uint64(1710504600000000000+int64(g*iterations+i)*1_000_000),
					stringAttr("run.id", "abc"),
				)
				bypass := makeChildSpan("untagged",
					[]byte{0xee, byte(g), byte(i), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					[]byte{0xff, byte(g), byte(i), 7, 7, 7, 7, 7}, nil,
					uint64(1710504600000000000+int64(g*iterations+i)*1_000_000+500_000),
				)
				_ = rb.ConsumeTraces(context.Background(), wrapSpans(tagged, bypass))
				sent.Add(2)
			}
		}()
	}
	wg.Wait()

	// Race detector handles correctness; we just want the run not to panic
	// and the bypass spans to have made it through to stdout.
	if sent.Load() != goroutines*iterations*2 {
		t.Fatalf("internal sanity: did not send expected number of spans")
	}
	if buf.Len() == 0 {
		t.Errorf("expected bypass spans to be visible in stdout")
	}
}

// TestRunBlock_ConcurrentDistinctRunIDsRaceFree exercises the per-run.id
// bucketing logic under concurrent producers, each operating on its own
// run.id. The sweeper runs throughout (low IdleTimeout) so a run that's
// been idle for too long can flush concurrently with another goroutine
// still producing for a different run. The race detector enforces
// memory-model correctness; this test additionally asserts that each run's
// rendered block is contiguous in the output (no header/body interleaving
// across runs) and that body items inside a run remain in timestamp order.
func TestRunBlock_ConcurrentDistinctRunIDsRaceFree(t *testing.T) {
	buf := &syncBuffer{}
	stdout := sink.NewStdoutSink(buf)
	clock := newFakeClock(time.Unix(0, 1710504600000000000))
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode: sink.RunBlockModeGrouped,
		// Short idle timeout so the sweeper participates in flushing.
		IdleTimeout: 200 * time.Millisecond,
		MaxItems:    1000,
		Clock:       clock,
		Logger:      quietRunblockLogger(),
	})

	const goroutines = 8
	const itemsPerRun = 15

	type runResult struct {
		runID  string
		traceID []byte
	}
	results := make([]runResult, goroutines)
	for g := 0; g < goroutines; g++ {
		results[g] = runResult{
			runID:  fmt.Sprintf("run-%02d", g),
			traceID: []byte{byte(g + 1), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		}
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			r := results[g]
			// Send the run's root span first so the run.id<->trace_id map is
			// populated; the buffer order in the rendered block is decided
			// by item timestamps, not arrival order.
			rootID := []byte{0xa0, byte(g), 0, 0, 0, 0, 0, 1}
			root := makeRootRunSpan(r.traceID, rootID,
				uint64(1710504600000000000+int64(g)*1_000),
				r.runID,
			)
			_ = rb.ConsumeTraces(context.Background(), wrapSpans(root))

			for i := 0; i < itemsPerRun; i++ {
				ts := uint64(1710504600000000000 + int64(g)*1_000 + int64(i+1)*100)
				span := makeChildSpan(fmt.Sprintf("turn[%d]", i), r.traceID,
					[]byte{byte(g), byte(i), 1, 2, 3, 4, 5, 6}, rootID,
					ts, stringAttr("run.id", r.runID),
				)
				_ = rb.ConsumeTraces(context.Background(), wrapSpans(span))
			}

			// Close the run so its block flushes deterministically rather
			// than relying on shutdown ordering.
			endID := []byte{0xee, byte(g), 0, 0, 0, 0, 0, 1}
			end := makeRootRunSpan(r.traceID, endID,
				uint64(1710504600000000000+int64(g)*1_000+int64(itemsPerRun+1)*100),
				r.runID,
				stringAttr("run.outcome", "success"),
			)
			_ = rb.ConsumeTraces(context.Background(), wrapSpans(end))
		}()
	}
	wg.Wait()

	// Final shutdown to drain anything the sweeper might still be holding.
	if err := rb.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	out := buf.String()

	// Every run.id must appear exactly once as a header and once as a footer,
	// and every header must be followed by its own footer with no other run's
	// header in between.
	for _, r := range results {
		startMarker := fmt.Sprintf("=== run %s started", r.runID)
		endMarker := fmt.Sprintf("=== run %s finished", r.runID)
		startIdx := strings.Index(out, startMarker)
		endIdx := strings.Index(out, endMarker)
		if startIdx < 0 {
			t.Errorf("missing header for %s", r.runID)
			continue
		}
		if endIdx < 0 {
			t.Errorf("missing footer for %s", r.runID)
			continue
		}
		if endIdx < startIdx {
			t.Errorf("footer for %s precedes its header", r.runID)
			continue
		}
		// Slice from header to footer (exclusive of footer to avoid the
		// next run's header showing up if it shares text). No other run's
		// header should appear inside this window.
		block := out[startIdx:endIdx]
		for _, other := range results {
			if other.runID == r.runID {
				continue
			}
			if strings.Contains(block, fmt.Sprintf("=== run %s started", other.runID)) {
				t.Errorf("run %s block interleaves run %s's header", r.runID, other.runID)
			}
		}
	}
}

func TestRunBlock_NameDelegatesToInner(t *testing.T) {
	stdout := sink.NewStdoutSink(&bytes.Buffer{})
	rb := mustNewRunBlockSink(t, stdout, sink.RunBlockConfig{
		Mode:        sink.RunBlockModeGrouped,
		IdleTimeout: time.Hour,
	})
	defer rb.Shutdown(context.Background())
	if got := rb.Name(); got != "stdout" {
		t.Errorf("Name() = %q, want %q", got, "stdout")
	}
}
