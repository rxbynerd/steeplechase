package format

import (
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// makeSpan is a small helper that keeps the call sites in this file readable.
func makeSpan(name string, traceID, spanID, parentID []byte, ts uint64, attrs ...*commonpb.KeyValue) *tracepb.Span {
	return &tracepb.Span{
		Name:              name,
		TraceId:           traceID,
		SpanId:            spanID,
		ParentSpanId:      parentID,
		StartTimeUnixNano: ts,
		Attributes:        attrs,
	}
}

func attr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func TestRenderRunBlock_Grouped_HappyPath(t *testing.T) {
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rootID := []byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8}
	turnID := []byte{0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8}

	root := makeSpan("run", traceID, rootID, nil, 1710504600000000000,
		attr("run.id", "abc"),
		attr("run.mode", "execution"),
		attr("run.provider", "anthropic"),
		attr("run.model", "claude-sonnet-4-6"),
	)
	turn := makeSpan("turn[1]", traceID, turnID, rootID, 1710504601000000000)

	items := []RunBlockItem{
		// Out of order to verify the renderer sorts by timestamp.
		{Timestamp: turn.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: turn},
		{Timestamp: root.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: root},
	}
	header := RunBlockHeader{
		RunID:    "abc",
		Mode:     "execution",
		Provider: "anthropic",
		Model:    "claude-sonnet-4-6",
	}
	footer := RunBlockFooter{RunID: "abc", Outcome: "success", Turns: "1"}

	lines := RenderRunBlock(RunBlockGrouped, header, items, footer)
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines (header+2 items+footer), got %d: %q", len(lines), lines)
	}

	if !strings.HasPrefix(lines[0], "=== run abc started") {
		t.Errorf("header missing run id/start: %q", lines[0])
	}
	for _, want := range []string{"mode=execution", "provider=anthropic", "model=claude-sonnet-4-6"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("header missing %q: %q", want, lines[0])
		}
	}

	// Body must be in chronological order despite the swapped input slice.
	if !strings.Contains(lines[1], "run") || !strings.Contains(lines[2], "turn[1]") {
		t.Errorf("body items not chronological: %q / %q", lines[1], lines[2])
	}
	if !strings.HasPrefix(lines[1], "[TRACE]") {
		t.Errorf("expected line-mode shape preserved: %q", lines[1])
	}

	if !strings.Contains(lines[3], "outcome=success") || !strings.Contains(lines[3], "turns=1") {
		t.Errorf("footer missing outcome/turns: %q", lines[3])
	}
}

func TestRenderRunBlock_HeaderFallsBackToEarliestItemTimestamp(t *testing.T) {
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	span := makeSpan("turn[1]", traceID, []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil, 1710504600123000000)

	items := []RunBlockItem{
		{Timestamp: span.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: span},
	}
	// No StartedAt set; renderer should fall back.
	header := RunBlockHeader{RunID: "abc"}
	footer := RunBlockFooter{RunID: "abc"}

	lines := RenderRunBlock(RunBlockGrouped, header, items, footer)
	if !strings.Contains(lines[0], "started 2024-03-15T") {
		t.Errorf("header should fall back to earliest item ts: %q", lines[0])
	}
	// No mode/provider/model in header — they must be elided cleanly.
	if strings.Contains(lines[0], "mode=") || strings.Contains(lines[0], "provider=") || strings.Contains(lines[0], "model=") {
		t.Errorf("optional header fields should be elided when empty: %q", lines[0])
	}
	if !strings.Contains(lines[len(lines)-1], "outcome=<unknown>") {
		t.Errorf("missing outcome should default to <unknown>: %q", lines[len(lines)-1])
	}
	if strings.Contains(lines[len(lines)-1], "turns=") {
		t.Errorf("turns should be elided when empty: %q", lines[len(lines)-1])
	}
}

func TestRenderRunBlock_Tree_IndentsByDepth(t *testing.T) {
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rootID := []byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8}
	turnID := []byte{0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8}
	toolID := []byte{0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8}

	root := makeSpan("run", traceID, rootID, nil, 1710504600000000000)
	turn := makeSpan("turn[1]", traceID, turnID, rootID, 1710504601000000000)
	tool := makeSpan("tool_call", traceID, toolID, turnID, 1710504602000000000)

	items := []RunBlockItem{
		{Timestamp: root.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: root},
		{Timestamp: turn.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: turn},
		{Timestamp: tool.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: tool},
	}

	lines := RenderRunBlock(RunBlockTree, RunBlockHeader{RunID: "abc"}, items, RunBlockFooter{RunID: "abc"})
	// header + 3 spans + footer
	if len(lines) != 5 {
		t.Fatalf("expected 5 lines, got %d: %q", len(lines), lines)
	}

	// The "[TRACE]  " prefix is fixed width; depth indent goes between it and
	// the timestamp/name. Depth 0 keeps the line shape unchanged.
	if !strings.HasPrefix(lines[1], "[TRACE]  2024-") {
		t.Errorf("root span should be at depth 0 (no indent): %q", lines[1])
	}
	if !strings.HasPrefix(lines[2], "[TRACE]    2024-") {
		t.Errorf("turn span should be indented one level (two extra spaces before timestamp): %q", lines[2])
	}
	if !strings.HasPrefix(lines[3], "[TRACE]      2024-") {
		t.Errorf("tool_call span should be indented two levels: %q", lines[3])
	}
}

func TestRenderRunBlock_Tree_OrphanSpansAtDepthZero(t *testing.T) {
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	parentID := []byte{0xfe, 0xed, 0xfa, 0xce, 0xfe, 0xed, 0xfa, 0xce}
	childID := []byte{0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8}

	// Child whose parent is NOT in the buffer.
	orphan := makeSpan("turn[1]", traceID, childID, parentID, 1710504600000000000)

	items := []RunBlockItem{
		{Timestamp: orphan.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: orphan},
	}
	lines := RenderRunBlock(RunBlockTree, RunBlockHeader{RunID: "abc"}, items, RunBlockFooter{RunID: "abc"})
	if !strings.HasPrefix(lines[1], "[TRACE]  2024-") {
		t.Errorf("orphan span should render at depth 0 (no extra indent): %q", lines[1])
	}
	if !strings.Contains(lines[1], "turn[1]") {
		t.Errorf("orphan span name missing: %q", lines[1])
	}
}

func TestRenderRunBlock_Tree_LogsAndMetricsRemainFlat(t *testing.T) {
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rootID := []byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8}
	turnID := []byte{0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8}

	root := makeSpan("run", traceID, rootID, nil, 1710504600000000000)
	turn := makeSpan("turn[1]", traceID, turnID, rootID, 1710504602000000000)

	logRecord := &logspb.LogRecord{
		TimeUnixNano:   1710504601000000000,
		SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
		Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hi"}},
	}
	dp := &metricspb.NumberDataPoint{
		TimeUnixNano: 1710504601500000000,
		Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 7},
	}

	items := []RunBlockItem{
		{Timestamp: root.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: root},
		{Timestamp: logRecord.TimeUnixNano, Kind: RunBlockItemLog, Log: logRecord},
		{Timestamp: dp.TimeUnixNano, Kind: RunBlockItemMetric, Metric: &RunBlockMetric{
			MetricName: "stirrup.harness.tokens.input",
			DataPoint:  &RunBlockMetricDataPoint{Number: dp, NumberIsSum: true},
		}},
		{Timestamp: turn.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: turn},
	}
	lines := RenderRunBlock(RunBlockTree, RunBlockHeader{RunID: "abc"}, items, RunBlockFooter{RunID: "abc"})
	// header + 4 items + footer = 6
	if len(lines) != 6 {
		t.Fatalf("expected 6 lines, got %d: %q", len(lines), lines)
	}
	// Chronological interleaving: root span (depth 0), log, metric, turn (depth 1).
	if !strings.HasPrefix(lines[1], "[TRACE]  2024-") || !strings.Contains(lines[1], " run ") {
		t.Errorf("expected root first at depth 0: %q", lines[1])
	}
	if !strings.HasPrefix(lines[2], "[LOG]") {
		t.Errorf("expected log next, with no indent: %q", lines[2])
	}
	if !strings.HasPrefix(lines[3], "[METRIC]") {
		t.Errorf("expected metric next, with no indent: %q", lines[3])
	}
	if !strings.HasPrefix(lines[4], "[TRACE]    2024-") || !strings.Contains(lines[4], "turn[1]") {
		t.Errorf("expected turn span indented one level: %q", lines[4])
	}
}

func TestRenderRunBlock_TruncationWarning(t *testing.T) {
	got := RenderRunBlockTruncationWarning("abc", 10000)
	want := "[WARN] run abc truncated at 10000 items"
	if got != want {
		t.Errorf("RenderRunBlockTruncationWarning = %q, want %q", got, want)
	}
}

func TestRenderRunBlock_GroupedPreservesLineModeShapes(t *testing.T) {
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	span := makeSpan("turn[1]", traceID, []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil, 1710504600123000000,
		attr("turn.number", "1"),
	)

	items := []RunBlockItem{
		{Timestamp: span.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: span},
	}
	got := RenderRunBlock(RunBlockGrouped, RunBlockHeader{RunID: "abc"}, items, RunBlockFooter{RunID: "abc"})
	want := FormatTraces([]*tracepb.ResourceSpans{{
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{span}}},
	}})
	if got[1] != want[0] {
		t.Errorf("grouped body line should be byte-for-byte identical to FormatTraces output:\n got: %q\nwant: %q", got[1], want[0])
	}
}
