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

// TestRenderRunBlock_Tree_MultipleDisconnectedSubtrees verifies that two
// independent (orphan) parent spans render at depth 0 and each of their
// children renders at depth 1. The previous tree tests covered a single
// connected tree only; without this case a regression that conflated the
// two trees (e.g. depth computed against the buffer's earliest span
// rather than the parent edge) would not be caught.
func TestRenderRunBlock_Tree_MultipleDisconnectedSubtrees(t *testing.T) {
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	parentA := []byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8}
	childA := []byte{0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf, 0xb0}
	parentB := []byte{0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8}
	childB := []byte{0xb9, 0xba, 0xbb, 0xbc, 0xbd, 0xbe, 0xbf, 0xc0}

	pA := makeSpan("parent_a", traceID, parentA, nil, 1710504600000000000)
	cA := makeSpan("child_a", traceID, childA, parentA, 1710504601000000000)
	pB := makeSpan("parent_b", traceID, parentB, nil, 1710504602000000000)
	cB := makeSpan("child_b", traceID, childB, parentB, 1710504603000000000)

	items := []RunBlockItem{
		{Timestamp: pA.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: pA},
		{Timestamp: cA.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: cA},
		{Timestamp: pB.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: pB},
		{Timestamp: cB.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: cB},
	}
	lines := RenderRunBlock(RunBlockTree, RunBlockHeader{RunID: "abc"}, items, RunBlockFooter{RunID: "abc"})
	if len(lines) != 6 {
		t.Fatalf("expected 6 lines (header+4 items+footer), got %d: %q", len(lines), lines)
	}
	// Both parents at depth 0; both children at depth 1. The line shape
	// "[TRACE]  " followed by depth*2 spaces puts depth-0 lines at "[TRACE]  2024-"
	// and depth-1 lines at "[TRACE]    2024-".
	if !strings.HasPrefix(lines[1], "[TRACE]  2024-") || !strings.Contains(lines[1], "parent_a") {
		t.Errorf("parent_a should render at depth 0: %q", lines[1])
	}
	if !strings.HasPrefix(lines[2], "[TRACE]    2024-") || !strings.Contains(lines[2], "child_a") {
		t.Errorf("child_a should render at depth 1: %q", lines[2])
	}
	if !strings.HasPrefix(lines[3], "[TRACE]  2024-") || !strings.Contains(lines[3], "parent_b") {
		t.Errorf("parent_b should render at depth 0: %q", lines[3])
	}
	if !strings.HasPrefix(lines[4], "[TRACE]    2024-") || !strings.Contains(lines[4], "child_b") {
		t.Errorf("child_b should render at depth 1: %q", lines[4])
	}
}

// TestRenderRunBlock_Tree_CycleProtection ensures computeSpanDepths does
// not infinite-recurse if two spans claim each other as parents (an
// invalid but possible OTLP shape from a buggy producer). The "hops >
// len(spans)" guard inside resolve terminates the recursion. We assert
// the renderer returns and that neither span vanishes from the output.
//
// Note on depth: the current guard prevents stack overflow but does not
// guarantee depth=0 for the cycle's nodes — once the budget fires, the
// memoised zero gets overwritten on the unwind, so the visible depth is
// bounded but non-zero. That bound is what we assert here; if a future
// change makes the cycle's nodes render at the bounded depth or at zero
// the test stays passing as long as termination holds.
func TestRenderRunBlock_Tree_CycleProtection(t *testing.T) {
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanA := []byte{0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa}
	spanB := []byte{0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb}

	// A's parent is B; B's parent is A. A direct cycle.
	a := makeSpan("cycle_a", traceID, spanA, spanB, 1710504600000000000)
	b := makeSpan("cycle_b", traceID, spanB, spanA, 1710504601000000000)

	items := []RunBlockItem{
		{Timestamp: a.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: a},
		{Timestamp: b.StartTimeUnixNano, Kind: RunBlockItemSpan, Span: b},
	}
	// The primary assertion is termination: if computeSpanDepths regressed
	// and entered an unbounded recursion, Go would surface a stack overflow
	// rather than reaching the assertions below.
	lines := RenderRunBlock(RunBlockTree, RunBlockHeader{RunID: "abc"}, items, RunBlockFooter{RunID: "abc"})
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines (header+2 items+footer), got %d: %q", len(lines), lines)
	}
	// Both span names must appear — neither should be silently dropped.
	body := lines[1] + "\n" + lines[2]
	if !strings.Contains(body, "cycle_a") {
		t.Errorf("cycle_a missing from rendered output: %q", body)
	}
	if !strings.Contains(body, "cycle_b") {
		t.Errorf("cycle_b missing from rendered output: %q", body)
	}
	// And the visible indent depth must be bounded by len(spans); without
	// the guard the recursion would never have stopped to produce a finite
	// depth. We allow up to len(spans)+1 levels (the depth at which the
	// guard fires and the unwind kicks in), which gives the renderer the
	// freedom to choose any reasonable cycle-handling policy short of
	// hanging.
	for i, line := range []string{lines[1], lines[2]} {
		const tracePrefix = "[TRACE]  "
		idx := strings.Index(line, "2024-")
		if idx < 0 {
			t.Errorf("line %d missing timestamp marker: %q", i+1, line)
			continue
		}
		indentLen := idx - len(tracePrefix)
		if indentLen < 0 || indentLen%2 != 0 {
			t.Errorf("line %d has malformed indent (len=%d): %q", i+1, indentLen, line)
			continue
		}
		// len(spans) = 2, so depth budget caps at 2; the unwind can produce
		// up to depth 3 (root + len(spans) increments). Indent is depth*2.
		if indentLen > 6 {
			t.Errorf("line %d indent %d exceeds cycle-bounded budget: %q", i+1, indentLen, line)
		}
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
