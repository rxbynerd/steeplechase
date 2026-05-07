package format

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// RunBlockItemKind classifies a buffered item so the renderer can route it
// through the matching per-line formatter.
type RunBlockItemKind int

const (
	RunBlockItemSpan RunBlockItemKind = iota
	RunBlockItemLog
	RunBlockItemMetric
)

// RunBlockItem is a single buffered telemetry record belonging to a run.
// Exactly one of Span / Log / Metric must be set, matching Kind. Timestamp
// is the wall-clock instant the item was generated (UnixNano) — items are
// emitted in ascending timestamp order on flush.
//
// Metric carries the resource attributes alongside the data point so the
// rendered line can merge resource-level labels (e.g. service.name) into
// the per-point output, just as FormatMetrics does in line mode.
type RunBlockItem struct {
	Timestamp uint64
	Kind      RunBlockItemKind

	Span *tracepb.Span

	Log *logspb.LogRecord

	Metric         *RunBlockMetric
}

// RunBlockMetric carries the minimum information needed to re-render a
// single metric data point through FormatMetrics. We keep the original
// resource attributes alongside the data point so the rendered line is
// indistinguishable from a line-mode emission of the same point.
type RunBlockMetric struct {
	MetricName     string
	DataPoint      *RunBlockMetricDataPoint
	ResourceAttrs  []*commonpb.KeyValue
}

// RunBlockMetricDataPoint is a discriminated union of the OTLP data-point
// shapes we render. Exactly one field should be non-nil; the renderer
// dispatches on whichever it finds.
type RunBlockMetricDataPoint struct {
	Number              *metricspb.NumberDataPoint
	NumberIsSum         bool // distinguishes sum vs gauge for line shape parity
	Histogram           *metricspb.HistogramDataPoint
	Summary             *metricspb.SummaryDataPoint
	ExponentialHistogram *metricspb.ExponentialHistogramDataPoint
}

// RunBlockHeader carries the fields used to render the per-run banner. Any
// field left zero/empty is elided from the rendered header line.
type RunBlockHeader struct {
	RunID     string
	StartedAt string // ISO8601, optional; falls back to earliest item timestamp
	Mode      string
	Provider  string
	Model     string
}

// RunBlockFooter carries the fields used to render the closing banner. If
// FinishedAt is empty the renderer falls back to the latest item timestamp.
// Outcome may be "<unknown>" when the block was flushed by idle timeout or
// shutdown rather than by observing the root span's end.
type RunBlockFooter struct {
	RunID      string
	FinishedAt string
	Outcome    string
	Turns      string // string so we can elide it cleanly when unknown
}

// RunBlockRenderMode selects the body layout for Render.
type RunBlockRenderMode int

const (
	// RunBlockGrouped renders all items flat in chronological order.
	RunBlockGrouped RunBlockRenderMode = iota
	// RunBlockTree renders spans as a parent->child tree (per trace_id) with
	// two-space indentation per depth level. Logs and metrics remain flat,
	// chronologically interleaved with the trace lines.
	RunBlockTree
)

// RenderRunBlock produces the full set of lines for one run: header, body,
// footer. Items are sorted by timestamp ascending before rendering. The
// caller is responsible for any trailing newline/blank-line policy.
func RenderRunBlock(mode RunBlockRenderMode, header RunBlockHeader, items []RunBlockItem, footer RunBlockFooter) []string {
	out := make([]string, 0, len(items)+2)
	out = append(out, renderHeader(header, items))

	sorted := make([]RunBlockItem, len(items))
	copy(sorted, items)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})

	switch mode {
	case RunBlockTree:
		out = append(out, renderTreeBody(sorted)...)
	default:
		out = append(out, renderGroupedBody(sorted)...)
	}

	out = append(out, renderFooter(footer, sorted))
	return out
}

// RenderRunBlockTruncationWarning produces the in-stream warn line emitted
// when a run's buffer overflows the configured cap. Sinks call this in place
// of the normal footer when force-flushing on overflow.
func RenderRunBlockTruncationWarning(runID string, count int) string {
	return fmt.Sprintf("[WARN] run %s truncated at %d items", runID, count)
}

func renderHeader(h RunBlockHeader, items []RunBlockItem) string {
	started := h.StartedAt
	if started == "" && len(items) > 0 {
		earliest := items[0].Timestamp
		for _, it := range items[1:] {
			if it.Timestamp < earliest {
				earliest = it.Timestamp
			}
		}
		started = FormatTimestamp(earliest)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "=== run %s started %s", h.RunID, started)
	if h.Mode != "" {
		fmt.Fprintf(&b, " mode=%s", h.Mode)
	}
	if h.Provider != "" {
		fmt.Fprintf(&b, " provider=%s", h.Provider)
	}
	if h.Model != "" {
		fmt.Fprintf(&b, " model=%s", h.Model)
	}
	b.WriteString(" ===")
	return b.String()
}

func renderFooter(f RunBlockFooter, sorted []RunBlockItem) string {
	finished := f.FinishedAt
	if finished == "" && len(sorted) > 0 {
		finished = FormatTimestamp(sorted[len(sorted)-1].Timestamp)
	}
	outcome := f.Outcome
	if outcome == "" {
		outcome = "<unknown>"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "=== run %s finished %s outcome=%s", f.RunID, finished, outcome)
	if f.Turns != "" {
		fmt.Fprintf(&b, " turns=%s", f.Turns)
	}
	b.WriteString(" ===")
	return b.String()
}

func renderGroupedBody(sorted []RunBlockItem) []string {
	out := make([]string, 0, len(sorted))
	for _, it := range sorted {
		out = append(out, renderItem(it, 0)...)
	}
	return out
}

// renderTreeBody preserves chronological interleaving across signals while
// indenting spans according to their depth in the parent tree built per
// trace_id. Orphan spans (those whose parent_span_id is unknown to the
// buffer) sit at depth zero.
func renderTreeBody(sorted []RunBlockItem) []string {
	depth := computeSpanDepths(sorted)

	out := make([]string, 0, len(sorted))
	for _, it := range sorted {
		if it.Kind == RunBlockItemSpan && it.Span != nil {
			d := depth[hex.EncodeToString(it.Span.SpanId)]
			out = append(out, renderItem(it, d)...)
			continue
		}
		out = append(out, renderItem(it, 0)...)
	}
	return out
}

// computeSpanDepths walks the parent_span_id edges of every span in the
// buffer and returns a map of span_id (hex) -> depth. Orphans whose parent
// is not present in the buffer get depth 0. Cycles defend against malformed
// input by capping at the number of buffered spans.
func computeSpanDepths(items []RunBlockItem) map[string]int {
	spans := make(map[string]*tracepb.Span)
	for _, it := range items {
		if it.Kind == RunBlockItemSpan && it.Span != nil && len(it.Span.SpanId) > 0 {
			spans[hex.EncodeToString(it.Span.SpanId)] = it.Span
		}
	}

	depth := make(map[string]int, len(spans))
	var resolve func(string, int) int
	resolve = func(id string, hops int) int {
		if d, ok := depth[id]; ok {
			return d
		}
		if hops > len(spans) {
			// Cycle protection: treat the offending span as a root.
			depth[id] = 0
			return 0
		}
		span := spans[id]
		if span == nil {
			return 0
		}
		if len(span.ParentSpanId) == 0 {
			depth[id] = 0
			return 0
		}
		parentID := hex.EncodeToString(span.ParentSpanId)
		if _, ok := spans[parentID]; !ok {
			// Parent not in buffer: orphan, render at root.
			depth[id] = 0
			return 0
		}
		d := resolve(parentID, hops+1) + 1
		depth[id] = d
		return d
	}
	for id := range spans {
		resolve(id, 0)
	}
	return depth
}

// renderItem produces the one or more lines for a single buffered item by
// running it through the matching line-mode formatter. The depth argument
// applies only to spans (and only in tree mode); zero produces output that
// is byte-for-byte identical to FormatTraces / FormatLogs / FormatMetrics.
func renderItem(it RunBlockItem, depth int) []string {
	switch it.Kind {
	case RunBlockItemSpan:
		return renderSpanItem(it.Span, depth)
	case RunBlockItemLog:
		return renderLogItem(it.Log)
	case RunBlockItemMetric:
		return renderMetricItem(it.Metric)
	default:
		return nil
	}
}

func renderSpanItem(span *tracepb.Span, depth int) []string {
	if span == nil {
		return nil
	}
	rs := []*tracepb.ResourceSpans{{
		ScopeSpans: []*tracepb.ScopeSpans{{
			Spans: []*tracepb.Span{span},
		}},
	}}
	lines := FormatTraces(rs)
	if depth <= 0 {
		return lines
	}
	indent := strings.Repeat("  ", depth)
	out := make([]string, len(lines))
	for i, line := range lines {
		out[i] = indentTraceLine(line, indent)
	}
	return out
}

// indentTraceLine inserts the provided indent immediately after the
// "[TRACE]  " prefix so the bracketed kind tag still aligns vertically with
// log and metric lines while the span name moves rightward by depth*2.
func indentTraceLine(line, indent string) string {
	const prefix = "[TRACE]  "
	if !strings.HasPrefix(line, prefix) {
		return indent + line
	}
	return prefix + indent + line[len(prefix):]
}

func renderLogItem(lr *logspb.LogRecord) []string {
	if lr == nil {
		return nil
	}
	rl := []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{lr},
		}},
	}}
	return FormatLogs(rl)
}

func renderMetricItem(m *RunBlockMetric) []string {
	if m == nil || m.DataPoint == nil {
		return nil
	}
	metric := buildSyntheticMetric(m)
	if metric == nil {
		return nil
	}
	rm := []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{Attributes: m.ResourceAttrs},
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Metrics: []*metricspb.Metric{metric},
		}},
	}}
	return FormatMetrics(rm)
}

// buildSyntheticMetric reconstructs a single-data-point metric proto so the
// existing FormatMetrics path can render it. We pick the matching wrapper
// (Sum vs Gauge for number points, etc.) so the line shape is identical to
// what line mode would have emitted for the original payload.
func buildSyntheticMetric(m *RunBlockMetric) *metricspb.Metric {
	dp := m.DataPoint
	switch {
	case dp.Number != nil:
		if dp.NumberIsSum {
			return &metricspb.Metric{
				Name: m.MetricName,
				Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
					DataPoints: []*metricspb.NumberDataPoint{dp.Number},
				}},
			}
		}
		return &metricspb.Metric{
			Name: m.MetricName,
			Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
				DataPoints: []*metricspb.NumberDataPoint{dp.Number},
			}},
		}
	case dp.Histogram != nil:
		return &metricspb.Metric{
			Name: m.MetricName,
			Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
				DataPoints: []*metricspb.HistogramDataPoint{dp.Histogram},
			}},
		}
	case dp.Summary != nil:
		return &metricspb.Metric{
			Name: m.MetricName,
			Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{
				DataPoints: []*metricspb.SummaryDataPoint{dp.Summary},
			}},
		}
	case dp.ExponentialHistogram != nil:
		return &metricspb.Metric{
			Name: m.MetricName,
			Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
				DataPoints: []*metricspb.ExponentialHistogramDataPoint{dp.ExponentialHistogram},
			}},
		}
	default:
		return nil
	}
}
