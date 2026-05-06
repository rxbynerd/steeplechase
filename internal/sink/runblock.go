package sink

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/rxbynerd/steeplechase/internal/format"
)

// linesWriter is the contract a sink must implement to be wrapped by
// RunBlockSink. RunBlockSink renders each flushed buffer to a slice of
// pre-formatted lines and forwards them through this method, which lets the
// inner sink keep its own write serialisation (e.g. *StdoutSink's mutex)
// intact for buffered flushes. *StdoutSink is the sole production
// implementation; NewRunBlockSink type-asserts inner to this interface up
// front and refuses to construct otherwise.
type linesWriter interface {
	writeRunBlockLines(ctx context.Context, lines []string) error
}

// RunBlockMode selects the rendering strategy used at flush time.
type RunBlockMode int

const (
	// RunBlockModeGrouped emits items flat in chronological order, wrapped in
	// header/footer banners.
	RunBlockModeGrouped RunBlockMode = iota
	// RunBlockModeTree emits spans as a parent->child tree per trace_id; logs
	// and metrics remain flat but interleaved by timestamp.
	RunBlockModeTree
)

// Clock is the minimal time source the sink depends on. Production wiring
// uses the real wall clock; tests inject a fake to exercise idle-timeout
// flushing without sleeping.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// RunBlockConfig configures the buffered stdout sink.
type RunBlockConfig struct {
	// Mode controls whether the body is rendered grouped or as a tree.
	Mode RunBlockMode

	// IdleTimeout is the per-run idle deadline. If no items have arrived for a
	// run for this long, the run is flushed with outcome=<unknown>. The
	// background sweeper polls at IdleTimeout/4 (minimum 100ms).
	IdleTimeout time.Duration

	// MaxItems caps the number of buffered items per run before the sink
	// force-flushes with a truncation warning and switches that run.id to
	// pass-through for any subsequent items.
	MaxItems int

	// Clock is the time source. Defaults to wall-clock.
	Clock Clock

	// Logger is used for slog-level diagnostics (overflow, sweep errors).
	// A nil logger falls back to slog.Default.
	Logger *slog.Logger
}

// RunBlockSink wraps an inner sink (in practice *StdoutSink) and buffers
// run-tagged OTLP items per run.id. On a flush trigger it renders the
// accumulated buffer as a header/body/footer block via the format package
// and forwards the resulting line stream to the inner sink (which preserves
// the existing mutex serialisation against any other writers it may share).
//
// Items without a discoverable run.id bypass the buffer entirely and are
// forwarded immediately, matching today's line-mode behaviour. The buffer's
// trace_id -> run.id map lets late-arriving child spans whose own
// run.id attribute is missing follow the same bucketing as their root.
type RunBlockSink struct {
	inner  Sink
	writer linesWriter // same object as inner, type-asserted at construction
	cfg    RunBlockConfig
	clock  Clock
	logger *slog.Logger

	// mu guards runs, traceToRun, and truncated. Lock order, when chained:
	// RunBlockSink.mu -> StdoutSink.mu (acquired inside writeRunBlockLines).
	// Never invert.
	mu sync.Mutex
	// runs holds open per-run.id buffers.
	runs map[string]*runBuffer
	// traceToRun maps trace_id (hex) to the run.id we last saw for that
	// trace. Cleared on flush. Spans-only.
	//
	// This map cannot grow without bound: every entry's run.id is also a
	// key in s.runs, and each run is forced to flush by one of the four
	// flushReason paths (root-end, idle sweep, overflow, shutdown).
	// flushLocked's defer deletes both the run from s.runs and every
	// traceToRun entry that pointed at it, so the map's high-water mark is
	// the live working-set of runs at any instant.
	traceToRun map[string]string
	// truncated tracks run.ids that overflowed; subsequent items for them
	// bypass the buffer and stream as line mode would.
	truncated map[string]bool

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// pendingItem couples an item with the run.id it should be appended to.
// It exists to keep the per-signal helper functions and the inline
// post-processing loops in agreement on the shape of "stuff to enqueue".
type pendingItem struct {
	runID string
	item  format.RunBlockItem
}

// runBuffer is the per-run.id state. All field access is guarded by the
// owning RunBlockSink.mu.
type runBuffer struct {
	runID string

	items []format.RunBlockItem

	// header fields are populated from the first time we observe the root
	// span (named "run", no parent) carrying the canonical run.* attributes.
	headerKnown bool
	mode        string
	provider    string
	model       string
	startedAt   string

	// footer fields are populated when we see the root span end with
	// run.outcome attached.
	rootEnded bool
	outcome   string
	turns     string
	endedAt   string

	// Last activity wall-clock for idle-timeout sweeping.
	lastActivity time.Time
}

// NewRunBlockSink wraps inner with run.id-keyed buffering. Inner must
// implement the unexported linesWriter interface — *StdoutSink is the only
// type that does today. Returning an error here (rather than falling back to
// a synthetic-LogRecord path) keeps the buffered flush path simple and
// avoids the malformed-output hazard of routing already-formatted lines
// through ConsumeLogs.
func NewRunBlockSink(inner Sink, cfg RunBlockConfig) (*RunBlockSink, error) {
	writer, ok := inner.(linesWriter)
	if !ok {
		return nil, fmt.Errorf("runblock: inner sink %T does not implement linesWriter (expected *StdoutSink)", inner)
	}

	clock := cfg.Clock
	if clock == nil {
		clock = realClock{}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 5 * time.Minute
	}
	if cfg.MaxItems <= 0 {
		cfg.MaxItems = 10000
	}

	s := &RunBlockSink{
		inner:      inner,
		writer:     writer,
		cfg:        cfg,
		clock:      clock,
		logger:     logger,
		runs:       map[string]*runBuffer{},
		traceToRun: map[string]string{},
		truncated:  map[string]bool{},
		stopCh:     make(chan struct{}),
	}
	s.startSweeper()
	return s, nil
}

// Name returns the inner sink's label so Prometheus metrics keep the same
// per-sink identity ("stdout") regardless of the buffering wrapper.
func (s *RunBlockSink) Name() string { return s.inner.Name() }

// Shutdown flushes every open buffer with outcome=<unknown> (treating
// shutdown as an idle-timeout flush) and then shuts the inner sink down.
// The sweeper goroutine is stopped before the final flush so it does not
// race with the shutdown path.
func (s *RunBlockSink) Shutdown(ctx context.Context) error {
	close(s.stopCh)
	s.wg.Wait()

	s.mu.Lock()
	open := make([]*runBuffer, 0, len(s.runs))
	for _, rb := range s.runs {
		open = append(open, rb)
	}
	// Determinism is nice for shutdown logs and tests.
	sort.Slice(open, func(i, j int) bool { return open[i].runID < open[j].runID })
	for _, rb := range open {
		s.flushLocked(ctx, rb, flushReasonShutdown)
	}
	s.mu.Unlock()

	return s.inner.Shutdown(ctx)
}

// flushReason explains why a buffer is being emitted; it picks the right
// footer shape and any in-stream warnings.
type flushReason int

const (
	flushReasonRootEnded flushReason = iota
	flushReasonIdle
	flushReasonOverflow
	flushReasonShutdown
)

// startSweeper launches the background idle-timeout watcher. The poll
// interval is IdleTimeout/4, clamped to a 100ms floor so very small
// timeouts in tests still get prompt sweeps without spinning the CPU.
func (s *RunBlockSink) startSweeper() {
	interval := s.cfg.IdleTimeout / 4
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-s.stopCh:
				return
			case <-t.C:
				s.sweepIdle()
			}
		}
	}()
}

func (s *RunBlockSink) sweepIdle() {
	now := s.clock.Now()
	ctx := context.Background()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rb := range s.runs {
		if now.Sub(rb.lastActivity) >= s.cfg.IdleTimeout {
			s.flushLocked(ctx, rb, flushReasonIdle)
		}
	}
}

// ConsumeMetrics buffers data points carrying a run.id (or whose resource
// carries one) and pass-throughs anything else. A request can mix items
// for several runs and unidentified items in arbitrary order.
func (s *RunBlockSink) ConsumeMetrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	bypass := &colmetricspb.ExportMetricsServiceRequest{}
	var pendings []pendingItem

	for _, rm := range req.ResourceMetrics {
		var resourceAttrs []*commonpb.KeyValue
		if rm.Resource != nil {
			resourceAttrs = rm.Resource.Attributes
		}
		resourceRunID := lookupAttr(resourceAttrs, "run.id")

		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if kept := s.bucketMetricDataPoints(m, resourceAttrs, resourceRunID, &pendings); kept != nil {
					ensureScopeMetric(bypass, rm, sm, kept)
				}
			}
		}
	}

	// Forward the bypass payload (if any) without holding the buffer lock.
	if hasResourceMetrics(bypass) {
		if err := s.inner.ConsumeMetrics(ctx, bypass); err != nil {
			return err
		}
	}

	if len(pendings) == 0 {
		return nil
	}

	s.mu.Lock()
	now := s.clock.Now()
	for _, p := range pendings {
		if s.truncated[p.runID] {
			s.mu.Unlock()
			// Truncated runs stream as line mode — do this without the lock.
			s.streamSingleMetric(ctx, p.item)
			s.mu.Lock()
			continue
		}
		rb := s.openRunLocked(p.runID, now)
		s.appendItemLocked(ctx, rb, p.item, now)
	}
	s.mu.Unlock()
	return nil
}

// ConsumeLogs mirrors ConsumeMetrics for log records. Log records carry no
// trace_id mapping logic — bucketing is by run.id only, checked on the
// record, scope, and resource attribute layers in that order.
func (s *RunBlockSink) ConsumeLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	bypass := &collogspb.ExportLogsServiceRequest{}
	var pendings []pendingItem

	for _, rl := range req.ResourceLogs {
		var resourceAttrs []*commonpb.KeyValue
		if rl.Resource != nil {
			resourceAttrs = rl.Resource.Attributes
		}
		resourceRunID := lookupAttr(resourceAttrs, "run.id")

		for _, sl := range rl.ScopeLogs {
			scopeRunID := ""
			if sl.Scope != nil {
				scopeRunID = lookupAttr(sl.Scope.Attributes, "run.id")
			}

			for _, lr := range sl.LogRecords {
				runID := lookupAttr(lr.Attributes, "run.id")
				if runID == "" {
					runID = scopeRunID
				}
				if runID == "" {
					runID = resourceRunID
				}
				if runID == "" {
					ensureScopeLog(bypass, rl, sl, lr)
					continue
				}
				pendings = append(pendings, pendingItem{
					runID: runID,
					item: format.RunBlockItem{
						Timestamp: lr.TimeUnixNano,
						Kind:      format.RunBlockItemLog,
						Log:       lr,
					},
				})
			}
		}
	}

	if hasResourceLogs(bypass) {
		if err := s.inner.ConsumeLogs(ctx, bypass); err != nil {
			return err
		}
	}

	if len(pendings) == 0 {
		return nil
	}

	s.mu.Lock()
	now := s.clock.Now()
	for _, p := range pendings {
		if s.truncated[p.runID] {
			s.mu.Unlock()
			s.streamSingleLog(ctx, p.item.Log)
			s.mu.Lock()
			continue
		}
		rb := s.openRunLocked(p.runID, now)
		s.appendItemLocked(ctx, rb, p.item, now)
	}
	s.mu.Unlock()
	return nil
}

// ConsumeTraces buckets spans by their own run.id attribute or, failing
// that, by the trace_id -> run.id map populated from prior spans. Spans
// with no run.id and no mapped trace bypass the buffer.
//
// Observing a span named "run" with no parent and a run.outcome attribute
// records the run as ended and triggers a flush of that run's buffer
// before this call returns, so any items in the same request that arrived
// after the root span (unlikely in practice but allowed) are flushed in
// the correct block.
func (s *RunBlockSink) ConsumeTraces(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	bypass := &coltracepb.ExportTraceServiceRequest{}
	type pendingSpan struct {
		runID string
		span  *tracepb.Span
		// rootEnd is true when this span itself is the run-end signal.
		rootEnd bool
	}
	var pendings []pendingSpan

	// First pass: extract run.id mappings so child spans without run.id but
	// matching trace_id can be bucketed in the same request.
	s.mu.Lock()
	for _, rs := range req.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				rid := lookupAttr(span.Attributes, "run.id")
				if rid != "" && len(span.TraceId) > 0 {
					s.traceToRun[hex.EncodeToString(span.TraceId)] = rid
				}
			}
		}
	}
	s.mu.Unlock()

	// Second pass: bucket each span.
	for _, rs := range req.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				runID := lookupAttr(span.Attributes, "run.id")
				if runID == "" && len(span.TraceId) > 0 {
					s.mu.Lock()
					runID = s.traceToRun[hex.EncodeToString(span.TraceId)]
					s.mu.Unlock()
				}
				if runID == "" {
					ensureScopeSpan(bypass, rs, ss, span)
					continue
				}
				pendings = append(pendings, pendingSpan{
					runID:   runID,
					span:    span,
					rootEnd: isRunRootEnd(span),
				})
			}
		}
	}

	if hasResourceSpans(bypass) {
		if err := s.inner.ConsumeTraces(ctx, bypass); err != nil {
			return err
		}
	}

	if len(pendings) == 0 {
		return nil
	}

	s.mu.Lock()
	now := s.clock.Now()
	for _, p := range pendings {
		if s.truncated[p.runID] {
			s.mu.Unlock()
			s.streamSingleSpan(ctx, p.span)
			s.mu.Lock()
			continue
		}
		rb := s.openRunLocked(p.runID, now)
		// If this span is the canonical run root (named "run", no parent),
		// fold its attributes into the header/footer state.
		if isRunRoot(p.span) {
			s.absorbRootLocked(rb, p.span)
		}
		item := format.RunBlockItem{
			Timestamp: p.span.StartTimeUnixNano,
			Kind:      format.RunBlockItemSpan,
			Span:      p.span,
		}
		s.appendItemLocked(ctx, rb, item, now)
		if p.rootEnd {
			rb.rootEnded = true
			s.flushLocked(ctx, rb, flushReasonRootEnded)
		}
	}
	s.mu.Unlock()
	return nil
}

// openRunLocked returns the existing buffer for runID or creates a fresh
// one. lastActivity is set to now in either case so a long-running run
// does not get swept while items are still arriving.
func (s *RunBlockSink) openRunLocked(runID string, now time.Time) *runBuffer {
	rb, ok := s.runs[runID]
	if !ok {
		rb = &runBuffer{runID: runID, lastActivity: now}
		s.runs[runID] = rb
	}
	rb.lastActivity = now
	return rb
}

// appendItemLocked adds item to rb and triggers an overflow flush when the
// configured cap is exceeded. The caller must hold s.mu.
func (s *RunBlockSink) appendItemLocked(ctx context.Context, rb *runBuffer, item format.RunBlockItem, now time.Time) {
	rb.items = append(rb.items, item)
	rb.lastActivity = now
	if s.cfg.MaxItems > 0 && len(rb.items) > s.cfg.MaxItems {
		s.flushLocked(ctx, rb, flushReasonOverflow)
	}
}

// flushLocked renders rb and writes it through the inner sink. The caller
// must hold s.mu; writeRunBlockLines acquires StdoutSink.mu internally, so
// the lock order while this runs is RunBlockSink.mu -> StdoutSink.mu and
// must not be inverted by any future caller. After flushing, the run's
// state (and any trace_id mapping pointing at it) is removed from the sink.
func (s *RunBlockSink) flushLocked(ctx context.Context, rb *runBuffer, reason flushReason) {
	defer func() {
		delete(s.runs, rb.runID)
		// Drop any trace_id -> run.id entries pointing at the flushed run.
		for tid, rid := range s.traceToRun {
			if rid == rb.runID {
				delete(s.traceToRun, tid)
			}
		}
	}()

	if reason == flushReasonOverflow {
		// On overflow we mark the run truncated so subsequent items go
		// straight to line-mode output; the warn line replaces the footer.
		s.truncated[rb.runID] = true
	}

	header := format.RunBlockHeader{
		RunID:     rb.runID,
		Mode:      rb.mode,
		Provider:  rb.provider,
		Model:     rb.model,
		StartedAt: rb.startedAt,
	}

	footer := format.RunBlockFooter{
		RunID:      rb.runID,
		FinishedAt: rb.endedAt,
	}
	switch reason {
	case flushReasonRootEnded:
		footer.Outcome = rb.outcome
		footer.Turns = rb.turns
	case flushReasonIdle, flushReasonShutdown:
		footer.Outcome = "<unknown>"
	case flushReasonOverflow:
		// Footer is replaced by the truncation warning below.
	}

	mode := format.RunBlockGrouped
	if s.cfg.Mode == RunBlockModeTree {
		mode = format.RunBlockTree
	}

	lines := format.RenderRunBlock(mode, header, rb.items, footer)
	if reason == flushReasonOverflow {
		// Replace the renderer-supplied footer with a single warn line. The
		// rendered slice is header + body + footer, so trim the footer.
		if len(lines) > 0 {
			lines = lines[:len(lines)-1]
		}
		lines = append(lines, format.RenderRunBlockTruncationWarning(rb.runID, len(rb.items)))
		s.logger.Warn("run buffer overflow",
			slog.String("run_id", rb.runID),
			slog.Int("items", len(rb.items)),
			slog.Int("max_items", s.cfg.MaxItems),
		)
	}

	// Forward the rendered lines through the writer the constructor
	// validated. StdoutSink's writeRunBlockLines acquires its own mutex
	// before writing, so the buffered flush still serialises against any
	// concurrent line-mode writes to the same writer.
	_ = s.writer.writeRunBlockLines(ctx, lines)
}

// streamSingleSpan, streamSingleLog, streamSingleMetric pass exactly one
// item through the inner sink in line-mode shape. Used when a run is in
// the truncated state. The lock must NOT be held when calling these.
func (s *RunBlockSink) streamSingleSpan(ctx context.Context, span *tracepb.Span) {
	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{span},
			}},
		}},
	}
	_ = s.inner.ConsumeTraces(ctx, req)
}

func (s *RunBlockSink) streamSingleLog(ctx context.Context, lr *logspb.LogRecord) {
	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{lr},
			}},
		}},
	}
	_ = s.inner.ConsumeLogs(ctx, req)
}

func (s *RunBlockSink) streamSingleMetric(ctx context.Context, item format.RunBlockItem) {
	if item.Metric == nil {
		return
	}
	metric := buildSingleMetricProto(item.Metric)
	if metric == nil {
		return
	}
	rm := &metricspb.ResourceMetrics{
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Metrics: []*metricspb.Metric{metric},
		}},
	}
	if len(item.Metric.ResourceAttrs) > 0 {
		rm.Resource = &resourcepb.Resource{Attributes: item.Metric.ResourceAttrs}
	}
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{rm},
	}
	_ = s.inner.ConsumeMetrics(ctx, req)
}

// buildSingleMetricProto rebuilds the OTLP wrapper around a single data
// point so it can be re-rendered through the inner sink. Mirrors the
// renderer-side helper but lives here because the truncated pass-through
// path needs the proto, not the rendered text.
func buildSingleMetricProto(m *format.RunBlockMetric) *metricspb.Metric {
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

// absorbRootLocked pulls header/footer-relevant fields from a "run" root
// span into rb. The root may carry only some of the fields; missing ones
// stay zero/empty so the renderer can elide them.
func (s *RunBlockSink) absorbRootLocked(rb *runBuffer, span *tracepb.Span) {
	rb.headerKnown = true
	if rb.startedAt == "" && span.StartTimeUnixNano > 0 {
		rb.startedAt = format.FormatTimestamp(span.StartTimeUnixNano)
	}
	if v := lookupAttr(span.Attributes, "run.mode"); v != "" {
		rb.mode = v
	}
	if v := lookupAttr(span.Attributes, "run.provider"); v != "" {
		rb.provider = v
	}
	if v := lookupAttr(span.Attributes, "run.model"); v != "" {
		rb.model = v
	}
	if v := lookupAttr(span.Attributes, "run.outcome"); v != "" {
		rb.outcome = v
	}
	if v := lookupAttr(span.Attributes, "run.turns"); v != "" {
		rb.turns = v
	}
	if span.EndTimeUnixNano > 0 {
		rb.endedAt = format.FormatTimestamp(span.EndTimeUnixNano)
	}
}

// bucketMetricDataPoints partitions a single Metric's data points into
// pendings (run-tagged) and a kept Metric (untagged). It returns nil when
// every data point in the metric was bucketed and the original metric
// would otherwise be empty in the bypass stream.
func (s *RunBlockSink) bucketMetricDataPoints(m *metricspb.Metric, resourceAttrs []*commonpb.KeyValue, resourceRunID string, pendings *[]pendingItem) *metricspb.Metric {
	if m == nil {
		return nil
	}
	// Each branch allocates a fresh aggregation wrapper rather than copying
	// by value, because OTLP proto types embed protoimpl.MessageState (a
	// sync.Mutex) and copying the value would trigger go vet's lock-copy
	// check.
	switch data := m.Data.(type) {
	case *metricspb.Metric_Sum:
		kept := splitNumberPoints(data.Sum.DataPoints, resourceAttrs, resourceRunID, m.Name, true, pendings)
		if len(kept) == 0 {
			return nil
		}
		return &metricspb.Metric{
			Name: m.Name, Description: m.Description, Unit: m.Unit,
			Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
				DataPoints:             kept,
				AggregationTemporality: data.Sum.AggregationTemporality,
				IsMonotonic:            data.Sum.IsMonotonic,
			}},
		}
	case *metricspb.Metric_Gauge:
		kept := splitNumberPoints(data.Gauge.DataPoints, resourceAttrs, resourceRunID, m.Name, false, pendings)
		if len(kept) == 0 {
			return nil
		}
		return &metricspb.Metric{
			Name: m.Name, Description: m.Description, Unit: m.Unit,
			Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: kept}},
		}
	case *metricspb.Metric_Histogram:
		kept := splitHistogramPoints(data.Histogram.DataPoints, resourceAttrs, resourceRunID, m.Name, pendings)
		if len(kept) == 0 {
			return nil
		}
		return &metricspb.Metric{
			Name: m.Name, Description: m.Description, Unit: m.Unit,
			Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
				DataPoints:             kept,
				AggregationTemporality: data.Histogram.AggregationTemporality,
			}},
		}
	case *metricspb.Metric_Summary:
		kept := splitSummaryPoints(data.Summary.DataPoints, resourceAttrs, resourceRunID, m.Name, pendings)
		if len(kept) == 0 {
			return nil
		}
		return &metricspb.Metric{
			Name: m.Name, Description: m.Description, Unit: m.Unit,
			Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{DataPoints: kept}},
		}
	case *metricspb.Metric_ExponentialHistogram:
		kept := splitExpHistPoints(data.ExponentialHistogram.DataPoints, resourceAttrs, resourceRunID, m.Name, pendings)
		if len(kept) == 0 {
			return nil
		}
		return &metricspb.Metric{
			Name: m.Name, Description: m.Description, Unit: m.Unit,
			Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
				DataPoints:             kept,
				AggregationTemporality: data.ExponentialHistogram.AggregationTemporality,
			}},
		}
	default:
		return m
	}
}

func splitNumberPoints(dps []*metricspb.NumberDataPoint, resourceAttrs []*commonpb.KeyValue, resourceRunID, metricName string, isSum bool, pendings *[]pendingItem) []*metricspb.NumberDataPoint {
	kept := make([]*metricspb.NumberDataPoint, 0, len(dps))
	for _, dp := range dps {
		runID := lookupAttr(dp.Attributes, "run.id")
		if runID == "" {
			runID = resourceRunID
		}
		if runID == "" {
			kept = append(kept, dp)
			continue
		}
		*pendings = append(*pendings, pendingItem{
			runID: runID,
			item: format.RunBlockItem{
				Timestamp: dp.TimeUnixNano,
				Kind:      format.RunBlockItemMetric,
				Metric: &format.RunBlockMetric{
					MetricName:    metricName,
					ResourceAttrs: resourceAttrs,
					DataPoint:     &format.RunBlockMetricDataPoint{Number: dp, NumberIsSum: isSum},
				},
			},
		})
	}
	return kept
}

func splitHistogramPoints(dps []*metricspb.HistogramDataPoint, resourceAttrs []*commonpb.KeyValue, resourceRunID, metricName string, pendings *[]pendingItem) []*metricspb.HistogramDataPoint {
	kept := make([]*metricspb.HistogramDataPoint, 0, len(dps))
	for _, dp := range dps {
		runID := lookupAttr(dp.Attributes, "run.id")
		if runID == "" {
			runID = resourceRunID
		}
		if runID == "" {
			kept = append(kept, dp)
			continue
		}
		*pendings = append(*pendings, pendingItem{
			runID: runID,
			item: format.RunBlockItem{
				Timestamp: dp.TimeUnixNano,
				Kind:      format.RunBlockItemMetric,
				Metric: &format.RunBlockMetric{
					MetricName:    metricName,
					ResourceAttrs: resourceAttrs,
					DataPoint:     &format.RunBlockMetricDataPoint{Histogram: dp},
				},
			},
		})
	}
	return kept
}

func splitSummaryPoints(dps []*metricspb.SummaryDataPoint, resourceAttrs []*commonpb.KeyValue, resourceRunID, metricName string, pendings *[]pendingItem) []*metricspb.SummaryDataPoint {
	kept := make([]*metricspb.SummaryDataPoint, 0, len(dps))
	for _, dp := range dps {
		runID := lookupAttr(dp.Attributes, "run.id")
		if runID == "" {
			runID = resourceRunID
		}
		if runID == "" {
			kept = append(kept, dp)
			continue
		}
		*pendings = append(*pendings, pendingItem{
			runID: runID,
			item: format.RunBlockItem{
				Timestamp: dp.TimeUnixNano,
				Kind:      format.RunBlockItemMetric,
				Metric: &format.RunBlockMetric{
					MetricName:    metricName,
					ResourceAttrs: resourceAttrs,
					DataPoint:     &format.RunBlockMetricDataPoint{Summary: dp},
				},
			},
		})
	}
	return kept
}

func splitExpHistPoints(dps []*metricspb.ExponentialHistogramDataPoint, resourceAttrs []*commonpb.KeyValue, resourceRunID, metricName string, pendings *[]pendingItem) []*metricspb.ExponentialHistogramDataPoint {
	kept := make([]*metricspb.ExponentialHistogramDataPoint, 0, len(dps))
	for _, dp := range dps {
		runID := lookupAttr(dp.Attributes, "run.id")
		if runID == "" {
			runID = resourceRunID
		}
		if runID == "" {
			kept = append(kept, dp)
			continue
		}
		*pendings = append(*pendings, pendingItem{
			runID: runID,
			item: format.RunBlockItem{
				Timestamp: dp.TimeUnixNano,
				Kind:      format.RunBlockItemMetric,
				Metric: &format.RunBlockMetric{
					MetricName:    metricName,
					ResourceAttrs: resourceAttrs,
					DataPoint:     &format.RunBlockMetricDataPoint{ExponentialHistogram: dp},
				},
			},
		})
	}
	return kept
}

// ensureScopeMetric appends m into bypass under the same Resource/Scope
// pair, creating new shells when the corresponding entry doesn't already
// exist. Resource and scope identity is decided by pointer equality, which
// matches how OTLP requests are constructed in practice.
func ensureScopeMetric(bypass *colmetricspb.ExportMetricsServiceRequest, rm *metricspb.ResourceMetrics, sm *metricspb.ScopeMetrics, m *metricspb.Metric) {
	var rmCopy *metricspb.ResourceMetrics
	for _, b := range bypass.ResourceMetrics {
		if b.Resource == rm.Resource && b.SchemaUrl == rm.SchemaUrl {
			rmCopy = b
			break
		}
	}
	if rmCopy == nil {
		rmCopy = &metricspb.ResourceMetrics{Resource: rm.Resource, SchemaUrl: rm.SchemaUrl}
		bypass.ResourceMetrics = append(bypass.ResourceMetrics, rmCopy)
	}

	var smCopy *metricspb.ScopeMetrics
	for _, b := range rmCopy.ScopeMetrics {
		if b.Scope == sm.Scope && b.SchemaUrl == sm.SchemaUrl {
			smCopy = b
			break
		}
	}
	if smCopy == nil {
		smCopy = &metricspb.ScopeMetrics{Scope: sm.Scope, SchemaUrl: sm.SchemaUrl}
		rmCopy.ScopeMetrics = append(rmCopy.ScopeMetrics, smCopy)
	}
	smCopy.Metrics = append(smCopy.Metrics, m)
}

func ensureScopeLog(bypass *collogspb.ExportLogsServiceRequest, rl *logspb.ResourceLogs, sl *logspb.ScopeLogs, lr *logspb.LogRecord) {
	var rlCopy *logspb.ResourceLogs
	for _, b := range bypass.ResourceLogs {
		if b.Resource == rl.Resource && b.SchemaUrl == rl.SchemaUrl {
			rlCopy = b
			break
		}
	}
	if rlCopy == nil {
		rlCopy = &logspb.ResourceLogs{Resource: rl.Resource, SchemaUrl: rl.SchemaUrl}
		bypass.ResourceLogs = append(bypass.ResourceLogs, rlCopy)
	}

	var slCopy *logspb.ScopeLogs
	for _, b := range rlCopy.ScopeLogs {
		if b.Scope == sl.Scope && b.SchemaUrl == sl.SchemaUrl {
			slCopy = b
			break
		}
	}
	if slCopy == nil {
		slCopy = &logspb.ScopeLogs{Scope: sl.Scope, SchemaUrl: sl.SchemaUrl}
		rlCopy.ScopeLogs = append(rlCopy.ScopeLogs, slCopy)
	}
	slCopy.LogRecords = append(slCopy.LogRecords, lr)
}

func ensureScopeSpan(bypass *coltracepb.ExportTraceServiceRequest, rs *tracepb.ResourceSpans, ss *tracepb.ScopeSpans, span *tracepb.Span) {
	var rsCopy *tracepb.ResourceSpans
	for _, b := range bypass.ResourceSpans {
		if b.Resource == rs.Resource && b.SchemaUrl == rs.SchemaUrl {
			rsCopy = b
			break
		}
	}
	if rsCopy == nil {
		rsCopy = &tracepb.ResourceSpans{Resource: rs.Resource, SchemaUrl: rs.SchemaUrl}
		bypass.ResourceSpans = append(bypass.ResourceSpans, rsCopy)
	}

	var ssCopy *tracepb.ScopeSpans
	for _, b := range rsCopy.ScopeSpans {
		if b.Scope == ss.Scope && b.SchemaUrl == ss.SchemaUrl {
			ssCopy = b
			break
		}
	}
	if ssCopy == nil {
		ssCopy = &tracepb.ScopeSpans{Scope: ss.Scope, SchemaUrl: ss.SchemaUrl}
		rsCopy.ScopeSpans = append(rsCopy.ScopeSpans, ssCopy)
	}
	ssCopy.Spans = append(ssCopy.Spans, span)
}

func hasResourceMetrics(req *colmetricspb.ExportMetricsServiceRequest) bool {
	for _, rm := range req.ResourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			if len(sm.Metrics) > 0 {
				return true
			}
		}
	}
	return false
}

func hasResourceLogs(req *collogspb.ExportLogsServiceRequest) bool {
	for _, rl := range req.ResourceLogs {
		for _, sl := range rl.ScopeLogs {
			if len(sl.LogRecords) > 0 {
				return true
			}
		}
	}
	return false
}

func hasResourceSpans(req *coltracepb.ExportTraceServiceRequest) bool {
	for _, rs := range req.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			if len(ss.Spans) > 0 {
				return true
			}
		}
	}
	return false
}

// lookupAttr returns the string-value of the first attribute matching key.
// Non-string values are stringified the same way the format package would
// render them. Empty for missing keys.
func lookupAttr(attrs []*commonpb.KeyValue, key string) string {
	for _, kv := range attrs {
		if kv.Key == key && kv.Value != nil {
			return format.FormatAnyValue(kv.Value)
		}
	}
	return ""
}

// isRunRoot reports whether a span looks like the canonical Stirrup run
// root: the name is "run" and there is no parent_span_id.
func isRunRoot(span *tracepb.Span) bool {
	return span != nil && span.Name == "run" && len(span.ParentSpanId) == 0
}

// isRunRootEnd is the flush trigger: the canonical run root with a
// run.outcome attribute attached. We treat the presence of run.outcome as
// the "this run is done" signal because Stirrup sets it on the root span
// at Finish() time.
func isRunRootEnd(span *tracepb.Span) bool {
	return isRunRoot(span) && lookupAttr(span.Attributes, "run.outcome") != ""
}

// Compile-time assertion that we satisfy the Sink interface.
var _ Sink = (*RunBlockSink)(nil)
