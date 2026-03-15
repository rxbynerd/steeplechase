package format

import (
	"fmt"
	"strings"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// FormatMetrics formats an OTLP ExportMetricsServiceRequest into human-readable lines.
func FormatMetrics(resourceMetrics []*metricspb.ResourceMetrics) []string {
	var lines []string
	for _, rm := range resourceMetrics {
		resourceAttrs := resourceAttributes(rm)
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				lines = append(lines, formatMetric(m, resourceAttrs)...)
			}
		}
	}
	return lines
}

func formatMetric(m *metricspb.Metric, resourceAttrs []*commonpb.KeyValue) []string {
	name := m.Name
	switch data := m.Data.(type) {
	case *metricspb.Metric_Gauge:
		return formatNumberDataPoints(name, data.Gauge.DataPoints, resourceAttrs)
	case *metricspb.Metric_Sum:
		return formatNumberDataPoints(name, data.Sum.DataPoints, resourceAttrs)
	case *metricspb.Metric_Histogram:
		return formatHistogramDataPoints(name, data.Histogram.DataPoints, resourceAttrs)
	case *metricspb.Metric_Summary:
		return formatSummaryDataPoints(name, data.Summary.DataPoints, resourceAttrs)
	case *metricspb.Metric_ExponentialHistogram:
		return formatExponentialHistogramDataPoints(name, data.ExponentialHistogram.DataPoints, resourceAttrs)
	default:
		return []string{fmt.Sprintf("[METRIC] %s (unknown data type)", name)}
	}
}

func formatNumberDataPoints(name string, dps []*metricspb.NumberDataPoint, resourceAttrs []*commonpb.KeyValue) []string {
	lines := make([]string, 0, len(dps))
	for _, dp := range dps {
		ts := FormatTimestamp(dp.TimeUnixNano)
		attrs := mergeAttributes(resourceAttrs, dp.Attributes)
		var value string
		switch v := dp.Value.(type) {
		case *metricspb.NumberDataPoint_AsInt:
			value = fmt.Sprintf("%d", v.AsInt)
		case *metricspb.NumberDataPoint_AsDouble:
			value = fmt.Sprintf("%g", v.AsDouble)
		default:
			value = "0"
		}
		line := fmt.Sprintf("[METRIC] %s %s = %s", ts, name, value)
		if attrStr := FormatAttributes(attrs); attrStr != "" {
			line += " " + attrStr
		}
		lines = append(lines, line)
	}
	return lines
}

func formatHistogramDataPoints(name string, dps []*metricspb.HistogramDataPoint, resourceAttrs []*commonpb.KeyValue) []string {
	lines := make([]string, 0, len(dps))
	for _, dp := range dps {
		ts := FormatTimestamp(dp.TimeUnixNano)
		attrs := mergeAttributes(resourceAttrs, dp.Attributes)
		line := fmt.Sprintf("[METRIC] %s %s count=%d sum=%g", ts, name, dp.Count, dp.GetSum())
		if attrStr := FormatAttributes(attrs); attrStr != "" {
			line += " " + attrStr
		}
		lines = append(lines, line)
	}
	return lines
}

func formatSummaryDataPoints(name string, dps []*metricspb.SummaryDataPoint, resourceAttrs []*commonpb.KeyValue) []string {
	lines := make([]string, 0, len(dps))
	for _, dp := range dps {
		ts := FormatTimestamp(dp.TimeUnixNano)
		attrs := mergeAttributes(resourceAttrs, dp.Attributes)
		line := fmt.Sprintf("[METRIC] %s %s count=%d sum=%g", ts, name, dp.Count, dp.Sum)
		if attrStr := FormatAttributes(attrs); attrStr != "" {
			line += " " + attrStr
		}
		lines = append(lines, line)
	}
	return lines
}

func formatExponentialHistogramDataPoints(name string, dps []*metricspb.ExponentialHistogramDataPoint, resourceAttrs []*commonpb.KeyValue) []string {
	lines := make([]string, 0, len(dps))
	for _, dp := range dps {
		ts := FormatTimestamp(dp.TimeUnixNano)
		attrs := mergeAttributes(resourceAttrs, dp.Attributes)
		line := fmt.Sprintf("[METRIC] %s %s count=%d sum=%g", ts, name, dp.Count, dp.GetSum())
		if attrStr := FormatAttributes(attrs); attrStr != "" {
			line += " " + attrStr
		}
		lines = append(lines, line)
	}
	return lines
}

func resourceAttributes(rm *metricspb.ResourceMetrics) []*commonpb.KeyValue {
	if rm.Resource != nil {
		return rm.Resource.Attributes
	}
	return nil
}

func mergeAttributes(resourceAttrs, dataPointAttrs []*commonpb.KeyValue) []*commonpb.KeyValue {
	if len(resourceAttrs) == 0 {
		return dataPointAttrs
	}
	if len(dataPointAttrs) == 0 {
		return resourceAttrs
	}
	// Data point attributes take precedence; build a set of keys to skip from resource
	dpKeys := make(map[string]struct{}, len(dataPointAttrs))
	for _, kv := range dataPointAttrs {
		dpKeys[kv.Key] = struct{}{}
	}
	merged := make([]*commonpb.KeyValue, 0, len(resourceAttrs)+len(dataPointAttrs))
	for _, kv := range resourceAttrs {
		if _, exists := dpKeys[kv.Key]; !exists {
			merged = append(merged, kv)
		}
	}
	merged = append(merged, dataPointAttrs...)
	return merged
}

// FormatMetricsSummary returns a one-line summary of metrics received.
func FormatMetricsSummary(resourceMetrics []*metricspb.ResourceMetrics) string {
	var metricNames []string
	for _, rm := range resourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				metricNames = append(metricNames, m.Name)
			}
		}
	}
	return fmt.Sprintf("Received %d metric(s): %s", len(metricNames), strings.Join(metricNames, ", "))
}
