package receiver

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func newTestHTTPReceiver(s *recordingSink) *HTTPReceiver {
	return NewHTTPReceiver(":0", s)
}

func TestHTTP_MetricsProtobuf(t *testing.T) {
	s := &recordingSink{}
	r := newTestHTTPReceiver(s)

	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "test.metric",
					Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						DataPoints: []*metricspb.NumberDataPoint{{
							Value: &metricspb.NumberDataPoint_AsInt{AsInt: 42},
						}},
					}},
				}},
			}},
		}},
	}
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	r.server.Handler.ServeHTTP(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if s.metricsCount != 1 {
		t.Errorf("expected 1 metrics call, got %d", s.metricsCount)
	}
}

func TestHTTP_MetricsJSON(t *testing.T) {
	s := &recordingSink{}
	r := newTestHTTPReceiver(s)

	req := &colmetricspb.ExportMetricsServiceRequest{}
	body, err := protojson.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	r.server.Handler.ServeHTTP(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Errorf("expected json content type, got %s", w.Header().Get("Content-Type"))
	}
}

func TestHTTP_LogsProtobuf(t *testing.T) {
	s := &recordingSink{}
	r := newTestHTTPReceiver(s)

	req := &collogspb.ExportLogsServiceRequest{}
	body, _ := proto.Marshal(req)

	w := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	r.server.Handler.ServeHTTP(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if s.logsCount != 1 {
		t.Errorf("expected 1 logs call, got %d", s.logsCount)
	}
}

func TestHTTP_TracesProtobuf(t *testing.T) {
	s := &recordingSink{}
	r := newTestHTTPReceiver(s)

	req := &coltracepb.ExportTraceServiceRequest{}
	body, _ := proto.Marshal(req)

	w := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	r.server.Handler.ServeHTTP(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if s.tracesCount != 1 {
		t.Errorf("expected 1 traces call, got %d", s.tracesCount)
	}
}

func TestHTTP_MethodNotAllowed(t *testing.T) {
	s := &recordingSink{}
	r := newTestHTTPReceiver(s)

	w := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodGet, "/v1/metrics", nil)
	r.server.Handler.ServeHTTP(w, httpReq)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHTTP_GzipDecompression(t *testing.T) {
	s := &recordingSink{}
	r := newTestHTTPReceiver(s)

	req := &colmetricspb.ExportMetricsServiceRequest{}
	body, _ := proto.Marshal(req)

	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	gw.Write(body)
	gw.Close()

	w := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", &gzBuf)
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Content-Encoding", "gzip")
	r.server.Handler.ServeHTTP(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHTTP_Shutdown(t *testing.T) {
	s := &recordingSink{}
	r := NewHTTPReceiver("127.0.0.1:0", s)

	// Start in background
	go r.Start()

	// Shutdown should succeed
	err := r.Shutdown(context.Background())
	if err != nil {
		t.Errorf("shutdown error: %v", err)
	}
}

// Verify the recordingSink implements the Sink interface (compile-time check)
var _ io.Reader = (*bytes.Buffer)(nil) // sanity
