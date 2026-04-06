package receiver

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/rxbynerd/steeplechase/internal/sinktest"
)

func newTestHTTPReceiver(s *sinktest.RecordingSink) *HTTPReceiver {
	return NewHTTPReceiver(":0", s, nil)
}

func TestHTTP_MetricsProtobuf(t *testing.T) {
	s := sinktest.NewRecordingSink("test")
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
	if s.MetricsCount() != 1 {
		t.Errorf("expected 1 metrics call, got %d", s.MetricsCount())
	}
}

func TestHTTP_MetricsJSON(t *testing.T) {
	s := sinktest.NewRecordingSink("test")
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
	s := sinktest.NewRecordingSink("test")
	r := newTestHTTPReceiver(s)

	req := &collogspb.ExportLogsServiceRequest{}
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	r.server.Handler.ServeHTTP(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if s.LogsCount() != 1 {
		t.Errorf("expected 1 logs call, got %d", s.LogsCount())
	}
}

func TestHTTP_TracesProtobuf(t *testing.T) {
	s := sinktest.NewRecordingSink("test")
	r := newTestHTTPReceiver(s)

	req := &coltracepb.ExportTraceServiceRequest{}
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	r.server.Handler.ServeHTTP(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if s.TracesCount() != 1 {
		t.Errorf("expected 1 traces call, got %d", s.TracesCount())
	}
}

func TestHTTP_MethodNotAllowed(t *testing.T) {
	s := sinktest.NewRecordingSink("test")
	r := newTestHTTPReceiver(s)

	w := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodGet, "/v1/metrics", nil)
	r.server.Handler.ServeHTTP(w, httpReq)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHTTP_GzipDecompression(t *testing.T) {
	s := sinktest.NewRecordingSink("test")
	r := newTestHTTPReceiver(s)

	req := &colmetricspb.ExportMetricsServiceRequest{}
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

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
	s := sinktest.NewRecordingSink("test")
	r := NewHTTPReceiver("127.0.0.1:0", s, nil)

	// Start in background
	go r.Start()

	// Shutdown should succeed
	err := r.Shutdown(context.Background())
	if err != nil {
		t.Errorf("shutdown error: %v", err)
	}
}

func TestHTTP_SinkConsumeError(t *testing.T) {
	es := sinktest.NewErrorSink("err", errors.New("sink failed"))
	r := NewHTTPReceiver(":0", es, nil)

	req := &colmetricspb.ExportMetricsServiceRequest{}
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	r.server.Handler.ServeHTTP(w, httpReq)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestHTTP_UnmarshalError(t *testing.T) {
	s := sinktest.NewRecordingSink("test")
	r := newTestHTTPReceiver(s)

	w := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader([]byte("not valid protobuf")))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	r.server.Handler.ServeHTTP(w, httpReq)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "decode error") {
		t.Errorf("expected decode error message, got %q", w.Body.String())
	}
}

func TestHTTP_OversizedBody(t *testing.T) {
	s := sinktest.NewRecordingSink("test")
	r := newTestHTTPReceiver(s)

	// Create body slightly over 4MB
	bigBody := make([]byte, maxBodySize+1)

	w := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(bigBody))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	r.server.Handler.ServeHTTP(w, httpReq)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "body exceeds") {
		t.Errorf("expected body exceeds message, got %q", w.Body.String())
	}
}

func TestHTTP_InvalidGzip(t *testing.T) {
	s := sinktest.NewRecordingSink("test")
	r := newTestHTTPReceiver(s)

	w := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader([]byte("not gzip data")))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Content-Encoding", "gzip")
	r.server.Handler.ServeHTTP(w, httpReq)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}
