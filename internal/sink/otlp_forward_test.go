package sink_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/rxbynerd/steeplechase/internal/sink"
)

// ---------------------------------------------------------------------------
// gRPC round-trip
// ---------------------------------------------------------------------------

func TestOTLPForward_GRPCRoundTrip(t *testing.T) {
	srv, addr, stop := startFakeGRPCServer(t)
	defer stop()

	s, err := sink.ParseDSN("otlp+grpc://" + addr + "?name=fake&retry_max_elapsed=100ms")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	defer s.Shutdown(context.Background())

	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "forwarded.metric",
				}},
			}},
		}},
	}
	if err := s.ConsumeMetrics(context.Background(), req); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	if got := srv.metricsCount(); got != 1 {
		t.Errorf("fake server metrics count = %d, want 1", got)
	}
	got := srv.lastMetrics()
	if got == nil {
		t.Fatal("server did not receive a metrics payload")
	}
	if !proto.Equal(got, req) {
		t.Errorf("forwarded payload differs from original")
	}
}

func TestOTLPForward_GRPCRetryableError(t *testing.T) {
	srv, addr, stop := startFakeGRPCServer(t)
	defer stop()

	// First two calls fail with Unavailable, third succeeds.
	srv.setMetricsResponse(func(attempt int) error {
		if attempt < 3 {
			return status.Error(codes.Unavailable, "try again")
		}
		return nil
	})

	s, err := sink.ParseDSN("otlp+grpc://" + addr + "?retry_initial=1ms&retry_max_interval=5ms&retry_max_elapsed=500ms")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	defer s.Shutdown(context.Background())

	if err := s.ConsumeMetrics(context.Background(), &colmetricspb.ExportMetricsServiceRequest{}); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	if srv.metricsCount() != 3 {
		t.Errorf("server got %d metrics calls, want 3", srv.metricsCount())
	}
}

func TestOTLPForward_GRPCPermanentError(t *testing.T) {
	srv, addr, stop := startFakeGRPCServer(t)
	defer stop()

	srv.setMetricsResponse(func(int) error { return status.Error(codes.InvalidArgument, "bad request") })

	s, err := sink.ParseDSN("otlp+grpc://" + addr + "?retry_initial=1ms&retry_max_interval=5ms&retry_max_elapsed=500ms")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	defer s.Shutdown(context.Background())

	err = s.ConsumeMetrics(context.Background(), &colmetricspb.ExportMetricsServiceRequest{})
	if err == nil {
		t.Fatal("expected error from permanent failure")
	}
	if srv.metricsCount() != 1 {
		t.Errorf("server got %d metrics calls, want 1 (no retry)", srv.metricsCount())
	}
}

// ---------------------------------------------------------------------------
// HTTP round-trip
// ---------------------------------------------------------------------------

func TestOTLPForward_HTTPRoundTrip(t *testing.T) {
	var receivedCount atomic.Int32
	var receivedBody atomic.Value

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		receivedCount.Add(1)
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		receivedBody.Store(body)
		w.WriteHeader(http.StatusOK)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	s, err := sink.ParseDSN("otlp+" + server.URL + "?name=http-fake&retry_max_elapsed=100ms&header=x-api-key:secret")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	defer s.Shutdown(context.Background())

	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{}},
	}
	if err := s.ConsumeMetrics(context.Background(), req); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	if receivedCount.Load() != 1 {
		t.Errorf("server got %d requests, want 1", receivedCount.Load())
	}
}

func TestOTLPForward_HTTPHeadersForwarded(t *testing.T) {
	headersReceived := make(chan http.Header, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		headersReceived <- r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	s, err := sink.ParseDSN("otlp+" + server.URL + "?header=x-api-key:secret&header=x-dataset:prod")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	defer s.Shutdown(context.Background())

	if err := s.ConsumeMetrics(context.Background(), &colmetricspb.ExportMetricsServiceRequest{}); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	select {
	case h := <-headersReceived:
		if h.Get("X-Api-Key") != "secret" {
			t.Errorf("x-api-key = %q, want secret", h.Get("X-Api-Key"))
		}
		if h.Get("X-Dataset") != "prod" {
			t.Errorf("x-dataset = %q, want prod", h.Get("X-Dataset"))
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive request in time")
	}
}

func TestOTLPForward_HTTPRetries5xx(t *testing.T) {
	var attempts atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			http.Error(w, "kaboom", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	s, err := sink.ParseDSN("otlp+" + server.URL + "?retry_initial=1ms&retry_max_interval=5ms&retry_max_elapsed=500ms")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	defer s.Shutdown(context.Background())

	if err := s.ConsumeMetrics(context.Background(), &colmetricspb.ExportMetricsServiceRequest{}); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	if attempts.Load() != 3 {
		t.Errorf("attempts = %d, want 3", attempts.Load())
	}
}

func TestOTLPForward_HTTPPermanent4xx(t *testing.T) {
	var attempts atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		http.Error(w, "bad auth", http.StatusForbidden)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	s, err := sink.ParseDSN("otlp+" + server.URL + "?retry_initial=1ms&retry_max_interval=5ms&retry_max_elapsed=500ms")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	defer s.Shutdown(context.Background())

	err = s.ConsumeMetrics(context.Background(), &colmetricspb.ExportMetricsServiceRequest{})
	if err == nil {
		t.Fatal("expected error from 403 response")
	}
	if attempts.Load() != 1 {
		t.Errorf("attempts = %d, want 1 (4xx is permanent)", attempts.Load())
	}
}

func TestOTLPForward_HTTPContextCancel(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		// Hold the request open; the client should disconnect on ctx cancel.
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	s, err := sink.ParseDSN("otlp+" + server.URL + "?retry_initial=1ms&retry_max_interval=5ms&retry_max_elapsed=2s&timeout=5s")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	defer s.Shutdown(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = s.ConsumeMetrics(ctx, &colmetricspb.ExportMetricsServiceRequest{})
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

// ---------------------------------------------------------------------------
// Fake gRPC server used by the gRPC tests above
// ---------------------------------------------------------------------------

type fakeGRPCServer struct {
	mu             sync.Mutex
	metrics        int
	lastMetricsReq *colmetricspb.ExportMetricsServiceRequest
	metricsRespFn  func(attempt int) error
}

type fakeMetricsServer struct {
	colmetricspb.UnimplementedMetricsServiceServer
	parent *fakeGRPCServer
}

func (s *fakeMetricsServer) Export(_ context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	p := s.parent
	p.mu.Lock()
	p.metrics++
	p.lastMetricsReq = req
	fn := p.metricsRespFn
	attempt := p.metrics
	p.mu.Unlock()
	if fn != nil {
		if err := fn(attempt); err != nil {
			return nil, err
		}
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

func startFakeGRPCServer(t *testing.T) (*fakeGRPCServer, string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &fakeGRPCServer{}
	g := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(g, &fakeMetricsServer{parent: srv})
	go g.Serve(lis)
	return srv, lis.Addr().String(), g.GracefulStop
}

func (s *fakeGRPCServer) setMetricsResponse(fn func(attempt int) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metricsRespFn = fn
}

func (s *fakeGRPCServer) metricsCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metrics
}

func (s *fakeGRPCServer) lastMetrics() *colmetricspb.ExportMetricsServiceRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastMetricsReq
}

