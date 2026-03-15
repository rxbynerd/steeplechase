package receiver

import (
	"context"
	"net"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type recordingSink struct {
	metricsCount int
	logsCount    int
	tracesCount  int
}

func (s *recordingSink) ConsumeMetrics(_ context.Context, req *colmetricspb.ExportMetricsServiceRequest) error {
	s.metricsCount++
	return nil
}

func (s *recordingSink) ConsumeLogs(_ context.Context, req *collogspb.ExportLogsServiceRequest) error {
	s.logsCount++
	return nil
}

func (s *recordingSink) ConsumeTraces(_ context.Context, req *coltracepb.ExportTraceServiceRequest) error {
	s.tracesCount++
	return nil
}

func startTestGRPCServer(t *testing.T, s *recordingSink) (string, func()) {
	t.Helper()
	// Use a random available port
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()

	srv := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(srv, &metricsServer{sink: s})
	collogspb.RegisterLogsServiceServer(srv, &logsServer{sink: s})
	coltracepb.RegisterTraceServiceServer(srv, &traceServer{sink: s})

	go srv.Serve(lis)
	return addr, srv.GracefulStop
}

func TestGRPC_ExportMetrics(t *testing.T) {
	s := &recordingSink{}
	addr, stop := startTestGRPCServer(t, s)
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := colmetricspb.NewMetricsServiceClient(conn)
	_, err = client.Export(context.Background(), &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "test.metric",
					Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						DataPoints: []*metricspb.NumberDataPoint{{
							Value: &metricspb.NumberDataPoint_AsInt{AsInt: 1},
						}},
					}},
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Export metrics: %v", err)
	}
	if s.metricsCount != 1 {
		t.Errorf("expected 1 metrics call, got %d", s.metricsCount)
	}
}

func TestGRPC_ExportLogs(t *testing.T) {
	s := &recordingSink{}
	addr, stop := startTestGRPCServer(t, s)
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := collogspb.NewLogsServiceClient(conn)
	_, err = client.Export(context.Background(), &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					TimeUnixNano: 1710504600123000000,
					Attributes: []*commonpb.KeyValue{
						{Key: "event.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "claude_code.api_request"}}},
					},
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Export logs: %v", err)
	}
	if s.logsCount != 1 {
		t.Errorf("expected 1 logs call, got %d", s.logsCount)
	}
}

func TestGRPC_ExportTraces(t *testing.T) {
	s := &recordingSink{}
	addr, stop := startTestGRPCServer(t, s)
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := coltracepb.NewTraceServiceClient(conn)
	_, err = client.Export(context.Background(), &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					Name:    "test-span",
					TraceId: make([]byte, 16),
					SpanId:  make([]byte, 8),
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Export traces: %v", err)
	}
	if s.tracesCount != 1 {
		t.Errorf("expected 1 traces call, got %d", s.tracesCount)
	}
}
