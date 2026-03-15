package receiver

import (
	"context"
	"fmt"
	"net"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // register gzip compressor

	"github.com/rxbynerd/steeplechase/internal/sink"
)

// GRPCReceiver serves OTLP over gRPC.
type GRPCReceiver struct {
	server *grpc.Server
	sink   sink.Sink
	addr   string
}

// NewGRPCReceiver creates a gRPC receiver on the given address.
func NewGRPCReceiver(addr string, s sink.Sink) *GRPCReceiver {
	srv := grpc.NewServer()
	r := &GRPCReceiver{
		server: srv,
		sink:   s,
		addr:   addr,
	}
	colmetricspb.RegisterMetricsServiceServer(srv, &metricsServer{sink: s})
	collogspb.RegisterLogsServiceServer(srv, &logsServer{sink: s})
	coltracepb.RegisterTraceServiceServer(srv, &traceServer{sink: s})
	return r
}

// Start begins listening. Blocks until the server stops.
func (r *GRPCReceiver) Start() error {
	lis, err := net.Listen("tcp", r.addr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	return r.server.Serve(lis)
}

// Stop gracefully stops the gRPC server.
func (r *GRPCReceiver) Stop() {
	r.server.GracefulStop()
}

// metricsServer implements the OTLP MetricsService.
type metricsServer struct {
	colmetricspb.UnimplementedMetricsServiceServer
	sink sink.Sink
}

func (s *metricsServer) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	if err := s.sink.ConsumeMetrics(ctx, req); err != nil {
		return nil, err
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

// logsServer implements the OTLP LogsService.
type logsServer struct {
	collogspb.UnimplementedLogsServiceServer
	sink sink.Sink
}

func (s *logsServer) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	if err := s.sink.ConsumeLogs(ctx, req); err != nil {
		return nil, err
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

// traceServer implements the OTLP TraceService.
type traceServer struct {
	coltracepb.UnimplementedTraceServiceServer
	sink sink.Sink
}

func (s *traceServer) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	if err := s.sink.ConsumeTraces(ctx, req); err != nil {
		return nil, err
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}
