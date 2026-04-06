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

	"github.com/rxbynerd/steeplechase/internal/metrics"
	"github.com/rxbynerd/steeplechase/internal/sink"
)

// GRPCReceiver serves OTLP over gRPC.
type GRPCReceiver struct {
	server *grpc.Server
	sink   sink.Sink
	addr   string
}

// NewGRPCReceiver creates a gRPC receiver on the given address. If rec is
// non-nil, each accepted request is counted via ObserveReceive.
func NewGRPCReceiver(addr string, s sink.Sink, rec *metrics.Recorder) *GRPCReceiver {
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(4 * 1024 * 1024), // 4 MB, matching HTTP limit
	)
	r := &GRPCReceiver{
		server: srv,
		sink:   s,
		addr:   addr,
	}
	colmetricspb.RegisterMetricsServiceServer(srv, &metricsServer{sink: s, rec: rec})
	collogspb.RegisterLogsServiceServer(srv, &logsServer{sink: s, rec: rec})
	coltracepb.RegisterTraceServiceServer(srv, &traceServer{sink: s, rec: rec})
	return r
}

// SinkName returns the name of the configured sink, useful for startup logs.
func (r *GRPCReceiver) SinkName() string { return r.sink.Name() }

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
	rec  *metrics.Recorder
}

func (s *metricsServer) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	if s.rec != nil {
		s.rec.ObserveReceive("grpc", metrics.SignalMetrics)
	}
	if err := s.sink.ConsumeMetrics(ctx, req); err != nil {
		return nil, err
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

// logsServer implements the OTLP LogsService.
type logsServer struct {
	collogspb.UnimplementedLogsServiceServer
	sink sink.Sink
	rec  *metrics.Recorder
}

func (s *logsServer) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	if s.rec != nil {
		s.rec.ObserveReceive("grpc", metrics.SignalLogs)
	}
	if err := s.sink.ConsumeLogs(ctx, req); err != nil {
		return nil, err
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

// traceServer implements the OTLP TraceService.
type traceServer struct {
	coltracepb.UnimplementedTraceServiceServer
	sink sink.Sink
	rec  *metrics.Recorder
}

func (s *traceServer) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	if s.rec != nil {
		s.rec.ObserveReceive("grpc", metrics.SignalTraces)
	}
	if err := s.sink.ConsumeTraces(ctx, req); err != nil {
		return nil, err
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}
