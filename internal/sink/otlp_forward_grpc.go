package sink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip" // register gzip compressor
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// grpcTransportConfig describes the options needed to build a grpcTransport.
// Values are populated by ParseDSN.
type grpcTransportConfig struct {
	Endpoint    string            // host:port
	TLS         tlsMode           // disabled, enabled, insecureSkipVerify
	CABundle    string            // path to PEM file, optional
	Headers     map[string]string // per-call metadata
	Timeout     time.Duration     // per-call deadline
	Compression string            // "gzip" or ""
	Keepalive   time.Duration     // ping interval, 0 disables
}

type tlsMode int

const (
	tlsDisabled tlsMode = iota
	tlsEnabled
	tlsInsecureSkipVerify
)

// grpcTransport forwards OTLP over gRPC. A single grpc.ClientConn is reused
// for all signals and all parallel calls (ClientConn is safe for concurrent
// use).
type grpcTransport struct {
	conn    *grpc.ClientConn
	metrics colmetricspb.MetricsServiceClient
	logs    collogspb.LogsServiceClient
	traces  coltracepb.TraceServiceClient
	headers map[string]string
	timeout time.Duration
}

// newGRPCTransport builds a grpcTransport from the given config.
func newGRPCTransport(cfg grpcTransportConfig) (*grpcTransport, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("grpc transport: endpoint is required")
	}

	opts := []grpc.DialOption{}

	switch cfg.TLS {
	case tlsDisabled:
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	case tlsEnabled:
		tlsCfg, err := buildTLSConfig(cfg.CABundle, false)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	case tlsInsecureSkipVerify:
		tlsCfg, err := buildTLSConfig(cfg.CABundle, true)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	}

	if cfg.Compression == "gzip" {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")))
	}

	if cfg.Keepalive > 0 {
		opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                cfg.Keepalive,
			Timeout:             20 * time.Second,
			PermitWithoutStream: true,
		}))
	}

	conn, err := grpc.NewClient(cfg.Endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", cfg.Endpoint, err)
	}

	return &grpcTransport{
		conn:    conn,
		metrics: colmetricspb.NewMetricsServiceClient(conn),
		logs:    collogspb.NewLogsServiceClient(conn),
		traces:  coltracepb.NewTraceServiceClient(conn),
		headers: cfg.Headers,
		timeout: cfg.Timeout,
	}, nil
}

// buildTLSConfig assembles a tls.Config for the gRPC client, optionally
// loading a custom CA bundle from disk.
func buildTLSConfig(caPath string, insecureSkipVerify bool) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: insecureSkipVerify,
	}
	if caPath == "" {
		return tlsCfg, nil
	}
	pem, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read ca bundle %s: %w", caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("ca bundle %s: no certificates parsed", caPath)
	}
	tlsCfg.RootCAs = pool
	return tlsCfg, nil
}

// withCallCtx applies per-call metadata and deadline to ctx.
func (t *grpcTransport) withCallCtx(parent context.Context) (context.Context, context.CancelFunc) {
	ctx := parent
	if len(t.headers) > 0 {
		kv := make([]string, 0, len(t.headers)*2)
		for k, v := range t.headers {
			kv = append(kv, k, v)
		}
		ctx = metadata.AppendToOutgoingContext(ctx, kv...)
	}
	if t.timeout > 0 {
		return context.WithTimeout(ctx, t.timeout)
	}
	return ctx, func() {}
}

func (t *grpcTransport) SendMetrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) error {
	ctx, cancel := t.withCallCtx(ctx)
	defer cancel()
	_, err := t.metrics.Export(ctx, req)
	return classifyGRPCError(err)
}

func (t *grpcTransport) SendLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) error {
	ctx, cancel := t.withCallCtx(ctx)
	defer cancel()
	_, err := t.logs.Export(ctx, req)
	return classifyGRPCError(err)
}

func (t *grpcTransport) SendTraces(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) error {
	ctx, cancel := t.withCallCtx(ctx)
	defer cancel()
	_, err := t.traces.Export(ctx, req)
	return classifyGRPCError(err)
}

// Close tears down the gRPC connection. Safe to call once.
func (t *grpcTransport) Close(_ context.Context) error {
	return t.conn.Close()
}

// classifyGRPCError maps a gRPC error into either a permanent (non-retryable)
// error or a plain retryable error, using the status code where available.
// Unknown errors are treated as retryable by default, which errs on the side
// of delivery at the risk of wasted work.
func classifyGRPCError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		// Non-grpc error (e.g. ctx cancellation handled at a higher layer).
		return err
	}
	switch st.Code() {
	case codes.OK:
		return nil
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists,
		codes.PermissionDenied, codes.FailedPrecondition, codes.OutOfRange,
		codes.Unimplemented, codes.Unauthenticated:
		return permanent(err)
	case codes.Canceled:
		return context.Canceled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	default:
		// Unavailable, ResourceExhausted, Internal, etc. -> retry.
		return err
	}
}
