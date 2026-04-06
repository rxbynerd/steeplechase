package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/rxbynerd/steeplechase/internal/admin"
	"github.com/rxbynerd/steeplechase/internal/metrics"
	"github.com/rxbynerd/steeplechase/internal/receiver"
	"github.com/rxbynerd/steeplechase/internal/sink"
)

var version = "dev"

// sinkFlags implements flag.Value so that --sink can be supplied multiple
// times, once per configured destination.
type sinkFlags []string

func (s *sinkFlags) String() string     { return strings.Join(*s, ",") }
func (s *sinkFlags) Set(v string) error { *s = append(*s, v); return nil }

func main() {
	var sinks sinkFlags
	showVersion := flag.Bool("version", false, "Print version and exit")
	grpcAddr := flag.String("grpc-addr", ":4317", "gRPC listen address")
	httpAddr := flag.String("http-addr", ":4318", "HTTP listen address")
	adminAddr := flag.String("admin-addr", ":9090", "Admin HTTP listen address (/healthz, /readyz, /metrics)")
	flag.Var(&sinks, "sink", "Sink DSN (repeatable). Examples: stdout, otlp+grpc://host:4317, otlp+http://host:4318?header=x-api-key:... . If omitted, defaults to stdout.")
	flag.Parse()

	if *showVersion {
		fmt.Println("steeplechase", version)
		os.Exit(0)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	reg := prometheus.NewRegistry()
	rec := metrics.NewRecorder(reg)
	rec.SetBuildInfo(version)

	root, leafNames, err := buildPipeline(sinks, rec, logger)
	if err != nil {
		log.Fatalf("sink configuration error: %v", err)
	}
	defer func() {
		// Best-effort shutdown of the root sink during an early exit path.
		_ = root.Shutdown(context.Background())
	}()

	grpcRecv := receiver.NewGRPCReceiver(*grpcAddr, root, rec)
	httpRecv := receiver.NewHTTPReceiver(*httpAddr, root, rec)
	adminSrv := admin.NewServer(*adminAddr, reg)

	errCh := make(chan error, 3)

	go func() {
		logger.Info("grpc receiver listening", "addr", *grpcAddr, "sink", root.Name())
		errCh <- grpcRecv.Start()
	}()
	go func() {
		logger.Info("http receiver listening", "addr", *httpAddr, "sink", root.Name())
		errCh <- httpRecv.Start()
	}()
	go func() {
		logger.Info("admin listener starting", "addr", *adminAddr)
		errCh <- adminSrv.Start()
	}()

	// Give listeners a moment to bind before flipping readyz. We don't block
	// on it; the Start funcs return only on shutdown or fatal error.
	adminSrv.MarkReady()
	logger.Info("steeplechase ready",
		"version", version,
		"sinks", leafNames,
	)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("shutting down", "signal", sig.String())
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("receiver exited", "err", err)
		}
	}

	adminSrv.MarkNotReady()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stop receivers first so no new work arrives, then close the admin
	// listener, then release sink resources. Each step runs in parallel.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		grpcRecv.Stop()
	}()
	go func() {
		defer wg.Done()
		if err := httpRecv.Shutdown(ctx); err != nil {
			logger.Warn("http shutdown", "err", err)
		}
	}()
	wg.Wait()

	if err := adminSrv.Shutdown(ctx); err != nil {
		logger.Warn("admin shutdown", "err", err)
	}
	if err := root.Shutdown(ctx); err != nil {
		logger.Warn("sink shutdown", "err", err)
	}

	logger.Info("shutdown complete")
}

// buildPipeline parses the provided sink DSNs, wraps each leaf in a
// MeteredSink so metrics are observed per destination, and composes the
// result under a FanoutSink when more than one sink is configured. It
// returns the top-level sink plus the list of leaf names for logging.
//
// When dsns is empty, the function falls back to a single StdoutSink so that
// `steeplechase` with no flags continues to behave like today's build.
func buildPipeline(dsns []string, rec *metrics.Recorder, logger *slog.Logger) (sink.Sink, []string, error) {
	var leaves []sink.Sink
	var names []string

	if len(dsns) == 0 {
		stdout := sink.NewStdoutSink(os.Stdout)
		leaves = append(leaves, sink.NewMeteredSink(stdout, rec))
		names = append(names, stdout.Name())
	} else {
		for _, dsn := range dsns {
			s, err := sink.ParseDSN(dsn)
			if err != nil {
				// Tear down any sinks already constructed before returning.
				for _, l := range leaves {
					_ = l.Shutdown(context.Background())
				}
				return nil, nil, fmt.Errorf("parse sink %q: %w", dsn, err)
			}
			leaves = append(leaves, sink.NewMeteredSink(s, rec))
			names = append(names, s.Name())
		}
	}

	if len(leaves) == 1 {
		return leaves[0], names, nil
	}
	return sink.NewFanoutSink(leaves, logger), names, nil
}
