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
	// stdout-format/run-timeout/run-max-items belong together: they
	// configure how a run.id-aware stdout sink groups telemetry into
	// per-run blocks. Defaults preserve today's line-by-line behaviour.
	stdoutFormat := flag.String("stdout-format", "line", "Stdout layout: line (default, today's behaviour), grouped (per-run.id buffer flushed on root-end), or tree (grouped, with spans rendered as a parent->child tree).")
	stdoutRunTimeout := flag.Duration("stdout-run-timeout", 5*time.Minute, "Per-run idle timeout for grouped/tree modes; ignored in line mode. A run with no items received for this long is force-flushed with outcome=<unknown>.")
	stdoutRunMaxItems := flag.Int("stdout-run-max-items", 10000, "Per-run buffered-item cap for grouped/tree modes; ignored in line mode. On overflow the run is force-flushed with a truncation warning and any subsequent items for that run.id stream as line mode would.")
	flag.Parse()

	if *showVersion {
		fmt.Println("steeplechase", version)
		os.Exit(0)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	stdoutCfg, err := parseStdoutFormat(*stdoutFormat, *stdoutRunTimeout, *stdoutRunMaxItems)
	if err != nil {
		log.Fatalf("stdout-format: %v", err)
	}

	reg := prometheus.NewRegistry()
	rec := metrics.NewRecorder(reg)
	rec.SetBuildInfo(version)

	root, leafNames, err := buildPipeline(sinks, rec, logger, stdoutCfg)
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

// stdoutOptions captures the parsed --stdout-format / --stdout-run-timeout /
// --stdout-run-max-items group. Wrapping is applied only when a stdout
// sink is in the pipeline and Mode != line; otherwise stdout is wired
// straight through, identical to today's build.
type stdoutOptions struct {
	Mode        string // "line", "grouped", or "tree"
	IdleTimeout time.Duration
	MaxItems    int
}

// parseStdoutFormat validates the flag triple. An invalid mode is a hard
// startup failure rather than silent fallback so typos don't yield a
// surprising layout in production.
func parseStdoutFormat(mode string, timeout time.Duration, maxItems int) (stdoutOptions, error) {
	switch mode {
	case "line", "grouped", "tree":
	default:
		return stdoutOptions{}, fmt.Errorf("invalid mode %q (want line, grouped, or tree)", mode)
	}
	if mode != "line" {
		if timeout <= 0 {
			return stdoutOptions{}, fmt.Errorf("--stdout-run-timeout must be > 0 when --stdout-format=%s", mode)
		}
		if maxItems <= 0 {
			return stdoutOptions{}, fmt.Errorf("--stdout-run-max-items must be > 0 when --stdout-format=%s", mode)
		}
	}
	return stdoutOptions{Mode: mode, IdleTimeout: timeout, MaxItems: maxItems}, nil
}

// buildPipeline parses the provided sink DSNs, wraps each leaf in a
// MeteredSink so metrics are observed per destination, and composes the
// result under a FanoutSink when more than one sink is configured. It
// returns the top-level sink plus the list of leaf names for logging.
//
// When dsns is empty, the function falls back to a single StdoutSink so that
// `steeplechase` with no flags continues to behave like today's build.
//
// stdoutCfg controls whether stdout sinks (whether the implicit default or
// explicit --sink stdout) are wrapped with run.id-aware buffering. The
// wrapper sits inside the MeteredSink so that buffered flushes still
// observe the existing per-sink Prometheus counters.
func buildPipeline(dsns []string, rec *metrics.Recorder, logger *slog.Logger, stdoutCfg stdoutOptions) (sink.Sink, []string, error) {
	var leaves []sink.Sink
	var names []string

	// teardown releases any sinks already constructed if a later one fails
	// to construct, so we don't leak goroutines (e.g. RunBlockSink's idle
	// sweeper) on the error path.
	teardown := func() {
		for _, l := range leaves {
			_ = l.Shutdown(context.Background())
		}
	}

	maybeWrap := func(s sink.Sink) (sink.Sink, error) {
		stdoutS, ok := s.(*sink.StdoutSink)
		if !ok || stdoutCfg.Mode == "line" {
			return s, nil
		}
		mode := sink.RunBlockModeGrouped
		if stdoutCfg.Mode == "tree" {
			mode = sink.RunBlockModeTree
		}
		return sink.NewRunBlockSink(stdoutS, sink.RunBlockConfig{
			Mode:        mode,
			IdleTimeout: stdoutCfg.IdleTimeout,
			MaxItems:    stdoutCfg.MaxItems,
			Logger:      logger,
		})
	}

	if len(dsns) == 0 {
		stdout := sink.NewStdoutSink(os.Stdout)
		wrapped, err := maybeWrap(stdout)
		if err != nil {
			return nil, nil, fmt.Errorf("wrap stdout sink: %w", err)
		}
		leaves = append(leaves, sink.NewMeteredSink(wrapped, rec))
		names = append(names, stdout.Name())
	} else {
		for _, dsn := range dsns {
			s, err := sink.ParseDSN(dsn)
			if err != nil {
				teardown()
				return nil, nil, fmt.Errorf("parse sink %q: %w", dsn, err)
			}
			wrapped, err := maybeWrap(s)
			if err != nil {
				teardown()
				return nil, nil, fmt.Errorf("wrap sink %q: %w", dsn, err)
			}
			leaves = append(leaves, sink.NewMeteredSink(wrapped, rec))
			names = append(names, s.Name())
		}
	}

	if len(leaves) == 1 {
		return leaves[0], names, nil
	}
	return sink.NewFanoutSink(leaves, logger), names, nil
}
