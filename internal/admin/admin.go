// Package admin exposes Steeplechase's operational HTTP surface: liveness
// and readiness probes plus a Prometheus /metrics endpoint. It runs on a
// dedicated listener so that ops traffic cannot interfere with the OTLP
// ingest ports.
package admin

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server runs the Steeplechase admin listener. Construct with NewServer, then
// call Start in a goroutine and Shutdown during process teardown.
type Server struct {
	srv   *http.Server
	ready atomic.Bool
}

// NewServer builds an admin server bound to addr. The Prometheus registry is
// exposed at GET /metrics and should be the same registry passed to
// metrics.NewRecorder so the same counters are reported to scrapers.
func NewServer(addr string, reg *prometheus.Registry) *Server {
	s := &Server{}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		// Leave EnableOpenMetrics off for broadest scraper compatibility.
		Registry: reg,
	}))

	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       5 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return s
}

// Start begins listening. Blocks until Shutdown is called; returns nil on
// clean shutdown or the error from ListenAndServe otherwise.
func (s *Server) Start() error {
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully shuts down the admin server. Safe to call after
// Start returned.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// MarkReady flips /readyz to 200. Call after all sinks and receivers have
// started successfully.
func (s *Server) MarkReady() { s.ready.Store(true) }

// MarkNotReady flips /readyz back to 503, e.g. during shutdown.
func (s *Server) MarkNotReady() { s.ready.Store(false) }

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if !s.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready\n"))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}
