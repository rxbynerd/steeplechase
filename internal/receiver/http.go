package receiver

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/rxbynerd/steeplechase/internal/sink"
)

const maxBodySize = 4 * 1024 * 1024 // 4 MB

// HTTPReceiver serves OTLP over HTTP.
type HTTPReceiver struct {
	server *http.Server
	sink   sink.Sink
}

// NewHTTPReceiver creates an HTTP receiver on the given address.
func NewHTTPReceiver(addr string, s sink.Sink) *HTTPReceiver {
	mux := http.NewServeMux()
	r := &HTTPReceiver{
		server: &http.Server{
			Addr:         addr,
			Handler:      mux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
		sink:   s,
	}
	mux.HandleFunc("/v1/metrics", r.handleMetrics)
	mux.HandleFunc("/v1/logs", r.handleLogs)
	mux.HandleFunc("/v1/traces", r.handleTraces)
	return r
}

// Start begins listening. Blocks until the server stops.
func (r *HTTPReceiver) Start() error {
	err := r.server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// Shutdown gracefully shuts down the HTTP server.
func (r *HTTPReceiver) Shutdown(ctx context.Context) error {
	return r.server.Shutdown(ctx)
}

func (r *HTTPReceiver) handleMetrics(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, ct, err := readBody(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var msg colmetricspb.ExportMetricsServiceRequest
	if err := unmarshal(body, ct, &msg); err != nil {
		http.Error(w, "decode error: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := r.sink.ConsumeMetrics(req.Context(), &msg); err != nil {
		log.Printf("sink error consuming metrics: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	resp := &colmetricspb.ExportMetricsServiceResponse{}
	writeResponse(w, ct, resp)
}

func (r *HTTPReceiver) handleLogs(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, ct, err := readBody(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var msg collogspb.ExportLogsServiceRequest
	if err := unmarshal(body, ct, &msg); err != nil {
		http.Error(w, "decode error: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := r.sink.ConsumeLogs(req.Context(), &msg); err != nil {
		log.Printf("sink error consuming logs: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	resp := &collogspb.ExportLogsServiceResponse{}
	writeResponse(w, ct, resp)
}

func (r *HTTPReceiver) handleTraces(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, ct, err := readBody(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var msg coltracepb.ExportTraceServiceRequest
	if err := unmarshal(body, ct, &msg); err != nil {
		http.Error(w, "decode error: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := r.sink.ConsumeTraces(req.Context(), &msg); err != nil {
		log.Printf("sink error consuming traces: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	resp := &coltracepb.ExportTraceServiceResponse{}
	writeResponse(w, ct, resp)
}

// readBody reads and optionally decompresses the request body, returning bytes and content type.
func readBody(req *http.Request) ([]byte, string, error) {
	reader, err := decompressBody(req)
	if err != nil {
		return nil, "", fmt.Errorf("decompress: %w", err)
	}
	defer reader.Close()

	limited := io.LimitReader(reader, maxBodySize+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, "", fmt.Errorf("read body: %w", err)
	}
	if len(data) > maxBodySize {
		return nil, "", fmt.Errorf("body exceeds %d bytes", maxBodySize)
	}

	ct := req.Header.Get("Content-Type")
	return data, ct, nil
}

func unmarshal(data []byte, contentType string, msg proto.Message) error {
	if isJSON(contentType) {
		return protojson.Unmarshal(data, msg)
	}
	// Default to protobuf
	return proto.Unmarshal(data, msg)
}

func writeResponse(w http.ResponseWriter, contentType string, msg proto.Message) {
	if isJSON(contentType) {
		w.Header().Set("Content-Type", "application/json")
		data, err := protojson.Marshal(msg)
		if err != nil {
			http.Error(w, "encode error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(data); err != nil {
			log.Printf("failed to write HTTP response: %v", err)
		}
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	data, err := proto.Marshal(msg)
	if err != nil {
		http.Error(w, "encode error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(data); err != nil {
		log.Printf("failed to write HTTP response: %v", err)
	}
}

func isJSON(contentType string) bool {
	ct := strings.TrimSpace(strings.SplitN(contentType, ";", 2)[0])
	return ct == "application/json"
}
