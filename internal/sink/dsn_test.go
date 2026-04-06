package sink

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestParseDSN_Stdout(t *testing.T) {
	s, err := ParseDSN("stdout")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name() != "stdout" {
		t.Errorf("Name = %q, want stdout", s.Name())
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown returned %v", err)
	}
}

func TestParseDSN_Empty(t *testing.T) {
	if _, err := ParseDSN(""); err == nil {
		t.Error("expected error for empty DSN")
	}
}

func TestParseDSN_UnknownScheme(t *testing.T) {
	if _, err := ParseDSN("https://example.com"); err == nil {
		t.Error("expected error for non-otlp scheme")
	}
	if _, err := ParseDSN("otlp+udp://example.com:4317"); err == nil {
		t.Error("expected error for unsupported otlp variant")
	}
}

func TestParseDSN_UnknownQueryKey(t *testing.T) {
	_, err := ParseDSN("otlp+grpc://example.com:4317?frobnicate=yes")
	if err == nil {
		t.Fatal("expected error for unknown query key")
	}
	if !strings.Contains(err.Error(), "frobnicate") {
		t.Errorf("error should mention the offending key, got: %v", err)
	}
}

func TestParseDSN_GRPCDefaults(t *testing.T) {
	s, err := ParseDSN("otlp+grpc://127.0.0.1:4317")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name() != "127.0.0.1:4317" {
		t.Errorf("Name = %q, want host:port default", s.Name())
	}
	fs, ok := s.(*OTLPForwardSink)
	if !ok {
		t.Fatalf("expected *OTLPForwardSink, got %T", s)
	}
	if fs.retry.MaxElapsed != 30*time.Second {
		t.Errorf("retry max elapsed = %v, want 30s", fs.retry.MaxElapsed)
	}
	_ = s.Shutdown(context.Background())
}

func TestParseDSN_GRPCFullOptions(t *testing.T) {
	dsn := "otlp+grpc://example.com:4317" +
		"?name=collector" +
		"&tls=true" +
		"&header=x-api-key:abc" +
		"&header=x-team:observability" +
		"&timeout=2s" +
		"&compression=none" +
		"&retry_initial=100ms" +
		"&retry_max_interval=1s" +
		"&retry_max_elapsed=5s" +
		"&keepalive=30s"

	s, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fs, ok := s.(*OTLPForwardSink)
	if !ok {
		t.Fatalf("expected *OTLPForwardSink, got %T", s)
	}
	if fs.Name() != "collector" {
		t.Errorf("Name = %q, want collector", fs.Name())
	}
	if fs.retry.Initial != 100*time.Millisecond {
		t.Errorf("retry initial = %v, want 100ms", fs.retry.Initial)
	}
	if fs.retry.MaxElapsed != 5*time.Second {
		t.Errorf("retry max elapsed = %v, want 5s", fs.retry.MaxElapsed)
	}
	_ = s.Shutdown(context.Background())
}

func TestParseDSN_HTTPBaseURL(t *testing.T) {
	s, err := ParseDSN("otlp+http://otel.example.com:4318/ingest")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fs, ok := s.(*OTLPForwardSink)
	if !ok {
		t.Fatalf("expected *OTLPForwardSink, got %T", s)
	}
	ht, ok := fs.transport.(*httpTransport)
	if !ok {
		t.Fatalf("expected *httpTransport, got %T", fs.transport)
	}
	if ht.metricsURL != "http://otel.example.com:4318/ingest/v1/metrics" {
		t.Errorf("metrics URL = %q", ht.metricsURL)
	}
	if ht.logsURL != "http://otel.example.com:4318/ingest/v1/logs" {
		t.Errorf("logs URL = %q", ht.logsURL)
	}
	if ht.tracesURL != "http://otel.example.com:4318/ingest/v1/traces" {
		t.Errorf("traces URL = %q", ht.tracesURL)
	}
	_ = s.Shutdown(context.Background())
}

func TestParseDSN_HTTPSImpliesTLS(t *testing.T) {
	s, err := ParseDSN("otlp+https://otel.example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fs, ok := s.(*OTLPForwardSink)
	if !ok {
		t.Fatalf("expected *OTLPForwardSink, got %T", s)
	}
	// Just verify the URL prefix; the fact that httptest.NewTLSServer coverage
	// lives in otlp_forward_test.go keeps this file focused on parsing.
	ht := fs.transport.(*httpTransport)
	if !strings.HasPrefix(ht.metricsURL, "https://") {
		t.Errorf("metrics URL should be https, got %q", ht.metricsURL)
	}
	_ = s.Shutdown(context.Background())
}

func TestParseDSN_InvalidHeader(t *testing.T) {
	_, err := ParseDSN("otlp+grpc://example.com:4317?header=novalue")
	if err == nil {
		t.Error("expected error for header without colon")
	}
}

func TestParseDSN_InvalidTimeout(t *testing.T) {
	_, err := ParseDSN("otlp+grpc://example.com:4317?timeout=not-a-duration")
	if err == nil {
		t.Error("expected error for invalid timeout")
	}
}

func TestParseDSN_InvalidCompression(t *testing.T) {
	_, err := ParseDSN("otlp+grpc://example.com:4317?compression=snappy")
	if err == nil {
		t.Error("expected error for unsupported compression")
	}
}

func TestParseDSN_TLSVariants(t *testing.T) {
	cases := []struct {
		dsn  string
		want tlsMode
	}{
		{"otlp+grpc://h:4317?tls=true", tlsEnabled},
		{"otlp+grpc://h:4317?tls=1", tlsEnabled},
		{"otlp+grpc://h:4317?tls=false", tlsDisabled},
		{"otlp+grpc://h:4317?tls=insecure", tlsInsecureSkipVerify},
		{"otlp+grpc://h:4317?tls=skip-verify", tlsInsecureSkipVerify},
	}
	for _, tc := range cases {
		t.Run(tc.dsn, func(t *testing.T) {
			s, err := ParseDSN(tc.dsn)
			if err != nil {
				t.Fatalf("ParseDSN: %v", err)
			}
			defer s.Shutdown(context.Background())
			// We can't directly inspect the grpcTransport's tls config after
			// construction (it's owned by grpc.ClientConn), so this test just
			// verifies parsing succeeds for each variant.
		})
	}
}
