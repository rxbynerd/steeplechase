package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// httpTransportConfig describes the options needed to build an httpTransport.
type httpTransportConfig struct {
	BaseURL     string            // e.g. https://otel.example.com or http://localhost:4318
	TLS         tlsMode           // applicable when BaseURL is https://
	CABundle    string            // path to PEM file, optional
	Headers     map[string]string // merged into every request
	Timeout     time.Duration     // per-request timeout on the http.Client
	Compression string            // "gzip" or ""
}

// httpTransport forwards OTLP over HTTP/1.1 using the standard net/http
// client. A single client is reused across calls (safe for concurrent use).
type httpTransport struct {
	client      *http.Client
	metricsURL  string
	logsURL     string
	tracesURL   string
	headers     map[string]string
	compression string
}

// newHTTPTransport builds an httpTransport from the given config. BaseURL may
// be given with or without a trailing slash.
func newHTTPTransport(cfg httpTransportConfig) (*httpTransport, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("http transport: base URL is required")
	}
	base, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("http transport: parse base URL: %w", err)
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, fmt.Errorf("http transport: base URL must be http or https, got %q", base.Scheme)
	}
	base.Path = strings.TrimRight(base.Path, "/")

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if base.Scheme == "https" {
		tlsCfg, err := httpTLSConfig(cfg.TLS, cfg.CABundle)
		if err != nil {
			return nil, err
		}
		tr.TLSClientConfig = tlsCfg
	}

	client := &http.Client{
		Timeout:   timeout,
		Transport: tr,
	}

	return &httpTransport{
		client:      client,
		metricsURL:  base.String() + "/v1/metrics",
		logsURL:     base.String() + "/v1/logs",
		tracesURL:   base.String() + "/v1/traces",
		headers:     cfg.Headers,
		compression: cfg.Compression,
	}, nil
}

// httpTLSConfig builds a *tls.Config for the HTTP transport. Unlike gRPC we
// only land here when the caller explicitly requested https://.
func httpTLSConfig(mode tlsMode, caPath string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	switch mode {
	case tlsInsecureSkipVerify:
		cfg.InsecureSkipVerify = true
	case tlsDisabled:
		// https:// implies TLS even if the DSN didn't set tls=true.
	}
	if caPath == "" {
		return cfg, nil
	}
	pem, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read ca bundle %s: %w", caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("ca bundle %s: no certificates parsed", caPath)
	}
	cfg.RootCAs = pool
	return cfg, nil
}

func (t *httpTransport) SendMetrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) error {
	return t.post(ctx, t.metricsURL, req)
}

func (t *httpTransport) SendLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) error {
	return t.post(ctx, t.logsURL, req)
}

func (t *httpTransport) SendTraces(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) error {
	return t.post(ctx, t.tracesURL, req)
}

// Close releases idle HTTP connections held by the transport. Calling Close
// does not invalidate the transport — further calls would open fresh
// connections — but the sink discards it after Shutdown, so this is safe.
func (t *httpTransport) Close(_ context.Context) error {
	t.client.CloseIdleConnections()
	return nil
}

// post marshals the given proto message and POSTs it to url. Handles gzip
// compression, custom headers, and response classification.
func (t *httpTransport) post(ctx context.Context, url string, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return permanent(fmt.Errorf("marshal: %w", err))
	}

	var reader io.Reader = bytes.NewReader(body)
	if t.compression == "gzip" {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		if _, err := gw.Write(body); err != nil {
			return permanent(fmt.Errorf("gzip write: %w", err))
		}
		if err := gw.Close(); err != nil {
			return permanent(fmt.Errorf("gzip close: %w", err))
		}
		reader = &buf
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, reader)
	if err != nil {
		return permanent(fmt.Errorf("new request: %w", err))
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	if t.compression == "gzip" {
		req.Header.Set("Content-Encoding", "gzip")
	}
	for k, v := range t.headers {
		req.Header.Set(k, v)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		// Network, TLS, or ctx-related errors all land here. The retry layer
		// will treat these as retryable unless they are ctx cancel/deadline.
		return err
	}
	defer resp.Body.Close()

	// Drain the body (small) so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	httpErr := fmt.Errorf("http %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	// 4xx (except 408/429) are non-retryable. 5xx, 408, and 429 retry.
	if resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests {
		return httpErr
	}
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return permanent(httpErr)
	}
	return httpErr
}
