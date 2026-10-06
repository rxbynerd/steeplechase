package sink

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestHTTPEncoding(t *testing.T) {
	cases := []struct {
		signal string
		msg    proto.Message
		input  string
	}{
		{"logs", &collogspb.ExportLogsServiceRequest{}, `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":"18446744073709551615","severityNumber":9,"body":{"stringValue":"hello"},"traceId":"AAECAwQFBgcICQoLDA0ODw==","spanId":"AAECAwQFBgc=","attributes":[{"key":"traceId","value":{"bytesValue":"AAECAwQFBgcICQoLDA0ODw=="}}]}]}]}]}`},
		{"metrics", &colmetricspb.ExportMetricsServiceRequest{}, `{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"requests","sum":{"aggregationTemporality":2,"dataPoints":[{"asInt":"9223372036854775807","exemplars":[{"traceId":"AAECAwQFBgcICQoLDA0ODw==","spanId":"AAECAwQFBgc=","asInt":"9223372036854775807"}]}]}}]}]}]}`},
		{"traces", &coltracepb.ExportTraceServiceRequest{}, `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"AAECAwQFBgcICQoLDA0ODw==","spanId":"AAECAwQFBgc=","parentSpanId":"AAECAwQFBgc=","kind":2,"status":{"code":1},"links":[{"traceId":"AAECAwQFBgcICQoLDA0ODw==","spanId":"AAECAwQFBgc="}]}]}]}]}`},
	}
	for _, tc := range cases {
		for _, encoding := range []string{"", "proto", "json"} {
			for _, compression := range []string{"none", "gzip"} {
				t.Run(tc.signal+"/"+encoding+"/"+compression, func(t *testing.T) {
					msg := proto.Clone(tc.msg)
					if err := protojson.Unmarshal([]byte(tc.input), msg); err != nil {
						t.Fatal(err)
					}
					server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Path != "/base/v1/"+tc.signal {
							t.Errorf("path = %s", r.URL.Path)
						}
						if r.Header.Get("Authorization") != "Bearer test-token" || r.Header.Get("User-Agent") != "steeplechase-test" {
							t.Errorf("headers = %v", r.Header)
						}
						var reader io.Reader = r.Body
						if compression == "gzip" {
							if r.Header.Get("Content-Encoding") != "gzip" {
								t.Error("missing gzip header")
							}
							gz, err := gzip.NewReader(r.Body)
							if err != nil {
								t.Error(err)
								return
							}
							defer gz.Close()
							reader = gz
						} else if r.Header.Get("Content-Encoding") != "" {
							t.Error("unexpected compression")
						}
						body, err := io.ReadAll(reader)
						if err != nil {
							t.Error(err)
							return
						}
						if encoding == "json" {
							if r.Header.Get("Content-Type") != "application/json" {
								t.Error("wrong JSON content type")
							}
							expected := strings.NewReplacer(`"traceId":"AAECAwQFBgcICQoLDA0ODw=="`, `"traceId":"000102030405060708090a0b0c0d0e0f"`, `"spanId":"AAECAwQFBgc="`, `"spanId":"0001020304050607"`, `"parentSpanId":"AAECAwQFBgc="`, `"parentSpanId":"0001020304050607"`).Replace(tc.input)
							var got, want any
							if err := json.Unmarshal(body, &got); err != nil {
								t.Error(err)
							}
							if err := json.Unmarshal([]byte(expected), &want); err != nil {
								t.Error(err)
							}
							if !reflect.DeepEqual(got, want) {
								t.Errorf("JSON = %s; want %s", body, expected)
							}
						} else {
							if r.Header.Get("Content-Type") != "application/x-protobuf" {
								t.Error("wrong protobuf content type")
							}
							got := proto.Clone(tc.msg)
							if err := proto.Unmarshal(body, got); err != nil {
								t.Error(err)
							}
							if !proto.Equal(msg, got) {
								t.Error("protobuf payload changed")
							}
						}
						w.WriteHeader(http.StatusOK)
					}))
					defer server.Close()
					dsn := "otlp+" + server.URL + "/base?tls=insecure&compression=" + compression + "&header=Authorization:Bearer%20test-token&header=User-Agent:steeplechase-test"
					if encoding != "" {
						dsn += "&encoding=" + encoding
					}
					s, err := ParseDSN(dsn)
					if err != nil {
						t.Fatal(err)
					}
					defer s.Shutdown(context.Background())
					switch req := msg.(type) {
					case *collogspb.ExportLogsServiceRequest:
						err = s.ConsumeLogs(context.Background(), req)
					case *colmetricspb.ExportMetricsServiceRequest:
						err = s.ConsumeMetrics(context.Background(), req)
					case *coltracepb.ExportTraceServiceRequest:
						err = s.ConsumeTraces(context.Background(), req)
					}
					if err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestParseDSNInvalidEncoding(t *testing.T) {
	for _, dsn := range []string{
		"otlp+http://localhost?encoding=xml",
		"otlp+https://localhost?encoding=",
		"otlp+http://localhost?encoding=json&encoding=proto",
		"otlp+grpc://localhost:4317?encoding=json",
		"mqtt://localhost:1883/logs?encoding=json",
	} {
		t.Run(dsn, func(t *testing.T) {
			if _, err := ParseDSN(dsn); err == nil {
				t.Fatal("expected encoding error")
			}
		})
	}
}
