package sink

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"

	"github.com/rxbynerd/steeplechase/internal/metrics"
)

func TestMQTTSinkPublishesSignalTopicsAndProtobuf(t *testing.T) {
	ft := &fakeMQTTTransport{}
	s := NewMQTTSink("mqtt-test", "otel/harness", 2, true, ft, fastRetryConfig())

	metricsReq := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{Name: "stirrup.harness.tokens.input"}},
			}},
		}},
	}
	if err := s.ConsumeMetrics(context.Background(), metricsReq); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	if err := s.ConsumeLogs(context.Background(), &collogspb.ExportLogsServiceRequest{}); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}
	if err := s.ConsumeTraces(context.Background(), &coltracepb.ExportTraceServiceRequest{}); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}

	publishes := ft.Publishes()
	if len(publishes) != 3 {
		t.Fatalf("publishes = %d, want 3", len(publishes))
	}
	wantTopics := []string{"otel/harness/metrics", "otel/harness/logs", "otel/harness/traces"}
	for i, want := range wantTopics {
		if publishes[i].topic != want {
			t.Errorf("publish[%d] topic = %q, want %q", i, publishes[i].topic, want)
		}
		if publishes[i].qos != 2 {
			t.Errorf("publish[%d] qos = %d, want 2", i, publishes[i].qos)
		}
		if !publishes[i].retained {
			t.Errorf("publish[%d] retained = false, want true", i)
		}
	}

	var got colmetricspb.ExportMetricsServiceRequest
	if err := proto.Unmarshal(publishes[0].payload, &got); err != nil {
		t.Fatalf("unmarshal metrics payload: %v", err)
	}
	if !proto.Equal(&got, metricsReq) {
		t.Errorf("metrics payload differs from original request")
	}
}

func TestMQTTSinkRetriesPublishFailures(t *testing.T) {
	ft := &fakeMQTTTransport{
		errs: []error{errors.New("offline"), errors.New("still offline")},
	}
	s := NewMQTTSink("mqtt-test", "otel", 1, false, ft, fastRetryConfig())

	if err := s.ConsumeMetrics(context.Background(), &colmetricspb.ExportMetricsServiceRequest{}); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	if got := len(ft.Publishes()); got != 3 {
		t.Errorf("publish attempts = %d, want 3", got)
	}
	if got := s.lastRetryCount(metrics.SignalMetrics); got != 2 {
		t.Errorf("last retry count = %d, want 2", got)
	}
}

func fastRetryConfig() retryConfig {
	return retryConfig{
		Initial:    time.Millisecond,
		Max:        time.Millisecond,
		MaxElapsed: 100 * time.Millisecond,
		Multiplier: 1,
		Jitter:     0,
	}
}

type fakeMQTTTransport struct {
	mu        sync.Mutex
	publishes []mqttPublish
	errs      []error
	closed    bool
}

type mqttPublish struct {
	topic    string
	qos      byte
	retained bool
	payload  []byte
}

func (f *fakeMQTTTransport) Publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	cp := append([]byte(nil), payload...)
	f.publishes = append(f.publishes, mqttPublish{
		topic:    topic,
		qos:      qos,
		retained: retained,
		payload:  cp,
	})
	if len(f.errs) == 0 {
		return nil
	}
	err := f.errs[0]
	f.errs = f.errs[1:]
	return err
}

func (f *fakeMQTTTransport) Close(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeMQTTTransport) Publishes() []mqttPublish {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]mqttPublish, len(f.publishes))
	copy(out, f.publishes)
	for i := range out {
		out[i].payload = append([]byte(nil), out[i].payload...)
	}
	return out
}
