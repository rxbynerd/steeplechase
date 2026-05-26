package sink

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/rxbynerd/steeplechase/internal/metrics"
)

type mqttTransport interface {
	Publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte) error
	Close(ctx context.Context) error
}

type mqttTransportConfig struct {
	BrokerURL string
	Username  string
	Password  string
	ClientID  string
	Timeout   time.Duration
}

type pahoMQTTTransport struct {
	client  mqtt.Client
	timeout time.Duration
	mu      sync.Mutex
}

func newMQTTTransport(cfg mqttTransportConfig) (*pahoMQTTTransport, error) {
	if cfg.BrokerURL == "" {
		return nil, fmt.Errorf("mqtt transport: broker URL is required")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(cfg.BrokerURL)
	opts.SetUsername(cfg.Username)
	opts.SetPassword(cfg.Password)
	opts.SetClientID(cfg.ClientID)
	opts.SetConnectTimeout(timeout)
	opts.SetAutoReconnect(true)
	opts.SetOrderMatters(false)

	return &pahoMQTTTransport{
		client:  mqtt.NewClient(opts),
		timeout: timeout,
	}, nil
}

func (t *pahoMQTTTransport) Publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte) error {
	if err := t.ensureConnected(ctx); err != nil {
		return err
	}
	token := t.client.Publish(topic, qos, retained, payload)
	return waitMQTTToken(ctx, token, t.timeout)
}

func (t *pahoMQTTTransport) Close(_ context.Context) error {
	t.client.Disconnect(250)
	return nil
}

func (t *pahoMQTTTransport) ensureConnected(ctx context.Context) error {
	if t.client.IsConnectionOpen() {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.client.IsConnectionOpen() {
		return nil
	}
	token := t.client.Connect()
	return waitMQTTToken(ctx, token, t.timeout)
}

func waitMQTTToken(ctx context.Context, token mqtt.Token, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	deadline := time.Now().Add(timeout)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		wait := 50 * time.Millisecond
		if remaining := time.Until(deadline); remaining <= 0 {
			return context.DeadlineExceeded
		} else if remaining < wait {
			wait = remaining
		}
		if ctxDeadline, ok := ctx.Deadline(); ok {
			if remaining := time.Until(ctxDeadline); remaining <= 0 {
				return ctx.Err()
			} else if remaining < wait {
				wait = remaining
			}
		}

		if token.WaitTimeout(wait) {
			if err := token.Error(); err != nil {
				return err
			}
			return nil
		}
	}
}

// MQTTSink publishes each OTLP export request as a protobuf payload to an
// MQTT topic under the configured base topic.
type MQTTSink struct {
	name      string
	baseTopic string
	qos       byte
	retained  bool
	transport mqttTransport
	retry     retryConfig

	lastMetricsRetries atomic.Int32
	lastLogsRetries    atomic.Int32
	lastTracesRetries  atomic.Int32
}

func NewMQTTSink(name string, baseTopic string, qos byte, retained bool, t mqttTransport, r retryConfig) *MQTTSink {
	return &MQTTSink{
		name:      name,
		baseTopic: baseTopic,
		qos:       qos,
		retained:  retained,
		transport: t,
		retry:     r,
	}
}

func (s *MQTTSink) Name() string { return s.name }

func (s *MQTTSink) Shutdown(ctx context.Context) error {
	return s.transport.Close(ctx)
}

func (s *MQTTSink) ConsumeMetrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) error {
	payload, err := proto.Marshal(req)
	if err != nil {
		return permanent(fmt.Errorf("marshal metrics: %w", err))
	}
	attempts, err := s.retry.Do(ctx, func(ctx context.Context) error {
		return s.transport.Publish(ctx, s.signalTopic("metrics"), s.qos, s.retained, payload)
	})
	s.lastMetricsRetries.Store(int32(attempts))
	return err
}

func (s *MQTTSink) ConsumeLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) error {
	payload, err := proto.Marshal(req)
	if err != nil {
		return permanent(fmt.Errorf("marshal logs: %w", err))
	}
	attempts, err := s.retry.Do(ctx, func(ctx context.Context) error {
		return s.transport.Publish(ctx, s.signalTopic("logs"), s.qos, s.retained, payload)
	})
	s.lastLogsRetries.Store(int32(attempts))
	return err
}

func (s *MQTTSink) ConsumeTraces(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) error {
	payload, err := proto.Marshal(req)
	if err != nil {
		return permanent(fmt.Errorf("marshal traces: %w", err))
	}
	attempts, err := s.retry.Do(ctx, func(ctx context.Context) error {
		return s.transport.Publish(ctx, s.signalTopic("traces"), s.qos, s.retained, payload)
	})
	s.lastTracesRetries.Store(int32(attempts))
	return err
}

func (s *MQTTSink) signalTopic(signal string) string {
	return s.baseTopic + "/" + signal
}

func (s *MQTTSink) lastRetryCount(signal metrics.Signal) int {
	switch signal {
	case metrics.SignalMetrics:
		return int(s.lastMetricsRetries.Load())
	case metrics.SignalLogs:
		return int(s.lastLogsRetries.Load())
	case metrics.SignalTraces:
		return int(s.lastTracesRetries.Load())
	}
	return 0
}

func defaultMQTTClientID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("steeplechase-%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
}
