package sink

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// ParseDSN constructs a Sink from a compact DSN string. Supported forms:
//
//	stdout                                    -> StdoutSink writing to os.Stdout
//	otlp+grpc://HOST:PORT[?k=v&...]           -> gRPC OTLPForwardSink
//	otlp+http://HOST:PORT[/BASE][?k=v&...]    -> HTTP OTLPForwardSink (plaintext)
//	otlp+https://HOST:PORT[/BASE][?k=v&...]   -> HTTP OTLPForwardSink over TLS
//	mqtt://[USER:PASS@]HOST:PORT/TOPIC[?k=v&...] -> MQTT MQTTSink
//
// Common query parameters (all optional):
//
//	name=<string>             label for metrics (default: host:port)
//	tls=true|false|insecure   TLS mode; grpc default false; https:// implies true
//	ca=<path>                 custom CA bundle (PEM)
//	header=<k>:<v>            outbound header, repeatable
//	timeout=<duration>        per-call deadline (default 10s)
//	compression=gzip|none     grpc default gzip, http default none
//	retry_initial=<duration>  first backoff (default 500ms)
//	retry_max_interval=<dur>  cap on any single backoff (default 10s)
//	retry_max_elapsed=<dur>   total retry budget (default 30s)
//	keepalive=<duration>      grpc only, 0 disables
//	qos=0|1|2                 mqtt only, default 1
//	retained=true|false       mqtt only, default false
//	client_id=<string>        mqtt only, default generated
//
// Unknown schemes and unknown query parameters cause ParseDSN to return an
// error rather than silently ignoring them — failing loud on DSN typos is
// important for a deployable configuration surface.
func ParseDSN(dsn string) (Sink, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, fmt.Errorf("sink dsn: empty")
	}
	if dsn == "stdout" {
		return NewStdoutSink(os.Stdout), nil
	}

	// Parse as URL. OTLP forwarding schemes use compound scheme names such as
	// otlp+grpc; net/url preserves those unchanged for the switch below.
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("sink dsn %q: %w", RedactDSN(dsn), err)
	}

	switch u.Scheme {
	case "otlp+grpc":
		return parseGRPCDSN(u)
	case "otlp+http", "otlp+https":
		return parseHTTPDSN(u)
	case "mqtt":
		return parseMQTTDSN(u)
	default:
		return nil, fmt.Errorf("sink dsn %q: unknown scheme %q (want stdout, otlp+grpc, otlp+http, otlp+https, mqtt)", RedactDSN(dsn), u.Scheme)
	}
}

// RedactDSN returns dsn with any password component hidden for logs and errors.
func RedactDSN(dsn string) string {
	u, err := url.Parse(strings.TrimSpace(dsn))
	if err != nil || u.User == nil {
		return dsn
	}
	return u.Redacted()
}

// knownGRPCKeys is the closed set of DSN query parameters recognised by the
// grpc sink; anything else returns an error.
var knownGRPCKeys = map[string]struct{}{
	"name":               {},
	"tls":                {},
	"ca":                 {},
	"header":             {},
	"timeout":            {},
	"compression":        {},
	"retry_initial":      {},
	"retry_max_interval": {},
	"retry_max_elapsed":  {},
	"keepalive":          {},
}

// knownHTTPKeys is the closed set for the http sink.
var knownHTTPKeys = map[string]struct{}{
	"name":               {},
	"tls":                {},
	"ca":                 {},
	"header":             {},
	"timeout":            {},
	"compression":        {},
	"retry_initial":      {},
	"retry_max_interval": {},
	"retry_max_elapsed":  {},
}

// knownMQTTKeys is the closed set for the mqtt sink.
var knownMQTTKeys = map[string]struct{}{
	"name":               {},
	"timeout":            {},
	"retry_initial":      {},
	"retry_max_interval": {},
	"retry_max_elapsed":  {},
	"qos":                {},
	"retained":           {},
	"client_id":          {},
}

func parseGRPCDSN(u *url.URL) (Sink, error) {
	if u.Host == "" {
		return nil, fmt.Errorf("sink dsn %q: missing host:port", redactedURL(u))
	}
	q, err := parseQuery(u, knownGRPCKeys)
	if err != nil {
		return nil, err
	}

	cfg := grpcTransportConfig{
		Endpoint:    u.Host,
		Headers:     q.Headers,
		Timeout:     q.Timeout,
		Compression: q.CompressionOr("gzip"),
		Keepalive:   q.Keepalive,
	}
	cfg.TLS, cfg.CABundle = q.TLSMode, q.CABundle
	// grpc plaintext by default — https:// doesn't apply here because grpc uses
	// its own scheme marker.

	t, err := newGRPCTransport(cfg)
	if err != nil {
		return nil, err
	}
	name := q.Name
	if name == "" {
		name = u.Host
	}
	return NewOTLPForwardSink(name, t, buildRetryConfig(q)), nil
}

func parseHTTPDSN(u *url.URL) (Sink, error) {
	if u.Host == "" {
		return nil, fmt.Errorf("sink dsn %q: missing host:port", redactedURL(u))
	}
	q, err := parseQuery(u, knownHTTPKeys)
	if err != nil {
		return nil, err
	}

	scheme := "http"
	if u.Scheme == "otlp+https" {
		scheme = "https"
	}
	base := &url.URL{
		Scheme: scheme,
		Host:   u.Host,
		Path:   u.Path,
	}

	cfg := httpTransportConfig{
		BaseURL:     base.String(),
		TLS:         q.TLSMode,
		CABundle:    q.CABundle,
		Headers:     q.Headers,
		Timeout:     q.Timeout,
		Compression: q.CompressionOr(""),
	}
	// https:// with no explicit tls= parameter implies real TLS verification.
	if scheme == "https" && cfg.TLS == tlsDisabled {
		cfg.TLS = tlsEnabled
	}

	t, err := newHTTPTransport(cfg)
	if err != nil {
		return nil, err
	}
	name := q.Name
	if name == "" {
		name = u.Host
	}
	return NewOTLPForwardSink(name, t, buildRetryConfig(q)), nil
}

func parseMQTTDSN(u *url.URL) (Sink, error) {
	if u.Host == "" || u.Hostname() == "" || u.Port() == "" {
		return nil, fmt.Errorf("sink dsn %q: missing host:port", redactedURL(u))
	}
	if u.Fragment != "" {
		return nil, fmt.Errorf("sink dsn %q: mqtt topic must not use URL fragments", redactedURL(u))
	}

	baseTopic := strings.Trim(u.Path, "/")
	if baseTopic == "" {
		return nil, fmt.Errorf("sink dsn %q: missing mqtt topic", redactedURL(u))
	}
	if strings.ContainsAny(baseTopic, "+#") {
		return nil, fmt.Errorf("sink dsn %q: mqtt topic must not contain publish wildcards", redactedURL(u))
	}

	q, err := parseQuery(u, knownMQTTKeys)
	if err != nil {
		return nil, err
	}
	values := u.Query()

	qos := byte(1)
	if v := values.Get("qos"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 2 {
			return nil, fmt.Errorf("sink dsn %q: invalid qos=%q (want 0, 1, or 2)", redactedURL(u), v)
		}
		qos = byte(n)
	}

	retained := false
	if v := values.Get("retained"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("sink dsn %q: invalid retained=%q (want true or false)", redactedURL(u), v)
		}
		retained = b
	}

	clientID := values.Get("client_id")
	if clientID == "" {
		clientID = defaultMQTTClientID()
	}

	username := u.User.Username()
	password, _ := u.User.Password()
	t, err := newMQTTTransport(mqttTransportConfig{
		BrokerURL: "tcp://" + u.Host,
		Username:  username,
		Password:  password,
		ClientID:  clientID,
		Timeout:   q.Timeout,
	})
	if err != nil {
		return nil, err
	}

	name := q.Name
	if name == "" {
		name = u.Host + "/" + baseTopic
	}
	return NewMQTTSink(name, baseTopic, qos, retained, t, buildRetryConfig(q)), nil
}

// parsedQuery holds all DSN query parameters after validation.
type parsedQuery struct {
	Name        string
	TLSMode     tlsMode
	CABundle    string
	Headers     map[string]string
	Timeout     time.Duration
	compression string
	Keepalive   time.Duration

	RetryInitial     time.Duration
	RetryMaxInterval time.Duration
	RetryMaxElapsed  time.Duration
}

// CompressionOr returns the parsed compression value or the provided default.
// "none" is normalised to an empty string.
func (q parsedQuery) CompressionOr(def string) string {
	if q.compression == "" {
		return def
	}
	if q.compression == "none" {
		return ""
	}
	return q.compression
}

func parseQuery(u *url.URL, allowed map[string]struct{}) (parsedQuery, error) {
	var out parsedQuery
	out.Headers = map[string]string{}
	out.Timeout = 10 * time.Second
	out.RetryInitial = 500 * time.Millisecond
	out.RetryMaxInterval = 10 * time.Second
	out.RetryMaxElapsed = 30 * time.Second

	q := u.Query()
	for key := range q {
		if _, ok := allowed[key]; !ok {
			return parsedQuery{}, fmt.Errorf("sink dsn %q: unknown query parameter %q", redactedURL(u), key)
		}
	}

	if v := q.Get("name"); v != "" {
		out.Name = v
	}
	if v := q.Get("tls"); v != "" {
		switch strings.ToLower(v) {
		case "true", "1", "yes":
			out.TLSMode = tlsEnabled
		case "false", "0", "no", "":
			out.TLSMode = tlsDisabled
		case "insecure", "skipverify", "skip-verify":
			out.TLSMode = tlsInsecureSkipVerify
		default:
			return parsedQuery{}, fmt.Errorf("sink dsn %q: invalid tls=%q", redactedURL(u), v)
		}
	}
	if v := q.Get("ca"); v != "" {
		out.CABundle = v
	}
	for _, h := range q["header"] {
		k, v, ok := strings.Cut(h, ":")
		if !ok {
			return parsedQuery{}, fmt.Errorf("sink dsn %q: header %q must be key:value", redactedURL(u), h)
		}
		out.Headers[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	if v := q.Get("timeout"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return parsedQuery{}, fmt.Errorf("sink dsn %q: timeout=%q: %w", redactedURL(u), v, err)
		}
		out.Timeout = d
	}
	if v := q.Get("compression"); v != "" {
		switch strings.ToLower(v) {
		case "gzip", "none":
			out.compression = strings.ToLower(v)
		default:
			return parsedQuery{}, fmt.Errorf("sink dsn %q: invalid compression=%q (want gzip or none)", redactedURL(u), v)
		}
	}
	if v := q.Get("keepalive"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return parsedQuery{}, fmt.Errorf("sink dsn %q: keepalive=%q: %w", redactedURL(u), v, err)
		}
		out.Keepalive = d
	}
	if v := q.Get("retry_initial"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return parsedQuery{}, fmt.Errorf("sink dsn %q: retry_initial=%q: %w", redactedURL(u), v, err)
		}
		out.RetryInitial = d
	}
	if v := q.Get("retry_max_interval"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return parsedQuery{}, fmt.Errorf("sink dsn %q: retry_max_interval=%q: %w", redactedURL(u), v, err)
		}
		out.RetryMaxInterval = d
	}
	if v := q.Get("retry_max_elapsed"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return parsedQuery{}, fmt.Errorf("sink dsn %q: retry_max_elapsed=%q: %w", redactedURL(u), v, err)
		}
		out.RetryMaxElapsed = d
	}
	return out, nil
}

func redactedURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	return u.Redacted()
}

func buildRetryConfig(q parsedQuery) retryConfig {
	return retryConfig{
		Initial:    q.RetryInitial,
		Max:        q.RetryMaxInterval,
		MaxElapsed: q.RetryMaxElapsed,
		Multiplier: 2.0,
		Jitter:     0.2,
	}
}
