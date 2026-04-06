# Steeplechase

A lightweight OTLP router for Claude Code telemetry. Receives OpenTelemetry metrics, logs, and traces on the standard OTLP ports and fans them out to one or more destinations: stdout, another OTLP backend, or any combination.

Single Go binary, no OTel Collector SDK.

## Quick Start

```bash
make build
./bin/steeplechase                    # defaults to a single stdout sink
```

Forward to another OTLP backend while also printing locally:

```bash
./bin/steeplechase \
  --sink stdout \
  --sink 'otlp+grpc://vector.internal:4317?compression=gzip&header=x-api-key:secret'
```

Configure Claude Code to send telemetry:

```bash
export CLAUDE_CODE_ENABLE_TELEMETRY=1
export OTEL_METRICS_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_METRIC_EXPORT_INTERVAL=10000
```

## Protocols

| Protocol | Port | Endpoint |
|----------|------|----------|
| gRPC | 4317 | - |
| HTTP/protobuf | 4318 | `/v1/metrics`, `/v1/logs`, `/v1/traces` |
| HTTP/JSON | 4318 | `/v1/metrics`, `/v1/logs`, `/v1/traces` |

## Flags

```
--grpc-addr   gRPC listen address (default :4317)
--http-addr   HTTP listen address (default :4318)
--admin-addr  Admin listener for /healthz, /readyz, /metrics (default :9090)
--sink        Sink DSN, repeatable. Defaults to stdout if none given.
--version     Print version and exit
```

## Routing

Every configured `--sink` receives every OTLP payload in parallel. Per-sink failures are logged with structured context and exposed as metrics, but they do not propagate back to the upstream client unless *every* sink fails. This best-effort policy prevents a single flaky backend from triggering upstream retries that would duplicate data at the healthy sinks.

### Sink DSN format

```
stdout                                         # write to process stdout
otlp+grpc://HOST:PORT[?k=v&...]                # forward over OTLP gRPC
otlp+http://HOST:PORT[/BASE][?k=v&...]         # forward over OTLP HTTP (plaintext)
otlp+https://HOST:PORT[/BASE][?k=v&...]        # forward over OTLP HTTP with TLS
```

Query parameters (all optional):

| Key | Default | Meaning |
|---|---|---|
| `name` | `host:port` | Label used in logs and Prometheus metrics |
| `tls` | off for grpc, on for https | `true`, `false`, or `insecure` (skip verify) |
| `ca` | — | Path to a PEM CA bundle |
| `header` | — | Outbound header, format `key:value`; repeatable |
| `timeout` | `10s` | Per-call deadline |
| `compression` | `gzip` (grpc), `none` (http) | `gzip` or `none` |
| `retry_initial` | `500ms` | First backoff interval |
| `retry_max_interval` | `10s` | Cap on any single backoff |
| `retry_max_elapsed` | `30s` | Total retry budget per call |
| `keepalive` | off | gRPC client keepalive ping interval |

Unknown query keys cause startup to fail loudly, so typos become hard errors instead of silently-dropped configuration.

## Admin Endpoints

A dedicated listener (default `:9090`) exposes:

| Path | Behaviour |
|---|---|
| `GET /healthz` | Always 200 while the process is alive (liveness probe). |
| `GET /readyz` | 200 once startup completes; 503 during shutdown so load balancers can drain. |
| `GET /metrics` | Prometheus text format. |

### Exported metrics

| Metric | Labels | Type |
|---|---|---|
| `steeplechase_receiver_accept_total` | receiver, signal | counter |
| `steeplechase_sink_receive_total` | sink, signal | counter |
| `steeplechase_sink_success_total` | sink, signal | counter |
| `steeplechase_sink_failure_total` | sink, signal, reason | counter |
| `steeplechase_sink_latency_seconds` | sink, signal | histogram |
| `steeplechase_sink_retries_total` | sink, signal | counter |
| `steeplechase_sink_inflight` | sink, signal | gauge |
| `steeplechase_build_info` | version | gauge |

`reason` is drawn from a closed label set: `timeout`, `canceled`, `permanent`, `other`.

## Output Format

```
[METRIC] 2026-03-15T10:30:00.123Z claude_code.token.usage = 1523 {type=input, model=claude-sonnet-4-6}
[EVENT]  2026-03-15T10:30:01.456Z claude_code.api_request {model=claude-sonnet-4-6, duration_ms=2341}
[LOG]    2026-03-15T10:30:03.012Z INFO "message body" {attr1=val1}
[TRACE]  2026-03-15T10:30:04.000Z span-name trace=abc123 span=def456
```

## What Claude Code Emits

**Metrics** (8 counters): `claude_code.session.count`, `claude_code.lines_of_code.count`, `claude_code.pull_request.count`, `claude_code.commit.count`, `claude_code.cost.usage`, `claude_code.token.usage`, `claude_code.code_edit_tool.decision`, `claude_code.active_time.total`

**Events** (5 types): `claude_code.user_prompt`, `claude_code.tool_result`, `claude_code.api_request`, `claude_code.api_error`, `claude_code.tool_decision`

## Development

```bash
make test    # Run tests with race detector
make vet     # Run go vet
make build   # Build binary
make all     # vet + test + build
```

## Docker

```bash
docker build -t steeplechase .
docker run -p 4317:4317 -p 4318:4318 -p 9090:9090 steeplechase
```
