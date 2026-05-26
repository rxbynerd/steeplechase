# Steeplechase

A lightweight OTLP router for AI coding harness telemetry. Receives OpenTelemetry metrics, logs, and traces on the standard OTLP ports and fans them out to one or more destinations: stdout, another OTLP backend, or any combination.

Built primarily to support [Stirrup](https://github.com/rxbynerd/stirrup) development. Claude Code is a secondary supported source — both are standard OTLP clients, and any other OTel-instrumented harness will work without changes.

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

Publish every received OTLP payload to an MQTT broker:

```bash
./bin/steeplechase \
  --sink 'mqtt://telemetry:secret@mqtt.internal:1883/coding-harness/otel'
```

The MQTT sink publishes protobuf-encoded OTLP export requests to
`coding-harness/otel/metrics`, `coding-harness/otel/logs`, and
`coding-harness/otel/traces`.

### Pointing Stirrup at Steeplechase

```bash
./stirrup harness \
  --prompt "Fix the failing test in main_test.go" \
  --trace-emitter otel \
  --otel-endpoint localhost:4317
```

Stirrup also emits its harness metrics over OTLP/gRPC to the same endpoint when one is configured. See [`docs/safety-rings.md`](https://github.com/rxbynerd/stirrup/blob/main/docs/safety-rings.md) in the Stirrup repo for production-shape configurations.

### Pointing Claude Code at Steeplechase

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
--grpc-addr             gRPC listen address (default :4317)
--http-addr             HTTP listen address (default :4318)
--admin-addr            Admin listener for /healthz, /readyz, /metrics (default :9090)
--sink                  Sink DSN, repeatable. Defaults to stdout if none given.
--stdout-format         Stdout layout: line (default), grouped, or tree.
--stdout-run-timeout    Per-run idle timeout for grouped/tree modes (default 5m).
--stdout-run-max-items  Per-run buffered-item cap for grouped/tree modes (default 10000).
--version               Print version and exit
```

The `--stdout-format` / `--stdout-run-timeout` / `--stdout-run-max-items` flags
configure how stdout groups telemetry that carries `run.id`. They affect
stdout sinks only; OTLP forwarding sinks always receive the original
payload unchanged. See [Output Format](#output-format) below for examples.

## Routing

Every configured `--sink` receives every OTLP payload in parallel. Per-sink failures are logged with structured context and exposed as metrics, but they do not propagate back to the upstream client unless *every* sink fails. This best-effort policy prevents a single flaky backend from triggering upstream retries that would duplicate data at the healthy sinks.

### Sink DSN format

```
stdout                                         # write to process stdout
otlp+grpc://HOST:PORT[?k=v&...]                # forward over OTLP gRPC
otlp+http://HOST:PORT[/BASE][?k=v&...]         # forward over OTLP HTTP (plaintext)
otlp+https://HOST:PORT[/BASE][?k=v&...]        # forward over OTLP HTTP with TLS
mqtt://[USER:PASS@]HOST:PORT/TOPIC[?k=v&...]   # publish OTLP protobuf to MQTT
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
| `qos` | `1` | MQTT publish QoS: `0`, `1`, or `2` |
| `retained` | `false` | MQTT retained publish flag |
| `client_id` | generated | MQTT client identifier |

Unknown query keys cause startup to fail loudly, so typos become hard errors instead of silently-dropped configuration.

MQTT sink passwords are accepted in the DSN user-info section and are redacted
from startup errors and logs. The configured `TOPIC` is a base topic; metrics,
logs, and traces are published under `/metrics`, `/logs`, and `/traces`
respectively. MQTT publish wildcard characters (`+`, `#`) are rejected in the
base topic.

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

The default layout (`--stdout-format=line`) prints one line per OTLP item
as it arrives:

```
[METRIC] 2026-03-15T10:30:00.123Z stirrup.harness.tokens.input = 1523 {run.id=abc, run.mode=execution}
[EVENT]  2026-03-15T10:30:01.456Z claude_code.api_request {model=claude-sonnet-4-6, duration_ms=2341}
[LOG]    2026-03-15T10:30:03.012Z INFO "message body" {attr1=val1}
[TRACE]  2026-03-15T10:30:04.000Z turn[3] trace=abc123 span=def456
```

### Per-run grouping

`--stdout-format=grouped` buffers every item that carries a `run.id` (Stirrup
sets this on its `run` root span and on every metric data point) and flushes
the run as a single block when the root span ends with a `run.outcome`
attribute, when the run has been idle for `--stdout-run-timeout`, or when
`--stdout-run-max-items` is exceeded. Items with no discoverable `run.id`
stream straight through unchanged.

```
=== run abc started 2026-03-15T10:30:00.000Z mode=execution provider=anthropic model=claude-sonnet-4-6 ===
[TRACE]  2026-03-15T10:30:00.000Z run trace=abc123 span=root1 {run.id=abc, run.mode=execution}
[METRIC] 2026-03-15T10:30:00.123Z stirrup.harness.tokens.input = 1523 {run.id=abc, run.mode=execution}
[TRACE]  2026-03-15T10:30:01.500Z turn[1] trace=abc123 span=turn1 {turn.number=1}
[TRACE]  2026-03-15T10:30:02.000Z tool_call trace=abc123 span=tool1 {tool.name=edit_file}
=== run abc finished 2026-03-15T10:30:03.000Z outcome=success turns=1 ===
```

If the run is flushed by idle timeout or shutdown the footer reads
`outcome=<unknown>`. Buffer overflow replaces the footer with a single
`[WARN] run abc truncated at <N> items` line, after which any further items
for that `run.id` stream as line mode would.

### Tree view of spans

`--stdout-format=tree` uses the same per-run buffering as `grouped`, but
spans are indented by depth from their `parent_span_id` chain. Logs and
metrics remain flat and chronologically interleaved with the trace lines.

```
=== run abc started 2026-03-15T10:30:00.000Z ===
[TRACE]  2026-03-15T10:30:00.000Z run trace=abc123 span=root1
[TRACE]    2026-03-15T10:30:01.500Z turn[1] trace=abc123 span=turn1
[TRACE]      2026-03-15T10:30:02.000Z tool_call trace=abc123 span=tool1
=== run abc finished 2026-03-15T10:30:03.000Z outcome=success ===
```

## Supported telemetry sources

Steeplechase routes by OTLP envelope and does not interpret signal-specific semantics, so any OTel-instrumented client targeting `:4317` (gRPC) or `:4318` (HTTP) will be accepted. The two harnesses below are the ones used in active development; representative shapes from each are exercised by the test suite.

### Stirrup (primary)

Stirrup emits OTel traces and metrics over OTLP/gRPC when started with `--trace-emitter otel`.

**Metrics** (`stirrup.harness.*` prefix):

- Counters: `runs`, `turns`, `tokens.input`, `tokens.output`, `tool_calls`, `tool_errors`, `provider_requests`, `provider_errors`, `context_compactions`, `security_events`, `verification_attempts`, `stalls`
- Histograms: `run_duration`, `turn_duration`, `tool_call_duration`, `provider_latency`, `provider_ttfb`
- Observable gauge: `context_tokens` (live per-run context window estimate, tagged with `run.id` and `run.mode`)

**Traces**: a `run` root span per harness invocation, with `turn[N]` and `tool_call` child spans. Common attributes include `run.id`, `run.mode`, `run.provider`, `run.model`, `turn.number`, `turn.tokens.input`, `turn.tokens.output`, `tool.name`, `tool.success`.

Stirrup currently routes its `slog` output to stderr rather than OTLP logs, so the log signal will normally be empty when Stirrup is the only source.

### Claude Code (secondary)

**Metrics** (8 counters): `claude_code.session.count`, `claude_code.lines_of_code.count`, `claude_code.pull_request.count`, `claude_code.commit.count`, `claude_code.cost.usage`, `claude_code.token.usage`, `claude_code.code_edit_tool.decision`, `claude_code.active_time.total`

**Events** (5 types, carried as OTLP log records with an `event.name` attribute): `claude_code.user_prompt`, `claude_code.tool_result`, `claude_code.api_request`, `claude_code.api_error`, `claude_code.tool_decision`

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
