# Steeplechase

A lightweight OTLP collector that captures all OpenTelemetry telemetry from Claude Code and prints it to STDOUT.

Designed for full-fidelity local capture with future forwarding to aggregation backends (Redis TimeSeries, AWS).

## Quick Start

```bash
make build
./bin/steeplechase
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
--grpc-addr  gRPC listen address (default :4317)
--http-addr  HTTP listen address (default :4318)
--version    Print version and exit
```

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
docker run -p 4317:4317 -p 4318:4318 steeplechase
```
