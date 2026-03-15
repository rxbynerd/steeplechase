# Steeplechase

Lightweight OTLP collector for Claude Code telemetry. Single Go binary, no OTel Collector SDK.

## Build & Test

```bash
make build        # Build to bin/steeplechase
make test         # go test -race ./...
make vet          # go vet ./...
go mod tidy       # After dependency changes
```

## Project Structure

- `cmd/steeplechase/main.go` - CLI entrypoint, flag parsing, signal handling
- `internal/sink/sink.go` - Sink interface (ConsumeMetrics, ConsumeLogs, ConsumeTraces)
- `internal/sink/stdout.go` - StdoutSink: mutex-serialized writes to io.Writer
- `internal/receiver/grpc.go` - gRPC server on :4317
- `internal/receiver/http.go` - HTTP server on :4318 (protobuf + JSON)
- `internal/receiver/decompress.go` - gzip decompression for HTTP
- `internal/format/` - Human-readable formatting for metrics, logs, traces

## Conventions

- Go module: `github.com/rxbynerd/steeplechase`
- Sink interface takes full proto request objects, not extracted fields
- HTTP Content-Type dispatch: `application/x-protobuf` or `application/json`
- Version baked in via `-ldflags "-X main.version=..."`
- Tests use `*_test.go` convention alongside source files
