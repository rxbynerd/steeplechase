# Steeplechase

Lightweight OTLP router for AI coding harness telemetry. Single Go binary, no OTel Collector SDK. Receives OTLP on :4317/:4318 and fans each payload out to one or more configured sinks (stdout, OTLP gRPC, OTLP HTTP).

Built primarily to support Stirrup (`github.com/rxbynerd/stirrup`) development. Claude Code is a secondary supported source; the routing path is signal-agnostic so any OTLP client works without code changes.

## Build & Test

```bash
make build        # Build to bin/steeplechase
make test         # go test -race ./...
make vet          # go vet ./...
go mod tidy       # After dependency changes
```

## Project Structure

- `cmd/steeplechase/main.go` - CLI entrypoint, flag parsing, pipeline construction, signal-driven shutdown
- `internal/sink/sink.go` - Sink interface (`ConsumeMetrics`, `ConsumeLogs`, `ConsumeTraces`, `Name`, `Shutdown`)
- `internal/sink/stdout.go` - StdoutSink: mutex-serialized writes to io.Writer
- `internal/sink/runblock.go` - RunBlockSink: wraps StdoutSink, buffers per-run.id and renders grouped/tree blocks via `internal/format/runblock.go`
- `internal/sink/fanout.go` - FanoutSink: parallel best-effort fan-out with slog per-child failure logging
- `internal/sink/metered.go` - MeteredSink: wraps any Sink and observes Prometheus metrics
- `internal/sink/otlp_forward.go` - OTLPForwardSink + transport interface
- `internal/sink/otlp_forward_grpc.go` - gRPC transport (TLS, gzip, headers, keepalive)
- `internal/sink/otlp_forward_http.go` - HTTP transport (TLS, gzip, headers, retry classification)
- `internal/sink/retry.go` - Exponential backoff + `permanentError` sentinel
- `internal/sink/dsn.go` - `ParseDSN` for `stdout`, `otlp+grpc://`, `otlp+http://`, `otlp+https://`, `mqtt://`
- `internal/sink/mqtt.go` - MQTT sink that publishes protobuf OTLP requests to `<topic>/metrics`, `<topic>/logs`, `<topic>/traces`
- `internal/sinktest/` - Shared test fakes: `RecordingSink`, `ErrorSink`, `SlowSink`
- `internal/receiver/grpc.go` - gRPC server on :4317, observes receiver metrics
- `internal/receiver/http.go` - HTTP server on :4318 (protobuf + JSON), observes receiver metrics
- `internal/receiver/decompress.go` - gzip decompression for HTTP
- `internal/metrics/` - Prometheus `Recorder` (sink + receiver counters, histograms, gauges)
- `internal/admin/` - Admin HTTP server: `/healthz`, `/readyz`, `/metrics`
- `internal/format/` - Human-readable formatting for metrics, logs, traces (used by StdoutSink)
- `internal/format/runblock.go` - Per-run block renderer (header/footer banners, grouped vs tree body, tree depth from `parent_span_id`)

## Conventions

- Go module: `github.com/rxbynerd/steeplechase`
- Sink interface takes full proto request objects, not extracted fields
- Sinks must be safe for concurrent use; `Name()` returns the metric/log label; `Shutdown()` releases resources
- MQTT sinks publish the original OTLP protobuf export request bytes under a configured base topic suffixed by signal; do not extract or reformat telemetry there
- Fan-out is best-effort: FanoutSink returns nil unless every child fails, keeping upstream retries from duplicating into healthy sinks
- HTTP Content-Type dispatch: `application/x-protobuf` or `application/json`
- DSN is the configuration surface. If options grow beyond what CLI flags handle, use TOML or HCLv2, **not** YAML or JSON
- Prometheus registry and `metrics.Recorder` are constructed in `main` and passed into receivers and `MeteredSink` wrappers; avoid global registries
- Version baked in via `-ldflags "-X main.version=..."` and surfaced as `steeplechase_build_info{version}`
- Tests use `*_test.go` convention alongside source files; routing-sink tests use `package sink_test` to avoid an import cycle with `internal/sinktest`
- `--stdout-format=grouped|tree` keys per-run buffers off `run.id` (set by Stirrup on the root span and on metric data-point attributes); the RunBlockSink also keeps a `trace_id -> run.id` map so child spans whose own `run.id` attribute is missing can still be bucketed correctly. Items with no discoverable `run.id` bypass the buffer entirely — keep this contract intact when extending the sink

## Local analytics (token usage)

Decision record: `docs/analytics.md` (spike #5, verdict **GO**). To slice token burn per day/project/agent with SQL, **forward to `smithclay/duckdb-otlp`'s `otlp_serve()` via an `otlp+http://` sink** — no Steeplechase code change. `scripts/analytics-spike.sh` reproduces the whole stack. Load-bearing constraints when implementing:

- **Do not build an embedded DuckDB sink.** DuckDB's single-writer lock is absolute across processes (a second process cannot open the live file even read-only) — query from inside the serving session, stop/flush, or target a DuckLake catalog for lock-free reads.
- Data lands in `otlp_metrics_sum`. Aggregate token counts with `sum(coalesce(int_value, double_value))` — real Claude Code emits **doubles**, so `sum(int_value)` silently returns NULL. The metric-name column is `name` (not `metric_name`). Attributes are VARCHAR JSON columns `metric_attributes` / `resource_attributes`; extract dotted keys via `->> '$."agent.name"'` and **parenthesize** JSON comparisons: `(resource_attributes ->> '$.project') = 'x'`.
- `otlp_serve()` requires a ≥16-char token on every route → pass it on the sink DSN as `header=x-api-key:<token>`. Pin the duckdb-otlp commit; its published schema docs are **stale** relative to the code.
- Keep the issue-#4 semconv mapper **OFF** for this path — it would rename the metric and attribute keys and break these queries.
