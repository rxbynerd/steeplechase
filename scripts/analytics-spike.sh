#!/usr/bin/env bash
#
# analytics-spike.sh — reproduce the duckdb-otlp local token-analytics spike (#5).
#
# Stands up the two-hop stack on a single machine:
#
#   synthetic OTLP/JSON ──▶ Steeplechase ──▶ otlp+http sink ──▶ duckdb-otlp otlp_serve() ──▶ DuckDB
#
# then sends a representative claude_code.token.usage payload (two projects, all
# four token types, agent.name present and absent) and runs the canonical query.
#
# This is a documentation/spike aid, not production tooling. See docs/analytics.md.
#
# Requirements: the duckdb-otlp extension already built (see docs/analytics.md §2),
#   the steeplechase binary built (`make build`), and `curl`.
#
# Usage:
#   DUCKDB_OTLP=/path/to/duckdb-otlp scripts/analytics-spike.sh
#
set -euo pipefail

# --- config ------------------------------------------------------------------
DUCKDB_OTLP="${DUCKDB_OTLP:-$HOME/Developer/duckdb-otlp}"
DUCKDB_BIN="${DUCKDB_BIN:-$DUCKDB_OTLP/build/release/duckdb}"
STEEPLECHASE_BIN="${STEEPLECHASE_BIN:-./bin/steeplechase}"
TOKEN="${TOKEN:-dev-token-0123456789}"      # must be >= 16 chars (otlp_serve requirement)
INGEST_PORT="${INGEST_PORT:-4319}"          # duckdb-otlp; MUST differ from Steeplechase :4318
SC_GRPC="${SC_GRPC:-:4327}"                  # spike Steeplechase ports (avoid clashing with a
SC_HTTP="${SC_HTTP:-:4328}"                  # running instance on :4317/:4318/:9090)
SC_ADMIN="${SC_ADMIN:-:9091}"
WORK="${WORK:-/tmp/sc-spike}"

say() { printf '\n=== %s ===\n' "$*"; }
cleanup() {
  say "tearing down"
  [[ -p "$WORK/cmd.pipe" ]] && printf "CALL otlp_stop('otlp:127.0.0.1:%s');\n.exit\n" "$INGEST_PORT" > "$WORK/cmd.pipe" 2>/dev/null || true
  sleep 1
  pkill -f "steeplechase --grpc-addr $SC_GRPC" 2>/dev/null || true
  pkill -f "sleep 1000000" 2>/dev/null || true
}
trap cleanup EXIT

[[ -x "$DUCKDB_BIN" ]] || { echo "duckdb-otlp shell not found at $DUCKDB_BIN — build it first (docs/analytics.md §2)"; exit 1; }
[[ -x "$STEEPLECHASE_BIN" ]] || { echo "steeplechase not found at $STEEPLECHASE_BIN — run 'make build'"; exit 1; }

rm -rf "$WORK"; mkdir -p "$WORK"; mkfifo "$WORK/cmd.pipe"

# --- A. start the duckdb-otlp ingest server (FIFO-driven, see docs §5.3) ------
say "starting duckdb-otlp otlp_serve() on 127.0.0.1:$INGEST_PORT"
sleep 1000000 > "$WORK/cmd.pipe" &                                   # hold FIFO open
"$DUCKDB_BIN" "$WORK/otlp.duckdb" < "$WORK/cmd.pipe" > "$WORK/duck.log" 2>&1 &
sleep 2
printf "INSTALL json; LOAD json;
CALL otlp_serve('otlp:127.0.0.1:%s', token := '%s', create_tables := true);
" "$INGEST_PORT" "$TOKEN" > "$WORK/cmd.pipe"
sleep 2

# --- B. start Steeplechase with the analytics sink in the fan-out ------------
say "starting Steeplechase ($SC_GRPC / $SC_HTTP) -> duckdb-otlp"
"$STEEPLECHASE_BIN" \
  --grpc-addr "$SC_GRPC" --http-addr "$SC_HTTP" --admin-addr "$SC_ADMIN" \
  --sink stdout \
  --sink "otlp+http://localhost:$INGEST_PORT?header=x-api-key:$TOKEN&name=duckdb-otlp" \
  > "$WORK/steeplechase.log" 2>&1 &
sleep 1

# --- C. send a representative synthetic payload through Steeplechase ----------
say "sending synthetic claude_code.token.usage (2 projects, 4 token types)"
NOW=$(date +%s); NS=$((NOW * 1000000000)); START=$(((NOW-120) * 1000000000))
dp() { # asInt type model agent query_source  -> one OTLP data point
  local v=$1 t=$2 m=$3 a=$4 q=$5 attrs
  attrs="{\"key\":\"type\",\"value\":{\"stringValue\":\"$t\"}},{\"key\":\"model\",\"value\":{\"stringValue\":\"$m\"}},{\"key\":\"query_source\",\"value\":{\"stringValue\":\"$q\"}}"
  [[ -n "$a" ]] && attrs="$attrs,{\"key\":\"agent.name\",\"value\":{\"stringValue\":\"$a\"}}"
  printf '{"asInt":"%s","startTimeUnixNano":"%s","timeUnixNano":"%s","attributes":[%s]}' "$v" "$START" "$NS" "$attrs"
}
rm_block() { # project  dp1 dp2 ...
  local project=$1; shift; local pts; pts=$(printf '%s,' "$@"); pts=${pts%,}
  printf '{"resource":{"attributes":[{"key":"project","value":{"stringValue":"%s"}},{"key":"service.name","value":{"stringValue":"claude-code"}}]},"scopeMetrics":[{"scope":{"name":"com.anthropic.claude_code"},"metrics":[{"name":"claude_code.token.usage","unit":"tokens","sum":{"aggregationTemporality":"AGGREGATION_TEMPORALITY_CUMULATIVE","isMonotonic":true,"dataPoints":[%s]}}]}]}' "$project" "$pts"
}
SC=$(rm_block steeplechase \
  "$(dp 1200 input  claude-sonnet-4-6 code-reviewer subagent)" \
  "$(dp 300  output claude-sonnet-4-6 code-reviewer subagent)" \
  "$(dp 5000 cacheRead claude-sonnet-4-6 code-reviewer subagent)" \
  "$(dp 800  cacheCreation claude-sonnet-4-6 code-reviewer subagent)" \
  "$(dp 900  input  claude-sonnet-4-6 '' main)" \
  "$(dp 250  output claude-sonnet-4-6 '' main)")
ST=$(rm_block stirrup \
  "$(dp 400 input  claude-opus-4-1 '' main)" \
  "$(dp 100 output claude-opus-4-1 '' main)" \
  "$(dp 700 input  claude-opus-4-1 general-purpose subagent)" \
  "$(dp 180 output claude-opus-4-1 general-purpose subagent)")
printf '{"resourceMetrics":[%s,%s]}' "$SC" "$ST" > "$WORK/load.json"
curl -sS -o /dev/null -w 'steeplechase ingest: HTTP %{http_code}\n' \
  "http://localhost:${SC_HTTP#:}/v1/metrics" -H 'Content-Type: application/json' --data-binary @"$WORK/load.json"

# (optional) drive a real Claude Code session into the same collector:
#   claude --settings <(printf '{"env":{"OTEL_EXPORTER_OTLP_ENDPOINT":"http://localhost:%s","OTEL_RESOURCE_ATTRIBUTES":"project=realrun","OTEL_METRIC_EXPORT_INTERVAL":"1000"}}' "${SC_GRPC#:}") -p "…"

# --- D. flush + run the canonical query --------------------------------------
say "flush + canonical query"
sleep 6   # > the ~5s auto-commit window
: > "$WORK/duck.log"
cat > "$WORK/q.sql" <<SQL
.mode box
CALL otlp_flush('otlp:127.0.0.1:$INGEST_PORT');
SELECT
  date_trunc('day', time_unix_nano)                         AS day,
  (resource_attributes ->> '\$.project')                    AS project,
  coalesce(metric_attributes ->> '\$."agent.name"',
           metric_attributes ->> '\$.query_source')         AS agent,
  (metric_attributes ->> '\$.type')                         AS token_type,
  sum(coalesce(int_value, double_value))                    AS tokens
FROM otlp_metrics_sum
WHERE name = 'claude_code.token.usage'
GROUP BY 1,2,3,4
ORDER BY project, agent, token_type;
SELECT '---done---' AS marker;
SQL
cat "$WORK/q.sql" > "$WORK/cmd.pipe"
sleep 3
cat "$WORK/duck.log"
echo
echo "Artifacts in $WORK (duck.log, steeplechase.log, otlp.duckdb, load.json)."
