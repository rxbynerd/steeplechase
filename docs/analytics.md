# Local token-usage analytics (spike #5)

Streaming Steeplechase telemetry into a locally queryable datastore so token
burn can be sliced **per day, per project, per agent, per token type** with SQL
instead of squinting at stdout.

This document is the deliverable of the timeboxed spike in
[issue #5](https://github.com/rxbynerd/steeplechase/issues/5): a reproducible
setup, the **working** SQL, findings against the three open questions, and a
**go/no-go** recommendation.

---

## TL;DR — Recommendation: **GO (adopt `duckdb-otlp` live ingest)**

For the single-developer local-analytics use case, `smithclay/duckdb-otlp`'s
`otlp_serve()` is a **viable bridge**. Driven live from real Claude Code
sessions routed through Steeplechase (zero Steeplechase code changes), it lands
`claude_code.token.usage` into `otlp_metrics_sum` with **every dimension we need
preserved and directly queryable** — metric name, `type`, `model`, `agent.name`,
`query_source`, and the resource-level `project` — with **no join required**.

Adopt it **with these load-bearing conditions** (each validated below):

1. **Pin the commit.** `duckdb-otlp` has no tagged release; build from a pinned
   SHA against DuckDB `v1.5.3`. The published community-extension/docs schema
   does **not** match the code.
2. **Aggregate with `sum(coalesce(int_value, double_value))`.** Real Claude Code
   emits token counts as **doubles**; a query using `sum(int_value)` alone
   silently returns `NULL` for real data.
3. **Parenthesize JSON filters:** `(resource_attributes ->> '$.project') = '…'`.
   The `->>` operator binds *looser* than `=` in DuckDB.
4. **Pass the ingest auth token via the sink DSN** (`header=x-api-key:…`).
   `otlp_serve()` mandates a ≥16-char token on every route.
5. **Pick a concurrency pattern** for the single-writer lock (see §5.3): query
   from *inside* the serving session, or stop/flush then read, or evaluate the
   DuckLake target for lock-free reads.
6. Accept a **~5 s at-most-once loss window** on hard crash and **always call
   `otlp_stop()` before exit**.

**Do not build an embedded DuckDB sink in Steeplechase.** The single-writer lock
(empirically: a second process cannot open the live file *even read-only*, §5.3)
is exactly why that was rejected, and the spike confirms the rejection was
correct.

**Fallback** (`smithclay/otlp2parquet`) remains the right call *only if* the
single-writer ergonomics or pre-release maturity become blockers — it is more
mature (14 tagged releases) and decouples writer from reader, at the cost of a
different schema and an extra "query globbed Parquet" step (§7).

---

## 1. What was tested

Two-hop path, exactly as the issue specifies (no Steeplechase code change):

```
 Claude Code ──OTLP/gRPC :4327──▶ Steeplechase ──┬─▶ stdout sink
 (or synthetic OTLP/JSON :4328)                  │
                                                 └─▶ otlp+http sink ──OTLP/HTTP─▶ duckdb-otlp otlp_serve() :4319 ─▶ DuckDB
```

Both ingress legs were exercised: synthetic OTLP/JSON over Steeplechase's
**HTTP** receiver, and **real** Claude Code telemetry over Steeplechase's
**gRPC** receiver. Steeplechase forwards both as OTLP/HTTP protobuf to
duckdb-otlp.

### Pinned versions (reproducibility)

| Component | Version / SHA |
| --- | --- |
| `duckdb-otlp` (`main`) | `92be607` |
| ├─ `duckdb` submodule | `14eca11bd9` (**v1.5.3**) |
| ├─ `external/otlp2records` (Rust) | `5edd011` (**v0.8.4**) |
| └─ `extension-ci-tools` | `4b3b37b` (v1.5-variegata) |
| DuckDB CLI / library | **v1.5.3** (Variegata, `14eca11bd9`) |
| Steeplechase | `81b418b` |
| Toolchain | Apple clang 21, cmake 4.3.3, ninja 1.13.2, cargo 1.95.0 |

The brew `duckdb` v1.5.3 CLI is built from the **same commit** as the submodule,
so the loadable `otlp.duckdb_extension` is ABI-compatible with it as well as with
the in-tree `build/release/duckdb` shell.

### The integration requirement we discovered

`otlp_serve()` **requires** a bearer token (≥16 chars) on every ingest route
(`src/otlp_server.cpp:243`), accepting either `Authorization: Bearer <t>` or
`x-api-key: <t>`. Steeplechase's `otlp+http://` sink can supply it because the
DSN supports repeatable `header=k:v` params merged onto every request
(`internal/sink/dsn.go`, `internal/sink/otlp_forward_http.go:168`). So the only
configuration needed on the Steeplechase side is:

```
otlp+http://localhost:4319?header=x-api-key:dev-token-0123456789&name=duckdb-otlp
```

If the token is wrong, duckdb-otlp returns **401**, which Steeplechase's forward
sink classifies as a *permanent* (non-retried) error — the row is dropped from
that sink, but the fan-out still succeeds because stdout accepted it.

---

## 2. Reproduce the stack

```bash
# --- A. Build duckdb-otlp from source (one-time) -----------------------------
cd /path/to/duckdb-otlp
git submodule update --init --recursive          # duckdb v1.5.3 + otlp2records (Rust)
GEN=ninja make                                   # ~few min on 18 cores; needs cmake, ninja, cargo
#   -> build/release/duckdb                       (shell with otlp linked in)
#   -> build/release/extension/otlp/otlp.duckdb_extension  (loadable)

# --- B. Start the ingest server (holds DuckDB's single writer lock) ----------
# otlp_serve() runs inside a DuckDB session; that session must stay alive AND is
# the only place you can query while it runs (see §5.3). Drive it via a FIFO:
SPIKE=/tmp/sc-spike; rm -rf $SPIKE; mkdir -p $SPIKE; mkfifo $SPIKE/cmd.pipe
sleep 1000000 > $SPIKE/cmd.pipe &                                  # hold FIFO open
/path/to/duckdb-otlp/build/release/duckdb $SPIKE/otlp.duckdb \
    < $SPIKE/cmd.pipe > $SPIKE/duck.log 2>&1 &                     # server process
printf "INSTALL json; LOAD json;
CALL otlp_serve('otlp:127.0.0.1:4319', token := 'dev-token-0123456789', create_tables := true);
" > $SPIKE/cmd.pipe

# --- C. Run Steeplechase with the analytics sink added to the fan-out --------
./bin/steeplechase \
  --sink stdout \
  --sink 'otlp+http://localhost:4319?header=x-api-key:dev-token-0123456789&name=duckdb-otlp'
# (use --grpc-addr/--http-addr/--admin-addr to avoid clashing with a running instance)

# --- D. Point a harness at Steeplechase --------------------------------------
export CLAUDE_CODE_ENABLE_TELEMETRY=1
export OTEL_METRICS_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_METRIC_EXPORT_INTERVAL=1000          # 1s for the spike (default 60s!)
export OTEL_RESOURCE_ATTRIBUTES="project=steeplechase"   # the per-project dim
claude -p "…"

# --- E. Flush + query (inside the serving session, via the FIFO) -------------
printf "CALL otlp_flush('otlp:127.0.0.1:4319');
<the query from §3>
" > $SPIKE/cmd.pipe; cat $SPIKE/duck.log
```

A ready-to-run version is checked in at
[`scripts/analytics-spike.sh`](../scripts/analytics-spike.sh).

> **`OTEL_METRIC_EXPORT_INTERVAL` matters.** Claude Code's default metric export
> interval is **60 s** and a short headless `claude -p` run can exit before the
> first export fires. Set it to ~1 s for the spike, or run long enough to cross a
> boundary. (Logs default to 5 s.)

> **If telemetry "vanishes":** Claude Code applies the `env` block in
> `~/.claude/settings.json` over the process environment. If that block pins
> `OTEL_EXPORTER_OTLP_ENDPOINT`, your `export …ENDPOINT=` is ignored and metrics
> go to the pinned collector. Override per-invocation **without editing the
> user's config** via `claude --settings <json-with-env-overrides>`.

---

## 3. The working query

The issue's draft query needed three corrections (each marked). This is the
verified form, returning sensible daily totals for **both** synthetic and real
data:

```sql
INSTALL json; LOAD json;   -- resource_/metric_attributes are VARCHAR JSON strings

SELECT
  date_trunc('day', time_unix_nano)                         AS day,        -- (a)
  (resource_attributes ->> '$.project')                     AS project,    -- (b) no join
  coalesce(metric_attributes ->> '$."agent.name"',                          -- (c) dotted key
           metric_attributes ->> '$.query_source')          AS agent,       --     fallback
  (metric_attributes ->> '$.type')                          AS token_type,
  sum(coalesce(int_value, double_value))                    AS tokens       -- (d) int OR double
FROM otlp_metrics_sum                                                       -- (e) table name
WHERE name = 'claude_code.token.usage'                                      -- (f) column is `name`
GROUP BY 1, 2, 3, 4
ORDER BY project, agent, token_type;
```

Corrections versus the issue's draft (which assumed the published-docs schema):

| # | Draft assumed | Reality (commit `92be607`) |
| --- | --- | --- |
| (b)(c) | `attributes ->> 'x'` | two columns: `resource_attributes` and `metric_attributes`, both **VARCHAR JSON** |
| (c) | `attributes ->> 'agent.name'` | dotted key needs JSON-path quoting: `->> '$."agent.name"'` |
| (d) | `sum(value)` | **no `value` column**; `int_value` BIGINT / `double_value` DOUBLE → `sum(coalesce(int_value, double_value))` |
| (f) | `metric_name` | column is **`name`** |
| filter | `resource_attributes ->> 'project' = 'x'` | `->>` binds looser than `=` → **must parenthesize**: `(… ->> '$.project') = 'x'` |

### Live output (synthetic + real, combined)

```
        day          │    project    │      agent      │  token_type   │ tokens
─────────────────────┼───────────────┼─────────────────┼───────────────┼────────
 2026-06-02 00:00:00 │ spike-realrun │ auxiliary       │ input         │ 465.0     ← real (haiku aux)
 2026-06-02 00:00:00 │ spike-realrun │ auxiliary       │ output        │ 16.0      ← real
 2026-06-02 00:00:00 │ spike-realrun │ main            │ cacheRead     │ 26042.0   ← real (opus main)
 2026-06-02 00:00:00 │ spike-realrun │ main            │ input         │ 2165.0    ← real
 2026-06-02 00:00:00 │ spike-realrun │ main            │ output        │ 1276.0    ← real
 2026-06-02 00:00:00 │ steeplechase  │ code-reviewer   │ cacheRead     │ 5000.0    ← synthetic (subagent)
 2026-06-02 00:00:00 │ steeplechase  │ main            │ input         │ 900.0     ← synthetic (main, no agent.name)
 2026-06-02 00:00:00 │ stirrup       │ general-purpose │ input         │ 700.0     ← synthetic (2nd project)
 …
```

`coalesce(agent.name, query_source)` correctly labels agent-less main-thread and
auxiliary rows (`main` / `auxiliary`) instead of dropping them.

Useful variants that also work:

```sql
-- Total tokens per project (the headline number)
SELECT (resource_attributes ->> '$.project') AS project,
       sum(coalesce(int_value, double_value)) AS tokens
FROM otlp_metrics_sum WHERE name='claude_code.token.usage' GROUP BY 1 ORDER BY 2 DESC;

-- Billable (input+output) vs cache, per project
SELECT (resource_attributes ->> '$.project') AS project,
       sum(coalesce(int_value,double_value)) FILTER (WHERE (metric_attributes->>'$.type') IN ('input','output'))   AS billable,
       sum(coalesce(int_value,double_value)) FILTER (WHERE (metric_attributes->>'$.type')='cacheRead')            AS cache_read,
       sum(coalesce(int_value,double_value)) FILTER (WHERE (metric_attributes->>'$.type')='cacheCreation')        AS cache_creation
FROM otlp_metrics_sum WHERE name='claude_code.token.usage' GROUP BY 1 ORDER BY 1;
```

---

## 4. The three open questions — answered

### Q1. Does `claude_code.token.usage` (a Sum) land with its name/type preserved? — **YES**

- Lands in table **`otlp_metrics_sum`**; the metric name is a queryable VARCHAR
  column **`name`**, stored verbatim. **No allow-listing or normalization** of
  metric names exists in the transform — arbitrary names (dots included) pass
  through byte-for-byte (`otlp2records/src/batch/metrics.rs`).
- Only OTLP **Summary** metrics and NaN/±Inf doubles are ever dropped; neither
  applies to integer/double token counters.
- **Value caveat (the big one):** there is no single `value` column. Integer
  data points populate `int_value` (BIGINT); double data points populate
  `double_value` (DOUBLE). **Real Claude Code emits doubles** — verified live:

  ```
     project    │ has_int │ has_double │ sum_int │ sum_double
  ──────────────┼─────────┼────────────┼─────────┼───────────
   spike-realrun │   0     │     8      │  NULL   │ 32702.0     ← real Claude Code  → double_value
   steeplechase  │   6     │     0      │  8450   │  NULL       ← synthetic asInt   → int_value
  ```
  Always aggregate with `sum(coalesce(int_value, double_value))`.

### Q2. Do data-point attributes (`type`/`model`/`agent.name`/`query_source`) survive as queryable? — **YES**

- Stored as one **VARCHAR column `metric_attributes`** holding a JSON object,
  e.g. `{"agent.name":"code-reviewer","model":"claude-sonnet-4-6","query_source":"subagent","type":"cacheRead"}`.
- Keys are preserved verbatim, **including dots** — extract with a quoted JSON
  path: `metric_attributes ->> '$."agent.name"'`. (`'$.agent.name'` would parse
  the dot as a path separator and return `NULL`.)
- `agent.name` is **absent for main-thread / auxiliary requests** — confirmed in
  real data (`query_source` ∈ {`main`,`auxiliary`}, no `agent.name`). The
  `coalesce(agent.name, query_source)` fallback is therefore mandatory, not
  cosmetic.

### Q3. Do resource-scoped attributes (the `project` dimension) survive as filterable columns? — **YES (the riskiest unknown is fine)**

- Stored as a **VARCHAR column `resource_attributes`** (JSON object),
  **denormalized onto every data-point row**. There is **no separate resource
  table and no join** — `(resource_attributes ->> '$.project')` is directly
  filterable/groupable on `otlp_metrics_sum`.
- Verified live with two synthetic projects (`steeplechase`, `stirrup`) and a
  real `project=spike-realrun`; all three sliced cleanly. `team.id` and other
  arbitrary resource keys survive too. (`service.name` is additionally hoisted
  into a dedicated `service_name` column but also remains in the JSON.)

### Bonus: is the ~5 s auto-commit cadence adequate? — **YES at single-dev volume**

Rows auto-sealed within ~5 s without any manual `otlp_flush()`; at sparse
token-metric volume the 64 MiB size trigger never fires, so the effective
cadence is the fixed 5 s timer. `seal_failures_total` stayed 0; a graceful
`otlp_stop()` committed remaining rows (22 rows persisted, server removed).

### Full `otlp_metrics_sum` schema (live `DESCRIBE`, 19 columns)

`time_unix_nano TIMESTAMP`, `start_time_unix_nano TIMESTAMP`, `name VARCHAR`,
`description VARCHAR`, `unit VARCHAR`, `int_value BIGINT`, `double_value DOUBLE`,
`service_name VARCHAR`, `service_namespace VARCHAR`, `service_instance_id VARCHAR`,
`resource_attributes VARCHAR`, `scope_name VARCHAR`, `scope_version VARCHAR`,
`scope_attributes VARCHAR`, `metric_attributes VARCHAR`, `flags INTEGER`,
`exemplars_json VARCHAR`, `aggregation_temporality INTEGER`, `is_monotonic BOOLEAN`.

(`time_unix_nano` is `TIMESTAMP` µs for the live-ingest server; the file reader
`read_otlp_metrics_sum()` returns `TIMESTAMP_NS` instead. `date_trunc('day', …)`
works on both.)

---

## 5. Gotchas & operational findings

### 5.1 Schema docs do not match the code
The published [schema reference](https://smithclay.github.io/duckdb-otlp/reference/schemas/)
describes `metric_name`, a single `value DOUBLE`, and `timestamp TIMESTAMP_MS`.
**None of these exist** at commit `92be607` (which has `name`,
`int_value`/`double_value`, `time_unix_nano`). Treat the **built extension** as
authoritative and re-check column shapes after any version bump.

### 5.2 `->>` precedence and the `int`/`double` split
Both covered in §3 — the two corrections most likely to silently produce wrong
or empty results.

### 5.3 Single-writer lock is **absolute** across processes (verified)
While `otlp_serve()` holds the writer, a second process **cannot open the file at
all — not even read-only**:

```
$ duckdb /tmp/sc-spike/otlp.duckdb -c "SELECT count(*) …"
IO Error: Could not set lock on file … Conflicting lock is held in …/duckdb (PID …)

$ duckdb -c "ATTACH '…/otlp.duckdb' (READ_ONLY); …"
IO Error: Could not set lock on file … Conflicting lock is held …
```

(`ATTACH … (READ_ONLY)` from a *second process* does **not** work — DuckDB's lock
is process-level.) Workable patterns for live querying:

- **Query inside the serving session** (what this spike did, via a FIFO/REPL).
- **`otlp_flush()` → `otlp_stop()` → open the file with a separate process.**
- **Evaluate the DuckLake target** (`otlp_serve(... )` against an attached
  DuckLake/Parquet catalog) for lock-free concurrent reads — duckdb-otlp's own
  answer to this constraint. *Not tested in this spike; recommended next step.*

This is the concrete reason an embedded Go DuckDB sink in Steeplechase stays
rejected.

### 5.4 Durability is best-effort (at-most-once, ~5 s window)
Buffered rows are in-memory only — no WAL/fsync before the seal commit
(`otlp_server_http.cpp:111` documents the 202-Accepted-≠-durable contract). A
SIGKILL/OOM before the next ~5 s seal loses that window. On an *implicit* DB
teardown (closing the session without `otlp_stop()`), buffered rows are dropped
silently. **Always `otlp_stop()` before exit.** For non-critical token analytics
this is acceptable.

### 5.5 Noisy seal logs
Each seal logs at **WARNING** level (`seal: catalog= rows=N batches=1`) even on
success — harmless but noisy if you tail the server log.

---

## 6. Risk register (from the reviewer pass)

Independent reliability and security reviews of the duckdb-otlp ingest path.

### Reliability

| Risk | Sev | Mitigation |
| --- | --- | --- |
| ~5 s of rows lost on crash/kill — no WAL | High | Accept window; `otlp_stop()` before shutdown |
| HTTP 202 ≠ durable (accept-then-buffer) | High | Treat ingest as best-effort; Steeplechase already does |
| Single-writer lock blocks 2nd-process reads (§5.3) | High | Query in-session / stop+read / DuckLake |
| `max_buffered_bytes` bounds *encoded* input, not decoded heap | Med | Set conservatively (16–32 MiB) for a laptop |
| Persistent seal failure (disk full) → unbounded 250 ms retry loop, no circuit breaker | Med | Monitor `seal_failures_total` / `seal_last_error` via `otlp_server_list()` |
| Breaking schema changes between releases; no tagged release | Med | Pin SHA; re-validate columns on upgrade |
| Seal cadence (5 s / 64 MiB) not tunable | Med | Accept 5 s window |
| HTTP hot path not covered by CI (manual concurrency test only) | Low | Run `test/manual/otlp_serve_concurrency.py` before adopting a new commit |

Positive signals: panic-safe Rust FFI (`catch_unwind`, no `unwrap`/`expect`),
careful seal error/rollback path, correct **503 backpressure** status (which
Steeplechase retries).

### Security (loopback single-dev threat model)

| Finding | Sev | Note |
| --- | --- | --- |
| **gzip decompression bomb** — `payload_max_length` caps *compressed* bytes only; gzip body expands unbounded in memory | High | Only reachable by a process hitting :4319 directly; **Steeplechase forwards uncompressed**, so the normal path doesn't trigger it. Keep `max_body_bytes` small; don't expose the port |
| `TimingSafeEqual` returns early on length mismatch (length oracle) | Med | Negligible for the 32-hex auto-token; matters only if exposed |
| Auth token appears in the `otlp_serve()` result set | Med | Capture once; don't re-`SELECT` it in logged sessions |
| `allow_other_hostname=true` silently permits `0.0.0.0`; **no TLS** | Med | Never set it without a TLS-terminating proxy |
| `/healthz` unauthenticated; error bodies echo internal text; no per-IP rate limit | Low | Acceptable on loopback |

Positive: **auth is mandatory** (128-bit CSPRNG token, ≥16-char minimum, checked
on every ingest route); non-localhost binds rejected by default ("Only localhost
is allowed").

**Verdict:** safe for **loopback** single-dev use (the spike's setup). Before any
non-loopback exposure: add a TLS proxy, fix the decompression cap, add per-IP
limits.

---

## 7. Fallback comparison — `smithclay/otlp2parquet`

Assessed from its repo/docs (not built in this spike, since the verdict is to
adopt duckdb-otlp).

| | `duckdb-otlp` `otlp_serve()` | `otlp2parquet` |
| --- | --- | --- |
| Maturity | Pre-release, **no tags** | **14 tagged releases** (v0.12.0, May 2026) |
| Ingest | OTLP/HTTP **+ gRPC-via-Steeplechase** (HTTP) | OTLP/HTTP only (no gRPC — irrelevant; Steeplechase forwards HTTP) |
| Output | Rows in a live DuckDB catalog | Partitioned Parquet on disk (`metrics/{type}/{service}/year=/hour=/…`) |
| Concurrency | **Single-writer lock** (§5.3) | **No lock** — writer & DuckDB readers fully decoupled |
| Query | `SELECT … FROM otlp_metrics_sum` | `SELECT … FROM read_parquet('dir/**/*.parquet')` |
| Schema for our dims | Verified: name/attrs/resource all preserved & queryable | ClickHouse-style PascalCase columns; attribute representation unverified |
| Auth | Mandatory bearer/x-api-key | None documented |

**When to switch:** if the single-writer ergonomics block the "ad-hoc SQL while
ingest runs" workflow and the DuckLake target (§5.3) proves fiddly, **or** if
pre-release churn becomes painful. otlp2parquet trades the convenient single-DB
query surface for lock-free, more-stable Parquet-on-disk.

---

## 8. Recommendation & next steps

**Adopt `duckdb-otlp` live ingest** for local token analytics. It meets the goal
end-to-end with zero Steeplechase code change; every dimension we care about is
preserved and directly queryable; the failure modes are understood and tolerable
for non-critical analytics.

Recommended follow-ups (not blockers):

1. **Decide the concurrency pattern** and document it: in-session query vs
   stop+read vs **evaluate the DuckLake target** for lock-free reads.
2. **Pin and capture** the duckdb-otlp SHA + DuckDB version in whatever runbook
   adopts this; re-validate the column shapes on every bump (docs are stale).
3. **Standardize the `project` dimension**: operators must set
   `OTEL_RESOURCE_ATTRIBUTES="project=…"` (no spaces in values). Consider a
   Steeplechase-side default/enrichment so the slice is never empty.
4. **Interplay with the #4 semconv mapper:** keep it **off** for this workflow —
   it would rename `claude_code.token.usage` and the attribute keys, breaking
   these queries. If both are wanted, the analytics query must target the
   *mapped* names.
5. The verdict is **not** "build custom," so no requirements-gap follow-up issue
   is needed — but the embedded-sink rejection is now empirically reconfirmed.
