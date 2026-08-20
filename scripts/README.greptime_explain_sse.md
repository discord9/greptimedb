# GreptimeDB `EXPLAIN ANALYZE` SSE client

`scripts/greptime_explain_sse.py` is a Python 3.9+ client with no third-party
packages. It consumes the experimental HTTP SSE stream and writes both a live,
human-readable operator timeline and optional raw JSONL evidence.

## Enable the API

On the **frontend**, enable the feature in the HTTP section (the option is
experimental):

```toml
[http]
addr = "127.0.0.1:4000"
experimental_enable_explain_analyze_stream = true
```

A frontend restart may be required for the configuration change to take
 effect. The client posts to
`/v1/sql/analyze/stream?snapshot_interval_ms=<u64>` with an URL-encoded `sql`
form field and `Accept: text/event-stream`.

Only one explicit `EXPLAIN ANALYZE VERBOSE <query>` statement is accepted. The
optional `FORMAT JSON` spelling is accepted as well. A normal `SELECT` is not
rewritten by this client. The server remains responsible for SQL parsing and
validation.

## Examples

Plain HTTP without authentication on a trusted network (the base URL is
expanded to the stream endpoint):

```console
python3 scripts/greptime_explain_sse.py \
  --url http://frontend:4000 \
  --sql 'EXPLAIN ANALYZE VERBOSE SELECT count(*) FROM demo.metrics' \
  --output evidence.jsonl
```

With a bearer token over HTTPS (do not put the token in support tickets or
shell history when that is not appropriate):

```console
python3 scripts/greptime_explain_sse.py \
  --url https://frontend.example:443/v1/sql/analyze/stream \
  --header 'Authorization: Bearer REDACTED_TOKEN' \
  --sql 'EXPLAIN ANALYZE VERBOSE FORMAT JSON SELECT * FROM demo.metrics LIMIT 10' \
  --snapshot-interval-ms 1000 --output evidence.jsonl
```

HTTPS with a customer CA bundle:

```console
python3 scripts/greptime_explain_sse.py \
  --url https://frontend.example:4000 \
  --ca-file /etc/ssl/customer/frontend-ca.pem \
  --sql 'EXPLAIN ANALYZE VERBOSE SELECT sum(value) FROM demo.metrics'
```

Use `--insecure` only for a deliberately untrusted test endpoint; it cannot be
combined with `--ca-file`. Redirects are never followed; use the final HTTPS
endpoint directly. `--snapshot-interval-ms` is a requested interval in the
inclusive range 1000..60000 ms. The server may automatically increase the
cadence to 10 or 30 seconds for large snapshots. `--timeout` is the
socket/read inactivity timeout, not a total query deadline. Its default is 660
seconds. `--quiet` suppresses the live diagnostic lines but does not suppress
errors or evidence output.

The optional JSONL output contains every complete received server payload and
terminal output. Treat it as sensitive evidence: redact it before sharing.
On platforms with POSIX permissions it is created owner-only (`0600`); it is
never printed automatically.

## Events and interpreting a run

* `metrics` is a best-effort **complete snapshot**, not a server delta. The
  client prints `METRICS` and a `SNAPSHOT_DIFF` based on canonical JSON
  comparison with the preceding snapshot. The displayed operator fields are
  intentionally schema-tolerant; the service currently emits nested JSON
  metrics with fields such as `name`, `output_rows`, `elapsed_compute`, and
  `metrics`.
* `final` is terminal and exits successfully. It includes `partial`, the
  metrics count, and the server-reported total elapsed time.
* `error` and `canceled` are terminal and exit nonzero. `canceled` may not be
  delivered if the HTTP client disconnects; the server documents cancellation
  as best effort.
* Unknown event names are displayed as `EVENT` and do not fail the run.
* EOF without a terminal event is a failed run and is reported as `EOF without
  terminal event`. If the client timeout expires, it reports the last event
  and leaves the already flushed JSONL records, including the latest snapshot.

A long period with no `metrics` can mean planning, stream registration, or a
blocked execution path. If metrics continue changing, inspect the operators
identified by the snapshot diffs. This endpoint is **not a profiler**: its
output alone cannot prove that CPU, Flight, or `flat_merge` is the root cause.

For support, collect at minimum: a redacted SQL statement; the command with
bearer tokens and other secrets removed; the JSONL evidence; frontend and
datanode logs for the specified time window; and the run's duration.
