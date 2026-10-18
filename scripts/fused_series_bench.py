#!/usr/bin/env python3
# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""End-to-end benchmark driver for the fused native-PromQL range-function path.

Reproduces the measurements in `docs/fused-series-report.md`: one seeded table, one
standalone server per arm, the same four query shapes run against each arm, and the
per-iteration timings plus the fused-hit counts written to JSON.

Three subcommands, so seeding, measuring and checking stay separable:

    scripts/fused_series_bench.py seed --data-home DIR
    scripts/fused_series_bench.py run --data-home DIR --bin PATH --arm off \
        --out FILE --log FILE --results DIR
    scripts/fused_series_bench.py run --data-home DIR --bin PATH --arm on \
        --partial on --out FILE --log FILE
    scripts/fused_series_bench.py compare DIR/cC-r1-L-C.json DIR/cC-r1-F-C.json

`seed` writes the fixture once. `run` starts one server over that data directory, runs
every case, and writes a JSON record; with `--results` it also writes the complete
correctness-pass result of each case, which `compare` then checks across arms, field by
field and outside the timing loop. An arm is one `(binary, query flag, partial-SUM switch)`
triple, and the three arms of `ARMS` come out of one binary started three ways:

| arm | `experimental_native_promql` | `GREPTIME_FUSED_PARTIAL_SUM` |
| --- | --- | --- |
| `off` | `false` | (irrelevant) |
| `on` | `true` | `0` |
| `sum` | `true` | `1` |

`--arm {off,on}` picks the native flag and `--partial {on,off}` the environment variable, which
is read by the server process and is not a config option; the record's `arm` is derived from
those two flags, so a record can never claim an arm it was not measured as. The driver does not
build binaries, so the caller decides which commit each arm is, and a crossed order
(L->N->F, N->F->L, F->L->N) is just one invocation per arm.

The `sum` arm needs a server that reads `GREPTIME_FUSED_PARTIAL_SUM` and logs
`Fused series selection succeeded: partial sum of ...`. Against a binary without that half the
`sum` arm fails at its first correctness pass -- one expected partial sum, none logged -- rather
than being timed as a second `on` arm. Nothing here builds or verifies the server side.

## Fixture, exactly as measured

The values below are what `docs/fused-series-report.md` reports; changing any of them
invalidates a comparison against that report. They are what the seeded
`~/.cache/fused-scale/data-seed` directory holds, and `seed` refuses to add to a
directory that already holds a different number of rows.

| Property | Value |
| --- | --- |
| Table | `vmbench`, `ENGINE=mito` |
| Tags (primary key, in order) | `host`, `instance`, `job`, `l1` |
| Field | `metric_value` DOUBLE |
| Time index | `ts` TIMESTAMP(3) |
| Series | 400: `host=h0..h399`, `instance=i0..i3` (100 hosts each), `job=j`, `l1=l` |
| Samples per series | 2 880 |
| Sample interval | 15 s |
| Seed anchor `START_MS` | `1700000000000` ms (no sample sits here) |
| First / last sample | `1700000015000` / `1700043200000` ms |
| Span | 43 185 s (11 h 59 m 45 s) |
| Sample k (0-based) | at `FIRST_SAMPLE_MS + k * 15000` |
| Value | `(k + 1) * 15 * (host + 1)`, strictly rising |
| Counter resets | **0** (verified: every step is +15 x (host + 1)) |
| NULL values | **0** |
| Rows | 400 x 2 880 = 1 152 000 |
| Storage partitions | 20 (mito default, measured on the seeded directory) |

A strictly rising value is a deliberate choice: a `rate` over it needs no counter
correction, so any cost that `rate` shows is the fixed per-window work plus whatever the
kernel spends looking for resets that are not there. That is exactly the cost this work
removes, and it is why a regressing shape cannot be blamed on reset arithmetic. A fixture
with resets is covered by the unit and integration tests instead, where the expected value
can be computed by hand.

## Two ranges, named apart

The **data range** (`FIRST_SAMPLE_MS .. LAST_SAMPLE_MS`) and the **query range**
(`QUERY_START_S .. QUERY_END_S`, in seconds, which is what `TQL EVAL` takes) are
different things and one interval apart on purpose:

- the first row is at `START_MS + 1 interval`, not at `START_MS`, so the fixture keeps
  `SAMPLE_COUNT` samples per series *inside* the query range;
- the query starts at `START_MS` (one interval before the first row), so the first grid
  point of every case has nothing behind its window and returns NaN, which the response
  drops. That is how the recorded measurements were taken, so it is kept.

`seed` and `run` both read `count(*)`, `min(ts)` and `max(ts)` back from the server and
fail unless they equal `ROW_COUNT`, `FIRST_SAMPLE_MS` and `LAST_SAMPLE_MS`.

## Cases

Every case is `TQL EVAL (start, end, '<step>') sum by (instance) (<expr>)` over the
seeded span, with the window given as a PromQL range in **seconds**.

| Name | Span | Step | Window | Grid points | Purpose |
| --- | --- | --- | --- | ---: | --- |
| `S` | 30 min | 1 s | 5 min | 1 801 | fine step, narrow window, short span |
| `G` | 2 h | 1 s | 5 min | 7 201 | fine step, narrow window, long span |
| `W` | 2 h | 1 s | 1 h | 7 201 | fine step, wide window |
| `C` | 2 h | 1 min | 5 min | 121 | the original business shape |
| `control` | 2 h | 1 s | 5 min | 7 201 | `max_over_time`, which never fuses |

`S` and `G` differ only in span, so the pair separates "more grid points" from "more
samples per window"; `G` and `W` differ only in window, so the pair isolates the window
width. `C` is the shape the original query actually has.

## What the record holds

Per case, per iteration: wall-clock milliseconds, output row count, response bytes, and the
number of `Fused series selection succeeded` lines the server logged **during that
iteration**, read from the byte offset the log had when the query started — plus
`partial_hits`, how many of those lines were the partial-SUM variant. Both markers share the
`selection succeeded` prefix, so the total count keeps meaning the same thing now that a third
arm logs a longer line. A correctness pass runs before the warmup, its result is what
`--results` saves, and its hit counts are recorded apart from the warmup and timed ones. The
record names the arm (`arm`, always one of `ARMS`, derived from the flags rather than
supplied by the caller) and the partial flag (`partial`) it ran with.

Every execution — correctness, every warmup, every timed iteration — is checked against an
explicit expected hit count derived from the case definition and the arm: `fuses: true` means
exactly one selection on the `on` and `sum` arms and zero on `off`, of which the `sum` arm must
report exactly one partial sum and the `on` arm none; `fuses: false` means zero of either,
always. A mismatch stops the run with the case, the phase, the expected counts and the observed
ones. Counting only the correctness pass let a warmup or a timed iteration quietly fall back to
the ordinary plan while the timings still looked plausible, and a `sum` arm that logged a plain
selection would be a `sum` run measuring the `on` arm.

## Comparing two arms

`--results DIR` makes `run` save the complete correctness-pass result of each case as
`<record stem>-<case>.json`: the column schema, every row, the label tuple, the timestamp
and the value. Only the correctness pass is saved, never a timed iteration, so the saved
file is never a fragment of a measurement. `compare` then reads the saved results of one
case for two or more arms and checks, in this order, reporting the first difference:

1. schema: column names and data types, in order;
2. labels: the full label tuple of every row;
3. timestamps: the `ts` of every row;
4. row count (an explicit check of its own — two lists of different length are never
   zipped, so a missing row can never hide behind a matching prefix);
5. values: `abs(a - b) <= 1e-9 * max(1, abs(b))`, with NaN matching NaN, infinities
   having to match by sign, and finite-vs-non-finite treated as a difference.

Which column is which is read from the saved schema rather than assumed: a
`Timestamp*` column is the timestamp, a numeric column is a value, and every other
column is a label. A result with no timestamp or no value column is refused, because
comparing it would check nothing.

## Traps this driver encodes

- `TQL EVAL (start, end, step)` takes its start and end in **seconds**, like the PromQL
  API, while the timestamps it returns are in **milliseconds** and `INSERT` literals are
  milliseconds. Passing milliseconds to `EVAL` asks for a range about 50 000 years in the
  future: every query returns zero rows with a valid schema, and `SELECT count(*)` still
  looks healthy. See `QUERY_START_S`, which is the only place the conversion happens.
- `region`, `zone` and `mode` are SQL reserved words and cannot be column names.
- `POST /v1/sql` needs form encoding; a JSON body is rejected with 415.
- Two servers need disjoint ports (`--http-addr`, `--grpc-bind-addr`, `--mysql-addr`,
  `--postgres-addr`), or they collide on the defaults. The defaults below sit inside the
  15000-15199 band this repository's benchmark runs are allowed to bind; pass `--http`
  and friends to move them.
"""

import argparse
import json
import math
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

TABLE = "vmbench"
TAG_COLUMNS = ["host", "instance", "job", "l1"]
FIELD = "metric_value"
TIME = "ts"

# The seeded fixture, exactly as measured.
HOSTS = 400
SAMPLE_COUNT = 2_880
SAMPLE_INTERVAL_SECS = 15
# The seed anchor. It is *not* the first sample: sample k (0-based) is written at
# `FIRST_SAMPLE_MS + k * SAMPLE_INTERVAL_MS`, so the anchor is exactly one interval
# before the first row.
START_MS = 1_700_000_000_000

SAMPLE_INTERVAL_MS = SAMPLE_INTERVAL_SECS * 1000
ROW_COUNT = HOSTS * SAMPLE_COUNT

# The data range, in milliseconds: where the seeded rows actually sit.
FIRST_SAMPLE_MS = START_MS + SAMPLE_INTERVAL_MS
LAST_SAMPLE_MS = START_MS + SAMPLE_COUNT * SAMPLE_INTERVAL_MS

# The query range, in seconds, which is what `TQL EVAL` takes. It deliberately starts at
# the anchor rather than at the first sample: the first grid point therefore has no row
# inside its window and returns NaN, which is how the recorded measurements were taken.
# `QUERY_END_S` is the end of the longest case; every case's own end comes from
# `case_window()`. Naming the two ranges apart is the point: `START_MS` is a millisecond
# data anchor and must never be handed to `EVAL`, and `LAST_SAMPLE_MS` is a millisecond
# data bound that must never be mistaken for the grid end.
QUERY_START_S = START_MS // 1000

# The measured cases. `step` and the window are PromQL arguments, so they are seconds.
CASES = [
    {
        "name": "S",
        "expr": "rate({table}[5m])",
        "span_minutes": 30,
        "step": "1s",
        "window_seconds": 300,
        "fuses": True,
        "pushes_partial": True,
    },
    {
        "name": "G",
        "expr": "rate({table}[5m])",
        "span_minutes": 120,
        "step": "1s",
        "window_seconds": 300,
        "fuses": True,
        "pushes_partial": True,
    },
    {
        "name": "W",
        "expr": "rate({table}[1h])",
        "span_minutes": 120,
        "step": "1s",
        "window_seconds": 3_600,
        "fuses": True,
        "pushes_partial": True,
    },
    {
        "name": "C",
        "expr": "rate({table}[5m])",
        "span_minutes": 120,
        "step": "1m",
        "window_seconds": 300,
        "fuses": True,
        "pushes_partial": True,
    },
    {
        "name": "control",
        "expr": "max_over_time({table}[5m])",
        "span_minutes": 120,
        "step": "1s",
        "window_seconds": 300,
        "fuses": False,
        "pushes_partial": False,
    },
]

MAX_SPAN_MINUTES = max(case["span_minutes"] for case in CASES)
QUERY_END_S = QUERY_START_S + MAX_SPAN_MINUTES * 60

# The types `compare` treats as values; every other non-timestamp column is a label.
NUMERIC_TYPES = {
    "Float64",
    "Float32",
    "Int64",
    "Int32",
    "Int16",
    "Int8",
    "UInt64",
    "UInt32",
    "UInt16",
    "UInt8",
}


def log(message):
    print(message, flush=True)


def check_ranges():
    """Fails unless the range constants still describe the measured fixture.

    Cheap, runs before any server starts, and catches the one edit that would silently
    invalidate a comparison: mixing up the data range and the query range.
    """
    if (FIRST_SAMPLE_MS, LAST_SAMPLE_MS) != (1_700_000_015_000, 1_700_043_200_000):
        raise SystemExit(
            f"the data range is {FIRST_SAMPLE_MS}..{LAST_SAMPLE_MS} ms, but the measured "
            "fixture's first and last samples are 1700000015000..1700043200000 ms"
        )
    if LAST_SAMPLE_MS - FIRST_SAMPLE_MS != (SAMPLE_COUNT - 1) * SAMPLE_INTERVAL_MS:
        raise SystemExit(
            f"the data range spans {LAST_SAMPLE_MS - FIRST_SAMPLE_MS} ms, not the "
            f"{(SAMPLE_COUNT - 1) * SAMPLE_INTERVAL_MS} ms of {SAMPLE_COUNT} samples at "
            f"{SAMPLE_INTERVAL_MS} ms"
        )
    if QUERY_START_S * 1000 != START_MS:
        raise SystemExit(
            f"the query range starts at {QUERY_START_S}s, which is not the seed anchor "
            f"{START_MS} ms rounded down to seconds"
        )
    if QUERY_END_S * 1000 > LAST_SAMPLE_MS:
        raise SystemExit(
            f"the query range ends at {QUERY_END_S}s, after the data range's last sample "
            f"{LAST_SAMPLE_MS} ms; a case would ask for timestamps the fixture does not have"
        )
    for case in CASES:
        _, end = case_window(case)
        if end > QUERY_END_S:
            raise SystemExit(
                f"case {case['name']} ends at {end}s, past QUERY_END_S ({QUERY_END_S}s)"
            )


def check_fixture(where, count, first_ts, last_ts):
    """Fails unless the server's own count/min/max equal the fixture constants."""
    if count != ROW_COUNT:
        raise SystemExit(
            f"{where}: {TABLE} holds {count} rows, but the fixture has "
            f"{ROW_COUNT} ({HOSTS} series x {SAMPLE_COUNT} samples)"
        )
    if first_ts != FIRST_SAMPLE_MS or last_ts != LAST_SAMPLE_MS:
        raise SystemExit(
            f"{where}: {TABLE} spans {first_ts}..{last_ts} ms, but the fixture's first and "
            f"last samples are {FIRST_SAMPLE_MS}..{LAST_SAMPLE_MS} ms"
        )


def arm_overlay(native, partial):
    """The environment overlay one arm adds to the server process, or `None` for no overlay.

    `GREPTIME_FUSED_PARTIAL_SUM` is read by the server, not a config option, so it travels in
    the child's environment. A flag-off arm gets no overlay at all rather than `{"0"}`-style
    padding: `partial=None` has to leave the child's environment exactly as it is today.
    """
    if not native:
        return None
    return {"GREPTIME_FUSED_PARTIAL_SUM": "1" if partial else "0"}


# The three arms this harness can run, as `(native flag, environment overlay)`. `sum` is the
# same binary as `on` with the partial-SUM step left on, which is why `sum / on` isolates that
# step alone: same code, same config, one environment variable apart.
ARMS = {
    "off": (False, arm_overlay(False, False)),
    "on": (True, arm_overlay(True, False)),
    "sum": (True, arm_overlay(True, True)),
}


def expected_hits(fuses, arm):
    """How many fused selections one execution of a fusing (or non-fusing) case must log.

    `arm` is one of `ARMS`. Both flag-on arms fuse, so both log exactly one; the partial-SUM
    switch does not change the prefix the selection line starts with. Taken as the `fuses` flag
    rather than the case dict so the scale script can ask without building one.
    """
    if not fuses:
        return 0
    return 0 if arm == "off" else 1


def expected_partial_hits(pushes_partial, arm):
    """How many of `expected_hits` must be the `partial sum` variant: only the `sum` arm.

    `pushes_partial` is not `fuses`: a query with no aggregation above the fused node still
    fuses, and still cannot push a partial SUM, because there is no aggregate to fold into the
    scan.  Asking this of the fusion flag alone would demand a line the planner can never log.
    """
    if not pushes_partial:
        return 0
    return 1 if arm == "sum" else 0


def check_hits(case, arm, phase, actual, partial):
    """Fails unless one execution logged exactly the expected selections and partial sums.

    `partial` is required rather than defaulted: a caller that does not sample
    `partial_hits_since()` would otherwise assert, silently, that the arm it is measuring never
    runs the partial SUM -- which is true only of the drivers that have no `sum` arm.
    """
    expected = expected_hits(case["fuses"], arm)
    expected_partial = expected_partial_hits(case["pushes_partial"], arm)
    if partial > actual:
        raise SystemExit(
            f"case {case['name']} phase {phase}: {partial} partial-sum line(s) but only "
            f"{actual} fused selection line(s); the counts are not about the same executions"
        )
    if actual != expected or partial != expected_partial:
        raise SystemExit(
            f"case {case['name']} phase {phase}: expected {expected} fused selection(s) on "
            f"the {arm} arm, {expected_partial} of them partial sums; the server logged "
            f"{actual} ({partial} partial sums)"
        )
    return expected


class Server:
    """One standalone server over a data directory, on its own port set."""

    def __init__(
        self, binary, data_home, http, grpc, mysql, postgres, native, log_path, partial=None
    ):
        self.binary = binary
        self.data_home = data_home
        self.native = native
        # The environment overlay this arm adds to the child: `None` (or an empty mapping) means
        # no `env=` is passed at all, so the server inherits this process's environment exactly
        # as it did before the partial-SUM arm existed.
        self.partial = dict(partial or {})
        self.http = http
        self.grpc = grpc
        self.mysql = mysql
        self.postgres = postgres
        self.log_path = log_path
        # The log filter the server runs with. `debug` is what the fused-hit count needs,
        # because the selection lines are logged at debug. It is also expensive: at `debug`
        # datafusion formats the whole physical plan for a log record on every planning pass,
        # which was 4.6% of CPU samples in a profile of case W. Set this to
        # `"info,query::datafusion::native_promql::fused=debug"` to keep the hit count and drop
        # everything else; `level` is parsed as tracing `Targets`, so directives work.
        self.log_level = "debug"
        self.process = None
        self.log_file = None
        # Where the log was when the query started, so hits can be counted per query.
        self.log_offset = 0

    def __enter__(self):
        config = os.path.join(
            self.data_home,
            "..",
            f"fused-bench-arm-{int(self.native)}-{int(bool(self.partial))}.toml",
        )
        with open(config, "w") as handle:
            handle.write(
                'mode = "standalone"\n\n'
                "[logging]\n"
                f'level = "{self.log_level}"\n\n'
                "[query]\n"
                f"experimental_native_promql = {str(self.native).lower()}\n"
            )
        self.log_file = open(self.log_path, "w")
        # Merged over `os.environ` rather than replacing it, so the child keeps PATH and the
        # rest of what it needs to start; an arm without an overlay passes no `env=` at all.
        env = {**os.environ, **self.partial} if self.partial else {}
        self.process = subprocess.Popen(
            [
                self.binary,
                "standalone",
                "start",
                "--config-file",
                config,
                "--data-home",
                self.data_home,
                "--http-addr",
                f"127.0.0.1:{self.http}",
                "--grpc-bind-addr",
                f"127.0.0.1:{self.grpc}",
                "--mysql-addr",
                f"127.0.0.1:{self.mysql}",
                "--postgres-addr",
                f"127.0.0.1:{self.postgres}",
            ],
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            **({"env": env} if env else {}),
        )
        deadline = time.time() + 600
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"the server exited early; see {self.log_path}")
            try:
                self.sql("SELECT 1", timeout=5)
                return self
            except Exception:
                time.sleep(1)
        raise RuntimeError(f"the server did not become ready; see {self.log_path}")

    def __exit__(self, *exc):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=120)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.log_file is not None:
            self.log_file.close()

    def sql(self, statement, timeout=3_600):
        body = urllib.parse.urlencode({"sql": statement}).encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.http}/v1/sql",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
        return json.loads(raw.decode()), len(raw)

    def rows(self, statement, timeout=3_600):
        payload, _ = self.sql(statement, timeout=timeout)
        return flatten(payload)[1]

    def cpu_seconds(self):
        """Total CPU seconds this server has used on every thread so far.

        Wall-clock alone cannot separate "does less work" from "does the same work with
        more parallelism", and the two arms differ in exactly that way. Reading the
        process counters makes the distinction measurable instead of arguable.
        """
        with open(f"/proc/{self.process.pid}/stat") as handle:
            # The comm field is parenthesised and may contain spaces, so split after it.
            fields = handle.read().rsplit(") ", 1)[1].split()
        ticks = fields[11:13]  # utime, stime
        return sum(int(tick) for tick in ticks) / os.sysconf("SC_CLK_TCK")

    def begin_query(self):
        """Records where the log ends, so the next query's hits can be counted alone."""
        self.log_offset = os.path.getsize(self.log_path) if os.path.exists(self.log_path) else 0

    def _tail_since(self):
        """The log bytes written since `begin_query`."""
        if not os.path.exists(self.log_path):
            return b""
        with open(self.log_path, "rb") as handle:
            handle.seek(self.log_offset)
            return handle.read()

    def hits_since(self):
        """The number of fused selections logged since `begin_query`.

        The partial-SUM arm logs `Fused series selection succeeded: partial sum of ...`, which
        starts with the same prefix as the plain one, so this count keeps its meaning: every
        fused selection of either kind.
        """
        return self._tail_since().count(b"Fused series selection succeeded")

    def partial_hits_since(self):
        """How many of those selections, line by line, were the partial-SUM variant.

        Counted per line rather than as a substring: a log record that quotes the selection
        message -- a plan dump, a retry notice -- would inflate a substring count past
        `hits_since()`, which is a nonsense ratio to hold a run to. Reading the same tail as
        `hits_since()` keeps the two counts about the same bytes.
        """
        marker = b"selection succeeded: partial sum"
        return sum(1 for line in self._tail_since().splitlines() if marker in line)


def flatten(payload):
    """`(schema, rows)` of a SQL response, with every block folded into one list.

    A TQL response is a single block; folding keeps a multi-block response comparable
    instead of silently comparing only the first block.
    """
    schema = None
    rows = []
    for block in payload.get("output", []):
        records = block.get("records")
        if not records:
            continue
        columns = records["schema"]["column_schemas"]
        if schema is None:
            schema = columns
        elif columns != schema:
            raise SystemExit(
                "the response holds blocks with different schemas, so its rows cannot be "
                "compared as one result"
            )
        rows.extend(records["rows"])
    return schema or [], rows


def create_table(server):
    columns = ", ".join(f'"{tag}" STRING NULL' for tag in TAG_COLUMNS)
    primary_key = ", ".join(f'"{tag}"' for tag in TAG_COLUMNS)
    server.sql(
        f'CREATE TABLE IF NOT EXISTS "{TABLE}" ('
        f"{columns}, "
        f'"{FIELD}" DOUBLE NULL, '
        f'"{TIME}" TIMESTAMP(3) NOT NULL, '
        f'TIME INDEX ("{TIME}"), '
        f"PRIMARY KEY ({primary_key})"
        ") ENGINE=mito"
    )


def seed(data_home, binary, http, grpc, mysql, postgres):
    """Writes the fixture once; safe to re-run because it checks the row count first."""
    check_ranges()
    os.makedirs(data_home, exist_ok=True)
    log_path = os.path.join(data_home, "..", "fused-bench-seed.log")
    with Server(binary, data_home, http, grpc, mysql, postgres, False, log_path) as server:
        create_table(server)
        count, first_ts, last_ts = server.rows(
            f'SELECT count(*), min("{TIME}"), max("{TIME}") FROM "{TABLE}"'
        )[0]
        if count == ROW_COUNT:
            check_fixture("seed", count, first_ts, last_ts)
            log(f"the fixture is already seeded: {count} rows at {first_ts}..{last_ts} ms")
            return
        if count:
            raise SystemExit(
                f"{TABLE} holds {count} rows, not the {ROW_COUNT} of the fixture; use a "
                "fresh data directory instead of mixing shapes"
            )
        log(f"seeding {ROW_COUNT} rows into {data_home}")
        # One INSERT per host keeps each statement small enough for the HTTP endpoint while
        # writing every sample of that series in a single batch.
        for host in range(HOSTS):
            started = time.time()
            values = []
            for index in range(SAMPLE_COUNT):
                timestamp = FIRST_SAMPLE_MS + index * SAMPLE_INTERVAL_MS
                # Strictly rising, and different per host so a wrongly grouped series cannot hide.
                value = (index + 1) * SAMPLE_INTERVAL_SECS * (host + 1)
                values.append(
                    f"('h{host}', 'i{host % 4}', 'j', 'l', {value}, {timestamp})"
                )
            server.sql(
                f'INSERT INTO "{TABLE}" '
                f'("host", "instance", "job", "l1", "{FIELD}", "{TIME}") VALUES '
                + ", ".join(values),
                # A single 2 880-row insert of a series is the slowest statement here.
                timeout=3_600,
            )
            if host % 25 == 0 or host == HOSTS - 1:
                log(f"  {host + 1}/{HOSTS} series ({time.time() - started:.1f}s each)")
        count, first_ts, last_ts = server.rows(
            f'SELECT count(*), min("{TIME}"), max("{TIME}") FROM "{TABLE}"'
        )[0]
        check_fixture("seed", count, first_ts, last_ts)
        log(f"seeded {count} rows at {first_ts}..{last_ts} ms")


def case_window(case):
    """The `(start, end)` of one case in seconds, which is what `TQL EVAL` takes."""
    return QUERY_START_S, QUERY_START_S + case["span_minutes"] * 60


def tql(case, aggregate=True):
    expr = case["expr"].format(table=TABLE)
    if aggregate:
        expr = f"sum by (instance) ({expr})"
    start, end = case_window(case)
    return f"TQL EVAL ({start}, {end}, '{case['step']}') {expr}"


def save_result(directory, stem, case, arm, commit, statement, schema, rows):
    """Saves one correctness-pass result, never overwriting an earlier one."""
    os.makedirs(directory, exist_ok=True)
    base = os.path.join(directory, f"{stem}-{case['name']}")
    path = f"{base}.json"
    suffix = 2
    while os.path.exists(path):
        path = f"{base}-{suffix}.json"
        suffix += 1
    with open(path, "w") as handle:
        json.dump(
            {
                "case": case["name"],
                "arm": arm,
                "commit": commit,
                "tql": statement,
                "schema": schema,
                "row_count": len(rows),
                "rows": rows,
            },
            handle,
        )
    return path


def value_mismatch(left, right):
    """`None` if the two value cells match, otherwise a short reason."""
    if left is None or right is None:
        return None if left is None and right is None else f"{left!r} vs {right!r} (one is NULL)"
    if isinstance(left, bool) or isinstance(right, bool):
        return None if left == right else f"{left!r} vs {right!r}"
    if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
        return None if left == right else f"{left!r} vs {right!r}"
    left_nan, right_nan = math.isnan(left), math.isnan(right)
    if left_nan or right_nan:
        if left_nan and right_nan:
            return None
        return f"{left!r} vs {right!r} (only one side is NaN)"
    left_inf, right_inf = math.isinf(left), math.isinf(right)
    if left_inf or right_inf:
        if not (left_inf and right_inf):
            return f"{left!r} vs {right!r} (one side is infinite, the other finite)"
        if (left > 0) != (right > 0):
            return f"{left!r} vs {right!r} (opposite infinities)"
        return None
    tolerance = 1e-9 * max(1.0, abs(right))
    if abs(left - right) > tolerance:
        return (
            f"{left!r} vs {right!r} (off by {abs(left - right):.3e}, "
            f"tolerance {tolerance:.3e})"
        )
    return None


def classify(schema):
    """Which columns are labels, which are the timestamp, which are values."""
    labels, times, values = [], [], []
    for index, column in enumerate(schema):
        if column["data_type"].startswith("Timestamp"):
            times.append(index)
        elif column["data_type"] in NUMERIC_TYPES:
            values.append(index)
        else:
            labels.append(index)
    return labels, times, values


def describe(result):
    """An arm's identity: the arm name from the record, plus the file it came from.

    `arm` is the arm the record ran (`off`, `on`, `sum`, or a caller's own label), so a file
    saved by the `sum` arm says `sum` and not `on`.
    """
    return f"{result['stem']} [arm {result['arm']}, {result.get('commit', 'unknown')}]"


def first_difference(left_rows, right_rows, columns):
    """Index of the first row whose `columns` cells differ, or `None`.

    Only the common prefix is looked at: two lists of different length are never zipped,
    so the length difference is left to its own check, which reports it explicitly.
    """
    for index in range(min(len(left_rows), len(right_rows))):
        left = tuple(left_rows[index][column] for column in columns)
        right = tuple(right_rows[index][column] for column in columns)
        if left != right:
            return index, left, right
    return None


def series_of(rows, labels, times, values):
    """Rows grouped by label tuple, each group ordered by its timestamps.

    The group key is the whole label tuple, so two label sets are never mixed. Ordering each
    group by timestamp is what lets two arms whose rows are interleaved differently still be
    compared sample by sample: the sample sequence of one series is a property of the answer,
    while the order the series themselves arrive in is not (see `compare_pair`).
    """
    grouped = {}
    for row in rows:
        key = tuple(row[column] for column in labels)
        grouped.setdefault(key, []).append(row)
    for key, group in grouped.items():
        group.sort(key=lambda row: tuple(row[column] for column in times))
    return grouped


def compare_pair(baseline, other):
    """Checks `other` against `baseline` sample by sample, stopping at the first difference.

    Compared, in this order: the schema, the row count, the set of series and how many rows each
    one has, then every series' own timestamp/value sequence. Cross-series row order is
    deliberately *not* a criterion. A query with no aggregation above the fused node has no
    `Sort` in its plan -- `FilterExec -> FusedSeriesExec -> SeriesScanExec`, twenty partitions
    merged -- so the order its series arrive in is not guaranteed to either arm, and demanding
    it would fail two correct answers. What every PromQL answer does guarantee is which samples
    each series holds, and that is what is checked.

    Whether the full row order also matched is reported rather than required, so a difference in
    it is recorded instead of hidden.
    """
    case = baseline["case"]
    left, right = describe(baseline), describe(other)

    # 1. schema: names and data types, in order.
    left_schema, right_schema = baseline["schema"], other["schema"]
    if len(left_schema) != len(right_schema):
        raise SystemExit(
            f"case {case}: schema differs: {left} has {len(left_schema)} columns, "
            f"{right} has {len(right_schema)}"
        )
    for index, (left_column, right_column) in enumerate(zip(left_schema, right_schema)):
        if left_column != right_column:
            raise SystemExit(
                f"case {case}: schema differs at column {index}: {left} has "
                f"{left_column['name']!r} {left_column['data_type']}, {right} has "
                f"{right_column['name']!r} {right_column['data_type']}"
            )
    labels, times, values = classify(left_schema)
    if not times or not values:
        raise SystemExit(
            f"case {case}: the result has no timestamp or no value column "
            f"({[column['data_type'] for column in left_schema]}); nothing to compare"
        )

    # 2. row count, as its own failure rather than a truncated comparison.
    if len(baseline["rows"]) != len(other["rows"]):
        raise SystemExit(
            f"case {case}: row count differs: {left} has {len(baseline['rows'])} rows, "
            f"{right} has {len(other['rows'])}"
        )

    # 3. the same series, each with the same number of rows. A series present on one side only,
    #    or holding a different number of samples, is a result difference and not an ordering one.
    left_series = series_of(baseline["rows"], labels, times, values)
    right_series = series_of(other["rows"], labels, times, values)
    if len(left_series) != len(right_series):
        raise SystemExit(
            f"case {case}: series count differs: {left} has {len(left_series)} series, "
            f"{right} has {len(right_series)}"
        )
    for key in sorted(left_series, key=repr):
        if key not in right_series:
            raise SystemExit(
                f"case {case}: series {key} is in {left} but not in {right}"
            )
        if len(left_series[key]) != len(right_series[key]):
            raise SystemExit(
                f"case {case}: series {key} has {len(left_series[key])} rows in {left} and "
                f"{len(right_series[key])} in {right}"
            )

    # 4. every series' own samples: the timestamp sequence, then the values.
    for key in sorted(left_series, key=repr):
        left_rows, right_rows = left_series[key], right_series[key]
        for position, (left_row, right_row) in enumerate(zip(left_rows, right_rows)):
            left_stamp = tuple(left_row[column] for column in times)
            right_stamp = tuple(right_row[column] for column in times)
            if left_stamp != right_stamp:
                raise SystemExit(
                    f"case {case}: series {key} differs at sample {position}: "
                    f"{left} has {left_stamp}, {right} has {right_stamp}"
                )
            for column in values:
                reason = value_mismatch(left_row[column], right_row[column])
                if reason:
                    raise SystemExit(
                        f"case {case}: series {key} differs at sample {position} "
                        f"(timestamp {left_stamp}) in column "
                        f"{left_schema[column]['name']!r}: {left} has {reason}"
                    )

    # 5. Informational: did the rows also arrive in the same order? Not a criterion, see above.
    same_order = all(
        tuple(left_row[column] for column in labels + times)
        == tuple(right_row[column] for column in labels + times)
        for left_row, right_row in zip(baseline["rows"], other["rows"])
    )
    return labels, times, values, same_order


def compare(args):
    """Checks the saved results of one case across arms."""
    results = []
    for path in args.results:
        with open(path) as handle:
            result = json.load(handle)
        for key in ("case", "arm", "schema", "rows"):
            if key not in result:
                raise SystemExit(
                    f"{path} is not a result file written by `run --results`: no {key!r}"
                )
        stem = os.path.basename(path)
        result["stem"] = stem[: -len(".json")] if stem.endswith(".json") else stem
        results.append(result)

    cases = sorted({result["case"] for result in results})
    if len(cases) != 1:
        raise SystemExit(f"the result files mix cases: {', '.join(cases)}")
    if args.case and args.case != cases[0]:
        raise SystemExit(f"asked to compare case {args.case}, but the files hold case {cases[0]}")
    statements = {result.get("tql") for result in results}
    if len(statements) != 1:
        raise SystemExit(
            "the result files hold different queries, so comparing them would be meaningless:\n  "
            + "\n  ".join(sorted(statements))
        )

    baseline = results[0]
    arms = ", ".join(describe(result) for result in results)
    log(f"case {cases[0]}: {len(results)} arm(s) {arms}, {len(baseline['rows'])} rows")
    for other in results[1:]:
        labels, times, values, same_order = compare_pair(baseline, other)
        order = "same row order" if same_order else "different cross-series row order"
        log(
            f"  identical to {describe(baseline)}: {len(baseline['schema'])} columns "
            f"({len(labels)} label(s), {len(times)} timestamp(s), {len(values)} value(s)), "
            f"{len(baseline['rows'])} rows, {order}"
        )
    log(
        f"ok: case {cases[0]} is identical on {len(results)} arm(s): "
        f"{', '.join(result['stem'] for result in results)}"
    )


def run(args):
    """Runs every case against one arm and writes the JSON record."""
    check_ranges()
    native = args.arm == "on"
    partial = args.partial == "on"
    if partial and not native:
        raise SystemExit(
            "--partial on needs the fused path, because the partial SUM is computed inside the "
            "fused node; pass --arm on as well (or leave --partial at its default, off)"
        )
    # The arm name is derived, never supplied: `--arm` and `--partial` already say everything
    # about how this server was started, and a caller-chosen label would let a record claim an
    # arm it was not measured as. A record's `arm` is therefore always one of `ARMS`.
    arm = "sum" if (native and partial) else args.arm
    overlay = arm_overlay(native, partial)
    with Server(
        args.bin,
        args.data_home,
        args.http,
        args.grpc,
        args.mysql,
        args.postgres,
        native,
        args.log,
        partial=overlay,
    ) as server:
        # The fixture is checked before anything is timed, so no measurement can come from
        # a table that is not the one the record claims.
        count, first_ts, last_ts = server.rows(
            f'SELECT count(*), min("{TIME}"), max("{TIME}") FROM "{TABLE}"'
        )[0]
        check_fixture("run", count, first_ts, last_ts)
        log(
            f"fixture: {count} rows, {first_ts}..{last_ts} ms "
            f"(expected {ROW_COUNT} rows, {FIRST_SAMPLE_MS}..{LAST_SAMPLE_MS} ms)"
        )

        record = {
            "arm": arm,
            "partial": args.partial,
            "bin": args.bin,
            "commit": args.commit,
            "build_profile": args.profile,
            "data_home": os.path.abspath(args.data_home),
            "fixture": {
                "table": TABLE,
                "tags": TAG_COLUMNS,
                "series": HOSTS,
                "samples_per_series": SAMPLE_COUNT,
                "sample_interval_seconds": SAMPLE_INTERVAL_SECS,
                "rows": ROW_COUNT,
                "start_ms": START_MS,
                "first_sample_ms": FIRST_SAMPLE_MS,
                "last_sample_ms": LAST_SAMPLE_MS,
                "query_start_s": QUERY_START_S,
                "observed_rows": count,
                "first_ts_ms": first_ts,
                "last_ts_ms": last_ts,
                "nulls": 0,
                "counter_resets": 0,
            },
            "warmup": args.warmup,
            "iterations": args.iterations,
            "cases": [],
        }

        wanted: set[str] = (
            {name.strip() for name in args.cases.split(",") if name.strip()}
            if args.cases
            else set()
        )
        for case in [c for c in CASES if not wanted or c["name"] in wanted]:
            statement = tql(case)
            query_start_s, query_end_s = case_window(case)
            # Correctness first, and its hits recorded apart from the timed ones. Its
            # result is the only one saved: a timed iteration must never be compared,
            # because a fragment of a measurement is not a result.
            server.begin_query()
            correctness_payload, _ = server.sql(statement)
            correctness_hits = server.hits_since()
            correctness_partial_hits = server.partial_hits_since()
            expected = check_hits(
                case, arm, "correctness", correctness_hits, correctness_partial_hits
            )
            schema, correctness_rows = flatten(correctness_payload)
            log(
                f"{case['name']}: {len(correctness_rows)} rows, {correctness_hits} hits, "
                f"correctness, expected {expected} "
                f"({correctness_partial_hits} partial sums, expected "
                f"{expected_partial_hits(case['pushes_partial'], arm)})"
            )
            if args.results:
                saved = save_result(
                    args.results,
                    args.results_stem,
                    case,
                    arm,
                    args.commit,
                    statement,
                    schema,
                    correctness_rows,
                )
                log(f"  saved the correctness result to {saved}")

            # Warmup, then the timed runs, each with its own hit count. The three kinds of hit
            # are kept apart so a total can never be mistaken for a per-iteration count, and
            # every single execution is held to the same expected counts, totals and partials.
            warmup_hits = []
            warmup_partial_hits = []
            for warmup in range(args.warmup):
                server.begin_query()
                server.sql(statement)
                hits = server.hits_since()
                partials = server.partial_hits_since()
                check_hits(
                    case,
                    arm,
                    f"warmup {warmup + 1}/{args.warmup}",
                    hits,
                    partials,
                )
                warmup_hits.append(hits)
                warmup_partial_hits.append(partials)
            timed_hits = []
            timed_partial_hits = []
            for iteration in range(args.iterations):
                server.begin_query()
                cpu_before = server.cpu_seconds()
                started = time.perf_counter()
                payload, size = server.sql(statement)
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                cpu_ms = (server.cpu_seconds() - cpu_before) * 1000.0
                hits = server.hits_since()
                partials = server.partial_hits_since()
                check_hits(
                    case,
                    arm,
                    f"iteration {iteration + 1}/{args.iterations}",
                    hits,
                    partials,
                )
                rows = sum(
                    len(block["records"]["rows"])
                    for block in payload.get("output", [])
                    if block.get("records")
                )
                timed_hits.append(hits)
                timed_partial_hits.append(partials)
                record["cases"].append(
                    {
                        "load1": os.getloadavg()[0],
                        "name": case["name"],
                        "expr": case["expr"],
                        "step": case["step"],
                        "span_minutes": case["span_minutes"],
                        "window_seconds": case["window_seconds"],
                        "tql": statement,
                        "query_start_s": query_start_s,
                        "query_end_s": query_end_s,
                        "rows": rows,
                        "bytes": size,
                        "ms": elapsed_ms,
                        "cpu_ms": cpu_ms,
                        "hits": hits,
                        "partial_hits": partials,
                        "expected_hits": expected,
                        "expected_partial_hits": expected_partial_hits(case["pushes_partial"], arm),
                    }
                )

            timed = [c for c in record["cases"] if c["name"] == case["name"]]
            record.setdefault("hits", []).append(
                {
                    "name": case["name"],
                    "fuses": case["fuses"],
                    "expected": expected,
                    "expected_partial": expected_partial_hits(case["pushes_partial"], arm),
                    "correctness": correctness_hits,
                    "correctness_partial": correctness_partial_hits,
                    "warmup": warmup_hits,
                    "warmup_partial": warmup_partial_hits,
                    "timed": timed_hits,
                    "timed_partial": timed_partial_hits,
                }
            )
            log(
                f"  {case['name']}: {len(timed)} timed runs, {timed[0]['rows']} rows, "
                f"hits correctness={correctness_hits}/{correctness_partial_hits} partial "
                f"warmup={sorted(set(warmup_hits))}/{sorted(set(warmup_partial_hits))} partial "
                f"timed={sorted(set(timed_hits))}/{sorted(set(timed_partial_hits))} partial "
                f"(expected {expected}/{expected_partial_hits(case['pushes_partial'], arm)}), "
                f"min {min(c['ms'] for c in timed):.1f} ms"
            )

        if arm == "off" and any(c["hits"] for c in record["cases"]):
            raise SystemExit("the flag-off arm logged fused selections; it is not the baseline")
        if arm != "sum" and any(c["partial_hits"] for c in record["cases"]):
            raise SystemExit(
                f"the {arm} arm logged partial sums; only the sum arm may, so this record is "
                "not the arm it claims to be"
            )

        with open(args.out, "w") as handle:
            json.dump(record, handle, indent=2)
        log(f"wrote {args.out}")


def self_test():
    """Checks the comparator against a serverless fixture: what it must catch, and must not.

    The comparator was relaxed once -- cross-series row order stopped being a criterion, because
    a query with no aggregation above the fused node has no `Sort` in its plan. A relaxation with
    no test is indistinguishable from a comparator that stopped comparing, so each property the
    comparison is supposed to have is asserted here: reordering is tolerated, and a changed value,
    a changed timestamp, a dropped sample, a dropped series and a changed label are not.
    """
    schema = [
        {"name": "ts", "data_type": "Timestamp(Millisecond, None)"},
        {"name": "value", "data_type": "Float64"},
        {"name": "host", "data_type": "String"},
    ]

    def result(rows):
        return {"case": "selftest", "arm": "x", "stem": "selftest-x", "schema": schema,
                "rows": rows}

    def row(stamp, value, host):
        return [stamp, value, host]

    base_rows = [
        row(1000, 1.0, "a"), row(2000, 2.0, "a"), row(3000, 3.0, "a"),
        row(1000, 10.0, "b"), row(2000, 20.0, "b"), row(3000, 30.0, "b"),
    ]
    baseline = result(base_rows)

    def expect_ok(name, rows, same_order):
        _, _, _, observed = compare_pair(baseline, result(rows))
        if observed != same_order:
            raise SystemExit(
                f"selftest {name}: reported row order {observed}, expected {same_order}"
            )

    def expect_fail(name, rows, want):
        try:
            compare_pair(baseline, result(rows))
        except SystemExit as failure:
            if want not in str(failure):
                raise SystemExit(f"selftest {name}: failed for the wrong reason: {failure}")
            return
        raise SystemExit(f"selftest {name}: the comparator accepted it")

    # Reordering the two series, both fully interleaved and in one block, is not a difference.
    expect_ok("interleaved", [
        row(1000, 1.0, "a"), row(1000, 10.0, "b"), row(2000, 2.0, "a"),
        row(2000, 20.0, "b"), row(3000, 3.0, "a"), row(3000, 30.0, "b"),
    ], False)
    # A series arriving last instead of first is not a difference either.
    expect_ok("series reordered", base_rows[3:] + base_rows[:3], False)
    # Identical input, identical order: reported as such, not as a difference.
    expect_ok("unchanged", list(base_rows), True)

    # Everything below is a real difference and must still be caught.
    expect_fail("value changed", [
        row(1000, 1.0, "a"), row(2000, 99.0, "a"), row(3000, 3.0, "a"),
        row(1000, 10.0, "b"), row(2000, 20.0, "b"), row(3000, 30.0, "b"),
    ], "differs at sample 1")
    expect_fail("timestamp changed", [
        row(1000, 1.0, "a"), row(2500, 2.0, "a"), row(3000, 3.0, "a"),
        row(1000, 10.0, "b"), row(2000, 20.0, "b"), row(3000, 30.0, "b"),
    ], "differs at sample 1")
    # A dropped sample changes the row count, which is reported before anything else.
    expect_fail("sample dropped", [
        row(1000, 1.0, "a"), row(3000, 3.0, "a"),
        row(1000, 10.0, "b"), row(2000, 20.0, "b"), row(3000, 30.0, "b"),
    ], "row count differs")
    # A sample moved from one series to another keeps the total, so only the per-series counts
    # can see it.
    expect_fail("sample moved between series", [
        row(1000, 1.0, "a"), row(2000, 2.0, "a"), row(3000, 3.0, "a"), row(4000, 4.0, "a"),
        row(1000, 10.0, "b"), row(2000, 20.0, "b"),
    ], "has 3 rows in")
    # Dropping a series changes the row count, which is reported before the series set.
    expect_fail("series dropped", base_rows[:3], "row count differs")
    # One series split in two: same row count, but a different set of series.
    expect_fail("series split", [
        row(1000, 1.0, "a"), row(2000, 2.0, "a"), row(3000, 3.0, "a"),
        row(1000, 10.0, "b"), row(2000, 20.0, "b"), row(3000, 30.0, "c"),
    ], "series count differs")
    expect_fail("label changed", [
        row(1000, 1.0, "a"), row(2000, 2.0, "a"), row(3000, 3.0, "a"),
        row(1000, 10.0, "c"), row(2000, 20.0, "c"), row(3000, 30.0, "c"),
    ], "but not in")
    # A duplicate in place of a distinct sample: same row count, same series, still a difference.
    expect_fail("duplicate instead of a sample", [
        row(1000, 1.0, "a"), row(1000, 1.0, "a"), row(3000, 3.0, "a"),
        row(1000, 10.0, "b"), row(2000, 20.0, "b"), row(3000, 30.0, "b"),
    ], "differs at sample 1")
    # Schema is still compared before anything else.
    other = result(list(base_rows))
    other["schema"] = [
        {"name": "ts", "data_type": "Timestamp(Millisecond, None)"},
        {"name": "value", "data_type": "Float64"},
        {"name": "host", "data_type": "Utf8"},
    ]
    try:
        compare_pair(baseline, other)
    except SystemExit as failure:
        if "schema differs" not in str(failure):
            raise SystemExit(f"selftest schema: failed for the wrong reason: {failure}")
    else:
        raise SystemExit("selftest schema: the comparator accepted a changed schema")

    print("selftest: reordering tolerated; value, timestamp, dropped sample, dropped series, "
          "changed label, duplicate and schema differences all still caught")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--data-home", required=True, help="the standalone data directory")
    common.add_argument("--bin", required=True, help="the greptime binary to run")
    common.add_argument("--http", type=int, default=15090)
    common.add_argument("--grpc", type=int, default=15092)
    common.add_argument("--mysql", type=int, default=15094)
    common.add_argument("--postgres", type=int, default=15096)

    seed_parser = sub.add_parser("seed", parents=[common], help="write the fixture once")
    seed_parser.set_defaults(func=lambda a: seed(a.data_home, a.bin, a.http, a.grpc, a.mysql, a.postgres))

    run_parser = sub.add_parser("run", parents=[common], help="measure one arm")
    run_parser.add_argument(
        "--arm",
        choices=["off", "on"],
        required=True,
        help="the experimental_native_promql flag: off is the ordinary plan, on the fused one",
    )
    run_parser.add_argument(
        "--partial",
        choices=["on", "off"],
        default="off",
        help=(
            "the GREPTIME_FUSED_PARTIAL_SUM environment variable the server reads: on computes "
            "the partial SUM inside the fused node, off does not. Ignored by --arm off, and "
            "refused with it, because a flag-off arm has no fused node to compute it in. "
            "`--arm on --partial off` is the `on` arm, `--arm on --partial on` the `sum` arm"
        ),
    )
    run_parser.add_argument("--out", required=True, help="the JSON record to write")
    run_parser.add_argument("--log", required=True, help="the server log to read hits from")
    run_parser.add_argument("--commit", default="unknown", help="the commit this binary is")
    run_parser.add_argument("--profile", default="nightly", help="the Cargo profile it was built with")
    run_parser.add_argument("--warmup", type=int, default=2)
    run_parser.add_argument("--iterations", type=int, default=9)
    run_parser.add_argument(
        "--cases",
        default="",
        help="comma-separated case names to run; empty runs every case",
    )
    run_parser.add_argument(
        "--results",
        default="",
        help="directory for the per-case correctness results `compare` reads",
    )
    run_parser.add_argument(
        "--results-stem",
        default="",
        help="prefix for the saved result names; defaults to the --out file name",
    )
    run_parser.set_defaults(func=run)

    sub.add_parser("selftest", help="check the comparator with no server")

    compare_parser = sub.add_parser(
        "compare", help="check saved results of one case across arms"
    )
    compare_parser.add_argument(
        "results", nargs="+", help="the result files written by `run --results`"
    )
    compare_parser.add_argument("--case", default="", help="the case the files must hold")
    compare_parser.set_defaults(func=compare)

    args = parser.parse_args()
    if args.command == "selftest":
        return self_test()
    if args.command == "run":
        if not args.results_stem:
            args.results_stem = os.path.basename(args.out)
            if args.results_stem.endswith(".json"):
                args.results_stem = args.results_stem[: -len(".json")]
    if args.command == "compare" and len(args.results) < 2:
        raise SystemExit("compare needs at least two result files, one per arm")
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
