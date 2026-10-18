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

Two subcommands, so seeding and measuring stay separable:

    scripts/fused_series_bench.py seed --data-home DIR
    scripts/fused_series_bench.py run --data-home DIR --bin PATH --arm off --out FILE

`seed` writes the fixture once. `run` starts one server over that data directory, runs
every case, and writes a JSON record. An arm is one `(binary, flag)` pair; the driver
does not build binaries, so the caller decides which commit each arm is, and a crossed
order (L->N->F, N->F->L, F->L->N) is just three invocations per arm.

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
| First / last sample | `1700000015000` / `1700043200000` ms |
| Span | 43 185 s (11 h 59 m 45 s) |
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

## Cases

Every case is `TQL EVAL (start, end, '<step>') sum by (instance) (<expr>)` over the
seeded span, with the window given as a PromQL range in **seconds**.

| Name | Span | Step | Window | Grid points | Purpose |
| --- | --- | --- | --- | ---: | --- |
| `S` | 30 min | 1 s | 5 min | 1 800 | fine step, narrow window, short span |
| `G` | 2 h | 1 s | 5 min | 7 200 | fine step, narrow window, long span |
| `W` | 2 h | 1 s | 1 h | 7 200 | fine step, wide window |
| `C` | 2 h | 1 min | 5 min | 120 | the original business shape |
| `control` | 2 h | 1 s | 5 min | 7 200 | `max_over_time`, which never fuses |

`S` and `G` differ only in span, so the pair separates "more grid points" from "more
samples per window"; `G` and `W` differ only in window, so the pair isolates the window
width. `C` is the shape the original query actually has.

## What the record holds

Per case, per iteration: wall-clock milliseconds, output row count, response bytes, and
the number of `Fused series selection succeeded` lines the server logged **during that
iteration**, read from the byte offset the log had when the query started. A correctness
pass runs before the warmup and its hit count is recorded in the summary line only, so no
warmup or timed iteration carries another pass's hits.

`--arm off` (flag off) must report zero hits in every case; `--arm on` must report hits in
the four `rate` cases and zero in `control`. The driver fails the run otherwise, because a
comparison between two arms that both ran the ordinary plan measures nothing.

## Traps this driver encodes

- `TQL EVAL (start, end, step)` takes its start and end in **seconds**, like the PromQL
  API, while the timestamps it returns are in **milliseconds** and `INSERT` literals are
  milliseconds. Passing milliseconds to `EVAL` asks for a range about 50 000 years in the
  future: every query returns zero rows with a valid schema, and `SELECT count(*)` still
  looks healthy. The driver derives the range from `START_MS // 1000`.
- `region`, `zone` and `mode` are SQL reserved words and cannot be column names.
- `POST /v1/sql` needs form encoding; a JSON body is rejected with 415.
- Two servers need disjoint ports (`--http-addr`, `--grpc-bind-addr`, `--mysql-addr`,
  `--postgres-addr`), or they collide on the defaults.
"""

import argparse
import json
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
START_MS = 1_700_000_000_000
# `START_MS + (SAMPLE_COUNT - 1) * SAMPLE_INTERVAL_SECS * 1000` = 1700043200000.
END_MS = START_MS + (SAMPLE_COUNT - 1) * SAMPLE_INTERVAL_SECS * 1000

# The measured cases. `start`/`end` are milliseconds; `step` and the window are PromQL
# arguments, so they are seconds.
CASES = [
    {
        "name": "S",
        "expr": "rate({table}[5m])",
        "span_minutes": 30,
        "step": "1s",
        "window_seconds": 300,
        "fuses": True,
    },
    {
        "name": "G",
        "expr": "rate({table}[5m])",
        "span_minutes": 120,
        "step": "1s",
        "window_seconds": 300,
        "fuses": True,
    },
    {
        "name": "W",
        "expr": "rate({table}[1h])",
        "span_minutes": 120,
        "step": "1s",
        "window_seconds": 3_600,
        "fuses": True,
    },
    {
        "name": "C",
        "expr": "rate({table}[5m])",
        "span_minutes": 120,
        "step": "1m",
        "window_seconds": 300,
        "fuses": True,
    },
    {
        "name": "control",
        "expr": "max_over_time({table}[5m])",
        "span_minutes": 120,
        "step": "1s",
        "window_seconds": 300,
        "fuses": False,
    },
]


def log(message):
    print(message, flush=True)


class Server:
    """One standalone server over a data directory, on its own port set."""

    def __init__(self, binary, data_home, http, grpc, mysql, postgres, native, log_path):
        self.binary = binary
        self.data_home = data_home
        self.native = native
        self.http = http
        self.grpc = grpc
        self.mysql = mysql
        self.postgres = postgres
        self.log_path = log_path
        self.process = None
        self.log_file = None
        # Where the log was when the query started, so hits can be counted per query.
        self.log_offset = 0

    def __enter__(self):
        config = os.path.join(self.data_home, "..", f"fused-bench-arm-{int(self.native)}.toml")
        with open(config, "w") as handle:
            handle.write(
                'mode = "standalone"\n\n'
                "[logging]\n"
                'level = "debug"\n\n'
                "[query]\n"
                f"experimental_native_promql = {str(self.native).lower()}\n"
            )
        self.log_file = open(self.log_path, "w")
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
        rows = []
        for block in payload.get("output", []):
            records = block.get("records")
            if records:
                rows.extend(records["rows"])
        return rows

    def begin_query(self):
        """Records where the log ends, so the next query's hits can be counted alone."""
        self.log_offset = os.path.getsize(self.log_path) if os.path.exists(self.log_path) else 0

    def hits_since(self):
        """The number of fused selections logged since `begin_query`."""
        if not os.path.exists(self.log_path):
            return 0
        with open(self.log_path, "rb") as handle:
            handle.seek(self.log_offset)
            tail = handle.read()
        return tail.count(b"Fused series selection succeeded")


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
    os.makedirs(data_home, exist_ok=True)
    log_path = os.path.join(data_home, "..", "fused-bench-seed.log")
    with Server(binary, data_home, http, grpc, mysql, postgres, False, log_path) as server:
        create_table(server)
        existing = server.rows(f'SELECT count(*) FROM "{TABLE}"')[0][0]
        if existing == HOSTS * SAMPLE_COUNT:
            log(f"the fixture is already seeded: {existing} rows")
            return
        if existing:
            raise SystemExit(
                f"{TABLE} holds {existing} rows, not the {HOSTS * SAMPLE_COUNT} of the "
                "fixture; use a fresh data directory instead of mixing shapes"
            )
        log(f"seeding {HOSTS * SAMPLE_COUNT} rows into {data_home}")
        # One INSERT per host keeps each statement small enough for the HTTP endpoint while
        # writing every sample of that series in a single batch.
        for host in range(HOSTS):
            started = time.time()
            values = []
            for index in range(SAMPLE_COUNT):
                timestamp = START_MS + index * SAMPLE_INTERVAL_SECS * 1000
                # Strictly rising, and different per host so a mis-grouped series cannot hide.
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
        total = server.rows(f'SELECT count(*) FROM "{TABLE}"')[0][0]
        assert total == HOSTS * SAMPLE_COUNT, f"seeded {total} rows"


def case_window(case):
    """The `(start, end)` of one case in seconds, which is what `TQL EVAL` takes."""
    return START_MS // 1000, START_MS // 1000 + case["span_minutes"] * 60


def tql(case, aggregate=True):
    expr = case["expr"].format(table=TABLE)
    if aggregate:
        expr = f"sum by (instance) ({expr})"
    start, end = case_window(case)
    return f"TQL EVAL ({start}, {end}, '{case['step']}') {expr}"


def run(args):
    """Runs every case against one arm and writes the JSON record."""
    with Server(
        args.bin,
        args.data_home,
        args.http,
        args.grpc,
        args.mysql,
        args.postgres,
        args.arm == "on",
        args.log,
    ) as server:
        shape = server.rows(f'SELECT count(*), min("{TIME}"), max("{TIME}") FROM "{TABLE}"')[0]
        expected_rows = HOSTS * SAMPLE_COUNT
        if shape[0] != expected_rows:
            raise SystemExit(
                f"{TABLE} holds {shape[0]} rows, not the fixture's {expected_rows}; "
                "seed a fresh data directory rather than mixing shapes"
            )
        log(f"fixture: {shape[0]} rows, {shape[1]}..{shape[2]} ms")

        record = {
            "arm": args.arm,
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
                "start_ms": START_MS,
                "end_ms": END_MS,
                "rows": shape[0],
                "first_ts_ms": shape[1],
                "last_ts_ms": shape[2],
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
            # Correctness first, and its hits recorded apart from the timed ones.
            server.begin_query()
            rows = server.rows(statement)
            correctness_hits = server.hits_since()
            log(f"{case['name']}: {len(rows)} rows, {correctness_hits} hits (correctness)")

            # Warmup, then the timed runs, each with its own hit count. The three kinds of hit
            # are kept apart so a total can never be mistaken for a per-iteration count.
            warmup_hits = []
            for _ in range(args.warmup):
                server.begin_query()
                server.sql(statement)
                warmup_hits.append(server.hits_since())
            timed_hits = []
            for _ in range(args.iterations):
                server.begin_query()
                started = time.perf_counter()
                payload, size = server.sql(statement)
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                hits = server.hits_since()
                rows = sum(
                    len(block["records"]["rows"])
                    for block in payload.get("output", [])
                    if block.get("records")
                )
                timed_hits.append(hits)
                record["cases"].append(
                    {
                        "load1": os.getloadavg()[0],
                        "name": case["name"],
                        "expr": case["expr"],
                        "step": case["step"],
                        "span_minutes": case["span_minutes"],
                        "window_seconds": case["window_seconds"],
                        "tql": statement,
                        "rows": rows,
                        "bytes": size,
                        "ms": elapsed_ms,
                        "hits": hits,
                    }
                )

            timed = [c for c in record["cases"] if c["name"] == case["name"]]
            record.setdefault("hits", []).append(
                {
                    "name": case["name"],
                    "fuses": case["fuses"],
                    "correctness": correctness_hits,
                    "warmup": warmup_hits,
                    "timed": timed_hits,
                }
            )
            if args.arm == "on" and correctness_hits == 0 and case["fuses"]:
                raise SystemExit(
                    f"{case['name']} did not fuse on the flag-on arm; the comparison would "
                    "measure the ordinary plan twice"
                )
            if args.arm == "on" and correctness_hits and not case["fuses"]:
                raise SystemExit(
                    f"{case['name']} must never fuse, but it logged {correctness_hits} selections"
                )
            if len(set(timed_hits)) > 1:
                raise SystemExit(
                    f"{case['name']}: the timed hit counts differ ({timed_hits}), so the "
                    "iterations did not all run the same plan"
                )
            log(
                f"  {case['name']}: {len(timed)} timed runs, {timed[0]['rows']} rows, "
                f"hits correctness={correctness_hits} warmup={set(warmup_hits)} "
                f"timed={set(timed_hits)}, "
                f"min {min(c['ms'] for c in timed):.1f} ms"
            )

        if args.arm == "off" and any(c["hits"] for c in record["cases"]):
            raise SystemExit("the flag-off arm logged fused selections; it is not the baseline")

        with open(args.out, "w") as handle:
            json.dump(record, handle, indent=2)
        log(f"wrote {args.out}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--data-home", required=True, help="the standalone data directory")
    common.add_argument("--bin", required=True, help="the greptime binary to run")
    common.add_argument("--http", type=int, default=14090)
    common.add_argument("--grpc", type=int, default=14092)
    common.add_argument("--mysql", type=int, default=14094)
    common.add_argument("--postgres", type=int, default=14096)

    seed_parser = sub.add_parser("seed", parents=[common], help="write the fixture once")
    seed_parser.set_defaults(func=lambda a: seed(a.data_home, a.bin, a.http, a.grpc, a.mysql, a.postgres))

    run_parser = sub.add_parser("run", parents=[common], help="measure one arm")
    run_parser.add_argument("--arm", choices=["off", "on"], required=True)
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
    run_parser.set_defaults(func=run)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
